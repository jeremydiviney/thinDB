//! Pre-compile subquery resolution pass.
//!
//! Walks the IR Op tree once before plan compilation and rewrites
//! every subquery node into a constant or materialized form so the
//! Filter operator never sees `.scalar_subquery` / `.exists_subquery`
//! / `.in_subquery` predicates.
//!
//! Tier 1 — uncorrelated subqueries:
//!   - `scalar_subquery` → run inner once, freeze value → `.leaf`
//!     (PG semantics: multi-row error, multi-col error; zero rows
//!     surfaces as a future NULL extension)
//!   - `exists_subquery` → run inner once, check row_count → `.always`
//!   - `in_subquery` → drain inner's single column → `.in_set`
//!
//! Tier 2 — correlated EXISTS / IN / scalar / range:
//!   For predicates whose inner WHERE includes `inner_col op outer_col`
//!   conjuncts (one side from the FROM-table, the other not), we
//!   materialize a per-outer-key lookup table once and rewrite the
//!   predicate into the `correlated_set` / `correlated_scalar` /
//!   `correlated_range` form. The Filter then evaluates per outer
//!   row via tuple lookup or min/max compare without re-executing
//!   the inner.
//!
//! Operators never see subquery variants. If a path here returns
//! `false` or `error.UnsupportedOp`, the predicate retains its
//! subquery form and compilation errors out — surfacing the
//! unsupported shape to the user.

const std = @import("std");
const Allocator = std.mem.Allocator;

const types = @import("../types.zig");
const TableSchema = types.TableSchema;
const Value = types.Value;

const exec = @import("../exec/exec.zig");
const Query = exec.Query;
const Batch = exec.Batch;
const PredicateExpr = exec.PredicateExpr;

const storage = @import("../storage/storage.zig");

const ir = @import("../ir/ir.zig");

const local = @import("local.zig");
const CompileCtx = local.CompileCtx;
const Error = local.Error;

/// Look up a session variable by name. Returns the resolved Value or
/// errors with `Error.UnknownSessionVar` (mapped to `UnsupportedOp`
/// at the public boundary for now) when the var isn't set.
fn lookupSessionVar(ctx: *CompileCtx, name: []const u8) !@import("../types.zig").Value {
    const vars = ctx.session.vars orelse return Error.UnsupportedOp;
    return vars.get(name) orelse Error.UnsupportedOp;
}

// =============================================================================
// Entry points — invoked by compileWithSession() before the dispatcher runs.
// =============================================================================

pub fn resolveSubqueriesInOp(ctx: *CompileCtx, op: *ir.Op) anyerror!void {
    switch (op.*) {
        .scan, .file_scan, .ddl, .show, .insert, .copy, .single_row => {},
        .explain => |e| try resolveSubqueriesInOp(ctx, e.inner),
        .set_var => |*sv| try resolveSubqueriesInExpr(ctx, &sv.value),
        .delete_op => |*d| {
            if (d.predicate) |*pred| try resolveSubqueriesInPredicate(ctx, pred);
        },
        .update_op => |*u| {
            if (u.predicate) |*pred| try resolveSubqueriesInPredicate(ctx, pred);
            for (u.assignments) |*a| try resolveSubqueriesInExpr(ctx, @constCast(&a.value));
        },
        .limit => |l| try resolveSubqueriesInOp(ctx, @constCast(l.upstream)),
        .select, .exclude => |p| try resolveSubqueriesInOp(ctx, @constCast(p.upstream)),
        .filter => |*f| {
            try resolveSubqueriesInPredicate(ctx, &f.predicate);
            try resolveSubqueriesInOp(ctx, @constCast(f.upstream));
        },
        .order_by => |o| try resolveSubqueriesInOp(ctx, @constCast(o.upstream)),
        .group_by => |g| try resolveSubqueriesInOp(ctx, @constCast(g.upstream)),
        .compute => |c| {
            for (c.derived) |*d| try resolveSubqueriesInExpr(ctx, @constCast(&d.expr));
            try resolveSubqueriesInOp(ctx, @constCast(c.upstream));
        },
        .join => |*j| {
            if (j.extra_predicate) |*pred| try resolveSubqueriesInPredicate(ctx, pred);
            try resolveSubqueriesInOp(ctx, @constCast(j.left));
            try resolveSubqueriesInOp(ctx, @constCast(j.right));
        },
        .materialize => |m| try resolveSubqueriesInOp(ctx, @constCast(m.upstream)),
        .batch => |b| for (b.statements) |sub| try resolveSubqueriesInOp(ctx, @constCast(sub)),
        .window => |w| try resolveSubqueriesInOp(ctx, @constCast(w.upstream)),
        .set_union => |u| {
            try resolveSubqueriesInOp(ctx, @constCast(u.left));
            try resolveSubqueriesInOp(ctx, @constCast(u.right));
        },
        .create_table_as => |c| try resolveSubqueriesInOp(ctx, @constCast(c.source)),
        .insert_select => |i| try resolveSubqueriesInOp(ctx, @constCast(i.source)),
    }
}

fn resolveSubqueriesInPredicate(ctx: *CompileCtx, pred: *PredicateExpr) anyerror!void {
    switch (pred.*) {
        .leaf, .leaf_col_col, .is_null, .is_not_null, .like, .always, .in_set, .correlated_set, .correlated_scalar, .correlated_range => {},
        .leaf_var => |v| {
            const resolved = try lookupSessionVar(ctx, v.var_name);
            pred.* = .{ .leaf = .{ .col = v.col, .op = v.op, .val = resolved } };
        },
        .scalar_subquery => |sq| {
            if (try maybeResolveCorrelatedScalar(ctx, pred, sq)) return;
            const val = try runScalarSubquery(ctx, sq.source);
            pred.* = .{ .leaf = .{ .col = sq.col, .op = sq.op, .val = val } };
        },
        .exists_subquery => |src| {
            // Detect correlation. If the inner has any leaf_col_col
            // referencing a column outside its FROM-table, treat as
            // correlated and materialize a key set. Otherwise fall
            // back to the uncorrelated EXISTS path.
            if (try maybeResolveCorrelatedExists(ctx, pred, src, false)) return;
            const has_rows = try runExistsSubquery(ctx, src);
            pred.* = .{ .always = has_rows };
        },
        .in_subquery => |s| {
            if (try maybeResolveCorrelatedIn(ctx, pred, s)) return;
            const values = try runInSubquery(ctx, s.source);
            pred.* = .{ .in_set = .{ .col = s.col, .values = values, .negate = s.negate } };
        },
        .@"and" => |children| for (children) |*c| try resolveSubqueriesInPredicate(ctx, @constCast(c)),
        .@"or" => |children| for (children) |*c| try resolveSubqueriesInPredicate(ctx, @constCast(c)),
        .not => |child| {
            // NOT EXISTS at parse time wraps an exists_subquery in
            // a `.not`; if that exists_subquery turns out to be
            // correlated, we want the negate to apply to the
            // correlated_set rather than wrapping in NOT. Handle
            // the unwrap inline.
            if (child.* == .exists_subquery) {
                const src = child.exists_subquery;
                if (try maybeResolveCorrelatedExists(ctx, pred, src, true)) return;
                // Uncorrelated case: resolve inner, NOT the result.
                const has_rows = try runExistsSubquery(ctx, src);
                pred.* = .{ .always = !has_rows };
                return;
            }
            try resolveSubqueriesInPredicate(ctx, @constCast(child));
        },
    }
}

fn resolveSubqueriesInExpr(ctx: *CompileCtx, e: *ir.Expr) anyerror!void {
    switch (e.*) {
        .col_ref, .lit => {},
        .var_ref => |name| {
            const resolved = try lookupSessionVar(ctx, name);
            e.* = .{ .lit = resolved };
        },
        .call => |c| {
            // Nullary temporal functions resolve to a statement-stable
            // literal from the wall clock captured at compile time, rather
            // than a per-row scalar kernel (PG/MySQL evaluate now() once
            // per statement).
            if (c.args.len == 0) {
                if (std.ascii.eqlIgnoreCase(c.fn_name, "now") or
                    std.ascii.eqlIgnoreCase(c.fn_name, "current_timestamp"))
                {
                    e.* = .{ .lit = .{ .datetime = ctx.now_micros } };
                    return;
                }
                if (std.ascii.eqlIgnoreCase(c.fn_name, "current_date")) {
                    e.* = .{ .lit = .{ .date = @intCast(@divFloor(ctx.now_micros, std.time.us_per_day)) } };
                    return;
                }
            }
            for (c.args) |*arg| try resolveSubqueriesInExpr(ctx, @constCast(arg));
        },
        .case => |cs| {
            for (cs.branches) |*br| {
                try resolveSubqueriesInPredicate(ctx, @constCast(&br.cond));
                try resolveSubqueriesInExpr(ctx, @constCast(&br.then));
            }
            if (cs.else_branch) |eb| try resolveSubqueriesInExpr(ctx, @constCast(eb));
        },
        .scalar_subquery => |opaque_ptr| {
            const val = try runScalarSubquery(ctx, opaque_ptr);
            e.* = .{ .lit = val };
        },
        .exists_subquery => |opaque_ptr| {
            const has_rows = try runExistsSubquery(ctx, opaque_ptr);
            e.* = .{ .lit = .{ .boolean = has_rows } };
        },
    }
}

// =============================================================================
// Uncorrelated subquery drains.
// =============================================================================

/// Compile + drain an inner Op enough to answer "are there any rows?"
/// Pulls the first batch; if its row_count > 0 the answer is TRUE.
/// Otherwise tries one more `next()` to handle batched-empty-then-data
/// from upstream operators that emit a heading empty batch.
fn runExistsSubquery(ctx: *CompileCtx, source_opaque: *const anyopaque) !bool {
    const inner: *ir.Op = @constCast(@ptrCast(@alignCast(source_opaque)));
    try resolveSubqueriesInOp(ctx, inner);

    var q = try local.compileOp(ctx, inner);
    defer q.deinit();

    while (try q.next()) |batch| {
        if (batch.row_count > 0) return true;
    }
    return false;
}

/// Drain an IN-subquery's inner. Inner must produce exactly one column.
/// Materializes every non-NULL cell into a Value slice owned by
/// `ctx.subqueryArena()` (text values dup'd into the same arena).
/// NULL handling per thinDB dialect: NULLs are dropped from the set —
/// see [[thindb-not-in-nonstandard]] memory for the rationale.
fn runInSubquery(ctx: *CompileCtx, source_opaque: *const anyopaque) ![]const Value {
    const inner: *ir.Op = @constCast(@ptrCast(@alignCast(source_opaque)));
    try resolveSubqueriesInOp(ctx, inner);

    var q = try local.compileOp(ctx, inner);
    defer q.deinit();

    const schema = q.outputSchema();
    if (schema.len != 1) return Error.BadRequest;

    const aa = ctx.subqueryArena();
    var out: std.ArrayList(Value) = .empty;

    // The materialized IN-set is resident for the rest of the query
    // (the Filter reads it), so charge it against the query budget. We
    // never release it here — it lives until the CompileCtx tears the
    // subquery arena down. The inner query's own transient buffers were
    // already accounted (and evicted) while draining it above.
    const acct = try ctx.queryAccountant();
    const per_value = @sizeOf(Value) + 32;

    while (try q.next()) |batch| {
        const view = batch.values[0];
        var i: usize = 0;
        while (i < batch.row_count) : (i += 1) {
            if (!view.isValid(i)) continue;
            if (acct) |a| try a.reserve(.subquery, per_value);
            const v = try extractScalarValueAt(aa, view, i);
            try out.append(aa, v);
        }
    }
    return try out.toOwnedSlice(aa);
}

/// Compile + drain an inner Op, expecting exactly one row × one
/// column. Returns the extracted scalar. Multi-row → error,
/// multi-col → error, zero rows in Tier 1 also errors (NULL handling
/// in PredicateExpr.leaf isn't well-defined yet — the caller can
/// re-emit IS NULL if they want zero-or-one semantics).
fn runScalarSubquery(ctx: *CompileCtx, source_opaque: *const anyopaque) !Value {
    const inner: *ir.Op = @constCast(@ptrCast(@alignCast(source_opaque)));
    // Resolve any further-nested subqueries first.
    try resolveSubqueriesInOp(ctx, inner);

    var q = try local.compileOp(ctx, inner);
    defer q.deinit();

    const schema = q.outputSchema();
    if (schema.len != 1) return Error.BadRequest;

    const first_batch: Batch = (try q.next()) orelse return Error.BadRequest; // zero rows
    if (first_batch.row_count != 1) return Error.BadRequest;
    if (try q.next() != null) return Error.BadRequest; // multi-row

    const view = first_batch.values[0];
    return try extractScalarValue(ctx.subqueryArena(), view);
}

fn extractScalarValue(allocator: Allocator, view: storage.ColumnView) !Value {
    return switch (view.data) {
        .int => |s| .{ .int = s[0] },
        .bigint => |s| .{ .bigint = s[0] },
        .smallint => |s| .{ .smallint = s[0] },
        .tinyint => |s| .{ .tinyint = s[0] },
        .largeint => |s| .{ .largeint = s[0] },
        .float => |s| .{ .float = s[0] },
        .double => |s| .{ .double = s[0] },
        .boolean => |s| .{ .boolean = s[0] != 0 },
        .date => |s| .{ .date = s[0] },
        .datetime => |s| .{ .datetime = s[0] },
        .decimal64 => |s| .{ .decimal64 = s[0] },
        .decimal128 => |s| .{ .decimal128 = s[0] },
        .uuid => |s| .{ .uuid = s[0] },
        .varchar => |sv| .{ .text = try allocator.dupe(u8, sv.rowBytes(0)) },
        .string => |sv| .{ .text = try allocator.dupe(u8, sv.rowBytes(0)) },
        .char => |sv| .{ .text = try allocator.dupe(u8, sv.rowBytes(0)) },
    };
}

fn extractScalarValueAt(allocator: Allocator, view: storage.ColumnView, idx: usize) !Value {
    return switch (view.data) {
        .int => |s| .{ .int = s[idx] },
        .bigint => |s| .{ .bigint = s[idx] },
        .smallint => |s| .{ .smallint = s[idx] },
        .tinyint => |s| .{ .tinyint = s[idx] },
        .largeint => |s| .{ .largeint = s[idx] },
        .float => |s| .{ .float = s[idx] },
        .double => |s| .{ .double = s[idx] },
        .boolean => |s| .{ .boolean = s[idx] != 0 },
        .date => |s| .{ .date = s[idx] },
        .datetime => |s| .{ .datetime = s[idx] },
        .decimal64 => |s| .{ .decimal64 = s[idx] },
        .decimal128 => |s| .{ .decimal128 = s[idx] },
        .uuid => |s| .{ .uuid = s[idx] },
        .varchar => |sv| .{ .text = try allocator.dupe(u8, sv.rowBytes(idx)) },
        .string => |sv| .{ .text = try allocator.dupe(u8, sv.rowBytes(idx)) },
        .char => |sv| .{ .text = try allocator.dupe(u8, sv.rowBytes(idx)) },
    };
}

// =============================================================================
// Correlation analysis — common to all correlated subquery resolvers.
// =============================================================================

/// One non-equi correlation conjunct, canonicalized so the predicate
/// always reads `inner_col op outer_col`. So `outer.y < inner.x`
/// flips to `(inner_col=x, op=.gt, outer_col=y)`.
const RangeCorr = struct {
    inner_col: []const u8,
    op: exec.PredicateOp,
    outer_col: []const u8,
};

/// Flip a comparison op so swapping the operands yields the same
/// truth value: `a op b` ↔ `b flip(op) a`.
fn flipRangeOp(op: exec.PredicateOp) exec.PredicateOp {
    return switch (op) {
        .lt => .gt,
        .lte => .gte,
        .gt => .lt,
        .gte => .lte,
        .eq, .neq => op,
    };
}

/// Inner-correlation analysis result. `inner_cols` and `outer_cols`
/// are parallel: for each i, `inner_cols[i]` is the inner-side
/// column name (what the rewritten inner projects) and
/// `outer_cols[i]` is the outer-side column name (what the eventual
/// per-row lookup keys against). Equi correlations only.
const CorrelationInfo = struct {
    inner_cols: std.ArrayList([]const u8),
    outer_cols: std.ArrayList([]const u8),
    /// Range correlations — captured separately because they need
    /// per-group sorting rather than tuple hashing.
    range_corrs: std.ArrayList(RangeCorr),
    /// Non-correlation predicates that should stay in the inner's
    /// WHERE clause (col-vs-lit or col-vs-col where both sides are
    /// inner-local). Slice into the original IR — read-only.
    kept_predicates: std.ArrayList(PredicateExpr),
    /// The underlying scan we'll project from in the rewritten inner.
    scan: ?*const ir.Op.Scan = null,

    fn init() CorrelationInfo {
        return .{
            .inner_cols = .empty,
            .outer_cols = .empty,
            .range_corrs = .empty,
            .kept_predicates = .empty,
        };
    }
    fn deinit(self: *CorrelationInfo, allocator: Allocator) void {
        self.inner_cols.deinit(allocator);
        self.outer_cols.deinit(allocator);
        self.range_corrs.deinit(allocator);
        self.kept_predicates.deinit(allocator);
    }
};

/// If the inner Op fits a canonical correlated shape — Select/Project
/// wrappers on top of `Filter(AND-conjunction, Scan(T))` — analyze
/// the AND-conjuncts to extract equi-correlations. Returns null if
/// the shape doesn't match (caller falls back to the uncorrelated
/// path). Returns an empty CorrelationInfo when the shape matches
/// but there are no correlations.
fn analyzeCorrelation(ctx: *CompileCtx, inner: *ir.Op) !?CorrelationInfo {
    // Walk through Project / Exclude layers to find the underlying
    // Filter (or Scan, if there's no WHERE).
    var cur: *const ir.Op = inner;
    while (true) {
        switch (cur.*) {
            .select, .exclude => |p| cur = p.upstream,
            .filter, .scan => break,
            else => return null,
        }
    }

    var filter_pred: ?PredicateExpr = null;
    var scan_op: *const ir.Op.Scan = undefined;
    switch (cur.*) {
        .filter => |*f| {
            filter_pred = f.predicate;
            switch (f.upstream.*) {
                .scan => |*s| scan_op = s,
                else => return null,
            }
        },
        .scan => |*s| scan_op = s,
        else => return null,
    }

    const catalog = local.catalogFor(ctx.db) orelse return null;
    const t = local.resolveTable(catalog, ctx.session.*, scan_op.table) catch return null;
    const inner_schema = t.schema;

    var info = CorrelationInfo.init();
    errdefer info.deinit(ctx.allocator);
    info.scan = scan_op;

    if (filter_pred) |pred| {
        try collectConjuncts(ctx, pred, inner_schema, scan_op.alias, &info);
    }

    return info;
}

/// Strict "does this col-ref belong to the inner scan?" check. Unlike
/// the generic `types.findColumn` smart matcher, this rejects refs
/// whose qualifier doesn't match the scan's alias — otherwise the
/// correlation analyzer would mistake `outer_alias.colname` for an
/// inner col whenever the bare column name happens to exist in the
/// inner table (very common: `region`, `id`, `created_at`, etc.).
fn refIsInnerLocal(ref: []const u8, inner_schema: TableSchema, scan_alias: ?[]const u8) bool {
    if (std.mem.lastIndexOfScalar(u8, ref, '.')) |dot| {
        const qualifier = ref[0..dot];
        const tail = ref[dot + 1 ..];
        const alias = scan_alias orelse return false; // qualified ref against unaliased scan: not local
        if (!std.mem.eql(u8, qualifier, alias)) return false;
        return inner_schema.columnIndex(tail) != null;
    }
    return inner_schema.columnIndex(ref) != null;
}

fn collectConjuncts(
    ctx: *CompileCtx,
    pred: PredicateExpr,
    inner_schema: TableSchema,
    scan_alias: ?[]const u8,
    info: *CorrelationInfo,
) !void {
    switch (pred) {
        .@"and" => |children| for (children) |c| try collectConjuncts(ctx, c, inner_schema, scan_alias, info),
        .leaf_col_col => |lc| {
            const left_local = refIsInnerLocal(lc.left, inner_schema, scan_alias);
            const right_local = refIsInnerLocal(lc.right, inner_schema, scan_alias);
            if (left_local and right_local) {
                // Pure inner predicate — keep in rewritten inner.
                try info.kept_predicates.append(ctx.allocator, pred);
            } else if (left_local and !right_local) {
                if (lc.op == .eq) {
                    try info.inner_cols.append(ctx.allocator, lc.left);
                    try info.outer_cols.append(ctx.allocator, lc.right);
                } else if (lc.op == .lt or lc.op == .lte or lc.op == .gt or lc.op == .gte) {
                    try info.range_corrs.append(ctx.allocator, .{
                        .inner_col = lc.left,
                        .op = lc.op,
                        .outer_col = lc.right,
                    });
                } else {
                    return Error.UnsupportedOp;
                }
            } else if (!left_local and right_local) {
                if (lc.op == .eq) {
                    try info.inner_cols.append(ctx.allocator, lc.right);
                    try info.outer_cols.append(ctx.allocator, lc.left);
                } else if (lc.op == .lt or lc.op == .lte or lc.op == .gt or lc.op == .gte) {
                    // Flip so inner is always on the left of the op.
                    try info.range_corrs.append(ctx.allocator, .{
                        .inner_col = lc.right,
                        .op = flipRangeOp(lc.op),
                        .outer_col = lc.left,
                    });
                } else {
                    return Error.UnsupportedOp;
                }
            } else {
                // Neither side in the inner table — can't possibly
                // be a correlation we can handle.
                return Error.UnsupportedOp;
            }
        },
        // col cmp literal / IS NULL / LIKE etc. — all inner-local;
        // keep as-is. (The leaf's col is assumed inner-local; the
        // eventual compile-time validateExpr will catch typos.)
        else => try info.kept_predicates.append(ctx.allocator, pred),
    }
}

/// Build a rewritten inner Op suitable for materialization. Drops
/// correlation predicates; if `extra_first_col` is non-null, projects
/// that column first (used by IN). Otherwise projects only the
/// correlation-key columns (used by EXISTS).
fn buildRewrittenInner(
    ctx: *CompileCtx,
    _: *ir.Op,
    info: CorrelationInfo,
    extra_first_col: ?[]const u8,
) !*ir.Op {
    const aa = ctx.subqueryArena();

    // Reuse the underlying Scan; build a fresh Filter/Select chain on
    // top so we don't mutate caller IR.
    const scan_clone = try aa.create(ir.Op);
    scan_clone.* = .{ .scan = info.scan.?.* };

    // Build kept-predicate AND-conjunction if any survive.
    var upstream: *ir.Op = scan_clone;
    if (info.kept_predicates.items.len > 0) {
        const new_pred: PredicateExpr = if (info.kept_predicates.items.len == 1)
            info.kept_predicates.items[0]
        else blk: {
            const kids = try aa.alloc(PredicateExpr, info.kept_predicates.items.len);
            for (info.kept_predicates.items, kids) |src, *dst| dst.* = src;
            break :blk PredicateExpr{ .@"and" = kids };
        };
        const filter = try aa.create(ir.Op);
        filter.* = .{ .filter = .{ .predicate = new_pred, .upstream = upstream } };
        upstream = filter;
    }

    // Build projection: optional extra col first, then inner_cols.
    const n_cols = info.inner_cols.items.len + @as(usize, if (extra_first_col != null) 1 else 0);
    const cols = try aa.alloc([]const u8, n_cols);
    var ci: usize = 0;
    if (extra_first_col) |c| {
        cols[ci] = c;
        ci += 1;
    }
    for (info.inner_cols.items) |c| {
        cols[ci] = c;
        ci += 1;
    }
    const project = try aa.create(ir.Op);
    project.* = .{ .select = .{ .columns = cols, .upstream = upstream } };
    return project;
}

// =============================================================================
// Correlated EXISTS / NOT EXISTS — equi + range paths.
// =============================================================================

/// Detect + decorrelate a correlated EXISTS / NOT EXISTS inner. Returns
/// true if the inner was correlated and `pred.*` was rewritten to a
/// `.correlated_set` / `.correlated_range`; false if the inner is
/// uncorrelated (caller falls back to the Tier 2 path).
fn maybeResolveCorrelatedExists(
    ctx: *CompileCtx,
    pred: *PredicateExpr,
    source_opaque: *const anyopaque,
    negate: bool,
) !bool {
    const inner: *ir.Op = @constCast(@ptrCast(@alignCast(source_opaque)));
    var info = (try analyzeCorrelation(ctx, inner)) orelse return false;
    defer info.deinit(ctx.allocator);

    // Range-correlation path: single open-ended op, or a pair of
    // ops that form a closed BETWEEN-style range on the same inner
    // column. Larger or mixed-shape multi-range conjuncts fall back
    // to the bail path — caller surfaces them as unsupported.
    if (info.range_corrs.items.len == 1) {
        return try resolveCorrelatedExistsRange(ctx, pred, info, negate);
    }
    if (info.range_corrs.items.len == 2 and isClosedRange(info.range_corrs.items)) {
        return try resolveCorrelatedExistsRange(ctx, pred, info, negate);
    }
    if (info.range_corrs.items.len > 1) return false;
    if (info.outer_cols.items.len == 0) return false;

    // Build rewritten inner: drop correlation predicates; project the
    // inner-side correlation keys (so the materialized rows are
    // exactly the lookup-tuple values).
    const rewritten = try buildRewrittenInner(ctx, inner, info, null);

    // Drain.
    var q = try local.compileOp(ctx, rewritten);
    defer q.deinit();

    const aa = ctx.subqueryArena();
    const outer_cols_owned = try aa.alloc([]const u8, info.outer_cols.items.len);
    for (info.outer_cols.items, outer_cols_owned) |c, *dst| dst.* = try aa.dupe(u8, c);

    var rows: std.ArrayList([]const Value) = .empty;
    while (try q.next()) |batch| {
        var i: usize = 0;
        while (i < batch.row_count) : (i += 1) {
            const tuple = try aa.alloc(Value, info.outer_cols.items.len);
            var has_null = false;
            for (0..info.outer_cols.items.len) |j| {
                const view = batch.values[j];
                if (!view.isValid(i)) {
                    has_null = true;
                    break;
                }
                tuple[j] = try extractScalarValueAt(aa, view, i);
            }
            // Drop NULL-containing tuples — dialect mirrors NOT IN.
            if (has_null) continue;
            try rows.append(aa, tuple);
        }
    }
    const rows_owned = try rows.toOwnedSlice(aa);

    pred.* = .{ .correlated_set = .{
        .outer_cols = outer_cols_owned,
        .rows = rows_owned,
        .negate = negate,
    } };
    return true;
}

/// Two range conjuncts form a closed BETWEEN-style range when they
/// target the SAME inner column and have one lower-bound op (`>` /
/// `>=`) and one upper-bound op (`<` / `<=`). Caller passes the
/// raw `info.range_corrs.items` slice — must already be length 2.
fn isClosedRange(corrs: []const RangeCorr) bool {
    std.debug.assert(corrs.len == 2);
    if (!std.mem.eql(u8, corrs[0].inner_col, corrs[1].inner_col)) return false;
    const is_lower_0 = corrs[0].op == .gt or corrs[0].op == .gte;
    const is_lower_1 = corrs[1].op == .gt or corrs[1].op == .gte;
    // Exactly one of the two must be the lower-bound side.
    return is_lower_0 != is_lower_1;
}

/// Materialize a range-correlated EXISTS inner. Projects
/// `(equi_inner_cols..., range_inner_col)`, drains, buckets rows by
/// the equi-key tuple, sorts each bucket's range values ascending.
/// Per outer row the eval is then a single min/max compare for the
/// open-ended case, or a bsearch for the closed BETWEEN case.
fn resolveCorrelatedExistsRange(
    ctx: *CompileCtx,
    pred: *PredicateExpr,
    info: CorrelationInfo,
    negate: bool,
) !bool {
    // Pick the lower-bound conjunct (for `range`) and, when present,
    // the upper-bound conjunct. Open-ended ranges have only one.
    var range = info.range_corrs.items[0];
    var upper: ?RangeCorr = null;
    if (info.range_corrs.items.len == 2) {
        const a = info.range_corrs.items[0];
        const b = info.range_corrs.items[1];
        const a_is_lower = a.op == .gt or a.op == .gte;
        if (a_is_lower) {
            range = a;
            upper = b;
        } else {
            range = b;
            upper = a;
        }
    }
    const aa = ctx.subqueryArena();

    // Build rewritten inner. Reuse buildRewrittenInner by routing the
    // range inner col through `extra_first_col` and the equi cols as
    // info.inner_cols — that way Select projects (range_col,
    // equi_inner_cols...). We'll un-permute on drain.
    const rewritten = try buildRewrittenInner(ctx, undefined, info, range.inner_col);

    var q = try local.compileOp(ctx, rewritten);
    defer q.deinit();

    const n_keys = info.inner_cols.items.len;

    // First pass: drain into flat (key_tuple, range_value) rows.
    const RowEntry = struct {
        key: []Value,
        value: Value,
    };
    var rows: std.ArrayList(RowEntry) = .empty;
    defer rows.deinit(ctx.allocator);

    while (try q.next()) |batch| {
        var i: usize = 0;
        while (i < batch.row_count) : (i += 1) {
            const range_view = batch.values[0];
            if (!range_view.isValid(i)) continue;
            var any_null = false;
            const key = try aa.alloc(Value, n_keys);
            for (0..n_keys) |j| {
                const view = batch.values[1 + j];
                if (!view.isValid(i)) {
                    any_null = true;
                    break;
                }
                key[j] = try extractScalarValueAt(aa, view, i);
            }
            if (any_null) continue;
            const v = try extractScalarValueAt(aa, range_view, i);
            try rows.append(ctx.allocator, .{ .key = key, .value = v });
        }
    }

    // Group rows by equi-key tuple. We materialize a parallel
    // (keys, values_lists) pair: keys[i] is the i-th unique key
    // tuple, values_lists[i] is its growing list of range values.
    // The n_keys == 0 case (pure range, no equi correlation) collapses
    // to a single group with an empty key.
    var unique_keys: std.ArrayList([]Value) = .empty;
    defer unique_keys.deinit(ctx.allocator);
    var values_lists: std.ArrayList(std.ArrayList(Value)) = .empty;
    defer {
        for (values_lists.items) |*vl| vl.deinit(ctx.allocator);
        values_lists.deinit(ctx.allocator);
    }

    for (rows.items) |row| {
        var bucket_idx: ?usize = null;
        for (unique_keys.items, 0..) |k, gi| {
            if (keysEqual(k, row.key)) {
                bucket_idx = gi;
                break;
            }
        }
        if (bucket_idx == null) {
            try unique_keys.append(ctx.allocator, row.key);
            try values_lists.append(ctx.allocator, .empty);
            bucket_idx = unique_keys.items.len - 1;
        }
        try values_lists.items[bucket_idx.?].append(ctx.allocator, row.value);
    }

    // Snapshot each bucket into the subquery arena, sorting along the way.
    const groups_owned = try aa.alloc(exec.predicate.CorrelatedRangeGroup, unique_keys.items.len);
    for (unique_keys.items, values_lists.items, groups_owned) |k, *vl, *out| {
        std.sort.pdq(Value, vl.items, {}, valueLessThan);
        const arena_vals = try aa.alloc(Value, vl.items.len);
        @memcpy(arena_vals, vl.items);
        out.* = .{ .key = k, .values = arena_vals };
    }

    const outer_keys_owned = try aa.alloc([]const u8, info.outer_cols.items.len);
    for (info.outer_cols.items, outer_keys_owned) |c, *dst| dst.* = try aa.dupe(u8, c);

    var outer_upper_col: ?[]const u8 = null;
    var op_upper: ?exec.PredicateOp = null;
    if (upper) |u| {
        outer_upper_col = try aa.dupe(u8, u.outer_col);
        op_upper = u.op;
    }

    pred.* = .{ .correlated_range = .{
        .outer_keys = outer_keys_owned,
        .outer_range_col = try aa.dupe(u8, range.outer_col),
        .op = range.op,
        .outer_range_col_upper = outer_upper_col,
        .op_upper = op_upper,
        .groups = groups_owned,
        .negate = negate,
    } };
    return true;
}

fn keysEqual(a: []const Value, b: []const Value) bool {
    if (a.len != b.len) return false;
    for (a, b) |av, bv| {
        if (std.meta.activeTag(av) != std.meta.activeTag(bv)) return false;
        if (av.compare(bv) != .eq) return false;
    }
    return true;
}

fn valueLessThan(_: void, a: Value, b: Value) bool {
    return a.compare(b) == .lt;
}

// =============================================================================
// Correlated IN / NOT IN.
// =============================================================================

/// Detect + decorrelate a correlated IN / NOT IN inner. Returns true
/// when correlated; otherwise the caller does the uncorrelated path.
fn maybeResolveCorrelatedIn(ctx: *CompileCtx, pred: *PredicateExpr, s: anytype) !bool {
    const inner: *ir.Op = @constCast(@ptrCast(@alignCast(s.source)));
    var info = (try analyzeCorrelation(ctx, inner)) orelse return false;
    defer info.deinit(ctx.allocator);
    if (info.outer_cols.items.len == 0) return false;
    // Range correlation in IN-subquery context isn't supported yet —
    // the IN set depends on the outer range value, which can't be
    // hash-keyed. Bail; caller surfaces as unsupported.
    if (info.range_corrs.items.len > 0) return false;

    // Rewritten inner projects the IN column FIRST (so the outer's
    // `s.col` matches against it), then the correlation keys.
    const rewritten = try buildRewrittenInner(ctx, inner, info, s.col);

    var q = try local.compileOp(ctx, rewritten);
    defer q.deinit();

    const aa = ctx.subqueryArena();
    const total_cols = 1 + info.outer_cols.items.len;
    const outer_cols_owned = try aa.alloc([]const u8, total_cols);
    outer_cols_owned[0] = try aa.dupe(u8, s.col);
    for (info.outer_cols.items, 1..) |c, j| outer_cols_owned[j] = try aa.dupe(u8, c);

    var rows: std.ArrayList([]const Value) = .empty;
    while (try q.next()) |batch| {
        var i: usize = 0;
        while (i < batch.row_count) : (i += 1) {
            const tuple = try aa.alloc(Value, total_cols);
            var has_null = false;
            for (0..total_cols) |j| {
                const view = batch.values[j];
                if (!view.isValid(i)) {
                    has_null = true;
                    break;
                }
                tuple[j] = try extractScalarValueAt(aa, view, i);
            }
            if (has_null) continue;
            try rows.append(aa, tuple);
        }
    }
    const rows_owned = try rows.toOwnedSlice(aa);

    pred.* = .{ .correlated_set = .{
        .outer_cols = outer_cols_owned,
        .rows = rows_owned,
        .negate = s.negate,
    } };
    return true;
}

// =============================================================================
// Correlated scalar subquery.
// =============================================================================

/// Detect + decorrelate a correlated scalar subquery. The inner must
/// be `GroupBy([], [agg], Filter(preds, Scan(T)))` — i.e., a single
/// global aggregate with optional filter. We rewrite by promoting
/// the correlation keys into the GROUP BY, drop correlation
/// predicates, and materialize key_tuple → agg_value. Returns true
/// when correlated and pred.* was rewritten.
fn maybeResolveCorrelatedScalar(ctx: *CompileCtx, pred: *PredicateExpr, sq: anytype) !bool {
    const inner: *ir.Op = @constCast(@ptrCast(@alignCast(sq.source)));

    // Walk through Select/Project layers to find a GroupBy.
    var cur: *const ir.Op = inner;
    while (true) {
        switch (cur.*) {
            .select, .exclude => |p| cur = p.upstream,
            .group_by, .filter, .scan => break,
            else => return false,
        }
    }
    if (cur.* != .group_by) return false;

    const gb = cur.group_by;
    if (gb.aggs.len != 1) return false;
    if (gb.group_cols.len != 0) return false; // already-grouped → unsupported v1

    // Find the Filter + Scan beneath the GroupBy.
    var filter_pred: ?PredicateExpr = null;
    var scan_op: *const ir.Op.Scan = undefined;
    switch (gb.upstream.*) {
        .filter => |*f| {
            filter_pred = f.predicate;
            switch (f.upstream.*) {
                .scan => |*s| scan_op = s,
                else => return false,
            }
        },
        .scan => |*s| scan_op = s,
        else => return false,
    }

    const catalog = local.catalogFor(ctx.db) orelse return false;
    const t = local.resolveTable(catalog, ctx.session.*, scan_op.table) catch return false;
    const inner_schema = t.schema;

    var info = CorrelationInfo.init();
    defer info.deinit(ctx.allocator);
    info.scan = scan_op;
    if (filter_pred) |p| try collectConjuncts(ctx, p, inner_schema, scan_op.alias, &info);
    if (info.outer_cols.items.len == 0) return false;
    // Range correlation in scalar subquery context isn't supported
    // yet — the materialized agg can't be keyed by an open-ended
    // range, so we'd need per-row eval. Bail.
    if (info.range_corrs.items.len > 0) return false;

    // Build rewritten inner:
    //   Scan(T)
    //   └ Filter(kept_predicates)        [if any]
    //     └ GroupBy(group_cols = inner_cols, aggs = [original_agg])
    //
    // The result rows are (inner_correlation_keys..., agg_value).
    const aa = ctx.subqueryArena();
    const scan_clone = try aa.create(ir.Op);
    scan_clone.* = .{ .scan = scan_op.* };

    var upstream: *ir.Op = scan_clone;
    if (info.kept_predicates.items.len > 0) {
        const new_pred: PredicateExpr = if (info.kept_predicates.items.len == 1)
            info.kept_predicates.items[0]
        else blk: {
            const kids = try aa.alloc(PredicateExpr, info.kept_predicates.items.len);
            for (info.kept_predicates.items, kids) |src, *dst| dst.* = src;
            break :blk PredicateExpr{ .@"and" = kids };
        };
        const f = try aa.create(ir.Op);
        f.* = .{ .filter = .{ .predicate = new_pred, .upstream = upstream } };
        upstream = f;
    }
    const group_cols = try aa.alloc([]const u8, info.inner_cols.items.len);
    for (info.inner_cols.items, group_cols) |c, *dst| dst.* = c;
    const aggs = try aa.alloc(ir.AggSpec, 1);
    aggs[0] = gb.aggs[0];
    const gb_new = try aa.create(ir.Op);
    gb_new.* = .{ .group_by = .{
        .group_cols = group_cols,
        .aggs = aggs,
        .upstream = upstream,
    } };

    // Drain. Output schema is [inner_correlation_keys..., agg_value].
    var q = try local.compileOp(ctx, gb_new);
    defer q.deinit();

    const schema = q.outputSchema();
    if (schema.len != info.inner_cols.items.len + 1) return false;
    const agg_col_idx = schema.len - 1;

    const outer_keys_owned = try aa.alloc([]const u8, info.outer_cols.items.len);
    for (info.outer_cols.items, outer_keys_owned) |c, *dst| dst.* = try aa.dupe(u8, c);

    var rows: std.ArrayList(exec.predicate.CorrelatedScalarRow) = .empty;
    while (try q.next()) |batch| {
        var i: usize = 0;
        while (i < batch.row_count) : (i += 1) {
            const key = try aa.alloc(Value, info.inner_cols.items.len);
            var any_null = false;
            for (0..info.inner_cols.items.len) |j| {
                const view = batch.values[j];
                if (!view.isValid(i)) {
                    any_null = true;
                    break;
                }
                key[j] = try extractScalarValueAt(aa, view, i);
            }
            if (any_null) continue;
            const agg_view = batch.values[agg_col_idx];
            if (!agg_view.isValid(i)) continue;
            const v = try extractScalarValueAt(aa, agg_view, i);
            try rows.append(aa, .{ .key = key, .value = v });
        }
    }
    const rows_owned = try rows.toOwnedSlice(aa);

    pred.* = .{ .correlated_scalar = .{
        .outer_compared = try aa.dupe(u8, sq.col),
        .op = sq.op,
        .outer_keys = outer_keys_owned,
        .rows = rows_owned,
    } };
    return true;
}
