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
const Batch = exec.Batch;
const PredicateExpr = exec.PredicateExpr;

const storage = @import("../storage/storage.zig");

const ir = @import("../ir/ir.zig");

const local = @import("local.zig");
const CompileCtx = local.CompileCtx;
const Error = local.Error;

/// Look up a session variable by name. An undefined variable — one never set,
/// or cleared when the connection was reset/returned to a pool — resolves to
/// SQL NULL, matching MySQL's user-variable semantics (referencing `@x` before
/// assigning it yields NULL, not an error). The optional `null` therefore covers
/// both "never set" and "explicitly `SET @x = NULL`"; callers treat both as NULL.
fn lookupSessionVar(ctx: *CompileCtx, name: []const u8) !?@import("../types.zig").Value {
    const vars = ctx.session.vars orelse return null;
    return vars.get(name) orelse null;
}

// =============================================================================
// Entry points — invoked by compileWithSession() before the dispatcher runs.
// =============================================================================

pub fn resolveSubqueriesInOp(ctx: *CompileCtx, op: *ir.Op) anyerror!void {
    switch (op.*) {
        .scan, .file_scan, .ddl, .show, .insert, .copy, .single_row => {},
        .alias => |a| try resolveSubqueriesInOp(ctx, @constCast(a.upstream)),
        .table_fn => |t| for (t.inputs) |inp| try resolveSubqueriesInOp(ctx, inp),
        .explain => |e| try resolveSubqueriesInOp(ctx, e.inner),
        .set_var => |*sv| try resolveSubqueriesInExpr(ctx, &sv.value, null),
        .delete_op => |*d| {
            if (d.predicate) |*pred| try resolveSubqueriesInPredicate(ctx, pred);
            for (d.derived) |*x| try resolveSubqueriesInExpr(ctx, @constCast(&x.expr), null);
        },
        .update_op => |*u| {
            if (u.predicate) |*pred| try resolveSubqueriesInPredicate(ctx, pred);
            for (u.derived) |*x| try resolveSubqueriesInExpr(ctx, @constCast(&x.expr), null);
            for (u.assignments) |*a| try resolveSubqueriesInExpr(ctx, @constCast(&a.value), null);
        },
        .limit => |l| try resolveSubqueriesInOp(ctx, @constCast(l.upstream)),
        .select, .exclude => |p| try resolveSubqueriesInOp(ctx, @constCast(p.upstream)),
        .filter => try resolveFilterSubqueries(ctx, op),
        .order_by => |o| try resolveSubqueriesInOp(ctx, @constCast(o.upstream)),
        .group_by => |g| try resolveSubqueriesInOp(ctx, @constCast(g.upstream)),
        .compute => |c| {
            var lowered: LoweredScalars = .{};
            for (c.derived) |*d| try resolveSubqueriesInExpr(ctx, @constCast(&d.expr), &lowered);
            try resolveSubqueriesInOp(ctx, @constCast(c.upstream));
            if (lowered.joins.items.len > 0) {
                const compute = try newOp(ctx, .{ .compute = .{
                    .derived = c.derived,
                    .upstream = try joinLoweredScalars(ctx, @constCast(c.upstream), lowered),
                } });
                op.* = .{ .exclude = .{ .columns = lowered.hidden.items, .upstream = compute } };
            }
        },
        .join => |*j| {
            if (j.extra_predicate) |*pred| try resolveSubqueriesInPredicate(ctx, pred);
            try resolveSubqueriesInOp(ctx, @constCast(j.left));
            try resolveSubqueriesInOp(ctx, @constCast(j.right));
        },
        .materialize => |m| try resolveSubqueriesInOp(ctx, @constCast(m.upstream)),
        .batch => |b| for (b.statements) |sub| try resolveSubqueriesInOp(ctx, @constCast(sub)),
        .window => |w| {
            // Window-call args carry `@var` offsets/defaults (`LAG(x, @n, 0)`);
            // resolve them to literals before the operator reads them.
            for (w.calls) |c| for (c.args) |*arg| try resolveSubqueriesInExpr(ctx, @constCast(arg), null);
            try resolveSubqueriesInOp(ctx, @constCast(w.upstream));
        },
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
        .leaf, .day_leaf, .leaf_col_col, .is_null, .is_not_null, .like, .always, .in_set, .correlated_set, .correlated_scalar, .correlated_range, .unknown => {},
        .leaf_var => |v| {
            // `col <op> @x` where @x is SQL NULL is UNKNOWN under 3VL (matches a
            // null literal on the RHS); otherwise compare against the value.
            pred.* = if (try lookupSessionVar(ctx, v.var_name)) |resolved|
                .{ .leaf = .{ .col = v.col, .op = v.op, .val = resolved } }
            else
                .unknown;
        },
        .scalar_subquery => |sq| {
            if (try maybeResolveCorrelatedScalar(ctx, pred, sq)) return;
            pred.* = switch (try runScalarSubquery(ctx, sq.source)) {
                .value => |val| .{ .leaf = .{ .col = sq.col, .op = sq.op, .val = val } },
                .null_of => .unknown,
            };
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

/// `lowered` collects the correlated scalar subqueries of a Compute's
/// expressions for the LEFT JOIN lowering; null elsewhere.
fn resolveSubqueriesInExpr(ctx: *CompileCtx, e: *ir.Expr, lowered: ?*LoweredScalars) anyerror!void {
    switch (e.*) {
        .col_ref, .lit, .null_lit => {},
        .var_ref => |name| {
            // A variable set to SQL NULL resolves to a NULL literal; the type
            // is unknown at this point (MySQL user vars are dynamically typed),
            // so a permissive `.int` placeholder rides the null bit.
            e.* = if (try lookupSessionVar(ctx, name)) |v| .{ .lit = v } else .{ .null_lit = .int };
        },
        .call => |c| {
            // Nullary temporal functions resolve to a statement-stable
            // literal from the wall clock captured at compile time, rather
            // than a per-row scalar kernel (PG/MySQL evaluate now() once
            // per statement).
            if (c.args.len == 0) {
                if (std.ascii.eqlIgnoreCase(c.fn_name, "now") or
                    std.ascii.eqlIgnoreCase(c.fn_name, "current_timestamp") or
                    std.ascii.eqlIgnoreCase(c.fn_name, "localtimestamp") or
                    std.ascii.eqlIgnoreCase(c.fn_name, "utc_timestamp") or
                    std.ascii.eqlIgnoreCase(c.fn_name, "current_time") or
                    std.ascii.eqlIgnoreCase(c.fn_name, "curtime") or
                    std.ascii.eqlIgnoreCase(c.fn_name, "localtime"))
                {
                    e.* = .{ .lit = .{ .datetime = ctx.now_micros } };
                    return;
                }
                if (std.ascii.eqlIgnoreCase(c.fn_name, "current_date") or
                    std.ascii.eqlIgnoreCase(c.fn_name, "curdate") or
                    std.ascii.eqlIgnoreCase(c.fn_name, "utc_date"))
                {
                    e.* = .{ .lit = .{ .date = @intCast(@divFloor(ctx.now_micros, std.time.us_per_day)) } };
                    return;
                }
            }
            for (c.args) |*arg| try resolveSubqueriesInExpr(ctx, @constCast(arg), lowered);
        },
        .case => |cs| {
            for (cs.branches) |*br| {
                if (lowered) |l| try lowerPredicateScalars(ctx, @constCast(&br.cond), l);
                try resolveSubqueriesInPredicate(ctx, @constCast(&br.cond));
                try resolveSubqueriesInExpr(ctx, @constCast(&br.then), lowered);
            }
            if (cs.else_branch) |eb| try resolveSubqueriesInExpr(ctx, @constCast(eb), lowered);
        },
        .scalar_subquery => |opaque_ptr| {
            if (lowered) |l| if (try lowerCorrelatedScalar(ctx, opaque_ptr, l)) |value| {
                e.* = .{ .col_ref = value };
                return;
            };
            e.* = switch (try runScalarSubquery(ctx, opaque_ptr)) {
                .value => |val| .{ .lit = val },
                .null_of => |ty| .{ .null_lit = ty },
            };
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
    const inner: *ir.Op = @ptrCast(@alignCast(@constCast(source_opaque)));
    try resolveSubqueriesInOp(ctx, inner);

    var q = try local.compileSubplan(ctx, inner);
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
    const inner: *ir.Op = @ptrCast(@alignCast(@constCast(source_opaque)));
    try resolveSubqueriesInOp(ctx, inner);

    var q = try local.compileSubplan(ctx, inner);
    defer q.deinit();

    const schema = q.outputSchema();
    if (schema.len != 1) return Error.BadRequest;

    const aa = try ctx.subqueryArena();
    var out: std.ArrayList(Value) = .empty;

    // The materialized IN-set is resident for the rest of the query
    // (the Filter reads it), so charge it against the query budget. We
    // never release it here — it lives until the CompileCtx tears the
    // subquery arena down. The inner query's own transient buffers were
    // already accounted (and evicted) while draining it above.
    const acct = try ctx.queryAccountant();
    const per_value = @sizeOf(Value) + 32;

    while (try q.next()) |batch| {
        if (batch.row_count == 0) continue;
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

/// A scalar subquery's result: its one value, or SQL NULL of its column
/// type when that value is NULL or the subquery returns no rows.
const ScalarResult = union(enum) {
    value: Value,
    null_of: types.Type,
};

/// Compile + drain an inner Op of one column and at most one row.
/// More rows or columns → error.
fn runScalarSubquery(ctx: *CompileCtx, source_opaque: *const anyopaque) !ScalarResult {
    const inner: *ir.Op = @ptrCast(@alignCast(@constCast(source_opaque)));
    // Resolve any further-nested subqueries first.
    try resolveSubqueriesInOp(ctx, inner);

    var q = try local.compileSubplan(ctx, inner);
    defer q.deinit();

    const schema = q.outputSchema();
    if (schema.len != 1) return Error.BadRequest;

    // Operators may emit heading/trailing zero-row batches; only rows count.
    var first_batch: Batch = undefined;
    while (true) {
        first_batch = (try q.next()) orelse return .{ .null_of = schema[0].type };
        if (first_batch.row_count > 0) break;
    }
    if (first_batch.row_count != 1) return Error.BadRequest;
    while (try q.next()) |rest| {
        if (rest.row_count > 0) return Error.BadRequest; // multi-row
    }

    const view = first_batch.values[0];
    if (!view.isValid(0)) return .{ .null_of = schema[0].type };
    return .{ .value = try extractScalarValue(try ctx.subqueryArena(), view) };
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
        .json => |sv| .{ .text = try allocator.dupe(u8, sv.rowBytes(0)) },
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
        .json => |sv| .{ .text = try allocator.dupe(u8, sv.rowBytes(idx)) },
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
///
/// What an EXISTS inner selects never changes its result, so its computed
/// select items (`SELECT 1`) are skipped rather than blocking the match.
const InnerSelectList = enum { used, ignored };

fn analyzeCorrelation(ctx: *CompileCtx, inner: *ir.Op, select_list: InnerSelectList) !?CorrelationInfo {
    // Walk through Project / Exclude layers to find the underlying
    // Filter (or Scan, if there's no WHERE).
    var cur: *const ir.Op = inner;
    while (true) {
        switch (cur.*) {
            .select, .exclude => |p| cur = p.upstream,
            .compute => |c| switch (select_list) {
                .ignored => cur = c.upstream,
                .used => return null,
            },
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
        try collectConjuncts(ctx, pred, inner_schema, rangeName(scan_op), &info);
    }

    return info;
}

/// Strict "does this col-ref belong to the inner scan?" check. Unlike
/// the generic `types.findColumn` smart matcher, this rejects refs
/// whose qualifier doesn't name the inner scan — otherwise the
/// correlation analyzer would mistake `outer_alias.colname` for an
/// inner col whenever the bare column name happens to exist in the
/// inner table (very common: `region`, `id`, `created_at`, etc.).
fn refIsInnerLocal(ref: []const u8, inner_schema: TableSchema, inner_name: []const u8) bool {
    if (types.splitQualifiedName(ref)) |split| {
        if (!types.columnNameEql(split.qualifier, inner_name)) return false;
        return inner_schema.columnIndex(split.bare) != null;
    }
    return inner_schema.columnIndex(ref) != null;
}

/// The name that qualifies a scan's columns: its alias, else its table.
fn rangeName(scan: *const ir.Op.Scan) []const u8 {
    return scan.alias orelse scan.table.name;
}

fn collectConjuncts(
    ctx: *CompileCtx,
    pred: PredicateExpr,
    inner_schema: TableSchema,
    inner_name: []const u8,
    info: *CorrelationInfo,
) !void {
    switch (pred) {
        .@"and" => |children| for (children) |c| try collectConjuncts(ctx, c, inner_schema, inner_name, info),
        .leaf_col_col => |lc| {
            const left_local = refIsInnerLocal(lc.left, inner_schema, inner_name);
            const right_local = refIsInnerLocal(lc.right, inner_schema, inner_name);
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
    const aa = try ctx.subqueryArena();

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
    const inner: *ir.Op = @ptrCast(@alignCast(@constCast(source_opaque)));
    var info = (try analyzeCorrelation(ctx, inner, .ignored)) orelse return false;
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
    var q = try local.compileSubplan(ctx, rewritten);
    defer q.deinit();

    const aa = try ctx.subqueryArena();
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
    const aa = try ctx.subqueryArena();

    // Build rewritten inner. Reuse buildRewrittenInner by routing the
    // range inner col through `extra_first_col` and the equi cols as
    // info.inner_cols — that way Select projects (range_col,
    // equi_inner_cols...). We'll un-permute on drain.
    const rewritten = try buildRewrittenInner(ctx, undefined, info, range.inner_col);

    var q = try local.compileSubplan(ctx, rewritten);
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
    const inner: *ir.Op = @ptrCast(@alignCast(@constCast(s.source)));
    var info = (try analyzeCorrelation(ctx, inner, .used)) orelse return false;
    defer info.deinit(ctx.allocator);
    if (info.outer_cols.items.len == 0) return false;
    // Range correlation in IN-subquery context isn't supported yet —
    // the IN set depends on the outer range value, which can't be
    // hash-keyed. Bail; caller surfaces as unsupported.
    if (info.range_corrs.items.len > 0) return false;
    const in_col = innerSelectedColumn(inner) orelse return false;

    // Rewritten inner projects the IN column FIRST (so the outer's
    // `s.col` matches against it), then the correlation keys.
    const rewritten = try buildRewrittenInner(ctx, inner, info, in_col);

    var q = try local.compileSubplan(ctx, rewritten);
    defer q.deinit();

    const aa = try ctx.subqueryArena();
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

/// The one column an IN subquery's inner selects, named as the inner scan
/// knows it.
fn innerSelectedColumn(inner: *const ir.Op) ?[]const u8 {
    const project = switch (inner.*) {
        .select => |p| p,
        else => return null,
    };
    if (project.columns.len != 1) return null;
    const col = project.columns[0];
    if (std.mem.endsWith(u8, col, "*")) return null;
    return col;
}

// =============================================================================
// Correlated scalar subqueries in a Compute or Filter — LEFT JOIN lowering.
// =============================================================================

/// A scalar subquery that is one global aggregate over `Filter(Scan)` or
/// `Scan`, with its WHERE conjuncts sorted into correlations and kept
/// predicates. Caller owns `info`.
const ScalarAggregate = struct {
    aggs: []const ir.AggSpec,
    /// Aggregate arguments computed per inner row (`SUM(qty * price)`).
    pre: []const ir.Derived,
    /// Expressions over the aggregates (`COALESCE(SUM(x), 0)`).
    post: []const ir.Derived,
    /// The one column the subquery projects: an aggregate or a `post` name.
    selected: []const u8,
    info: CorrelationInfo,
};

fn analyzeScalarAggregate(ctx: *CompileCtx, source: *const anyopaque) !?ScalarAggregate {
    var cur: *const ir.Op = @ptrCast(@alignCast(source));
    var selected: ?[]const u8 = null;
    var post: []const ir.Derived = &.{};
    while (true) {
        switch (cur.*) {
            .select => |s| {
                if (selected) |name| {
                    selected = selectSourceColumn(s, name) orelse return null;
                } else {
                    if (s.columns.len != 1 or std.mem.endsWith(u8, s.columns[0], "*")) return null;
                    selected = s.columns[0];
                }
                cur = s.upstream;
            },
            .exclude => |e| cur = e.upstream,
            .compute => |c| {
                if (post.len > 0) return null;
                post = c.derived;
                cur = c.upstream;
            },
            .group_by => break,
            else => return null,
        }
    }
    const gb = cur.group_by;
    if (gb.aggs.len == 0 or gb.group_cols.len != 0) return null;
    if (selected == null) {
        if (gb.aggs.len != 1 or post.len > 0) return null;
        selected = gb.aggs[0].as;
    }
    if (!namesAny(gb.aggs, post, selected.?)) return null;

    var pre: []const ir.Derived = &.{};
    var below = gb.upstream;
    if (below.* == .compute) {
        pre = below.compute.derived;
        below = below.compute.upstream;
    }
    var filter_pred: ?PredicateExpr = null;
    const scan_op: *const ir.Op.Scan = switch (below.*) {
        .filter => |*f| blk: {
            filter_pred = f.predicate;
            break :blk switch (f.upstream.*) {
                .scan => |*s| s,
                else => return null,
            };
        },
        .scan => |*s| s,
        else => return null,
    };
    const catalog = local.catalogFor(ctx.db) orelse return null;
    const t = local.resolveTable(catalog, ctx.session.*, scan_op.table) catch return null;

    var info = CorrelationInfo.init();
    errdefer info.deinit(ctx.allocator);
    info.scan = scan_op;
    if (filter_pred) |p| try collectConjuncts(ctx, p, t.schema, rangeName(scan_op), &info);
    return .{ .aggs = gb.aggs, .pre = pre, .post = post, .selected = selected.?, .info = info };
}

/// The upstream column a Select output `name` reads.
fn selectSourceColumn(s: ir.Op.Project, name: []const u8) ?[]const u8 {
    for (s.columns, 0..) |col, i| {
        const output = if (s.outputs) |outs| outs[i] orelse col else col;
        if (types.columnNameEql(output, name)) return col;
    }
    return null;
}

fn namesAny(aggs: []const ir.AggSpec, post: []const ir.Derived, name: []const u8) bool {
    for (aggs) |a| if (types.columnNameEql(a.as, name)) return true;
    for (post) |d| if (types.columnNameEql(d.name, name)) return true;
    return false;
}

/// The correlated scalar subqueries lowered out of one Compute or Filter.
/// Each becomes a LEFT JOIN against its inner aggregate grouped by the
/// correlation keys: one row per key, so the join never repeats an outer
/// row, and an outer row with no inner rows misses the join. Above the
/// joins, each subquery reads as one value column.
const LoweredScalars = struct {
    joins: std.ArrayList(LoweredJoin) = .empty,
    /// Each aggregate as the outer row reads it, zero-row value on a miss.
    values: std.ArrayList(ir.Derived) = .empty,
    /// The subqueries' expressions over those values.
    post_values: std.ArrayList(ir.Derived) = .empty,
    /// Join-side and value columns, dropped once the operator has read them.
    hidden: std.ArrayList([]const u8) = .empty,
};

const LoweredJoin = struct {
    on: []const ir.JoinKeyPair,
    right: *ir.Op,
};

fn newOp(ctx: *CompileCtx, value: ir.Op) !*ir.Op {
    const op = try ctx.nodeArena().create(ir.Op);
    op.* = value;
    return op;
}

fn conjunction(ctx: *CompileCtx, preds: []const PredicateExpr) !PredicateExpr {
    if (preds.len == 1) return preds[0];
    return .{ .@"and" = try ctx.nodeArena().dupe(PredicateExpr, preds) };
}

/// Lower a correlated global-aggregate subquery into `lowered`, returning the
/// column its value reads as; null leaves any other shape to the other paths.
fn lowerCorrelatedScalar(ctx: *CompileCtx, source: *const anyopaque, lowered: *LoweredScalars) !?[]const u8 {
    var shape = (try analyzeScalarAggregate(ctx, source)) orelse return null;
    defer shape.info.deinit(ctx.allocator);
    const info = &shape.info;
    if (info.outer_cols.items.len == 0 or info.range_corrs.items.len > 0) return null;

    const na = ctx.nodeArena();
    const alias = try std.fmt.allocPrint(na, "__csq{d}", .{ctx.lowered_scalars});
    ctx.lowered_scalars += 1;

    var inner = try newOp(ctx, .{ .scan = info.scan.?.* });
    if (info.kept_predicates.items.len > 0) {
        inner = try newOp(ctx, .{ .filter = .{ .predicate = try conjunction(ctx, info.kept_predicates.items), .upstream = inner } });
    }
    if (shape.pre.len > 0) {
        inner = try newOp(ctx, .{ .compute = .{ .derived = shape.pre, .upstream = inner } });
    }
    const aggs = try na.dupe(ir.AggSpec, shape.aggs);
    for (aggs, 0..) |*a, j| a.as = try std.fmt.allocPrint(na, "__csq_a{d}", .{j});
    inner = try newOp(ctx, .{ .group_by = .{
        .group_cols = try na.dupe([]const u8, info.inner_cols.items),
        .aggs = aggs,
        .upstream = inner,
    } });

    // Output names no outer column shares, so a bare outer ref never
    // suffix-matches a join-side column.
    const n_keys = info.inner_cols.items.len;
    const columns = try na.alloc([]const u8, n_keys + aggs.len);
    const outputs = try na.alloc(?[]const u8, n_keys + aggs.len);
    const on = try na.alloc(ir.JoinKeyPair, n_keys);
    for (info.inner_cols.items, info.outer_cols.items, 0..) |inner_col, outer_col, i| {
        columns[i] = inner_col;
        outputs[i] = try std.fmt.allocPrint(na, "__csq_k{d}", .{i});
        on[i] = .{ .left = outer_col, .right = try std.fmt.allocPrint(na, "{s}.__csq_k{d}", .{ alias, i }) };
        try lowered.hidden.append(na, on[i].right);
    }
    for (aggs, n_keys..) |a, i| {
        columns[i] = a.as;
        outputs[i] = null;
    }
    inner = try newOp(ctx, .{ .select = .{ .columns = columns, .outputs = outputs, .upstream = inner } });
    inner = try newOp(ctx, .{ .materialize = .{ .upstream = inner, .structural_cse = true } });
    const right = try newOp(ctx, .{ .alias = .{ .alias = alias, .upstream = inner } });
    try resolveSubqueriesInOp(ctx, right);
    try lowered.joins.append(na, .{ .on = on, .right = right });

    // The subquery's own names for its aggregates and expressions, each
    // relabelled to the outer column that carries it.
    const renames = try na.alloc(exec.predicate.ColRename, aggs.len + shape.post.len);
    for (shape.aggs, aggs, 0..) |original, a, j| {
        const joined = try std.fmt.allocPrint(na, "{s}.{s}", .{ alias, a.as });
        const value = try std.fmt.allocPrint(na, "{s}_a{d}", .{ alias, j });
        renames[j] = .{ .from = original.as, .to = value };
        try lowered.hidden.append(na, joined);
        try lowered.hidden.append(na, value);
        try lowered.values.append(na, .{ .name = value, .expr = try missedJoinValue(ctx, a.func, joined) });
    }
    for (shape.post, aggs.len..) |d, k| {
        const value = try std.fmt.allocPrint(na, "{s}_p{d}", .{ alias, k - aggs.len });
        const expr = try exec.expr_mod.deepCloneRenamed(na, d.expr, renames[0..k]);
        renames[k] = .{ .from = d.name, .to = value };
        try lowered.hidden.append(na, value);
        try lowered.post_values.append(na, .{ .name = value, .expr = expr });
    }
    return exec.predicate.renameOf(renames, shape.selected);
}

/// An outer row that misses the join reads the aggregate over zero rows:
/// 0 for the counts, NULL for everything else.
fn missedJoinValue(ctx: *CompileCtx, func: ir.AggFunc, agg_col: []const u8) !ir.Expr {
    return switch (func) {
        .count, .count_if, .count_distinct => blk: {
            const args = try ctx.nodeArena().alloc(ir.Expr, 2);
            args[0] = .{ .col_ref = agg_col };
            args[1] = .{ .lit = .{ .bigint = 0 } };
            break :blk .{ .call = .{ .fn_name = "coalesce", .args = args } };
        },
        else => .{ .col_ref = agg_col },
    };
}

/// `input` LEFT JOINed with each lowered subquery, value columns on top.
fn joinLoweredScalars(ctx: *CompileCtx, input: *ir.Op, lowered: LoweredScalars) !*ir.Op {
    var left = input;
    for (lowered.joins.items) |j| {
        left = try newOp(ctx, .{ .join = .{
            .algorithm = .auto,
            .join_type = .left,
            .on = j.on,
            .ranges = &.{},
            .extra_predicate = null,
            .skew_ratio_threshold = 0.3,
            .skew_absolute_threshold = 20_000,
            .skew_sample_interval = 10,
            .left = left,
            .right = j.right,
        } });
    }
    const values = try newOp(ctx, .{ .compute = .{ .derived = lowered.values.items, .upstream = left } });
    if (lowered.post_values.items.len == 0) return values;
    return newOp(ctx, .{ .compute = .{ .derived = lowered.post_values.items, .upstream = values } });
}

/// Rewrite each correlated `col op (scalar subquery)` in `pred` into a
/// comparison against the lowered subquery's value column.
fn lowerPredicateScalars(ctx: *CompileCtx, pred: *PredicateExpr, lowered: *LoweredScalars) anyerror!void {
    switch (pred.*) {
        .scalar_subquery => |sq| if (try lowerCorrelatedScalar(ctx, sq.source, lowered)) |value| {
            pred.* = .{ .leaf_col_col = .{ .left = sq.col, .op = sq.op, .right = value } };
        },
        .@"and", .@"or" => |children| for (children) |*c| try lowerPredicateScalars(ctx, @constCast(c), lowered),
        .not => |child| try lowerPredicateScalars(ctx, @constCast(child), lowered),
        else => {},
    }
}

/// A Filter whose conjuncts read correlated scalar subqueries evaluates them
/// above the lowered joins; its other conjuncts stay below, where they still
/// narrow the scan.
fn resolveFilterSubqueries(ctx: *CompileCtx, op: *ir.Op) !void {
    const f = op.filter;
    const conjuncts = switch (f.predicate) {
        .@"and" => |children| try ctx.nodeArena().dupe(PredicateExpr, children),
        else => try ctx.nodeArena().dupe(PredicateExpr, &.{f.predicate}),
    };
    var lowered: LoweredScalars = .{};
    var above: std.ArrayList(PredicateExpr) = .empty;
    var below: std.ArrayList(PredicateExpr) = .empty;
    for (conjuncts) |*c| {
        const joins_before = lowered.joins.items.len;
        try lowerPredicateScalars(ctx, c, &lowered);
        try resolveSubqueriesInPredicate(ctx, c);
        const side = if (lowered.joins.items.len > joins_before) &above else &below;
        try side.append(ctx.nodeArena(), c.*);
    }
    try resolveSubqueriesInOp(ctx, @constCast(f.upstream));
    if (lowered.joins.items.len == 0) {
        op.filter.predicate = try conjunction(ctx, below.items);
        return;
    }
    var input: *ir.Op = @constCast(f.upstream);
    if (below.items.len > 0) {
        input = try newOp(ctx, .{ .filter = .{ .predicate = try conjunction(ctx, below.items), .upstream = input } });
    }
    const upper = try newOp(ctx, .{ .filter = .{
        .predicate = try conjunction(ctx, above.items),
        .upstream = try joinLoweredScalars(ctx, input, lowered),
    } });
    op.* = .{ .exclude = .{ .columns = lowered.hidden.items, .upstream = upper } };
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
    var shape = (try analyzeScalarAggregate(ctx, sq.source)) orelse return false;
    defer shape.info.deinit(ctx.allocator);
    const info = &shape.info;
    const scan_op = info.scan.?;
    if (info.outer_cols.items.len == 0) return false;
    if (shape.aggs.len != 1 or shape.pre.len > 0 or shape.post.len > 0) return false;
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
    const aa = try ctx.subqueryArena();
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
    aggs[0] = shape.aggs[0];
    const gb_new = try aa.create(ir.Op);
    gb_new.* = .{ .group_by = .{
        .group_cols = group_cols,
        .aggs = aggs,
        .upstream = upstream,
    } };

    // Drain. Output schema is [inner_correlation_keys..., agg_value].
    var q = try local.compileSubplan(ctx, gb_new);
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
