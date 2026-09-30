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
//!   - `in_subquery` → drain inner's single column → `.in_set`; a row
//!     value's columns → `.correlated_set` of whole tuples
//!
//! Tier 2 — correlated EXISTS / IN / scalar / range:
//!   For predicates whose inner WHERE includes `inner_col op outer_col`
//!   conjuncts (one side bound by the subquery's own scope, the other by
//!   an enclosing query's, as SQL scopes names), we materialize a
//!   per-outer-key lookup table once and rewrite the predicate into the
//!   `correlated_set` / `correlated_scalar` / `correlated_range` form.
//!   The Filter then evaluates per outer row via tuple lookup or min/max
//!   compare without re-executing the inner.
//!
//! Operators never see subquery variants. A subquery compiled on its own
//! must read nothing outside itself: one that reads an enclosing query in
//! a way these paths can't decorrelate fails with
//! `error.UnsupportedCorrelatedSubquery`, since compiled alone its outer
//! names would bind to inner columns of the same bare name.

const std = @import("std");
const Allocator = std.mem.Allocator;

const types = @import("../types.zig");
const Value = types.Value;
const Dialect = types.Dialect;

const exec = @import("../exec/exec.zig");
const Batch = exec.Batch;
const PredicateExpr = exec.PredicateExpr;
const time_fn = @import("../exec/scalar_fn_time.zig");

const storage = @import("../storage/storage.zig");

const ir = @import("../ir/ir.zig");
const parser = @import("../sql/parser.zig");

const local = @import("local.zig");
const wire_format = @import("wire_format.zig");
const mysql_handshake = @import("mysql/handshake.zig");
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
        .scan, .file_scan, .ddl, .show, .copy, .single_row, .admin => {},
        .insert => |i| try resolveDuplicateAssignments(ctx, i.on_duplicate),
        .alias => |a| try resolveSubqueriesInOp(ctx, @constCast(a.upstream)),
        .table_fn => |t| for (t.inputs) |inp| try resolveSubqueriesInOp(ctx, inp),
        .explain => |e| try resolveSubqueriesInOp(ctx, e.inner),
        .set_var => |*sv| try resolveSubqueriesInExpr(ctx, &sv.value, null),
        .delete_op => |*d| {
            if (d.source) |s| try resolveSubqueriesInOp(ctx, s);
            switch (try resolveDmlPredicate(ctx, d.table, d.predicate, d.derived, &.{})) {
                .in_place => |p| {
                    d.predicate = p.predicate;
                    d.derived = p.derived;
                },
                .rows => |rows| {
                    d.source = try dmlSource(ctx, rows, &.{});
                    d.targets = try dmlTargets(ctx, rows.table);
                    d.predicate = null;
                    d.derived = &.{};
                },
            }
        },
        .update_op => |*u| {
            // A multi-table UPDATE's values read its joined rows, not the
            // target's alone.
            const values = if (u.source) |s| blk: {
                try resolveSubqueriesInOp(ctx, s);
                for (u.assignments) |*a| try resolveSubqueriesInExpr(ctx, @constCast(&a.value), null);
                break :blk &.{};
            } else u.assignments;
            switch (try resolveDmlPredicate(ctx, u.table, u.predicate, u.derived, values)) {
                .in_place => |p| {
                    u.predicate = p.predicate;
                    u.derived = p.derived;
                },
                .rows => |rows| {
                    const assignments = try ctx.nodeArena().dupe(ir.Assignment, u.assignments);
                    u.source = try dmlSource(ctx, rows, try assignedValues(ctx, assignments));
                    u.assignments = assignments;
                    u.targets = try dmlTargets(ctx, rows.table);
                    u.predicate = null;
                    u.derived = &.{};
                },
            }
        },
        .limit => |l| try resolveSubqueriesInOp(ctx, @constCast(l.upstream)),
        .select => {
            try hoistOuterAggregates(ctx, op);
            try resolveSubqueriesInOp(ctx, op.select.upstream);
        },
        .exclude => |p| try resolveSubqueriesInOp(ctx, @constCast(p.upstream)),
        .filter => try resolveFilterSubqueries(ctx, op),
        .order_by => |o| try resolveSubqueriesInOp(ctx, @constCast(o.upstream)),
        .group_by => |g| try resolveSubqueriesInOp(ctx, @constCast(g.upstream)),
        .compute => |c| {
            // The input resolves first: a domain reads it as it will run.
            try resolveSubqueriesInOp(ctx, @constCast(c.upstream));
            var lowered: LoweredScalars = .{ .domain = .{ .input = c.upstream } };
            for (c.derived) |*d| try resolveSubqueriesInExpr(ctx, @constCast(&d.expr), &lowered);
            if (lowered.any()) {
                const compute = try newOp(ctx, .{ .compute = .{
                    .derived = c.derived,
                    .upstream = try joinLoweredScalars(ctx, lowered.domain.?.operatorInput(), lowered),
                } });
                op.* = .{ .exclude = .{ .columns = lowered.hidden.items, .upstream = compute } };
            }
        },
        .join => |*j| {
            // The inputs resolve first: a domain reads the pairs as they
            // will run.
            try resolveSubqueriesInOp(ctx, @constCast(j.left));
            try resolveSubqueriesInOp(ctx, @constCast(j.right));
            if (j.extra_predicate) |*pred| try resolveSubqueriesInPredicate(ctx, pred, null);
            if (j.residual != null) try resolveResidual(ctx, op);
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
        .insert_select => |i| {
            try resolveSubqueriesInOp(ctx, @constCast(i.source));
            try resolveDuplicateAssignments(ctx, i.on_duplicate);
        },
    }
}

/// ON DUPLICATE KEY UPDATE values are evaluated per duplicate row with only
/// the stored and incoming rows in scope, so a subquery or `@var` in one
/// binds to its literal first, as an UPDATE's assignments do.
fn resolveDuplicateAssignments(ctx: *CompileCtx, on_duplicate: ?ir.OnDuplicate) anyerror!void {
    const od = on_duplicate orelse return;
    for (od.assignments) |*a| try resolveSubqueriesInExpr(ctx, @constCast(&a.value), null);
}

fn resolveSubqueriesInPredicate(ctx: *CompileCtx, pred: *PredicateExpr, lowered: ?*LoweredScalars) anyerror!void {
    switch (pred.*) {
        .leaf, .day_leaf, .leaf_col_col, .is_null, .is_not_null, .like, .always, .in_set, .text_as_number, .text_as_number_set, .correlated_set, .correlated_scalar, .correlated_range, .unknown => {},
        .leaf_var => |v| {
            // `col <op> @x` where @x is SQL NULL is UNKNOWN under 3VL (matches a
            // null literal on the RHS); otherwise compare against the value.
            pred.* = if (try lookupSessionVar(ctx, v.var_name)) |resolved|
                .{ .leaf = .{ .col = v.col, .op = v.op, .val = resolved, .from_statement = true } }
            else
                .unknown;
        },
        .scalar_subquery => |sq| {
            if (try maybeResolveCorrelatedScalar(ctx, pred, sq)) return;
            if (lowered) |l| if (l.late_scalars) {
                try lowerPredicateScalars(ctx, pred, l);
                if (pred.* != .scalar_subquery) return;
            };
            pred.* = switch (try runScalarSubquery(ctx, sq.source)) {
                .value => |tv| .{ .leaf = .{ .col = sq.col, .op = sq.op, .val = try comparableValue(try ctx.subqueryArena(), tv), .from_statement = true } },
                .null_of => .unknown,
            };
        },
        .exists_subquery => |src| {
            if (try resolveCorrelatedBlock(ctx, pred, src, false, null, lowered)) return;
            pred.* = .{ .always = try runExistsSubquery(ctx, src) };
        },
        .in_subquery => |s| {
            if (try resolveCorrelatedBlock(ctx, pred, s.source, s.negate, s, lowered)) return;
            if (s.rest_cols.len > 0) return try resolveRowIn(ctx, pred, s);
            const drained = try runInSubquery(ctx, s.source);
            pred.* = .{ .in_set = .{ .col = s.col, .values = drained.values, .negate = s.negate, .value_type = drained.ty } };
        },
        .@"and" => |children| for (children) |*c| try resolveSubqueriesInPredicate(ctx, @constCast(c), lowered),
        .@"or" => |children| for (children) |*c| try resolveSubqueriesInPredicate(ctx, @constCast(c), lowered),
        .not => |child| {
            // NOT EXISTS at parse time wraps an exists_subquery in
            // a `.not`; if that exists_subquery turns out to be
            // correlated, we want the negate to apply to the
            // correlated_set rather than wrapping in NOT. Handle
            // the unwrap inline.
            if (child.* == .exists_subquery) {
                const src = child.exists_subquery;
                if (try resolveCorrelatedBlock(ctx, pred, src, true, null, lowered)) return;
                pred.* = .{ .always = !try runExistsSubquery(ctx, src) };
                return;
            }
            try resolveSubqueriesInPredicate(ctx, @constCast(child), lowered);
        },
    }
}

/// MySQL's information functions, answered from the session like
/// LAST_INSERT_ID(); null for any other name, or where the session has no
/// value for it. The text is copied: the session's strings change with USE
/// while a compiled query can still hold the literal.
fn sessionInfoExpr(ctx: *CompileCtx, name: []const u8) !?ir.Expr {
    const session = ctx.session.*;
    if (std.ascii.eqlIgnoreCase(name, "connection_id")) {
        const id = session.connection_id orelse return null;
        return .{ .lit = .{ .bigint = id } };
    }
    const text: []const u8 = text: {
        if (std.ascii.eqlIgnoreCase(name, "version")) break :text session.server_version orelse return null;
        // No role is ever active.
        if (std.ascii.eqlIgnoreCase(name, "current_role")) break :text "NONE";
        const user_fns = [_][]const u8{ "user", "current_user", "session_user", "system_user" };
        for (user_fns) |f| if (std.ascii.eqlIgnoreCase(name, f)) break :text session.user orelse return null;
        if (session.dialect != .mysql) return null;
        if (!std.ascii.eqlIgnoreCase(name, "database") and !std.ascii.eqlIgnoreCase(name, "schema")) return null;
        if (session.current_db == null or session.current_schema.len == 0) return .{ .null_lit = .string };
        break :text session.current_schema;
    };
    return .{ .lit = .{ .text = try (try ctx.subqueryArena()).dupe(u8, text) } };
}

/// The fraction digits a CURTIME / CURRENT_TIME / UTC_TIME call shows (its
/// literal precision argument, 0 without one); null for any other call.
fn clockTimeFsp(c: exec.expr_mod.Expr.Call) ?u8 {
    const names = [_][]const u8{ "current_time", "curtime", "utc_time" };
    for (names) |n| {
        if (!std.ascii.eqlIgnoreCase(c.fn_name, n)) continue;
        if (c.args.len == 0) return 0;
        if (c.args.len != 1 or c.args[0] != .lit) return null;
        return switch (c.args[0].lit) {
            inline .tinyint, .smallint, .int, .bigint => |fsp| if (fsp >= 0 and fsp <= 6) @intCast(fsp) else null,
            else => null,
        };
    }
    return null;
}

/// The microseconds in one step of each precision, 0 to 6 fraction digits.
const FSP_STEP_MICROS = [_]i64{ 1_000_000, 100_000, 10_000, 1_000, 100, 10, 1 };

/// The fraction digits a NOW / CURRENT_TIMESTAMP / SYSDATE call keeps: its
/// literal precision argument, else 0 in MySQL, whose bare forms give whole
/// seconds, and 6 in the other dialects; null for any other call.
fn timestampFsp(dialect: Dialect, c: exec.expr_mod.Expr.Call) ?u8 {
    const names = [_][]const u8{ "now", "current_timestamp", "localtimestamp", "localtime", "utc_timestamp", "sysdate" };
    for (names) |n| {
        if (!std.ascii.eqlIgnoreCase(c.fn_name, n)) continue;
        if (c.args.len == 0) return if (dialect == .mysql) 0 else 6;
        if (c.args.len != 1 or c.args[0] != .lit) return null;
        return switch (c.args[0].lit) {
            inline .tinyint, .smallint, .int, .bigint => |fsp| if (fsp >= 0 and fsp <= 6) @intCast(fsp) else null,
            else => null,
        };
    }
    return null;
}

/// CHARSET or COLLATION of a system function's result (`CHARSET(VERSION())`),
/// which MySQL gives as utf8mb3 text. It's answered before the function
/// becomes a literal, whose text is utf8mb4 like any other; null for any
/// other call.
fn systemTextTypeName(c: exec.expr_mod.Expr.Call) ?[]const u8 {
    if (c.args.len != 1 or c.args[0] != .call) return null;
    const name: []const u8 = if (std.ascii.eqlIgnoreCase(c.fn_name, "charset"))
        "utf8mb3"
    else if (std.ascii.eqlIgnoreCase(c.fn_name, "collation"))
        "utf8mb3_general_ci"
    else
        return null;
    const arg = c.args[0].call;
    if (std.mem.eql(u8, arg.fn_name, exec.scalar_fn.SYSTEM_VARIABLE_FN))
        return if (systemVariableValue(arg, "") catch null) |v| (if (v == .text) name else null) else null;
    const system_fns = [_][]const u8{ "version", "database", "schema", "user", "current_user", "session_user", "system_user", "current_role", "uuid", "charset", "collation" };
    for (system_fns) |f| if (std.ascii.eqlIgnoreCase(arg.fn_name, f)) return name;
    return null;
}

/// The value `@@name` (a `SYSTEM_VARIABLE_FN` call) stands for: what thinDB
/// reports for the variable, an integer when it reads as one
/// (`@@auto_increment_increment`), else its text. Null for any other call.
fn systemVariableValue(c: exec.expr_mod.Expr.Call, current_schema: []const u8) error{NameTooLong}!?types.Value {
    if (!std.mem.eql(u8, c.fn_name, exec.scalar_fn.SYSTEM_VARIABLE_FN) or c.args.len != 1 or c.args[0] != .lit or c.args[0].lit != .text) return null;
    var buf: [128]u8 = undefined;
    const typed = c.args[0].lit.text;
    if (typed.len > buf.len) return error.NameTooLong;
    const text = mysql_handshake.systemVariableValue(std.ascii.lowerString(&buf, typed), current_schema);
    if (std.fmt.parseInt(i64, text, 10)) |n| return .{ .bigint = n } else |_| return .{ .text = text };
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
            if (clockTimeFsp(c)) |fsp| {
                // thinDB has no TIME type: a TIME is its text. The clock is
                // UTC, so the session's time of day is UTC's.
                var buf: [32]u8 = undefined;
                const text = try time_fn.clockTime(&buf, ctx.now_micros, fsp);
                e.* = .{ .lit = .{ .text = try (try ctx.subqueryArena()).dupe(u8, text) } };
                return;
            }
            if (timestampFsp(ctx.session.dialect, c)) |fsp| {
                // Truncated, not rounded, to the precision, as MySQL does.
                e.* = .{ .lit = .{ .datetime = ctx.now_micros - @mod(ctx.now_micros, FSP_STEP_MICROS[fsp]) } };
                return;
            }
            if (c.args.len == 0) {
                if (std.ascii.eqlIgnoreCase(c.fn_name, "current_date") or
                    std.ascii.eqlIgnoreCase(c.fn_name, "curdate") or
                    std.ascii.eqlIgnoreCase(c.fn_name, "utc_date"))
                {
                    e.* = .{ .lit = .{ .date = @intCast(@divFloor(ctx.now_micros, std.time.us_per_day)) } };
                    return;
                }
                if (std.ascii.eqlIgnoreCase(c.fn_name, "last_insert_id")) {
                    e.* = .{ .lit = .{ .bigint = std.math.cast(i64, ctx.session.last_insert_id) orelse std.math.maxInt(i64) } };
                    return;
                }
                if (std.ascii.eqlIgnoreCase(c.fn_name, "row_count")) {
                    e.* = .{ .lit = .{ .bigint = ctx.session.row_count } };
                    return;
                }
                if (try sessionInfoExpr(ctx, c.fn_name)) |info| {
                    e.* = info;
                    return;
                }
                if (std.ascii.eqlIgnoreCase(c.fn_name, "uuid_short")) {
                    const args = try (try ctx.subqueryArena()).alloc(ir.Expr, 1);
                    args[0] = .{ .lit = .{ .bigint = @divFloor(ctx.now_micros, std.time.us_per_s) } };
                    e.* = .{ .call = .{ .fn_name = exec.scalar_fn.UUID_SHORT_FN, .args = args } };
                    return;
                }
                if (std.ascii.eqlIgnoreCase(c.fn_name, "uuid")) {
                    // Kernels carry no Io, so UUID() takes its entropy as a
                    // fresh per-statement seed from the database's.
                    var seed: [8]u8 = undefined;
                    ctx.catalog.io.random(&seed);
                    const args = try (try ctx.subqueryArena()).alloc(ir.Expr, 1);
                    args[0] = .{ .lit = .{ .bigint = std.mem.readInt(i64, &seed, .little) } };
                    e.* = .{ .call = .{ .fn_name = c.fn_name, .args = args } };
                    return;
                }
            }
            if (systemTextTypeName(c)) |text| {
                e.* = .{ .lit = .{ .text = text } };
                return;
            }
            if (systemVariableValue(c, ctx.session.current_schema) catch null) |v| {
                e.* = .{ .lit = if (v == .text) .{ .text = try (try ctx.subqueryArena()).dupe(u8, v.text) } else v };
                return;
            }
            for (c.args) |*arg| try resolveSubqueriesInExpr(ctx, @constCast(arg), lowered);
        },
        .case => |cs| {
            for (cs.operands) |*o| try resolveSubqueriesInExpr(ctx, @constCast(&o.expr), lowered);
            for (cs.branches) |*br| {
                if (lowered) |l| try lowerPredicateScalars(ctx, @constCast(&br.cond), l);
                try resolveSubqueriesInPredicate(ctx, @constCast(&br.cond), lowered);
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
                .value => |tv| try valueExpr(try ctx.subqueryArena(), tv),
                .null_of => |ty| .{ .null_lit = ty },
            };
        },
        .exists_subquery => |opaque_ptr| {
            // Read as a CASE condition, a correlated EXISTS decorrelates as
            // one in a WHERE does.
            const na = ctx.nodeArena();
            const branches = try na.alloc(exec.expr_mod.Expr.Branch, 1);
            branches[0] = .{ .cond = .{ .exists_subquery = opaque_ptr }, .then = .{ .lit = .{ .boolean = true } } };
            try resolveSubqueriesInPredicate(ctx, &branches[0].cond, lowered);
            if (branches[0].cond == .always) {
                e.* = .{ .lit = .{ .boolean = branches[0].cond.always } };
                return;
            }
            const no_rows = try na.create(ir.Expr);
            no_rows.* = .{ .lit = .{ .boolean = false } };
            e.* = .{ .case = .{ .branches = branches, .else_branch = no_rows } };
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
    try prepareSubplan(ctx, inner);

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
/// An IN subquery's drained values, with the type of the column they came
/// from.
const DrainedSet = struct {
    values: []const Value,
    ty: types.Type,
};

fn runInSubquery(ctx: *CompileCtx, source_opaque: *const anyopaque) !DrainedSet {
    const inner: *ir.Op = @ptrCast(@alignCast(@constCast(source_opaque)));
    try prepareSubplan(ctx, inner);

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
            try out.append(aa, try extractKeyValueAt(aa, view, schema[0].type, i));
        }
    }
    return .{ .values = try out.toOwnedSlice(aa), .ty = schema[0].type };
}

/// `(a, b) IN (SELECT x, y ...)`, uncorrelated: the inner's rows become one
/// tuple set.
fn resolveRowIn(ctx: *CompileCtx, pred: *PredicateExpr, s: exec.predicate.InSubquery) !void {
    const inner: *ir.Op = @ptrCast(@alignCast(@constCast(s.source)));
    try prepareSubplan(ctx, inner);

    var q = try local.compileSubplan(ctx, inner);
    defer q.deinit();
    const width = 1 + s.rest_cols.len;
    if (q.outputSchema().len != width) return Error.BadRequest;

    const aa = try ctx.subqueryArena();
    const outer_cols = try aa.alloc([]const u8, width);
    outer_cols[0] = try aa.dupe(u8, s.col);
    for (s.rest_cols, outer_cols[1..]) |c, *dst| dst.* = try aa.dupe(u8, c);
    pred.* = try inTupleSet(aa, .{
        .outer_cols = outer_cols,
        .rows = try drainTuples(ctx, &q, width),
        .negate = s.negate,
        .inner_types = try columnTypes(aa, q.outputSchema()),
    }, width);
}

/// The inner's rows as `width`-value tuples in key order
/// (`exec.predicate.sortKeyed`); a tuple holding a NULL can never match, so
/// it drops (the thinDB IN-set dialect).
fn drainTuples(ctx: *CompileCtx, q: anytype, width: usize) ![]const []const Value {
    const aa = try ctx.subqueryArena();
    const acct = try ctx.queryAccountant();
    const per_tuple = width * (@sizeOf(Value) + 32);
    var rows: std.ArrayList([]const Value) = .empty;
    while (try q.next()) |batch| {
        var i: usize = 0;
        next_row: while (i < batch.row_count) : (i += 1) {
            for (batch.values[0..width]) |view| if (!view.isValid(i)) continue :next_row;
            if (acct) |a| try a.reserve(.subquery, per_tuple);
            const tuple = try aa.alloc(Value, width);
            for (tuple, batch.values[0..width], q.outputSchema()[0..width]) |*v, view, column| {
                v.* = try extractKeyValueAt(aa, view, column.type, i);
            }
            try rows.append(aa, tuple);
        }
    }
    const owned = try rows.toOwnedSlice(aa);
    exec.predicate.sortKeyed([]const Value, owned);
    return owned;
}

/// A probe of a tuple set whose first `in_width` outer columns are an IN's
/// compared side (none for EXISTS).
fn inTupleSet(aa: Allocator, set: exec.predicate.CorrelatedSet, in_width: usize) !PredicateExpr {
    return nullGuarded(aa, set.outer_cols[0..in_width], set.negate, .{ .correlated_set = set });
}

/// A NULL among IN's compared columns never matches either way, as for a
/// single-column IN set, so NOT IN keeps those rows out.
fn nullGuarded(aa: Allocator, in_cols: []const []const u8, negate: bool, probe: PredicateExpr) !PredicateExpr {
    if (!negate or in_cols.len == 0) return probe;
    const kids = try aa.alloc(PredicateExpr, in_cols.len + 1);
    for (in_cols, kids[0..in_cols.len]) |c, *kid| kid.* = .{ .is_not_null = c };
    kids[in_cols.len] = probe;
    return .{ .@"and" = kids };
}

/// The types of `columns`, in the subquery arena.
fn columnTypes(aa: Allocator, columns: []const types.Column) ![]const types.Type {
    const out = try aa.alloc(types.Type, columns.len);
    for (columns, out) |c, *t| t.* = c.type;
    return out;
}

/// A scalar subquery's result: its one value, or SQL NULL of its column
/// type when that value is NULL or the subquery returns no rows.
const ScalarResult = union(enum) {
    value: TypedValue,
    null_of: types.Type,
};

/// A decimal `Value` carries only its mantissa, so a subquery value keeps
/// its column type until it reaches a consumer that can place it.
const TypedValue = struct {
    val: Value,
    ty: types.Type,
};

/// The value as a predicate literal. A decimal travels as its exact digits:
/// validation parses numeric text onto the compared column's own scale.
fn comparableValue(arena: Allocator, tv: TypedValue) !Value {
    const mantissa: i128 = switch (tv.val) {
        .decimal64 => |m| m,
        .decimal128 => |m| m,
        else => return tv.val,
    };
    var text: std.ArrayList(u8) = .empty;
    try wire_format.formatDecimal(arena, &text, mantissa, tv.ty);
    return .{ .text = try text.toOwnedSlice(arena) };
}

/// The value as an expression: a decimal re-enters through a cast of its
/// exact digits to its own type, since a bare decimal literal has no scale.
fn valueExpr(arena: Allocator, tv: TypedValue) !ir.Expr {
    if (!tv.ty.isDecimal()) return .{ .lit = tv.val };
    const args = try arena.alloc(ir.Expr, 1);
    args[0] = .{ .lit = try comparableValue(arena, tv) };
    const fn_name = (try exec.scalar_fn.castFnName(arena, tv.ty)) orelse return Error.TypeMismatch;
    return .{ .call = .{ .fn_name = fn_name, .args = args } };
}

/// Compile + drain an inner Op of one column and at most one row.
/// More rows or columns → error.
fn runScalarSubquery(ctx: *CompileCtx, source_opaque: *const anyopaque) !ScalarResult {
    const inner: *ir.Op = @ptrCast(@alignCast(@constCast(source_opaque)));
    try prepareSubplan(ctx, inner);

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
    if (first_batch.row_count != 1) return error.SubqueryMultipleRows;
    while (try q.next()) |rest| {
        if (rest.row_count > 0) return error.SubqueryMultipleRows;
    }

    const view = first_batch.values[0];
    if (!view.isValid(0)) return .{ .null_of = schema[0].type };
    return .{ .value = .{ .val = try extractScalarValue(try ctx.subqueryArena(), view), .ty = schema[0].type } };
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

/// A correlation key or IN value as a predicate literal: validation brings
/// it to the outer column's type (see `comparableValue`).
fn extractKeyValueAt(allocator: Allocator, view: storage.ColumnView, ty: types.Type, idx: usize) !Value {
    return comparableValue(allocator, .{ .val = try extractScalarValueAt(allocator, view, idx), .ty = ty });
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
// Name scope: which names a subquery block binds itself.
// =============================================================================

/// How deep scope analysis follows nested relations.
const SCOPE_DEPTH_LIMIT = 64;

/// How many operators one column enumeration visits. CTE bodies are shared
/// subtrees, so walking them as a tree can cost far more than the DAG.
const SCOPE_WALK_BUDGET = 4096;

/// A relation a block's FROM names. `columns` lists every name its rows
/// could carry (a superset), null when they can't be enumerated here.
const Range = struct {
    name: ?[]const u8,
    columns: ?[]const []const u8,
};

/// The names a subquery block binds itself: the relations its FROM names and
/// the columns its operators compute. Any other name comes from an
/// enclosing query.
const Scope = struct {
    ranges: []const Range,
    derived: []const []const u8,
    /// Computed columns that read only the enclosing row, handed to the
    /// enclosing query to compute (`from`, the block's own name).
    outer: []const exec.predicate.ColRename = &.{},

    /// Whether `ref` binds inside the block, as SQL scopes names: a qualified
    /// name by its qualifier, an unqualified one to the innermost block that
    /// has the column. A relation whose name or columns are unknown could
    /// bind anything, so it claims every such reference.
    fn binds(self: Scope, ref: []const u8) bool {
        for (self.outer) |r| if (types.columnNameEql(r.from, ref)) return false;
        for (self.derived) |name| if (types.columnNameEql(name, ref)) return true;
        if (types.splitQualifiedName(ref)) |split| {
            const qualifier = lastSegment(split.qualifier);
            for (self.ranges) |r| {
                const name = r.name orelse return true;
                if (types.columnNameEql(name, qualifier)) return true;
            }
            return false;
        }
        for (self.ranges) |r| {
            const columns = r.columns orelse return true;
            for (columns) |c| if (types.columnNameEql(types.unqualifiedName(c), ref)) return true;
        }
        return false;
    }

    /// Whether `ref` names a column of a relation the block reads, not one
    /// it computes.
    fn bindsRelation(self: Scope, ref: []const u8) bool {
        for (self.derived) |name| if (types.columnNameEql(name, ref)) return false;
        return self.binds(ref);
    }

    /// The scope of the operators above a select, where its output names
    /// bind too. An item named by its text reads like a qualified column
    /// (`x.k + z.id` as column `k + z.id` of `x`), so only its select
    /// tells it apart.
    fn above(self: Scope, na: Allocator, p: ir.Op.Project) Allocator.Error!Scope {
        const outputs = p.outputs orelse return self;
        var derived: std.ArrayList([]const u8) = .empty;
        try derived.appendSlice(na, self.derived);
        for (outputs) |output| if (output) |o| try derived.append(na, o);
        var copy = self;
        copy.derived = derived.items;
        return copy;
    }
};

/// The relation name that ends a qualifier (`db.t` → `t`).
fn lastSegment(qualifier: []const u8) []const u8 {
    const dot = std.mem.lastIndexOfScalar(u8, qualifier, '.') orelse return qualifier;
    return qualifier[dot + 1 ..];
}

/// A query block: its operators from the top down, over the relation its
/// FROM reads.
const Block = struct {
    chain: []const *const ir.Op,
    from: *const ir.Op,
};

fn splitBlock(ctx: *CompileCtx, top: *const ir.Op) !Block {
    var chain: std.ArrayList(*const ir.Op) = .empty;
    var cur = top;
    while (blockUpstream(cur)) |upstream| {
        try chain.append(ctx.nodeArena(), cur);
        cur = upstream;
    }
    return .{ .chain = chain.items, .from = cur };
}

/// The upstream of an operator that sits in a block above its FROM; null for
/// a relation.
fn blockUpstream(op: *const ir.Op) ?*const ir.Op {
    return switch (op.*) {
        .limit => |l| l.upstream,
        .select, .exclude => |p| p.upstream,
        .order_by => |o| o.upstream,
        .compute => |c| c.upstream,
        .window => |w| w.upstream,
        .filter => |f| f.upstream,
        .group_by => |g| g.upstream,
        else => null,
    };
}

const ScopeBuilder = struct {
    ctx: *CompileCtx,
    /// Enumerate each relation's columns. A scope that only binds qualified
    /// names needs the relations' names alone.
    with_columns: bool,
    ranges: std.ArrayList(Range) = .empty,
    derived: std.ArrayList([]const u8) = .empty,

    fn build(ctx: *CompileCtx, block: Block, with_columns: bool) !Scope {
        var builder: ScopeBuilder = .{ .ctx = ctx, .with_columns = with_columns };
        try builder.relation(block.from, 0);
        for (block.chain) |op| try builder.computed(op);
        return .{ .ranges = builder.ranges.items, .derived = builder.derived.items };
    }

    fn relation(self: *ScopeBuilder, op: *const ir.Op, depth: u32) Allocator.Error!void {
        const na = self.ctx.nodeArena();
        if (depth >= SCOPE_DEPTH_LIMIT) return self.ranges.append(na, .{ .name = null, .columns = null });
        switch (op.*) {
            .scan => |s| try self.ranges.append(na, .{
                .name = s.alias orelse s.table.name,
                .columns = if (self.with_columns) try tableColumns(self.ctx, s.table) else null,
            }),
            .alias => |a| try self.ranges.append(na, .{ .name = a.alias, .columns = try self.columnsOf(a.upstream) }),
            .materialize => |m| try self.ranges.append(na, .{ .name = m.name, .columns = try self.columnsOf(m.upstream) }),
            .table_fn => |t| try self.ranges.append(na, .{
                .name = t.alias orelse t.name,
                .columns = if (self.with_columns) try tableFnColumns(self.ctx, t) else null,
            }),
            .file_scan => |f| try self.ranges.append(na, .{ .name = f.alias, .columns = null }),
            .single_row => {},
            .join => |j| {
                try self.relation(j.left, depth + 1);
                try self.relation(j.right, depth + 1);
                if (j.residual) |r| for (r.derived) |d| try self.derived.append(na, d.name);
            },
            else => if (blockUpstream(op)) |upstream| {
                // A join input the block's own operators wrap, as a lowered
                // scalar subquery's join reads the rows below it.
                try self.computed(op);
                try self.relation(upstream, depth + 1);
            } else try self.ranges.append(na, .{ .name = null, .columns = null }),
        }
    }

    fn computed(self: *ScopeBuilder, op: *const ir.Op) !void {
        const na = self.ctx.nodeArena();
        switch (op.*) {
            .compute => |c| for (c.derived) |d| try self.derived.append(na, d.name),
            .window => |w| for (w.calls) |call| try self.derived.append(na, call.output_name),
            .group_by => |g| for (g.aggs) |a| try self.derived.append(na, a.as),
            else => {},
        }
    }

    fn columnsOf(self: *ScopeBuilder, op: *const ir.Op) !?[]const []const u8 {
        if (!self.with_columns) return null;
        var budget: u32 = SCOPE_WALK_BUDGET;
        return try outputColumns(self.ctx, op, 0, &budget);
    }
};

/// Every name a relation's rows could carry, a superset of its output
/// names, or null when they can't be enumerated here. A missing name would
/// let an inner column pass for an outer one, so any doubt widens the list
/// or answers null.
fn outputColumns(ctx: *CompileCtx, op: *const ir.Op, depth: u32, budget: *u32) Allocator.Error!?[]const []const u8 {
    if (depth >= SCOPE_DEPTH_LIMIT or budget.* == 0) return null;
    budget.* -= 1;
    const na = ctx.nodeArena();
    var names: std.ArrayList([]const u8) = .empty;
    switch (op.*) {
        .scan => |s| return try tableColumns(ctx, s.table),
        .table_fn => |t| return try tableFnColumns(ctx, t),
        .single_row => return &.{},
        .alias => |a| return try outputColumns(ctx, a.upstream, depth + 1, budget),
        .materialize => |m| return try outputColumns(ctx, m.upstream, depth + 1, budget),
        .set_union => |u| return try outputColumns(ctx, u.left, depth + 1, budget),
        .limit, .order_by, .filter, .exclude => return try outputColumns(ctx, blockUpstream(op).?, depth + 1, budget),
        .select => |p| for (p.columns, 0..) |c, i| {
            if (isStar(c)) {
                const upstream = try outputColumns(ctx, p.upstream, depth + 1, budget) orelse return null;
                try names.appendSlice(na, upstream);
                continue;
            }
            const output = if (p.outputs) |outs| (if (i < outs.len) outs[i] else null) else null;
            try names.append(na, output orelse c);
        },
        .compute => |c| {
            try names.appendSlice(na, try outputColumns(ctx, c.upstream, depth + 1, budget) orelse return null);
            for (c.derived) |d| try names.append(na, d.name);
        },
        .window => |w| {
            try names.appendSlice(na, try outputColumns(ctx, w.upstream, depth + 1, budget) orelse return null);
            for (w.calls) |call| try names.append(na, call.output_name);
        },
        .group_by => |g| {
            try names.appendSlice(na, g.group_cols);
            for (g.aggs) |a| try names.append(na, a.as);
        },
        .join => |j| {
            try names.appendSlice(na, try outputColumns(ctx, j.left, depth + 1, budget) orelse return null);
            try names.appendSlice(na, try outputColumns(ctx, j.right, depth + 1, budget) orelse return null);
        },
        else => return null,
    }
    return names.items;
}

fn isStar(column: []const u8) bool {
    return std.mem.eql(u8, column, "*") or std.mem.endsWith(u8, column, ".*");
}

fn tableColumns(ctx: *CompileCtx, ref: ir.TableRef) Allocator.Error!?[]const []const u8 {
    const table = local.resolveTable(ctx.catalog, ctx.session.*, ref) catch return null;
    const names = try ctx.nodeArena().alloc([]const u8, table.schema.columns.len);
    for (table.schema.columns, names) |col, *name| name.* = col.name;
    return names;
}

fn tableFnColumns(ctx: *CompileCtx, t: ir.Op.TableFn) Allocator.Error!?[]const []const u8 {
    const registry = ctx.udf_registry orelse return null;
    const entry = registry.tableByName(t.name) orelse return null;
    const names = try ctx.nodeArena().alloc([]const u8, entry.output_schema.len);
    for (entry.output_schema, names) |col, *name| name.* = col.name;
    return names;
}

/// Where the names a block reads bind: inside it (`inner`), or in an
/// enclosing query (`outer`).
const RefSides = struct {
    scope: Scope,
    /// Count qualified names only. Compiled alone, an unqualified name binds
    /// to the block's own column when it has one, as SQL scopes it, and
    /// fails as unknown otherwise; only a qualified outer name can bind to
    /// the wrong column.
    qualified_only: bool = false,
    /// The operands of the CASE whose condition is being read: names the
    /// CASE makes up, not columns.
    case_operands: []const exec.expr_mod.Expr.Operand = &.{},
    inner: bool = false,
    outer: bool = false,
    /// A nested subquery was read, whose own names aren't counted here.
    nested: bool = false,

    fn readName(self: *RefSides, ref: []const u8) void {
        if (ref.len == 0 or exec.expr_mod.operandListed(self.case_operands, ref)) return;
        if (self.qualified_only and types.splitQualifiedName(ref) == null) return;
        if (self.scope.binds(ref)) self.inner = true else self.outer = true;
    }

    /// A subquery marker reads its compared columns; its own block is read
    /// once it resolves.
    fn readPredicate(self: *RefSides, pred: PredicateExpr) void {
        switch (pred) {
            .leaf, .day_leaf, .text_as_number => |l| self.readName(l.col),
            .leaf_col_col => |c| {
                self.readName(c.left);
                self.readName(c.right);
            },
            .is_null, .is_not_null => |col| self.readName(col),
            .like => |l| self.readName(l.col),
            .in_set, .text_as_number_set => |s| self.readName(s.col),
            .leaf_var => |v| self.readName(v.col),
            .scalar_subquery => |s| {
                self.nested = true;
                self.readName(s.col);
            },
            .in_subquery => |s| {
                self.nested = true;
                self.readName(s.col);
                for (s.rest_cols) |col| self.readName(col);
            },
            .correlated_set => |s| for (s.outer_cols) |col| self.readName(col),
            .correlated_scalar => |s| {
                self.readName(s.outer_compared);
                for (s.outer_keys) |col| self.readName(col);
            },
            .correlated_range => |r| {
                for (r.outer_keys) |col| self.readName(col);
                self.readName(r.outer_range_col);
                if (r.outer_range_col_upper) |col| self.readName(col);
            },
            .@"and", .@"or" => |children| for (children) |child| self.readPredicate(child),
            .not => |child| self.readPredicate(child.*),
            .exists_subquery => self.nested = true,
            .always, .unknown => {},
        }
    }

    fn readExpr(self: *RefSides, e: ir.Expr) void {
        switch (e) {
            .col_ref => |name| self.readName(name),
            .call => |c| for (c.args) |arg| self.readExpr(arg),
            .case => |cs| {
                for (cs.operands) |o| self.readExpr(o.expr);
                const enclosing = self.case_operands;
                self.case_operands = cs.operands;
                for (cs.branches) |br| self.readPredicate(br.cond);
                self.case_operands = enclosing;
                for (cs.branches) |br| self.readExpr(br.then);
                if (cs.else_branch) |eb| self.readExpr(eb.*);
            },
            .scalar_subquery, .exists_subquery => self.nested = true,
            .lit, .null_lit, .var_ref => {},
        }
    }

    /// The names a block operator reads itself, not those of its upstream.
    fn readOp(self: *RefSides, op: *const ir.Op) void {
        switch (op.*) {
            .select => |p| for (p.columns) |col| if (!isStar(col)) self.readName(col),
            .order_by => |o| for (o.specs) |s| self.readName(s.col),
            .compute => |c| for (c.derived) |d| self.readExpr(d.expr),
            .window => |w| {
                for (w.specs) |spec| {
                    for (spec.partition_by) |col| self.readName(col);
                    for (spec.order_by) |s| self.readName(s.col);
                }
                for (w.calls) |call| for (call.args) |arg| self.readExpr(arg);
            },
            .filter => |f| self.readPredicate(f.predicate),
            .group_by => |g| {
                for (g.group_cols) |col| self.readName(col);
                for (g.aggs) |a| {
                    if (a.col) |col| self.readName(col);
                    if (a.arg2_col) |col| self.readName(col);
                    for (a.udf_arg_cols) |col| self.readName(col);
                }
            },
            .join => |j| {
                for (j.on) |pair| {
                    self.readName(pair.left);
                    self.readName(pair.right);
                }
                for (j.ranges) |r| {
                    self.readName(r.left);
                    self.readName(r.right);
                }
                if (j.extra_predicate) |p| self.readPredicate(p);
                if (j.residual) |r| {
                    for (r.derived) |d| self.readExpr(d.expr);
                    self.readPredicate(r.predicate);
                }
            },
            else => {},
        }
    }
};

fn readsOuter(scope: Scope, op: *const ir.Op) bool {
    var sides: RefSides = .{ .scope = scope };
    sides.readOp(op);
    return sides.outer;
}

// =============================================================================
// Scope guard: a subquery compiled on its own must read nothing outside it.
// =============================================================================

/// Resolve a subquery's own subqueries, then check it reads no enclosing
/// query: compiled alone, an outer-qualified name would bind to whatever
/// inner column shares its bare name.
fn prepareSubplan(ctx: *CompileCtx, op: *ir.Op) !void {
    try resolveSubqueriesInOp(ctx, op);
    try requireOwnScope(ctx, op, 0);
}

const ScopeError = error{UnsupportedCorrelatedSubquery} || Allocator.Error;

fn requireOwnScope(ctx: *CompileCtx, op: *const ir.Op, depth: u32) ScopeError!void {
    if (depth >= SCOPE_DEPTH_LIMIT) return;
    if (op.* == .set_union) {
        try requireOwnScope(ctx, op.set_union.left, depth + 1);
        try requireOwnScope(ctx, op.set_union.right, depth + 1);
        return;
    }
    const block = try splitBlock(ctx, op);
    var sides: RefSides = .{ .scope = try ScopeBuilder.build(ctx, block, false), .qualified_only = true };
    var i = block.chain.len;
    while (i > 0) {
        i -= 1;
        const o = block.chain[i];
        sides.readOp(o);
        if (o.* == .select) sides.scope = try sides.scope.above(ctx.nodeArena(), o.select);
    }
    if (sides.outer) return error.UnsupportedCorrelatedSubquery;
    try requireRelationScope(ctx, block.from, depth + 1);
}

fn requireRelationScope(ctx: *CompileCtx, op: *const ir.Op, depth: u32) ScopeError!void {
    if (depth >= SCOPE_DEPTH_LIMIT) return;
    switch (op.*) {
        .scan, .file_scan, .single_row => {},
        .alias => |a| try requireRelationScope(ctx, a.upstream, depth + 1),
        // A named boundary is a CTE, view or function, whose body is its own
        // statement-level scope.
        .materialize => |m| if (m.name == null) try requireOwnScope(ctx, m.upstream, depth + 1),
        .table_fn => |t| for (t.inputs) |input| try requireOwnScope(ctx, input, depth + 1),
        .set_union => try requireOwnScope(ctx, op, depth + 1),
        .join => |j| {
            const block: Block = .{ .chain = &.{}, .from = op };
            var sides: RefSides = .{ .scope = try ScopeBuilder.build(ctx, block, false), .qualified_only = true };
            sides.readOp(op);
            if (sides.outer) return error.UnsupportedCorrelatedSubquery;
            try requireRelationScope(ctx, j.left, depth + 1);
            try requireRelationScope(ctx, j.right, depth + 1);
        },
        else => if (blockUpstream(op) != null) try requireOwnScope(ctx, op, depth + 1),
    }
}

// =============================================================================
// Correlation analysis, common to all correlated subquery resolvers.
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

/// A subquery WHERE's conjuncts, sorted. `inner_cols` and `outer_cols` are
/// parallel: `inner_cols[i] = outer_cols[i]` is one equi correlation, the
/// inner name what the rewritten subquery projects and the outer one what
/// each outer row probes with. Lists live in the node arena.
const CorrelationInfo = struct {
    inner_cols: std.ArrayList([]const u8) = .empty,
    outer_cols: std.ArrayList([]const u8) = .empty,
    /// Range correlations, kept apart: they probe sorted values rather than
    /// hashed tuples.
    range_corrs: std.ArrayList(RangeCorr) = .empty,
    /// Conjuncts that read only the subquery's own names, kept in its WHERE.
    kept_predicates: std.ArrayList(PredicateExpr) = .empty,
    /// Correlation operands computed from the outer row alone (`x.k + 1` in
    /// `y.id = x.k + 1`). The outer query computes them, under these names,
    /// for its rows to probe with.
    outer_values: std.ArrayList(ir.Derived) = .empty,
    /// Each outer value's name in the subquery, to its name in `outer_values`.
    outer_renames: std.ArrayList(exec.predicate.ColRename) = .empty,
    /// The relation the WHERE filters, as the rewritten subquery reads it.
    input: *const ir.Op,

    fn correlated(self: *const CorrelationInfo) bool {
        return self.outer_cols.items.len > 0 or self.range_corrs.items.len > 0;
    }

    /// Point each correlation at the outer query's name for its outer value.
    fn renameOuterValues(self: *CorrelationInfo) void {
        const renames = self.outer_renames.items;
        for (self.outer_cols.items) |*col| col.* = exec.predicate.renameOf(renames, col.*);
        for (self.range_corrs.items) |*r| r.outer_col = exec.predicate.renameOf(renames, r.outer_col);
    }
};

/// The columns a compute under a subquery's WHERE keeps computing. One that
/// reads only the outer row moves to `info.outer_values` instead, and
/// `scope` stops binding its name. Null when one reads both rows, or a
/// nested subquery.
fn splitOuterValues(ctx: *CompileCtx, scope: *Scope, info: *CorrelationInfo, derived: []const ir.Derived) !?[]const ir.Derived {
    const na = ctx.nodeArena();
    var kept: std.ArrayList(ir.Derived) = .empty;
    for (derived) |d| {
        var sides: RefSides = .{ .scope = scope.* };
        sides.readExpr(d.expr);
        if (sides.nested) return null;
        if (!sides.outer) {
            try kept.append(na, d);
            continue;
        }
        if (sides.inner) return null;
        const name = try std.fmt.allocPrint(na, "__csq_o{d}", .{ctx.lowered_scalars});
        ctx.lowered_scalars += 1;
        try info.outer_values.append(na, .{ .name = name, .expr = d.expr });
        try info.outer_renames.append(na, .{ .from = d.name, .to = name });
        scope.outer = info.outer_renames.items;
    }
    return kept.items;
}

/// Whether an exclude drops a column the subquery handed to the outer query.
fn excludesOuterValue(info: *const CorrelationInfo, columns: []const []const u8) bool {
    for (columns) |col| for (info.outer_renames.items) |r| if (types.columnNameEql(col, r.from)) return true;
    return false;
}

/// The correlations and kept predicates of a subquery's WHERE: `below` is
/// `Filter(where, input)` or the input alone, the input being the FROM
/// under any columns the WHERE computes. Null when that input reads the
/// outer row other than as a correlation's operand, or a conjunct ties to
/// it some other way.
fn analyzeWhere(ctx: *CompileCtx, below: *const ir.Op) !?CorrelationInfo {
    const input = if (below.* == .filter) below.filter.upstream else below;
    const block = try splitBlock(ctx, input);
    for (block.chain) |op| if (op.* != .compute and op.* != .exclude) return null;
    var scope = try ScopeBuilder.build(ctx, block, true);
    var info: CorrelationInfo = .{ .input = input };
    var rebuilt: *ir.Op = @constCast(block.from);
    var i = block.chain.len;
    while (i > 0) {
        i -= 1;
        switch (block.chain[i].*) {
            .compute => |c| {
                const derived = (try splitOuterValues(ctx, &scope, &info, c.derived)) orelse return null;
                if (derived.len > 0) rebuilt = try newOp(ctx, .{ .compute = .{ .derived = derived, .upstream = rebuilt } });
            },
            .exclude => |e| {
                if (excludesOuterValue(&info, e.columns)) return null;
                var copy = e;
                copy.upstream = rebuilt;
                rebuilt = try newOp(ctx, .{ .exclude = copy });
            },
            else => unreachable,
        }
    }
    if (info.outer_values.items.len > 0) info.input = rebuilt;
    if (below.* == .filter and !try collectConjuncts(ctx, below.filter.predicate, scope, &info)) return null;
    info.renameOuterValues();
    return info;
}

/// Sort a WHERE's conjuncts into correlations with the outer row and
/// predicates the subquery keeps. False when a conjunct reads the outer row
/// any other way.
fn collectConjuncts(ctx: *CompileCtx, pred: PredicateExpr, scope: Scope, info: *CorrelationInfo) Allocator.Error!bool {
    const na = ctx.nodeArena();
    switch (pred) {
        .@"and" => |children| {
            for (children) |child| if (!try collectConjuncts(ctx, child, scope, info)) return false;
            return true;
        },
        .leaf_col_col => |lc| {
            const left_inner = scope.binds(lc.left);
            const right_inner = scope.binds(lc.right);
            if (left_inner and right_inner) {
                try info.kept_predicates.append(na, pred);
                return true;
            }
            if (!left_inner and !right_inner) return false;
            const corr: RangeCorr = if (left_inner)
                .{ .inner_col = lc.left, .op = lc.op, .outer_col = lc.right }
            else
                .{ .inner_col = lc.right, .op = flipRangeOp(lc.op), .outer_col = lc.left };
            switch (corr.op) {
                .eq => {
                    try info.inner_cols.append(na, corr.inner_col);
                    try info.outer_cols.append(na, corr.outer_col);
                },
                .lt, .lte, .gt, .gte => try info.range_corrs.append(na, corr),
                .neq => return false,
            }
            return true;
        },
        else => {
            var sides: RefSides = .{ .scope = scope };
            sides.readPredicate(pred);
            if (sides.outer) return false;
            try info.kept_predicates.append(na, pred);
            return true;
        },
    }
}

/// The relation a rewritten subquery reads: a fresh node for a table scan,
/// the shared node otherwise, so a CTE's readers still share its stage.
fn reuseInput(ctx: *CompileCtx, input: *const ir.Op) !*ir.Op {
    if (input.* == .scan) return try newOp(ctx, input.*);
    return @constCast(input);
}

/// The subquery's input rows under its own (non-correlation) predicates.
fn keptRows(ctx: *CompileCtx, info: *const CorrelationInfo) !*ir.Op {
    const input = try reuseInput(ctx, info.input);
    if (info.kept_predicates.items.len == 0) return input;
    return try newOp(ctx, .{ .filter = .{ .predicate = try conjunction(ctx, info.kept_predicates.items), .upstream = input } });
}

// =============================================================================
// Correlated EXISTS / NOT EXISTS / IN / NOT IN.
// =============================================================================

/// An EXISTS or IN subquery block ready to decorrelate: the operators kept
/// above its FROM, bottom first, with its WHERE's conjuncts sorted.
const CorrelatedBlock = struct {
    info: CorrelationInfo,
    /// Operators the rewritten subquery keeps, bottom first: filters,
    /// computes, excludes and groupings.
    kept: []const *const ir.Op,
    /// The kept filter that is the block's WHERE.
    where: ?*const ir.Op,
    /// The columns IN compares against, as the kept operators name them.
    selected: []const []const u8,
    /// A global aggregate with no filter above it: one row for every outer
    /// row, whatever the correlation.
    one_row: bool,
};

/// Null when the block isn't correlated through its WHERE, or when some
/// other part of it reads the outer row: the uncorrelated paths take it.
/// `in_width` is IN's column count; null for EXISTS.
fn analyzeBlockCorrelation(ctx: *CompileCtx, top: *const ir.Op, in_width: ?usize) !?CorrelatedBlock {
    const na = ctx.nodeArena();
    const block = try splitBlock(ctx, top);
    var chain = block.chain;
    if (chain.len > 0 and chain[0].* == .limit) {
        // A LIMIT inside IN or with an OFFSET decides which rows each
        // outer row sees; one inside EXISTS never changes whether any exist.
        const l = chain[0].limit;
        if (in_width != null or l.offset != 0 or l.n == 0) return null;
        chain = chain[1..];
    }
    var selected: []const []const u8 = &.{};
    if (in_width) |width| {
        var names: ?[]const []const u8 = null;
        while (chain.len > 0 and chain[0].* == .select) : (chain = chain[1..]) {
            const s = chain[0].select;
            if (names) |outer_names| {
                const mapped = try na.alloc([]const u8, outer_names.len);
                for (outer_names, mapped) |name, *dst| dst.* = selectSourceColumn(s, name) orelse return null;
                names = mapped;
            } else {
                for (s.columns) |col| if (isStar(col)) return null;
                names = s.columns;
            }
        }
        selected = names orelse return null;
        if (selected.len != width) return Error.BadRequest;
    } else {
        // Whether any rows exist never depends on what the block computes
        // above its last filter or grouping.
        const last = for (chain, 0..) |op, i| {
            if (op.* == .filter or op.* == .group_by) break i;
        } else chain.len;
        chain = chain[last..];
    }

    var scope = try ScopeBuilder.build(ctx, block, true);
    var info: CorrelationInfo = .{ .input = block.from };
    var kept: std.ArrayList(*const ir.Op) = .empty;
    var where: ?*const ir.Op = null;
    var grouped = false;
    var global_group = false;
    var filtered_above_global = false;
    var i = chain.len;
    while (i > 0) {
        i -= 1;
        const op = chain[i];
        switch (op.*) {
            .order_by => continue,
            .filter => |f| if (!grouped and where == null) {
                where = op;
                if (!try collectConjuncts(ctx, f.predicate, scope, &info)) return null;
            } else {
                if (global_group) filtered_above_global = true;
                if (readsOuter(scope, op)) return null;
            },
            .compute => |c| if (where == null and !grouped) {
                const derived = (try splitOuterValues(ctx, &scope, &info, c.derived)) orelse return null;
                if (derived.len == 0) continue;
                if (derived.len < c.derived.len) {
                    try kept.append(na, try newOp(ctx, .{ .compute = .{ .derived = derived, .upstream = c.upstream } }));
                    continue;
                }
            } else if (readsOuter(scope, op)) return null,
            .exclude => |e| if (excludesOuterValue(&info, e.columns)) return null,
            .group_by => |g| {
                if (readsOuter(scope, op)) return null;
                grouped = true;
                if (g.group_cols.len == 0) global_group = true;
            },
            else => return null,
        }
        try kept.append(na, op);
    }
    info.renameOuterValues();
    if (!info.correlated()) return null;
    for (selected) |name| if (!scope.binds(name)) return null;
    if (global_group) {
        if (in_width != null or filtered_above_global) return null;
    } else if (grouped and info.range_corrs.items.len > 0) {
        // Grouping by a range column would split the groups each outer row
        // aggregates over.
        return null;
    }
    for (kept.items) |op| if (op.* == .exclude) for (op.exclude.columns) |col| {
        for (info.inner_cols.items) |key| if (sameColumn(col, key)) return null;
        for (info.range_corrs.items) |r| if (sameColumn(col, r.inner_col)) return null;
    };
    return .{
        .info = info,
        .kept = kept.items,
        .where = where,
        .selected = selected,
        .one_row = global_group,
    };
}

/// Whether two names read the same column of one block: equal, or one
/// unqualified and naming the other's column.
fn sameColumn(a: []const u8, b: []const u8) bool {
    if (types.columnNameEql(a, b)) return true;
    if (types.splitQualifiedName(a) != null and types.splitQualifiedName(b) != null) return false;
    return types.columnNameEql(types.unqualifiedName(a), types.unqualifiedName(b));
}

/// The block without its correlations, projecting `columns`: its WHERE
/// keeps only the subquery's own conjuncts, and each grouping also groups
/// by the equi correlation keys, so every key's rows aggregate apart.
fn rebuildBlock(ctx: *CompileCtx, block: *const CorrelatedBlock, columns: []const []const u8) !*ir.Op {
    const na = ctx.nodeArena();
    var cur = try reuseInput(ctx, block.info.input);
    for (block.kept) |op| {
        if (op == block.where) {
            if (block.info.kept_predicates.items.len == 0) continue;
            cur = try newOp(ctx, .{ .filter = .{ .predicate = try conjunction(ctx, block.info.kept_predicates.items), .upstream = cur } });
            continue;
        }
        cur = try newOp(ctx, switch (op.*) {
            .filter => |f| .{ .filter = .{ .predicate = f.predicate, .upstream = cur } },
            .compute => |c| .{ .compute = .{ .derived = c.derived, .upstream = cur } },
            .exclude => |e| blk: {
                var copy = e;
                copy.upstream = cur;
                break :blk .{ .exclude = copy };
            },
            .group_by => |g| blk: {
                var copy = g;
                var group_cols: std.ArrayList([]const u8) = .empty;
                try group_cols.appendSlice(na, g.group_cols);
                for (block.info.inner_cols.items) |key| {
                    for (group_cols.items) |col| {
                        if (sameColumn(col, key)) break;
                    } else try group_cols.append(na, key);
                }
                copy.group_cols = group_cols.items;
                copy.upstream = cur;
                break :blk .{ .group_by = copy };
            },
            // analyzeBlockCorrelation keeps no other operator.
            else => unreachable,
        });
    }
    return try newOp(ctx, .{ .select = .{ .columns = columns, .upstream = cur } });
}

/// Decorrelate an EXISTS (`in_subquery` null) or IN subquery that reads the
/// outer row. One whose WHERE ties its rows to the outer row by equalities
/// and ranges is keyed on its own columns (`planKeyedBlock`), as is each
/// disjunct of one whose WHERE ORs such ties (`resolveDisjunctBlocks`); any
/// other is lifted onto its domain when `lowered` has one. A correlation
/// operand computed from the outer row alone goes to `lowered`, for the
/// operator reading `pred` to compute below itself. False leaves the
/// subquery to the uncorrelated paths.
fn resolveCorrelatedBlock(ctx: *CompileCtx, pred: *PredicateExpr, source: *const anyopaque, negate: bool, in_subquery: ?exec.predicate.InSubquery, lowered: ?*LoweredScalars) !bool {
    const top: *const ir.Op = @ptrCast(@alignCast(source));
    const in_width: ?usize = if (in_subquery) |s| 1 + s.rest_cols.len else null;
    if (try analyzeBlockCorrelation(ctx, top, in_width)) |block| {
        if (try planKeyedBlock(ctx, &block, in_subquery, lowered)) |plan| {
            pred.* = try runKeyedPlan(ctx, plan, negate, lowered);
            return true;
        }
    }
    if (try resolveDisjunctBlocks(ctx, pred, top, negate, in_subquery, lowered)) return true;
    const l = lowered orelse return false;
    return try resolveDomainBlock(ctx, pred, top, negate, in_subquery, l);
}

/// A keyed block's subquery without its correlations, and the outer columns
/// (IN's compared columns first) that probe its rows by the inner side of
/// each correlation. `rewritten` is null for a block with one row for every
/// outer row.
const KeyedPlan = struct {
    block: *const CorrelatedBlock,
    rewritten: ?*ir.Op,
    outer_keys: []const []const u8,
    n_in: usize,
    bounds: ?RangeBounds,
};

/// Null when the block can't be keyed: more ranges than one closed range, an
/// outer value with no operator to compute it, or a rewrite that still reads
/// the outer row, as through a subquery nested in it.
fn planKeyedBlock(ctx: *CompileCtx, block: *const CorrelatedBlock, in_subquery: ?exec.predicate.InSubquery, lowered: ?*LoweredScalars) !?KeyedPlan {
    if (block.one_row) return .{ .block = block, .rewritten = null, .outer_keys = &.{}, .n_in = 0, .bounds = null };
    const ranges = block.info.range_corrs.items;
    if (ranges.len > 2 or (ranges.len == 2 and !isClosedRange(ranges))) return null;
    if (block.info.outer_values.items.len > 0 and lowered == null) return null;

    const aa = try ctx.subqueryArena();
    const n_in = block.selected.len;
    const inner_keys = try std.mem.concat(aa, []const u8, &.{ block.selected, block.info.inner_cols.items });
    const bounds = if (ranges.len > 0) rangeBounds(ranges) else null;
    const range_col: []const []const u8 = if (bounds) |b| try aa.dupe([]const u8, &.{b.lower.inner_col}) else &.{};
    const rewritten = try rebuildBlock(ctx, block, try std.mem.concat(aa, []const u8, &.{ range_col, inner_keys }));
    if (try readsFree(ctx, rewritten)) return null;

    const outer_keys = try aa.alloc([]const u8, inner_keys.len);
    if (in_subquery) |s| {
        outer_keys[0] = try aa.dupe(u8, s.col);
        for (s.rest_cols, outer_keys[1..n_in]) |c, *dst| dst.* = try aa.dupe(u8, c);
    }
    for (block.info.outer_cols.items, outer_keys[n_in..]) |c, *dst| dst.* = try aa.dupe(u8, c);
    return .{ .block = block, .rewritten = rewritten, .outer_keys = outer_keys, .n_in = n_in, .bounds = bounds };
}

/// Runs a keyed plan's subquery once: its rows become the set, or the
/// ranges, each outer row probes.
fn runKeyedPlan(ctx: *CompileCtx, plan: KeyedPlan, negate: bool, lowered: ?*LoweredScalars) !PredicateExpr {
    const rewritten = plan.rewritten orelse return .{ .always = !negate };
    try prepareSubplan(ctx, rewritten);
    const aa = try ctx.subqueryArena();
    const pred = if (plan.bounds) |b|
        try nullGuarded(aa, plan.outer_keys[0..plan.n_in], negate, try correlatedRange(ctx, rewritten, b, plan.outer_keys, negate))
    else blk: {
        var q = try local.compileSubplan(ctx, rewritten);
        defer q.deinit();
        break :blk try inTupleSet(aa, .{
            .outer_cols = plan.outer_keys,
            .rows = try drainTuples(ctx, &q, plan.outer_keys.len),
            .negate = negate,
            .inner_types = try columnTypes(aa, q.outputSchema()[0..plan.outer_keys.len]),
        }, plan.n_in);
    };
    if (lowered) |l| try l.computeOuterValues(ctx, &plan.block.info);
    return pred;
}

/// A subquery whose WHERE ORs ties to the outer row that no one key covers,
/// as `i.v = o.v + 1 OR i.k = o.k`, as one keyed copy per disjunct. A row
/// passes the WHERE iff it passes some copy's, so EXISTS and IN hold iff
/// they hold for some copy, and NOT EXISTS and NOT IN iff for none. That
/// needs a block that keeps or drops each row by itself, so one that groups
/// or windows isn't split. False unless every copy is keyed.
fn resolveDisjunctBlocks(ctx: *CompileCtx, pred: *PredicateExpr, top: *const ir.Op, negate: bool, in_subquery: ?exec.predicate.InSubquery, lowered: ?*LoweredScalars) !bool {
    const na = ctx.nodeArena();
    const in_width: ?usize = if (in_subquery) |s| 1 + s.rest_cols.len else null;
    const block = try splitBlock(ctx, top);
    var where: ?usize = null;
    for (block.chain, 0..) |op, i| switch (op.*) {
        .group_by, .window => return false,
        .filter => where = i,
        else => {},
    };
    const w = where orelse return false;
    const where_pred = block.chain[w].filter.predicate;
    const conjuncts = if (where_pred == .@"and") where_pred.@"and" else try na.dupe(PredicateExpr, &.{where_pred});
    for (conjuncts, 0..) |c, at| {
        if (c != .@"or") continue;
        const plans = try na.alloc(KeyedPlan, c.@"or".len);
        const keyed = for (c.@"or", plans) |disjunct, *plan| {
            const arm_where = try na.dupe(PredicateExpr, conjuncts);
            arm_where[at] = disjunct;
            const arm = try na.create(CorrelatedBlock);
            arm.* = (try analyzeBlockCorrelation(ctx, try withWhere(ctx, block, w, .{ .@"and" = arm_where }), in_width)) orelse break false;
            plan.* = (try planKeyedBlock(ctx, arm, in_subquery, lowered)) orelse break false;
        } else true;
        if (!keyed) continue;
        const arms = try (try ctx.subqueryArena()).alloc(PredicateExpr, plans.len);
        for (plans, arms) |plan, *arm| arm.* = try runKeyedPlan(ctx, plan, negate, lowered);
        pred.* = if (negate) .{ .@"and" = arms } else .{ .@"or" = arms };
        return true;
    }
    return false;
}

/// A copy of `block`'s operators over its FROM, the filter at `where`
/// reading `predicate` instead.
fn withWhere(ctx: *CompileCtx, block: Block, where: usize, predicate: PredicateExpr) !*ir.Op {
    var cur: *ir.Op = @constCast(block.from);
    var i = block.chain.len;
    while (i > 0) {
        i -= 1;
        const op = try newOp(ctx, block.chain[i].*);
        if (i == where) op.filter.predicate = predicate;
        relink(op, cur);
        cur = op;
    }
    return cur;
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

/// A range correlation's probe: an open-ended one's single conjunct, or a
/// closed one's lower-bound conjunct and its upper bound.
const RangeBounds = struct {
    lower: RangeCorr,
    upper: ?RangeCorr,
};

fn rangeBounds(corrs: []const RangeCorr) RangeBounds {
    if (corrs.len == 1) return .{ .lower = corrs[0], .upper = null };
    const first_is_lower = corrs[0].op == .gt or corrs[0].op == .gte;
    return if (first_is_lower)
        .{ .lower = corrs[0], .upper = corrs[1] }
    else
        .{ .lower = corrs[1], .upper = corrs[0] };
}

/// Drain a range-correlated subquery. `rewritten` projects the range's
/// inner column, then the key columns `outer_keys` probe with; its rows are
/// bucketed by key tuple, the buckets in key order and each one's range
/// values sorted ascending. Per outer row the eval is then a single min/max
/// compare for the open-ended case, or a bsearch for the closed BETWEEN case.
fn correlatedRange(ctx: *CompileCtx, rewritten: *ir.Op, bounds: RangeBounds, outer_keys: []const []const u8, negate: bool) !PredicateExpr {
    const aa = try ctx.subqueryArena();
    var q = try local.compileSubplan(ctx, rewritten);
    defer q.deinit();

    const n_keys = outer_keys.len;

    // First pass: drain into flat (key_tuple, range_value) rows.
    var rows: std.ArrayList(exec.predicate.CorrelatedScalarRow) = .empty;
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
                key[j] = try extractKeyValueAt(aa, view, q.outputSchema()[1 + j].type, i);
            }
            if (any_null) continue;
            const v = try extractScalarValueAt(aa, range_view, i);
            // A NaN meets no bound, and it sorts last, where it would stand
            // in for the group's largest value.
            const nan = switch (v) {
                .float => |f| std.math.isNan(f),
                .double => |f| std.math.isNan(f),
                else => false,
            };
            if (nan) continue;
            try rows.append(ctx.allocator, .{ .key = key, .value = v });
        }
    }

    // Rows in key order put each equi-key tuple's rows side by side, one
    // group per run. The n_keys == 0 case (pure range, no equi
    // correlation) collapses to a single group with an empty key.
    exec.predicate.sortKeyed(exec.predicate.CorrelatedScalarRow, rows.items);
    var groups: std.ArrayList(exec.predicate.CorrelatedRangeGroup) = .empty;
    var start: usize = 0;
    while (start < rows.items.len) {
        var end = start + 1;
        while (end < rows.items.len and keysEqual(rows.items[start].key, rows.items[end].key)) end += 1;
        const values = try aa.alloc(Value, end - start);
        for (rows.items[start..end], values) |row, *v| v.* = row.value;
        std.sort.pdq(Value, values, {}, valueLessThan);
        try groups.append(aa, .{ .key = rows.items[start].key, .values = values });
        start = end;
    }
    const groups_owned = try groups.toOwnedSlice(aa);

    return .{ .correlated_range = .{
        .outer_keys = outer_keys,
        .outer_range_col = try aa.dupe(u8, bounds.lower.outer_col),
        .op = bounds.lower.op,
        .outer_range_col_upper = if (bounds.upper) |u| try aa.dupe(u8, u.outer_col) else null,
        .op_upper = if (bounds.upper) |u| u.op else null,
        .groups = groups_owned,
        .negate = negate,
        .key_types = try columnTypes(aa, q.outputSchema()[1 .. 1 + n_keys]),
        .range_type = q.outputSchema()[0].type,
    } };
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
// Correlated scalar subqueries in a Compute or Filter — LEFT JOIN lowering.
// =============================================================================

/// A scalar subquery that is one global aggregate over its WHERE and FROM,
/// with its WHERE conjuncts sorted into correlations and kept predicates.
const ScalarAggregate = struct {
    /// The global aggregate.
    group: *const ir.Op,
    aggs: []const ir.AggSpec,
    /// Aggregate arguments computed per inner row (`SUM(qty * price)`).
    pre: []const ir.Derived,
    /// Expressions over the aggregates (`COALESCE(SUM(x), 0)`).
    post: []const ir.Derived,
    /// The one column the subquery projects: an aggregate or a `post` name.
    selected: []const u8,
    /// Null when the WHERE ties to the outer row other than by equalities
    /// and ranges on its own columns.
    info: ?CorrelationInfo,
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
    return .{ .group = cur, .aggs = gb.aggs, .pre = pre, .post = post, .selected = selected.?, .info = try analyzeWhere(ctx, below) };
}

/// A scalar subquery that reads one inner row per outer row: a column or
/// expression over its WHERE and FROM, optionally ordered and limited, with
/// its WHERE conjuncts sorted into correlations and kept predicates.
const ScalarLookup = struct {
    /// Per-row expressions (the selected expression, ORDER BY keys), each
    /// layer over the one before it.
    computes: []const []const ir.Derived,
    order: []const ir.SortSpec,
    limit: ?ir.Op.Limit,
    selected: []const u8,
    /// As `ScalarAggregate.info`.
    info: ?CorrelationInfo,
};

fn analyzeScalarLookup(ctx: *CompileCtx, source: *const anyopaque) !?ScalarLookup {
    const na = ctx.nodeArena();
    var cur: *const ir.Op = @ptrCast(@alignCast(source));
    var selected: ?[]const u8 = null;
    var order: ?[]const ir.SortSpec = null;
    var limit: ?ir.Op.Limit = null;
    var computes: std.ArrayList([]const ir.Derived) = .empty;
    if (cur.* == .limit) {
        limit = cur.limit;
        cur = cur.limit.upstream;
    }
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
            .order_by => |o| {
                if (order != null) return null;
                order = o.specs;
                cur = o.upstream;
            },
            .compute => |c| {
                try computes.append(na, c.derived);
                cur = c.upstream;
            },
            .filter => break,
            else => if (blockUpstream(cur) == null) break else return null,
        }
    }
    std.mem.reverse([]const ir.Derived, computes.items);
    return .{
        .computes = computes.items,
        .order = order orelse &.{},
        .limit = limit,
        .selected = selected orelse return null,
        .info = try analyzeWhere(ctx, cur),
    };
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
    /// Comparison right sides over the subqueries' results (`x > (SELECT
    /// ...) - 1`), evaluated per outer row.
    compared_values: std.ArrayList(ir.Derived) = .empty,
    /// Correlation operands the subqueries read from the outer row alone,
    /// computed below the joins.
    outer_values: std.ArrayList(ir.Derived) = .empty,
    /// Join-side and value columns, dropped once the operator has read them.
    hidden: std.ArrayList([]const u8) = .empty,
    /// The operator's input, which domains read; null where the operator
    /// has no input to join.
    domain: ?DomainSource = null,
    /// Lower a correlated scalar comparison only once the keyed probe
    /// declines it: a statement's or join's predicate keeps that probe, and
    /// joins only what it can't take.
    late_scalars: bool = false,

    /// Whether the operator reads anything computed or joined below it.
    fn any(self: *const LoweredScalars) bool {
        return self.joins.items.len > 0 or self.outer_values.items.len > 0;
    }

    fn computeOuterValues(self: *LoweredScalars, ctx: *CompileCtx, info: *const CorrelationInfo) !void {
        const na = ctx.nodeArena();
        for (info.outer_values.items) |v| {
            try self.outer_values.append(na, v);
            try self.hidden.append(na, v.name);
        }
    }
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

/// Lower a correlated scalar subquery into `lowered`, returning the column
/// its value reads as; null leaves it to the uncorrelated path. One keyed by
/// equalities on its own columns groups by them; any other is lifted onto
/// its domain.
fn lowerCorrelatedScalar(ctx: *CompileCtx, source: *const anyopaque, lowered: *LoweredScalars) !?[]const u8 {
    if (try analyzeScalarAggregate(ctx, source)) |shape| {
        if (equiCorrelated(shape.info)) {
            if (try lowerScalarAggregate(ctx, shape, &shape.info.?, lowered)) |value| return value;
        }
        if (try lowerDomainAggregate(ctx, shape, lowered)) |value| return value;
        return try lowerUnkeyedAggregate(ctx, source, shape, lowered);
    }
    if (try analyzeScalarLookup(ctx, source)) |shape| {
        if (equiCorrelated(shape.info)) {
            if (try lowerScalarLookup(ctx, shape, &shape.info.?, lowered)) |value| return value;
        }
    }
    return try lowerDomainScalar(ctx, source, lowered);
}

fn equiCorrelated(info: ?CorrelationInfo) bool {
    const i = info orelse return false;
    return i.outer_cols.items.len > 0 and i.range_corrs.items.len == 0;
}

fn groupByKeys(ctx: *CompileCtx, info: *const CorrelationInfo, aggs: []const ir.AggSpec, input: *ir.Op) !*ir.Op {
    return try newOp(ctx, .{ .group_by = .{
        .group_cols = try ctx.nodeArena().dupe([]const u8, info.inner_cols.items),
        .aggs = aggs,
        .upstream = input,
    } });
}

/// Null when the aggregate, keyed on its correlations, still reads the
/// outer row.
fn lowerScalarAggregate(ctx: *CompileCtx, shape: ScalarAggregate, info: *const CorrelationInfo, lowered: *LoweredScalars) !?[]const u8 {
    const na = ctx.nodeArena();
    var inner = try keptRows(ctx, info);
    if (shape.pre.len > 0) {
        inner = try newOp(ctx, .{ .compute = .{ .derived = shape.pre, .upstream = inner } });
    }
    const aggs = try na.dupe(ir.AggSpec, shape.aggs);
    for (aggs, 0..) |*a, j| a.as = try std.fmt.allocPrint(na, "__csq_a{d}", .{j});
    const grouped = try groupByKeys(ctx, info, aggs, inner);
    if (try readsFree(ctx, grouped)) return null;
    return try aggregateValues(ctx, shape, try joinGroupedInner(ctx, info, aggs, grouped, lowered), lowered);
}

/// A scalar aggregate lifted onto its domain: grouped per domain row. A
/// domain row no inner row reaches has no group, so its outer rows miss the
/// join and read the aggregate over zero rows.
fn lowerDomainAggregate(ctx: *CompileCtx, shape: ScalarAggregate, lowered: *LoweredScalars) !?[]const u8 {
    const source = if (lowered.domain) |*d| d else return null;
    const lifted = (try liftSubquery(ctx, shape.group, source, true, null)) orelse return null;
    const na = ctx.nodeArena();
    const values = try na.alloc([]const u8, shape.aggs.len);
    const outputs = try na.alloc([]const u8, shape.aggs.len);
    for (shape.aggs, values, outputs, 0..) |a, *value, *output, j| {
        value.* = a.as;
        output.* = try std.fmt.allocPrint(na, "__csq_a{d}", .{j});
    }
    const alias = try joinKeyed(ctx, lifted.rows, try domainKeys(ctx, lifted, &.{}, &.{}), values, outputs, lowered);
    return try aggregateValues(ctx, shape, alias, lowered);
}

/// A scalar aggregate whose aggregation reads no enclosing name, while its
/// expressions over the aggregates do: the aggregation's one row joins
/// every outer row, which then computes those expressions.
fn lowerUnkeyedAggregate(ctx: *CompileCtx, source: *const anyopaque, shape: ScalarAggregate, lowered: *LoweredScalars) !?[]const u8 {
    const grouped = (try freeNamesOf(ctx, shape.group)) orelse return null;
    if (grouped.len > 0) return null;
    const free = (try freeNamesOf(ctx, @ptrCast(@alignCast(source)))) orelse return null;
    if (free.len == 0) return null;
    const na = ctx.nodeArena();
    const values = try na.alloc([]const u8, shape.aggs.len);
    const outputs = try na.alloc([]const u8, shape.aggs.len);
    for (shape.aggs, values, outputs, 0..) |a, *value, *output, j| {
        value.* = a.as;
        output.* = try std.fmt.allocPrint(na, "__csq_a{d}", .{j});
    }
    const alias = try joinKeyed(ctx, @constCast(shape.group), .{ .inner = &.{}, .outer = &.{}, .null_safe_from = 0 }, values, outputs, lowered);
    return try aggregateValues(ctx, shape, alias, lowered);
}

/// The aggregates joined under `alias` as each outer row reads them, then
/// the subquery's expressions over them. Returns the selected one's column.
fn aggregateValues(ctx: *CompileCtx, shape: ScalarAggregate, alias: []const u8, lowered: *LoweredScalars) ![]const u8 {
    const na = ctx.nodeArena();
    // The subquery's own names for its aggregates and expressions, each
    // relabelled to the outer column that carries it.
    const renames = try na.alloc(exec.predicate.ColRename, shape.aggs.len + shape.post.len);
    for (shape.aggs, 0..) |a, j| {
        const joined = try std.fmt.allocPrint(na, "{s}.__csq_a{d}", .{ alias, j });
        const value = try std.fmt.allocPrint(na, "{s}_a{d}", .{ alias, j });
        renames[j] = .{ .from = a.as, .to = value };
        try lowered.hidden.append(na, joined);
        try lowered.hidden.append(na, value);
        try lowered.values.append(na, .{ .name = value, .expr = try missedJoinValue(ctx, a.func, joined) });
    }
    for (shape.post, shape.aggs.len..) |d, k| {
        const value = try std.fmt.allocPrint(na, "{s}_p{d}", .{ alias, k - shape.aggs.len });
        const expr = try exec.expr_mod.deepCloneRenamed(na, d.expr, renames[0..k]);
        renames[k] = .{ .from = d.name, .to = value };
        try lowered.hidden.append(na, value);
        try lowered.post_values.append(na, .{ .name = value, .expr = expr });
    }
    return exec.predicate.renameOf(renames, shape.selected);
}

/// Each key's inner rows, grouped to one row: the selected value and how
/// many rows the key matched. Null when that grouping still reads the outer
/// row.
fn lowerScalarLookup(ctx: *CompileCtx, shape: ScalarLookup, info: *const CorrelationInfo, lowered: *LoweredScalars) !?[]const u8 {
    var inner = try keptRows(ctx, info);
    for (shape.computes) |derived| {
        inner = try newOp(ctx, .{ .compute = .{ .derived = derived, .upstream = inner } });
    }
    if (shape.limit) |limit| inner = try limitPerKey(ctx, info, shape.order, limit, inner);
    const aggs = try lookupAggs(ctx, shape.selected);
    const grouped = try groupByKeys(ctx, info, aggs, inner);
    if (try readsFree(ctx, grouped)) return null;
    return try singleRowValue(ctx, try joinGroupedInner(ctx, info, aggs, grouped, lowered), lowered);
}

/// Any other scalar subquery lifted onto its domain, its rows per domain
/// row read as a lookup's are.
fn lowerDomainScalar(ctx: *CompileCtx, source_op: *const anyopaque, lowered: *LoweredScalars) !?[]const u8 {
    const source = if (lowered.domain) |*d| d else return null;
    const lifted = (try liftSubquery(ctx, @ptrCast(@alignCast(source_op)), source, false, 1)) orelse return null;
    const grouped = try newOp(ctx, .{ .group_by = .{
        .group_cols = lifted.keys,
        .aggs = try lookupAggs(ctx, lifted.selected[0]),
        .upstream = lifted.rows,
    } });
    const values = [_][]const u8{ "__csq_a0", "__csq_a1" };
    const alias = try joinKeyed(ctx, grouped, try domainKeys(ctx, lifted, &.{}, &.{}), &values, null, lowered);
    return try singleRowValue(ctx, alias, lowered);
}

/// A lookup's value and how many rows its key matched.
fn lookupAggs(ctx: *CompileCtx, selected: []const u8) ![]const ir.AggSpec {
    const aggs = try ctx.nodeArena().alloc(ir.AggSpec, 2);
    aggs[0] = .{ .func = .any_value, .col = selected, .as = "__csq_a0" };
    aggs[1] = .{ .func = .count, .col = null, .as = "__csq_a1" };
    return aggs;
}

/// The looked-up value above the join under `alias`, read through
/// `single_row`, which fails the statement when an outer row's key matched
/// more than one row, and reads NULL when it matched none.
fn singleRowValue(ctx: *CompileCtx, alias: []const u8, lowered: *LoweredScalars) ![]const u8 {
    const na = ctx.nodeArena();
    const args = try na.alloc(ir.Expr, 2);
    args[0] = .{ .col_ref = try std.fmt.allocPrint(na, "{s}.__csq_a1", .{alias}) };
    args[1] = .{ .col_ref = try std.fmt.allocPrint(na, "{s}.__csq_a0", .{alias}) };
    const value = try std.fmt.allocPrint(na, "{s}_a0", .{alias});
    try lowered.hidden.append(na, args[0].col_ref);
    try lowered.hidden.append(na, args[1].col_ref);
    try lowered.hidden.append(na, value);
    try lowered.values.append(na, .{ .name = value, .expr = .{ .call = .{ .fn_name = exec.scalar_fn.SINGLE_ROW_FN, .args = args } } });
    return value;
}

/// The subquery's `ORDER BY ... LIMIT n OFFSET m` applied within each key.
fn limitPerKey(ctx: *CompileCtx, info: *const CorrelationInfo, order: []const ir.SortSpec, limit: ir.Op.Limit, input: *ir.Op) !*ir.Op {
    const rank = "__csq_rn";
    return try rankFilter(ctx, rank, limit, try rankWithin(ctx, info.inner_cols.items, order, rank, input));
}

/// `input` with each row's number within its `keys`, in `order`, as `rank`.
fn rankWithin(ctx: *CompileCtx, keys: []const []const u8, order: []const ir.SortSpec, rank: []const u8, input: *ir.Op) !*ir.Op {
    const na = ctx.nodeArena();
    const specs = try na.alloc(ir.WindowSpec, 1);
    specs[0] = .{
        .partition_by = try na.dupe([]const u8, keys),
        .order_by = order,
        .frame = if (order.len > 0) ir.Frame.default_with_order else ir.Frame.default_no_order,
    };
    const calls = try na.alloc(ir.WindowCall, 1);
    calls[0] = .{ .spec_idx = 0, .func = .row_number, .args = &.{}, .ignore_nulls = false, .output_name = rank };
    return try newOp(ctx, .{ .window = .{ .specs = specs, .calls = calls, .upstream = input } });
}

/// The rows `LIMIT n OFFSET m` keeps of each key's: those `rank` numbers
/// m+1 through m+n.
fn rankFilter(ctx: *CompileCtx, rank: []const u8, limit: ir.Op.Limit, input: *ir.Op) !*ir.Op {
    const max_rn: u64 = std.math.maxInt(i64);
    const first: i64 = @intCast(@min(limit.offset, max_rn));
    const last: i64 = @intCast(@min(limit.offset +| limit.n, max_rn));
    const bounds = try ctx.nodeArena().alloc(PredicateExpr, 2);
    bounds[0] = .{ .leaf = .{ .col = rank, .op = .gt, .val = .{ .bigint = first } } };
    bounds[1] = .{ .leaf = .{ .col = rank, .op = .lte, .val = .{ .bigint = last } } };
    return try newOp(ctx, .{ .filter = .{ .predicate = .{ .@"and" = bounds }, .upstream = input } });
}

/// The keys a lowered subquery's grouped rows join the operator's rows on:
/// each `inner` column meets the `outer` column beside it.
const JoinKeys = struct {
    inner: []const []const u8,
    outer: []const []const u8,
    /// Keys from this one on match NULL to NULL: a domain holds a NULL
    /// outer value as a row of its own.
    null_safe_from: usize,
};

fn joinGroupedInner(ctx: *CompileCtx, info: *const CorrelationInfo, aggs: []const ir.AggSpec, grouped: *ir.Op, lowered: *LoweredScalars) ![]const u8 {
    const values = try ctx.nodeArena().alloc([]const u8, aggs.len);
    for (aggs, values) |a, *value| value.* = a.as;
    const keys = info.inner_cols.items;
    const alias = try joinKeyed(ctx, grouped, .{ .inner = keys, .outer = info.outer_cols.items, .null_safe_from = keys.len }, values, null, lowered);
    try lowered.computeOuterValues(ctx, info);
    return alias;
}

/// LEFT JOIN target for `grouped` — the inner rows grouped to one row per
/// key — on `keys`, carrying its `values` columns, renamed to `outputs`
/// when given. Returns the alias the joined columns read under.
fn joinKeyed(ctx: *CompileCtx, grouped: *ir.Op, keys: JoinKeys, values: []const []const u8, outputs: ?[]const []const u8, lowered: *LoweredScalars) ![]const u8 {
    const na = ctx.nodeArena();
    const alias = try std.fmt.allocPrint(na, "__csq{d}", .{ctx.lowered_scalars});
    ctx.lowered_scalars += 1;

    // Output names no outer column shares, so a bare outer ref never
    // suffix-matches a join-side column.
    const n_keys = keys.inner.len;
    const columns = try na.alloc([]const u8, n_keys + values.len);
    const names = try na.alloc(?[]const u8, n_keys + values.len);
    const on = try na.alloc(ir.JoinKeyPair, n_keys);
    for (keys.inner, keys.outer, 0..) |inner_col, outer_col, i| {
        columns[i] = inner_col;
        names[i] = try std.fmt.allocPrint(na, "__csq_k{d}", .{i});
        on[i] = .{
            .left = outer_col,
            .right = try std.fmt.allocPrint(na, "{s}.__csq_k{d}", .{ alias, i }),
            .null_safe = i >= keys.null_safe_from,
        };
        try lowered.hidden.append(na, on[i].right);
    }
    for (values, n_keys..) |value, i| {
        columns[i] = value;
        names[i] = if (outputs) |o| o[i - n_keys] else null;
    }
    var inner = try newOp(ctx, .{ .select = .{ .columns = columns, .outputs = names, .upstream = grouped } });
    inner = try newOp(ctx, .{ .materialize = .{ .upstream = inner, .structural_cse = true } });
    const right = try newOp(ctx, .{ .alias = .{ .alias = alias, .upstream = inner } });
    try prepareSubplan(ctx, right);
    try lowered.joins.append(na, .{ .on = on, .right = right });
    return alias;
}

/// An outer row that misses the join reads the aggregate over zero rows
/// (`aggregate.emptyValue`): 0 for the counts, NULL for most others.
fn missedJoinValue(ctx: *CompileCtx, func: ir.AggFunc, agg_col: []const u8) !ir.Expr {
    const empty = exec.aggregate_op.emptyValue(func) orelse return .{ .col_ref = agg_col };
    const args = try ctx.nodeArena().alloc(ir.Expr, 2);
    args[0] = .{ .col_ref = agg_col };
    args[1] = .{ .lit = empty };
    return .{ .call = .{ .fn_name = "coalesce", .args = args } };
}

/// The expression of `SELECT <expr>` with no FROM, the form a comparison's
/// right side takes when it holds no column; null for any other subquery.
fn singleRowExpr(source: *const anyopaque) ?*ir.Expr {
    const op: *const ir.Op = @ptrCast(@alignCast(source));
    if (op.* != .select) return null;
    const upstream = op.select.upstream;
    if (upstream.* != .compute) return null;
    const c = upstream.compute;
    if (c.upstream.* != .single_row or c.derived.len != 1) return null;
    return @constCast(&c.derived[0].expr);
}

/// `input` LEFT JOINed with each lowered subquery, value columns on top.
fn joinLoweredScalars(ctx: *CompileCtx, input: *ir.Op, lowered: LoweredScalars) !*ir.Op {
    var top = input;
    if (lowered.outer_values.items.len > 0) {
        top = try newOp(ctx, .{ .compute = .{ .derived = lowered.outer_values.items, .upstream = top } });
    }
    for (lowered.joins.items) |j| {
        top = try newOp(ctx, .{ .join = .{
            .algorithm = .auto,
            .join_type = .left,
            .on = j.on,
            .ranges = &.{},
            .extra_predicate = null,
            .skew_ratio_threshold = 0.3,
            .skew_absolute_threshold = 20_000,
            .skew_sample_interval = 10,
            .left = top,
            .right = j.right,
        } });
    }
    for ([_][]const ir.Derived{ lowered.values.items, lowered.post_values.items, lowered.compared_values.items }) |derived| {
        if (derived.len > 0) top = try newOp(ctx, .{ .compute = .{ .derived = derived, .upstream = top } });
    }
    return top;
}

/// Rewrite each correlated `col op (scalar subquery)` in `pred` into a
/// comparison against the lowered subquery's value column.
fn lowerPredicateScalars(ctx: *CompileCtx, pred: *PredicateExpr, lowered: *LoweredScalars) anyerror!void {
    switch (pred.*) {
        .scalar_subquery => |sq| if (try lowerCorrelatedScalar(ctx, sq.source, lowered)) |value| {
            pred.* = .{ .leaf_col_col = .{ .left = sq.col, .op = sq.op, .right = value } };
        } else if (singleRowExpr(sq.source)) |expr| {
            // `col op <expression>` with no column is evaluated once, over a
            // single row, so it can prune as a literal. A correlated subquery
            // inside it reads the outer row: lowered, the expression becomes a
            // value column the comparison reads per row.
            const joins_before = lowered.joins.items.len;
            try resolveSubqueriesInExpr(ctx, expr, lowered);
            if (lowered.joins.items.len == joins_before) return;
            const na = ctx.nodeArena();
            const name = try std.fmt.allocPrint(na, "__csq_cmp{d}", .{ctx.lowered_scalars});
            ctx.lowered_scalars += 1;
            try lowered.compared_values.append(na, .{ .name = name, .expr = expr.* });
            try lowered.hidden.append(na, name);
            pred.* = .{ .leaf_col_col = .{ .left = sq.col, .op = sq.op, .right = name } };
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
    // The input resolves first: a domain reads it as it will run.
    try resolveSubqueriesInOp(ctx, @constCast(f.upstream));
    const resolved = try lowerConjuncts(ctx, f.upstream, f.predicate, false);
    if (!resolved.lowered.any()) {
        op.filter.predicate = try conjunction(ctx, resolved.conjuncts);
        return;
    }
    op.* = (try resolved.filteredRows(ctx)).*;
}

/// A predicate over an operator's input, its conjuncts' subqueries resolved
/// against those rows.
const LoweredConjuncts = struct {
    /// As written, each resolved.
    conjuncts: []const PredicateExpr,
    /// Whether each conjunct reads a lowered join or outer value.
    above: []const bool,
    lowered: LoweredScalars,

    /// The input rows the predicate keeps: conjuncts that read nothing
    /// lowered filter below the joins, where they still narrow the scan,
    /// the others above them; the lowered columns are then dropped.
    fn filteredRows(self: *const LoweredConjuncts, ctx: *CompileCtx) !*ir.Op {
        const na = ctx.nodeArena();
        var above: std.ArrayList(PredicateExpr) = .empty;
        var below: std.ArrayList(PredicateExpr) = .empty;
        for (self.conjuncts, self.above) |c, is_above| try (if (is_above) &above else &below).append(na, c);
        var input: *ir.Op = self.lowered.domain.?.operatorInput();
        if (below.items.len > 0) {
            input = try newOp(ctx, .{ .filter = .{ .predicate = try conjunction(ctx, below.items), .upstream = input } });
        }
        const upper = try newOp(ctx, .{ .filter = .{
            .predicate = try conjunction(ctx, above.items),
            .upstream = try joinLoweredScalars(ctx, input, self.lowered),
        } });
        return try newOp(ctx, .{ .exclude = .{ .columns = self.lowered.hidden.items, .upstream = upper } });
    }
};

/// Resolve each conjunct of `pred`, a predicate over `input`'s rows, with
/// those rows as the domain. `late_scalars` as `LoweredScalars`.
fn lowerConjuncts(ctx: *CompileCtx, input: *ir.Op, pred: PredicateExpr, late_scalars: bool) !LoweredConjuncts {
    const na = ctx.nodeArena();
    const conjuncts = switch (pred) {
        .@"and" => |children| try na.dupe(PredicateExpr, children),
        else => try na.dupe(PredicateExpr, &.{pred}),
    };
    var narrowing: std.ArrayList(PredicateExpr) = .empty;
    for (conjuncts) |c| if (!readsSubquery(c)) try narrowing.append(na, c);
    var lowered: LoweredScalars = .{
        .domain = .{
            .input = input,
            .narrow = if (narrowing.items.len > 0) try conjunction(ctx, narrowing.items) else null,
        },
        .late_scalars = late_scalars,
    };
    const above = try na.alloc(bool, conjuncts.len);
    for (conjuncts, above) |*c, *is_above| {
        const joins_before = lowered.joins.items.len;
        const values_before = lowered.outer_values.items.len;
        if (!late_scalars) try lowerPredicateScalars(ctx, c, &lowered);
        try resolveSubqueriesInPredicate(ctx, c, &lowered);
        is_above.* = lowered.joins.items.len > joins_before or lowered.outer_values.items.len > values_before;
    }
    return .{ .conjuncts = conjuncts, .above = above, .lowered = lowered };
}

/// A DELETE's or UPDATE's predicate resolved against the target's rows.
const DmlPredicate = union(enum) {
    /// Keyed lookups only: the statement filters its own scan as before.
    in_place: struct { predicate: ?PredicateExpr, derived: []const ir.Derived },
    /// A subquery lowered to joins: the statement writes the rows these
    /// select, found again by the target's key.
    rows: DmlRows,
};

const DmlRows = struct {
    rows: *ir.Op,
    table: ir.TableRef,
    columns: []const []const u8,
};

/// Keyed paths resolve first, so a predicate they take keeps its plan.
/// One a subquery lowers to joins needs a key to find each row it selects:
/// without one there's no sound row identity, and the statement fails as
/// unsupported. The values the predicate computes first, and those an
/// UPDATE assigns (`values`), read the target's rows as their domain, as a
/// filter's computes read its input.
fn resolveDmlPredicate(ctx: *CompileCtx, table: ir.TableRef, pred: ?PredicateExpr, derived: []const ir.Derived, values: []const ir.Assignment) !DmlPredicate {
    const na = ctx.nodeArena();
    const swapped: SwappedComparisons = if (pred) |p| try swapSubqueryComparisons(ctx, p, derived) else .{ .predicate = .{ .always = true }, .derived = derived };
    var input = try newOp(ctx, .{ .scan = .{ .table = table, .alias = table.name } });
    var computed: LoweredScalars = .{ .domain = .{ .input = input } };
    for (swapped.derived) |*d| try resolveSubqueriesInExpr(ctx, @constCast(&d.expr), &computed);
    for (values) |*a| try resolveSubqueriesInExpr(ctx, @constCast(&a.value), &computed);
    if (computed.any()) input = try joinLoweredScalars(ctx, computed.domain.?.operatorInput(), computed);
    if (swapped.derived.len > 0) input = try newOp(ctx, .{ .compute = .{ .derived = swapped.derived, .upstream = input } });
    const resolved = try lowerConjuncts(ctx, input, swapped.predicate, true);
    const lowered = resolved.lowered;
    if (lowered.joins.items.len == 0 and !computed.any()) return .{ .in_place = .{
        .predicate = if (pred == null) null else try conjunction(ctx, resolved.conjuncts),
        .derived = if (lowered.outer_values.items.len == 0) swapped.derived else try std.mem.concat(na, ir.Derived, &.{ swapped.derived, lowered.outer_values.items }),
    } };
    const t = try local.resolveTable(ctx.catalog, ctx.session.*, table);
    if (!t.schema.unique) return error.UnsupportedCorrelatedSubquery;
    const columns = try na.alloc([]const u8, t.schema.columns.len);
    for (t.schema.columns, columns) |col, *name| name.* = try std.fmt.allocPrint(na, "{s}.{s}", .{ table.name, col.name });
    const rows = if (lowered.any())
        try resolved.filteredRows(ctx)
    else
        try newOp(ctx, .{ .filter = .{ .predicate = try conjunction(ctx, resolved.conjuncts), .upstream = input } });
    return .{ .rows = .{ .rows = rows, .table = table, .columns = columns } };
}

const SwappedComparisons = struct {
    predicate: PredicateExpr,
    derived: []const ir.Derived,
};

/// A statement's predicate with each comparison that reads a correlated
/// scalar subquery the statement computes as a value first (`(SELECT ...) =
/// 1`) spelled `1 = (SELECT ...)`, as the parser spells the subquery on the
/// right: the keyed path then filters in place on any target, as it does
/// that order. A literal operand becomes a value the comparison reads. A
/// subquery value read any other way stays computed.
fn swapSubqueryComparisons(ctx: *CompileCtx, pred: PredicateExpr, derived: []const ir.Derived) !SwappedComparisons {
    const na = ctx.nodeArena();
    var swap: ComparisonSwap = .{ .ctx = ctx, .derived = derived };
    const out = try swap.predicate(pred);
    if (swap.swapped.items.len == 0) return .{ .predicate = pred, .derived = derived };
    var reads: std.ArrayListUnmanaged([]const u8) = .empty;
    try exec.predicate.collectColumnNames(na, &reads, out);
    var kept: std.ArrayList(ir.Derived) = .empty;
    for (derived) |d| {
        const unread = listed(swap.swapped.items, d.name) and !listed(reads.items, d.name) and !derivedReads(derived, d.name);
        if (!unread) try kept.append(na, d);
    }
    try kept.appendSlice(na, swap.literals.items);
    return .{ .predicate = out, .derived = kept.items };
}

const ComparisonSwap = struct {
    ctx: *CompileCtx,
    derived: []const ir.Derived,
    swapped: std.ArrayList([]const u8) = .empty,
    literals: std.ArrayList(ir.Derived) = .empty,

    fn predicate(self: *ComparisonSwap, pred: PredicateExpr) !PredicateExpr {
        const na = self.ctx.nodeArena();
        switch (pred) {
            .leaf => |l| {
                if (l.as_boolean) return pred;
                const source = (try self.subqueryOf(l.col)) orelse return pred;
                const name = try std.fmt.allocPrint(na, "__csq_lit{d}", .{self.ctx.lowered_scalars});
                self.ctx.lowered_scalars += 1;
                try self.literals.append(na, .{ .name = name, .expr = .{ .lit = l.val } });
                return .{ .scalar_subquery = .{ .col = name, .op = flipRangeOp(l.op), .source = source } };
            },
            .leaf_col_col => |c| {
                const left = try self.subqueryOf(c.left);
                const right = try self.subqueryOf(c.right);
                if (left != null and right != null) return pred;
                if (right) |source| return .{ .scalar_subquery = .{ .col = c.left, .op = c.op, .source = source } };
                if (left) |source| return .{ .scalar_subquery = .{ .col = c.right, .op = flipRangeOp(c.op), .source = source } };
                return pred;
            },
            .@"and", .@"or" => |children| {
                const copies = try na.alloc(PredicateExpr, children.len);
                for (children, copies) |child, *dst| dst.* = try self.predicate(child);
                return if (pred == .@"and") .{ .@"and" = copies } else .{ .@"or" = copies };
            },
            .not => |child| {
                const copy = try na.create(PredicateExpr);
                copy.* = try self.predicate(child.*);
                return .{ .not = copy };
            },
            else => return pred,
        }
    }

    /// The subquery a correlated scalar value named `name` holds alone.
    fn subqueryOf(self: *ComparisonSwap, name: []const u8) !?*const anyopaque {
        for (self.derived) |d| {
            if (!types.columnNameEql(d.name, name)) continue;
            const source = switch (d.expr) {
                .scalar_subquery => |s| s,
                else => return null,
            };
            if (!try readsFree(self.ctx, @ptrCast(@alignCast(source)))) return null;
            try self.swapped.append(self.ctx.nodeArena(), d.name);
            return source;
        }
        return null;
    }
};

/// Whether any of `derived` reads `name`.
fn derivedReads(derived: []const ir.Derived, name: []const u8) bool {
    for (derived) |d| if (exprReadsName(d.expr, name)) return true;
    return false;
}

fn exprReadsName(e: ir.Expr, name: []const u8) bool {
    return switch (e) {
        .col_ref => |ref| types.columnNameEql(ref, name),
        .lit, .null_lit, .var_ref, .scalar_subquery, .exists_subquery => false,
        .call => |c| for (c.args) |arg| {
            if (exprReadsName(arg, name)) break true;
        } else false,
        .case => |cs| {
            for (cs.operands) |o| if (exprReadsName(o.expr, name)) return true;
            for (cs.branches) |br| if (exec.predicate.touchesColumn(br.cond, name) or exprReadsName(br.then, name)) return true;
            return if (cs.else_branch) |eb| exprReadsName(eb.*, name) else false;
        },
    };
}

/// The SELECT a statement over `rows` writes from: every target column,
/// then each assignment's value.
fn dmlSource(ctx: *CompileCtx, rows: DmlRows, values: []const ir.Derived) !*ir.Op {
    const na = ctx.nodeArena();
    if (values.len == 0) return try newOp(ctx, .{ .select = .{ .columns = rows.columns, .upstream = rows.rows } });
    const columns = try na.alloc([]const u8, rows.columns.len + values.len);
    @memcpy(columns[0..rows.columns.len], rows.columns);
    for (values, columns[rows.columns.len..]) |v, *col| col.* = v.name;
    const computed = try newOp(ctx, .{ .compute = .{ .derived = values, .upstream = rows.rows } });
    return try newOp(ctx, .{ .select = .{ .columns = columns, .upstream = computed } });
}

/// Each assignment's value as a column of the statement's SELECT, which the
/// assignment then reads by name.
fn assignedValues(ctx: *CompileCtx, assignments: []ir.Assignment) ![]const ir.Derived {
    const na = ctx.nodeArena();
    const values = try na.alloc(ir.Derived, assignments.len);
    for (assignments, values, 0..) |*a, *value, i| {
        value.* = .{ .name = try std.fmt.allocPrint(na, "__set_{d}", .{i}), .expr = a.value };
        a.value = .{ .col_ref = value.name };
    }
    return values;
}

fn dmlTargets(ctx: *CompileCtx, table: ir.TableRef) ![]const ir.DmlTarget {
    return try ctx.nodeArena().dupe(ir.DmlTarget, &.{.{ .table = table, .qualifier = table.name }});
}

/// An outer join's ON condition reads a subquery no keyed path takes: its
/// domain is the pairs the join's keys and ranges match. Each input's rows
/// are numbered once; the pairs the condition keeps are found as number
/// pairs, which the outer join then matches on in its place. The values the
/// condition computes first read that domain too.
fn resolveResidual(ctx: *CompileCtx, op: *ir.Op) !void {
    const j = op.join;
    const res = &op.join.residual.?;
    if (j.extra_predicate != null) {
        for (res.derived) |*d| try resolveSubqueriesInExpr(ctx, @constCast(&d.expr), null);
        res.derived = try resolveWithOuterValues(ctx, &res.predicate, res.derived);
        return;
    }
    const na = ctx.nodeArena();
    const n = ctx.lowered_scalars;
    ctx.lowered_scalars += 1;
    const left_rid = try std.fmt.allocPrint(na, "__rid_l{d}", .{n});
    const right_rid = try std.fmt.allocPrint(na, "__rid_r{d}", .{n});
    const matched_left = try std.fmt.allocPrint(na, "__rid_ml{d}", .{n});
    const matched_right = try std.fmt.allocPrint(na, "__rid_mr{d}", .{n});
    const left = try numberedRows(ctx, j.left, left_rid);
    const right = try numberedRows(ctx, j.right, right_rid);
    var pairs = try newOp(ctx, .{ .join = .{
        .algorithm = j.algorithm,
        .join_type = .inner,
        .on = j.on,
        .ranges = j.ranges,
        .extra_predicate = null,
        .skew_ratio_threshold = j.skew_ratio_threshold,
        .skew_absolute_threshold = j.skew_absolute_threshold,
        .skew_sample_interval = j.skew_sample_interval,
        .left = left,
        .right = right,
    } });
    var computed: LoweredScalars = .{ .domain = .{ .input = pairs } };
    for (res.derived) |*d| try resolveSubqueriesInExpr(ctx, @constCast(&d.expr), &computed);
    if (computed.any()) pairs = try joinLoweredScalars(ctx, computed.domain.?.operatorInput(), computed);
    if (res.derived.len > 0) pairs = try newOp(ctx, .{ .compute = .{ .derived = res.derived, .upstream = pairs } });
    const resolved = try lowerConjuncts(ctx, pairs, res.predicate, true);
    const lowered = resolved.lowered;
    if (lowered.joins.items.len == 0 and !computed.any()) {
        res.predicate = try conjunction(ctx, resolved.conjuncts);
        if (lowered.outer_values.items.len > 0) res.derived = try std.mem.concat(na, ir.Derived, &.{ res.derived, lowered.outer_values.items });
        return;
    }
    const kept = if (lowered.any())
        try resolved.filteredRows(ctx)
    else
        try newOp(ctx, .{ .filter = .{ .predicate = try conjunction(ctx, resolved.conjuncts), .upstream = pairs } });
    const matched = try newOp(ctx, .{ .select = .{
        .columns = try na.dupe([]const u8, &.{ left_rid, right_rid }),
        .outputs = try na.dupe(?[]const u8, &.{ matched_left, matched_right }),
        .upstream = kept,
    } });
    var first = j;
    first.join_type = if (j.join_type == .left or j.join_type == .full) .left else .inner;
    first.on = try na.dupe(ir.JoinKeyPair, &.{.{ .left = left_rid, .right = matched_left }});
    first.ranges = &.{};
    first.residual = null;
    first.left = left;
    first.right = matched;
    var second = j;
    second.on = try std.mem.concat(na, ir.JoinKeyPair, &.{ j.on, &.{.{ .left = matched_right, .right = right_rid }} });
    second.ranges = &.{};
    second.residual = null;
    second.left = try newOp(ctx, .{ .join = first });
    second.right = right;
    op.* = .{ .exclude = .{
        .columns = try na.dupe([]const u8, &.{ left_rid, matched_right }),
        .upstream = try newOp(ctx, .{ .join = second }),
    } };
}

/// A join input's rows, each numbered once as `rid`. A table scan
/// compiles without its alias inside the buffer, which the alias then
/// qualifies again, as the join reads the scan's columns.
fn numberedRows(ctx: *CompileCtx, input: *ir.Op, rid: []const u8) !*ir.Op {
    const rows = try newOp(ctx, .{ .materialize = .{ .upstream = try rankWithin(ctx, &.{}, &.{}, rid, input) } });
    const alias = scanAlias(input) orelse return rows;
    return try newOp(ctx, .{ .alias = .{ .alias = alias, .upstream = rows } });
}

/// The alias of the table scan under a join input's computes and filters.
fn scanAlias(op: *const ir.Op) ?[]const u8 {
    var cur = op;
    while (true) switch (cur.*) {
        .scan => |sc| return sc.alias,
        .compute => |c| cur = c.upstream,
        .filter => |f| cur = f.upstream,
        else => return null,
    };
}

// =============================================================================
// Correlated scalar subquery.
// =============================================================================

/// Decorrelate a correlated scalar subquery that is one global aggregate:
/// grouped by the correlation keys instead, its rows map each key to the
/// aggregate the outer row compares with. Returns true when correlated and
/// pred.* was rewritten.
fn maybeResolveCorrelatedScalar(ctx: *CompileCtx, pred: *PredicateExpr, sq: anytype) !bool {
    const shape = (try analyzeScalarAggregate(ctx, sq.source)) orelse return false;
    const info = if (shape.info) |*i| i else return false;
    if (info.outer_cols.items.len == 0 or info.outer_values.items.len > 0) return false;
    if (shape.aggs.len != 1 or shape.pre.len > 0 or shape.post.len > 0) return false;
    // A range correlation can't key the aggregate: each outer row would
    // aggregate its own range of rows.
    if (info.range_corrs.items.len > 0) return false;

    const aa = try ctx.subqueryArena();
    const gb_new = try groupByKeys(ctx, info, shape.aggs, try keptRows(ctx, info));
    if (try readsFree(ctx, gb_new)) return false;
    try prepareSubplan(ctx, gb_new);

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
                key[j] = try extractKeyValueAt(aa, view, schema[j].type, i);
            }
            if (any_null) continue;
            const agg_view = batch.values[agg_col_idx];
            if (!agg_view.isValid(i)) continue;
            const v = try extractScalarValueAt(aa, agg_view, i);
            try rows.append(aa, .{ .key = key, .value = v });
        }
    }
    const rows_owned = try rows.toOwnedSlice(aa);
    exec.predicate.sortKeyed(exec.predicate.CorrelatedScalarRow, rows_owned);

    pred.* = .{ .correlated_scalar = .{
        .outer_compared = try aa.dupe(u8, sq.col),
        .op = sq.op,
        .outer_keys = outer_keys_owned,
        .rows = rows_owned,
        .value_type = schema[agg_col_idx].type,
        .key_types = try columnTypes(aa, schema[0..info.inner_cols.items.len]),
        .missing = exec.aggregate_op.emptyValue(shape.aggs[0].func),
    } };
    return true;
}

// =============================================================================
// Domain decorrelation: a correlated subquery no keyed path takes, lifted
// onto the distinct enclosing values it reads.
// =============================================================================

const LiftError = error{ NotLiftable, BadRequest } || Allocator.Error;

/// The rows an operator reads, which a lifted subquery's domain is drawn
/// from. A source cheap and deterministic to run again is replayed for the
/// domain; any other is materialized once and read by both.
const DomainSource = struct {
    input: *ir.Op,
    /// The operator's conjuncts that read no subquery: a row they drop
    /// never reads the subquery, so its values stay out of the domain.
    narrow: ?PredicateExpr = null,
    shared: ?*ir.Op = null,

    fn rows(self: *DomainSource, ctx: *CompileCtx) !*ir.Op {
        const base = if (self.shared) |shared|
            shared
        else if (replayable(ctx, self.input, 0))
            try self.input.cloneTree(ctx.nodeArena())
        else blk: {
            const shared = try newOp(ctx, .{ .materialize = .{ .upstream = self.input } });
            self.shared = shared;
            break :blk shared;
        };
        const narrow = self.narrow orelse return base;
        return try newOp(ctx, .{ .filter = .{ .predicate = narrow, .upstream = base } });
    }

    /// The operator's input: the shared buffer once the domain reads one.
    fn operatorInput(self: *const DomainSource) *ir.Op {
        return self.shared orelse self.input;
    }

    /// Whether the operator's rows carry each of `names`: above a grouping
    /// or a select, only what that keeps and what's computed over it;
    /// otherwise what the input's FROM and operators bind.
    fn carries(self: *const DomainSource, ctx: *CompileCtx, names: []const []const u8) !bool {
        const na = ctx.nodeArena();
        var computed: std.ArrayList([]const u8) = .empty;
        var cur: *const ir.Op = self.input;
        while (true) switch (cur.*) {
            .filter => |f| cur = f.upstream,
            .order_by => |o| cur = o.upstream,
            .limit => |l| cur = l.upstream,
            .compute => |c| {
                for (c.derived) |d| try computed.append(na, d.name);
                cur = c.upstream;
            },
            .window => |w| {
                for (w.calls) |call| try computed.append(na, call.output_name);
                cur = w.upstream;
            },
            .group_by => |g| {
                for (names) |name| {
                    if (!listed(computed.items, name) and !keptByGrouping(g, name)) return false;
                }
                return true;
            },
            .select => |p| {
                if (hasStar(p.columns)) break;
                for (names) |name| {
                    if (!listed(computed.items, name) and !projects(p, name)) return false;
                }
                return true;
            },
            else => break,
        };
        const scope = try ScopeBuilder.build(ctx, try splitBlock(ctx, self.input), true);
        for (names) |name| if (!scope.binds(name)) return false;
        return true;
    }
};

/// Whether running `op` twice yields the same rows cheaply: a scan under
/// row-local operators, or a buffer already materialized.
fn replayable(ctx: *CompileCtx, op: *const ir.Op, depth: u32) bool {
    if (depth >= SCOPE_DEPTH_LIMIT) return false;
    return switch (op.*) {
        .scan, .single_row, .materialize => true,
        .alias => |a| replayable(ctx, a.upstream, depth + 1),
        .filter => |f| !readsSubquery(f.predicate) and replayable(ctx, f.upstream, depth + 1),
        .select, .exclude => |p| replayable(ctx, p.upstream, depth + 1),
        .compute => |c| {
            for (c.derived) |d| if (!deterministic(ctx, d.expr)) return false;
            return replayable(ctx, c.upstream, depth + 1);
        },
        else => false,
    };
}

fn deterministic(ctx: *CompileCtx, e: ir.Expr) bool {
    return switch (e) {
        .col_ref, .lit, .null_lit, .var_ref => true,
        .call => |c| {
            if (volatileFn(ctx, c.fn_name)) return false;
            for (c.args) |arg| if (!deterministic(ctx, arg)) return false;
            return true;
        },
        .case => |cs| {
            for (cs.operands) |o| if (!deterministic(ctx, o.expr)) return false;
            for (cs.branches) |br| if (!deterministic(ctx, br.then)) return false;
            if (cs.else_branch) |eb| return deterministic(ctx, eb.*);
            return true;
        },
        .scalar_subquery, .exists_subquery => false,
    };
}

fn readsSubquery(pred: PredicateExpr) bool {
    return switch (pred) {
        .scalar_subquery, .in_subquery, .exists_subquery => true,
        .@"and", .@"or" => |children| for (children) |child| {
            if (readsSubquery(child)) break true;
        } else false,
        .not => |child| readsSubquery(child.*),
        else => false,
    };
}

fn volatileFn(ctx: *CompileCtx, name: []const u8) bool {
    if (parser.isNondeterministicFn(name) or std.mem.eql(u8, name, exec.scalar_fn.UUID_SHORT_FN)) return true;
    for (exec.scalar_fn.overloadsOf(name)) |f| if (f.volatility == .@"volatile") return true;
    const registry = ctx.udf_registry orelse return false;
    for (registry.scalarEntries()) |entry| {
        if (std.ascii.eqlIgnoreCase(entry.name, name) and entry.volatility == .@"volatile") return true;
    }
    return false;
}

/// The names a block reads from enclosing queries, bound as SQL scopes
/// them, each renamed (when `rename`) to the domain column that carries it.
/// Copies the block's operators, and each relation that reads an enclosing
/// query; a relation that doesn't is kept as it is, so a CTE's readers still
/// share its stage.
const FreeNames = struct {
    ctx: *CompileCtx,
    rename: bool = true,
    /// The domain's alias, made at the first free name.
    domain: ?[]const u8 = null,
    /// The block's scope and those of the blocks nested in it, innermost
    /// last.
    scopes: std.ArrayList(Scope) = .empty,
    case_operands: []const exec.expr_mod.Expr.Operand = &.{},
    /// Each free name, and the domain column it became.
    free: std.ArrayList([]const u8) = .empty,
    columns: std.ArrayList([]const u8) = .empty,
    /// Names read so far, those free, and nested blocks entered.
    reads: usize = 0,
    free_reads: usize = 0,
    nested_blocks: usize = 0,
    /// Computed columns that read only free names, by scope depth.
    outer_only: std.ArrayList(Computed) = .empty,
    /// A domain alias whose columns are watched for: whether a renamed
    /// relation reads its domain.
    watch: ?[]const u8 = null,
    watched: bool = false,
    /// Stars `expand` couldn't spell out, in operators the copy keeps.
    stars: usize = 0,

    const Computed = struct { depth: usize, name: []const u8 };

    fn enter(self: *FreeNames, b: Block) LiftError!void {
        if (self.scopes.items.len >= SCOPE_DEPTH_LIMIT) return error.NotLiftable;
        try self.scopes.append(self.ctx.nodeArena(), try ScopeBuilder.build(self.ctx, b, true));
    }

    fn bound(self: *const FreeNames, ref: []const u8) bool {
        var i = self.scopes.items.len;
        while (i > 0) {
            i -= 1;
            if (self.scopes.items[i].binds(ref)) return true;
        }
        return false;
    }

    fn readName(self: *FreeNames, ref: []const u8) LiftError![]const u8 {
        if (ref.len == 0 or exec.expr_mod.operandListed(self.case_operands, ref)) return ref;
        if (self.watch) |alias| {
            if (domainColumnOf(alias, ref)) self.watched = true;
        }
        self.reads += 1;
        if (self.bound(ref)) return ref;
        self.free_reads += 1;
        for (self.free.items, 0..) |f, i| if (types.columnNameEql(f, ref)) return if (self.rename) self.columns.items[i] else ref;
        const na = self.ctx.nodeArena();
        try self.free.append(na, ref);
        if (!self.rename) return ref;
        const domain = self.domain orelse blk: {
            const alias = try std.fmt.allocPrint(na, "__dom{d}", .{self.ctx.lowered_scalars});
            self.ctx.lowered_scalars += 1;
            self.domain = alias;
            break :blk alias;
        };
        const column = try std.fmt.allocPrint(na, "{s}.__d{d}", .{ domain, self.columns.items.len });
        try self.columns.append(na, column);
        return column;
    }

    fn readNames(self: *FreeNames, refs: []const []const u8) LiftError![]const []const u8 {
        const out = try self.ctx.nodeArena().alloc([]const u8, refs.len);
        for (refs, out) |ref, *dst| dst.* = try self.readName(ref);
        return out;
    }

    /// An aggregate's argument. One that reads only the enclosing row
    /// aggregates in the enclosing query, which a lifted block can't. An
    /// analysis that only tells the free names apart reads it like any other.
    fn aggArg(self: *FreeNames, col: []const u8) LiftError![]const u8 {
        if (!self.rename) return try self.readName(col);
        const depth = self.scopes.items.len;
        for (self.outer_only.items) |c| if (c.depth == depth and types.columnNameEql(c.name, col)) return error.NotLiftable;
        if (!self.bound(col)) return error.NotLiftable;
        return col;
    }

    fn readPredicate(self: *FreeNames, pred: PredicateExpr) LiftError!PredicateExpr {
        const na = self.ctx.nodeArena();
        var out = pred;
        switch (out) {
            .leaf, .day_leaf, .text_as_number => |*l| l.col = try self.readName(l.col),
            .leaf_col_col => |*c| {
                c.left = try self.readName(c.left);
                c.right = try self.readName(c.right);
            },
            .is_null, .is_not_null => |*col| col.* = try self.readName(col.*),
            .like => |*l| l.col = try self.readName(l.col),
            .in_set, .text_as_number_set => |*s| s.col = try self.readName(s.col),
            .leaf_var => |*v| v.col = try self.readName(v.col),
            .scalar_subquery => |*s| {
                s.col = try self.readName(s.col);
                s.source = try self.readNested(s.source);
            },
            .in_subquery => |*s| {
                s.col = try self.readName(s.col);
                s.rest_cols = try self.readNames(s.rest_cols);
                s.source = try self.readNested(s.source);
            },
            .exists_subquery => |*source| source.* = try self.readNested(source.*),
            .@"and", .@"or" => |*children| {
                const copies = try na.alloc(PredicateExpr, children.len);
                for (children.*, copies) |child, *dst| dst.* = try self.readPredicate(child);
                children.* = copies;
            },
            .not => |*child| {
                const copy = try na.create(PredicateExpr);
                copy.* = try self.readPredicate(child.*.*);
                child.* = copy;
            },
            .always, .unknown => {},
            .correlated_set, .correlated_scalar, .correlated_range => return error.NotLiftable,
        }
        return out;
    }

    fn readExpr(self: *FreeNames, e: ir.Expr) LiftError!ir.Expr {
        const na = self.ctx.nodeArena();
        switch (e) {
            .col_ref => |ref| return .{ .col_ref = try self.readName(ref) },
            .lit, .null_lit, .var_ref => return e,
            .call => |c| {
                var copy = c;
                const args = try na.alloc(ir.Expr, c.args.len);
                for (c.args, args) |arg, *dst| dst.* = try self.readExpr(arg);
                copy.args = args;
                return .{ .call = copy };
            },
            .case => |cs| {
                var copy = cs;
                const operands = try na.dupe(exec.expr_mod.Expr.Operand, cs.operands);
                for (operands) |*o| o.expr = try self.readExpr(o.expr);
                copy.operands = operands;
                const branches = try na.dupe(exec.expr_mod.Expr.Branch, cs.branches);
                for (branches) |*br| {
                    const enclosing = self.case_operands;
                    self.case_operands = cs.operands;
                    br.cond = try self.readPredicate(br.cond);
                    self.case_operands = enclosing;
                    br.then = try self.readExpr(br.then);
                }
                copy.branches = branches;
                if (cs.else_branch) |eb| {
                    const else_copy = try na.create(ir.Expr);
                    else_copy.* = try self.readExpr(eb.*);
                    copy.else_branch = else_copy;
                }
                return .{ .case = copy };
            },
            .scalar_subquery => |source| return .{ .scalar_subquery = try self.readNested(source) },
            .exists_subquery => |source| return .{ .exists_subquery = try self.readNested(source) },
        }
    }

    fn readNested(self: *FreeNames, source: *const anyopaque) LiftError!*ir.Op {
        self.nested_blocks += 1;
        return try self.readBlock(@ptrCast(@alignCast(source)), false);
    }

    /// A copy of the block under `top`, set operation arms each their own.
    /// `expand` spells out each `*` its selects and its relations' read:
    /// lifted, the rows carry domain columns a star would take too.
    fn readBlock(self: *FreeNames, top: *const ir.Op, expand: bool) LiftError!*ir.Op {
        if (top.* == .set_union) {
            var u = top.set_union;
            u.left = try self.readBlock(u.left, expand);
            u.right = try self.readBlock(u.right, expand);
            return try newOp(self.ctx, .{ .set_union = u });
        }
        const b = try splitBlock(self.ctx, top);
        try self.enter(b);
        const computed = self.outer_only.items.len;
        var cur = try self.readRelation(b.from, 0, expand);
        var i = b.chain.len;
        while (i > 0) {
            i -= 1;
            const copy = try self.readOp(b.chain[i], expand);
            relink(copy, cur);
            cur = copy;
        }
        self.outer_only.shrinkRetainingCapacity(computed);
        _ = self.scopes.pop();
        return cur;
    }

    /// A relation the block reads, copied with its names renamed when it
    /// reads an enclosing query; the same node when it doesn't.
    fn readRelation(self: *FreeNames, op: *const ir.Op, depth: u32, expand: bool) LiftError!*ir.Op {
        if (depth >= SCOPE_DEPTH_LIMIT) return error.NotLiftable;
        const free_reads = self.free_reads;
        const stars = self.stars;
        const copy = try self.copyRelation(op, depth, expand);
        if (self.free_reads != free_reads) return copy;
        self.stars = stars;
        return @constCast(op);
    }

    fn copyRelation(self: *FreeNames, op: *const ir.Op, depth: u32, expand: bool) LiftError!*ir.Op {
        const na = self.ctx.nodeArena();
        switch (op.*) {
            .scan, .file_scan, .single_row => return @constCast(op),
            .alias => |a| {
                var copy = a;
                copy.upstream = try self.readRelation(a.upstream, depth + 1, expand);
                return try newOp(self.ctx, .{ .alias = copy });
            },
            .materialize => |m| {
                // A named boundary is a CTE, view or function: its own
                // statement.
                if (m.name != null) return @constCast(op);
                var copy = m;
                copy.upstream = try self.readBlock(m.upstream, expand);
                return try newOp(self.ctx, .{ .materialize = copy });
            },
            .table_fn => |t| {
                // A function's input runs once, not per enclosing row.
                const free_reads = self.free_reads;
                for (t.inputs) |input| _ = try self.readBlock(input, false);
                if (self.free_reads != free_reads) return error.NotLiftable;
                return @constCast(op);
            },
            .set_union => return try self.readBlock(op, expand),
            .join => |j| {
                var copy = j;
                const on = try na.dupe(ir.JoinKeyPair, j.on);
                for (on) |*pair| {
                    pair.left = try self.readName(pair.left);
                    pair.right = try self.readName(pair.right);
                }
                const ranges = try na.dupe(ir.JoinRangePredicate, j.ranges);
                for (ranges) |*r| {
                    r.left = try self.readName(r.left);
                    r.right = try self.readName(r.right);
                }
                copy.on = on;
                copy.ranges = ranges;
                if (j.extra_predicate) |p| copy.extra_predicate = try self.readPredicate(p);
                if (j.residual) |r| {
                    const derived = try na.dupe(ir.Derived, r.derived);
                    for (derived) |*d| d.expr = try self.readExpr(d.expr);
                    copy.residual = .{ .derived = derived, .predicate = try self.readPredicate(r.predicate) };
                }
                copy.left = try self.readRelation(j.left, depth + 1, expand);
                copy.right = try self.readRelation(j.right, depth + 1, expand);
                return try newOp(self.ctx, .{ .join = copy });
            },
            else => if (blockUpstream(op) != null) {
                return try self.readBlock(op, expand);
            } else return error.NotLiftable,
        }
    }

    /// A copy of one of a block's operators over the same upstream, its
    /// names renamed.
    fn readOp(self: *FreeNames, o: *const ir.Op, expand: bool) LiftError!*ir.Op {
        const na = self.ctx.nodeArena();
        var copy = o.*;
        switch (copy) {
            .limit, .exclude => {},
            .select => |*p| {
                if (expand and hasStar(p.columns)) {
                    if (try expandStars(self.ctx, p.*)) |expanded| p.* = expanded else self.stars += 1;
                }
                const columns = try na.alloc([]const u8, p.columns.len);
                var outputs: ?[]?[]const u8 = null;
                for (p.columns, columns, 0..) |col, *dst, i| {
                    dst.* = if (isStar(col)) col else try self.readName(col);
                    if (std.mem.eql(u8, dst.*, col) or outputName(p.*, i) != null) continue;
                    // An item that returns an enclosing column keeps that
                    // column's name, not the domain column's it now reads.
                    const named = outputs orelse blk: {
                        const kept = try na.alloc(?[]const u8, p.columns.len);
                        for (kept, 0..) |*output, j| output.* = outputName(p.*, j);
                        outputs = kept;
                        break :blk kept;
                    };
                    named[i] = referenceName(p.*, i);
                }
                p.columns = columns;
                if (outputs) |named| p.outputs = named;
                try self.bindOutputs(p.*);
            },
            .order_by => |*ob| {
                const specs = try na.dupe(ir.SortSpec, ob.specs);
                for (specs) |*s| s.col = try self.readName(s.col);
                ob.specs = specs;
            },
            .compute => |*c| {
                const derived = try na.dupe(ir.Derived, c.derived);
                for (derived) |*d| {
                    const reads = self.reads;
                    const free_reads = self.free_reads;
                    const nested_blocks = self.nested_blocks;
                    d.expr = try self.readExpr(d.expr);
                    const free = self.free_reads - free_reads;
                    if (free > 0 and free == self.reads - reads and self.nested_blocks == nested_blocks) {
                        try self.outer_only.append(na, .{ .depth = self.scopes.items.len, .name = d.name });
                    }
                }
                c.derived = derived;
            },
            .window => |*w| {
                const specs = try na.dupe(ir.WindowSpec, w.specs);
                for (specs) |*s| {
                    s.partition_by = try self.readNames(s.partition_by);
                    const order = try na.dupe(ir.SortSpec, s.order_by);
                    for (order) |*o_spec| o_spec.col = try self.readName(o_spec.col);
                    s.order_by = order;
                }
                const calls = try na.dupe(ir.WindowCall, w.calls);
                for (calls) |*call| {
                    const args = try na.alloc(ir.Expr, call.args.len);
                    for (call.args, args) |arg, *dst| dst.* = try self.readExpr(arg);
                    call.args = args;
                }
                w.specs = specs;
                w.calls = calls;
            },
            .filter => |*f| f.predicate = try self.readPredicate(f.predicate),
            .group_by => |*g| {
                g.group_cols = try self.readNames(g.group_cols);
                const aggs = try na.dupe(ir.AggSpec, g.aggs);
                for (aggs) |*a| {
                    if (a.col) |col| a.col = try self.aggArg(col);
                    if (a.arg2_col) |col| a.arg2_col = try self.aggArg(col);
                    for (a.udf_arg_cols) |col| _ = try self.aggArg(col);
                }
                g.aggs = aggs;
            },
            else => return error.NotLiftable,
        }
        return try newOp(self.ctx, copy);
    }

    fn bindOutputs(self: *FreeNames, p: ir.Op.Project) !void {
        const scope = &self.scopes.items[self.scopes.items.len - 1];
        scope.* = try scope.above(self.ctx.nodeArena(), p);
    }
};

fn relink(op: *ir.Op, upstream: *ir.Op) void {
    switch (op.*) {
        .limit => |*l| l.upstream = upstream,
        .select, .exclude => |*p| p.upstream = upstream,
        .order_by => |*o| o.upstream = upstream,
        .compute => |*c| c.upstream = upstream,
        .window => |*w| w.upstream = upstream,
        .filter => |*f| f.upstream = upstream,
        .group_by => |*g| g.upstream = upstream,
        else => {},
    }
}

/// The names `top` reads from enclosing queries; null when that can't be
/// told.
fn freeNamesOf(ctx: *CompileCtx, top: *const ir.Op) !?[]const []const u8 {
    var names: FreeNames = .{ .ctx = ctx, .rename = false };
    _ = names.readBlock(top, false) catch |err| switch (err) {
        error.NotLiftable => return null,
        else => |e| return e,
    };
    return names.free.items;
}

/// Whether a rewritten subplan still reads an enclosing query, as through
/// a subquery nested in it. One that can't be told is left to the scope
/// guard.
fn readsFree(ctx: *CompileCtx, op: *const ir.Op) !bool {
    const free = (try freeNamesOf(ctx, op)) orelse return false;
    return free.len > 0;
}

/// A subquery lifted onto its domain: its rows for every domain row.
const Lifted = struct {
    rows: *ir.Op,
    /// The domain columns the rows carry, one per free name.
    keys: []const []const u8,
    /// Those names as the enclosing query reads them.
    outer: []const []const u8,
    /// The columns the subquery projects.
    selected: []const []const u8,
};

/// Null when the subquery reads no enclosing query, or when it does so in a
/// way lifting can't carry: the uncorrelated paths take it. `width`, when
/// given, is how many columns its consumer reads.
fn liftSubquery(ctx: *CompileCtx, top: *const ir.Op, source: *DomainSource, global: bool, width: ?usize) !?Lifted {
    return liftBlock(ctx, top, source, global, width) catch |err| switch (err) {
        error.NotLiftable => null,
        else => |e| e,
    };
}

/// The block joined with its domain: the distinct combinations of the
/// enclosing values it reads, each renamed to the domain column carrying
/// it, then carried through the block's rows (`Lifter`). Its stars are
/// spelled out first, as its rows then carry the domain columns too.
fn liftBlock(ctx: *CompileCtx, top: *const ir.Op, source: *DomainSource, global: bool, width: ?usize) LiftError!Lifted {
    var names: FreeNames = .{ .ctx = ctx };
    const renamed = try names.readBlock(top, true);
    const outer = names.free.items;
    const keys = names.columns.items;
    if (outer.len == 0 or names.stars > 0) return error.NotLiftable;
    if (!try source.carries(ctx, outer)) return error.NotLiftable;
    const selected = try topSelected(ctx, renamed);
    if (width) |w| {
        if (selected.len == 0) return error.NotLiftable;
        if (selected.len != w) return error.BadRequest;
        for (selected) |s| for (keys) |key| if (types.columnNameEql(s, key)) return error.NotLiftable;
    }
    var lifter: Lifter = .{ .ctx = ctx, .source = source, .alias = names.domain.?, .outer = outer, .keys = keys };
    return .{ .rows = try lifter.block(renamed, global), .keys = keys, .outer = outer, .selected = selected };
}

/// The columns a block projects; a set operation's are its first arm's.
fn topSelected(ctx: *CompileCtx, top: *const ir.Op) ![]const []const u8 {
    var cur = top;
    while (cur.* == .set_union) cur = cur.set_union.left;
    return try selectedColumns(ctx, (try splitBlock(ctx, cur)).chain);
}

/// Lifts a block, its enclosing names renamed to domain columns, onto the
/// domain. Each relation that reads a domain column lifts to carry all of
/// them; one that doesn't is read as it is. WHERE conjuncts comparing a
/// FROM column with a domain column become the join's keys and ranges; the
/// others filter the pairs. Each grouping, window partition and LIMIT then
/// applies within a domain row. A global aggregate at the top of a scalar
/// aggregate (`global`) leaves a domain row with no group to its consumer,
/// which reads the aggregate over none; any other gets that row here.
const Lifter = struct {
    ctx: *CompileCtx,
    source: *DomainSource,
    alias: []const u8,
    outer: []const []const u8,
    keys: []const []const u8,

    /// Whether `op` reads a domain column.
    fn dependent(self: *const Lifter, op: *const ir.Op) LiftError!bool {
        var names: FreeNames = .{ .ctx = self.ctx, .rename = false, .watch = self.alias };
        _ = try names.readBlock(op, false);
        return names.watched;
    }

    /// `top` is a renamed copy, whose operators the lift may take over.
    fn block(self: *Lifter, top: *ir.Op, global: bool) LiftError!*ir.Op {
        if (top.* == .set_union) return try self.setUnion(top);
        const ctx = self.ctx;
        const na = ctx.nodeArena();
        const b = try splitBlock(ctx, top);
        if (!liftable(b.chain)) return error.NotLiftable;
        var kept: std.ArrayList(PredicateExpr) = .empty;
        var where: ?usize = null;
        var cur: *ir.Op = undefined;
        if (try self.dependent(b.from)) {
            cur = try self.relation(@constCast(b.from));
        } else {
            const scope = try liftScope(ctx, b);
            var on: std.ArrayList(ir.JoinKeyPair) = .empty;
            var ranges: std.ArrayList(ir.JoinRangePredicate) = .empty;
            const keyed = try na.alloc(bool, self.keys.len);
            @memset(keyed, false);
            where = whereIndex(b.chain);
            if (where) |w| {
                const pred = b.chain[w].filter.predicate;
                const conjuncts = if (pred == .@"and") pred.@"and" else try na.dupe(PredicateExpr, &.{pred});
                for (conjuncts) |c| {
                    if (domainCorrelation(c, scope, self.keys)) |corr| switch (corr.op) {
                        .eq => if (!keyed[corr.key]) {
                            keyed[corr.key] = true;
                            const twin = try std.fmt.allocPrint(na, "{s}.__j{d}", .{ self.alias, corr.key });
                            try on.append(na, .{ .left = corr.from, .right = twin });
                            continue;
                        },
                        .lt, .lte, .gt, .gte => {
                            try ranges.append(na, .{ .left = corr.from, .op = corr.op, .right = self.keys[corr.key] });
                            continue;
                        },
                        .neq => {},
                    };
                    try kept.append(na, c);
                }
            }
            cur = try joinOn(ctx, .inner, on.items, ranges.items, try reuseInput(ctx, b.from), try domainRows(ctx, self.source, self.alias, self.outer, keyed));
        }
        const rank = "__csq_rn";
        var ranked = false;
        var i = b.chain.len;
        while (i > 0) {
            i -= 1;
            const op = @constCast(b.chain[i]);
            if (where != null and where.? == i) {
                if (kept.items.len > 0) {
                    cur = try newOp(ctx, .{ .filter = .{ .predicate = try conjunction(ctx, kept.items), .upstream = cur } });
                }
                continue;
            }
            switch (op.*) {
                .group_by => |*g| {
                    const whole = g.group_cols.len == 0 and !(global and i == 0);
                    g.group_cols = try withKeys(ctx, g.group_cols, self.keys);
                    if (whole) {
                        relink(op, cur);
                        cur = try self.everyDomainRow(op);
                        continue;
                    }
                },
                .window => |*w| {
                    const specs = try na.dupe(ir.WindowSpec, w.specs);
                    for (specs) |*s| s.partition_by = try withKeys(ctx, s.partition_by, self.keys);
                    w.specs = specs;
                },
                .order_by => |o| {
                    // Order matters only to the LIMIT above it, which then
                    // applies within each domain row.
                    if (limitIn(b.chain[0..i])) {
                        cur = try rankWithin(ctx, self.keys, o.specs, rank, cur);
                        ranked = true;
                    }
                    continue;
                },
                .limit => |l| {
                    if (!ranked) cur = try rankWithin(ctx, self.keys, &.{}, rank, cur);
                    cur = try rankFilter(ctx, rank, l, cur);
                    ranked = false;
                    continue;
                },
                .select => |*p| p.* = try selectKeys(ctx, p.*, self.keys, if (ranked) rank else null),
                else => {},
            }
            relink(op, cur);
            cur = op;
        }
        return cur;
    }

    /// A global aggregate's one row for each domain row: its groups LEFT
    /// JOINed onto the domain, where a domain row with none reads each
    /// aggregate over no rows.
    fn everyDomainRow(self: *Lifter, grouped: *ir.Op) LiftError!*ir.Op {
        const ctx = self.ctx;
        const na = ctx.nodeArena();
        const tag = ctx.lowered_scalars;
        ctx.lowered_scalars += 1;
        const g = &grouped.group_by;
        const aggs = try na.dupe(ir.AggSpec, g.aggs);
        const n_keys = self.keys.len;
        const columns = try na.alloc([]const u8, n_keys + aggs.len);
        const outputs = try na.alloc(?[]const u8, n_keys + aggs.len);
        const on = try na.alloc(ir.JoinKeyPair, n_keys);
        for (self.keys, 0..) |key, i| {
            const twin = try std.fmt.allocPrint(na, "__csq_g{d}k{d}", .{ tag, i });
            columns[i] = key;
            outputs[i] = twin;
            on[i] = .{ .left = key, .right = twin, .null_safe = true };
        }
        const values = try na.alloc(ir.Derived, aggs.len);
        for (aggs, values, n_keys..) |*a, *value, i| {
            const temp = try std.fmt.allocPrint(na, "__csq_g{d}a{d}", .{ tag, i - n_keys });
            value.* = .{ .name = a.as, .expr = try missedJoinValue(ctx, a.func, temp) };
            a.as = temp;
            columns[i] = temp;
            outputs[i] = temp;
        }
        g.aggs = aggs;
        const unkeyed = try na.alloc(bool, n_keys);
        @memset(unkeyed, false);
        const groups = try newOp(ctx, .{ .select = .{ .columns = columns, .outputs = outputs, .upstream = grouped } });
        const joined = try joinOn(ctx, .left, on, &.{}, try domainRows(ctx, self.source, self.alias, self.outer, unkeyed), groups);
        const computed = try newOp(ctx, .{ .compute = .{ .derived = values, .upstream = joined } });
        return try newOp(ctx, .{ .exclude = .{ .columns = columns[n_keys..], .upstream = computed } });
    }

    fn setUnion(self: *Lifter, op: *ir.Op) LiftError!*ir.Op {
        var u = op.set_union;
        u.left = try self.arm(u.left);
        u.right = try self.arm(u.right);
        return try newOp(self.ctx, .{ .set_union = u });
    }

    /// A set operation's arm, its columns then the domain columns: lifted
    /// when it reads them, else paired with every domain row.
    fn arm(self: *Lifter, op: *ir.Op) LiftError!*ir.Op {
        if (op.* == .set_union) return try self.setUnion(op);
        const ctx = self.ctx;
        const na = ctx.nodeArena();
        const selected = try selectedColumns(ctx, (try splitBlock(ctx, op)).chain);
        if (selected.len == 0) return error.NotLiftable;
        for (selected, 0..) |s, i| for (selected[0..i]) |t| {
            if (types.columnNameEql(types.unqualifiedName(s), types.unqualifiedName(t))) return error.NotLiftable;
        };
        const rows = if (try self.dependent(op)) try self.block(op, false) else try self.cross(op);
        const columns = try std.mem.concat(na, []const u8, &.{ selected, self.keys });
        const outputs = try na.alloc(?[]const u8, columns.len);
        for (columns, outputs, 0..) |col, *output, i| output.* = if (i < selected.len) null else col;
        return try newOp(ctx, .{ .select = .{ .columns = columns, .outputs = outputs, .upstream = rows } });
    }

    /// `op`'s rows, each paired with every domain row.
    fn cross(self: *Lifter, op: *const ir.Op) LiftError!*ir.Op {
        const unkeyed = try self.ctx.nodeArena().alloc(bool, self.keys.len);
        @memset(unkeyed, false);
        return try joinOn(self.ctx, .inner, &.{}, &.{}, try reuseInput(self.ctx, op), try domainRows(self.ctx, self.source, self.alias, self.outer, unkeyed));
    }

    /// A relation that reads domain columns, lifted to carry all of them
    /// under their own names.
    fn relation(self: *Lifter, op: *ir.Op) LiftError!*ir.Op {
        switch (op.*) {
            .alias => |a| {
                // The alias qualifies the domain columns too: they pass it
                // under plain names, a qualified qualifier reading as a
                // database's.
                const na = self.ctx.nodeArena();
                const plain = try na.alloc([]const u8, self.keys.len);
                const qualified = try na.alloc([]const u8, self.keys.len);
                for (plain, qualified, 0..) |*name, *q, i| {
                    name.* = try std.fmt.allocPrint(na, "{s}_d{d}", .{ self.alias, i });
                    q.* = try std.fmt.allocPrint(na, "{s}.{s}", .{ a.alias, name.* });
                }
                var copy = a;
                copy.upstream = try copyColumns(self.ctx, self.keys, plain, try self.relation(a.upstream), true);
                return try copyColumns(self.ctx, qualified, self.keys, try newOp(self.ctx, .{ .alias = copy }), true);
            },
            .materialize => |m| {
                if (m.name != null) return error.NotLiftable;
                var copy = m;
                copy.upstream = try self.block(m.upstream, false);
                return try newOp(self.ctx, .{ .materialize = copy });
            },
            .join => return try self.join(op),
            .set_union => return try self.setUnion(op),
            else => return if (blockUpstream(op) != null) try self.block(op, false) else error.NotLiftable,
        }
    }

    /// Each side that reads domain columns lifts, and a side an outer join
    /// preserves pairs with every domain row, so its unmatched rows stay
    /// apart per domain row. Two sides that both carry the domain also
    /// match on it. A FULL join would leave rows unmatched on either side
    /// with no domain row to belong to.
    fn join(self: *Lifter, op: *ir.Op) LiftError!*ir.Op {
        const ctx = self.ctx;
        const na = ctx.nodeArena();
        var j = op.join;
        const dep_left = try self.dependent(j.left);
        const dep_right = try self.dependent(j.right);
        const carry_left = switch (j.join_type) {
            .inner => dep_left or !dep_right,
            .left => true,
            .right => dep_left,
            .full => return error.NotLiftable,
        };
        const carry_right = j.join_type == .right or dep_right;
        j.left = if (dep_left) try self.relation(j.left) else if (carry_left) try self.cross(j.left) else j.left;
        j.right = if (dep_right) try self.relation(j.right) else if (carry_right) try self.cross(j.right) else j.right;
        if (!carry_left or !carry_right) return try newOp(ctx, .{ .join = j });
        const n = self.keys.len;
        const on = try na.alloc(ir.JoinKeyPair, j.on.len + n);
        @memcpy(on[0..j.on.len], j.on);
        const left_keys = try na.alloc([]const u8, n);
        const right_keys = try na.alloc([]const u8, n);
        for (self.keys, left_keys, right_keys, on[j.on.len..], 0..) |key, *l, *r, *pair, i| {
            l.* = if (j.join_type == .right) try std.fmt.allocPrint(na, "{s}.__l{d}", .{ self.alias, i }) else key;
            r.* = try std.fmt.allocPrint(na, "{s}.__r{d}", .{ self.alias, i });
            pair.* = .{ .left = l.*, .right = r.*, .null_safe = true };
        }
        j.on = on;
        if (j.join_type != .right) {
            // The join drops its right keys: the left side's domain
            // columns go on.
            j.right = try copyColumns(ctx, self.keys, right_keys, j.right, true);
            return try newOp(ctx, .{ .join = j });
        }
        // A RIGHT join nulls the left keys of a right row that found no
        // match: the right side's domain columns go on, and it matches on
        // copies of them.
        j.left = try copyColumns(ctx, self.keys, left_keys, j.left, true);
        j.right = try copyColumns(ctx, self.keys, right_keys, j.right, false);
        return try newOp(ctx, .{ .exclude = .{ .columns = left_keys, .upstream = try newOp(ctx, .{ .join = j }) } });
    }
};

fn joinOn(ctx: *CompileCtx, join_type: ir.JoinType, on: []const ir.JoinKeyPair, ranges: []const ir.JoinRangePredicate, left: *ir.Op, right: *ir.Op) !*ir.Op {
    return try newOp(ctx, .{ .join = .{
        .algorithm = .auto,
        .join_type = join_type,
        .on = on,
        .ranges = ranges,
        .extra_predicate = null,
        .skew_ratio_threshold = 0.3,
        .skew_absolute_threshold = 20_000,
        .skew_sample_interval = 10,
        .left = left,
        .right = right,
    } });
}

/// `input` with each of `columns` also under the name beside it in
/// `names`, or only under that name when `move`.
fn copyColumns(ctx: *CompileCtx, columns: []const []const u8, names: []const []const u8, input: *ir.Op, move: bool) !*ir.Op {
    const derived = try ctx.nodeArena().alloc(ir.Derived, columns.len);
    for (columns, names, derived) |col, name, *d| d.* = .{ .name = name, .expr = .{ .col_ref = col } };
    const computed = try newOp(ctx, .{ .compute = .{ .derived = derived, .upstream = input } });
    if (!move) return computed;
    return try newOp(ctx, .{ .exclude = .{ .columns = columns, .upstream = computed } });
}

/// The names a block binds, as the WHERE of its lifted copy reads them.
fn liftScope(ctx: *CompileCtx, b: Block) !Scope {
    const na = ctx.nodeArena();
    var scope = try ScopeBuilder.build(ctx, b, true);
    var derived: std.ArrayList([]const u8) = .empty;
    try derived.appendSlice(na, scope.derived);
    for (b.chain) |op| if (op.* == .select) if (op.select.outputs) |outs| for (outs) |o| if (o) |name| try derived.append(na, name);
    scope.derived = derived.items;
    return scope;
}

/// Whether the lifted operators can each apply per domain row: one LIMIT
/// at most, over one ORDER BY at most.
fn liftable(chain: []const *const ir.Op) bool {
    var limits: usize = 0;
    var orders: usize = 0;
    for (chain) |op| switch (op.*) {
        .limit => limits += 1,
        .order_by => if (limits > 0) {
            orders += 1;
        },
        else => {},
    };
    return limits <= 1 and orders <= 1;
}

fn limitIn(chain: []const *const ir.Op) bool {
    for (chain) |op| if (op.* == .limit) return true;
    return false;
}

/// The chain index of the block's WHERE: a filter with only computes and
/// excludes between it and the FROM.
fn whereIndex(chain: []const *const ir.Op) ?usize {
    var i = chain.len;
    while (i > 0) {
        i -= 1;
        switch (chain[i].*) {
            .compute, .exclude => continue,
            .filter => return i,
            else => return null,
        }
    }
    return null;
}

/// The columns the block's top select or grouping projects.
fn selectedColumns(ctx: *CompileCtx, chain: []const *const ir.Op) ![]const []const u8 {
    for (chain) |op| switch (op.*) {
        .limit, .order_by => continue,
        .select => |p| {
            const out = try ctx.nodeArena().alloc([]const u8, p.columns.len);
            for (p.columns, out, 0..) |col, *dst, i| dst.* = if (p.outputs) |outs| outs[i] orelse col else col;
            return out;
        },
        .group_by => |g| {
            const out = try ctx.nodeArena().alloc([]const u8, g.group_cols.len + g.aggs.len);
            @memcpy(out[0..g.group_cols.len], g.group_cols);
            for (g.aggs, out[g.group_cols.len..]) |a, *dst| dst.* = a.as;
            return out;
        },
        else => return &.{},
    };
    return &.{};
}

fn hasStar(columns: []const []const u8) bool {
    for (columns) |col| if (isStar(col)) return true;
    return false;
}

/// Whether `ref` is a column of the domain under `alias`.
fn domainColumnOf(alias: []const u8, ref: []const u8) bool {
    return ref.len > alias.len and std.mem.startsWith(u8, ref, alias) and ref[alias.len] == '.';
}

fn listed(names: []const []const u8, name: []const u8) bool {
    for (names) |n| if (types.columnNameEql(n, name)) return true;
    return false;
}

/// Whether a grouping's rows carry `name`: one of its keys or aggregates.
fn keptByGrouping(g: ir.Op.GroupBy, name: []const u8) bool {
    for (g.group_cols) |col| if (sameColumn(col, name)) return true;
    for (g.aggs) |a| if (types.columnNameEql(a.as, name)) return true;
    return false;
}

/// The name a select gives item `i`, a column reference without an output
/// name, as its compile does: the bare column, or the reference as written
/// where another item's name is that bare column too.
fn referenceName(p: ir.Op.Project, i: usize) []const u8 {
    const bare = types.unqualifiedName(p.columns[i]);
    for (p.columns, 0..) |other, j| {
        if (j == i or isStar(other)) continue;
        if (types.columnNameEql(outputName(p, j) orelse types.unqualifiedName(other), bare)) return p.columns[i];
    }
    return bare;
}

fn outputName(p: ir.Op.Project, i: usize) ?[]const u8 {
    const outputs = p.outputs orelse return null;
    return if (i < outputs.len) outputs[i] else null;
}

/// Whether a select without stars projects `name`.
fn projects(p: ir.Op.Project, name: []const u8) bool {
    for (p.columns, 0..) |col, i| {
        const output = if (p.outputs) |outs| outs[i] orelse col else col;
        if (sameColumn(output, name)) return true;
    }
    return false;
}

/// A select with each star spelled out as the columns it expands to, named
/// as they'd be; null when those can't be told here, or two outputs would
/// share a name.
fn expandStars(ctx: *CompileCtx, p: ir.Op.Project) !?ir.Op.Project {
    if (p.replace_on_collision) |flags| for (flags) |f| if (f) return null;
    const na = ctx.nodeArena();
    var budget: u32 = SCOPE_WALK_BUDGET;
    const upstream = (try exactColumns(ctx, p.upstream, 0, &budget)) orelse return null;
    var skip: usize = 0;
    while (skip < p.star_skip_trailing and skip < upstream.len) : (skip += 1) {
        if (!local.selectPipelineDerives(p.upstream, upstream[upstream.len - 1 - skip])) break;
    }
    const star_columns = upstream[0 .. upstream.len - skip];
    var columns: std.ArrayList([]const u8) = .empty;
    var outputs: std.ArrayList(?[]const u8) = .empty;
    for (p.columns, 0..) |col, i| {
        if (std.mem.eql(u8, col, "*")) {
            for (star_columns) |name| {
                try columns.append(na, name);
                try outputs.append(na, types.unqualifiedName(name));
            }
        } else if (isStar(col)) {
            const qualifier = col[0 .. col.len - 2];
            const before = columns.items.len;
            for (star_columns) |name| if (local.stripAlias(qualifier, name)) |bare| {
                try columns.append(na, name);
                try outputs.append(na, bare);
            };
            if (columns.items.len == before) return null;
        } else {
            try columns.append(na, col);
            try outputs.append(na, if (p.outputs) |outs| outs[i] else null);
        }
    }
    for (columns.items, outputs.items, 0..) |col, output, i| {
        const name = output orelse types.unqualifiedName(col);
        for (columns.items[0..i], outputs.items[0..i]) |prior_col, prior_output| {
            if (types.columnNameEql(prior_output orelse types.unqualifiedName(prior_col), name)) return null;
        }
    }
    var out = p;
    out.columns = columns.items;
    out.outputs = outputs.items;
    out.replace_on_collision = null;
    out.star_skip_trailing = 0;
    return out;
}

/// A relation's column names as its rows carry them, in order; null when
/// they can't be told here.
fn exactColumns(ctx: *CompileCtx, op: *const ir.Op, depth: u32, budget: *u32) Allocator.Error!?[]const []const u8 {
    if (depth >= SCOPE_DEPTH_LIMIT or budget.* == 0) return null;
    budget.* -= 1;
    const na = ctx.nodeArena();
    var names: std.ArrayList([]const u8) = .empty;
    switch (op.*) {
        .scan => |s| {
            const columns = (try tableColumns(ctx, s.table)) orelse return null;
            const alias = s.alias orelse return columns;
            for (columns) |c| try names.append(na, try std.fmt.allocPrint(na, "{s}.{s}", .{ alias, c }));
        },
        .table_fn => |t| return try tableFnColumns(ctx, t),
        .single_row => return &.{},
        .alias => |a| {
            const inner = (try exactColumns(ctx, a.upstream, depth + 1, budget)) orelse return null;
            for (inner) |c| try names.append(na, try std.fmt.allocPrint(na, "{s}.{s}", .{ a.alias, c }));
        },
        .materialize => |m| return try exactColumns(ctx, m.upstream, depth + 1, budget),
        .set_union => |u| return try exactColumns(ctx, u.left, depth + 1, budget),
        .limit, .order_by, .filter => return try exactColumns(ctx, blockUpstream(op).?, depth + 1, budget),
        .select => |p| {
            if (hasStar(p.columns)) {
                const expanded = (try expandStars(ctx, p)) orelse return null;
                for (expanded.columns, expanded.outputs.?) |c, o| try names.append(na, o orelse types.unqualifiedName(c));
            } else for (p.columns, 0..) |c, i| {
                const output = if (p.outputs) |outs| outs[i] else null;
                try names.append(na, output orelse types.unqualifiedName(c));
            }
        },
        .compute => |c| {
            try names.appendSlice(na, (try exactColumns(ctx, c.upstream, depth + 1, budget)) orelse return null);
            for (c.derived) |d| {
                if (listed(names.items, d.name)) return null;
                try names.append(na, d.name);
            }
        },
        .window => |w| {
            try names.appendSlice(na, (try exactColumns(ctx, w.upstream, depth + 1, budget)) orelse return null);
            for (w.calls) |call| try names.append(na, call.output_name);
        },
        .group_by => |g| {
            try names.appendSlice(na, g.group_cols);
            for (g.aggs) |a| try names.append(na, a.as);
        },
        .exclude => |p| {
            const upstream = (try exactColumns(ctx, p.upstream, depth + 1, budget)) orelse return null;
            for (p.columns) |c| if (!listed(upstream, c)) return null;
            for (upstream) |c| if (!listed(p.columns, c)) try names.append(na, c);
        },
        else => return null,
    }
    return names.items;
}

/// A WHERE conjunct comparing a column of the block's FROM with a domain
/// column, read as `from op key`.
const DomainCorrelation = struct {
    from: []const u8,
    op: exec.PredicateOp,
    key: usize,
};

fn domainCorrelation(pred: PredicateExpr, scope: Scope, keys: []const []const u8) ?DomainCorrelation {
    if (pred != .leaf_col_col) return null;
    const lc = pred.leaf_col_col;
    for (keys, 0..) |key, k| {
        if (std.mem.eql(u8, lc.right, key) and scope.bindsRelation(lc.left)) return .{ .from = lc.left, .op = lc.op, .key = k };
        if (std.mem.eql(u8, lc.left, key) and scope.bindsRelation(lc.right)) return .{ .from = lc.right, .op = flipRangeOp(lc.op), .key = k };
    }
    return null;
}

/// `columns` and each domain key it doesn't already hold.
fn withKeys(ctx: *CompileCtx, columns: []const []const u8, keys: []const []const u8) ![]const []const u8 {
    var out: std.ArrayList([]const u8) = .empty;
    try out.appendSlice(ctx.nodeArena(), columns);
    for (keys) |key| {
        for (columns) |col| {
            if (types.columnNameEql(col, key)) break;
        } else try out.append(ctx.nodeArena(), key);
    }
    return out.items;
}

/// A select that also passes the domain keys, and the rank a LIMIT above
/// it still reads.
fn selectKeys(ctx: *CompileCtx, p: ir.Op.Project, keys: []const []const u8, rank: ?[]const u8) !ir.Op.Project {
    const na = ctx.nodeArena();
    var columns: std.ArrayList([]const u8) = .empty;
    var outputs: std.ArrayList(?[]const u8) = .empty;
    var collide: std.ArrayList(bool) = .empty;
    try columns.appendSlice(na, p.columns);
    if (p.outputs) |outs| try outputs.appendSlice(na, outs) else try outputs.appendNTimes(na, null, p.columns.len);
    if (p.replace_on_collision) |r| try collide.appendSlice(na, r);
    const extra = if (rank) |r| try std.mem.concat(na, []const u8, &.{ keys, &.{r} }) else keys;
    next: for (extra) |col| {
        for (outputs.items) |o| if (o) |name| if (types.columnNameEql(name, col)) continue :next;
        try columns.append(na, col);
        try outputs.append(na, col);
        if (p.replace_on_collision != null) try collide.append(na, false);
    }
    var out = p;
    out.columns = columns.items;
    out.outputs = outputs.items;
    if (p.replace_on_collision != null) out.replace_on_collision = collide.items;
    return out;
}

/// The domain: each distinct combination of the enclosing values `outer`
/// in the operator's rows, as `alias.__d{i}`, with a twin `__j{i}` for each
/// one a join key reads (the join drops its right key columns).
fn domainRows(ctx: *CompileCtx, source: *DomainSource, alias: []const u8, outer: []const []const u8, keyed: []const bool) !*ir.Op {
    const na = ctx.nodeArena();
    const aggs = try na.alloc(ir.AggSpec, 1);
    aggs[0] = .{ .func = .count, .col = null, .as = "__dom_rows" };
    const distinct = try newOp(ctx, .{ .group_by = .{
        .group_cols = outer,
        .aggs = aggs,
        .upstream = try source.rows(ctx),
    } });
    var columns: std.ArrayList([]const u8) = .empty;
    var outputs: std.ArrayList(?[]const u8) = .empty;
    for (outer, 0..) |name, i| {
        try columns.append(na, name);
        try outputs.append(na, try std.fmt.allocPrint(na, "__d{d}", .{i}));
    }
    for (outer, keyed, 0..) |name, k, i| if (k) {
        try columns.append(na, name);
        try outputs.append(na, try std.fmt.allocPrint(na, "__j{d}", .{i}));
    };
    const select = try newOp(ctx, .{ .select = .{ .columns = columns.items, .outputs = outputs.items, .upstream = distinct } });
    return try newOp(ctx, .{ .alias = .{ .alias = alias, .upstream = select } });
}

/// The keys a lifted subquery's grouped rows join back on: `in_inner` (its
/// columns an IN compares) meeting `in_outer`, then each domain column
/// meeting the enclosing value it carries, NULL matching NULL.
fn domainKeys(ctx: *CompileCtx, lifted: Lifted, in_inner: []const []const u8, in_outer: []const []const u8) !JoinKeys {
    const na = ctx.nodeArena();
    return .{
        .inner = try std.mem.concat(na, []const u8, &.{ in_inner, lifted.keys }),
        .outer = try std.mem.concat(na, []const u8, &.{ in_outer, lifted.outer }),
        .null_safe_from = in_inner.len,
    };
}

/// An EXISTS or IN subquery lifted onto the operator's domain, its rows
/// grouped to one marker row per key (IN's compared columns, then the
/// domain columns) and LEFT JOINed back: an outer row whose key found a
/// marker has a match. False when there's no domain, or the subquery
/// doesn't lift.
fn resolveDomainBlock(ctx: *CompileCtx, pred: *PredicateExpr, top: *const ir.Op, negate: bool, in_subquery: ?exec.predicate.InSubquery, lowered: *LoweredScalars) !bool {
    const source = if (lowered.domain) |*d| d else return false;
    const na = ctx.nodeArena();
    var in_cols: []const []const u8 = &.{};
    var body = top;
    if (in_subquery) |s| {
        in_cols = try std.mem.concat(na, []const u8, &.{ &.{s.col}, s.rest_cols });
    } else {
        body = try existenceBody(ctx, top);
        if (body.* == .group_by and body.group_by.group_cols.len == 0) {
            // A global aggregate has a row whatever the outer row.
            const free = (try freeNamesOf(ctx, body)) orelse return false;
            if (free.len == 0 or !try source.carries(ctx, free)) return false;
            pred.* = .{ .always = !negate };
            return true;
        }
    }
    const width: ?usize = if (in_subquery != null) in_cols.len else null;
    const lifted = (try liftSubquery(ctx, body, source, false, width)) orelse return false;
    const compared: []const []const u8 = if (in_subquery != null) lifted.selected else &.{};
    const aggs = try na.alloc(ir.AggSpec, 1);
    aggs[0] = .{ .func = .count, .col = null, .as = "__csq_m" };
    const grouped = try newOp(ctx, .{ .group_by = .{
        .group_cols = try std.mem.concat(na, []const u8, &.{ compared, lifted.keys }),
        .aggs = aggs,
        .upstream = lifted.rows,
    } });
    const values = [_][]const u8{"__csq_m"};
    const alias = try joinKeyed(ctx, grouped, try domainKeys(ctx, lifted, compared, in_cols), &values, null, lowered);
    const marker = try std.fmt.allocPrint(na, "{s}.__csq_m", .{alias});
    try lowered.hidden.append(na, marker);
    const probe: PredicateExpr = if (negate) .{ .is_null = marker } else .{ .is_not_null = marker };
    pred.* = try nullGuarded(na, in_cols, negate, probe);
    return true;
}

/// The part of an EXISTS block whether any row exists depends on: what it
/// computes above its last filter or grouping only shapes the rows, as
/// does a LIMIT that keeps the first.
fn existenceBody(ctx: *CompileCtx, top: *const ir.Op) !*const ir.Op {
    var cur = top;
    if (cur.* == .limit) {
        const l = cur.limit;
        if (l.offset != 0 or l.n == 0) return top;
        cur = l.upstream;
    }
    const block = try splitBlock(ctx, cur);
    for (block.chain) |op| if (op.* == .filter or op.* == .group_by) return op;
    return block.from;
}

// =============================================================================
// Aggregates over enclosing columns only.
// =============================================================================

/// A subquery's aggregate whose arguments all read the enclosing query
/// aggregates there, as SQL scopes it: `(SELECT SUM(x.v) FROM y ...)` sums
/// `x.v` over the enclosing group. Each such aggregate of a subquery in a
/// select's list or HAVING moves into the enclosing grouping, and the
/// subquery reads its value as an enclosing column. A select that groups
/// nothing gets a global grouping for them over the rows its WHERE keeps,
/// as they make it an aggregate query; an aggregate in a WHERE, or inside
/// another aggregate, stays where it is. A subquery left with no aggregate
/// and no GROUP BY no longer aggregates: it yields a row for each row its
/// WHERE keeps.
fn hoistOuterAggregates(ctx: *CompileCtx, select: *ir.Op) !void {
    const na = ctx.nodeArena();
    var path: std.ArrayList(*ir.Op) = .empty;
    var cur: *ir.Op = select.select.upstream;
    while (true) switch (cur.*) {
        .order_by, .limit, .compute, .filter, .exclude => {
            try path.append(na, cur);
            cur = @constCast(blockUpstream(cur).?);
        },
        else => break,
    };
    const grouped = cur.* == .group_by;
    var eligible = path.items;
    var input: *ir.Op = undefined;
    if (grouped) {
        input = cur.group_by.upstream;
    } else {
        // The select's list: the computes above its WHERE, over rows that
        // no grouping, window or LIMIT shapes.
        var n: usize = 0;
        while (n < path.items.len and path.items[n].* != .filter) : (n += 1) {}
        eligible = path.items[0..n];
        if (n == 0 or path.items[n - 1].* != .compute) return;
        for (path.items[n..]) |o| if (o.* == .order_by or o.* == .limit) return;
        switch (cur.*) {
            .scan, .file_scan, .alias, .join, .materialize, .single_row => {},
            else => return,
        }
        input = path.items[n - 1].compute.upstream;
    }
    if (!opsReadSubquery(eligible, grouped)) return;

    var hoist: OuterAggregates = .{ .ctx = ctx, .scope = try ScopeBuilder.build(ctx, try splitBlock(ctx, input), true) };
    for (eligible) |o| switch (o.*) {
        .compute => |*c| {
            var derived: ?[]ir.Derived = null;
            for (c.derived, 0..) |d, i| if (try hoist.expr(d.expr)) |e| {
                if (derived == null) derived = try na.dupe(ir.Derived, c.derived);
                derived.?[i].expr = e;
            };
            if (derived) |d| c.derived = d;
        },
        .filter => |*f| if (grouped) {
            if (try hoist.predicate(f.predicate)) |p| f.predicate = p;
        },
        else => {},
    };
    if (hoist.aggs.items.len == 0) return;
    var rows = input;
    if (hoist.args.items.len > 0) rows = try newOp(ctx, .{ .compute = .{ .derived = hoist.args.items, .upstream = rows } });
    if (grouped) {
        cur.group_by.aggs = try std.mem.concat(na, ir.AggSpec, &.{ cur.group_by.aggs, hoist.aggs.items });
        cur.group_by.upstream = rows;
    } else {
        eligible[eligible.len - 1].compute.upstream = try newOp(ctx, .{ .group_by = .{
            .group_cols = &.{},
            .aggs = hoist.aggs.items,
            .upstream = rows,
        } });
    }
}

/// Whether a compute of `ops`, or a filter when `filters`, reads a subquery.
fn opsReadSubquery(ops: []const *ir.Op, filters: bool) bool {
    for (ops) |o| switch (o.*) {
        .compute => |c| for (c.derived) |d| if (exprReadsSubquery(d.expr)) return true,
        .filter => |f| if (filters and readsSubquery(f.predicate)) return true,
        else => {},
    };
    return false;
}

fn exprReadsSubquery(e: ir.Expr) bool {
    return switch (e) {
        .col_ref, .lit, .null_lit, .var_ref => false,
        .scalar_subquery, .exists_subquery => true,
        .call => |c| for (c.args) |arg| {
            if (exprReadsSubquery(arg)) break true;
        } else false,
        .case => |cs| {
            for (cs.operands) |o| if (exprReadsSubquery(o.expr)) return true;
            for (cs.branches) |br| if (readsSubquery(br.cond) or exprReadsSubquery(br.then)) return true;
            if (cs.else_branch) |eb| return exprReadsSubquery(eb.*);
            return false;
        },
    };
}

/// The subqueries of a select's list or HAVING, each with its aggregates
/// over enclosing names moved out.
const OuterAggregates = struct {
    ctx: *CompileCtx,
    /// What the enclosing grouping's input binds.
    scope: Scope,
    /// The aggregates the enclosing grouping takes on, and the arguments
    /// they compute over its input.
    aggs: std.ArrayList(ir.AggSpec) = .empty,
    args: std.ArrayList(ir.Derived) = .empty,

    const Rename = exec.predicate.ColRename;

    /// `e` with its subqueries rewritten; null when none changes.
    fn expr(self: *OuterAggregates, e: ir.Expr) Allocator.Error!?ir.Expr {
        const na = self.ctx.nodeArena();
        switch (e) {
            .col_ref, .lit, .null_lit, .var_ref => return null,
            .scalar_subquery => |source| return .{ .scalar_subquery = (try self.subquery(source)) orelse return null },
            .exists_subquery => |source| return .{ .exists_subquery = (try self.subquery(source)) orelse return null },
            .call => |c| {
                var args: ?[]ir.Expr = null;
                for (c.args, 0..) |arg, i| if (try self.expr(arg)) |rewritten| {
                    if (args == null) args = try na.dupe(ir.Expr, c.args);
                    args.?[i] = rewritten;
                };
                var copy = c;
                copy.args = args orelse return null;
                return .{ .call = copy };
            },
            .case => |cs| {
                var changed = false;
                var copy = cs;
                const operands = try na.dupe(exec.expr_mod.Expr.Operand, cs.operands);
                for (operands) |*o| if (try self.expr(o.expr)) |rewritten| {
                    o.expr = rewritten;
                    changed = true;
                };
                const branches = try na.dupe(exec.expr_mod.Expr.Branch, cs.branches);
                for (branches) |*br| {
                    if (try self.predicate(br.cond)) |rewritten| {
                        br.cond = rewritten;
                        changed = true;
                    }
                    if (try self.expr(br.then)) |rewritten| {
                        br.then = rewritten;
                        changed = true;
                    }
                }
                if (cs.else_branch) |eb| if (try self.expr(eb.*)) |rewritten| {
                    const else_copy = try na.create(ir.Expr);
                    else_copy.* = rewritten;
                    copy.else_branch = else_copy;
                    changed = true;
                };
                if (!changed) return null;
                copy.operands = operands;
                copy.branches = branches;
                return .{ .case = copy };
            },
        }
    }

    /// `p` with its subqueries rewritten; null when none changes.
    fn predicate(self: *OuterAggregates, p: PredicateExpr) Allocator.Error!?PredicateExpr {
        const na = self.ctx.nodeArena();
        switch (p) {
            .scalar_subquery => |s| {
                var copy = s;
                copy.source = (try self.subquery(s.source)) orelse return null;
                return .{ .scalar_subquery = copy };
            },
            .in_subquery => |s| {
                var copy = s;
                copy.source = (try self.subquery(s.source)) orelse return null;
                return .{ .in_subquery = copy };
            },
            .exists_subquery => |source| return .{ .exists_subquery = (try self.subquery(source)) orelse return null },
            .@"and", .@"or" => |children| {
                var copies: ?[]PredicateExpr = null;
                for (children, 0..) |child, i| if (try self.predicate(child)) |rewritten| {
                    if (copies == null) copies = try na.dupe(PredicateExpr, children);
                    copies.?[i] = rewritten;
                };
                const out = copies orelse return null;
                return if (p == .@"and") .{ .@"and" = out } else .{ .@"or" = out };
            },
            .not => |child| {
                const copy = try na.create(PredicateExpr);
                copy.* = (try self.predicate(child.*)) orelse return null;
                return .{ .not = copy };
            },
            else => return null,
        }
    }

    /// The subquery with its aggregates over enclosing names moved out: a
    /// copy of its operators, where each reads the enclosing column in
    /// place of the aggregate. Null when it has none.
    fn subquery(self: *OuterAggregates, source: *const anyopaque) Allocator.Error!?*ir.Op {
        const ctx = self.ctx;
        const na = ctx.nodeArena();
        const top: *const ir.Op = @ptrCast(@alignCast(source));
        const b = try splitBlock(ctx, top);
        var at: ?usize = null;
        for (b.chain, 0..) |op, i| if (op.* == .group_by) {
            at = i;
        };
        const gi = at orelse return null;
        for (b.chain[0..gi]) |op| switch (op.*) {
            .select, .compute, .filter, .order_by, .limit => {},
            else => return null,
        };
        const g = b.chain[gi].group_by;
        const below = b.chain[gi + 1 ..];
        const inner = try ScopeBuilder.build(ctx, b, true);
        const moves = try na.alloc(bool, g.aggs.len);
        var any = false;
        for (g.aggs, moves) |a, *move| {
            move.* = self.enclosing(a, below, inner);
            any = any or move.*;
        }
        if (!any) return null;

        var renames: std.ArrayList(Rename) = .empty;
        var kept: std.ArrayList(ir.AggSpec) = .empty;
        var moved: std.ArrayList(Rename) = .empty;
        for (g.aggs, moves) |a, move| {
            if (!move) {
                try kept.append(na, a);
                continue;
            }
            var spec = a;
            spec.as = try self.fresh("__oagg");
            if (a.col) |col| spec.col = try self.movedArg(col, below, &moved);
            if (a.arg2_col) |col| spec.arg2_col = try self.movedArg(col, below, &moved);
            const udf_args = try na.dupe([]const u8, a.udf_arg_cols);
            for (udf_args) |*col| col.* = try self.movedArg(col.*, below, &moved);
            spec.udf_arg_cols = udf_args;
            try self.aggs.append(na, spec);
            try renames.append(na, .{ .from = a.as, .to = spec.as });
        }

        var cur: *ir.Op = @constCast(b.from);
        var i = b.chain.len;
        while (i > 0) {
            i -= 1;
            var copy = b.chain[i].*;
            switch (copy) {
                .group_by => |*grouping| if (i == gi) {
                    if (kept.items.len == 0 and grouping.group_cols.len == 0) continue;
                    if (kept.items.len == 0) try kept.append(na, .{ .func = .count, .col = null, .as = try self.fresh("__oagg") });
                    grouping.aggs = kept.items;
                },
                .compute => |*c| {
                    var derived: std.ArrayList(ir.Derived) = .empty;
                    for (c.derived) |d| {
                        if (i > gi and renamedBy(moved.items, d.name) and !aggsRead(kept.items, d.name)) continue;
                        try derived.append(na, .{ .name = d.name, .expr = try exec.expr_mod.deepCloneRenamed(na, d.expr, renames.items) });
                    }
                    if (derived.items.len == 0) continue;
                    c.derived = derived.items;
                },
                .filter => |*f| f.predicate = try exec.predicate.deepClonePredicateRenamed(na, f.predicate, renames.items),
                .select => |*p| {
                    const columns = try na.dupe([]const u8, p.columns);
                    const outputs = try na.alloc(?[]const u8, p.columns.len);
                    for (columns, outputs, 0..) |*col, *output, j| {
                        output.* = if (p.outputs) |outs| outs[j] else null;
                        if (!renamedBy(renames.items, col.*)) continue;
                        // A column read in place of an aggregate keeps its
                        // name.
                        output.* = output.* orelse col.*;
                        col.* = exec.predicate.renameOf(renames.items, col.*);
                    }
                    p.columns = columns;
                    p.outputs = outputs;
                },
                .order_by => |*o| {
                    const specs = try na.dupe(ir.SortSpec, o.specs);
                    for (specs) |*s| s.col = exec.predicate.renameOf(renames.items, s.col);
                    o.specs = specs;
                },
                else => {},
            }
            const node = try newOp(ctx, copy);
            relink(node, cur);
            cur = node;
        }
        if (gi > 0) return cur;
        // The grouping was the block's top: a select keeps its columns.
        const columns = try na.alloc([]const u8, g.group_cols.len + g.aggs.len);
        const outputs = try na.alloc(?[]const u8, columns.len);
        for (g.group_cols, columns[0..g.group_cols.len], outputs[0..g.group_cols.len]) |col, *dst, *output| {
            dst.* = col;
            output.* = col;
        }
        for (g.aggs, columns[g.group_cols.len..], outputs[g.group_cols.len..]) |a, *dst, *output| {
            dst.* = exec.predicate.renameOf(renames.items, a.as);
            output.* = a.as;
        }
        return try newOp(ctx, .{ .select = .{ .columns = columns, .outputs = outputs, .upstream = cur } });
    }

    /// Whether every argument of `a` reads the enclosing query: a name the
    /// subquery doesn't bind and the enclosing grouping's input does, or a
    /// column computed below the subquery's grouping from such names only.
    fn enclosing(self: *const OuterAggregates, a: ir.AggSpec, below: []const *const ir.Op, inner: Scope) bool {
        var args: usize = 0;
        if (a.col) |col| {
            if (!self.enclosingArg(col, below, inner)) return false;
            args += 1;
        }
        if (a.arg2_col) |col| {
            if (!self.enclosingArg(col, below, inner)) return false;
            args += 1;
        }
        for (a.udf_arg_cols) |col| {
            if (!self.enclosingArg(col, below, inner)) return false;
            args += 1;
        }
        return args > 0;
    }

    fn enclosingArg(self: *const OuterAggregates, col: []const u8, below: []const *const ir.Op, inner: Scope) bool {
        if (!inner.binds(col)) return self.scope.binds(col);
        const d = computedBelow(below, col) orelse return false;
        var refs: usize = 0;
        return self.enclosingExpr(d.expr, inner, &refs) and refs > 0;
    }

    fn enclosingExpr(self: *const OuterAggregates, e: ir.Expr, inner: Scope, refs: *usize) bool {
        switch (e) {
            .col_ref => |ref| {
                refs.* += 1;
                return !inner.binds(ref) and self.scope.binds(ref);
            },
            .lit, .null_lit => return true,
            .call => |c| {
                if (volatileFn(self.ctx, c.fn_name)) return false;
                for (c.args) |arg| if (!self.enclosingExpr(arg, inner, refs)) return false;
                return true;
            },
            else => return false,
        }
    }

    /// The argument as the enclosing grouping reads it: a column computed
    /// below the subquery's grouping is computed over the enclosing input
    /// instead.
    fn movedArg(self: *OuterAggregates, col: []const u8, below: []const *const ir.Op, moved: *std.ArrayList(Rename)) ![]const u8 {
        const na = self.ctx.nodeArena();
        const d = computedBelow(below, col) orelse return col;
        for (moved.items) |m| if (types.columnNameEql(m.from, d.name)) return m.to;
        const name = try self.fresh("__oarg");
        try self.args.append(na, .{ .name = name, .expr = d.expr });
        try moved.append(na, .{ .from = d.name, .to = name });
        return name;
    }

    fn fresh(self: *OuterAggregates, prefix: []const u8) ![]const u8 {
        const name = try std.fmt.allocPrint(self.ctx.nodeArena(), "{s}{d}", .{ prefix, self.ctx.lowered_scalars });
        self.ctx.lowered_scalars += 1;
        return name;
    }
};

fn computedBelow(below: []const *const ir.Op, name: []const u8) ?ir.Derived {
    for (below) |op| if (op.* == .compute) {
        for (op.compute.derived) |d| if (types.columnNameEql(d.name, name)) return d;
    };
    return null;
}

fn renamedBy(renames: []const exec.predicate.ColRename, name: []const u8) bool {
    for (renames) |r| if (types.columnNameEql(r.from, name)) return true;
    return false;
}

fn aggsRead(aggs: []const ir.AggSpec, name: []const u8) bool {
    for (aggs) |a| {
        if (a.col) |col| if (types.columnNameEql(col, name)) return true;
        if (a.arg2_col) |col| if (types.columnNameEql(col, name)) return true;
        for (a.udf_arg_cols) |col| if (types.columnNameEql(col, name)) return true;
    }
    return false;
}

/// Resolve the predicate of a statement or a join, whose operator computes
/// `derived` before reading it: correlation operands computed from the
/// outer row alone join them.
fn resolveWithOuterValues(ctx: *CompileCtx, pred: *PredicateExpr, derived: []const ir.Derived) ![]const ir.Derived {
    var sink: LoweredScalars = .{};
    try resolveSubqueriesInPredicate(ctx, pred, &sink);
    if (sink.outer_values.items.len == 0) return derived;
    return try std.mem.concat(ctx.nodeArena(), ir.Derived, &.{ derived, sink.outer_values.items });
}
