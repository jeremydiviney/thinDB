//! Predicate type system + leaf evaluation.
//!
//! `PredicateExpr` is the boolean expression tree consumed by `Filter`.
//! `evaluateMaskWithPred` is the per-leaf row-mask kernel. `validateExpr`
//! type-checks an expression against a schema before evaluation.
//! `statsOverlapPredicate` is the row-group prune helper used by `Scan` and
//! by `Table.delete`.

const std = @import("std");

const types = @import("../types.zig");
const Column = types.Column;
const Value = types.Value;
const ValueTag = types.ValueTag;

const storage = @import("../storage/storage.zig");
const ColumnView = storage.ColumnView;

const exec = @import("exec.zig");
const Error = exec.Error;

pub const PredicateOp = enum { eq, neq, lt, lte, gt, gte };

pub const Predicate = struct {
    col: []const u8,
    op: PredicateOp,
    val: Value,
};

/// Boolean expression over Predicates.
///
///   - `.leaf`       — a single column-op-value comparison
///   - `.is_null`    — column value is NULL
///   - `.is_not_null`— column value is non-NULL
///   - `.@"and"`     — all children must match
///   - `.@"or"`      — at least one child must match
///   - `.not`        — child must NOT match
pub const PredicateExpr = union(enum) {
    leaf: Predicate,
    /// `col1 op col2` — both sides are column refs. Required for
    /// TPC-H queries (Q12's `l_commitdate < l_receiptdate`) and for
    /// detecting correlated subqueries (`l_orderkey = o_orderkey`
    /// inside an EXISTS inner). NULL on either side → row fails the
    /// predicate (two-valued logic).
    leaf_col_col: ColColPred,
    is_null: []const u8,
    is_not_null: []const u8,
    /// SQL LIKE pattern match. `pattern` is a SQL pattern with two
    /// wildcards: `%` (zero-or-more chars) and `_` (exactly one char).
    /// Other bytes are literal. NOT LIKE lowers to `.not` wrapping a
    /// `.like` — no separate variant.
    like: LikePred,
    @"and": []const PredicateExpr,
    @"or": []const PredicateExpr,
    not: *const PredicateExpr,
    /// `col cmp_op (SELECT single_value_from_anywhere)`. Resolved at
    /// compile time by running the inner once, freezing the single
    /// value, and rewriting this node into a `.leaf`. The `source`
    /// pointer is `*const ir.Op` opaqued to dodge the cycle with the
    /// IR module. Operators never see this variant.
    scalar_subquery: ScalarSubquery,
    /// `EXISTS (SELECT ...)` — pre-compile pass runs the inner once,
    /// checks row_count > 0, replaces with `.always`. NOT EXISTS is
    /// produced by wrapping in `.not` at parse time. Source is an
    /// opaque `*const ir.Op` (same cycle dodge as scalar_subquery).
    exists_subquery: *const anyopaque,
    /// Constant per-row predicate — TRUE matches every row, FALSE
    /// none. Used as the resolved form of EXISTS / NOT EXISTS and
    /// (later) NOT IN against an empty subquery result.
    always: bool,
    /// `col [NOT] IN (SELECT ...)` — pre-compile pass drains the inner,
    /// materializes its single column into a Value slice, rewrites
    /// this node to `.in_set`. NULL handling per thinDB dialect: see
    /// [[thindb-not-in-nonstandard]] — NULLs are dropped from the set
    /// in both IN and NOT IN.
    in_subquery: InSubquery,
    /// Materialized set-membership filter — `.in_set.values` is the
    /// inner subquery's column reified into Values; evaluator does
    /// linear scan. v1 set sizes are small (typical < 1k) — hash-set
    /// optimization is a follow-up.
    in_set: InSet,
    /// Resolved form of a correlated subquery (EXISTS / NOT EXISTS /
    /// IN / NOT IN). The pre-compile pass dropped the correlation
    /// predicates from the inner, drained the rewritten inner, and
    /// stored each result row's correlation-key tuple here (plus the
    /// outer's IN-column value for IN-form, prefixed).
    ///
    /// Per outer row: assemble a tuple from `outer_cols`, linear-scan
    /// `rows` for a match, apply `negate`. NULL in any outer col →
    /// the tuple can't match (consistent with the NOT IN dialect:
    /// see [[thindb-not-in-nonstandard]]).
    correlated_set: CorrelatedSet,
    /// Resolved form of a correlated scalar subquery
    /// (`outer.x op (SELECT agg(y) FROM B WHERE B.k = outer.k ...)`).
    /// The pre-compile pass added the correlation keys to the inner's
    /// GROUP BY and dropped the correlation predicates. Each result
    /// row is `(key_tuple, agg_value)`. Per outer row: look up by
    /// `outer_keys`, then compare `outer_compared op agg_value`. No
    /// matching key → predicate fails (the standard SQL semantics
    /// for a missing scalar-subquery result).
    correlated_scalar: CorrelatedScalar,
};

pub const LikePred = struct {
    col: []const u8,
    pattern: []const u8,
};

pub const ScalarSubquery = struct {
    col: []const u8,
    op: PredicateOp,
    source: *const anyopaque,
};

pub const InSubquery = struct {
    col: []const u8,
    source: *const anyopaque,
    negate: bool,
};

pub const InSet = struct {
    col: []const u8,
    values: []const Value,
    negate: bool,
};

pub const ColColPred = struct {
    left: []const u8,
    op: PredicateOp,
    right: []const u8,
};

pub const CorrelatedScalarRow = struct {
    key: []const Value,
    value: Value,
};

pub const CorrelatedScalar = struct {
    /// The outer column being compared against the subquery result.
    outer_compared: []const u8,
    /// The comparison operator from the outer predicate.
    op: PredicateOp,
    /// Outer correlation keys, parallel to each row's `key` tuple.
    outer_keys: []const []const u8,
    /// Materialized rows. `key` tuples are unique (the inner's
    /// GROUP BY on the correlation columns guarantees that).
    rows: []const CorrelatedScalarRow,
};

pub const CorrelatedSet = struct {
    /// The outer-side column names whose values, taken together,
    /// form the lookup tuple per row. For EXISTS, these are the
    /// outer correlation keys. For IN, the first element is the
    /// outer's IN-column followed by the outer correlation keys.
    outer_cols: []const []const u8,
    /// Materialized rows. Each inner slice has length equal to
    /// `outer_cols.len`; entries are the inner subquery's drained
    /// values in the parallel order.
    rows: []const []const Value,
    /// `true` = NOT IN / NOT EXISTS; outer row passes iff its
    /// tuple does NOT appear in `rows`.
    negate: bool,
};

pub fn likeExpr(col: []const u8, pattern: []const u8) PredicateExpr {
    return .{ .like = .{ .col = col, .pattern = pattern } };
}

/// Build a leaf predicate expression. Shorthand for `.{ .leaf = ... }`.
pub fn leafExpr(col: []const u8, op: PredicateOp, val: Value) PredicateExpr {
    return .{ .leaf = .{ .col = col, .op = op, .val = val } };
}

pub fn isNullExpr(col: []const u8) PredicateExpr {
    return .{ .is_null = col };
}

pub fn isNotNullExpr(col: []const u8) PredicateExpr {
    return .{ .is_not_null = col };
}

/// Deep-clone a PredicateExpr into `out_arena`. Mirrors
/// `expr.deepClone` for the boolean side of a CASE/WHERE expression;
/// used when an Expr.case needs to outlive its source arena.
pub fn deepClonePredicate(out_arena: std.mem.Allocator, p: PredicateExpr) std.mem.Allocator.Error!PredicateExpr {
    return switch (p) {
        .leaf => |lf| .{ .leaf = .{
            .col = try out_arena.dupe(u8, lf.col),
            .op = lf.op,
            .val = try cloneValue(out_arena, lf.val),
        } },
        .leaf_col_col => |lc| .{ .leaf_col_col = .{
            .left = try out_arena.dupe(u8, lc.left),
            .op = lc.op,
            .right = try out_arena.dupe(u8, lc.right),
        } },
        .is_null => |c| .{ .is_null = try out_arena.dupe(u8, c) },
        .is_not_null => |c| .{ .is_not_null = try out_arena.dupe(u8, c) },
        .like => |lp| .{ .like = .{
            .col = try out_arena.dupe(u8, lp.col),
            .pattern = try out_arena.dupe(u8, lp.pattern),
        } },
        .scalar_subquery => |sq| .{ .scalar_subquery = .{
            .col = try out_arena.dupe(u8, sq.col),
            .op = sq.op,
            .source = sq.source,
        } },
        .exists_subquery => |src| .{ .exists_subquery = src },
        .always => |b| .{ .always = b },
        .in_subquery => |s| .{ .in_subquery = .{
            .col = try out_arena.dupe(u8, s.col),
            .source = s.source,
            .negate = s.negate,
        } },
        .in_set => |s| blk: {
            const vals = try out_arena.alloc(Value, s.values.len);
            for (s.values, vals) |v, *out| out.* = try cloneValue(out_arena, v);
            break :blk .{ .in_set = .{
                .col = try out_arena.dupe(u8, s.col),
                .values = vals,
                .negate = s.negate,
            } };
        },
        .correlated_set => |s| blk: {
            const outer_cols = try out_arena.alloc([]const u8, s.outer_cols.len);
            for (s.outer_cols, outer_cols) |src, *dst| dst.* = try out_arena.dupe(u8, src);
            const rows = try out_arena.alloc([]const Value, s.rows.len);
            for (s.rows, rows) |src, *dst| {
                const tuple = try out_arena.alloc(Value, src.len);
                for (src, tuple) |v, *t| t.* = try cloneValue(out_arena, v);
                dst.* = tuple;
            }
            break :blk .{ .correlated_set = .{
                .outer_cols = outer_cols,
                .rows = rows,
                .negate = s.negate,
            } };
        },
        .correlated_scalar => |s| blk: {
            const outer_keys = try out_arena.alloc([]const u8, s.outer_keys.len);
            for (s.outer_keys, outer_keys) |src, *dst| dst.* = try out_arena.dupe(u8, src);
            const rows = try out_arena.alloc(CorrelatedScalarRow, s.rows.len);
            for (s.rows, rows) |src, *dst| {
                const key = try out_arena.alloc(Value, src.key.len);
                for (src.key, key) |v, *k| k.* = try cloneValue(out_arena, v);
                dst.* = .{ .key = key, .value = try cloneValue(out_arena, src.value) };
            }
            break :blk .{ .correlated_scalar = .{
                .outer_compared = try out_arena.dupe(u8, s.outer_compared),
                .op = s.op,
                .outer_keys = outer_keys,
                .rows = rows,
            } };
        },
        .@"and" => |kids| blk: {
            const dup = try out_arena.alloc(PredicateExpr, kids.len);
            for (kids, 0..) |k, i| dup[i] = try deepClonePredicate(out_arena, k);
            break :blk .{ .@"and" = dup };
        },
        .@"or" => |kids| blk: {
            const dup = try out_arena.alloc(PredicateExpr, kids.len);
            for (kids, 0..) |k, i| dup[i] = try deepClonePredicate(out_arena, k);
            break :blk .{ .@"or" = dup };
        },
        .not => |child| blk: {
            const dup = try out_arena.create(PredicateExpr);
            dup.* = try deepClonePredicate(out_arena, child.*);
            break :blk .{ .not = dup };
        },
    };
}

fn cloneValue(out_arena: std.mem.Allocator, v: Value) std.mem.Allocator.Error!Value {
    return switch (v) {
        .text => |s| .{ .text = try out_arena.dupe(u8, s) },
        else => v,
    };
}

/// Type-check a PredicateExpr against a schema. Every leaf must reference an
/// existing column with a value-tag matching that column's type. String
/// columns only accept `.eq` and `.neq`.
///
/// Performs lossless integer-literal widening when the column type is
/// wider than the literal (e.g. column BIGINT, literal `.int` → mutate to
/// `.bigint`). Pure narrowing isn't done — we don't want silent data
/// loss in the predicate semantics.
pub fn validateExpr(expr: *PredicateExpr, schema: []const Column) !void {
    switch (expr.*) {
        .leaf => |*p| {
            const col_idx = types.findColumn(schema, p.col) orelse return Error.ColumnNotFound;
            const col_type = schema[col_idx].type;
            const col_tag = ValueTag.fromType(col_type);
            const val_tag = std.meta.activeTag(p.val);
            if (col_tag != val_tag) {
                tryWidenLiteral(&p.val, col_tag) catch return Error.PredicateTypeMismatch;
            }
            if (col_type.isString() and p.op != .eq and p.op != .neq) {
                return Error.UnsupportedOperatorForType;
            }
        },
        .leaf_col_col => |lc| {
            const li = types.findColumn(schema, lc.left) orelse return Error.ColumnNotFound;
            const ri = types.findColumn(schema, lc.right) orelse return Error.ColumnNotFound;
            const lt = schema[li].type;
            const rt = schema[ri].type;
            // String columns only support eq / neq (same as col-vs-literal).
            if (lt.isString() != rt.isString()) return Error.PredicateTypeMismatch;
            if (lt.isString() and lc.op != .eq and lc.op != .neq) return Error.UnsupportedOperatorForType;
            // Numeric / temporal: both sides must share the same tag.
            // No widening for col-vs-col in v1.
            if (std.meta.activeTag(lt) != std.meta.activeTag(rt)) return Error.PredicateTypeMismatch;
        },
        .is_null, .is_not_null => |col_name| {
            _ = types.findColumn(schema, col_name) orelse return Error.ColumnNotFound;
        },
        .like => |lp| {
            const idx = types.findColumn(schema, lp.col) orelse return Error.ColumnNotFound;
            if (!schema[idx].type.isString()) return Error.UnsupportedOperatorForType;
        },
        .@"and" => |children| {
            for (children) |*c| try validateExpr(@constCast(c), schema);
        },
        .@"or" => |children| {
            for (children) |*c| try validateExpr(@constCast(c), schema);
        },
        .not => |child| try validateExpr(@constCast(child), schema),
        // Scalar subqueries must be resolved (rewritten to `.leaf`) by
        // the pre-compile pass before validation runs. Reaching this
        // branch means the resolver missed a node — surface loudly.
        .scalar_subquery, .exists_subquery, .in_subquery => return Error.PredicateTypeMismatch,
        // `.always` is a constant-bool resolved form; nothing to
        // validate against schema.
        .always => {},
        // `.in_set` — column must exist and types must agree. The
        // pre-compile pass already type-checked at resolution; this
        // is a safety net.
        .in_set => |s| {
            const col_idx = types.findColumn(schema, s.col) orelse return Error.ColumnNotFound;
            const col_tag = ValueTag.fromType(schema[col_idx].type);
            for (s.values) |v| {
                if (std.meta.activeTag(v) != col_tag) return Error.PredicateTypeMismatch;
            }
        },
        // `.correlated_set` — every outer_col must exist; per-row
        // tuples must match each col's value tag. Same safety net.
        .correlated_set => |s| {
            for (s.outer_cols) |c_name| {
                _ = findCol(schema, c_name) orelse return Error.ColumnNotFound;
            }
            for (s.rows) |row| {
                if (row.len != s.outer_cols.len) return Error.PredicateTypeMismatch;
                for (row, s.outer_cols) |v, c_name| {
                    const col_idx = findCol(schema, c_name).?;
                    const expected = ValueTag.fromType(schema[col_idx].type);
                    if (std.meta.activeTag(v) != expected) return Error.PredicateTypeMismatch;
                }
            }
        },
        // `.correlated_scalar` — outer_compared + outer_keys all
        // exist; per-row keys + value type tags match.
        .correlated_scalar => |s| {
            _ = findCol(schema, s.outer_compared) orelse return Error.ColumnNotFound;
            for (s.outer_keys) |c_name| {
                _ = findCol(schema, c_name) orelse return Error.ColumnNotFound;
            }
        },
    }
}

fn findCol(schema: []const Column, name: []const u8) ?usize {
    return types.findColumn(schema, name);
}

/// Lossless widening for an integer / float literal to match a wider
/// column type. Errors when the source literal can't be losslessly
/// represented in the target type (caller treats that as a type
/// mismatch).
fn tryWidenLiteral(val: *Value, target: ValueTag) error{NoWidening}!void {
    switch (val.*) {
        .tinyint => |v| switch (target) {
            .smallint => val.* = .{ .smallint = v },
            .int => val.* = .{ .int = v },
            .bigint => val.* = .{ .bigint = v },
            .largeint => val.* = .{ .largeint = v },
            else => return error.NoWidening,
        },
        .smallint => |v| switch (target) {
            .int => val.* = .{ .int = v },
            .bigint => val.* = .{ .bigint = v },
            .largeint => val.* = .{ .largeint = v },
            else => return error.NoWidening,
        },
        .int => |v| switch (target) {
            .bigint => val.* = .{ .bigint = v },
            .largeint => val.* = .{ .largeint = v },
            else => return error.NoWidening,
        },
        .bigint => |v| switch (target) {
            .largeint => val.* = .{ .largeint = v },
            else => return error.NoWidening,
        },
        .float => |v| switch (target) {
            .double => val.* = .{ .double = v },
            else => return error.NoWidening,
        },
        else => return error.NoWidening,
    }
}

/// Push every leaf reachable through top-level ANDs down to the upstream so
/// Scan can use them for row-group min/max pruning. OR/NOT branches are
/// skipped — they don't have monotonic stats overlap semantics.
pub fn pushExprDown(upstream: *exec.Query, expr: PredicateExpr) !void {
    switch (expr) {
        .leaf => |p| {
            upstream.addPrune(p) catch |err| switch (err) {
                error.ColumnNotFound => {},
                else => return err,
            };
        },
        .@"and" => |children| {
            for (children) |c| try pushExprDown(upstream, c);
        },
        else => {},
    }
}

/// SQL LIKE matcher. `pattern` uses `%` (zero-or-more) and `_` (one).
/// Recursive backtracking matcher — acceptable for v1's modest pattern
/// lengths. No escape syntax in v1 (`\%` / `\_` not supported).
pub fn likeMatch(text: []const u8, pattern: []const u8) bool {
    var ti: usize = 0;
    var pi: usize = 0;
    var star_ti: ?usize = null;
    var star_pi: usize = 0;
    while (ti < text.len) {
        if (pi < pattern.len and pattern[pi] == '%') {
            star_pi = pi;
            star_ti = ti;
            pi += 1;
        } else if (pi < pattern.len and (pattern[pi] == '_' or pattern[pi] == text[ti])) {
            pi += 1;
            ti += 1;
        } else if (star_ti) |sti| {
            // Backtrack to last %, consume one more char from text.
            pi = star_pi + 1;
            ti = sti + 1;
            star_ti = sti + 1;
        } else return false;
    }
    while (pi < pattern.len and pattern[pi] == '%') pi += 1;
    return pi == pattern.len;
}

/// Evaluate a full boolean predicate over a Batch (typed columns +
/// nulls + AND/OR/NOT). Writes per-row match bits into `out`. The
/// caller supplies an allocator for AND/OR scratch (one per recursive
/// level). NULL never matches a comparison; `IS NULL` / `IS NOT NULL`
/// inspect the validity bitmap.
pub fn evaluatePredicate(
    allocator: std.mem.Allocator,
    expr: PredicateExpr,
    schema: []const Column,
    batch: anytype,
    out: []bool,
) anyerror!void {
    switch (expr) {
        .leaf => |p| {
            const col_idx = findCol(schema, p.col) orelse return Error.ColumnNotFound;
            try evaluateMaskWithPred(batch.values[col_idx], p, batch.row_count, out);
            // Two-valued logic: NULL never matches a comparison.
            const view = batch.values[col_idx];
            if (view.nulls != null) {
                for (0..batch.row_count) |i| {
                    if (!view.isValid(i)) out[i] = false;
                }
            }
        },
        .leaf_col_col => |lc| {
            const li = findCol(schema, lc.left) orelse return Error.ColumnNotFound;
            const ri = findCol(schema, lc.right) orelse return Error.ColumnNotFound;
            try evaluateColColMask(batch.values[li], batch.values[ri], lc.op, batch.row_count, out);
        },
        .is_null => |col_name| {
            const col_idx = findCol(schema, col_name) orelse return Error.ColumnNotFound;
            const view = batch.values[col_idx];
            for (0..batch.row_count) |i| out[i] = !view.isValid(i);
        },
        .is_not_null => |col_name| {
            const col_idx = findCol(schema, col_name) orelse return Error.ColumnNotFound;
            const view = batch.values[col_idx];
            for (0..batch.row_count) |i| out[i] = view.isValid(i);
        },
        .like => |lp| {
            const col_idx = findCol(schema, lp.col) orelse return Error.ColumnNotFound;
            const view = batch.values[col_idx];
            try evaluateLikeMask(view, lp.pattern, batch.row_count, out);
        },
        .@"and" => |children| {
            if (children.len == 0) {
                @memset(out, true);
                return;
            }
            try evaluatePredicate(allocator, children[0], schema, batch, out);
            if (children.len == 1) return;
            const scratch = try allocator.alloc(bool, out.len);
            defer allocator.free(scratch);
            for (children[1..]) |child| {
                try evaluatePredicate(allocator, child, schema, batch, scratch);
                for (out, scratch) |*o, s| o.* = o.* and s;
            }
        },
        .@"or" => |children| {
            if (children.len == 0) {
                @memset(out, false);
                return;
            }
            try evaluatePredicate(allocator, children[0], schema, batch, out);
            if (children.len == 1) return;
            const scratch = try allocator.alloc(bool, out.len);
            defer allocator.free(scratch);
            for (children[1..]) |child| {
                try evaluatePredicate(allocator, child, schema, batch, scratch);
                for (out, scratch) |*o, s| o.* = o.* or s;
            }
        },
        .not => |child| {
            try evaluatePredicate(allocator, child.*, schema, batch, out);
            for (out) |*o| o.* = !o.*;
        },
        // Resolved by the pre-compile pass.
        .scalar_subquery, .exists_subquery, .in_subquery => return Error.PredicateTypeMismatch,
        .always => |b| @memset(out, b),
        .in_set => |s| {
            const col_idx = findCol(schema, s.col) orelse return Error.ColumnNotFound;
            try evaluateInSetMask(batch.values[col_idx], s.values, s.negate, batch.row_count, out);
        },
        .correlated_set => |s| try evaluateCorrelatedSetMask(s, schema, batch, out),
        .correlated_scalar => |s| try evaluateCorrelatedScalarMask(s, schema, batch, out),
    }
}

/// Per-row: build key from outer_keys, look up matching CorrelatedScalarRow,
/// then compare outer_compared op row.value. Missing key → row fails.
pub fn evaluateCorrelatedScalarMask(s: CorrelatedScalar, schema: []const Column, batch: anytype, out: []bool) !void {
    const n_keys = s.outer_keys.len;
    var key_idx_buf: [16]usize = undefined;
    if (n_keys > key_idx_buf.len) return Error.PredicateTypeMismatch;
    const key_idxs = key_idx_buf[0..n_keys];
    for (s.outer_keys, key_idxs) |c_name, *idx_out| {
        idx_out.* = findCol(schema, c_name) orelse return Error.ColumnNotFound;
    }
    const cmp_idx = findCol(schema, s.outer_compared) orelse return Error.ColumnNotFound;
    const cmp_view = batch.values[cmp_idx];

    var i: usize = 0;
    while (i < batch.row_count) : (i += 1) {
        // NULL on outer comparison column or any outer key → fails.
        if (!cmp_view.isValid(i)) {
            out[i] = false;
            continue;
        }
        var any_null = false;
        for (key_idxs) |idx| {
            if (!batch.values[idx].isValid(i)) {
                any_null = true;
                break;
            }
        }
        if (any_null) {
            out[i] = false;
            continue;
        }

        // Linear-scan rows for matching key.
        var found_value: ?Value = null;
        for (s.rows) |row| {
            var all_match = true;
            for (key_idxs, row.key) |idx, ref_val| {
                if (!cellMatchesValue(batch.values[idx], i, ref_val)) {
                    all_match = false;
                    break;
                }
            }
            if (all_match) {
                found_value = row.value;
                break;
            }
        }
        if (found_value) |v| {
            out[i] = try compareCellToValue(cmp_view, i, s.op, v);
        } else {
            out[i] = false;
        }
    }
}

fn compareCellToValue(view: ColumnView, idx: usize, op: PredicateOp, ref: Value) !bool {
    return switch (view.data) {
        .int => |s| if (ref == .int) cmp(i32, s[idx], ref.int, op) else false,
        .bigint => |s| if (ref == .bigint) cmp(i64, s[idx], ref.bigint, op) else false,
        .smallint => |s| if (ref == .smallint) cmp(i16, s[idx], ref.smallint, op) else false,
        .tinyint => |s| if (ref == .tinyint) cmp(i8, s[idx], ref.tinyint, op) else false,
        .largeint => |s| if (ref == .largeint) cmp(i128, s[idx], ref.largeint, op) else false,
        .float => |s| if (ref == .float) cmp(f32, s[idx], ref.float, op) else false,
        .double => |s| if (ref == .double) cmp(f64, s[idx], ref.double, op) else false,
        .boolean => |s| if (ref == .boolean) cmp(u8, s[idx], @intFromBool(ref.boolean), op) else false,
        .date => |s| if (ref == .date) cmp(i32, s[idx], ref.date, op) else false,
        .datetime => |s| if (ref == .datetime) cmp(i64, s[idx], ref.datetime, op) else false,
        .decimal64 => |s| if (ref == .decimal64) cmp(i64, s[idx], ref.decimal64, op) else false,
        .decimal128 => |s| if (ref == .decimal128) cmp(i128, s[idx], ref.decimal128, op) else false,
        .uuid => |s| if (ref == .uuid) cmp(u128, s[idx], ref.uuid, op) else false,
        // Strings: only eq/neq supported.
        .varchar => |sv| if (ref == .text) blk: {
            const eq = std.mem.eql(u8, sv.rowBytes(idx), ref.text);
            break :blk if (op == .eq) eq else if (op == .neq) !eq else false;
        } else false,
        .string => |sv| if (ref == .text) blk: {
            const eq = std.mem.eql(u8, sv.rowBytes(idx), ref.text);
            break :blk if (op == .eq) eq else if (op == .neq) !eq else false;
        } else false,
        .char => |sv| if (ref == .text) blk: {
            const eq = std.mem.eql(u8, sv.rowBytes(idx), ref.text);
            break :blk if (op == .eq) eq else if (op == .neq) !eq else false;
        } else false,
    };
}

/// Per-row tuple lookup against a materialized correlated set.
/// Assembles each row's outer-side tuple, linear-scans `rows` for a
/// match. NULL in any outer col → the tuple can't match.
pub fn evaluateCorrelatedSetMask(s: CorrelatedSet, schema: []const Column, batch: anytype, out: []bool) !void {
    const n_cols = s.outer_cols.len;
    if (n_cols == 0) return Error.PredicateTypeMismatch;

    var col_idx_buf: [16]usize = undefined;
    if (n_cols > col_idx_buf.len) return Error.PredicateTypeMismatch;
    const col_idxs = col_idx_buf[0..n_cols];
    for (s.outer_cols, col_idxs) |c_name, *idx_out| {
        idx_out.* = findCol(schema, c_name) orelse return Error.ColumnNotFound;
    }

    var i: usize = 0;
    while (i < batch.row_count) : (i += 1) {
        // NULL in any outer col → no match possible.
        var any_null = false;
        for (col_idxs) |idx| {
            if (!batch.values[idx].isValid(i)) {
                any_null = true;
                break;
            }
        }
        if (any_null) {
            out[i] = s.negate; // NULL → can't match; NOT IN passes, IN fails.
            continue;
        }
        // Scan rows for a tuple match.
        var found = false;
        for (s.rows) |row| {
            var all_match = true;
            for (col_idxs, row) |idx, ref_val| {
                if (!cellMatchesValue(batch.values[idx], i, ref_val)) {
                    all_match = false;
                    break;
                }
            }
            if (all_match) {
                found = true;
                break;
            }
        }
        out[i] = if (s.negate) !found else found;
    }
}

/// Equality check between a single cell of a ColumnView and a Value
/// of the same type. Returns false on type mismatch (defensive).
fn cellMatchesValue(view: ColumnView, idx: usize, ref: Value) bool {
    return switch (view.data) {
        .int => |s| ref == .int and s[idx] == ref.int,
        .bigint => |s| ref == .bigint and s[idx] == ref.bigint,
        .smallint => |s| ref == .smallint and s[idx] == ref.smallint,
        .tinyint => |s| ref == .tinyint and s[idx] == ref.tinyint,
        .largeint => |s| ref == .largeint and s[idx] == ref.largeint,
        .float => |s| ref == .float and s[idx] == ref.float,
        .double => |s| ref == .double and s[idx] == ref.double,
        .boolean => |s| ref == .boolean and (s[idx] != 0) == ref.boolean,
        .date => |s| ref == .date and s[idx] == ref.date,
        .datetime => |s| ref == .datetime and s[idx] == ref.datetime,
        .decimal64 => |s| ref == .decimal64 and s[idx] == ref.decimal64,
        .decimal128 => |s| ref == .decimal128 and s[idx] == ref.decimal128,
        .uuid => |s| ref == .uuid and s[idx] == ref.uuid,
        .varchar => |sv| ref == .text and std.mem.eql(u8, sv.rowBytes(idx), ref.text),
        .string => |sv| ref == .text and std.mem.eql(u8, sv.rowBytes(idx), ref.text),
        .char => |sv| ref == .text and std.mem.eql(u8, sv.rowBytes(idx), ref.text),
    };
}

/// Per-row set-membership check. `negate=false` → IN, `true` → NOT IN.
/// Set is guaranteed NULL-free (the resolver drops NULLs at materialization).
/// Two-valued logic: NULL in the column never matches → IN false, NOT IN
/// also false (consistent with the IN side).
pub fn evaluateInSetMask(view: ColumnView, values: []const Value, negate: bool, n: usize, mask: []bool) !void {
    // Outer per-column-type dispatch keeps the inner loop type-mono.
    switch (view.data) {
        .int => |col| {
            for (0..n) |i| {
                if (!view.isValid(i)) {
                    mask[i] = false;
                    continue;
                }
                var found = false;
                for (values) |v| {
                    if (v == .int and v.int == col[i]) {
                        found = true;
                        break;
                    }
                }
                mask[i] = if (negate) !found else found;
            }
        },
        .bigint => |col| {
            for (0..n) |i| {
                if (!view.isValid(i)) {
                    mask[i] = false;
                    continue;
                }
                var found = false;
                for (values) |v| {
                    if (v == .bigint and v.bigint == col[i]) {
                        found = true;
                        break;
                    }
                }
                mask[i] = if (negate) !found else found;
            }
        },
        .varchar => |sv| try evalInSetStringy(sv, values, negate, view, n, mask),
        .string => |sv| try evalInSetStringy(sv, values, negate, view, n, mask),
        .char => |sv| try evalInSetStringy(sv, values, negate, view, n, mask),
        else => return Error.UnsupportedOperatorForType,
    }
}

fn evalInSetStringy(sv: anytype, values: []const Value, negate: bool, view: ColumnView, n: usize, mask: []bool) !void {
    for (0..n) |i| {
        if (!view.isValid(i)) {
            mask[i] = false;
            continue;
        }
        const cell = sv.rowBytes(i);
        var found = false;
        for (values) |v| {
            if (v == .text and std.mem.eql(u8, v.text, cell)) {
                found = true;
                break;
            }
        }
        mask[i] = if (negate) !found else found;
    }
}

/// Per-row col-vs-col comparison. Both views must share the same
/// primitive type tag (validateExpr enforces). NULL on either side
/// → mask[i] = false (two-valued logic).
pub fn evaluateColColMask(left: ColumnView, right: ColumnView, op: PredicateOp, n: usize, mask: []bool) !void {
    switch (left.data) {
        .int => |l| {
            const r = right.data.int;
            for (0..n) |i| mask[i] = cmp(i32, l[i], r[i], op);
        },
        .bigint => |l| {
            const r = right.data.bigint;
            for (0..n) |i| mask[i] = cmp(i64, l[i], r[i], op);
        },
        .smallint => |l| {
            const r = right.data.smallint;
            for (0..n) |i| mask[i] = cmp(i16, l[i], r[i], op);
        },
        .tinyint => |l| {
            const r = right.data.tinyint;
            for (0..n) |i| mask[i] = cmp(i8, l[i], r[i], op);
        },
        .largeint => |l| {
            const r = right.data.largeint;
            for (0..n) |i| mask[i] = cmp(i128, l[i], r[i], op);
        },
        .float => |l| {
            const r = right.data.float;
            for (0..n) |i| mask[i] = cmp(f32, l[i], r[i], op);
        },
        .double => |l| {
            const r = right.data.double;
            for (0..n) |i| mask[i] = cmp(f64, l[i], r[i], op);
        },
        .boolean => |l| {
            const r = right.data.boolean;
            for (0..n) |i| mask[i] = cmp(u8, l[i], r[i], op);
        },
        .date => |l| {
            const r = right.data.date;
            for (0..n) |i| mask[i] = cmp(i32, l[i], r[i], op);
        },
        .datetime => |l| {
            const r = right.data.datetime;
            for (0..n) |i| mask[i] = cmp(i64, l[i], r[i], op);
        },
        .decimal64 => |l| {
            const r = right.data.decimal64;
            for (0..n) |i| mask[i] = cmp(i64, l[i], r[i], op);
        },
        .decimal128 => |l| {
            const r = right.data.decimal128;
            for (0..n) |i| mask[i] = cmp(i128, l[i], r[i], op);
        },
        .uuid => |l| {
            const r = right.data.uuid;
            for (0..n) |i| mask[i] = cmp(u128, l[i], r[i], op);
        },
        .varchar => |l| switch (right.data) {
            .varchar => |r| stringCmpCol(l, r, op, n, mask),
            .string => |r| stringCmpCol(l, r, op, n, mask),
            .char => |r| stringCmpCol(l, r, op, n, mask),
            else => unreachable,
        },
        .string => |l| switch (right.data) {
            .varchar => |r| stringCmpCol(l, r, op, n, mask),
            .string => |r| stringCmpCol(l, r, op, n, mask),
            .char => |r| stringCmpCol(l, r, op, n, mask),
            else => unreachable,
        },
        .char => |l| switch (right.data) {
            .varchar => |r| stringCmpCol(l, r, op, n, mask),
            .string => |r| stringCmpCol(l, r, op, n, mask),
            .char => |r| stringCmpCol(l, r, op, n, mask),
            else => unreachable,
        },
    }
    // NULL on either side → false.
    if (left.nulls != null) {
        for (0..n) |i| if (!left.isValid(i)) {
            mask[i] = false;
        };
    }
    if (right.nulls != null) {
        for (0..n) |i| if (!right.isValid(i)) {
            mask[i] = false;
        };
    }
}

fn stringCmpCol(l: anytype, r: anytype, op: PredicateOp, n: usize, mask: []bool) void {
    for (0..n) |i| {
        const eq = std.mem.eql(u8, l.rowBytes(i), r.rowBytes(i));
        mask[i] = if (op == .eq) eq else !eq;
    }
}

/// Per-row LIKE evaluation: matches NULL → false (two-valued logic).
/// Only valid against string-typed columns (validateExpr enforces).
pub fn evaluateLikeMask(view: ColumnView, pattern: []const u8, n: usize, mask: []bool) !void {
    switch (view.data) {
        .varchar => |sv| {
            for (0..n) |i| {
                if (!view.isValid(i)) {
                    mask[i] = false;
                    continue;
                }
                mask[i] = likeMatch(sv.rowBytes(i), pattern);
            }
        },
        .string => |sv| {
            for (0..n) |i| {
                if (!view.isValid(i)) {
                    mask[i] = false;
                    continue;
                }
                mask[i] = likeMatch(sv.rowBytes(i), pattern);
            }
        },
        .char => |sv| {
            for (0..n) |i| {
                if (!view.isValid(i)) {
                    mask[i] = false;
                    continue;
                }
                mask[i] = likeMatch(sv.rowBytes(i), pattern);
            }
        },
        else => return Error.UnsupportedOperatorForType,
    }
}

/// Evaluate a single leaf predicate against a column view, writing per-row
/// match bits into `mask`. Two-valued logic: NULL never matches.
pub fn evaluateMaskWithPred(view: ColumnView, p: Predicate, n: usize, mask: []bool) !void {
    const op = p.op;
    switch (view.data) {
        .int => |s| {
            const want = p.val.int;
            for (s[0..n], 0..) |v, i| mask[i] = cmp(i32, v, want, op);
        },
        .bigint => |s| {
            const want = p.val.bigint;
            for (s[0..n], 0..) |v, i| mask[i] = cmp(i64, v, want, op);
        },
        .boolean => |s| {
            const want = @intFromBool(p.val.boolean);
            for (s[0..n], 0..) |v, i| mask[i] = cmp(u8, v, want, op);
        },
        .varchar => |sv| {
            if (op != .eq and op != .neq) return Error.UnsupportedOperatorForType;
            for (0..n) |i| {
                const eq = std.mem.eql(u8, sv.rowBytes(i), p.val.text);
                mask[i] = if (op == .eq) eq else !eq;
            }
        },
        .string => |sv| {
            if (op != .eq and op != .neq) return Error.UnsupportedOperatorForType;
            for (0..n) |i| {
                const eq = std.mem.eql(u8, sv.rowBytes(i), p.val.text);
                mask[i] = if (op == .eq) eq else !eq;
            }
        },
        .float => |s| {
            const want = p.val.float;
            for (s[0..n], 0..) |v, i| mask[i] = cmp(f32, v, want, op);
        },
        .double => |s| {
            const want = p.val.double;
            for (s[0..n], 0..) |v, i| mask[i] = cmp(f64, v, want, op);
        },
        .date => |s| {
            const want = p.val.date;
            for (s[0..n], 0..) |v, i| mask[i] = cmp(i32, v, want, op);
        },
        .datetime => |s| {
            const want = p.val.datetime;
            for (s[0..n], 0..) |v, i| mask[i] = cmp(i64, v, want, op);
        },
        .tinyint => |s| {
            const want = p.val.tinyint;
            for (s[0..n], 0..) |v, i| mask[i] = cmp(i8, v, want, op);
        },
        .smallint => |s| {
            const want = p.val.smallint;
            for (s[0..n], 0..) |v, i| mask[i] = cmp(i16, v, want, op);
        },
        .largeint => |s| {
            const want = p.val.largeint;
            for (s[0..n], 0..) |v, i| mask[i] = cmp(i128, v, want, op);
        },
        .char => |sv| {
            if (op != .eq and op != .neq) return Error.UnsupportedOperatorForType;
            for (0..n) |i| {
                const eq = std.mem.eql(u8, sv.rowBytes(i), p.val.text);
                mask[i] = if (op == .eq) eq else !eq;
            }
        },
        .decimal64 => |s| {
            const want = p.val.decimal64;
            for (s[0..n], 0..) |v, i| mask[i] = cmp(i64, v, want, op);
        },
        .decimal128 => |s| {
            const want = p.val.decimal128;
            for (s[0..n], 0..) |v, i| mask[i] = cmp(i128, v, want, op);
        },
        .uuid => |s| {
            const want = p.val.uuid;
            for (s[0..n], 0..) |v, i| mask[i] = cmp(u128, v, want, op);
        },
    }
    // Two-valued logic: a NULL value never matches a comparison.
    if (view.nulls != null) {
        for (0..n) |i| {
            if (!view.isValid(i)) mask[i] = false;
        }
    }
}

fn cmp(comptime T: type, a: T, b: T, op: PredicateOp) bool {
    return switch (op) {
        .eq => a == b,
        .neq => a != b,
        .lt => a < b,
        .lte => a <= b,
        .gt => a > b,
        .gte => a >= b,
    };
}

/// Returns true if the row-group stats could contain rows matching `op val`.
/// Used by Scan and DELETE to decide whether to skip a row group entirely.
///
/// Stats are i128 with per-type encoding (see `format.Stats`). The
/// predicate value is encoded with the same scheme so signed i128
/// comparison gives the right answer for every type.
///
/// String predicates use the 16-byte prefix encoding; range ops are
/// rejected by `validateExpr` so this only handles eq/neq for strings.
/// Tied prefixes (`>16` byte strings sharing the same first 16 bytes)
/// stay on the conservative side — eq keeps the row group, neq never
/// prunes.
pub fn statsOverlapPredicate(s: storage.format.Stats, op: PredicateOp, v: Value) bool {
    const wanted: i128 = switch (v) {
        .int => |x| x,
        .bigint => |x| x,
        .boolean => |x| @intFromBool(x),
        .date => |x| x,
        .datetime => |x| x,
        .tinyint => |x| x,
        .smallint => |x| x,
        .decimal64 => |x| x,
        .largeint, .decimal128 => |x| x,
        .uuid => |x| storage.format.encodeUnsignedU128(x),
        .text => |x| {
            if (op == .neq) return true;
            if (op != .eq) return true;
            const enc = storage.format.encodeStringPrefix(x);
            return enc >= s.min and enc <= s.max;
        },
        // Floats still carry no stats — keep conservatively.
        .float, .double => return true,
    };
    return switch (op) {
        .eq => wanted >= s.min and wanted <= s.max,
        .neq => !(s.min == s.max and s.min == wanted),
        .lt => s.min < wanted,
        .lte => s.min <= wanted,
        .gt => s.max > wanted,
        .gte => s.max >= wanted,
    };
}
