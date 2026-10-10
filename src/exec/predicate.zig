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
const simd = @import("../util/simd.zig");
const like_pattern = @import("../util/like.zig");
const Error = exec.Error;
const scalar_fn_common = @import("scalar_fn_common.zig");
const json_binary = @import("json_binary.zig");
const cast = @import("cast.zig");
const scalar_fn_time = @import("scalar_fn_time.zig");
const decimal_pow10 = @import("scalar_fn_decimal.zig").pow10;
const decimal_rescale = @import("scalar_fn_decimal.zig").rescale;

pub const PredicateOp = enum { eq, neq, lt, lte, gt, gte };

pub const Predicate = struct {
    col: []const u8,
    op: PredicateOp,
    val: Value,
    /// `val` comes from the statement itself: a constant written in its
    /// text, or a user variable or scalar subquery it reads. Text no DATE or
    /// DATETIME reads then fails the statement, as MySQL raises error 1525.
    /// A bound parameter's value or an API caller's never matches instead,
    /// as MySQL returns no rows for a prepared statement's parameter.
    from_statement: bool = false,
    /// The leaf is a condition's test of `col` itself (`WHERE x`, `x OR y`),
    /// not a comparison the statement spells: `col` read as
    /// `CAST(col AS BOOLEAN)` against `val`, 0. That is `col <> 0` for a
    /// number, but text reads as `textBoolean` reads it, where a comparison
    /// with a number reads it as a number (`'0.5' <> 0` holds, `'0.5'` is
    /// UNKNOWN).
    as_boolean: bool = false,
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
    day_leaf: Predicate,
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
    /// A text column compared with a number (`code = 12`, `code > 1.5`):
    /// validation's rewrite of such a `.leaf`, since no literal of the
    /// column's type stands for a number (`'12.0'` equals 12, `'12'` too).
    /// Each row's text is read as a number the way a CAST reads it, and text
    /// that isn't a number compares as NULL. Only the generic evaluator
    /// handles it; pruning and fused kernels decline.
    text_as_number: Predicate,
    /// An `.in_set` over a text column whose set holds numbers, rewritten by
    /// validation for the same reason: each number meets the row's text read
    /// as a number, each text value meets it bytewise.
    text_as_number_set: InSet,
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
    /// Resolved form of a correlated EXISTS / NOT EXISTS whose inner
    /// includes a single range conjunct of the form `inner.x op outer.y`
    /// (op ∈ {<, <=, >, >=}). The inner-side `x` values for each
    /// equi-correlation key are materialized, sorted ascending, with
    /// min/max cached. Per outer row: look up by `outer_keys`, then
    /// check whether any inner value satisfies `value op outer_y` —
    /// for open-ended ops a single compare to min or max is enough.
    /// Combined with optional equi-key correlation (`outer_keys`),
    /// the lookup happens within the matching group only.
    correlated_range: CorrelatedRange,
    /// Predicate RHS is a pending var-ref (`WHERE qty > @threshold`).
    /// The pre-compile pass looks up `var_name` in the active Session's
    /// vars and rewrites this into a `.leaf` with the resolved Value.
    /// Operators never see this variant.
    leaf_var: VarPred,
    /// Three-valued UNKNOWN for every row — the lowered form of a
    /// comparison against a NULL literal (`v = NULL`, `v > NULL`).
    /// Evaluates to no-match like `.always = false`, but its NEGATION is
    /// itself (NOT UNKNOWN is UNKNOWN), which `.always` can't express.
    unknown,
};

pub const VarPred = struct {
    col: []const u8,
    op: PredicateOp,
    var_name: []const u8,
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
    /// A row value's remaining columns (`(a, b) IN (SELECT x, y ...)`),
    /// matched against the inner's second and later columns.
    rest_cols: []const []const u8 = &.{},
};

pub const InSet = struct {
    col: []const u8,
    values: []const Value,
    negate: bool,
    /// Type of the subquery column the values were drained from; null for
    /// a literal list.
    value_type: ?types.Type = null,
    /// As a prune offer (`Query.addPruneSet`): the offering consumer drops
    /// every row whose value is not in `values`, NULL included, so a scan
    /// may filter those rows out, not only skip row groups. Meaningless in
    /// a predicate expression.
    rows: bool = false,
};

/// The column an OR tests when every arm is `col = literal` on that one
/// column: the parser spells a literal `col IN (a, b, ...)` this way, so such
/// an OR is an IN list and can be matched as a set.
pub fn eqDisjunctionColumn(arms: []const PredicateExpr) ?[]const u8 {
    if (arms.len == 0 or arms[0] != .leaf) return null;
    const col = arms[0].leaf.col;
    for (arms) |arm| switch (arm) {
        .leaf => |l| if (l.op != .eq or !types.columnNameEql(l.col, col)) return null,
        else => return null,
    };
    return col;
}

pub const ColColPred = struct {
    left: []const u8,
    op: PredicateOp,
    right: []const u8,
};

/// Whether the predicate references column `name`. CONSERVATIVE: unresolved
/// or opaque variants (subqueries, correlated sets) answer TRUE — callers
/// use this to prove a pushdown is safe, so "don't know" must mean "yes".
pub fn touchesColumn(expr: PredicateExpr, name: []const u8) bool {
    return switch (expr) {
        .leaf, .day_leaf, .text_as_number => |l| types.columnNameEql(l.col, name),
        .leaf_col_col => |c| types.columnNameEql(c.left, name) or types.columnNameEql(c.right, name),
        .is_null, .is_not_null => |c| types.columnNameEql(c, name),
        .like => |l| types.columnNameEql(l.col, name),
        .in_set, .text_as_number_set => |s| types.columnNameEql(s.col, name),
        .@"and", .@"or" => |arms| blk: {
            for (arms) |a| {
                if (touchesColumn(a, name)) break :blk true;
            }
            break :blk false;
        },
        .not => |n| touchesColumn(n.*, name),
        .always => false,
        else => true,
    };
}

/// Every column the predicate reads, appended to `out` once each. A
/// subquery marker reads its compared columns; its inner query is its own.
pub fn collectColumnNames(allocator: std.mem.Allocator, out: *std.ArrayListUnmanaged([]const u8), expr: PredicateExpr) std.mem.Allocator.Error!void {
    switch (expr) {
        .leaf, .day_leaf, .text_as_number => |l| try appendColumnName(allocator, out, l.col),
        .leaf_col_col => |c| {
            try appendColumnName(allocator, out, c.left);
            try appendColumnName(allocator, out, c.right);
        },
        .is_null, .is_not_null => |c| try appendColumnName(allocator, out, c),
        .like => |l| try appendColumnName(allocator, out, l.col),
        .in_set, .text_as_number_set => |s| try appendColumnName(allocator, out, s.col),
        .leaf_var => |v| try appendColumnName(allocator, out, v.col),
        .scalar_subquery => |s| try appendColumnName(allocator, out, s.col),
        .in_subquery => |s| {
            try appendColumnName(allocator, out, s.col);
            for (s.rest_cols) |c| try appendColumnName(allocator, out, c);
        },
        .correlated_set => |s| for (s.outer_cols) |c| try appendColumnName(allocator, out, c),
        .correlated_scalar => |s| {
            try appendColumnName(allocator, out, s.outer_compared);
            for (s.outer_keys) |c| try appendColumnName(allocator, out, c);
        },
        .correlated_range => |r| {
            for (r.outer_keys) |c| try appendColumnName(allocator, out, c);
            try appendColumnName(allocator, out, r.outer_range_col);
            if (r.outer_range_col_upper) |upper| try appendColumnName(allocator, out, upper);
        },
        .@"and", .@"or" => |kids| for (kids) |k| try collectColumnNames(allocator, out, k),
        .not => |k| try collectColumnNames(allocator, out, k.*),
        .exists_subquery, .always, .unknown => {},
    }
}

fn appendColumnName(allocator: std.mem.Allocator, out: *std.ArrayListUnmanaged([]const u8), name: []const u8) std.mem.Allocator.Error!void {
    if (name.len == 0) return;
    for (out.items) |existing| if (types.columnNameEql(existing, name)) return;
    try out.append(allocator, name);
}

/// Whether the predicate is made only of per-row kernels: no subquery
/// lookup set and no UNKNOWN.
pub fn kernelsOnly(expr: PredicateExpr) bool {
    return switch (expr) {
        .leaf, .day_leaf, .leaf_col_col, .is_null, .is_not_null, .like, .in_set, .text_as_number, .text_as_number_set, .always => true,
        .@"and", .@"or" => |children| blk: {
            for (children) |child| if (!kernelsOnly(child)) break :blk false;
            break :blk true;
        },
        .not => |child| kernelsOnly(child.*),
        else => false,
    };
}

pub fn touches_resolved_column(expr: PredicateExpr, schema: []const Column, idx: usize) bool {
    return switch (expr) {
        .leaf, .day_leaf, .text_as_number => |leaf| types.findColumn(schema, leaf.col) == idx,
        .leaf_col_col => |pair| types.findColumn(schema, pair.left) == idx or types.findColumn(schema, pair.right) == idx,
        .is_null, .is_not_null => |col| types.findColumn(schema, col) == idx,
        .like => |like| types.findColumn(schema, like.col) == idx,
        .in_set, .text_as_number_set => |set| types.findColumn(schema, set.col) == idx,
        .@"and", .@"or" => |children| blk: {
            for (children) |child| if (touches_resolved_column(child, schema, idx)) break :blk true;
            break :blk false;
        },
        .not => |child| touches_resolved_column(child.*, schema, idx),
        .always => false,
        else => true,
    };
}

/// Structural equality: column names case-insensitively, literals by
/// value, children in order. Forms backed by a subquery never compare
/// equal — each is its own evaluation.
pub fn eql(a: PredicateExpr, b: PredicateExpr) bool {
    if (std.meta.activeTag(a) != std.meta.activeTag(b)) return false;
    return switch (a) {
        .leaf => |l| leafEql(l, b.leaf),
        .day_leaf => |l| leafEql(l, b.day_leaf),
        .text_as_number => |l| leafEql(l, b.text_as_number),
        .leaf_col_col => |c| c.op == b.leaf_col_col.op and
            types.columnNameEql(c.left, b.leaf_col_col.left) and
            types.columnNameEql(c.right, b.leaf_col_col.right),
        .is_null => |c| types.columnNameEql(c, b.is_null),
        .is_not_null => |c| types.columnNameEql(c, b.is_not_null),
        .like => |l| types.columnNameEql(l.col, b.like.col) and std.mem.eql(u8, l.pattern, b.like.pattern),
        .@"and" => |arms| armsEql(arms, b.@"and"),
        .@"or" => |arms| armsEql(arms, b.@"or"),
        .not => |n| eql(n.*, b.not.*),
        .always => |v| v == b.always,
        .in_set => |s| setEql(s, b.in_set),
        .text_as_number_set => |s| setEql(s, b.text_as_number_set),
        .leaf_var => |v| v.op == b.leaf_var.op and
            types.columnNameEql(v.col, b.leaf_var.col) and
            std.mem.eql(u8, v.var_name, b.leaf_var.var_name),
        .unknown => true,
        .scalar_subquery, .exists_subquery, .in_subquery, .correlated_set, .correlated_scalar, .correlated_range => false,
    };
}

fn leafEql(a: Predicate, b: Predicate) bool {
    return a.op == b.op and a.as_boolean == b.as_boolean and types.columnNameEql(a.col, b.col) and a.val.eql(b.val);
}

fn setEql(a: InSet, b: InSet) bool {
    if (a.negate != b.negate or a.values.len != b.values.len or !types.columnNameEql(a.col, b.col)) return false;
    for (a.values, b.values) |x, y| if (!x.eql(y)) return false;
    return true;
}

fn armsEql(a: []const PredicateExpr, b: []const PredicateExpr) bool {
    if (a.len != b.len) return false;
    for (a, b) |x, y| if (!eql(x, y)) return false;
    return true;
}

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
    /// Type of each row's `value`: a decimal `Value` carries no scale, and
    /// the outer column may be a different type altogether.
    value_type: types.Type,
    /// Types of the inner correlation key columns, parallel to `outer_keys`.
    key_types: []const types.Type,
    /// The aggregate over no rows, which an outer row whose key matched
    /// none compares with (COUNT's 0); null where that aggregate is NULL.
    missing: ?Value = null,
    /// `rows` are in key order (`sortKeyed`) and stay so at the outer
    /// columns' types (`keysSearchable`), so a lookup binary-searches them.
    sorted: bool = false,
};

pub const CorrelatedRangeGroup = struct {
    /// Equi-correlation key for this group (parallel to outer_keys
    /// in the parent CorrelatedRange). Empty slice when there are
    /// no equi keys — that case has exactly one group.
    key: []const Value,
    /// Inner-side range-column values for rows matching this key,
    /// sorted ascending. NULLs were dropped at materialization.
    values: []const Value,
};

pub const CorrelatedRange = struct {
    /// Outer-side correlation key column names (equi part). Parallel
    /// to each group's `key` tuple. Empty when no equi keys.
    outer_keys: []const []const u8,
    /// Outer-side range column (the `y` in `inner.x op outer.y`).
    /// For closed ranges this is the lower-bound outer column.
    outer_range_col: []const u8,
    /// Op in canonical "inner op outer" form. So `outer.y < inner.x`
    /// becomes op = `.gt` (inner > outer). Limited to {lt, lte, gt, gte}
    /// — `.eq` is captured as equi correlation, `.neq` isn't useful.
    /// For closed ranges this is the lower-bound op (≥ or >).
    op: PredicateOp,
    /// Upper-bound outer column for closed (BETWEEN-style) ranges.
    /// Null for open-ended ranges (e.g. plain `inner.x > outer.lo`).
    /// When set, `op_upper` must also be set and `op` is the
    /// lower-bound op.
    outer_range_col_upper: ?[]const u8 = null,
    /// Upper-bound op for closed ranges. Either `.lt` or `.lte`.
    op_upper: ?PredicateOp = null,
    /// One group per distinct equi-key tuple of the inner's values. Two
    /// can be equal at the outer columns' types (text `'07'` and `'7'`
    /// against a number).
    groups: []const CorrelatedRangeGroup,
    /// `groups` are in key order (`sortKeyed`) and stay so at the outer
    /// columns' types (`keysSearchable`), so a lookup binary-searches them.
    sorted: bool = false,
    /// `true` = NOT EXISTS — outer row passes iff no inner value
    /// satisfies the range op.
    negate: bool,
    /// Types of the inner correlation key columns, parallel to `outer_keys`.
    key_types: []const types.Type,
    /// Type of the inner range column.
    range_type: types.Type,
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
    /// Types of the inner columns, parallel to `outer_cols`.
    inner_types: []const types.Type,
    /// `rows` are in tuple order (`sortKeyed`) and stay so at the outer
    /// columns' types (`keysSearchable`), so a lookup binary-searches them.
    sorted: bool = false,
};

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

/// One column relabel for pushing an expression through a renaming
/// projection: a reference to `from` (the projection's output label)
/// becomes `to` (the source column's label on the far side).
pub const ColRename = struct { from: []const u8, to: []const u8 };

/// Resolve a column reference through a rename list; identity when absent.
pub fn renameOf(renames: []const ColRename, name: []const u8) []const u8 {
    for (renames) |r| {
        if (types.columnNameEql(r.from, name)) return r.to;
    }
    return name;
}

/// Deep-clone a PredicateExpr into `out_arena`. Mirrors
/// `expr.deepClone` for the boolean side of a CASE/WHERE expression;
/// used when an Expr.case needs to outlive its source arena.
pub fn deepClonePredicate(out_arena: std.mem.Allocator, p: PredicateExpr) std.mem.Allocator.Error!PredicateExpr {
    return deepClonePredicateRenamed(out_arena, p, &.{});
}

/// deepClonePredicate with column-reference substitution (see `ColRename`).
pub fn deepClonePredicateRenamed(out_arena: std.mem.Allocator, p: PredicateExpr, renames: []const ColRename) std.mem.Allocator.Error!PredicateExpr {
    return switch (p) {
        .leaf => |lf| .{ .leaf = try cloneLeaf(out_arena, lf, renames) },
        .day_leaf => |lf| .{ .day_leaf = try cloneLeaf(out_arena, lf, renames) },
        .text_as_number => |lf| .{ .text_as_number = try cloneLeaf(out_arena, lf, renames) },
        .leaf_col_col => |lc| .{ .leaf_col_col = .{
            .left = try out_arena.dupe(u8, renameOf(renames, lc.left)),
            .op = lc.op,
            .right = try out_arena.dupe(u8, renameOf(renames, lc.right)),
        } },
        .is_null => |c| .{ .is_null = try out_arena.dupe(u8, renameOf(renames, c)) },
        .is_not_null => |c| .{ .is_not_null = try out_arena.dupe(u8, renameOf(renames, c)) },
        .like => |lp| .{ .like = .{
            .col = try out_arena.dupe(u8, renameOf(renames, lp.col)),
            .pattern = try out_arena.dupe(u8, lp.pattern),
        } },
        .scalar_subquery => |sq| .{ .scalar_subquery = .{
            .col = try out_arena.dupe(u8, renameOf(renames, sq.col)),
            .op = sq.op,
            .source = sq.source,
        } },
        .exists_subquery => |src| .{ .exists_subquery = src },
        .always => |b| .{ .always = b },
        .in_subquery => |s| blk: {
            const rest_cols = try out_arena.alloc([]const u8, s.rest_cols.len);
            for (s.rest_cols, rest_cols) |src, *dst| dst.* = try out_arena.dupe(u8, renameOf(renames, src));
            break :blk .{ .in_subquery = .{
                .col = try out_arena.dupe(u8, renameOf(renames, s.col)),
                .source = s.source,
                .negate = s.negate,
                .rest_cols = rest_cols,
            } };
        },
        .in_set => |s| .{ .in_set = try cloneInSet(out_arena, s, renames) },
        .text_as_number_set => |s| .{ .text_as_number_set = try cloneInSet(out_arena, s, renames) },
        .correlated_set => |s| blk: {
            const outer_cols = try out_arena.alloc([]const u8, s.outer_cols.len);
            for (s.outer_cols, outer_cols) |src, *dst| dst.* = try out_arena.dupe(u8, renameOf(renames, src));
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
                .inner_types = try out_arena.dupe(types.Type, s.inner_types),
                .sorted = s.sorted,
            } };
        },
        .correlated_scalar => |s| blk: {
            const outer_keys = try out_arena.alloc([]const u8, s.outer_keys.len);
            for (s.outer_keys, outer_keys) |src, *dst| dst.* = try out_arena.dupe(u8, renameOf(renames, src));
            const rows = try out_arena.alloc(CorrelatedScalarRow, s.rows.len);
            for (s.rows, rows) |src, *dst| {
                const key = try out_arena.alloc(Value, src.key.len);
                for (src.key, key) |v, *k| k.* = try cloneValue(out_arena, v);
                dst.* = .{ .key = key, .value = try cloneValue(out_arena, src.value) };
            }
            break :blk .{ .correlated_scalar = .{
                .outer_compared = try out_arena.dupe(u8, renameOf(renames, s.outer_compared)),
                .op = s.op,
                .outer_keys = outer_keys,
                .rows = rows,
                .value_type = s.value_type,
                .key_types = try out_arena.dupe(types.Type, s.key_types),
                .missing = if (s.missing) |m| try cloneValue(out_arena, m) else null,
                .sorted = s.sorted,
            } };
        },
        .correlated_range => |s| blk: {
            const outer_keys = try out_arena.alloc([]const u8, s.outer_keys.len);
            for (s.outer_keys, outer_keys) |src, *dst| dst.* = try out_arena.dupe(u8, renameOf(renames, src));
            const groups = try out_arena.alloc(CorrelatedRangeGroup, s.groups.len);
            for (s.groups, groups) |src, *dst| {
                const key = try out_arena.alloc(Value, src.key.len);
                for (src.key, key) |v, *k| k.* = try cloneValue(out_arena, v);
                const values = try out_arena.alloc(Value, src.values.len);
                for (src.values, values) |v, *o| o.* = try cloneValue(out_arena, v);
                dst.* = .{ .key = key, .values = values };
            }
            const upper_col_dup: ?[]const u8 = if (s.outer_range_col_upper) |c| try out_arena.dupe(u8, renameOf(renames, c)) else null;
            break :blk .{ .correlated_range = .{
                .outer_keys = outer_keys,
                .outer_range_col = try out_arena.dupe(u8, renameOf(renames, s.outer_range_col)),
                .op = s.op,
                .outer_range_col_upper = upper_col_dup,
                .op_upper = s.op_upper,
                .groups = groups,
                .negate = s.negate,
                .key_types = try out_arena.dupe(types.Type, s.key_types),
                .range_type = s.range_type,
                .sorted = s.sorted,
            } };
        },
        .@"and" => |kids| blk: {
            const dup = try out_arena.alloc(PredicateExpr, kids.len);
            for (kids, 0..) |k, i| dup[i] = try deepClonePredicateRenamed(out_arena, k, renames);
            break :blk .{ .@"and" = dup };
        },
        .@"or" => |kids| blk: {
            const dup = try out_arena.alloc(PredicateExpr, kids.len);
            for (kids, 0..) |k, i| dup[i] = try deepClonePredicateRenamed(out_arena, k, renames);
            break :blk .{ .@"or" = dup };
        },
        .not => |child| blk: {
            const dup = try out_arena.create(PredicateExpr);
            dup.* = try deepClonePredicateRenamed(out_arena, child.*, renames);
            break :blk .{ .not = dup };
        },
        .leaf_var => |v| .{ .leaf_var = .{
            .col = try out_arena.dupe(u8, renameOf(renames, v.col)),
            .op = v.op,
            .var_name = try out_arena.dupe(u8, v.var_name),
        } },
        .unknown => .unknown,
    };
}

fn cloneLeaf(out_arena: std.mem.Allocator, lf: Predicate, renames: []const ColRename) std.mem.Allocator.Error!Predicate {
    return .{
        .col = try out_arena.dupe(u8, renameOf(renames, lf.col)),
        .op = lf.op,
        .val = try cloneValue(out_arena, lf.val),
        .from_statement = lf.from_statement,
        .as_boolean = lf.as_boolean,
    };
}

fn cloneInSet(out_arena: std.mem.Allocator, s: InSet, renames: []const ColRename) std.mem.Allocator.Error!InSet {
    const vals = try out_arena.alloc(Value, s.values.len);
    for (s.values, vals) |v, *out| out.* = try cloneValue(out_arena, v);
    return .{
        .col = try out_arena.dupe(u8, renameOf(renames, s.col)),
        .values = vals,
        .negate = s.negate,
        .value_type = s.value_type,
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
            if (ValueTag.fromType(col_type) != std.meta.activeTag(p.val)) {
                switch (placeLiteral(p.val, col_type)) {
                    .exact => |v| p.val = v,
                    .between => |b| foldBetween(expr, b.lo, b.hi),
                    .beyond => |side| foldBeyond(expr, side),
                    .null_text => expr.* = .unknown,
                    .parse_rows => {
                        const leaf = p.*;
                        expr.* = .{ .text_as_number = leaf };
                    },
                    .not_temporal => {
                        try rejectUnreadTemporal(col_type, p.val.text, p.from_statement);
                        expr.* = .unknown;
                    },
                    .incomparable => return Error.PredicateTypeMismatch,
                }
            }
        },
        .day_leaf => |*p| {
            const col_idx = types.findColumn(schema, p.col) orelse return Error.ColumnNotFound;
            const col_type = schema[col_idx].type;
            if (col_type != .date and col_type != .datetime) return Error.PredicateTypeMismatch;
            if (std.meta.activeTag(p.val) != .int) {
                tryWidenLiteral(&p.val, .int) catch return Error.PredicateTypeMismatch;
            }
        },
        .leaf_col_col => |lc| {
            const li = types.findColumn(schema, lc.left) orelse return Error.ColumnNotFound;
            const ri = types.findColumn(schema, lc.right) orelse return Error.ColumnNotFound;
            if (!typesComparable(schema[li].type, schema[ri].type) and !temporalBesideNumber(schema[li].type, schema[ri].type)) return Error.PredicateTypeMismatch;
        },
        .is_null, .is_not_null => |col_name| {
            _ = types.findColumn(schema, col_name) orelse return Error.ColumnNotFound;
        },
        .text_as_number => |p| try expectTextColumn(schema, p.col),
        .text_as_number_set => |s| try expectTextColumn(schema, s.col),
        .like => |lp| {
            const idx = types.findColumn(schema, lp.col) orelse return Error.ColumnNotFound;
            const ty = schema[idx].type;
            if (!ty.isString() and !cast.assignsByRule(ty, .string)) return Error.UnsupportedOperatorForType;
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
        // `.in_set` — column must exist; each value coerces to the column
        // type. A literal that CANNOT represent in the column type can never
        // equal it (`x IN (2.5, 5)` on an INT column: 2.5 matches nothing)
        // and is dropped from the set — correct for the negated form too
        // (`x <> 2.5` is always true for an INT x under this dialect's
        // NULL-skipping NOT IN). Text that doesn't read as the DATE or
        // DATETIME it meets drops the same way: set values are rows an IN
        // subquery returned, which MySQL skips with a warning; only a
        // `.leaf` the statement spells raises `InvalidTemporalLiteral`. A
        // number against a text column stays a number and the node becomes
        // `.text_as_number_set`. Values are arena-owned parse output;
        // in-place rewrite mirrors the `.leaf` arm.
        .in_set => |*s| {
            const col_idx = types.findColumn(schema, s.col) orelse return Error.ColumnNotFound;
            const col_type = schema[col_idx].type;
            if (s.value_type) |vt| if (!typesComparable(col_type, vt)) return Error.PredicateTypeMismatch;
            const col_tag = ValueTag.fromType(col_type);
            var needs_rewrite = false;
            for (s.values) |v| {
                if (std.meta.activeTag(v) != col_tag) {
                    needs_rewrite = true;
                    break;
                }
            }
            // Write only when a coercion is actually needed — like the
            // `.leaf` arm, matching-tag predicates stay untouched so
            // statically-built IR (rodata value slices) never faults.
            if (needs_rewrite) {
                const vals = @constCast(s.values);
                var keep: usize = 0;
                var parse_rows = false;
                for (s.values) |v| {
                    vals[keep] = if (std.meta.activeTag(v) == col_tag) v else switch (placeLiteral(v, col_type)) {
                        .exact => |c| c,
                        .parse_rows => blk: {
                            parse_rows = true;
                            break :blk v;
                        },
                        .between, .beyond, .null_text, .not_temporal, .incomparable => continue,
                    };
                    keep += 1;
                }
                s.values = vals[0..keep];
                if (parse_rows) {
                    const set = s.*;
                    expr.* = .{ .text_as_number_set = set };
                }
            }
        },
        // `.correlated_set` — every outer_col must exist; each tuple
        // value comes to its column's type, and a tuple holding a value
        // no row of its column can equal never matches, so it drops.
        .correlated_set => |*s| {
            var col_types_buf: [16]types.Type = undefined;
            const col_types = try outerColumnTypes(schema, s.outer_cols, &col_types_buf);
            for (s.rows) |row| if (row.len != s.outer_cols.len) return Error.PredicateTypeMismatch;
            try expectComparable(schema, s.outer_cols, s.inner_types);
            if (keyTuplesNeedCoercion(s.rows, col_types)) {
                const rows = @constCast(s.rows);
                var keep: usize = 0;
                for (s.rows) |row| {
                    if (!coerceKeyTuple(@constCast(row), col_types)) continue;
                    rows[keep] = row;
                    keep += 1;
                }
                s.rows = rows[0..keep];
            }
            s.sorted = keysSearchable([]const Value, s.rows, col_types);
        },
        // `.correlated_scalar` — outer_compared + outer_keys all exist;
        // keys come to the outer key columns' types like a set tuple.
        .correlated_scalar => |*s| {
            const cmp_idx = findCol(schema, s.outer_compared) orelse return Error.ColumnNotFound;
            if (!typesComparable(schema[cmp_idx].type, s.value_type)) return Error.PredicateTypeMismatch;
            try expectComparable(schema, s.outer_keys, s.key_types);
            var col_types_buf: [16]types.Type = undefined;
            const col_types = try outerColumnTypes(schema, s.outer_keys, &col_types_buf);
            var needs = false;
            for (s.rows) |row| needs = needs or keyTuplesNeedCoercion(&.{row.key}, col_types);
            if (needs) {
                const rows = @constCast(s.rows);
                var keep: usize = 0;
                for (s.rows) |row| {
                    if (!coerceKeyTuple(@constCast(row.key), col_types)) continue;
                    rows[keep] = row;
                    keep += 1;
                }
                s.rows = rows[0..keep];
            }
            s.sorted = keysSearchable(CorrelatedScalarRow, s.rows, col_types);
        },
        // `.correlated_range` — outer_range_col + each outer_keys
        // entry must exist on the outer schema; group keys come to the
        // key columns' types. Group values are pre-sorted at
        // materialization; trust their tags.
        .correlated_range => |*s| {
            try expectComparable(schema, &.{s.outer_range_col}, &.{s.range_type});
            if (s.outer_range_col_upper) |upper| try expectComparable(schema, &.{upper}, &.{s.range_type});
            try expectComparable(schema, s.outer_keys, s.key_types);
            var col_types_buf: [16]types.Type = undefined;
            const col_types = try outerColumnTypes(schema, s.outer_keys, &col_types_buf);
            var needs = false;
            for (s.groups) |g| needs = needs or keyTuplesNeedCoercion(&.{g.key}, col_types);
            if (needs) {
                const groups = @constCast(s.groups);
                var keep: usize = 0;
                for (s.groups) |g| {
                    if (!coerceKeyTuple(@constCast(g.key), col_types)) continue;
                    groups[keep] = g;
                    keep += 1;
                }
                s.groups = groups[0..keep];
            }
            s.sorted = keysSearchable(CorrelatedRangeGroup, s.groups, col_types);
        },
        // `.leaf_var` must have been resolved by the pre-compile
        // pass. Reaching here means the resolver missed a node.
        .leaf_var => return Error.PredicateTypeMismatch,
        .unknown => {},
    }
}

/// MySQL's ER_WRONG_VALUE prints at most 128 characters of the value.
const INVALID_TEMPORAL_VALUE_MAX = 128;

/// `InvalidTemporalLiteral` carries no payload, and operators raise it while
/// they build, far below the wire layer that reports it; the message naming
/// the constant waits here on the raising thread.
threadlocal var invalid_temporal_message_buf: ["Incorrect DATETIME value: ''".len + INVALID_TEMPORAL_VALUE_MAX]u8 = undefined;
threadlocal var invalid_temporal_message_len: usize = 0;

fn recordInvalidTemporal(col_type: types.Type, text: []const u8) void {
    var value = text[0..@min(text.len, INVALID_TEMPORAL_VALUE_MAX)];
    while (value.len < text.len and value.len > 0 and text[value.len] & 0xC0 == 0x80) value.len -= 1;
    const type_name = if (col_type == .date) "DATE" else "DATETIME";
    const message = std.fmt.bufPrint(&invalid_temporal_message_buf, "Incorrect {s} value: '{s}'", .{ type_name, value }) catch unreachable;
    invalid_temporal_message_len = message.len;
}

/// Text no date reads, compared with a DATE or DATETIME: a constant the
/// statement spells fails it, as MySQL raises `Incorrect DATE value`; any
/// other value never matches.
fn rejectUnreadTemporal(col_type: types.Type, text: []const u8, from_statement: bool) Error!void {
    if (!from_statement) return;
    recordInvalidTemporal(col_type, text);
    return Error.InvalidTemporalLiteral;
}

/// A number constant against a DATE or DATETIME, or a DATE or DATETIME
/// constant against a number, that a comparison outside a `.leaf` holds
/// (NULLIF's), placed as `validateExpr` places a leaf's: the value of
/// `col_type` it names, or null when no value of the type equals it.
pub fn placeComparedValue(val: Value, col_type: types.Type) ?Value {
    return switch (placeLiteral(val, col_type)) {
        .exact => |v| v,
        else => null,
    };
}

/// Text a comparison outside a `.leaf` holds against a value of `col_type`
/// (NULLIF's), placed as `validateExpr` places a leaf's: the value it names,
/// or null when no value of the type equals it.
pub fn placeComparedText(text: []const u8, col_type: types.Type, from_statement: bool) Error!?Value {
    return switch (placeLiteral(.{ .text = text }, col_type)) {
        .exact => |v| v,
        .not_temporal => {
            try rejectUnreadTemporal(col_type, text, from_statement);
            return null;
        },
        .between, .beyond, .null_text, .parse_rows, .incomparable => null,
    };
}

/// The message for this thread's last `InvalidTemporalLiteral` (MySQL's
/// wording: `Incorrect DATE value: 'abc'`), taken once so a later error
/// can't report a stale constant. Null when it was raised on another thread
/// or already taken. The slice lives until this thread raises again.
pub fn takeInvalidTemporalMessage() ?[]const u8 {
    const len = invalid_temporal_message_len;
    if (len == 0) return null;
    invalid_temporal_message_len = 0;
    return invalid_temporal_message_buf[0..len];
}

fn findCol(schema: []const Column, name: []const u8) ?usize {
    return types.findColumn(schema, name);
}

/// Each outer column against the type of the inner column it's compared
/// with, by the comparison rule: the pair compares or the statement fails,
/// however many rows the subquery drained.
fn expectComparable(schema: []const Column, outer: []const []const u8, inner_types: []const types.Type) Error!void {
    if (outer.len != inner_types.len) return Error.PredicateTypeMismatch;
    for (outer, inner_types) |name, ty| {
        const idx = findCol(schema, name) orelse return Error.ColumnNotFound;
        if (!typesComparable(schema[idx].type, ty)) return Error.PredicateTypeMismatch;
    }
}

fn outerColumnTypes(schema: []const Column, names: []const []const u8, buf: *[16]types.Type) Error![]const types.Type {
    if (names.len > buf.len) return Error.PredicateTypeMismatch;
    for (names, buf[0..names.len]) |name, *ty| ty.* = schema[findCol(schema, name) orelse return Error.ColumnNotFound].type;
    return buf[0..names.len];
}

fn keyTuplesNeedCoercion(tuples: []const []const Value, col_types: []const types.Type) bool {
    for (tuples) |tuple| for (tuple, col_types) |v, ty| {
        if (std.meta.activeTag(v) != ValueTag.fromType(ty)) return true;
    };
    return false;
}

/// Brings a materialized key tuple to its outer columns' types in place.
/// False when some value can't equal any value of its column, so the tuple
/// never matches. A number against a text column stays a number:
/// `cellMatchesValue` reads the row's text as one.
fn coerceKeyTuple(tuple: []Value, col_types: []const types.Type) bool {
    for (tuple, col_types) |*v, ty| {
        if (std.meta.activeTag(v.*) == ValueTag.fromType(ty)) continue;
        switch (placeLiteral(v.*, ty)) {
            .exact => |c| v.* = c,
            .parse_rows => {},
            .between, .beyond, .null_text, .not_temporal, .incomparable => return false,
        }
    }
    return true;
}

/// Where a comparison literal lands against a column type, under the
/// comparison rule (`typesComparable`).
const LiteralPlacement = union(enum) {
    /// The column type holds the literal exactly.
    exact: Value,
    /// The literal falls strictly between two neighbouring column values.
    between: struct { lo: Value, hi: Value },
    /// The literal lies past every value the column type can hold.
    beyond: Side,
    /// Text that doesn't parse as the column's number: the comparison is
    /// NULL (StarRocks: `'12abc' = 12` is NULL).
    null_text,
    /// Text that doesn't read as a date or datetime against a DATE or
    /// DATETIME column. A `from_statement` constant fails the statement, as
    /// MySQL raises `Incorrect DATE value`; anything else never matches.
    not_temporal,
    /// A number against a text column: each row's text is read as a number,
    /// so no one value of the column's type stands for the literal.
    parse_rows,
    incomparable,
};

const Side = enum { above, below };

fn sideOf(negative: bool) Side {
    return if (negative) .below else .above;
}

fn placeLiteral(val: Value, col_type: types.Type) LiteralPlacement {
    // The literal meets JSON as MySQL converts it (`valueJsonOperand`).
    if (col_type == .json) return if (valueJsonOperand(val) != null) .{ .exact = val } else .incomparable;
    const kind = comparisonKind(col_type);
    // Text meets a temporal column only as `textMicros` reads it:
    // `coerceValue` takes a date prefix, so it would read
    // '2026-09-26 25:00:00' as a valid date.
    if (val != .text or kind != .temporal) {
        var exact = val;
        if (coerceValue(&exact, col_type)) |_| return .{ .exact = exact } else |_| {}
    }
    if (kind == .temporal) if (numberDigits(val)) |n| return placeTemporalNumber(n, col_type);
    const lit: Scalar = switch (val) {
        .text => |t| textScalar(t, col_type) orelse return if (kind == .temporal) .not_temporal else .null_text,
        // No scale to place it by: only a decimal column (coerceValue) takes it.
        .decimal64, .decimal128 => return .incomparable,
        // A number column meets a DATE or DATETIME as its number, as MySQL
        // compares them.
        .date => |days| if (kind == .number) .{ .integer = scalar_fn_common.dateNumber(days) } else valueScalar(val, 0),
        .datetime => |micros| if (kind == .number) .{ .decimal = scalar_fn_common.datetimeNumber(micros) } else valueScalar(val, 0),
        else => valueScalar(val, 0),
    };
    if (!scalarComparableTo(lit, col_type)) {
        const number = lit == .integer or lit == .float;
        return if (number and kind == .text) .parse_rows else .incomparable;
    }
    return switch (col_type) {
        .tinyint, .smallint, .int, .bigint, .largeint => placeOnGrid(lit, 0, col_type),
        .decimal64, .decimal128 => |spec| placeOnGrid(lit, spec.s, col_type),
        .float => .{ .exact = .{ .float = @floatCast(scalarF64(lit) orelse return .incomparable) } },
        .double => .{ .exact = .{ .double = scalarF64(lit) orelse return .incomparable } },
        .date => placeOnDays(lit),
        .datetime => .{ .exact = .{ .datetime = lit.micros } },
        .varchar, .string, .char, .json => .{ .exact = .{ .text = lit.text } },
        .boolean, .uuid => .incomparable,
    };
}

/// A number literal's exact digits; null for any other value, and for a
/// decimal, which carries no scale here.
fn numberDigits(val: Value) ?ScaledInt {
    return switch (valueScalar(val, 0)) {
        .integer => |v| .{ .m = v, .s = 0 },
        .float => |v| scalar_fn_common.floatDigits(v),
        .decimal, .micros, .text, .uuid => null,
    };
}

/// A number against a DATE or DATETIME column, as MySQL compares them. A
/// number that reads as datetime fields (`numberDatetimeFields`) is that
/// value, a DATE taking its day; fields no day has (`20260900`) lie between
/// the latest value before them and the next. Any other number compares with
/// each row's own number (YYYYMMDD, YYYYMMDDhhmmss), which orders as the rows
/// do, so it too lands between two values.
fn placeTemporalNumber(n: ScaledInt, col_type: types.Type) LiteralPlacement {
    if (scalar_fn_time.numberDatetimeFields(n)) |f| {
        const clock = (@as(i64, f.hour) * 3600 + f.minute * 60 + f.second) * std.time.us_per_s + f.micros;
        return placeFields(.{ f.year, f.month, f.day }, col_type, clock);
    }
    const whole = @divFloor(n.m, decimal_pow10(n.s));
    const clock_digits: i128 = if (col_type == .datetime) 1_000_000 else 1;
    const ymd = @divFloor(whole, clock_digits);
    if (ymd < 10101) return .{ .beyond = .below };
    if (ymd > 99991231) return .{ .beyond = .above };
    const hms: i64 = @intCast(@mod(whole, clock_digits));
    const hour = @divFloor(hms, 10_000);
    const minute = @mod(@divFloor(hms, 100), 100);
    const second = @mod(hms, 100);
    const clock_past_range = hour > 23 or minute > 59 or second > 59;
    // A time of day past its range lies after the day's last second.
    const clock: i64 = if (clock_past_range)
        std.time.us_per_day - std.time.us_per_s
    else
        (hour * 3600 + minute * 60 + second) * std.time.us_per_s;
    const year: i32 = @intCast(@divFloor(ymd, 10000));
    const date_fields: struct { i32, u32, u32 } = .{ year, @intCast(@mod(@divFloor(ymd, 100), 100)), @intCast(@mod(ymd, 100)) };
    const placed = placeFields(date_fields, col_type, clock);
    if (!clock_past_range or placed != .exact) return placed;
    const last_second = placed.exact.datetime;
    return .{ .between = .{ .lo = .{ .datetime = last_second + std.time.us_per_s - 1 }, .hi = .{ .datetime = last_second + std.time.us_per_s } } };
}

/// A year, month and day against a DATE or DATETIME column, at `clock`
/// microseconds into the day. Fields no day has lie after the latest value
/// before them: a zero month after the year before, a month past 12 after
/// its year, a zero day after the month before, and a day past its month's
/// end after that month.
fn placeFields(f: struct { i32, u32, u32 }, col_type: types.Type, clock: i64) LiteralPlacement {
    const us_per_day = std.time.us_per_day;
    const year, const month, const day = f;
    if (scalar_fn_common.validDate(year, month, day)) {
        const days = scalar_fn_common.ymdToDays(year, month, day);
        if (col_type == .date) return .{ .exact = .{ .date = days } };
        return .{ .exact = .{ .datetime = @as(i64, days) * us_per_day + clock } };
    }
    const before: struct { i32, u32, u32 } = if (month == 0 or (month == 1 and day == 0))
        .{ year - 1, 12, 31 }
    else if (month > 12)
        .{ year, 12, 31 }
    else if (day == 0)
        .{ year, month - 1, scalar_fn_common.lastDayOfMonth(year, month - 1) }
    else
        .{ year, month, scalar_fn_common.lastDayOfMonth(year, month) };
    if (before[0] < 0) return .{ .beyond = .below };
    const lo_day = scalar_fn_common.ymdToDays(before[0], before[1], before[2]);
    if (col_type == .date) return .{ .between = .{ .lo = .{ .date = lo_day }, .hi = .{ .date = lo_day + 1 } } };
    const next_midnight = (@as(i64, lo_day) + 1) * us_per_day;
    return .{ .between = .{ .lo = .{ .datetime = next_midnight - 1 }, .hi = .{ .datetime = next_midnight } } };
}

/// Text compared against a column: it meets a number or a temporal by
/// parsing; null when it doesn't parse.
fn textScalar(text: []const u8, col_type: types.Type) ?Scalar {
    return switch (comparisonKind(col_type)) {
        .number => textNumber(text),
        .temporal => textMicros(text),
        .text, .uuid => .{ .text = text },
    };
}

fn scalarComparableTo(lit: Scalar, col_type: types.Type) bool {
    return switch (comparisonKind(col_type)) {
        .number => lit == .integer or lit == .float or lit == .decimal,
        .temporal => lit == .micros,
        .text => lit == .text,
        .uuid => false,
    };
}

/// A number against an integer (`scale` 0) or decimal column: exact when it
/// lands on the column's grid of 10^-scale steps, else the two grid points
/// around it.
/// A double lands by its shortest digits (`floatDigits`), as it converts.
fn placeOnGrid(lit: Scalar, scale: u8, col_type: types.Type) LiteralPlacement {
    const d: ScaledInt = switch (lit) {
        .integer => |v| .{ .m = v, .s = 0 },
        .decimal => |d| d,
        .float => |v| scalar_fn_common.floatDigits(v) orelse
            return if (std.math.isFinite(v)) .{ .beyond = sideOf(v < 0) } else .incomparable,
        .micros, .text, .uuid => return .incomparable,
    };
    const steps: struct { lo: i128, hi: i128 } = blk: {
        if (d.s <= scale) {
            const m = mulPow10(d.m, scale - d.s) orelse return .{ .beyond = sideOf(d.m < 0) };
            break :blk .{ .lo = m, .hi = m };
        }
        const step = decimal_pow10(d.s - scale);
        const lo = @divFloor(d.m, step);
        break :blk .{ .lo = lo, .hi = if (lo * step == d.m) lo else lo + 1 };
    };
    // Past the type's range on one side only when `lo` is its maximum or
    // `hi` its minimum: then the literal is still beyond every column value.
    const lo = gridValue(steps.lo, col_type) orelse return .{ .beyond = sideOf(steps.hi <= 0) };
    const hi = gridValue(steps.hi, col_type) orelse return .{ .beyond = .above };
    if (steps.lo == steps.hi) return .{ .exact = lo };
    return .{ .between = .{ .lo = lo, .hi = hi } };
}

fn gridValue(m: i128, col_type: types.Type) ?Value {
    return switch (col_type) {
        .tinyint => .{ .tinyint = std.math.cast(i8, m) orelse return null },
        .smallint => .{ .smallint = std.math.cast(i16, m) orelse return null },
        .int => .{ .int = std.math.cast(i32, m) orelse return null },
        .bigint => .{ .bigint = std.math.cast(i64, m) orelse return null },
        .largeint => .{ .largeint = m },
        .decimal64 => .{ .decimal64 = std.math.cast(i64, m) orelse return null },
        .decimal128 => .{ .decimal128 = m },
        else => null,
    };
}

/// A datetime against a DATE column: the date is midnight, so a time of day
/// puts the literal between that day and the next.
fn placeOnDays(lit: Scalar) LiteralPlacement {
    const lo_day = @divFloor(lit.micros, std.time.us_per_day);
    const lo = std.math.cast(i32, lo_day) orelse return .incomparable;
    if (lo_day * std.time.us_per_day == lit.micros) return .{ .exact = .{ .date = lo } };
    const hi = std.math.cast(i32, lo_day + 1) orelse return .incomparable;
    return .{ .between = .{ .lo = .{ .date = lo }, .hi = .{ .date = hi } } };
}

fn mulPow10(m: i128, n: u8) ?i128 {
    if (n > 38) return null;
    return std.math.mul(i128, m, decimal_pow10(n)) catch null;
}

/// `col op x` where x lies strictly between the neighbouring column values
/// `lo` and `hi`: `=` never matches, `<>` matches every non-NULL row, and a
/// range keeps its meaning against the bound on its side — MySQL semantics:
/// `x < 2.5` ⇔ `x < 3`, `x <= 2.5` ⇔ `x <= 2`, `d < '2024-03-05 10:00'` ⇔
/// `d < '2024-03-06'`.
fn foldBetween(expr: *PredicateExpr, lo: Value, hi: Value) void {
    const p = expr.leaf;
    expr.* = switch (p.op) {
        .eq => .{ .always = false },
        .neq => .{ .is_not_null = p.col },
        .lt, .gte => .{ .leaf = .{ .col = p.col, .op = p.op, .val = hi } },
        .lte, .gt => .{ .leaf = .{ .col = p.col, .op = p.op, .val = lo } },
    };
}

/// `col op x` where x lies past every value of the column's type: `=`
/// never matches, and every other operator matches either every non-NULL
/// row or none (`smallint_col < 100000` holds for every smallint).
fn foldBeyond(expr: *PredicateExpr, side: Side) void {
    const p = expr.leaf;
    const matches_all = switch (p.op) {
        .eq => false,
        .neq => true,
        .lt, .lte => side == .above,
        .gt, .gte => side == .below,
    };
    expr.* = if (matches_all) .{ .is_not_null = p.col } else .{ .always = false };
}

/// Pub: the keyed-access bloom gate (api/comparison.appendPredicateValueBytes)
/// must coerce literals identically to predicate evaluation, or a single
/// text-vs-DATE key column silently disables bloom pruning for the statement.
/// THE literal-coercion entry: adopt `target`'s type when the value is
/// losslessly representable there (int-family widening/narrow-with-fit,
/// int/float → decimal at the target's scale, text → temporal by parsing,
/// float → double). Callers that can tolerate a non-coercible literal
/// (a writer that handles the raw tag) `catch` and keep the original.
pub fn coerceValue(val: *Value, target: types.Type) error{NoWidening}!void {
    return coerceValueMode(val, target, .exact);
}

/// Result-unification coercion (CASE branches, LAG defaults): a fractional
/// literal ROUNDS to the decimal target's scale, as the MySQL cast does.
/// Comparisons must not use this — `dc = 10.499` would silently match 10.50.
pub fn coerceValueRounded(val: *Value, target: types.Type) error{NoWidening}!void {
    return coerceValueMode(val, target, .round);
}

const DecimalFit = enum { exact, round };

fn coerceValueMode(val: *Value, target: types.Type, fit: DecimalFit) error{NoWidening}!void {
    if (target.decimalSpec()) |spec| return coerceLiteralToDecimal(val, spec, target == .decimal128, fit);
    return tryWidenLiteral(val, ValueTag.fromType(target));
}

pub fn tryWidenLiteral(val: *Value, target: ValueTag) error{NoWidening}!void {
    // Text literal compared against a temporal column: parse it.
    if (val.* == .text) {
        switch (target) {
            .date => {
                // A datetime string equals a DATE only at midnight; any other
                // time of day lies between two dates (placeLiteral folds it).
                const d = if (scalar_fn_common.parseDateTimeString(val.text)) |us| blk: {
                    if (@mod(us, std.time.us_per_day) != 0) return error.NoWidening;
                    break :blk std.math.cast(i32, @divFloor(us, std.time.us_per_day)) orelse return error.NoWidening;
                } else |_| scalar_fn_common.parseDateString(val.text) catch return error.NoWidening;
                val.* = .{ .date = d };
                return;
            },
            .datetime => {
                const dt = scalar_fn_common.parseDateTimeString(val.text) catch return error.NoWidening;
                val.* = .{ .datetime = dt };
                return;
            },
            else => return error.NoWidening,
        }
    }

    if (val.* == .date and target == .datetime) {
        const days = val.date;
        val.* = .{ .datetime = @as(i64, days) * std.time.us_per_day };
        return;
    }

    // Integer-family literal → integer-family / boolean column. Widening
    // is always safe; narrowing is gated on the value fitting the target.
    if (val.* == .float and target == .double) {
        const f = val.float;
        val.* = .{ .double = f };
        return;
    }
    const iv: i128 = switch (val.*) {
        .tinyint => |v| v,
        .smallint => |v| v,
        .int => |v| v,
        .bigint => |v| v,
        .largeint => |v| v,
        .boolean => |v| @intFromBool(v),
        // A whole-valued float literal adopts an integer target exactly
        // (`x = 2.0` on an INT column). Fractional values stay NoWidening —
        // comparison folding (placeLiteral) handles those.
        .float => |v| blk: {
            const f: f64 = v;
            if (!std.math.isFinite(f) or @trunc(f) != f or @abs(f) >= 1.7e38) return error.NoWidening;
            break :blk @intFromFloat(f);
        },
        .double => |v| blk: {
            if (!std.math.isFinite(v) or @trunc(v) != v or @abs(v) >= 1.7e38) return error.NoWidening;
            break :blk @intFromFloat(v);
        },
        else => return error.NoWidening,
    };
    switch (target) {
        .tinyint => val.* = .{ .tinyint = fitInt(i8, iv) catch return error.NoWidening },
        .smallint => val.* = .{ .smallint = fitInt(i16, iv) catch return error.NoWidening },
        .int => val.* = .{ .int = fitInt(i32, iv) catch return error.NoWidening },
        .bigint => val.* = .{ .bigint = fitInt(i64, iv) catch return error.NoWidening },
        .largeint => val.* = .{ .largeint = iv },
        .float => {
            if (iv < -(@as(i128, 1) << 24) or iv > (@as(i128, 1) << 24)) return error.NoWidening;
            val.* = .{ .float = @floatFromInt(iv) };
        },
        .double => {
            if (iv < -(@as(i128, 1) << 53) or iv > (@as(i128, 1) << 53)) return error.NoWidening;
            val.* = .{ .double = @floatFromInt(iv) };
        },
        .boolean => {
            if (iv != 0 and iv != 1) return error.NoWidening;
            val.* = .{ .boolean = iv == 1 };
        },
        else => return error.NoWidening,
    }
}

fn fitInt(comptime T: type, v: i128) error{OutOfRange}!T {
    if (v < std.math.minInt(T) or v > std.math.maxInt(T)) return error.OutOfRange;
    return @intCast(v);
}

/// Scale a numeric/text literal to a decimal column's mantissa (value × 10^s),
/// so the comparison kernel — which compares raw mantissas — sees both operands
/// at the same scale. A literal beyond the column precision still coerces (the
/// comparison just resolves to a constant); only an i128 overflow is rejected.
fn coerceLiteralToDecimal(val: *Value, spec: types.DecimalSpec, is128: bool, fit: DecimalFit) error{NoWidening}!void {
    const m: i128 = switch (val.*) {
        .tinyint => |v| try intToDecimalMantissa(v, spec.s),
        .smallint => |v| try intToDecimalMantissa(v, spec.s),
        .int => |v| try intToDecimalMantissa(v, spec.s),
        .bigint => |v| try intToDecimalMantissa(v, spec.s),
        .largeint => |v| try intToDecimalMantissa(v, spec.s),
        .boolean => |v| try intToDecimalMantissa(@intFromBool(v), spec.s),
        .float => |v| try floatToDecimalMantissa(v, spec.s, fit),
        .double => |v| try floatToDecimalMantissa(v, spec.s, fit),
        .decimal64 => |v| v,
        .decimal128 => |v| v,
        else => return error.NoWidening,
    };
    if (is128) {
        val.* = .{ .decimal128 = m };
    } else {
        val.* = .{ .decimal64 = std.math.cast(i64, m) orelse return error.NoWidening };
    }
}

fn intToDecimalMantissa(iv: i128, scale: u8) error{NoWidening}!i128 {
    var m = iv;
    var i: u8 = 0;
    while (i < scale) : (i += 1) m = std.math.mul(i128, m, 10) catch return error.NoWidening;
    return m;
}

/// A double as a decimal column's mantissa, by its shortest digits
/// (`floatDigits`): `.exact` only when no digit falls past the scale.
fn floatToDecimalMantissa(v: f64, scale: u8, fit: DecimalFit) error{NoWidening}!i128 {
    const d = scalar_fn_common.floatDigits(v) orelse return error.NoWidening;
    if (fit == .exact and d.s > scale and @rem(d.m, decimal_pow10(d.s - scale)) != 0) return error.NoWidening;
    return decimal_rescale(d.m, d.s, scale) orelse error.NoWidening;
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

/// SQL LIKE matcher: `%` (zero-or-more), `_` (one) and backslash escapes, as
/// `util/like.zig` defines them. Convenience wrapper that compiles + matches
/// in one shot; loops over many rows should `compileLike`
/// once and reuse the plan (see `evaluateLikeMask`).
pub fn likeMatch(text: []const u8, pattern: []const u8) bool {
    return compileLike(pattern).match(text);
}

const max_like_segments = 16;

/// A LIKE pattern compiled once for fast per-row matching. With no `_`, the
/// pattern is a sequence of literal segments separated by `%` (zero-or-more),
/// so it matches via ordered substring search (`std.mem.indexOfPos`, an
/// optimized scan) — anchored at an end only when the pattern doesn't start /
/// end with `%`. This subsumes the common shapes (`lit`, `lit%`, `%lit`,
/// `%lit%`, `%a%b%`). Patterns with `_` or an escape, or more than
/// `max_like_segments` literal pieces, fall back to the backtracking matcher.
pub const LikePlan = struct {
    general: bool,
    pattern: []const u8,
    empty: bool = false,
    anchored_start: bool = false,
    anchored_end: bool = false,
    nseg: usize = 0,
    segs: [max_like_segments][]const u8 = undefined,

    pub fn match(self: *const LikePlan, text: []const u8) bool {
        if (self.general) return like_pattern.match(text, self.pattern);
        if (self.empty) return text.len == 0;
        if (self.nseg == 0) return true; // pattern is all `%` → matches anything
        var pos: usize = 0;
        var i: usize = 0;
        while (i < self.nseg) : (i += 1) {
            const seg = self.segs[i];
            const is_first = i == 0;
            const is_last = i == self.nseg - 1;
            if (is_first and self.anchored_start) {
                if (text.len < seg.len or !std.mem.eql(u8, text[0..seg.len], seg)) return false;
                pos = seg.len;
                if (is_last and self.anchored_end) return pos == text.len;
            } else if (is_last and self.anchored_end) {
                if (text.len < seg.len) return false;
                const start = text.len - seg.len;
                if (start < pos or !std.mem.eql(u8, text[start..], seg)) return false;
                pos = text.len;
            } else {
                const found = findSubstring(text, pos, seg) orelse return false;
                pos = found + seg.len;
            }
        }
        return true;
    }
};

/// Classify a LIKE pattern into a `LikePlan`. No allocation — segments are
/// slices into `pattern`, which outlives the plan.
pub fn compileLike(pattern: []const u8) LikePlan {
    if (like_pattern.needsGeneralMatch(pattern)) return .{ .general = true, .pattern = pattern };
    if (pattern.len == 0) return .{ .general = false, .pattern = pattern, .empty = true };
    var plan: LikePlan = .{
        .general = false,
        .pattern = pattern,
        .anchored_start = pattern[0] != '%',
        .anchored_end = pattern[pattern.len - 1] != '%',
    };
    var it = std.mem.splitScalar(u8, pattern, '%');
    while (it.next()) |s| {
        if (s.len == 0) continue;
        if (plan.nseg == max_like_segments) return .{ .general = true, .pattern = pattern };
        plan.segs[plan.nseg] = s;
        plan.nseg += 1;
    }
    return plan;
}

/// Find `needle` in `haystack` at or after `start`. Seeds on the first byte
/// via `indexOfScalarPos` (a vectorized memchr) and confirms the rest with
/// `eql` — no per-call skip-table setup, which matters when this runs once per
/// row over millions of rows. Returns the match offset, or null.
fn findSubstring(haystack: []const u8, start: usize, needle: []const u8) ?usize {
    if (needle.len == 0) return start;
    if (needle.len > haystack.len) return null;
    if (needle.len == 1) return std.mem.indexOfScalarPos(u8, haystack, start, needle[0]);
    const last = haystack.len - needle.len;
    var i = start;
    while (i <= last) {
        const p = std.mem.indexOfScalarPos(u8, haystack, i, needle[0]) orelse return null;
        if (p > last) return null;
        if (std.mem.eql(u8, haystack[p + 1 ..][0 .. needle.len - 1], needle[1..])) return p;
        i = p + 1;
    }
    return null;
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
        },
        .day_leaf => |p| {
            const col_idx = findCol(schema, p.col) orelse return Error.ColumnNotFound;
            try evaluateDayMask(batch.values[col_idx], p, batch.row_count, out);
        },
        .leaf_col_col => |lc| {
            const li = findCol(schema, lc.left) orelse return Error.ColumnNotFound;
            const ri = findCol(schema, lc.right) orelse return Error.ColumnNotFound;
            evaluateColColMask(batch.values[li], schema[li].type, batch.values[ri], schema[ri].type, lc.op, batch.row_count, out);
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
            try evaluateLikeColumn(allocator, batch.values[col_idx], schema[col_idx].type, lp.pattern, batch.row_count, out, null);
        },
        .@"and" => |children| {
            if (children.len == 0) {
                @memset(out, true);
                return;
            }
            if (children.len > IN_SET_LINEAR_MAX) return evaluateWideJunction(allocator, .@"and", children, schema, batch, out);
            try evaluatePredicate(allocator, children[0], schema, batch, out);
            if (children.len == 1) return;
            const scratch = try allocator.alloc(bool, out.len);
            defer allocator.free(scratch);
            for (children[1..]) |child| {
                try evaluatePredicate(allocator, child, schema, batch, scratch);
                simd.combineMaskInto(.@"and", out, scratch);
            }
        },
        .@"or" => |children| {
            if (children.len == 0) {
                @memset(out, false);
                return;
            }
            if (children.len > IN_SET_LINEAR_MAX) return evaluateWideJunction(allocator, .@"or", children, schema, batch, out);
            try evaluatePredicate(allocator, children[0], schema, batch, out);
            if (children.len == 1) return;
            const scratch = try allocator.alloc(bool, out.len);
            defer allocator.free(scratch);
            for (children[1..]) |child| {
                try evaluatePredicate(allocator, child, schema, batch, scratch);
                simd.combineMaskInto(.@"or", out, scratch);
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
            try evaluateInSetMask(allocator, batch.values[col_idx], s.values, s.negate, batch.row_count, out);
        },
        .text_as_number => |p| {
            const col_idx = findCol(schema, p.col) orelse return Error.ColumnNotFound;
            evaluateTextAsNumberMask(batch.values[col_idx], schema[col_idx].type, p, batch.row_count, out);
        },
        .text_as_number_set => |s| {
            const col_idx = findCol(schema, s.col) orelse return Error.ColumnNotFound;
            evaluateTextAsNumberSetMask(batch.values[col_idx], schema[col_idx].type, s, batch.row_count, out);
        },
        .correlated_set => |s| try evaluateCorrelatedSetMask(s, schema, batch, out, null),
        .correlated_scalar => |s| try evaluateCorrelatedScalarMask(s, schema, batch, out, null),
        .correlated_range => |s| try evaluateCorrelatedRangeMask(s, schema, batch, out, null),
        .leaf_var => return Error.PredicateTypeMismatch,
        .unknown => @memset(out, false),
    }
}

/// Mask-guided predicate evaluation. Identical results to `evaluatePredicate`
/// but threads an `active` set through conjunctions so expensive leaves (LIKE)
/// skip rows an earlier conjunct already eliminated. `active`, when non-null,
/// marks rows still worth testing; inactive rows in `out` are don't-care (the
/// caller's AND masks them off). Shared by `Filter` and the scan-side in-place
/// filter so both get the same short-circuit behaviour.
pub fn evaluateExprGuided(
    allocator: std.mem.Allocator,
    expr: PredicateExpr,
    schema: []const Column,
    batch: anytype,
    out: []bool,
    active: ?[]const bool,
) anyerror!void {
    switch (expr) {
        .leaf => |p| {
            const col_idx = findCol(schema, p.col) orelse return Error.ColumnNotFound;
            try evaluateMaskWithPred(batch.values[col_idx], p, batch.row_count, out);
        },
        .day_leaf => |p| {
            const col_idx = findCol(schema, p.col) orelse return Error.ColumnNotFound;
            try evaluateDayMask(batch.values[col_idx], p, batch.row_count, out);
        },
        .leaf_col_col => |lc| {
            const li = findCol(schema, lc.left) orelse return Error.ColumnNotFound;
            const ri = findCol(schema, lc.right) orelse return Error.ColumnNotFound;
            evaluateColColMask(batch.values[li], schema[li].type, batch.values[ri], schema[ri].type, lc.op, batch.row_count, out);
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
            try evaluateLikeColumn(allocator, batch.values[col_idx], schema[col_idx].type, lp.pattern, batch.row_count, out, active);
        },
        .@"and" => |children| {
            if (children.len == 0) {
                @memset(out, true);
                return;
            }
            if (children.len > IN_SET_LINEAR_MAX) return evaluateWideJunctionGuided(allocator, .@"and", children, schema, batch, out, active);
            try evaluateExprGuided(allocator, children[0], schema, batch, out, active);
            if (children.len == 1) return;
            const scratch = try allocator.alloc(bool, out.len);
            defer allocator.free(scratch);
            for (children[1..]) |child| {
                try evaluateExprGuided(allocator, child, schema, batch, scratch, out);
                simd.combineMaskInto(.@"and", out, scratch);
            }
        },
        .@"or" => |children| {
            if (children.len == 0) {
                @memset(out, false);
                return;
            }
            if (children.len > IN_SET_LINEAR_MAX) return evaluateWideJunctionGuided(allocator, .@"or", children, schema, batch, out, active);
            try evaluateExprGuided(allocator, children[0], schema, batch, out, active);
            if (children.len == 1) return;
            const scratch = try allocator.alloc(bool, out.len);
            defer allocator.free(scratch);
            // Not-yet-true active mask: a row already TRUE in `out` needn't be
            // evaluated by later disjuncts, so the expensive ones (LIKE/regex)
            // skip it. Cheap kernels ignore `active` and recompute, but OR-ing a
            // value into an already-true row leaves it true (idempotent) — so a
            // row true after one disjunct stays true regardless of order.
            const still_open = try allocator.alloc(bool, out.len);
            defer allocator.free(still_open);
            for (children[1..]) |child| {
                simd.andNotMaskInto(still_open, active, out);
                try evaluateExprGuided(allocator, child, schema, batch, scratch, still_open);
                simd.combineMaskInto(.@"or", out, scratch);
            }
        },
        .not => |child| {
            try evaluateExprGuided(allocator, child.*, schema, batch, out, active);
            for (out) |*o| o.* = !o.*;
        },
        .scalar_subquery, .exists_subquery, .in_subquery => return Error.PredicateTypeMismatch,
        .always => |b| @memset(out, b),
        .in_set => |s| {
            const col_idx = findCol(schema, s.col) orelse return Error.ColumnNotFound;
            try evaluateInSetMask(allocator, batch.values[col_idx], s.values, s.negate, batch.row_count, out);
        },
        .text_as_number => |p| {
            const col_idx = findCol(schema, p.col) orelse return Error.ColumnNotFound;
            evaluateTextAsNumberMask(batch.values[col_idx], schema[col_idx].type, p, batch.row_count, out);
        },
        .text_as_number_set => |s| {
            const col_idx = findCol(schema, s.col) orelse return Error.ColumnNotFound;
            evaluateTextAsNumberSetMask(batch.values[col_idx], schema[col_idx].type, s, batch.row_count, out);
        },
        .correlated_set => |s| try evaluateCorrelatedSetMask(s, schema, batch, out, active),
        .correlated_scalar => |s| try evaluateCorrelatedScalarMask(s, schema, batch, out, active),
        .correlated_range => |s| try evaluateCorrelatedRangeMask(s, schema, batch, out, active),
        .leaf_var => return Error.PredicateTypeMismatch,
        .unknown => @memset(out, false),
    }
}

/// Per-row: build key from outer_keys, look up matching CorrelatedScalarRow,
/// then compare outer_compared op row.value. A key that matched no row, NULL
/// included, compares with `missing`, and fails without one. A row `active`
/// excludes isn't looked up and fails. A key matching two rows is an
/// `UnsupportedCorrelatedSubquery`: inner keys that differ but come to one
/// outer value (text `'07'` and `'7'` against a number) split that value's
/// inner rows between two aggregates, and neither is the subquery's value.
pub fn evaluateCorrelatedScalarMask(s: CorrelatedScalar, schema: []const Column, batch: anytype, out: []bool, active: ?[]const bool) !void {
    const n_keys = s.outer_keys.len;
    var key_idx_buf: [16]usize = undefined;
    if (n_keys > key_idx_buf.len) return Error.PredicateTypeMismatch;
    const key_idxs = key_idx_buf[0..n_keys];
    for (s.outer_keys, key_idxs) |c_name, *idx_out| {
        idx_out.* = findCol(schema, c_name) orelse return Error.ColumnNotFound;
    }
    const cmp_idx = findCol(schema, s.outer_compared) orelse return Error.ColumnNotFound;
    const cmp_view = batch.values[cmp_idx];
    const cmp_type = schema[cmp_idx].type;

    var i: usize = 0;
    while (i < batch.row_count) : (i += 1) {
        if (active) |a| {
            if (!a[i]) {
                out[i] = false;
                continue;
            }
        }
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
        const found = if (any_null) null else findKeyed(CorrelatedScalarRow, s.rows, s.sorted, batch.values, key_idxs, i, 0);
        if (found) |at| {
            if (findKeyed(CorrelatedScalarRow, s.rows, s.sorted, batch.values, key_idxs, i, at + 1) != null) return Error.UnsupportedCorrelatedSubquery;
        }
        const found_value: ?Value = if (found) |at| s.rows[at].value else null;
        if (found_value orelse s.missing) |v| {
            out[i] = orderMatches(scalarOrder(cellScalar(cmp_view, cmp_type, i), valueScalar(v, decimalScale(s.value_type))), s.op);
        } else {
            out[i] = false;
        }
    }
}

fn evaluateDayMask(view: ColumnView, p: Predicate, n: usize, out: []bool) !void {
    if (p.val != .int) return Error.PredicateTypeMismatch;
    const want = p.val.int;
    var i: usize = 0;
    while (i < n) : (i += 1) {
        if (!view.isValid(i)) {
            out[i] = false;
            continue;
        }
        const day: i32 = switch (view.data) {
            .date => |s| scalar_fn_common.daysToYmd(s[i]).day,
            .datetime => |s| scalar_fn_common.daysToYmd(scalar_fn_common.daysFromDatetime(s[i])).day,
            else => return Error.PredicateTypeMismatch,
        };
        out[i] = cmp(i32, day, want, p.op);
    }
}

/// Lexicographic byte comparison of `a` against `b` under `op` (used for string
/// column-vs-literal predicates). NULL handling is the caller's; this is pure
/// ordering of two present values.
fn cmpStr(a: []const u8, b: []const u8, op: PredicateOp) bool {
    return switch (std.mem.order(u8, a, b)) {
        .lt => op == .lt or op == .lte or op == .neq,
        .eq => op == .eq or op == .lte or op == .gte,
        .gt => op == .gt or op == .gte or op == .neq,
    };
}

/// `col = ''` / `col <> ''` from the offset array alone — a row is empty iff its
/// two adjacent offsets are equal, so the (very common) empty-string filter never
/// constructs a byte slice or runs `mem.order` per row. The adjacent-offset
/// compare auto-vectorizes; NULL clearing is the caller's, as for `cmpStr`.
fn emptyStringMask(sv: anytype, want_empty: bool, n: usize, mask: []bool) void {
    const offs = sv.offsets;
    for (0..n) |i| mask[i] = (offs[i + 1] == offs[i]) == want_empty;
}

/// Per-row range-correlation check. For each outer row:
///   1. Build the equi-key tuple from `outer_keys`. NULL in any key → no match.
///   2. Find the groups with that key tuple (`findKeyed`). Inner keys
///      that differ can come to one outer value (text `'07'` and `'7'`
///      against a number), so more than one group can match.
///   3. Within those groups, check whether any inner value satisfies
///      `value op outer_range_value` (and the upper bound, when closed)
///      under the comparison rule, since the inner range column's type
///      needn't be the outer column's.
///   4. Apply `negate` (NOT EXISTS).
///
/// Empty group / no matching group → no inner row matches → EXISTS
/// false, NOT EXISTS true. A row `active` excludes isn't looked up and
/// fails.
pub fn evaluateCorrelatedRangeMask(s: CorrelatedRange, schema: []const Column, batch: anytype, out: []bool, active: ?[]const bool) !void {
    const n_keys = s.outer_keys.len;
    var key_idx_buf: [16]usize = undefined;
    if (n_keys > key_idx_buf.len) return Error.PredicateTypeMismatch;
    const key_idxs = key_idx_buf[0..n_keys];
    for (s.outer_keys, key_idxs) |c_name, *idx_out| {
        idx_out.* = findCol(schema, c_name) orelse return Error.ColumnNotFound;
    }
    const range_idx = findCol(schema, s.outer_range_col) orelse return Error.ColumnNotFound;
    const range_view = batch.values[range_idx];
    const range_upper_idx: ?usize = if (s.outer_range_col_upper) |c|
        findCol(schema, c) orelse return Error.ColumnNotFound
    else
        null;
    const bounds = RangeProbe{
        .op = s.op,
        .op_upper = s.op_upper,
        .value_scale = decimalScale(s.range_type),
        .sorted = rangeValuesOrdered(s.range_type, schema[range_idx].type) and
            (range_upper_idx == null or rangeValuesOrdered(s.range_type, schema[range_upper_idx.?].type)),
    };

    var i: usize = 0;
    while (i < batch.row_count) : (i += 1) {
        if (active) |a| {
            if (!a[i]) {
                out[i] = false;
                continue;
            }
        }
        // NULL on outer range col or any equi key → predicate fails
        // (no inner row can satisfy a NULL comparison).
        if (!range_view.isValid(i)) {
            out[i] = s.negate;
            continue;
        }
        if (range_upper_idx) |ui| {
            if (!batch.values[ui].isValid(i)) {
                out[i] = s.negate;
                continue;
            }
        }
        var any_null = false;
        for (key_idxs) |idx| {
            if (!batch.values[idx].isValid(i)) {
                any_null = true;
                break;
            }
        }
        if (any_null) {
            out[i] = s.negate;
            continue;
        }

        const lower = cellScalar(range_view, schema[range_idx].type, i);
        const upper: ?Scalar = if (range_upper_idx) |ui| cellScalar(batch.values[ui], schema[ui].type, i) else null;
        var exists = false;
        var from: usize = 0;
        while (!exists) {
            const at = findKeyed(CorrelatedRangeGroup, s.groups, s.sorted, batch.values, key_idxs, i, from) orelse break;
            exists = try bounds.anyMatches(s.groups[at].values, lower, upper);
            from = at + 1;
        }
        out[i] = if (s.negate) !exists else exists;
    }
}

/// A group's inner values probed against one outer row's bound(s).
const RangeProbe = struct {
    /// `value op lower`: `.gt`/`.gte`/`.lt`/`.lte` when open, `.gt`/`.gte` when closed.
    op: PredicateOp,
    /// `value op_upper upper`, `.lt` or `.lte`; set iff the range is closed.
    op_upper: ?PredicateOp,
    value_scale: u8,
    /// The values' ascending order is also their order as they compare with
    /// the outer bounds, so the ends answer an open range and a binary search
    /// a closed one. Otherwise every value is compared.
    sorted: bool,

    fn anyMatches(self: RangeProbe, values: []const Value, lower: Scalar, upper: ?Scalar) Error!bool {
        if (values.len == 0) return false;
        if (!self.sorted) {
            for (values) |v| if (try self.satisfies(v, lower, upper)) return true;
            return false;
        }
        if (upper == null) return switch (self.op) {
            .gt, .gte => try self.satisfies(values[values.len - 1], lower, null),
            .lt, .lte => try self.satisfies(values[0], lower, null),
            else => Error.PredicateTypeMismatch,
        };
        if (self.op != .gt and self.op != .gte) return Error.PredicateTypeMismatch;
        var lo: usize = 0;
        var hi: usize = values.len;
        while (lo < hi) {
            const mid = lo + (hi - lo) / 2;
            if (orderMatches(scalarOrder(valueScalar(values[mid], self.value_scale), lower), self.op)) hi = mid else lo = mid + 1;
        }
        return lo < values.len and try self.satisfies(values[lo], lower, upper);
    }

    fn satisfies(self: RangeProbe, v: Value, lower: Scalar, upper: ?Scalar) Error!bool {
        const x = valueScalar(v, self.value_scale);
        if (!orderMatches(scalarOrder(x, lower), self.op)) return false;
        const u = upper orelse return true;
        const op_upper = self.op_upper orelse return Error.PredicateTypeMismatch;
        if (op_upper != .lt and op_upper != .lte) return Error.PredicateTypeMismatch;
        return orderMatches(scalarOrder(x, u), op_upper);
    }
};

/// Whether values of `value_type`, sorted in that type's order, stay sorted
/// as they compare with a column of `outer_type`. Text sorts by bytes but
/// meets a number or a temporal by parsing, so it doesn't.
fn rangeValuesOrdered(value_type: types.Type, outer_type: types.Type) bool {
    return comparisonKind(value_type) != .text or comparisonKind(outer_type) == .text;
}

/// Per-row tuple lookup against a materialized correlated set.
/// Assembles each row's outer-side tuple and looks it up in `rows`
/// (`findKeyed`). NULL in any outer col → the tuple can't match. A row
/// `active` excludes isn't looked up and fails.
pub fn evaluateCorrelatedSetMask(s: CorrelatedSet, schema: []const Column, batch: anytype, out: []bool, active: ?[]const bool) !void {
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
        if (active) |a| {
            if (!a[i]) {
                out[i] = false;
                continue;
            }
        }
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
        const found = findKeyed([]const Value, s.rows, s.sorted, batch.values, col_idxs, i, 0) != null;
        out[i] = if (s.negate) !found else found;
    }
}

/// A materialized row's key tuple; a set's row is its own.
fn rowKey(row: anytype) []const Value {
    return switch (@TypeOf(row)) {
        []const Value => row,
        CorrelatedScalarRow, CorrelatedRangeGroup => row.key,
        else => @compileError("no key tuple on " ++ @typeName(@TypeOf(row))),
    };
}

fn keyTupleOrder(a: []const Value, b: []const Value) std.math.Order {
    for (a, b) |x, y| {
        const order = x.compare(y);
        if (order != .eq) return order;
    }
    return .eq;
}

fn keyLessThan(comptime Row: type) fn (void, Row, Row) bool {
    return struct {
        fn lessThan(_: void, a: Row, b: Row) bool {
            return keyTupleOrder(rowKey(a), rowKey(b)) == .lt;
        }
    }.lessThan;
}

/// Puts materialized correlated rows in key order, equal keys as they came,
/// so a lookup can binary-search them. Each key position holds one type,
/// the inner column's; `validateExpr` checks the order still holds once the
/// keys come to the outer columns' types.
pub fn sortKeyed(comptime Row: type, rows: []Row) void {
    std.mem.sort(Row, rows, {}, keyLessThan(Row));
}

/// Whether `findKeyed` may binary-search the rows: their keys are in key
/// order, and each value has its outer column's type, whose equal values
/// are identical, so the order agrees with `cellMatchesValue`. Floats
/// (NaN, -0.0) and JSON keep the scan, as does a number against a text
/// column, which reads each row's text as a number.
fn keysSearchable(comptime Row: type, rows: []const Row, col_types: []const types.Type) bool {
    for (col_types) |ty| switch (ty) {
        .float, .double, .json => return false,
        else => {},
    };
    for (rows, 0..) |row, at| {
        for (rowKey(row), col_types) |v, ty| {
            if (std.meta.activeTag(v) != ValueTag.fromType(ty)) return false;
        }
        if (at > 0 and keyTupleOrder(rowKey(rows[at - 1]), rowKey(row)) == .gt) return false;
    }
    return true;
}

/// The first of `rows[from..]` whose key equals row `i`'s cells at
/// `key_idxs`: a binary search when `sorted` (`keysSearchable`), so a
/// lookup costs O(log rows), and otherwise a scan. Sorted rows put equal keys
/// side by side, so the next match after one at `at` can only be `at + 1`.
fn findKeyed(comptime Row: type, rows: []const Row, sorted: bool, values: []const ColumnView, key_idxs: []const usize, i: usize, from: usize) ?usize {
    if (sorted) search: {
        const at = if (from == 0) lowerBoundKeyed(Row, rows, values, key_idxs, i) orelse break :search else from;
        if (at >= rows.len) return null;
        const order = cellsKeyOrder(values, key_idxs, i, rowKey(rows[at])) orelse break :search;
        return if (order == .eq) at else null;
    }
    for (rows[from..], from..) |row, at| {
        for (key_idxs, rowKey(row)) |idx, ref| {
            if (!cellMatchesValue(values[idx], i, ref)) break;
        } else return at;
    }
    return null;
}

/// The first position whose key isn't below row `i`'s cells; null when a
/// key value isn't of its cell's type, which only a scan compares.
fn lowerBoundKeyed(comptime Row: type, rows: []const Row, values: []const ColumnView, key_idxs: []const usize, i: usize) ?usize {
    var lo: usize = 0;
    var hi: usize = rows.len;
    while (lo < hi) {
        const mid = lo + (hi - lo) / 2;
        switch (cellsKeyOrder(values, key_idxs, i, rowKey(rows[mid])) orelse return null) {
            .gt => lo = mid + 1,
            .lt, .eq => hi = mid,
        }
    }
    return lo;
}

fn cellsKeyOrder(values: []const ColumnView, key_idxs: []const usize, i: usize, key: []const Value) ?std.math.Order {
    for (key_idxs, key) |idx, ref| {
        const order = cellKeyOrder(values[idx], i, ref) orelse return null;
        if (order != .eq) return order;
    }
    return .eq;
}

/// A cell against a key value of its own type, in `Value.compare` order;
/// null for any other value.
fn cellKeyOrder(view: ColumnView, idx: usize, ref: Value) ?std.math.Order {
    return switch (view.data) {
        inline .int, .bigint, .smallint, .tinyint, .largeint, .date, .datetime, .decimal64, .decimal128, .uuid => |col, tag| {
            if (std.meta.activeTag(ref) != @field(ValueTag, @tagName(tag))) return null;
            return std.math.order(col[idx], @field(ref, @tagName(tag)));
        },
        .boolean => |col| if (ref == .boolean) std.math.order(@intFromBool(col[idx] != 0), @intFromBool(ref.boolean)) else null,
        .varchar, .string, .char => |sv| if (ref == .text) std.mem.order(u8, sv.rowBytes(idx), ref.text) else null,
        .float, .double, .json => null,
    };
}

/// Equality check between a single cell of a ColumnView and a Value of the
/// same type, or a number against a text cell (see `coerceKeyTuple`).
/// Returns false on any other type mismatch (defensive).
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
        .json => |sv| jsonOrder(sv.rowBytes(idx), valueJsonOperand(ref)) == .eq,
        .varchar, .string, .char => |sv| switch (ref) {
            .text => |t| std.mem.eql(u8, sv.rowBytes(idx), t),
            else => textOrder(sv.rowBytes(idx), valueScalar(ref, 0)) == .eq,
        },
    };
}

/// Longest IN list checked by scanning it per row.
pub const IN_SET_LINEAR_MAX = 8;
/// Fewest rows worth building a lookup set for: below this, scanning a long
/// list per row costs about what sorting it would.
const IN_SET_LOOKUP_MIN_ROWS = 32;
/// Widest literal range a numeric IN-list tests through a bitmap (8 KiB).
const IN_SET_BITMAP_SPAN = 1 << 16;

/// An AND or OR with more arms than an IN list scans linearly, which is
/// where `foldSetArms` can fold one: the set lookups first, then each
/// remaining arm. Out of line, leaving the recursive evaluator's few-armed
/// junctions as small as they were.
noinline fn evaluateWideJunction(allocator: std.mem.Allocator, comptime junction: Junction, children: []const PredicateExpr, schema: []const Column, batch: anytype, out: []bool) anyerror!void {
    const fold = try foldSetArms(allocator, junction, children, schema, batch, out);
    defer fold.deinit(allocator);
    var rest = fold.rest;
    if (!fold.folded) {
        try evaluatePredicate(allocator, rest[0], schema, batch, out);
        rest = rest[1..];
    }
    if (rest.len == 0) return;
    const scratch = try allocator.alloc(bool, out.len);
    defer allocator.free(scratch);
    for (rest) |child| {
        try evaluatePredicate(allocator, child, schema, batch, scratch);
        simd.combineMaskInto(junction, out, scratch);
    }
}

/// `evaluateWideJunction` for `evaluateExprGuided`, threading `active`
/// the same way its few-armed AND and OR do.
noinline fn evaluateWideJunctionGuided(allocator: std.mem.Allocator, comptime junction: Junction, children: []const PredicateExpr, schema: []const Column, batch: anytype, out: []bool, active: ?[]const bool) anyerror!void {
    const fold = try foldSetArms(allocator, junction, children, schema, batch, out);
    defer fold.deinit(allocator);
    var rest = fold.rest;
    if (!fold.folded) {
        try evaluateExprGuided(allocator, rest[0], schema, batch, out, active);
        rest = rest[1..];
    }
    if (rest.len == 0) return;
    const scratch = try allocator.alloc(bool, out.len);
    defer allocator.free(scratch);
    switch (junction) {
        .@"and" => for (rest) |child| {
            try evaluateExprGuided(allocator, child, schema, batch, scratch, out);
            simd.combineMaskInto(.@"and", out, scratch);
        },
        .@"or" => {
            const still_open = try allocator.alloc(bool, out.len);
            defer allocator.free(still_open);
            for (rest) |child| {
                simd.andNotMaskInto(still_open, active, out);
                try evaluateExprGuided(allocator, child, schema, batch, scratch, still_open);
                simd.combineMaskInto(.@"or", out, scratch);
            }
        },
    }
}

/// What `foldSetArms` left of an OR's or an AND's arms.
const SetArmFold = struct {
    /// `out` holds the folded set lookups, ORed (OR) or ANDed (AND). False
    /// when nothing folded, `rest` then being every arm.
    folded: bool,
    /// The arms still to evaluate one at a time.
    rest: []const PredicateExpr,
    owned: bool,

    fn deinit(self: SetArmFold, allocator: std.mem.Allocator) void {
        if (self.owned) allocator.free(self.rest);
    }
};

pub const Junction = simd.MaskOp;

/// The leaf operator a literal IN list spells under `junction`: `col IN
/// (a, ...)` parses to an OR of `col = a`, `col NOT IN (a, ...)` to an AND
/// of `col <> a`.
pub fn setArmOp(comptime junction: Junction) PredicateOp {
    return if (junction == .@"or") .eq else .neq;
}

/// An OR's `col = literal` arms (an AND's `col <> literal` arms) grouped by
/// column, each column holding more than IN_SET_LINEAR_MAX of them matched
/// once through the set lookup of `evaluateInSetMask` (negated under AND)
/// rather than one pass per literal. The parser spells a literal
/// `col [NOT] IN (a, b, ...)` as such arms, and `orderPredicate` splices it
/// into any OR (AND) around it, so a long list costs one probe per row
/// wherever it sits. Same answer: validation gave every literal the column's
/// type, which is what the set compares, and a NULL row fails every arm and
/// both set forms.
fn foldSetArms(allocator: std.mem.Allocator, comptime junction: Junction, arms: []const PredicateExpr, schema: []const Column, batch: anytype, out: []bool) !SetArmFold {
    const unfolded: SetArmFold = .{ .folded = false, .rest = arms, .owned = false };
    if (arms.len <= IN_SET_LINEAR_MAX or batch.row_count < IN_SET_LOOKUP_MIN_ROWS) return unfolded;
    const arm_cols = try allocator.alloc(?usize, arms.len);
    defer allocator.free(arm_cols);
    const counts = try allocator.alloc(usize, schema.len);
    defer allocator.free(counts);
    @memset(counts, 0);
    var last_name: ?[]const u8 = null;
    var last_col: ?usize = null;
    for (arms, arm_cols) |arm, *col| {
        col.* = null;
        if (arm != .leaf or arm.leaf.op != setArmOp(junction) or arm.leaf.as_boolean) continue;
        const name = arm.leaf.col;
        // An IN list's arms share one column-name slice.
        const same = if (last_name) |l| l.ptr == name.ptr and l.len == name.len else false;
        if (!same) {
            last_name = name;
            const idx = findCol(schema, name) orelse return Error.ColumnNotFound;
            last_col = switch (batch.values[idx].data) {
                // Float equality (NaN, -0.0) and two-valued booleans keep their leaves.
                .float, .double, .boolean => null,
                else => idx,
            };
        }
        col.* = last_col;
        if (last_col) |idx| counts[idx] += 1;
    }

    const negate = junction == .@"and";
    var folded = false;
    var n_folded: usize = 0;
    var values: std.ArrayListUnmanaged(Value) = .empty;
    defer values.deinit(allocator);
    var scratch: []bool = &.{};
    defer if (scratch.len > 0) allocator.free(scratch);
    for (counts, 0..) |count, idx| {
        if (count <= IN_SET_LINEAR_MAX) continue;
        values.clearRetainingCapacity();
        try values.ensureTotalCapacity(allocator, count);
        for (arms, arm_cols) |arm, col| {
            if (col) |c| if (c == idx) values.appendAssumeCapacity(arm.leaf.val);
        }
        if (!folded) {
            try evaluateInSetMask(allocator, batch.values[idx], values.items, negate, batch.row_count, out);
        } else {
            if (scratch.len == 0) scratch = try allocator.alloc(bool, out.len);
            try evaluateInSetMask(allocator, batch.values[idx], values.items, negate, batch.row_count, scratch);
            simd.combineMaskInto(junction, out, scratch);
        }
        folded = true;
        n_folded += count;
    }
    if (!folded) return unfolded;

    const rest = try allocator.alloc(PredicateExpr, arms.len - n_folded);
    var at: usize = 0;
    for (arms, arm_cols) |arm, col| {
        if (col) |c| if (counts[c] > IN_SET_LINEAR_MAX) continue;
        rest[at] = arm;
        at += 1;
    }
    return .{ .folded = true, .rest = rest, .owned = true };
}

/// Per-row set-membership check. `negate=false` → IN, `true` → NOT IN.
/// Set is guaranteed NULL-free (the resolver drops NULLs at materialization).
/// Two-valued logic: NULL in the column never matches → IN false, NOT IN
/// also false (consistent with the IN side).
///
/// A numeric list is looked up per row (a bitmap over a narrow literal range,
/// else a binary search over a sorted copy) and a long text list through a
/// hash set, so neither a DELETE/UPDATE with thousands of keys (#340) nor a
/// scan with a short list pays a branch per literal per row.
pub fn evaluateInSetMask(allocator: std.mem.Allocator, view: ColumnView, values: []const Value, negate: bool, n: usize, mask: []bool) !void {
    if (n >= IN_SET_LOOKUP_MIN_ROWS) {
        switch (view.data) {
            inline .int, .bigint, .smallint, .tinyint, .largeint, .date, .datetime, .decimal64, .decimal128, .uuid => |col, tag| {
                return evalInNumericSet(allocator, @field(ValueTag, @tagName(tag)), view, col, values, negate, n, mask);
            },
            inline .varchar, .string, .char => |sv| if (values.len > IN_SET_LINEAR_MAX) return evalInTextHashSet(allocator, sv, view, values, negate, n, mask),
            // Float `==` (NaN, -0.0) and two-valued booleans stay on the scan,
            // as does JSON, whose equal values needn't share bytes.
            .float, .double, .boolean, .json => {},
        }
    }
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
        // Validation brought every set value to the column's type.
        else => for (0..n) |i| {
            if (!view.isValid(i)) {
                mask[i] = false;
                continue;
            }
            var found = false;
            for (values) |v| {
                if (cellMatchesValue(view, i, v)) {
                    found = true;
                    break;
                }
            }
            mask[i] = if (negate) !found else found;
        },
    }
}

fn expectTextColumn(schema: []const Column, name: []const u8) Error!void {
    const idx = types.findColumn(schema, name) orelse return Error.ColumnNotFound;
    if (comparisonKind(schema[idx].type) != .text) return Error.PredicateTypeMismatch;
}

/// `.text_as_number`: each row's text read as a number against the literal,
/// or as a BOOLEAN's 1 or 0 for an `as_boolean` leaf.
fn evaluateTextAsNumberMask(view: ColumnView, col_type: types.Type, p: Predicate, n: usize, mask: []bool) void {
    const rhs = valueScalar(p.val, 0);
    for (0..n) |i| {
        if (!view.isValid(i)) {
            mask[i] = false;
            continue;
        }
        const cell = cellScalar(view, col_type, i);
        const order = if (!p.as_boolean) scalarOrder(cell, rhs) else if (scalar_fn_common.textBoolean(cell.text)) |b|
            numberOrder(.{ .integer = @intFromBool(b) }, rhs)
        else
            null;
        mask[i] = orderMatches(order, p.op);
    }
}

/// `.text_as_number_set`, element by element: `a IN (b, c)` is
/// `a = b OR a = c`. IN holds when some value equals the row, NOT IN when
/// every value is known to differ; text that isn't a number leaves each
/// comparison with a number NULL, so it fails both.
fn evaluateTextAsNumberSetMask(view: ColumnView, col_type: types.Type, s: InSet, n: usize, mask: []bool) void {
    for (0..n) |i| {
        if (!view.isValid(i)) {
            mask[i] = false;
            continue;
        }
        const cell = cellScalar(view, col_type, i);
        const number: ?Scalar = if (cell == .text) textNumber(cell.text) else cell;
        var found = false;
        var unknown = false;
        for (s.values) |v| {
            const order = if (v == .text) scalarOrder(cell, valueScalar(v, 0)) else if (number) |x| scalarOrder(x, valueScalar(v, 0)) else null;
            if (order == .eq) {
                found = true;
                break;
            }
            unknown = unknown or order == null;
        }
        mask[i] = if (s.negate) !found and !unknown else found;
    }
}

/// Same matching as the per-row scan: only values of the column's own tag
/// can equal a cell.
fn evalInNumericSet(
    allocator: std.mem.Allocator,
    comptime tag: ValueTag,
    view: ColumnView,
    col: anytype,
    values: []const Value,
    negate: bool,
    n: usize,
    mask: []bool,
) !void {
    const T = std.meta.Elem(@TypeOf(col));
    const buf = try allocator.alloc(T, values.len);
    defer allocator.free(buf);
    var len: usize = 0;
    for (values) |v| {
        if (std.meta.activeTag(v) != tag) continue;
        buf[len] = @field(v, @tagName(tag));
        len += 1;
    }
    const set = buf[0..len];
    if (set.len == 0) {
        for (0..n) |i| mask[i] = negate and view.isValid(i);
        return;
    }
    std.sort.pdq(T, set, {}, std.sort.asc(T));
    const lo = set[0];
    const hi = set[set.len - 1];
    // Offsets from `lo` in the unsigned twin: a value below `lo` wraps past
    // `span`, so one compare bounds both ends.
    const U = std.meta.Int(.unsigned, @bitSizeOf(T));
    const span: U = @bitCast(hi -% lo);
    if (span < IN_SET_BITMAP_SPAN) {
        var bitmap: [IN_SET_BITMAP_SPAN / 8]u8 = undefined;
        const used = bitmap[0 .. @as(usize, @intCast(span / 8)) + 1];
        @memset(used, 0);
        for (set) |v| {
            const off: usize = @intCast(@as(U, @bitCast(v -% lo)));
            used[off / 8] |= @as(u8, 1) << @intCast(off % 8);
        }
        for (0..n) |i| {
            const off: U = @bitCast(col[i] -% lo);
            const at: usize = @intCast(@min(off, span));
            const found = off <= span and (used[at / 8] >> @intCast(at % 8)) & 1 != 0;
            mask[i] = view.isValid(i) and found != negate;
        }
        return;
    }
    for (0..n) |i| {
        const x = col[i];
        const found = x >= lo and x <= hi and sortedContains(T, set, x);
        mask[i] = view.isValid(i) and found != negate;
    }
}

/// `sorted` is ascending and non-empty. `base` ends on the last element
/// ≤ `x` (or the first, when every element is greater).
fn sortedContains(comptime T: type, sorted: []const T, x: T) bool {
    var base: usize = 0;
    var len = sorted.len;
    while (len > 1) {
        const half = len / 2;
        if (sorted[base + half] <= x) base += half;
        len -= half;
    }
    return sorted[base] == x;
}

fn evalInTextHashSet(allocator: std.mem.Allocator, sv: anytype, view: ColumnView, values: []const Value, negate: bool, n: usize, mask: []bool) !void {
    var set: std.StringHashMapUnmanaged(void) = .empty;
    defer set.deinit(allocator);
    try set.ensureTotalCapacity(allocator, @intCast(values.len));
    for (values) |v| if (v == .text) set.putAssumeCapacity(v.text, {});
    for (0..n) |i| mask[i] = view.isValid(i) and set.contains(sv.rowBytes(i)) != negate;
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

/// One side of a comparison in the form the comparison rule works on:
/// integers exact, decimals as (mantissa, scale), DATE and DATETIME as µs
/// since the epoch, text as its bytes.
const Scalar = union(enum) {
    integer: i128,
    float: f64,
    decimal: ScaledInt,
    micros: i64,
    text: []const u8,
    uuid: u128,
};

/// `m × 10^-s`.
const ScaledInt = scalar_fn_common.ScaledInt;

pub const ComparisonKind = enum { number, temporal, text, uuid };

pub fn comparisonKind(ty: types.Type) ComparisonKind {
    return switch (ty) {
        .tinyint, .smallint, .int, .bigint, .largeint, .boolean, .float, .double, .decimal64, .decimal128 => .number,
        .date, .datetime => .temporal,
        .varchar, .string, .char, .json => .text,
        .uuid => .uuid,
    };
}

/// THE comparison rule, as StarRocks and MySQL apply it: numbers compare by
/// value across integer, decimal and float types; a DATE meets a DATETIME at
/// midnight; text meets a number or a temporal by parsing (text that doesn't
/// parse compares as NULL); text against text is bytewise. A number meets a
/// DATE or DATETIME only as a literal or a column pair, as MySQL compares
/// them (`placeTemporalNumber`, `temporalBesideNumber`); a join key, a
/// subquery's column or a correlated key never pairs the two.
pub fn typesComparable(a: types.Type, b: types.Type) bool {
    const ka = comparisonKind(a);
    const kb = comparisonKind(b);
    if (ka == kb) return true;
    if (ka == .uuid or kb == .uuid) return false;
    return ka == .text or kb == .text;
}

fn cellScalar(view: ColumnView, ty: types.Type, i: usize) Scalar {
    const scale = decimalScale(ty);
    return switch (view.data) {
        .tinyint => |s| .{ .integer = s[i] },
        .smallint => |s| .{ .integer = s[i] },
        .int => |s| .{ .integer = s[i] },
        .bigint => |s| .{ .integer = s[i] },
        .largeint => |s| .{ .integer = s[i] },
        .boolean => |s| .{ .integer = s[i] },
        .float => |s| .{ .float = s[i] },
        .double => |s| .{ .float = s[i] },
        .decimal64 => |s| .{ .decimal = .{ .m = s[i], .s = scale } },
        .decimal128 => |s| .{ .decimal = .{ .m = s[i], .s = scale } },
        .date => |s| .{ .micros = @as(i64, s[i]) * std.time.us_per_day },
        .datetime => |s| .{ .micros = s[i] },
        .uuid => |s| .{ .uuid = s[i] },
        .varchar, .string, .char, .json => |sv| .{ .text = sv.rowBytes(i) },
    };
}

/// A value on the other side of a comparison with JSON, converted to JSON as
/// MySQL converts it: text is a JSON string (JSONB, a JSON expression's value,
/// stays that value), a number a JSON number and a boolean a JSON boolean.
/// Null for a value MySQL ranks among JSON's temporal and opaque kinds, which
/// no JSONB value here holds, and for a decimal, which carries no scale here.
fn valueJsonOperand(v: Value) ?json_binary.Operand {
    return switch (v) {
        .text => |t| json_binary.textOperand(t),
        .boolean => |b| .{ .boolean = b },
        .float => |x| .{ .number = .{ .double = x } },
        .double => |x| .{ .number = .{ .double = x } },
        .tinyint => |x| .{ .number = .{ .exact = .{ .m = x, .s = 0 } } },
        .smallint => |x| .{ .number = .{ .exact = .{ .m = x, .s = 0 } } },
        .int => |x| .{ .number = .{ .exact = .{ .m = x, .s = 0 } } },
        .bigint => |x| .{ .number = .{ .exact = .{ .m = x, .s = 0 } } },
        .largeint => |x| .{ .number = .{ .exact = .{ .m = x, .s = 0 } } },
        .decimal64, .decimal128, .date, .datetime, .uuid => null,
    };
}

/// Row `i` of a column compared with JSON (`valueJsonOperand`).
fn cellJsonOperand(view: ColumnView, ty: types.Type, i: usize) ?json_binary.Operand {
    return switch (view.data) {
        .json => |sv| json_binary.textOperand(sv.rowBytes(i)),
        .varchar, .string, .char => |sv| .{ .string = sv.rowBytes(i) },
        .boolean => |s| .{ .boolean = s[i] != 0 },
        .float => |s| .{ .number = .{ .double = s[i] } },
        .double => |s| .{ .number = .{ .double = s[i] } },
        .date, .datetime, .uuid => null,
        .tinyint, .smallint, .int, .bigint, .largeint, .decimal64, .decimal128 => switch (cellScalar(view, ty, i)) {
            .integer => |m| .{ .number = .{ .exact = .{ .m = m, .s = 0 } } },
            .decimal => |d| .{ .number = .{ .exact = d } },
            else => null,
        },
    };
}

/// A JSON cell against a converted value; null when the value has no JSON
/// form to compare by.
fn jsonOrder(cell: []const u8, other: ?json_binary.Operand) ?std.math.Order {
    return json_binary.compareOperands(json_binary.textOperand(cell), other orelse return null);
}

/// A value in comparison form; `decimal_scale` places a decimal mantissa.
fn valueScalar(v: Value, decimal_scale: u8) Scalar {
    return switch (v) {
        .tinyint => |x| .{ .integer = x },
        .smallint => |x| .{ .integer = x },
        .int => |x| .{ .integer = x },
        .bigint => |x| .{ .integer = x },
        .largeint => |x| .{ .integer = x },
        .boolean => |x| .{ .integer = @intFromBool(x) },
        .float => |x| .{ .float = x },
        .double => |x| .{ .float = x },
        .decimal64 => |m| .{ .decimal = .{ .m = m, .s = decimal_scale } },
        .decimal128 => |m| .{ .decimal = .{ .m = m, .s = decimal_scale } },
        .date => |x| .{ .micros = @as(i64, x) * std.time.us_per_day },
        .datetime => |x| .{ .micros = x },
        .uuid => |x| .{ .uuid = x },
        .text => |t| .{ .text = t },
    };
}

fn decimalScale(ty: types.Type) u8 {
    return if (ty.decimalSpec()) |spec| spec.s else 0;
}

/// Order of `a` against `b` under the comparison rule; null when the pair
/// compares as NULL (text that doesn't parse as the other side's kind, a NaN)
/// or isn't comparable.
fn scalarOrder(a: Scalar, b: Scalar) ?std.math.Order {
    if (a == .text and b != .text) return textOrder(a.text, b);
    if (b == .text and a != .text) return if (textOrder(b.text, a)) |o| o.invert() else null;
    return switch (a) {
        .text => |at| std.mem.order(u8, at, b.text),
        .micros => |am| if (b == .micros) std.math.order(am, b.micros) else null,
        .uuid => |au| if (b == .uuid) std.math.order(au, b.uuid) else null,
        .integer, .float, .decimal => numberOrder(a, b),
    };
}

fn textOrder(text: []const u8, other: Scalar) ?std.math.Order {
    return switch (other) {
        .micros => |m| std.math.order((textMicros(text) orelse return null).micros, m),
        .integer, .float, .decimal => numberOrder(textNumber(text) orelse return null, other),
        .text, .uuid => null,
    };
}

fn numberOrder(a: Scalar, b: Scalar) ?std.math.Order {
    if (a == .float or b == .float) {
        const af = scalarF64(a) orelse return null;
        const bf = scalarF64(b) orelse return null;
        if (std.math.isNan(af) or std.math.isNan(bf)) return null;
        return std.math.order(af, bf);
    }
    const ad = scalarDecimal(a) orelse return null;
    const bd = scalarDecimal(b) orelse return null;
    if (ad.s == bd.s) return std.math.order(ad.m, bd.m);
    if (ad.s < bd.s) return rescaledOrder(ad.m, bd.s - ad.s, bd.m);
    return rescaledOrder(bd.m, ad.s - bd.s, ad.m).invert();
}

/// Order of `m × 10^shift` against `other`. A product past i128 lies beyond
/// every value `other` can hold, so the sign of `m` alone decides.
fn rescaledOrder(m: i128, shift: u8, other: i128) std.math.Order {
    const scaled = mulPow10(m, shift) orelse return if (m > 0) .gt else .lt;
    return std.math.order(scaled, other);
}

fn scalarF64(v: Scalar) ?f64 {
    return switch (v) {
        .integer => |i| @floatFromInt(i),
        .float => |f| f,
        .decimal => |d| @as(f64, @floatFromInt(d.m)) / std.math.pow(f64, 10.0, @floatFromInt(d.s)),
        .micros, .text, .uuid => null,
    };
}

fn scalarDecimal(v: Scalar) ?ScaledInt {
    return switch (v) {
        .integer => |i| .{ .m = i, .s = 0 },
        .decimal => |d| d,
        .float, .micros, .text, .uuid => null,
    };
}

/// Text read as a number, the way a CAST reads it.
fn textNumber(raw: []const u8) ?Scalar {
    return switch (scalar_fn_common.textNumber(raw) orelse return null) {
        .exact => |d| .{ .decimal = d },
        .float => |f| .{ .float = f },
    };
}

/// Text as a comparison with a DATE or DATETIME reads it: MySQL's
/// str_to_datetime (`scalar_fn_time.parseDatetime`), so '2026-9-1' and
/// '20260901' are dates and '2026-09-31' or '2026-09-26 25:00:00' are none.
fn textMicros(text: []const u8) ?Scalar {
    return .{ .micros = (scalar_fn_time.parseDatetime(text) orelse return null).value };
}

fn orderMatches(order: ?std.math.Order, op: PredicateOp) bool {
    const o = order orelse return false;
    return switch (op) {
        .eq => o == .eq,
        .neq => o != .eq,
        .lt => o == .lt,
        .lte => o != .gt,
        .gt => o == .gt,
        .gte => o != .lt,
    };
}

/// Both sides store values the comparison can use as they are.
fn sameRepresentation(a: types.Type, b: types.Type) bool {
    if (a.isString() and b.isString()) return true;
    if (std.meta.activeTag(a) != std.meta.activeTag(b)) return false;
    if (a.decimalSpec()) |da| if (b.decimalSpec()) |db| return da.s == db.s;
    return true;
}

/// Per-row col-vs-col comparison under the comparison rule. NULL on either
/// side → mask[i] = false (two-valued logic).
pub fn evaluateColColMask(left: ColumnView, left_type: types.Type, right: ColumnView, right_type: types.Type, op: PredicateOp, n: usize, mask: []bool) void {
    if (left_type == .json or right_type == .json) {
        for (0..n) |i| {
            const l = cellJsonOperand(left, left_type, i);
            const r = cellJsonOperand(right, right_type, i);
            mask[i] = l != null and r != null and orderMatches(json_binary.compareOperands(l.?, r.?), op);
        }
    } else if (sameRepresentation(left_type, right_type)) {
        sameRepresentationMask(left, right, op, n, mask);
    } else if (temporalBesideNumber(left_type, right_type)) {
        for (0..n) |i| mask[i] = orderMatches(numberOrder(cellNumber(left, left_type, i), cellNumber(right, right_type, i)), op);
    } else {
        for (0..n) |i| mask[i] = orderMatches(scalarOrder(cellScalar(left, left_type, i), cellScalar(right, right_type, i)), op);
    }
    clearNullRows(left.nulls, mask[0..n]);
    clearNullRows(right.nulls, mask[0..n]);
}

/// A DATE or DATETIME column against a number column, which MySQL compares
/// by the temporal's number (`cellNumber`). Not a join key pair: the two
/// hash apart.
pub fn temporalBesideNumber(a: types.Type, b: types.Type) bool {
    const ka = comparisonKind(a);
    const kb = comparisonKind(b);
    return (ka == .temporal and kb == .number) or (ka == .number and kb == .temporal);
}

/// Row `i` as a number: a DATE as YYYYMMDD, a DATETIME as
/// YYYYMMDDhhmmss.ffffff.
fn cellNumber(view: ColumnView, ty: types.Type, i: usize) Scalar {
    return switch (view.data) {
        .date => |s| .{ .integer = scalar_fn_common.dateNumber(s[i]) },
        .datetime => |s| .{ .decimal = scalar_fn_common.datetimeNumber(s[i]) },
        else => cellScalar(view, ty, i),
    };
}

fn sameRepresentationMask(left: ColumnView, right: ColumnView, op: PredicateOp, n: usize, mask: []bool) void {
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
        .varchar, .string, .char, .json => |l| {
            const r = scalar_fn_common.stringViewOf(right);
            for (0..n) |i| mask[i] = cmpStr(l.rowBytes(i), r.rowBytes(i), op);
        },
    }
}

/// LIKE over a column of any type with a text form: a number, boolean, DATE
/// or DATETIME matches as the text MySQL writes for it (`12 LIKE '1%'`),
/// converted a batch at a time by the assignment rule.
fn evaluateLikeColumn(allocator: std.mem.Allocator, view: ColumnView, ty: types.Type, pattern: []const u8, n: usize, mask: []bool, active: ?[]const bool) !void {
    if (ty.isString()) return evaluateLikeMask(view, pattern, n, mask, active);
    const text = try cast.assignColumn(allocator, view, ty, .string, n);
    defer cast.freeAssignedColumn(allocator, text);
    try evaluateLikeMask(text, pattern, n, mask, active);
}

/// Per-row LIKE evaluation: matches NULL → false (two-valued logic).
/// Only valid against string-typed columns (validateExpr enforces).
/// `active`, when non-null, marks the rows still worth testing — a row whose
/// `active[i]` is false is already eliminated by an earlier conjunct, so the
/// (expensive) substring match is skipped via `and` short-circuit. Inactive
/// rows are written false; the conjunction AND masks them anyway.
pub fn evaluateLikeMask(view: ColumnView, pattern: []const u8, n: usize, mask: []bool, active: ?[]const bool) !void {
    // Compile the pattern once per batch, then match each row against the plan.
    const plan = compileLike(pattern);
    switch (view.data) {
        .varchar, .string, .char, .json => |sv| {
            if (active) |act| {
                for (0..n) |i| mask[i] = act[i] and view.isValid(i) and plan.match(sv.rowBytes(i));
            } else {
                for (0..n) |i| mask[i] = view.isValid(i) and plan.match(sv.rowBytes(i));
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
        .int => |s| cmpInto(i32, s[0..n], p.val.int, mask[0..n], op),
        .bigint => |s| cmpInto(i64, s[0..n], p.val.bigint, mask[0..n], op),
        .boolean => |s| cmpInto(u8, s[0..n], @intFromBool(p.val.boolean), mask[0..n], op),
        .json => |sv| {
            const want = valueJsonOperand(p.val) orelse return Error.PredicateTypeMismatch;
            for (0..n) |i| mask[i] = orderMatches(json_binary.compareOperands(json_binary.textOperand(sv.rowBytes(i)), want), op);
        },
        .varchar, .char => |sv| {
            if (p.val.text.len == 0 and (op == .eq or op == .neq)) {
                emptyStringMask(sv, op == .eq, n, mask[0..n]);
            } else {
                for (0..n) |i| mask[i] = cmpStr(sv.rowBytes(i), p.val.text, op);
            }
        },
        .string => |sv| {
            if (p.val.text.len == 0 and (op == .eq or op == .neq)) {
                emptyStringMask(sv, op == .eq, n, mask[0..n]);
            } else {
                for (0..n) |i| mask[i] = cmpStr(sv.rowBytes(i), p.val.text, op);
            }
        },
        .float => |s| cmpInto(f32, s[0..n], p.val.float, mask[0..n], op),
        .double => |s| cmpInto(f64, s[0..n], p.val.double, mask[0..n], op),
        .date => |s| cmpInto(i32, s[0..n], p.val.date, mask[0..n], op),
        .datetime => |s| cmpInto(i64, s[0..n], p.val.datetime, mask[0..n], op),
        .tinyint => |s| cmpInto(i8, s[0..n], p.val.tinyint, mask[0..n], op),
        .smallint => |s| cmpInto(i16, s[0..n], p.val.smallint, mask[0..n], op),
        .largeint => |s| cmpInto(i128, s[0..n], p.val.largeint, mask[0..n], op),
        .decimal64 => |s| cmpInto(i64, s[0..n], p.val.decimal64, mask[0..n], op),
        .decimal128 => |s| cmpInto(i128, s[0..n], p.val.decimal128, mask[0..n], op),
        .uuid => |s| cmpInto(u128, s[0..n], p.val.uuid, mask[0..n], op),
    }
    clearNullRows(view.nulls, mask[0..n]);
}

/// Two-valued logic: a NULL value never matches a comparison, so clear the
/// mask of every row whose validity bit is 0. A comparison runs as one SIMD
/// pass, so a per-row bit test would cost more than the compare itself: a
/// 64-row word of the bitmap that is all valid is skipped whole.
fn clearNullRows(nulls: ?[]const u8, mask: []bool) void {
    const bitmap = nulls orelse return;
    var i: usize = 0;
    while (i + 64 <= mask.len) : (i += 64) {
        const word = std.mem.readInt(u64, bitmap[i / 8 ..][0..8], .little);
        if (word == std.math.maxInt(u64)) continue;
        for (mask[i..][0..64], 0..) |*m, bit| {
            if ((word >> @intCast(bit)) & 1 == 0) m.* = false;
        }
    }
    while (i < mask.len) : (i += 1) {
        if (!storage.column.isValidBit(bitmap, i)) mask[i] = false;
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

/// Vectorized leaf comparison: `mask[i] = (data[i] <op> want)`. The runtime
/// `op` is resolved to a comptime `simd.CmpOp` so the vector compare in
/// `simd.compareInto` monomorphizes (the scalar per-row `cmp` with a runtime op
/// switch wouldn't auto-vectorize).
fn cmpInto(comptime T: type, data: []const T, want: T, mask: []bool, op: PredicateOp) void {
    switch (op) {
        inline else => |o| {
            const cop = comptime std.meta.stringToEnum(simd.CmpOp, @tagName(o)).?;
            simd.compareInto(T, cop, data, want, mask);
        },
    }
}

/// Outcome of translating a leaf comparison `col OP const` into the FOR
/// (Frame-of-Reference) code domain of one block, where each row stores
/// `code = value - base` with `code ∈ [0, span]`.
///
///   - `.none`             — no valid row can match (every survivor mask bit
///                           is cleared); the caller skips the SIMD compare.
///   - `.all`              — every valid (non-NULL) row matches; the caller
///                           sets the mask true and just ANDs the validity bits.
///   - `.compare`          — compare each code against `code` under `op` (the
///                           code is guaranteed in `[0, span]`, so the unsigned
///                           narrow-width comparison is identical to comparing
///                           the native values).
pub const ForLeafPlan = union(enum) {
    none,
    all,
    compare: struct { op: PredicateOp, code: u64 },
};

/// Translate `col OP v` into the FOR code domain for a block with `base` and
/// `span = max_value - base` (so valid codes occupy `[0, span]`). All math is
/// in i128 so an out-of-range constant is handled without wraparound. Returns
/// `null` when `v` carries no usable numeric domain (only the FOR-eligible
/// integer-family / temporal / decimal64 / boolean types do — exactly the set
/// the writer ever FOR-encodes), in which case the caller must not use the
/// FOR-aware path.
///
/// The per-op boundary logic mirrors comparing the reconstructed native value
/// `base + code` against the constant `C`, expressed on `D = C - base`:
///   - eq : code == D when 0 ≤ D ≤ span, else none
///   - neq: code != D when 0 ≤ D ≤ span, else all
///   - lt : D ≤ 0 → none; D > span → all; else code < D
///   - lte: D < 0 → none; D ≥ span → all; else code ≤ D
///   - gt : D ≥ span → none; D < 0 → all; else code > D
///   - gte: D > span → none; D ≤ 0 → all; else code ≥ D
pub fn translateForLeaf(base: i128, span: u128, op: PredicateOp, v: Value) ?ForLeafPlan {
    const c = valueToRangeI128(v) orelse return null;
    const d: i128 = c - base;
    const span_i: i128 = @intCast(span);
    return switch (op) {
        .eq => if (d < 0 or d > span_i) .none else .{ .compare = .{ .op = .eq, .code = @intCast(d) } },
        .neq => if (d < 0 or d > span_i) .all else .{ .compare = .{ .op = .neq, .code = @intCast(d) } },
        .lt => if (d <= 0) .none else if (d > span_i) .all else .{ .compare = .{ .op = .lt, .code = @intCast(d) } },
        .lte => if (d < 0) .none else if (d >= span_i) .all else .{ .compare = .{ .op = .lte, .code = @intCast(d) } },
        .gt => if (d >= span_i) .none else if (d < 0) .all else .{ .compare = .{ .op = .gt, .code = @intCast(d) } },
        .gte => if (d > span_i) .none else if (d <= 0) .all else .{ .compare = .{ .op = .gte, .code = @intCast(d) } },
    };
}

/// True iff a column of this type carries a usable numeric min/max range
/// in the propagated `ColStat`. The int family, temporal, boolean, and
/// decimal types store their literal value directly as the i128 range key
/// (matching `valueToRangeI128`). Strings (prefix-encoded), uuid (top-bit
/// XOR), and floats (no stats) are excluded — their manifest stats aren't a
/// usable numeric range in the value's own domain.
pub fn typeHasRange(t: types.Type) bool {
    return switch (t) {
        .int, .bigint, .smallint, .tinyint, .largeint => true,
        .boolean, .date, .datetime => true,
        .decimal64, .decimal128 => true,
        .varchar, .string, .char, .json, .uuid, .float, .double => false,
    };
}

/// Map a predicate literal `Value` into the i128 range domain used by
/// `ColStat.min`/`.max`. Returns null for types that carry no usable
/// numeric range (strings, uuid, floats) — mirrors `typeHasRange`.
pub fn valueToRangeI128(v: Value) ?i128 {
    return switch (v) {
        .int => |x| x,
        .bigint => |x| x,
        .boolean => |x| @intFromBool(x),
        .date => |x| x,
        .datetime => |x| x,
        .tinyint => |x| x,
        .smallint => |x| x,
        .largeint => |x| x,
        .decimal64 => |x| x,
        .decimal128 => |x| x,
        .text, .uuid, .float, .double => null,
    };
}

/// True when a `col OP literal` leaf by itself proves the column non-blank —
/// no row with `''` can satisfy it. `''` is the global string minimum, so any
/// `>` bound excludes it, and `=`/`>=` with a non-empty literal do too. Used
/// for cross-leaf blank-aware pruning (a range hint on a column whose other
/// conjuncts exclude blanks may use the blank-excluded min) and by the
/// zonemap top-N corner.
pub fn leafExcludesBlank(op: PredicateOp, v: Value) bool {
    const txt = switch (v) {
        .text => |t| t,
        else => return false,
    };
    return switch (op) {
        .neq => txt.len == 0,
        .gt => true,
        .gte, .eq => txt.len > 0,
        .lt, .lte => false,
    };
}

/// Returns true if the row-group stats could contain rows matching `op val`.
/// Used by Scan and DELETE to decide whether to skip a row group entirely.
/// Wrapper over `statsOverlapPredicateBlankAware` with no blank-exclusion
/// proof — the conservative leaf-local form.
pub fn statsOverlapPredicate(s: storage.format.Stats, op: PredicateOp, v: Value) bool {
    return statsOverlapPredicateBlankAware(s, op, v, false);
}

/// Stats are i128 with per-type encoding (see `format.Stats`). The
/// predicate value is encoded with the same scheme so signed i128
/// comparison gives the right answer for every type.
///
/// String predicates use the 16-byte prefix encoding. `eq` and the range ops
/// (lt/lte/gt/gte) prune via the prefix class, staying conservative on a class
/// tie (prefix loss beyond 16 bytes), so no match is ever wrongly skipped.
/// Strings additionally consult the blank-excluded min (`Stats.sum`):
///   - its `maxInt` SENTINEL (the range holds no non-blank value) is exact —
///     no prefix ambiguity — so any op only non-blank rows can satisfy
///     (`<> ''`, `> x`, `= x`/`>= x` with non-empty x) prunes outright;
///   - `= x` (non-empty) also prunes when x's class is strictly below the
///     non-blank min ('' rows can't equal x, so the plain min is noise);
///   - `< x`/`<= x` may substitute the non-blank min for the plain min ONLY
///     when `blanks_excluded` proves another conjunct rules out `''` (a blank
///     row would otherwise satisfy the upper bound). A `sum` of 0 means "no
///     info" (vestigial empty stats slot) and disables all of the above.
pub fn statsOverlapPredicateBlankAware(s: storage.format.Stats, op: PredicateOp, v: Value, blanks_excluded: bool) bool {
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
            const enc = storage.format.encodeStringPrefix(x);
            const nb_min = s.sum;
            const nb_known = nb_min != 0;
            const all_blank = nb_known and nb_min == std.math.maxInt(i128);
            return switch (op) {
                .eq => blk: {
                    if (x.len > 0 and nb_known) {
                        if (all_blank or enc < nb_min) break :blk false;
                    }
                    break :blk enc >= s.min and enc <= s.max;
                },
                // Values may differ past the prefix — never prune via the
                // class range. `<> ''` is the exception: the all-blank
                // sentinel is exact.
                .neq => !(x.len == 0 and all_blank),
                .lt, .lte => blk: {
                    if (blanks_excluded and nb_known) {
                        if (all_blank) break :blk false;
                        break :blk nb_min <= enc;
                    }
                    break :blk s.min <= enc;
                },
                // Any match is > x ≥ '', hence non-blank: the sentinel prunes.
                .gt => if (all_blank) false else s.max >= enc,
                .gte => blk: {
                    if (x.len > 0 and all_blank) break :blk false;
                    break :blk s.max >= enc;
                },
            };
        },
        // Floats: encode the literal with the same order-preserving transform as
        // the stats. A NaN literal can't match any range/eq, but the stats skip
        // NaN, so stay conservative for it (never prune on a NaN literal).
        .float => |x| blk: {
            if (std.math.isNan(x)) return true;
            break :blk storage.format.encodeFloatOrder(@as(f64, x));
        },
        .double => |x| blk: {
            if (std.math.isNan(x)) return true;
            break :blk storage.format.encodeFloatOrder(x);
        },
    };
    const min, const max = switch (v) {
        .float, .double => .{ storage.format.canonicalFloatOrder(s.min), storage.format.canonicalFloatOrder(s.max) },
        else => .{ s.min, s.max },
    };
    return switch (op) {
        .eq => wanted >= min and wanted <= max,
        .neq => !(min == max and min == wanted),
        .lt => min < wanted,
        .lte => min <= wanted,
        .gt => max > wanted,
        .gte => max >= wanted,
    };
}

test "statsOverlapPredicateBlankAware: blank-excluded min pruning for strings" {
    const t = std.testing;
    const enc = storage.format.encodeStringPrefix;
    const sentinel = std.math.maxInt(i128);

    // A typical string RG: plain min is '' (blanks present), real values in
    // ["beta", "delta"], so the blank-excluded min is "beta".
    const rg: storage.format.Stats = .{
        .min = enc(""),
        .max = enc("delta"),
        .sum = enc("beta"),
    };
    // An all-blank RG: every value is '' (or NULL) — exact sentinel.
    const blank_rg: storage.format.Stats = .{
        .min = enc(""),
        .max = enc(""),
        .sum = sentinel,
    };
    // Pre-v11-shaped slot: sum = 0 means "no info", everything conservative.
    const no_info: storage.format.Stats = .{ .min = enc(""), .max = enc("delta") };

    const txt = struct {
        fn v(s: []const u8) Value {
            return .{ .text = s };
        }
    }.v;

    // eq: a non-empty literal below the non-blank min can't match.
    try t.expect(!statsOverlapPredicate(rg, .eq, txt("alpha")));
    try t.expect(statsOverlapPredicate(rg, .eq, txt("beta")));
    try t.expect(statsOverlapPredicate(rg, .eq, txt("")));
    try t.expect(statsOverlapPredicate(no_info, .eq, txt("alpha"))); // conservative

    // The all-blank sentinel prunes every only-non-blank-can-match op.
    try t.expect(!statsOverlapPredicate(blank_rg, .neq, txt("")));
    try t.expect(!statsOverlapPredicate(blank_rg, .gt, txt("")));
    try t.expect(!statsOverlapPredicate(blank_rg, .gte, txt("a")));
    try t.expect(!statsOverlapPredicate(blank_rg, .eq, txt("a")));
    // ...but blanks themselves still match where they should.
    try t.expect(statsOverlapPredicate(blank_rg, .eq, txt("")));
    try t.expect(statsOverlapPredicate(blank_rg, .gte, txt("")));
    try t.expect(statsOverlapPredicate(blank_rg, .lt, txt("a")));
    // neq against a non-empty literal never prunes (prefix ambiguity).
    try t.expect(statsOverlapPredicate(rg, .neq, txt("beta")));

    // lt/lte: leaf-local stays on the plain min ('' matches any upper bound)…
    try t.expect(statsOverlapPredicate(rg, .lt, txt("alpha")));
    // …but a cross-leaf blank-exclusion proof switches to the non-blank min.
    try t.expect(!statsOverlapPredicateBlankAware(rg, .lt, txt("alpha"), true));
    try t.expect(statsOverlapPredicateBlankAware(rg, .lt, txt("carrot"), true));
    // Prefix-class tie stays conservative (non-strict compare).
    try t.expect(statsOverlapPredicateBlankAware(rg, .lte, txt("beta"), true));
    try t.expect(!statsOverlapPredicateBlankAware(blank_rg, .lt, txt("zzz"), true));
    try t.expect(statsOverlapPredicateBlankAware(no_info, .lt, txt("alpha"), true)); // no info → conservative

    // gt: blanks never satisfy it, so the sentinel prunes even leaf-locally;
    // a real upper bound still works off max.
    try t.expect(statsOverlapPredicate(rg, .gt, txt("carrot")));
    try t.expect(!statsOverlapPredicate(rg, .gt, txt("delta1"))); // above max class
}

test "leafExcludesBlank classifies the provable shapes" {
    const t = std.testing;
    try t.expect(leafExcludesBlank(.neq, .{ .text = "" }));
    try t.expect(leafExcludesBlank(.gt, .{ .text = "" }));
    try t.expect(leafExcludesBlank(.gt, .{ .text = "m" }));
    try t.expect(leafExcludesBlank(.eq, .{ .text = "x" }));
    try t.expect(leafExcludesBlank(.gte, .{ .text = "a" }));
    try t.expect(!leafExcludesBlank(.eq, .{ .text = "" }));
    try t.expect(!leafExcludesBlank(.gte, .{ .text = "" }));
    try t.expect(!leafExcludesBlank(.neq, .{ .text = "x" }));
    try t.expect(!leafExcludesBlank(.lt, .{ .text = "z" }));
    try t.expect(!leafExcludesBlank(.gt, .{ .bigint = 5 }));
}

test "clearNullRows clears exactly the NULL rows across word boundaries" {
    var prng = std.Random.DefaultPrng.init(0xc1ea7);
    const rand = prng.random();
    inline for (.{ 1, 7, 63, 64, 65, 130, 200 }) |n| {
        var bitmap: [(n + 7) / 8]u8 = undefined;
        rand.bytes(&bitmap);
        // An all-valid word takes the skip path.
        if (n >= 64) @memset(bitmap[0..8], 0xFF);
        var mask: [n]bool = undefined;
        for (&mask) |*m| m.* = rand.boolean();
        var want: [n]bool = undefined;
        for (&want, mask, 0..) |*w, m, i| w.* = m and storage.column.isValidBit(&bitmap, i);
        clearNullRows(&bitmap, &mask);
        try std.testing.expectEqualSlices(bool, &want, &mask);
    }
    var untouched = [_]bool{ true, false, true };
    clearNullRows(null, &untouched);
    try std.testing.expectEqualSlices(bool, &[_]bool{ true, false, true }, &untouched);
}

fn expectInSetMaskMatchesScan(view: ColumnView, values: []const Value, rows: usize) !void {
    var got: [200]bool = undefined;
    var want: [200]bool = undefined;
    for ([_]bool{ false, true }) |negate| {
        try evaluateInSetMask(std.testing.allocator, view, values, negate, rows, got[0..rows]);
        for (want[0..rows], 0..) |*w, i| {
            const found = for (values) |v| {
                if (cellMatchesValue(view, i, v)) break true;
            } else false;
            w.* = view.isValid(i) and found != negate;
        }
        try std.testing.expectEqualSlices(bool, want[0..rows], got[0..rows]);
    }
}

test "evaluateInSetMask: set lookups agree with the per-row scan" {
    var prng = std.Random.DefaultPrng.init(0x340);
    const rand = prng.random();
    const n = 200;
    var nulls: [(n + 7) / 8]u8 = undefined;
    rand.bytes(&nulls);
    var values: [300]Value = undefined;
    // Both sides of the lookup thresholds: short and long lists, few and many rows.
    const list_lens = [_]usize{ 0, 1, 8, 9, 40, 300 };
    const row_counts = [_]usize{ 5, 31, 32, n };

    inline for (.{ .int, .bigint, .smallint, .tinyint, .largeint, .date, .datetime, .decimal64, .decimal128, .uuid }) |tag| {
        const T = @FieldType(Value, @tagName(tag));
        var col: [n]T = undefined;
        for (&col) |*c| c.* = @intCast(rand.intRangeAtMost(u8, 0, 60));
        for ([_]?[]const u8{ null, &nulls }) |bitmap| {
            const view: ColumnView = .{ .data = @unionInit(storage.column.ValueView, @tagName(tag), &col), .nulls = bitmap };
            for (list_lens) |len| {
                for (values[0..len], 0..) |*v, i| {
                    // A value of another type never matches, on either path.
                    v.* = if (i % 13 == 5) .{ .double = 7 } else @unionInit(Value, @tagName(tag), @intCast(rand.intRangeAtMost(u8, 0, 70)));
                }
                for (row_counts) |rows| try expectInSetMaskMatchesScan(view, values[0..len], rows);
            }
        }
    }

    // Literal ranges at the type's extremes, inside and past the bitmap's
    // span: a value below the smallest literal must not wrap into it.
    inline for (.{ .int, .bigint, .smallint, .tinyint, .largeint, .datetime, .decimal128, .uuid }) |tag| {
        const T = @FieldType(Value, @tagName(tag));
        const lo = std.math.minInt(T);
        const hi = std.math.maxInt(T);
        const pool = [_]T{ lo, lo + 1, lo + 3, 0, 1, 5, hi - 2, hi };
        var col: [n]T = undefined;
        for (&col) |*c| c.* = pool[rand.uintLessThan(usize, pool.len)];
        const view: ColumnView = .{ .data = @unionInit(storage.column.ValueView, @tagName(tag), &col), .nulls = &nulls };
        const sets = [_][]const T{ &.{ lo, lo + 3 }, &.{ hi - 2, hi }, &.{ 1, 5 }, &pool, &.{ lo, 0, hi } };
        for (sets) |set| {
            for (values[0..set.len], set) |*v, x| v.* = @unionInit(Value, @tagName(tag), x);
            try expectInSetMaskMatchesScan(view, values[0..set.len], n);
        }
    }

    const words = [_][]const u8{ "", "a", "ab", "abc", "b", "east", "west", "north", "south", "é", "longer than sixteen bytes" };
    var offsets: [n + 1]u32 = undefined;
    var bytes: std.ArrayList(u8) = .empty;
    defer bytes.deinit(std.testing.allocator);
    offsets[0] = 0;
    for (1..n + 1) |i| {
        try bytes.appendSlice(std.testing.allocator, words[rand.uintLessThan(usize, words.len)]);
        offsets[i] = @intCast(bytes.items.len);
    }
    const sv: storage.StringView = .{ .offsets = &offsets, .bytes = bytes.items };
    for ([_]?[]const u8{ null, &nulls }) |bitmap| {
        const view: ColumnView = .{ .data = .{ .varchar = sv }, .nulls = bitmap };
        for (list_lens) |len| {
            for (values[0..len]) |*v| v.* = .{ .text = words[rand.uintLessThan(usize, words.len)] };
            if (len > 0) values[0] = .{ .text = "absent" };
            for (row_counts) |rows| try expectInSetMaskMatchesScan(view, values[0..len], rows);
        }
    }
}

test "an OR of equalities on one column matches as its arms do, NOT and all" {
    const t = std.testing;
    var prng = std.Random.DefaultPrng.init(0x340);
    const rand = prng.random();
    const n = 200;
    var nulls: [(n + 7) / 8]u8 = undefined;
    rand.bytes(&nulls);
    var ids: [n]i64 = undefined;
    for (&ids) |*c| c.* = rand.intRangeAtMost(i64, -5, 60);
    var scores: [n]f64 = undefined;
    for (&scores) |*c| c.* = @floatFromInt(rand.intRangeAtMost(u8, 0, 20));
    const words = [_][]const u8{ "", "a", "ab", "east", "west", "north", "south", "é" };
    var offsets: [n + 1]u32 = undefined;
    var bytes: std.ArrayList(u8) = .empty;
    defer bytes.deinit(t.allocator);
    offsets[0] = 0;
    for (1..n + 1) |i| {
        try bytes.appendSlice(t.allocator, words[rand.uintLessThan(usize, words.len)]);
        offsets[i] = @intCast(bytes.items.len);
    }
    const schema = [_]Column{
        .{ .name = "id", .type = .bigint, .nullable = true },
        .{ .name = "region", .type = .{ .varchar = 16 }, .nullable = true },
        .{ .name = "score", .type = .double },
    };
    const views = [_]ColumnView{
        .{ .data = .{ .bigint = &ids }, .nulls = &nulls },
        .{ .data = .{ .varchar = .{ .offsets = &offsets, .bytes = bytes.items } }, .nulls = &nulls },
        .{ .data = .{ .double = &scores } },
    };

    var arms: [40]PredicateExpr = undefined;
    // Arm counts on both sides of the set lookup's threshold, row counts too.
    for ([_]usize{ 1, 8, 9, 40 }) |len| {
        for ([_][]const u8{ "id", "REGION", "score" }) |col| {
            for (arms[0..len]) |*arm| arm.* = .{ .leaf = .{ .col = col, .op = .eq, .val = switch (col[0]) {
                'i' => .{ .bigint = rand.intRangeAtMost(i64, -10, 70) },
                'R' => .{ .text = words[rand.uintLessThan(usize, words.len)] },
                else => .{ .double = @floatFromInt(rand.intRangeAtMost(u8, 0, 25)) },
            } } };
            const any_arm: PredicateExpr = .{ .@"or" = arms[0..len] };
            const none_of: PredicateExpr = .{ .not = &any_arm };
            for ([_]usize{ 31, 32, n }) |rows| {
                const batch = .{ .values = &views, .row_count = rows };
                var want: [n]bool = @splat(false);
                var arm_mask: [n]bool = undefined;
                for (arms[0..len]) |arm| {
                    try evaluateMaskWithPred(views[findCol(&schema, arm.leaf.col).?], arm.leaf, rows, arm_mask[0..rows]);
                    for (want[0..rows], arm_mask[0..rows]) |*w, m| w.* = w.* or m;
                }
                var got: [n]bool = undefined;
                try evaluatePredicate(t.allocator, any_arm, &schema, batch, got[0..rows]);
                try t.expectEqualSlices(bool, want[0..rows], got[0..rows]);
                try evaluateExprGuided(t.allocator, any_arm, &schema, batch, got[0..rows], null);
                try t.expectEqualSlices(bool, want[0..rows], got[0..rows]);
                try evaluatePredicate(t.allocator, none_of, &schema, batch, got[0..rows]);
                for (want[0..rows], got[0..rows]) |w, g| try t.expectEqual(!w, g);
            }
        }
    }

    const mixed = [_]PredicateExpr{
        .{ .leaf = .{ .col = "id", .op = .eq, .val = .{ .bigint = 1 } } },
        .{ .leaf = .{ .col = "id", .op = .gt, .val = .{ .bigint = 50 } } },
    };
    try t.expectEqualStrings("id", eqDisjunctionColumn(mixed[0..1]).?);
    try t.expect(eqDisjunctionColumn(&mixed) == null);
    const two_columns = [_]PredicateExpr{ mixed[0], .{ .leaf = .{ .col = "score", .op = .eq, .val = .{ .double = 1 } } } };
    try t.expect(eqDisjunctionColumn(&two_columns) == null);
}

test "an OR's equality arms and an AND's inequality arms fold per column beside the rest" {
    const t = std.testing;
    var prng = std.Random.DefaultPrng.init(0x592);
    const rand = prng.random();
    const n = 200;
    var nulls: [(n + 7) / 8]u8 = undefined;
    rand.bytes(&nulls);
    var ids: [n]i64 = undefined;
    for (&ids) |*c| c.* = rand.intRangeAtMost(i64, -5, 300);
    var scores: [n]f64 = undefined;
    for (&scores) |*c| c.* = @floatFromInt(rand.intRangeAtMost(u8, 0, 20));
    const words = [_][]const u8{ "", "a", "ab", "east", "west", "north", "south", "é", "x1", "x2", "x3", "x4" };
    var offsets: [n + 1]u32 = undefined;
    var bytes: std.ArrayList(u8) = .empty;
    defer bytes.deinit(t.allocator);
    offsets[0] = 0;
    for (1..n + 1) |i| {
        try bytes.appendSlice(t.allocator, words[rand.uintLessThan(usize, words.len)]);
        offsets[i] = @intCast(bytes.items.len);
    }
    const schema = [_]Column{
        .{ .name = "id", .type = .bigint, .nullable = true },
        .{ .name = "region", .type = .{ .varchar = 16 }, .nullable = true },
        .{ .name = "score", .type = .double },
    };
    const views = [_]ColumnView{
        .{ .data = .{ .bigint = &ids }, .nulls = &nulls },
        .{ .data = .{ .varchar = .{ .offsets = &offsets, .bytes = bytes.items } }, .nulls = &nulls },
        .{ .data = .{ .double = &scores } },
    };

    // Long lists on `id` and `region` (folded), a long list on the float
    // `score` (kept as leaves), a short `region` list under the threshold,
    // a range arm and a nested AND, interleaved.
    const range_kids = [_]PredicateExpr{
        .{ .leaf = .{ .col = "id", .op = .gt, .val = .{ .bigint = 250 } } },
        .{ .leaf = .{ .col = "score", .op = .lt, .val = .{ .double = 3 } } },
    };
    for ([_]usize{ 0, 3, 9, 40 }) |id_len| {
        for ([_]usize{ 0, 5, 12 }) |region_len| {
            var arms: std.ArrayList(PredicateExpr) = .empty;
            defer arms.deinit(t.allocator);
            for (0..id_len) |_| try arms.append(t.allocator, .{ .leaf = .{ .col = "id", .op = .eq, .val = .{ .bigint = rand.intRangeAtMost(i64, -10, 310) } } });
            for (0..region_len) |_| try arms.append(t.allocator, .{ .leaf = .{ .col = "REGION", .op = .eq, .val = .{ .text = words[rand.uintLessThan(usize, words.len)] } } });
            for (0..10) |_| try arms.append(t.allocator, .{ .leaf = .{ .col = "score", .op = .eq, .val = .{ .double = @floatFromInt(rand.intRangeAtMost(u8, 0, 25)) } } });
            try arms.append(t.allocator, .{ .@"and" = &range_kids });
            try arms.append(t.allocator, .{ .leaf = .{ .col = "id", .op = .lt, .val = .{ .bigint = 0 } } });
            rand.shuffle(PredicateExpr, arms.items);
            const any_arm: PredicateExpr = .{ .@"or" = arms.items };
            // The NOT IN mirror: every equality as `<>`, under an AND.
            const conjuncts = try t.allocator.dupe(PredicateExpr, arms.items);
            defer t.allocator.free(conjuncts);
            for (conjuncts) |*c| {
                if (c.* == .leaf and c.leaf.op == .eq) c.leaf.op = .neq;
            }
            const all_of: PredicateExpr = .{ .@"and" = conjuncts };

            for ([_]usize{ 31, 32, n }) |rows| {
                const batch = .{ .values = &views, .row_count = rows };
                var want: [n]bool = @splat(false);
                var want_all: [n]bool = @splat(true);
                var arm_mask: [n]bool = undefined;
                for (arms.items) |arm| {
                    try evaluatePredicate(t.allocator, arm, &schema, batch, arm_mask[0..rows]);
                    for (want[0..rows], arm_mask[0..rows]) |*w, m| w.* = w.* or m;
                }
                for (conjuncts) |c| {
                    try evaluatePredicate(t.allocator, c, &schema, batch, arm_mask[0..rows]);
                    for (want_all[0..rows], arm_mask[0..rows]) |*w, m| w.* = w.* and m;
                }
                var got: [n]bool = undefined;
                try evaluatePredicate(t.allocator, any_arm, &schema, batch, got[0..rows]);
                try t.expectEqualSlices(bool, want[0..rows], got[0..rows]);
                try evaluateExprGuided(t.allocator, any_arm, &schema, batch, got[0..rows], null);
                try t.expectEqualSlices(bool, want[0..rows], got[0..rows]);
                try evaluatePredicate(t.allocator, all_of, &schema, batch, got[0..rows]);
                try t.expectEqualSlices(bool, want_all[0..rows], got[0..rows]);
                try evaluateExprGuided(t.allocator, all_of, &schema, batch, got[0..rows], null);
                try t.expectEqualSlices(bool, want_all[0..rows], got[0..rows]);
            }
        }
    }
}

test "textNumber parses what StarRocks compares as a number" {
    const t = std.testing;
    const cases = .{
        .{ "12", ScaledInt{ .m = 12, .s = 0 } },
        .{ " -1.50 ", ScaledInt{ .m = -150, .s = 2 } },
        .{ "+.5", ScaledInt{ .m = 5, .s = 1 } },
        .{ "7.", ScaledInt{ .m = 7, .s = 0 } },
    };
    inline for (cases) |c| try t.expectEqual(Scalar{ .decimal = c[1] }, textNumber(c[0]).?);
    try t.expectEqual(Scalar{ .float = 1500.0 }, textNumber("1.5e3").?);
    inline for (.{ "", "abc", "12abc", "-", ".", "1e999" }) |bad| try t.expect(textNumber(bad) == null);
}

test "placeLiteral lands each literal on the column's values" {
    const t = std.testing;
    const dec_10_2: types.Type = .{ .decimal64 = .{ .p = 10, .s = 2 } };
    try t.expectEqual(Value{ .smallint = 2 }, placeLiteral(.{ .bigint = 2 }, .smallint).exact);
    try t.expectEqual(Value{ .decimal64 = 150 }, placeLiteral(.{ .text = "1.5" }, dec_10_2).exact);
    try t.expectEqual(Value{ .int = 3 }, placeLiteral(.{ .double = 3.0 }, .int).exact);
    try t.expectEqual(Value{ .datetime = 19787 * std.time.us_per_day }, placeLiteral(.{ .date = 19787 }, .datetime).exact);

    const between = placeLiteral(.{ .double = 2.5 }, .int).between;
    try t.expectEqual(Value{ .int = 2 }, between.lo);
    try t.expectEqual(Value{ .int = 3 }, between.hi);
    const on_days = placeLiteral(.{ .text = "2024-03-05 10:00:00" }, .date).between;
    try t.expectEqual(Value{ .date = 19787 }, on_days.lo);
    try t.expectEqual(Value{ .date = 19788 }, on_days.hi);

    try t.expectEqual(Side.above, placeLiteral(.{ .int = 100000 }, .smallint).beyond);
    try t.expectEqual(Side.below, placeLiteral(.{ .int = -100000 }, .smallint).beyond);
    try t.expectEqual(Side.above, placeLiteral(.{ .double = 32767.5 }, .smallint).beyond);
    try t.expectEqual(Side.below, placeLiteral(.{ .double = -32768.5 }, .smallint).beyond);

    try t.expect(placeLiteral(.{ .text = "12abc" }, .int) == .null_text);
    try t.expect(placeLiteral(.{ .uuid = 1 }, .int) == .incomparable);
    try t.expect(placeLiteral(.{ .bigint = 1 }, .uuid) == .incomparable);
}

test "placeLiteral reads a number against a DATE or DATETIME as MySQL does" {
    const t = std.testing;
    const us_per_day = std.time.us_per_day;
    const days = scalar_fn_common.ymdToDays;
    const sep26 = days(2026, 9, 26);
    const clock: i64 = (10 * 3600 + 5 * 60 + 3) * std.time.us_per_s;
    const at_sep26: i64 = @as(i64, sep26) * us_per_day;
    const exact = .{
        .{ Value{ .bigint = 20260926 }, types.Type.date, Value{ .date = sep26 } },
        .{ Value{ .int = 260926 }, types.Type.date, Value{ .date = sep26 } },
        .{ Value{ .bigint = 20260926000001 }, types.Type.date, Value{ .date = sep26 } },
        .{ Value{ .double = 20260926.9 }, types.Type.date, Value{ .date = sep26 } },
        .{ Value{ .bigint = 20260926 }, types.Type.datetime, Value{ .datetime = at_sep26 } },
        .{ Value{ .bigint = 20260926100503 }, types.Type.datetime, Value{ .datetime = at_sep26 + clock } },
        .{ Value{ .double = 20260926100503.5 }, types.Type.datetime, Value{ .datetime = at_sep26 + clock + 500_000 } },
        // A number column meets a date or datetime as its number.
        .{ Value{ .date = sep26 }, types.Type.int, Value{ .int = 20260926 } },
        .{ Value{ .datetime = at_sep26 + clock }, types.Type.bigint, Value{ .bigint = 20260926100503 } },
    };
    inline for (exact) |c| try t.expectEqual(c[2], placeLiteral(c[0], c[1]).exact);

    const between = .{
        // No day has a zero day or month: they lie before the month or year.
        .{ Value{ .bigint = 20260900 }, types.Type.date, Value{ .date = days(2026, 8, 31) }, Value{ .date = days(2026, 9, 1) } },
        .{ Value{ .bigint = 260900 }, types.Type.date, Value{ .date = days(2026, 8, 31) }, Value{ .date = days(2026, 9, 1) } },
        .{ Value{ .bigint = 20260231 }, types.Type.date, Value{ .date = days(2026, 2, 28) }, Value{ .date = days(2026, 3, 1) } },
        .{ Value{ .bigint = 20010100 }, types.Type.datetime, Value{ .datetime = @as(i64, days(2001, 1, 1)) * us_per_day - 1 }, Value{ .datetime = @as(i64, days(2001, 1, 1)) * us_per_day } },
        // No datetime reads: compared with each row's number.
        .{ Value{ .bigint = 20261399 }, types.Type.date, Value{ .date = days(2026, 12, 31) }, Value{ .date = days(2027, 1, 1) } },
        .{ Value{ .bigint = 20260926250000 }, types.Type.datetime, Value{ .datetime = at_sep26 + us_per_day - 1 }, Value{ .datetime = at_sep26 + us_per_day } },
        .{ Value{ .datetime = at_sep26 + clock + 500_000 }, types.Type.bigint, Value{ .bigint = 20260926100503 }, Value{ .bigint = 20260926100504 } },
    };
    inline for (between) |c| {
        const got = placeLiteral(c[0], c[1]).between;
        try t.expectEqual(c[2], got.lo);
        try t.expectEqual(c[3], got.hi);
    }

    const beyond = .{
        .{ Value{ .bigint = 2026 }, types.Type.date, Side.below },
        .{ Value{ .bigint = 0 }, types.Type.date, Side.below },
        .{ Value{ .bigint = -20260926 }, types.Type.date, Side.below },
        .{ Value{ .boolean = true }, types.Type.date, Side.below },
        .{ Value{ .bigint = 99 }, types.Type.datetime, Side.below },
        .{ Value{ .bigint = 99991232 }, types.Type.date, Side.above },
        .{ Value{ .bigint = 99991231240000 }, types.Type.date, Side.above },
        .{ Value{ .date = sep26 }, types.Type.smallint, Side.above },
    };
    inline for (beyond) |c| try t.expectEqual(c[2], placeLiteral(c[0], c[1]).beyond);
}

test "placeLiteral reads text against a DATE or DATETIME as MySQL does" {
    const t = std.testing;
    const day: i32 = 20722; // 2026-09-26
    const readable = .{
        .{ "2026-09-26", day },
        .{ "20260926", day },
        .{ "260926", day },
        .{ "2026-9-26", day },
        .{ "2026/09/26", day },
        .{ " 2026-09-26", day },
        .{ "2026-09-26x", day },
        .{ "2026-09-26 00:00:00", day },
    };
    inline for (readable) |c| try t.expectEqual(Value{ .date = c[1] }, placeLiteral(.{ .text = c[0] }, .date).exact);
    const at_ten = @as(i64, day) * std.time.us_per_day + 10 * std.time.us_per_hour;
    inline for (.{ "2026-09-26 10:00:00", "2026-09-26T10:00", "2026-09-26 10" }) |text| {
        try t.expectEqual(Value{ .datetime = at_ten }, placeLiteral(.{ .text = text }, .datetime).exact);
    }
    try t.expectEqual(Value{ .date = day }, placeLiteral(.{ .text = "2026-09-26 10:00" }, .date).between.lo);

    const unreadable = .{ "", "   ", "abc", "0", "2026", "202609", "2026-09", "2026-02-30", "2026-09-31", "2026-13-01", "0000-00-00", "2026-09-00", "10000-01-01", "2026-09-26 25:00:00", "2026-09-26 10:60:00", "2026-09-26 10:00:60" };
    inline for (unreadable) |text| {
        try t.expect(placeLiteral(.{ .text = text }, .date) == .not_temporal);
        try t.expect(placeLiteral(.{ .text = text }, .datetime) == .not_temporal);
    }
}

test "validateExpr raises on a constant no date reads, and drops it from a set" {
    const t = std.testing;
    const schema = [_]Column{.{ .name = "d", .type = .date, .nullable = true }};
    const ops = [_]PredicateOp{ .eq, .neq, .lt, .lte, .gt, .gte };
    for (ops) |op| {
        var expr: PredicateExpr = .{ .leaf = .{ .col = "d", .op = op, .val = .{ .text = "abc" }, .from_statement = true } };
        try t.expectError(Error.InvalidTemporalLiteral, validateExpr(&expr, &schema));
        try t.expectEqualStrings("Incorrect DATE value: 'abc'", takeInvalidTemporalMessage().?);
        try t.expect(takeInvalidTemporalMessage() == null);

        var bound: PredicateExpr = .{ .leaf = .{ .col = "d", .op = op, .val = .{ .text = "abc" } } };
        try validateExpr(&bound, &schema);
        try t.expect(bound == .unknown);
        try t.expect(takeInvalidTemporalMessage() == null);
    }

    var long_value: [200]u8 = undefined;
    @memset(&long_value, 'x');
    long_value[127] = 0xC3;
    long_value[128] = 0xA9;
    var long: PredicateExpr = .{ .leaf = .{ .col = "d", .op = .eq, .val = .{ .text = &long_value }, .from_statement = true } };
    try t.expectError(Error.InvalidTemporalLiteral, validateExpr(&long, &schema));
    try t.expectEqualStrings("Incorrect DATE value: '" ++ "x" ** 127 ++ "'", takeInvalidTemporalMessage().?);

    var values = [_]Value{ .{ .text = "abc" }, .{ .text = "2026-09-26" } };
    var set: PredicateExpr = .{ .in_set = .{ .col = "d", .values = &values, .negate = false, .value_type = .string } };
    try validateExpr(&set, &schema);
    try t.expectEqualSlices(Value, &.{.{ .date = 20722 }}, set.in_set.values);
}
