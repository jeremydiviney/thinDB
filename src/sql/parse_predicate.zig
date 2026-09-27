//! Boolean-expression parsing — predicates for WHERE / HAVING / QUALIFY
//! and CASE WHEN conditions. Extracted from parser.zig; uses the
//! `anytype` pattern (same as parse_window.zig and parse_ddl.zig) so
//! there's no circular import.
//!
//! Grammar:
//!   bool_expr  := or_expr
//!   or_expr    := xor_expr ('OR' xor_expr)*
//!   xor_expr   := and_expr ('XOR' and_expr)*
//!   and_expr   := not_expr (('AND' | '&&') not_expr)*
//!   not_expr   := 'NOT' not_expr | atom
//!   atom       := '(' or_expr ')'
//!                | scalar value_op ...   (scalar led by a literal, a sign,
//!                                         or a group a value operator follows)
//!                | row cmp_op row
//!                | row ['NOT'] 'IN' '(' (row (',' row)* | select) ')'
//!                | 'NULL' ('IS' ['NOT'] 'NULL' | cmp_op expr)
//!                | lit ('IS' ['NOT'] 'NULL' | cmp_op (qualified_col | lit | @var | 'NULL'))
//!                | lit ['NOT'] ('BETWEEN' | 'LIKE' | 'IN') ...
//!                | qualified_col 'IS' ['NOT'] 'NULL'
//!                | qualified_col ['NOT'] 'BETWEEN' lit 'AND' lit
//!                | qualified_col ['NOT'] 'LIKE' string_lit
//!                | qualified_col ['NOT'] 'IN' '(' lit (',' lit)* ')'
//!                | qualified_col cmp_op lit

const std = @import("std");

const exec_predicate = @import("../exec/predicate.zig");
const exec_expr = @import("../exec/expr.zig");
const PredicateExpr = exec_predicate.PredicateExpr;
const PredicateOp = exec_predicate.PredicateOp;

const types = @import("../types.zig");
const Value = types.Value;
const ir = @import("../ir/ir.zig");
const parse_window = @import("parse_window.zig");

pub fn parseBoolExpr(p: anytype) @TypeOf(p.*).Err!PredicateExpr {
    return try parseOr(p);
}

// The parseOr/parseXor/parseAnd/parseNot/parseAtom chain is mutually
// recursive; Zig can't infer error sets through a cycle, so each returns
// the Parser's concrete `Err` set explicitly.
pub fn parseOr(p: anytype) @TypeOf(p.*).Err!PredicateExpr {
    var lhs = try parseXor(p);
    // On the MySQL wire `||` is a synonym for OR (PG/neutral reserve it for
    // string concatenation, handled in the expression parser).
    while (p.cur.tag == .kw_or or (p.cur.tag == .pipe_pipe and p.lex.dialect == .mysql)) {
        try p.advance();
        const rhs = try parseXor(p);
        const children = try p.arena.alloc(PredicateExpr, 2);
        children[0] = lhs;
        children[1] = rhs;
        lhs = .{ .@"or" = children };
    }
    return lhs;
}

/// `a XOR b` holds when exactly one side does and is UNKNOWN when either
/// side is, which `(a OR b) AND NOT (a AND b)` keeps under 3VL.
pub fn parseXor(p: anytype) @TypeOf(p.*).Err!PredicateExpr {
    var lhs = try parseAnd(p);
    while (xorKeywordHere(p)) {
        try p.advance();
        const rhs = try parseAnd(p);
        const either = try p.arena.alloc(PredicateExpr, 2);
        either[0] = lhs;
        either[1] = rhs;
        const both = try p.arena.alloc(PredicateExpr, 2);
        both[0] = lhs;
        both[1] = rhs;
        const kids = try p.arena.alloc(PredicateExpr, 2);
        kids[0] = .{ .@"or" = either };
        kids[1] = try negatePredicate(p, .{ .@"and" = both });
        lhs = .{ .@"and" = kids };
    }
    return lhs;
}

/// XOR lexes as an identifier.
pub fn xorKeywordHere(p: anytype) bool {
    return p.cur.tag == .identifier and std.ascii.eqlIgnoreCase(p.cur.text, "xor");
}

pub fn parseAnd(p: anytype) @TypeOf(p.*).Err!PredicateExpr {
    var lhs = try parseNot(p);
    while (p.cur.tag == .kw_and or p.cur.tag == .amp_amp) {
        try p.advance();
        const rhs = try parseNot(p);
        const children = try p.arena.alloc(PredicateExpr, 2);
        children[0] = lhs;
        children[1] = rhs;
        lhs = .{ .@"and" = children };
    }
    return lhs;
}

pub fn parseNot(p: anytype) @TypeOf(p.*).Err!PredicateExpr {
    if (p.cur.tag == .kw_not) {
        try p.advance();
        const inner = try parseNot(p);
        return try negatePredicate(p, inner);
    }
    return try parseAtom(p);
}

fn flipOp(op: PredicateOp) PredicateOp {
    return switch (op) {
        .eq => .neq,
        .neq => .eq,
        .lt => .gte,
        .gte => .lt,
        .gt => .lte,
        .lte => .gt,
    };
}

/// 3VL-correct negation, applied at parse time by pushing NOT down to the
/// leaves. The mask evaluators collapse UNKNOWN to false, so a mask-level
/// `.not` flip would turn every NULL row TRUE — `NOT (v > 15)` must keep
/// excluding NULL v. Comparisons flip their operator instead (every leaf
/// kernel excludes NULL rows regardless of op), AND/OR De Morgan, IS [NOT]
/// NULL swaps, and the negatable subquery markers flip their own flag. LIKE
/// has no negated form, so it keeps the `.not` wrapper behind an IS NOT NULL
/// guard. EXISTS stays wrapped — subquery_resolve pattern-matches
/// `.not(.exists_subquery)` to thread the negation into the correlated set.
pub fn negatePredicate(p: anytype, e: PredicateExpr) @TypeOf(p.*).Err!PredicateExpr {
    switch (e) {
        .leaf => |l| return .{ .leaf = .{ .col = l.col, .op = flipOp(l.op), .val = l.val } },
        .day_leaf => |l| return .{ .day_leaf = .{ .col = l.col, .op = flipOp(l.op), .val = l.val } },
        .text_as_number => |l| return .{ .text_as_number = .{ .col = l.col, .op = flipOp(l.op), .val = l.val } },
        .leaf_col_col => |c| return .{ .leaf_col_col = .{ .left = c.left, .op = flipOp(c.op), .right = c.right } },
        .is_null => |c| return .{ .is_not_null = c },
        .is_not_null => |c| return .{ .is_null = c },
        .always => |b| return .{ .always = !b },
        // NOT UNKNOWN is UNKNOWN.
        .unknown => return .unknown,
        .not => |child| return child.*,
        .@"and" => |kids| {
            const out = try p.arena.alloc(PredicateExpr, kids.len);
            for (kids, out) |k, *o| o.* = try negatePredicate(p, k);
            return .{ .@"or" = out };
        },
        .@"or" => |kids| {
            const out = try p.arena.alloc(PredicateExpr, kids.len);
            for (kids, out) |k, *o| o.* = try negatePredicate(p, k);
            return .{ .@"and" = out };
        },
        .in_set => |s| return .{ .in_set = .{ .col = s.col, .values = s.values, .negate = !s.negate } },
        .text_as_number_set => |s| return .{ .text_as_number_set = .{ .col = s.col, .values = s.values, .negate = !s.negate } },
        .in_subquery => |s| return .{ .in_subquery = .{ .col = s.col, .source = s.source, .negate = !s.negate, .rest_cols = s.rest_cols } },
        .scalar_subquery => |sq| return .{ .scalar_subquery = .{ .col = sq.col, .op = flipOp(sq.op), .source = sq.source } },
        .like => |l| {
            const child = try p.arena.create(PredicateExpr);
            child.* = e;
            const kids = try p.arena.alloc(PredicateExpr, 2);
            kids[0] = .{ .is_not_null = l.col };
            kids[1] = .{ .not = child };
            return .{ .@"and" = kids };
        },
        // exists_subquery (resolver matches `.not(.exists)`), correlated_* /
        // leaf_var (never parser-built): keep the wrapper.
        else => {
            const child = try p.arena.create(PredicateExpr);
            child.* = e;
            return .{ .not = child };
        },
    }
}

pub fn parseAtom(p: anytype) @TypeOf(p.*).Err!PredicateExpr {
    const PE = @TypeOf(p.*).Err;
    if (p.cur.tag == .lparen) {
        if (try rowValueAhead(p)) return try parseRowValuePredicate(p);
        if (try parenthesizedOperandAhead(p)) return try parseScalarLhs(p);
        try p.advance();
        const inner = try parseOr(p);
        try p.expect(.rparen);
        return inner;
    }
    // EXISTS (SELECT ...) — produces a constant-bool predicate after
    // the pre-compile pass runs the inner. NOT EXISTS is parsed via
    // parseNot wrapping this atom.
    if (p.cur.tag == .kw_exists) {
        try p.advance();
        try p.expect(.lparen);
        if (p.cur.tag != .kw_select and p.cur.tag != .kw_with) return PE.SqlExpectedSelect;
        const source = try p.parseStatement();
        try p.expect(.rparen);
        return .{ .exists_subquery = @ptrCast(source) };
    }
    // NULL on the LHS — generated SQL guards optional parameters with
    // `(:param IS NULL OR ...)`. IS [NOT] NULL folds to a constant, and any
    // comparison with NULL is UNKNOWN, as `col = NULL` is in parseColOps. A
    // bare NULL (`NULL XOR x`) is UNKNOWN too.
    if (p.cur.tag == .kw_null) {
        try p.advance();
        const null_lhs: ir.Expr = .{ .null_lit = .string };
        if (p.cur.tag == .kw_is) return try parseIsOps(p, null_lhs);
        if (p.cur.tag == .null_safe_eq) return (try parseComparisonTail(p, null_lhs)).?;
        if (isPredicateEnd(p)) return .unknown;
        if (!isComparisonToken(p.cur.tag)) return PE.SqlExpectedToken;
        _ = try parseComparisonToken(p);
        _ = try p.parseScalar();
        return .unknown;
    }
    if (isLiteralLhsTokenStart(p.cur.tag) or p.cur.tag == .minus or p.cur.tag == .plus or p.cur.tag == .tilde) {
        return try parseScalarLhs(p);
    }
    // `@var op X` — a session var on the LHS (constant guard, e.g.
    // `@comparisonMonths > 1`). Symmetric to the literal-LHS form above: the
    // var resolves to a literal pre-compile, so both sides materialize as
    // constant columns. A bare `@var` is truthiness (`@var <> 0`).
    if (p.cur.tag == .at_identifier) {
        const var_name = try p.arena.dupe(u8, p.cur.text);
        try p.advance();
        const lhs_expr = ir.Expr{ .var_ref = var_name };
        if (try parseComparisonTail(p, lhs_expr)) |pred| return pred;
        return try makeExprComparisonPredicate(p, lhs_expr, .neq, .{ .lit = .{ .int = 0 } });
    }
    // A CASE expression or a keyword-named call (`IF(...)`) can only be a
    // scalar operand here.
    if (p.cur.tag == .kw_case or p.keywordCallAhead()) return try parseExprOps(p, try p.parseCallAtom());
    if (p.cur.tag != .identifier) return PE.SqlExpectedIdent;
    var col_dup = try parseQualifiedColRef(p);
    if (p.cur.tag != .lparen) {
        if (try p.bareTemporalCall(col_dup)) |call| return try parseExprOps(p, call);
    }

    // JSON extraction on the LHS: `doc->'$.x' op rhs` / `doc->>'$.x' op rhs`.
    // Desugars to a json_extract/json_value call and routes through the
    // generic expression-comparison path.
    if (p.cur.tag == .arrow or p.cur.tag == .arrow2) {
        const lhs = try p.consumeJsonArrows(.{ .col_ref = col_dup });
        if (try parseComparisonTail(p, lhs)) |pred| return pred;
        return try makeExprComparisonPredicate(p, lhs, .eq, .{ .lit = .{ .boolean = true } });
    }

    // Aggregate reference inside a predicate — only meaningful in HAVING
    // (e.g. `HAVING COUNT(*) > 100000`). Canonicalize to the aggregate's
    // output-column name; a post-parse pass rewrites it to the matching
    // SELECT aggregate's alias.
    if (p.cur.tag == .lparen) {
        // `DATE_ADD(d, INTERVAL n DAY)` and its spellings aren't a plain
        // argument list, and never name an aggregate or window.
        if (p.scalarCallHasOwnSyntax(col_dup)) {
            return try parseExprOps(p, try p.parseScalarCallAfterName(col_dup));
        }
        var saw_distinct = false;
        const args = try p.parseCallArgList(col_dup, &saw_distinct);
        // Window call in a predicate position — only where the projection's
        // hoisting channels are live (CASE WHEN conditions inside the select
        // list); WHERE/HAVING contexts keep rejecting OVER. Checked before
        // the aggregate arm so `SUM(x) OVER (...)` hoists as a window.
        if (p.cur.tag == .kw_over and p.aggregateExprRefsEnabled()) {
            if (saw_distinct) return PE.SqlInvalidProjection;
            try p.advance();
            const spec_kind = try parse_window.parseWindowSpecOrRef(p);
            const wfunc = ir.windowFuncForName(col_dup) orelse return PE.SqlInvalidProjection;
            parse_window.validateWindowCall(wfunc, args, false) catch return PE.SqlInvalidProjection;
            const hidden = try p.materializeWindowExpr(.{
                .func = wfunc,
                .args = args,
                .ignore_nulls = false,
                .spec_kind = spec_kind,
            });
            const lhs = try p.continueBinaryFrom(.{ .col_ref = hidden });
            if (try parseComparisonTail(p, lhs)) |pred| return pred;
            const anchored = switch (lhs) {
                .col_ref => |c| c,
                else => try p.materializePredicateExpr(lhs),
            };
            return try parseColOps(p, anchored);
        }
        if (p.aggregateFuncForName(col_dup)) |func| {
            if (p.aggregateExprRefsEnabled()) {
                col_dup = try p.materializeAggregateExpr(col_dup, func, args, saw_distinct);
            } else {
                if (saw_distinct) return PE.SqlInvalidProjection;
                col_dup = try p.aggSortName(col_dup, args, false);
            }
        } else if (saw_distinct) {
            return PE.SqlInvalidProjection;
        } else if (std.ascii.eqlIgnoreCase(col_dup, "day") and args.len == 1 and args[0] == .col_ref and isComparisonToken(p.cur.tag) and p.cur.tag != .null_safe_eq) {
            return try makeDayComparison(p, args[0].col_ref);
        } else {
            return try parseExprOps(p, try p.makeScalarCallExpr(col_dup, args));
        }
    }

    // Arithmetic continuation from a bare column (`i + 1 > 3`,
    // `qty * 2 IN (...)`): the expression materializes to a hidden
    // computed column and the normal operator tail anchors to it.
    if (try isArithAhead(p)) {
        const lhs = try p.continueBinaryFrom(.{ .col_ref = col_dup });
        col_dup = try p.materializePredicateExpr(lhs);
    }
    return try parseColOps(p, col_dup);
}

/// A predicate whose left side is a whole scalar expression led by a
/// literal, a sign, or a parenthesized operand (`1 + x > 2`, `-x < 0`,
/// `(id) = 1`, `(a + b) * 2 > x`, `(SELECT ...) = 0`). The right side is a
/// whole scalar expression too. A side that parses to a lone literal keeps
/// its constant-aware form:
///   - lit op col   → flipped to `col reverse_op lit` as a normal leaf
///   - lit op lit   → evaluated at parse time when both literals share a
///     type, emitted as `.always`
///   - lit op NULL  → UNKNOWN
///   - lit IS [NOT] NULL → a literal is never NULL, so `.always`
///   - lit [NOT] BETWEEN / LIKE / IN → the literal anchors to a hidden
///     computed column and takes the column operator tail
/// `lit op @var` (e.g. `1 = @includeEstimates`) is a constant guard: the
/// var resolves to a literal pre-compile, so both sides materialize as
/// constant columns and the comparison keeps or drops every row.
fn parseScalarLhs(p: anytype) @TypeOf(p.*).Err!PredicateExpr {
    const lhs = try p.parseScalar();
    const lhs_val = switch (leafOperand(lhs)) {
        .lit => |v| v,
        else => return try parseExprOps(p, lhs),
    };
    switch (p.cur.tag) {
        .kw_is => return try parseIsOps(p, lhs),
        .kw_not, .kw_between, .kw_like, .kw_regexp, .kw_in => {
            return try parseColOps(p, try p.materializePredicateExpr(lhs));
        },
        else => if (try soundsLikeAhead(p)) return try parseColOps(p, try p.materializePredicateExpr(lhs)),
    }
    // A lone literal is truthiness, as a bare column is (`WHERE 1`).
    if (isPredicateEnd(p)) return try literalComparison(p, lhs_val, .neq, .{ .int = 0 });
    if (p.cur.tag == .null_safe_eq) return (try parseComparisonTail(p, lhs)).?;
    const op_lhs = try parseComparisonToken(p);
    const rhs = try p.parseScalar();
    return switch (leafOperand(rhs)) {
        .col_ref => |col| .{ .leaf = .{ .col = col, .op = reverseOp(op_lhs), .val = lhs_val } },
        .lit => |rhs_val| try literalComparison(p, lhs_val, op_lhs, rhs_val),
        .null_lit => .unknown,
        else => try makeExprComparisonPredicate(p, lhs, op_lhs, rhs),
    };
}

/// The operator tail after a scalar expression (a call, a parenthesized
/// operand, or arithmetic led by a literal) on a predicate's left side.
fn parseExprOps(p: anytype, expr: ir.Expr) @TypeOf(p.*).Err!PredicateExpr {
    const lhs = try p.continueBinaryFrom(expr);
    if (try parseComparisonTail(p, lhs)) |pred| return pred;
    switch (p.cur.tag) {
        // `ABS(x) BETWEEN ...`, `fn(x) IN (...)`, `fn(x) IS NULL`: anchor the
        // call to a hidden computed column and reuse the operator tail. A
        // parenthesized bare column (`(s) IN (...)`) stays that column.
        .kw_is, .kw_not, .kw_between, .kw_like, .kw_regexp, .kw_in => {
            return try parseColOps(p, try anchorColumn(p, lhs));
        },
        // A bare expression is MySQL truthiness (`WHERE fn(x)`, `WHERE 1 + x`):
        // non-zero and non-NULL, as for a bare column.
        else => {
            if (try soundsLikeAhead(p)) return try parseColOps(p, try p.materializePredicateExpr(lhs));
            return try makeExprComparisonPredicate(p, lhs, .neq, .{ .lit = .{ .int = 0 } });
        },
    }
}

fn isArithToken(tag: anytype) bool {
    return switch (tag) {
        .plus, .minus, .star, .slash, .percent, .kw_div, .amp, .pipe, .caret, .shl, .shr => true,
        else => false,
    };
}

/// Whether a binary operator continues the operand before the cursor:
/// an operator token, or MySQL's `MOD` word.
fn isArithAhead(p: anytype) @TypeOf(p.*).Err!bool {
    return isArithToken(p.cur.tag) or try p.modOperatorAhead();
}

/// Tokens that close a predicate: the call/CASE punctuation around an IF
/// or WHEN condition, boolean connectives, and the clause keywords that
/// can follow a WHERE / HAVING.
fn isPredicateEnd(p: anytype) bool {
    return switch (p.cur.tag) {
        .rparen, .comma, .kw_and, .amp_amp, .kw_or, .kw_then, .eof, .semicolon, .kw_group, .kw_order, .kw_limit, .kw_having => true,
        .pipe_pipe => p.lex.dialect == .mysql,
        else => xorKeywordHere(p),
    };
}

/// What an `IS [NOT] ...` tests: NULL (UNKNOWN is its synonym), TRUE,
/// FALSE, or `DISTINCT FROM` another value.
const IsTest = enum { null, true, false, distinct_from };

const IsTail = struct { what: IsTest, negated: bool };

/// Consumes `IS [NOT]` and the tested word; after `DISTINCT FROM` the
/// cursor sits on the other value.
fn parseIsTail(p: anytype) @TypeOf(p.*).Err!IsTail {
    const PE = @TypeOf(p.*).Err;
    try p.expect(.kw_is);
    const negated = p.cur.tag == .kw_not;
    if (negated) try p.advance();
    const what: IsTest = switch (p.cur.tag) {
        .kw_null => .null,
        .kw_true => .true,
        .kw_false => .false,
        .kw_distinct => .distinct_from,
        .identifier => if (std.ascii.eqlIgnoreCase(p.cur.text, "unknown")) .null else return PE.SqlExpectedNull,
        else => return PE.SqlExpectedNull,
    };
    try p.advance();
    if (what == .distinct_from) try p.expect(.kw_from);
    return .{ .what = what, .negated = negated };
}

/// `x IS [NOT] NULL | UNKNOWN | TRUE | FALSE | DISTINCT FROM y`. None of
/// them is ever UNKNOWN: IS TRUE holds for a non-NULL non-zero value, so
/// its negation keeps the NULL rows.
fn parseIsOps(p: anytype, lhs: ir.Expr) @TypeOf(p.*).Err!PredicateExpr {
    const tail = try parseIsTail(p);
    if (tail.what == .distinct_from) {
        const same = try nullSafeEqual(p, lhs, try p.parseScalar());
        return if (tail.negated) same else try negatePredicate(p, same);
    }
    const operand = leafOperand(lhs);
    const tested: PredicateExpr = switch (operand) {
        .null_lit => .{ .always = tail.what == .null },
        .lit => |v| switch (tail.what) {
            .null => .{ .always = false },
            .true => if (literalTruth(v)) |t| .{ .always = t } else try literalComparison(p, v, .neq, .{ .int = 0 }),
            .false => if (literalTruth(v)) |t| .{ .always = !t } else try literalComparison(p, v, .eq, .{ .int = 0 }),
            .distinct_from => unreachable,
        },
        else => blk: {
            const col = try anchorColumn(p, operand);
            break :blk switch (tail.what) {
                .null => .{ .is_null = col },
                .true => try notNullAnd(p, col, .{ .leaf = .{ .col = col, .op = .neq, .val = .{ .int = 0 } } }),
                .false => try notNullAnd(p, col, .{ .leaf = .{ .col = col, .op = .eq, .val = .{ .int = 0 } } }),
                .distinct_from => unreachable,
            };
        },
    };
    return if (tail.negated) try negatePredicate(p, tested) else tested;
}

fn literalTruth(v: Value) ?bool {
    return switch (v) {
        .boolean => |b| b,
        .int => |x| x != 0,
        .bigint => |x| x != 0,
        .double => |x| x != 0,
        else => null,
    };
}

/// `pred` guarded by `col IS NOT NULL`, so that negating it keeps the
/// NULL rows: the leaf kernels exclude NULL whatever the operator.
fn notNullAnd(p: anytype, col: []const u8, pred: PredicateExpr) @TypeOf(p.*).Err!PredicateExpr {
    const kids = try p.arena.alloc(PredicateExpr, 2);
    kids[0] = .{ .is_not_null = col };
    kids[1] = pred;
    return .{ .@"and" = kids };
}

/// The column a predicate tests for an operand: itself, or a hidden
/// computed column holding the expression.
fn anchorColumn(p: anytype, e: ir.Expr) @TypeOf(p.*).Err![]const u8 {
    return switch (e) {
        .col_ref => |c| c,
        else => try p.materializePredicateExpr(e),
    };
}

/// `a <=> b` (IS NOT DISTINCT FROM): equal, or both NULL. It is never
/// UNKNOWN, so its negation keeps the rows where only one side is NULL.
fn nullSafeEqual(p: anytype, lhs_expr: ir.Expr, rhs_expr: ir.Expr) @TypeOf(p.*).Err!PredicateExpr {
    const lhs = leafOperand(lhs_expr);
    const rhs = leafOperand(rhs_expr);
    if (lhs == .null_lit or rhs == .null_lit) {
        const other = if (lhs == .null_lit) rhs else lhs;
        return switch (other) {
            .null_lit => .{ .always = true },
            .lit => .{ .always = false },
            else => .{ .is_null = try anchorColumn(p, other) },
        };
    }
    if (lhs == .lit and rhs == .lit) return try literalComparison(p, lhs.lit, .eq, rhs.lit);
    if (lhs == .lit or rhs == .lit) {
        const col = try anchorColumn(p, if (lhs == .lit) rhs else lhs);
        const val = if (lhs == .lit) lhs.lit else rhs.lit;
        return try notNullAnd(p, col, .{ .leaf = .{ .col = col, .op = .eq, .val = val } });
    }
    const a = try anchorColumn(p, lhs);
    const b = try anchorColumn(p, rhs);
    const both_null = try p.arena.alloc(PredicateExpr, 2);
    both_null[0] = .{ .is_null = a };
    both_null[1] = .{ .is_null = b };
    const equal = try p.arena.alloc(PredicateExpr, 3);
    equal[0] = .{ .is_not_null = a };
    equal[1] = .{ .is_not_null = b };
    equal[2] = .{ .leaf_col_col = .{ .left = a, .op = .eq, .right = b } };
    const kids = try p.arena.alloc(PredicateExpr, 2);
    kids[0] = .{ .@"and" = both_null };
    kids[1] = .{ .@"and" = equal };
    return .{ .@"or" = kids };
}

/// A comparison operator and its right operand after `lhs`, or null when
/// no comparison operator sits at the cursor.
fn parseComparisonTail(p: anytype, lhs: ir.Expr) @TypeOf(p.*).Err!?PredicateExpr {
    if (p.cur.tag == .null_safe_eq) {
        try p.advance();
        return try nullSafeEqual(p, lhs, try p.parseScalar());
    }
    if (!isComparisonToken(p.cur.tag)) return null;
    const op = try parseComparisonToken(p);
    return try makeExprComparisonPredicate(p, lhs, op, try p.parseScalar());
}

/// MySQL's `a SOUNDS LIKE b`, which is `SOUNDEX(a) = SOUNDEX(b)`, at the
/// cursor. SOUNDS lexes as an identifier.
fn soundsLikeAhead(p: anytype) @TypeOf(p.*).Err!bool {
    if (p.cur.tag != .identifier or !std.ascii.eqlIgnoreCase(p.cur.text, "sounds")) return false;
    var look = p.lex.*;
    return (try look.next()).tag == .kw_like;
}

fn soundexCall(p: anytype, arg: ir.Expr) @TypeOf(p.*).Err!ir.Expr {
    const args = try p.arena.alloc(ir.Expr, 1);
    args[0] = arg;
    return .{ .call = .{ .fn_name = try p.arena.dupe(u8, "soundex"), .args = args } };
}

/// The operator tail shared by every LHS that resolves to a column name —
/// plain columns, hidden computed columns, hidden window/aggregate outputs:
/// IS [NOT] NULL, [NOT] BETWEEN, [NOT] LIKE, [NOT] IN, comparisons.
fn parseColOps(p: anytype, col_dup: []const u8) @TypeOf(p.*).Err!PredicateExpr {
    const PE = @TypeOf(p.*).Err;

    if (p.cur.tag == .kw_is) return try parseIsOps(p, .{ .col_ref = col_dup });

    if (try soundsLikeAhead(p)) {
        try p.advance();
        try p.advance();
        const lhs = try soundexCall(p, .{ .col_ref = col_dup });
        return try makeExprComparisonPredicate(p, lhs, .eq, try soundexCall(p, try p.parseScalar()));
    }

    // Optional NOT — gates BETWEEN / LIKE / IN below.
    var negate_predicate = false;
    if (p.cur.tag == .kw_not) {
        try p.advance();
        negate_predicate = true;
    }

    // BETWEEN lo AND hi  →  (col >= lo) AND (col <= hi)
    // NOT BETWEEN        →  (col <  lo) OR  (col >  hi)
    if (p.cur.tag == .kw_between) {
        try p.advance();
        const lo = try p.parseScalar();
        if (p.cur.tag != .kw_and) return PE.SqlExpectedKeyword;
        try p.advance();
        const hi = try p.parseScalar();
        return try makeBetweenExpr(p, col_dup, lo, hi, negate_predicate);
    }

    // LIKE 'pattern'  /  NOT LIKE 'pattern'
    if (p.cur.tag == .kw_like) {
        try p.advance();
        if (p.cur.tag != .string) return PE.SqlExpectedValue;
        var pattern: []const u8 = try p.arena.dupe(u8, p.cur.value.string);
        try p.advance();
        if (p.cur.tag == .identifier and std.ascii.eqlIgnoreCase(p.cur.text, "escape")) {
            try p.advance();
            if (p.cur.tag != .string) return PE.SqlExpectedValue;
            const escape = p.cur.value.string;
            if (escape.len > 1) return PE.SqlExpectedValue;
            pattern = try likePatternWithEscape(p.arena, pattern, escape);
            try p.advance();
        }
        var pe: PredicateExpr = .{ .like = .{ .col = col_dup, .pattern = pattern } };
        if (negate_predicate) pe = try negatePredicate(p, pe);
        return pe;
    }

    // s REGEXP pattern / s RLIKE pattern (MySQL): regexp_like, a search
    // anywhere in s, as REGEXP_LIKE and MySQL both read it.
    if (p.cur.tag == .kw_regexp) {
        try p.advance();
        const args = try p.arena.alloc(ir.Expr, 2);
        args[0] = .{ .col_ref = col_dup };
        args[1] = try p.parseScalar();
        const call: ir.Expr = .{ .call = .{ .fn_name = try p.arena.dupe(u8, "regexp_like"), .args = args } };
        var pe = try makeExprComparisonPredicate(p, call, .neq, .{ .lit = .{ .int = 0 } });
        if (negate_predicate) pe = try negatePredicate(p, pe);
        return pe;
    }

    // IN (...) — three forms:
    //   - IN (SELECT ...)   → captured as .in_subquery, resolved later
    //   - IN (WITH ... SELECT ...) → same
    //   - IN (lit, lit, ...) → desugars to OR-chain of equality leaves
    // NOT IN wraps either form via .not (literal-list form) or via
    // the .in_subquery.negate flag (subquery form).
    if (p.cur.tag == .kw_in) {
        try p.advance();
        try p.expect(.lparen);
        if (p.cur.tag == .kw_select or p.cur.tag == .kw_with) {
            const source = try p.parseStatement();
            try p.expect(.rparen);
            return .{ .in_subquery = .{
                .col = col_dup,
                .source = @ptrCast(source),
                .negate = negate_predicate,
            } };
        }
        var values: std.ArrayList(Value) = .empty;
        defer values.deinit(p.arena);
        // Entries no Value holds exactly compare as the decimals they are.
        var inexact: std.ArrayList(PredicateExpr) = .empty;
        defer inexact.deinit(p.arena);
        var saw_value = false;
        while (true) {
            // NULL literals are dropped from the set in both IN and NOT IN
            // (the thinDB dialect — same treatment the subquery resolver
            // gives NULLs it drains; see thindb-not-in-nonstandard).
            if (p.cur.tag == .kw_null) {
                try p.advance();
                saw_value = true;
            } else if (try inexactFractionAhead(p)) {
                try inexact.append(p.arena, try makeComparisonExprPredicate(p, col_dup, .eq, try p.parseScalar()));
                saw_value = true;
            } else {
                const v = try p.parseValue();
                try values.append(p.arena, v);
                saw_value = true;
            }
            if (p.cur.tag != .comma) break;
            try p.advance();
        }
        try p.expect(.rparen);
        if (!saw_value) return PE.SqlExpectedValue;
        // Every entry was NULL: nothing can match IN (); the negated form
        // is vacuously true under the drop-NULLs dialect (negatePredicate
        // flips the .always).
        if (values.items.len + inexact.items.len == 0) {
            var pe: PredicateExpr = .{ .always = false };
            if (negate_predicate) pe = try negatePredicate(p, pe);
            return pe;
        }

        const kids = try p.arena.alloc(PredicateExpr, values.items.len + inexact.items.len);
        for (values.items, kids[0..values.items.len]) |v, *kid| {
            kid.* = .{ .leaf = .{ .col = col_dup, .op = .eq, .val = v } };
        }
        @memcpy(kids[values.items.len..], inexact.items);
        var pe: PredicateExpr = if (kids.len == 1) kids[0] else .{ .@"or" = kids };
        if (negate_predicate) pe = try negatePredicate(p, pe);
        return pe;
    }

    // Any other use of bare NOT inside parseAtom is a parse error —
    // boolean-level NOT was already consumed by parseNot.
    if (negate_predicate) return PE.SqlExpectedKeyword;

    // A bare column where the predicate ends (`IF(isActive, 1, 0)`,
    // `WHERE flag AND ...`) is MySQL truthiness: non-zero and non-NULL.
    if (isPredicateEnd(p)) {
        return .{ .leaf = .{ .col = col_dup, .op = .neq, .val = .{ .int = 0 } } };
    }
    if (p.cur.tag == .null_safe_eq) return (try parseComparisonTail(p, .{ .col_ref = col_dup })).?;

    // Comparison.
    const op: PredicateOp = switch (p.cur.tag) {
        .eq => .eq,
        .neq => .neq,
        .lt => .lt,
        .lte => .lte,
        .gt => .gt,
        .gte => .gte,
        else => return PE.SqlExpectedToken,
    };
    try p.advance();

    // Column-vs-column comparison: `col1 op col2`. Detected when the
    // RHS starts with a plain identifier rather than a literal —
    // EXCEPT for the temporal-literal keywords `DATE` / `DATETIME` /
    // `TIMESTAMP`, which `parseValue` claims (see below).
    if (p.cur.tag == .identifier and !isTypedLiteralKeyword(p.cur.text)) {
        const rhs_expr = try p.parseCallArg();
        return try makeComparisonExprPredicate(p, col_dup, op, rhs_expr);
    }

    // Scalar subquery on the RHS: `col cmp (SELECT ...)`. The parser
    // captures the inner Op; a pre-compile pass runs it once and
    // rewrites this predicate node into a `.leaf` literal.
    if (p.cur.tag == .lparen) {
        try p.advance();
        if (p.cur.tag == .kw_select or p.cur.tag == .kw_with) {
            const source = try p.parseStatement();
            try p.expect(.rparen);
            // `col > (SELECT ...) - 1`: the subquery is one operand of a
            // longer expression, compared like any other.
            if (isArithToken(p.cur.tag)) {
                const rhs = try p.continueBinaryFrom(.{ .scalar_subquery = @ptrCast(source) });
                return try makeComparisonExprPredicate(p, col_dup, op, rhs);
            }
            return .{ .scalar_subquery = .{
                .col = col_dup,
                .op = op,
                .source = @ptrCast(source),
            } };
        }
        // A parenthesized group that leads a longer expression
        // (`(a + b) * 2`) continues past its `)`.
        const group = try p.parseScalar();
        try p.expect(.rparen);
        return try makeComparisonExprPredicate(p, col_dup, op, try p.continueBinaryFrom(group));
    }

    // Session var on the RHS: `col op @name`. Build a leaf_var
    // placeholder; the pre-compile pass resolves it to a `.leaf`
    // using the active Session's vars map.
    if (p.cur.tag == .at_identifier) {
        const var_name = try p.arena.dupe(u8, p.cur.text);
        try p.advance();
        return .{ .leaf_var = .{ .col = col_dup, .op = op, .var_name = var_name } };
    }

    // Comparison against a NULL literal is UNKNOWN for every row (use
    // IS [NOT] NULL to test for NULLs). `.unknown` rather than
    // `.always = false` so NOT (v = NULL) stays UNKNOWN too.
    if (p.cur.tag == .kw_null) {
        try p.advance();
        return .unknown;
    }

    // A literal, or an expression a literal leads (`x > 1 + y`); a lone
    // literal stays a plain leaf.
    return try makeComparisonExprPredicate(p, col_dup, op, try p.parseScalar());
}

fn makeScalarExprPredicate(p: anytype, col: []const u8, op: PredicateOp, expr: ir.Expr) @TypeOf(p.*).Err!PredicateExpr {
    const value_name = try p.arena.dupe(u8, "__predicate_value");
    const single = try p.allocOp(.{ .single_row = {} });

    const derived = try p.arena.alloc(ir.Derived, 1);
    derived[0] = .{ .name = value_name, .expr = expr };
    const compute = try p.allocOp(.{ .compute = .{ .derived = derived, .upstream = single } });

    const cols = try p.arena.alloc([]const u8, 1);
    cols[0] = value_name;
    const select = try p.allocOp(.{ .select = .{ .columns = cols, .upstream = compute } });

    return .{ .scalar_subquery = .{
        .col = col,
        .op = op,
        .source = @ptrCast(select),
    } };
}

fn makeComparisonExprPredicate(p: anytype, col: []const u8, op: PredicateOp, expr: ir.Expr) @TypeOf(p.*).Err!PredicateExpr {
    return switch (leafOperand(expr)) {
        .col_ref => |rhs_dup| .{ .leaf_col_col = .{ .left = col, .op = op, .right = rhs_dup } },
        .lit => |val| .{ .leaf = .{ .col = col, .op = op, .val = val } },
        .null_lit => .unknown,
        else => blk: {
            if (p.predicateDerivedEnabled() and exprHasColumnRef(expr)) {
                const rhs_col = try p.materializePredicateExpr(expr);
                break :blk PredicateExpr{ .leaf_col_col = .{ .left = col, .op = op, .right = rhs_col } };
            }
            break :blk try makeScalarExprPredicate(p, col, op, expr);
        },
    };
}

fn exprHasColumnRef(expr: ir.Expr) bool {
    return switch (expr) {
        .col_ref => true,
        .call => |c| blk: {
            for (c.args) |arg| {
                if (exprHasColumnRef(arg)) break :blk true;
            }
            break :blk false;
        },
        .case => |c| blk: {
            for (c.branches) |branch| {
                if (predicateHasColumnRef(branch.cond)) break :blk true;
                if (exprHasColumnRef(branch.then)) break :blk true;
            }
            if (c.else_branch) |else_branch| {
                if (exprHasColumnRef(else_branch.*)) break :blk true;
            }
            break :blk false;
        },
        else => false,
    };
}

fn predicateHasColumnRef(pred: PredicateExpr) bool {
    return switch (pred) {
        .leaf, .day_leaf, .leaf_col_col, .is_null, .is_not_null, .like, .in_set, .text_as_number, .text_as_number_set, .leaf_var => true,
        .@"and", .@"or" => |children| blk: {
            for (children) |child| {
                if (predicateHasColumnRef(child)) break :blk true;
            }
            break :blk false;
        },
        .not => |child| predicateHasColumnRef(child.*),
        else => false,
    };
}

/// Whether the parenthesized group at the cursor is the operand of a value
/// operator: a comparison, IS, [NOT] IN / BETWEEN / LIKE / REGEXP, SOUNDS LIKE,
/// arithmetic, a JSON arrow or a cast follows its `)`. No predicate grammar
/// continues a boolean group with any of them, so the group is a value
/// whatever it holds: `(id) = 1`, `(UPPER(s)) LIKE 'A%'`, `(a + b) * 2 > x`,
/// and `(a > 1) IS TRUE` or `(a = 1) = 1`, where the condition is read as
/// a value (parseScalar's parenthesized value takes a predicate).
fn parenthesizedOperandAhead(p: anytype) @TypeOf(p.*).Err!bool {
    var look = p.lex.*;
    var depth: usize = 1;
    while (depth > 0) {
        switch ((try look.next()).tag) {
            .eof => return false,
            .lparen => depth += 1,
            .rparen => depth -= 1,
            else => {},
        }
    }
    const op_tok = try look.next();
    return isComparisonToken(op_tok.tag) or isArithToken(op_tok.tag) or switch (op_tok.tag) {
        .kw_between, .kw_in, .kw_is, .kw_like, .kw_regexp, .kw_not, .arrow, .arrow2 => true,
        .pipe_pipe, .coloncolon => look.dialect != .mysql,
        .identifier => std.ascii.eqlIgnoreCase(op_tok.text, "mod") or
            (std.ascii.eqlIgnoreCase(op_tok.text, "sounds") and (try look.next()).tag == .kw_like),
        else => false,
    };
}

/// A parenthesized list with a top-level comma, followed by a comparison or
/// [NOT] IN: `(a, b) = (1, 2)`, `(a, b) IN ((1, 2), (3, 4))`.
fn rowValueAhead(p: anytype) @TypeOf(p.*).Err!bool {
    var look = p.lex.*;
    var depth: usize = 1;
    var saw_comma = false;
    const first = try look.next();
    if (first.tag == .kw_select or first.tag == .kw_with) return false;
    var tok = first;
    while (true) : (tok = try look.next()) {
        switch (tok.tag) {
            .eof => return false,
            .lparen => depth += 1,
            .rparen => {
                depth -= 1;
                if (depth == 0) break;
            },
            .comma => if (depth == 1) {
                saw_comma = true;
            },
            else => {},
        }
    }
    if (!saw_comma) return false;
    const op_tok = try look.next();
    return isComparisonToken(op_tok.tag) or op_tok.tag == .kw_in or op_tok.tag == .kw_not;
}

/// Row values compare element by element, as MySQL defines them:
///   (a, b) = (x, y)   → a = x AND b = y
///   (a, b) <> (x, y)  → a <> x OR b <> y
///   (a, b) < (x, y)   → a < x OR (a = x AND b < y)
///   (a, b) IN (r1, r2) → (a, b) = r1 OR (a, b) = r2
/// `(a, b) IN (SELECT x, y ...)` keeps its subquery for the resolver, with
/// every element anchored to a column.
fn parseRowValuePredicate(p: anytype) @TypeOf(p.*).Err!PredicateExpr {
    const PE = @TypeOf(p.*).Err;
    const lhs = try parseRowValue(p);
    for (lhs) |*element| element.* = try anchorRowElement(p, element.*);
    if (p.cur.tag == .null_safe_eq) {
        try p.advance();
        const rhs = try parseRowValue(p);
        if (rhs.len != lhs.len) return PE.SqlRowValueWidthMismatch;
        const kids = try p.arena.alloc(PredicateExpr, lhs.len);
        for (lhs, rhs, kids) |l, r, *kid| kid.* = try nullSafeEqual(p, l, r);
        return .{ .@"and" = kids };
    }
    if (isComparisonToken(p.cur.tag)) {
        const op = try parseComparisonToken(p);
        const rhs = try parseRowValue(p);
        if (rhs.len != lhs.len) return PE.SqlRowValueWidthMismatch;
        return try rowComparison(p, lhs, op, rhs);
    }
    var negate = false;
    if (p.cur.tag == .kw_not) {
        try p.advance();
        negate = true;
    }
    if (p.cur.tag != .kw_in) return PE.SqlExpectedKeyword;
    try p.advance();
    try p.expect(.lparen);
    if (p.cur.tag == .kw_select or p.cur.tag == .kw_with) {
        const source = try p.parseStatement();
        try p.expect(.rparen);
        const cols = try p.arena.alloc([]const u8, lhs.len);
        for (lhs, cols) |element, *col| col.* = switch (element) {
            .col_ref => |name| name,
            else => try p.materializePredicateExpr(element),
        };
        return .{ .in_subquery = .{
            .col = cols[0],
            .source = @ptrCast(source),
            .negate = negate,
            .rest_cols = cols[1..],
        } };
    }
    var rows: std.ArrayList(PredicateExpr) = .empty;
    while (true) {
        const row = try parseRowValue(p);
        if (row.len != lhs.len) return PE.SqlRowValueWidthMismatch;
        try rows.append(p.arena, try rowComparison(p, lhs, .eq, row));
        if (p.cur.tag != .comma) break;
        try p.advance();
    }
    try p.expect(.rparen);
    var pe: PredicateExpr = if (rows.items.len == 1) rows.items[0] else .{ .@"or" = try rows.toOwnedSlice(p.arena) };
    if (negate) pe = try negatePredicate(p, pe);
    return pe;
}

fn parseRowValue(p: anytype) @TypeOf(p.*).Err![]ir.Expr {
    const PE = @TypeOf(p.*).Err;
    try p.expect(.lparen);
    if (p.cur.tag == .kw_select or p.cur.tag == .kw_with) return PE.SqlExpectedValue;
    var elements: std.ArrayList(ir.Expr) = .empty;
    while (true) {
        try elements.append(p.arena, try p.parseScalar());
        if (p.cur.tag != .comma) break;
        try p.advance();
    }
    try p.expect(.rparen);
    return try elements.toOwnedSlice(p.arena);
}

/// A left-hand element takes part in several comparisons, so an expression
/// is computed once, as a hidden column.
fn anchorRowElement(p: anytype, element: ir.Expr) @TypeOf(p.*).Err!ir.Expr {
    return switch (element) {
        .col_ref, .lit, .null_lit => element,
        else => .{ .col_ref = try p.materializePredicateExpr(element) },
    };
}

fn rowComparison(p: anytype, lhs: []const ir.Expr, op: PredicateOp, rhs: []const ir.Expr) @TypeOf(p.*).Err!PredicateExpr {
    const last = lhs.len - 1;
    switch (op) {
        .eq, .neq => {
            const kids = try p.arena.alloc(PredicateExpr, lhs.len);
            for (lhs, rhs, kids) |l, r, *kid| kid.* = try elementComparison(p, l, op, r);
            return if (op == .eq) .{ .@"and" = kids } else .{ .@"or" = kids };
        },
        .lt, .lte, .gt, .gte => {
            const strict: PredicateOp = switch (op) {
                .lt, .lte => .lt,
                else => .gt,
            };
            var acc = try elementComparison(p, lhs[last], op, rhs[last]);
            var i = last;
            while (i > 0) {
                i -= 1;
                const tie = try p.arena.alloc(PredicateExpr, 2);
                tie[0] = try elementComparison(p, lhs[i], .eq, rhs[i]);
                tie[1] = acc;
                const either = try p.arena.alloc(PredicateExpr, 2);
                either[0] = try elementComparison(p, lhs[i], strict, rhs[i]);
                either[1] = .{ .@"and" = tie };
                acc = .{ .@"or" = either };
            }
            return acc;
        },
    }
}

fn elementComparison(p: anytype, lhs_operand: ir.Expr, op: PredicateOp, rhs_operand: ir.Expr) @TypeOf(p.*).Err!PredicateExpr {
    const lhs = leafOperand(lhs_operand);
    const rhs = leafOperand(rhs_operand);
    if (lhs == .null_lit or rhs == .null_lit) return .unknown;
    if (lhs == .lit) switch (rhs) {
        .lit => |rhs_val| return try literalComparison(p, lhs.lit, op, rhs_val),
        .col_ref => |col| return .{ .leaf = .{ .col = col, .op = reverseOp(op), .val = lhs.lit } },
        else => {},
    };
    return try makeExprComparisonPredicate(p, lhs, op, rhs);
}

fn parseParenthesizedScalarComparison(p: anytype) @TypeOf(p.*).Err!PredicateExpr {
    var look = p.lex.*;
    const first = try look.next();
    const lhs = if (first.tag == .kw_select or first.tag == .kw_with)
        try p.parseScalar()
    else blk: {
        try p.expect(.lparen);
        const group = try p.parseScalar();
        try p.expect(.rparen);
        break :blk try p.continueBinaryFrom(group);
    };
    if (try parseComparisonTail(p, lhs)) |pred| return pred;
    const anchored = switch (lhs) {
        .col_ref => |c| c,
        else => try p.materializePredicateExpr(lhs),
    };
    return try parseColOps(p, anchored);
}

pub fn makeExprComparisonPredicate(p: anytype, lhs: ir.Expr, op: PredicateOp, rhs: ir.Expr) @TypeOf(p.*).Err!PredicateExpr {
    const lhs_col = switch (lhs) {
        .col_ref => |c| c,
        else => try p.materializePredicateExpr(lhs),
    };
    return switch (leafOperand(rhs)) {
        .col_ref => |rhs_col| .{ .leaf_col_col = .{ .left = lhs_col, .op = op, .right = rhs_col } },
        .lit => |val| .{ .leaf = .{ .col = lhs_col, .op = op, .val = val } },
        .null_lit => .unknown,
        else => blk: {
            const rhs_col = try p.materializePredicateExpr(rhs);
            break :blk PredicateExpr{ .leaf_col_col = .{ .left = lhs_col, .op = op, .right = rhs_col } };
        },
    };
}

/// The token at the cursor, or the one after it when the cursor is a sign.
pub fn unsignedTokenAhead(p: anytype) @TypeOf(p.*).Err!@TypeOf(p.cur) {
    if (p.cur.tag != .minus and p.cur.tag != .plus) return p.cur;
    var look = p.lex.*;
    return try look.next();
}

/// Whether the value at the cursor is a signed or unsigned fractional
/// literal whose digits no DOUBLE holds (`exec_expr.fractionFitsDouble`).
pub fn inexactFractionAhead(p: anytype) @TypeOf(p.*).Err!bool {
    const tok = try unsignedTokenAhead(p);
    return tok.tag == .floating and !exec_expr.fractionFitsDouble(tok.text, tok.value.floating);
}

fn isComparisonToken(tag: anytype) bool {
    return switch (tag) {
        .eq, .neq, .lt, .lte, .gt, .gte, .null_safe_eq => true,
        else => false,
    };
}

/// A LIKE pattern with its own ESCAPE character, rewritten to the one the
/// matcher reads, backslash (MySQL's default): the escape before any
/// character becomes a backslash, and a literal backslash doubles. An empty
/// ESCAPE turns escaping off.
fn likePatternWithEscape(arena: std.mem.Allocator, pattern: []const u8, escape: []const u8) std.mem.Allocator.Error![]const u8 {
    if (escape.len == 1 and escape[0] == '\\') return pattern;
    var out: std.ArrayList(u8) = .empty;
    try out.ensureTotalCapacity(arena, pattern.len * 2);
    var i: usize = 0;
    while (i < pattern.len) : (i += 1) {
        const c = pattern[i];
        if (c == '\\') {
            out.appendSliceAssumeCapacity("\\\\");
        } else if (escape.len == 1 and c == escape[0] and i + 1 < pattern.len) {
            i += 1;
            out.appendAssumeCapacity('\\');
            out.appendAssumeCapacity(pattern[i]);
        } else {
            out.appendAssumeCapacity(c);
        }
    }
    return out.items;
}

fn parseComparisonToken(p: anytype) @TypeOf(p.*).Err!PredicateOp {
    const PE = @TypeOf(p.*).Err;
    const op: PredicateOp = switch (p.cur.tag) {
        .eq => .eq,
        .neq => .neq,
        .lt => .lt,
        .lte => .lte,
        .gt => .gt,
        .gte => .gte,
        else => return PE.SqlExpectedToken,
    };
    try p.advance();
    return op;
}

fn makeDayComparison(p: anytype, col: []const u8) @TypeOf(p.*).Err!PredicateExpr {
    const PE = @TypeOf(p.*).Err;
    const op: PredicateOp = switch (p.cur.tag) {
        .eq => .eq,
        .neq => .neq,
        .lt => .lt,
        .lte => .lte,
        .gt => .gt,
        .gte => .gte,
        else => return PE.SqlExpectedToken,
    };
    try p.advance();
    const rhs = try p.parseScalar();
    return switch (leafOperand(rhs)) {
        .lit => |val| .{ .day_leaf = .{ .col = try p.arena.dupe(u8, col), .op = op, .val = val } },
        else => blk: {
            const args = try p.arena.alloc(ir.Expr, 1);
            args[0] = ir.Expr{ .col_ref = try p.arena.dupe(u8, col) };
            const lhs = ir.Expr{ .call = .{
                .fn_name = try p.arena.dupe(u8, "day"),
                .args = args,
            } };
            break :blk try makeExprComparisonPredicate(p, lhs, op, rhs);
        },
    };
}

/// The column reference at the cursor, as `col` or `table.col`
/// (`Parser.dupQualifiedColRef`). Caller has already verified the
/// current token is `.identifier`.
pub fn parseQualifiedColRef(p: anytype) @TypeOf(p.*).Err![]const u8 {
    const first = p.cur.text;
    try p.advance();
    return try p.dupQualifiedColRef(first);
}

fn isTypedLiteralKeyword(s: []const u8) bool {
    return std.ascii.eqlIgnoreCase(s, "date") or
        std.ascii.eqlIgnoreCase(s, "datetime") or
        std.ascii.eqlIgnoreCase(s, "timestamp");
}

fn isLiteralLhsTokenStart(tag: anytype) bool {
    return switch (tag) {
        .integer, .big_integer, .floating, .string, .kw_true, .kw_false => true,
        else => false,
    };
}

fn reverseOp(op: PredicateOp) PredicateOp {
    return switch (op) {
        .eq => .eq,
        .neq => .neq,
        .lt => .gt,
        .lte => .gte,
        .gt => .lt,
        .gte => .lte,
    };
}

/// Compile-time comparison of two literal Values. Both sides must
/// share the same active tag (no widening). Returns error.Invalid on
/// any mismatch — the caller surfaces it as a parse error.
/// `lit op lit`: a constant when both literals share a type; otherwise the
/// engine's comparison coercion decides (`1 = 1.0`, `2.5 > 1`).
/// A comparison operand as the leaf builders take it: a decimal constant
/// compares as the Value that holds it exactly (`exec_expr.exactLiteralValue`),
/// so `x > 1.5` stays a leaf and keeps zonemap pruning. A fraction no double
/// holds stays an expression and compares exactly as a decimal.
fn leafOperand(e: ir.Expr) ir.Expr {
    if (exec_expr.decimalLiteral(e) == null) return e;
    return .{ .lit = exec_expr.exactLiteralValue(e) orelse return e };
}

fn literalComparison(p: anytype, lhs: Value, op: PredicateOp, rhs: Value) @TypeOf(p.*).Err!PredicateExpr {
    if (compareLiterals(lhs, op, rhs)) |result| return .{ .always = result } else |_| {}
    return try makeExprComparisonPredicate(p, .{ .lit = lhs }, op, .{ .lit = rhs });
}

fn compareLiterals(a: Value, op: PredicateOp, b: Value) !bool {
    if (std.meta.activeTag(a) != std.meta.activeTag(b)) return error.Invalid;
    const order = a.compare(b);
    return switch (op) {
        .eq => order == .eq,
        .neq => order != .eq,
        .lt => order == .lt,
        .lte => order != .gt,
        .gt => order == .gt,
        .gte => order != .lt,
    };
}

fn makeBetweenExpr(p: anytype, col: []const u8, lo: ir.Expr, hi: ir.Expr, negate: bool) @TypeOf(p.*).Err!PredicateExpr {
    const kids = try p.arena.alloc(PredicateExpr, 2);
    if (negate) {
        kids[0] = try makeComparisonExprPredicate(p, col, .lt, lo);
        kids[1] = try makeComparisonExprPredicate(p, col, .gt, hi);
        return .{ .@"or" = kids };
    }
    kids[0] = try makeComparisonExprPredicate(p, col, .gte, lo);
    kids[1] = try makeComparisonExprPredicate(p, col, .lte, hi);
    return .{ .@"and" = kids };
}
