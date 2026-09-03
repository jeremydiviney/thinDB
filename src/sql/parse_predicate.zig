//! Boolean-expression parsing — predicates for WHERE / HAVING / QUALIFY
//! and CASE WHEN conditions. Extracted from parser.zig; uses the
//! `anytype` pattern (same as parse_window.zig and parse_ddl.zig) so
//! there's no circular import.
//!
//! Grammar:
//!   bool_expr  := or_expr
//!   or_expr    := and_expr ('OR' and_expr)*
//!   and_expr   := not_expr ('AND' not_expr)*
//!   not_expr   := 'NOT' not_expr | atom
//!   atom       := '(' or_expr ')'
//!                | qualified_col 'IS' ['NOT'] 'NULL'
//!                | qualified_col ['NOT'] 'BETWEEN' lit 'AND' lit
//!                | qualified_col ['NOT'] 'LIKE' string_lit
//!                | qualified_col ['NOT'] 'IN' '(' lit (',' lit)* ')'
//!                | qualified_col cmp_op lit

const std = @import("std");

const exec_predicate = @import("../exec/predicate.zig");
const PredicateExpr = exec_predicate.PredicateExpr;
const PredicateOp = exec_predicate.PredicateOp;

const types = @import("../types.zig");
const Value = types.Value;
const ir = @import("../ir/ir.zig");
const parse_window = @import("parse_window.zig");

pub fn parseBoolExpr(p: anytype) @TypeOf(p.*).Err!PredicateExpr {
    return try parseOr(p);
}

// The parseOr/parseAnd/parseNot/parseAtom quartet is mutually recursive;
// Zig can't infer error sets through a cycle, so all four return the
// Parser's concrete `Err` set explicitly.
pub fn parseOr(p: anytype) @TypeOf(p.*).Err!PredicateExpr {
    var lhs = try parseAnd(p);
    // On the MySQL wire `||` is a synonym for OR (PG/neutral reserve it for
    // string concatenation, handled in the expression parser).
    while (p.cur.tag == .kw_or or (p.cur.tag == .pipe_pipe and p.lex.dialect == .mysql)) {
        try p.advance();
        const rhs = try parseAnd(p);
        const children = try p.arena.alloc(PredicateExpr, 2);
        children[0] = lhs;
        children[1] = rhs;
        lhs = .{ .@"or" = children };
    }
    return lhs;
}

pub fn parseAnd(p: anytype) @TypeOf(p.*).Err!PredicateExpr {
    var lhs = try parseNot(p);
    while (p.cur.tag == .kw_and) {
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
fn negatePredicate(p: anytype, e: PredicateExpr) @TypeOf(p.*).Err!PredicateExpr {
    switch (e) {
        .leaf => |l| return .{ .leaf = .{ .col = l.col, .op = flipOp(l.op), .val = l.val } },
        .day_leaf => |l| return .{ .day_leaf = .{ .col = l.col, .op = flipOp(l.op), .val = l.val } },
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
        .in_subquery => |s| return .{ .in_subquery = .{ .col = s.col, .source = s.source, .negate = !s.negate } },
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
        if (try parenthesizedScalarComparisonAhead(p)) {
            return try parseParenthesizedScalarComparison(p);
        }
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
    // Literal-on-LHS comparison: `lit op X`. Two sub-cases handled:
    //   - lit op col   → flipped to `col reverse_op lit` as a normal leaf
    //   - lit op lit   → evaluated at parse time, emitted as `.always`
    // Subquery on either side of a literal-LHS comparison is rejected
    // — workaround is to write the column on the LHS.
    if (isLiteralLhsTokenStart(p.cur.tag)) {
        const lhs_val = try p.parseValue();
        const op_lhs: PredicateOp = switch (p.cur.tag) {
            .eq => .eq,
            .neq => .neq,
            .lt => .lt,
            .lte => .lte,
            .gt => .gt,
            .gte => .gte,
            else => return PE.SqlExpectedToken,
        };
        try p.advance();
        if (p.cur.tag == .identifier and !isTypedLiteralKeyword(p.cur.text)) {
            const col_dup = try parseQualifiedColRef(p);
            return .{ .leaf = .{ .col = col_dup, .op = reverseOp(op_lhs), .val = lhs_val } };
        }
        if (isLiteralTokenStart(p.cur.tag, p.cur.text)) {
            const rhs_val = try p.parseValue();
            const result = compareLiterals(lhs_val, op_lhs, rhs_val) catch return PE.SqlExpectedValue;
            return .{ .always = result };
        }
        // `lit op @var` (e.g. `1 = @includeEstimates`): a constant guard. Both
        // sides materialize as constant columns — the var resolves to a literal
        // in the pre-compile pass — so the comparison keeps or drops every row.
        if (p.cur.tag == .at_identifier) {
            const rhs_expr = try p.parseAddSub();
            return try makeExprComparisonPredicate(p, .{ .lit = lhs_val }, op_lhs, rhs_expr);
        }
        return PE.SqlExpectedValue;
    }
    // `@var op X` — a session var on the LHS (constant guard, e.g.
    // `@comparisonMonths > 1`). Symmetric to the literal-LHS form above: the
    // var resolves to a literal pre-compile, so both sides materialize as
    // constant columns. A bare `@var` is truthiness (`@var <> 0`).
    if (p.cur.tag == .at_identifier) {
        const var_name = try p.arena.dupe(u8, p.cur.text);
        try p.advance();
        const lhs_expr = ir.Expr{ .var_ref = var_name };
        if (isComparisonToken(p.cur.tag)) {
            const op = try parseComparisonToken(p);
            const rhs = try p.parseAddSub();
            return try makeExprComparisonPredicate(p, lhs_expr, op, rhs);
        }
        return try makeExprComparisonPredicate(p, lhs_expr, .neq, .{ .lit = .{ .int = 0 } });
    }
    if (p.cur.tag != .identifier) return PE.SqlExpectedIdent;
    var col_dup = try parseQualifiedColRef(p);

    // JSON extraction on the LHS: `doc->'$.x' op rhs` / `doc->>'$.x' op rhs`.
    // Desugars to a json_extract/json_value call and routes through the
    // generic expression-comparison path.
    if (p.cur.tag == .arrow or p.cur.tag == .arrow2) {
        const lhs = try p.consumeJsonArrows(.{ .col_ref = col_dup });
        if (!isComparisonToken(p.cur.tag)) {
            return try makeExprComparisonPredicate(p, lhs, .eq, .{ .lit = .{ .boolean = true } });
        }
        const op = try parseComparisonToken(p);
        const rhs = try p.parseAddSub();
        return try makeExprComparisonPredicate(p, lhs, op, rhs);
    }

    // Aggregate reference inside a predicate — only meaningful in HAVING
    // (e.g. `HAVING COUNT(*) > 100000`). Canonicalize to the aggregate's
    // output-column name; a post-parse pass rewrites it to the matching
    // SELECT aggregate's alias.
    if (p.cur.tag == .lparen) {
        var saw_distinct = false;
        const args = try p.parseCallArgList(&saw_distinct);
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
            if (isComparisonToken(p.cur.tag)) {
                const op = try parseComparisonToken(p);
                const rhs = try p.parseAddSub();
                return try makeExprComparisonPredicate(p, lhs, op, rhs);
            }
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
                col_dup = try p.aggSortName(col_dup, args);
            }
        } else if (saw_distinct) {
            return PE.SqlInvalidProjection;
        } else if (std.ascii.eqlIgnoreCase(col_dup, "day") and args.len == 1 and args[0] == .col_ref) {
            return try makeDayComparison(p, args[0].col_ref);
        } else {
            var lhs = ir.Expr{ .call = .{
                .fn_name = try p.arena.dupe(u8, col_dup),
                .args = args,
            } };
            lhs = try p.continueBinaryFrom(lhs);
            if (isComparisonToken(p.cur.tag)) {
                const op = try parseComparisonToken(p);
                const rhs = try p.parseAddSub();
                return try makeExprComparisonPredicate(p, lhs, op, rhs);
            }
            switch (p.cur.tag) {
                // `ABS(x) BETWEEN ...`, `fn(x) IN (...)`, `fn(x) IS NULL`:
                // anchor the call to a hidden computed column and reuse the
                // operator tail.
                .kw_is, .kw_not, .kw_between, .kw_like, .kw_in => {
                    const anchored = try p.materializePredicateExpr(lhs);
                    return try parseColOps(p, anchored);
                },
                // Bare call = truthiness (`WHERE fn(x)`).
                else => return try makeExprComparisonPredicate(
                    p,
                    lhs,
                    .eq,
                    .{ .lit = .{ .boolean = true } },
                ),
            }
        }
    }

    // Arithmetic continuation from a bare column (`i + 1 > 3`,
    // `qty * 2 IN (...)`): the expression materializes to a hidden
    // computed column and the normal operator tail anchors to it.
    if (isArithToken(p.cur.tag)) {
        const lhs = try p.continueBinaryFrom(.{ .col_ref = col_dup });
        col_dup = try p.materializePredicateExpr(lhs);
    }
    return try parseColOps(p, col_dup);
}

fn isArithToken(tag: anytype) bool {
    return switch (tag) {
        .plus, .minus, .star, .slash, .percent, .kw_div => true,
        else => false,
    };
}

/// The operator tail shared by every LHS that resolves to a column name —
/// plain columns, hidden computed columns, hidden window/aggregate outputs:
/// IS [NOT] NULL, [NOT] BETWEEN, [NOT] LIKE, [NOT] IN, comparisons.
fn parseColOps(p: anytype, col_dup: []const u8) @TypeOf(p.*).Err!PredicateExpr {
    const PE = @TypeOf(p.*).Err;

    // IS NULL / IS NOT NULL.
    if (p.cur.tag == .kw_is) {
        try p.advance();
        var negated = false;
        if (p.cur.tag == .kw_not) {
            negated = true;
            try p.advance();
        }
        if (p.cur.tag != .kw_null) return PE.SqlExpectedNull;
        try p.advance();
        return if (negated) .{ .is_not_null = col_dup } else .{ .is_null = col_dup };
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
        const lo = try p.parseAddSub();
        if (p.cur.tag != .kw_and) return PE.SqlExpectedKeyword;
        try p.advance();
        const hi = try p.parseAddSub();
        return try makeBetweenExpr(p, col_dup, lo, hi, negate_predicate);
    }

    // LIKE 'pattern'  /  NOT LIKE 'pattern'
    if (p.cur.tag == .kw_like) {
        try p.advance();
        if (p.cur.tag != .string) return PE.SqlExpectedValue;
        const pattern = try p.arena.dupe(u8, p.cur.value.string);
        try p.advance();
        var pe: PredicateExpr = .{ .like = .{ .col = col_dup, .pattern = pattern } };
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
        var saw_value = false;
        while (true) {
            // NULL literals are dropped from the set in both IN and NOT IN
            // (the thinDB dialect — same treatment the subquery resolver
            // gives NULLs it drains; see thindb-not-in-nonstandard).
            if (p.cur.tag == .kw_null) {
                try p.advance();
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
        if (values.items.len == 0) {
            var pe: PredicateExpr = .{ .always = false };
            if (negate_predicate) pe = try negatePredicate(p, pe);
            return pe;
        }

        const kids = try p.arena.alloc(PredicateExpr, values.items.len);
        for (values.items, kids) |v, *kid| {
            kid.* = .{ .leaf = .{ .col = col_dup, .op = .eq, .val = v } };
        }
        var pe: PredicateExpr = if (kids.len == 1) kids[0] else .{ .@"or" = kids };
        if (negate_predicate) pe = try negatePredicate(p, pe);
        return pe;
    }

    // Any other use of bare NOT inside parseAtom is a parse error —
    // boolean-level NOT was already consumed by parseNot.
    if (negate_predicate) return PE.SqlExpectedKeyword;

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
        const saved = p.cur;
        try p.advance();
        if (p.cur.tag == .kw_select or p.cur.tag == .kw_with) {
            const source = try p.parseStatement();
            try p.expect(.rparen);
            return .{ .scalar_subquery = .{
                .col = col_dup,
                .op = op,
                .source = @ptrCast(source),
            } };
        }
        // Not a subquery — restore the `(` token so parseValue sees a
        // parenthesized literal. parseValue doesn't accept that today
        // (literals are bare); surface the same error parseValue would.
        const rhs_expr = try p.parseAddSub();
        try p.expect(.rparen);
        _ = saved;
        return try makeComparisonExprPredicate(p, col_dup, op, rhs_expr);
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

    const val = try p.parseValue();
    return .{ .leaf = .{ .col = col_dup, .op = op, .val = val } };
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
    return switch (expr) {
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
        .leaf, .day_leaf, .leaf_col_col, .is_null, .is_not_null, .like, .in_set, .leaf_var => true,
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

fn parenthesizedScalarComparisonAhead(p: anytype) @TypeOf(p.*).Err!bool {
    var look = p.lex.*;
    var depth: usize = 1;
    var saw_arithmetic = false;
    // `(CASE ... END) > x` carries no depth-1 arithmetic but is still a
    // scalar comparison: parseAddSub dispatches CASE, and no predicate
    // grammar accepts a CASE-led paren group, so this only widens parses.
    var case_start = false;
    var first_tok = true;
    while (true) {
        const tok = try look.next();
        if (first_tok) {
            first_tok = false;
            case_start = tok.tag == .kw_case;
        }
        switch (tok.tag) {
            .eof => return false,
            .lparen => depth += 1,
            .rparen => {
                depth -= 1;
                if (depth == 0) break;
            },
            .plus, .minus, .star, .slash, .percent, .kw_div => {
                if (depth == 1) saw_arithmetic = true;
            },
            else => {},
        }
    }
    if (!saw_arithmetic and !case_start) return false;
    const op_tok = try look.next();
    return isComparisonToken(op_tok.tag) or switch (op_tok.tag) {
        // `(expr) BETWEEN/IN/IS/LIKE/NOT ...` — the group anchors to a
        // hidden computed column and takes the normal operator tail.
        .kw_between, .kw_in, .kw_is, .kw_like, .kw_not => true,
        else => false,
    };
}

fn parseParenthesizedScalarComparison(p: anytype) @TypeOf(p.*).Err!PredicateExpr {
    try p.expect(.lparen);
    const lhs = try p.parseAddSub();
    try p.expect(.rparen);
    if (isComparisonToken(p.cur.tag)) {
        const op = try parseComparisonToken(p);
        const rhs = try p.parseAddSub();
        return try makeExprComparisonPredicate(p, lhs, op, rhs);
    }
    const anchored = switch (lhs) {
        .col_ref => |c| c,
        else => try p.materializePredicateExpr(lhs),
    };
    return try parseColOps(p, anchored);
}

fn makeExprComparisonPredicate(p: anytype, lhs: ir.Expr, op: PredicateOp, rhs: ir.Expr) @TypeOf(p.*).Err!PredicateExpr {
    const lhs_col = switch (lhs) {
        .col_ref => |c| c,
        else => try p.materializePredicateExpr(lhs),
    };
    return switch (rhs) {
        .col_ref => |rhs_col| .{ .leaf_col_col = .{ .left = lhs_col, .op = op, .right = rhs_col } },
        .lit => |val| .{ .leaf = .{ .col = lhs_col, .op = op, .val = val } },
        .null_lit => .unknown,
        else => blk: {
            const rhs_col = try p.materializePredicateExpr(rhs);
            break :blk PredicateExpr{ .leaf_col_col = .{ .left = lhs_col, .op = op, .right = rhs_col } };
        },
    };
}

fn isComparisonToken(tag: anytype) bool {
    return switch (tag) {
        .eq, .neq, .lt, .lte, .gt, .gte => true,
        else => false,
    };
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
    const rhs = try p.parseAddSub();
    return switch (rhs) {
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

/// Parse an `identifier (. identifier)?` column reference at the
/// current position. The two-segment form is preserved as the dotted
/// string `qualifier.col` so a downstream lookup against a scan
/// renamed via `FROM t AS alias` finds the right column. The plain
/// form is preserved verbatim. Caller has already verified the
/// current token is `.identifier`.
pub fn parseQualifiedColRef(p: anytype) @TypeOf(p.*).Err![]const u8 {
    const PE = @TypeOf(p.*).Err;
    const first = p.cur.text;
    try p.advance();
    if (p.cur.tag != .dot) {
        return try p.arena.dupe(u8, first);
    }
    try p.advance();
    if (p.cur.tag != .identifier) return PE.SqlExpectedIdent;
    const second = p.cur.text;
    try p.advance();
    const buf = try p.arena.alloc(u8, first.len + 1 + second.len);
    @memcpy(buf[0..first.len], first);
    buf[first.len] = '.';
    @memcpy(buf[first.len + 1 ..], second);
    return buf;
}

fn isTypedLiteralKeyword(s: []const u8) bool {
    return std.ascii.eqlIgnoreCase(s, "date") or
        std.ascii.eqlIgnoreCase(s, "datetime") or
        std.ascii.eqlIgnoreCase(s, "timestamp");
}

fn isLiteralTokenStart(tag: anytype, text: []const u8) bool {
    return switch (tag) {
        .integer, .floating, .string, .kw_true, .kw_false => true,
        .identifier => isTypedLiteralKeyword(text),
        else => false,
    };
}

fn isLiteralLhsTokenStart(tag: anytype) bool {
    return switch (tag) {
        .integer, .floating, .string, .kw_true, .kw_false => true,
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

fn makeBetween(p: anytype, col: []const u8, lo: Value, hi: Value, negate: bool) !PredicateExpr {
    const kids = try p.arena.alloc(PredicateExpr, 2);
    if (negate) {
        kids[0] = .{ .leaf = .{ .col = col, .op = .lt, .val = lo } };
        kids[1] = .{ .leaf = .{ .col = col, .op = .gt, .val = hi } };
        return .{ .@"or" = kids };
    }
    kids[0] = .{ .leaf = .{ .col = col, .op = .gte, .val = lo } };
    kids[1] = .{ .leaf = .{ .col = col, .op = .lte, .val = hi } };
    return .{ .@"and" = kids };
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
