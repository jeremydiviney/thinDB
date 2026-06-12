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
        .leaf_col_col => |c| return .{ .leaf_col_col = .{ .left = c.left, .op = flipOp(c.op), .right = c.right } },
        .is_null => |c| return .{ .is_not_null = c },
        .is_not_null => |c| return .{ .is_null = c },
        .always => |b| return .{ .always = !b },
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
    if (isLiteralTokenStart(p.cur.tag, p.cur.text)) {
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
        return PE.SqlExpectedValue;
    }
    if (p.cur.tag != .identifier) return PE.SqlExpectedIdent;
    var col_dup = try parseQualifiedColRef(p);

    // Aggregate reference inside a predicate — only meaningful in HAVING
    // (e.g. `HAVING COUNT(*) > 100000`). Canonicalize to the aggregate's
    // output-column name; a post-parse pass rewrites it to the matching
    // SELECT aggregate's alias.
    if (p.cur.tag == .lparen) {
        const args = try p.parseCallArgList(null);
        col_dup = try p.aggSortName(col_dup, args);
    }

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
        const lo = try p.parseValue();
        if (p.cur.tag != .kw_and) return PE.SqlExpectedKeyword;
        try p.advance();
        const hi = try p.parseValue();
        return try makeBetween(p, col_dup, lo, hi, negate_predicate);
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
        while (true) {
            const v = try p.parseValue();
            try values.append(p.arena, v);
            if (p.cur.tag != .comma) break;
            try p.advance();
        }
        try p.expect(.rparen);
        if (values.items.len == 0) return PE.SqlExpectedValue;

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
        const rhs_dup = try parseQualifiedColRef(p);
        return .{ .leaf_col_col = .{ .left = col_dup, .op = op, .right = rhs_dup } };
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
        _ = saved;
        return PE.SqlExpectedValue;
    }

    // Session var on the RHS: `col op @name`. Build a leaf_var
    // placeholder; the pre-compile pass resolves it to a `.leaf`
    // using the active Session's vars map.
    if (p.cur.tag == .at_identifier) {
        const var_name = try p.arena.dupe(u8, p.cur.text);
        try p.advance();
        return .{ .leaf_var = .{ .col = col_dup, .op = op, .var_name = var_name } };
    }

    const val = try p.parseValue();
    return .{ .leaf = .{ .col = col_dup, .op = op, .val = val } };
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
