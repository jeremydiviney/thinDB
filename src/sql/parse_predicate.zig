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
    while (p.cur.tag == .kw_or) {
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
        const child = try p.arena.create(PredicateExpr);
        child.* = inner;
        return .{ .not = child };
    }
    return try parseAtom(p);
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
            var rhs_col = p.cur.text;
            try p.advance();
            if (p.cur.tag == .dot) {
                try p.advance();
                if (p.cur.tag != .identifier) return PE.SqlExpectedIdent;
                rhs_col = p.cur.text;
                try p.advance();
            }
            const col_dup = try p.arena.dupe(u8, rhs_col);
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
    var col_name = p.cur.text;
    try p.advance();
    // Optional qualifier table.col — use last segment.
    if (p.cur.tag == .dot) {
        try p.advance();
        if (p.cur.tag != .identifier) return PE.SqlExpectedIdent;
        col_name = p.cur.text;
        try p.advance();
    }
    const col_dup = try p.arena.dupe(u8, col_name);

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
        if (negate_predicate) {
            const child = try p.arena.create(PredicateExpr);
            child.* = pe;
            pe = .{ .not = child };
        }
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
        if (negate_predicate) {
            const child = try p.arena.create(PredicateExpr);
            child.* = pe;
            pe = .{ .not = child };
        }
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
        var rhs_col = p.cur.text;
        try p.advance();
        if (p.cur.tag == .dot) {
            try p.advance();
            if (p.cur.tag != .identifier) return PE.SqlExpectedIdent;
            rhs_col = p.cur.text;
            try p.advance();
        }
        const rhs_dup = try p.arena.dupe(u8, rhs_col);
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

    const val = try p.parseValue();
    return .{ .leaf = .{ .col = col_dup, .op = op, .val = val } };
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
