//! SQL parser → IR Op tree. Hand-written recursive descent over the
//! lexer's token stream.
//!
//! Grammar (v1):
//!
//!   stmt          ::= select_stmt ';'?
//!   select_stmt   ::= 'SELECT' [DISTINCT] proj_list 'FROM' ident
//!                     ['WHERE' bool_expr]
//!                     ['GROUP' 'BY' ident (',' ident)*]
//!                     ['ORDER' 'BY' order_item (',' order_item)*]
//!                     ['LIMIT' integer]
//!   proj_list     ::= '*' | proj_item (',' proj_item)*
//!   proj_item     ::= ident ('.' ident)? [AS ident]
//!                   | agg_call [AS ident]
//!   agg_call      ::= ident '(' ('*' | ident) ')'
//!   order_item    ::= ident ['ASC'|'DESC']
//!   bool_expr     ::= or_expr
//!   or_expr       ::= and_expr ('OR' and_expr)*
//!   and_expr      ::= not_expr ('AND' not_expr)*
//!   not_expr      ::= 'NOT' not_expr | atom
//!   atom          ::= '(' bool_expr ')'
//!                   | qualified_col 'IS' ['NOT'] 'NULL'
//!                   | qualified_col cmp_op value
//!   cmp_op        ::= '=' | '!=' | '<' | '<=' | '>' | '>='
//!   value         ::= integer | floating | string | 'TRUE' | 'FALSE' | 'NULL'
//!
//! Deferred to follow-up:
//!   - JOIN clauses
//!   - HAVING
//!   - non-aggregate function calls in SELECT (would need a Compute step)
//!   - subqueries / CTEs (WITH)
//!   - qualified column refs in WHERE / GROUP BY (lexer accepts the dot;
//!     parser currently uses only the last identifier)

const std = @import("std");
const Allocator = std.mem.Allocator;

const lexer_mod = @import("lexer.zig");
const Lexer = lexer_mod.Lexer;
const Token = lexer_mod.Token;
const TokenTag = lexer_mod.TokenTag;
const LexError = lexer_mod.LexError;

const ir = @import("../ir/ir.zig");
const types = @import("../types.zig");
const Value = types.Value;
const exec_predicate = @import("../exec/predicate.zig");
const PredicateExpr = exec_predicate.PredicateExpr;
const PredicateOp = exec_predicate.PredicateOp;

pub const ParseError = error{
    SqlExpectedSelect,
    SqlExpectedFrom,
    SqlExpectedIdent,
    SqlExpectedKeyword,
    SqlExpectedToken,
    SqlExpectedValue,
    SqlExpectedNull,
    SqlExpectedAggKnown,
    SqlInvalidProjection,
    SqlMixedAggAndPlainProjection,
    SqlTrailingTokens,
} || LexError;

const AggNames = [_]struct { name: []const u8, func: ir.AggFunc }{
    .{ .name = "count", .func = .count },
    .{ .name = "sum", .func = .sum },
    .{ .name = "min", .func = .min },
    .{ .name = "max", .func = .max },
    .{ .name = "avg", .func = .avg },
    .{ .name = "stddev_pop", .func = .stddev_pop },
    .{ .name = "stddev_samp", .func = .stddev_samp },
    .{ .name = "var_pop", .func = .var_pop },
    .{ .name = "var_samp", .func = .var_samp },
    .{ .name = "count_distinct", .func = .count_distinct },
};

fn aggForName(name: []const u8) ?ir.AggFunc {
    for (AggNames) |entry| {
        if (std.ascii.eqlIgnoreCase(name, entry.name)) return entry.func;
    }
    return null;
}

pub fn parse(arena: Allocator, sql: []const u8) ParseError!*ir.Op {
    var lex = Lexer.init(arena, sql);
    var parser = Parser{ .arena = arena, .lex = &lex, .cur = try lex.next() };
    const op = try parser.parseStatement();
    // Optional trailing semicolon already consumed; everything after
    // it should be EOF.
    try parser.expectEof();
    return op;
}

const ProjItem = struct {
    /// Output name for this projected column (post-alias).
    name: []const u8,
    kind: union(enum) {
        /// Plain column reference. `column` is the column name.
        col: []const u8,
        /// Aggregate call. `agg_func` is the function; `agg_col` is the
        /// argument column name (null for COUNT(*)).
        agg: struct { func: ir.AggFunc, col: ?[]const u8 },
    },
};

const Parser = struct {
    arena: Allocator,
    lex: *Lexer,
    cur: Token,

    fn advance(self: *Parser) ParseError!void {
        self.cur = try self.lex.next();
    }

    fn expect(self: *Parser, tag: TokenTag) ParseError!void {
        if (self.cur.tag != tag) return ParseError.SqlExpectedToken;
        try self.advance();
    }

    fn expectEof(self: *Parser) ParseError!void {
        if (self.cur.tag == .semicolon) try self.advance();
        if (self.cur.tag != .eof) return ParseError.SqlTrailingTokens;
    }

    fn parseStatement(self: *Parser) ParseError!*ir.Op {
        if (self.cur.tag != .kw_select) return ParseError.SqlExpectedSelect;
        try self.advance();

        // Optional DISTINCT — recorded but not yet plumbed through
        // (would need a dedicated operator). Reject for now to avoid
        // silent incorrect results.
        if (self.cur.tag == .kw_distinct) return ParseError.SqlInvalidProjection;

        // Projection list.
        const proj = try self.parseProjection();

        // FROM ident.
        if (self.cur.tag != .kw_from) return ParseError.SqlExpectedFrom;
        try self.advance();
        const table_name = try self.expectIdent();

        // Build the bottom of the pipeline.
        var root = try self.allocOp(.{ .scan = .{ .table_name = try self.arena.dupe(u8, table_name) } });

        // Optional WHERE.
        if (self.cur.tag == .kw_where) {
            try self.advance();
            const pred = try self.parseBoolExpr();
            root = try self.allocOp(.{ .filter = .{ .predicate = pred, .upstream = root } });
        }

        // Optional GROUP BY.
        var group_cols: []const []const u8 = &.{};
        if (self.cur.tag == .kw_group) {
            try self.advance();
            try self.expect(.kw_by);
            group_cols = try self.parseIdentList();
        }

        // Decide between a Project, a Group-by, or a Group-by + Project
        // based on the projection list shape.
        const has_agg = blk: {
            for (proj) |p| switch (p.kind) {
                .agg => break :blk true,
                else => {},
            };
            break :blk false;
        };

        // Parse-time peek for ORDER BY and LIMIT clauses — we need to
        // know the pipeline shape before deciding where to insert
        // OrderBy. Specifically: for non-aggregated queries, OrderBy
        // must come BEFORE the Project so it sees the full upstream
        // schema (SQL's "ORDER BY references original columns" rule).
        // For aggregated queries it comes AFTER GroupBy because the
        // grouped schema is the only one available.
        var pending_order_specs: ?[]const @import("../exec/sort.zig").SortSpec = null;
        if (self.cur.tag == .kw_order) {
            try self.advance();
            try self.expect(.kw_by);
            pending_order_specs = try self.parseOrderBy();
        }

        // Optional LIMIT.
        var pending_limit: ?u64 = null;
        if (self.cur.tag == .kw_limit) {
            try self.advance();
            if (self.cur.tag != .integer) return ParseError.SqlExpectedValue;
            const n = self.cur.value.integer;
            try self.advance();
            if (n < 0) return ParseError.SqlExpectedValue;
            pending_limit = @intCast(n);
        }

        if (has_agg or group_cols.len > 0) {
            // Validate: every plain-col projection must be in group_cols.
            for (proj) |p| switch (p.kind) {
                .col => |c| {
                    var found = false;
                    for (group_cols) |g| if (std.mem.eql(u8, g, c)) {
                        found = true;
                        break;
                    };
                    if (!found) return ParseError.SqlMixedAggAndPlainProjection;
                },
                .agg => {},
            };

            // Build agg specs from the projection.
            var aggs_buf: std.ArrayList(ir.AggSpec) = .empty;
            for (proj) |p| switch (p.kind) {
                .agg => |a| try aggs_buf.append(self.arena, .{
                    .func = a.func,
                    .col = a.col,
                    .as = p.name,
                }),
                else => {},
            };
            const aggs_slice = try aggs_buf.toOwnedSlice(self.arena);
            root = try self.allocOp(.{ .group_by = .{
                .group_cols = group_cols,
                .aggs = aggs_slice,
                .upstream = root,
            } });

            // Apply ORDER BY on the grouped schema.
            if (pending_order_specs) |specs| {
                root = try self.allocOp(.{ .order_by = .{ .specs = specs, .upstream = root } });
            }

            // Reorder output to match the SELECT list if necessary. The
            // GroupBy emits group_cols first then aggs in registered order;
            // a Project on top reorders/keeps only the SELECT items.
            if (!projMatchesGroupByOrder(proj, group_cols)) {
                const out_names = try self.arena.alloc([]const u8, proj.len);
                for (proj, out_names) |p, *o| o.* = p.name;
                root = try self.allocOp(.{ .select = .{ .columns = out_names, .upstream = root } });
            }
        } else {
            // Non-aggregated. OrderBy applies BEFORE the Project so it
            // can reference any column from the input schema.
            if (pending_order_specs) |specs| {
                root = try self.allocOp(.{ .order_by = .{ .specs = specs, .upstream = root } });
            }
            // Plain projection: select the listed columns. `SELECT *`
            // is encoded as an empty proj slice → no Project node added.
            if (proj.len > 0) {
                const cols = try self.arena.alloc([]const u8, proj.len);
                for (proj, cols) |p, *out| out.* = p.kind.col;
                root = try self.allocOp(.{ .select = .{ .columns = cols, .upstream = root } });
            }
        }
        // Optional LIMIT applies last.
        if (pending_limit) |n| {
            root = try self.allocOp(.{ .limit = .{ .n = n, .upstream = root } });
        }

        return root;
    }

    fn parseProjection(self: *Parser) ParseError![]const ProjItem {
        // `SELECT *` → empty proj list (means "all columns from
        // upstream"); the Scan + downstream operators inherit the
        // table's full schema.
        if (self.cur.tag == .star) {
            try self.advance();
            return &.{};
        }
        var items: std.ArrayList(ProjItem) = .empty;
        defer items.deinit(self.arena);
        while (true) {
            const item = try self.parseProjItem();
            try items.append(self.arena, item);
            if (self.cur.tag != .comma) break;
            try self.advance();
        }
        return try items.toOwnedSlice(self.arena);
    }

    fn parseProjItem(self: *Parser) ParseError!ProjItem {
        // Identifier or aggregate call. Look two tokens ahead: if `(`
        // follows an identifier, it's a call.
        if (self.cur.tag != .identifier) return ParseError.SqlExpectedIdent;
        const first = self.cur.text;
        try self.advance();

        // Function call?
        if (self.cur.tag == .lparen) {
            try self.advance();
            const func = aggForName(first) orelse return ParseError.SqlExpectedAggKnown;
            // `count(*)` or `func(col)`.
            var arg_col: ?[]const u8 = null;
            if (self.cur.tag == .star) {
                try self.advance();
            } else if (self.cur.tag == .identifier) {
                arg_col = try self.arena.dupe(u8, self.cur.text);
                try self.advance();
            } else return ParseError.SqlExpectedIdent;
            try self.expect(.rparen);
            // Optional AS alias.
            const alias = try self.maybeAlias(blk: {
                // Default name: `func(arg)`.
                var buf: std.ArrayList(u8) = .empty;
                defer buf.deinit(self.arena);
                try buf.appendSlice(self.arena, first);
                try buf.append(self.arena, '(');
                try buf.appendSlice(self.arena, arg_col orelse "*");
                try buf.append(self.arena, ')');
                break :blk try buf.toOwnedSlice(self.arena);
            });
            return ProjItem{
                .name = alias,
                .kind = .{ .agg = .{ .func = func, .col = arg_col } },
            };
        }

        // Qualified column? `table.col` — for the parser's v1 we accept
        // the dotted form but use only the last segment (the engine
        // doesn't track per-table column qualification yet).
        var col_name = first;
        if (self.cur.tag == .dot) {
            try self.advance();
            if (self.cur.tag != .identifier) return ParseError.SqlExpectedIdent;
            col_name = self.cur.text;
            try self.advance();
        }
        const dup_col = try self.arena.dupe(u8, col_name);
        const alias = try self.maybeAlias(dup_col);
        return ProjItem{ .name = alias, .kind = .{ .col = dup_col } };
    }

    fn maybeAlias(self: *Parser, fallback: []const u8) ParseError![]const u8 {
        if (self.cur.tag == .kw_as) {
            try self.advance();
            const name = try self.expectIdent();
            return try self.arena.dupe(u8, name);
        }
        // Implicit alias: `expr alias_ident` (no AS keyword) — common
        // in MySQL/StarRocks. Only if next token is a bare identifier.
        if (self.cur.tag == .identifier) {
            // But we have to be careful — keywords like FROM are NOT
            // identifiers, so the lookahead naturally stops at them.
            const name = self.cur.text;
            try self.advance();
            return try self.arena.dupe(u8, name);
        }
        return fallback;
    }

    fn parseOrderBy(self: *Parser) ParseError![]const @import("../exec/sort.zig").SortSpec {
        const SortSpec = @import("../exec/sort.zig").SortSpec;
        var items: std.ArrayList(SortSpec) = .empty;
        defer items.deinit(self.arena);
        while (true) {
            if (self.cur.tag != .identifier) return ParseError.SqlExpectedIdent;
            const col = try self.arena.dupe(u8, self.cur.text);
            try self.advance();
            var desc = false;
            if (self.cur.tag == .kw_asc) {
                try self.advance();
            } else if (self.cur.tag == .kw_desc) {
                desc = true;
                try self.advance();
            }
            try items.append(self.arena, .{ .col = col, .desc = desc });
            if (self.cur.tag != .comma) break;
            try self.advance();
        }
        return try items.toOwnedSlice(self.arena);
    }

    fn parseIdentList(self: *Parser) ParseError![]const []const u8 {
        var items: std.ArrayList([]const u8) = .empty;
        defer items.deinit(self.arena);
        while (true) {
            const name = try self.expectIdent();
            try items.append(self.arena, try self.arena.dupe(u8, name));
            if (self.cur.tag != .comma) break;
            try self.advance();
        }
        return try items.toOwnedSlice(self.arena);
    }

    fn expectIdent(self: *Parser) ParseError![]const u8 {
        if (self.cur.tag != .identifier) return ParseError.SqlExpectedIdent;
        const name = self.cur.text;
        try self.advance();
        return name;
    }

    // -----------------------------------------------------------------------
    // WHERE / boolean expression parsing.
    // -----------------------------------------------------------------------

    fn parseBoolExpr(self: *Parser) ParseError!PredicateExpr {
        return try self.parseOr();
    }

    fn parseOr(self: *Parser) ParseError!PredicateExpr {
        var lhs = try self.parseAnd();
        while (self.cur.tag == .kw_or) {
            try self.advance();
            const rhs = try self.parseAnd();
            const children = try self.arena.alloc(PredicateExpr, 2);
            children[0] = lhs;
            children[1] = rhs;
            lhs = .{ .@"or" = children };
        }
        return lhs;
    }

    fn parseAnd(self: *Parser) ParseError!PredicateExpr {
        var lhs = try self.parseNot();
        while (self.cur.tag == .kw_and) {
            try self.advance();
            const rhs = try self.parseNot();
            const children = try self.arena.alloc(PredicateExpr, 2);
            children[0] = lhs;
            children[1] = rhs;
            lhs = .{ .@"and" = children };
        }
        return lhs;
    }

    fn parseNot(self: *Parser) ParseError!PredicateExpr {
        if (self.cur.tag == .kw_not) {
            try self.advance();
            const inner = try self.parseNot();
            const child = try self.arena.create(PredicateExpr);
            child.* = inner;
            return .{ .not = child };
        }
        return try self.parseAtom();
    }

    fn parseAtom(self: *Parser) ParseError!PredicateExpr {
        if (self.cur.tag == .lparen) {
            try self.advance();
            const inner = try self.parseOr();
            try self.expect(.rparen);
            return inner;
        }
        if (self.cur.tag != .identifier) return ParseError.SqlExpectedIdent;
        var col_name = self.cur.text;
        try self.advance();
        // Optional qualifier table.col — use last segment.
        if (self.cur.tag == .dot) {
            try self.advance();
            if (self.cur.tag != .identifier) return ParseError.SqlExpectedIdent;
            col_name = self.cur.text;
            try self.advance();
        }
        const col_dup = try self.arena.dupe(u8, col_name);

        // IS NULL / IS NOT NULL.
        if (self.cur.tag == .kw_is) {
            try self.advance();
            var negated = false;
            if (self.cur.tag == .kw_not) {
                negated = true;
                try self.advance();
            }
            if (self.cur.tag != .kw_null) return ParseError.SqlExpectedNull;
            try self.advance();
            return if (negated) .{ .is_not_null = col_dup } else .{ .is_null = col_dup };
        }

        // Comparison.
        const op: PredicateOp = switch (self.cur.tag) {
            .eq => .eq,
            .neq => .neq,
            .lt => .lt,
            .lte => .lte,
            .gt => .gt,
            .gte => .gte,
            else => return ParseError.SqlExpectedToken,
        };
        try self.advance();
        const val = try self.parseValue();
        return .{ .leaf = .{ .col = col_dup, .op = op, .val = val } };
    }

    fn parseValue(self: *Parser) ParseError!Value {
        const tok = self.cur;
        switch (tok.tag) {
            .integer => {
                try self.advance();
                const v = tok.value.integer;
                // Default literal type: int (i32). Promote to bigint
                // if out of i32 range.
                if (v >= std.math.minInt(i32) and v <= std.math.maxInt(i32)) {
                    return .{ .int = @intCast(v) };
                }
                return .{ .bigint = v };
            },
            .floating => {
                try self.advance();
                return .{ .double = tok.value.floating };
            },
            .string => {
                try self.advance();
                return .{ .text = try self.arena.dupe(u8, tok.value.string) };
            },
            .kw_true => {
                try self.advance();
                return .{ .boolean = true };
            },
            .kw_false => {
                try self.advance();
                return .{ .boolean = false };
            },
            else => return ParseError.SqlExpectedValue,
        }
    }

    fn allocOp(self: *Parser, op: ir.Op) ParseError!*ir.Op {
        const out = try self.arena.create(ir.Op);
        out.* = op;
        return out;
    }
};

/// Returns true when the projection's name+order already match the
/// natural output of a GroupBy: `[group_cols..., agg_aliases...]`.
fn projMatchesGroupByOrder(proj: []const ProjItem, group_cols: []const []const u8) bool {
    if (proj.len != group_cols.len + countAggs(proj)) return false;
    var i: usize = 0;
    // Group cols must come first, in order.
    while (i < group_cols.len) : (i += 1) {
        switch (proj[i].kind) {
            .col => |c| if (!std.mem.eql(u8, c, group_cols[i])) return false,
            else => return false,
        }
    }
    // Remaining items must be aggs.
    while (i < proj.len) : (i += 1) {
        switch (proj[i].kind) {
            .agg => {},
            else => return false,
        }
    }
    return true;
}

fn countAggs(proj: []const ProjItem) usize {
    var n: usize = 0;
    for (proj) |p| switch (p.kind) {
        .agg => n += 1,
        else => {},
    };
    return n;
}
