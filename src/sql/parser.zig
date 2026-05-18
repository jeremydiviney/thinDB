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
    SqlExpectedJoinOn,
    SqlOnRefsUnknownTable,
    SqlOnNonEquiUnsupported,
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
        /// Scalar function call expression. Lowered to a Compute step
        /// before the final projection.
        expr: ir.Expr,
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

        // FROM clause — supports a single table or chained JOINs.
        if (self.cur.tag != .kw_from) return ParseError.SqlExpectedFrom;
        try self.advance();
        var root = try self.parseFromClause();

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
        const has_expr = blk: {
            for (proj) |p| switch (p.kind) {
                .expr => break :blk true,
                else => {},
            };
            break :blk false;
        };
        if (has_expr and (has_agg or group_cols.len > 0)) {
            // v1 keeps these mutually exclusive — scalar expressions
            // combined with aggregation needs a clear pre-vs-post
            // aggregate decision the parser doesn't yet make.
            return ParseError.SqlMixedAggAndPlainProjection;
        }

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
                .expr => unreachable, // gated above (has_expr & has_agg/group_cols rejected)
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
            // Non-aggregated. ORDER BY applies BEFORE the Project so it
            // can reference any column from the input schema. If we have
            // scalar expressions, materialize them via a Compute step
            // BEFORE the OrderBy (so OrderBy can reference computed
            // column aliases too).
            if (has_expr) {
                var derived_buf: std.ArrayList(ir.Derived) = .empty;
                for (proj) |p| switch (p.kind) {
                    .expr => |e| try derived_buf.append(self.arena, .{ .name = p.name, .expr = e }),
                    else => {},
                };
                const derived_slice = try derived_buf.toOwnedSlice(self.arena);
                root = try self.allocOp(.{ .compute = .{ .derived = derived_slice, .upstream = root } });
            }
            if (pending_order_specs) |specs| {
                root = try self.allocOp(.{ .order_by = .{ .specs = specs, .upstream = root } });
            }
            // Plain projection: select the listed columns. `SELECT *`
            // is encoded as an empty proj slice → no Project node added.
            if (proj.len > 0) {
                const cols = try self.arena.alloc([]const u8, proj.len);
                for (proj, cols) |p, *out| {
                    switch (p.kind) {
                        .col => |c| out.* = c,
                        // Computed exprs surface under their derived name (alias).
                        .expr => out.* = p.name,
                        .agg => unreachable, // already handled by the agg branch
                    }
                }
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
        // Identifier or function call. Look ahead: `(` after an identifier
        // means a call.
        if (self.cur.tag != .identifier) return ParseError.SqlExpectedIdent;
        const first = self.cur.text;
        try self.advance();

        // Function call?
        if (self.cur.tag == .lparen) {
            // Try aggregate first; fall through to scalar otherwise.
            if (aggForName(first)) |func| {
                return try self.finishAggCall(first, func);
            }
            // Scalar function call → record as an Expr; lowered to a
            // Compute step later.
            const expr = try self.finishScalarCall(first);
            const default_name = try self.exprDefaultName(expr);
            const alias = try self.maybeAlias(default_name);
            return ProjItem{ .name = alias, .kind = .{ .expr = expr } };
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

    /// Finish parsing an aggregate call after seeing `funcname (`. Cursor
    /// is positioned on whatever follows the open paren.
    fn finishAggCall(self: *Parser, func_name: []const u8, func: ir.AggFunc) ParseError!ProjItem {
        try self.advance(); // consume '('
        var arg_col: ?[]const u8 = null;
        if (self.cur.tag == .star) {
            try self.advance();
        } else if (self.cur.tag == .identifier) {
            arg_col = try self.arena.dupe(u8, self.cur.text);
            try self.advance();
        } else return ParseError.SqlExpectedIdent;
        try self.expect(.rparen);
        const default_name = blk: {
            var buf: std.ArrayList(u8) = .empty;
            defer buf.deinit(self.arena);
            try buf.appendSlice(self.arena, func_name);
            try buf.append(self.arena, '(');
            try buf.appendSlice(self.arena, arg_col orelse "*");
            try buf.append(self.arena, ')');
            break :blk try buf.toOwnedSlice(self.arena);
        };
        const alias = try self.maybeAlias(default_name);
        return ProjItem{ .name = alias, .kind = .{ .agg = .{ .func = func, .col = arg_col } } };
    }

    /// Finish parsing a scalar function call (`funcname (args...)`) after
    /// the lookahead determines it isn't an aggregate. Returns the
    /// resulting Expr.
    ///
    /// v1 restriction: every arg must be a simple column reference. The
    /// Compute operator doesn't yet evaluate nested calls or literal
    /// args (those need an expression-evaluator extension; tracked as
    /// a follow-up). Reject them at parse time with a clear error.
    fn finishScalarCall(self: *Parser, func_name: []const u8) ParseError!ir.Expr {
        try self.advance(); // consume '('
        const fname_dup = try self.arena.dupe(u8, func_name);
        var args: std.ArrayList(ir.Expr) = .empty;
        if (self.cur.tag != .rparen) {
            while (true) {
                if (self.cur.tag != .identifier) return ParseError.SqlInvalidProjection;
                const a = try self.parseColRefExpr();
                try args.append(self.arena, a);
                if (self.cur.tag != .comma) break;
                try self.advance();
            }
        }
        try self.expect(.rparen);
        const args_slice = try args.toOwnedSlice(self.arena);
        return ir.Expr{ .call = .{ .fn_name = fname_dup, .args = args_slice } };
    }

    /// Parse a bare column reference (possibly qualified). Used inside
    /// scalar function arguments where v1 doesn't yet accept literals
    /// or nested calls.
    fn parseColRefExpr(self: *Parser) ParseError!ir.Expr {
        if (self.cur.tag != .identifier) return ParseError.SqlExpectedIdent;
        const name = self.cur.text;
        try self.advance();
        // Nested function call attempt → reject with the unified projection
        // error so callers see a clear "v1 doesn't support this" signal.
        if (self.cur.tag == .lparen) return ParseError.SqlInvalidProjection;
        // Qualified column? use last segment.
        var col_name = name;
        if (self.cur.tag == .dot) {
            try self.advance();
            if (self.cur.tag != .identifier) return ParseError.SqlExpectedIdent;
            col_name = self.cur.text;
            try self.advance();
        }
        return ir.Expr{ .col_ref = try self.arena.dupe(u8, col_name) };
    }

    /// Default name when a scalar expression has no AS alias — use the
    /// function's invocation text. For nested calls, the user really
    /// should provide an alias; we render a best-effort name from the
    /// outer call.
    fn exprDefaultName(self: *Parser, e: ir.Expr) ParseError![]const u8 {
        switch (e) {
            .col_ref => |c| return try self.arena.dupe(u8, c),
            .lit => return try self.arena.dupe(u8, "literal"),
            .call => |c| {
                var buf: std.ArrayList(u8) = .empty;
                defer buf.deinit(self.arena);
                try buf.appendSlice(self.arena, c.fn_name);
                try buf.append(self.arena, '(');
                for (c.args, 0..) |arg, i| {
                    if (i > 0) try buf.appendSlice(self.arena, ", ");
                    switch (arg) {
                        .col_ref => |cn| try buf.appendSlice(self.arena, cn),
                        else => try buf.appendSlice(self.arena, "..."),
                    }
                }
                try buf.append(self.arena, ')');
                return try buf.toOwnedSlice(self.arena);
            },
        }
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

    // -----------------------------------------------------------------------
    // FROM clause + JOIN chaining.
    //
    //   from_clause := ident (join_kind 'JOIN' ident 'ON' equi_conds)*
    //   join_kind   := 'INNER'? | 'LEFT' 'OUTER'? | 'RIGHT' 'OUTER'? | 'FULL' 'OUTER'?
    //   equi_conds  := qualified_col '=' qualified_col ('AND' qualified_col '=' qualified_col)*
    //
    // Each ON clause must be a conjunction of pure equi-conditions
    // between qualified column refs. The parser resolves which side
    // (left subtree vs new right table) each column belongs to by
    // tracking the running set of "left-side" table names. v1 doesn't
    // support inequality / range joins or general predicates in ON —
    // a follow-up that adds Op.Join.ranges + extra_predicate handling
    // will lift those restrictions.
    // -----------------------------------------------------------------------
    fn parseFromClause(self: *Parser) ParseError!*ir.Op {
        const first_table = try self.expectIdent();
        const first_dup = try self.arena.dupe(u8, first_table);
        var root = try self.allocOp(.{ .scan = .{ .table_name = first_dup } });

        // Running set of table names that constitute the current left
        // subtree. Each new JOIN's ON clause must reference one of
        // these + the new right table.
        var left_names: std.ArrayList([]const u8) = .empty;
        try left_names.append(self.arena, first_dup);
        defer left_names.deinit(self.arena);

        while (isJoinStart(self.cur.tag)) {
            const jtype = try self.parseJoinKind();
            const right_table = try self.expectIdent();
            const right_dup = try self.arena.dupe(u8, right_table);
            const right_scan = try self.allocOp(.{ .scan = .{ .table_name = right_dup } });

            if (self.cur.tag != .kw_on) return ParseError.SqlExpectedJoinOn;
            try self.advance();
            const pairs = try self.parseOnEquiJoin(left_names.items, right_dup);

            root = try self.allocOp(.{ .join = .{
                .algorithm = .auto,
                .join_type = jtype,
                .on = pairs,
                .ranges = &.{},
                .extra_predicate = null,
                .skew_ratio_threshold = 0.3,
                .skew_absolute_threshold = 20_000,
                .skew_sample_interval = 10,
                .left = root,
                .right = right_scan,
            } });
            try left_names.append(self.arena, right_dup);
        }
        return root;
    }

    fn isJoinStart(tag: TokenTag) bool {
        return switch (tag) {
            .kw_join, .kw_inner, .kw_left, .kw_right, .kw_full => true,
            else => false,
        };
    }

    fn parseJoinKind(self: *Parser) ParseError!ir.JoinType {
        // Default join kind is INNER. Modifiers: INNER, LEFT, RIGHT, FULL
        // (any of those may be followed by 'OUTER' which we accept and
        // ignore — it's syntactic noise per SQL standard).
        var jtype: ir.JoinType = .inner;
        switch (self.cur.tag) {
            .kw_inner => {
                try self.advance();
            },
            .kw_left => {
                jtype = .left;
                try self.advance();
                if (self.cur.tag == .kw_outer) try self.advance();
            },
            .kw_right => {
                jtype = .right;
                try self.advance();
                if (self.cur.tag == .kw_outer) try self.advance();
            },
            .kw_full => {
                jtype = .full;
                try self.advance();
                if (self.cur.tag == .kw_outer) try self.advance();
            },
            else => {},
        }
        if (self.cur.tag != .kw_join) return ParseError.SqlExpectedKeyword;
        try self.advance();
        return jtype;
    }

    fn parseOnEquiJoin(
        self: *Parser,
        left_table_names: []const []const u8,
        right_table_name: []const u8,
    ) ParseError![]const ir.JoinKeyPair {
        var pairs: std.ArrayList(ir.JoinKeyPair) = .empty;
        while (true) {
            // One condition: qualified.col '=' qualified.col
            const a_tbl = try self.expectIdent();
            try self.expect(.dot);
            const a_col = try self.expectIdent();
            if (self.cur.tag != .eq) return ParseError.SqlOnNonEquiUnsupported;
            try self.advance();
            const b_tbl = try self.expectIdent();
            try self.expect(.dot);
            const b_col = try self.expectIdent();

            // Resolve which side each column belongs to.
            const a_is_left = nameIn(a_tbl, left_table_names);
            const a_is_right = std.mem.eql(u8, a_tbl, right_table_name);
            const b_is_left = nameIn(b_tbl, left_table_names);
            const b_is_right = std.mem.eql(u8, b_tbl, right_table_name);

            const left_col_dup = try self.arena.dupe(u8, a_col);
            const right_col_dup = try self.arena.dupe(u8, b_col);

            if (a_is_left and b_is_right) {
                try pairs.append(self.arena, .{ .left = left_col_dup, .right = right_col_dup });
            } else if (a_is_right and b_is_left) {
                // Swap so canonical (left, right) ordering is preserved.
                try pairs.append(self.arena, .{ .left = right_col_dup, .right = left_col_dup });
            } else {
                return ParseError.SqlOnRefsUnknownTable;
            }

            if (self.cur.tag != .kw_and) break;
            try self.advance();
        }
        return try pairs.toOwnedSlice(self.arena);
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
    // Remaining items must be aggs (expr is rejected upstream when
    // combined with aggregation).
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

fn nameIn(needle: []const u8, names: []const []const u8) bool {
    for (names) |n| if (std.mem.eql(u8, n, needle)) return true;
    return false;
}
