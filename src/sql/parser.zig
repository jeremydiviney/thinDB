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
//!                     ['LIMIT' integer | 'LIMIT' 0 ',' integer]
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

const parse_window = @import("parse_window.zig");
pub const ParsedWindowCall = parse_window.ParsedWindowCall;
const parse_ddl = @import("parse_ddl.zig");
const parse_predicate = @import("parse_predicate.zig");

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
    SqlCteRedefined,
    SqlSubqueryNeedsAlias,
    /// COPY with a file-path source/target. thinDB only speaks
    /// `STDIN`/`STDOUT`; server-side file paths have auth implications
    /// we don't want to inherit from upstream PG.
    SqlCopyFileNotSupported,
    /// COPY with an unsupported FORMAT option (we only do text).
    SqlCopyUnsupportedFormat,
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

    // Skip leading empty statements (e.g. ";;SELECT ...").
    while (parser.cur.tag == .semicolon) try parser.advance();
    if (parser.cur.tag == .eof) return ParseError.SqlExpectedSelect;

    var statements: std.ArrayList(*ir.Op) = .empty;
    defer statements.deinit(arena);

    while (true) {
        const op = try parser.parseStatement();
        // Post-parse pass: refcount each *ir.Op reachable from this
        // statement's root, then wrap CTE roots in Materialize per
        // their hint. CTE scope is per-statement.
        try parser.applyAutoMaterialize(op);
        try statements.append(arena, op);
        // Reset CTE state — each statement parses with a fresh scope.
        parser.ctes.clearRetainingCapacity();
        parser.cte_roots.clearRetainingCapacity();

        // Consume any number of `;` and stop at EOF.
        var saw_sep = false;
        while (parser.cur.tag == .semicolon) {
            try parser.advance();
            saw_sep = true;
        }
        if (parser.cur.tag == .eof) break;
        if (!saw_sep) return ParseError.SqlTrailingTokens;
    }

    if (statements.items.len == 1) return statements.items[0];

    const owned = try arena.alloc(*ir.Op, statements.items.len);
    for (statements.items, 0..) |s, i| owned[i] = s;
    return try parser.allocOp(.{ .batch = .{ .statements = owned } });
}

const ProjItem = struct {
    /// Output name for this projected column (post-alias).
    name: []const u8,
    kind: union(enum) {
        /// Plain column reference. `column` is the column name.
        col: []const u8,
        /// Aggregate call. `agg_func` is the function; `agg_col` is
        /// the argument column name (null for COUNT(*) — or null
        /// when `arg_expr` is set and the actual argument is a
        /// computed expression). `arg_expr` is the source Expr when
        /// the user wrote `SUM(a * b)` or similar — the parser
        /// hoists it into a synthetic Compute column whose name then
        /// fills in `col` before the GroupBy is built.
        agg: struct { func: ir.AggFunc, col: ?[]const u8, arg_expr: ?ir.Expr = null },
        /// Scalar function call expression. Lowered to a Compute step
        /// before the final projection.
        expr: ir.Expr,
        /// Window function call. Lowered to a Window step + the
        /// projection picks up `name` from its output. The spec is
        /// stored inline at parse time; `applyWindowOps` deduplicates
        /// equivalent specs across all window calls in the SELECT and
        /// assigns each call a spec_idx.
        window: ParsedWindowCall,
    },
};


/// Per-CTE materialization hint from the SQL surface.
///   .auto  — refcount-driven: wrap when references ≥ 2.
///   .force — `MATERIALIZED` keyword: always wrap.
///   .never — `NOT MATERIALIZED` keyword: always inline.
pub const MaterializeHint = enum { auto, force, never };

const CteEntry = struct {
    op: *ir.Op,
    hint: MaterializeHint,
};

pub const Parser = struct {
    /// Exported so `parse_window.zig` (which takes the parser via `anytype`
    /// to avoid a circular import) can name our error set via
    /// `@TypeOf(p.*).Err`.
    pub const Err = ParseError;

    arena: Allocator,
    lex: *Lexer,
    cur: Token,
    /// CTE registry: name → resolved *ir.Op + materialization hint.
    /// Populated by `parseCteList`; consulted in `parseFromTarget`.
    /// Flat scope: nested SELECTs can reference outer CTEs but
    /// redefining an existing name errors.
    ctes: std.StringHashMapUnmanaged(CteEntry) = .empty,
    /// Ordered list of CTE root *Op for the post-parse auto-detect
    /// refcount pass. Order matches declaration in the WITH clause.
    cte_roots: std.ArrayListUnmanaged(*ir.Op) = .empty,
    /// Named windows declared in the trailing `WINDOW name AS (...)`
    /// clause of the current SELECT. Populated by `parseWindowClause`
    /// before projection lowering; consumed by `parseWindowSpecOrRef`
    /// when a function uses `OVER name`. Cleared at the end of each
    /// SELECT so different statements in a batch don't bleed into
    /// each other.
    named_windows: std.StringHashMapUnmanaged(ir.WindowSpec) = .empty,

    pub fn advance(self: *Parser) ParseError!void {
        self.cur = try self.lex.next();
    }

    pub fn expect(self: *Parser, tag: TokenTag) ParseError!void {
        if (self.cur.tag != tag) return ParseError.SqlExpectedToken;
        try self.advance();
    }

    fn expectEof(self: *Parser) ParseError!void {
        if (self.cur.tag == .semicolon) try self.advance();
        if (self.cur.tag != .eof) return ParseError.SqlTrailingTokens;
    }

    pub fn parseStatement(self: *Parser) ParseError!*ir.Op {
        // DDL / SHOW / INSERT are leading-keyword forms that don't combine
        // with WITH. They have no projection / FROM / WHERE / etc.;
        // dispatch before the SELECT-only path.
        switch (self.cur.tag) {
            .kw_create, .kw_drop, .kw_use => return try parse_ddl.parseDdl(self),
            .kw_show => return try parse_ddl.parseShow(self),
            .kw_insert => return try parse_ddl.parseInsert(self),
            .kw_copy => return try parse_ddl.parseCopy(self),
            .kw_set => return try self.parseSetVar(),
            .kw_delete => return try self.parseDelete(),
            .kw_update => return try self.parseUpdate(),
            else => {},
        }
        // Optional WITH clause: zero-or-more named CTEs precede the
        // main SELECT. Stored in self.ctes so the FROM clause can
        // resolve references.
        if (self.cur.tag == .kw_with) {
            try self.parseCteList();
        }
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

        // Optional HAVING — post-aggregate filter. Predicate may
        // reference grouped columns and aggregate aliases declared
        // in the projection. Validation happens at compile time
        // against the post-GroupBy output schema.
        var pending_having: ?PredicateExpr = null;
        if (self.cur.tag == .kw_having) {
            try self.advance();
            pending_having = try self.parseBoolExpr();
        }

        // Optional WINDOW clause — named windows declared here resolve
        // OVER name references seen in the projection. Comes after GROUP
        // BY per the SQL standard.
        if (self.cur.tag == .kw_window) {
            try parse_window.parseWindowClause(self);
        }

        // Optional QUALIFY <bool_expr> — Snowflake/BigQuery/DuckDB-style
        // post-window filter. Per the SQL extension, comes after WINDOW
        // and before ORDER BY. The predicate may reference window-output
        // aliases (since it's evaluated AFTER the Window step in the
        // pipeline); the engine validates column refs at compile time
        // against the post-window schema.
        var pending_qualify: ?PredicateExpr = null;
        if (self.cur.tag == .kw_qualify) {
            try self.advance();
            pending_qualify = try self.parseBoolExpr();
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
        const has_window = blk: {
            for (proj) |p| switch (p.kind) {
                .window => break :blk true,
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
        if (has_window and (has_agg or group_cols.len > 0)) {
            // Window functions on top of GROUP BY need an inner-subquery
            // shape that the parser doesn't yet emit. Reject explicitly.
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
            if (self.cur.tag == .comma) {
                // MySQL-style LIMIT offset,count. thinDB has no Offset
                // operator yet, but Workbench emits LIMIT 0,N for result
                // browsing; zero offset is exactly equivalent to LIMIT N.
                if (n != 0) return ParseError.SqlExpectedValue;
                try self.advance();
                if (self.cur.tag != .integer) return ParseError.SqlExpectedValue;
                const count = self.cur.value.integer;
                try self.advance();
                if (count < 0) return ParseError.SqlExpectedValue;
                pending_limit = @intCast(count);
            } else {
                pending_limit = @intCast(n);
            }
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
                // .expr and .window gated above (mixed with aggregation
                // is rejected) — reaching them here is a bug.
                .expr, .window => unreachable,
            };

            // Aggregate-on-expression hoist: any agg whose argument is
            // an Expr (e.g. SUM(a * b)) gets a synthetic Compute
            // column inserted before the GroupBy, and the AggSpec
            // references that synthetic name. Aggs whose arg is a
            // plain column ref pass through unchanged.
            var derived_buf: std.ArrayList(ir.Derived) = .empty;
            var synth_counter: usize = 0;
            var agg_cols: std.ArrayList(?[]const u8) = .empty;
            defer agg_cols.deinit(self.arena);
            for (proj) |p| switch (p.kind) {
                .agg => |a| {
                    if (a.arg_expr) |e| {
                        const owned_name = std.fmt.allocPrint(self.arena, "__agg_arg_{d}", .{synth_counter}) catch return ParseError.OutOfMemory;
                        synth_counter += 1;
                        try derived_buf.append(self.arena, .{ .name = owned_name, .expr = e });
                        try agg_cols.append(self.arena, owned_name);
                    } else {
                        try agg_cols.append(self.arena, a.col);
                    }
                },
                else => {},
            };
            if (derived_buf.items.len > 0) {
                const derived_slice = try derived_buf.toOwnedSlice(self.arena);
                root = try self.allocOp(.{ .compute = .{ .derived = derived_slice, .upstream = root } });
            }

            // Build agg specs from the projection.
            var aggs_buf: std.ArrayList(ir.AggSpec) = .empty;
            var agg_i: usize = 0;
            for (proj) |p| switch (p.kind) {
                .agg => |a| {
                    try aggs_buf.append(self.arena, .{
                        .func = a.func,
                        .col = agg_cols.items[agg_i],
                        .as = p.name,
                    });
                    agg_i += 1;
                },
                else => {},
            };
            const aggs_slice = try aggs_buf.toOwnedSlice(self.arena);
            root = try self.allocOp(.{ .group_by = .{
                .group_cols = group_cols,
                .aggs = aggs_slice,
                .upstream = root,
            } });

            // HAVING after GroupBy, before ORDER BY. Filter operator
            // sees the post-aggregate schema, so references to agg
            // aliases (`COUNT(*) > 10`-style — but as `cnt > 10` since
            // we already alias every agg) resolve cleanly.
            if (pending_having) |pred| {
                root = try self.allocOp(.{ .filter = .{ .predicate = pred, .upstream = root } });
            }

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
            // HAVING without GROUP BY / aggregates is rejected — would
            // be silently equivalent to WHERE, which masks user intent.
            if (pending_having != null) return ParseError.SqlInvalidProjection;
            // Non-aggregated. Pipeline shape:
            //   Compute(scalars) → Window(windows) → OrderBy → Project → Limit
            // Compute first so window args can reference computed columns;
            // Window before OrderBy so ORDER BY can reference window outputs;
            // Project last so it selects from the union of input + derived.
            if (has_expr) {
                var derived_buf: std.ArrayList(ir.Derived) = .empty;
                for (proj) |p| switch (p.kind) {
                    .expr => |e| try derived_buf.append(self.arena, .{ .name = p.name, .expr = e }),
                    else => {},
                };
                const derived_slice = try derived_buf.toOwnedSlice(self.arena);
                root = try self.allocOp(.{ .compute = .{ .derived = derived_slice, .upstream = root } });
            }
            if (has_window) {
                if (try buildWindowOp(self.arena, proj, root, &self.named_windows)) |win| {
                    root = win;
                }
            }
            if (pending_qualify) |pred| {
                // QUALIFY without any window in the SELECT is allowed
                // by most dialects (acts as a HAVING), but reject for
                // now — encourage users to use WHERE instead.
                if (!has_window) return ParseError.SqlInvalidProjection;
                root = try self.allocOp(.{ .filter = .{ .predicate = pred, .upstream = root } });
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
                        // Computed exprs and window outputs surface under
                        // their derived name (the alias).
                        .expr, .window => out.* = p.name,
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

        // UNION / UNION ALL chains. SQL semantics: left-associative;
        // every right-hand side is itself a full SELECT pipeline.
        // v1 only ships UNION ALL — UNION (distinct) errors with
        // SqlInvalidProjection until a dedup pass lands.
        while (self.cur.tag == .kw_union) {
            try self.advance();
            const all = self.cur.tag == .kw_all;
            if (all) try self.advance();
            if (!all) return ParseError.SqlInvalidProjection;
            const rhs = try self.parseStatement();
            root = try self.allocOp(.{ .set_union = .{
                .left = root,
                .right = rhs,
                .all = true,
            } });
        }

        return root;
    }


    pub fn parseTableRef(self: *Parser) ParseError!ir.TableRef {
        var parts_buf: [3][]const u8 = undefined;
        var n: usize = 0;
        if (self.cur.tag != .identifier) return ParseError.SqlExpectedIdent;
        parts_buf[0] = try self.arena.dupe(u8, self.cur.text);
        n = 1;
        try self.advance();
        while (self.cur.tag == .dot) {
            if (n == parts_buf.len) return ParseError.SqlExpectedIdent;
            try self.advance();
            if (self.cur.tag != .identifier) return ParseError.SqlExpectedIdent;
            parts_buf[n] = try self.arena.dupe(u8, self.cur.text);
            n += 1;
            try self.advance();
        }
        return switch (n) {
            1 => ir.TableRef{ .name = parts_buf[0] },
            2 => ir.TableRef{ .schema = parts_buf[0], .name = parts_buf[1] },
            3 => ir.TableRef{ .database = parts_buf[0], .schema = parts_buf[1], .name = parts_buf[2] },
            else => unreachable,
        };
    }


    pub fn dupedIdent(self: *Parser) ParseError![]const u8 {
        if (self.cur.tag != .identifier) return ParseError.SqlExpectedIdent;
        const out = try self.arena.dupe(u8, self.cur.text);
        try self.advance();
        return out;
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
        // Parenthesized expression at projection start: `(expr) [AS name]`.
        // Routes through the expression parser (which handles binary
        // operators) without going through the identifier path.
        if (self.cur.tag == .lparen) {
            const expr = try self.parseCallArg();
            const default_name = try self.exprDefaultName(expr);
            const alias = try self.maybeAlias(default_name);
            return ProjItem{ .name = alias, .kind = .{ .expr = expr } };
        }

        // CASE expression at projection start. Same routing — parseCallArg
        // would also handle it (parseCallAtom dispatches on CASE), but
        // taking it here lets the projection alias the result.
        if (self.cur.tag == .kw_case) {
            const expr = try self.parseCaseExpr();
            const default_name = try self.exprDefaultName(expr);
            const alias = try self.maybeAlias(default_name);
            return ProjItem{ .name = alias, .kind = .{ .expr = expr } };
        }

        // EXISTS (SELECT ...) at projection start. Routed via the
        // shared expr-atom parser; we just lift the resulting Expr
        // into an aliased .expr ProjItem.
        if (self.cur.tag == .kw_exists) {
            const expr = try self.parseCallAtom();
            const default_name = try self.exprDefaultName(expr);
            const alias = try self.maybeAlias(default_name);
            return ProjItem{ .name = alias, .kind = .{ .expr = expr } };
        }

        // EXTRACT(field FROM expr) — special function form. Detected
        // here before the identifier-then-`(` branch below misparses
        // the field name as a regular call arg.
        if (self.cur.tag == .identifier and std.ascii.eqlIgnoreCase(self.cur.text, "extract")) {
            const saved = self.cur;
            try self.advance();
            if (self.cur.tag == .lparen) {
                const expr = try self.parseExtractCall(saved);
                const default_name = try self.exprDefaultName(expr);
                const alias = try self.maybeAlias(default_name);
                return ProjItem{ .name = alias, .kind = .{ .expr = expr } };
            }
            // Bare `extract` as a column name — treat as col_ref.
            const dup_col = try self.arena.dupe(u8, saved.text);
            const alias = try self.maybeAlias(dup_col);
            return ProjItem{ .name = alias, .kind = .{ .col = dup_col } };
        }

        // Identifier or function call. Look ahead: `(` after an identifier
        // means a call.
        if (self.cur.tag != .identifier) return ParseError.SqlExpectedIdent;
        const first = self.cur.text;
        try self.advance();

        // Function call?
        if (self.cur.tag == .lparen) {
            // Parse the call shape (name + paren-wrapped args) once. Then
            // decide between aggregate / scalar / window based on what
            // follows.
            const args = try self.parseCallArgList();
            const fname_dup = try self.arena.dupe(u8, first);

            // Optional [IGNORE | RESPECT] NULLS between `)` and `OVER`.
            const ignore_nulls = try parse_window.parseIgnoreNulls(self);

            // OVER (...) makes this a window call regardless of which
            // function name was used. SUM / AVG / etc. become aggregate-
            // window flavors; ROW_NUMBER / LAG / FIRST_VALUE / etc.
            // require OVER and reject everywhere else.
            if (self.cur.tag == .kw_over) {
                try self.advance();
                const spec_kind = try parse_window.parseWindowSpecOrRef(self);
                const wfunc = ir.windowFuncForName(first) orelse
                    return ParseError.SqlInvalidProjection;
                parse_window.validateWindowCall(wfunc, args, ignore_nulls) catch
                    return ParseError.SqlInvalidProjection;
                const call: ParsedWindowCall = .{
                    .func = wfunc,
                    .args = args,
                    .ignore_nulls = ignore_nulls,
                    .spec_kind = spec_kind,
                };
                const default_name = try parse_window.defaultName(self.arena, first, args);
                const alias = try self.maybeAlias(default_name);
                return ProjItem{ .name = alias, .kind = .{ .window = call } };
            }

            // IGNORE NULLS without OVER is a parse error per SQL standard.
            if (ignore_nulls) return ParseError.SqlInvalidProjection;

            // Window-only functions (row_number, rank, lag, first_value,
            // etc.) require OVER. Reject them when used as a non-window
            // call so the error is clear at the call site instead of
            // surfacing as "unknown function" later.
            if (ir.windowFuncForName(first)) |wfunc| {
                if (!parse_window.isAggregateAlsoFunc(wfunc)) return ParseError.SqlInvalidProjection;
                // Aggregate-named functions (sum/avg/count/min/max) fall
                // through to the aggregate path below.
            }

            // Aggregate path: rebuild the (func, col) shape the existing
            // aggregate lowering expects. Aggregates only accept a single
            // column-ref arg (or `*` for COUNT(*)); reject anything else.
            if (aggForName(first)) |func| {
                return try self.aggCallFromArgs(first, func, args);
            }

            // Scalar function call. If a binary operator follows, the
            // whole thing is a binary expression with the call as the
            // leftmost operand — lift into a .expr ProjItem. Otherwise
            // stay as a bare scalar call.
            const scalar_atom = ir.Expr{ .call = .{ .fn_name = fname_dup, .args = args } };
            const expr = try self.continueBinaryFrom(scalar_atom);
            const default_name = try self.exprDefaultName(expr);
            const alias = try self.maybeAlias(default_name);
            return ProjItem{ .name = alias, .kind = .{ .expr = expr } };
        }

        // Qualified column? `table.col` — preserved as the dotted
        // string `qualifier.col` so a downstream lookup against a
        // scan renamed by `FROM t AS alias` finds the right column.
        const dup_col = blk: {
            if (self.cur.tag == .dot) {
                try self.advance();
                if (self.cur.tag != .identifier) return ParseError.SqlExpectedIdent;
                const second = self.cur.text;
                try self.advance();
                const buf = try self.arena.alloc(u8, first.len + 1 + second.len);
                @memcpy(buf[0..first.len], first);
                buf[first.len] = '.';
                @memcpy(buf[first.len + 1 ..], second);
                break :blk @as([]const u8, buf);
            }
            break :blk try self.arena.dupe(u8, first);
        };

        // If a binary operator follows the column ref, lift it into a
        // binary expression (e.g., `qty + 1`, `price * 1.05`).
        if (isBinaryOpToken(self.cur.tag)) {
            const col_atom = ir.Expr{ .col_ref = dup_col };
            const expr = try self.continueBinaryFrom(col_atom);
            const default_name = try self.exprDefaultName(expr);
            const alias = try self.maybeAlias(default_name);
            return ProjItem{ .name = alias, .kind = .{ .expr = expr } };
        }

        const alias = try self.maybeAlias(dup_col);
        return ProjItem{ .name = alias, .kind = .{ .col = dup_col } };
    }

    /// True when `tag` is one of the binary arithmetic operators we
    /// recognize in expression position (+ - * / %).
    fn isBinaryOpToken(tag: TokenTag) bool {
        return switch (tag) {
            .plus, .minus, .star, .slash, .percent => true,
            else => false,
        };
    }

    /// Given an already-parsed left operand `atom`, continue parsing a
    /// binary chain at the precedence-climbing levels. Used by
    /// `parseProjItem` (which has already consumed the leading
    /// identifier/call/dotted-col but not yet checked for an
    /// operator). Returns `atom` unchanged if no operator follows.
    fn continueBinaryFrom(self: *Parser, atom: ir.Expr) ParseError!ir.Expr {
        // First, extend the atom into a MulDiv-level expression
        // (same precedence the inner parseMulDiv would have produced).
        var lhs = atom;
        while (self.cur.tag == .star or self.cur.tag == .slash or self.cur.tag == .percent) {
            const fn_name: []const u8 = switch (self.cur.tag) {
                .star => "mul",
                .slash => "div",
                .percent => "mod",
                else => unreachable,
            };
            try self.advance();
            const rhs = try self.parseCallAtom();
            lhs = try self.makeBinary(fn_name, lhs, rhs);
        }
        // Then extend into an AddSub-level expression.
        while (self.cur.tag == .plus or self.cur.tag == .minus) {
            const is_minus = self.cur.tag == .minus;
            try self.advance();
            if (self.cur.tag == .kw_interval) {
                lhs = try self.applyInterval(lhs, is_minus);
            } else {
                const fn_name: []const u8 = if (is_minus) "sub" else "add";
                const rhs = try self.parseMulDiv();
                lhs = try self.makeBinary(fn_name, lhs, rhs);
            }
        }
        return lhs;
    }

    /// Cursor sits on the INTERVAL keyword. Consume the
    /// `INTERVAL '<integer>' (DAY|MONTH|YEAR)` form and rewrite to a
    /// calendar-aware scalar call on `lhs`. `negate` flips the sign
    /// for the `lhs - INTERVAL ...` form.
    fn applyInterval(self: *Parser, lhs: ir.Expr, negate: bool) ParseError!ir.Expr {
        try self.expect(.kw_interval);
        // Accept both `'90'` (string) and bare integer for the
        // quantity — MySQL / DuckDB use string, PG uses bare integer.
        var n: i64 = 0;
        if (self.cur.tag == .string) {
            const s = self.cur.value.string;
            n = std.fmt.parseInt(i64, s, 10) catch return ParseError.SqlExpectedValue;
            try self.advance();
        } else if (self.cur.tag == .integer) {
            n = self.cur.value.integer;
            try self.advance();
        } else return ParseError.SqlExpectedValue;
        if (negate) n = -n;

        if (self.cur.tag != .identifier) return ParseError.SqlExpectedIdent;
        const unit_word = self.cur.text;
        const fn_name: []const u8 = if (std.ascii.eqlIgnoreCase(unit_word, "day") or std.ascii.eqlIgnoreCase(unit_word, "days"))
            "date_add"
        else if (std.ascii.eqlIgnoreCase(unit_word, "month") or std.ascii.eqlIgnoreCase(unit_word, "months"))
            "date_add_months"
        else if (std.ascii.eqlIgnoreCase(unit_word, "year") or std.ascii.eqlIgnoreCase(unit_word, "years"))
            "date_add_years"
        else
            return ParseError.SqlExpectedKeyword;
        try self.advance();

        // Build a 2-arg call: fn(date_expr, n).
        const args = try self.arena.alloc(ir.Expr, 2);
        args[0] = lhs;
        if (n < std.math.minInt(i32) or n > std.math.maxInt(i32)) return ParseError.SqlExpectedValue;
        args[1] = ir.Expr{ .lit = .{ .int = @intCast(n) } };
        const name_dup = try self.arena.dupe(u8, fn_name);
        return ir.Expr{ .call = .{ .fn_name = name_dup, .args = args } };
    }

    /// Parse `(arg, arg, ...)`. Cursor is on `(` going in, on the token
    /// after `)` coming out. Returns the args slice. `*` is encoded as
    /// `Expr.col_ref = "*"` (downstream callers — aggregates — recognize
    /// the sentinel; everyone else rejects it).
    fn parseCallArgList(self: *Parser) ParseError![]const ir.Expr {
        try self.expect(.lparen);
        var args: std.ArrayList(ir.Expr) = .empty;
        if (self.cur.tag == .star) {
            try self.advance();
            try args.append(self.arena, ir.Expr{ .col_ref = "*" });
        } else if (self.cur.tag != .rparen) {
            while (true) {
                const a = try self.parseCallArg();
                try args.append(self.arena, a);
                if (self.cur.tag != .comma) break;
                try self.advance();
            }
        }
        try self.expect(.rparen);
        return try args.toOwnedSlice(self.arena);
    }

    /// Build a ProjItem.agg from a pre-parsed args slice. Aggregates
    /// accept either a single column-ref arg or `*` (COUNT only). The
    /// args have already been parsed via the generic call-args path
    /// (which leaves *-as-col_ref `"*"`).
    fn aggCallFromArgs(
        self: *Parser,
        func_name: []const u8,
        func: ir.AggFunc,
        args: []const ir.Expr,
    ) ParseError!ProjItem {
        if (args.len != 1) return ParseError.SqlInvalidProjection;
        var arg_col: ?[]const u8 = null;
        var arg_expr: ?ir.Expr = null;
        switch (args[0]) {
            .col_ref => |c| {
                if (std.mem.eql(u8, c, "*")) {
                    // *-form is COUNT-only.
                    if (func != .count) return ParseError.SqlInvalidProjection;
                } else {
                    arg_col = try self.arena.dupe(u8, c);
                }
            },
            // Expression arg — e.g. SUM(a * b) or COUNT(upper(name)).
            // Hoisted into a synthetic Compute column before the
            // GroupBy step; see parseStatement's pre-aggregate pass.
            .call, .case, .lit => arg_expr = args[0],
            // Subqueries / EXISTS as aggregate args are out of scope
            // for v1; users can pre-aggregate via a CTE.
            else => return ParseError.SqlInvalidProjection,
        }
        const default_name = blk: {
            var buf: std.ArrayList(u8) = .empty;
            defer buf.deinit(self.arena);
            try buf.appendSlice(self.arena, func_name);
            try buf.append(self.arena, '(');
            if (arg_expr != null) {
                try buf.appendSlice(self.arena, "expr");
            } else {
                try buf.appendSlice(self.arena, arg_col orelse "*");
            }
            try buf.append(self.arena, ')');
            break :blk try buf.toOwnedSlice(self.arena);
        };
        const alias = try self.maybeAlias(default_name);
        return ProjItem{ .name = alias, .kind = .{ .agg = .{
            .func = func,
            .col = arg_col,
            .arg_expr = arg_expr,
        } } };
    }


    /// One argument to a scalar function call. Entry point for the
    /// expression sub-language used inside call args / projections.
    /// Precedence layers:
    ///   parseCallArg → parseAddSub  (lowest: + -)
    ///                → parseMulDiv  (next:   * / %)
    ///                → parseCallAtom (leaf: ident / call / literal)
    /// Aggregates and window functions are rejected inside this
    /// sub-language (atom layer enforces it).
    fn parseCallArg(self: *Parser) ParseError!ir.Expr {
        return try self.parseAddSub();
    }

    /// `+` / `-` binary operators, lowest precedence in the expr
    /// sub-language. Left-associative.
    fn parseAddSub(self: *Parser) ParseError!ir.Expr {
        var lhs = try self.parseMulDiv();
        while (self.cur.tag == .plus or self.cur.tag == .minus) {
            const is_minus = self.cur.tag == .minus;
            try self.advance();
            if (self.cur.tag == .kw_interval) {
                lhs = try self.applyInterval(lhs, is_minus);
            } else {
                const fn_name: []const u8 = if (is_minus) "sub" else "add";
                const rhs = try self.parseMulDiv();
                lhs = try self.makeBinary(fn_name, lhs, rhs);
            }
        }
        return lhs;
    }

    /// `*` / `/` / `%` binary operators, higher precedence than + / -.
    /// Left-associative.
    fn parseMulDiv(self: *Parser) ParseError!ir.Expr {
        var lhs = try self.parseCallAtom();
        while (self.cur.tag == .star or self.cur.tag == .slash or self.cur.tag == .percent) {
            const fn_name: []const u8 = switch (self.cur.tag) {
                .star => "mul",
                .slash => "div",
                .percent => "mod",
                else => unreachable,
            };
            try self.advance();
            const rhs = try self.parseCallAtom();
            lhs = try self.makeBinary(fn_name, lhs, rhs);
        }
        return lhs;
    }

    /// Parse the contents of `EXTRACT(field FROM expr)` — cursor enters
    /// on the `(`, exits past `)`. The field name is case-insensitive
    /// and matched against year/month/day/hour/minute/second; the
    /// resulting Expr is a regular scalar call to that function.
    fn parseExtractCall(self: *Parser, _: Token) ParseError!ir.Expr {
        try self.expect(.lparen);
        if (self.cur.tag != .identifier) return ParseError.SqlExpectedIdent;
        const field = self.cur.text;
        const fn_name: []const u8 = if (std.ascii.eqlIgnoreCase(field, "year"))
            "year"
        else if (std.ascii.eqlIgnoreCase(field, "month"))
            "month"
        else if (std.ascii.eqlIgnoreCase(field, "day"))
            "day"
        else if (std.ascii.eqlIgnoreCase(field, "hour"))
            "hour"
        else if (std.ascii.eqlIgnoreCase(field, "minute"))
            "minute"
        else if (std.ascii.eqlIgnoreCase(field, "second"))
            "second"
        else
            return ParseError.SqlExpectedKeyword;
        try self.advance();
        if (self.cur.tag != .kw_from) return ParseError.SqlExpectedFrom;
        try self.advance();
        const arg = try self.parseCallArg();
        try self.expect(.rparen);
        const args = try self.arena.alloc(ir.Expr, 1);
        args[0] = arg;
        const name_dup = try self.arena.dupe(u8, fn_name);
        return ir.Expr{ .call = .{ .fn_name = name_dup, .args = args } };
    }

    /// Parse a searched CASE expression:
    ///   CASE WHEN bool THEN expr (WHEN bool THEN expr)* [ELSE expr] END
    /// Cursor enters on `CASE`, exits past `END`. Simple-form CASE
    /// (`CASE col WHEN v THEN ...`) is not supported in v1; users should
    /// rewrite to searched form (`CASE WHEN col = v THEN ...`).
    fn parseCaseExpr(self: *Parser) ParseError!ir.Expr {
        try self.expect(.kw_case);
        if (self.cur.tag != .kw_when) return ParseError.SqlExpectedKeyword;

        var branches: std.ArrayList(ir.Expr.Branch) = .empty;
        defer branches.deinit(self.arena);

        while (self.cur.tag == .kw_when) {
            try self.advance();
            const cond = try self.parseBoolExpr();
            if (self.cur.tag != .kw_then) return ParseError.SqlExpectedKeyword;
            try self.advance();
            const then_expr = try self.parseCallArg();
            try branches.append(self.arena, .{ .cond = cond, .then = then_expr });
        }

        var else_branch: ?*const ir.Expr = null;
        if (self.cur.tag == .kw_else) {
            try self.advance();
            const eb = try self.arena.create(ir.Expr);
            eb.* = try self.parseCallArg();
            else_branch = eb;
        }

        if (self.cur.tag != .kw_end) return ParseError.SqlExpectedKeyword;
        try self.advance();

        const branches_owned = try branches.toOwnedSlice(self.arena);
        return ir.Expr{ .case = .{ .branches = branches_owned, .else_branch = else_branch } };
    }

    /// Leaf of the expr sub-language. Same shape as the original
    /// `parseCallArg`: column ref (possibly qualified), literal, or
    /// nested scalar call. Aggregates and window functions are
    /// rejected — they belong at the top level of the SELECT list.
    fn parseCallAtom(self: *Parser) ParseError!ir.Expr {
        if (self.cur.tag == .kw_case) return try self.parseCaseExpr();
        // EXISTS (SELECT ...) in expression position — projects a
        // boolean. Resolved by the pre-compile pass into `.lit`.
        if (self.cur.tag == .kw_exists) {
            try self.advance();
            try self.expect(.lparen);
            if (self.cur.tag != .kw_select and self.cur.tag != .kw_with) return ParseError.SqlExpectedSelect;
            const source = try self.parseStatement();
            try self.expect(.rparen);
            return ir.Expr{ .exists_subquery = @ptrCast(source) };
        }
        // EXTRACT(field FROM expr) — SQL-standard. Lowers to a regular
        // scalar call (year/month/day/hour/minute/second). `field` is
        // an identifier (not a reserved keyword) so check the *next*
        // token for `(`.
        if (self.cur.tag == .identifier and std.ascii.eqlIgnoreCase(self.cur.text, "extract")) {
            // Peek: the next two tokens must look like `( ident`. If
            // they don't, fall through to the identifier branch (treats
            // `extract` as a column name).
            const saved = self.cur;
            try self.advance();
            if (self.cur.tag == .lparen) {
                return try self.parseExtractCall(saved);
            }
            // Roll-back path: we already consumed `extract` and looked
            // at the next token. Re-emit it as a col_ref since the
            // grammar can't peek-then-backtrack arbitrarily.
            const col = try self.arena.dupe(u8, saved.text);
            return ir.Expr{ .col_ref = col };
        }
        switch (self.cur.tag) {
            .identifier => {
                const name = self.cur.text;
                try self.advance();
                if (self.cur.tag == .lparen) {
                    if (aggForName(name)) |_| return ParseError.SqlInvalidProjection;
                    if (ir.windowFuncForName(name)) |_| return ParseError.SqlInvalidProjection;
                    const fname_dup = try self.arena.dupe(u8, name);
                    const nested_args = try self.parseCallArgList();
                    return ir.Expr{ .call = .{ .fn_name = fname_dup, .args = nested_args } };
                }
                const col_dup = try self.dupQualifiedColRef(name);
                return ir.Expr{ .col_ref = col_dup };
            },
            .plus, .minus, .integer, .floating, .string, .kw_true, .kw_false => {
                const v = try self.parseValue();
                return ir.Expr{ .lit = v };
            },
            .at_identifier => {
                const var_name = try self.arena.dupe(u8, self.cur.text);
                try self.advance();
                return ir.Expr{ .var_ref = var_name };
            },
            .lparen => {
                // Parenthesized sub-expression OR scalar subquery.
                // `(SELECT ...)` / `(WITH ... SELECT ...)` is captured
                // as a scalar_subquery node; everything else recurses
                // through the binary expression parser.
                try self.advance();
                if (self.cur.tag == .kw_select or self.cur.tag == .kw_with) {
                    const source = try self.parseStatement();
                    try self.expect(.rparen);
                    return ir.Expr{ .scalar_subquery = @ptrCast(source) };
                }
                const inner = try self.parseAddSub();
                try self.expect(.rparen);
                return inner;
            },
            else => return ParseError.SqlExpectedValue,
        }
    }

    fn makeBinary(self: *Parser, fn_name: []const u8, lhs: ir.Expr, rhs: ir.Expr) ParseError!ir.Expr {
        const args = try self.arena.alloc(ir.Expr, 2);
        args[0] = lhs;
        args[1] = rhs;
        const fname_dup = try self.arena.dupe(u8, fn_name);
        return ir.Expr{ .call = .{ .fn_name = fname_dup, .args = args } };
    }

    /// Parse a bare column reference (possibly qualified). Used inside
    /// contexts that don't accept full expressions.
    fn parseColRefExpr(self: *Parser) ParseError!ir.Expr {
        if (self.cur.tag != .identifier) return ParseError.SqlExpectedIdent;
        const name = self.cur.text;
        try self.advance();
        if (self.cur.tag == .lparen) return ParseError.SqlInvalidProjection;
        const col_dup = try self.dupQualifiedColRef(name);
        return ir.Expr{ .col_ref = col_dup };
    }

    /// Helper for the `identifier (. identifier)?` shape. The two-part
    /// form is preserved as the dotted string `qualifier.col` so the
    /// reference survives long enough for a renamed scan (`FROM t AS
    /// alias`) to resolve it. Caller has already consumed the first
    /// identifier and passes its text as `first`.
    fn dupQualifiedColRef(self: *Parser, first: []const u8) ParseError![]const u8 {
        if (self.cur.tag != .dot) return try self.arena.dupe(u8, first);
        try self.advance();
        if (self.cur.tag != .identifier) return ParseError.SqlExpectedIdent;
        const second = self.cur.text;
        try self.advance();
        const buf = try self.arena.alloc(u8, first.len + 1 + second.len);
        @memcpy(buf[0..first.len], first);
        buf[first.len] = '.';
        @memcpy(buf[first.len + 1 ..], second);
        return buf;
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
            .case => return try self.arena.dupe(u8, "case"),
            .scalar_subquery => return try self.arena.dupe(u8, "subquery"),
            .exists_subquery => return try self.arena.dupe(u8, "exists"),
            .var_ref => |name| {
                const buf = try self.arena.alloc(u8, name.len + 1);
                buf[0] = '@';
                @memcpy(buf[1..], name);
                return buf;
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
        const first = try self.parseFromTarget();
        var root = first.op;

        // Running set of names that constitute the current left
        // subtree. Each new JOIN's ON clause must reference one of
        // these + the new right target's name. CTE references and
        // subquery aliases participate alongside plain table names.
        var left_names: std.ArrayList([]const u8) = .empty;
        try left_names.append(self.arena, first.name);
        defer left_names.deinit(self.arena);

        while (isJoinStart(self.cur.tag)) {
            const jtype = try self.parseJoinKind();
            const right = try self.parseFromTarget();

            if (self.cur.tag != .kw_on) return ParseError.SqlExpectedJoinOn;
            try self.advance();
            const pairs = try self.parseOnEquiJoin(left_names.items, right.name);

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
                .right = right.op,
            } });
            try left_names.append(self.arena, right.name);
        }
        return root;
    }

    /// One source in a FROM clause: bare identifier (table or CTE
    /// reference) or a parenthesized subquery with an alias. Returns
    /// the resolved *ir.Op plus the name to use for ON-clause
    /// qualifier resolution.
    fn parseFromTarget(self: *Parser) ParseError!struct { name: []const u8, op: *ir.Op } {
        if (self.cur.tag == .lparen) {
            // Anonymous subquery: ( select_stmt ) [AS] alias
            try self.advance();
            const op = try self.parseStatement();
            try self.expect(.rparen);
            // Optional AS, mandatory alias.
            if (self.cur.tag == .kw_as) try self.advance();
            if (self.cur.tag != .identifier) return ParseError.SqlSubqueryNeedsAlias;
            const alias = try self.arena.dupe(u8, self.cur.text);
            try self.advance();
            return .{ .name = alias, .op = op };
        }
        // Plain identifier — first check the CTE map (single-part name
        // only). If it's a CTE, use the stored op. Otherwise parse 1-,
        // 2-, or 3-part `[db.][schema.]table` into a TableRef-backed
        // Scan.
        const first = try self.expectIdent();
        const first_dup = try self.arena.dupe(u8, first);
        var op: *ir.Op = undefined;
        var resolved_name: []const u8 = first_dup;
        if (self.cur.tag != .dot and self.ctes.get(first) != null) {
            op = self.ctes.get(first).?.op;
        } else {
            var parts_buf: [3][]const u8 = undefined;
            parts_buf[0] = first_dup;
            var parts_len: usize = 1;
            while (self.cur.tag == .dot) {
                if (parts_len == parts_buf.len) return ParseError.SqlExpectedIdent;
                try self.advance();
                if (self.cur.tag != .identifier) return ParseError.SqlExpectedIdent;
                parts_buf[parts_len] = try self.arena.dupe(u8, self.cur.text);
                parts_len += 1;
                try self.advance();
            }
            const ref: ir.TableRef = switch (parts_len) {
                1 => .{ .name = parts_buf[0] },
                2 => .{ .schema = parts_buf[0], .name = parts_buf[1] },
                3 => .{ .database = parts_buf[0], .schema = parts_buf[1], .name = parts_buf[2] },
                else => unreachable,
            };
            op = try self.allocOp(.{ .scan = .{ .table = ref, .alias = null } });
            resolved_name = parts_buf[parts_len - 1];
        }

        // Optional AS alias.
        if (self.cur.tag == .kw_as) {
            try self.advance();
            if (self.cur.tag != .identifier) return ParseError.SqlExpectedIdent;
            resolved_name = try self.arena.dupe(u8, self.cur.text);
            if (op.* == .scan) {
                op.scan.alias = resolved_name;
            }
            try self.advance();
        } else if (self.cur.tag == .identifier) {
            // Implicit alias: bare identifier after the FROM target.
            // SQL clause keywords (JOIN/WHERE/ON/...) aren't .identifier
            // tokens so they don't trigger this.
            resolved_name = try self.arena.dupe(u8, self.cur.text);
            if (op.* == .scan) {
                op.scan.alias = resolved_name;
            }
            try self.advance();
        }
        return .{ .name = resolved_name, .op = op };
    }

    /// Parse a `WITH cte AS [MATERIALIZED|NOT MATERIALIZED]? (...) [, ...]*`
    /// block. Each CTE is added to `self.ctes` with its hint; later CTEs
    /// in the same WITH can reference earlier ones (each parseStatement
    /// call sees the already-populated map).
    fn parseCteList(self: *Parser) ParseError!void {
        try self.advance(); // consume WITH
        while (true) {
            if (self.cur.tag != .identifier) return ParseError.SqlExpectedIdent;
            const name = try self.arena.dupe(u8, self.cur.text);
            try self.advance();
            if (self.cur.tag != .kw_as) return ParseError.SqlExpectedKeyword;
            try self.advance();

            // Optional materialization hint between AS and (.
            //   MATERIALIZED        → force
            //   NOT MATERIALIZED    → never
            //   (no hint)           → auto (refcount-driven post-pass)
            var hint: MaterializeHint = .auto;
            if (self.cur.tag == .kw_materialized) {
                hint = .force;
                try self.advance();
            } else if (self.cur.tag == .kw_not) {
                try self.advance();
                if (self.cur.tag != .kw_materialized) return ParseError.SqlExpectedKeyword;
                try self.advance();
                hint = .never;
            }

            try self.expect(.lparen);
            const op = try self.parseStatement();
            try self.expect(.rparen);

            const gop = try self.ctes.getOrPut(self.arena, name);
            if (gop.found_existing) return ParseError.SqlCteRedefined;
            gop.value_ptr.* = .{ .op = op, .hint = hint };
            try self.cte_roots.append(self.arena, op);

            if (self.cur.tag != .comma) break;
            try self.advance();
        }
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

    /// Parse `DELETE FROM <table> [WHERE <bool_expr>]`. The
    /// predicate uses the full PredicateExpr grammar so AND/OR/IN
    /// (literal-list AND subquery) work — pre-compile resolution
    /// handles subqueries and `@var` references before the per-row
    /// evaluation runs.
    pub fn parseDelete(self: *Parser) ParseError!*ir.Op {
        try self.expect(.kw_delete);
        try self.expect(.kw_from);
        const tref = try self.parseTableRef();
        var pred: ?PredicateExpr = null;
        if (self.cur.tag == .kw_where) {
            try self.advance();
            pred = try self.parseBoolExpr();
        }
        return try self.allocOp(.{ .delete_op = .{ .table = tref, .predicate = pred } });
    }

    /// Parse `UPDATE <table> SET col = expr [, ...] [WHERE <bool_expr>]`.
    /// RHS exprs use the full Expr grammar so users can write
    /// `SET x = x + 1`, `SET label = lower(name)`, or
    /// `SET y = (SELECT AVG(y) FROM t)`. Subqueries / `@vars` resolve
    /// in the pre-compile pass before the per-row evaluation runs.
    pub fn parseUpdate(self: *Parser) ParseError!*ir.Op {
        try self.expect(.kw_update);
        const tref = try self.parseTableRef();
        try self.expect(.kw_set);

        var assigns: std.ArrayList(ir.Assignment) = .empty;
        defer assigns.deinit(self.arena);
        while (true) {
            if (self.cur.tag != .identifier) return ParseError.SqlExpectedIdent;
            const col_name = try self.arena.dupe(u8, self.cur.text);
            try self.advance();
            if (self.cur.tag != .eq) return ParseError.SqlExpectedToken;
            try self.advance();
            const val = try self.parseAddSub();
            try assigns.append(self.arena, .{ .col = col_name, .value = val });
            if (self.cur.tag != .comma) break;
            try self.advance();
        }
        const assigns_owned = try assigns.toOwnedSlice(self.arena);

        var pred: ?PredicateExpr = null;
        if (self.cur.tag == .kw_where) {
            try self.advance();
            pred = try self.parseBoolExpr();
        }
        return try self.allocOp(.{ .update_op = .{
            .table = tref,
            .assignments = assigns_owned,
            .predicate = pred,
        } });
    }

    /// Parse `SET @name = expr`. The RHS is a general Expr so users
    /// can write `SET @cutoff = (SELECT MAX(amount) FROM orders)` —
    /// the pre-compile pass resolves any scalar subquery into a
    /// literal, and `compileSetVar` requires the result to be a `.lit`.
    pub fn parseSetVar(self: *Parser) ParseError!*ir.Op {
        try self.expect(.kw_set);
        if (self.cur.tag != .at_identifier) return ParseError.SqlExpectedIdent;
        const name = try self.arena.dupe(u8, self.cur.text);
        try self.advance();
        if (self.cur.tag != .eq) return ParseError.SqlExpectedToken;
        try self.advance();
        const value_expr = try self.parseAddSub();
        return try self.allocOp(.{ .set_var = .{ .name = name, .value = value_expr } });
    }

    pub fn parseOrderBy(self: *Parser) ParseError![]const @import("../exec/sort.zig").SortSpec {
        const SortSpec = @import("../exec/sort.zig").SortSpec;
        var items: std.ArrayList(SortSpec) = .empty;
        defer items.deinit(self.arena);
        while (true) {
            if (self.cur.tag != .identifier) return ParseError.SqlExpectedIdent;
            const first = self.cur.text;
            try self.advance();
            const col = try self.dupQualifiedColRef(first);
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

    pub fn parseIdentList(self: *Parser) ParseError![]const []const u8 {
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

    pub fn expectIdent(self: *Parser) ParseError![]const u8 {
        if (self.cur.tag != .identifier) return ParseError.SqlExpectedIdent;
        const name = self.cur.text;
        try self.advance();
        return name;
    }

    // -----------------------------------------------------------------------
    // WHERE / boolean expression parsing.
    // -----------------------------------------------------------------------

    // Predicate parsing lives in `parse_predicate.zig`; thin delegate
    // here so the SELECT pipeline keeps its existing call sites.
    fn parseBoolExpr(self: *Parser) ParseError!PredicateExpr {
        return try parse_predicate.parseBoolExpr(self);
    }

    pub fn parseValue(self: *Parser) ParseError!Value {
        const tok = self.cur;
        switch (tok.tag) {
            .plus, .minus => {
                const negate = tok.tag == .minus;
                try self.advance();
                const signed_tok = self.cur;
                switch (signed_tok.tag) {
                    .integer => {
                        try self.advance();
                        const raw = signed_tok.value.integer;
                        const v = if (negate) -raw else raw;
                        // Default literal type: int (i32). Promote to bigint
                        // if out of i32 range.
                        if (v >= std.math.minInt(i32) and v <= std.math.maxInt(i32)) {
                            return .{ .int = @intCast(v) };
                        }
                        return .{ .bigint = v };
                    },
                    .floating => {
                        try self.advance();
                        const v = if (negate) -signed_tok.value.floating else signed_tok.value.floating;
                        return .{ .double = v };
                    },
                    else => return ParseError.SqlExpectedValue,
                }
            },
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
            // Typed temporal literals: DATE '2024-01-15',
            //   DATETIME '2024-01-15 12:34:56', TIMESTAMP alias.
            // The keyword sits in the identifier namespace today
            // (parsed as `.identifier` because the CREATE TABLE
            // grammar uses these as column-type names) — detected
            // here by name + a lookahead at the following string.
            .identifier => {
                const word = tok.text;
                if (std.ascii.eqlIgnoreCase(word, "date")) {
                    try self.advance();
                    if (self.cur.tag != .string) return ParseError.SqlExpectedValue;
                    const s = self.cur.value.string;
                    const days = parseDateString(s) catch return ParseError.SqlExpectedValue;
                    try self.advance();
                    return .{ .date = days };
                }
                if (std.ascii.eqlIgnoreCase(word, "datetime") or
                    std.ascii.eqlIgnoreCase(word, "timestamp"))
                {
                    try self.advance();
                    if (self.cur.tag != .string) return ParseError.SqlExpectedValue;
                    const s = self.cur.value.string;
                    const micros = parseDateTimeString(s) catch return ParseError.SqlExpectedValue;
                    try self.advance();
                    return .{ .datetime = micros };
                }
                return ParseError.SqlExpectedValue;
            },
            else => return ParseError.SqlExpectedValue,
        }
    }

    /// Inline copy of `net.local.parseDateLiteral` — keeping parser.zig
    /// free of the net layer dep.
    fn parseDateString(s: []const u8) !i32 {
        if (s.len < 10) return error.Invalid;
        if (s[4] != '-' or s[7] != '-') return error.Invalid;
        const year = try std.fmt.parseInt(i32, s[0..4], 10);
        const month = try std.fmt.parseInt(u32, s[5..7], 10);
        const day = try std.fmt.parseInt(u32, s[8..10], 10);
        if (month < 1 or month > 12 or day < 1 or day > 31) return error.Invalid;
        return ymdToDays(year, month, day);
    }

    fn parseDateTimeString(s: []const u8) !i64 {
        if (s.len < 19) return error.Invalid;
        if (s[4] != '-' or s[7] != '-') return error.Invalid;
        const sep = s[10];
        if (sep != ' ' and sep != 'T') return error.Invalid;
        if (s[13] != ':' or s[16] != ':') return error.Invalid;
        const year = try std.fmt.parseInt(i32, s[0..4], 10);
        const month = try std.fmt.parseInt(u32, s[5..7], 10);
        const day = try std.fmt.parseInt(u32, s[8..10], 10);
        const hour = try std.fmt.parseInt(u32, s[11..13], 10);
        const minute = try std.fmt.parseInt(u32, s[14..16], 10);
        const second = try std.fmt.parseInt(u32, s[17..19], 10);
        if (hour > 23 or minute > 59 or second > 59) return error.Invalid;
        const days = ymdToDays(year, month, day);
        const day_secs: i64 = @as(i64, days) * 86400 + @as(i64, hour) * 3600 + @as(i64, minute) * 60 + @as(i64, second);
        return day_secs * 1_000_000;
    }

    fn ymdToDays(year: i32, month: u32, day: u32) i32 {
        return @import("../exec/scalar_fn_common.zig").ymdToDays(year, month, day);
    }

    pub fn allocOp(self: *Parser, op: ir.Op) ParseError!*ir.Op {
        const out = try self.arena.create(ir.Op);
        out.* = op;
        return out;
    }

    // -----------------------------------------------------------------------
    // Post-parse auto-detect refcount pass: walks the final IR tree, counts
    // parent references per *ir.Op, then wraps each CTE's stored op in a
    // Materialize node when the hint says so or when the op has ≥ 2
    // references and no NOT-MATERIALIZED override. The wrap is in-place
    // (mutates *cte_op contents); existing references still hold the old
    // pointer, which now resolves to a Materialize.
    // -----------------------------------------------------------------------

    fn applyAutoMaterialize(self: *Parser, root: *ir.Op) ParseError!void {
        if (self.ctes.count() == 0) return;

        var refs: std.AutoHashMapUnmanaged(*ir.Op, u32) = .empty;
        defer refs.deinit(self.arena);
        try countRefs(self.arena, &refs, root);

        var it = self.ctes.iterator();
        while (it.next()) |entry| {
            const cte_op = entry.value_ptr.op;
            const count = refs.get(cte_op) orelse 0;
            const should_wrap = switch (entry.value_ptr.hint) {
                .never => false,
                .force => true,
                .auto => count >= 2,
            };
            if (!should_wrap) continue;
            // In-place wrap: move the existing contents into a new
            // arena-owned Op, then overwrite the original with a
            // Materialize variant pointing at it. All references that
            // already hold &cte_op continue to work — they now see the
            // Materialize wrapper.
            const inner = try self.arena.create(ir.Op);
            inner.* = cte_op.*;
            cte_op.* = .{ .materialize = .{ .upstream = inner } };
        }
    }
};

fn countRefs(
    arena: Allocator,
    refs: *std.AutoHashMapUnmanaged(*ir.Op, u32),
    op: *ir.Op,
) ParseError!void {
    switch (op.*) {
        .scan => {},
        .limit => |l| try visitChild(arena, refs, l.upstream),
        .select => |p| try visitChild(arena, refs, p.upstream),
        .exclude => |p| try visitChild(arena, refs, p.upstream),
        .filter => |f| try visitChild(arena, refs, f.upstream),
        .order_by => |o| try visitChild(arena, refs, o.upstream),
        .group_by => |g| try visitChild(arena, refs, g.upstream),
        .compute => |c| try visitChild(arena, refs, c.upstream),
        .join => |j| {
            try visitChild(arena, refs, j.left);
            try visitChild(arena, refs, j.right);
        },
        .materialize => |m| try visitChild(arena, refs, m.upstream),
        .ddl, .show, .insert, .copy, .set_var, .delete_op, .update_op => {},
        .batch => |b| for (b.statements) |sub| try visitChild(arena, refs, sub),
        .window => |w| try visitChild(arena, refs, w.upstream),
        .set_union => |u| {
            try visitChild(arena, refs, u.left);
            try visitChild(arena, refs, u.right);
        },
        .create_table_as => |c| try visitChild(arena, refs, c.source),
        .insert_select => |i| try visitChild(arena, refs, i.source),
    }
}

fn visitChild(
    arena: Allocator,
    refs: *std.AutoHashMapUnmanaged(*ir.Op, u32),
    child: *ir.Op,
) ParseError!void {
    const gop = try refs.getOrPut(arena, child);
    if (gop.found_existing) {
        // Already counted via another parent — bump count but DON'T
        // re-walk the subtree (would double-count grandchildren).
        gop.value_ptr.* += 1;
        return;
    }
    gop.value_ptr.* = 1;
    try countRefs(arena, refs, child);
}

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

/// Lower the window ProjItems collected from a SELECT into a single
/// `ir.WindowOp`. Walks all ProjItems, resolves named-window references
/// against `named_windows`, deduplicates equivalent specs by structural
/// equality, builds the spec table and the WindowCall list, and wraps
/// `upstream` in an `Op{ .window = ... }`. The returned op is the new
/// pipeline root; the caller chains OrderBy / Project / Limit
/// downstream of it.
///
/// `null` returned when no projection item is a window call — caller
/// keeps `upstream` as-is.
fn buildWindowOp(
    arena: Allocator,
    proj: []const ProjItem,
    upstream: *ir.Op,
    named_windows: *const std.StringHashMapUnmanaged(ir.WindowSpec),
) ParseError!?*ir.Op {
    var count: usize = 0;
    for (proj) |p| switch (p.kind) {
        .window => count += 1,
        else => {},
    };
    if (count == 0) return null;

    var specs_buf: std.ArrayList(ir.WindowSpec) = .empty;
    defer specs_buf.deinit(arena);
    var calls_buf: std.ArrayList(ir.WindowCall) = .empty;
    defer calls_buf.deinit(arena);

    for (proj) |p| switch (p.kind) {
        .window => |w| {
            const resolved_spec: ir.WindowSpec = switch (w.spec_kind) {
                .inline_spec => |s| s,
                .named => |name| blk: {
                    const stored = named_windows.get(name) orelse
                        return ParseError.SqlInvalidProjection;
                    break :blk parse_window.cloneWindowSpec(arena, stored) catch
                        return ParseError.OutOfMemory;
                },
            };
            const spec_idx = blk: {
                for (specs_buf.items, 0..) |existing, i| {
                    if (parse_window.windowSpecsEqual(existing, resolved_spec)) break :blk i;
                }
                try specs_buf.append(arena, resolved_spec);
                break :blk specs_buf.items.len - 1;
            };
            try calls_buf.append(arena, .{
                .spec_idx = @intCast(spec_idx),
                .func = w.func,
                .args = w.args,
                .ignore_nulls = w.ignore_nulls,
                .output_name = p.name,
            });
        },
        else => {},
    };

    const specs_slice = try specs_buf.toOwnedSlice(arena);
    const calls_slice = try calls_buf.toOwnedSlice(arena);
    const op = try arena.create(ir.Op);
    op.* = .{ .window = .{
        .specs = specs_slice,
        .calls = calls_slice,
        .upstream = upstream,
    } };
    return op;
}

fn nameIn(needle: []const u8, names: []const []const u8) bool {
    for (names) |n| if (std.mem.eql(u8, n, needle)) return true;
    return false;
}

