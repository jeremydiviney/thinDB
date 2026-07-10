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
const udf_mod = @import("../udf.zig");

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
    /// `SEPARABLE BY` in an unsupported position: v1 accepts it only as the
    /// trailing clause of a CTE body (LOCAL scope). GLOBAL, statement-root,
    /// subquery, and duplicate declarations reject here.
    SqlSeparableScope,
    /// COPY with a file-path source/target. thinDB only speaks
    /// `STDIN`/`STDOUT`; server-side file paths have auth implications
    /// we don't want to inherit from upstream PG.
    SqlCopyFileNotSupported,
    /// COPY with an unsupported FORMAT option (we only do text).
    SqlCopyUnsupportedFormat,
    SqlUnsupportedFileFunction,
    SqlUnsupportedFileOption,
    /// SQL table-function call-site errors: argument count mismatch or
    /// nested expansion beyond the recursion bound.
    SqlFunctionArgMismatch,
} || LexError;

const AggNames = [_]struct { name: []const u8, func: ir.AggFunc }{
    .{ .name = "count", .func = .count },
    .{ .name = "sum", .func = .sum },
    .{ .name = "min", .func = .min },
    .{ .name = "max", .func = .max },
    .{ .name = "avg", .func = .avg },
    .{ .name = "count_if", .func = .count_if },
    .{ .name = "countif", .func = .count_if },
    .{ .name = "bool_and", .func = .bool_and },
    .{ .name = "bool_or", .func = .bool_or },
    .{ .name = "any_value", .func = .any_value },
    .{ .name = "first", .func = .first },
    .{ .name = "last", .func = .last },
    .{ .name = "max_by", .func = .max_by },
    .{ .name = "bit_and", .func = .bit_and },
    .{ .name = "bit_or", .func = .bit_or },
    .{ .name = "bit_xor", .func = .bit_xor },
    .{ .name = "std", .func = .stddev_samp },
    .{ .name = "stddev", .func = .stddev_samp },
    .{ .name = "stddev_pop", .func = .stddev_pop },
    .{ .name = "stddev_samp", .func = .stddev_samp },
    .{ .name = "var_pop", .func = .var_pop },
    .{ .name = "var_samp", .func = .var_samp },
    .{ .name = "variance", .func = .var_samp },
    .{ .name = "variance_pop", .func = .var_pop },
    .{ .name = "variance_samp", .func = .var_samp },
    .{ .name = "count_distinct", .func = .count_distinct },
    .{ .name = "approx_count_distinct", .func = .count_distinct },
    .{ .name = "percentile_cont", .func = .percentile },
    .{ .name = "percentile", .func = .percentile },
    .{ .name = "median", .func = .percentile },
    .{ .name = "group_concat", .func = .group_concat },
    .{ .name = "string_agg", .func = .group_concat },
};

fn aggForName(name: []const u8) ?ir.AggFunc {
    for (AggNames) |entry| {
        if (std.ascii.eqlIgnoreCase(name, entry.name)) return entry.func;
    }
    return null;
}

/// Canonical lowercase function name for an aggregate, used to rebuild
/// the `func(arg)` reference name a HAVING clause would produce. Returns
/// null for functions not addressable as a bare `func(col)` HAVING ref.
fn aggFuncName(f: ir.AggFunc) ?[]const u8 {
    return switch (f) {
        .count, .count_distinct => "count",
        .sum => "sum",
        .min => "min",
        .max => "max",
        .avg => "avg",
        .count_if => "count_if",
        .bool_and => "bool_and",
        .bool_or => "bool_or",
        .any_value => "any_value",
        .first => "first",
        .last => "last",
        .max_by => "max_by",
        .bit_and => "bit_and",
        .bit_or => "bit_or",
        .bit_xor => "bit_xor",
        .sum_distinct => "sum",
        .avg_distinct => "avg",
        .stddev_pop => "stddev_pop",
        .stddev_samp => "stddev_samp",
        .var_pop => "var_pop",
        .var_samp => "var_samp",
        .percentile, .group_concat, .udf => null,
    };
}

fn explainFormatFromName(name: []const u8) ir.ExplainFormat {
    if (std.ascii.eqlIgnoreCase(name, "json")) return .json;
    return .text;
}

/// SQL-standard bare (no-paren) temporal functions. They lex as plain
/// identifiers; the parser rewrites them to the nullary call form so the
/// now()/current_date compile-time substitution resolves them to real
/// wall-clock instead of treating them as column references.
fn bareTemporalFn(name: []const u8) ?[]const u8 {
    if (std.ascii.eqlIgnoreCase(name, "current_timestamp")) return "current_timestamp";
    if (std.ascii.eqlIgnoreCase(name, "localtimestamp")) return "localtimestamp";
    if (std.ascii.eqlIgnoreCase(name, "utc_timestamp")) return "utc_timestamp";
    if (std.ascii.eqlIgnoreCase(name, "current_time")) return "current_time";
    if (std.ascii.eqlIgnoreCase(name, "curtime")) return "curtime";
    if (std.ascii.eqlIgnoreCase(name, "localtime")) return "localtime";
    if (std.ascii.eqlIgnoreCase(name, "current_date")) return "current_date";
    if (std.ascii.eqlIgnoreCase(name, "curdate")) return "curdate";
    if (std.ascii.eqlIgnoreCase(name, "utc_date")) return "utc_date";
    return null;
}

fn keywordScalarName(tag: TokenTag) ?[]const u8 {
    return switch (tag) {
        .kw_replace => "replace",
        .kw_if => "if",
        .kw_left => "left",
        .kw_right => "right",
        else => null,
    };
}

fn unitFirstArgCall(name: []const u8) bool {
    return std.ascii.eqlIgnoreCase(name, "date_diff") or
        std.ascii.eqlIgnoreCase(name, "timestampdiff") or
        std.ascii.eqlIgnoreCase(name, "timestampadd");
}

const DateAddSubKind = enum { add, sub };

fn dateAddSubName(name: []const u8) ?DateAddSubKind {
    if (std.ascii.eqlIgnoreCase(name, "date_add")) return .add;
    if (std.ascii.eqlIgnoreCase(name, "date_sub")) return .sub;
    return null;
}

fn fileFormatForFunction(name: []const u8) ?ir.FileFormat {
    if (std.ascii.eqlIgnoreCase(name, "read_csv")) return .csv;
    if (std.ascii.eqlIgnoreCase(name, "read_json")) return .json;
    if (std.ascii.eqlIgnoreCase(name, "read_parquet")) return .parquet;
    return null;
}

/// Parse with no specific wire flavor (`.neutral`): permissive/ANSI-leaning.
/// The embedded/native path and tests use this. Wire servers call
/// `parseDialect` with their pinned dialect so flavor-specific syntax is
/// enforced.
pub fn parse(arena: Allocator, sql: []const u8) ParseError!*ir.Op {
    return parseDialect(arena, sql, .neutral);
}

pub fn parseDialect(arena: Allocator, sql: []const u8, dialect: types.Dialect) ParseError!*ir.Op {
    return parseDialectWithUdfs(arena, sql, dialect, null);
}

pub fn parseDialectWithUdfs(
    arena: Allocator,
    sql: []const u8,
    dialect: types.Dialect,
    udf_registry: ?*const udf_mod.UdfRegistry,
) ParseError!*ir.Op {
    return parseWithContext(arena, sql, dialect, udf_registry, null);
}

pub fn parseWithContext(
    arena: Allocator,
    sql: []const u8,
    dialect: types.Dialect,
    udf_registry: ?*const udf_mod.UdfRegistry,
    sql_fns: ?udf_mod.SqlFnCtx,
) ParseError!*ir.Op {
    var lex = Lexer.init(arena, sql);
    lex.dialect = dialect;
    var parser = Parser{
        .arena = arena,
        .lex = &lex,
        .cur = try lex.next(),
        .udf_registry = udf_registry,
        .sql_fns = sql_fns,
    };

    // Skip leading empty statements (e.g. ";;SELECT ...").
    while (parser.cur.tag == .semicolon) try parser.advance();
    if (parser.cur.tag == .eof) return ParseError.SqlExpectedSelect;

    var statements: std.ArrayList(*ir.Op) = .empty;
    defer statements.deinit(arena);

    while (true) {
        const op = try parser.parseStatement();
        // A SEPARABLE BY still pending here terminated the OUTERMOST select
        // — not a CTE body. v1 supports LOCAL (CTE) scope only.
        if (parser.pending_separable != null) return ParseError.SqlSeparableScope;
        // Post-parse pass: wrap each shared CTE root in Materialize (every
        // boundary materializes; see applyAutoMaterialize). CTE scope is
        // per-statement.
        try parser.applyAutoMaterialize();
        try statements.append(arena, op);
        // Reset CTE state — each statement parses with a fresh scope.
        parser.ctes.clearRetainingCapacity();

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

/// Validate a SQL table-function body at CREATE time: parse it with every
/// parameter bound to NULL. Catches syntax errors up front; column/type
/// resolution still happens per call at compile.
pub fn validateFnBody(
    arena: Allocator,
    body: []const u8,
    param_names: []const []const u8,
) ParseError!void {
    var bindings: std.StringHashMapUnmanaged(Token) = .empty;
    for (param_names) |pn| {
        try bindings.put(arena, pn, .{ .tag = .kw_null, .text = "null" });
    }
    var lex = Lexer.init(arena, body);
    var sub = Parser{
        .arena = arena,
        .lex = &lex,
        .cur = .{ .tag = .semicolon, .text = "" },
        .param_bindings = &bindings,
    };
    try sub.advance();
    _ = try sub.parseStatement();
    try sub.applyAutoMaterialize();
    if (sub.cur.tag != .eof and sub.cur.tag != .semicolon) return ParseError.SqlTrailingTokens;
}

const ParsedAgg = struct {
    func: ir.AggFunc,
    udf_name: ?[]const u8 = null,
    udf_arg_cols: []const []const u8 = &.{},
    col: ?[]const u8,
    arg_expr: ?ir.Expr = null,
    arg2_col: ?[]const u8 = null,
    arg2_expr: ?ir.Expr = null,
    separator: ?[]const u8 = null,
    params: ir.AggParams = .none,
};

const AggExprRef = struct {
    name: []const u8,
    agg: ParsedAgg,
};

const ParsedAggCall = struct {
    default_name: []const u8,
    agg: ParsedAgg,
};

const WindowExprRef = struct {
    name: []const u8,
    call: ParsedWindowCall,
};

const ProjItem = struct {
    /// Output name for this projected column (post-alias).
    name: []const u8,
    kind: union(enum) {
        /// Plain column reference. `column` is the column name.
        col: []const u8,
        /// Star expansion. Null means `*`; otherwise `qualifier.*`.
        star: ?[]const u8,
        /// Aggregate call. `agg_func` is the function; `agg_col` is
        /// the argument column name (null for COUNT(*) — or null
        /// when `arg_expr` is set and the actual argument is a
        /// computed expression). `arg_expr` is the source Expr when
        /// the user wrote `SUM(a * b)` or similar — the parser
        /// hoists it into a synthetic Compute column whose name then
        /// fills in `col` before the GroupBy is built.
        agg: ParsedAgg,
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

/// Per-CTE materialization hint from the SQL surface. Every CTE boundary
/// materializes (stage-per-block execution); the hint only picks sharing:
///   .auto / .force — one shared stage, every reference reads its result.
///   .never — `NOT MATERIALIZED`: regenerate per reference (each gets its
///            own materialize node, so forked uses recompute independently).
pub const MaterializeHint = enum { auto, force, never };

const CteEntry = struct {
    op: *ir.Op,
    hint: MaterializeHint,
    separable: ?ir.SeparableSpec = null,
};

const FromTarget = struct {
    name: []const u8,
    op: *ir.Op,
};

const JoinExprSide = enum { none, left, right, mixed };

const JoinOnPlan = struct {
    on: []const ir.JoinKeyPair,
    ranges: []const ir.JoinRangePredicate,
    left_derived: []const ir.Derived,
    right_derived: []const ir.Derived,
    left_filter: ?PredicateExpr,
    right_filter: ?PredicateExpr,
    hidden_left: []const []const u8,
};

const QualifiedJoinCol = struct {
    side: JoinExprSide,
    name: []const u8,
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
    /// Trailing `SEPARABLE BY` clause parsed at the end of a SELECT block,
    /// waiting for its owner: parseCteList moves it onto the CTE entry it
    /// terminates. Non-null at any other boundary = clause in an
    /// unsupported position.
    pending_separable: ?ir.SeparableSpec = null,
    /// Named windows declared in the trailing `WINDOW name AS (...)`
    /// clause of the current SELECT. Populated by `parseWindowClause`
    /// before projection lowering; consumed by `parseWindowSpecOrRef`
    /// when a function uses `OVER name`. Cleared at the end of each
    /// SELECT so different statements in a batch don't bleed into
    /// each other.
    named_windows: std.StringHashMapUnmanaged(ir.WindowSpec) = .empty,
    /// Optional catalog-owned registry used only to classify aggregate UDF
    /// names during parse. Scalar UDFs stay normal Expr.call nodes.
    udf_registry: ?*const udf_mod.UdfRegistry = null,
    /// Optional SQL inline-table-function context (registry + the
    /// session's current database). Consulted by `parseFromTarget` when a
    /// FROM target looks like `name(...)`; absent = no expansion.
    sql_fns: ?udf_mod.SqlFnCtx = null,
    /// Non-null while parsing a function BODY: parameter name → the
    /// call's literal argument token, substituted in `advance`.
    param_bindings: ?*const std.StringHashMapUnmanaged(Token) = null,
    /// Nested expansion depth (a function body referencing another
    /// function). Bounded to reject accidental recursion.
    fn_expand_depth: u8 = 0,
    predicate_derived: std.ArrayList(ir.Derived) = .empty,
    predicate_derived_counter: usize = 0,
    predicate_derived_enabled: bool = false,
    aggregate_expr_refs: std.ArrayList(AggExprRef) = .empty,
    aggregate_expr_counter: usize = 0,
    aggregate_expr_refs_enabled: bool = false,
    window_expr_refs: std.ArrayList(WindowExprRef) = .empty,
    window_expr_counter: usize = 0,
    window_partition_expr_refs: std.ArrayList(ir.Derived) = .empty,
    window_partition_expr_counter: usize = 0,

    pub fn advance(self: *Parser) ParseError!void {
        self.cur = try self.lex.next();
        // SQL table-function expansion: inside a function body, parameter
        // identifiers resolve to the call's literal argument tokens. One
        // interception point makes parameters work anywhere an expression
        // can appear. Case-insensitive, like column names.
        if (self.param_bindings) |bindings| {
            if (self.cur.tag == .identifier) {
                var it = bindings.iterator();
                while (it.next()) |e| {
                    if (std.ascii.eqlIgnoreCase(e.key_ptr.*, self.cur.text)) {
                        self.cur = e.value_ptr.*;
                        break;
                    }
                }
            }
        }
    }

    /// The full SQL source being parsed.
    pub fn sourceText(self: *const Parser) []const u8 {
        return self.lex.src;
    }

    /// Current lexer byte offset: while `cur` holds token X this is X's
    /// source END (the parser runs one token ahead). The CREATE FUNCTION
    /// body capture recovers raw-text spans from it — token `.text` can't
    /// serve as an offset source (identifier text is an arena-lowercased
    /// copy, not a source slice).
    pub fn lexPos(self: *const Parser) usize {
        return self.lex.pos;
    }

    pub fn aggregateFuncForName(self: *const Parser, name: []const u8) ?ir.AggFunc {
        if (aggForName(name)) |func| return func;
        if (self.udf_registry) |registry| {
            if (registry.hasAggregateName(name)) return .udf;
        }
        return null;
    }

    pub fn expect(self: *Parser, tag: TokenTag) ParseError!void {
        if (self.cur.tag != tag) return ParseError.SqlExpectedToken;
        try self.advance();
    }

    fn expectEof(self: *Parser) ParseError!void {
        if (self.cur.tag == .semicolon) try self.advance();
        if (self.cur.tag != .eof) return ParseError.SqlTrailingTokens;
    }

    /// Consume the optional clauses between `EXPLAIN` and the explained
    /// statement, in both dialect spellings, returning the requested
    /// output format. Accepts MySQL `[ANALYZE] [FORMAT [=] fmt]` and PG
    /// `[ANALYZE] | ( option [, option]* )`. `ANALYZE` is consumed but
    /// currently has no effect (aliases plain EXPLAIN); unknown options
    /// inside parentheses are skipped. Only `FORMAT JSON` selects JSON;
    /// every other format name (TEXT/TREE/TRADITIONAL/…) maps to text.
    fn parseExplainOptions(self: *Parser) ParseError!ir.ExplainFormat {
        if (self.cur.tag == .lparen) {
            try self.advance(); // (
            var format: ir.ExplainFormat = .text;
            while (self.cur.tag != .rparen) {
                if (self.cur.tag == .eof) return ParseError.SqlExpectedToken;
                if (self.cur.tag == .identifier and std.ascii.eqlIgnoreCase(self.cur.text, "format")) {
                    try self.advance(); // FORMAT
                    if (self.cur.tag == .identifier) {
                        format = explainFormatFromName(self.cur.text);
                        try self.advance();
                    }
                } else {
                    try self.advance(); // skip an unrelated option token
                }
                if (self.cur.tag == .comma) try self.advance();
            }
            try self.advance(); // )
            return format;
        }
        if (self.cur.tag == .identifier and std.ascii.eqlIgnoreCase(self.cur.text, "analyze")) {
            try self.advance();
        }
        if (self.cur.tag == .identifier and std.ascii.eqlIgnoreCase(self.cur.text, "format")) {
            try self.advance(); // FORMAT
            if (self.cur.tag == .eq) try self.advance(); // optional =
            if (self.cur.tag == .identifier) {
                const fmt = explainFormatFromName(self.cur.text);
                try self.advance();
                return fmt;
            }
        }
        return .text;
    }

    /// ANSI / PG row-limiting clause (the alternative to LIMIT), parsed
    /// after ORDER BY:
    ///   [OFFSET n {ROW|ROWS}] [FETCH {FIRST|NEXT} [count] {ROW|ROWS} ONLY]
    /// `count` defaults to 1. Both clauses are optional (absent → no change).
    fn parseFetchOffset(self: *Parser, limit_out: *?u64, offset_out: *u64) ParseError!void {
        if (self.cur.tag == .kw_offset) {
            try self.advance();
            if (self.cur.tag != .integer) return ParseError.SqlExpectedValue;
            const off = self.cur.value.integer;
            try self.advance();
            if (off < 0) return ParseError.SqlExpectedValue;
            offset_out.* = @intCast(off);
            if (self.cur.tag == .kw_row or self.cur.tag == .kw_rows) try self.advance();
        }
        if (self.cur.tag == .identifier and std.ascii.eqlIgnoreCase(self.cur.text, "fetch")) {
            try self.advance();
            if (!(self.cur.tag == .identifier and
                (std.ascii.eqlIgnoreCase(self.cur.text, "first") or std.ascii.eqlIgnoreCase(self.cur.text, "next"))))
                return ParseError.SqlExpectedKeyword;
            try self.advance();
            var count: i64 = 1;
            if (self.cur.tag == .integer) {
                count = self.cur.value.integer;
                try self.advance();
                if (count < 0) return ParseError.SqlExpectedValue;
            }
            if (self.cur.tag == .kw_row or self.cur.tag == .kw_rows) try self.advance() else return ParseError.SqlExpectedKeyword;
            if (self.cur.tag == .identifier and std.ascii.eqlIgnoreCase(self.cur.text, "only")) try self.advance() else return ParseError.SqlExpectedKeyword;
            limit_out.* = @intCast(count);
        }
    }

    pub fn parseStatement(self: *Parser) ParseError!*ir.Op {
        // DDL / SHOW / INSERT are leading-keyword forms that don't combine
        // with WITH. They have no projection / FROM / WHERE / etc.;
        // dispatch before the SELECT-only path.
        switch (self.cur.tag) {
            .kw_create, .kw_drop, .kw_use, .kw_alter, .kw_rename, .kw_truncate => return try parse_ddl.parseDdl(self),
            .kw_show => return try parse_ddl.parseShow(self),
            .kw_explain => {
                try self.advance(); // consume EXPLAIN
                const format = try self.parseExplainOptions();
                const inner = try self.parseStatement();
                return try self.allocOp(.{ .explain = .{ .inner = inner, .format = format } });
            },
            .kw_insert => return try parse_ddl.parseInsert(self),
            .kw_replace => return try parse_ddl.parseReplace(self),
            .kw_copy => return try parse_ddl.parseCopy(self),
            .kw_set => return try self.parseSetVar(),
            .kw_delete => return try self.parseDelete(),
            .kw_update => return try self.parseUpdate(),
            // REFRESH MATERIALIZED VIEW — `refresh` is contextual (not a
            // reserved keyword), so match it by identifier text here.
            .identifier => {
                if (std.ascii.eqlIgnoreCase(self.cur.text, "refresh")) {
                    return try parse_ddl.parseRefresh(self);
                }
            },
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

        // Optional DISTINCT — desugars to grouping on every projected item
        // (resolved after the projection list is parsed). DISTINCT combined
        // with aggregates / GROUP BY / HAVING / window functions / `*` is
        // rejected: those need a second dedup layer above the aggregate.
        var distinct = false;
        if (self.cur.tag == .kw_distinct) {
            distinct = true;
            try self.advance();
        }

        // Projection list. Scalar projection expressions may contain
        // aggregate calls; collect those as hidden aggregate outputs so the
        // scalar wrapper can run above GROUP BY.
        const agg_ref_mark = self.aggregate_expr_refs.items.len;
        const window_ref_mark = self.window_expr_refs.items.len;
        const window_partition_ref_mark = self.window_partition_expr_refs.items.len;
        const projection_pred_mark = self.predicate_derived.items.len;
        const old_aggregate_expr_refs_enabled = self.aggregate_expr_refs_enabled;
        const old_predicate_derived_enabled = self.predicate_derived_enabled;
        self.aggregate_expr_refs_enabled = true;
        self.predicate_derived_enabled = true;
        errdefer self.aggregate_expr_refs_enabled = old_aggregate_expr_refs_enabled;
        errdefer self.predicate_derived_enabled = old_predicate_derived_enabled;
        errdefer self.aggregate_expr_refs.shrinkRetainingCapacity(agg_ref_mark);
        errdefer self.window_expr_refs.shrinkRetainingCapacity(window_ref_mark);
        errdefer self.window_partition_expr_refs.shrinkRetainingCapacity(window_partition_ref_mark);
        errdefer self.predicate_derived.shrinkRetainingCapacity(projection_pred_mark);
        const proj = try self.parseProjection();
        self.aggregate_expr_refs_enabled = old_aggregate_expr_refs_enabled;
        self.predicate_derived_enabled = old_predicate_derived_enabled;
        const aggregate_expr_refs = try self.arena.dupe(AggExprRef, self.aggregate_expr_refs.items[agg_ref_mark..]);
        self.aggregate_expr_refs.shrinkRetainingCapacity(agg_ref_mark);
        const window_expr_refs = try self.arena.dupe(WindowExprRef, self.window_expr_refs.items[window_ref_mark..]);
        self.window_expr_refs.shrinkRetainingCapacity(window_ref_mark);
        const projection_predicate_derived = try self.arena.dupe(ir.Derived, self.predicate_derived.items[projection_pred_mark..]);
        self.predicate_derived.shrinkRetainingCapacity(projection_pred_mark);

        // FROM clause — supports a single table or chained JOINs. A
        // missing FROM is a FROM-less SELECT (`SELECT 1+1`, `SELECT now()`):
        // evaluate the projection over one synthetic row.
        var root: *ir.Op = undefined;
        if (self.cur.tag == .kw_from) {
            try self.advance();
            root = try self.parseFromClause();
        } else {
            root = try self.allocOp(.{ .single_row = {} });
        }

        // Optional WHERE.
        if (self.cur.tag == .kw_where) {
            try self.advance();
            const derived_mark = self.predicate_derived.items.len;
            const old_enabled = self.predicate_derived_enabled;
            self.predicate_derived_enabled = true;
            defer self.predicate_derived_enabled = old_enabled;
            const pred = try self.parseBoolExpr();
            if (self.predicate_derived.items.len > derived_mark) {
                const derived = try self.arena.dupe(ir.Derived, self.predicate_derived.items[derived_mark..]);
                self.predicate_derived.shrinkRetainingCapacity(derived_mark);
                root = try self.allocOp(.{ .compute = .{ .derived = derived, .upstream = root } });
            }
            root = try self.allocOp(.{ .filter = .{ .predicate = pred, .upstream = root } });
        }

        // Optional GROUP BY. Each item is a general expression so we
        // accept ordinals (`GROUP BY 1`), aliases (`GROUP BY m`), plain
        // columns, and computed keys (`GROUP BY ClientIP - 1`,
        // `GROUP BY date_trunc(...)`). Resolution against the projection
        // happens once we know the SELECT shape.
        var group_exprs: []const ir.Expr = &.{};
        if (self.cur.tag == .kw_group) {
            try self.advance();
            try self.expect(.kw_by);
            group_exprs = try self.parseGroupByExprs();
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
        const window_partition_expr_refs = try self.arena.dupe(ir.Derived, self.window_partition_expr_refs.items[window_partition_ref_mark..]);
        self.window_partition_expr_refs.shrinkRetainingCapacity(window_partition_ref_mark);

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
            if (aggregate_expr_refs.len > 0) break :blk true;
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
            if (window_expr_refs.len > 0) break :blk true;
            for (proj) |p| switch (p.kind) {
                .window => break :blk true,
                else => {},
            };
            break :blk false;
        };
        const has_group = group_exprs.len > 0;
        if (aggregate_expr_refs.len > 0 and !has_group) return ParseError.SqlInvalidProjection;
        var hidden_agg_cols: []const []const u8 = &.{};
        if (aggregate_expr_refs.len > 0) {
            const cols = try self.arena.alloc([]const u8, aggregate_expr_refs.len);
            for (aggregate_expr_refs, cols) |ref, *dst| dst.* = ref.name;
            hidden_agg_cols = cols;
        }
        var hidden_window_cols: []const []const u8 = &.{};
        if (window_expr_refs.len > 0) {
            const cols = try self.arena.alloc([]const u8, window_expr_refs.len);
            for (window_expr_refs, cols) |ref, *dst| dst.* = ref.name;
            hidden_window_cols = cols;
        }
        var post_window_expr: []bool = &.{};
        if (hidden_window_cols.len > 0) {
            post_window_expr = try self.arena.alloc(bool, proj.len);
            @memset(post_window_expr, false);
            for (proj, 0..) |p, i| switch (p.kind) {
                .expr => |e| {
                    if (exprReferencesAnyColumn(e, hidden_window_cols)) post_window_expr[i] = true;
                },
                else => {},
            };
        }
        // Resolve GROUP BY items against the projection. Produces the
        // grouping key column names and a per-projection flag marking
        // which items are grouping keys (so a computed grouping key like
        // `extract(...) AS m` is allowed alongside aggregates and gets
        // hoisted into a pre-aggregate Compute). For non-grouped queries
        // this is empty and the scalar-only path below runs unchanged.
        var group_cols: []const []const u8 = &.{};
        var grouping_key: []bool = &.{};
        // Per-projection flag: a deterministic scalar expression over grouped
        // output columns (e.g. `k + 1` under `GROUP BY k`) that isn't itself a
        // grouping key — computed once per group ABOVE the aggregate.
        var post_group_expr: []bool = &.{};
        if (distinct) {
            // SELECT DISTINCT a, b, expr ≡ SELECT a, b, expr GROUP BY 1, 2, 3:
            // every projected item becomes a grouping key (markGroupKey
            // rejects `*`, aggregates, and window items). The grouped path
            // below then adds a hidden COUNT(*) — the engines need at least
            // one aggregate — and a final Project drops it.
            if (has_agg or has_group or pending_having != null) return ParseError.SqlInvalidProjection;
            var dcols: std.ArrayList([]const u8) = .empty;
            defer dcols.deinit(self.arena);
            const dgk = try self.arena.alloc(bool, proj.len);
            @memset(dgk, false);
            for (proj, 0..) |_, i| try self.markGroupKey(proj, dgk, i, &dcols);
            group_cols = try dcols.toOwnedSlice(self.arena);
            grouping_key = dgk;
        } else if (has_agg or has_group) {
            const res = try self.resolveGroupBy(proj, group_exprs);
            group_cols = res.cols;
            grouping_key = res.gk;
            post_group_expr = try self.arena.alloc(bool, proj.len);
            @memset(post_group_expr, false);
            // Every projection must be an aggregate, a grouping key, or a
            // deterministic scalar expression over grouped output columns.
            // Anything that reads a non-grouped source column is ambiguous.
            for (proj, 0..) |p, i| switch (p.kind) {
                .agg => {},
                .col => |c| if (!grouping_key[i] and !nameInList(c, group_cols)) return ParseError.SqlMixedAggAndPlainProjection,
                .expr => |e| {
                    if (!grouping_key[i]) {
                        if (!exprAvailableAfterGroup(e, group_cols, hidden_agg_cols)) return ParseError.SqlMixedAggAndPlainProjection;
                        post_group_expr[i] = true;
                    }
                },
                .window => {},
                .star => return ParseError.SqlMixedAggAndPlainProjection,
            };
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
            pending_order_specs = try self.parseOrderBy(proj);
        }

        // Optional LIMIT, in either of MySQL's two forms:
        //   LIMIT count
        //   LIMIT offset, count        (offset first)
        //   LIMIT count OFFSET offset  (OFFSET keyword)
        var pending_limit: ?u64 = null;
        var pending_offset: u64 = 0;
        if (self.cur.tag == .kw_limit) {
            try self.advance();
            if (self.cur.tag != .integer) return ParseError.SqlExpectedValue;
            const n = self.cur.value.integer;
            try self.advance();
            if (n < 0) return ParseError.SqlExpectedValue;
            if (self.cur.tag == .comma) {
                // LIMIT offset, count — first number is the offset.
                try self.advance();
                if (self.cur.tag != .integer) return ParseError.SqlExpectedValue;
                const count = self.cur.value.integer;
                try self.advance();
                if (count < 0) return ParseError.SqlExpectedValue;
                pending_offset = @intCast(n);
                pending_limit = @intCast(count);
            } else {
                pending_limit = @intCast(n);
                if (self.cur.tag == .kw_offset) {
                    try self.advance();
                    if (self.cur.tag != .integer) return ParseError.SqlExpectedValue;
                    const off = self.cur.value.integer;
                    try self.advance();
                    if (off < 0) return ParseError.SqlExpectedValue;
                    pending_offset = @intCast(off);
                }
            }
        } else {
            try self.parseFetchOffset(&pending_limit, &pending_offset);
        }

        if (has_agg or has_group or distinct) {
            if (projection_predicate_derived.len > 0) {
                root = try self.allocOp(.{ .compute = .{ .derived = projection_predicate_derived, .upstream = root } });
            }
            // Functional-dependency group-key collapse (pre-execution
            // rewrite). A computed grouping key that is a pure, deterministic
            // function of other *retained* group keys — or a constant — adds
            // no grouping distinctions, so we drop it from the GroupBy and
            // recompute it once per output group ABOVE the aggregate. This
            // shrinks the group key and moves the per-row arithmetic from the
            // full input to the (typically far smaller) group output. The
            // anchor set is the plain-column group keys; a `.expr` key collapses
            // iff every column it references is one of those anchors (or it
            // references none, i.e. a constant). Soundness: anchors are always
            // retained, so the collapsed key remains a deterministic function
            // of surviving keys — grouping on the anchors alone yields the same
            // partition (NULLs included: f(NULL) is computed per group).
            const collapse = try self.arena.alloc(bool, proj.len);
            @memset(collapse, false);
            // Only collapse onto a non-empty anchor set: at least one
            // plain-column grouping key must survive. Otherwise (a GROUP BY
            // made entirely of constants / expressions) collapsing everything
            // would turn the aggregate global, which differs on empty input
            // (`GROUP BY 1` yields zero rows; a global aggregate yields one).
            var has_anchor = false;
            for (proj, 0..) |p, i| {
                if (grouping_key[i] and p.kind == .col) has_anchor = true;
            }
            if (has_anchor) {
                for (proj, 0..) |p, i| {
                    if (!grouping_key[i]) continue;
                    switch (p.kind) {
                        .expr => |e| if (exprCollapsesOnto(proj, grouping_key, e)) {
                            collapse[i] = true;
                        },
                        else => {},
                    }
                }
            }

            // Pre-aggregate Compute. Two kinds of synthetic columns land
            // here, computed before the GroupBy:
            //   1. computed grouping keys — a `.expr` projection that a
            //      GROUP BY item resolves to (e.g. `extract(...) AS m`,
            //      `ClientIP - 1`). Output name = the projection's name,
            //      which the GroupBy then groups on. Collapsed keys are
            //      omitted here and recomputed in the post-aggregate Compute.
            //   2. aggregate-on-expression args (e.g. SUM(a * b)) — a
            //      synthetic `__agg_arg_N` column the AggSpec references.
            var derived_buf: std.ArrayList(ir.Derived) = .empty;
            for (proj, 0..) |p, i| switch (p.kind) {
                .expr => |e| if (grouping_key[i] and !collapse[i]) {
                    try derived_buf.append(self.arena, .{ .name = p.name, .expr = e });
                },
                else => {},
            };
            var synth_counter: usize = 0;
            var agg_cols: std.ArrayList(?[]const u8) = .empty;
            defer agg_cols.deinit(self.arena);
            var agg_arg2_cols: std.ArrayList(?[]const u8) = .empty;
            defer agg_arg2_cols.deinit(self.arena);
            for (proj) |p| switch (p.kind) {
                .agg => |a| try self.appendAggInputs(a, &derived_buf, &agg_cols, &agg_arg2_cols, &synth_counter),
                else => {},
            };
            for (aggregate_expr_refs) |ref| {
                try self.appendAggInputs(ref.agg, &derived_buf, &agg_cols, &agg_arg2_cols, &synth_counter);
            }
            if (derived_buf.items.len > 0) {
                const derived_slice = try derived_buf.toOwnedSlice(self.arena);
                root = try self.allocOp(.{ .compute = .{ .derived = derived_slice, .upstream = root } });
            }

            // Build agg specs from the projection.
            var aggs_buf: std.ArrayList(ir.AggSpec) = .empty;
            var agg_i: usize = 0;
            for (proj) |p| switch (p.kind) {
                .agg => |a| try self.appendAggSpec(&aggs_buf, a, p.name, agg_cols.items, agg_arg2_cols.items, &agg_i),
                else => {},
            };
            for (aggregate_expr_refs) |ref| {
                try self.appendAggSpec(&aggs_buf, ref.agg, ref.name, agg_cols.items, agg_arg2_cols.items, &agg_i);
            }
            // Drop collapsed keys from the grouping columns. Collapsed `.expr`
            // keys live in `group_cols` under their projection name; build the
            // reduced list (and remember the dropped names so we recompute
            // them above the aggregate).
            var collapsed_names: std.ArrayList([]const u8) = .empty;
            defer collapsed_names.deinit(self.arena);
            var collapsed_exprs: std.ArrayList(ir.Derived) = .empty;
            defer collapsed_exprs.deinit(self.arena);
            for (proj, 0..) |p, i| {
                if (!collapse[i]) continue;
                switch (p.kind) {
                    .expr => |e| {
                        try collapsed_names.append(self.arena, p.name);
                        try collapsed_exprs.append(self.arena, .{ .name = p.name, .expr = e });
                    },
                    else => {},
                }
            }
            // Post-group scalar expressions (e.g. `k + 1` under `GROUP BY k`)
            // are computed once per group above the aggregate too, but they are
            // NOT grouping keys — they read the retained group columns and are
            // never dropped from the group set.
            for (proj, 0..) |p, i| {
                if (i >= post_group_expr.len or !post_group_expr[i]) continue;
                switch (p.kind) {
                    .expr => |e| try collapsed_exprs.append(self.arena, .{ .name = p.name, .expr = e }),
                    else => {},
                }
            }
            const retained_group_cols = if (collapsed_names.items.len == 0)
                group_cols
            else blk: {
                var kept: std.ArrayList([]const u8) = .empty;
                for (group_cols) |gc| {
                    var dropped = false;
                    for (collapsed_names.items) |cn| {
                        if (types.columnNameEql(gc, cn)) {
                            dropped = true;
                            break;
                        }
                    }
                    if (!dropped) try kept.append(self.arena, gc);
                }
                break :blk try kept.toOwnedSlice(self.arena);
            };

            // DISTINCT contributes no aggregates of its own, but the grouped
            // cores require at least one — add a hidden COUNT(*); the forced
            // Project below drops it from the output.
            if (distinct) {
                try aggs_buf.append(self.arena, .{ .func = .count, .col = null, .as = "__distinct_count" });
            }

            const aggs_slice = try aggs_buf.toOwnedSlice(self.arena);
            root = try self.allocOp(.{ .group_by = .{
                .group_cols = retained_group_cols,
                .aggs = aggs_slice,
                .upstream = root,
            } });

            // Recompute the collapsed keys once per output group, directly
            // above the GroupBy so HAVING / ORDER BY / the final Project all
            // see them. Each derived expression now reads the retained group
            // columns (one row per group instead of one row per input row).
            if (collapsed_exprs.items.len > 0) {
                const above = try collapsed_exprs.toOwnedSlice(self.arena);
                root = try self.allocOp(.{ .compute = .{ .derived = above, .upstream = root } });
            }

            // HAVING after GroupBy, before ORDER BY. The Filter sees the
            // post-aggregate schema. A raw aggregate reference (e.g.
            // `HAVING COUNT(*) > 100000`) arrives as the canonical name
            // `count(*)`; rewrite it to the matching SELECT aggregate's
            // alias so it binds to the grouped output column.
            if (pending_having) |*pred| {
                try self.rewriteHavingAggRefs(pred, proj);
                root = try self.allocOp(.{ .filter = .{ .predicate = pred.*, .upstream = root } });
            }

            if (has_window) {
                if (try buildWindowOp(self.arena, proj, window_expr_refs, window_partition_expr_refs, root, &self.named_windows)) |win| {
                    root = win;
                }
            }
            if (post_window_expr.len > 0) {
                var post_window_buf: std.ArrayList(ir.Derived) = .empty;
                for (proj, 0..) |p, i| {
                    if (!post_window_expr[i]) continue;
                    switch (p.kind) {
                        .expr => |e| try post_window_buf.append(self.arena, .{ .name = p.name, .expr = e }),
                        else => {},
                    }
                }
                if (post_window_buf.items.len > 0) {
                    const derived_slice = try post_window_buf.toOwnedSlice(self.arena);
                    root = try self.allocOp(.{ .compute = .{ .derived = derived_slice, .upstream = root } });
                }
            }
            if (pending_qualify) |pred| {
                if (!has_window) return ParseError.SqlInvalidProjection;
                root = try self.allocOp(.{ .filter = .{ .predicate = pred, .upstream = root } });
            }

            // Apply ORDER BY on the grouped schema.
            if (pending_order_specs) |specs| {
                root = try self.allocOp(.{ .order_by = .{ .specs = specs, .upstream = root } });
            }

            // Reorder output to match the SELECT list if necessary. The
            // GroupBy emits group_cols first then aggs in registered order;
            // a Project on top reorders/keeps only the SELECT items. DISTINCT
            // always projects — its hidden COUNT(*) must not reach the output.
            if (distinct or has_window or aggregate_expr_refs.len > 0 or !projMatchesGroupByOrder(proj, group_cols) or projectionHasRenamedCols(proj)) {
                root = try self.addSelectProject(root, proj, 0);
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
            if (projection_predicate_derived.len > 0) {
                root = try self.allocOp(.{ .compute = .{ .derived = projection_predicate_derived, .upstream = root } });
            }
            if (has_expr) {
                var derived_buf: std.ArrayList(ir.Derived) = .empty;
                for (proj, 0..) |p, i| switch (p.kind) {
                    .expr => |e| {
                        if (i < post_window_expr.len and post_window_expr[i]) continue;
                        try derived_buf.append(self.arena, .{ .name = p.name, .expr = e });
                    },
                    else => {},
                };
                if (derived_buf.items.len > 0) {
                    const derived_slice = try derived_buf.toOwnedSlice(self.arena);
                    root = try self.allocOp(.{ .compute = .{ .derived = derived_slice, .upstream = root } });
                }
            }
            if (has_window) {
                if (try buildWindowOp(self.arena, proj, window_expr_refs, window_partition_expr_refs, root, &self.named_windows)) |win| {
                    root = win;
                }
            }
            if (post_window_expr.len > 0) {
                var post_window_buf: std.ArrayList(ir.Derived) = .empty;
                for (proj, 0..) |p, i| {
                    if (!post_window_expr[i]) continue;
                    switch (p.kind) {
                        .expr => |e| try post_window_buf.append(self.arena, .{ .name = p.name, .expr = e }),
                        else => {},
                    }
                }
                if (post_window_buf.items.len > 0) {
                    const derived_slice = try post_window_buf.toOwnedSlice(self.arena);
                    root = try self.allocOp(.{ .compute = .{ .derived = derived_slice, .upstream = root } });
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
            if (!isBareStarProjection(proj)) {
                root = try self.addSelectProject(root, proj, selectDerivedCount(proj));
            }
        }
        // Optional LIMIT / OFFSET applies last. A bare OFFSET (no limit,
        // e.g. ANSI `OFFSET n ROWS`) still needs the operator, with an
        // effectively-unbounded count.
        if (pending_limit != null or pending_offset > 0) {
            const n = pending_limit orelse std.math.maxInt(u64);
            root = try self.allocOp(.{ .limit = .{ .n = n, .offset = pending_offset, .upstream = root } });
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

        try self.parseSeparableClause();
        return root;
    }

    /// Trailing `[GLOBAL | LOCAL] SEPARABLE BY (col[, col...])` — the block
    /// separability declaration. The clause belongs to the CTE whose body it
    /// terminates; parseCteList consumes `pending_separable` and attaches it
    /// to that CTE's materialize node. A clause left pending anywhere else
    /// (statement root, subquery, union arm) is rejected — v1 supports LOCAL
    /// CTE scope only.
    fn parseSeparableClause(self: *Parser) ParseError!void {
        if (self.cur.tag != .identifier) return;
        var global = false;
        if (std.ascii.eqlIgnoreCase(self.cur.text, "global")) {
            global = true;
        } else if (std.ascii.eqlIgnoreCase(self.cur.text, "local")) {
            global = false;
        } else if (!std.ascii.eqlIgnoreCase(self.cur.text, "separable")) {
            return;
        }
        if (global or std.ascii.eqlIgnoreCase(self.cur.text, "local")) {
            try self.advance();
            if (self.cur.tag != .identifier or !std.ascii.eqlIgnoreCase(self.cur.text, "separable"))
                return ParseError.SqlExpectedKeyword;
        }
        try self.advance(); // SEPARABLE
        if (global) return ParseError.SqlSeparableScope; // v1: LOCAL only
        if (self.cur.tag != .kw_by) return ParseError.SqlExpectedKeyword;
        try self.advance();
        try self.expect(.lparen);
        var cols: std.ArrayListUnmanaged([]const u8) = .empty;
        while (true) {
            try cols.append(self.arena, try self.dupedIdent());
            if (self.cur.tag != .comma) break;
            try self.advance();
        }
        try self.expect(.rparen);
        if (self.pending_separable != null) return ParseError.SqlSeparableScope;
        self.pending_separable = .{ .cols = cols.items };
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
            // After a dot only a name can follow, so reserved words are
            // identifiers here — `information_schema.tables`, `x.columns`.
            if (self.cur.tag != .identifier and !wordLikeToken(self.cur.text)) return ParseError.SqlExpectedIdent;
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

    fn wordLikeToken(text: []const u8) bool {
        if (text.len == 0) return false;
        if (!std.ascii.isAlphabetic(text[0]) and text[0] != '_') return false;
        for (text[1..]) |c| {
            if (!std.ascii.isAlphanumeric(c) and c != '_') return false;
        }
        return true;
    }

    pub fn dupedIdent(self: *Parser) ParseError![]const u8 {
        if (self.cur.tag != .identifier) return ParseError.SqlExpectedIdent;
        const out = try self.arena.dupe(u8, self.cur.text);
        try self.advance();
        return out;
    }

    fn addSelectProject(
        self: *Parser,
        upstream: *ir.Op,
        proj: []const ProjItem,
        star_skip_trailing: u32,
    ) ParseError!*ir.Op {
        const cols = try self.arena.alloc([]const u8, proj.len);
        const outputs = try self.arena.alloc(?[]const u8, proj.len);
        const replace_on_collision = try self.arena.alloc(bool, proj.len);
        var any_output = false;
        var any_replace = false;
        for (proj, 0..) |p, i| {
            cols[i] = try self.projectSourceName(p);
            outputs[i] = projectOutputName(p);
            if (outputs[i] != null) any_output = true;
            replace_on_collision[i] = projectMayReplaceOutput(p);
            if (replace_on_collision[i]) any_replace = true;
        }
        return try self.allocOp(.{ .select = .{
            .columns = cols,
            .outputs = if (any_output) outputs else null,
            .replace_on_collision = if (any_replace) replace_on_collision else null,
            .star_skip_trailing = star_skip_trailing,
            .upstream = upstream,
        } });
    }

    fn projectSourceName(self: *Parser, p: ProjItem) ParseError![]const u8 {
        return switch (p.kind) {
            .star => |qual| if (qual) |q|
                try std.fmt.allocPrint(self.arena, "{s}.*", .{q})
            else
                "*",
            .col => |c| c,
            .agg, .expr, .window => p.name,
        };
    }

    fn projectOutputName(p: ProjItem) ?[]const u8 {
        return switch (p.kind) {
            .col => |c| if (types.columnNameEql(c, p.name)) null else p.name,
            else => null,
        };
    }

    fn projectMayReplaceOutput(p: ProjItem) bool {
        return switch (p.kind) {
            .expr, .window => true,
            .col => projectOutputName(p) != null,
            else => false,
        };
    }

    fn parseProjection(self: *Parser) ParseError![]const ProjItem {
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
        if (self.cur.tag == .star) {
            try self.advance();
            return ProjItem{ .name = "*", .kind = .{ .star = null } };
        }

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

        // Literal at projection start: `SELECT 1`, `SELECT 'x'`,
        // `SELECT 1 + 2`. Route through the expression parser so binary
        // operators and aliasing work. (`GROUP BY 1` then references it
        // as ordinal 1.)
        switch (self.cur.tag) {
            .plus, .minus, .integer, .floating, .string, .kw_true, .kw_false, .kw_null => {
                const expr = try self.parseAddSub();
                const default_name = try self.exprDefaultName(expr);
                const alias = try self.maybeAlias(default_name);
                return ProjItem{ .name = alias, .kind = .{ .expr = expr } };
            },
            else => {},
        }

        // EXTRACT(field FROM expr) — special function form. Detected
        // here before the identifier-then-`(` branch below misparses
        // the field name as a regular call arg.
        if (self.cur.tag == .identifier and std.ascii.eqlIgnoreCase(self.cur.text, "extract")) {
            const saved = self.cur;
            try self.advance();
            if (self.cur.tag == .lparen) {
                const expr = try self.parseExtractCall();
                const default_name = try self.exprDefaultName(expr);
                const alias = try self.maybeAlias(default_name);
                return ProjItem{ .name = alias, .kind = .{ .expr = expr } };
            }
            // Bare `extract` as a column name — treat as col_ref.
            const dup_col = try self.arena.dupe(u8, saved.text);
            const alias = try self.maybeAlias(dup_col);
            return ProjItem{ .name = alias, .kind = .{ .col = dup_col } };
        }

        // CAST(expr AS type) at projection top level. Detected before the
        // identifier-then-`(` call branch (which would choke on `AS`).
        if (self.cur.tag == .identifier and std.ascii.eqlIgnoreCase(self.cur.text, "cast")) {
            const saved = self.cur;
            try self.advance();
            if (self.cur.tag == .lparen) {
                try self.advance();
                if (try self.tryParseCastWrappedAggregate()) |agg| return agg;
                const inner = try self.parseCallArg();
                if (self.cur.tag != .kw_as) return ParseError.SqlExpectedKeyword;
                try self.advance();
                var expr = try self.parseCastTarget(inner);
                try self.expect(.rparen);
                while (self.cur.tag == .coloncolon and self.lex.dialect != .mysql) {
                    try self.advance();
                    expr = try self.parseCastTarget(expr);
                }
                expr = try self.continueBinaryFrom(expr);
                const default_name = try self.exprDefaultName(expr);
                const alias = try self.maybeAlias(default_name);
                return ProjItem{ .name = alias, .kind = .{ .expr = expr } };
            }
            const dup_col = try self.arena.dupe(u8, saved.text);
            const alias = try self.maybeAlias(dup_col);
            return ProjItem{ .name = alias, .kind = .{ .col = dup_col } };
        }

        // Identifier or function call. Look ahead: `(` after an identifier
        // means a call.
        if (keywordScalarName(self.cur.tag)) |first| {
            try self.advance();
            if (self.cur.tag != .lparen) return ParseError.SqlExpectedToken;
            const scalar_atom = try self.parseScalarCallAfterName(first);
            const expr = try self.continueBinaryFrom(scalar_atom);
            const default_name = try self.exprDefaultName(expr);
            const alias = try self.maybeAlias(default_name);
            return ProjItem{ .name = alias, .kind = .{ .expr = expr } };
        }
        if (self.cur.tag != .identifier) return ParseError.SqlExpectedIdent;
        const first = self.cur.text;
        try self.advance();

        if (try self.typedTemporalLiteralAfterName(first)) |lit| {
            var expr = lit;
            expr = try self.continueBinaryFrom(expr);
            const default_name = try self.exprDefaultName(expr);
            const alias = try self.maybeAlias(default_name);
            return ProjItem{ .name = alias, .kind = .{ .expr = expr } };
        }

        // Function call?
        if (self.cur.tag == .lparen) {
            if (dateAddSubName(first)) |_| {
                const scalar_atom = try self.parseDateAddSubCallAfterName(first);
                const expr = try self.continueBinaryFrom(scalar_atom);
                const default_name = try self.exprDefaultName(expr);
                const alias = try self.maybeAlias(default_name);
                return ProjItem{ .name = alias, .kind = .{ .expr = expr } };
            }
            // Parse the call shape (name + paren-wrapped args) once. Then
            // decide between aggregate / scalar / window based on what
            // follows.
            var saw_distinct = false;
            const args = try self.parseCallArgList(&saw_distinct);
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
                if (self.cur.tag == .kw_is) {
                    try self.advance();
                    var negated = false;
                    if (self.cur.tag == .kw_not) {
                        negated = true;
                        try self.advance();
                    }
                    if (self.cur.tag != .kw_null) return ParseError.SqlExpectedNull;
                    try self.advance();
                    const hidden_name = try self.materializeWindowExpr(call);
                    const expr = try self.windowNullCheckExpr(hidden_name, negated);
                    const default_name: []const u8 = if (negated) "is_not_null" else "is_null";
                    const alias = try self.maybeAlias(default_name);
                    return ProjItem{ .name = alias, .kind = .{ .expr = expr } };
                }
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
            if (self.aggregateFuncForName(first)) |func| {
                if (saw_distinct) {
                    const distinct_func: ir.AggFunc = switch (func) {
                        .count => .count_distinct,
                        .sum => .sum_distinct,
                        .avg => .avg_distinct,
                        else => return ParseError.SqlInvalidProjection,
                    };
                    return try self.aggCallFromArgs(first, distinct_func, args);
                }
                return try self.aggCallFromArgs(first, func, args);
            }

            // DISTINCT is only valid inside an aggregate; a scalar/window
            // call with DISTINCT is a syntax error.
            if (saw_distinct) return ParseError.SqlInvalidProjection;

            // Scalar function call. If a binary operator follows, the
            // whole thing is a binary expression with the call as the
            // leftmost operand — lift into a .expr ProjItem. Otherwise
            // stay as a bare scalar call.
            const scalar_atom = try self.makeScalarCallExpr(first, args);
            const expr = try self.continueBinaryFrom(scalar_atom);
            const default_name = try self.exprDefaultName(expr);
            const alias = try self.maybeAlias(default_name);
            return ProjItem{ .name = alias, .kind = .{ .expr = expr } };
        }

        // Bare CURRENT_TIMESTAMP / CURRENT_DATE (no parens) — nullary
        // temporal functions, not column refs.
        if (self.cur.tag != .dot) {
            if (bareTemporalFn(first)) |fn_name| {
                var e = ir.Expr{ .call = .{ .fn_name = try self.arena.dupe(u8, fn_name), .args = &.{} } };
                e = try self.continueBinaryFrom(e);
                const default_name = try self.exprDefaultName(e);
                const alias = try self.maybeAlias(default_name);
                return ProjItem{ .name = alias, .kind = .{ .expr = e } };
            }
        }

        // Qualified column? `table.col` — preserved as the dotted
        // string `qualifier.col` so a downstream lookup against a
        // scan renamed by `FROM t AS alias` finds the right column.
        const dup_col = blk: {
            if (self.cur.tag == .dot) {
                try self.advance();
                if (self.cur.tag == .star) {
                    try self.advance();
                    const qual = try self.arena.dupe(u8, first);
                    const name = try std.fmt.allocPrint(self.arena, "{s}.*", .{qual});
                    return ProjItem{ .name = name, .kind = .{ .star = qual } };
                }
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

        // A `col::type` postfix cast (PG) and/or a trailing binary
        // operator (`qty + 1`) lift the column ref into an expression.
        if ((self.cur.tag == .coloncolon and self.lex.dialect != .mysql) or isBinaryOpToken(self.cur.tag)) {
            var expr = ir.Expr{ .col_ref = dup_col };
            while (self.cur.tag == .coloncolon and self.lex.dialect != .mysql) {
                try self.advance();
                expr = try self.parseCastTarget(expr);
            }
            expr = try self.continueBinaryFrom(expr);
            const default_name = try self.exprDefaultName(expr);
            const alias = try self.maybeAlias(default_name);
            return ProjItem{ .name = alias, .kind = .{ .expr = expr } };
        }

        const alias = try self.maybeAlias(dup_col);
        return ProjItem{ .name = alias, .kind = .{ .col = dup_col } };
    }

    fn parseScalarCallAfterName(self: *Parser, name: []const u8) ParseError!ir.Expr {
        if (dateAddSubName(name)) |_| return try self.parseDateAddSubCallAfterName(name);
        if (std.ascii.eqlIgnoreCase(name, "if")) return try self.parseIfCallAfterName();
        const args = try self.parseCallArgList(null);
        return try self.makeScalarCallExpr(name, args);
    }

    fn parseIfCallAfterName(self: *Parser) ParseError!ir.Expr {
        try self.expect(.lparen);
        const cond = try self.parseBoolExpr();
        try self.expect(.comma);
        const then_expr = try self.parseCallArg();
        try self.expect(.comma);
        const else_expr = try self.arena.create(ir.Expr);
        else_expr.* = try self.parseCallArg();
        try self.expect(.rparen);

        const branches = try self.arena.alloc(ir.Expr.Branch, 1);
        branches[0] = .{ .cond = cond, .then = then_expr };
        return ir.Expr{ .case = .{ .branches = branches, .else_branch = else_expr } };
    }

    fn normalizeScalarCallArgs(self: *Parser, name: []const u8, args: []const ir.Expr) ParseError![]const ir.Expr {
        if (!unitFirstArgCall(name) or args.len == 0) return args;
        const unit = switch (args[0]) {
            .col_ref => |c| c,
            else => return args,
        };
        if (std.mem.eql(u8, unit, "*")) return args;
        const normalized = try self.arena.alloc(ir.Expr, args.len);
        @memcpy(normalized, args);
        normalized[0] = .{ .lit = .{ .text = try self.arena.dupe(u8, unit) } };
        return normalized;
    }

    fn makeScalarCallExpr(self: *Parser, name: []const u8, args: []const ir.Expr) ParseError!ir.Expr {
        if (std.ascii.eqlIgnoreCase(name, "months_add")) {
            if (args.len != 2) return ParseError.SqlInvalidProjection;
            return ir.Expr{ .call = .{
                .fn_name = try self.arena.dupe(u8, "date_add_months"),
                .args = args,
            } };
        }
        if (std.ascii.eqlIgnoreCase(name, "months_diff")) {
            if (args.len != 2) return ParseError.SqlInvalidProjection;
            const normalized = try self.arena.alloc(ir.Expr, 3);
            normalized[0] = .{ .lit = .{ .text = try self.arena.dupe(u8, "month") } };
            normalized[1] = args[1];
            normalized[2] = args[0];
            return ir.Expr{ .call = .{
                .fn_name = try self.arena.dupe(u8, "date_diff"),
                .args = normalized,
            } };
        }
        return ir.Expr{ .call = .{
            .fn_name = try self.arena.dupe(u8, name),
            .args = try self.normalizeScalarCallArgs(name, args),
        } };
    }

    /// True when `tag` is one of the binary arithmetic operators we
    /// recognize in expression position (+ - * / %).
    fn isBinaryOpToken(tag: TokenTag) bool {
        return switch (tag) {
            .plus, .minus, .star, .slash, .percent, .pipe_pipe, .arrow, .arrow2 => true,
            else => false,
        };
    }

    /// `||` is string concatenation on PG/neutral but logical OR on MySQL;
    /// only treat it as a concat expression operator off the MySQL wire.
    fn concatOpHere(self: *Parser) bool {
        return self.cur.tag == .pipe_pipe and self.lex.dialect != .mysql;
    }

    /// Given an already-parsed left operand `atom`, continue parsing a
    /// binary chain at the precedence-climbing levels. Used by
    /// `parseProjItem` (which has already consumed the leading
    /// identifier/call/dotted-col but not yet checked for an
    /// operator). Returns `atom` unchanged if no operator follows.
    fn continueBinaryFrom(self: *Parser, atom: ir.Expr) ParseError!ir.Expr {
        // JSON `->`/`->>` bind tighter than arithmetic — consume any that
        // trail the already-parsed atom before climbing precedence levels.
        var lhs = try self.consumeJsonArrows(atom);
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
        while (self.cur.tag == .plus or self.cur.tag == .minus or self.concatOpHere()) {
            if (self.concatOpHere()) {
                try self.advance();
                const rhs = try self.parseMulDiv();
                lhs = try self.makeBinary("concat", lhs, rhs);
                continue;
            }
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
        var amount = try self.parseAddSub();
        amount = try self.normalizeIntervalAmount(amount, negate);

        if (self.cur.tag != .identifier) return ParseError.SqlExpectedIdent;
        const unit_word = self.cur.text;
        const fn_name = try self.intervalFunctionName(unit_word);
        try self.advance();

        // Build a 2-arg call: fn(date_expr, n).
        const args = try self.arena.alloc(ir.Expr, 2);
        args[0] = lhs;
        args[1] = amount;
        const name_dup = try self.arena.dupe(u8, fn_name);
        return ir.Expr{ .call = .{ .fn_name = name_dup, .args = args } };
    }

    fn intervalFunctionName(_: *Parser, unit_word: []const u8) ParseError![]const u8 {
        if (std.ascii.eqlIgnoreCase(unit_word, "day") or std.ascii.eqlIgnoreCase(unit_word, "days")) return "date_add";
        if (std.ascii.eqlIgnoreCase(unit_word, "month") or std.ascii.eqlIgnoreCase(unit_word, "months")) return "date_add_months";
        if (std.ascii.eqlIgnoreCase(unit_word, "year") or std.ascii.eqlIgnoreCase(unit_word, "years")) return "date_add_years";
        return ParseError.SqlExpectedKeyword;
    }

    fn normalizeIntervalAmount(self: *Parser, amount: ir.Expr, negate: bool) ParseError!ir.Expr {
        var out = switch (amount) {
            .lit => |v| switch (v) {
                .text => |s| blk: {
                    const n = std.fmt.parseInt(i64, s, 10) catch return ParseError.SqlExpectedValue;
                    if (n < std.math.minInt(i32) or n > std.math.maxInt(i32)) return ParseError.SqlExpectedValue;
                    break :blk ir.Expr{ .lit = .{ .int = @intCast(n) } };
                },
                else => amount,
            },
            else => amount,
        };
        if (negate) out = try self.negateExpr(out);
        return out;
    }

    fn negateExpr(self: *Parser, expr: ir.Expr) ParseError!ir.Expr {
        switch (expr) {
            .lit => |v| switch (v) {
                .int => |x| return ir.Expr{ .lit = .{ .int = -x } },
                .bigint => |x| return ir.Expr{ .lit = .{ .bigint = -x } },
                .smallint => |x| return ir.Expr{ .lit = .{ .smallint = -x } },
                .tinyint => |x| return ir.Expr{ .lit = .{ .tinyint = -x } },
                .float => |x| return ir.Expr{ .lit = .{ .float = -x } },
                .double => |x| return ir.Expr{ .lit = .{ .double = -x } },
                else => {},
            },
            else => {},
        }
        return try self.makeBinary("sub", ir.Expr{ .lit = .{ .int = 0 } }, expr);
    }

    fn parseDateAddSubCallAfterName(self: *Parser, name: []const u8) ParseError!ir.Expr {
        const kind = dateAddSubName(name) orelse return ParseError.SqlInvalidProjection;
        try self.expect(.lparen);
        const base = try self.parseCallArg();
        try self.expect(.comma);

        if (self.cur.tag == .kw_interval) {
            try self.advance();
            var amount = try self.parseAddSub();
            amount = try self.normalizeIntervalAmount(amount, kind == .sub);
            if (self.cur.tag != .identifier) return ParseError.SqlExpectedIdent;
            const fn_name = try self.intervalFunctionName(self.cur.text);
            try self.advance();
            try self.expect(.rparen);
            const args = try self.arena.alloc(ir.Expr, 2);
            args[0] = base;
            args[1] = amount;
            return ir.Expr{ .call = .{ .fn_name = try self.arena.dupe(u8, fn_name), .args = args } };
        }

        const amount = try self.parseCallArg();
        try self.expect(.rparen);
        const args = try self.arena.alloc(ir.Expr, 2);
        args[0] = base;
        args[1] = amount;
        const fn_name: []const u8 = if (kind == .sub) "date_sub" else "date_add";
        return ir.Expr{ .call = .{ .fn_name = try self.arena.dupe(u8, fn_name), .args = args } };
    }

    fn tryParseCastWrappedAggregate(self: *Parser) ParseError!?ProjItem {
        if (self.cur.tag != .identifier) return null;
        const first = self.cur.text;
        const func = self.aggregateFuncForName(first) orelse return null;
        try self.advance();
        if (self.cur.tag != .lparen) return ParseError.SqlExpectedToken;
        var saw_distinct = false;
        const args = try self.parseCallArgList(&saw_distinct);
        if (self.cur.tag != .kw_as) return ParseError.SqlExpectedKeyword;
        try self.advance();
        _ = try parse_ddl.parseColumnType(self);
        try self.expect(.rparen);
        if (saw_distinct) {
            const distinct_func: ir.AggFunc = switch (func) {
                .count => .count_distinct,
                .sum => .sum_distinct,
                .avg => .avg_distinct,
                else => return ParseError.SqlInvalidProjection,
            };
            return try self.aggCallFromArgs(first, distinct_func, args);
        }
        return try self.aggCallFromArgs(first, func, args);
    }

    /// Parse `(arg, arg, ...)`. Cursor is on `(` going in, on the token
    /// after `)` coming out. Returns the args slice. `*` is encoded as
    /// `Expr.col_ref = "*"` (downstream callers — aggregates — recognize
    /// the sentinel; everyone else rejects it).
    /// Parse `( arg, arg, ... )` or `( * )`. A leading `DISTINCT` is
    /// consumed and reported via `distinct_out` (for aggregate calls like
    /// `COUNT(DISTINCT col)`). Passing `null` for `distinct_out` rejects
    /// DISTINCT — it's only valid inside an aggregate.
    pub fn parseCallArgList(self: *Parser, distinct_out: ?*bool) ParseError![]const ir.Expr {
        try self.expect(.lparen);
        if (self.cur.tag == .kw_distinct) {
            try self.advance();
            if (distinct_out) |p| p.* = true else return ParseError.SqlInvalidProjection;
        } else if (distinct_out) |p| {
            p.* = false;
        }
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

    fn appendAggInputs(
        self: *Parser,
        a: ParsedAgg,
        derived_buf: *std.ArrayList(ir.Derived),
        agg_cols: *std.ArrayList(?[]const u8),
        agg_arg2_cols: *std.ArrayList(?[]const u8),
        synth_counter: *usize,
    ) ParseError!void {
        if (a.arg_expr) |e| {
            const owned_name = std.fmt.allocPrint(self.arena, "__agg_arg_{d}", .{synth_counter.*}) catch return ParseError.OutOfMemory;
            synth_counter.* += 1;
            try derived_buf.append(self.arena, .{ .name = owned_name, .expr = e });
            try agg_cols.append(self.arena, owned_name);
        } else {
            try agg_cols.append(self.arena, a.col);
        }
        if (a.arg2_expr) |e| {
            const owned_name = std.fmt.allocPrint(self.arena, "__agg_arg_{d}", .{synth_counter.*}) catch return ParseError.OutOfMemory;
            synth_counter.* += 1;
            try derived_buf.append(self.arena, .{ .name = owned_name, .expr = e });
            try agg_arg2_cols.append(self.arena, owned_name);
        } else {
            try agg_arg2_cols.append(self.arena, a.arg2_col);
        }
    }

    fn appendAggSpec(
        self: *Parser,
        aggs_buf: *std.ArrayList(ir.AggSpec),
        a: ParsedAgg,
        out_name: []const u8,
        agg_cols: []const ?[]const u8,
        agg_arg2_cols: []const ?[]const u8,
        agg_i: *usize,
    ) ParseError!void {
        try aggs_buf.append(self.arena, .{
            .func = a.func,
            .udf_name = a.udf_name,
            .udf_arg_cols = a.udf_arg_cols,
            .col = agg_cols[agg_i.*],
            .arg2_col = agg_arg2_cols[agg_i.*],
            .as = out_name,
            .params = if (a.func == .group_concat)
                switch (a.params) {
                    .separator => a.params,
                    else => .{ .separator = a.separator orelse "," },
                }
            else
                a.params,
        });
        agg_i.* += 1;
    }

    /// Build a ParsedAgg from a pre-parsed args slice. Aggregates
    /// accept either a single column-ref arg or `*` (COUNT only). The
    /// args have already been parsed via the generic call-args path
    /// (which leaves *-as-col_ref `"*"`).
    fn parsedAggFromArgs(
        self: *Parser,
        func_name: []const u8,
        func: ir.AggFunc,
        args: []const ir.Expr,
    ) ParseError!ParsedAggCall {
        if (func == .udf) {
            if (args.len == 0) return ParseError.SqlInvalidProjection;
            const arg_cols = try self.arena.alloc([]const u8, args.len);
            for (args, arg_cols) |arg, *dst| {
                dst.* = switch (arg) {
                    .col_ref => |c| blk: {
                        if (std.mem.eql(u8, c, "*")) return ParseError.SqlInvalidProjection;
                        break :blk try self.arena.dupe(u8, c);
                    },
                    else => return ParseError.SqlInvalidProjection,
                };
            }
            const default_name = blk: {
                var buf: std.ArrayList(u8) = .empty;
                defer buf.deinit(self.arena);
                try buf.appendSlice(self.arena, func_name);
                try buf.append(self.arena, '(');
                for (arg_cols, 0..) |c, i| {
                    if (i > 0) try buf.append(self.arena, ',');
                    try buf.appendSlice(self.arena, c);
                }
                try buf.append(self.arena, ')');
                break :blk try buf.toOwnedSlice(self.arena);
            };
            return .{ .default_name = default_name, .agg = .{
                .func = .udf,
                .udf_name = try self.arena.dupe(u8, func_name),
                .udf_arg_cols = arg_cols,
                .col = arg_cols[0],
            } };
        }

        // GROUP_CONCAT / STRING_AGG take an optional second positional arg:
        // the delimiter string literal. PERCENTILE_CONT(x, p) stores p as
        // an aggregate param; MEDIAN(x) is percentile_cont(x, 0.5).
        if (func == .max_by) {
            if (args.len != 2) return ParseError.SqlInvalidProjection;
            var value_col: ?[]const u8 = null;
            var value_expr: ?ir.Expr = null;
            switch (args[0]) {
                .col_ref => |c| {
                    if (std.mem.eql(u8, c, "*")) return ParseError.SqlInvalidProjection;
                    value_col = try self.arena.dupe(u8, c);
                },
                .call, .case, .lit => value_expr = args[0],
                else => return ParseError.SqlInvalidProjection,
            }
            var key_col: ?[]const u8 = null;
            var key_expr: ?ir.Expr = null;
            switch (args[1]) {
                .col_ref => |c| {
                    if (std.mem.eql(u8, c, "*")) return ParseError.SqlInvalidProjection;
                    key_col = try self.arena.dupe(u8, c);
                },
                .call, .case, .lit => key_expr = args[1],
                else => return ParseError.SqlInvalidProjection,
            }
            const default_name = try self.arena.dupe(u8, "max_by(expr)");
            return .{ .default_name = default_name, .agg = .{
                .func = func,
                .col = value_col,
                .arg_expr = value_expr,
                .arg2_col = key_col,
                .arg2_expr = key_expr,
            } };
        }

        var separator: ?[]const u8 = null;
        var params: ir.AggParams = .none;
        var value_args = args;
        if (func == .group_concat and args.len == 2) {
            separator = switch (args[1]) {
                .lit => |v| switch (v) {
                    .text => |s| try self.arena.dupe(u8, s),
                    else => return ParseError.SqlInvalidProjection,
                },
                else => return ParseError.SqlInvalidProjection,
            };
            params = .{ .separator = separator.? };
            value_args = args[0..1];
        } else if (func == .percentile) {
            if (std.ascii.eqlIgnoreCase(func_name, "median")) {
                if (args.len != 1) return ParseError.SqlInvalidProjection;
                params = .{ .percentile = 0.5 };
                value_args = args[0..1];
            } else {
                if (args.len != 2) return ParseError.SqlInvalidProjection;
                params = .{ .percentile = switch (args[1]) {
                    .lit => |v| switch (v) {
                        .double => |x| x,
                        .float => |x| x,
                        .int => |x| @floatFromInt(x),
                        .bigint => |x| @floatFromInt(x),
                        else => return ParseError.SqlInvalidProjection,
                    },
                    else => return ParseError.SqlInvalidProjection,
                } };
                value_args = args[0..1];
            }
        }
        if (value_args.len != 1) return ParseError.SqlInvalidProjection;
        var arg_col: ?[]const u8 = null;
        var arg_expr: ?ir.Expr = null;
        switch (value_args[0]) {
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
            .call, .case, .lit => arg_expr = value_args[0],
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
        return .{ .default_name = default_name, .agg = .{
            .func = func,
            .udf_name = if (func == .udf) try self.arena.dupe(u8, func_name) else null,
            .col = arg_col,
            .arg_expr = arg_expr,
            .separator = separator,
            .params = params,
        } };
    }

    fn aggCallFromArgs(
        self: *Parser,
        func_name: []const u8,
        func: ir.AggFunc,
        args: []const ir.Expr,
    ) ParseError!ProjItem {
        const parsed = try self.parsedAggFromArgs(func_name, func, args);
        const alias = try self.maybeAlias(parsed.default_name);
        return ProjItem{ .name = alias, .kind = .{ .agg = parsed.agg } };
    }

    pub fn aggregateExprRefsEnabled(self: *const Parser) bool {
        return self.aggregate_expr_refs_enabled;
    }

    pub fn materializeAggregateExpr(
        self: *Parser,
        func_name: []const u8,
        func: ir.AggFunc,
        args: []const ir.Expr,
        saw_distinct: bool,
    ) ParseError![]const u8 {
        if (!self.aggregate_expr_refs_enabled) return ParseError.SqlInvalidProjection;
        const actual_func: ir.AggFunc = if (saw_distinct) switch (func) {
            .count => .count_distinct,
            .sum => .sum_distinct,
            .avg => .avg_distinct,
            else => return ParseError.SqlInvalidProjection,
        } else func;
        const parsed = try self.parsedAggFromArgs(func_name, actual_func, args);
        const name = std.fmt.allocPrint(self.arena, "__agg_expr_{d}", .{self.aggregate_expr_counter}) catch return ParseError.OutOfMemory;
        self.aggregate_expr_counter += 1;
        try self.aggregate_expr_refs.append(self.arena, .{ .name = name, .agg = parsed.agg });
        return name;
    }

    fn materializeWindowExpr(self: *Parser, call: ParsedWindowCall) ParseError![]const u8 {
        const name = std.fmt.allocPrint(self.arena, "__window_expr_{d}", .{self.window_expr_counter}) catch return ParseError.OutOfMemory;
        self.window_expr_counter += 1;
        try self.window_expr_refs.append(self.arena, .{ .name = name, .call = call });
        return name;
    }

    pub fn materializeWindowPartitionExpr(self: *Parser, expr: ir.Expr) ParseError![]const u8 {
        const name = std.fmt.allocPrint(self.arena, "__window_part_{d}", .{self.window_partition_expr_counter}) catch return ParseError.OutOfMemory;
        self.window_partition_expr_counter += 1;
        try self.window_partition_expr_refs.append(self.arena, .{ .name = name, .expr = expr });
        return name;
    }

    fn windowNullCheckExpr(self: *Parser, col_name: []const u8, negated: bool) ParseError!ir.Expr {
        const branches = try self.arena.alloc(ir.Expr.Branch, 1);
        branches[0] = .{
            .cond = if (negated)
                PredicateExpr{ .is_not_null = col_name }
            else
                PredicateExpr{ .is_null = col_name },
            .then = ir.Expr{ .lit = .{ .boolean = true } },
        };
        const else_branch = try self.arena.create(ir.Expr);
        else_branch.* = ir.Expr{ .lit = .{ .boolean = false } };
        return ir.Expr{ .case = .{ .branches = branches, .else_branch = else_branch } };
    }

    /// One argument to a scalar function call. Entry point for the
    /// expression sub-language used inside call args / projections.
    /// Precedence layers:
    ///   parseCallArg → parseAddSub  (lowest: + -)
    ///                → parseMulDiv  (next:   * / %)
    ///                → parseCallAtom (leaf: ident / call / literal)
    /// Aggregates and window functions are rejected inside this
    /// sub-language (atom layer enforces it).
    pub fn parseCallArg(self: *Parser) ParseError!ir.Expr {
        return try self.parseAddSub();
    }

    /// `+` / `-` binary operators, lowest precedence in the expr
    /// sub-language. Left-associative.
    pub fn parseAddSub(self: *Parser) ParseError!ir.Expr {
        var lhs = try self.parseMulDiv();
        while (self.cur.tag == .plus or self.cur.tag == .minus or self.concatOpHere()) {
            if (self.concatOpHere()) {
                try self.advance();
                const rhs = try self.parseMulDiv();
                lhs = try self.makeBinary("concat", lhs, rhs);
                continue;
            }
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
    fn parseExtractCall(self: *Parser) ParseError!ir.Expr {
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
        var atom = try self.parseCallAtomBase();
        // `expr::type` postfix cast (PG). Binds tighter than binary ops;
        // rejected on MySQL (where `::` is not a cast operator).
        while (self.cur.tag == .coloncolon and self.lex.dialect != .mysql) {
            try self.advance();
            atom = try self.parseCastTarget(atom);
        }
        return try self.consumeJsonArrows(atom);
    }

    /// MySQL JSON extraction: `doc -> '$.path'` desugars to
    /// JSON_EXTRACT(doc, path); `doc ->> '$.path'` additionally unquotes via
    /// JSON_VALUE. Binds like a postfix so `a->'$.x' = 'y'` groups as
    /// `(a->'$.x') = 'y'`, and left-associatively so `a->'$.x'->'$.y'`
    /// chains.
    pub fn consumeJsonArrows(self: *Parser, atom_in: ir.Expr) ParseError!ir.Expr {
        var atom = atom_in;
        while (self.cur.tag == .arrow or self.cur.tag == .arrow2) {
            const fn_name: []const u8 = if (self.cur.tag == .arrow2) "json_value" else "json_extract";
            try self.advance();
            const path = try self.parseCallAtomBase();
            const args = try self.arena.alloc(ir.Expr, 2);
            args[0] = atom;
            args[1] = path;
            atom = ir.Expr{ .call = .{ .fn_name = try self.arena.dupe(u8, fn_name), .args = args } };
        }
        return atom;
    }

    /// Map a CAST target type onto the existing conversion scalar-fn that
    /// implements it (the registry's implicit-cast ranking then coerces
    /// the source width, e.g. smallint→bigint before to_int). Returns null
    /// for targets without a conversion kernel yet — those CASTs error.
    fn castFnName(ty: types.Type) ?[]const u8 {
        return switch (ty) {
            .int => "to_int",
            .bigint => "to_bigint",
            .smallint => "to_smallint",
            .tinyint => "to_tinyint",
            .largeint => "to_largeint",
            .float, .double => "to_double",
            .boolean => "to_boolean",
            .date => "to_date",
            .datetime => "to_datetime",
            .varchar, .char, .string => "to_string",
            .json => "to_json",
            // No conversion kernel yet: decimal, uuid.
            .decimal64, .decimal128, .uuid => null,
        };
    }

    fn castExprToType(self: *Parser, inner: ir.Expr, ty: types.Type) ParseError!ir.Expr {
        if (inner == .null_lit) return ir.Expr{ .null_lit = ty };
        if (inner == .case) {
            const branches = try self.arena.alloc(ir.Expr.Branch, inner.case.branches.len);
            for (inner.case.branches, branches) |src, *dst| {
                dst.* = .{
                    .cond = src.cond,
                    .then = try self.castExprToType(src.then, ty),
                };
            }
            var else_branch: ?*const ir.Expr = null;
            if (inner.case.else_branch) |eb| {
                const owned = try self.arena.create(ir.Expr);
                owned.* = try self.castExprToType(eb.*, ty);
                else_branch = owned;
            }
            return ir.Expr{ .case = .{ .branches = branches, .else_branch = else_branch } };
        }
        // DECIMAL target: the (p,s) ride in the function name so the scalar
        // resolver (which sees types, not the would-be literal values) can
        // build the right scale-aware cast kernel. See scalar_fn.resolveToDecimal.
        if (ty.decimalSpec()) |sp| {
            const fn_name = try std.fmt.allocPrint(self.arena, "to_decimal:{d}:{d}", .{ sp.p, sp.s });
            const dargs = try self.arena.alloc(ir.Expr, 1);
            dargs[0] = inner;
            return ir.Expr{ .call = .{ .fn_name = fn_name, .args = dargs } };
        }
        const fn_name = castFnName(ty) orelse return ParseError.SqlInvalidProjection;
        const args = try self.arena.alloc(ir.Expr, 1);
        args[0] = inner;
        return ir.Expr{ .call = .{ .fn_name = try self.arena.dupe(u8, fn_name), .args = args } };
    }

    /// Parse the target type (cursor on the type name) and wrap `inner`
    /// in a `cast_as_<T>(inner)` scalar call. Used by both CAST(... AS T)
    /// and the `inner::T` postfix.
    fn parseCastTarget(self: *Parser, inner: ir.Expr) ParseError!ir.Expr {
        const ty = try parse_ddl.parseColumnType(self);
        return try self.castExprToType(inner, ty);
    }

    fn parseCallAtomBase(self: *Parser) ParseError!ir.Expr {
        if (self.cur.tag == .kw_case) return try self.parseCaseExpr();
        // CAST(expr AS type) — SQL-standard explicit cast (both dialects).
        if (self.cur.tag == .identifier and std.ascii.eqlIgnoreCase(self.cur.text, "cast")) {
            const saved = self.cur;
            try self.advance();
            if (self.cur.tag == .lparen) {
                try self.advance();
                const inner = try self.parseCallArg();
                if (self.cur.tag != .kw_as) return ParseError.SqlExpectedKeyword;
                try self.advance();
                const result = try self.parseCastTarget(inner);
                try self.expect(.rparen);
                return result;
            }
            return ir.Expr{ .col_ref = try self.arena.dupe(u8, saved.text) };
        }
        // EXISTS (SELECT ...) in expression position — projects a
        // boolean. Resolved by the pre-compile pass into `.lit`.
        if (self.cur.tag == .kw_exists) {
            try self.advance();
            try self.expect(.lparen);
            if (self.cur.tag != .kw_select and self.cur.tag != .kw_with) return ParseError.SqlExpectedSelect;
            const source = try self.parseStatement();
            if (self.pending_separable != null) return ParseError.SqlSeparableScope;
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
                return try self.parseExtractCall();
            }
            // Roll-back path: we already consumed `extract` and looked
            // at the next token. Re-emit it as a col_ref since the
            // grammar can't peek-then-backtrack arbitrarily.
            const col = try self.arena.dupe(u8, saved.text);
            return ir.Expr{ .col_ref = col };
        }
        switch (self.cur.tag) {
            .kw_replace, .kw_if, .kw_left, .kw_right => {
                const name = keywordScalarName(self.cur.tag).?;
                try self.advance();
                if (self.cur.tag != .lparen) return ParseError.SqlExpectedToken;
                return try self.parseScalarCallAfterName(name);
            },
            .identifier => {
                const name = self.cur.text;
                try self.advance();
                if (try self.typedTemporalLiteralAfterName(name)) |lit| return lit;
                if (self.cur.tag == .lparen) {
                    if (dateAddSubName(name)) |_| return try self.parseDateAddSubCallAfterName(name);
                    var saw_distinct = false;
                    const nested_args = try self.parseCallArgList(&saw_distinct);
                    const ignore_nulls = try parse_window.parseIgnoreNulls(self);
                    if (self.cur.tag == .kw_over) {
                        if (saw_distinct) return ParseError.SqlInvalidProjection;
                        try self.advance();
                        const spec_kind = try parse_window.parseWindowSpecOrRef(self);
                        const wfunc = ir.windowFuncForName(name) orelse
                            return ParseError.SqlInvalidProjection;
                        parse_window.validateWindowCall(wfunc, nested_args, ignore_nulls) catch
                            return ParseError.SqlInvalidProjection;
                        const hidden_name = try self.materializeWindowExpr(.{
                            .func = wfunc,
                            .args = nested_args,
                            .ignore_nulls = ignore_nulls,
                            .spec_kind = spec_kind,
                        });
                        return ir.Expr{ .col_ref = hidden_name };
                    }
                    if (ignore_nulls) return ParseError.SqlInvalidProjection;
                    if (self.aggregateFuncForName(name)) |func| {
                        const agg_name = try self.materializeAggregateExpr(name, func, nested_args, saw_distinct);
                        return ir.Expr{ .col_ref = agg_name };
                    }
                    if (ir.windowFuncForName(name)) |_| return ParseError.SqlInvalidProjection;
                    if (saw_distinct) return ParseError.SqlInvalidProjection;
                    return try self.makeScalarCallExpr(name, nested_args);
                }
                // Bare CURRENT_TIMESTAMP / CURRENT_DATE → nullary call.
                if (self.cur.tag != .dot) {
                    if (bareTemporalFn(name)) |fn_name|
                        return ir.Expr{ .call = .{ .fn_name = try self.arena.dupe(u8, fn_name), .args = &.{} } };
                }
                const col_dup = try self.dupQualifiedColRef(name);
                return ir.Expr{ .col_ref = col_dup };
            },
            .kw_null => {
                try self.advance();
                return ir.Expr{ .null_lit = .string };
            },
            .plus => {
                try self.advance();
                return try self.parseCallAtom();
            },
            .minus => {
                try self.advance();
                const rhs = try self.parseCallAtom();
                return try self.negateExpr(rhs);
            },
            .integer, .floating, .string, .kw_true, .kw_false => {
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
                    if (self.pending_separable != null) return ParseError.SqlSeparableScope;
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

    fn typedTemporalLiteralAfterName(self: *Parser, name: []const u8) ParseError!?ir.Expr {
        if (self.cur.tag != .string) return null;
        const s = self.cur.value.string;
        if (std.ascii.eqlIgnoreCase(name, "date")) {
            const days = parseDateString(s) catch return ParseError.SqlExpectedValue;
            try self.advance();
            return ir.Expr{ .lit = .{ .date = days } };
        }
        if (std.ascii.eqlIgnoreCase(name, "datetime") or
            std.ascii.eqlIgnoreCase(name, "timestamp"))
        {
            const micros = parseDateTimeString(s) catch return ParseError.SqlExpectedValue;
            try self.advance();
            return ir.Expr{ .lit = .{ .datetime = micros } };
        }
        return null;
    }

    fn makeBinary(self: *Parser, fn_name: []const u8, lhs: ir.Expr, rhs: ir.Expr) ParseError!ir.Expr {
        const args = try self.arena.alloc(ir.Expr, 2);
        args[0] = lhs;
        args[1] = rhs;
        const fname_dup = try self.arena.dupe(u8, fn_name);
        return ir.Expr{ .call = .{ .fn_name = fname_dup, .args = args } };
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
            .lit => |v| return try self.renderLit(v),
            .null_lit => return try self.arena.dupe(u8, "NULL"),
            .call => |c| {
                var buf: std.ArrayList(u8) = .empty;
                defer buf.deinit(self.arena);
                try buf.appendSlice(self.arena, c.fn_name);
                try buf.append(self.arena, '(');
                for (c.args, 0..) |arg, i| {
                    if (i > 0) try buf.appendSlice(self.arena, ", ");
                    // Recurse so distinct args yield distinct names — a
                    // SELECT can have `ClientIP - 1, ClientIP - 2`, which
                    // would otherwise collapse to one name and collide.
                    const arg_name = try self.exprDefaultName(arg);
                    try buf.appendSlice(self.arena, arg_name);
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

    fn renderLit(self: *Parser, v: @import("../types.zig").Value) ParseError![]const u8 {
        const aa = self.arena;
        return switch (v) {
            .int => |x| try std.fmt.allocPrint(aa, "{d}", .{x}),
            .bigint => |x| try std.fmt.allocPrint(aa, "{d}", .{x}),
            .smallint => |x| try std.fmt.allocPrint(aa, "{d}", .{x}),
            .tinyint => |x| try std.fmt.allocPrint(aa, "{d}", .{x}),
            .largeint => |x| try std.fmt.allocPrint(aa, "{d}", .{x}),
            .boolean => |b| try aa.dupe(u8, if (b) "true" else "false"),
            .float => |x| try std.fmt.allocPrint(aa, "{d}", .{x}),
            .double => |x| try std.fmt.allocPrint(aa, "{d}", .{x}),
            .text => |s| try std.fmt.allocPrint(aa, "'{s}'", .{s}),
            .date => |x| try std.fmt.allocPrint(aa, "{d}", .{x}),
            .datetime => |x| try std.fmt.allocPrint(aa, "{d}", .{x}),
            .decimal64 => |x| try std.fmt.allocPrint(aa, "{d}", .{x}),
            .decimal128 => |x| try std.fmt.allocPrint(aa, "{d}", .{x}),
            .uuid => try aa.dupe(u8, "uuid"),
        };
    }

    fn maybeAlias(self: *Parser, fallback: []const u8) ParseError![]const u8 {
        if (self.cur.tag == .kw_as) {
            try self.advance();
            const name = try self.expectIdent();
            return try self.arena.dupe(u8, name);
        }
        // Implicit alias: `expr alias_ident` (no AS keyword) — common
        // in MySQL/StarRocks. Only if next token is a bare identifier.
        if (self.cur.tag == .identifier and !self.identStartsSeparable()) {
            // But we have to be careful — keywords like FROM are NOT
            // identifiers, so the lookahead naturally stops at them.
            const name = self.cur.text;
            try self.advance();
            return try self.arena.dupe(u8, name);
        }
        return fallback;
    }

    /// The trailing separability clause starts with a bare identifier
    /// (SEPARABLE / GLOBAL / LOCAL are not keywords), so implicit AS-less
    /// alias positions must not swallow its lead-in. An alias literally
    /// named one of these still works with an explicit AS.
    fn identStartsSeparable(self: *const Parser) bool {
        if (self.cur.tag != .identifier) return false;
        const t = self.cur.text;
        return std.ascii.eqlIgnoreCase(t, "separable") or
            std.ascii.eqlIgnoreCase(t, "global") or
            std.ascii.eqlIgnoreCase(t, "local");
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
            const on_plan = try self.parseOnJoin(left_names.items, right.name, jtype);

            var left_op = root;
            var right_op = right.op;
            if (on_plan.left_derived.len > 0) {
                left_op = try self.allocOp(.{ .compute = .{ .derived = on_plan.left_derived, .upstream = left_op } });
            }
            if (on_plan.right_derived.len > 0) {
                right_op = try self.allocOp(.{ .compute = .{ .derived = on_plan.right_derived, .upstream = right_op } });
            }
            if (on_plan.left_filter) |pred| {
                left_op = try self.allocOp(.{ .filter = .{ .predicate = pred, .upstream = left_op } });
            }
            if (on_plan.right_filter) |pred| {
                right_op = try self.allocOp(.{ .filter = .{ .predicate = pred, .upstream = right_op } });
            }

            root = try self.allocOp(.{ .join = .{
                .algorithm = .auto,
                .join_type = jtype,
                .on = on_plan.on,
                .ranges = on_plan.ranges,
                .extra_predicate = null,
                .skew_ratio_threshold = 0.3,
                .skew_absolute_threshold = 20_000,
                .skew_sample_interval = 10,
                .left = left_op,
                .right = right_op,
            } });
            // A synthetic left-side ON expression (e.g. `ON upper(l.k) = r.k`)
            // stages a hidden `__join_on_left_*` column the join key reads; drop
            // it from the join output so it can't leak into `SELECT *`.
            if (on_plan.hidden_left.len > 0) {
                root = try self.allocOp(.{ .exclude = .{ .columns = on_plan.hidden_left, .upstream = root } });
            }
            try left_names.append(self.arena, right.name);
        }
        return root;
    }

    /// One source in a FROM clause: bare identifier (table or CTE
    /// reference) or a parenthesized subquery with an alias. Returns
    /// the resolved *ir.Op plus the name to use for ON-clause
    /// qualifier resolution.
    fn parseFromTarget(self: *Parser) ParseError!FromTarget {
        if (self.cur.tag == .kw_table) {
            return self.parseTableFnTarget();
        }
        if (self.cur.tag == .lparen) {
            // Anonymous subquery: ( select_stmt ) [AS] alias. Like every CTE
            // boundary, it materializes: the subquery runs as its own stage
            // and the outer block scans the buffered result.
            try self.advance();
            const op = try self.parseStatement();
            if (self.pending_separable != null) return ParseError.SqlSeparableScope;
            try self.expect(.rparen);
            const wrapped = try self.allocOp(.{ .materialize = .{ .upstream = op } });
            // Optional AS, mandatory alias.
            if (self.cur.tag == .kw_as) try self.advance();
            if (self.cur.tag != .identifier) return ParseError.SqlSubqueryNeedsAlias;
            const alias = try self.arena.dupe(u8, self.cur.text);
            try self.advance();
            return .{ .name = alias, .op = try self.applyAliasToFromOp(wrapped, alias, false) };
        }
        // Plain identifier — first check the CTE map (single-part name
        // only). If it's a CTE, use the stored op. Otherwise parse 1-,
        // 2-, or 3-part `[db.][schema.]table` into a TableRef-backed
        // Scan.
        if (self.cur.tag == .string) {
            const path = try self.arena.dupe(u8, self.cur.value.string);
            try self.advance();
            const op = try self.allocOp(.{ .file_scan = .{
                .format = .auto,
                .path = path,
                .alias = null,
            } });
            return try self.applyFromAlias(op, path);
        }
        const first = try self.expectIdent();
        const first_dup = try self.arena.dupe(u8, first);
        var op: *ir.Op = undefined;
        var resolved_name: []const u8 = first_dup;
        var alias_in_place = true;
        if (self.cur.tag == .lparen) {
            if (self.lookupSqlFn(first)) |def| {
                op = try self.expandSqlFunction(def);
                alias_in_place = false;
            } else {
                const format = fileFormatForFunction(first) orelse return ParseError.SqlUnsupportedFileFunction;
                op = try self.parseFileTableFunction(format);
            }
        } else if (self.cur.tag != .dot and self.ctes.get(first) != null) {
            const entry = self.ctes.get(first).?;
            // NOT MATERIALIZED = regenerate per use: each reference gets its
            // OWN materialize node over the shared body subtree, so the
            // engine builds (and frees) an independent stage per branch.
            // MATERIALIZED / default share one node — the post-parse pass
            // wraps the body in place so every reference reads one stage.
            op = if (entry.hint == .never)
                try self.allocOp(.{ .materialize = .{ .upstream = entry.op } })
            else
                entry.op;
            alias_in_place = false;
        } else if (self.cur.tag != .dot and self.plainViewDef(first) != null) {
            // A plain (non-materialized) view expands inline, like a
            // zero-arg inline function. Materialized views are real tables
            // and fall through to the scan path below.
            op = try self.expandView(self.plainViewDef(first).?);
            alias_in_place = false;
        } else {
            var parts_buf: [3][]const u8 = undefined;
            parts_buf[0] = first_dup;
            var parts_len: usize = 1;
            while (self.cur.tag == .dot) {
                if (parts_len == parts_buf.len) return ParseError.SqlExpectedIdent;
                try self.advance();
                // After a dot only a name can follow, so reserved words are
                // identifiers here — `information_schema.tables`.
                if (self.cur.tag != .identifier and !wordLikeToken(self.cur.text)) return ParseError.SqlExpectedIdent;
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
            op = try self.applyAliasToFromOp(op, resolved_name, alias_in_place);
            try self.advance();
        } else if (self.cur.tag == .identifier and !self.identStartsSeparable()) {
            // Implicit alias: bare identifier after the FROM target.
            // SQL clause keywords (JOIN/WHERE/ON/...) aren't .identifier
            // tokens so they don't trigger this.
            resolved_name = try self.arena.dupe(u8, self.cur.text);
            op = try self.applyAliasToFromOp(op, resolved_name, alias_in_place);
            try self.advance();
        }
        return .{ .name = resolved_name, .op = op };
    }

    /// `TABLE( fname( (subquery) ) [PARTITION BY col, ...]
    ///        [ORDER BY col [ASC|DESC], ...] ) [AS] [alias]`
    /// — a table-valued UDF call. The input subquery must be parenthesized.
    /// No PARTITION BY = GLOBAL execution (one partition holding all rows);
    /// the registered function's declared execution mode is enforced at
    /// compile.
    fn parseTableFnTarget(self: *Parser) ParseError!FromTarget {
        try self.advance(); // TABLE
        try self.expect(.lparen);
        const fname = try self.dupedIdent();
        try self.expect(.lparen);
        var inputs: std.ArrayList(*ir.Op) = .empty;
        defer inputs.deinit(self.arena);
        var fn_args: std.ArrayList(?Value) = .empty;
        defer fn_args.deinit(self.arena);
        while (true) {
            if (self.cur.tag == .lparen) {
                try self.advance();
                const inner = try self.parseStatement();
                if (self.pending_separable != null) return ParseError.SqlSeparableScope;
                try self.expect(.rparen);
                try inputs.append(self.arena, inner);
            } else if (self.cur.tag == .kw_null) {
                try self.advance();
                try fn_args.append(self.arena, null);
            } else {
                try fn_args.append(self.arena, try self.parseValue());
            }
            if (self.cur.tag != .comma) break;
            try self.advance();
        }
        try self.expect(.rparen);
        if (inputs.items.len == 0) return ParseError.SqlExpectedToken;

        var partition_by: std.ArrayList([]const u8) = .empty;
        defer partition_by.deinit(self.arena);
        if (self.cur.tag == .kw_partition) {
            try self.advance();
            if (self.cur.tag != .kw_by) return ParseError.SqlExpectedKeyword;
            try self.advance();
            while (true) {
                try partition_by.append(self.arena, try self.dupedIdent());
                if (self.cur.tag != .comma) break;
                try self.advance();
            }
        }
        var order_by: std.ArrayList(ir.SortSpec) = .empty;
        defer order_by.deinit(self.arena);
        if (self.cur.tag == .kw_order) {
            try self.advance();
            if (self.cur.tag != .kw_by) return ParseError.SqlExpectedKeyword;
            try self.advance();
            while (true) {
                const col = try self.dupedIdent();
                var desc = false;
                if (self.cur.tag == .kw_asc) {
                    try self.advance();
                } else if (self.cur.tag == .kw_desc) {
                    desc = true;
                    try self.advance();
                }
                try order_by.append(self.arena, .{ .col = col, .desc = desc });
                if (self.cur.tag != .comma) break;
                try self.advance();
            }
        }
        try self.expect(.rparen); // close TABLE(

        var resolved_name: []const u8 = fname;
        if (self.cur.tag == .kw_as) {
            try self.advance();
            resolved_name = try self.dupedIdent();
        } else if (self.cur.tag == .identifier and !self.identStartsSeparable()) {
            resolved_name = try self.dupedIdent();
        }
        const op = try self.allocOp(.{ .table_fn = .{
            .name = fname,
            .inputs = try self.arena.dupe(*ir.Op, inputs.items),
            .args = try self.arena.dupe(?Value, fn_args.items),
            .partition_by = try self.arena.dupe([]const u8, partition_by.items),
            .order_by = try self.arena.dupe(ir.SortSpec, order_by.items),
            .alias = if (resolved_name.ptr != fname.ptr) resolved_name else null,
        } });
        return .{ .name = resolved_name, .op = op };
    }

    fn lookupSqlFn(self: *Parser, name: []const u8) ?*const udf_mod.SqlTableFn {
        const ctx = self.sql_fns orelse return null;
        return ctx.registry.get(ctx.db, name);
    }

    /// A non-materialized view named `name`, or null. Materialized views own
    /// a backing table and resolve through the normal scan path instead.
    fn plainViewDef(self: *Parser, name: []const u8) ?*const udf_mod.ViewDef {
        const ctx = self.sql_fns orelse return null;
        const vr = ctx.views orelse return null;
        const def = vr.get(ctx.db, name) orelse return null;
        return if (def.materialized) null else def;
    }

    /// Expand a plain-view reference: re-parse the stored defining query and
    /// wrap it in a single-reference Materialize (which compiles inline, so
    /// the view body behaves as if written in place). No parameters.
    fn expandView(self: *Parser, def: *const udf_mod.ViewDef) ParseError!*ir.Op {
        if (self.fn_expand_depth >= 16) return ParseError.SqlFunctionArgMismatch;
        const body = try self.arena.dupe(u8, def.body);
        var sub_lex = Lexer.init(self.arena, body);
        sub_lex.dialect = self.lex.dialect;
        var sub = Parser{
            .arena = self.arena,
            .lex = &sub_lex,
            .cur = .{ .tag = .semicolon, .text = "" },
            .udf_registry = self.udf_registry,
            .sql_fns = self.sql_fns,
            .fn_expand_depth = self.fn_expand_depth + 1,
        };
        try sub.advance();
        const op = try sub.parseStatement();
        if (sub.pending_separable != null) return ParseError.SqlSeparableScope;
        try sub.applyAutoMaterialize();
        if (sub.cur.tag != .eof and sub.cur.tag != .semicolon) return ParseError.SqlExpectedToken;
        return try self.allocOp(.{ .materialize = .{ .upstream = op } });
    }

    /// Expand a SQL inline table function call site: parse the literal
    /// argument list, then re-parse the function body with each parameter
    /// identifier bound to its argument token. The result is wrapped in
    /// its own single-reference Materialize node — the shape the compiler
    /// expects at every block boundary, and single-ref materialize bodies
    /// COMPILE INLINE (no stage, full pushdown/fusion), so each call site
    /// behaves as if its body had been written in place.
    fn expandSqlFunction(self: *Parser, def: *const udf_mod.SqlTableFn) ParseError!*ir.Op {
        if (self.fn_expand_depth >= 16) return ParseError.SqlFunctionArgMismatch;
        try self.expect(.lparen);
        // Defensive copies into the arena: the registry entry can be
        // replaced by concurrent DDL after this lookup (same documented
        // race as table DDL mid-query).
        const body = try self.arena.dupe(u8, def.body);
        const n_params = def.param_names.len;
        const param_names = try self.arena.alloc([]const u8, n_params);
        for (def.param_names, param_names) |src_name, *dst| dst.* = try self.arena.dupe(u8, src_name);

        var bindings: std.StringHashMapUnmanaged(Token) = .empty;
        var argi: usize = 0;
        if (self.cur.tag != .rparen) {
            while (true) {
                if (argi >= n_params) return ParseError.SqlFunctionArgMismatch;
                try bindings.put(self.arena, param_names[argi], try self.captureLiteralToken());
                argi += 1;
                if (self.cur.tag != .comma) break;
                try self.advance();
            }
        }
        try self.expect(.rparen);
        if (argi != n_params) return ParseError.SqlFunctionArgMismatch;

        var sub_lex = Lexer.init(self.arena, body);
        sub_lex.dialect = self.lex.dialect;
        var sub = Parser{
            .arena = self.arena,
            .lex = &sub_lex,
            .cur = .{ .tag = .semicolon, .text = "" },
            .udf_registry = self.udf_registry,
            .sql_fns = self.sql_fns,
            .param_bindings = &bindings,
            .fn_expand_depth = self.fn_expand_depth + 1,
        };
        try sub.advance(); // load the first body token (with param interception)
        const op = try sub.parseStatement();
        if (sub.pending_separable != null) return ParseError.SqlSeparableScope;
        try sub.applyAutoMaterialize();
        if (sub.cur.tag != .eof and sub.cur.tag != .semicolon) return ParseError.SqlExpectedToken;
        return try self.allocOp(.{ .materialize = .{ .upstream = op } });
    }

    /// The current token as a literal argument for a function expansion:
    /// number / string / TRUE / FALSE / NULL, with a leading minus folded
    /// into the numeric token. Anything else (column refs, expressions,
    /// subqueries) is rejected — v1 arguments are literals.
    fn captureLiteralToken(self: *Parser) ParseError!Token {
        switch (self.cur.tag) {
            .integer, .floating, .string, .kw_null, .kw_true, .kw_false => {
                const t = self.cur;
                try self.advance();
                return t;
            },
            .minus => {
                try self.advance();
                var t = self.cur;
                switch (t.tag) {
                    .integer => t.value = .{ .integer = -t.value.integer },
                    .floating => t.value = .{ .floating = -t.value.floating },
                    else => return ParseError.SqlExpectedValue,
                }
                try self.advance();
                return t;
            },
            else => return ParseError.SqlExpectedValue,
        }
    }

    fn parseFileTableFunction(self: *Parser, format: ir.FileFormat) ParseError!*ir.Op {
        try self.expect(.lparen);
        if (self.cur.tag != .string) return ParseError.SqlExpectedValue;
        const path = try self.arena.dupe(u8, self.cur.value.string);
        try self.advance();

        var options: ir.FileScanOptions = .{};
        while (self.cur.tag == .comma) {
            try self.advance();
            const name = try self.parseFileOptionName();
            if (self.cur.tag != .eq) return ParseError.SqlExpectedToken;
            try self.advance();
            try self.parseFileOptionValue(format, name, &options);
        }
        try self.expect(.rparen);

        return try self.allocOp(.{ .file_scan = .{
            .format = format,
            .path = path,
            .alias = null,
            .options = options,
        } });
    }

    fn applyFromAlias(self: *Parser, op: *ir.Op, fallback: []const u8) ParseError!FromTarget {
        var aliased_op = op;
        var resolved_name = fallback;
        if (self.cur.tag == .kw_as) {
            try self.advance();
            if (self.cur.tag != .identifier) return ParseError.SqlExpectedIdent;
            resolved_name = try self.arena.dupe(u8, self.cur.text);
            aliased_op = try self.applyAliasToFromOp(aliased_op, resolved_name, true);
            try self.advance();
        } else if (self.cur.tag == .identifier and !self.identStartsSeparable()) {
            resolved_name = try self.arena.dupe(u8, self.cur.text);
            aliased_op = try self.applyAliasToFromOp(aliased_op, resolved_name, true);
            try self.advance();
        }
        return .{ .name = resolved_name, .op = aliased_op };
    }

    fn applyAliasToFromOp(self: *Parser, op: *ir.Op, alias: []const u8, allow_in_place: bool) ParseError!*ir.Op {
        if (allow_in_place) {
            switch (op.*) {
                .scan => {
                    op.scan.alias = alias;
                    return op;
                },
                .file_scan => {
                    op.file_scan.alias = alias;
                    return op;
                },
                else => {},
            }
        }
        return try self.allocOp(.{ .alias = .{ .alias = alias, .upstream = op } });
    }

    fn parseFileOptionName(self: *Parser) ParseError![]const u8 {
        return switch (self.cur.tag) {
            .identifier => blk: {
                const name = self.cur.text;
                try self.advance();
                break :blk name;
            },
            .kw_null => blk: {
                try self.advance();
                break :blk "null";
            },
            else => ParseError.SqlExpectedIdent,
        };
    }

    fn parseFileOptionValue(self: *Parser, format: ir.FileFormat, name: []const u8, options: *ir.FileScanOptions) ParseError!void {
        if (format == .csv) {
            if (std.ascii.eqlIgnoreCase(name, "header")) {
                options.csv.header = try self.parseFileBool();
                return;
            }
            if (std.ascii.eqlIgnoreCase(name, "delim") or std.ascii.eqlIgnoreCase(name, "sep")) {
                options.csv.delim = try self.parseFileString();
                return;
            }
            if (std.ascii.eqlIgnoreCase(name, "quote")) {
                options.csv.quote = try self.parseFileString();
                return;
            }
            if (std.ascii.eqlIgnoreCase(name, "escape")) {
                options.csv.escape = try self.parseFileString();
                return;
            }
            if (std.ascii.eqlIgnoreCase(name, "nullstr") or std.ascii.eqlIgnoreCase(name, "null")) {
                options.csv.nullstr = try self.parseFileString();
                return;
            }
            if (std.ascii.eqlIgnoreCase(name, "skip")) {
                options.csv.skip = try self.parseFileU64();
                return;
            }
            if (std.ascii.eqlIgnoreCase(name, "sample_size")) {
                options.csv.sample_size = try self.parseFileU64();
                return;
            }
            if (std.ascii.eqlIgnoreCase(name, "auto_detect")) {
                options.csv.auto_detect = try self.parseFileBool();
                return;
            }
            if (std.ascii.eqlIgnoreCase(name, "all_varchar")) {
                options.csv.all_varchar = try self.parseFileBool();
                return;
            }
        } else if (format == .json) {
            if (std.ascii.eqlIgnoreCase(name, "sample_size")) {
                options.json.sample_size = try self.parseFileU64();
                return;
            }
        }
        return ParseError.SqlUnsupportedFileOption;
    }

    fn parseFileBool(self: *Parser) ParseError!bool {
        return switch (self.cur.tag) {
            .kw_true => blk: {
                try self.advance();
                break :blk true;
            },
            .kw_false => blk: {
                try self.advance();
                break :blk false;
            },
            .identifier => blk: {
                const text = self.cur.text;
                try self.advance();
                if (std.ascii.eqlIgnoreCase(text, "true")) break :blk true;
                if (std.ascii.eqlIgnoreCase(text, "false")) break :blk false;
                return ParseError.SqlExpectedValue;
            },
            else => ParseError.SqlExpectedValue,
        };
    }

    fn parseFileString(self: *Parser) ParseError![]const u8 {
        if (self.cur.tag != .string) return ParseError.SqlExpectedValue;
        const value = try self.arena.dupe(u8, self.cur.value.string);
        try self.advance();
        return value;
    }

    fn parseFileU64(self: *Parser) ParseError!u64 {
        if (self.cur.tag != .integer) return ParseError.SqlExpectedValue;
        const value = self.cur.value.integer;
        if (value < 0) return ParseError.SqlExpectedValue;
        try self.advance();
        return @intCast(value);
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
            // A trailing SEPARABLE BY inside the parens belongs to THIS CTE.
            const separable = self.pending_separable;
            self.pending_separable = null;

            const gop = try self.ctes.getOrPut(self.arena, name);
            if (gop.found_existing) return ParseError.SqlCteRedefined;
            gop.value_ptr.* = .{ .op = op, .hint = hint, .separable = separable };

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

    fn parseOnJoin(
        self: *Parser,
        left_table_names: []const []const u8,
        right_table_name: []const u8,
        jtype: ir.JoinType,
    ) ParseError!JoinOnPlan {
        var pairs: std.ArrayList(ir.JoinKeyPair) = .empty;
        var ranges: std.ArrayList(ir.JoinRangePredicate) = .empty;
        var left_derived: std.ArrayList(ir.Derived) = .empty;
        var right_derived: std.ArrayList(ir.Derived) = .empty;
        var left_filters: std.ArrayList(PredicateExpr) = .empty;
        var right_filters: std.ArrayList(PredicateExpr) = .empty;
        var hidden_left: std.ArrayList([]const u8) = .empty;
        var synth_counter: usize = 0;

        while (true) {
            const lhs = try self.parseCallArg();
            if (self.cur.tag == .kw_is) {
                try self.advance();
                var negated = false;
                if (self.cur.tag == .kw_not) {
                    negated = true;
                    try self.advance();
                }
                if (self.cur.tag != .kw_null) return ParseError.SqlExpectedNull;
                try self.advance();
                try self.addJoinNullCondition(
                    lhs,
                    negated,
                    left_table_names,
                    right_table_name,
                    &left_filters,
                    &right_filters,
                );
            } else if (self.cur.tag == .kw_between) {
                try self.advance();
                const lower = try self.parseCallArg();
                if (self.cur.tag != .kw_and) return ParseError.SqlExpectedKeyword;
                try self.advance();
                const upper = try self.parseCallArg();

                try self.addJoinBetweenCondition(
                    lhs,
                    lower,
                    upper,
                    left_table_names,
                    right_table_name,
                    &pairs,
                    &ranges,
                    &left_derived,
                    &right_derived,
                    &left_filters,
                    &right_filters,
                    &hidden_left,
                    &synth_counter,
                );
            } else {
                const op = try self.parseJoinComparisonOp();
                try self.advance();
                const rhs = try self.parseCallArg();

                try self.addJoinCondition(
                    lhs,
                    op,
                    rhs,
                    left_table_names,
                    right_table_name,
                    &pairs,
                    &ranges,
                    &left_derived,
                    &right_derived,
                    &left_filters,
                    &right_filters,
                    &hidden_left,
                    &synth_counter,
                );
            }

            if (self.cur.tag != .kw_and) break;
            try self.advance();
        }
        // OR / general boolean ON predicates aren't join keys; reject rather
        // than silently mis-join.
        if (self.cur.tag == .kw_or or (self.cur.tag == .pipe_pipe and self.lex.dialect == .mysql)) {
            return ParseError.SqlOnNonEquiUnsupported;
        }
        // A column range join (`l.a < r.b`) only has well-defined semantics for
        // an inner join; outer joins would need true ON-extra-predicate support.
        if (jtype != .inner and ranges.items.len > 0) return ParseError.SqlOnNonEquiUnsupported;
        if (pairs.items.len == 0 and ranges.items.len == 0) return ParseError.SqlOnNonEquiUnsupported;
        self.dropRedundantOuterJoinNotNullFilters(jtype, &left_filters, &right_filters, pairs.items, ranges.items);
        return .{
            .on = try pairs.toOwnedSlice(self.arena),
            .ranges = try ranges.toOwnedSlice(self.arena),
            .left_derived = try left_derived.toOwnedSlice(self.arena),
            .right_derived = try right_derived.toOwnedSlice(self.arena),
            .left_filter = try self.joinFilterFromParts(&left_filters),
            .right_filter = try self.joinFilterFromParts(&right_filters),
            .hidden_left = try hidden_left.toOwnedSlice(self.arena),
        };
    }

    fn dropRedundantOuterJoinNotNullFilters(
        self: *Parser,
        jtype: ir.JoinType,
        left_filters: *std.ArrayList(PredicateExpr),
        right_filters: *std.ArrayList(PredicateExpr),
        pairs: []const ir.JoinKeyPair,
        ranges: []const ir.JoinRangePredicate,
    ) void {
        _ = self;
        switch (jtype) {
            .inner => {},
            .left => dropRedundantJoinKeyNotNullFilters(left_filters, true, pairs, ranges),
            .right => dropRedundantJoinKeyNotNullFilters(right_filters, false, pairs, ranges),
            .full => {
                dropRedundantJoinKeyNotNullFilters(left_filters, true, pairs, ranges);
                dropRedundantJoinKeyNotNullFilters(right_filters, false, pairs, ranges);
            },
        }
    }

    fn dropRedundantJoinKeyNotNullFilters(
        filters: *std.ArrayList(PredicateExpr),
        comptime left_side: bool,
        pairs: []const ir.JoinKeyPair,
        ranges: []const ir.JoinRangePredicate,
    ) void {
        var i: usize = 0;
        while (i < filters.items.len) {
            const redundant = switch (filters.items[i]) {
                .is_not_null => |col| joinKeyContainsColumn(left_side, pairs, ranges, col),
                else => false,
            };
            if (redundant) {
                _ = filters.orderedRemove(i);
            } else {
                i += 1;
            }
        }
    }

    fn joinKeyContainsColumn(
        comptime left_side: bool,
        pairs: []const ir.JoinKeyPair,
        ranges: []const ir.JoinRangePredicate,
        col: []const u8,
    ) bool {
        for (pairs) |pair| {
            const key = if (left_side) pair.left else pair.right;
            if (types.columnNameEql(key, col)) return true;
        }
        for (ranges) |range| {
            const key = if (left_side) range.left else range.right;
            if (types.columnNameEql(key, col)) return true;
        }
        return false;
    }

    fn addJoinNullCondition(
        self: *Parser,
        expr: ir.Expr,
        negated: bool,
        left_table_names: []const []const u8,
        right_table_name: []const u8,
        left_filters: *std.ArrayList(PredicateExpr),
        right_filters: *std.ArrayList(PredicateExpr),
    ) ParseError!void {
        const side = try self.joinExprSide(expr, left_table_names, right_table_name);
        if (side != .left and side != .right) return ParseError.SqlOnNonEquiUnsupported;
        if (expr != .col_ref) return ParseError.SqlOnNonEquiUnsupported;
        const col = try self.joinColName(expr);
        const pred = if (negated)
            PredicateExpr{ .is_not_null = col }
        else
            PredicateExpr{ .is_null = col };
        if (side == .left) {
            try left_filters.append(self.arena, pred);
        } else {
            try right_filters.append(self.arena, pred);
        }
    }

    fn parseJoinComparisonOp(self: *Parser) ParseError!PredicateOp {
        return switch (self.cur.tag) {
            .eq => .eq,
            .lt => .lt,
            .lte => .lte,
            .gt => .gt,
            .gte => .gte,
            .neq => ParseError.SqlOnNonEquiUnsupported,
            else => ParseError.SqlExpectedToken,
        };
    }

    fn addJoinBetweenCondition(
        self: *Parser,
        lhs: ir.Expr,
        lower: ir.Expr,
        upper: ir.Expr,
        left_table_names: []const []const u8,
        right_table_name: []const u8,
        pairs: *std.ArrayList(ir.JoinKeyPair),
        ranges: *std.ArrayList(ir.JoinRangePredicate),
        left_derived: *std.ArrayList(ir.Derived),
        right_derived: *std.ArrayList(ir.Derived),
        left_filters: *std.ArrayList(PredicateExpr),
        right_filters: *std.ArrayList(PredicateExpr),
        hidden_left: *std.ArrayList([]const u8),
        synth_counter: *usize,
    ) ParseError!void {
        try self.addJoinCondition(
            lhs,
            .gte,
            lower,
            left_table_names,
            right_table_name,
            pairs,
            ranges,
            left_derived,
            right_derived,
            left_filters,
            right_filters,
            hidden_left,
            synth_counter,
        );
        try self.addJoinCondition(
            lhs,
            .lte,
            upper,
            left_table_names,
            right_table_name,
            pairs,
            ranges,
            left_derived,
            right_derived,
            left_filters,
            right_filters,
            hidden_left,
            synth_counter,
        );
    }

    fn addJoinCondition(
        self: *Parser,
        lhs: ir.Expr,
        op: PredicateOp,
        rhs: ir.Expr,
        left_table_names: []const []const u8,
        right_table_name: []const u8,
        pairs: *std.ArrayList(ir.JoinKeyPair),
        ranges: *std.ArrayList(ir.JoinRangePredicate),
        left_derived: *std.ArrayList(ir.Derived),
        right_derived: *std.ArrayList(ir.Derived),
        left_filters: *std.ArrayList(PredicateExpr),
        right_filters: *std.ArrayList(PredicateExpr),
        hidden_left: *std.ArrayList([]const u8),
        synth_counter: *usize,
    ) ParseError!void {
        const lhs_side = try self.joinExprSide(lhs, left_table_names, right_table_name);
        const rhs_side = try self.joinExprSide(rhs, left_table_names, right_table_name);
        if (lhs_side == .mixed or rhs_side == .mixed) return ParseError.SqlOnRefsUnknownTable;
        if (try self.addJoinSideFilter(lhs, lhs_side, op, rhs, rhs_side, left_filters, right_filters)) return;
        if (try self.addJoinSideFilter(rhs, rhs_side, reverseRangeOp(op), lhs, lhs_side, left_filters, right_filters)) return;
        if (lhs_side == .none or rhs_side == .none) return ParseError.SqlOnNonEquiUnsupported;

        if (op == .eq) {
            if (lhs_side == .left and rhs_side == .right) {
                const left_name = try self.materializeJoinOperand(lhs, .left, left_derived, right_derived, hidden_left, synth_counter);
                const right_name = try self.materializeJoinOperand(rhs, .right, left_derived, right_derived, hidden_left, synth_counter);
                try pairs.append(self.arena, .{ .left = left_name, .right = right_name });
                return;
            }
            if (lhs_side == .right and rhs_side == .left) {
                const left_name = try self.materializeJoinOperand(rhs, .left, left_derived, right_derived, hidden_left, synth_counter);
                const right_name = try self.materializeJoinOperand(lhs, .right, left_derived, right_derived, hidden_left, synth_counter);
                try pairs.append(self.arena, .{ .left = left_name, .right = right_name });
                return;
            }
            return ParseError.SqlOnRefsUnknownTable;
        }

        if (!isRangeJoinOp(op)) return ParseError.SqlOnNonEquiUnsupported;
        if (lhs_side == .left and rhs_side == .right) {
            try ranges.append(self.arena, .{
                .left = try self.materializeJoinOperand(lhs, .left, left_derived, right_derived, hidden_left, synth_counter),
                .op = op,
                .right = try self.materializeJoinOperand(rhs, .right, left_derived, right_derived, hidden_left, synth_counter),
            });
            return;
        }
        if (lhs_side == .right and rhs_side == .left) {
            try ranges.append(self.arena, .{
                .left = try self.materializeJoinOperand(rhs, .left, left_derived, right_derived, hidden_left, synth_counter),
                .op = reverseRangeOp(op),
                .right = try self.materializeJoinOperand(lhs, .right, left_derived, right_derived, hidden_left, synth_counter),
            });
            return;
        }
        return ParseError.SqlOnRefsUnknownTable;
    }

    fn addJoinSideFilter(
        self: *Parser,
        side_expr: ir.Expr,
        side: JoinExprSide,
        op: PredicateOp,
        literal_expr: ir.Expr,
        literal_side: JoinExprSide,
        left_filters: *std.ArrayList(PredicateExpr),
        right_filters: *std.ArrayList(PredicateExpr),
    ) ParseError!bool {
        if (literal_side != .none) return false;
        if (side != .left and side != .right) return false;
        if (side_expr != .col_ref) return ParseError.SqlOnNonEquiUnsupported;
        const col = try self.joinColName(side_expr);
        const pred = switch (literal_expr) {
            .lit => |v| PredicateExpr{ .leaf = .{ .col = col, .op = op, .val = v } },
            .null_lit => PredicateExpr.unknown,
            // `col <op> @var`: the pre-compile pass rewrites leaf_var to a
            // literal leaf once the session value is known.
            .var_ref => |vn| PredicateExpr{ .leaf_var = .{ .col = col, .op = op, .var_name = try self.arena.dupe(u8, vn) } },
            .call => try self.makeJoinScalarFilter(col, op, literal_expr),
            else => return ParseError.SqlOnNonEquiUnsupported,
        };
        if (side == .left) {
            try left_filters.append(self.arena, pred);
        } else {
            try right_filters.append(self.arena, pred);
        }
        return true;
    }

    fn makeJoinScalarFilter(self: *Parser, col: []const u8, op: PredicateOp, expr: ir.Expr) ParseError!PredicateExpr {
        const value_name = try self.arena.dupe(u8, "__join_filter_value");
        const single = try self.allocOp(.{ .single_row = {} });

        const derived = try self.arena.alloc(ir.Derived, 1);
        derived[0] = .{ .name = value_name, .expr = expr };
        const compute = try self.allocOp(.{ .compute = .{ .derived = derived, .upstream = single } });

        const cols = try self.arena.alloc([]const u8, 1);
        cols[0] = value_name;
        const select = try self.allocOp(.{ .select = .{ .columns = cols, .upstream = compute } });

        return .{ .scalar_subquery = .{
            .col = col,
            .op = op,
            .source = @ptrCast(select),
        } };
    }

    fn joinFilterFromParts(self: *Parser, parts: *std.ArrayList(PredicateExpr)) ParseError!?PredicateExpr {
        return switch (parts.items.len) {
            0 => null,
            1 => parts.items[0],
            else => blk: {
                const children = try self.arena.dupe(PredicateExpr, parts.items);
                break :blk PredicateExpr{ .@"and" = children };
            },
        };
    }

    fn materializeJoinOperand(
        self: *Parser,
        expr: ir.Expr,
        side: JoinExprSide,
        left_derived: *std.ArrayList(ir.Derived),
        right_derived: *std.ArrayList(ir.Derived),
        hidden_left: *std.ArrayList([]const u8),
        synth_counter: *usize,
    ) ParseError![]const u8 {
        if (expr == .col_ref and side != .right) return try self.joinColName(expr);
        const prefix: []const u8 = if (side == .left) "__join_on_left" else "__join_on_right";
        const name = try std.fmt.allocPrint(self.arena, "{s}_{d}", .{ prefix, synth_counter.* });
        synth_counter.* += 1;
        const derived = ir.Derived{ .name = name, .expr = expr };
        if (side == .left) {
            try left_derived.append(self.arena, derived);
            try hidden_left.append(self.arena, name);
        } else if (side == .right) {
            try right_derived.append(self.arena, derived);
        } else {
            return ParseError.SqlOnRefsUnknownTable;
        }
        return name;
    }

    fn joinColName(self: *Parser, expr: ir.Expr) ParseError![]const u8 {
        if (expr != .col_ref) return ParseError.SqlOnNonEquiUnsupported;
        return try self.arena.dupe(u8, expr.col_ref);
    }

    fn joinExprSide(
        self: *Parser,
        expr: ir.Expr,
        left_table_names: []const []const u8,
        right_table_name: []const u8,
    ) ParseError!JoinExprSide {
        return switch (expr) {
            .col_ref => |name| (try self.splitJoinCol(name, left_table_names, right_table_name)).side,
            // A session `@var` is a per-statement constant (resolved once at
            // compile time), so it sides like a literal — never a join key.
            .lit, .null_lit, .var_ref => .none,
            .call => |c| blk: {
                if (!try self.joinScalarAllowed(c.fn_name)) return ParseError.SqlOnNonEquiUnsupported;
                var side: JoinExprSide = .none;
                for (c.args) |arg| {
                    side = combineJoinSides(side, try self.joinExprSide(arg, left_table_names, right_table_name));
                    if (side == .mixed) break :blk .mixed;
                }
                break :blk side;
            },
            .case, .scalar_subquery, .exists_subquery => ParseError.SqlOnNonEquiUnsupported,
        };
    }

    fn splitJoinCol(
        self: *Parser,
        name: []const u8,
        left_table_names: []const []const u8,
        right_table_name: []const u8,
    ) ParseError!QualifiedJoinCol {
        _ = self;
        const dot = std.mem.indexOfScalar(u8, name, '.') orelse return ParseError.SqlOnRefsUnknownTable;
        const qualifier = name[0..dot];
        const col = name[dot + 1 ..];
        if (nameIn(qualifier, left_table_names)) return .{ .side = .left, .name = col };
        if (types.columnNameEql(qualifier, right_table_name)) return .{ .side = .right, .name = col };
        if (left_table_names.len == 0 and right_table_name.len == 0) return .{ .side = .none, .name = col };
        return ParseError.SqlOnRefsUnknownTable;
    }

    fn joinScalarAllowed(self: *Parser, name: []const u8) ParseError!bool {
        if (isNondeterministicFn(name)) return false;
        if (self.udf_registry) |registry| {
            for (registry.scalarEntries()) |entry| {
                if (!std.ascii.eqlIgnoreCase(entry.name, name)) continue;
                if (entry.volatility == .@"volatile") return false;
            }
        }
        return true;
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

    pub fn parseOrderBy(self: *Parser, proj: []const ProjItem) ParseError![]const @import("../exec/sort.zig").SortSpec {
        const SortSpec = @import("../exec/sort.zig").SortSpec;
        var items: std.ArrayList(SortSpec) = .empty;
        defer items.deinit(self.arena);
        while (true) {
            var col: []const u8 = undefined;
            if (self.cur.tag == .integer) {
                // `ORDER BY <n>` — 1-based ordinal into the SELECT list
                // (PG/MySQL). A plain column sorts on its underlying name
                // (the sort runs before the final projection); a computed /
                // aggregate / window item sorts on its output alias.
                const k = self.cur.value.integer;
                try self.advance();
                if (k < 1 or k > @as(i64, @intCast(proj.len))) return ParseError.SqlInvalidProjection;
                const p = proj[@intCast(k - 1)];
                col = switch (p.kind) {
                    .col => |c| try self.arena.dupe(u8, c),
                    .star => return ParseError.SqlInvalidProjection,
                    else => try self.arena.dupe(u8, p.name),
                };
            } else {
                if (self.cur.tag != .identifier) return ParseError.SqlExpectedIdent;
                const first = self.cur.text;
                try self.advance();
                col = if (self.cur.tag == .lparen) blk: {
                    var distinct = false;
                    const args = try self.parseCallArgList(&distinct);
                    // `ORDER BY agg(arg)` (e.g. ORDER BY COUNT(*) DESC) binds
                    // to the aggregate's canonical output column name (the
                    // sort runs after the aggregate). Aliased aggregates are
                    // referenced by alias via the plain-ident path.
                    if (self.aggregateFuncForName(first) != null) break :blk try self.aggSortName(first, args);
                    // `ORDER BY scalar_fn(args)` (e.g. ORDER BY DATE_TRUNC(...))
                    // binds to the matching SELECT expression's output column.
                    const fname = try self.arena.dupe(u8, first);
                    const call_expr = ir.Expr{ .call = .{ .fn_name = fname, .args = args } };
                    const idx = findGroupMatch(proj, call_expr) orelse return ParseError.SqlInvalidProjection;
                    break :blk proj[idx].name;
                } else try self.dupQualifiedColRef(first);
            }
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

    /// Canonical output-column name for an aggregate referenced in ORDER
    /// BY, matching `aggCallFromArgs`'s default-name format
    /// (`func(arg)` / `func(*)`). Only single col-ref / `*` args are
    /// bindable — anything else can't be matched to a projected column.
    pub fn aggSortName(self: *Parser, func_name: []const u8, args: []const ir.Expr) ParseError![]const u8 {
        if (args.len != 1 and self.aggregateFuncForName(func_name) != .udf) return ParseError.SqlInvalidProjection;
        var buf: std.ArrayList(u8) = .empty;
        defer buf.deinit(self.arena);
        try buf.appendSlice(self.arena, func_name);
        try buf.append(self.arena, '(');
        for (args, 0..) |arg, i| {
            if (i > 0) try buf.append(self.arena, ',');
            const argname: []const u8 = switch (arg) {
                .col_ref => |c| c,
                else => return ParseError.SqlInvalidProjection,
            };
            try buf.appendSlice(self.arena, argname);
        }
        try buf.append(self.arena, ')');
        return try buf.toOwnedSlice(self.arena);
    }

    /// Rewrite raw aggregate references in a HAVING predicate (parsed as
    /// the canonical name `func(arg)`) to the matching SELECT aggregate's
    /// alias, so they bind to the grouped output column.
    fn rewriteHavingAggRefs(self: *Parser, expr: *PredicateExpr, proj: []const ProjItem) ParseError!void {
        switch (expr.*) {
            .leaf => |*l| {
                if (try self.aggAliasFor(proj, l.col)) |alias| l.col = alias;
            },
            .leaf_col_col => |*lc| {
                if (try self.aggAliasFor(proj, lc.left)) |a| lc.left = a;
                if (try self.aggAliasFor(proj, lc.right)) |a| lc.right = a;
            },
            .@"and", .@"or" => |children| for (children) |*c| try self.rewriteHavingAggRefs(@constCast(c), proj),
            .not => |child| try self.rewriteHavingAggRefs(@constCast(child), proj),
            else => {},
        }
    }

    fn aggAliasFor(self: *Parser, proj: []const ProjItem, name: []const u8) ParseError!?[]const u8 {
        for (proj) |p| switch (p.kind) {
            .agg => |a| {
                // count_distinct / expression-arg aggregates aren't
                // expressible as a HAVING reference today; skip them.
                if (a.arg_expr != null) continue;
                const fname = if (a.func == .udf)
                    (a.udf_name orelse continue)
                else
                    (aggFuncName(a.func) orelse continue);
                const arg = a.col orelse "*";
                const cname = std.fmt.allocPrint(self.arena, "{s}({s})", .{ fname, arg }) catch return ParseError.OutOfMemory;
                if (std.ascii.eqlIgnoreCase(cname, name)) return p.name;
            },
            else => {},
        };
        return null;
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

    pub fn parseQualifiedIdentList(self: *Parser) ParseError![]const []const u8 {
        var items: std.ArrayList([]const u8) = .empty;
        defer items.deinit(self.arena);
        while (true) {
            const first = try self.expectIdent();
            const name = try self.dupQualifiedColRef(first);
            try items.append(self.arena, name);
            if (self.cur.tag != .comma) break;
            try self.advance();
        }
        return try items.toOwnedSlice(self.arena);
    }

    /// Parse the comma-separated GROUP BY item list as general
    /// expressions (covers ordinals, aliases, columns, and computed keys).
    pub fn parseGroupByExprs(self: *Parser) ParseError![]const ir.Expr {
        var items: std.ArrayList(ir.Expr) = .empty;
        defer items.deinit(self.arena);
        while (true) {
            const e = try self.parseAddSub();
            try items.append(self.arena, e);
            if (self.cur.tag != .comma) break;
            try self.advance();
        }
        return try items.toOwnedSlice(self.arena);
    }

    const GroupByResolution = struct { cols: []const []const u8, gk: []bool };

    /// Resolve GROUP BY items against the projection. Each item binds to
    /// a projected column (by ordinal, by name/alias, or by structural
    /// expression match), yielding the grouping-key column names and a
    /// per-projection flag. A bare column not present in the SELECT list
    /// is grouped directly.
    fn resolveGroupBy(self: *Parser, proj: []const ProjItem, group_exprs: []const ir.Expr) ParseError!GroupByResolution {
        const gk = try self.arena.alloc(bool, proj.len);
        @memset(gk, false);
        var cols: std.ArrayList([]const u8) = .empty;
        defer cols.deinit(self.arena);
        for (group_exprs) |ge| {
            if (ordinalOf(ge)) |k| {
                if (k < 1 or k > proj.len) return ParseError.SqlInvalidProjection;
                try self.markGroupKey(proj, gk, k - 1, &cols);
                continue;
            }
            if (findGroupMatch(proj, ge)) |idx| {
                try self.markGroupKey(proj, gk, idx, &cols);
                continue;
            }
            switch (ge) {
                .col_ref => |c| try cols.append(self.arena, try self.arena.dupe(u8, c)),
                else => return ParseError.SqlInvalidProjection,
            }
        }
        return .{ .cols = try cols.toOwnedSlice(self.arena), .gk = gk };
    }

    fn markGroupKey(
        self: *Parser,
        proj: []const ProjItem,
        gk: []bool,
        idx: usize,
        cols: *std.ArrayList([]const u8),
    ) ParseError!void {
        switch (proj[idx].kind) {
            // A plain column groups on its underlying column name (the
            // upstream schema has that, not any SELECT alias). A computed
            // expression is hoisted into the pre-aggregate Compute under
            // the projection's name, which the GroupBy then groups on.
            .col => |c| {
                gk[idx] = true;
                try cols.append(self.arena, c);
            },
            .expr => {
                gk[idx] = true;
                try cols.append(self.arena, proj[idx].name);
            },
            // Can't group by an aggregate or window output.
            .star, .agg, .window => return ParseError.SqlInvalidProjection,
        }
    }

    /// A bare positive integer literal in GROUP BY is a 1-based ordinal
    /// reference into the SELECT list.
    fn ordinalOf(e: ir.Expr) ?usize {
        const v = switch (e) {
            .lit => |val| val,
            else => return null,
        };
        const n: i128 = switch (v) {
            .int => |x| x,
            .bigint => |x| x,
            .smallint => |x| x,
            .tinyint => |x| x,
            else => return null,
        };
        if (n < 1) return null;
        return @intCast(n);
    }

    fn findGroupMatch(proj: []const ProjItem, ge: ir.Expr) ?usize {
        switch (ge) {
            .col_ref => |name| {
                // Output-name (alias) match takes precedence.
                for (proj, 0..) |p, i| {
                    if (std.ascii.eqlIgnoreCase(p.name, name)) return i;
                }
                // Else bind to an aliased plain column by its underlying name,
                // so `GROUP BY URL` matches a `URL AS Dst` projection item.
                for (proj, 0..) |p, i| switch (p.kind) {
                    .col => |c| if (groupColumnNameEql(c, name)) return i,
                    else => {},
                };
                return null;
            },
            else => {
                for (proj, 0..) |p, i| switch (p.kind) {
                    .expr => |e| if (exprEqual(e, ge)) return i,
                    else => {},
                };
                return null;
            },
        }
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
        return @import("../exec/scalar_fn_common.zig").parseDateTimeString(s);
    }

    fn ymdToDays(year: i32, month: u32, day: u32) i32 {
        return @import("../exec/scalar_fn_common.zig").ymdToDays(year, month, day);
    }

    pub fn allocOp(self: *Parser, op: ir.Op) ParseError!*ir.Op {
        const out = try self.arena.create(ir.Op);
        out.* = op;
        return out;
    }

    pub fn materializePredicateExpr(self: *Parser, expr: ir.Expr) ParseError![]const u8 {
        if (!self.predicate_derived_enabled) return ParseError.SqlInvalidProjection;
        const name = std.fmt.allocPrint(self.arena, "__pred_expr_{d}", .{self.predicate_derived_counter}) catch return ParseError.OutOfMemory;
        self.predicate_derived_counter += 1;
        try self.predicate_derived.append(self.arena, .{ .name = name, .expr = expr });
        return name;
    }

    pub fn predicateDerivedEnabled(self: *const Parser) bool {
        return self.predicate_derived_enabled;
    }

    // -----------------------------------------------------------------------
    // Post-parse pass: wraps each shared CTE's stored op in a Materialize
    // node (.auto / .force — every boundary materializes; NOT MATERIALIZED
    // bodies were wrapped per-reference at FROM resolution instead). The wrap
    // is in-place (mutates *cte_op contents); existing references still hold
    // the old pointer, which now resolves to a Materialize.
    // -----------------------------------------------------------------------

    fn applyAutoMaterialize(self: *Parser) ParseError!void {
        if (self.ctes.count() == 0) return;

        var it = self.ctes.iterator();
        while (it.next()) |entry| {
            // Every CTE boundary materializes; the hint only chooses SHARED
            // (one stage, every reference reads it — .force and .auto) vs
            // REGENERATED (.never — parseFromTarget already wrapped each
            // reference in its own node, so the body stays bare here).
            const should_wrap = switch (entry.value_ptr.hint) {
                .never => entry.value_ptr.separable != null,
                .force, .auto => true,
            };
            if (!should_wrap) continue;
            const cte_op = entry.value_ptr.op;
            // In-place wrap: move the existing contents into a new
            // arena-owned Op, then overwrite the original with a
            // Materialize variant pointing at it. All references that
            // already hold &cte_op continue to work — they now see the
            // Materialize wrapper.
            const inner = try self.arena.create(ir.Op);
            inner.* = cte_op.*;
            cte_op.* = .{
                .materialize = .{
                    .upstream = inner,
                    // Explicit AS MATERIALIZED: the staged compiler must buffer
                    // even a single-reference body it would otherwise inline.
                    // SEPARABLE implies the same demand — the marked block is
                    // the slice fan-in point, so it must be a real stage. An
                    // outer mark's private closure swallows inner marks BEFORE
                    // any staging decision, so blanket-marking a stack still
                    // creates exactly one fan-in stage, not one per mark.
                    .forced = entry.value_ptr.hint == .force or entry.value_ptr.separable != null,
                    .separable = entry.value_ptr.separable,
                },
            };
        }
    }
};

/// Returns true when the projection's name+order already match the
/// natural output of a GroupBy: `[group_cols..., agg_aliases...]`.
fn isBareStarProjection(proj: []const ProjItem) bool {
    if (proj.len != 1) return false;
    return switch (proj[0].kind) {
        .star => |qual| qual == null,
        else => false,
    };
}

fn selectDerivedCount(proj: []const ProjItem) u32 {
    var n: u32 = 0;
    for (proj) |p| switch (p.kind) {
        .expr, .window => n += 1,
        else => {},
    };
    return n;
}

fn projectionHasRenamedCols(proj: []const ProjItem) bool {
    for (proj) |p| switch (p.kind) {
        .col => |c| if (!types.columnNameEql(c, p.name)) return true,
        else => {},
    };
    return false;
}

fn projMatchesGroupByOrder(proj: []const ProjItem, group_cols: []const []const u8) bool {
    if (proj.len != group_cols.len + countAggs(proj)) return false;
    var i: usize = 0;
    // Group cols must come first, in order.
    while (i < group_cols.len) : (i += 1) {
        switch (proj[i].kind) {
            .col => |c| if (!groupColumnNameEql(c, group_cols[i])) return false,
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

/// Scalar functions whose result depends on more than their arguments
/// (wall clock, RNG, ...). A group key built from one of these is NOT a
/// pure function of the other keys, so it must never be collapsed. The
/// registry doesn't expose these yet, but list them so the rewrite stays
/// correct the moment they land.
fn isNondeterministicFn(name: []const u8) bool {
    const names = [_][]const u8{
        "now",          "current_date", "current_timestamp",
        "current_time", "localtime",    "localtimestamp",
        "random",       "rand",         "uuid",
        "uuid_short",   "sysdate",      "unix_timestamp",
    };
    for (names) |n| if (std.ascii.eqlIgnoreCase(n, name)) return true;
    return false;
}

/// True when every column `e` references is the name of a *retained* group
/// key (a plain-column grouping key), and `e` is deterministic. A constant
/// expression (no column refs) trivially qualifies. CASE / subquery /
/// var_ref are treated conservatively as non-collapsible. This is the
/// detection scope for functional-dependency group-key collapse — kept
/// deliberately narrow: `base_col OP literal`, nested arithmetic over
/// anchors, and pure constants. The anchor set is the plain-column grouping
/// keys (those are always retained, never collapsed onto each other).
fn nameInList(name: []const u8, cols: []const []const u8) bool {
    for (cols) |c| {
        if (groupColumnNameEql(name, c)) return true;
    }
    return false;
}

fn groupColumnNameEql(a: []const u8, b: []const u8) bool {
    if (types.columnNameEql(a, b)) return true;
    if (std.mem.lastIndexOfScalar(u8, a, '.')) |dot| {
        if (std.mem.indexOfScalar(u8, b, '.') == null and types.columnNameEql(a[dot + 1 ..], b)) return true;
    }
    if (std.mem.lastIndexOfScalar(u8, b, '.')) |dot| {
        if (std.mem.indexOfScalar(u8, a, '.') == null and types.columnNameEql(a, b[dot + 1 ..])) return true;
    }
    return false;
}

/// True when `e` is a deterministic scalar expression every column reference of
/// which names a GROUP BY column — so it can be evaluated once per output group
/// (above the aggregate) rather than per input row. Mirrors `exprCollapsesOnto`
/// but keys on the GROUP BY column names directly (the expression itself is not
/// a grouping key). CASE / subquery / var_ref bail conservatively.
fn groupedOutputNameAvailable(name: []const u8, group_cols: []const []const u8, extra_cols: []const []const u8) bool {
    return nameInList(name, group_cols) or nameInList(name, extra_cols);
}

fn exprAvailableAfterGroup(e: ir.Expr, group_cols: []const []const u8, extra_cols: []const []const u8) bool {
    return switch (e) {
        .lit => true,
        .null_lit => true,
        .col_ref => |name| groupedOutputNameAvailable(name, group_cols, extra_cols),
        .call => |c| blk: {
            if (isNondeterministicFn(c.fn_name)) break :blk false;
            for (c.args) |arg| {
                if (!exprAvailableAfterGroup(arg, group_cols, extra_cols)) break :blk false;
            }
            break :blk true;
        },
        .case => |c| blk: {
            for (c.branches) |branch| {
                if (!predicateAvailableAfterGroup(branch.cond, group_cols, extra_cols)) break :blk false;
                if (!exprAvailableAfterGroup(branch.then, group_cols, extra_cols)) break :blk false;
            }
            if (c.else_branch) |else_branch| {
                if (!exprAvailableAfterGroup(else_branch.*, group_cols, extra_cols)) break :blk false;
            }
            break :blk true;
        },
        else => false,
    };
}

fn predicateAvailableAfterGroup(p: PredicateExpr, group_cols: []const []const u8, extra_cols: []const []const u8) bool {
    return switch (p) {
        .leaf => |l| groupedOutputNameAvailable(l.col, group_cols, extra_cols),
        .day_leaf => |l| groupedOutputNameAvailable(l.col, group_cols, extra_cols),
        .leaf_col_col => |lc| groupedOutputNameAvailable(lc.left, group_cols, extra_cols) and
            groupedOutputNameAvailable(lc.right, group_cols, extra_cols),
        .is_null => |c| groupedOutputNameAvailable(c, group_cols, extra_cols),
        .is_not_null => |c| groupedOutputNameAvailable(c, group_cols, extra_cols),
        .like => |l| groupedOutputNameAvailable(l.col, group_cols, extra_cols),
        .in_set => |s| groupedOutputNameAvailable(s.col, group_cols, extra_cols),
        .leaf_var => |v| groupedOutputNameAvailable(v.col, group_cols, extra_cols),
        .@"and", .@"or" => |children| blk: {
            for (children) |child| {
                if (!predicateAvailableAfterGroup(child, group_cols, extra_cols)) break :blk false;
            }
            break :blk true;
        },
        .not => |child| predicateAvailableAfterGroup(child.*, group_cols, extra_cols),
        .always, .unknown => true,
        else => false,
    };
}

fn exprReferencesAnyColumn(e: ir.Expr, cols: []const []const u8) bool {
    return switch (e) {
        .col_ref => |name| nameInList(name, cols),
        .call => |c| blk: {
            for (c.args) |arg| {
                if (exprReferencesAnyColumn(arg, cols)) break :blk true;
            }
            break :blk false;
        },
        .case => |c| blk: {
            for (c.branches) |branch| {
                if (predicateReferencesAnyColumn(branch.cond, cols)) break :blk true;
                if (exprReferencesAnyColumn(branch.then, cols)) break :blk true;
            }
            if (c.else_branch) |else_branch| {
                if (exprReferencesAnyColumn(else_branch.*, cols)) break :blk true;
            }
            break :blk false;
        },
        else => false,
    };
}

fn predicateReferencesAnyColumn(p: PredicateExpr, cols: []const []const u8) bool {
    return switch (p) {
        .leaf => |l| nameInList(l.col, cols),
        .day_leaf => |l| nameInList(l.col, cols),
        .leaf_col_col => |lc| nameInList(lc.left, cols) or nameInList(lc.right, cols),
        .is_null => |c| nameInList(c, cols),
        .is_not_null => |c| nameInList(c, cols),
        .like => |l| nameInList(l.col, cols),
        .in_set => |s| nameInList(s.col, cols),
        .leaf_var => |v| nameInList(v.col, cols),
        .@"and", .@"or" => |children| blk: {
            for (children) |child| {
                if (predicateReferencesAnyColumn(child, cols)) break :blk true;
            }
            break :blk false;
        },
        .not => |child| predicateReferencesAnyColumn(child.*, cols),
        else => false,
    };
}

fn exprCollapsesOnto(proj: []const ProjItem, grouping_key: []const bool, e: ir.Expr) bool {
    return switch (e) {
        .lit => true,
        .null_lit => true,
        .col_ref => |name| isPlainGroupKey(proj, grouping_key, name),
        .call => |c| blk: {
            if (isNondeterministicFn(c.fn_name)) break :blk false;
            for (c.args) |arg| {
                if (!exprCollapsesOnto(proj, grouping_key, arg)) break :blk false;
            }
            break :blk true;
        },
        // CASE branches carry predicate conditions whose column refs aren't
        // walked here; subquery / var_ref are resolved elsewhere. Bail.
        else => false,
    };
}

/// Is `name` a plain-column grouping key (an FD-collapse anchor)?
fn isPlainGroupKey(proj: []const ProjItem, grouping_key: []const bool, name: []const u8) bool {
    for (proj, 0..) |p, i| {
        if (!grouping_key[i]) continue;
        switch (p.kind) {
            .col => |c| if (groupColumnNameEql(c, name)) return true,
            else => {},
        }
    }
    return false;
}

/// Structural equality of two expressions. Used to bind a GROUP BY
/// expression to the matching SELECT projection (e.g. `GROUP BY
/// date_trunc(...)` ↔ `SELECT date_trunc(...) AS M`). CASE / subquery /
/// var_ref nodes are conservatively treated as unequal — grouping by
/// those goes through alias references instead.
fn exprEqual(a: ir.Expr, b: ir.Expr) bool {
    if (std.meta.activeTag(a) != std.meta.activeTag(b)) return false;
    return switch (a) {
        .col_ref => |x| std.ascii.eqlIgnoreCase(x, b.col_ref),
        .lit => |x| valueEqual(x, b.lit),
        .null_lit => |x| std.meta.eql(x, b.null_lit),
        .call => |x| blk: {
            const y = b.call;
            if (!std.mem.eql(u8, x.fn_name, y.fn_name)) break :blk false;
            if (x.args.len != y.args.len) break :blk false;
            for (x.args, y.args) |xa, ya| if (!exprEqual(xa, ya)) break :blk false;
            break :blk true;
        },
        else => false,
    };
}

fn valueEqual(a: @import("../types.zig").Value, b: @import("../types.zig").Value) bool {
    if (std.meta.activeTag(a) != std.meta.activeTag(b)) return false;
    return switch (a) {
        .text => |s| std.mem.eql(u8, s, b.text),
        .boolean => |x| x == b.boolean,
        .float => |x| x == b.float,
        .double => |x| x == b.double,
        .uuid => |x| x == b.uuid,
        .int => |x| x == b.int,
        .bigint => |x| x == b.bigint,
        .smallint => |x| x == b.smallint,
        .tinyint => |x| x == b.tinyint,
        .largeint => |x| x == b.largeint,
        .date => |x| x == b.date,
        .datetime => |x| x == b.datetime,
        .decimal64 => |x| x == b.decimal64,
        .decimal128 => |x| x == b.decimal128,
    };
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
    hidden_windows: []const WindowExprRef,
    hidden_partition_exprs: []const ir.Derived,
    upstream: *ir.Op,
    named_windows: *const std.StringHashMapUnmanaged(ir.WindowSpec),
) ParseError!?*ir.Op {
    var count: usize = 0;
    for (proj) |p| switch (p.kind) {
        .window => count += 1,
        else => {},
    };
    count += hidden_windows.len;
    if (count == 0) return null;

    var specs_buf: std.ArrayList(ir.WindowSpec) = .empty;
    defer specs_buf.deinit(arena);
    var calls_buf: std.ArrayList(ir.WindowCall) = .empty;
    defer calls_buf.deinit(arena);
    var pre_window_buf: std.ArrayList(ir.Derived) = .empty;
    defer pre_window_buf.deinit(arena);
    for (hidden_partition_exprs) |derived| {
        try pre_window_buf.append(arena, derived);
    }
    var window_arg_counter: usize = 0;

    for (proj) |p| switch (p.kind) {
        .window => |w| {
            const normalized = try normalizeWindowCallArgs(arena, w, &pre_window_buf, &window_arg_counter);
            var resolved_spec: ir.WindowSpec = switch (normalized.spec_kind) {
                .inline_spec => |s| s,
                .named => |name| blk: {
                    const stored = named_windows.get(name) orelse
                        return ParseError.SqlInvalidProjection;
                    break :blk parse_window.cloneWindowSpec(arena, stored) catch
                        return ParseError.OutOfMemory;
                },
            };
            try rewriteWindowAggRefs(arena, proj, &resolved_spec);
            const spec_idx = blk: {
                for (specs_buf.items, 0..) |existing, i| {
                    if (parse_window.windowSpecsEqual(existing, resolved_spec)) break :blk i;
                }
                try specs_buf.append(arena, resolved_spec);
                break :blk specs_buf.items.len - 1;
            };
            try calls_buf.append(arena, .{
                .spec_idx = @intCast(spec_idx),
                .func = normalized.func,
                .args = normalized.args,
                .ignore_nulls = normalized.ignore_nulls,
                .output_name = p.name,
            });
        },
        else => {},
    };
    for (hidden_windows) |ref| {
        const normalized = try normalizeWindowCallArgs(arena, ref.call, &pre_window_buf, &window_arg_counter);
        var resolved_spec: ir.WindowSpec = switch (normalized.spec_kind) {
            .inline_spec => |s| s,
            .named => |name| blk: {
                const stored = named_windows.get(name) orelse
                    return ParseError.SqlInvalidProjection;
                break :blk parse_window.cloneWindowSpec(arena, stored) catch
                    return ParseError.OutOfMemory;
            },
        };
        try rewriteWindowAggRefs(arena, proj, &resolved_spec);
        const spec_idx = blk: {
            for (specs_buf.items, 0..) |existing, i| {
                if (parse_window.windowSpecsEqual(existing, resolved_spec)) break :blk i;
            }
            try specs_buf.append(arena, resolved_spec);
            break :blk specs_buf.items.len - 1;
        };
        try calls_buf.append(arena, .{
            .spec_idx = @intCast(spec_idx),
            .func = normalized.func,
            .args = normalized.args,
            .ignore_nulls = normalized.ignore_nulls,
            .output_name = ref.name,
        });
    }

    var window_upstream = upstream;
    if (pre_window_buf.items.len > 0) {
        const derived_slice = try pre_window_buf.toOwnedSlice(arena);
        const compute_op = try arena.create(ir.Op);
        compute_op.* = .{ .compute = .{ .derived = derived_slice, .upstream = upstream } };
        window_upstream = compute_op;
    }

    const specs_slice = try specs_buf.toOwnedSlice(arena);
    const calls_slice = try calls_buf.toOwnedSlice(arena);
    const op = try arena.create(ir.Op);
    op.* = .{ .window = .{
        .specs = specs_slice,
        .calls = calls_slice,
        .upstream = window_upstream,
    } };
    return op;
}

fn normalizeWindowCallArgs(
    arena: Allocator,
    call: ParsedWindowCall,
    pre_window_buf: *std.ArrayList(ir.Derived),
    window_arg_counter: *usize,
) ParseError!ParsedWindowCall {
    if (call.args.len == 0) return call;
    var changed = false;
    const args = try arena.alloc(ir.Expr, call.args.len);
    for (call.args, args, 0..) |arg, *dst, i| {
        if (windowArgNeedsPrecompute(call.func, i, arg)) {
            const owned_name = std.fmt.allocPrint(arena, "__window_arg_{d}", .{window_arg_counter.*}) catch return ParseError.OutOfMemory;
            window_arg_counter.* += 1;
            try pre_window_buf.append(arena, .{ .name = owned_name, .expr = arg });
            dst.* = ir.Expr{ .col_ref = owned_name };
            changed = true;
        } else {
            dst.* = arg;
        }
    }
    if (!changed) return call;
    return .{
        .func = call.func,
        .args = args,
        .ignore_nulls = call.ignore_nulls,
        .spec_kind = call.spec_kind,
    };
}

fn windowArgNeedsPrecompute(func: ir.WindowFunc, arg_idx: usize, arg: ir.Expr) bool {
    return switch (func) {
        .sum, .avg, .count, .min, .max => switch (arg) {
            .col_ref => false,
            else => true,
        },
        .lag, .lead, .first_value, .last_value => arg_idx == 0 and switch (arg) {
            .col_ref => false,
            else => true,
        },
        .nth_value => arg_idx == 0 and switch (arg) {
            .col_ref => false,
            else => true,
        },
        else => false,
    };
}

fn rewriteWindowAggRefs(arena: Allocator, proj: []const ProjItem, spec: *ir.WindowSpec) ParseError!void {
    var partition_changed = false;
    var new_partition: []const []const u8 = spec.partition_by;
    for (spec.partition_by) |col| {
        if (try aggAliasForProjection(arena, proj, col)) |_| {
            partition_changed = true;
            break;
        }
    }
    if (partition_changed) {
        const out = try arena.alloc([]const u8, spec.partition_by.len);
        for (spec.partition_by, out) |col, *dst| {
            dst.* = (try aggAliasForProjection(arena, proj, col)) orelse col;
        }
        new_partition = out;
    }
    spec.partition_by = new_partition;

    var order_changed = false;
    var new_order: []const @import("../exec/sort.zig").SortSpec = spec.order_by;
    for (spec.order_by) |s| {
        if (try aggAliasForProjection(arena, proj, s.col)) |_| {
            order_changed = true;
            break;
        }
    }
    if (order_changed) {
        const out = try arena.alloc(@import("../exec/sort.zig").SortSpec, spec.order_by.len);
        for (spec.order_by, out) |s, *dst| {
            dst.* = s;
            if (try aggAliasForProjection(arena, proj, s.col)) |alias| dst.col = alias;
        }
        new_order = out;
    }
    spec.order_by = new_order;
}

fn aggAliasForProjection(arena: Allocator, proj: []const ProjItem, name: []const u8) ParseError!?[]const u8 {
    for (proj) |p| switch (p.kind) {
        .agg => |a| {
            if (a.arg_expr != null) continue;
            const fname = if (a.func == .udf)
                (a.udf_name orelse continue)
            else
                (aggFuncName(a.func) orelse continue);
            const arg = a.col orelse "*";
            const cname = std.fmt.allocPrint(arena, "{s}({s})", .{ fname, arg }) catch return ParseError.OutOfMemory;
            if (std.ascii.eqlIgnoreCase(cname, name)) return p.name;
        },
        else => {},
    };
    return null;
}

fn nameIn(needle: []const u8, names: []const []const u8) bool {
    for (names) |n| if (types.columnNameEql(n, needle)) return true;
    return false;
}

fn combineJoinSides(a: JoinExprSide, b: JoinExprSide) JoinExprSide {
    if (a == .mixed or b == .mixed) return .mixed;
    if (a == .none) return b;
    if (b == .none) return a;
    return if (a == b) a else .mixed;
}

fn isRangeJoinOp(op: PredicateOp) bool {
    return switch (op) {
        .lt, .lte, .gt, .gte => true,
        else => false,
    };
}

fn reverseRangeOp(op: PredicateOp) PredicateOp {
    return switch (op) {
        .lt => .gt,
        .lte => .gte,
        .gt => .lt,
        .gte => .lte,
        else => op,
    };
}

test "sql table function: CREATE parse, body capture, validation, expansion" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const aa = arena.allocator();

    const stmt = "CREATE FUNCTION t_f(pid INT, div INT) RETURNS TABLE AS (SELECT c FROM t WHERE a = pid AND b = div)";
    const op = try parse(aa, stmt);
    try std.testing.expect(op.* == .ddl);
    const cf = op.ddl.create_sql_function;
    try std.testing.expectEqualStrings("t_f", cf.name);
    try std.testing.expectEqual(@as(usize, 2), cf.param_names.len);
    try std.testing.expectEqualStrings("SELECT c FROM t WHERE a = pid AND b = div", cf.body);
    try validateFnBody(aa, cf.body, cf.param_names);
}
