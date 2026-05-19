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
    SqlCteRedefined,
    SqlSubqueryNeedsAlias,
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
    // Post-parse pass: refcount each *ir.Op reachable from the root,
    // then wrap CTE roots in Materialize per their hint (force / never /
    // auto + refcount ≥ 2). Mutates CTE ops in place so existing
    // references see the new wrapper.
    try parser.applyAutoMaterialize(op);
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

/// Per-CTE materialization hint from the SQL surface.
///   .auto  — refcount-driven: wrap when references ≥ 2.
///   .force — `MATERIALIZED` keyword: always wrap.
///   .never — `NOT MATERIALIZED` keyword: always inline.
pub const MaterializeHint = enum { auto, force, never };

const CteEntry = struct {
    op: *ir.Op,
    hint: MaterializeHint,
};

const Parser = struct {
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
        // DDL / SHOW / INSERT are leading-keyword forms that don't combine
        // with WITH. They have no projection / FROM / WHERE / etc.;
        // dispatch before the SELECT-only path.
        switch (self.cur.tag) {
            .kw_create, .kw_drop, .kw_use => return try self.parseDdl(),
            .kw_show => return try self.parseShow(),
            .kw_insert => return try self.parseInsert(),
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

    fn parseDdl(self: *Parser) ParseError!*ir.Op {
        const head = self.cur.tag;
        try self.advance();
        switch (head) {
            .kw_create => {
                if (self.cur.tag == .kw_database) {
                    try self.advance();
                    const name = try self.dupedIdent();
                    return try self.allocOp(.{ .ddl = .{ .create_database = name } });
                }
                if (self.cur.tag == .kw_schema) {
                    try self.advance();
                    const name = try self.dupedIdent();
                    return try self.allocOp(.{ .ddl = .{ .create_schema = name } });
                }
                if (self.cur.tag == .kw_table) {
                    try self.advance();
                    return try self.parseCreateTableBody();
                }
                return ParseError.SqlExpectedKeyword;
            },
            .kw_drop => {
                if (self.cur.tag == .kw_database) {
                    try self.advance();
                    const name = try self.dupedIdent();
                    return try self.allocOp(.{ .ddl = .{ .drop_database = name } });
                }
                if (self.cur.tag == .kw_schema) {
                    try self.advance();
                    const name = try self.dupedIdent();
                    return try self.allocOp(.{ .ddl = .{ .drop_schema = name } });
                }
                if (self.cur.tag == .kw_table) {
                    try self.advance();
                    return try self.parseDropTableBody();
                }
                return ParseError.SqlExpectedKeyword;
            },
            .kw_use => {
                const first = try self.dupedIdent();
                if (self.cur.tag == .dot) {
                    try self.advance();
                    const second = try self.dupedIdent();
                    return try self.allocOp(.{ .ddl = .{ .use_database_schema = .{
                        .database = first,
                        .schema = second,
                    } } });
                }
                return try self.allocOp(.{ .ddl = .{ .use_schema = first } });
            },
            else => unreachable,
        }
    }

    /// CREATE TABLE [IF NOT EXISTS] [db.][schema.]name ( column_def, ... [, PRIMARY KEY (..)] )
    fn parseCreateTableBody(self: *Parser) ParseError!*ir.Op {
        var if_not_exists = false;
        if (self.cur.tag == .kw_if) {
            try self.advance();
            if (self.cur.tag != .kw_not) return ParseError.SqlExpectedKeyword;
            try self.advance();
            if (self.cur.tag != .kw_exists) return ParseError.SqlExpectedKeyword;
            try self.advance();
            if_not_exists = true;
        }
        const ref = try self.parseTableRef();
        try self.expect(.lparen);

        var cols: std.ArrayList(ir.ColumnDef) = .empty;
        defer cols.deinit(self.arena);
        var inline_pk: ?[]const u8 = null;
        var table_pk: ?[]const []const u8 = null;

        while (true) {
            if (self.cur.tag == .kw_primary) {
                try self.advance();
                if (self.cur.tag != .kw_key) return ParseError.SqlExpectedKeyword;
                try self.advance();
                try self.expect(.lparen);
                table_pk = try self.parseIdentList();
                try self.expect(.rparen);
            } else {
                const col = try self.parseColumnDef();
                try cols.append(self.arena, col.def);
                if (col.is_pk) {
                    if (inline_pk != null) return ParseError.SqlInvalidProjection;
                    inline_pk = col.def.name;
                }
            }
            if (self.cur.tag != .comma) break;
            try self.advance();
        }
        try self.expect(.rparen);

        // Tolerate trailing engine-options noise like `ENGINE=...` / `CHARSET=...`
        // by eating any tokens up to EOF or semicolon. Keeps MySQL clients happy.
        while (self.cur.tag != .eof and self.cur.tag != .semicolon) {
            try self.advance();
        }

        if (inline_pk != null and table_pk != null) {
            return ParseError.SqlInvalidProjection;
        }
        const order_key: []const []const u8 = if (table_pk) |tpk|
            tpk
        else if (inline_pk) |ipk| blk: {
            const one = try self.arena.alloc([]const u8, 1);
            one[0] = ipk;
            break :blk one;
        } else return ParseError.SqlInvalidProjection;

        const owned_cols = try self.arena.alloc(ir.ColumnDef, cols.items.len);
        for (cols.items, 0..) |c, i| owned_cols[i] = c;

        return try self.allocOp(.{ .ddl = .{ .create_table = .{
            .table = ref,
            .if_not_exists = if_not_exists,
            .columns = owned_cols,
            .order_key = order_key,
        } } });
    }

    fn parseDropTableBody(self: *Parser) ParseError!*ir.Op {
        var if_exists = false;
        if (self.cur.tag == .kw_if) {
            try self.advance();
            if (self.cur.tag != .kw_exists) return ParseError.SqlExpectedKeyword;
            try self.advance();
            if_exists = true;
        }
        const ref = try self.parseTableRef();
        return try self.allocOp(.{ .ddl = .{ .drop_table = .{
            .table = ref,
            .if_exists = if_exists,
        } } });
    }

    fn parseInsert(self: *Parser) ParseError!*ir.Op {
        try self.advance(); // consume INSERT
        if (self.cur.tag != .kw_into) return ParseError.SqlExpectedKeyword;
        try self.advance();
        const ref = try self.parseTableRef();

        var cols_opt: ?[]const []const u8 = null;
        if (self.cur.tag == .lparen) {
            try self.advance();
            cols_opt = try self.parseIdentList();
            try self.expect(.rparen);
        }

        if (self.cur.tag != .kw_values) return ParseError.SqlExpectedKeyword;
        try self.advance();

        var rows: std.ArrayList([]const ?Value) = .empty;
        defer rows.deinit(self.arena);
        while (true) {
            try self.expect(.lparen);
            var row_vals: std.ArrayList(?Value) = .empty;
            defer row_vals.deinit(self.arena);
            while (true) {
                const v = try self.parseInsertValue();
                try row_vals.append(self.arena, v);
                if (self.cur.tag != .comma) break;
                try self.advance();
            }
            try self.expect(.rparen);
            const row_owned = try self.arena.alloc(?Value, row_vals.items.len);
            for (row_vals.items, 0..) |v, i| row_owned[i] = v;
            try rows.append(self.arena, row_owned);
            if (self.cur.tag != .comma) break;
            try self.advance();
        }
        const rows_owned = try self.arena.alloc([]const ?Value, rows.items.len);
        for (rows.items, 0..) |r, i| rows_owned[i] = r;

        return try self.allocOp(.{ .insert = .{
            .table = ref,
            .columns = cols_opt,
            .rows = rows_owned,
        } });
    }

    fn parseTableRef(self: *Parser) ParseError!ir.TableRef {
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

    const ColDefResult = struct { def: ir.ColumnDef, is_pk: bool };

    fn parseColumnDef(self: *Parser) ParseError!ColDefResult {
        if (self.cur.tag != .identifier) return ParseError.SqlExpectedIdent;
        const name = try self.arena.dupe(u8, self.cur.text);
        try self.advance();

        const ty = try self.parseColumnType();

        var nullable = true; // SQL standard default; flipped to false by NOT NULL or PRIMARY KEY
        var is_pk = false;
        var saw_not_null = false;
        while (true) {
            switch (self.cur.tag) {
                .kw_not => {
                    try self.advance();
                    if (self.cur.tag != .kw_null) return ParseError.SqlExpectedNull;
                    try self.advance();
                    nullable = false;
                    saw_not_null = true;
                },
                .kw_primary => {
                    try self.advance();
                    if (self.cur.tag != .kw_key) return ParseError.SqlExpectedKeyword;
                    try self.advance();
                    is_pk = true;
                    nullable = false;
                },
                .kw_null => {
                    if (saw_not_null) return ParseError.SqlExpectedKeyword;
                    try self.advance();
                    nullable = true;
                },
                else => break,
            }
        }
        return .{
            .def = .{ .name = name, .column_type = ty, .nullable = nullable },
            .is_pk = is_pk,
        };
    }

    fn parseColumnType(self: *Parser) ParseError!types.Type {
        if (self.cur.tag != .identifier) return ParseError.SqlExpectedIdent;
        const name = self.cur.text;
        try self.advance();

        // BIGINT / INT / INTEGER / SMALLINT / TINYINT
        if (asciiEqlAny(name, &.{"bigint"})) return .bigint;
        if (asciiEqlAny(name, &.{ "int", "integer" })) return .int;
        if (asciiEqlAny(name, &.{"smallint"})) return .smallint;
        if (asciiEqlAny(name, &.{"tinyint"})) return .smallint;
        if (asciiEqlAny(name, &.{ "float", "real" })) return .float;
        if (asciiEqlAny(name, &.{"double"})) {
            // Optional PRECISION trailing keyword.
            if (self.cur.tag == .identifier and std.ascii.eqlIgnoreCase(self.cur.text, "precision")) {
                try self.advance();
            }
            return .double;
        }
        if (asciiEqlAny(name, &.{ "decimal", "numeric" })) {
            try self.expect(.lparen);
            if (self.cur.tag != .integer) return ParseError.SqlExpectedValue;
            const p_raw = self.cur.value.integer;
            try self.advance();
            try self.expect(.comma);
            if (self.cur.tag != .integer) return ParseError.SqlExpectedValue;
            const s_raw = self.cur.value.integer;
            try self.advance();
            try self.expect(.rparen);
            if (p_raw < 1 or p_raw > 38 or s_raw < 0 or s_raw > p_raw) return ParseError.SqlExpectedValue;
            const p_u: u8 = @intCast(p_raw);
            const s_u: u8 = @intCast(s_raw);
            return if (p_u <= 18)
                types.Type{ .decimal64 = .{ .p = p_u, .s = s_u } }
            else
                types.Type{ .decimal128 = .{ .p = p_u, .s = s_u } };
        }
        if (asciiEqlAny(name, &.{"varchar"})) {
            try self.expect(.lparen);
            if (self.cur.tag != .integer) return ParseError.SqlExpectedValue;
            const n_raw = self.cur.value.integer;
            try self.advance();
            try self.expect(.rparen);
            if (n_raw < 1) return ParseError.SqlExpectedValue;
            return types.Type{ .varchar = @intCast(n_raw) };
        }
        if (asciiEqlAny(name, &.{ "text", "string" })) return .string;
        if (asciiEqlAny(name, &.{ "boolean", "bool" })) return .boolean;
        if (asciiEqlAny(name, &.{"date"})) return .date;
        if (asciiEqlAny(name, &.{ "datetime", "timestamp" })) return .datetime;
        if (asciiEqlAny(name, &.{"uuid"})) return .uuid;
        return ParseError.SqlExpectedKeyword;
    }

    fn parseInsertValue(self: *Parser) ParseError!?Value {
        if (self.cur.tag == .kw_null) {
            try self.advance();
            return null;
        }
        return try self.parseValue();
    }

    fn parseShow(self: *Parser) ParseError!*ir.Op {
        try self.advance(); // consume SHOW
        switch (self.cur.tag) {
            .kw_databases => {
                try self.advance();
                return try self.allocOp(.{ .show = .databases });
            },
            .kw_schemas => {
                try self.advance();
                var db: ?[]const u8 = null;
                if (self.cur.tag == .kw_from) {
                    try self.advance();
                    db = try self.dupedIdent();
                }
                return try self.allocOp(.{ .show = .{ .schemas = db } });
            },
            .kw_tables => {
                try self.advance();
                var ref: ir.TableRef = .{ .name = "" };
                if (self.cur.tag == .kw_from) {
                    try self.advance();
                    const first = try self.dupedIdent();
                    if (self.cur.tag == .dot) {
                        try self.advance();
                        const second = try self.dupedIdent();
                        ref = .{ .database = first, .schema = second, .name = "" };
                    } else {
                        ref = .{ .schema = first, .name = "" };
                    }
                }
                return try self.allocOp(.{ .show = .{ .tables = ref } });
            },
            else => return ParseError.SqlExpectedKeyword,
        }
    }

    fn dupedIdent(self: *Parser) ParseError![]const u8 {
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
    /// the lookahead determines it isn't an aggregate. Each arg may be a
    /// column reference (optionally qualified) or a literal — Compute
    /// materializes literals into a per-batch constant column. Nested
    /// scalar calls are rejected (task #154 covers the recursive
    /// evaluator).
    fn finishScalarCall(self: *Parser, func_name: []const u8) ParseError!ir.Expr {
        try self.advance(); // consume '('
        const fname_dup = try self.arena.dupe(u8, func_name);
        var args: std.ArrayList(ir.Expr) = .empty;
        if (self.cur.tag != .rparen) {
            while (true) {
                const a = try self.parseCallArg();
                try args.append(self.arena, a);
                if (self.cur.tag != .comma) break;
                try self.advance();
            }
        }
        try self.expect(.rparen);
        const args_slice = try args.toOwnedSlice(self.arena);
        return ir.Expr{ .call = .{ .fn_name = fname_dup, .args = args_slice } };
    }

    /// One argument to a scalar function call — column ref, literal,
    /// or nested scalar call. Aggregates can't nest inside any call.
    fn parseCallArg(self: *Parser) ParseError!ir.Expr {
        switch (self.cur.tag) {
            .identifier => {
                const name = self.cur.text;
                try self.advance();
                if (self.cur.tag == .lparen) {
                    // Nested call. Aggregates aren't allowed here per
                    // standard SQL — they belong at the top level of
                    // the SELECT list.
                    if (aggForName(name)) |_| return ParseError.SqlInvalidProjection;
                    return try self.finishScalarCall(name);
                }
                // Qualified column? use last segment.
                var col_name = name;
                if (self.cur.tag == .dot) {
                    try self.advance();
                    if (self.cur.tag != .identifier) return ParseError.SqlExpectedIdent;
                    col_name = self.cur.text;
                    try self.advance();
                }
                return ir.Expr{ .col_ref = try self.arena.dupe(u8, col_name) };
            },
            .integer, .floating, .string, .kw_true, .kw_false => {
                const v = try self.parseValue();
                return ir.Expr{ .lit = v };
            },
            else => return ParseError.SqlExpectedValue,
        }
    }

    /// Parse a bare column reference (possibly qualified). Used inside
    /// contexts that don't accept full expressions.
    fn parseColRefExpr(self: *Parser) ParseError!ir.Expr {
        if (self.cur.tag != .identifier) return ParseError.SqlExpectedIdent;
        const name = self.cur.text;
        try self.advance();
        if (self.cur.tag == .lparen) return ParseError.SqlInvalidProjection;
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
            op = try self.allocOp(.{ .scan = .{ .table = ref } });
            resolved_name = parts_buf[parts_len - 1];
        }

        // Optional AS alias.
        if (self.cur.tag == .kw_as) {
            try self.advance();
            if (self.cur.tag != .identifier) return ParseError.SqlExpectedIdent;
            resolved_name = try self.arena.dupe(u8, self.cur.text);
            try self.advance();
        } else if (self.cur.tag == .identifier) {
            // Implicit alias: bare identifier after the FROM target.
            // SQL clause keywords (JOIN/WHERE/ON/...) aren't .identifier
            // tokens so they don't trigger this.
            resolved_name = try self.arena.dupe(u8, self.cur.text);
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
        .ddl, .show, .insert => {},
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

fn nameIn(needle: []const u8, names: []const []const u8) bool {
    for (names) |n| if (std.mem.eql(u8, n, needle)) return true;
    return false;
}

fn asciiEqlAny(s: []const u8, candidates: []const []const u8) bool {
    for (candidates) |c| {
        if (std.ascii.eqlIgnoreCase(s, c)) return true;
    }
    return false;
}
