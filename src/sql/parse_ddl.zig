//! DDL + DML + COPY + SHOW parsing — extracted from parser.zig.
//!
//! All free functions take the Parser via `anytype` (the only caller
//! ever passes `*parser.Parser`); avoiding a circular import keeps the
//! type plumbing simple. Helpers borrow Parser methods via duck typing
//! (`p.expect`, `p.parseTableRef`, etc.).

const std = @import("std");

const ir = @import("../ir/ir.zig");
const types = @import("../types.zig");
const Value = types.Value;

pub const ColDefResult = struct { def: ir.ColumnDef, is_pk: bool };

pub fn parseDdl(p: anytype) !*ir.Op {
    const PE = @TypeOf(p.*).Err;
    const head = p.cur.tag;
    try p.advance();
    switch (head) {
        .kw_create => {
            if (p.cur.tag == .kw_database) {
                try p.advance();
                const name = try p.dupedIdent();
                return try p.allocOp(.{ .ddl = .{ .create_database = name } });
            }
            if (p.cur.tag == .kw_schema) {
                try p.advance();
                const name = try p.dupedIdent();
                return try p.allocOp(.{ .ddl = .{ .create_schema = name } });
            }
            // CREATE [OR REPLACE] FUNCTION — `function`/`returns` are
            // contextual keywords (matched by identifier text) so columns
            // named "function" keep working everywhere else.
            var or_replace = false;
            if (p.cur.tag == .kw_or) {
                try p.advance();
                if (p.cur.tag != .kw_replace) return PE.SqlExpectedKeyword;
                try p.advance();
                or_replace = true;
            }
            if (isIdentText(p, "function")) {
                try p.advance();
                return try parseCreateFunctionBody(p, or_replace);
            }
            // CREATE [OR REPLACE] [MATERIALIZED] VIEW name AS <select>.
            // `materialized` is a reserved keyword; `view` is contextual.
            if (p.cur.tag == .kw_materialized) {
                try p.advance();
                if (!isIdentText(p, "view")) return PE.SqlExpectedKeyword;
                try p.advance();
                return try parseCreateViewBody(p, or_replace, true);
            }
            if (isIdentText(p, "view")) {
                try p.advance();
                return try parseCreateViewBody(p, or_replace, false);
            }
            if (or_replace) return PE.SqlExpectedKeyword;
            var is_temp = false;
            if (p.cur.tag == .kw_temp or p.cur.tag == .kw_temporary) {
                is_temp = true;
                try p.advance();
            }
            if (p.cur.tag == .kw_table) {
                try p.advance();
                return try parseCreateTableBody(p, is_temp);
            }
            return PE.SqlExpectedKeyword;
        },
        .kw_drop => {
            if (p.cur.tag == .kw_database) {
                try p.advance();
                const name = try p.dupedIdent();
                return try p.allocOp(.{ .ddl = .{ .drop_database = name } });
            }
            if (p.cur.tag == .kw_schema) {
                try p.advance();
                const name = try p.dupedIdent();
                return try p.allocOp(.{ .ddl = .{ .drop_schema = name } });
            }
            if (isIdentText(p, "function")) {
                try p.advance();
                var if_exists = false;
                if (p.cur.tag == .kw_if) {
                    try p.advance();
                    if (p.cur.tag != .kw_exists) return PE.SqlExpectedKeyword;
                    try p.advance();
                    if_exists = true;
                }
                const name = try p.dupedIdent();
                return try p.allocOp(.{ .ddl = .{ .drop_sql_function = .{
                    .name = name,
                    .if_exists = if_exists,
                } } });
            }
            // DROP [MATERIALIZED] VIEW [IF EXISTS] name.
            {
                var materialized = false;
                if (p.cur.tag == .kw_materialized) {
                    materialized = true;
                    try p.advance();
                }
                if (materialized or isIdentText(p, "view")) {
                    if (!isIdentText(p, "view")) return PE.SqlExpectedKeyword;
                    try p.advance();
                    var if_exists = false;
                    if (p.cur.tag == .kw_if) {
                        try p.advance();
                        if (p.cur.tag != .kw_exists) return PE.SqlExpectedKeyword;
                        try p.advance();
                        if_exists = true;
                    }
                    const name = try p.dupedIdent();
                    return try p.allocOp(.{ .ddl = .{ .drop_view = .{
                        .name = name,
                        .if_exists = if_exists,
                        .materialized = materialized,
                    } } });
                }
            }
            if (p.cur.tag == .kw_temp or p.cur.tag == .kw_temporary) {
                try p.advance();
            }
            if (p.cur.tag == .kw_table) {
                try p.advance();
                return try parseDropTableBody(p);
            }
            return PE.SqlExpectedKeyword;
        },
        .kw_use => {
            const first = try p.dupedIdent();
            if (p.cur.tag == .dot) {
                try p.advance();
                const second = try p.dupedIdent();
                return try p.allocOp(.{ .ddl = .{ .use_database_schema = .{
                    .database = first,
                    .schema = second,
                } } });
            }
            return try p.allocOp(.{ .ddl = .{ .use_schema = first } });
        },
        .kw_rename => {
            if (p.cur.tag != .kw_table) return PE.SqlExpectedKeyword;
            try p.advance();
            const from = try p.parseTableRef();
            if (p.cur.tag != .kw_to) return PE.SqlExpectedKeyword;
            try p.advance();
            const to = try p.parseTableRef();
            return try p.allocOp(.{ .ddl = .{ .rename_table = .{ .from = from, .to = to } } });
        },
        .kw_alter => {
            if (p.cur.tag != .kw_table) return PE.SqlExpectedKeyword;
            try p.advance();
            const table = try p.parseTableRef();
            if (p.cur.tag != .kw_add) return PE.SqlExpectedKeyword;
            try p.advance();
            if (p.cur.tag == .kw_column) try p.advance();
            const col = try parseColumnDef(p);
            if (col.is_pk or col.def.auto_increment) return PE.SqlInvalidProjection;
            return try p.allocOp(.{ .ddl = .{ .alter_table_add_column = .{
                .table = table,
                .column = col.def,
            } } });
        },
        .kw_truncate => {
            if (p.cur.tag == .kw_table) try p.advance();
            const table = try p.parseTableRef();
            return try p.allocOp(.{ .ddl = .{ .truncate_table = table } });
        },
        else => unreachable,
    }
}

fn isIdentText(p: anytype, comptime text: []const u8) bool {
    return p.cur.tag == .identifier and std.ascii.eqlIgnoreCase(p.cur.text, text);
}

/// CREATE [OR REPLACE] FUNCTION name([pname ptype, ...]) RETURNS TABLE AS ( body )
///
/// A SQL inline table function. The body SELECT is captured as RAW TEXT
/// (token-boundary balanced-paren scan) — it is validated by a trial
/// parse at registration and re-parsed with bound parameters at every
/// call site. `FUNCTION`/`RETURNS`/`TABLE` here; the leading CREATE [OR
/// REPLACE] was consumed by the caller.
pub fn parseCreateFunctionBody(p: anytype, or_replace: bool) !*ir.Op {
    const PE = @TypeOf(p.*).Err;
    const name = try p.dupedIdent();

    // `CREATE FUNCTION name LANGUAGE zig AS $$source$$` — a compiled table
    // UDF. Shapes live in the source's comptime declarations, so there is
    // no SQL parameter list.
    if (isIdentText(p, "language")) {
        try p.advance();
        if (!isIdentText(p, "zig")) return PE.SqlExpectedKeyword;
        try p.advance();
        if (isIdentText(p, "using")) {
            try p.advance();
            if (p.cur.tag != .string) return PE.SqlExpectedToken;
            const path = p.cur.value.string;
            try p.advance();
            return try p.allocOp(.{ .ddl = .{ .create_zig_function = .{
                .name = name,
                .or_replace = or_replace,
                .source = path,
                .using_path = true,
            } } });
        }
        if (p.cur.tag != .kw_as) return PE.SqlExpectedKeyword;
        try p.advance();
        if (p.cur.tag != .string) return PE.SqlExpectedToken;
        const source = p.cur.value.string;
        try p.advance();
        return try p.allocOp(.{ .ddl = .{ .create_zig_function = .{
            .name = name,
            .or_replace = or_replace,
            .source = source,
        } } });
    }

    try p.expect(.lparen);
    var param_names: std.ArrayList([]const u8) = .empty;
    defer param_names.deinit(p.arena);
    var param_types: std.ArrayList(types.Type) = .empty;
    defer param_types.deinit(p.arena);
    if (p.cur.tag != .rparen) {
        while (true) {
            try param_names.append(p.arena, try p.dupedIdent());
            try param_types.append(p.arena, try parseColumnType(p));
            if (p.cur.tag != .comma) break;
            try p.advance();
        }
    }
    try p.expect(.rparen);

    if (!isIdentText(p, "returns")) return PE.SqlExpectedKeyword;
    try p.advance();
    if (p.cur.tag != .kw_table) return PE.SqlExpectedKeyword;
    try p.advance();
    if (p.cur.tag != .kw_as) return PE.SqlExpectedKeyword;
    try p.advance();

    // Raw body capture via LEXER POSITION: while the parser holds token X,
    // `lex.pos` sits at X's source end (the parser is exactly one token
    // ahead). Token `.text` can't be used for offsets — identifier text is
    // an arena-lowercased copy, not a source slice. So: the position at
    // the opening paren is the body start; the position recorded before
    // each advance is the end of the last body token when the matching
    // close paren breaks the loop.
    if (p.cur.tag != .lparen) return PE.SqlExpectedToken;
    const src = p.sourceText();
    const body_start = p.lexPos();
    try p.advance();
    var depth: usize = 0;
    var body_end = body_start;
    while (true) {
        switch (p.cur.tag) {
            .eof => return PE.SqlExpectedToken,
            .lparen => depth += 1,
            .rparen => {
                if (depth == 0) break;
                depth -= 1;
            },
            else => {},
        }
        body_end = p.lexPos();
        try p.advance();
    }
    const body = std.mem.trim(u8, src[body_start..body_end], " \t\r\n");
    if (body.len == 0) return PE.SqlExpectedSelect;
    try p.advance(); // consume the closing rparen

    return try p.allocOp(.{ .ddl = .{ .create_sql_function = .{
        .name = name,
        .or_replace = or_replace,
        .param_names = try p.arena.dupe([]const u8, param_names.items),
        .param_types = try p.arena.dupe(types.Type, param_types.items),
        .body = try p.arena.dupe(u8, body),
    } } });
}

/// CREATE [OR REPLACE] [MATERIALIZED] VIEW name AS <select>. The defining
/// query is captured as raw text (to end of statement), mirroring the
/// inline-function body capture. `MATERIALIZED VIEW` builds a backing table
/// at compile time; a plain view is expanded inline at reference.
pub fn parseCreateViewBody(p: anytype, or_replace: bool, materialized: bool) !*ir.Op {
    const PE = @TypeOf(p.*).Err;
    const name = try p.dupedIdent();
    // Explicit view column lists (`VIEW v (a, b) AS ...`) are not supported
    // in v1 — the column names come from the SELECT's own output.
    if (p.cur.tag == .lparen) return PE.SqlExpectedKeyword;
    if (p.cur.tag != .kw_as) return PE.SqlExpectedKeyword;

    // Raw body capture by lexer position (see parseCreateFunctionBody): the
    // position while the parser holds `AS` is the body start; each token's
    // end position updates `body_end` until a top-level `;` / EOF closes it.
    const src = p.sourceText();
    const body_start = p.lexPos();
    try p.advance();
    var depth: usize = 0;
    var body_end = body_start;
    while (true) {
        switch (p.cur.tag) {
            .eof => break,
            .semicolon => if (depth == 0) break,
            .lparen => depth += 1,
            .rparen => {
                if (depth > 0) depth -= 1;
            },
            else => {},
        }
        body_end = p.lexPos();
        try p.advance();
    }
    const raw = std.mem.trim(u8, src[body_start..body_end], " \t\r\n");
    const body = stripOuterParens(raw);
    if (body.len == 0) return PE.SqlExpectedSelect;

    return try p.allocOp(.{ .ddl = .{ .create_view = .{
        .name = name,
        .or_replace = or_replace,
        .materialized = materialized,
        .body = try p.arena.dupe(u8, body),
    } } });
}

/// REFRESH MATERIALIZED VIEW name. `refresh` was matched contextually by the
/// caller (parseStatement); this consumes it and the rest.
pub fn parseRefresh(p: anytype) !*ir.Op {
    const PE = @TypeOf(p.*).Err;
    try p.advance(); // consume "refresh"
    if (p.cur.tag != .kw_materialized) return PE.SqlExpectedKeyword;
    try p.advance();
    if (!isIdentText(p, "view")) return PE.SqlExpectedKeyword;
    try p.advance();
    const name = try p.dupedIdent();
    return try p.allocOp(.{ .ddl = .{ .refresh_view = name } });
}

/// Strip a fully-wrapping outer paren pair (`(SELECT ...)` → `SELECT ...`),
/// repeatedly. String literals are skipped so parens inside them don't throw
/// off the balance. Leaves a non-wrapped body untouched.
fn stripOuterParens(body: []const u8) []const u8 {
    var s = body;
    while (s.len >= 2 and s[0] == '(') {
        var depth: usize = 0;
        var close: ?usize = null;
        var in_str = false;
        var quote: u8 = 0;
        for (s, 0..) |c, i| {
            if (in_str) {
                if (c == quote) in_str = false;
                continue;
            }
            switch (c) {
                '\'', '"' => {
                    in_str = true;
                    quote = c;
                },
                '(' => depth += 1,
                ')' => {
                    depth -= 1;
                    if (depth == 0) {
                        close = i;
                        break;
                    }
                },
                else => {},
            }
        }
        const m = close orelse break;
        if (m != s.len - 1) break;
        s = std.mem.trim(u8, s[1..m], " \t\r\n");
    }
    return s;
}

/// CREATE [TEMP|TEMPORARY] TABLE [IF NOT EXISTS] [db.][schema.]name
///   ( column_def, ... [, PRIMARY KEY (..)] )
///
/// CTAS form: `CREATE TABLE name AS SELECT ...` — column list omitted;
/// schema inferred from the source query at compile time.
pub fn parseCreateTableBody(p: anytype, is_temp: bool) !*ir.Op {
    const PE = @TypeOf(p.*).Err;
    var if_not_exists = false;
    if (p.cur.tag == .kw_if) {
        try p.advance();
        if (p.cur.tag != .kw_not) return PE.SqlExpectedKeyword;
        try p.advance();
        if (p.cur.tag != .kw_exists) return PE.SqlExpectedKeyword;
        try p.advance();
        if_not_exists = true;
    }
    const ref = try p.parseTableRef();

    // CTAS path: `CREATE TABLE name AS SELECT ...`. No column list.
    if (p.cur.tag == .kw_as) {
        try p.advance();
        if (p.cur.tag != .kw_select) return PE.SqlExpectedSelect;
        const source = try p.parseStatement();
        return try p.allocOp(.{ .create_table_as = .{
            .table = ref,
            .if_not_exists = if_not_exists,
            .is_temp = is_temp,
            .source = source,
        } });
    }

    try p.expect(.lparen);

    var cols: std.ArrayList(ir.ColumnDef) = .empty;
    defer cols.deinit(p.arena);
    var inline_pk: ?[]const u8 = null;
    var table_pk: ?[]const []const u8 = null;

    while (true) {
        if (p.cur.tag == .kw_primary) {
            try p.advance();
            if (p.cur.tag != .kw_key) return PE.SqlExpectedKeyword;
            try p.advance();
            try p.expect(.lparen);
            table_pk = try p.parseIdentList();
            try p.expect(.rparen);
        } else {
            const col = try parseColumnDef(p);
            try cols.append(p.arena, col.def);
            if (col.is_pk) {
                if (inline_pk != null) return PE.SqlInvalidProjection;
                inline_pk = col.def.name;
            }
        }
        if (p.cur.tag != .comma) break;
        try p.advance();
    }
    try p.expect(.rparen);

    // Non-unique order key: `ORDER BY (col, ...)`. The table gets a sort /
    // clustering key without a uniqueness constraint — inserts append and
    // duplicate key values are kept (vs PRIMARY KEY, which upserts). Exactly
    // one of PRIMARY KEY / ORDER BY may appear.
    var sort_key: ?[]const []const u8 = null;
    if (p.cur.tag == .kw_order) {
        try p.advance();
        if (p.cur.tag != .kw_by) return PE.SqlExpectedKeyword;
        try p.advance();
        try p.expect(.lparen);
        sort_key = try p.parseIdentList();
        try p.expect(.rparen);
    }

    // StarRocks-style trailing options: PROPERTIES ("key" = "value", ...).
    // Recognized keys error on bad values; unknown keys are rejected so a
    // typo'd option never silently no-ops.
    var compression: ?types.TableCompression = null;
    if (p.cur.tag == .identifier and std.ascii.eqlIgnoreCase(p.cur.text, "properties")) {
        try p.advance();
        try p.expect(.lparen);
        while (true) {
            const key = try parsePropertyText(p);
            try p.expect(.eq);
            const value = try parsePropertyText(p);
            if (std.ascii.eqlIgnoreCase(key, "compression")) {
                compression = if (std.ascii.eqlIgnoreCase(value, "none"))
                    .none
                else if (std.ascii.eqlIgnoreCase(value, "zstd"))
                    .zstd
                else if (std.ascii.eqlIgnoreCase(value, "lz4"))
                    .lz4
                else if (std.ascii.eqlIgnoreCase(value, "lz4_fsst"))
                    .lz4_fsst
                else
                    return PE.SqlInvalidProjection;
            } else {
                return PE.SqlInvalidProjection;
            }
            if (p.cur.tag != .comma) break;
            try p.advance();
        }
        try p.expect(.rparen);
    }

    // Tolerate trailing engine-options noise like `ENGINE=...` / `CHARSET=...`
    // by eating any tokens up to EOF or semicolon. Keeps MySQL clients happy.
    while (p.cur.tag != .eof and p.cur.tag != .semicolon) {
        try p.advance();
    }

    if (inline_pk != null and table_pk != null) {
        return PE.SqlInvalidProjection;
    }
    const has_pk = inline_pk != null or table_pk != null;
    // Exactly one key clause: PRIMARY KEY (unique) or ORDER BY (non-unique).
    if (has_pk and sort_key != null) return PE.SqlInvalidProjection;
    const unique = has_pk;
    const order_key: []const []const u8 = if (table_pk) |tpk|
        tpk
    else if (inline_pk) |ipk| blk: {
        const one = try p.arena.alloc([]const u8, 1);
        one[0] = ipk;
        break :blk one;
    } else if (sort_key) |sk|
        sk
    else
        return PE.SqlInvalidProjection;

    const owned_cols = try p.arena.alloc(ir.ColumnDef, cols.items.len);
    for (cols.items, 0..) |c, i| owned_cols[i] = c;

    return try p.allocOp(.{ .ddl = .{ .create_table = .{
        .table = ref,
        .if_not_exists = if_not_exists,
        .is_temp = is_temp,
        .columns = owned_cols,
        .order_key = order_key,
        .unique = unique,
        .compression = compression,
    } } });
}

/// One PROPERTIES key or value. The MySQL dialect lexes `"compression"` as a
/// string literal while PG/neutral lexes it as a quoted identifier — accept
/// both, plus bare identifiers. Returned text borrows the parser arena.
fn parsePropertyText(p: anytype) ![]const u8 {
    const PE = @TypeOf(p.*).Err;
    const text = switch (p.cur.tag) {
        .string => p.cur.value.string,
        .identifier => p.cur.text,
        else => return PE.SqlExpectedIdent,
    };
    const owned = try p.arena.dupe(u8, text);
    try p.advance();
    return owned;
}

pub fn parseDropTableBody(p: anytype) !*ir.Op {
    const PE = @TypeOf(p.*).Err;
    var if_exists = false;
    if (p.cur.tag == .kw_if) {
        try p.advance();
        if (p.cur.tag != .kw_exists) return PE.SqlExpectedKeyword;
        try p.advance();
        if_exists = true;
    }
    const ref = try p.parseTableRef();
    return try p.allocOp(.{ .ddl = .{ .drop_table = .{
        .table = ref,
        .if_exists = if_exists,
    } } });
}

pub fn parseInsert(p: anytype) !*ir.Op {
    return parseInsertLike(p, .insert);
}

pub fn parseReplace(p: anytype) !*ir.Op {
    return parseInsertLike(p, .replace);
}

fn parseInsertLike(p: anytype, mode: ir.InsertMode) !*ir.Op {
    const PE = @TypeOf(p.*).Err;
    try p.advance(); // consume INSERT / REPLACE
    if (p.cur.tag == .kw_into) {
        try p.advance();
    } else if (mode != .replace) {
        return PE.SqlExpectedKeyword;
    }
    const ref = try p.parseTableRef();

    var cols_opt: ?[]const []const u8 = null;
    if (p.cur.tag == .lparen) {
        try p.advance();
        cols_opt = try p.parseIdentList();
        try p.expect(.rparen);
    }

    // INSERT INTO t (cols) SELECT ... — source rows from a query
    // rather than a VALUES list. Parsed before the VALUES branch so
    // SELECT/WITH show up in the same dispatch position.
    if (p.cur.tag == .kw_select or p.cur.tag == .kw_with) {
        const source = try p.parseStatement();
        return try p.allocOp(.{ .insert_select = .{
            .mode = mode,
            .table = ref,
            .columns = cols_opt,
            .source = source,
        } });
    }

    if (p.cur.tag != .kw_values) return PE.SqlExpectedKeyword;
    try p.advance();

    var rows: std.ArrayList([]const ?Value) = .empty;
    defer rows.deinit(p.arena);
    while (true) {
        try p.expect(.lparen);
        var row_vals: std.ArrayList(?Value) = .empty;
        defer row_vals.deinit(p.arena);
        while (true) {
            const v = try parseInsertValue(p);
            try row_vals.append(p.arena, v);
            if (p.cur.tag != .comma) break;
            try p.advance();
        }
        try p.expect(.rparen);
        const row_owned = try p.arena.alloc(?Value, row_vals.items.len);
        for (row_vals.items, 0..) |v, i| row_owned[i] = v;
        try rows.append(p.arena, row_owned);
        if (p.cur.tag != .comma) break;
        try p.advance();
    }
    const rows_owned = try p.arena.alloc([]const ?Value, rows.items.len);
    for (rows.items, 0..) |r, i| rows_owned[i] = r;

    // MySQL upsert clause: `ON DUPLICATE KEY UPDATE col = VALUES(col), ...`.
    // thinDB's INSERT already upserts on a unique table (last-writer-wins) and
    // appends on a non-unique one — exactly this clause's full-row-replace
    // semantics, and the form Flink's JDBC upsert sink emits. So validate that
    // shape and drop it. Partial / expression updates (`c = c + VALUES(c)`)
    // can't be a full replace, so reject them rather than silently mis-apply.
    if (p.cur.tag == .kw_on) {
        try p.advance();
        if (!isIdentText(p, "duplicate")) return PE.SqlExpectedKeyword;
        try p.advance();
        if (p.cur.tag != .kw_key) return PE.SqlExpectedKeyword;
        try p.advance();
        if (p.cur.tag != .kw_update) return PE.SqlExpectedKeyword;
        try p.advance();
        while (true) {
            const target = try p.dupedIdent();
            if (p.cur.tag != .eq) return PE.SqlExpectedKeyword;
            try p.advance();
            if (p.cur.tag != .kw_values) return PE.SqlInvalidProjection;
            try p.advance();
            try p.expect(.lparen);
            const src = try p.dupedIdent();
            try p.expect(.rparen);
            if (!std.ascii.eqlIgnoreCase(target, src)) return PE.SqlInvalidProjection;
            if (p.cur.tag != .comma) break;
            try p.advance();
        }
    }

    return try p.allocOp(.{ .insert = .{
        .mode = mode,
        .table = ref,
        .columns = cols_opt,
        .rows = rows_owned,
    } });
}

/// COPY [db.][schema.]table [(col, ...)] FROM STDIN [WITH (...)]
/// COPY [db.][schema.]table [(col, ...)] TO STDOUT [WITH (...)]
/// File-path forms (`FROM 'path'` / `TO 'path'`) are rejected.
///
/// `TO`, `STDIN`, `STDOUT`, `FORMAT`, `TEXT` are NOT lexer keywords
/// (they collide with column-type names like `TEXT`). We accept
/// them as identifiers and match case-insensitively here.
pub fn parseCopy(p: anytype) !*ir.Op {
    const PE = @TypeOf(p.*).Err;
    try p.advance(); // consume COPY
    const ref = try p.parseTableRef();

    var cols_opt: ?[]const []const u8 = null;
    if (p.cur.tag == .lparen) {
        try p.advance();
        cols_opt = try p.parseIdentList();
        try p.expect(.rparen);
    }

    const direction: ir.CopyOp.Direction = switch (p.cur.tag) {
        .kw_from => .from_stdin,
        .kw_to => .to_stdout,
        .identifier => blk: {
            if (std.ascii.eqlIgnoreCase(p.cur.text, "to")) break :blk .to_stdout;
            return PE.SqlExpectedKeyword;
        },
        else => return PE.SqlExpectedKeyword,
    };
    try p.advance();

    switch (p.cur.tag) {
        .identifier => {
            const text = p.cur.text;
            if (std.ascii.eqlIgnoreCase(text, "stdin")) {
                if (direction != .from_stdin) return PE.SqlExpectedKeyword;
            } else if (std.ascii.eqlIgnoreCase(text, "stdout")) {
                if (direction != .to_stdout) return PE.SqlExpectedKeyword;
            } else return PE.SqlExpectedKeyword;
            try p.advance();
        },
        .string => return PE.SqlCopyFileNotSupported,
        else => return PE.SqlExpectedKeyword,
    }

    if (p.cur.tag == .kw_with) {
        try p.advance();
        try p.expect(.lparen);
        while (true) {
            try parseCopyOption(p);
            if (p.cur.tag != .comma) break;
            try p.advance();
        }
        try p.expect(.rparen);
    }

    return try p.allocOp(.{ .copy = .{
        .direction = direction,
        .table = ref,
        .columns = cols_opt,
    } });
}

fn parseCopyOption(p: anytype) !void {
    const PE = @TypeOf(p.*).Err;
    if (p.cur.tag != .identifier or
        !std.ascii.eqlIgnoreCase(p.cur.text, "format"))
        return PE.SqlCopyUnsupportedFormat;
    try p.advance();
    if (p.cur.tag != .identifier or
        !std.ascii.eqlIgnoreCase(p.cur.text, "text"))
        return PE.SqlCopyUnsupportedFormat;
    try p.advance();
}

pub fn parseColumnDef(p: anytype) !ColDefResult {
    const PE = @TypeOf(p.*).Err;
    if (p.cur.tag != .identifier) return PE.SqlExpectedIdent;
    const name = try p.arena.dupe(u8, p.cur.text);
    try p.advance();

    var nullable = true; // SQL standard default; flipped to false by NOT NULL or PRIMARY KEY
    var auto_increment = false;
    // PG SERIAL family is shorthand for an integer column that
    // auto-increments and is NOT NULL — map it onto AUTO_INCREMENT.
    const ty: types.Type = blk: {
        if (p.cur.tag == .identifier) {
            const serial_ty: ?types.Type =
                if (asciiEqlAny(p.cur.text, &.{ "serial", "serial4" })) .int else if (asciiEqlAny(p.cur.text, &.{ "bigserial", "serial8" })) .bigint else if (asciiEqlAny(p.cur.text, &.{ "smallserial", "serial2" })) .smallint else null;
            if (serial_ty) |st| {
                try p.advance();
                auto_increment = true;
                nullable = false;
                break :blk st;
            }
        }
        break :blk try parseColumnType(p);
    };

    var is_pk = false;
    var saw_not_null = false;
    var default_value: ?types.Value = null;
    while (true) {
        switch (p.cur.tag) {
            .kw_not => {
                try p.advance();
                if (p.cur.tag != .kw_null) return PE.SqlExpectedNull;
                try p.advance();
                nullable = false;
                saw_not_null = true;
            },
            .kw_primary => {
                try p.advance();
                if (p.cur.tag != .kw_key) return PE.SqlExpectedKeyword;
                try p.advance();
                is_pk = true;
                nullable = false;
            },
            .kw_null => {
                if (saw_not_null) return PE.SqlExpectedKeyword;
                try p.advance();
                nullable = true;
            },
            // DEFAULT <literal> — column-level default for omitted-column
            // INSERTs. v1 accepts only literal values (no expressions /
            // function calls). The Value tag's type must match the
            // column type; we don't enforce that here at the parse layer
            // (the compile path validates it once the schema is known).
            .kw_default => {
                try p.advance();
                default_value = try p.parseValue();
            },
            .kw_auto_increment => {
                try p.advance();
                auto_increment = true;
            },
            // GENERATED [ALWAYS | BY DEFAULT] AS IDENTITY [( ... )] — the
            // SQL-standard auto-increment spelling. Mapped onto
            // AUTO_INCREMENT + NOT NULL; any sequence-option parenthesis
            // is consumed and ignored.
            .identifier => {
                if (!std.ascii.eqlIgnoreCase(p.cur.text, "generated")) break;
                try p.advance();
                if (p.cur.tag == .identifier and std.ascii.eqlIgnoreCase(p.cur.text, "always")) {
                    try p.advance();
                } else if (p.cur.tag == .kw_by) {
                    try p.advance();
                    if (!(p.cur.tag == .kw_default)) return PE.SqlExpectedKeyword;
                    try p.advance();
                }
                if (p.cur.tag != .kw_as) return PE.SqlExpectedKeyword;
                try p.advance();
                if (!(p.cur.tag == .identifier and std.ascii.eqlIgnoreCase(p.cur.text, "identity"))) return PE.SqlExpectedKeyword;
                try p.advance();
                if (p.cur.tag == .lparen) {
                    var depth: usize = 0;
                    while (true) {
                        if (p.cur.tag == .lparen) depth += 1
                        else if (p.cur.tag == .rparen) {
                            depth -= 1;
                            if (depth == 0) {
                                try p.advance();
                                break;
                            }
                        } else if (p.cur.tag == .eof) return PE.SqlExpectedToken;
                        try p.advance();
                    }
                }
                auto_increment = true;
                nullable = false;
            },
            else => break,
        }
    }
    return .{
        .def = .{
            .name = name,
            .column_type = ty,
            .nullable = nullable,
            .default_value = default_value,
            .auto_increment = auto_increment,
        },
        .is_pk = is_pk,
    };
}

pub fn parseColumnType(p: anytype) !types.Type {
    const PE = @TypeOf(p.*).Err;
    if (p.cur.tag != .identifier) return PE.SqlExpectedIdent;
    const name = p.cur.text;
    try p.advance();

    // PG type-name aliases (int4/int8/...) sit alongside the standard
    // names so DDL and casts emitted by PG clients/ORMs parse unchanged.
    if (asciiEqlAny(name, &.{ "bigint", "int8" })) return .bigint;
    if (asciiEqlAny(name, &.{ "int", "integer", "int4" })) return .int;
    if (asciiEqlAny(name, &.{ "smallint", "int2" })) return .smallint;
    if (asciiEqlAny(name, &.{"tinyint"})) return .smallint;
    if (asciiEqlAny(name, &.{ "float", "real", "float4" })) return .float;
    if (asciiEqlAny(name, &.{"float8"})) return .double;
    if (asciiEqlAny(name, &.{"double"})) {
        if (p.cur.tag == .identifier and std.ascii.eqlIgnoreCase(p.cur.text, "precision")) {
            try p.advance();
        }
        return .double;
    }
    if (asciiEqlAny(name, &.{ "decimal", "numeric" })) {
        try p.expect(.lparen);
        if (p.cur.tag != .integer) return PE.SqlExpectedValue;
        const p_raw = p.cur.value.integer;
        try p.advance();
        try p.expect(.comma);
        if (p.cur.tag != .integer) return PE.SqlExpectedValue;
        const s_raw = p.cur.value.integer;
        try p.advance();
        try p.expect(.rparen);
        if (p_raw < 1 or p_raw > 38 or s_raw < 0 or s_raw > p_raw) return PE.SqlExpectedValue;
        const p_u: u8 = @intCast(p_raw);
        const s_u: u8 = @intCast(s_raw);
        return if (p_u <= 18)
            types.Type{ .decimal64 = .{ .p = p_u, .s = s_u } }
        else
            types.Type{ .decimal128 = .{ .p = p_u, .s = s_u } };
    }
    if (asciiEqlAny(name, &.{"varchar"})) {
        try p.expect(.lparen);
        if (p.cur.tag != .integer) return PE.SqlExpectedValue;
        const n_raw = p.cur.value.integer;
        try p.advance();
        try p.expect(.rparen);
        if (n_raw < 1) return PE.SqlExpectedValue;
        return types.Type{ .varchar = @intCast(n_raw) };
    }
    if (asciiEqlAny(name, &.{ "text", "string" })) return .string;
    if (asciiEqlAny(name, &.{ "boolean", "bool" })) return .boolean;
    if (asciiEqlAny(name, &.{"date"})) return .date;
    // timestamptz is accepted as a synonym; thinDB datetimes are UTC-naive.
    if (asciiEqlAny(name, &.{ "datetime", "timestamp", "timestamptz" })) return .datetime;
    if (asciiEqlAny(name, &.{"uuid"})) return .uuid;
    if (asciiEqlAny(name, &.{ "json", "jsonb" })) return .json;
    return PE.SqlExpectedKeyword;
}

fn parseInsertValue(p: anytype) !?Value {
    if (p.cur.tag == .kw_null) {
        try p.advance();
        return null;
    }
    return try p.parseValue();
}

pub fn parseShow(p: anytype) !*ir.Op {
    const PE = @TypeOf(p.*).Err;
    try p.advance(); // consume SHOW
    switch (p.cur.tag) {
        .kw_databases => {
            try p.advance();
            return try p.allocOp(.{ .show = .databases });
        },
        .kw_schemas => {
            try p.advance();
            var db: ?[]const u8 = null;
            if (p.cur.tag == .kw_from) {
                try p.advance();
                db = try p.dupedIdent();
            }
            return try p.allocOp(.{ .show = .{ .schemas = db } });
        },
        .kw_tables => {
            try p.advance();
            var ref: ir.TableRef = .{ .name = "" };
            if (p.cur.tag == .kw_from) {
                try p.advance();
                const first = try p.dupedIdent();
                if (p.cur.tag == .dot) {
                    try p.advance();
                    const second = try p.dupedIdent();
                    ref = .{ .database = first, .schema = second, .name = "" };
                } else {
                    ref = .{ .schema = first, .name = "" };
                }
            }
            return try p.allocOp(.{ .show = .{ .tables = ref } });
        },
        .kw_create => {
            try p.advance();
            if (!isIdentText(p, "function")) return PE.SqlExpectedKeyword;
            try p.advance();
            const name = try p.dupedIdent();
            return try p.allocOp(.{ .show = .{ .create_function = name } });
        },
        else => {
            // Contextual: SHOW FUNCTIONS / SHOW FUNCTION STATUS (the real
            // MySQL spelling). FUNCTION/FUNCTIONS are not reserved words.
            if (isIdentText(p, "functions")) {
                try p.advance();
                return try p.allocOp(.{ .show = .functions });
            }
            if (isIdentText(p, "function")) {
                try p.advance();
                if (!isIdentText(p, "status")) return PE.SqlExpectedKeyword;
                try p.advance();
                return try p.allocOp(.{ .show = .functions });
            }
            return PE.SqlExpectedKeyword;
        },
    }
}

fn asciiEqlAny(s: []const u8, candidates: []const []const u8) bool {
    for (candidates) |c| {
        if (std.ascii.eqlIgnoreCase(s, c)) return true;
    }
    return false;
}
