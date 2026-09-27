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

fn parseIfNotExists(p: anytype) !bool {
    const PE = @TypeOf(p.*).Err;
    if (p.cur.tag != .kw_if) return false;
    try p.advance();
    if (p.cur.tag != .kw_not) return PE.SqlExpectedKeyword;
    try p.advance();
    if (p.cur.tag != .kw_exists) return PE.SqlExpectedKeyword;
    try p.advance();
    return true;
}

fn parseIfExists(p: anytype) !bool {
    const PE = @TypeOf(p.*).Err;
    if (p.cur.tag != .kw_if) return false;
    try p.advance();
    if (p.cur.tag != .kw_exists) return PE.SqlExpectedKeyword;
    try p.advance();
    return true;
}

pub fn parseDdl(p: anytype) !*ir.Op {
    const PE = @TypeOf(p.*).Err;
    const head = p.cur.tag;
    try p.advance();
    switch (head) {
        .kw_create => {
            if (p.cur.tag == .kw_database or p.cur.tag == .kw_schema) {
                const is_database = p.cur.tag == .kw_database;
                try p.advance();
                const if_not_exists = try parseIfNotExists(p);
                const ns: ir.CreateNamespace = .{ .name = try p.dupedIdentLower(), .if_not_exists = if_not_exists };
                const d: ir.DdlOp = if (is_database) .{ .create_database = ns } else .{ .create_schema = ns };
                return try p.allocOp(.{ .ddl = d });
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
            if (isBareWord(p, "index") or isBareWord(p, "unique") or isBareWord(p, "fulltext") or isBareWord(p, "spatial")) {
                return try parseCreateIndex(p);
            }
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
            if (p.lex.dialect != .postgres and isBareWord(p, "prepare")) return PE.SqlPrepareExecuteUnsupported;
            if (p.cur.tag == .kw_database or p.cur.tag == .kw_schema) {
                const is_database = p.cur.tag == .kw_database;
                try p.advance();
                const if_exists = try parseIfExists(p);
                const ns: ir.DropNamespace = .{ .name = try p.dupedIdentLower(), .if_exists = if_exists };
                const d: ir.DdlOp = if (is_database) .{ .drop_database = ns } else .{ .drop_schema = ns };
                return try p.allocOp(.{ .ddl = d });
            }
            // `DROP INDEX name ON t`: indexes are never kept (parseCreateIndex).
            if (isBareWord(p, "index")) {
                try p.advance();
                _ = try p.dupedIdent();
                try p.expect(.kw_on);
                const table = try p.parseTableRef();
                try skipToStatementEnd(p);
                return try tableCheckOp(p, table);
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
                const name = try p.dupedIdentLower();
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
                    const name = try p.dupedIdentLower();
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
            const first = try p.dupedIdentLower();
            if (p.cur.tag == .dot) {
                try p.advance();
                const second = try p.dupedIdentLower();
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
            var actions: std.ArrayList(ir.AlterAction) = .empty;
            while (true) {
                if (try parseAlterAction(p)) |action| try actions.append(p.arena, action);
                if (p.cur.tag != .comma) break;
                try p.advance();
            }
            return try p.allocOp(.{ .ddl = .{ .alter_table = .{
                .table = table,
                .actions = try actions.toOwnedSlice(p.arena),
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

/// One `ALTER TABLE` action, in MySQL's spellings:
///   ADD [COLUMN] def | DROP [COLUMN] name | RENAME COLUMN a TO b
///   | CHANGE [COLUMN] old def | MODIFY [COLUMN] def | RENAME [TO | AS] t2
/// Null for an action that changes nothing thinDB keeps: an index or
/// constraint added, dropped or renamed (see parseTableConstraint), a table
/// option, `ENABLE` / `DISABLE KEYS`, `FORCE`.
fn parseAlterAction(p: anytype) !?ir.AlterAction {
    const PE = @TypeOf(p.*).Err;
    if (try skipTableOption(p)) return null;
    switch (p.cur.tag) {
        .kw_add => {
            try p.advance();
            if (try parseTableConstraint(p)) |c| return switch (c) {
                .primary_key => PE.SqlInvalidProjection,
                .ignored => null,
            };
            if (p.cur.tag == .kw_column) try p.advance();
            const col = try parseColumnDef(p);
            if (col.is_pk or col.def.auto_increment) return PE.SqlInvalidProjection;
            try rejectColumnPosition(p);
            return .{ .add_column = col.def };
        },
        .kw_drop => {
            try p.advance();
            if (isBareWord(p, "foreign")) {
                try p.advance();
                try p.expect(.kw_key);
                _ = try p.dupedIdent();
                return null;
            }
            if (p.cur.tag == .kw_key or isBareWord(p, "index") or isBareWord(p, "check") or isBareWord(p, "constraint")) {
                try p.advance();
                _ = try p.dupedIdent();
                return null;
            }
            if (p.cur.tag == .kw_primary) return PE.SqlInvalidProjection;
            if (p.cur.tag == .kw_column) try p.advance();
            return .{ .drop_column = try p.dupedIdent() };
        },
        .kw_rename => {
            try p.advance();
            if (p.cur.tag == .kw_key or isBareWord(p, "index")) {
                try p.advance();
                _ = try p.dupedIdent();
                try p.expect(.kw_to);
                _ = try p.dupedIdent();
                return null;
            }
            if (p.cur.tag == .kw_column) {
                try p.advance();
                const from = try p.dupedIdent();
                try p.expect(.kw_to);
                return .{ .rename_column = .{ .from = from, .to = try p.dupedIdent() } };
            }
            if (p.cur.tag == .kw_to or p.cur.tag == .kw_as) try p.advance();
            return .{ .rename_table = try p.parseTableRef() };
        },
        // `ALTER INDEX name VISIBLE | INVISIBLE`.
        .kw_alter => {
            try p.advance();
            if (!isBareWord(p, "index")) return PE.SqlExpectedKeyword;
            try p.advance();
            _ = try p.dupedIdent();
            if (!isBareWord(p, "visible") and !isBareWord(p, "invisible")) return PE.SqlExpectedKeyword;
            try p.advance();
            return null;
        },
        else => {},
    }
    if (isBareWord(p, "enable") or isBareWord(p, "disable")) {
        try p.advance();
        if (!isBareWord(p, "keys")) return PE.SqlExpectedKeyword;
        try p.advance();
        return null;
    }
    if (isBareWord(p, "force")) {
        try p.advance();
        return null;
    }
    const change = isIdentText(p, "change");
    if (!change and !isIdentText(p, "modify")) return PE.SqlExpectedKeyword;
    try p.advance();
    if (p.cur.tag == .kw_column) try p.advance();
    const from: ?[]const u8 = if (change) try p.dupedIdent() else null;
    const col = try parseColumnDef(p);
    if (col.is_pk) return PE.SqlInvalidProjection;
    try rejectColumnPosition(p);
    return .{ .change_column = .{ .from = from orelse col.def.name, .column = col.def } };
}

fn rejectColumnPosition(p: anytype) !void {
    const PE = @TypeOf(p.*).Err;
    if (isIdentText(p, "first") or isIdentText(p, "after")) return PE.SqlColumnPositionUnsupported;
}

fn isIdentText(p: anytype, comptime text: []const u8) bool {
    return p.cur.tag == .identifier and std.ascii.eqlIgnoreCase(p.cur.text, text);
}

/// The unquoted word `text`. MySQL reserves the clause words (`INDEX`,
/// `UNIQUE`, `CHECK`, ...), so only a quoted one can name a column.
fn isBareWord(p: anytype, comptime text: []const u8) bool {
    return isIdentText(p, text) and !p.cur.quoted;
}

fn isIndexHead(p: anytype) bool {
    return p.cur.tag == .kw_key or isBareWord(p, "index") or isBareWord(p, "fulltext") or isBareWord(p, "spatial");
}

fn isConstraintHead(p: anytype) bool {
    return isBareWord(p, "unique") or isBareWord(p, "foreign") or isBareWord(p, "check");
}

const TableConstraint = union(enum) {
    /// `[CONSTRAINT name] PRIMARY KEY (cols)`.
    primary_key: []const []const u8,
    /// An index or an unenforced constraint: accepted, not kept.
    ignored,
};

/// A table-level index or constraint in a CREATE TABLE column list or after
/// ALTER TABLE ADD; null when the item at the cursor is a column.
///
/// Secondary indexes are advisory to a columnar scan, so `KEY` / `INDEX` /
/// `FULLTEXT` / `SPATIAL` are accepted and dropped. `UNIQUE`, `FOREIGN KEY`
/// and `CHECK` are accepted as informational and not enforced, as analytics
/// warehouses (Snowflake, Redshift, BigQuery) do: they hold in the OLTP
/// database the data is loaded from. Only PRIMARY KEY is kept.
fn parseTableConstraint(p: anytype) !?TableConstraint {
    const PE = @TypeOf(p.*).Err;
    if (isBareWord(p, "constraint")) {
        try p.advance();
        if (p.cur.tag == .identifier and !isConstraintHead(p)) try p.advance();
        if (p.cur.tag == .kw_primary) return .{ .primary_key = try parsePrimaryKeyClause(p) };
        if (!isConstraintHead(p)) return PE.SqlExpectedKeyword;
    } else if (p.cur.tag == .kw_primary) {
        return .{ .primary_key = try parsePrimaryKeyClause(p) };
    } else if (!isConstraintHead(p) and !isIndexHead(p)) return null;
    try p.advance();
    try skipToItemEnd(p);
    return .ignored;
}

/// `PRIMARY KEY [USING type] (cols) [index options]`.
fn parsePrimaryKeyClause(p: anytype) ![]const []const u8 {
    try p.advance();
    try p.expect(.kw_key);
    if (isBareWord(p, "using")) {
        try p.advance();
        try p.advance();
    }
    try p.expect(.lparen);
    const cols = try p.parseIdentList();
    try p.expect(.rparen);
    try skipToItemEnd(p);
    return cols;
}

/// Consumes the rest of the current comma-separated item, up to the next
/// `,` or `)` outside parentheses (not consumed) or the statement's end.
fn skipToItemEnd(p: anytype) !void {
    const PE = @TypeOf(p.*).Err;
    var depth: usize = 0;
    while (true) {
        switch (p.cur.tag) {
            .lparen => depth += 1,
            .rparen => {
                if (depth == 0) return;
                depth -= 1;
            },
            .comma => if (depth == 0) return,
            .eof, .semicolon => return if (depth == 0) {} else PE.SqlExpectedToken,
            else => {},
        }
        try p.advance();
    }
}

fn skipToStatementEnd(p: anytype) !void {
    while (p.cur.tag != .eof and p.cur.tag != .semicolon) try p.advance();
}

/// A parenthesized group, parentheses included.
fn skipParenGroup(p: anytype) !void {
    const PE = @TypeOf(p.*).Err;
    try p.expect(.lparen);
    var depth: usize = 1;
    while (depth > 0) {
        switch (p.cur.tag) {
            .lparen => depth += 1,
            .rparen => depth -= 1,
            .eof, .semicolon => return PE.SqlExpectedToken,
            else => {},
        }
        try p.advance();
    }
}

/// A column-level `[CONSTRAINT name] CHECK (expr) [[NOT] ENFORCED]` or
/// `REFERENCES t [(cols)] [MATCH m] [ON DELETE | ON UPDATE action]...`:
/// informational, like the table-level forms (parseTableConstraint).
fn skipColumnConstraint(p: anytype) !void {
    const PE = @TypeOf(p.*).Err;
    if (isBareWord(p, "constraint")) {
        try p.advance();
        if (p.cur.tag == .identifier and !isBareWord(p, "check")) try p.advance();
        if (!isBareWord(p, "check")) return PE.SqlExpectedKeyword;
    }
    if (isBareWord(p, "check")) {
        try p.advance();
        try skipParenGroup(p);
        if (isBareWord(p, "enforced")) {
            try p.advance();
        } else if (p.cur.tag == .kw_not) {
            var look = p.lex.*;
            const next = try look.next();
            if (next.tag == .identifier and std.ascii.eqlIgnoreCase(next.text, "enforced")) {
                try p.advance();
                try p.advance();
            }
        }
        return;
    }
    try p.advance(); // REFERENCES
    _ = try p.parseTableRef();
    if (p.cur.tag == .lparen) try skipParenGroup(p);
    if (isBareWord(p, "match")) {
        try p.advance();
        try p.advance();
    }
    while (p.cur.tag == .kw_on) {
        try p.advance();
        if (p.cur.tag != .kw_delete and p.cur.tag != .kw_update) return PE.SqlExpectedKeyword;
        try p.advance();
        if (isBareWord(p, "restrict") or isBareWord(p, "cascade")) {
            try p.advance();
        } else if (p.cur.tag == .kw_set) {
            try p.advance();
            if (p.cur.tag != .kw_null and p.cur.tag != .kw_default) return PE.SqlExpectedKeyword;
            try p.advance();
        } else if (isBareWord(p, "no")) {
            try p.advance();
            if (!isBareWord(p, "action")) return PE.SqlExpectedKeyword;
            try p.advance();
        } else return PE.SqlExpectedKeyword;
    }
}

/// A MySQL table option as an ALTER TABLE action: storage engine, character
/// set, comment, row format, and the online-DDL `ALGORITHM` / `LOCK`
/// clauses. None has a single-node columnar meaning; each is accepted and
/// dropped. `AUTO_INCREMENT = n` is dropped too: new ids continue past the
/// table's largest, so they stay unique.
fn skipTableOption(p: anytype) !bool {
    const PE = @TypeOf(p.*).Err;
    if (isBareWord(p, "convert")) {
        // CONVERT TO CHARACTER SET name [COLLATE name]
        try p.advance();
        try p.expect(.kw_to);
        if (!isBareWord(p, "character")) return PE.SqlExpectedKeyword;
        try p.advance();
        try p.expect(.kw_set);
        try p.advance();
        if (isBareWord(p, "collate")) {
            try p.advance();
            try p.advance();
        }
        return true;
    }
    const had_default = p.cur.tag == .kw_default;
    if (had_default) try p.advance();
    if (isBareWord(p, "character")) {
        try p.advance();
        try p.expect(.kw_set);
    } else if (isBareWord(p, "charset") or isBareWord(p, "collate")) {
        try p.advance();
    } else if (had_default) {
        return PE.SqlExpectedKeyword;
    } else if (p.cur.tag == .kw_auto_increment or isTableOptionWord(p)) {
        try p.advance();
    } else return false;
    if (p.cur.tag == .eq) try p.advance();
    switch (p.cur.tag) {
        .identifier, .string, .integer, .kw_default => try p.advance(),
        else => return PE.SqlExpectedValue,
    }
    return true;
}

fn isTableOptionWord(p: anytype) bool {
    if (p.cur.tag != .identifier or p.cur.quoted) return false;
    return asciiEqlAny(p.cur.text, &.{
        "comment",            "engine",         "row_format",       "algorithm",
        "lock",               "key_block_size", "stats_persistent", "stats_auto_recalc",
        "stats_sample_pages", "checksum",       "pack_keys",        "avg_row_length",
        "min_rows",           "max_rows",       "delay_key_write",  "insert_method",
    });
}

/// `CREATE [UNIQUE | FULLTEXT | SPATIAL] INDEX name ON t (key parts) ...`.
/// A secondary index is advisory and a UNIQUE one an unenforced constraint
/// (parseTableConstraint), so the statement only checks that t exists.
fn parseCreateIndex(p: anytype) !*ir.Op {
    const PE = @TypeOf(p.*).Err;
    if (!isBareWord(p, "index")) try p.advance();
    if (!isBareWord(p, "index")) return PE.SqlExpectedKeyword;
    try p.advance();
    while (p.cur.tag != .kw_on) {
        if (p.cur.tag == .eof or p.cur.tag == .semicolon) return PE.SqlExpectedKeyword;
        try p.advance();
    }
    try p.advance();
    const table = try p.parseTableRef();
    try skipToStatementEnd(p);
    return try tableCheckOp(p, table);
}

/// An ALTER TABLE with no actions: fails when the table doesn't exist and
/// otherwise changes nothing.
fn tableCheckOp(p: anytype, table: ir.TableRef) !*ir.Op {
    return try p.allocOp(.{ .ddl = .{ .alter_table = .{ .table = table, .actions = &.{} } } });
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
    const name = try p.dupedIdentLower();

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
    const name = try p.dupedIdentLower();
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
    const name = try p.dupedIdentLower();
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

    // `CREATE TABLE t2 LIKE t1`, or MySQL's `(LIKE t1)`.
    const like_in_parens = p.cur.tag == .lparen and blk: {
        var look = p.lex.*;
        break :blk (try look.next()).tag == .kw_like;
    };
    if (p.cur.tag == .kw_like or like_in_parens) {
        if (like_in_parens) try p.advance();
        try p.advance();
        const source = try p.parseTableRef();
        if (like_in_parens) try p.expect(.rparen);
        return try p.allocOp(.{ .ddl = .{ .create_table = .{
            .table = ref,
            .if_not_exists = if_not_exists,
            .is_temp = is_temp,
            .columns = &.{},
            .order_key = &.{},
            .like = source,
        } } });
    }

    // CTAS path: `CREATE TABLE name AS SELECT ...`. No column list.
    if (p.cur.tag == .kw_as) {
        try p.advance();
        if (!p.startsQuery(p.cur.tag)) return PE.SqlExpectedSelect;
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
        if (try parseTableConstraint(p)) |c| switch (c) {
            .primary_key => |key_cols| {
                if (table_pk != null) return PE.SqlInvalidProjection;
                table_pk = key_cols;
            },
            .ignored => {},
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
    //
    // StarRocks dialect: `DISTRIBUTED BY HASH (col, ...) [BUCKETS n]` also
    // parses (either order relative to ORDER BY, each at most once). The
    // hash columns serve as a NON-unique order key only when no real key
    // clause was given — SR's duplicate-key default keeps duplicates, so
    // implying PRIMARY KEY here would silently dedupe. BUCKETS is a
    // distribution detail with no single-node meaning; parsed and ignored.
    var sort_key: ?[]const []const u8 = null;
    var dist_key: ?[]const []const u8 = null;
    while (true) {
        if (p.cur.tag == .kw_order and sort_key == null) {
            try p.advance();
            if (p.cur.tag != .kw_by) return PE.SqlExpectedKeyword;
            try p.advance();
            try p.expect(.lparen);
            sort_key = try p.parseIdentList();
            try p.expect(.rparen);
        } else if (p.cur.tag == .identifier and dist_key == null and
            std.ascii.eqlIgnoreCase(p.cur.text, "distributed"))
        {
            try p.advance();
            if (p.cur.tag != .kw_by) return PE.SqlExpectedKeyword;
            try p.advance();
            if (!(p.cur.tag == .identifier and std.ascii.eqlIgnoreCase(p.cur.text, "hash"))) {
                return PE.SqlExpectedKeyword;
            }
            try p.advance();
            try p.expect(.lparen);
            dist_key = try p.parseIdentList();
            try p.expect(.rparen);
            if (p.cur.tag == .identifier and std.ascii.eqlIgnoreCase(p.cur.text, "buckets")) {
                try p.advance();
                if (p.cur.tag != .integer) return PE.SqlExpectedKeyword;
                try p.advance();
            }
        } else if (p.cur.tag == .identifier and std.ascii.eqlIgnoreCase(p.cur.text, "engine")) {
            // MySQL / StarRocks `ENGINE = name`: no single-node meaning.
            try p.advance();
            try p.expect(.eq);
            if (p.cur.tag != .identifier) return PE.SqlExpectedIdent;
            try p.advance();
        } else if (p.cur.tag == .kw_primary) {
            // StarRocks places the key clause AFTER the column list.
            if (table_pk != null or inline_pk != null) return PE.SqlInvalidProjection;
            try p.advance();
            if (p.cur.tag != .kw_key) return PE.SqlExpectedKeyword;
            try p.advance();
            try p.expect(.lparen);
            table_pk = try p.parseIdentList();
            try p.expect(.rparen);
        } else if (p.cur.tag == .identifier and asciiEqlAny(p.cur.text, &.{ "unique", "duplicate" })) {
            // StarRocks `UNIQUE KEY (cols)` upserts like PRIMARY KEY;
            // `DUPLICATE KEY (cols)` is a non-unique sort key.
            const is_unique = std.ascii.eqlIgnoreCase(p.cur.text, "unique");
            try p.advance();
            if (p.cur.tag != .kw_key) return PE.SqlExpectedKeyword;
            try p.advance();
            try p.expect(.lparen);
            const key_cols = try p.parseIdentList();
            try p.expect(.rparen);
            if (is_unique) {
                if (table_pk != null or inline_pk != null) return PE.SqlInvalidProjection;
                table_pk = key_cols;
            } else {
                if (sort_key != null) return PE.SqlInvalidProjection;
                sort_key = key_cols;
            }
        } else if (p.cur.tag == .identifier and std.ascii.eqlIgnoreCase(p.cur.text, "comment")) {
            // Table COMMENT [=] 'text': accepted, not stored.
            try p.advance();
            if (p.cur.tag == .eq) try p.advance();
            _ = try parsePropertyText(p);
        } else break;
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
            } else if (std.ascii.eqlIgnoreCase(key, "replication_num") or std.ascii.eqlIgnoreCase(key, "storage_medium")) {
                // StarRocks replication factor / storage tier — no
                // single-node meaning. Accepted so SR-dialect DDL runs
                // verbatim; values ignored.
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
    // At most one key clause: PRIMARY KEY (unique) or ORDER BY (non-unique).
    // With neither, the table orders by its first column, as CTAS does.
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
    else if (dist_key) |dk|
        dk
    else if (cols.items.len > 0) blk: {
        const one = try p.arena.alloc([]const u8, 1);
        one[0] = cols.items[0].name;
        break :blk one;
    } else return PE.SqlInvalidProjection;

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
    var tables: std.ArrayList(ir.TableRef) = .empty;
    while (true) {
        try tables.append(p.arena, try p.parseTableRef());
        if (p.cur.tag != .comma) break;
        try p.advance();
    }
    // MySQL accepts and ignores RESTRICT / CASCADE.
    if (isIdentText(p, "restrict") or isIdentText(p, "cascade")) try p.advance();
    return try p.allocOp(.{ .ddl = .{ .drop_table = .{
        .tables = try tables.toOwnedSlice(p.arena),
        .if_exists = if_exists,
    } } });
}

pub fn parseInsert(p: anytype) !*ir.Op {
    return parseInsertLike(p, .insert);
}

pub fn parseReplace(p: anytype) !*ir.Op {
    return parseInsertLike(p, .replace);
}

fn parseInsertLike(p: anytype, mode_in: ir.InsertMode) !*ir.Op {
    const PE = @TypeOf(p.*).Err;
    try p.advance(); // consume INSERT / REPLACE
    var mode = mode_in;
    // MySQL's scheduling modifiers mean nothing to a single-writer table.
    while (p.cur.tag == .identifier and asciiEqlAny(p.cur.text, &.{ "low_priority", "delayed", "high_priority" })) try p.advance();
    if (mode == .insert and p.cur.tag == .kw_ignore) {
        mode = .ignore;
        try p.advance();
    }
    if (p.cur.tag == .kw_into) try p.advance();
    const ref = try p.parseTableRef();

    var cols_opt: ?[]const []const u8 = null;
    if (p.cur.tag == .lparen) {
        try p.advance();
        cols_opt = try p.parseIdentList();
        try p.expect(.rparen);
    }

    // INSERT INTO t (cols) SELECT ... — source rows from a query
    // rather than a VALUES list. Parsed before the VALUES branch so
    // SELECT/WITH/TABLE show up in the same dispatch position.
    if (p.cur.tag != .kw_values and p.startsQuery(p.cur.tag)) {
        const source = try p.parseStatement();
        return try p.allocOp(.{ .insert_select = .{
            .mode = mode,
            .table = ref,
            .columns = cols_opt,
            .source = source,
            .on_duplicate = try parseOnDuplicate(p, mode, null, null),
        } });
    }

    var rows: InsertRows = .{};
    if (p.cur.tag == .kw_set) {
        if (cols_opt != null) return PE.SqlExpectedKeyword;
        try p.advance();
        var names: std.ArrayList([]const u8) = .empty;
        while (true) {
            try names.append(p.arena, try p.dupedIdent());
            if (p.cur.tag != .eq) return PE.SqlExpectedToken;
            try p.advance();
            try rows.cell(p);
            if (p.cur.tag != .comma) break;
            try p.advance();
        }
        try rows.endRow(p);
        return try rows.finish(p, mode, ref, names.items, try parseOnDuplicate(p, mode, null, null));
    }

    if (p.cur.tag != .kw_values and !isIdentText(p, "value")) return PE.SqlExpectedKeyword;
    try p.advance();
    while (true) {
        if (p.cur.tag == .kw_row) try p.advance();
        try p.expect(.lparen);
        while (true) {
            try rows.cell(p);
            if (p.cur.tag != .comma) break;
            try p.advance();
        }
        try p.expect(.rparen);
        try rows.endRow(p);
        if (p.cur.tag != .comma) break;
        try p.advance();
    }

    var row_alias: ?[]const u8 = null;
    var row_alias_columns: ?[]const []const u8 = null;
    if (p.cur.tag == .kw_as) {
        try p.advance();
        row_alias = try p.dupedIdent();
        if (p.cur.tag == .lparen) {
            try p.advance();
            row_alias_columns = try p.parseIdentList();
            try p.expect(.rparen);
        }
    }
    return try rows.finish(p, mode, ref, cols_opt, try parseOnDuplicate(p, mode, row_alias, row_alias_columns));
}

/// An INSERT's rows as parsed. Literal rows stay Values, the path bulk
/// loaders take. The first cell that isn't a lone literal turns every row
/// into expressions.
const InsertRows = struct {
    literal: std.ArrayList([]const ?Value) = .empty,
    exprs: std.ArrayList([]const ir.Expr) = .empty,
    as_exprs: bool = false,
    row_vals: std.ArrayList(?Value) = .empty,
    cells: std.ArrayList(ir.Expr) = .empty,
    width: ?usize = null,

    fn cell(self: *InsertRows, p: anytype) !void {
        const literal = try literalCellAhead(p);
        if (!literal and !self.as_exprs) {
            self.as_exprs = true;
            for (self.literal.items) |row| try self.exprs.append(p.arena, try literalExprs(p, row));
            for (self.row_vals.items) |v| try self.cells.append(p.arena, literalExpr(v));
        }
        if (!self.as_exprs) {
            try self.row_vals.append(p.arena, try parseInsertValue(p));
        } else {
            try self.cells.append(p.arena, if (literal) literalExpr(try parseInsertValue(p)) else try p.parseValueExpr());
        }
    }

    fn endRow(self: *InsertRows, p: anytype) !void {
        const PE = @TypeOf(p.*).Err;
        const row_width = if (self.as_exprs) self.cells.items.len else self.row_vals.items.len;
        if (row_width != (self.width orelse row_width)) return PE.SqlRowValueWidthMismatch;
        self.width = row_width;
        if (self.as_exprs) {
            try self.exprs.append(p.arena, try p.arena.dupe(ir.Expr, self.cells.items));
            self.cells.clearRetainingCapacity();
        } else {
            try self.literal.append(p.arena, self.row_vals.items);
        }
        self.row_vals = .empty;
    }

    fn finish(
        self: *InsertRows,
        p: anytype,
        mode: ir.InsertMode,
        ref: ir.TableRef,
        columns: ?[]const []const u8,
        on_duplicate: ?ir.OnDuplicate,
    ) !*ir.Op {
        if (self.as_exprs) return try p.allocOp(.{ .insert_select = .{
            .mode = mode,
            .table = ref,
            .columns = columns,
            .source = try valuesQuery(p, self.exprs.items, try valueColumnNames(p, self.width.?)),
            .on_duplicate = on_duplicate,
        } });
        return try p.allocOp(.{ .insert = .{
            .mode = mode,
            .table = ref,
            .columns = columns,
            .rows = self.literal.items,
            .on_duplicate = on_duplicate,
        } });
    }
};

/// `ON DUPLICATE KEY UPDATE col = expr [, ...]`, or null when the INSERT has
/// none. Compile decides what the assignments amount to, since that turns on
/// the table's key.
fn parseOnDuplicate(
    p: anytype,
    mode: ir.InsertMode,
    row_alias: ?[]const u8,
    row_alias_columns: ?[]const []const u8,
) !?ir.OnDuplicate {
    const PE = @TypeOf(p.*).Err;
    if (p.cur.tag != .kw_on or mode == .replace) return null;
    try p.advance();
    if (!isIdentText(p, "duplicate")) return PE.SqlExpectedKeyword;
    try p.advance();
    if (p.cur.tag != .kw_key) return PE.SqlExpectedKeyword;
    try p.advance();
    if (p.cur.tag != .kw_update) return PE.SqlExpectedKeyword;
    try p.advance();

    const outer_values_refs = p.insert_values_refs;
    p.insert_values_refs = true;
    defer p.insert_values_refs = outer_values_refs;
    var assignments: std.ArrayList(ir.Assignment) = .empty;
    while (true) {
        var col = try p.dupedIdent();
        // A qualified target can only name the insert's own table.
        while (p.cur.tag == .dot) {
            try p.advance();
            col = try p.dupedIdent();
        }
        if (p.cur.tag != .eq) return PE.SqlExpectedToken;
        try p.advance();
        try assignments.append(p.arena, .{ .col = col, .value = try p.parseValueExpr() });
        if (p.cur.tag != .comma) break;
        try p.advance();
    }
    return .{
        .assignments = assignments.items,
        .row_alias = row_alias,
        .row_alias_columns = row_alias_columns,
    };
}

/// Whether the VALUES cell at the cursor is a lone literal: NULL, TRUE,
/// FALSE, a string, a number with an optional sign, or a DATE, DATETIME or
/// TIMESTAMP string.
fn literalCellAhead(p: anytype) !bool {
    var look = p.lex.*;
    var tok = p.cur;
    switch (tok.tag) {
        .kw_null, .kw_true, .kw_false, .string, .integer, .floating => {},
        .plus, .minus => {
            tok = try look.next();
            if (tok.tag != .integer and tok.tag != .floating) return false;
        },
        .identifier => {
            if (!asciiEqlAny(tok.text, &.{ "date", "datetime", "timestamp" })) return false;
            tok = try look.next();
            if (tok.tag != .string) return false;
        },
        else => return false,
    }
    const after = try look.next();
    return after.tag == .comma or after.tag == .rparen;
}

fn literalExpr(v: ?Value) ir.Expr {
    return if (v) |lit| .{ .lit = lit } else .{ .null_lit = .string };
}

fn literalExprs(p: anytype, row: []const ?Value) ![]const ir.Expr {
    const out = try p.arena.alloc(ir.Expr, row.len);
    for (row, out) |v, *e| e.* = literalExpr(v);
    return out;
}

fn valueColumnNames(p: anytype, width: usize) ![]const []const u8 {
    const names = try p.arena.alloc([]const u8, width);
    for (names, 0..) |*name, i| name.* = try std.fmt.allocPrint(p.arena, "__value_{d}", .{i});
    return names;
}

/// The rows of a VALUES list as a query: each row a FROM-less SELECT
/// naming its cells `names`, all of them a UNION ALL, balanced so a long
/// list nests only log2(rows) deep.
pub fn valuesQuery(p: anytype, rows: []const []const ir.Expr, names: []const []const u8) !*ir.Op {
    if (rows.len > 1) {
        const half = rows.len / 2;
        return try p.allocOp(.{ .set_union = .{
            .left = try valuesQuery(p, rows[0..half], names),
            .right = try valuesQuery(p, rows[half..], names),
            .all = true,
        } });
    }
    const row = rows[0];
    const derived = try p.arena.alloc(ir.Derived, row.len);
    for (row, derived, names) |e, *d, name| d.* = .{ .name = name, .expr = e };
    const single = try p.allocOp(.{ .single_row = {} });
    const compute = try p.allocOp(.{ .compute = .{ .derived = derived, .upstream = single } });
    return try p.allocOp(.{ .select = .{ .columns = names, .upstream = compute } });
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
    var default_now = false;
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
                if (p.cur.tag == .identifier and asciiEqlAny(p.cur.text, &.{ "current_timestamp", "now", "localtimestamp", "localtime" })) {
                    try p.advance();
                    if (p.cur.tag == .lparen) {
                        try p.advance();
                        if (p.cur.tag == .integer) {
                            if (p.cur.value.integer > 6) return PE.SqlExpectedValue;
                            try p.advance();
                        }
                        try p.expect(.rparen);
                    }
                    default_now = true;
                } else {
                    default_value = try p.parseValue();
                }
            },
            .kw_auto_increment => {
                try p.advance();
                auto_increment = true;
            },
            // MySQL: in a column definition, KEY alone means PRIMARY KEY.
            .kw_key => {
                try p.advance();
                is_pk = true;
                nullable = false;
            },
            // GENERATED [ALWAYS | BY DEFAULT] AS IDENTITY [( ... )] — the
            // SQL-standard auto-increment spelling. Mapped onto
            // AUTO_INCREMENT + NOT NULL; any sequence-option parenthesis
            // is consumed and ignored.
            .identifier => {
                // MySQL / StarRocks column COMMENT: accepted, not stored.
                if (std.ascii.eqlIgnoreCase(p.cur.text, "comment")) {
                    try p.advance();
                    _ = try parsePropertyText(p);
                    continue;
                }
                // MySQL CHARACTER SET / CHARSET / COLLATE: accepted, not
                // stored; text is always UTF-8 compared bytewise.
                if (asciiEqlAny(p.cur.text, &.{ "character", "charset", "collate" })) {
                    const is_character = std.ascii.eqlIgnoreCase(p.cur.text, "character");
                    try p.advance();
                    if (is_character) try p.expect(.kw_set);
                    if (p.cur.tag != .identifier and p.cur.tag != .string) return PE.SqlExpectedIdent;
                    try p.advance();
                    continue;
                }
                // Column-level UNIQUE, CHECK and REFERENCES: informational,
                // like the table-level forms (parseTableConstraint).
                if (isBareWord(p, "unique")) {
                    try p.advance();
                    if (p.cur.tag == .kw_key) try p.advance();
                    continue;
                }
                if (isBareWord(p, "check") or isBareWord(p, "constraint") or isBareWord(p, "references")) {
                    try skipColumnConstraint(p);
                    continue;
                }
                // NDB storage hints.
                if (isBareWord(p, "column_format") or isBareWord(p, "storage")) {
                    try p.advance();
                    try p.advance();
                    continue;
                }
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
                        if (p.cur.tag == .lparen) depth += 1 else if (p.cur.tag == .rparen) {
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
            .default_now = default_now,
            .auto_increment = auto_increment,
        },
        .is_pk = is_pk,
    };
}

/// MySQL integer display width (`int(11)`) and the SIGNED / UNSIGNED /
/// ZEROFILL modifiers after it. The width is a formatting hint with no
/// storage meaning; UNSIGNED picks `unsigned_ty`.
fn integerType(p: anytype, signed_ty: types.Type, unsigned_ty: types.Type) !types.Type {
    _ = try optionalLength(p);
    const unsigned = p.cur.tag == .identifier and std.ascii.eqlIgnoreCase(p.cur.text, "unsigned");
    try skipIntegerModifiers(p);
    return if (unsigned) unsigned_ty else signed_ty;
}

fn skipIntegerModifiers(p: anytype) !void {
    while (p.cur.tag == .identifier and asciiEqlAny(p.cur.text, &.{ "signed", "unsigned", "zerofill" })) {
        try p.advance();
    }
}

/// MySQL's FLOAT(p) / DOUBLE(m, d) precision hints and UNSIGNED: accepted,
/// storage stays IEEE.
fn numericModifiers(p: anytype, ty: types.Type) !types.Type {
    const PE = @TypeOf(p.*).Err;
    if (p.cur.tag == .lparen) {
        try p.advance();
        if (p.cur.tag != .integer) return PE.SqlExpectedValue;
        try p.advance();
        if (p.cur.tag == .comma) {
            try p.advance();
            if (p.cur.tag != .integer) return PE.SqlExpectedValue;
            try p.advance();
        }
        try p.expect(.rparen);
    }
    try skipIntegerModifiers(p);
    return ty;
}

/// `(n)` after a type name, n >= 1.
fn optionalLength(p: anytype) !?u32 {
    const PE = @TypeOf(p.*).Err;
    if (p.cur.tag != .lparen) return null;
    try p.advance();
    if (p.cur.tag != .integer) return PE.SqlExpectedValue;
    const n_raw = p.cur.value.integer;
    try p.advance();
    try p.expect(.rparen);
    if (n_raw < 1 or n_raw > std.math.maxInt(u32)) return PE.SqlExpectedValue;
    return @intCast(n_raw);
}

/// VARCHAR(n); without a length (PostgreSQL) it is unbounded text.
fn varcharType(p: anytype) !types.Type {
    const n = try optionalLength(p) orelse return .string;
    return types.Type{ .varchar = n };
}

/// ENUM / SET's label list, `('a', 'b', ...)`.
fn skipLabelList(p: anytype) !void {
    const PE = @TypeOf(p.*).Err;
    try p.expect(.lparen);
    while (true) {
        if (p.cur.tag != .string) return PE.SqlExpectedValue;
        try p.advance();
        if (p.cur.tag != .comma) break;
        try p.advance();
    }
    try p.expect(.rparen);
}

/// A fractional-seconds precision, `(0)` to `(6)`. Storage is always
/// microseconds, so it is range-checked and otherwise ignored.
fn skipFsp(p: anytype) !void {
    const PE = @TypeOf(p.*).Err;
    if (p.cur.tag != .lparen) return;
    try p.advance();
    if (p.cur.tag != .integer) return PE.SqlExpectedValue;
    const fsp = p.cur.value.integer;
    try p.advance();
    try p.expect(.rparen);
    if (fsp < 0 or fsp > 6) return PE.SqlExpectedValue;
}

pub fn parseColumnType(p: anytype) !types.Type {
    const PE = @TypeOf(p.*).Err;
    // SET('a', 'b', ...) stores its labels as comma-joined text, like ENUM.
    if (p.cur.tag == .kw_set) {
        try p.advance();
        try skipLabelList(p);
        return .string;
    }
    if (p.cur.tag != .identifier) return PE.SqlExpectedIdent;
    const name = p.cur.text;
    try p.advance();

    // PG type-name aliases (int4/int8/...) sit alongside the standard
    // names so DDL and casts emitted by PG clients/ORMs parse unchanged.
    // An UNSIGNED integer widens to the next type that holds its range,
    // except BIGINT UNSIGNED, which keeps 64 bits.
    if (asciiEqlAny(name, &.{ "bigint", "int8" })) return integerType(p, .bigint, .bigint);
    if (asciiEqlAny(name, &.{ "int", "integer", "int4" })) return integerType(p, .int, .bigint);
    if (asciiEqlAny(name, &.{ "mediumint", "middleint", "int3" })) return integerType(p, .int, .int);
    if (asciiEqlAny(name, &.{ "smallint", "int2" })) return integerType(p, .smallint, .int);
    if (asciiEqlAny(name, &.{ "tinyint", "int1" })) return integerType(p, .tinyint, .smallint);
    if (asciiEqlAny(name, &.{ "float", "real", "float4" })) return numericModifiers(p, .float);
    if (asciiEqlAny(name, &.{"float8"})) return .double;
    if (asciiEqlAny(name, &.{"double"})) {
        if (p.cur.tag == .identifier and std.ascii.eqlIgnoreCase(p.cur.text, "precision")) {
            try p.advance();
        }
        return numericModifiers(p, .double);
    }
    if (asciiEqlAny(name, &.{ "decimal", "numeric", "dec", "fixed" })) {
        // MySQL's defaults: DECIMAL = DECIMAL(10, 0), DECIMAL(p) = DECIMAL(p, 0).
        var p_raw: i64 = 10;
        var s_raw: i64 = 0;
        if (p.cur.tag == .lparen) {
            try p.advance();
            if (p.cur.tag != .integer) return PE.SqlExpectedValue;
            p_raw = p.cur.value.integer;
            try p.advance();
            if (p.cur.tag == .comma) {
                try p.advance();
                if (p.cur.tag != .integer) return PE.SqlExpectedValue;
                s_raw = p.cur.value.integer;
                try p.advance();
            }
            try p.expect(.rparen);
        }
        try skipIntegerModifiers(p);
        if (p_raw < 1 or p_raw > 38 or s_raw < 0 or s_raw > p_raw) return PE.SqlExpectedValue;
        const p_u: u8 = @intCast(p_raw);
        const s_u: u8 = @intCast(s_raw);
        return if (p_u <= 18)
            types.Type{ .decimal64 = .{ .p = p_u, .s = s_u } }
        else
            types.Type{ .decimal128 = .{ .p = p_u, .s = s_u } };
    }
    if (asciiEqlAny(name, &.{ "char", "character", "nchar" })) {
        if (p.cur.tag == .identifier and std.ascii.eqlIgnoreCase(p.cur.text, "varying")) {
            try p.advance();
            return varcharType(p);
        }
        return types.Type{ .char = try optionalLength(p) orelse 1 };
    }
    if (asciiEqlAny(name, &.{ "varchar", "nvarchar" })) return varcharType(p);
    if (asciiEqlAny(name, &.{ "text", "string", "tinytext", "mediumtext", "longtext" })) return .string;
    // ENUM('a', 'b', ...) stores its labels as text; the list isn't enforced.
    if (asciiEqlAny(name, &.{"enum"})) {
        try skipLabelList(p);
        return .string;
    }
    // Byte strings store as text, which holds any bytes. BINARY(n)'s zero
    // padding isn't applied.
    if (asciiEqlAny(name, &.{ "binary", "varbinary", "tinyblob", "blob", "mediumblob", "longblob", "bytea" })) {
        _ = try optionalLength(p);
        return .string;
    }
    // BIT(1) is MySQL's flag type; a wider BIT(n) holds an n-bit unsigned
    // integer (BIT(64) values past BIGINT's range wrap negative).
    if (asciiEqlAny(name, &.{"bit"})) {
        const n = try optionalLength(p) orelse 1;
        if (n > 64) return PE.SqlExpectedValue;
        return if (n == 1) .boolean else .bigint;
    }
    if (asciiEqlAny(name, &.{"year"})) {
        _ = try optionalLength(p);
        return .smallint;
    }
    // MySQL's TIME is a signed duration up to 838:59:59; it stores as its
    // text, `HH:MM:SS[.ffffff]`.
    if (asciiEqlAny(name, &.{"time"})) {
        try skipFsp(p);
        return .string;
    }
    if (asciiEqlAny(name, &.{ "boolean", "bool" })) return .boolean;
    if (asciiEqlAny(name, &.{"date"})) return .date;
    // timestamptz is accepted as a synonym; thinDB datetimes are UTC-naive.
    if (asciiEqlAny(name, &.{ "datetime", "timestamp", "timestamptz" })) {
        try skipFsp(p);
        return .datetime;
    }
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

/// A MySQL administrative statement at the cursor, or null when the word
/// there opens none. PostgreSQL spells these its own way and its wire
/// answers them, so its dialect is left alone.
///
/// Transaction verbs parse here too, for the forms and batches the MySQL
/// wire's text match does not catch (`START TRANSACTION READ ONLY`,
/// `BEGIN; ...; COMMIT`). SQL-level PREPARE / EXECUTE is rejected: it
/// would need a per-session registry of statement texts, and clients
/// prepare through the binary protocol instead.
pub fn parseAdmin(p: anytype) !?*ir.Op {
    const PE = @TypeOf(p.*).Err;
    if (p.lex.dialect == .postgres or p.cur.tag != .identifier or p.cur.quoted) return null;
    const word = p.cur.text;
    if (asciiEqlAny(word, &.{ "prepare", "execute", "deallocate" })) return PE.SqlPrepareExecuteUnsupported;
    if (std.ascii.eqlIgnoreCase(word, "begin")) {
        try p.advance();
        if (isBareWord(p, "work")) try p.advance();
        return try p.allocOp(.{ .admin = .begin_transaction });
    }
    if (std.ascii.eqlIgnoreCase(word, "start")) {
        try p.advance();
        if (!isBareWord(p, "transaction")) return PE.SqlExpectedKeyword;
        try p.advance();
        try skipTransactionCharacteristics(p);
        return try p.allocOp(.{ .admin = .begin_transaction });
    }
    if (asciiEqlAny(word, &.{ "commit", "rollback" })) {
        const rollback = std.ascii.eqlIgnoreCase(word, "rollback");
        try p.advance();
        if (isBareWord(p, "work")) try p.advance();
        if (rollback and p.cur.tag == .kw_to) {
            try p.advance();
            if (isBareWord(p, "savepoint")) try p.advance();
            _ = try p.dupedIdent();
            return try p.allocOp(.{ .admin = .ignored });
        }
        const chain = try skipCompletionOptions(p);
        return try p.allocOp(.{ .admin = if (chain) .begin_transaction else .end_transaction });
    }
    if (std.ascii.eqlIgnoreCase(word, "savepoint")) {
        try p.advance();
        _ = try p.dupedIdent();
        return try p.allocOp(.{ .admin = .ignored });
    }
    if (std.ascii.eqlIgnoreCase(word, "release")) {
        try p.advance();
        if (!isBareWord(p, "savepoint")) return PE.SqlExpectedKeyword;
        try p.advance();
        _ = try p.dupedIdent();
        return try p.allocOp(.{ .admin = .ignored });
    }
    if (asciiEqlAny(word, &.{ "lock", "unlock" })) {
        try p.advance();
        if (p.cur.tag != .kw_table and p.cur.tag != .kw_tables and !isBareWord(p, "instance")) return PE.SqlExpectedKeyword;
        try skipToStatementEnd(p);
        return try p.allocOp(.{ .admin = .ignored });
    }
    if (std.ascii.eqlIgnoreCase(word, "flush")) {
        try p.advance();
        if (p.cur.tag == .eof or p.cur.tag == .semicolon) return PE.SqlExpectedKeyword;
        try skipToStatementEnd(p);
        return try p.allocOp(.{ .admin = .ignored });
    }
    if (std.ascii.eqlIgnoreCase(word, "do")) {
        // DO evaluates for side effects only, and thinDB's functions have
        // none, so the expressions are checked for syntax and dropped.
        try p.advance();
        while (true) {
            _ = try p.parseValueExpr();
            if (p.cur.tag != .comma) break;
            try p.advance();
        }
        return try p.allocOp(.{ .admin = .ignored });
    }
    const kind: ir.TableMaintenance.Kind = if (std.ascii.eqlIgnoreCase(word, "analyze"))
        .analyze
    else if (std.ascii.eqlIgnoreCase(word, "optimize"))
        .optimize
    else if (std.ascii.eqlIgnoreCase(word, "check"))
        .check
    else if (std.ascii.eqlIgnoreCase(word, "repair"))
        .repair
    else
        return null;
    try p.advance();
    if (kind != .check and (isBareWord(p, "no_write_to_binlog") or isBareWord(p, "local"))) try p.advance();
    if (p.cur.tag != .kw_table and p.cur.tag != .kw_tables) return PE.SqlExpectedKeyword;
    try p.advance();
    var tables: std.ArrayList(ir.TableRef) = .empty;
    while (true) {
        try tables.append(p.arena, try p.parseTableRef());
        if (p.cur.tag != .comma) break;
        try p.advance();
    }
    // Trailing options (CHECK ... QUICK, REPAIR ... USE_FRM, ANALYZE ...
    // UPDATE HISTOGRAM ON c) tune work thinDB does not do.
    try skipToStatementEnd(p);
    return try p.allocOp(.{ .admin = .{ .table_maintenance = .{ .kind = kind, .tables = tables.items } } });
}

/// `START TRANSACTION` characteristics: `WITH CONSISTENT SNAPSHOT`,
/// `READ ONLY`, `READ WRITE`, comma-separated.
fn skipTransactionCharacteristics(p: anytype) !void {
    const PE = @TypeOf(p.*).Err;
    while (true) {
        if (p.cur.tag == .kw_with) {
            try p.advance();
            if (!isBareWord(p, "consistent")) return PE.SqlExpectedKeyword;
            try p.advance();
            if (!isBareWord(p, "snapshot")) return PE.SqlExpectedKeyword;
            try p.advance();
        } else if (isBareWord(p, "read")) {
            try p.advance();
            if (!isBareWord(p, "only") and !isBareWord(p, "write")) return PE.SqlExpectedKeyword;
            try p.advance();
        } else return;
        if (p.cur.tag != .comma) return;
        try p.advance();
    }
}

/// `COMMIT` / `ROLLBACK` options `[AND [NO] CHAIN] [[NO] RELEASE]`. Returns
/// whether AND CHAIN opens the next transaction at once.
fn skipCompletionOptions(p: anytype) !bool {
    const PE = @TypeOf(p.*).Err;
    var chain = false;
    if (p.cur.tag == .kw_and) {
        try p.advance();
        const no = isBareWord(p, "no");
        if (no) try p.advance();
        if (!isBareWord(p, "chain")) return PE.SqlExpectedKeyword;
        try p.advance();
        chain = !no;
    }
    if (isBareWord(p, "no")) {
        try p.advance();
        if (!isBareWord(p, "release")) return PE.SqlExpectedKeyword;
        try p.advance();
    } else if (isBareWord(p, "release")) try p.advance();
    return chain;
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
                db = try p.dupedIdentLower();
            }
            return try p.allocOp(.{ .show = .{ .schemas = db } });
        },
        .kw_tables => {
            try p.advance();
            var ref: ir.TableRef = .{ .name = "" };
            if (p.cur.tag == .kw_from) {
                try p.advance();
                const first = try p.dupedIdentLower();
                if (p.cur.tag == .dot) {
                    try p.advance();
                    const second = try p.dupedIdentLower();
                    ref = .{ .database = first, .schema = second, .name = "" };
                } else {
                    ref = .{ .schema = first, .name = "" };
                }
            }
            return try p.allocOp(.{ .show = .{ .tables = ref } });
        },
        .kw_create => {
            try p.advance();
            if (p.cur.tag == .kw_database or p.cur.tag == .kw_schema) {
                try p.advance();
                const if_not_exists = try parseIfNotExists(p);
                const name = try p.dupedIdentLower();
                return try p.allocOp(.{ .show = .{ .create_database = .{ .name = name, .if_not_exists = if_not_exists } } });
            }
            if (!isIdentText(p, "function")) return PE.SqlExpectedKeyword;
            try p.advance();
            const name = try p.dupedIdentLower();
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
