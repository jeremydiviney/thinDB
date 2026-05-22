//! SQL lexer — tokenizes a single SQL statement into a stream of
//! tokens for the parser. Keywords are case-insensitive. Unquoted
//! identifiers are case-folded to ASCII lowercase so MySQL clients
//! (with `lower_case_table_names=1`) and PG clients (which lowercase
//! unquoted) behave the same. Backtick-quoted identifiers preserve
//! case and may contain any non-backtick byte. String literals are
//! single-quoted with `''` for an embedded quote.

const std = @import("std");
const Allocator = std.mem.Allocator;
const types = @import("../types.zig");

pub const TokenTag = enum {
    // Literals.
    integer,
    floating,
    string,
    identifier,

    // Keywords.
    kw_select,
    kw_from,
    kw_where,
    kw_group,
    kw_by,
    kw_order,
    kw_limit,
    kw_offset,
    kw_asc,
    kw_desc,
    kw_distinct,
    kw_as,
    kw_and,
    kw_or,
    kw_not,
    kw_null,
    kw_is,
    kw_true,
    kw_false,
    kw_join,
    kw_inner,
    kw_left,
    kw_right,
    kw_full,
    kw_outer,
    kw_on,
    kw_having,
    kw_with,
    kw_materialized,
    kw_create,
    kw_drop,
    kw_database,
    kw_databases,
    kw_schema,
    kw_schemas,
    kw_use,
    kw_show,
    kw_explain,
    kw_tables,
    kw_table,
    kw_primary,
    kw_key,
    kw_if,
    kw_exists,
    kw_into,
    kw_values,
    kw_insert,
    kw_copy,
    kw_temp,
    kw_temporary,
    // Window-function keywords.
    kw_over,
    kw_partition,
    kw_window,
    kw_rows,
    kw_range,
    kw_groups,
    kw_between,
    kw_preceding,
    kw_following,
    kw_unbounded,
    kw_current,
    kw_row,
    kw_ignore,
    kw_respect,
    kw_nulls,
    kw_qualify,
    kw_default,
    /// MySQL-style `AUTO_INCREMENT` column attribute. Lexed as a single
    /// token (matches MySQL's grammar) — the underscore is part of the
    /// identifier scan, so `auto_increment` parses as one ident and we
    /// promote it to a keyword in `keywordFor`.
    kw_auto_increment,
    // CASE / WHEN / THEN / ELSE / END — searched CASE expression in
    // projections and any expr position.
    kw_case,
    kw_when,
    kw_then,
    kw_else,
    kw_end,
    kw_like,
    kw_in,
    kw_interval,
    kw_union,
    kw_all,
    /// MySQL-style `SET` for user-defined variables: `SET @name = expr`.
    /// Same `SET` keyword used for session config in PG; thinDB v1
    /// only accepts the MySQL form.
    kw_set,
    kw_delete,
    kw_update,

    /// MySQL-style user-defined variable: `@name`. The `text` field
    /// carries the name without the `@` prefix. Resolved to a literal
    /// by the pre-compile pass using the active Session.vars.
    at_identifier,

    // Operators / punctuation.
    eq, // =
    neq, // != or <>
    lt, // <
    lte, // <=
    gt, // >
    gte, // >=
    star, // *
    plus, // +
    minus, // -
    slash, // /
    percent, // %
    pipe_pipe, // || (PG/ANSI string concat; MySQL logical OR)
    coloncolon, // :: (PG cast operator)
    comma, // ,
    dot, // .
    lparen, // (
    rparen, // )
    semicolon, // ;
    /// `?` placeholder used by MySQL prepared-statement clients. The
    /// SQL parser does not accept this in the v1 SELECT/INSERT grammar;
    /// it surfaces as a token so callers that pre-tokenize (the
    /// prepared-statement registry) can count `?` outside string/
    /// backtick context to learn the parameter count.
    question, // ?
    /// `$N` numbered placeholder used by PostgreSQL Extended Query
    /// clients (asyncpg, psycopg, JDBC, node-pg). The numeric index is
    /// carried in `Token.value.dollar_param`. Same role as `.question`:
    /// the parser doesn't accept this in the v1 grammar; the PG
    /// Extended-Query layer rewrites occurrences to literals before
    /// re-parsing.
    dollar_param, // $1, $2, ...

    eof,
};

pub const Token = struct {
    tag: TokenTag,
    /// Source byte range — useful for parser error messages. The slice
    /// is borrowed from the input SQL string.
    text: []const u8,
    /// Pre-parsed numeric/string payload, populated only for the
    /// literal variants. Saves the parser from re-parsing on access.
    value: union(enum) {
        none,
        integer: i64,
        floating: f64,
        /// String literal contents with `''` un-escaped to `'`. Owned
        /// by the lexer's arena when un-escaping happens; otherwise a
        /// borrowed slice into the input.
        string: []const u8,
        /// Numbered placeholder index (`$1` → 1). Populated only for
        /// the `.dollar_param` token tag.
        dollar_param: u32,
    } = .none,
};

pub const LexError = error{
    LexUnterminatedString,
    LexUnterminatedIdentifier,
    LexInvalidNumber,
    LexUnexpectedChar,
} || Allocator.Error;

pub const Lexer = struct {
    arena: Allocator,
    src: []const u8,
    pos: usize = 0,
    /// SQL flavor being lexed. Governs the dialect-divergent tokens:
    /// `"..."` is an identifier on PG/neutral but a string literal on
    /// MySQL, and backtick identifiers are rejected on PG.
    dialect: types.Dialect = .neutral,

    pub fn init(arena: Allocator, src: []const u8) Lexer {
        return .{ .arena = arena, .src = src };
    }

    pub fn next(self: *Lexer) LexError!Token {
        try self.skipWhitespaceAndComments();
        if (self.pos >= self.src.len) return Token{ .tag = .eof, .text = "" };

        const start = self.pos;
        const ch = self.src[self.pos];

        // Single/double-character operators.
        switch (ch) {
            '=' => {
                self.pos += 1;
                return Token{ .tag = .eq, .text = self.src[start..self.pos] };
            },
            ',' => {
                self.pos += 1;
                return Token{ .tag = .comma, .text = self.src[start..self.pos] };
            },
            '.' => {
                self.pos += 1;
                return Token{ .tag = .dot, .text = self.src[start..self.pos] };
            },
            '(' => {
                self.pos += 1;
                return Token{ .tag = .lparen, .text = self.src[start..self.pos] };
            },
            ')' => {
                self.pos += 1;
                return Token{ .tag = .rparen, .text = self.src[start..self.pos] };
            },
            ';' => {
                self.pos += 1;
                return Token{ .tag = .semicolon, .text = self.src[start..self.pos] };
            },
            '*' => {
                self.pos += 1;
                return Token{ .tag = .star, .text = self.src[start..self.pos] };
            },
            '+' => {
                self.pos += 1;
                return Token{ .tag = .plus, .text = self.src[start..self.pos] };
            },
            '%' => {
                self.pos += 1;
                return Token{ .tag = .percent, .text = self.src[start..self.pos] };
            },
            // `-` and `/` only land here after skipWhitespaceAndComments,
            // which already consumed any `--` line comment or `/* */`
            // block comment opener. A bare `-` or `/` is therefore an
            // arithmetic operator.
            '-' => {
                self.pos += 1;
                return Token{ .tag = .minus, .text = self.src[start..self.pos] };
            },
            '/' => {
                self.pos += 1;
                return Token{ .tag = .slash, .text = self.src[start..self.pos] };
            },
            '?' => {
                self.pos += 1;
                return Token{ .tag = .question, .text = self.src[start..self.pos] };
            },
            '|' => {
                if (self.peekChar(1) == '|') {
                    self.pos += 2;
                    return Token{ .tag = .pipe_pipe, .text = self.src[start..self.pos] };
                }
                return LexError.LexUnexpectedChar;
            },
            ':' => {
                if (self.peekChar(1) == ':') {
                    self.pos += 2;
                    return Token{ .tag = .coloncolon, .text = self.src[start..self.pos] };
                }
                return LexError.LexUnexpectedChar;
            },
            '$' => return try self.lexDollarParam(),
            '@' => return try self.lexAtVar(),
            '!' => {
                if (self.peekChar(1) == '=') {
                    self.pos += 2;
                    return Token{ .tag = .neq, .text = self.src[start..self.pos] };
                }
                return LexError.LexUnexpectedChar;
            },
            '<' => {
                if (self.peekChar(1) == '=') {
                    self.pos += 2;
                    return Token{ .tag = .lte, .text = self.src[start..self.pos] };
                }
                if (self.peekChar(1) == '>') {
                    self.pos += 2;
                    return Token{ .tag = .neq, .text = self.src[start..self.pos] };
                }
                self.pos += 1;
                return Token{ .tag = .lt, .text = self.src[start..self.pos] };
            },
            '>' => {
                if (self.peekChar(1) == '=') {
                    self.pos += 2;
                    return Token{ .tag = .gte, .text = self.src[start..self.pos] };
                }
                self.pos += 1;
                return Token{ .tag = .gt, .text = self.src[start..self.pos] };
            },
            '\'' => return try self.lexString(),
            '"' => return try self.lexDoubleQuoted(),
            '`' => return try self.lexBacktickIdent(),
            '0'...'9' => return try self.lexNumber(),
            'a'...'z', 'A'...'Z', '_' => {
                // PG escape-string prefix `E'...'` / `e'...'`.
                if ((ch == 'E' or ch == 'e') and self.peekChar(1) == '\'' and self.dialect != .mysql)
                    return try self.lexEscapeString();
                return try self.lexIdent();
            },
            else => return LexError.LexUnexpectedChar,
        }
    }

    fn peekChar(self: *Lexer, offset: usize) ?u8 {
        const idx = self.pos + offset;
        if (idx >= self.src.len) return null;
        return self.src[idx];
    }

    fn skipWhitespaceAndComments(self: *Lexer) LexError!void {
        while (self.pos < self.src.len) {
            const ch = self.src[self.pos];
            if (std.ascii.isWhitespace(ch)) {
                self.pos += 1;
                continue;
            }
            // -- line comment
            if (ch == '-' and self.peekChar(1) == '-') {
                while (self.pos < self.src.len and self.src[self.pos] != '\n') : (self.pos += 1) {}
                continue;
            }
            // # line comment (MySQL only)
            if (ch == '#' and self.dialect == .mysql) {
                while (self.pos < self.src.len and self.src[self.pos] != '\n') : (self.pos += 1) {}
                continue;
            }
            // /* block comment */
            if (ch == '/' and self.peekChar(1) == '*') {
                self.pos += 2;
                while (self.pos + 1 < self.src.len) : (self.pos += 1) {
                    if (self.src[self.pos] == '*' and self.src[self.pos + 1] == '/') {
                        self.pos += 2;
                        break;
                    }
                }
                continue;
            }
            break;
        }
    }

    fn lexString(self: *Lexer) LexError!Token {
        // MySQL processes C-style backslash escapes in ordinary string
        // literals; PG/neutral treat backslash literally (standard SQL,
        // standard_conforming_strings on). `''` always escapes a quote.
        const start = self.pos;
        const content = try self.scanStringContent(self.dialect == .mysql);
        return Token{ .tag = .string, .text = self.src[start..self.pos], .value = .{ .string = content } };
    }

    /// PG escape-string `E'...'` — backslash escapes are always processed
    /// regardless of dialect. Cursor is on the `E`/`e`.
    fn lexEscapeString(self: *Lexer) LexError!Token {
        const start = self.pos;
        self.pos += 1; // skip the E prefix
        const content = try self.scanStringContent(true);
        return Token{ .tag = .string, .text = self.src[start..self.pos], .value = .{ .string = content } };
    }

    /// Cursor is on the opening `'`. Consume through the closing `'` and
    /// return the (un-escaped) content. `''` is always a literal quote;
    /// when `process_backslash` is set, `\x` C-style escapes are honored.
    fn scanStringContent(self: *Lexer, process_backslash: bool) LexError![]const u8 {
        self.pos += 1; // opening quote
        const body_start = self.pos;
        var needs_build = false;
        while (self.pos < self.src.len) {
            const c = self.src[self.pos];
            if (c == '\\' and process_backslash) {
                needs_build = true;
                self.pos += if (self.pos + 1 < self.src.len) 2 else 1;
                continue;
            }
            if (c == '\'') {
                if (self.peekChar(1) == '\'') {
                    needs_build = true;
                    self.pos += 2;
                    continue;
                }
                const raw = self.src[body_start..self.pos];
                self.pos += 1; // closing quote
                if (!needs_build) return raw;
                return try self.buildUnescaped(raw, process_backslash);
            }
            self.pos += 1;
        }
        return LexError.LexUnterminatedString;
    }

    fn buildUnescaped(self: *Lexer, raw: []const u8, process_backslash: bool) LexError![]const u8 {
        const buf = try self.arena.alloc(u8, raw.len);
        var out: usize = 0;
        var i: usize = 0;
        while (i < raw.len) : (i += 1) {
            const c = raw[i];
            if (c == '\'' and i + 1 < raw.len and raw[i + 1] == '\'') {
                buf[out] = '\'';
                out += 1;
                i += 1;
                continue;
            }
            if (c == '\\' and process_backslash and i + 1 < raw.len) {
                i += 1;
                buf[out] = switch (raw[i]) {
                    'n' => '\n',
                    't' => '\t',
                    'r' => '\r',
                    '0' => 0,
                    'b' => 8,
                    'Z' => 26,
                    else => raw[i], // \\, \', \", \<other> → the literal char
                };
                out += 1;
                continue;
            }
            buf[out] = c;
            out += 1;
        }
        return buf[0..out];
    }

    fn lexNumber(self: *Lexer) LexError!Token {
        const start = self.pos;
        var seen_dot = false;
        while (self.pos < self.src.len) : (self.pos += 1) {
            const c = self.src[self.pos];
            if (c == '.') {
                if (seen_dot) break;
                seen_dot = true;
                continue;
            }
            if (!std.ascii.isDigit(c)) break;
        }
        const text = self.src[start..self.pos];
        if (seen_dot) {
            const v = std.fmt.parseFloat(f64, text) catch return LexError.LexInvalidNumber;
            return Token{ .tag = .floating, .text = text, .value = .{ .floating = v } };
        } else {
            const v = std.fmt.parseInt(i64, text, 10) catch return LexError.LexInvalidNumber;
            return Token{ .tag = .integer, .text = text, .value = .{ .integer = v } };
        }
    }

    fn lexIdent(self: *Lexer) LexError!Token {
        const start = self.pos;
        while (self.pos < self.src.len) : (self.pos += 1) {
            const c = self.src[self.pos];
            if (!std.ascii.isAlphanumeric(c) and c != '_') break;
        }
        const text = self.src[start..self.pos];
        if (keywordFor(text)) |kw| return Token{ .tag = kw, .text = text };
        const lowered = try self.arena.alloc(u8, text.len);
        for (text, 0..) |c, i| lowered[i] = std.ascii.toLower(c);
        return Token{ .tag = .identifier, .text = lowered };
    }

    fn lexDollarParam(self: *Lexer) LexError!Token {
        const start = self.pos;
        self.pos += 1; // consume '$'
        const digits_start = self.pos;
        while (self.pos < self.src.len and std.ascii.isDigit(self.src[self.pos])) : (self.pos += 1) {}
        const digits = self.src[digits_start..self.pos];
        if (digits.len == 0) return LexError.LexUnexpectedChar;
        const n = std.fmt.parseInt(u32, digits, 10) catch return LexError.LexInvalidNumber;
        return Token{
            .tag = .dollar_param,
            .text = self.src[start..self.pos],
            .value = .{ .dollar_param = n },
        };
    }

    fn lexAtVar(self: *Lexer) LexError!Token {
        self.pos += 1; // consume '@'
        const name_start = self.pos;
        while (self.pos < self.src.len) : (self.pos += 1) {
            const c = self.src[self.pos];
            if (!(std.ascii.isAlphanumeric(c) or c == '_')) break;
        }
        const name = self.src[name_start..self.pos];
        if (name.len == 0) return LexError.LexUnexpectedChar;
        return Token{ .tag = .at_identifier, .text = name };
    }

    /// `"..."`. On MySQL this is a string literal (same as `'...'`); on
    /// PG/neutral it is a delimited, case-preserving identifier. `""`
    /// escapes an embedded double-quote in both modes.
    fn lexDoubleQuoted(self: *Lexer) LexError!Token {
        const as_string = self.dialect == .mysql;
        const start = self.pos;
        self.pos += 1; // opening "
        var contains_escape = false;
        while (self.pos < self.src.len) : (self.pos += 1) {
            if (self.src[self.pos] != '"') continue;
            if (self.peekChar(1) == '"') {
                contains_escape = true;
                self.pos += 1; // skip first "; loop's += 1 skips the second
                continue;
            }
            self.pos += 1; // closing "
            const raw = self.src[start + 1 .. self.pos - 1];
            const text = self.src[start..self.pos];
            const content = if (!contains_escape) raw else blk: {
                const buf = try self.arena.alloc(u8, raw.len);
                var out: usize = 0;
                var i: usize = 0;
                while (i < raw.len) : (i += 1) {
                    buf[out] = raw[i];
                    out += 1;
                    if (raw[i] == '"' and i + 1 < raw.len and raw[i + 1] == '"') i += 1;
                }
                break :blk buf[0..out];
            };
            if (as_string) return Token{ .tag = .string, .text = text, .value = .{ .string = content } };
            if (content.len == 0) return LexError.LexUnexpectedChar;
            return Token{ .tag = .identifier, .text = content };
        }
        return if (as_string) LexError.LexUnterminatedString else LexError.LexUnterminatedIdentifier;
    }

    fn lexBacktickIdent(self: *Lexer) LexError!Token {
        // Backtick identifiers are a MySQL extension; PG has no such
        // quoting, so reject them on a PG connection.
        if (self.dialect == .postgres) return LexError.LexUnexpectedChar;
        const start = self.pos;
        self.pos += 1;
        while (self.pos < self.src.len) : (self.pos += 1) {
            if (self.src[self.pos] == '`') {
                const text = self.src[start + 1 .. self.pos];
                self.pos += 1;
                if (text.len == 0) return LexError.LexUnexpectedChar;
                return Token{ .tag = .identifier, .text = text };
            }
        }
        return LexError.LexUnterminatedIdentifier;
    }
};

fn keywordFor(s: []const u8) ?TokenTag {
    // Tiny set; linear scan is fine. ASCII-case-insensitive compare.
    const kws = [_]struct { name: []const u8, tag: TokenTag }{
        .{ .name = "select", .tag = .kw_select },
        .{ .name = "from", .tag = .kw_from },
        .{ .name = "where", .tag = .kw_where },
        .{ .name = "group", .tag = .kw_group },
        .{ .name = "by", .tag = .kw_by },
        .{ .name = "order", .tag = .kw_order },
        .{ .name = "limit", .tag = .kw_limit },
        .{ .name = "offset", .tag = .kw_offset },
        .{ .name = "asc", .tag = .kw_asc },
        .{ .name = "desc", .tag = .kw_desc },
        .{ .name = "distinct", .tag = .kw_distinct },
        .{ .name = "as", .tag = .kw_as },
        .{ .name = "and", .tag = .kw_and },
        .{ .name = "or", .tag = .kw_or },
        .{ .name = "not", .tag = .kw_not },
        .{ .name = "null", .tag = .kw_null },
        .{ .name = "is", .tag = .kw_is },
        .{ .name = "true", .tag = .kw_true },
        .{ .name = "false", .tag = .kw_false },
        .{ .name = "join", .tag = .kw_join },
        .{ .name = "inner", .tag = .kw_inner },
        .{ .name = "left", .tag = .kw_left },
        .{ .name = "right", .tag = .kw_right },
        .{ .name = "full", .tag = .kw_full },
        .{ .name = "outer", .tag = .kw_outer },
        .{ .name = "on", .tag = .kw_on },
        .{ .name = "having", .tag = .kw_having },
        .{ .name = "with", .tag = .kw_with },
        .{ .name = "materialized", .tag = .kw_materialized },
        .{ .name = "create", .tag = .kw_create },
        .{ .name = "drop", .tag = .kw_drop },
        .{ .name = "database", .tag = .kw_database },
        .{ .name = "databases", .tag = .kw_databases },
        .{ .name = "schema", .tag = .kw_schema },
        .{ .name = "schemas", .tag = .kw_schemas },
        .{ .name = "use", .tag = .kw_use },
        .{ .name = "show", .tag = .kw_show },
        .{ .name = "explain", .tag = .kw_explain },
        .{ .name = "tables", .tag = .kw_tables },
        .{ .name = "table", .tag = .kw_table },
        .{ .name = "primary", .tag = .kw_primary },
        .{ .name = "key", .tag = .kw_key },
        .{ .name = "if", .tag = .kw_if },
        .{ .name = "exists", .tag = .kw_exists },
        .{ .name = "into", .tag = .kw_into },
        .{ .name = "values", .tag = .kw_values },
        .{ .name = "insert", .tag = .kw_insert },
        .{ .name = "copy", .tag = .kw_copy },
        .{ .name = "temp", .tag = .kw_temp },
        .{ .name = "temporary", .tag = .kw_temporary },
        .{ .name = "over", .tag = .kw_over },
        .{ .name = "partition", .tag = .kw_partition },
        .{ .name = "window", .tag = .kw_window },
        .{ .name = "rows", .tag = .kw_rows },
        .{ .name = "range", .tag = .kw_range },
        .{ .name = "groups", .tag = .kw_groups },
        .{ .name = "between", .tag = .kw_between },
        .{ .name = "preceding", .tag = .kw_preceding },
        .{ .name = "following", .tag = .kw_following },
        .{ .name = "unbounded", .tag = .kw_unbounded },
        .{ .name = "current", .tag = .kw_current },
        .{ .name = "row", .tag = .kw_row },
        .{ .name = "ignore", .tag = .kw_ignore },
        .{ .name = "respect", .tag = .kw_respect },
        .{ .name = "nulls", .tag = .kw_nulls },
        .{ .name = "qualify", .tag = .kw_qualify },
        .{ .name = "default", .tag = .kw_default },
        .{ .name = "auto_increment", .tag = .kw_auto_increment },
        .{ .name = "case", .tag = .kw_case },
        .{ .name = "when", .tag = .kw_when },
        .{ .name = "then", .tag = .kw_then },
        .{ .name = "else", .tag = .kw_else },
        .{ .name = "end", .tag = .kw_end },
        .{ .name = "like", .tag = .kw_like },
        .{ .name = "in", .tag = .kw_in },
        .{ .name = "interval", .tag = .kw_interval },
        .{ .name = "union", .tag = .kw_union },
        .{ .name = "all", .tag = .kw_all },
        .{ .name = "set", .tag = .kw_set },
        .{ .name = "delete", .tag = .kw_delete },
        .{ .name = "update", .tag = .kw_update },
    };
    for (kws) |kw| {
        if (std.ascii.eqlIgnoreCase(s, kw.name)) return kw.tag;
    }
    return null;
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

test "lexer: simple SELECT" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    var lx = Lexer.init(arena.allocator(), "SELECT id FROM users");

    try std.testing.expectEqual(@as(TokenTag, .kw_select), (try lx.next()).tag);
    const id = try lx.next();
    try std.testing.expectEqual(@as(TokenTag, .identifier), id.tag);
    try std.testing.expectEqualStrings("id", id.text);
    try std.testing.expectEqual(@as(TokenTag, .kw_from), (try lx.next()).tag);
    try std.testing.expectEqual(@as(TokenTag, .identifier), (try lx.next()).tag);
    try std.testing.expectEqual(@as(TokenTag, .eof), (try lx.next()).tag);
}

test "lexer: case-insensitive keywords + unquoted idents lowercased" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    var lx = Lexer.init(arena.allocator(), "select Foo from Bar");
    try std.testing.expectEqual(@as(TokenTag, .kw_select), (try lx.next()).tag);
    const id1 = try lx.next();
    try std.testing.expectEqual(@as(TokenTag, .identifier), id1.tag);
    try std.testing.expectEqualStrings("foo", id1.text);
    try std.testing.expectEqual(@as(TokenTag, .kw_from), (try lx.next()).tag);
    const id2 = try lx.next();
    try std.testing.expectEqual(@as(TokenTag, .identifier), id2.tag);
    try std.testing.expectEqualStrings("bar", id2.text);
}

test "lexer: backtick-quoted identifier preserves case + allows spaces" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    var lx = Lexer.init(arena.allocator(), "select `Foo Bar`");
    try std.testing.expectEqual(@as(TokenTag, .kw_select), (try lx.next()).tag);
    const id = try lx.next();
    try std.testing.expectEqual(@as(TokenTag, .identifier), id.tag);
    try std.testing.expectEqualStrings("Foo Bar", id.text);
    try std.testing.expectEqual(@as(TokenTag, .eof), (try lx.next()).tag);
}

test "lexer: double-quote is an identifier on PG/neutral, string on MySQL" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    // PG: case-preserving delimited identifier, "" un-escaped.
    {
        var lx = Lexer.init(arena.allocator(), "\"Foo\"\"Bar\"");
        lx.dialect = .postgres;
        const id = try lx.next();
        try std.testing.expectEqual(@as(TokenTag, .identifier), id.tag);
        try std.testing.expectEqualStrings("Foo\"Bar", id.text);
    }
    // neutral behaves like PG (ANSI): identifier.
    {
        var lx = Lexer.init(arena.allocator(), "\"col\"");
        const id = try lx.next();
        try std.testing.expectEqual(@as(TokenTag, .identifier), id.tag);
        try std.testing.expectEqualStrings("col", id.text);
    }
    // MySQL: string literal, "" un-escaped.
    {
        var lx = Lexer.init(arena.allocator(), "\"a\"\"b\"");
        lx.dialect = .mysql;
        const s = try lx.next();
        try std.testing.expectEqual(@as(TokenTag, .string), s.tag);
        try std.testing.expectEqualStrings("a\"b", s.value.string);
    }
}

test "lexer: backtick identifier rejected on PG, accepted on MySQL/neutral" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    {
        var lx = Lexer.init(arena.allocator(), "`x`");
        lx.dialect = .postgres;
        try std.testing.expectError(LexError.LexUnexpectedChar, lx.next());
    }
    {
        var lx = Lexer.init(arena.allocator(), "`x`");
        lx.dialect = .mysql;
        const id = try lx.next();
        try std.testing.expectEqual(@as(TokenTag, .identifier), id.tag);
        try std.testing.expectEqualStrings("x", id.text);
    }
}

test "lexer: MySQL processes backslash escapes, PG/neutral treat backslash literally" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const aa = arena.allocator();
    {
        var lx = Lexer.init(aa, "'a\\tb'"); // SQL: 'a\tb'
        lx.dialect = .mysql;
        const s = try lx.next();
        try std.testing.expectEqual(@as(TokenTag, .string), s.tag);
        try std.testing.expectEqualStrings("a\tb", s.value.string);
    }
    {
        var lx = Lexer.init(aa, "'a\\tb'"); // neutral: backslash is literal
        const s = try lx.next();
        try std.testing.expectEqualStrings("a\\tb", s.value.string);
    }
    {
        // '' escapes a quote in every dialect.
        var lx = Lexer.init(aa, "'it''s'");
        const s = try lx.next();
        try std.testing.expectEqualStrings("it's", s.value.string);
    }
}

test "lexer: PG E'...' escape strings process backslashes" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    var lx = Lexer.init(arena.allocator(), "E'a\\nb'"); // neutral
    const s = try lx.next();
    try std.testing.expectEqual(@as(TokenTag, .string), s.tag);
    try std.testing.expectEqualStrings("a\nb", s.value.string);
}

test "lexer: # is a line comment on MySQL only" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const aa = arena.allocator();
    {
        var lx = Lexer.init(aa, "1 # cmt\n+ 2");
        lx.dialect = .mysql;
        try std.testing.expectEqual(@as(TokenTag, .integer), (try lx.next()).tag);
        try std.testing.expectEqual(@as(TokenTag, .plus), (try lx.next()).tag);
    }
    {
        var lx = Lexer.init(aa, "#x"); // neutral: # is not a comment
        try std.testing.expectError(LexError.LexUnexpectedChar, lx.next());
    }
}

test "lexer: unterminated backtick errors cleanly" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    var lx = Lexer.init(arena.allocator(), "`open ident");
    try std.testing.expectError(LexError.LexUnterminatedIdentifier, lx.next());
}

test "lexer: operators including != <> <= >=" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    var lx = Lexer.init(arena.allocator(), "a != b <> c <= d >= e < f > g = h");
    const expected_tags = [_]TokenTag{
        .identifier, .neq, .identifier, .neq, .identifier, .lte, .identifier,
        .gte,        .identifier, .lt, .identifier, .gt, .identifier, .eq,
        .identifier,
    };
    for (expected_tags) |tag| {
        try std.testing.expectEqual(tag, (try lx.next()).tag);
    }
    try std.testing.expectEqual(@as(TokenTag, .eof), (try lx.next()).tag);
}

test "lexer: integer + float + string literals" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    var lx = Lexer.init(arena.allocator(), "42 3.14 'hello' 'it''s'");

    const int_tok = try lx.next();
    try std.testing.expectEqual(@as(TokenTag, .integer), int_tok.tag);
    try std.testing.expectEqual(@as(i64, 42), int_tok.value.integer);

    const flt_tok = try lx.next();
    try std.testing.expectEqual(@as(TokenTag, .floating), flt_tok.tag);
    try std.testing.expectApproxEqAbs(@as(f64, 3.14), flt_tok.value.floating, 1e-9);

    const str_tok = try lx.next();
    try std.testing.expectEqual(@as(TokenTag, .string), str_tok.tag);
    try std.testing.expectEqualStrings("hello", str_tok.value.string);

    const escaped = try lx.next();
    try std.testing.expectEqual(@as(TokenTag, .string), escaped.tag);
    try std.testing.expectEqualStrings("it's", escaped.value.string);
}

test "lexer: skips line and block comments" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    var lx = Lexer.init(arena.allocator(),
        \\SELECT id -- this is a line comment
        \\/* multi-line
        \\   comment */ FROM users
    );
    try std.testing.expectEqual(@as(TokenTag, .kw_select), (try lx.next()).tag);
    try std.testing.expectEqual(@as(TokenTag, .identifier), (try lx.next()).tag);
    try std.testing.expectEqual(@as(TokenTag, .kw_from), (try lx.next()).tag);
    try std.testing.expectEqual(@as(TokenTag, .identifier), (try lx.next()).tag);
}

test "lexer: question mark outside strings produces .question token" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    var lx = Lexer.init(arena.allocator(), "SELECT * FROM t WHERE a = ? AND b = ?");
    const tags = [_]TokenTag{
        .kw_select, .star, .kw_from, .identifier, .kw_where,
        .identifier, .eq, .question, .kw_and, .identifier,
        .eq, .question, .eof,
    };
    for (tags) |tag| {
        try std.testing.expectEqual(tag, (try lx.next()).tag);
    }
}

test "lexer: question mark inside string literal is content, not a placeholder" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    var lx = Lexer.init(arena.allocator(), "'why?' ?");
    const tok = try lx.next();
    try std.testing.expectEqual(@as(TokenTag, .string), tok.tag);
    try std.testing.expectEqualStrings("why?", tok.value.string);
    try std.testing.expectEqual(@as(TokenTag, .question), (try lx.next()).tag);
}

test "lexer: $N outside strings produces .dollar_param token with index" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    var lx = Lexer.init(arena.allocator(), "SELECT * FROM t WHERE a = $1 AND b = $42");
    const expected_tags = [_]TokenTag{
        .kw_select,  .star, .kw_from,      .identifier, .kw_where,
        .identifier, .eq,   .dollar_param, .kw_and,     .identifier,
        .eq,         .dollar_param,
    };
    var idx: u32 = 0;
    for (expected_tags) |tag| {
        const tok = try lx.next();
        try std.testing.expectEqual(tag, tok.tag);
        if (tok.tag == .dollar_param) {
            idx += 1;
            const want: u32 = if (idx == 1) 1 else 42;
            try std.testing.expectEqual(want, tok.value.dollar_param);
        }
    }
    try std.testing.expectEqual(@as(TokenTag, .eof), (try lx.next()).tag);
}

test "lexer: $N inside string literal is content, not a placeholder" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    var lx = Lexer.init(arena.allocator(), "'price: $1' $2");
    const tok = try lx.next();
    try std.testing.expectEqual(@as(TokenTag, .string), tok.tag);
    try std.testing.expectEqualStrings("price: $1", tok.value.string);
    const param = try lx.next();
    try std.testing.expectEqual(@as(TokenTag, .dollar_param), param.tag);
    try std.testing.expectEqual(@as(u32, 2), param.value.dollar_param);
}

test "lexer: $ without trailing digits errors" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    var lx = Lexer.init(arena.allocator(), "$");
    try std.testing.expectError(LexError.LexUnexpectedChar, lx.next());
}

test "lexer: unterminated string errors cleanly" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    var lx = Lexer.init(arena.allocator(), "'open string");
    try std.testing.expectError(LexError.LexUnterminatedString, lx.next());
}
