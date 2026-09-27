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
    /// An integer literal past the BIGINT range. Its decimal digits ride in
    /// `Token.value.big_integer`; the parser types it as MySQL does.
    big_integer,
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
    kw_cross,
    kw_div,
    kw_inner,
    kw_left,
    kw_right,
    kw_full,
    kw_outer,
    kw_on,
    kw_having,
    kw_with,
    kw_materialized,
    kw_alter,
    kw_rename,
    kw_truncate,
    kw_add,
    kw_column,
    kw_to,
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
    kw_replace,
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
    kw_regexp,
    kw_in,
    kw_interval,
    kw_union,
    kw_intersect,
    /// EXCEPT, or MINUS, StarRocks' and Oracle's spelling of it.
    kw_except,
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
    /// MySQL system variable: `@@name`, `@@session.name`. The `text` field
    /// carries the name without the `@@` prefix, scope included.
    system_variable,

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
    arrow, // -> (MySQL JSON extract: JSON_EXTRACT)
    arrow2, // ->> (MySQL JSON extract + unquote: JSON_UNQUOTE(JSON_EXTRACT))
    pipe_pipe, // || (PG/ANSI string concat; MySQL logical OR)
    null_safe_eq, // <=> (MySQL null-safe equality)
    amp, // & (bitwise AND)
    amp_amp, // && (MySQL logical AND)
    pipe, // | (bitwise OR)
    caret, // ^ (MySQL bitwise XOR; PG/neutral exponentiation)
    tilde, // ~ (bitwise NOT)
    shl, // <<
    shr, // >>
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
        /// Decimal digits of a `.big_integer`, without leading zeros.
        big_integer: []const u8,
        floating: f64,
        /// String literal contents with `''` un-escaped to `'`. Owned
        /// by the lexer's arena when un-escaping happens; otherwise a
        /// borrowed slice into the input.
        string: []const u8,
        /// Numbered placeholder index (`$1` → 1). Populated only for
        /// the `.dollar_param` token tag.
        dollar_param: u32,
    } = .none,
    /// A backquoted or double-quoted identifier: never a keyword, even when
    /// spelled like one of the contextual clause words (`INDEX`, `UNIQUE`).
    quoted: bool = false,
};

pub const LexError = error{
    LexUnterminatedString,
    LexUnterminatedIdentifier,
    LexInvalidNumber,
    LexUnexpectedChar,
    /// A charset introducer whose literal thinDB would have to transcode:
    /// a UTF-16/32 charset, or non-ASCII bytes under a non-UTF-8 charset.
    LexCharsetUnsupported,
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
                // `.5` is a number unless the dot qualifies a name (`t.x`).
                const c1 = self.peekChar(1);
                if (c1 != null and std.ascii.isDigit(c1.?) and !self.followsName(start)) return try self.lexNumber();
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
                // `->` / `->>` MySQL JSON extraction operators.
                if (self.pos < self.src.len and self.src[self.pos] == '>') {
                    self.pos += 1;
                    if (self.pos < self.src.len and self.src[self.pos] == '>') {
                        self.pos += 1;
                        return Token{ .tag = .arrow2, .text = self.src[start..self.pos] };
                    }
                    return Token{ .tag = .arrow, .text = self.src[start..self.pos] };
                }
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
                self.pos += 1;
                return Token{ .tag = .pipe, .text = self.src[start..self.pos] };
            },
            '&' => {
                if (self.peekChar(1) == '&') {
                    self.pos += 2;
                    return Token{ .tag = .amp_amp, .text = self.src[start..self.pos] };
                }
                self.pos += 1;
                return Token{ .tag = .amp, .text = self.src[start..self.pos] };
            },
            '^' => {
                self.pos += 1;
                return Token{ .tag = .caret, .text = self.src[start..self.pos] };
            },
            '~' => {
                self.pos += 1;
                return Token{ .tag = .tilde, .text = self.src[start..self.pos] };
            },
            ':' => {
                if (self.peekChar(1) == ':') {
                    self.pos += 2;
                    return Token{ .tag = .coloncolon, .text = self.src[start..self.pos] };
                }
                return LexError.LexUnexpectedChar;
            },
            '$' => {
                // `$N` numbered placeholder vs PG dollar-quoted string
                // (`$$...$$` / `$tag$...$tag$`). A digit after `$` means a
                // placeholder; anything else is a dollar-quote on PG/neutral.
                const c1 = self.peekChar(1);
                if (c1 != null and std.ascii.isDigit(c1.?)) return try self.lexDollarParam();
                // `$$` opens a dollar-quoted string on EVERY dialect —
                // MySQL has no competing `$$` syntax, and LANGUAGE zig
                // function bodies arrive dollar-quoted over the mysql wire.
                if (self.dialect != .mysql or c1 == '$') return try self.lexDollarQuote();
                return try self.lexDollarParam();
            },
            '@' => return try self.lexAtVar(),
            '!' => {
                if (self.peekChar(1) == '=') {
                    self.pos += 2;
                    return Token{ .tag = .neq, .text = self.src[start..self.pos] };
                }
                return LexError.LexUnexpectedChar;
            },
            '<' => {
                if (self.peekChar(1) == '=' and self.peekChar(2) == '>') {
                    self.pos += 3;
                    return Token{ .tag = .null_safe_eq, .text = self.src[start..self.pos] };
                }
                if (self.peekChar(1) == '=') {
                    self.pos += 2;
                    return Token{ .tag = .lte, .text = self.src[start..self.pos] };
                }
                if (self.peekChar(1) == '>') {
                    self.pos += 2;
                    return Token{ .tag = .neq, .text = self.src[start..self.pos] };
                }
                if (self.peekChar(1) == '<') {
                    self.pos += 2;
                    return Token{ .tag = .shl, .text = self.src[start..self.pos] };
                }
                self.pos += 1;
                return Token{ .tag = .lt, .text = self.src[start..self.pos] };
            },
            '>' => {
                if (self.peekChar(1) == '=') {
                    self.pos += 2;
                    return Token{ .tag = .gte, .text = self.src[start..self.pos] };
                }
                if (self.peekChar(1) == '>') {
                    self.pos += 2;
                    return Token{ .tag = .shr, .text = self.src[start..self.pos] };
                }
                self.pos += 1;
                return Token{ .tag = .gt, .text = self.src[start..self.pos] };
            },
            '\'' => return try self.lexString(start),
            '"' => return try self.lexDoubleQuoted(),
            '`' => return try self.lexBacktickIdent(),
            '0'...'9' => {
                if (ch == '0' and self.dialect != .postgres) {
                    if (try self.scanHexNumber()) |bytes| return Token{ .tag = .string, .text = self.src[start..self.pos], .value = .{ .string = bytes } };
                    if (try self.scanBitNumber()) |digits| return try self.bitIntegerToken(start, digits);
                }
                return try self.lexNumber();
            },
            'a'...'z', 'A'...'Z', '_' => {
                const quote_follows = self.peekChar(1) == '\'';
                // PG escape-string prefix `E'...'` / `e'...'`.
                if ((ch == 'E' or ch == 'e') and quote_follows and self.dialect != .mysql)
                    return try self.lexEscapeString();
                if ((ch == 'N' or ch == 'n') and quote_follows) {
                    self.pos += 1;
                    return try self.lexString(start);
                }
                if (quote_follows and self.dialect != .postgres) {
                    if (ch == 'X' or ch == 'x') {
                        const bytes = try self.scanHexQuoted();
                        return Token{ .tag = .string, .text = self.src[start..self.pos], .value = .{ .string = bytes } };
                    }
                    if (ch == 'B' or ch == 'b') return try self.bitIntegerToken(start, try self.scanBitQuoted());
                }
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

    pub fn skipWhitespaceAndComments(self: *Lexer) LexError!void {
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

    /// Cursor is on the opening `'`; `start` is where the token began, before
    /// an `N` national-string prefix or a charset introducer.
    fn lexString(self: *Lexer, start: usize) LexError!Token {
        // MySQL processes C-style backslash escapes in ordinary string
        // literals; PG/neutral treat backslash literally (standard SQL,
        // standard_conforming_strings on). `''` always escapes a quote.
        const content = try self.scanQuotedContent('\'', self.dialect == .mysql);
        return try self.joinAdjacentStrings(start, content);
    }

    /// MySQL reads quoted strings separated only by whitespace or comments
    /// as one literal: `'a' 'b'` is `'ab'`. PostgreSQL joins them only
    /// across a newline, so the other dialects keep them apart.
    fn joinAdjacentStrings(self: *Lexer, start: usize, first: []const u8) LexError!Token {
        var content = first;
        while (self.dialect == .mysql) {
            const before_gap = self.pos;
            try self.skipWhitespaceAndComments();
            const c = self.peekChar(0);
            const piece = if (c == '\'' or c == '"')
                try self.scanQuotedContent(c.?, true)
            else {
                self.pos = before_gap;
                break;
            };
            content = try std.mem.concat(self.arena, u8, &.{ content, piece });
        }
        return Token{ .tag = .string, .text = self.src[start..self.pos], .value = .{ .string = content } };
    }

    /// PG escape-string `E'...'` — backslash escapes are always processed
    /// regardless of dialect. Cursor is on the `E`/`e`.
    fn lexEscapeString(self: *Lexer) LexError!Token {
        const start = self.pos;
        self.pos += 1; // skip the E prefix
        const content = try self.scanQuotedContent('\'', true);
        return Token{ .tag = .string, .text = self.src[start..self.pos], .value = .{ .string = content } };
    }

    /// Cursor is on the opening `quote`. Consume through the closing one and
    /// return the (un-escaped) content. A doubled quote is always a literal
    /// quote; when `process_backslash` is set, `\x` C-style escapes are
    /// honored.
    fn scanQuotedContent(self: *Lexer, quote: u8, process_backslash: bool) LexError![]const u8 {
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
            if (c == quote) {
                if (self.peekChar(1) == quote) {
                    needs_build = true;
                    self.pos += 2;
                    continue;
                }
                const raw = self.src[body_start..self.pos];
                self.pos += 1; // closing quote
                if (!needs_build) return raw;
                return try self.buildUnescaped(raw, quote, process_backslash);
            }
            self.pos += 1;
        }
        return LexError.LexUnterminatedString;
    }

    fn buildUnescaped(self: *Lexer, raw: []const u8, quote: u8, process_backslash: bool) LexError![]const u8 {
        const buf = try self.arena.alloc(u8, raw.len);
        var out: usize = 0;
        var i: usize = 0;
        while (i < raw.len) : (i += 1) {
            const c = raw[i];
            if (c == quote and i + 1 < raw.len and raw[i + 1] == quote) {
                buf[out] = quote;
                out += 1;
                i += 1;
                continue;
            }
            if (c == '\\' and process_backslash and i + 1 < raw.len) {
                i += 1;
                const e = raw[i];
                // Keep the backslash before a digit so regex backreferences
                // (\1..\9, e.g. REGEXP_REPLACE(x, pat, '\1')) reach the regex
                // engine intact, matching DuckDB/PostgreSQL — which never treat
                // backslash as an escape in ordinary strings. Real C escapes
                // (\n \t \r \0 \b \Z) still process for MySQL-client parity.
                // \% and \_ keep their backslash too, as in MySQL: LIKE reads
                // them as a literal % and _.
                if ((e >= '1' and e <= '9') or e == '%' or e == '_') {
                    buf[out] = '\\';
                    buf[out + 1] = e;
                    out += 2;
                    continue;
                }
                buf[out] = switch (e) {
                    'n' => '\n',
                    't' => '\t',
                    'r' => '\r',
                    '0' => 0,
                    'b' => 8,
                    'Z' => 26,
                    else => e, // \\, \', \", \<other> → the literal char
                };
                out += 1;
                continue;
            }
            buf[out] = c;
            out += 1;
        }
        return buf[0..out];
    }

    /// `1`, `1.5`, `.5`, and with an exponent `1e3`, `2.5E-3`, `.5e+2`. A
    /// fraction or an exponent makes a DOUBLE, as in MySQL, StarRocks and
    /// DuckDB; a value beyond the double range is an error, not ±inf.
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
        const is_float = self.lexExponent() or seen_dot;
        const text = self.src[start..self.pos];
        if (is_float) {
            const v = std.fmt.parseFloat(f64, text) catch return LexError.LexInvalidNumber;
            if (!std.math.isFinite(v)) return LexError.LexInvalidNumber;
            return Token{ .tag = .floating, .text = text, .value = .{ .floating = v } };
        } else {
            const v = std.fmt.parseInt(i64, text, 10) catch |err| switch (err) {
                error.Overflow => return Token{ .tag = .big_integer, .text = text, .value = .{ .big_integer = std.mem.trimStart(u8, text, "0") } },
                error.InvalidCharacter => return LexError.LexInvalidNumber,
            };
            return Token{ .tag = .integer, .text = text, .value = .{ .integer = v } };
        }
    }

    /// MySQL `0x41`: the bytes its hex digits spell, an odd count padded
    /// with a leading zero. Null when no hex digit follows the `0x`, which
    /// leaves `0xyz` reading as `0 AS xyz` like `1e` reads as `1 AS e`.
    /// Cursor is on the `0`.
    fn scanHexNumber(self: *Lexer) LexError!?[]const u8 {
        if (self.peekChar(1) != 'x') return null;
        const digits = self.digitRun(self.pos + 2, std.ascii.isHex);
        if (digits.len == 0) return null;
        try self.rejectIdentTail(self.pos + 2 + digits.len);
        self.pos += 2 + digits.len;
        return try self.hexBytes(digits);
    }

    /// `X'41'`: an even number of hex digits. Cursor is on the `X`.
    fn scanHexQuoted(self: *Lexer) LexError![]const u8 {
        const digits = try self.quotedDigits(std.ascii.isHex);
        if (digits.len % 2 != 0) return LexError.LexInvalidNumber;
        return try self.hexBytes(digits);
    }

    /// MySQL `0b101`: the binary digits, or null when none follows the `0b`.
    /// Cursor is on the `0`.
    fn scanBitNumber(self: *Lexer) LexError!?[]const u8 {
        if (self.peekChar(1) != 'b') return null;
        const digits = self.digitRun(self.pos + 2, isBitDigit);
        if (digits.len == 0) return null;
        try self.rejectIdentTail(self.pos + 2 + digits.len);
        self.pos += 2 + digits.len;
        return digits;
    }

    /// `b'101'`. Cursor is on the `b`.
    fn scanBitQuoted(self: *Lexer) LexError![]const u8 {
        return try self.quotedDigits(isBitDigit);
    }

    fn digitRun(self: *Lexer, from: usize, comptime is_digit: fn (u8) bool) []const u8 {
        var end = from;
        while (end < self.src.len and is_digit(self.src[end])) end += 1;
        return self.src[from..end];
    }

    /// A digit run running straight into a name (`0x41g`, `0b102`) is no
    /// literal; MySQL reads it as an identifier, which thinDB's names never
    /// start with a digit to allow.
    fn rejectIdentTail(self: *Lexer, end: usize) LexError!void {
        if (end < self.src.len and (std.ascii.isAlphanumeric(self.src[end]) or self.src[end] == '_'))
            return LexError.LexInvalidNumber;
    }

    /// Cursor is on the prefix letter before the opening `'`; every byte up
    /// to the closing `'` must satisfy `is_digit`.
    fn quotedDigits(self: *Lexer, comptime is_digit: fn (u8) bool) LexError![]const u8 {
        const body_start = self.pos + 2;
        const close = std.mem.indexOfScalarPos(u8, self.src, body_start, '\'') orelse return LexError.LexUnterminatedString;
        const digits = self.src[body_start..close];
        for (digits) |d| if (!is_digit(d)) return LexError.LexInvalidNumber;
        self.pos = close + 1;
        return digits;
    }

    fn hexBytes(self: *Lexer, digits: []const u8) LexError![]const u8 {
        const bytes = try self.arena.alloc(u8, (digits.len + 1) / 2);
        const pad = digits.len % 2;
        for (bytes, 0..) |*b, i| {
            const hi: u8 = if (i == 0 and pad == 1) 0 else hexValue(digits[2 * i - pad]);
            b.* = hi << 4 | hexValue(digits[2 * i + 1 - pad]);
        }
        return bytes;
    }

    /// A bit-value literal reads as its unsigned integer: thinDB stores BIT
    /// columns as BOOLEAN / BIGINT, the types these literals are written
    /// for. Past BIGINT it is a `.big_integer`; past 64 bits, an error.
    fn bitIntegerToken(self: *Lexer, start: usize, digits: []const u8) LexError!Token {
        const text = self.src[start..self.pos];
        const significant = std.mem.trimStart(u8, digits, "0");
        if (significant.len > 64) return LexError.LexInvalidNumber;
        const v: u64 = if (significant.len == 0) 0 else std.fmt.parseInt(u64, significant, 2) catch return LexError.LexInvalidNumber;
        if (std.math.cast(i64, v)) |small| return Token{ .tag = .integer, .text = text, .value = .{ .integer = small } };
        return Token{ .tag = .big_integer, .text = text, .value = .{ .big_integer = try std.fmt.allocPrint(self.arena, "{d}", .{v}) } };
    }

    /// The bytes of a bit-value literal read as a string (after a charset
    /// introducer): the value left-padded to whole bytes.
    fn bitBytes(self: *Lexer, digits: []const u8) LexError![]const u8 {
        const bytes = try self.arena.alloc(u8, (digits.len + 7) / 8);
        @memset(bytes, 0);
        for (digits, 0..) |d, i| {
            const bit = digits.len - 1 - i;
            if (d == '1') bytes[bytes.len - 1 - bit / 8] |= @as(u8, 1) << @intCast(bit % 8);
        }
        return bytes;
    }

    /// Consume an exponent (`e` or `E`, an optional sign, digits) when one
    /// follows the mantissa. Without a digit the `e` is not part of the
    /// number, so `SELECT 1e` still reads as `1 AS e`.
    fn lexExponent(self: *Lexer) bool {
        const c = self.peekChar(0) orelse return false;
        if (c != 'e' and c != 'E') return false;
        var end = self.pos + 1;
        if (end < self.src.len and (self.src[end] == '+' or self.src[end] == '-')) end += 1;
        if (end >= self.src.len or !std.ascii.isDigit(self.src[end])) return false;
        while (end < self.src.len and std.ascii.isDigit(self.src[end])) end += 1;
        self.pos = end;
        return true;
    }

    /// Whether the byte before `pos` ends a name or a parenthesized
    /// expression, where a following dot qualifies rather than starts a
    /// number.
    fn followsName(self: *Lexer, pos: usize) bool {
        if (pos == 0) return false;
        const prev = self.src[pos - 1];
        return std.ascii.isAlphanumeric(prev) or prev == '_' or prev == '`' or prev == '"' or prev == ')' or prev == ']';
    }

    fn lexIdent(self: *Lexer) LexError!Token {
        const start = self.pos;
        while (self.pos < self.src.len) : (self.pos += 1) {
            const c = self.src[self.pos];
            if (!std.ascii.isAlphanumeric(c) and c != '_') break;
        }
        const text = self.src[start..self.pos];
        if (keywordFor(text)) |kw| return Token{ .tag = kw, .text = text };
        if (text[0] == '_' and self.dialect != .postgres) {
            if (charsetOf(text[1..])) |charset| {
                if (try self.lexIntroducedLiteral(start, charset)) |tok| return tok;
            }
        }
        // Identifiers keep the case the client typed: MySQL/PG echo names
        // as-typed in result columns while COMPARING case-insensitively.
        // Every downstream match must therefore be case-insensitive
        // (columnNameEql / eqlIgnoreCase / lowercase-at-map-boundary) —
        // lowering here made grouped output labels silently lowercase.
        return Token{ .tag = .identifier, .text = text };
    }

    /// MySQL charset introducer `_utf8mb4'abc'`, `_binary X'41'`: the
    /// literal that follows, read as a string. Null when no string, hex or
    /// bit literal follows, so `_latin1` stays an ordinary name. Cursor is
    /// just past the introducer.
    fn lexIntroducedLiteral(self: *Lexer, start: usize, charset: Charset) LexError!?Token {
        const after_name = self.pos;
        try self.skipWhitespaceAndComments();
        const c = self.peekChar(0) orelse 0;
        const quote_follows = self.peekChar(1) == '\'';
        const content: []const u8 = blk: {
            if (c == '\'') break :blk (try self.lexString(start)).value.string;
            if (c == '"' and self.dialect == .mysql) break :blk (try self.lexDoubleQuoted()).value.string;
            if ((c == 'X' or c == 'x') and quote_follows) break :blk try self.scanHexQuoted();
            if ((c == 'B' or c == 'b') and quote_follows) break :blk try self.bitBytes(try self.scanBitQuoted());
            if (c == '0') {
                if (try self.scanHexNumber()) |bytes| break :blk bytes;
                if (try self.scanBitNumber()) |digits| break :blk try self.bitBytes(digits);
            }
            self.pos = after_name;
            return null;
        };
        try charset.admit(content);
        return Token{ .tag = .string, .text = self.src[start..self.pos], .value = .{ .string = content } };
    }

    /// PG dollar-quoted string: `$tag$ ... $tag$` (tag may be empty:
    /// `$$ ... $$`). Contents are raw — no escape processing. Cursor is on
    /// the opening `$`.
    fn lexDollarQuote(self: *Lexer) LexError!Token {
        const start = self.pos;
        self.pos += 1; // opening $
        while (self.pos < self.src.len and self.src[self.pos] != '$') {
            const c = self.src[self.pos];
            if (!(std.ascii.isAlphanumeric(c) or c == '_')) return LexError.LexUnexpectedChar;
            self.pos += 1;
        }
        if (self.pos >= self.src.len) return LexError.LexUnterminatedString;
        self.pos += 1; // closing $ of the opening delimiter
        const delim = self.src[start..self.pos]; // "$tag$"
        const content_start = self.pos;
        while (self.pos + delim.len <= self.src.len) {
            if (std.mem.eql(u8, self.src[self.pos .. self.pos + delim.len], delim)) {
                const content = self.src[content_start..self.pos];
                self.pos += delim.len;
                return Token{ .tag = .string, .text = self.src[start..self.pos], .value = .{ .string = content } };
            }
            self.pos += 1;
        }
        return LexError.LexUnterminatedString;
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
        const system = self.pos < self.src.len and self.src[self.pos] == '@';
        if (system) self.pos += 1;
        const name_start = self.pos;
        while (self.pos < self.src.len) : (self.pos += 1) {
            const c = self.src[self.pos];
            if (!(std.ascii.isAlphanumeric(c) or c == '_' or (system and c == '.'))) break;
        }
        const name = self.src[name_start..self.pos];
        if (name.len == 0) return LexError.LexUnexpectedChar;
        return Token{ .tag = if (system) .system_variable else .at_identifier, .text = name };
    }

    /// `"..."`. On MySQL this is a string literal, backslash escapes and all,
    /// the same as `'...'`; on PG/neutral it is a delimited, case-preserving
    /// identifier. `""` escapes an embedded double-quote in both modes.
    fn lexDoubleQuoted(self: *Lexer) LexError!Token {
        const start = self.pos;
        if (self.dialect == .mysql) return try self.joinAdjacentStrings(start, try self.scanQuotedContent('"', true));
        const content = self.scanQuotedContent('"', false) catch |err| return switch (err) {
            LexError.LexUnterminatedString => LexError.LexUnterminatedIdentifier,
            else => err,
        };
        if (content.len == 0) return LexError.LexUnexpectedChar;
        return Token{ .tag = .identifier, .text = content, .quoted = true };
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
                return Token{ .tag = .identifier, .text = text, .quoted = true };
            }
        }
        return LexError.LexUnterminatedIdentifier;
    }
};

fn hexValue(c: u8) u8 {
    // Callers pass only bytes `std.ascii.isHex` accepted.
    return std.fmt.charToDigit(c, 16) catch unreachable;
}

fn isBitDigit(c: u8) bool {
    return c == '0' or c == '1';
}

/// How a charset introducer's literal maps onto thinDB's UTF-8 text, which
/// stores the literal's bytes without transcoding.
const Charset = enum {
    /// UTF-8 is thinDB's own encoding; binary is raw bytes.
    verbatim,
    /// Agrees with UTF-8 on ASCII, so ASCII content reads the same.
    ascii_compatible,
    /// UCS-2 / UTF-16 / UTF-32: no byte reads the same as in UTF-8.
    wide,

    fn admit(self: Charset, bytes: []const u8) LexError!void {
        switch (self) {
            .verbatim => {},
            .ascii_compatible => for (bytes) |b| {
                if (b >= 0x80) return LexError.LexCharsetUnsupported;
            },
            .wide => return LexError.LexCharsetUnsupported,
        }
    }
};

/// MySQL 8.4's character sets, by introducer name.
fn charsetOf(name: []const u8) ?Charset {
    const verbatim = [_][]const u8{ "binary", "utf8", "utf8mb3", "utf8mb4" };
    const wide = [_][]const u8{ "ucs2", "utf16", "utf16le", "utf32" };
    const ascii_compatible = [_][]const u8{
        "armscii8", "ascii",   "big5",   "cp1250", "cp1251",  "cp1256",   "cp1257",  "cp850",
        "cp852",    "cp866",   "cp932",  "dec8",   "eucjpms", "euckr",    "gb18030", "gb2312",
        "gbk",      "geostd8", "greek",  "hebrew", "hp8",     "keybcs2",  "koi8r",   "koi8u",
        "latin1",   "latin2",  "latin5", "latin7", "macce",   "macroman", "sjis",    "swe7",
        "tis620",   "ujis",
    };
    for (verbatim) |n| if (std.ascii.eqlIgnoreCase(name, n)) return .verbatim;
    for (wide) |n| if (std.ascii.eqlIgnoreCase(name, n)) return .wide;
    for (ascii_compatible) |n| if (std.ascii.eqlIgnoreCase(name, n)) return .ascii_compatible;
    return null;
}

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
        .{ .name = "cross", .tag = .kw_cross },
        .{ .name = "div", .tag = .kw_div },
        .{ .name = "inner", .tag = .kw_inner },
        .{ .name = "left", .tag = .kw_left },
        .{ .name = "right", .tag = .kw_right },
        .{ .name = "full", .tag = .kw_full },
        .{ .name = "outer", .tag = .kw_outer },
        .{ .name = "on", .tag = .kw_on },
        .{ .name = "having", .tag = .kw_having },
        .{ .name = "with", .tag = .kw_with },
        .{ .name = "materialized", .tag = .kw_materialized },
        .{ .name = "alter", .tag = .kw_alter },
        .{ .name = "rename", .tag = .kw_rename },
        .{ .name = "truncate", .tag = .kw_truncate },
        .{ .name = "add", .tag = .kw_add },
        .{ .name = "column", .tag = .kw_column },
        .{ .name = "to", .tag = .kw_to },
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
        .{ .name = "replace", .tag = .kw_replace },
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
        .{ .name = "regexp", .tag = .kw_regexp },
        .{ .name = "rlike", .tag = .kw_regexp },
        .{ .name = "in", .tag = .kw_in },
        .{ .name = "interval", .tag = .kw_interval },
        .{ .name = "union", .tag = .kw_union },
        .{ .name = "intersect", .tag = .kw_intersect },
        .{ .name = "except", .tag = .kw_except },
        .{ .name = "minus", .tag = .kw_except },
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

/// Whether `src` holds more than one statement: a `;` outside strings,
/// quoted names and comments with another token after it. Text that fails
/// to lex counts as one statement, left for the parser to reject.
pub fn isMultiStatement(arena: Allocator, src: []const u8, dialect: types.Dialect) Allocator.Error!bool {
    return (try splitStatements(arena, src, dialect)).len > 1;
}

/// The statements of `src`, split at each `;` outside strings, quoted
/// names and comments, empty ones dropped. Text that fails to lex is one
/// statement, left for the parser to reject.
pub fn splitStatements(arena: Allocator, src: []const u8, dialect: types.Dialect) Allocator.Error![]const []const u8 {
    var lex = Lexer.init(arena, src);
    lex.dialect = dialect;
    var statements: std.ArrayList([]const u8) = .empty;
    var start: usize = 0;
    var has_token = false;
    while (true) {
        const tok = lex.next() catch |err| switch (err) {
            error.OutOfMemory => return error.OutOfMemory,
            else => return try arena.dupe([]const u8, &.{src}),
        };
        switch (tok.tag) {
            .eof, .semicolon => {
                const end = if (tok.tag == .eof) src.len else lex.pos - 1;
                if (has_token) try statements.append(arena, std.mem.trim(u8, src[start..end], &std.ascii.whitespace));
                if (tok.tag == .eof) return statements.items;
                start = lex.pos;
                has_token = false;
            },
            else => has_token = true,
        }
    }
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

test "lexer: case-insensitive keywords + unquoted idents keep typed case" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    var lx = Lexer.init(arena.allocator(), "select Foo from Bar");
    try std.testing.expectEqual(@as(TokenTag, .kw_select), (try lx.next()).tag);
    const id1 = try lx.next();
    try std.testing.expectEqual(@as(TokenTag, .identifier), id1.tag);
    // Identifiers lex as-typed: result labels must echo the client's case
    // (MySQL/PG behavior); matching stays case-insensitive downstream.
    try std.testing.expectEqualStrings("Foo", id1.text);
    try std.testing.expectEqual(@as(TokenTag, .kw_from), (try lx.next()).tag);
    const id2 = try lx.next();
    try std.testing.expectEqual(@as(TokenTag, .identifier), id2.tag);
    try std.testing.expectEqualStrings("Bar", id2.text);
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
        var lx = Lexer.init(aa, "'a\\%b\\_c\\\\d'"); // SQL: 'a\%b\_c\\d'
        lx.dialect = .mysql;
        const s = try lx.next();
        try std.testing.expectEqualStrings("a\\%b\\_c\\d", s.value.string);
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

test "lexer: MySQL double-quoted strings escape like single-quoted ones" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const aa = arena.allocator();
    const cases = .{
        .{ "\"a\\nb\"", "a\nb" },
        .{ "\"say \\\"hi\\\"\"", "say \"hi\"" },
        .{ "\"a\"\"b\"", "a\"b" },
        .{ "\"it's\"", "it's" },
        .{ "\"a''b\"", "a''b" },
        .{ "\"\\1\"", "\\1" },
    };
    inline for (cases) |c| {
        var lx = Lexer.init(aa, c[0]);
        lx.dialect = .mysql;
        const s = try lx.next();
        try std.testing.expectEqual(@as(TokenTag, .string), s.tag);
        try std.testing.expectEqualStrings(c[1], s.value.string);
        try std.testing.expectEqual(@as(TokenTag, .eof), (try lx.next()).tag);
    }
    {
        var lx = Lexer.init(aa, "\"a\\nb\"");
        const s = try lx.next();
        try std.testing.expectEqual(@as(TokenTag, .identifier), s.tag);
        try std.testing.expectEqualStrings("a\\nb", s.text);
    }
    {
        var lx = Lexer.init(aa, "\"open");
        try std.testing.expectError(LexError.LexUnterminatedIdentifier, lx.next());
    }
    {
        var lx = Lexer.init(aa, "\"open");
        lx.dialect = .mysql;
        try std.testing.expectError(LexError.LexUnterminatedString, lx.next());
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

test "lexer: PG dollar-quoted strings; $N stays a placeholder" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const aa = arena.allocator();
    {
        var lx = Lexer.init(aa, "$$a'b\"c$$"); // neutral, raw contents
        const s = try lx.next();
        try std.testing.expectEqual(@as(TokenTag, .string), s.tag);
        try std.testing.expectEqualStrings("a'b\"c", s.value.string);
    }
    {
        var lx = Lexer.init(aa, "$tag$ hi $tag$");
        const s = try lx.next();
        try std.testing.expectEqualStrings(" hi ", s.value.string);
    }
    {
        var lx = Lexer.init(aa, "$1"); // numbered placeholder, not a quote
        const t = try lx.next();
        try std.testing.expectEqual(@as(TokenTag, .dollar_param), t.tag);
    }
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

test "lexer: operators including != <> <= >= <=> and the bitwise ones" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    var lx = Lexer.init(arena.allocator(), "a != b <> c <= d >= e < f > g = h <=> i & j && k | l ^ m << n >> ~o");
    const expected_tags = [_]TokenTag{
        .identifier, .neq,          .identifier, .neq,        .identifier, .lte,        .identifier,
        .gte,        .identifier,   .lt,         .identifier, .gt,         .identifier, .eq,
        .identifier, .null_safe_eq, .identifier, .amp,        .identifier, .amp_amp,    .identifier,
        .pipe,       .identifier,   .caret,      .identifier, .shl,        .identifier, .shr,
        .tilde,      .identifier,
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

test "lexer: scientific notation and a leading dot make float literals" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const cases = .{
        .{ "1e3", 1000.0 },
        .{ "1E3", 1000.0 },
        .{ "2.5E-3", 0.0025 },
        .{ "6.02e+23", 6.02e23 },
        .{ "1.e2", 100.0 },
        .{ ".5", 0.5 },
        .{ ".5e2", 50.0 },
        .{ "0e0", 0.0 },
        .{ "1e-400", 0.0 },
    };
    inline for (cases) |c| {
        var lx = Lexer.init(arena.allocator(), c[0]);
        const tok = try lx.next();
        try std.testing.expectEqual(@as(TokenTag, .floating), tok.tag);
        try std.testing.expectEqualStrings(c[0], tok.text);
        try std.testing.expectEqual(@as(f64, c[1]), tok.value.floating);
        try std.testing.expectEqual(@as(TokenTag, .eof), (try lx.next()).tag);
    }

    // An `e` without exponent digits is not part of the number, and a dot
    // after a name still qualifies it.
    const splits = .{
        .{ "1e", &[_]TokenTag{ .integer, .identifier } },
        .{ "1e+", &[_]TokenTag{ .integer, .identifier, .plus } },
        .{ "2.5e", &[_]TokenTag{ .floating, .identifier } },
        .{ "t.x", &[_]TokenTag{ .identifier, .dot, .identifier } },
        .{ "t.5", &[_]TokenTag{ .identifier, .dot, .integer } },
        .{ "f(1).5", &[_]TokenTag{ .identifier, .lparen, .integer, .rparen, .dot, .integer } },
        .{ "1e3-1", &[_]TokenTag{ .floating, .minus, .integer } },
        .{ "-.5", &[_]TokenTag{ .minus, .floating } },
    };
    inline for (splits) |c| {
        var lx = Lexer.init(arena.allocator(), c[0]);
        for (c[1]) |want| try std.testing.expectEqual(want, (try lx.next()).tag);
        try std.testing.expectEqual(@as(TokenTag, .eof), (try lx.next()).tag);
    }

    var overflow = Lexer.init(arena.allocator(), "1e400");
    try std.testing.expectError(LexError.LexInvalidNumber, overflow.next());
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
        .kw_select,  .star,     .kw_from,  .identifier, .kw_where,
        .identifier, .eq,       .question, .kw_and,     .identifier,
        .eq,         .question, .eof,
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
        .kw_select,  .star,         .kw_from,      .identifier, .kw_where,
        .identifier, .eq,           .dollar_param, .kw_and,     .identifier,
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

test "lexer: a bare $ errors" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    // On MySQL `$` is a stray char; on PG/neutral it opens a dollar-quote
    // that never closes. Both are errors.
    {
        var lx = Lexer.init(arena.allocator(), "$");
        lx.dialect = .mysql;
        try std.testing.expectError(LexError.LexUnexpectedChar, lx.next());
    }
    {
        var lx = Lexer.init(arena.allocator(), "$");
        try std.testing.expectError(LexError.LexUnterminatedString, lx.next());
    }
}

test "lexer: unterminated string errors cleanly" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    var lx = Lexer.init(arena.allocator(), "'open string");
    try std.testing.expectError(LexError.LexUnterminatedString, lx.next());
}

test "lexer: integers past BIGINT are big_integer tokens carrying their digits" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    var lx = Lexer.init(arena.allocator(), "9223372036854775807 9223372036854775808 0018446744073709551616");
    const max = try lx.next();
    try std.testing.expectEqual(@as(TokenTag, .integer), max.tag);
    try std.testing.expectEqual(@as(i64, std.math.maxInt(i64)), max.value.integer);
    const past = try lx.next();
    try std.testing.expectEqual(@as(TokenTag, .big_integer), past.tag);
    try std.testing.expectEqualStrings("9223372036854775808", past.value.big_integer);
    const padded = try lx.next();
    try std.testing.expectEqual(@as(TokenTag, .big_integer), padded.tag);
    try std.testing.expectEqualStrings("18446744073709551616", padded.value.big_integer);
    try std.testing.expectEqualStrings("0018446744073709551616", padded.text);
}

test "lexer: hex literals are byte strings on MySQL and neutral" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const cases = .{
        .{ "X'41'", "A" },
        .{ "x'4a4B'", "JK" },
        .{ "X''", "" },
        .{ "0x41", "A" },
        .{ "0x123", "\x01\x23" },
        .{ "0x0041", "\x00A" },
    };
    inline for (.{ types.Dialect.mysql, types.Dialect.neutral }) |dialect| {
        inline for (cases) |c| {
            var lx = Lexer.init(arena.allocator(), c[0]);
            lx.dialect = dialect;
            const tok = try lx.next();
            try std.testing.expectEqual(@as(TokenTag, .string), tok.tag);
            try std.testing.expectEqualStrings(c[1], tok.value.string);
            try std.testing.expectEqualStrings(c[0], tok.text);
            try std.testing.expectEqual(@as(TokenTag, .eof), (try lx.next()).tag);
        }
    }
    inline for (.{ "X'4'", "X'4G'", "0x41g", "X'41" }) |bad| {
        var lx = Lexer.init(arena.allocator(), bad);
        lx.dialect = .mysql;
        try std.testing.expect(std.meta.isError(lx.next()));
    }
    // No hex digit after `0x`: the `0` stays a number, like `1e`.
    var bare = Lexer.init(arena.allocator(), "0xyz");
    bare.dialect = .mysql;
    try std.testing.expectEqual(@as(TokenTag, .integer), (try bare.next()).tag);
    try std.testing.expectEqual(@as(TokenTag, .identifier), (try bare.next()).tag);
    // PostgreSQL keeps its own reading of `X'..'` and `0x..`.
    var pg = Lexer.init(arena.allocator(), "X'41'");
    pg.dialect = .postgres;
    try std.testing.expectEqual(@as(TokenTag, .identifier), (try pg.next()).tag);
}

test "lexer: isMultiStatement counts statements, not semicolons" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const cases = .{
        .{ "SELECT 1", false },
        .{ "SELECT 1;", false },
        .{ ";; SELECT 1 ;; -- done\n", false },
        .{ "SELECT ';' AS s; /* ; */", false },
        .{ "SELECT \"a;b\", `c;d`", false },
        .{ "SELECT 'unterminated; SELECT 2", false },
        .{ "SELECT 1;SELECT 2", true },
        .{ "SET NAMES utf8mb4; INSERT INTO t VALUES (1)", true },
    };
    inline for (cases) |c| {
        try std.testing.expectEqual(c[1], try isMultiStatement(arena.allocator(), c[0], .mysql));
    }
}

test "lexer: splitStatements cuts at semicolons outside quotes and comments" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const cases = .{
        .{ "SELECT 1", &[_][]const u8{"SELECT 1"} },
        .{ " SET a = 1 ;; SELECT ';' ; ", &[_][]const u8{ "SET a = 1", "SELECT ';'" } },
        .{ "SELECT $$a;b$$; /* ; */ SELECT 2 -- ;", &[_][]const u8{ "SELECT $$a;b$$", "/* ; */ SELECT 2 -- ;" } },
        .{ "SELECT 'open; SELECT 2", &[_][]const u8{"SELECT 'open; SELECT 2"} },
    };
    inline for (cases) |c| {
        const got = try splitStatements(arena.allocator(), c[0], .postgres);
        try std.testing.expectEqual(c[1].len, got.len);
        for (c[1], got) |want, have| try std.testing.expectEqualStrings(want, have);
    }
}

test "lexer: @@ names a system variable, scope included" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    var lx = Lexer.init(arena.allocator(), "@@session.sql_mode @@autocommit @v");
    const scoped = try lx.next();
    try std.testing.expectEqual(TokenTag.system_variable, scoped.tag);
    try std.testing.expectEqualStrings("session.sql_mode", scoped.text);
    const bare = try lx.next();
    try std.testing.expectEqual(TokenTag.system_variable, bare.tag);
    try std.testing.expectEqualStrings("autocommit", bare.text);
    const user = try lx.next();
    try std.testing.expectEqual(TokenTag.at_identifier, user.tag);
    try std.testing.expectEqualStrings("v", user.text);
}

test "lexer: bit-value literals are integers" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const cases = .{
        .{ "b'101'", 5 },
        .{ "B'0'", 0 },
        .{ "b''", 0 },
        .{ "0b101", 5 },
        .{ "0b0111111111111111111111111111111111111111111111111111111111111111", std.math.maxInt(i64) },
    };
    inline for (cases) |c| {
        var lx = Lexer.init(arena.allocator(), c[0]);
        lx.dialect = .mysql;
        const tok = try lx.next();
        try std.testing.expectEqual(@as(TokenTag, .integer), tok.tag);
        try std.testing.expectEqual(@as(i64, c[1]), tok.value.integer);
    }
    var wide = Lexer.init(arena.allocator(), "b'1111111111111111111111111111111111111111111111111111111111111111'");
    wide.dialect = .mysql;
    const big = try wide.next();
    try std.testing.expectEqual(@as(TokenTag, .big_integer), big.tag);
    try std.testing.expectEqualStrings("18446744073709551615", big.value.big_integer);
    inline for (.{ "b'102'", "0b102", "b'11111111111111111111111111111111111111111111111111111111111111111'" }) |bad| {
        var lx = Lexer.init(arena.allocator(), bad);
        lx.dialect = .mysql;
        try std.testing.expectError(LexError.LexInvalidNumber, lx.next());
    }
}

test "lexer: national strings and adjacent string concatenation" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const cases = .{
        .{ "N'abc'", "abc" },
        .{ "'a' 'b'", "ab" },
        .{ "'a'\n/* gap */ 'b' \"c\"", "abc" },
        .{ "N'a' 'b'", "ab" },
    };
    inline for (cases) |c| {
        var lx = Lexer.init(arena.allocator(), c[0]);
        lx.dialect = .mysql;
        const tok = try lx.next();
        try std.testing.expectEqual(@as(TokenTag, .string), tok.tag);
        try std.testing.expectEqualStrings(c[1], tok.value.string);
        try std.testing.expectEqual(@as(TokenTag, .eof), (try lx.next()).tag);
    }
    // A string followed by a name keeps its own extent.
    var alias = Lexer.init(arena.allocator(), "'a' x");
    alias.dialect = .mysql;
    try std.testing.expectEqualStrings("'a'", (try alias.next()).text);
    inline for (.{ types.Dialect.postgres, types.Dialect.neutral }) |dialect| {
        var lx = Lexer.init(arena.allocator(), "'a' 'b'");
        lx.dialect = dialect;
        try std.testing.expectEqualStrings("a", (try lx.next()).value.string);
        try std.testing.expectEqualStrings("b", (try lx.next()).value.string);
    }
}

test "lexer: charset introducers read the literal that follows as a string" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const cases = .{
        .{ "_utf8mb4'abc'", "abc" },
        .{ "_UTF8MB4 'abc' 'd'", "abcd" },
        .{ "_latin1'x'", "x" },
        .{ "_binary X'41'", "A" },
        .{ "_binary 0x4142", "AB" },
        .{ "_utf8mb4 b'1000001'", "A" },
        .{ "_binary b'0000000001000001'", "\x00A" },
        .{ "_utf8mb4 \"q\"", "q" },
    };
    inline for (cases) |c| {
        var lx = Lexer.init(arena.allocator(), c[0]);
        lx.dialect = .mysql;
        const tok = try lx.next();
        try std.testing.expectEqual(@as(TokenTag, .string), tok.tag);
        try std.testing.expectEqualStrings(c[1], tok.value.string);
        try std.testing.expectEqualStrings(c[0], tok.text);
        try std.testing.expectEqual(@as(TokenTag, .eof), (try lx.next()).tag);
    }
    // Without a literal after it, or with an unknown charset, it is a name.
    inline for (.{ "_latin1 + 1", "_foo'x'" }) |src| {
        var lx = Lexer.init(arena.allocator(), src);
        lx.dialect = .mysql;
        try std.testing.expectEqual(@as(TokenTag, .identifier), (try lx.next()).tag);
    }
    // thinDB stores literal bytes as UTF-8 text without transcoding.
    inline for (.{ "_latin1 X'E9'", "_utf16'ab'" }) |src| {
        var lx = Lexer.init(arena.allocator(), src);
        lx.dialect = .mysql;
        try std.testing.expectError(LexError.LexCharsetUnsupported, lx.next());
    }
}
