//! SQL lexer — tokenizes a single SQL statement into a stream of
//! tokens for the parser. Keywords are case-insensitive. Unquoted
//! identifiers are case-folded to ASCII lowercase so MySQL clients
//! (with `lower_case_table_names=1`) and PG clients (which lowercase
//! unquoted) behave the same. Backtick-quoted identifiers preserve
//! case and may contain any non-backtick byte. String literals are
//! single-quoted with `''` for an embedded quote.

const std = @import("std");
const Allocator = std.mem.Allocator;

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
    kw_tables,
    kw_table,
    kw_primary,
    kw_key,
    kw_if,
    kw_exists,
    kw_into,
    kw_values,
    kw_insert,

    // Operators / punctuation.
    eq, // =
    neq, // != or <>
    lt, // <
    lte, // <=
    gt, // >
    gte, // >=
    star, // *
    comma, // ,
    dot, // .
    lparen, // (
    rparen, // )
    semicolon, // ;

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
            '`' => return try self.lexBacktickIdent(),
            '0'...'9' => return try self.lexNumber(),
            'a'...'z', 'A'...'Z', '_' => return try self.lexIdent(),
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
        const start = self.pos;
        self.pos += 1; // opening quote
        var contains_escape = false;
        while (self.pos < self.src.len) : (self.pos += 1) {
            if (self.src[self.pos] == '\'') {
                if (self.peekChar(1) == '\'') {
                    // SQL '' escape — record + skip both quotes.
                    contains_escape = true;
                    self.pos += 1; // skip first quote; loop's += 1 skips the second
                    continue;
                }
                // Closing quote.
                self.pos += 1;
                const raw = self.src[start + 1 .. self.pos - 1];
                const text = self.src[start..self.pos];
                if (!contains_escape) {
                    return Token{ .tag = .string, .text = text, .value = .{ .string = raw } };
                }
                // Un-escape '' → '. Allocate fresh in arena.
                var buf = try self.arena.alloc(u8, raw.len);
                var out_len: usize = 0;
                var i: usize = 0;
                while (i < raw.len) : (i += 1) {
                    buf[out_len] = raw[i];
                    out_len += 1;
                    if (raw[i] == '\'' and i + 1 < raw.len and raw[i + 1] == '\'') i += 1;
                }
                return Token{ .tag = .string, .text = text, .value = .{ .string = buf[0..out_len] } };
            }
        }
        return LexError.LexUnterminatedString;
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

    fn lexBacktickIdent(self: *Lexer) LexError!Token {
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
        .{ .name = "tables", .tag = .kw_tables },
        .{ .name = "table", .tag = .kw_table },
        .{ .name = "primary", .tag = .kw_primary },
        .{ .name = "key", .tag = .kw_key },
        .{ .name = "if", .tag = .kw_if },
        .{ .name = "exists", .tag = .kw_exists },
        .{ .name = "into", .tag = .kw_into },
        .{ .name = "values", .tag = .kw_values },
        .{ .name = "insert", .tag = .kw_insert },
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

test "lexer: unterminated string errors cleanly" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    var lx = Lexer.init(arena.allocator(), "'open string");
    try std.testing.expectError(LexError.LexUnterminatedString, lx.next());
}
