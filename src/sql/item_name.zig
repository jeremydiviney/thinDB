//! Default names for unaliased SELECT items, as each dialect's clients see
//! them. MySQL names an item by its source text; PostgreSQL by the rules of
//! its `FigureColname`. Both work from the item's text, re-lexed with the
//! session's lexer settings, so the name never depends on how the parser
//! lowered the expression.

const std = @import("std");
const Allocator = std.mem.Allocator;
const lexer = @import("lexer.zig");
const Lexer = lexer.Lexer;
const Token = lexer.Token;
const TokenTag = lexer.TokenTag;

/// MySQL cuts a generated column name at 255 bytes.
const MYSQL_MAX_NAME_BYTES = 255;

/// PostgreSQL's name for a column it can't name.
pub const POSTGRES_UNNAMED = "?column?";

/// The whitespace MySQL strips from the front of a string literal's name.
const MYSQL_NAME_SPACE = " \t\n\r\x0b\x0c";

/// MySQL's name for an unaliased item: its source text, except that a
/// literal or a column, alone or in parentheses, is named by itself. A
/// string literal is named by its value, NULL by `NULL`, a number by its
/// spelling, a column by its bare name. `template` carries the session's
/// lexer settings.
pub fn mysqlName(arena: Allocator, template: Lexer, text: []const u8) Allocator.Error![]const u8 {
    const tokens = lexTokens(arena, template, text) catch |err| switch (err) {
        error.OutOfMemory => return error.OutOfMemory,
        else => return truncated(text),
    };
    const inner = unwrapParens(tokens);
    if (columnChainEnd(inner, 0) == inner.len) return truncated(inner[inner.len - 1].text);
    if (inner.len == 1) switch (inner[0].tag) {
        .string => if (isQuotedString(inner[0].text)) {
            return truncated(std.mem.trimStart(u8, inner[0].value.string, MYSQL_NAME_SPACE));
        },
        .integer, .big_integer, .floating => return truncated(inner[0].text),
        .kw_null => return "NULL",
        else => {},
    };
    return truncated(text);
}

/// PostgreSQL's name for an unaliased item: a column's or function's own
/// name, a cast's type when its operand has none, `case`, `exists`, and
/// `?column?` for anything else. `subquery_name` is the first output of a
/// scalar subquery that makes up the whole item.
pub fn postgresName(arena: Allocator, template: Lexer, text: []const u8, subquery_name: ?[]const u8) Allocator.Error![]const u8 {
    const tokens = lexTokens(arena, template, text) catch |err| switch (err) {
        error.OutOfMemory => return error.OutOfMemory,
        else => return POSTGRES_UNNAMED,
    };
    const figurer: Figurer = .{ .arena = arena, .subquery_name = subquery_name };
    return (try figurer.figure(tokens, true)).name orelse POSTGRES_UNNAMED;
}

fn lexTokens(arena: Allocator, template: Lexer, text: []const u8) lexer.LexError![]const Token {
    var lx = template;
    lx.src = text;
    lx.pos = 0;
    var tokens: std.ArrayList(Token) = .empty;
    while (true) {
        const tok = try lx.next();
        if (tok.tag == .eof) break;
        try tokens.append(arena, tok);
    }
    return tokens.items;
}

fn truncated(name: []const u8) []const u8 {
    if (name.len <= MYSQL_MAX_NAME_BYTES) return name;
    var end: usize = MYSQL_MAX_NAME_BYTES;
    while (end > 0 and name[end] & 0xC0 == 0x80) end -= 1;
    return name[0..end];
}

/// A string literal written in quotes (with an optional `N`, `E` or charset
/// introducer prefix), as opposed to a hex or bit literal, which MySQL names
/// by its spelling.
fn isQuotedString(text: []const u8) bool {
    if (text.len == 0) return false;
    switch (text[0]) {
        '\'', '"' => return true,
        'N', 'n', 'E', 'e' => return text.len > 1 and text[1] == '\'',
        '_' => {
            var i: usize = 1;
            while (i < text.len and (std.ascii.isAlphanumeric(text[i]) or text[i] == '_')) i += 1;
            while (i < text.len and std.ascii.isWhitespace(text[i])) i += 1;
            return i < text.len and (text[i] == '\'' or text[i] == '"');
        },
        else => return false,
    }
}

/// `tokens` without parentheses that wrap all of it.
fn unwrapParens(tokens: []const Token) []const Token {
    var inner = tokens;
    while (inner.len >= 2 and inner[0].tag == .lparen and closingParen(inner, 0) == inner.len - 1) {
        inner = inner[1 .. inner.len - 1];
    }
    return inner;
}

/// The index of the `)` closing the `(` at `open`.
fn closingParen(tokens: []const Token, open: usize) ?usize {
    var depth: usize = 0;
    for (tokens[open..], open..) |tok, i| switch (tok.tag) {
        .lparen => depth += 1,
        .rparen => {
            depth -= 1;
            if (depth == 0) return i;
        },
        else => {},
    };
    return null;
}

/// The end of the `name(.name)*` chain starting at `start`, or `start` when
/// no identifier starts there.
fn columnChainEnd(tokens: []const Token, start: usize) usize {
    if (start >= tokens.len or tokens[start].tag != .identifier) return start;
    var end = start + 1;
    while (end + 1 < tokens.len and tokens[end].tag == .dot and tokens[end + 1].tag == .identifier) end += 2;
    return end;
}

fn isWord(tok: Token, word: []const u8) bool {
    return tok.tag == .identifier and !tok.quoted and std.ascii.eqlIgnoreCase(tok.text, word);
}

const Figure = struct {
    name: ?[]const u8 = null,
    /// FigureColname's strength: 2 for a name the expression owns (a column,
    /// a function), 1 for a fallback (a type name, `case`), 0 for none.
    strength: u2 = 0,
};

const Figurer = struct {
    arena: Allocator,
    subquery_name: ?[]const u8,

    fn figure(self: Figurer, tokens: []const Token, whole_item: bool) Allocator.Error!Figure {
        var toks = tokens;
        while (toks.len >= 2 and toks[0].tag == .lparen and closingParen(toks, 0) == toks.len - 1) {
            toks = toks[1 .. toks.len - 1];
            if (startsQuery(toks)) {
                if (whole_item) if (self.subquery_name) |name| return .{ .name = name, .strength = 2 };
                return .{};
            }
        }
        if (toks.len == 0) return .{};
        var i: usize = 0;
        var fig = try self.primary(toks, &i) orelse return .{};
        while (i < toks.len) {
            if (toks[i].tag == .coloncolon) {
                const end = typeEnd(toks, i + 1);
                if (fig.strength < 2) fig = .{ .name = try self.typeName(toks[i + 1 .. end]), .strength = 1 };
                i = end;
            } else if (isWord(toks[i], "collate") and i + 1 < toks.len) {
                i = @max(columnChainEnd(toks, i + 1), i + 2);
            } else return .{};
        }
        return fig;
    }

    /// The operand at `i.*`, advancing past it; null when the tokens there
    /// begin an operator expression rather than a primary.
    fn primary(self: Figurer, toks: []const Token, i: *usize) Allocator.Error!?Figure {
        const tok = toks[i.*];
        switch (tok.tag) {
            .lparen => {
                const close = closingParen(toks, i.*) orelse return null;
                const inner = toks[i.* + 1 .. close];
                i.* = close + 1;
                if (startsQuery(inner)) return .{};
                return try self.figure(inner, false);
            },
            .kw_case => return try self.caseFigure(toks, i),
            .kw_exists => {
                i.* += 1;
                if (i.* < toks.len and toks[i.*].tag == .lparen) i.* = (closingParen(toks, i.*) orelse return null) + 1;
                return .{ .name = "exists", .strength = 2 };
            },
            .kw_true, .kw_false => {
                i.* += 1;
                return .{ .name = "bool", .strength = 1 };
            },
            .kw_null, .integer, .big_integer, .floating, .string => {
                i.* += 1;
                return .{};
            },
            .kw_interval => {
                i.* += 1;
                if (i.* < toks.len and toks[i.*].tag == .string) {
                    i.* += 1;
                    while (i.* < toks.len and (toks[i.*].tag == .kw_to or isIntervalUnit(toks[i.*]))) i.* += 1;
                    return .{ .name = "interval", .strength = 1 };
                }
                if (i.* < toks.len and toks[i.*].tag == .lparen) return try self.callFigure(toks, i, tok);
                return null;
            },
            .identifier => {
                const end = columnChainEnd(toks, i.*);
                const last = toks[end - 1];
                if (end < toks.len and toks[end].tag == .lparen) {
                    i.* = end;
                    return try self.callFigure(toks, i, last);
                }
                if (end == i.* + 1 and end < toks.len and toks[end].tag == .string) {
                    i.* = end + 1;
                    return .{ .name = try self.typeName(toks[end - 1 .. end]), .strength = 1 };
                }
                i.* = end;
                return .{ .name = try self.folded(last), .strength = 2 };
            },
            else => {
                if (!isKeyword(tok.tag) or i.* + 1 >= toks.len or toks[i.* + 1].tag != .lparen) return null;
                i.* += 1;
                return try self.callFigure(toks, i, tok);
            },
        }
    }

    /// A call whose name is `name` and whose `(` is at `i.*`, advancing past
    /// its arguments and any aggregate or window clauses after them.
    fn callFigure(self: Figurer, toks: []const Token, i: *usize, name: Token) Allocator.Error!?Figure {
        const close = closingParen(toks, i.*) orelse return null;
        const args = toks[i.* + 1 .. close];
        i.* = close + 1;
        skipCallClauses(toks, i);
        const fname = try self.folded(name);
        if (std.mem.eql(u8, fname, "cast")) return try self.castFigure(args);
        if (std.mem.eql(u8, fname, "trim")) {
            const side = if (args.len > 0 and isWord(args[0], "leading"))
                "ltrim"
            else if (args.len > 0 and isWord(args[0], "trailing"))
                "rtrim"
            else
                "btrim";
            return .{ .name = side, .strength = 2 };
        }
        return .{ .name = fname, .strength = 2 };
    }

    /// `CAST(operand AS type)`: the operand's name when it owns one, else the
    /// type's.
    fn castFigure(self: Figurer, args: []const Token) Allocator.Error!?Figure {
        var depth: usize = 0;
        for (args, 0..) |tok, k| switch (tok.tag) {
            .lparen => depth += 1,
            .rparen => depth -= 1,
            .kw_as => if (depth == 0) {
                const operand = try self.figure(args[0..k], false);
                if (operand.strength == 2) return operand;
                return .{ .name = try self.typeName(args[k + 1 ..]), .strength = 1 };
            },
            else => {},
        };
        return null;
    }

    /// `CASE ... END`: its ELSE result's name when that owns one, else `case`.
    fn caseFigure(self: Figurer, toks: []const Token, i: *usize) Allocator.Error!?Figure {
        var depth: usize = 0;
        var else_at: ?usize = null;
        var k = i.*;
        while (k < toks.len) : (k += 1) switch (toks[k].tag) {
            .kw_case => depth += 1,
            .kw_else => if (depth == 1) {
                else_at = k;
            },
            .kw_end => {
                depth -= 1;
                if (depth == 0) break;
            },
            else => {},
        };
        if (k == toks.len) return null;
        i.* = k + 1;
        if (else_at) |e| {
            const result = try self.figure(toks[e + 1 .. k], false);
            if (result.strength == 2) return result;
        }
        return .{ .name = "case", .strength = 1 };
    }

    /// PostgreSQL's name for a type as written: the internal name its SQL
    /// spelling maps to (`int4` for INTEGER, `bpchar` for CHAR), else the
    /// last part of the name.
    fn typeName(self: Figurer, toks: []const Token) Allocator.Error![]const u8 {
        var first: usize = 0;
        while (first + 2 < toks.len and toks[first + 1].tag == .dot) first += 2;
        if (first >= toks.len) return POSTGRES_UNNAMED;
        const word = try self.folded(toks[first]);
        const rest = toks[first + 1 ..];
        const varying = for (rest) |t| {
            if (isWord(t, "varying")) break true;
        } else false;
        const with_zone = for (rest, 0..) |t, k| {
            if (t.tag == .kw_with and k + 2 < rest.len and isWord(rest[k + 1], "time") and isWord(rest[k + 2], "zone")) break true;
        } else false;
        const renames = [_]struct { []const u8, []const u8 }{
            .{ "int", "int4" },      .{ "integer", "int4" },    .{ "smallint", "int2" },
            .{ "bigint", "int8" },   .{ "real", "float4" },     .{ "float", "float8" },
            .{ "double", "float8" }, .{ "decimal", "numeric" }, .{ "dec", "numeric" },
            .{ "boolean", "bool" },
        };
        for (renames) |r| if (std.mem.eql(u8, word, r[0])) return r[1];
        if (std.mem.eql(u8, word, "char") or std.mem.eql(u8, word, "character") or std.mem.eql(u8, word, "nchar")) {
            return if (varying) "varchar" else "bpchar";
        }
        if (std.mem.eql(u8, word, "bit")) return if (varying) "varbit" else "bit";
        if (std.mem.eql(u8, word, "timestamp")) return if (with_zone) "timestamptz" else "timestamp";
        if (std.mem.eql(u8, word, "time")) return if (with_zone) "timetz" else "time";
        return word;
    }

    /// An identifier as PostgreSQL stores it: folded to lower case unless
    /// quoted.
    fn folded(self: Figurer, tok: Token) Allocator.Error![]const u8 {
        if (tok.quoted) return tok.text;
        return try std.ascii.allocLowerString(self.arena, tok.text);
    }
};

/// A keyword that names a function when a `(` follows it (`LEFT(`,
/// `REPLACE(`, `ROW(`), unlike a prefix operator such as `NOT (`.
fn isKeyword(tag: TokenTag) bool {
    return switch (tag) {
        .kw_not, .kw_all, .kw_distinct, .kw_select, .kw_with => false,
        else => std.mem.startsWith(u8, @tagName(tag), "kw_"),
    };
}

fn startsQuery(toks: []const Token) bool {
    return toks.len > 0 and (toks[0].tag == .kw_select or toks[0].tag == .kw_with);
}

/// The end of the type name starting at `start` after a `::`: its words,
/// one parenthesized modifier list, and `WITH TIME ZONE`.
fn typeEnd(toks: []const Token, start: usize) usize {
    var end = start;
    var saw_modifiers = false;
    while (end < toks.len) {
        const tok = toks[end];
        if (tok.tag == .identifier and !isOperatorWord(tok)) {
            end += 1;
        } else if (tok.tag == .kw_interval) {
            end += 1;
        } else if (tok.tag == .dot and end + 1 < toks.len and toks[end + 1].tag == .identifier) {
            end += 2;
        } else if (tok.tag == .kw_with and end + 1 < toks.len and isWord(toks[end + 1], "time")) {
            end += 1;
        } else if (tok.tag == .lparen and !saw_modifiers and end > start) {
            end = (closingParen(toks, end) orelse return toks.len) + 1;
            saw_modifiers = true;
        } else break;
    }
    return end;
}

/// Identifier-spelled operators that can follow an operand.
fn isOperatorWord(tok: Token) bool {
    const words = [_][]const u8{ "ilike", "similar", "isnull", "notnull", "collate", "at", "escape" };
    for (words) |w| if (isWord(tok, w)) return true;
    return false;
}

fn isIntervalUnit(tok: Token) bool {
    const units = [_][]const u8{ "year", "month", "day", "hour", "minute", "second" };
    for (units) |u| if (isWord(tok, u)) return true;
    return false;
}

/// Past the clauses that may follow a call's arguments: `WITHIN GROUP (...)`,
/// `FILTER (...)`, `IGNORE NULLS` / `RESPECT NULLS` and `OVER (...)` or
/// `OVER name`.
fn skipCallClauses(toks: []const Token, i: *usize) void {
    while (i.* < toks.len) {
        const tok = toks[i.*];
        if (isWord(tok, "within") and i.* + 2 < toks.len and toks[i.* + 1].tag == .kw_group and toks[i.* + 2].tag == .lparen) {
            i.* = (closingParen(toks, i.* + 2) orelse return) + 1;
        } else if (isWord(tok, "filter") and i.* + 1 < toks.len and toks[i.* + 1].tag == .lparen) {
            i.* = (closingParen(toks, i.* + 1) orelse return) + 1;
        } else if ((tok.tag == .kw_ignore or tok.tag == .kw_respect) and i.* + 1 < toks.len and isWord(toks[i.* + 1], "nulls")) {
            i.* += 2;
        } else if (tok.tag == .kw_over and i.* + 1 < toks.len) {
            i.* = if (toks[i.* + 1].tag == .lparen) (closingParen(toks, i.* + 1) orelse return) + 1 else i.* + 2;
        } else return;
    }
}

test "mysqlName is the source text, or a lone literal's or column's own name" {
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();
    const template: Lexer = .{ .arena = arena, .src = "", .dialect = .mysql };
    const cases = .{
        .{ "2 + 3", "2 + 3" },
        .{ "a+1", "a+1" },
        .{ "CAST(x AS CHAR)", "CAST(x AS CHAR)" },
        .{ "'abc'", "abc" },
        .{ "\"abc\"", "abc" },
        .{ "('abc')", "abc" },
        .{ "'   ab  '", "ab  " },
        .{ "N'abc'", "abc" },
        .{ "_utf8mb4'abc'", "abc" },
        .{ "_binary X'41'", "_binary X'41'" },
        .{ "'a' 'b'", "ab" },
        .{ "X'41'", "X'41'" },
        .{ "0x41", "0x41" },
        .{ "NULL", "NULL" },
        .{ "(null)", "NULL" },
        .{ "true", "true" },
        .{ "(TRUE)", "(TRUE)" },
        .{ "1.50", "1.50" },
        .{ "((1e3))", "1e3" },
        .{ "-5", "-5" },
        .{ "(-5)", "(-5)" },
        .{ "(1+1)", "(1+1)" },
        .{ "(d.a)", "a" },
        .{ "(`a`)", "a" },
        .{ "CURRENT_DATE", "CURRENT_DATE" },
        .{ "(SELECT 1)", "(SELECT 1)" },
    };
    inline for (cases) |c| {
        try std.testing.expectEqualStrings(c[1], try mysqlName(arena, template, c[0]));
    }
}

test "mysqlName cuts a long name at 255 bytes, on a character boundary" {
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();
    const template: Lexer = .{ .arena = arena, .src = "", .dialect = .mysql };
    const ascii = try std.mem.concat(arena, u8, &.{ "CONCAT('", "a" ** 300, "')" });
    try std.testing.expectEqual(@as(usize, 255), (try mysqlName(arena, template, ascii)).len);
    const wide = try std.mem.concat(arena, u8, &.{ "CONCAT('", "\u{e9}" ** 200, "')" });
    const cut = try mysqlName(arena, template, wide);
    try std.testing.expectEqual(@as(usize, 254), cut.len);
    try std.testing.expect(std.unicode.utf8ValidateSlice(cut));
}

test "postgresName follows FigureColname" {
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();
    const template: Lexer = .{ .arena = arena, .src = "", .dialect = .postgres };
    const cases = .{
        .{ "COUNT(*)", "count" },
        .{ "count(DISTINCT a)", "count" },
        .{ "concat(s, 'x')", "concat" },
        .{ "pg_catalog.lower(s)", "lower" },
        .{ "\"MyFn\"(s)", "MyFn" },
        .{ "a + 1", "?column?" },
        .{ "-a", "?column?" },
        .{ "'abc'", "?column?" },
        .{ "1.50", "?column?" },
        .{ "NULL", "?column?" },
        .{ "TRUE", "bool" },
        .{ "a IS NULL", "?column?" },
        .{ "(a)", "a" },
        .{ "((count(*)))", "count" },
        .{ "count(*) + 1", "?column?" },
        .{ "CAST(1 AS CHAR)", "bpchar" },
        .{ "CAST(1 AS varchar(10))", "varchar" },
        .{ "CAST(a AS text)", "a" },
        .{ "CAST(count(*) AS bigint)", "count" },
        .{ "1::int", "int4" },
        .{ "'1'::double precision", "float8" },
        .{ "x::numeric(10, 2)", "x" },
        .{ "now()::timestamp with time zone", "now" },
        .{ "'2024-01-01'::timestamp with time zone", "timestamptz" },
        .{ "a::int + 1", "?column?" },
        .{ "DATE '2024-01-01'", "date" },
        .{ "INTERVAL '1' DAY", "interval" },
        .{ "CASE WHEN a THEN 1 ELSE 2 END", "case" },
        .{ "CASE WHEN a THEN 1 ELSE b END", "b" },
        .{ "CASE WHEN a THEN 1 END", "case" },
        .{ "CASE WHEN a THEN 1 ELSE CASE WHEN b THEN 2 ELSE c END END", "c" },
        .{ "EXISTS (SELECT 1)", "exists" },
        .{ "NOT EXISTS (SELECT 1)", "?column?" },
        .{ "COALESCE(a, 0)", "coalesce" },
        .{ "TRIM(s)", "btrim" },
        .{ "TRIM(LEADING 'x' FROM s)", "ltrim" },
        .{ "EXTRACT(year FROM d)", "extract" },
        .{ "LEFT(s, 2)", "left" },
        .{ "CURRENT_DATE", "current_date" },
        .{ "row_number() OVER (ORDER BY a)", "row_number" },
        .{ "sum(a) OVER w", "sum" },
        .{ "sum(a) FILTER (WHERE b > 1)", "sum" },
        .{ "sum(a) OVER (ORDER BY a) / 2", "?column?" },
        .{ "s COLLATE \"C\"", "s" },
        .{ "(SELECT a FROM t)", "a" },
    };
    inline for (cases) |c| {
        try std.testing.expectEqualStrings(c[1], try postgresName(arena, template, c[0], "a"));
    }
    try std.testing.expectEqualStrings("?column?", try postgresName(arena, template, "(SELECT 1)", null));
}
