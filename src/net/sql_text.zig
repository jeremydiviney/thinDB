//! Shared SQL-text helpers used by both wire protocols' prepared-statement
//! paths. The MySQL and PG implementations rewrite SQL: substitute bound
//! parameter values as literals, re-parse, run. The placeholder syntax
//! differs (`?` vs `$N`) and the lexical rules differ slightly (PG
//! recognises double-quoted identifiers; MySQL recognises backticks) but
//! the inner string/comment-skipping walk and the literal-rendering
//! helpers are identical.
//!
//! Functions here are pure given an allocator: they read input bytes,
//! return an allocator-owned slice the caller frees. No retained state.

const std = @import("std");
const Allocator = std.mem.Allocator;

/// Per-protocol lexical knobs for `substituteWith`. Identifier quoting
/// is the only divergence between PG and MySQL outside the actual
/// placeholder syntax.
const QuoteRules = struct {
    /// True ↔ a double-quote opens a SQL-standard quoted identifier
    /// (PG). False ↔ double-quote is just data (MySQL — backticks open
    /// identifiers, double-quotes can appear in string contexts).
    double_quote_is_identifier: bool,
    /// True ↔ a backtick opens a MySQL-style quoted identifier. False
    /// for PG.
    backtick_is_identifier: bool,
};

const mysql_quotes: QuoteRules = .{
    .double_quote_is_identifier = false,
    .backtick_is_identifier = true,
};

const pg_quotes: QuoteRules = .{
    .double_quote_is_identifier = true,
    .backtick_is_identifier = true,
};

/// Trim surrounding whitespace + trailing `;`, then lowercase ASCII.
/// Both wires' canned-probe matchers normalize input identically; this
/// is the shared helper. Caller owns the returned slice.
pub fn normalizeForCannedMatch(allocator: Allocator, sql: []const u8) ![]u8 {
    var s = stripLeadingComments(std.mem.trim(u8, sql, " \t\r\n"));
    while (s.len > 0 and s[s.len - 1] == ';') s = std.mem.trim(u8, s[0 .. s.len - 1], " \t\r\n");
    const out = try allocator.alloc(u8, s.len);
    for (s, 0..) |c, i| out[i] = std.ascii.toLower(c);
    return out;
}

fn stripLeadingComments(sql: []const u8) []const u8 {
    var s = sql;
    while (true) {
        s = std.mem.trim(u8, s, " \t\r\n");
        if (std.mem.startsWith(u8, s, "/*")) {
            const end = std.mem.indexOf(u8, s[2..], "*/") orelse return s;
            s = s[end + 4 ..];
            continue;
        }
        if (std.mem.startsWith(u8, s, "--")) {
            const end = std.mem.indexOfScalar(u8, s, '\n') orelse return "";
            s = s[end + 1 ..];
            continue;
        }
        return s;
    }
}

/// Render a byte slice as a SQL string literal: wrap in single quotes,
/// double up any embedded `'`.
pub fn renderStringLiteral(allocator: Allocator, s: []const u8) ![]const u8 {
    var out: std.ArrayList(u8) = .empty;
    defer out.deinit(allocator);
    try out.append(allocator, '\'');
    for (s) |b| {
        if (b == '\'') {
            try out.append(allocator, '\'');
            try out.append(allocator, '\'');
        } else {
            try out.append(allocator, b);
        }
    }
    try out.append(allocator, '\'');
    return try out.toOwnedSlice(allocator);
}

/// Walk one SQL token forward, copying it verbatim into `out`. Returns
/// the post-token cursor. When `i` lands on a placeholder-starting byte
/// (handled by the caller before this function is reached), returns `i`
/// unchanged so the caller emits the substitution.
fn copyOneTokenInto(
    out: *std.ArrayList(u8),
    allocator: Allocator,
    sql: []const u8,
    rules: QuoteRules,
    i_in: usize,
) !usize {
    var i = i_in;
    const c = sql[i];
    switch (c) {
        '\'' => {
            try out.append(allocator, c);
            i += 1;
            while (i < sql.len) {
                const ch = sql[i];
                if (ch == '\'') {
                    if (i + 1 < sql.len and sql[i + 1] == '\'') {
                        try out.appendSlice(allocator, sql[i .. i + 2]);
                        i += 2;
                        continue;
                    }
                    try out.append(allocator, ch);
                    i += 1;
                    break;
                }
                try out.append(allocator, ch);
                i += 1;
            }
        },
        '"' => {
            if (!rules.double_quote_is_identifier) {
                try out.append(allocator, c);
                return i + 1;
            }
            try out.append(allocator, c);
            i += 1;
            while (i < sql.len) {
                const ch = sql[i];
                try out.append(allocator, ch);
                i += 1;
                if (ch == '"') break;
            }
        },
        '`' => {
            if (!rules.backtick_is_identifier) {
                try out.append(allocator, c);
                return i + 1;
            }
            try out.append(allocator, c);
            i += 1;
            while (i < sql.len) {
                const ch = sql[i];
                try out.append(allocator, ch);
                i += 1;
                if (ch == '`') break;
            }
        },
        '-' => {
            if (i + 1 < sql.len and sql[i + 1] == '-') {
                while (i < sql.len and sql[i] != '\n') : (i += 1) {
                    try out.append(allocator, sql[i]);
                }
            } else {
                try out.append(allocator, c);
                i += 1;
            }
        },
        '/' => {
            if (i + 1 < sql.len and sql[i + 1] == '*') {
                try out.appendSlice(allocator, sql[i .. i + 2]);
                i += 2;
                while (i + 1 < sql.len) : (i += 1) {
                    try out.append(allocator, sql[i]);
                    if (sql[i] == '*' and sql[i + 1] == '/') {
                        try out.append(allocator, sql[i + 1]);
                        i += 2;
                        break;
                    }
                }
            } else {
                try out.append(allocator, c);
                i += 1;
            }
        },
        else => {
            try out.append(allocator, c);
            i += 1;
        },
    }
    return i;
}

/// Substitute each `?` outside string/identifier/comment context with
/// `params[k]` (or `NULL` when the entry is null). MySQL semantics.
pub fn substituteQuestionPlaceholders(
    allocator: Allocator,
    sql: []const u8,
    params: []const ?[]const u8,
) ![]u8 {
    var out: std.ArrayList(u8) = .empty;
    defer out.deinit(allocator);

    var i: usize = 0;
    var param_idx: usize = 0;
    while (i < sql.len) {
        if (sql[i] == '?') {
            if (param_idx >= params.len) return error.MissingParameter;
            const text = params[param_idx];
            param_idx += 1;
            if (text) |t| {
                try out.appendSlice(allocator, t);
            } else {
                try out.appendSlice(allocator, "NULL");
            }
            i += 1;
            continue;
        }
        i = try copyOneTokenInto(&out, allocator, sql, mysql_quotes, i);
    }
    return try out.toOwnedSlice(allocator);
}

pub const DollarError = error{
    MalformedBindParam,
    BindParamCountMismatch,
};

/// Substitute each `$N` outside string/identifier/comment context with
/// `params[N-1]` (or `NULL` when the entry is null). PG semantics.
pub fn substituteDollarPlaceholders(
    allocator: Allocator,
    sql: []const u8,
    params: []const ?[]const u8,
) ![]u8 {
    var out: std.ArrayList(u8) = .empty;
    defer out.deinit(allocator);

    var i: usize = 0;
    while (i < sql.len) {
        if (sql[i] == '$') {
            const digits_start = i + 1;
            var j = digits_start;
            while (j < sql.len and sql[j] >= '0' and sql[j] <= '9') : (j += 1) {}
            if (j == digits_start) {
                try out.append(allocator, '$');
                i += 1;
                continue;
            }
            const idx = std.fmt.parseInt(u32, sql[digits_start..j], 10) catch {
                return DollarError.MalformedBindParam;
            };
            if (idx == 0 or idx > params.len) return DollarError.BindParamCountMismatch;
            if (params[idx - 1]) |lit| {
                try out.appendSlice(allocator, lit);
            } else {
                try out.appendSlice(allocator, "NULL");
            }
            i = j;
            continue;
        }
        i = try copyOneTokenInto(&out, allocator, sql, pg_quotes, i);
    }
    return try out.toOwnedSlice(allocator);
}

test "normalizeForCannedMatch strips trailing semicolons + lowercases" {
    const allocator = std.testing.allocator;
    const out = try normalizeForCannedMatch(allocator, "  SELECT VERSION() ;;  ");
    defer allocator.free(out);
    try std.testing.expectEqualStrings("select version()", out);
}

test "normalizeForCannedMatch strips leading comments" {
    const allocator = std.testing.allocator;
    const out = try normalizeForCannedMatch(allocator, " /* wb */ SHOW VARIABLES;");
    defer allocator.free(out);
    try std.testing.expectEqualStrings("show variables", out);
}

test "renderStringLiteral escapes embedded quotes" {
    const allocator = std.testing.allocator;
    const got = try renderStringLiteral(allocator, "it's");
    defer allocator.free(got);
    try std.testing.expectEqualStrings("'it''s'", got);
}

test "substituteQuestionPlaceholders preserves strings and comments" {
    const allocator = std.testing.allocator;
    const params = [_]?[]const u8{ "42", "'hello''world'", null };
    const out = try substituteQuestionPlaceholders(
        allocator,
        "SELECT * FROM t WHERE a = ? AND b = ? AND c = '?' AND d = ?",
        params[0..],
    );
    defer allocator.free(out);
    try std.testing.expectEqualStrings(
        "SELECT * FROM t WHERE a = 42 AND b = 'hello''world' AND c = '?' AND d = NULL",
        out,
    );
}

test "substituteDollarPlaceholders replaces $N in order, preserves strings" {
    const allocator = std.testing.allocator;
    const params = [_]?[]const u8{ "42", "'hello''world'", null };
    const out = try substituteDollarPlaceholders(
        allocator,
        "SELECT * FROM t WHERE a = $1 AND b = $2 AND c = '$1' AND d = $3",
        params[0..],
    );
    defer allocator.free(out);
    try std.testing.expectEqualStrings(
        "SELECT * FROM t WHERE a = 42 AND b = 'hello''world' AND c = '$1' AND d = NULL",
        out,
    );
}

test "substituteDollarPlaceholders honours -- and /* */ comments" {
    const allocator = std.testing.allocator;
    const params = [_]?[]const u8{"42"};
    const out = try substituteDollarPlaceholders(
        allocator,
        "SELECT $1 -- $1 in comment\n /* also $1 */ FROM t",
        params[0..],
    );
    defer allocator.free(out);
    try std.testing.expectEqualStrings(
        "SELECT 42 -- $1 in comment\n /* also $1 */ FROM t",
        out,
    );
}

test "substituteDollarPlaceholders errors on out-of-range index" {
    const allocator = std.testing.allocator;
    const params = [_]?[]const u8{"1"};
    try std.testing.expectError(
        DollarError.BindParamCountMismatch,
        substituteDollarPlaceholders(allocator, "SELECT $1 + $5", params[0..]),
    );
}
