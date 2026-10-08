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
const BoundSpan = @import("../sql/lexer.zig").BoundSpan;
const SessionOption = @import("../ir/ir.zig").SessionOption;

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
    const s = normalizeForCannedMatchKeepCase(sql);
    const out = try allocator.alloc(u8, s.len);
    for (s, 0..) |c, i| out[i] = std.ascii.toLower(c);
    return out;
}

/// The same trim/comment/semicolon normalization WITHOUT lowercasing —
/// byte-for-byte parallel to `normalizeForCannedMatch`'s output, so an
/// offset into one indexes the same character in the other. Matchers work
/// on the lowered buffer; display text (column labels echo the client's
/// case) slices from this one. Borrows from `sql`.
pub fn normalizeForCannedMatchKeepCase(sql: []const u8) []const u8 {
    var s = stripLeadingComments(std.mem.trim(u8, sql, " \t\r\n"));
    while (s.len > 0 and s[s.len - 1] == ';') s = std.mem.trim(u8, s[0 .. s.len - 1], " \t\r\n");
    return s;
}

/// Whether a canned-normalized statement SETs a thinDB session option
/// (`set thindb_max_dop = 4`, also spelled with `session`, `@@` or
/// `@@session.`). Both wires ack the SETs they have no counterpart for
/// without running them; these the engine has to run.
pub fn setsSessionOption(lc: []const u8) bool {
    if (!std.mem.startsWith(u8, lc, "set ")) return false;
    var rest = std.mem.trimStart(u8, lc[4..], " \t");
    for ([_][]const u8{ "@@session.", "@@", "session " }) |scope| {
        if (std.mem.startsWith(u8, rest, scope)) {
            rest = std.mem.trimStart(u8, rest[scope.len..], " \t");
            break;
        }
    }
    const name_end = std.mem.indexOfAny(u8, rest, " \t=") orelse rest.len;
    return SessionOption.fromSqlName(rest[0..name_end]) != null;
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

/// A statement with each placeholder replaced by its bound parameter's
/// literal, and where those literals sit, for `Lexer.bound_params`.
pub const BoundSql = struct {
    sql: []u8,
    params: []BoundSpan,

    pub fn deinit(self: BoundSql, allocator: Allocator) void {
        allocator.free(self.sql);
        allocator.free(self.params);
    }
};

fn appendParam(
    allocator: Allocator,
    out: *std.ArrayList(u8),
    spans: *std.ArrayList(BoundSpan),
    literal: ?[]const u8,
) !void {
    const text = literal orelse return out.appendSlice(allocator, "NULL");
    const start = out.items.len;
    try out.appendSlice(allocator, text);
    try spans.append(allocator, .{ .start = start, .end = out.items.len });
}

fn finishBound(allocator: Allocator, out: *std.ArrayList(u8), spans: *std.ArrayList(BoundSpan)) !BoundSql {
    const params = try spans.toOwnedSlice(allocator);
    errdefer allocator.free(params);
    return .{ .sql = try out.toOwnedSlice(allocator), .params = params };
}

/// Substitute each `?` outside string/identifier/comment context with
/// `params[k]` (or `NULL` when the entry is null). MySQL semantics.
pub fn substituteQuestionPlaceholders(
    allocator: Allocator,
    sql: []const u8,
    params: []const ?[]const u8,
) !BoundSql {
    var out: std.ArrayList(u8) = .empty;
    defer out.deinit(allocator);
    var spans: std.ArrayList(BoundSpan) = .empty;
    defer spans.deinit(allocator);

    var i: usize = 0;
    var param_idx: usize = 0;
    while (i < sql.len) {
        if (sql[i] == '?') {
            if (param_idx >= params.len) return error.MissingParameter;
            try appendParam(allocator, &out, &spans, params[param_idx]);
            param_idx += 1;
            i += 1;
            continue;
        }
        i = try copyOneTokenInto(&out, allocator, sql, mysql_quotes, i);
    }
    return try finishBound(allocator, &out, &spans);
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
) !BoundSql {
    var out: std.ArrayList(u8) = .empty;
    defer out.deinit(allocator);
    var spans: std.ArrayList(BoundSpan) = .empty;
    defer spans.deinit(allocator);

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
            try appendParam(allocator, &out, &spans, params[idx - 1]);
            i = j;
            continue;
        }
        i = try copyOneTokenInto(&out, allocator, sql, pg_quotes, i);
    }
    return try finishBound(allocator, &out, &spans);
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
    defer out.deinit(allocator);
    try std.testing.expectEqualStrings(
        "SELECT * FROM t WHERE a = 42 AND b = 'hello''world' AND c = '?' AND d = NULL",
        out.sql,
    );
    try std.testing.expectEqual(@as(usize, 2), out.params.len);
    try std.testing.expectEqualStrings("42", out.sql[out.params[0].start..out.params[0].end]);
    try std.testing.expectEqualStrings("'hello''world'", out.sql[out.params[1].start..out.params[1].end]);
}

test "substituteDollarPlaceholders replaces $N in order, preserves strings" {
    const allocator = std.testing.allocator;
    const params = [_]?[]const u8{ "42", "'hello''world'", null };
    const out = try substituteDollarPlaceholders(
        allocator,
        "SELECT * FROM t WHERE a = $1 AND b = $2 AND c = '$1' AND d = $3 AND e = $2",
        params[0..],
    );
    defer out.deinit(allocator);
    try std.testing.expectEqualStrings(
        "SELECT * FROM t WHERE a = 42 AND b = 'hello''world' AND c = '$1' AND d = NULL AND e = 'hello''world'",
        out.sql,
    );
    try std.testing.expectEqual(@as(usize, 3), out.params.len);
    try std.testing.expectEqualStrings("'hello''world'", out.sql[out.params[2].start..out.params[2].end]);
}

test "substituteDollarPlaceholders honours -- and /* */ comments" {
    const allocator = std.testing.allocator;
    const params = [_]?[]const u8{"42"};
    const out = try substituteDollarPlaceholders(
        allocator,
        "SELECT $1 -- $1 in comment\n /* also $1 */ FROM t",
        params[0..],
    );
    defer out.deinit(allocator);
    try std.testing.expectEqualStrings(
        "SELECT 42 -- $1 in comment\n /* also $1 */ FROM t",
        out.sql,
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
