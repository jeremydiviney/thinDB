//! Config-file parser for `thindb-server`. Turns a flat `key = value` file
//! into synthetic CLI argument tokens, so the exact same argument-parsing loop
//! handles file-provided and command-line settings uniformly. Keys mirror the
//! CLI flag names (with or without the leading `--`).
//!
//! Precedence is achieved by the caller applying the file tokens BEFORE the
//! real CLI tokens: a later (command-line) flag overwrites the same setting
//! read from the file. So `CLI flag > config file > built-in default`.
//!
//! Format:
//!   - `key = value`            → `--key`, `value`
//!   - a line beginning with `#` is a comment; blank lines are ignored
//!   - values may be wrapped in matching single or double quotes (stripped),
//!     which is how to give a value that has leading/trailing spaces
//!   - inline comments are NOT supported (so a value — e.g. a password — may
//!     contain `#`); put comments on their own line
//!   - boolean flags (the valueless CLI switches, see `bool_flags`) take a
//!     truthy/falsy value in the file: `no-wal = true` emits `--no-wal`,
//!     `no-wal = false` emits nothing
//!
//! Because the file's values become in-process argument strings — never the
//! real process argv — a password set here does NOT appear in `ps`.

const std = @import("std");

pub const ParseError = error{InvalidConfigLine} || std.mem.Allocator.Error;

/// CLI switches that take no value. In the file they carry a truthy/falsy
/// value; only a truthy value emits the bare `--flag`.
const bool_flags = [_][]const u8{ "no-wal", "no-compaction", "profile-ops", "trace-group-by" };

fn isBoolFlag(key: []const u8) bool {
    for (bool_flags) |f| if (std.mem.eql(u8, key, f)) return true;
    return false;
}

fn truthy(v: []const u8) bool {
    if (v.len == 0) return true;
    if (std.mem.eql(u8, v, "0")) return false;
    if (std.ascii.eqlIgnoreCase(v, "false")) return false;
    if (std.ascii.eqlIgnoreCase(v, "off")) return false;
    if (std.ascii.eqlIgnoreCase(v, "no")) return false;
    return true;
}

fn stripQuotes(v: []const u8) []const u8 {
    if (v.len >= 2 and ((v[0] == '"' and v[v.len - 1] == '"') or (v[0] == '\'' and v[v.len - 1] == '\'')))
        return v[1 .. v.len - 1];
    return v;
}

/// Parse `text` (a config-file body) and append the resulting CLI argument
/// tokens to `out`. Tokens are allocated with `alloc` and owned by the caller.
pub fn parseInto(alloc: std.mem.Allocator, text: []const u8, out: *std.ArrayList([]const u8)) ParseError!void {
    var lines = std.mem.splitScalar(u8, text, '\n');
    while (lines.next()) |raw| {
        const line = std.mem.trim(u8, raw, " \t\r");
        if (line.len == 0 or line[0] == '#') continue;

        const eq = std.mem.indexOfScalar(u8, line, '=') orelse return error.InvalidConfigLine;
        var key = std.mem.trim(u8, line[0..eq], " \t");
        const val = stripQuotes(std.mem.trim(u8, line[eq + 1 ..], " \t"));
        if (key.len == 0) return error.InvalidConfigLine;
        if (std.mem.startsWith(u8, key, "--")) key = key[2..];

        if (isBoolFlag(key)) {
            if (truthy(val)) try out.append(alloc, try std.fmt.allocPrint(alloc, "--{s}", .{key}));
            continue;
        }
        try out.append(alloc, try std.fmt.allocPrint(alloc, "--{s}", .{key}));
        try out.append(alloc, try alloc.dupe(u8, val));
    }
}

// ---------------------------------------------------------------------------
// tests
// ---------------------------------------------------------------------------

fn parse(alloc: std.mem.Allocator, text: []const u8) !std.ArrayList([]const u8) {
    var out: std.ArrayList([]const u8) = .empty;
    errdefer out.deinit(alloc);
    try parseInto(alloc, text, &out);
    return out;
}

test "value settings become --key value token pairs" {
    const a = std.testing.allocator;
    var out = try parse(a,
        \\mysql-port = 13310
        \\bind = 0.0.0.0
        \\cache-size = 12G
    );
    defer {
        for (out.items) |s| a.free(s);
        out.deinit(a);
    }
    try std.testing.expectEqual(@as(usize, 6), out.items.len);
    try std.testing.expectEqualStrings("--mysql-port", out.items[0]);
    try std.testing.expectEqualStrings("13310", out.items[1]);
    try std.testing.expectEqualStrings("--bind", out.items[2]);
    try std.testing.expectEqualStrings("0.0.0.0", out.items[3]);
    try std.testing.expectEqualStrings("--cache-size", out.items[4]);
    try std.testing.expectEqualStrings("12G", out.items[5]);
}

test "comments and blank lines are ignored, -- prefix accepted" {
    const a = std.testing.allocator;
    var out = try parse(a,
        \\# a comment
        \\
        \\--pg-port = 0
        \\   # indented comment
    );
    defer {
        for (out.items) |s| a.free(s);
        out.deinit(a);
    }
    try std.testing.expectEqual(@as(usize, 2), out.items.len);
    try std.testing.expectEqualStrings("--pg-port", out.items[0]);
    try std.testing.expectEqualStrings("0", out.items[1]);
}

test "boolean flags: truthy emits bare flag, falsy emits nothing" {
    const a = std.testing.allocator;
    var out = try parse(a,
        \\no-wal = true
        \\no-compaction = false
        \\profile-ops = 1
    );
    defer {
        for (out.items) |s| a.free(s);
        out.deinit(a);
    }
    try std.testing.expectEqual(@as(usize, 2), out.items.len);
    try std.testing.expectEqualStrings("--no-wal", out.items[0]);
    try std.testing.expectEqualStrings("--profile-ops", out.items[1]);
}

test "quotes are stripped; values may hold spaces and '#'" {
    const a = std.testing.allocator;
    var out = try parse(a,
        \\cache-size = "12G"
        \\mysql-password = 'p@ss #word with spaces'
    );
    defer {
        for (out.items) |s| a.free(s);
        out.deinit(a);
    }
    try std.testing.expectEqual(@as(usize, 4), out.items.len);
    try std.testing.expectEqualStrings("12G", out.items[1]);
    try std.testing.expectEqualStrings("--mysql-password", out.items[2]);
    try std.testing.expectEqualStrings("p@ss #word with spaces", out.items[3]);
}

test "a line without '=' is an error" {
    const a = std.testing.allocator;
    var out: std.ArrayList([]const u8) = .empty;
    defer {
        for (out.items) |s| a.free(s);
        out.deinit(a);
    }
    try std.testing.expectError(error.InvalidConfigLine, parseInto(a, "mysql-port = 3306\njust-a-word\n", &out));
    // the valid line before the error was still appended
    try std.testing.expectEqual(@as(usize, 2), out.items.len);
}
