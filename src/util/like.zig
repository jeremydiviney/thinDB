//! SQL LIKE pattern semantics, shared by the query path (`exec/predicate.zig`,
//! which adds a fast path for patterns without `_` or escapes) and WAL replay
//! (`engine/wal_codec.zig`), so a replayed DELETE matches exactly the rows the
//! live one did.
//!
//! `%` matches any run of bytes and `_` exactly one byte. A backslash escapes
//! the next byte, MySQL's default: `\%` is a literal `%`. A trailing backslash
//! matches itself. The parser rewrites a pattern with its own ESCAPE character
//! to this form.

const std = @import("std");

pub fn match(text: []const u8, pattern: []const u8) bool {
    var ti: usize = 0;
    var pi: usize = 0;
    var star_ti: ?usize = null;
    var star_pi: usize = 0;
    while (ti < text.len) {
        if (pi < pattern.len and pattern[pi] == '%') {
            star_pi = pi;
            star_ti = ti;
            pi += 1;
            continue;
        }
        if (pi < pattern.len) {
            const escaped = pattern[pi] == '\\' and pi + 1 < pattern.len;
            const want = if (escaped) pattern[pi + 1] else pattern[pi];
            if ((want == '_' and !escaped) or want == text[ti]) {
                pi += if (escaped) 2 else 1;
                ti += 1;
                continue;
            }
        }
        // Backtrack to the last %, which takes one more byte of the text.
        const sti = star_ti orelse return false;
        pi = star_pi + 1;
        ti = sti + 1;
        star_ti = sti + 1;
    }
    while (pi < pattern.len and pattern[pi] == '%') pi += 1;
    return pi == pattern.len;
}

/// Whether the pattern has a byte the plain segment matcher can't read:
/// `_`, or an escape.
pub fn needsGeneralMatch(pattern: []const u8) bool {
    return std.mem.indexOfAny(u8, pattern, "_\\") != null;
}

test "like: wildcards, escapes and a trailing backslash" {
    const cases = .{
        .{ "abc", "a%", true },
        .{ "abc", "a_c", true },
        .{ "abc", "a_", false },
        .{ "a%c", "a\\%c", true },
        .{ "abc", "a\\%c", false },
        .{ "a_c", "a\\_c", true },
        .{ "abc", "a\\_c", false },
        .{ "a\\c", "a\\\\c", true },
        .{ "ab\\", "ab\\", true },
        .{ "xa%yb", "%a\\%%b", true },
        .{ "xayb", "%a\\%%b", false },
        .{ "", "%", true },
        .{ "", "_", false },
    };
    inline for (cases) |c| {
        errdefer std.debug.print("case failed: {s} LIKE {s}\n", .{ c[0], c[1] });
        try std.testing.expectEqual(c[2], match(c[0], c[1]));
    }
}
