//! ClickHouse TabSeparated field decoding, the format ClickBench's hits.tsv
//! is written in.

const std = @import("std");

/// `field` with its escape sequences decoded into `out`, which needs
/// `field.len` bytes (decoding never grows a field). `\b \f \n \r \t \0`
/// stand for their control bytes, and a backslash before any other byte
/// stands for that byte (`\\`, `\'`).
pub fn unescapeField(field: []const u8, out: []u8) []u8 {
    var n: usize = 0;
    var i: usize = 0;
    while (i < field.len) : (i += 1) {
        var c = field[i];
        if (c == '\\' and i + 1 < field.len) {
            i += 1;
            c = switch (field[i]) {
                'b' => 0x08,
                'f' => 0x0C,
                'n' => '\n',
                'r' => '\r',
                't' => '\t',
                '0' => 0,
                else => |escaped| escaped,
            };
        }
        out[n] = c;
        n += 1;
    }
    return out[0..n];
}

test "unescapeField decodes TabSeparated escapes" {
    var buf: [32]u8 = undefined;
    const cases = .{
        .{ "plain", "plain" },
        .{ "", "" },
        .{ "a\\tb", "a\tb" },
        .{ "line\\nnext\\r", "line\nnext\r" },
        .{ "back\\\\slash", "back\\slash" },
        .{ "it\\'s", "it's" },
        .{ "\\0\\b\\f", "\x00\x08\x0c" },
        .{ "\\\\t", "\\t" },
        .{ "trailing\\", "trailing\\" },
    };
    inline for (cases) |c| try std.testing.expectEqualStrings(c[1], unescapeField(c[0], &buf));
}
