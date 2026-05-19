//! Error-code mapping from thinDB's internal error set to MySQL wire codes.

const std = @import("std");
const error_map = @import("../error_map.zig");

pub const Mapped = struct {
    code: u16,
    sqlstate: [5]u8,
    message: []const u8,
};

/// Map any internal error to a MySQL (code, sqlstate, message) triple.
/// `fallback_msg` is used for the default 1064 mapping when we don't
/// recognize the error name; pass null to use `@errorName(err)`.
pub fn mapInternal(err: anyerror, fallback_msg: ?[]const u8) Mapped {
    return switch (error_map.classify(@errorName(err))) {
        .table_not_found => .{ .code = 1146, .sqlstate = "42S02".*, .message = "Table not found" },
        .database_not_found => .{ .code = 1049, .sqlstate = "42000".*, .message = "Unknown database" },
        .database_already_exists => .{ .code = 1007, .sqlstate = "HY000".*, .message = "Database exists" },
        .schema_not_found => .{ .code = 1146, .sqlstate = "42S02".*, .message = "Schema not found" },
        .schema_already_exists => .{ .code = 1050, .sqlstate = "42S01".*, .message = "Schema exists" },
        .table_already_exists => .{ .code = 1050, .sqlstate = "42S01".*, .message = "Table exists" },
        .column_not_found => .{ .code = 1054, .sqlstate = "42S22".*, .message = "Unknown column" },
        .unknown => .{ .code = 1064, .sqlstate = "42000".*, .message = fallback_msg orelse @errorName(err) },
    };
}

test "mapInternal recognizes catalog errors" {
    const m = mapInternal(error.TableNotFound, "x");
    try std.testing.expectEqual(@as(u16, 1146), m.code);
    try std.testing.expectEqualStrings("42S02", &m.sqlstate);
}

test "mapInternal falls back to 1064" {
    const m = mapInternal(error.NotARealError, "fallback");
    try std.testing.expectEqual(@as(u16, 1064), m.code);
    try std.testing.expectEqualStrings("fallback", m.message);
}

test "mapInternal null fallback uses the error name" {
    const m = mapInternal(error.NotARealError, null);
    try std.testing.expectEqual(@as(u16, 1064), m.code);
    try std.testing.expectEqualStrings("NotARealError", m.message);
}
