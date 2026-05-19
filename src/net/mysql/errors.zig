//! Error-code mapping from thinDB's internal error set to MySQL wire codes.

const std = @import("std");

pub const Mapped = struct {
    code: u16,
    sqlstate: [5]u8,
    message: []const u8,
};

/// Map any internal error to a MySQL (code, sqlstate, message) triple.
/// `fallback_msg` is used for the default 1064 mapping when we don't
/// recognize the error name.
pub fn mapInternal(err: anyerror, fallback_msg: []const u8) Mapped {
    const name = @errorName(err);
    if (std.mem.eql(u8, name, "TableNotFound"))
        return .{ .code = 1146, .sqlstate = "42S02".*, .message = "Table not found" };
    if (std.mem.eql(u8, name, "DatabaseNotFound"))
        return .{ .code = 1049, .sqlstate = "42000".*, .message = "Unknown database" };
    if (std.mem.eql(u8, name, "DatabaseAlreadyExists"))
        return .{ .code = 1007, .sqlstate = "HY000".*, .message = "Database exists" };
    if (std.mem.eql(u8, name, "SchemaNotFound"))
        return .{ .code = 1146, .sqlstate = "42S02".*, .message = "Schema not found" };
    if (std.mem.eql(u8, name, "SchemaAlreadyExists"))
        return .{ .code = 1050, .sqlstate = "42S01".*, .message = "Schema exists" };
    if (std.mem.eql(u8, name, "TableAlreadyExists"))
        return .{ .code = 1050, .sqlstate = "42S01".*, .message = "Table exists" };
    if (std.mem.eql(u8, name, "ColumnNotFound"))
        return .{ .code = 1054, .sqlstate = "42S22".*, .message = "Unknown column" };
    return .{ .code = 1064, .sqlstate = "42000".*, .message = fallback_msg };
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
