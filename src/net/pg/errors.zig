//! Error-code mapping from thinDB's internal error set to PostgreSQL
//! SQLSTATE codes + ErrorResponse field encoding.

const std = @import("std");
const Allocator = std.mem.Allocator;

const packet = @import("packet.zig");

pub const Mapped = struct {
    sqlstate: [5]u8,
    message: []const u8,
};

/// Map any internal error to a (sqlstate, message) pair suitable for an
/// ErrorResponse. Unrecognized errors fall through to `42000` with
/// `@errorName(err)` as the message.
pub fn mapInternal(err: anyerror) Mapped {
    const name = @errorName(err);
    if (std.mem.eql(u8, name, "TableNotFound"))
        return .{ .sqlstate = "42P01".*, .message = "relation does not exist" };
    if (std.mem.eql(u8, name, "DatabaseNotFound"))
        return .{ .sqlstate = "3D000".*, .message = "database does not exist" };
    if (std.mem.eql(u8, name, "DatabaseAlreadyExists"))
        return .{ .sqlstate = "42P04".*, .message = "database already exists" };
    if (std.mem.eql(u8, name, "SchemaNotFound"))
        return .{ .sqlstate = "3F000".*, .message = "schema does not exist" };
    if (std.mem.eql(u8, name, "SchemaAlreadyExists"))
        return .{ .sqlstate = "42P06".*, .message = "schema already exists" };
    if (std.mem.eql(u8, name, "TableAlreadyExists"))
        return .{ .sqlstate = "42P07".*, .message = "relation already exists" };
    if (std.mem.eql(u8, name, "ColumnNotFound"))
        return .{ .sqlstate = "42703".*, .message = "column does not exist" };
    return .{ .sqlstate = "42000".*, .message = name };
}

/// Send an ErrorResponse `E` frame with severity ERROR + the supplied
/// sqlstate + message. Fields are encoded as (tag-byte, NUL-string)
/// pairs terminated by a single 0 byte.
pub fn sendErrorResponse(
    allocator: Allocator,
    w: *std.Io.Writer,
    sqlstate: [5]u8,
    message: []const u8,
) !void {
    var payload: std.ArrayList(u8) = .empty;
    defer payload.deinit(allocator);

    try payload.append(allocator, 'S');
    try packet.appendCString(allocator, &payload, "ERROR");
    try payload.append(allocator, 'V');
    try packet.appendCString(allocator, &payload, "ERROR");
    try payload.append(allocator, 'C');
    try packet.appendCString(allocator, &payload, &sqlstate);
    try payload.append(allocator, 'M');
    try packet.appendCString(allocator, &payload, message);
    try payload.append(allocator, 0);

    try packet.writeFrame(w, 'E', payload.items);
}

test "mapInternal recognizes table-not-found" {
    const m = mapInternal(error.TableNotFound);
    try std.testing.expectEqualStrings("42P01", &m.sqlstate);
}

test "mapInternal falls back to 42000 with error name" {
    const m = mapInternal(error.NotARealThing);
    try std.testing.expectEqualStrings("42000", &m.sqlstate);
    try std.testing.expectEqualStrings("NotARealThing", m.message);
}
