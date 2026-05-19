//! PostgreSQL startup-phase exchange.
//!
//! Client sends a StartupMessage (no type byte, protocol version 196608
//! for 3.0) carrying user/database/application_name key-value pairs.
//! Server replies AuthenticationOk + several ParameterStatus frames +
//! BackendKeyData + ReadyForQuery and the conversation moves to the
//! Simple Query phase.
//!
//! If the client opens with an SSLRequest (magic protocol 80877103) we
//! reply with a single byte `'N'` (no SSL) and read the real
//! StartupMessage on the next round.

const std = @import("std");
const Allocator = std.mem.Allocator;

const packet = @import("packet.zig");

pub const protocol_v3: u32 = 196608;
pub const ssl_request_magic: u32 = 80877103;
pub const cancel_request_magic: u32 = 80877102;
pub const gss_request_magic: u32 = 80877104;

pub const server_version: []const u8 = "16.0 (thinDB)";

/// What we learned from the client's StartupMessage. Borrowed views into
/// the request payload; copy before the payload buffer is freed if any
/// field must outlive the read.
pub const StartupParams = struct {
    user: []const u8 = "",
    database: ?[]const u8 = null,
    application_name: ?[]const u8 = null,
    client_encoding: ?[]const u8 = null,
};

pub const FirstFrame = union(enum) {
    ssl_request,
    gss_request,
    cancel_request: struct { process_id: u32, secret_key: u32 },
    startup: StartupParams,
};

/// Parse the first startup-phase frame. The caller has already read the
/// 4-byte length and 4-byte protocol field of a typed frame; we accept
/// the whole payload (everything after the length prefix) and classify.
pub fn parseFirstFrame(payload: []const u8) !FirstFrame {
    var cursor: usize = 0;
    const proto = try packet.readU32(payload, &cursor);
    return switch (proto) {
        ssl_request_magic => .ssl_request,
        gss_request_magic => .gss_request,
        cancel_request_magic => blk: {
            const pid = try packet.readU32(payload, &cursor);
            const key = try packet.readU32(payload, &cursor);
            break :blk .{ .cancel_request = .{ .process_id = pid, .secret_key = key } };
        },
        else => .{ .startup = try parseStartupParams(payload[cursor..]) },
    };
}

fn parseStartupParams(body: []const u8) !StartupParams {
    var params = StartupParams{};
    var cursor: usize = 0;
    while (cursor < body.len) {
        if (body[cursor] == 0) {
            cursor += 1;
            break;
        }
        const key = try packet.readCString(body, &cursor);
        if (cursor >= body.len) return packet.Error.FrameTruncated;
        const val = try packet.readCString(body, &cursor);
        if (std.mem.eql(u8, key, "user")) params.user = val;
        if (std.mem.eql(u8, key, "database")) params.database = val;
        if (std.mem.eql(u8, key, "application_name")) params.application_name = val;
        if (std.mem.eql(u8, key, "client_encoding")) params.client_encoding = val;
    }
    return params;
}

/// Reply to an SSLRequest with a single byte 'N' (no SSL).
pub fn sendSslDenial(w: *std.Io.Writer) !void {
    try w.writeAll("N");
}

/// AuthenticationOk: `R` frame with payload = 0 (success code).
pub fn sendAuthenticationOk(allocator: Allocator, w: *std.Io.Writer) !void {
    var payload: std.ArrayList(u8) = .empty;
    defer payload.deinit(allocator);
    try packet.appendU32(allocator, &payload, 0);
    try packet.writeFrame(w, 'R', payload.items);
}

/// ParameterStatus: `S` frame carrying name + value (both NUL-terminated).
pub fn sendParameterStatus(
    allocator: Allocator,
    w: *std.Io.Writer,
    name: []const u8,
    value: []const u8,
) !void {
    var payload: std.ArrayList(u8) = .empty;
    defer payload.deinit(allocator);
    try packet.appendCString(allocator, &payload, name);
    try packet.appendCString(allocator, &payload, value);
    try packet.writeFrame(w, 'S', payload.items);
}

/// BackendKeyData: `K` frame with process_id + secret_key. Used by clients
/// to issue cancellation requests against a specific backend — we don't
/// implement cancellation but must send the frame.
pub fn sendBackendKeyData(
    allocator: Allocator,
    w: *std.Io.Writer,
    process_id: u32,
    secret_key: u32,
) !void {
    var payload: std.ArrayList(u8) = .empty;
    defer payload.deinit(allocator);
    try packet.appendU32(allocator, &payload, process_id);
    try packet.appendU32(allocator, &payload, secret_key);
    try packet.writeFrame(w, 'K', payload.items);
}

/// ReadyForQuery: `Z` frame carrying a single status byte.
///   'I' = idle (no transaction)
///   'T' = in a transaction block
///   'E' = in a failed transaction block
pub fn sendReadyForQuery(allocator: Allocator, w: *std.Io.Writer, status: u8) !void {
    var payload: std.ArrayList(u8) = .empty;
    defer payload.deinit(allocator);
    try payload.append(allocator, status);
    try packet.writeFrame(w, 'Z', payload.items);
}

/// Send the canonical post-AuthenticationOk parameter set.
pub fn sendStandardParameterStatus(
    allocator: Allocator,
    w: *std.Io.Writer,
    application_name: []const u8,
    session_user: []const u8,
) !void {
    try sendParameterStatus(allocator, w, "server_version", server_version);
    try sendParameterStatus(allocator, w, "client_encoding", "UTF8");
    try sendParameterStatus(allocator, w, "server_encoding", "UTF8");
    try sendParameterStatus(allocator, w, "DateStyle", "ISO, MDY");
    try sendParameterStatus(allocator, w, "IntervalStyle", "postgres");
    try sendParameterStatus(allocator, w, "TimeZone", "UTC");
    try sendParameterStatus(allocator, w, "integer_datetimes", "on");
    try sendParameterStatus(allocator, w, "standard_conforming_strings", "on");
    try sendParameterStatus(allocator, w, "application_name", application_name);
    try sendParameterStatus(allocator, w, "is_superuser", "off");
    try sendParameterStatus(allocator, w, "session_authorization", session_user);
}

test "parseFirstFrame classifies SSLRequest" {
    const allocator = std.testing.allocator;
    var payload: std.ArrayList(u8) = .empty;
    defer payload.deinit(allocator);
    try packet.appendU32(allocator, &payload, ssl_request_magic);
    const result = try parseFirstFrame(payload.items);
    try std.testing.expectEqual(@as(std.meta.Tag(FirstFrame), .ssl_request), std.meta.activeTag(result));
}

test "parseFirstFrame extracts user and database from StartupMessage" {
    const allocator = std.testing.allocator;
    var payload: std.ArrayList(u8) = .empty;
    defer payload.deinit(allocator);
    try packet.appendU32(allocator, &payload, protocol_v3);
    try packet.appendCString(allocator, &payload, "user");
    try packet.appendCString(allocator, &payload, "alice");
    try packet.appendCString(allocator, &payload, "database");
    try packet.appendCString(allocator, &payload, "main");
    try packet.appendCString(allocator, &payload, "application_name");
    try packet.appendCString(allocator, &payload, "psql");
    try payload.append(allocator, 0);

    const result = try parseFirstFrame(payload.items);
    switch (result) {
        .startup => |p| {
            try std.testing.expectEqualStrings("alice", p.user);
            try std.testing.expect(p.database != null);
            try std.testing.expectEqualStrings("main", p.database.?);
            try std.testing.expect(p.application_name != null);
            try std.testing.expectEqualStrings("psql", p.application_name.?);
        },
        else => return error.TestUnexpectedResult,
    }
}
