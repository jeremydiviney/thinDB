//! MySQL Protocol::HandshakeV10 implementation. Trust auth — any
//! credentials accepted; we read the response and discard it.

const std = @import("std");
const Allocator = std.mem.Allocator;

const packet = @import("packet.zig");

pub const server_version: []const u8 = "8.0.32-thinDB";

pub const CLIENT_LONG_PASSWORD: u32 = 0x00000001;
pub const CLIENT_LONG_FLAG: u32 = 0x00000004;
pub const CLIENT_CONNECT_WITH_DB: u32 = 0x00000008;
/// Clients that negotiate this bit may pack multiple `;`-separated
/// statements into a single COM_QUERY frame. Server responds with a
/// chain of result sets, setting SERVER_MORE_RESULTS_EXISTS on all
/// but the last terminator.
pub const CLIENT_MULTI_STATEMENTS: u32 = 0x00010000;
pub const CLIENT_PROTOCOL_41: u32 = 0x00000200;
pub const CLIENT_SECURE_CONNECTION: u32 = 0x00008000;
pub const CLIENT_PLUGIN_AUTH: u32 = 0x00080000;
pub const CLIENT_CONNECT_ATTRS: u32 = 0x00100000;
pub const CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA: u32 = 0x00200000;
pub const CLIENT_DEPRECATE_EOF: u32 = 0x01000000;

pub const server_capabilities: u32 =
    CLIENT_LONG_PASSWORD |
    CLIENT_LONG_FLAG |
    CLIENT_CONNECT_WITH_DB |
    CLIENT_MULTI_STATEMENTS |
    CLIENT_PROTOCOL_41 |
    CLIENT_SECURE_CONNECTION |
    CLIENT_PLUGIN_AUTH |
    CLIENT_DEPRECATE_EOF;

pub const SERVER_STATUS_AUTOCOMMIT: u16 = 0x0002;
/// Set on the status_flags of all OK/EOF packets that terminate a
/// non-final result set in a multi-statement response. The client
/// keeps reading until it gets a terminator with this bit cleared.
pub const SERVER_MORE_RESULTS_EXISTS: u16 = 0x0008;

/// What we learned from the client's HandshakeResponse41 packet. All
/// borrowed views into the response payload; copy before the payload
/// buffer is freed if you need them to outlive the read.
pub const ClientHandshake = struct {
    capabilities: u32,
    max_packet_size: u32,
    character_set: u8,
    username: []const u8,
    initial_database: ?[]const u8,
    auth_plugin: ?[]const u8,
};

/// Write the server's HandshakeV10 greeting packet (always seq_id=0).
/// `connection_id` is a server-chosen identifier surfaced to the client.
pub fn sendInitialHandshake(
    allocator: Allocator,
    w: *std.Io.Writer,
    connection_id: u32,
) !void {
    var payload: std.ArrayList(u8) = .empty;
    defer payload.deinit(allocator);

    try payload.append(allocator, 10);
    try packet.appendNulString(allocator, &payload, server_version);

    var cid_buf: [4]u8 = undefined;
    std.mem.writeInt(u32, &cid_buf, connection_id, .little);
    try payload.appendSlice(allocator, &cid_buf);

    const auth_part1 = [_]u8{ 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h' };
    try payload.appendSlice(allocator, &auth_part1);
    try payload.append(allocator, 0);

    var cap_buf: [4]u8 = undefined;
    std.mem.writeInt(u32, &cap_buf, server_capabilities, .little);
    try payload.appendSlice(allocator, cap_buf[0..2]);

    try payload.append(allocator, 0xff);

    var status_buf: [2]u8 = undefined;
    std.mem.writeInt(u16, &status_buf, SERVER_STATUS_AUTOCOMMIT, .little);
    try payload.appendSlice(allocator, &status_buf);

    try payload.appendSlice(allocator, cap_buf[2..4]);

    try payload.append(allocator, 21);

    const reserved = [_]u8{0} ** 10;
    try payload.appendSlice(allocator, &reserved);

    const auth_part2 = [_]u8{ 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 0 };
    try payload.appendSlice(allocator, &auth_part2);

    try packet.appendNulString(allocator, &payload, "mysql_native_password");

    try packet.writePacket(w, 0, payload.items);
}

/// Parse a HandshakeResponse41 packet. Returns borrowed slices into
/// `bytes`.
pub fn parseHandshakeResponse(bytes: []const u8) !ClientHandshake {
    var cursor: usize = 0;
    const caps_u64 = try packet.readFixedU32(bytes, &cursor);
    const caps: u32 = @intCast(caps_u64);
    const max_size: u32 = @intCast(try packet.readFixedU32(bytes, &cursor));
    const charset = try packet.readFixedU8(bytes, &cursor);
    if (cursor + 23 > bytes.len) return packet.Error.PacketTruncated;
    cursor += 23;
    const username = try packet.readNulString(bytes, &cursor);

    if ((caps & CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA) != 0) {
        _ = try packet.readLenEncString(bytes, &cursor);
    } else if ((caps & CLIENT_SECURE_CONNECTION) != 0) {
        const auth_len = try packet.readFixedU8(bytes, &cursor);
        if (cursor + auth_len > bytes.len) return packet.Error.PacketTruncated;
        cursor += auth_len;
    } else {
        _ = try packet.readNulString(bytes, &cursor);
    }

    var initial_db: ?[]const u8 = null;
    if ((caps & CLIENT_CONNECT_WITH_DB) != 0 and cursor < bytes.len) {
        initial_db = try packet.readNulString(bytes, &cursor);
    }

    var auth_plugin: ?[]const u8 = null;
    if ((caps & CLIENT_PLUGIN_AUTH) != 0 and cursor < bytes.len) {
        auth_plugin = try packet.readNulString(bytes, &cursor);
    }

    return .{
        .capabilities = caps,
        .max_packet_size = max_size,
        .character_set = charset,
        .username = username,
        .initial_database = initial_db,
        .auth_plugin = auth_plugin,
    };
}

/// OK packet sent immediately after a successful handshake.
/// Sequence id is fixed at 2 (server greeting=0, client response=1).
pub fn sendHandshakeOk(allocator: Allocator, w: *std.Io.Writer) !void {
    try sendOkPacket(allocator, w, 2, 0, 0);
}

/// Build and send an OK_Packet at the given sequence id.
pub fn sendOkPacket(
    allocator: Allocator,
    w: *std.Io.Writer,
    seq_id: u8,
    affected_rows: u64,
    last_insert_id: u64,
) !void {
    try sendOkPacketStatus(allocator, w, seq_id, affected_rows, last_insert_id, 0);
}

/// Like `sendOkPacket` but ORs `extra_status` into the status_flags
/// (e.g. SERVER_MORE_RESULTS_EXISTS for non-final results in a
/// multi-statement response).
pub fn sendOkPacketStatus(
    allocator: Allocator,
    w: *std.Io.Writer,
    seq_id: u8,
    affected_rows: u64,
    last_insert_id: u64,
    extra_status: u16,
) !void {
    var payload: std.ArrayList(u8) = .empty;
    defer payload.deinit(allocator);

    try payload.append(allocator, 0x00);
    try packet.appendLenEncInt(allocator, &payload, affected_rows);
    try packet.appendLenEncInt(allocator, &payload, last_insert_id);

    var status_buf: [2]u8 = undefined;
    std.mem.writeInt(u16, &status_buf, SERVER_STATUS_AUTOCOMMIT | extra_status, .little);
    try payload.appendSlice(allocator, &status_buf);

    var warn_buf: [2]u8 = .{ 0, 0 };
    try payload.appendSlice(allocator, &warn_buf);

    try packet.writePacket(w, seq_id, payload.items);
}

/// EOF-style OK_Packet (header 0xFE) used to terminate result sets when
/// CLIENT_DEPRECATE_EOF is advertised. Same shape as OK but with the
/// 0xFE header byte.
pub fn sendEofOkPacket(allocator: Allocator, w: *std.Io.Writer, seq_id: u8) !void {
    try sendEofOkPacketStatus(allocator, w, seq_id, 0);
}

pub fn sendEofOkPacketStatus(
    allocator: Allocator,
    w: *std.Io.Writer,
    seq_id: u8,
    extra_status: u16,
) !void {
    var payload: std.ArrayList(u8) = .empty;
    defer payload.deinit(allocator);

    try payload.append(allocator, 0xFE);
    try packet.appendLenEncInt(allocator, &payload, 0);
    try packet.appendLenEncInt(allocator, &payload, 0);

    var status_buf: [2]u8 = undefined;
    std.mem.writeInt(u16, &status_buf, SERVER_STATUS_AUTOCOMMIT | extra_status, .little);
    try payload.appendSlice(allocator, &status_buf);

    var warn_buf: [2]u8 = .{ 0, 0 };
    try payload.appendSlice(allocator, &warn_buf);

    try packet.writePacket(w, seq_id, payload.items);
}

/// Legacy EOF_Packet (header 0xFE + warnings(2) + status_flags(2)).
/// Emitted as the column-def / row-set terminator when the client did
/// NOT negotiate CLIENT_DEPRECATE_EOF. Older clients (mysql2,
/// MySQL Connector/J pre-5.1.x) require this between column defs and
/// rows, and again after the rows.
pub fn sendLegacyEofPacket(allocator: Allocator, w: *std.Io.Writer, seq_id: u8) !void {
    try sendLegacyEofPacketStatus(allocator, w, seq_id, 0);
}

pub fn sendLegacyEofPacketStatus(
    allocator: Allocator,
    w: *std.Io.Writer,
    seq_id: u8,
    extra_status: u16,
) !void {
    var payload: std.ArrayList(u8) = .empty;
    defer payload.deinit(allocator);

    try payload.append(allocator, 0xFE);

    var warn_buf: [2]u8 = .{ 0, 0 };
    try payload.appendSlice(allocator, &warn_buf);

    var status_buf: [2]u8 = undefined;
    std.mem.writeInt(u16, &status_buf, SERVER_STATUS_AUTOCOMMIT | extra_status, .little);
    try payload.appendSlice(allocator, &status_buf);

    try packet.writePacket(w, seq_id, payload.items);
}

/// Send an ERR_Packet at the given sequence id.
pub fn sendErrPacket(
    allocator: Allocator,
    w: *std.Io.Writer,
    seq_id: u8,
    code: u16,
    sqlstate: [5]u8,
    message: []const u8,
) !void {
    var payload: std.ArrayList(u8) = .empty;
    defer payload.deinit(allocator);

    try payload.append(allocator, 0xFF);

    var code_buf: [2]u8 = undefined;
    std.mem.writeInt(u16, &code_buf, code, .little);
    try payload.appendSlice(allocator, &code_buf);

    try payload.append(allocator, '#');
    try payload.appendSlice(allocator, &sqlstate);
    try payload.appendSlice(allocator, message);

    try packet.writePacket(w, seq_id, payload.items);
}

test "handshake response parses minimal Protocol41 payload" {
    var bytes: std.ArrayList(u8) = .empty;
    defer bytes.deinit(std.testing.allocator);

    var caps_buf: [4]u8 = undefined;
    std.mem.writeInt(u32, &caps_buf, CLIENT_PROTOCOL_41 | CLIENT_SECURE_CONNECTION, .little);
    try bytes.appendSlice(std.testing.allocator, &caps_buf);

    var ms_buf: [4]u8 = undefined;
    std.mem.writeInt(u32, &ms_buf, 0x01000000, .little);
    try bytes.appendSlice(std.testing.allocator, &ms_buf);

    try bytes.append(std.testing.allocator, 0xff);
    try bytes.appendSlice(std.testing.allocator, &([_]u8{0} ** 23));
    try bytes.appendSlice(std.testing.allocator, "alice\x00");
    try bytes.append(std.testing.allocator, 0);

    const parsed = try parseHandshakeResponse(bytes.items);
    try std.testing.expectEqualStrings("alice", parsed.username);
    try std.testing.expectEqual(@as(u8, 0xff), parsed.character_set);
}
