//! MySQL wire-format primitives.
//!
//! Every MySQL packet has a 4-byte header: 3-byte little-endian payload
//! length + 1-byte sequence id. Sequence id increments per packet inside
//! a command's request/response cycle and resets to 0 on each new
//! client command.

const std = @import("std");
const Allocator = std.mem.Allocator;

pub const Error = error{
    PacketTooLarge,
    PacketTruncated,
    BadLenEnc,
};

pub const header_size: usize = 4;
pub const max_payload_len: usize = (1 << 24) - 1;

/// Encode a length-encoded integer into `out`.
pub fn appendLenEncInt(allocator: Allocator, out: *std.ArrayList(u8), v: u64) !void {
    if (v < 251) {
        try out.append(allocator, @intCast(v));
    } else if (v < (1 << 16)) {
        try out.append(allocator, 0xFC);
        var b: [2]u8 = undefined;
        std.mem.writeInt(u16, &b, @intCast(v), .little);
        try out.appendSlice(allocator, &b);
    } else if (v < (1 << 24)) {
        try out.append(allocator, 0xFD);
        var b: [4]u8 = undefined;
        std.mem.writeInt(u32, &b, @intCast(v), .little);
        try out.appendSlice(allocator, b[0..3]);
    } else {
        try out.append(allocator, 0xFE);
        var b: [8]u8 = undefined;
        std.mem.writeInt(u64, &b, v, .little);
        try out.appendSlice(allocator, &b);
    }
}

/// Encode a length-encoded string (length prefix + raw bytes).
pub fn appendLenEncString(allocator: Allocator, out: *std.ArrayList(u8), s: []const u8) !void {
    try appendLenEncInt(allocator, out, @intCast(s.len));
    try out.appendSlice(allocator, s);
}

pub fn appendNulString(allocator: Allocator, out: *std.ArrayList(u8), s: []const u8) !void {
    try out.appendSlice(allocator, s);
    try out.append(allocator, 0);
}

/// Read a length-encoded integer at `cursor` in `bytes`. Advances cursor.
pub fn readLenEncInt(bytes: []const u8, cursor: *usize) Error!u64 {
    if (cursor.* >= bytes.len) return Error.PacketTruncated;
    const first = bytes[cursor.*];
    cursor.* += 1;
    return switch (first) {
        0...250 => @as(u64, first),
        0xFC => readFixed(u16, bytes, cursor),
        0xFD => readFixed24(bytes, cursor),
        0xFE => readFixed(u64, bytes, cursor),
        else => Error.BadLenEnc,
    };
}

/// Read a length-encoded string at `cursor`. Returns a borrowed slice.
pub fn readLenEncString(bytes: []const u8, cursor: *usize) Error![]const u8 {
    const len = try readLenEncInt(bytes, cursor);
    if (cursor.* + len > bytes.len) return Error.PacketTruncated;
    const s = bytes[cursor.* .. cursor.* + @as(usize, @intCast(len))];
    cursor.* += @intCast(len);
    return s;
}

/// Read a NUL-terminated string. Returns the bytes before the NUL; cursor
/// lands AFTER the NUL.
pub fn readNulString(bytes: []const u8, cursor: *usize) Error![]const u8 {
    const start = cursor.*;
    while (cursor.* < bytes.len and bytes[cursor.*] != 0) : (cursor.* += 1) {}
    if (cursor.* >= bytes.len) return Error.PacketTruncated;
    const s = bytes[start..cursor.*];
    cursor.* += 1;
    return s;
}

pub fn readFixedU8(bytes: []const u8, cursor: *usize) Error!u8 {
    if (cursor.* + 1 > bytes.len) return Error.PacketTruncated;
    const v = bytes[cursor.*];
    cursor.* += 1;
    return v;
}

pub fn readFixedU16(bytes: []const u8, cursor: *usize) Error!u16 {
    return @intCast(try readFixed(u16, bytes, cursor));
}

pub fn readFixedU32(bytes: []const u8, cursor: *usize) Error!u32 {
    return @intCast(try readFixed(u32, bytes, cursor));
}

fn readFixed(comptime T: type, bytes: []const u8, cursor: *usize) Error!u64 {
    const size = @sizeOf(T);
    if (cursor.* + size > bytes.len) return Error.PacketTruncated;
    const v = std.mem.readInt(T, bytes[cursor.*..][0..size], .little);
    cursor.* += size;
    return @intCast(v);
}

fn readFixed24(bytes: []const u8, cursor: *usize) Error!u64 {
    if (cursor.* + 3 > bytes.len) return Error.PacketTruncated;
    const v: u32 = @as(u32, bytes[cursor.*]) |
        (@as(u32, bytes[cursor.* + 1]) << 8) |
        (@as(u32, bytes[cursor.* + 2]) << 16);
    cursor.* += 3;
    return @intCast(v);
}

/// Write a single MySQL packet (header + payload) to `w`. Payload must
/// fit in 16 MiB (per-packet protocol limit). Caller flushes.
pub fn writePacket(w: *std.Io.Writer, seq_id: u8, payload: []const u8) !void {
    if (payload.len > max_payload_len) return Error.PacketTooLarge;
    var hdr: [header_size]u8 = undefined;
    hdr[0] = @intCast(payload.len & 0xFF);
    hdr[1] = @intCast((payload.len >> 8) & 0xFF);
    hdr[2] = @intCast((payload.len >> 16) & 0xFF);
    hdr[3] = seq_id;
    try w.writeAll(&hdr);
    try w.writeAll(payload);
}

pub const Header = struct { len: u32, seq_id: u8 };

/// Read one packet's 4-byte header. Blocking here is "idle between
/// commands" — unbounded by design. Split from `readBody` so the server
/// can bound the payload wait (net_read_timeout, #164) without putting
/// a timeout on idle connections.
pub fn readHeader(r: *std.Io.Reader) !Header {
    var hdr: [header_size]u8 = undefined;
    try r.readSliceAll(&hdr);
    return .{
        .len = @as(u32, hdr[0]) | (@as(u32, hdr[1]) << 8) | (@as(u32, hdr[2]) << 16),
        .seq_id = hdr[3],
    };
}

/// Read a packet payload of `len` bytes into an allocator-owned slice
/// (caller frees). Blocking here is "mid-packet" — the client has
/// committed to a length and the rest must arrive promptly.
pub fn readBody(allocator: Allocator, r: *std.Io.Reader, len: u32) ![]u8 {
    const payload = try allocator.alloc(u8, len);
    errdefer allocator.free(payload);
    try r.readSliceAll(payload);
    return payload;
}

/// Read one MySQL packet header + payload. Returns the sequence id and
/// an allocator-owned payload slice (caller frees).
pub fn readPacket(allocator: Allocator, r: *std.Io.Reader) !struct { seq_id: u8, payload: []u8 } {
    const hdr = try readHeader(r);
    const payload = try readBody(allocator, r, hdr.len);
    return .{ .seq_id = hdr.seq_id, .payload = payload };
}

test "lenenc int round-trips across size brackets" {
    const allocator = std.testing.allocator;
    const cases = [_]u64{ 0, 1, 250, 251, 0xFF, 0xFFFF, 0x10000, 0xFFFFFF, 0x1000000, std.math.maxInt(u32), std.math.maxInt(u64) };
    for (cases) |v| {
        var buf: std.ArrayList(u8) = .empty;
        defer buf.deinit(allocator);
        try appendLenEncInt(allocator, &buf, v);
        var cursor: usize = 0;
        const got = try readLenEncInt(buf.items, &cursor);
        try std.testing.expectEqual(v, got);
        try std.testing.expectEqual(buf.items.len, cursor);
    }
}

test "lenenc string round-trips" {
    const allocator = std.testing.allocator;
    var buf: std.ArrayList(u8) = .empty;
    defer buf.deinit(allocator);
    try appendLenEncString(allocator, &buf, "hello");
    try appendLenEncString(allocator, &buf, "");
    try appendLenEncString(allocator, &buf, "a longer string with spaces");

    var cursor: usize = 0;
    try std.testing.expectEqualStrings("hello", try readLenEncString(buf.items, &cursor));
    try std.testing.expectEqualStrings("", try readLenEncString(buf.items, &cursor));
    try std.testing.expectEqualStrings("a longer string with spaces", try readLenEncString(buf.items, &cursor));
    try std.testing.expectEqual(buf.items.len, cursor);
}

test "nul string reads up to terminator" {
    const data = "user\x00password\x00";
    var cursor: usize = 0;
    try std.testing.expectEqualStrings("user", try readNulString(data, &cursor));
    try std.testing.expectEqualStrings("password", try readNulString(data, &cursor));
    try std.testing.expectEqual(@as(usize, data.len), cursor);
}
