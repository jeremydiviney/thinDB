//! PostgreSQL wire-format primitives.
//!
//! Two frame shapes exist on the wire:
//!   - **Startup frames** (initial StartupMessage / SSLRequest): a 4-byte
//!     big-endian length (inclusive) followed by the payload. No type byte.
//!   - **Normal frames** (after startup): a 1-byte type code + 4-byte BE
//!     length (inclusive of itself) + payload.
//!
//! All integers on the wire are big-endian — opposite of MySQL. NUL-
//! terminated C strings carry key/value pairs in the startup message and
//! error fields.

const std = @import("std");
const Allocator = std.mem.Allocator;

pub const Error = error{
    FrameTooLarge,
    FrameTruncated,
    BadLength,
};

pub const startup_header_size: usize = 4;
pub const frame_header_size: usize = 5;
pub const max_frame_len: u32 = (1 << 30);

pub fn appendU16(allocator: Allocator, out: *std.ArrayList(u8), v: u16) !void {
    var buf: [2]u8 = undefined;
    std.mem.writeInt(u16, &buf, v, .big);
    try out.appendSlice(allocator, &buf);
}

pub fn appendU32(allocator: Allocator, out: *std.ArrayList(u8), v: u32) !void {
    var buf: [4]u8 = undefined;
    std.mem.writeInt(u32, &buf, v, .big);
    try out.appendSlice(allocator, &buf);
}

pub fn appendI16(allocator: Allocator, out: *std.ArrayList(u8), v: i16) !void {
    var buf: [2]u8 = undefined;
    std.mem.writeInt(i16, &buf, v, .big);
    try out.appendSlice(allocator, &buf);
}

pub fn appendI32(allocator: Allocator, out: *std.ArrayList(u8), v: i32) !void {
    var buf: [4]u8 = undefined;
    std.mem.writeInt(i32, &buf, v, .big);
    try out.appendSlice(allocator, &buf);
}

/// Append a NUL-terminated string.
pub fn appendCString(allocator: Allocator, out: *std.ArrayList(u8), s: []const u8) !void {
    try out.appendSlice(allocator, s);
    try out.append(allocator, 0);
}

pub fn readU16(bytes: []const u8, cursor: *usize) Error!u16 {
    if (cursor.* + 2 > bytes.len) return Error.FrameTruncated;
    const v = std.mem.readInt(u16, bytes[cursor.*..][0..2], .big);
    cursor.* += 2;
    return v;
}

pub fn readU32(bytes: []const u8, cursor: *usize) Error!u32 {
    if (cursor.* + 4 > bytes.len) return Error.FrameTruncated;
    const v = std.mem.readInt(u32, bytes[cursor.*..][0..4], .big);
    cursor.* += 4;
    return v;
}

pub fn readI32(bytes: []const u8, cursor: *usize) Error!i32 {
    if (cursor.* + 4 > bytes.len) return Error.FrameTruncated;
    const v = std.mem.readInt(i32, bytes[cursor.*..][0..4], .big);
    cursor.* += 4;
    return v;
}

/// Read a NUL-terminated string. Returns the bytes before the NUL; cursor
/// lands AFTER the NUL.
pub fn readCString(bytes: []const u8, cursor: *usize) Error![]const u8 {
    const start = cursor.*;
    while (cursor.* < bytes.len and bytes[cursor.*] != 0) : (cursor.* += 1) {}
    if (cursor.* >= bytes.len) return Error.FrameTruncated;
    const s = bytes[start..cursor.*];
    cursor.* += 1;
    return s;
}

/// Write a typed frame (`type_byte` + length + payload).
pub fn writeFrame(w: *std.Io.Writer, type_byte: u8, payload: []const u8) !void {
    const total: usize = 4 + payload.len;
    if (total > max_frame_len) return Error.FrameTooLarge;
    var hdr: [5]u8 = undefined;
    hdr[0] = type_byte;
    std.mem.writeInt(u32, hdr[1..5], @intCast(total), .big);
    try w.writeAll(&hdr);
    try w.writeAll(payload);
}

/// Write an untyped startup frame (length-prefix only). Used only by the
/// server side for parity in tests; real servers don't emit startup frames.
pub fn writeStartupFrame(w: *std.Io.Writer, payload: []const u8) !void {
    const total: usize = 4 + payload.len;
    if (total > max_frame_len) return Error.FrameTooLarge;
    var hdr: [4]u8 = undefined;
    std.mem.writeInt(u32, &hdr, @intCast(total), .big);
    try w.writeAll(&hdr);
    try w.writeAll(payload);
}

/// Read one startup-phase frame (length-prefix only, no type byte).
/// Returns the payload (the bytes AFTER the 4-byte length). Caller frees.
pub fn readStartupFrame(allocator: Allocator, r: *std.Io.Reader) !struct { payload: []u8 } {
    var hdr: [4]u8 = undefined;
    try r.readSliceAll(&hdr);
    const total = std.mem.readInt(u32, &hdr, .big);
    if (total < 4) return Error.BadLength;
    const body_len = total - 4;
    if (body_len > max_frame_len) return Error.FrameTooLarge;
    const payload = try allocator.alloc(u8, body_len);
    errdefer allocator.free(payload);
    try r.readSliceAll(payload);
    return .{ .payload = payload };
}

/// Read one normal-phase frame (type byte + length + payload). Returns
/// the type byte plus an allocator-owned payload slice (caller frees).
pub fn readFrame(allocator: Allocator, r: *std.Io.Reader) !struct { type_byte: u8, payload: []u8 } {
    var hdr: [5]u8 = undefined;
    try r.readSliceAll(&hdr);
    const type_byte = hdr[0];
    const total = std.mem.readInt(u32, hdr[1..5], .big);
    if (total < 4) return Error.BadLength;
    const body_len = total - 4;
    if (body_len > max_frame_len) return Error.FrameTooLarge;
    const payload = try allocator.alloc(u8, body_len);
    errdefer allocator.free(payload);
    try r.readSliceAll(payload);
    return .{ .type_byte = type_byte, .payload = payload };
}

test "appendU32 writes big-endian" {
    const allocator = std.testing.allocator;
    var out: std.ArrayList(u8) = .empty;
    defer out.deinit(allocator);
    try appendU32(allocator, &out, 0x01020304);
    try std.testing.expectEqualSlices(u8, &.{ 0x01, 0x02, 0x03, 0x04 }, out.items);
}

test "readU32 round-trips" {
    const allocator = std.testing.allocator;
    var out: std.ArrayList(u8) = .empty;
    defer out.deinit(allocator);
    try appendU32(allocator, &out, 196608);
    var cursor: usize = 0;
    try std.testing.expectEqual(@as(u32, 196608), try readU32(out.items, &cursor));
    try std.testing.expectEqual(out.items.len, cursor);
}

test "readCString stops at NUL and advances past" {
    const data = "user\x00postgres\x00";
    var cursor: usize = 0;
    try std.testing.expectEqualStrings("user", try readCString(data, &cursor));
    try std.testing.expectEqualStrings("postgres", try readCString(data, &cursor));
    try std.testing.expectEqual(@as(usize, data.len), cursor);
}
