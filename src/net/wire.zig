//! Wire protocol shared by TCP transport (and reused by the in-process
//! transport where it makes sense). Two concerns live here:
//!
//!   1. **Frames** — every message client↔server is a tagged + length-
//!      prefixed envelope. Decoupling framing from content means new
//!      message types land without touching the read loop.
//!
//!   2. **Batch wire format** — the columnar payload streamed back to
//!      the client for query results AND the format INSERT uses to ship
//!      bulk rows to the server. One encoder/decoder for both directions.
//!
//! Frame layout (8-byte header + payload):
//!
//!   [msg_type u8][reserved u8][reserved u16][payload_len u32 LE][payload]
//!
//! Reserved bytes are zeroed for now; future use: per-message flags
//! (compression, more len-bits, etc.).
//!
//! Batch layout:
//!
//!   [row_count u32 LE]
//!   [col_count u32 LE]
//!   per column (in schema order):
//!     [name_len u32][name bytes]
//!     [type_tag u8]                (ValueTag — stable enum values)
//!     [nullable u8]                (1 if column carries a null bitmap)
//!     [type_extra u32]             (VARCHAR/CHAR length; for DECIMAL:
//!                                   (precision << 8) | scale; else 0)
//!     for fixed-width data:
//!       [data_len u32][data bytes (raw little-endian per element)]
//!     for string-like (varchar/string/char):
//!       [offsets_len u32][offsets bytes (u32 LE × row_count+1)]
//!       [bytes_len u32][bytes]
//!     if nullable:
//!       [nulls_len u32][nulls bytes (validity bitmap)]
//!
//! The format is intentionally similar to the on-disk row-group layout
//! (column-major, raw bytes, length-prefixed) but without zstd
//! compression. Compression on the wire is a future option.

const std = @import("std");
const Allocator = std.mem.Allocator;

const types = @import("../types.zig");
const Type = types.Type;
const TypeTag = types.TypeTag;
const ValueTag = types.ValueTag;
const Schema = types.Schema;
const Column = types.Column;

const storage = @import("../storage/storage.zig");
const ColumnView = storage.ColumnView;
const StringView = storage.StringView;
const column_mod = storage.column;

const exec = @import("../exec/exec.zig");
const Batch = exec.Batch;

pub const MsgType = enum(u8) {
    // Request types (client → server)
    req_query = 0x01,
    req_delete = 0x02,
    req_insert = 0x03,
    req_create_table = 0x04,
    req_drop_table = 0x05,
    req_rename_table = 0x06,
    req_alter_table = 0x07,
    req_flush = 0x08,
    req_compact = 0x09,
    // Response types (server → client)
    resp_ok = 0x80,
    resp_error = 0x81,
    resp_batch = 0x82,
    resp_end = 0x83,
};

pub const Error = error{
    WireBadMagic,
    WireTooSmall,
    WireUnknownMsgType,
    WireCorrupt,
    WireUnknownType,
};

pub const frame_header_size: usize = 8;

/// Write a framed message into `out`: header + payload bytes.
/// Caller owns `out`. Useful for both server-side response building and
/// client-side request building.
pub fn writeFrame(
    allocator: Allocator,
    out: *std.ArrayList(u8),
    msg_type: MsgType,
    payload: []const u8,
) !void {
    var hdr: [frame_header_size]u8 = .{0} ** frame_header_size;
    hdr[0] = @intFromEnum(msg_type);
    // hdr[1..4] reserved, zero
    std.mem.writeInt(u32, hdr[4..8], @intCast(payload.len), .little);
    try out.appendSlice(allocator, &hdr);
    try out.appendSlice(allocator, payload);
}

/// Parse a single frame header from `bytes` starting at `offset`. Returns
/// the message type + the payload slice (borrowed from `bytes`). Advances
/// `*offset` past the payload.
pub fn readFrame(bytes: []const u8, offset: *usize) Error!struct { msg_type: MsgType, payload: []const u8 } {
    if (offset.* + frame_header_size > bytes.len) return Error.WireTooSmall;
    const tag_byte = bytes[offset.*];
    // Tag must be a known MsgType. We allow gaps in the numeric range so
    // do an exhaustive validation rather than a simple `<` check.
    const msg_type = validMsgType(tag_byte) orelse return Error.WireUnknownMsgType;
    const payload_len = std.mem.readInt(u32, bytes[offset.* + 4 ..][0..4], .little);
    const payload_start = offset.* + frame_header_size;
    const payload_end = payload_start + payload_len;
    if (payload_end > bytes.len) return Error.WireTooSmall;
    offset.* = payload_end;
    return .{
        .msg_type = msg_type,
        .payload = bytes[payload_start..payload_end],
    };
}

fn validMsgType(b: u8) ?MsgType {
    inline for (@typeInfo(MsgType).@"enum".fields) |f| {
        if (f.value == b) return @enumFromInt(b);
    }
    return null;
}

// ---------------------------------------------------------------------------
// Batch wire format
// ---------------------------------------------------------------------------

/// Encode a Batch into `out`. The format is column-major, length-prefixed,
/// and round-trips the schema (names + types + nullability) so the
/// receiver can reconstruct everything without needing to know the schema
/// out-of-band. That's important for query results from arbitrary
/// pipelines (server's output schema isn't known until it runs).
pub fn encodeBatch(allocator: Allocator, out: *std.ArrayList(u8), batch: Batch) !void {
    try appendU32(allocator, out, @intCast(batch.row_count));
    try appendU32(allocator, out, @intCast(batch.schema.len));

    for (batch.schema, batch.values) |col, view| {
        // Column header
        try appendU32(allocator, out, @intCast(col.name.len));
        try out.appendSlice(allocator, col.name);
        try out.append(allocator, @intFromEnum(@as(TypeTag, col.type)));
        try out.append(allocator, @intFromBool(col.nullable));
        try appendU32(allocator, out, typeExtra(col.type));

        // Column data
        try encodeColumnData(allocator, out, col.type, view, batch.row_count);

        // Null bitmap
        if (view.nulls) |nb| {
            try appendU32(allocator, out, @intCast(nb.len));
            try out.appendSlice(allocator, nb);
        } else {
            try appendU32(allocator, out, 0);
        }
    }
}

fn typeExtra(t: Type) u32 {
    return switch (t) {
        .varchar => |n| n,
        .char => |n| n,
        .decimal64 => |spec| (@as(u32, spec.p) << 8) | @as(u32, spec.s),
        .decimal128 => |spec| (@as(u32, spec.p) << 8) | @as(u32, spec.s),
        else => 0,
    };
}

fn encodeColumnData(
    allocator: Allocator,
    out: *std.ArrayList(u8),
    t: Type,
    view: ColumnView,
    row_count: usize,
) !void {
    switch (view.data) {
        .int => |s| try writeFixedBytes(allocator, out, std.mem.sliceAsBytes(s[0..row_count])),
        .bigint => |s| try writeFixedBytes(allocator, out, std.mem.sliceAsBytes(s[0..row_count])),
        .boolean => |s| try writeFixedBytes(allocator, out, s[0..row_count]),
        .float => |s| try writeFixedBytes(allocator, out, std.mem.sliceAsBytes(s[0..row_count])),
        .double => |s| try writeFixedBytes(allocator, out, std.mem.sliceAsBytes(s[0..row_count])),
        .date => |s| try writeFixedBytes(allocator, out, std.mem.sliceAsBytes(s[0..row_count])),
        .datetime => |s| try writeFixedBytes(allocator, out, std.mem.sliceAsBytes(s[0..row_count])),
        .tinyint => |s| try writeFixedBytes(allocator, out, std.mem.sliceAsBytes(s[0..row_count])),
        .smallint => |s| try writeFixedBytes(allocator, out, std.mem.sliceAsBytes(s[0..row_count])),
        .largeint => |s| try writeFixedBytes(allocator, out, std.mem.sliceAsBytes(s[0..row_count])),
        .decimal64 => |s| try writeFixedBytes(allocator, out, std.mem.sliceAsBytes(s[0..row_count])),
        .decimal128 => |s| try writeFixedBytes(allocator, out, std.mem.sliceAsBytes(s[0..row_count])),
        .varchar => |sv| try writeStringColumn(allocator, out, sv, row_count),
        .string => |sv| try writeStringColumn(allocator, out, sv, row_count),
        .char => |sv| try writeStringColumn(allocator, out, sv, row_count),
    }
    _ = t;
}

fn writeFixedBytes(allocator: Allocator, out: *std.ArrayList(u8), bytes: []const u8) !void {
    try appendU32(allocator, out, @intCast(bytes.len));
    try out.appendSlice(allocator, bytes);
}

fn writeStringColumn(
    allocator: Allocator,
    out: *std.ArrayList(u8),
    sv: StringView,
    row_count: usize,
) !void {
    // offsets: row_count + 1 entries × 4 bytes
    const offsets_bytes = sv.offsets[0 .. row_count + 1];
    try appendU32(allocator, out, @intCast(offsets_bytes.len * @sizeOf(u32)));
    try out.appendSlice(allocator, std.mem.sliceAsBytes(offsets_bytes));
    // bytes: up to offsets[row_count]
    const total_bytes = sv.offsets[row_count];
    try appendU32(allocator, out, total_bytes);
    try out.appendSlice(allocator, sv.bytes[0..total_bytes]);
}

// ---------------------------------------------------------------------------
// Little-endian helpers
// ---------------------------------------------------------------------------

fn appendU32(allocator: Allocator, out: *std.ArrayList(u8), v: u32) !void {
    var b: [4]u8 = undefined;
    std.mem.writeInt(u32, &b, v, .little);
    try out.appendSlice(allocator, &b);
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

test "wire: frame round-trips" {
    const allocator = std.testing.allocator;
    var buf: std.ArrayList(u8) = .empty;
    defer buf.deinit(allocator);

    const payload = "hello, server";
    try writeFrame(allocator, &buf, .req_query, payload);

    var off: usize = 0;
    const frame = try readFrame(buf.items, &off);
    try std.testing.expectEqual(MsgType.req_query, frame.msg_type);
    try std.testing.expectEqualStrings(payload, frame.payload);
    try std.testing.expectEqual(buf.items.len, off);
}

test "wire: multiple frames in one buffer (response streaming)" {
    const allocator = std.testing.allocator;
    var buf: std.ArrayList(u8) = .empty;
    defer buf.deinit(allocator);

    try writeFrame(allocator, &buf, .resp_batch, "batch1");
    try writeFrame(allocator, &buf, .resp_batch, "batch2");
    try writeFrame(allocator, &buf, .resp_end, "");

    var off: usize = 0;
    const f1 = try readFrame(buf.items, &off);
    try std.testing.expectEqual(MsgType.resp_batch, f1.msg_type);
    try std.testing.expectEqualStrings("batch1", f1.payload);
    const f2 = try readFrame(buf.items, &off);
    try std.testing.expectEqual(MsgType.resp_batch, f2.msg_type);
    try std.testing.expectEqualStrings("batch2", f2.payload);
    const f3 = try readFrame(buf.items, &off);
    try std.testing.expectEqual(MsgType.resp_end, f3.msg_type);
    try std.testing.expectEqual(@as(usize, 0), f3.payload.len);
}

test "wire: truncated header rejected" {
    const allocator = std.testing.allocator;
    var buf: std.ArrayList(u8) = .empty;
    defer buf.deinit(allocator);
    try buf.appendSlice(allocator, &[_]u8{ 0x01, 0, 0, 0, 0xFF, 0xFF });
    var off: usize = 0;
    try std.testing.expectError(Error.WireTooSmall, readFrame(buf.items, &off));
}

test "wire: unknown message type rejected" {
    const allocator = std.testing.allocator;
    var buf: std.ArrayList(u8) = .empty;
    defer buf.deinit(allocator);
    try buf.appendSlice(allocator, &[_]u8{ 0xEE, 0, 0, 0, 0, 0, 0, 0 });
    var off: usize = 0;
    try std.testing.expectError(Error.WireUnknownMsgType, readFrame(buf.items, &off));
}

test "wire: encodeBatch produces sensible length-prefixed output" {
    const allocator = std.testing.allocator;
    const thindb = @import("../root.zig");

    // Build a real batch by scanning a small in-memory table — easiest
    // way to get a well-formed ColumnView without re-implementing the
    // memtable.
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var conn = try thindb.local(allocator, std.testing.io, tmp.dir, .{});
    defer conn.close();

    const schema = thindb.Schema{
        .columns = &.{
            .{ .name = "id", .type = .bigint },
            .{ .name = "qty", .type = .int },
        },
        .order_key = &.{"id"},
        .unique = false,
    };
    const ok = [_][]const u8{"id"};
    const opts = thindb.TableOptions{ .order_key = &ok, .row_group_size = 1024 };
    const db = thindb.net.underlyingDb(conn);
    const t = try db.table("t", schema, opts);
    try t.insert(&.{
        .{ .id = @as(i64, 1), .qty = @as(i32, 10) },
        .{ .id = @as(i64, 2), .qty = @as(i32, 20) },
        .{ .id = @as(i64, 3), .qty = @as(i32, 30) },
    });

    var q = try conn.scan("t");
    defer q.deinit();
    const b = (try q.next()).?;

    var enc: std.ArrayList(u8) = .empty;
    defer enc.deinit(allocator);
    try encodeBatch(allocator, &enc, b);

    // Header is at minimum: row_count(4) + col_count(4).
    try std.testing.expect(enc.items.len > 8);
    // First two u32s match the batch.
    try std.testing.expectEqual(@as(u32, 3), std.mem.readInt(u32, enc.items[0..4], .little));
    try std.testing.expectEqual(@as(u32, 2), std.mem.readInt(u32, enc.items[4..8], .little));
}
