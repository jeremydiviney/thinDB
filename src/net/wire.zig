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

const api = @import("../api/api.zig");
const AlterOp = api.AlterOp;

const ir = @import("../ir/ir.zig");

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

/// Write a framed message directly to an Io.Writer (streaming variant
/// of `writeFrame`). Used by the TCP server to push response frames
/// straight onto the socket without an intermediate ArrayList.
pub fn writeFrameToIo(
    w: *std.Io.Writer,
    msg_type: MsgType,
    payload: []const u8,
) std.Io.Writer.Error!void {
    var hdr: [frame_header_size]u8 = .{0} ** frame_header_size;
    hdr[0] = @intFromEnum(msg_type);
    std.mem.writeInt(u32, hdr[4..8], @intCast(payload.len), .little);
    try w.writeAll(&hdr);
    try w.writeAll(payload);
}

/// Encode a `[len u32][bytes]` length-prefixed string into `out`.
pub fn appendLenString(allocator: Allocator, out: *std.ArrayList(u8), s: []const u8) !void {
    try appendU32(allocator, out, @intCast(s.len));
    try out.appendSlice(allocator, s);
}

/// Encode a Schema into `out` (unique + columns + order_key).
///
/// Layout:
///   [unique u8]
///   [col_count u32]
///   per column: [name_len u32][name][type_tag u8][nullable u8][type_extra u32]
///   [order_key_count u32]
///   per key: [name_len u32][name]
pub fn encodeSchema(allocator: Allocator, out: *std.ArrayList(u8), schema: Schema) !void {
    try out.append(allocator, @intFromBool(schema.unique));

    try appendU32(allocator, out, @intCast(schema.columns.len));
    for (schema.columns) |c| {
        try appendLenString(allocator, out, c.name);
        try out.append(allocator, @intFromEnum(@as(TypeTag, c.type)));
        try out.append(allocator, @intFromBool(c.nullable));
        try appendU32(allocator, out, typeExtra(c.type));
    }

    try appendU32(allocator, out, @intCast(schema.order_key.len));
    for (schema.order_key) |k| try appendLenString(allocator, out, k);
}

/// Owned-decode counterpart of `encodeSchema`. All slices land in
/// `allocator` (an arena works well — caller can drop everything in one
/// shot). The strings themselves are dup'd because they're borrowed
/// from the input buffer and outlive the request handler.
pub fn decodeSchema(allocator: Allocator, bytes: []const u8, cursor: *usize) !Schema {
    if (cursor.* + 1 > bytes.len) return Error.WireCorrupt;
    const unique = bytes[cursor.*] != 0;
    cursor.* += 1;

    const col_count = try readU32(bytes, cursor);
    const cols = try allocator.alloc(Column, col_count);
    for (cols) |*c| {
        const name = try readLenString(bytes, cursor);
        if (cursor.* + 1 + 1 + 4 > bytes.len) return Error.WireCorrupt;
        const tag_byte = bytes[cursor.*];
        cursor.* += 1;
        const nullable = bytes[cursor.*] != 0;
        cursor.* += 1;
        const extra = std.mem.readInt(u32, bytes[cursor.*..][0..4], .little);
        cursor.* += 4;
        const t = try typeFromTagAndExtra(tag_byte, extra);
        c.* = .{
            .name = try allocator.dupe(u8, name),
            .type = t,
            .nullable = nullable,
        };
    }

    const order_key_count = try readU32(bytes, cursor);
    const order_key = try allocator.alloc([]const u8, order_key_count);
    for (order_key) |*k| {
        const s = try readLenString(bytes, cursor);
        k.* = try allocator.dupe(u8, s);
    }

    return .{ .columns = cols, .order_key = order_key, .unique = unique };
}

/// Encode one AlterOp. Layout:
///   [op_tag u8]                          0=add, 1=drop, 2=rename
///   add:     [name_len u32][name][type_tag u8][nullable u8][type_extra u32][value (tagged)]
///   drop:    [name_len u32][name]
///   rename:  [from_len u32][from][to_len u32][to]
pub fn encodeAlterOp(allocator: Allocator, out: *std.ArrayList(u8), op: AlterOp) !void {
    switch (op) {
        .add => |a| {
            try out.append(allocator, 0);
            try appendLenString(allocator, out, a.name);
            try out.append(allocator, @intFromEnum(@as(TypeTag, a.type)));
            try out.append(allocator, @intFromBool(a.nullable));
            try appendU32(allocator, out, typeExtra(a.type));
            try ir.encodeValue(allocator, out, a.default);
        },
        .drop => |name| {
            try out.append(allocator, 1);
            try appendLenString(allocator, out, name);
        },
        .rename => |r| {
            try out.append(allocator, 2);
            try appendLenString(allocator, out, r.from);
            try appendLenString(allocator, out, r.to);
        },
    }
}

/// Inverse of `encodeAlterOp`. Allocates into `allocator` — an arena
/// is appropriate.
pub fn decodeAlterOp(allocator: Allocator, bytes: []const u8, cursor: *usize) !AlterOp {
    if (cursor.* + 1 > bytes.len) return Error.WireCorrupt;
    const tag = bytes[cursor.*];
    cursor.* += 1;
    return switch (tag) {
        0 => blk: {
            const name = try readLenString(bytes, cursor);
            if (cursor.* + 1 + 1 + 4 > bytes.len) return Error.WireCorrupt;
            const tag_byte = bytes[cursor.*];
            cursor.* += 1;
            const nullable = bytes[cursor.*] != 0;
            cursor.* += 1;
            const extra = std.mem.readInt(u32, bytes[cursor.*..][0..4], .little);
            cursor.* += 4;
            const t = try typeFromTagAndExtra(tag_byte, extra);
            const v = ir.decodeValue(bytes, cursor) catch return Error.WireCorrupt;
            break :blk AlterOp{ .add = .{
                .name = try allocator.dupe(u8, name),
                .type = t,
                .nullable = nullable,
                .default = try dupeValue(allocator, v),
            } };
        },
        1 => blk: {
            const name = try readLenString(bytes, cursor);
            break :blk AlterOp{ .drop = try allocator.dupe(u8, name) };
        },
        2 => blk: {
            const from = try readLenString(bytes, cursor);
            const to = try readLenString(bytes, cursor);
            break :blk AlterOp{ .rename = .{
                .from = try allocator.dupe(u8, from),
                .to = try allocator.dupe(u8, to),
            } };
        },
        else => Error.WireCorrupt,
    };
}

fn dupeValue(allocator: Allocator, v: @import("../types.zig").Value) !@import("../types.zig").Value {
    return switch (v) {
        .text => |s| .{ .text = try allocator.dupe(u8, s) },
        else => v,
    };
}

/// Inverse of appendLenString — returns a borrowed slice into `bytes`.
pub fn readLenString(bytes: []const u8, cursor: *usize) ![]const u8 {
    if (cursor.* + 4 > bytes.len) return Error.WireCorrupt;
    const len = std.mem.readInt(u32, bytes[cursor.*..][0..4], .little);
    cursor.* += 4;
    if (cursor.* + len > bytes.len) return Error.WireCorrupt;
    const s = bytes[cursor.* .. cursor.* + len];
    cursor.* += len;
    return s;
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

pub fn typeExtra(t: Type) u32 {
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
// Batch decode
// ---------------------------------------------------------------------------

/// Decode a Batch from `bytes`. All allocations land in `allocator` —
/// the returned DecodedBatch holds owned Schema + per-column data arrays
/// (decoder always copies into freshly-allocated storage so the caller
/// can free the source bytes immediately).
///
/// Caller frees via `DecodedBatch.deinit`.
pub fn decodeBatch(allocator: Allocator, bytes: []const u8) !DecodedBatch {
    var cursor: usize = 0;
    const row_count = try readU32(bytes, &cursor);
    const col_count = try readU32(bytes, &cursor);

    var schema = try allocator.alloc(Column, col_count);
    errdefer allocator.free(schema);
    var views = try allocator.alloc(ColumnView, col_count);
    errdefer allocator.free(views);
    var owned = try allocator.alloc(OwnedColumnBuffers, col_count);
    errdefer allocator.free(owned);
    var inited: u32 = 0;
    errdefer {
        var i: u32 = 0;
        while (i < inited) : (i += 1) owned[i].deinit(allocator);
    }

    var ci: u32 = 0;
    while (ci < col_count) : (ci += 1) {
        // Header
        const name_len = try readU32(bytes, &cursor);
        if (cursor + name_len > bytes.len) return Error.WireCorrupt;
        const name = try allocator.dupe(u8, bytes[cursor .. cursor + name_len]);
        errdefer allocator.free(name);
        cursor += name_len;

        if (cursor + 1 + 1 + 4 > bytes.len) return Error.WireCorrupt;
        const type_tag_byte = bytes[cursor];
        cursor += 1;
        const nullable = bytes[cursor] != 0;
        cursor += 1;
        const type_extra = std.mem.readInt(u32, bytes[cursor..][0..4], .little);
        cursor += 4;

        const col_type = try typeFromTagAndExtra(type_tag_byte, type_extra);

        // Data section
        owned[ci] = try decodeColumnData(allocator, col_type, bytes, &cursor, row_count);
        inited = ci + 1;

        // Nulls
        const nulls_len = try readU32(bytes, &cursor);
        const nulls: ?[]const u8 = if (nulls_len > 0) blk: {
            if (cursor + nulls_len > bytes.len) return Error.WireCorrupt;
            const dup = try allocator.dupe(u8, bytes[cursor .. cursor + nulls_len]);
            cursor += nulls_len;
            break :blk dup;
        } else null;
        owned[ci].nulls = nulls;

        schema[ci] = .{ .name = name, .type = col_type, .nullable = nullable };
        views[ci] = .{ .data = owned[ci].view(col_type, row_count), .nulls = nulls };
    }

    return .{
        .allocator = allocator,
        .row_count = row_count,
        .schema = schema,
        .views = views,
        .owned = owned,
    };
}

pub const DecodedBatch = struct {
    allocator: Allocator,
    row_count: u32,
    schema: []Column,
    views: []ColumnView,
    owned: []OwnedColumnBuffers,

    pub fn deinit(self: *DecodedBatch) void {
        for (self.owned) |*o| o.deinit(self.allocator);
        for (self.schema) |c| self.allocator.free(c.name);
        self.allocator.free(self.schema);
        self.allocator.free(self.views);
        self.allocator.free(self.owned);
        self.* = undefined;
    }

    pub fn batch(self: *const DecodedBatch) Batch {
        return .{
            .schema = self.schema,
            .values = self.views,
            .row_count = self.row_count,
        };
    }
};

/// Per-column owned storage produced by the batch decoder. Releases all
/// allocations on `deinit`. The view layout matches storage.ColumnView's
/// data union — same shape as what an in-process scan produces.
pub const OwnedColumnBuffers = struct {
    /// Allocated 16-byte aligned so `bytesAsSlice(T)` produces a properly
    /// aligned typed slice for every primitive type (i128 is the most
    /// restrictive at 16 bytes).
    data: []align(16) u8,
    offsets: ?[]u32 = null, // string-column only
    nulls: ?[]const u8 = null,

    pub fn deinit(self: *OwnedColumnBuffers, allocator: Allocator) void {
        allocator.free(self.data);
        if (self.offsets) |o| allocator.free(o);
        if (self.nulls) |n| allocator.free(n);
        self.* = undefined;
    }

    pub fn view(self: *const OwnedColumnBuffers, t: Type, row_count: u32) column_mod.ValueView {
        const n: usize = row_count;
        return switch (t) {
            .int => .{ .int = std.mem.bytesAsSlice(i32, self.data)[0..n] },
            .bigint => .{ .bigint = std.mem.bytesAsSlice(i64, self.data)[0..n] },
            .boolean => .{ .boolean = self.data[0..n] },
            .float => .{ .float = std.mem.bytesAsSlice(f32, self.data)[0..n] },
            .double => .{ .double = std.mem.bytesAsSlice(f64, self.data)[0..n] },
            .date => .{ .date = std.mem.bytesAsSlice(i32, self.data)[0..n] },
            .datetime => .{ .datetime = std.mem.bytesAsSlice(i64, self.data)[0..n] },
            .tinyint => .{ .tinyint = std.mem.bytesAsSlice(i8, self.data)[0..n] },
            .smallint => .{ .smallint = std.mem.bytesAsSlice(i16, self.data)[0..n] },
            .largeint => .{ .largeint = std.mem.bytesAsSlice(i128, self.data)[0..n] },
            .decimal64 => .{ .decimal64 = std.mem.bytesAsSlice(i64, self.data)[0..n] },
            .decimal128 => .{ .decimal128 = std.mem.bytesAsSlice(i128, self.data)[0..n] },
            .varchar => .{ .varchar = .{ .offsets = self.offsets.?, .bytes = self.data } },
            .string => .{ .string = .{ .offsets = self.offsets.?, .bytes = self.data } },
            .char => .{ .char = .{ .offsets = self.offsets.?, .bytes = self.data } },
        };
    }
};

fn decodeColumnData(
    allocator: Allocator,
    t: Type,
    bytes: []const u8,
    cursor: *usize,
    row_count: u32,
) !OwnedColumnBuffers {
    switch (t) {
        .varchar, .string, .char => {
            // offsets section
            const offsets_len = try readU32(bytes, cursor);
            if (cursor.* + offsets_len > bytes.len) return Error.WireCorrupt;
            const n_offsets = offsets_len / @sizeOf(u32);
            const offsets = try allocator.alloc(u32, n_offsets);
            errdefer allocator.free(offsets);
            @memcpy(std.mem.sliceAsBytes(offsets), bytes[cursor.* .. cursor.* + offsets_len]);
            cursor.* += offsets_len;
            // bytes section — string bytes don't need alignment
            const bytes_len = try readU32(bytes, cursor);
            if (cursor.* + bytes_len > bytes.len) return Error.WireCorrupt;
            const dup = try allocAlignedDup(allocator, bytes[cursor.* .. cursor.* + bytes_len]);
            cursor.* += bytes_len;
            _ = row_count;
            return .{ .data = dup, .offsets = offsets };
        },
        else => {
            const data_len = try readU32(bytes, cursor);
            if (cursor.* + data_len > bytes.len) return Error.WireCorrupt;
            // Allocate to 16-byte alignment so bytesAsSlice produces a
            // properly-aligned typed slice for any of our primitive
            // types (max alignment requirement is i128 = 16).
            const dup = try allocAlignedDup(allocator, bytes[cursor.* .. cursor.* + data_len]);
            cursor.* += data_len;
            return .{ .data = dup };
        },
    }
}

fn allocAlignedDup(allocator: Allocator, src: []const u8) ![]align(16) u8 {
    const dst = try allocator.alignedAlloc(u8, .@"16", src.len);
    @memcpy(dst, src);
    return dst;
}

pub fn typeFromTagAndExtra(tag: u8, extra: u32) !Type {
    if (tag > @intFromEnum(TypeTag.decimal128)) return Error.WireUnknownType;
    const tt: TypeTag = @enumFromInt(tag);
    return switch (tt) {
        .int => .int,
        .bigint => .bigint,
        .boolean => .boolean,
        .float => .float,
        .double => .double,
        .date => .date,
        .datetime => .datetime,
        .tinyint => .tinyint,
        .smallint => .smallint,
        .largeint => .largeint,
        .varchar => .{ .varchar = @intCast(extra) },
        .string => .string,
        .char => .{ .char = @intCast(extra) },
        .decimal64 => .{ .decimal64 = .{
            .p = @intCast((extra >> 8) & 0xFF),
            .s = @intCast(extra & 0xFF),
        } },
        .decimal128 => .{ .decimal128 = .{
            .p = @intCast((extra >> 8) & 0xFF),
            .s = @intCast(extra & 0xFF),
        } },
    };
}

fn readU32(bytes: []const u8, cursor: *usize) !u32 {
    if (cursor.* + 4 > bytes.len) return Error.WireCorrupt;
    const v = std.mem.readInt(u32, bytes[cursor.*..][0..4], .little);
    cursor.* += 4;
    return v;
}

// ---------------------------------------------------------------------------
// Little-endian helpers
// ---------------------------------------------------------------------------

pub fn appendU32(allocator: Allocator, out: *std.ArrayList(u8), v: u32) !void {
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

test "wire: batch encode -> decode round-trip preserves data" {
    const allocator = std.testing.allocator;
    const thindb = @import("../root.zig");

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var conn = try thindb.local(allocator, std.testing.io, tmp.dir, .{});
    defer conn.close();

    const schema = thindb.Schema{
        .columns = &.{
            .{ .name = "id", .type = .bigint },
            .{ .name = "qty", .type = .int },
            .{ .name = "tag", .type = .string },
            .{ .name = "active", .type = .boolean, .nullable = true },
        },
        .order_key = &.{"id"},
        .unique = false,
    };
    const ok = [_][]const u8{"id"};
    const opts = thindb.TableOptions{ .order_key = &ok, .row_group_size = 1024 };
    const db = thindb.net.underlyingDb(conn);
    const t = try db.table("t", schema, opts);
    try t.insert(&.{
        .{ .id = @as(i64, 1), .qty = @as(i32, 10), .tag = "alpha", .active = @as(?bool, true) },
        .{ .id = @as(i64, 2), .qty = @as(i32, 20), .tag = "beta", .active = @as(?bool, null) },
        .{ .id = @as(i64, 3), .qty = @as(i32, 30), .tag = "gamma", .active = @as(?bool, false) },
    });

    var q = try conn.scan("t");
    defer q.deinit();
    const b = (try q.next()).?;

    var enc: std.ArrayList(u8) = .empty;
    defer enc.deinit(allocator);
    try encodeBatch(allocator, &enc, b);

    var decoded = try decodeBatch(allocator, enc.items);
    defer decoded.deinit();

    try std.testing.expectEqual(@as(u32, 3), decoded.row_count);
    try std.testing.expectEqual(@as(usize, 4), decoded.schema.len);
    try std.testing.expectEqualStrings("id", decoded.schema[0].name);
    try std.testing.expectEqualStrings("tag", decoded.schema[2].name);
    try std.testing.expect(decoded.schema[3].nullable);

    const db2 = decoded.batch();
    try std.testing.expectEqualSlices(i64, &[_]i64{ 1, 2, 3 }, db2.values[0].data.bigint);
    try std.testing.expectEqualSlices(i32, &[_]i32{ 10, 20, 30 }, db2.values[1].data.int);
    try std.testing.expectEqualStrings("alpha", db2.values[2].data.string.rowBytes(0));
    try std.testing.expectEqualStrings("gamma", db2.values[2].data.string.rowBytes(2));
    // active is nullable: rows 0 + 2 valid, row 1 null
    try std.testing.expect(db2.values[3].isValid(0));
    try std.testing.expect(!db2.values[3].isValid(1));
    try std.testing.expect(db2.values[3].isValid(2));
}
