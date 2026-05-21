//! Encode an `exec.Batch` stream as MySQL text-protocol result-set packets.
//! Walks the executable Query, emits ColumnCount + per-column ColumnDef41
//! + per-row ResultsetRow + a terminating EOF marker. Terminator format
//! depends on whether the client advertised CLIENT_DEPRECATE_EOF: the
//! flag also controls whether a separator EOF is sent between the
//! column-def sequence and the first DataRow.

const std = @import("std");
const Allocator = std.mem.Allocator;

const exec = @import("../../exec/exec.zig");
const Batch = exec.Batch;

const types = @import("../../types.zig");
const Column = types.Column;
const TypeTag = types.TypeTag;

const packet = @import("packet.zig");
const handshake = @import("handshake.zig");
const errors = @import("errors.zig");
const canned = @import("canned.zig");
const wire_format = @import("../wire_format.zig");

pub const MYSQL_TYPE_TINY: u8 = 0x01;
pub const MYSQL_TYPE_SHORT: u8 = 0x02;
pub const MYSQL_TYPE_LONG: u8 = 0x03;
pub const MYSQL_TYPE_FLOAT: u8 = 0x04;
pub const MYSQL_TYPE_DOUBLE: u8 = 0x05;
pub const MYSQL_TYPE_NULL: u8 = 0x06;
pub const MYSQL_TYPE_TIMESTAMP: u8 = 0x07;
pub const MYSQL_TYPE_LONGLONG: u8 = 0x08;
pub const MYSQL_TYPE_DATE: u8 = 0x0a;
pub const MYSQL_TYPE_DATETIME: u8 = 0x0c;
pub const MYSQL_TYPE_NEWDECIMAL: u8 = 0xf6;
pub const MYSQL_TYPE_VAR_STRING: u8 = 0xfd;
pub const MYSQL_TYPE_STRING: u8 = 0xfe;

const NOT_NULL_FLAG: u16 = 0x0001;
/// Numeric-type column marker. mysql CLI right-aligns columns that
/// advertise this flag; some drivers also use it as a hint for
/// integer-vs-string typing decisions when type_byte alone is
/// ambiguous (e.g., NEWDECIMAL).
const NUM_FLAG: u16 = 0x8000;

const CHARSET_UTF8MB4: u16 = 0x21;
const CHARSET_BINARY: u16 = 0x3f;

fn isNumericType(t: types.Type) bool {
    return switch (t) {
        .tinyint, .smallint, .int, .bigint, .largeint => true,
        .boolean => true,
        .float, .double => true,
        .decimal64, .decimal128 => true,
        else => false,
    };
}

fn mysqlTypeOf(t: types.Type) struct { type_byte: u8, decimals: u8, len: u32, charset: u16 } {
    return switch (t) {
        .tinyint => .{ .type_byte = MYSQL_TYPE_TINY, .decimals = 0, .len = 4, .charset = CHARSET_BINARY },
        .smallint => .{ .type_byte = MYSQL_TYPE_SHORT, .decimals = 0, .len = 6, .charset = CHARSET_BINARY },
        .int => .{ .type_byte = MYSQL_TYPE_LONG, .decimals = 0, .len = 11, .charset = CHARSET_BINARY },
        .bigint, .largeint => .{ .type_byte = MYSQL_TYPE_LONGLONG, .decimals = 0, .len = 20, .charset = CHARSET_BINARY },
        .boolean => .{ .type_byte = MYSQL_TYPE_TINY, .decimals = 0, .len = 1, .charset = CHARSET_BINARY },
        .float => .{ .type_byte = MYSQL_TYPE_FLOAT, .decimals = 0x1f, .len = 12, .charset = CHARSET_BINARY },
        .double => .{ .type_byte = MYSQL_TYPE_DOUBLE, .decimals = 0x1f, .len = 22, .charset = CHARSET_BINARY },
        .date => .{ .type_byte = MYSQL_TYPE_DATE, .decimals = 0, .len = 10, .charset = CHARSET_BINARY },
        .datetime => .{ .type_byte = MYSQL_TYPE_DATETIME, .decimals = 6, .len = 26, .charset = CHARSET_BINARY },
        .decimal64 => |spec| .{ .type_byte = MYSQL_TYPE_NEWDECIMAL, .decimals = spec.s, .len = @as(u32, spec.p) + 2, .charset = CHARSET_BINARY },
        .decimal128 => |spec| .{ .type_byte = MYSQL_TYPE_NEWDECIMAL, .decimals = spec.s, .len = @as(u32, spec.p) + 2, .charset = CHARSET_BINARY },
        .uuid => .{ .type_byte = MYSQL_TYPE_STRING, .decimals = 0, .len = 36, .charset = CHARSET_UTF8MB4 },
        .varchar => |n| .{ .type_byte = MYSQL_TYPE_VAR_STRING, .decimals = 0, .len = @as(u32, n) * 4, .charset = CHARSET_UTF8MB4 },
        .char => |n| .{ .type_byte = MYSQL_TYPE_VAR_STRING, .decimals = 0, .len = @as(u32, n) * 4, .charset = CHARSET_UTF8MB4 },
        .string => .{ .type_byte = MYSQL_TYPE_VAR_STRING, .decimals = 0, .len = 65535, .charset = CHARSET_UTF8MB4 },
    };
}

pub fn mysqlTypeInfo(t: types.Type) struct { type_byte: u8, decimals: u8, len: u32, charset: u16 } {
    return mysqlTypeOf(t);
}

pub fn appendColumnDef(
    allocator: Allocator,
    out: *std.ArrayList(u8),
    schema_name: []const u8,
    table_name: []const u8,
    col: Column,
) !void {
    try packet.appendLenEncString(allocator, out, "def");
    try packet.appendLenEncString(allocator, out, schema_name);
    try packet.appendLenEncString(allocator, out, table_name);
    try packet.appendLenEncString(allocator, out, table_name);
    try packet.appendLenEncString(allocator, out, col.name);
    try packet.appendLenEncString(allocator, out, col.name);
    try packet.appendLenEncInt(allocator, out, 0x0c);

    const info = mysqlTypeOf(col.type);

    var cs_buf: [2]u8 = undefined;
    std.mem.writeInt(u16, &cs_buf, info.charset, .little);
    try out.appendSlice(allocator, &cs_buf);

    var len_buf: [4]u8 = undefined;
    std.mem.writeInt(u32, &len_buf, info.len, .little);
    try out.appendSlice(allocator, &len_buf);

    try out.append(allocator, info.type_byte);

    var flags: u16 = 0;
    if (!col.nullable) flags |= NOT_NULL_FLAG;
    if (isNumericType(col.type)) flags |= NUM_FLAG;
    var flag_buf: [2]u8 = undefined;
    std.mem.writeInt(u16, &flag_buf, flags, .little);
    try out.appendSlice(allocator, &flag_buf);

    try out.append(allocator, info.decimals);
    try out.append(allocator, 0);
    try out.append(allocator, 0);
}

/// Format a single cell as its MySQL text representation into `scratch`,
/// returning the slice that holds the text. NULL is signaled by
/// returning null.
fn formatCell(scratch: *std.ArrayList(u8), allocator: Allocator, schema_col: Column, view: anytype, row: usize) !?[]const u8 {
    if (!view.isValid(row)) return null;
    scratch.clearRetainingCapacity();
    var num_buf: [64]u8 = undefined;
    const data = view.data;
    switch (data) {
        .int => |s| try scratch.appendSlice(allocator, try std.fmt.bufPrint(&num_buf, "{d}", .{s[row]})),
        .bigint => |s| try scratch.appendSlice(allocator, try std.fmt.bufPrint(&num_buf, "{d}", .{s[row]})),
        .smallint => |s| try scratch.appendSlice(allocator, try std.fmt.bufPrint(&num_buf, "{d}", .{s[row]})),
        .tinyint => |s| try scratch.appendSlice(allocator, try std.fmt.bufPrint(&num_buf, "{d}", .{s[row]})),
        .largeint => |s| try scratch.appendSlice(allocator, try std.fmt.bufPrint(&num_buf, "{d}", .{s[row]})),
        .boolean => |s| try scratch.appendSlice(allocator, try std.fmt.bufPrint(&num_buf, "{d}", .{s[row]})),
        .float => |s| try scratch.appendSlice(allocator, try std.fmt.bufPrint(&num_buf, "{d}", .{s[row]})),
        .double => |s| try scratch.appendSlice(allocator, try std.fmt.bufPrint(&num_buf, "{d}", .{s[row]})),
        .date => |s| {
            var buf: [16]u8 = undefined;
            try scratch.appendSlice(allocator, try wire_format.formatDate(&buf, s[row]));
        },
        .datetime => |s| {
            var buf: [40]u8 = undefined;
            try scratch.appendSlice(allocator, try wire_format.formatDateTime(&buf, s[row]));
        },
        .decimal64 => |s| try wire_format.formatDecimal(allocator, scratch, @as(i128, s[row]), schema_col.type),
        .decimal128 => |s| try wire_format.formatDecimal(allocator, scratch, s[row], schema_col.type),
        .uuid => |s| {
            var buf: [40]u8 = undefined;
            try scratch.appendSlice(allocator, try wire_format.formatUuid(&buf, s[row]));
        },
        .string, .varchar, .char => |sv| try scratch.appendSlice(allocator, sv.rowBytes(row)),
    }
    return scratch.items;
}

/// Emit the appropriate result-set terminator for `client_caps`.
/// DEPRECATE_EOF set: one OK-shaped EOF packet (header 0xFE, OK body).
/// DEPRECATE_EOF clear: legacy EOF_Packet (header 0xFE + warnings + status).
pub fn sendResultTerminator(
    allocator: Allocator,
    w: *std.Io.Writer,
    seq_id: *u8,
    client_caps: u32,
) !void {
    try sendResultTerminatorStatus(allocator, w, seq_id, client_caps, 0);
}

/// Same as sendResultTerminator but ORs `extra_status` into the
/// status_flags. Used by multi-statement handlers to set
/// SERVER_MORE_RESULTS_EXISTS on non-final result-set terminators.
pub fn sendResultTerminatorStatus(
    allocator: Allocator,
    w: *std.Io.Writer,
    seq_id: *u8,
    client_caps: u32,
    extra_status: u16,
) !void {
    if ((client_caps & handshake.CLIENT_DEPRECATE_EOF) != 0) {
        try handshake.sendEofOkPacketStatus(allocator, w, seq_id.*, extra_status);
    } else {
        try handshake.sendLegacyEofPacketStatus(allocator, w, seq_id.*, extra_status);
    }
    seq_id.* +%= 1;
}

/// Emit the column-def → row-data boundary if the client expects it.
/// Legacy clients require a single EOF packet between the ColumnDef
/// sequence and the first DataRow; DEPRECATE_EOF clients omit it.
pub fn sendColumnDefBoundary(
    allocator: Allocator,
    w: *std.Io.Writer,
    seq_id: *u8,
    client_caps: u32,
) !void {
    if ((client_caps & handshake.CLIENT_DEPRECATE_EOF) == 0) {
        try handshake.sendLegacyEofPacket(allocator, w, seq_id.*);
        seq_id.* +%= 1;
    }
}

/// Stream a Query's batches as a MySQL text-protocol result set. The
/// passed `*Query` is owned by the caller — caller deinits.
pub fn sendQueryResult(
    allocator: Allocator,
    w: *std.Io.Writer,
    query: anytype,
    schema_name: []const u8,
    table_name: []const u8,
    seq_id: *u8,
    client_caps: u32,
) !void {
    try sendQueryResultStatus(allocator, w, query, schema_name, table_name, seq_id, client_caps, 0);
}

/// Same as sendQueryResult but ORs `extra_status` into the terminator's
/// status_flags. Multi-statement responses pass SERVER_MORE_RESULTS_EXISTS
/// here for every non-final result set.
pub fn sendQueryResultStatus(
    allocator: Allocator,
    w: *std.Io.Writer,
    query: anytype,
    schema_name: []const u8,
    table_name: []const u8,
    seq_id: *u8,
    client_caps: u32,
    extra_status: u16,
) !void {
    const schema = query.outputSchema();
    try sendResultHeader(allocator, w, schema, schema_name, table_name, seq_id);
    try sendColumnDefBoundary(allocator, w, seq_id, client_caps);

    var scratch: std.ArrayList(u8) = .empty;
    defer scratch.deinit(allocator);
    var row_payload: std.ArrayList(u8) = .empty;
    defer row_payload.deinit(allocator);

    while (true) {
        // A runtime error mid-stream (e.g. MemoryBudgetExceeded from a
        // blocking sort / GROUP BY) terminates the result set with an ERR
        // packet rather than dropping the connection — the column defs are
        // already on the wire, and MySQL clients accept an ERR packet in
        // place of the terminating EOF/OK.
        const maybe_batch = query.next() catch |err| {
            const mapped = errors.mapInternal(err, null);
            try handshake.sendErrPacket(allocator, w, seq_id.*, mapped.code, mapped.sqlstate, mapped.message);
            seq_id.* +%= 1;
            return;
        };
        const batch = maybe_batch orelse break;
        var r: usize = 0;
        while (r < batch.row_count) : (r += 1) {
            row_payload.clearRetainingCapacity();
            for (batch.schema, 0..) |col, ci| {
                const text_opt = try formatCell(&scratch, allocator, col, batch.values[ci], r);
                if (text_opt) |text| {
                    try packet.appendLenEncString(allocator, &row_payload, text);
                } else {
                    try row_payload.append(allocator, 0xFB);
                }
            }
            try packet.writePacket(w, seq_id.*, row_payload.items);
            seq_id.* +%= 1;
        }
    }

    try sendResultTerminatorStatus(allocator, w, seq_id, client_caps, extra_status);
}

/// Send only the ColumnCount + ColumnDef41 packets. Used by canned-row
/// helpers that synthesize the row payload directly.
pub fn sendResultHeader(
    allocator: Allocator,
    w: *std.Io.Writer,
    schema: []const Column,
    schema_name: []const u8,
    table_name: []const u8,
    seq_id: *u8,
) !void {
    var col_count: std.ArrayList(u8) = .empty;
    defer col_count.deinit(allocator);
    try packet.appendLenEncInt(allocator, &col_count, @intCast(schema.len));
    try packet.writePacket(w, seq_id.*, col_count.items);
    seq_id.* +%= 1;

    var coldef: std.ArrayList(u8) = .empty;
    defer coldef.deinit(allocator);
    for (schema) |col| {
        coldef.clearRetainingCapacity();
        try appendColumnDef(allocator, &coldef, schema_name, table_name, col);
        try packet.writePacket(w, seq_id.*, coldef.items);
        seq_id.* +%= 1;
    }
}

/// Emit a single text row using the supplied per-column textual cells
/// (NULL = null entry). Uses the current `seq_id`, then advances it.
pub fn sendTextRow(
    allocator: Allocator,
    w: *std.Io.Writer,
    cells: []const ?[]const u8,
    seq_id: *u8,
) !void {
    var payload: std.ArrayList(u8) = .empty;
    defer payload.deinit(allocator);
    for (cells) |c| {
        if (c) |text| {
            try packet.appendLenEncString(allocator, &payload, text);
        } else {
            try payload.append(allocator, 0xFB);
        }
    }
    try packet.writePacket(w, seq_id.*, payload.items);
    seq_id.* +%= 1;
}

/// Send a canned single-value result set (one column, one row).
pub fn sendSingleValueResult(
    allocator: Allocator,
    w: *std.Io.Writer,
    col_name: []const u8,
    value: ?[]const u8,
    seq_id: *u8,
    client_caps: u32,
) !void {
    const cols = [_]Column{.{ .name = col_name, .type = .string, .nullable = value == null }};
    try sendResultHeader(allocator, w, cols[0..], "", "", seq_id);
    try sendColumnDefBoundary(allocator, w, seq_id, client_caps);
    const cells = [_]?[]const u8{value};
    try sendTextRow(allocator, w, cells[0..], seq_id);
    try sendResultTerminator(allocator, w, seq_id, client_caps);
}

/// Send a `SHOW VARIABLES`-style two-column row.
pub fn sendVariableRow(
    allocator: Allocator,
    w: *std.Io.Writer,
    name: []const u8,
    value: []const u8,
    seq_id: *u8,
    client_caps: u32,
) !void {
    const cols = [_]Column{
        .{ .name = "Variable_name", .type = .string },
        .{ .name = "Value", .type = .string },
    };
    try sendResultHeader(allocator, w, cols[0..], "", "", seq_id);
    try sendColumnDefBoundary(allocator, w, seq_id, client_caps);
    const cells = [_]?[]const u8{ name, value };
    try sendTextRow(allocator, w, cells[0..], seq_id);
    try sendResultTerminator(allocator, w, seq_id, client_caps);
}

/// Send an empty `SHOW VARIABLES`-shaped result set.
pub fn sendEmptyVariables(allocator: Allocator, w: *std.Io.Writer, seq_id: *u8, client_caps: u32) !void {
    const cols = [_]Column{
        .{ .name = "Variable_name", .type = .string },
        .{ .name = "Value", .type = .string },
    };
    try sendResultHeader(allocator, w, cols[0..], "", "", seq_id);
    try sendColumnDefBoundary(allocator, w, seq_id, client_caps);
    try sendResultTerminator(allocator, w, seq_id, client_caps);
}

/// Send a single-column result set whose rows are textual strings (e.g.,
/// SHOW DATABASES output we synthesized at the wire layer).
pub fn sendSingleColumnRows(
    allocator: Allocator,
    w: *std.Io.Writer,
    col_name: []const u8,
    values: []const []const u8,
    seq_id: *u8,
    client_caps: u32,
) !void {
    const cols = [_]Column{.{ .name = col_name, .type = .string }};
    try sendResultHeader(allocator, w, cols[0..], "", "", seq_id);
    try sendColumnDefBoundary(allocator, w, seq_id, client_caps);
    for (values) |v| {
        const cells = [_]?[]const u8{v};
        try sendTextRow(allocator, w, cells[0..], seq_id);
    }
    try sendResultTerminator(allocator, w, seq_id, client_caps);
}

