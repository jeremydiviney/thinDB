//! Encode an `exec.Batch` stream as MySQL text-protocol result-set packets.
//! Walks the executable Query, emits ColumnCount + per-column ColumnDef41
//! + per-row ResultsetRow + an OK-style end marker (CLIENT_DEPRECATE_EOF).

const std = @import("std");
const Allocator = std.mem.Allocator;

const exec = @import("../../exec/exec.zig");
const Batch = exec.Batch;

const types = @import("../../types.zig");
const Column = types.Column;
const TypeTag = types.TypeTag;

const packet = @import("packet.zig");
const handshake = @import("handshake.zig");
const canned = @import("canned.zig");

const MYSQL_TYPE_TINY: u8 = 0x01;
const MYSQL_TYPE_SHORT: u8 = 0x02;
const MYSQL_TYPE_LONG: u8 = 0x03;
const MYSQL_TYPE_FLOAT: u8 = 0x04;
const MYSQL_TYPE_DOUBLE: u8 = 0x05;
const MYSQL_TYPE_LONGLONG: u8 = 0x08;
const MYSQL_TYPE_DATE: u8 = 0x0a;
const MYSQL_TYPE_DATETIME: u8 = 0x0c;
const MYSQL_TYPE_NEWDECIMAL: u8 = 0xf6;
const MYSQL_TYPE_VAR_STRING: u8 = 0xfd;
const MYSQL_TYPE_STRING: u8 = 0xfe;

const NOT_NULL_FLAG: u16 = 0x0001;

const CHARSET_UTF8MB4: u16 = 0x21;
const CHARSET_BINARY: u16 = 0x3f;

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

fn appendColumnDef(
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
        .date => |s| try formatDate(allocator, scratch, s[row]),
        .datetime => |s| try formatDateTime(allocator, scratch, s[row]),
        .decimal64 => |s| try formatDecimal(allocator, scratch, @as(i128, s[row]), schema_col.type),
        .decimal128 => |s| try formatDecimal(allocator, scratch, s[row], schema_col.type),
        .uuid => |s| try formatUuid(allocator, scratch, s[row]),
        .string, .varchar, .char => |sv| try scratch.appendSlice(allocator, sv.rowBytes(row)),
    }
    return scratch.items;
}

fn formatDate(allocator: Allocator, out: *std.ArrayList(u8), days: i32) !void {
    const ymd = civilFromDays(@intCast(days));
    var buf: [16]u8 = undefined;
    const text = try std.fmt.bufPrint(&buf, "{d:0>4}-{d:0>2}-{d:0>2}", .{ ymd.y, ymd.m, ymd.d });
    try out.appendSlice(allocator, text);
}

fn formatDateTime(allocator: Allocator, out: *std.ArrayList(u8), micros: i64) !void {
    const sec = @divFloor(micros, 1_000_000);
    var us = @rem(micros, 1_000_000);
    var s = sec;
    if (us < 0) {
        us += 1_000_000;
        s -= 1;
    }
    const day = @divFloor(s, 86_400);
    var tod = @rem(s, 86_400);
    if (tod < 0) tod += 86_400;
    const ymd = civilFromDays(@intCast(day));
    const hours = @divFloor(tod, 3600);
    const minutes = @divFloor(@rem(tod, 3600), 60);
    const seconds = @rem(tod, 60);
    var buf: [40]u8 = undefined;
    const text = if (us == 0)
        try std.fmt.bufPrint(&buf, "{d:0>4}-{d:0>2}-{d:0>2} {d:0>2}:{d:0>2}:{d:0>2}", .{ ymd.y, ymd.m, ymd.d, hours, minutes, seconds })
    else
        try std.fmt.bufPrint(&buf, "{d:0>4}-{d:0>2}-{d:0>2} {d:0>2}:{d:0>2}:{d:0>2}.{d:0>6}", .{ ymd.y, ymd.m, ymd.d, hours, minutes, seconds, us });
    try out.appendSlice(allocator, text);
}

const Ymd = struct { y: i32, m: u32, d: u32 };

fn civilFromDays(days_since_epoch: i64) Ymd {
    const z = days_since_epoch + 719468;
    const era_div: i64 = if (z >= 0) @divFloor(z, 146097) else @divFloor(z - 146096, 146097);
    const era = era_div;
    const doe: u64 = @intCast(z - era * 146097);
    const yoe: u64 = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    const y_iso: i64 = @as(i64, @intCast(yoe)) + era * 400;
    const doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    const mp = (5 * doy + 2) / 153;
    const d = doy - (153 * mp + 2) / 5 + 1;
    const m = if (mp < 10) mp + 3 else mp - 9;
    const y = y_iso + @as(i64, @intFromBool(m <= 2));
    return .{ .y = @intCast(y), .m = @intCast(m), .d = @intCast(d) };
}

fn formatDecimal(allocator: Allocator, out: *std.ArrayList(u8), v: i128, t: types.Type) !void {
    const spec = t.decimalSpec() orelse return;
    var num_buf: [64]u8 = undefined;
    if (spec.s == 0) {
        try out.appendSlice(allocator, try std.fmt.bufPrint(&num_buf, "{d}", .{v}));
        return;
    }
    const negative = v < 0;
    const abs: u128 = if (negative) @intCast(-@as(i128, v)) else @intCast(v);
    var divisor: u128 = 1;
    var i: usize = 0;
    while (i < spec.s) : (i += 1) divisor *= 10;
    const whole = abs / divisor;
    const frac = abs % divisor;
    if (negative) try out.append(allocator, '-');
    try out.appendSlice(allocator, try std.fmt.bufPrint(&num_buf, "{d}.", .{whole}));
    var pad_buf: [40]u8 = undefined;
    const written = std.fmt.bufPrint(&pad_buf, "{d}", .{frac}) catch unreachable;
    var pad: usize = 0;
    while (pad + written.len < spec.s) : (pad += 1) try out.append(allocator, '0');
    try out.appendSlice(allocator, written);
}

fn formatUuid(allocator: Allocator, out: *std.ArrayList(u8), v: u128) !void {
    var bytes: [16]u8 = undefined;
    std.mem.writeInt(u128, &bytes, v, .big);
    var buf: [40]u8 = undefined;
    const text = try std.fmt.bufPrint(&buf, "{x:0>2}{x:0>2}{x:0>2}{x:0>2}-{x:0>2}{x:0>2}-{x:0>2}{x:0>2}-{x:0>2}{x:0>2}-{x:0>2}{x:0>2}{x:0>2}{x:0>2}{x:0>2}{x:0>2}", .{
        bytes[0], bytes[1], bytes[2],  bytes[3],
        bytes[4], bytes[5], bytes[6],  bytes[7],
        bytes[8], bytes[9], bytes[10], bytes[11],
        bytes[12], bytes[13], bytes[14], bytes[15],
    });
    try out.appendSlice(allocator, text);
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
) !void {
    const schema = query.outputSchema();
    try sendResultHeader(allocator, w, schema, schema_name, table_name, seq_id);

    var scratch: std.ArrayList(u8) = .empty;
    defer scratch.deinit(allocator);
    var row_payload: std.ArrayList(u8) = .empty;
    defer row_payload.deinit(allocator);

    while (try query.next()) |batch| {
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

    try handshake.sendEofOkPacket(allocator, w, seq_id.*);
    seq_id.* +%= 1;
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
) !void {
    const cols = [_]Column{.{ .name = col_name, .type = .string, .nullable = value == null }};
    try sendResultHeader(allocator, w, cols[0..], "", "", seq_id);
    const cells = [_]?[]const u8{value};
    try sendTextRow(allocator, w, cells[0..], seq_id);
    try handshake.sendEofOkPacket(allocator, w, seq_id.*);
    seq_id.* +%= 1;
}

/// Send a `SHOW VARIABLES`-style two-column row.
pub fn sendVariableRow(
    allocator: Allocator,
    w: *std.Io.Writer,
    name: []const u8,
    value: []const u8,
    seq_id: *u8,
) !void {
    const cols = [_]Column{
        .{ .name = "Variable_name", .type = .string },
        .{ .name = "Value", .type = .string },
    };
    try sendResultHeader(allocator, w, cols[0..], "", "", seq_id);
    const cells = [_]?[]const u8{ name, value };
    try sendTextRow(allocator, w, cells[0..], seq_id);
    try handshake.sendEofOkPacket(allocator, w, seq_id.*);
    seq_id.* +%= 1;
}

/// Send an empty `SHOW VARIABLES`-shaped result set.
pub fn sendEmptyVariables(allocator: Allocator, w: *std.Io.Writer, seq_id: *u8) !void {
    const cols = [_]Column{
        .{ .name = "Variable_name", .type = .string },
        .{ .name = "Value", .type = .string },
    };
    try sendResultHeader(allocator, w, cols[0..], "", "", seq_id);
    try handshake.sendEofOkPacket(allocator, w, seq_id.*);
    seq_id.* +%= 1;
}

/// Send a single-column result set whose rows are textual strings (e.g.,
/// SHOW DATABASES output we synthesized at the wire layer).
pub fn sendSingleColumnRows(
    allocator: Allocator,
    w: *std.Io.Writer,
    col_name: []const u8,
    values: []const []const u8,
    seq_id: *u8,
) !void {
    const cols = [_]Column{.{ .name = col_name, .type = .string }};
    try sendResultHeader(allocator, w, cols[0..], "", "", seq_id);
    for (values) |v| {
        const cells = [_]?[]const u8{v};
        try sendTextRow(allocator, w, cells[0..], seq_id);
    }
    try handshake.sendEofOkPacket(allocator, w, seq_id.*);
    seq_id.* +%= 1;
}

test "civilFromDays unix epoch is 1970-01-01" {
    const ymd = civilFromDays(0);
    try std.testing.expectEqual(@as(i32, 1970), ymd.y);
    try std.testing.expectEqual(@as(u32, 1), ymd.m);
    try std.testing.expectEqual(@as(u32, 1), ymd.d);
}

test "civilFromDays handles a recent date" {
    const ymd = civilFromDays(19000);
    try std.testing.expectEqual(@as(i32, 2022), ymd.y);
    try std.testing.expectEqual(@as(u32, 1), ymd.m);
    try std.testing.expectEqual(@as(u32, 8), ymd.d);
}

test "formatDecimal pads scale digits" {
    const allocator = std.testing.allocator;
    var out: std.ArrayList(u8) = .empty;
    defer out.deinit(allocator);
    try formatDecimal(allocator, &out, 123, .{ .decimal64 = .{ .p = 5, .s = 2 } });
    try std.testing.expectEqualStrings("1.23", out.items);

    out.clearRetainingCapacity();
    try formatDecimal(allocator, &out, 5, .{ .decimal64 = .{ .p = 5, .s = 2 } });
    try std.testing.expectEqualStrings("0.05", out.items);
}
