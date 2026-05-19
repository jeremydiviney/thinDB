//! Encode an `exec.Batch` stream as PostgreSQL Simple-Query result frames.
//! Emits one RowDescription, one DataRow per row, and a terminating
//! CommandComplete. Always uses text format (format code 0).

const std = @import("std");
const Allocator = std.mem.Allocator;

const exec = @import("../../exec/exec.zig");
const Batch = exec.Batch;

const types = @import("../../types.zig");
const Column = types.Column;
const Type = types.Type;

const packet = @import("packet.zig");

/// PostgreSQL type OIDs. Constants taken from src/include/catalog/pg_type.dat.
const OID_BOOL: u32 = 16;
const OID_INT2: u32 = 21;
const OID_INT4: u32 = 23;
const OID_INT8: u32 = 20;
const OID_FLOAT4: u32 = 700;
const OID_FLOAT8: u32 = 701;
const OID_NUMERIC: u32 = 1700;
const OID_TEXT: u32 = 25;
const OID_DATE: u32 = 1082;
const OID_TIMESTAMP: u32 = 1114;
const OID_UUID: u32 = 2950;

const TypeInfo = struct {
    oid: u32,
    /// PG type size in bytes; -1 for variable-length.
    type_size: i16,
    type_mod: i32 = -1,
};

fn pgTypeOf(t: Type) TypeInfo {
    return switch (t) {
        .tinyint, .smallint => .{ .oid = OID_INT2, .type_size = 2 },
        .int => .{ .oid = OID_INT4, .type_size = 4 },
        .bigint, .largeint => .{ .oid = OID_INT8, .type_size = 8 },
        .boolean => .{ .oid = OID_BOOL, .type_size = 1 },
        .float => .{ .oid = OID_FLOAT4, .type_size = 4 },
        .double => .{ .oid = OID_FLOAT8, .type_size = 8 },
        .date => .{ .oid = OID_DATE, .type_size = 4 },
        .datetime => .{ .oid = OID_TIMESTAMP, .type_size = 8 },
        .decimal64, .decimal128 => .{ .oid = OID_NUMERIC, .type_size = -1 },
        .uuid => .{ .oid = OID_UUID, .type_size = 16 },
        .varchar, .string, .char => .{ .oid = OID_TEXT, .type_size = -1 },
    };
}

/// Send a RowDescription `T` frame describing `schema`.
pub fn sendRowDescription(
    allocator: Allocator,
    w: *std.Io.Writer,
    schema: []const Column,
) !void {
    var payload: std.ArrayList(u8) = .empty;
    defer payload.deinit(allocator);

    try packet.appendI16(allocator, &payload, @intCast(schema.len));
    for (schema) |col| {
        const info = pgTypeOf(col.type);
        try packet.appendCString(allocator, &payload, col.name);
        try packet.appendU32(allocator, &payload, 0);
        try packet.appendI16(allocator, &payload, 0);
        try packet.appendU32(allocator, &payload, info.oid);
        try packet.appendI16(allocator, &payload, info.type_size);
        try packet.appendI32(allocator, &payload, info.type_mod);
        try packet.appendI16(allocator, &payload, 0);
    }
    try packet.writeFrame(w, 'T', payload.items);
}

/// Send a CommandComplete `C` frame whose payload is `tag` + NUL.
pub fn sendCommandComplete(
    allocator: Allocator,
    w: *std.Io.Writer,
    tag: []const u8,
) !void {
    var payload: std.ArrayList(u8) = .empty;
    defer payload.deinit(allocator);
    try packet.appendCString(allocator, &payload, tag);
    try packet.writeFrame(w, 'C', payload.items);
}

/// Send a single DataRow `D` frame whose cells are pre-formatted text
/// (null = SQL NULL).
pub fn sendDataRow(
    allocator: Allocator,
    w: *std.Io.Writer,
    cells: []const ?[]const u8,
) !void {
    var payload: std.ArrayList(u8) = .empty;
    defer payload.deinit(allocator);

    try packet.appendI16(allocator, &payload, @intCast(cells.len));
    for (cells) |c| {
        if (c) |text| {
            try packet.appendI32(allocator, &payload, @intCast(text.len));
            try payload.appendSlice(allocator, text);
        } else {
            try packet.appendI32(allocator, &payload, -1);
        }
    }
    try packet.writeFrame(w, 'D', payload.items);
}

/// Format a single cell as its PG text representation into `scratch`,
/// returning the slice that holds the text. NULL is signaled by null.
fn formatCell(
    scratch: *std.ArrayList(u8),
    allocator: Allocator,
    schema_col: Column,
    view: anytype,
    row: usize,
) !?[]const u8 {
    if (!view.isValid(row)) return null;
    scratch.clearRetainingCapacity();
    var num_buf: [64]u8 = undefined;
    switch (view.data) {
        .int => |s| try scratch.appendSlice(allocator, try std.fmt.bufPrint(&num_buf, "{d}", .{s[row]})),
        .bigint => |s| try scratch.appendSlice(allocator, try std.fmt.bufPrint(&num_buf, "{d}", .{s[row]})),
        .smallint => |s| try scratch.appendSlice(allocator, try std.fmt.bufPrint(&num_buf, "{d}", .{s[row]})),
        .tinyint => |s| try scratch.appendSlice(allocator, try std.fmt.bufPrint(&num_buf, "{d}", .{s[row]})),
        .largeint => |s| try scratch.appendSlice(allocator, try std.fmt.bufPrint(&num_buf, "{d}", .{s[row]})),
        .boolean => |s| try scratch.appendSlice(allocator, if (s[row] != 0) "t" else "f"),
        .float => |s| try formatFloat(allocator, scratch, @as(f64, s[row])),
        .double => |s| try formatFloat(allocator, scratch, s[row]),
        .date => |s| try formatDate(allocator, scratch, s[row]),
        .datetime => |s| try formatDateTime(allocator, scratch, s[row]),
        .decimal64 => |s| try formatDecimal(allocator, scratch, @as(i128, s[row]), schema_col.type),
        .decimal128 => |s| try formatDecimal(allocator, scratch, s[row], schema_col.type),
        .uuid => |s| try formatUuid(allocator, scratch, s[row]),
        .string, .varchar, .char => |sv| try scratch.appendSlice(allocator, sv.rowBytes(row)),
    }
    return scratch.items;
}

fn formatFloat(allocator: Allocator, out: *std.ArrayList(u8), v: f64) !void {
    if (std.math.isNan(v)) {
        try out.appendSlice(allocator, "NaN");
        return;
    }
    if (std.math.isInf(v)) {
        try out.appendSlice(allocator, if (v > 0) "Infinity" else "-Infinity");
        return;
    }
    var buf: [64]u8 = undefined;
    const text = try std.fmt.bufPrint(&buf, "{d}", .{v});
    try out.appendSlice(allocator, text);
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

fn formatDecimal(allocator: Allocator, out: *std.ArrayList(u8), v: i128, t: Type) !void {
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
        bytes[0],  bytes[1],  bytes[2],  bytes[3],
        bytes[4],  bytes[5],  bytes[6],  bytes[7],
        bytes[8],  bytes[9],  bytes[10], bytes[11],
        bytes[12], bytes[13], bytes[14], bytes[15],
    });
    try out.appendSlice(allocator, text);
}

/// Drain `query` (a `*CompiledQuery`) as a SELECT-shaped result set.
/// Returns the row count emitted (used for the CommandComplete tag).
pub fn sendQueryResult(
    allocator: Allocator,
    w: *std.Io.Writer,
    query: anytype,
) !u64 {
    const schema = query.outputSchema();
    try sendRowDescription(allocator, w, schema);

    var scratch: std.ArrayList(u8) = .empty;
    defer scratch.deinit(allocator);

    var cells: std.ArrayList(?[]const u8) = .empty;
    defer cells.deinit(allocator);

    var row_count: u64 = 0;
    while (try query.next()) |batch| {
        var r: usize = 0;
        while (r < batch.row_count) : (r += 1) {
            cells.clearRetainingCapacity();
            for (batch.schema, 0..) |col, ci| {
                const text_opt = try formatCell(&scratch, allocator, col, batch.values[ci], r);
                if (text_opt) |text| {
                    const copy = try allocator.dupe(u8, text);
                    try cells.append(allocator, copy);
                } else {
                    try cells.append(allocator, null);
                }
            }
            try sendDataRow(allocator, w, cells.items);
            for (cells.items) |c| if (c) |s| allocator.free(s);
            row_count += 1;
        }
    }
    return row_count;
}

/// Send an inline single-column / one-or-more-row result set whose cells
/// are pre-formatted text values. Used by canned probe responses.
pub fn sendStringColumnResult(
    allocator: Allocator,
    w: *std.Io.Writer,
    col_name: []const u8,
    rows: []const []const u8,
) !void {
    const cols = [_]Column{.{ .name = col_name, .type = .string }};
    try sendRowDescription(allocator, w, cols[0..]);
    for (rows) |v| {
        const cells = [_]?[]const u8{v};
        try sendDataRow(allocator, w, cells[0..]);
    }
}

test "pgTypeOf maps thinDB types to expected OIDs" {
    try std.testing.expectEqual(OID_INT4, pgTypeOf(.int).oid);
    try std.testing.expectEqual(OID_INT8, pgTypeOf(.bigint).oid);
    try std.testing.expectEqual(OID_BOOL, pgTypeOf(.boolean).oid);
    try std.testing.expectEqual(OID_TEXT, pgTypeOf(.string).oid);
    try std.testing.expectEqual(OID_DATE, pgTypeOf(.date).oid);
    try std.testing.expectEqual(OID_TIMESTAMP, pgTypeOf(.datetime).oid);
    try std.testing.expectEqual(OID_UUID, pgTypeOf(.uuid).oid);
    try std.testing.expectEqual(@as(i16, -1), pgTypeOf(.string).type_size);
    try std.testing.expectEqual(@as(i16, 8), pgTypeOf(.bigint).type_size);
}

test "civilFromDays round-trips through 1970-01-01" {
    const ymd = civilFromDays(0);
    try std.testing.expectEqual(@as(i32, 1970), ymd.y);
    try std.testing.expectEqual(@as(u32, 1), ymd.m);
    try std.testing.expectEqual(@as(u32, 1), ymd.d);
}

test "formatDecimal pads scale digits" {
    const allocator = std.testing.allocator;
    var out: std.ArrayList(u8) = .empty;
    defer out.deinit(allocator);
    try formatDecimal(allocator, &out, 123, .{ .decimal64 = .{ .p = 5, .s = 2 } });
    try std.testing.expectEqualStrings("1.23", out.items);
}
