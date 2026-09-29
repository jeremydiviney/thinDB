//! Shared text-format helpers for the SQL wire protocols.
//! Wire-protocol-agnostic: produce the canonical text representation that
//! both PG's "DataRow" and MySQL's "ProtocolText::ResultsetRow" consume
//! directly. Boolean and NULL handling live in the per-protocol modules
//! because their on-wire format genuinely differs.

const std = @import("std");
const Allocator = std.mem.Allocator;

const types = @import("../types.zig");

const exec_text = @import("../exec/scalar_fn_common.zig");
pub const daysToYmd = exec_text.daysToYmd;
pub const ymdToDays = exec_text.ymdToDays;
pub const daysFromDatetime = exec_text.daysFromDatetime;
pub const microsToHms = exec_text.microsToHms;
pub const formatDate = exec_text.formatDate;
pub const formatDateTime = exec_text.formatDateTime;
pub const FLOAT_TEXT_MAX = exec_text.FLOAT_TEXT_MAX;
pub const floatText = exec_text.floatText;

pub fn formatUuid(buf: []u8, v: u128) ![]const u8 {
    var bytes: [16]u8 = undefined;
    std.mem.writeInt(u128, &bytes, v, .big);
    return std.fmt.bufPrint(buf, "{x:0>2}{x:0>2}{x:0>2}{x:0>2}-{x:0>2}{x:0>2}-{x:0>2}{x:0>2}-{x:0>2}{x:0>2}-{x:0>2}{x:0>2}{x:0>2}{x:0>2}{x:0>2}{x:0>2}", .{
        bytes[0],  bytes[1],  bytes[2],  bytes[3],
        bytes[4],  bytes[5],  bytes[6],  bytes[7],
        bytes[8],  bytes[9],  bytes[10], bytes[11],
        bytes[12], bytes[13], bytes[14], bytes[15],
    });
}

/// Decimal width is unbounded (i128 whole part + scale), so it lands into
/// a caller-owned ArrayList rather than a fixed buffer.
pub fn formatDecimal(allocator: Allocator, out: *std.ArrayList(u8), v: i128, t: types.Type) !void {
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

test "formatDate produces YYYY-MM-DD" {
    var buf: [16]u8 = undefined;
    const text = try formatDate(&buf, 0);
    try std.testing.expectEqualStrings("1970-01-01", text);
}

test "formatDate prints year 0 before March as the day it is (issue #393)" {
    var buf: [16]u8 = undefined;
    inline for (.{ "0000-01-01", "0000-02-28", "0000-02-29", "0000-03-01", "1969-12-31", "2000-02-29", "9999-12-31" }) |text| {
        const days = ymdToDays(try std.fmt.parseInt(i32, text[0..4], 10), try std.fmt.parseInt(u32, text[5..7], 10), try std.fmt.parseInt(u32, text[8..10], 10));
        try std.testing.expectEqualStrings(text, try formatDate(&buf, days));
    }
}
