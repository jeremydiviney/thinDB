//! Shared text-format helpers for the SQL wire protocols.
//! Wire-protocol-agnostic: produce the canonical text representation that
//! both PG's "DataRow" and MySQL's "ProtocolText::ResultsetRow" consume
//! directly. Boolean and NULL handling live in the per-protocol modules
//! because their on-wire format genuinely differs.

const std = @import("std");
const Allocator = std.mem.Allocator;

const types = @import("../types.zig");

pub const Ymd = struct { y: i32, m: u32, d: u32 };

pub fn civilFromDays(days_since_epoch: i64) Ymd {
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

/// Inverse of `civilFromDays`. Howard Hinnant's "days_from_civil"
/// algorithm. Returns days since 1970-01-01. Returns `error.InvalidDate`
/// when month or day is zero so callers can surface a typed failure.
pub fn daysFromCivil(year: i32, month: u32, day: u32) i32 {
    const y: i32 = year - @as(i32, @intFromBool(month <= 2));
    const era: i32 = if (y >= 0) @divFloor(y, 400) else @divFloor(y - 399, 400);
    const yoe: u32 = @intCast(y - era * 400);
    const m: u32 = if (month > 2) month - 3 else month + 9;
    const doy: u32 = (153 * m + 2) / 5 + day - 1;
    const doe: u32 = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    return era * 146_097 + @as(i32, @intCast(doe)) - 719_468;
}

pub fn formatDate(buf: []u8, days_since_epoch: i32) ![]const u8 {
    const ymd = civilFromDays(@intCast(days_since_epoch));
    return std.fmt.bufPrint(buf, "{d:0>4}-{d:0>2}-{d:0>2}", .{ @as(u32, @intCast(ymd.y)), ymd.m, ymd.d });
}

pub fn formatDateTime(buf: []u8, micros_since_epoch: i64) ![]const u8 {
    const sec = @divFloor(micros_since_epoch, 1_000_000);
    var us = @rem(micros_since_epoch, 1_000_000);
    var s = sec;
    if (us < 0) {
        us += 1_000_000;
        s -= 1;
    }
    const day = @divFloor(s, 86_400);
    var tod = @rem(s, 86_400);
    if (tod < 0) tod += 86_400;
    const ymd = civilFromDays(@intCast(day));
    // Zig 0.16's `{d:0>N}` prints a leading `+` for signed values; cast
    // to unsigned before formatting (values are guaranteed non-negative
    // after the normalization above).
    const hours: u32 = @intCast(@divFloor(tod, 3600));
    const minutes: u32 = @intCast(@divFloor(@rem(tod, 3600), 60));
    const seconds: u32 = @intCast(@rem(tod, 60));
    const us_u: u32 = @intCast(us);
    const year_u: u32 = @intCast(ymd.y);
    if (us == 0)
        return std.fmt.bufPrint(buf, "{d:0>4}-{d:0>2}-{d:0>2} {d:0>2}:{d:0>2}:{d:0>2}", .{ year_u, ymd.m, ymd.d, hours, minutes, seconds });
    return std.fmt.bufPrint(buf, "{d:0>4}-{d:0>2}-{d:0>2} {d:0>2}:{d:0>2}:{d:0>2}.{d:0>6}", .{ year_u, ymd.m, ymd.d, hours, minutes, seconds, us_u });
}

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

test "formatDate produces YYYY-MM-DD" {
    var buf: [16]u8 = undefined;
    const text = try formatDate(&buf, 0);
    try std.testing.expectEqualStrings("1970-01-01", text);
}

test "daysFromCivil round-trips against civilFromDays" {
    const cases = [_]struct { y: i32, m: u32, d: u32 }{
        .{ .y = 1970, .m = 1, .d = 1 },
        .{ .y = 2000, .m = 2, .d = 29 },
        .{ .y = 2024, .m = 5, .d = 19 },
        .{ .y = 1969, .m = 12, .d = 31 },
        .{ .y = 1900, .m = 3, .d = 1 },
    };
    for (cases) |c| {
        const days = daysFromCivil(c.y, c.m, c.d);
        const back = civilFromDays(@intCast(days));
        try std.testing.expectEqual(c.y, back.y);
        try std.testing.expectEqual(c.m, back.m);
        try std.testing.expectEqual(c.d, back.d);
    }
}
