//! MySQL's network-address functions: INET_ATON / INET_NTOA over IPv4
//! numbers, INET6_ATON / INET6_NTOA over the 4- or 16-byte binary form, and
//! the IS_IPV4 / IS_IPV6 / IS_IPV4_COMPAT / IS_IPV4_MAPPED tests. Parsing
//! and printing follow MySQL's own rules (item_inetfunc.cc) rather than the
//! platform's inet_pton, so the answers match MySQL byte for byte.

const std = @import("std");
const Allocator = std.mem.Allocator;

const common = @import("scalar_fn_common.zig");
const ColumnView = common.ColumnView;
const ColumnStore = common.ColumnStore;
const stringViewOf = common.stringViewOf;
const stringStoreOf = common.stringStoreOf;

/// INET_ATON(s): the dotted-quad IPv4 address as a number. Short forms fill
/// the missing groups with zeros before the last one (`127.1` is
/// 127.0.0.1); an empty group reads as 0. NULL for anything else.
pub fn inetAtonKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const sv = stringViewOf(args[0]);
    const base = out.data.rowCount();
    for (0..row_count) |i| {
        const number: ?u32 = if (args[0].isValid(i)) inetAton(sv.rowBytes(i)) else null;
        try out.data.bigint.append(allocator, number orelse 0);
        try out.appendValidBit(allocator, base + i, number != null);
    }
}

fn inetAton(s: []const u8) ?u32 {
    var result: u64 = 0;
    var group: u64 = 0;
    var dots: usize = 0;
    var last: u8 = '.';
    for (s) |c| {
        last = c;
        switch (c) {
            '0'...'9' => {
                group = group * 10 + (c - '0');
                if (group > 255) return null;
            },
            '.' => {
                dots += 1;
                if (dots > 3) return null;
                result = (result << 8) + group;
                group = 0;
            },
            else => return null,
        }
    }
    if (last == '.') return null;
    if (dots < 3) result <<= @intCast(8 * (3 - dots));
    return @intCast((result << 8) + group);
}

/// INET_NTOA(n): the IPv4 address `n` numbers, in dotted-quad form. NULL
/// when `n` is outside 0..2^32-1. A DOUBLE rounds half to even and text
/// reads its leading integer, as MySQL converts them.
pub fn inetNtoaKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const ss = stringStoreOf(out);
    const base = out.data.rowCount();
    var buf: [16]u8 = undefined;
    for (0..row_count) |i| {
        const number: ?u32 = if (args[0].isValid(i)) ipv4Number(args[0], i) else null;
        if (number) |n| {
            var bytes: [4]u8 = undefined;
            std.mem.writeInt(u32, &bytes, n, .big);
            try ss.appendValue(allocator, formatIpv4(&buf, bytes));
        } else {
            try ss.appendValue(allocator, "");
        }
        try out.appendValidBit(allocator, base + i, number != null);
    }
}

fn ipv4Number(arg: ColumnView, row: usize) ?u32 {
    return switch (arg.data) {
        .bigint => |s| std.math.cast(u32, s[row]),
        .double => |s| blk: {
            const n = common.roundHalfEven(s[row]);
            break :blk if (n >= 0 and n <= std.math.maxInt(u32)) @intFromFloat(n) else null;
        },
        .varchar, .string, .char => std.math.cast(u32, leadingInteger(stringViewOf(arg).rowBytes(row))),
        else => unreachable, // the overloads take a BIGINT, DOUBLE or text
    };
}

/// The integer text starts with, as MySQL reads a string where it wants an
/// integer: leading spaces, a sign, then digits up to the first other byte.
/// 0 when no digit follows; saturates rather than overflowing.
fn leadingInteger(s: []const u8) i64 {
    var i: usize = 0;
    while (i < s.len and std.ascii.isWhitespace(s[i])) i += 1;
    const negative = i < s.len and s[i] == '-';
    if (i < s.len and (s[i] == '-' or s[i] == '+')) i += 1;
    var v: i64 = 0;
    while (i < s.len and std.ascii.isDigit(s[i])) : (i += 1) {
        v = std.math.mul(i64, v, 10) catch return if (negative) std.math.minInt(i64) else std.math.maxInt(i64);
        v = std.math.add(i64, v, s[i] - '0') catch return if (negative) std.math.minInt(i64) else std.math.maxInt(i64);
    }
    return if (negative) -v else v;
}

fn formatIpv4(buf: []u8, bytes: [4]u8) []const u8 {
    return std.fmt.bufPrint(buf, "{d}.{d}.{d}.{d}", .{ bytes[0], bytes[1], bytes[2], bytes[3] }) catch unreachable;
}

/// INET6_ATON(s): an IPv4 address as its 4 bytes, an IPv6 address as its 16;
/// NULL for anything else.
pub fn inet6AtonKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const sv = stringViewOf(args[0]);
    const ss = stringStoreOf(out);
    const base = out.data.rowCount();
    for (0..row_count) |i| {
        const text = sv.rowBytes(i);
        var bytes: [16]u8 = undefined;
        const packed_len: ?usize = if (!args[0].isValid(i))
            null
        else if (parseIpv4(text)) |v4| blk: {
            bytes[0..4].* = v4;
            break :blk 4;
        } else if (parseIpv6(text)) |v6| blk: {
            bytes = v6;
            break :blk 16;
        } else null;
        try ss.appendValue(allocator, if (packed_len) |n| bytes[0..n] else "");
        try out.appendValidBit(allocator, base + i, packed_len != null);
    }
}

/// INET6_NTOA(b): the text of a 4-byte IPv4 or 16-byte IPv6 address, with
/// the longest run of zero groups written `::` and an IPv4-compatible or
/// IPv4-mapped address ending in dotted quad. NULL for any other length.
pub fn inet6NtoaKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const sv = stringViewOf(args[0]);
    const ss = stringStoreOf(out);
    const base = out.data.rowCount();
    var buf: [48]u8 = undefined;
    for (0..row_count) |i| {
        const bytes = sv.rowBytes(i);
        const text: ?[]const u8 = if (!args[0].isValid(i))
            null
        else switch (bytes.len) {
            4 => formatIpv4(&buf, bytes[0..4].*),
            16 => formatIpv6(&buf, bytes[0..16].*),
            else => null,
        };
        try ss.appendValue(allocator, text orelse "");
        try out.appendValidBit(allocator, base + i, text != null);
    }
}

pub fn isIpv4Kernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const sv = stringViewOf(args[0]);
    for (0..row_count) |i| try out.data.bigint.append(allocator, @intFromBool(parseIpv4(sv.rowBytes(i)) != null));
}

pub fn isIpv6Kernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const sv = stringViewOf(args[0]);
    for (0..row_count) |i| try out.data.bigint.append(allocator, @intFromBool(parseIpv6(sv.rowBytes(i)) != null));
}

/// IS_IPV4_COMPAT(b): a 16-byte address of 96 zero bits then an IPv4
/// address other than 0.0.0.0 and 0.0.0.1 (IN6_IS_ADDR_V4COMPAT).
pub fn isIpv4CompatKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const sv = stringViewOf(args[0]);
    for (0..row_count) |i| {
        const b = sv.rowBytes(i);
        const compat = b.len == 16 and std.mem.allEqual(u8, b[0..12], 0) and std.mem.readInt(u32, b[12..16], .big) > 1;
        try out.data.bigint.append(allocator, @intFromBool(compat));
    }
}

/// IS_IPV4_MAPPED(b): a 16-byte address of 80 zero bits, 16 one bits, then
/// an IPv4 address.
pub fn isIpv4MappedKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const sv = stringViewOf(args[0]);
    for (0..row_count) |i| {
        const b = sv.rowBytes(i);
        const mapped = b.len == 16 and std.mem.allEqual(u8, b[0..10], 0) and b[10] == 0xff and b[11] == 0xff;
        try out.data.bigint.append(allocator, @intFromBool(mapped));
    }
}

/// A strict dotted quad: four groups of one to three digits, each at most
/// 255.
fn parseIpv4(s: []const u8) ?[4]u8 {
    if (s.len < 7 or s.len > 15) return null;
    var bytes: [4]u8 = undefined;
    var group: u32 = 0;
    var digits: usize = 0;
    var dots: usize = 0;
    for (s) |c| switch (c) {
        '0'...'9' => {
            digits += 1;
            if (digits > 3) return null;
            group = group * 10 + (c - '0');
            if (group > 255) return null;
        },
        '.' => {
            if (digits == 0 or dots == 3) return null;
            bytes[dots] = @intCast(group);
            dots += 1;
            group = 0;
            digits = 0;
        },
        else => return null,
    };
    if (digits == 0 or dots != 3) return null;
    bytes[3] = @intCast(group);
    return bytes;
}

/// An IPv6 address: up to eight groups of one to four hex digits, at most
/// one `::` standing for a run of zero groups, and optionally a dotted quad
/// as the last 32 bits.
fn parseIpv6(s: []const u8) ?[16]u8 {
    if (s.len < 2 or s.len > 39) return null;
    var bytes = [_]u8{0} ** 16;
    var i: usize = 0;
    if (s[0] == ':') {
        if (s[1] != ':') return null;
        i = 1;
    }
    var dst: usize = 0;
    var gap: ?usize = null;
    var group_start = i;
    var digits: usize = 0;
    var group: u16 = 0;
    while (i < s.len) {
        const c = s[i];
        i += 1;
        if (c == ':') {
            group_start = i;
            if (digits == 0) {
                if (gap != null) return null;
                gap = dst;
                continue;
            }
            if (i == s.len or dst + 2 > 16) return null;
            std.mem.writeInt(u16, bytes[dst..][0..2], group, .big);
            dst += 2;
            digits = 0;
            group = 0;
        } else if (c == '.') {
            if (dst + 4 > 16) return null;
            bytes[dst..][0..4].* = parseIpv4(s[group_start..]) orelse return null;
            dst += 4;
            digits = 0;
            break;
        } else {
            const nibble = std.fmt.charToDigit(c, 16) catch return null;
            if (digits == 4) return null;
            group = (group << 4) | nibble;
            digits += 1;
        }
    }
    if (digits > 0) {
        if (dst + 2 > 16) return null;
        std.mem.writeInt(u16, bytes[dst..][0..2], group, .big);
        dst += 2;
    }
    if (gap) |g| {
        if (dst == 16) return null;
        const tail = dst - g;
        std.mem.copyBackwards(u8, bytes[16 - tail ..], bytes[g..dst]);
        @memset(bytes[g .. 16 - tail], 0);
    } else if (dst < 16) return null;
    return bytes;
}

fn formatIpv6(buf: []u8, bytes: [16]u8) []const u8 {
    var words: [8]u16 = undefined;
    for (&words, 0..) |*w, k| w.* = std.mem.readInt(u16, bytes[2 * k ..][0..2], .big);
    var gap_pos: ?usize = null;
    var gap_len: usize = 0;
    var run_pos: usize = 0;
    var run_len: usize = 0;
    for (words, 0..) |w, k| {
        if (w != 0) {
            run_len = 0;
            continue;
        }
        if (run_len == 0) run_pos = k;
        run_len += 1;
        if (run_len > gap_len) {
            gap_pos = run_pos;
            gap_len = run_len;
        }
    }
    var w: std.Io.Writer = .fixed(buf);
    var k: usize = 0;
    while (k < 8) : (k += 1) {
        if (gap_pos == k) {
            if (k == 0) w.writeByte(':') catch unreachable;
            w.writeByte(':') catch unreachable;
            k += gap_len - 1;
        } else if (k == 6 and gap_pos == 0 and (gap_len == 6 or (gap_len == 5 and words[5] == 0xffff))) {
            w.print("{d}.{d}.{d}.{d}", .{ bytes[12], bytes[13], bytes[14], bytes[15] }) catch unreachable;
            break;
        } else {
            w.print("{x}", .{words[k]}) catch unreachable;
            if (k != 7) w.writeByte(':') catch unreachable;
        }
    }
    return w.buffered();
}

test "inetAton: MySQL's short forms and rejects" {
    const cases = .{
        .{ "10.0.0.1", 167772161 }, .{ "127.1", 2130706433 },     .{ "1.2.3", 16908291 },
        .{ "1..2", 16777218 },      .{ ".1", 1 },                 .{ "255.255.255.255", 4294967295 },
        .{ "1", 1 },                .{ "01.02.03.04", 16909060 },
    };
    inline for (cases) |c| try std.testing.expectEqual(@as(?u32, c[1]), inetAton(c[0]));
    inline for (.{ "", "256.0.0.1", "1.2.3.4.5", "abc", " 1.2.3.4", "1.2.3.", "0x1.2.3.4" }) |bad| {
        try std.testing.expectEqual(@as(?u32, null), inetAton(bad));
    }
}

test "IPv6 parse and format round-trip MySQL's spellings" {
    const cases = .{
        .{ "fdfe::5a55:caff:fefa:9089", "fdfe::5a55:caff:fefa:9089" },
        .{ "::ffff:10.0.0.1", "::ffff:10.0.0.1" },
        .{ "::10.0.0.1", "::10.0.0.1" },
        .{ "1:0:2:3:4:5:6:7", "1::2:3:4:5:6:7" },
        .{ "1:0:0:2:0:0:3:4", "1::2:0:0:3:4" },
        .{ "::", "::" },
        .{ "::1", "::1" },
        .{ "1::", "1::" },
        .{ "0:0:0:0:0:ffff:0:1", "::ffff:0.0.0.1" },
        .{ "1:2:3:4:5:6:1.2.3.4", "1:2:3:4:5:6:102:304" },
        .{ "ABCD::", "abcd::" },
    };
    inline for (cases) |c| {
        var buf: [48]u8 = undefined;
        try std.testing.expectEqualStrings(c[1], formatIpv6(&buf, parseIpv6(c[0]).?));
    }
    inline for (.{ "bad", "1:2:3:4:5:6:7:8:9", "1::2::3", "12345::", ":1", "1:", "127.1", "1.2.3.256" }) |bad| {
        try std.testing.expectEqual(@as(?[16]u8, null), parseIpv6(bad));
    }
}
