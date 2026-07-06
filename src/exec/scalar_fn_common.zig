//! Shared helpers used by the scalar-function kernel files. Lives here
//! so the per-category kernel modules (string/math/date/cond) can
//! import without circular references.

const std = @import("std");

const storage = @import("../storage/storage.zig");
pub const ColumnView = storage.ColumnView;

const store = @import("../engine/store.zig");
pub const ColumnStore = store.ColumnStore;

const types = @import("../types.zig");

/// Kernel variant that receives the call's argument `Type`s (with any
/// `DecimalSpec`) and the computed output `Type`. Plain `Kernel`s see only
/// `ColumnView`s, which carry no scale — decimal kernels need the scale, so
/// they run on this signature instead. See `scalar_fn_decimal.zig`.
pub const TypedKernelFn = *const fn (
    allocator: std.mem.Allocator,
    arg_types: []const types.Type,
    out_type: types.Type,
    args: []const ColumnView,
    out: *ColumnStore,
    row_count: usize,
) anyerror!void;

pub inline fn stringViewOf(v: ColumnView) storage.StringView {
    return switch (v.data) {
        .varchar => |sv| sv,
        .string => |sv| sv,
        .char => |sv| sv,
        .json => |sv| sv,
        else => unreachable, // resolve() already gated on type
    };
}

pub inline fn stringStoreOf(out: *ColumnStore) *store.StringStore {
    return switch (out.data) {
        .varchar => |*ss| ss,
        .string => |*ss| ss,
        .char => |*ss| ss,
        .json => |*ss| ss,
        else => unreachable,
    };
}

/// Extract (year, month1to12, day1to31) from a day-since-epoch i32.
/// Returns null for pre-1970 dates.
/// Days-since-epoch → (year, month, day). Hinnant's civil_from_days — the
/// exact O(1) inverse of `ymdToDays`. The std `EpochDay` path scans year by
/// year from 1970 (~56 iterations for a 2020s date), which dominates every
/// date-part kernel (YEAR/MONTH/DAY/quarter/last_day/…) on hot columns.
pub fn daysToYmd(days: i32) ?struct { year: u16, month: u4, day: u5 } {
    if (days < 0) return null;
    const z = days + 719468;
    const era = @divFloor(z, 146097);
    const doe = z - era * 146097;
    const yoe = @divTrunc(doe - @divTrunc(doe, 1460) + @divTrunc(doe, 36524) - @divTrunc(doe, 146096), 365);
    const doy = doe - (365 * yoe + @divTrunc(yoe, 4) - @divTrunc(yoe, 100));
    const mp = @divTrunc(5 * doy + 2, 153);
    const d = doy - @divTrunc(153 * mp + 2, 5) + 1;
    const m = if (mp < 10) mp + 3 else mp - 9;
    const y = yoe + era * 400 + @as(i32, if (m <= 2) 1 else 0);
    return .{
        .year = @intCast(y),
        .month = @intCast(m),
        .day = @intCast(d),
    };
}

/// Extract (hour, minute, second) from a datetime i64 (micros).
/// Returns null for pre-1970 datetimes.
pub fn microsToHms(micros: i64) ?struct { hour: u5, minute: u6, second: u6 } {
    if (micros < 0) return null;
    const secs: u64 = @intCast(@divTrunc(micros, 1_000_000));
    const epoch_seconds = std.time.epoch.EpochSeconds{ .secs = secs };
    const day_seconds = epoch_seconds.getDaySeconds();
    return .{
        .hour = day_seconds.getHoursIntoDay(),
        .minute = day_seconds.getMinutesIntoHour(),
        .second = day_seconds.getSecondsIntoMinute(),
    };
}

pub fn daysFromDatetime(micros: i64) i32 {
    // Floor-division so pre-epoch micros round towards -infinity.
    const secs = @divFloor(micros, 1_000_000);
    return @intCast(@divFloor(secs, 86_400));
}

/// Days since 1970-01-01 for a (year, month, day) tuple. Inverse of
/// `daysToYmd`. Uses Hinnant's civil_from_days algorithm — exact, handles
/// BC dates, no leap-second nonsense. `month` is 1..12, `day` is 1..31.
///
/// Reference: Howard Hinnant, "chrono-Compatible Low-Level Date
/// Algorithms" — civil_from_days.
pub fn ymdToDays(year: i32, month: u32, day: u32) i32 {
    var y = year;
    if (month <= 2) y -= 1;
    const era = @divFloor(y, 400);
    const yoe: u32 = @intCast(y - era * 400);
    const m_adj: i32 = if (month > 2) @as(i32, @intCast(month)) - 3 else @as(i32, @intCast(month)) + 9;
    const doy: u32 = @intCast(@divTrunc(153 * m_adj + 2, 5) + @as(i32, @intCast(day)) - 1);
    const doe: u32 = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    return @as(i32, era * 146097) + @as(i32, @intCast(doe)) - 719468;
}

/// Parse a `YYYY-MM-DD` date string to days-since-epoch. Accepts a trailing
/// time component (so a datetime string parses as its date part). Errors on a
/// malformed prefix — callers use that to fall back / reject.
pub fn parseDateString(s: []const u8) !i32 {
    if (s.len < 10) return error.Invalid;
    if (s[4] != '-' or s[7] != '-') return error.Invalid;
    const year = try std.fmt.parseInt(i32, s[0..4], 10);
    const month = try std.fmt.parseInt(u32, s[5..7], 10);
    const day = try std.fmt.parseInt(u32, s[8..10], 10);
    if (month < 1 or month > 12 or day < 1 or day > 31) return error.Invalid;
    return ymdToDays(year, month, day);
}

/// Parse a `YYYY-MM-DD[ T]HH:MM:SS` datetime string to micros-since-epoch.
pub fn parseDateTimeString(s: []const u8) !i64 {
    if (s.len < 19) return error.Invalid;
    if (s[4] != '-' or s[7] != '-') return error.Invalid;
    const sep = s[10];
    if (sep != ' ' and sep != 'T') return error.Invalid;
    if (s[13] != ':' or s[16] != ':') return error.Invalid;
    const year = try std.fmt.parseInt(i32, s[0..4], 10);
    const month = try std.fmt.parseInt(u32, s[5..7], 10);
    const day = try std.fmt.parseInt(u32, s[8..10], 10);
    const hour = try std.fmt.parseInt(u32, s[11..13], 10);
    const minute = try std.fmt.parseInt(u32, s[14..16], 10);
    const second = try std.fmt.parseInt(u32, s[17..19], 10);
    if (hour > 23 or minute > 59 or second > 59) return error.Invalid;
    const days = ymdToDays(year, month, day);
    const day_secs: i64 = @as(i64, days) * 86400 + @as(i64, hour) * 3600 + @as(i64, minute) * 60 + @as(i64, second);
    return day_secs * 1_000_000;
}

/// Days in month for a given (year, 1-indexed month). Handles Feb leap-year.
pub fn lastDayOfMonth(year: i32, month: u32) u32 {
    return switch (month) {
        1, 3, 5, 7, 8, 10, 12 => 31,
        4, 6, 9, 11 => 30,
        2 => if (isLeapYear(year)) @as(u32, 29) else 28,
        else => unreachable,
    };
}

pub fn isLeapYear(year: i32) bool {
    if (@rem(year, 4) != 0) return false;
    if (@rem(year, 100) != 0) return true;
    return @rem(year, 400) == 0;
}

test "daysToYmd round-trips ymdToDays and matches std decomposition" {
    // Walk every day from 1970-01-01 through 9999-12-31; daysToYmd must be the
    // exact inverse of ymdToDays and agree with std's (slow) year-by-year scan.
    var days: i32 = 0;
    const last = ymdToDays(9999, 12, 31);
    while (days <= last) : (days += 1) {
        const ymd = daysToYmd(days) orelse unreachable;
        try std.testing.expectEqual(days, ymdToDays(ymd.year, ymd.month, ymd.day));

        const u_days: u47 = @intCast(days);
        const yd = (std.time.epoch.EpochDay{ .day = u_days }).calculateYearDay();
        const md = yd.calculateMonthDay();
        try std.testing.expectEqual(yd.year, ymd.year);
        try std.testing.expectEqual(md.month.numeric(), ymd.month);
        try std.testing.expectEqual(md.day_index + 1, ymd.day);
    }
    try std.testing.expectEqual(@as(?@TypeOf(daysToYmd(0).?), null), daysToYmd(-1));
}
