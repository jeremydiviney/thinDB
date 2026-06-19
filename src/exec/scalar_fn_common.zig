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
        else => unreachable, // resolve() already gated on type
    };
}

pub inline fn stringStoreOf(out: *ColumnStore) *store.StringStore {
    return switch (out.data) {
        .varchar => |*ss| ss,
        .string => |*ss| ss,
        .char => |*ss| ss,
        else => unreachable,
    };
}

/// Extract (year, month1to12, day1to31) from a day-since-epoch i32.
/// Returns null for pre-1970 dates.
pub fn daysToYmd(days: i32) ?struct { year: u16, month: u4, day: u5 } {
    if (days < 0) return null;
    const u_days: u47 = @intCast(days);
    const epoch_day = std.time.epoch.EpochDay{ .day = u_days };
    const year_day = epoch_day.calculateYearDay();
    const month_day = year_day.calculateMonthDay();
    return .{
        .year = year_day.year,
        .month = month_day.month.numeric(),
        .day = month_day.day_index + 1,
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
