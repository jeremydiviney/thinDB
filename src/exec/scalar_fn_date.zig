//! Date / datetime scalar kernels. Includes the per-component extractors
//! (year, month, day, hour, ...) for both DATE and DATETIME, calendar
//! arithmetic (datediff, date_add, date_sub), epoch conversion
//! (unix_timestamp, from_unixtime), MySQL-style calendar helpers
//! (dayofweek, dayofyear, quarter, last_day), and date_format.

const std = @import("std");
const Allocator = std.mem.Allocator;

const common = @import("scalar_fn_common.zig");
const datefmt = @import("scalar_fn_datefmt.zig");
const ColumnView = common.ColumnView;
const ColumnStore = common.ColumnStore;
const stringViewOf = common.stringViewOf;
const stringStoreOf = common.stringStoreOf;
const daysToYmd = common.daysToYmd;
const microsToHms = common.microsToHms;
const daysFromDatetime = common.daysFromDatetime;

const Kernel = *const fn (allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) anyerror!void;

// ---------------------------------------------------------------------------
// Component extractors (year/month/day for date + datetime; hour/minute/sec
// for datetime).
// ---------------------------------------------------------------------------

pub fn yearFromDateKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const s = args[0].data.date;
    var i: usize = 0;
    while (i < row_count) : (i += 1) {
        try out.data.int.append(allocator, daysToYmd(s[i]).year);
    }
}

pub fn yearFromDatetimeKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const s = args[0].data.datetime;
    var i: usize = 0;
    while (i < row_count) : (i += 1) {
        try out.data.int.append(allocator, daysToYmd(daysFromDatetime(s[i])).year);
    }
}

pub fn monthFromDateKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const s = args[0].data.date;
    var i: usize = 0;
    while (i < row_count) : (i += 1) {
        try out.data.int.append(allocator, daysToYmd(s[i]).month);
    }
}

pub fn monthFromDatetimeKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const s = args[0].data.datetime;
    var i: usize = 0;
    while (i < row_count) : (i += 1) {
        try out.data.int.append(allocator, daysToYmd(daysFromDatetime(s[i])).month);
    }
}

pub fn dayFromDateKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const s = args[0].data.date;
    var i: usize = 0;
    while (i < row_count) : (i += 1) {
        try out.data.int.append(allocator, daysToYmd(s[i]).day);
    }
}

pub fn dayFromDatetimeKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const s = args[0].data.datetime;
    var i: usize = 0;
    while (i < row_count) : (i += 1) {
        try out.data.int.append(allocator, daysToYmd(daysFromDatetime(s[i])).day);
    }
}

pub fn hourKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const s = args[0].data.datetime;
    var i: usize = 0;
    while (i < row_count) : (i += 1) {
        try out.data.int.append(allocator, microsToHms(s[i]).hour);
    }
}

pub fn minuteKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const s = args[0].data.datetime;
    var i: usize = 0;
    while (i < row_count) : (i += 1) {
        try out.data.int.append(allocator, microsToHms(s[i]).minute);
    }
}

pub fn secondKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const s = args[0].data.datetime;
    var i: usize = 0;
    while (i < row_count) : (i += 1) {
        try out.data.int.append(allocator, microsToHms(s[i]).second);
    }
}

// ---------------------------------------------------------------------------
// Arithmetic + epoch conversion.
// ---------------------------------------------------------------------------

pub fn datediffKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const a = args[0].data.date;
    const b = args[1].data.date;
    var i: usize = 0;
    while (i < row_count) : (i += 1) try out.data.int.append(allocator, a[i] - b[i]);
}

/// DATEDIFF over datetimes compares their dates only, as in MySQL.
pub fn datediffDatetimeKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const a = args[0].data.datetime;
    const b = args[1].data.datetime;
    for (a[0..row_count], b[0..row_count]) |x, y| try out.data.int.append(allocator, daysFromDatetime(x) - daysFromDatetime(y));
}

pub fn dateAddKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const d = args[0].data.date;
    const n = args[1].data.int;
    var i: usize = 0;
    while (i < row_count) : (i += 1) try out.data.date.append(allocator, d[i] + n[i]);
}

pub fn dateSubKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const d = args[0].data.date;
    const n = args[1].data.int;
    var i: usize = 0;
    while (i < row_count) : (i += 1) try out.data.date.append(allocator, d[i] - n[i]);
}

/// Add `n` calendar months to a DATE, clamping the day component when the
/// destination month is shorter (`2024-01-31 + 1 month → 2024-02-29`).
/// Negative `n` works the same way in reverse.
pub fn dateAddMonthsKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const d = args[0].data.date;
    const n = args[1].data.int;
    var i: usize = 0;
    while (i < row_count) : (i += 1) {
        try out.data.date.append(allocator, addMonths(d[i], n[i]));
    }
}

pub fn dateAddYearsKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const d = args[0].data.date;
    const n = args[1].data.int;
    var i: usize = 0;
    while (i < row_count) : (i += 1) {
        try out.data.date.append(allocator, addMonths(d[i], n[i] * 12));
    }
}

/// DATE_ADD over a DATETIME by whole days, months or years, keeping the time
/// of day; a month or year step clamps the day to the destination month.
fn DatetimeAddUnit(comptime unit: DateUnit, comptime negate: bool) type {
    return struct {
        fn kernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
            const dts = args[0].data.datetime;
            const ns = args[1].data.int;
            for (dts[0..row_count], ns[0..row_count]) |dt, n| try out.data.datetime.append(allocator, addUnitToDatetime(unit, dt, if (negate) -n else n));
        }
    };
}

pub const datetimeAddDaysKernel = DatetimeAddUnit(.day, false).kernel;
pub const datetimeSubDaysKernel = DatetimeAddUnit(.day, true).kernel;
pub const datetimeAddMonthsKernel = DatetimeAddUnit(.month, false).kernel;
pub const datetimeAddYearsKernel = DatetimeAddUnit(.year, false).kernel;

/// A DATETIME moved by a count of `step_micros`-long steps.
fn DatetimeAddSteps(comptime step_micros: i64) type {
    return struct {
        fn kernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
            const dts = args[0].data.datetime;
            const ns = args[1].data.bigint;
            for (dts[0..row_count], ns[0..row_count]) |dt, n| {
                const step = std.math.mul(i64, n, step_micros) catch return error.ArithmeticOverflow;
                try out.data.datetime.append(allocator, std.math.add(i64, dt, step) catch return error.ArithmeticOverflow);
            }
        }
    };
}

pub const datetimeAddSecondsKernel = DatetimeAddSteps(std.time.us_per_s).kernel;
pub const datetimeAddMicrosKernel = DatetimeAddSteps(1).kernel;

/// MAKEDATE(year, day_of_year), as MySQL: a year below 100 is 1970-2069,
/// days past the year's end run into the next, and a day of year below 1
/// or a result outside years 0-9999 is NULL.
pub fn makedateKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const base = out.data.rowCount();
    const years = args[0].data.int;
    const day_of_years = args[1].data.int;
    for (0..row_count) |i| {
        const days: ?i32 = if (args[0].isValid(i) and args[1].isValid(i)) makeDate(years[i], day_of_years[i]) else null;
        try out.data.date.append(allocator, days orelse 0);
        try out.appendValidBit(allocator, base + i, days != null);
    }
}

fn makeDate(year: i32, day_of_year: i32) ?i32 {
    if (day_of_year <= 0 or year < 0 or year > 9999) return null;
    const full_year = if (year >= 100) year else year + @as(i32, if (year < 70) 2000 else 1900);
    const days = @as(i64, common.ymdToDays(full_year, 1, 1)) + day_of_year - 1;
    if (days > LAST_DATE_DAYS) return null;
    return @intCast(days);
}

/// 9999-12-31, the last day MySQL's day numbers reach.
const LAST_DATE_DAYS: i64 = 2_932_896;

/// MySQL's day number of 1970-01-01 (TO_DAYS).
const DAY_NUMBER_OF_EPOCH: i64 = 719_528;

/// MySQL's day number (TO_DAYS) of a date: days since year 0, which MySQL
/// counts as 365 days long. Null for 0000-02-29, which MySQL has no day
/// for, and before year 0.
fn dayNumber(days: i32) ?i64 {
    const ymd = daysToYmd(days);
    if (ymd.year < 0) return null;
    if (ymd.year == 0 and ymd.month <= 2) {
        if (ymd.month == 2 and ymd.day == 29) return null;
        return @as(i64, days) + DAY_NUMBER_OF_EPOCH + 1;
    }
    return @as(i64, days) + DAY_NUMBER_OF_EPOCH;
}

pub fn toDaysKernel(comptime temporal: enum { date, datetime }) Kernel {
    return struct {
        fn kernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
            const base = out.data.rowCount();
            for (0..row_count) |i| {
                const days = switch (temporal) {
                    .date => args[0].data.date[i],
                    .datetime => daysFromDatetime(args[0].data.datetime[i]),
                };
                const number = if (args[0].isValid(i)) dayNumber(days) else null;
                try out.data.bigint.append(allocator, number orelse 0);
                try out.appendValidBit(allocator, base + i, number != null);
            }
        }
    }.kernel;
}

/// TO_SECONDS: seconds since year 0, MySQL's day number times 86400 plus
/// the time of day.
pub fn toSecondsKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const base = out.data.rowCount();
    const dts = args[0].data.datetime;
    for (0..row_count) |i| {
        const number = if (args[0].isValid(i)) dayNumber(daysFromDatetime(dts[i])) else null;
        const seconds = if (number) |n| n * 86_400 + @divFloor(@mod(dts[i], std.time.us_per_day), std.time.us_per_s) else 0;
        try out.data.bigint.append(allocator, seconds);
        try out.appendValidBit(allocator, base + i, number != null);
    }
}

/// FROM_DAYS: the date of a MySQL day number. Numbers before 0001-01-01
/// or past 9999-12-31 are NULL; MySQL gives its zero date for most of
/// them, which a DATE can't hold.
pub fn fromDaysKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const base = out.data.rowCount();
    const numbers = args[0].data.bigint;
    for (0..row_count) |i| {
        const n = numbers[i];
        const valid = args[0].isValid(i) and n >= 366 and n <= LAST_DATE_DAYS + DAY_NUMBER_OF_EPOCH;
        try out.data.date.append(allocator, if (valid) @intCast(n - DAY_NUMBER_OF_EPOCH) else 0);
        try out.appendValidBit(allocator, base + i, valid);
    }
}

/// A PERIOD_ADD/PERIOD_DIFF period, `YYMM` or `YYYYMM`: MySQL rejects a
/// period that isn't positive or whose month isn't 1-12.
fn validPeriod(period: i64) bool {
    return period > 0 and @mod(period, 100) != 0 and @mod(period, 100) <= 12;
}

/// Months since year 0 of a valid period; a two-digit year is 1970-2069.
fn periodMonths(period: i64) u64 {
    const p: u64 = @intCast(period);
    var year = p / 100;
    if (year < 70) year += 2000 else if (year < 100) year += 1900;
    return year * 12 + p % 100 - 1;
}

/// The period `YYYYMM` of a month count, in MySQL's unsigned arithmetic,
/// which wraps.
fn monthsPeriod(months: u64) u64 {
    if (months == 0) return 0;
    var year = months / 12;
    if (year < 100) year += if (year < 70) 2000 else 1900;
    return year *% 100 +% months % 12 + 1;
}

/// PERIOD_ADD(period, months). A period MySQL rejects with an error is NULL.
pub fn periodAddKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const base = out.data.rowCount();
    const periods = args[0].data.bigint;
    const months = args[1].data.bigint;
    for (0..row_count) |i| {
        const valid = args[0].isValid(i) and args[1].isValid(i) and validPeriod(periods[i]);
        const sum = if (valid) monthsPeriod(periodMonths(periods[i]) +% @as(u64, @bitCast(months[i]))) else 0;
        try out.data.bigint.append(allocator, @bitCast(sum));
        try out.appendValidBit(allocator, base + i, valid);
    }
}

/// PERIOD_DIFF(p1, p2): months from p2 to p1. A period MySQL rejects with an
/// error is NULL.
pub fn periodDiffKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const base = out.data.rowCount();
    const a = args[0].data.bigint;
    const b = args[1].data.bigint;
    for (0..row_count) |i| {
        const valid = args[0].isValid(i) and args[1].isValid(i) and validPeriod(a[i]) and validPeriod(b[i]);
        const diff: u64 = if (valid) periodMonths(a[i]) -% periodMonths(b[i]) else 0;
        try out.data.bigint.append(allocator, @bitCast(diff));
        try out.appendValidBit(allocator, base + i, valid);
    }
}

/// A CONVERT_TZ zone as seconds east of UTC: an offset `+HH:MM` from -13:59
/// to +14:00, `SYSTEM` (thinDB's clock runs in UTC) or `UTC`. Other named
/// zones need MySQL's time zone tables, which thinDB doesn't carry: null,
/// as in a MySQL without them.
fn zoneOffsetSeconds(zone: []const u8) ?i64 {
    if (std.ascii.eqlIgnoreCase(zone, "SYSTEM") or std.ascii.eqlIgnoreCase(zone, "UTC")) return 0;
    if (zone.len < 4 or (zone[0] != '+' and zone[0] != '-')) return null;
    var pos: usize = 1;
    var hours: i64 = 0;
    while (pos < zone.len and std.ascii.isDigit(zone[pos]) and hours < 100) : (pos += 1) hours = hours * 10 + (zone[pos] - '0');
    if (pos + 1 >= zone.len or zone[pos] != ':') return null;
    pos += 1;
    var minutes: i64 = 0;
    while (pos < zone.len and std.ascii.isDigit(zone[pos]) and minutes < 100) : (pos += 1) minutes = minutes * 10 + (zone[pos] - '0');
    if (pos != zone.len or minutes > 59) return null;
    const offset = (hours * 60 + minutes) * (if (zone[0] == '-') @as(i64, -60) else 60);
    if (offset < -(13 * 3600 + 59 * 60) or offset > 14 * 3600) return null;
    return offset;
}

/// 3001-01-18 23:59:59 UTC, the last second MySQL converts between zones.
const MAX_ZONED_SECONDS: i64 = 32_536_771_199;

/// CONVERT_TZ(dt, from, to). A value whose UTC instant is outside what
/// MySQL converts (1970-01-01 00:00:01 to 3001-01-18 23:59:59) comes back
/// unchanged, as in MySQL.
pub fn convertTzKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const base = out.data.rowCount();
    const dts = args[0].data.datetime;
    const from = stringViewOf(args[1]);
    const to = stringViewOf(args[2]);
    for (0..row_count) |i| {
        const valid = args[0].isValid(i) and args[1].isValid(i) and args[2].isValid(i);
        const from_offset = if (valid) zoneOffsetSeconds(from.rowBytes(i)) else null;
        const to_offset = if (valid) zoneOffsetSeconds(to.rowBytes(i)) else null;
        const converted: ?i64 = if (from_offset != null and to_offset != null) convertZone(dts[i], from_offset.?, to_offset.?) else null;
        try out.data.datetime.append(allocator, converted orelse 0);
        try out.appendValidBit(allocator, base + i, converted != null);
    }
}

fn convertZone(local: i64, from_offset: i64, to_offset: i64) i64 {
    const utc = local -| from_offset * std.time.us_per_s;
    const utc_seconds = @divFloor(utc, std.time.us_per_s);
    if (utc_seconds < 1 or utc_seconds > MAX_ZONED_SECONDS) return local;
    return utc + to_offset * std.time.us_per_s;
}

fn addMonths(days: i32, n_months: i32) i32 {
    const ymd = daysToYmd(days);
    // Compute (year, month_0_indexed) zero-based math, then re-bias.
    const total_m0: i32 = ymd.year * 12 + (@as(i32, ymd.month) - 1) + n_months;
    const new_year: i32 = @divFloor(total_m0, 12);
    const new_month: u32 = @intCast(@mod(total_m0, 12) + 1);
    const last = common.lastDayOfMonth(new_year, new_month);
    const clamped_day: u32 = @min(@as(u32, ymd.day), last);
    return common.ymdToDays(new_year, new_month, clamped_day);
}

pub fn unixTimestampKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const s = args[0].data.datetime;
    var i: usize = 0;
    while (i < row_count) : (i += 1) try out.data.bigint.append(allocator, @divFloor(s[i], 1_000_000));
}

pub fn fromUnixtimeKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const s = args[0].data.bigint;
    var i: usize = 0;
    while (i < row_count) : (i += 1) try out.data.datetime.append(allocator, s[i] * 1_000_000);
}

/// CAST(datetime AS date) — drop the time-of-day (floor to the day).
pub fn dateIdentityKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const s = args[0].data.date;
    var i: usize = 0;
    while (i < row_count) : (i += 1) try out.data.date.append(allocator, s[i]);
}

pub fn datetimeIdentityKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const s = args[0].data.datetime;
    var i: usize = 0;
    while (i < row_count) : (i += 1) try out.data.datetime.append(allocator, s[i]);
}

pub fn datetimeToDateKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const s = args[0].data.datetime;
    var i: usize = 0;
    while (i < row_count) : (i += 1) try out.data.date.append(allocator, @intCast(@divFloor(s[i], std.time.us_per_day)));
}

/// CAST(date AS datetime) — midnight UTC of that day.
pub fn dateToDatetimeKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const s = args[0].data.date;
    var i: usize = 0;
    while (i < row_count) : (i += 1) try out.data.datetime.append(allocator, @as(i64, s[i]) * std.time.us_per_day);
}

/// CAST(text AS DATE). Text that isn't a date is NULL, as in MySQL.
pub fn stringToDateKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    if (row_count == 0) return;
    const base = out.data.rowCount();
    const text = stringViewOf(args[0]);
    for (0..row_count) |i| {
        const days = if (args[0].isValid(i)) common.textToDate(text.rowBytes(i)) else null;
        try out.data.date.append(allocator, days orelse 0);
        try out.appendValidBit(allocator, base + i, days != null);
    }
}

/// CAST(text AS DATETIME). Text that isn't a date is NULL, as in MySQL.
pub fn stringToDatetimeKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    if (row_count == 0) return;
    const base = out.data.rowCount();
    const text = stringViewOf(args[0]);
    for (0..row_count) |i| {
        const micros = if (args[0].isValid(i)) common.textToDatetime(text.rowBytes(i)) else null;
        try out.data.datetime.append(allocator, micros orelse 0);
        try out.appendValidBit(allocator, base + i, micros != null);
    }
}

/// DATE_TRUNC(unit, datetime) → datetime truncated down to the unit
/// boundary. `unit` is a constant string naming a `DateUnit`; weeks start
/// on Monday.
pub fn dateTruncKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    if (row_count == 0) return;
    // The unit is the same for every row, so identify it ONCE — the per-row
    // hot loop is then a branch-free arithmetic truncation, not six repeated
    // case-insensitive string compares.
    const unit = try parseDateUnit(stringViewOf(args[0]).rowBytes(0));
    const s = args[1].data.datetime[0..row_count];

    const base = out.data.datetime.items.len;
    try out.data.datetime.resize(allocator, base + row_count);
    const dst = out.data.datetime.items[base..][0..row_count];

    const us: i64 = 1_000_000;
    switch (unit) {
        // Sub-day units are a floor to a fixed micro-quantum — a comptime
        // divisor that lowers to a multiply-shift and vectorizes.
        .second => truncToQuantum(us, s, dst),
        .minute => truncToQuantum(60 * us, s, dst),
        .hour => truncToQuantum(3600 * us, s, dst),
        .day => truncToQuantum(86_400 * us, s, dst),
        // Week/month/quarter/year boundaries aren't fixed micro-quanta from
        // the epoch — scalar calendar math.
        .week, .month, .quarter, .year => for (dst, s) |*d, v| {
            d.* = truncCalendar(v, unit);
        },
    }
}

/// A unit named by the constant string argument of `date_trunc`,
/// `date_diff`/`timestampdiff` and `timestampadd`.
const DateUnit = enum { second, minute, hour, day, week, month, quarter, year };

/// An unknown unit fails the call: a value passed through unchanged would be a
/// plausible-looking wrong answer.
fn parseDateUnit(unit: []const u8) error{ComputeUnsupportedExpr}!DateUnit {
    const table = .{
        .{ DateUnit.second, .{ "second", "seconds", "ss" } },
        .{ DateUnit.minute, .{ "minute", "minutes", "mi" } },
        .{ DateUnit.hour, .{ "hour", "hours", "hh" } },
        .{ DateUnit.day, .{ "day", "days", "dd" } },
        .{ DateUnit.week, .{ "week", "weeks", "wk" } },
        .{ DateUnit.month, .{ "month", "months", "mm" } },
        .{ DateUnit.quarter, .{ "quarter", "quarters", "qq" } },
        .{ DateUnit.year, .{ "year", "years", "yy" } },
    };
    inline for (table) |e| {
        inline for (e[1]) |spelling| if (std.ascii.eqlIgnoreCase(unit, spelling)) return e[0];
    }
    return error.ComputeUnsupportedExpr;
}

/// Vectorized floor-to-multiple: `dst[i] = s[i] - (s[i] mod q)`, the largest
/// multiple of `q` not exceeding `s[i]` — identical to `@divFloor(s[i], q) * q`
/// for every sign. `q` is comptime so `@mod` lowers to a multiply-shift.
fn truncToQuantum(comptime q: i64, s: []const i64, dst: []i64) void {
    const N = comptime (std.simd.suggestVectorLength(i64) orelse 1);
    var i: usize = 0;
    if (N > 1) {
        const qv: @Vector(N, i64) = @splat(q);
        while (i + N <= s.len) : (i += N) {
            const v: @Vector(N, i64) = s[i..][0..N].*;
            dst[i..][0..N].* = v - @mod(v, qv);
        }
    }
    while (i < s.len) : (i += 1) dst[i] = s[i] - @mod(s[i], q);
}

/// Truncate to the start of a calendar unit (week, month, quarter or year),
/// which can't be expressed as a fixed micro-quantum.
fn truncCalendar(v: i64, unit: DateUnit) i64 {
    const days = daysFromDatetime(v);
    // 1970-01-01 was a Thursday: +3 puts Monday at 0.
    if (unit == .week) return @as(i64, days - @mod(days + 3, 7)) * std.time.us_per_day;
    const ymd = daysToYmd(days);
    const month: u32 = switch (unit) {
        .year => 1,
        .quarter => (@as(u32, ymd.month) - 1) / 3 * 3 + 1,
        else => ymd.month,
    };
    return @as(i64, common.ymdToDays(ymd.year, month, 1)) * std.time.us_per_day;
}

// ---------------------------------------------------------------------------
// MySQL-style calendar helpers (dayofweek / dayofyear / quarter / last_day),
// plus internal helpers used by date_format too.
// ---------------------------------------------------------------------------

/// Days-since-epoch → MySQL weekday index (1=Sunday … 7=Saturday).
fn dayofweekFromDays(days: i32) i32 {
    // 1970-01-01 was a Thursday → MySQL index 5. Days arithmetic in mod 7.
    const d = @mod(days, 7);
    const offset_from_thu: i32 = @mod(d + 4, 7); // 4 = (Thu=5) - 1
    return offset_from_thu + 1;
}

pub fn dayofweekFromDateKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const s = args[0].data.date;
    var i: usize = 0;
    while (i < row_count) : (i += 1) try out.data.int.append(allocator, dayofweekFromDays(s[i]));
}

pub fn dayofweekFromDatetimeKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const s = args[0].data.datetime;
    var i: usize = 0;
    while (i < row_count) : (i += 1) try out.data.int.append(allocator, dayofweekFromDays(daysFromDatetime(s[i])));
}

fn dayofyearFromDays(days: i32) i32 {
    return days - common.ymdToDays(daysToYmd(days).year, 1, 1) + 1;
}

pub fn dayofyearFromDateKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const s = args[0].data.date;
    var i: usize = 0;
    while (i < row_count) : (i += 1) try out.data.int.append(allocator, dayofyearFromDays(s[i]));
}

pub fn dayofyearFromDatetimeKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const s = args[0].data.datetime;
    var i: usize = 0;
    while (i < row_count) : (i += 1) try out.data.int.append(allocator, dayofyearFromDays(daysFromDatetime(s[i])));
}

fn quarterFromDays(days: i32) i32 {
    return @divTrunc(@as(i32, daysToYmd(days).month) - 1, 3) + 1;
}

pub fn quarterFromDateKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const s = args[0].data.date;
    var i: usize = 0;
    while (i < row_count) : (i += 1) try out.data.int.append(allocator, quarterFromDays(s[i]));
}

pub fn quarterFromDatetimeKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const s = args[0].data.datetime;
    var i: usize = 0;
    while (i < row_count) : (i += 1) try out.data.int.append(allocator, quarterFromDays(daysFromDatetime(s[i])));
}

/// LAST_DAY: the last day of the date's month.
fn lastDayFromDays(days: i32) i32 {
    const ymd = daysToYmd(days);
    return days - @as(i32, ymd.day) + @as(i32, @intCast(common.lastDayOfMonth(ymd.year, ymd.month)));
}

pub fn lastDayFromDateKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const s = args[0].data.date;
    var i: usize = 0;
    while (i < row_count) : (i += 1) try out.data.date.append(allocator, lastDayFromDays(s[i]));
}

pub fn lastDayFromDatetimeKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const s = args[0].data.datetime;
    var i: usize = 0;
    while (i < row_count) : (i += 1) try out.data.date.append(allocator, lastDayFromDays(daysFromDatetime(s[i])));
}

// ---------------------------------------------------------------------------
// Additional date/time names and unit-based diff/add helpers.
// ---------------------------------------------------------------------------

const day_names = datefmt.day_names;
const month_names = datefmt.month_names;

pub fn daynameFromDateKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const s = args[0].data.date;
    const ss = stringStoreOf(out);
    var i: usize = 0;
    while (i < row_count) : (i += 1) try ss.appendValue(allocator, day_names[@intCast(dayofweekFromDays(s[i]) - 1)]);
}

pub fn daynameFromDatetimeKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const s = args[0].data.datetime;
    const ss = stringStoreOf(out);
    var i: usize = 0;
    while (i < row_count) : (i += 1) try ss.appendValue(allocator, day_names[@intCast(dayofweekFromDays(daysFromDatetime(s[i])) - 1)]);
}

pub fn monthnameFromDateKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const s = args[0].data.date;
    const ss = stringStoreOf(out);
    var i: usize = 0;
    while (i < row_count) : (i += 1) {
        try ss.appendValue(allocator, month_names[@as(usize, daysToYmd(s[i]).month) - 1]);
    }
}

pub fn monthnameFromDatetimeKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const s = args[0].data.datetime;
    const ss = stringStoreOf(out);
    var i: usize = 0;
    while (i < row_count) : (i += 1) {
        try ss.appendValue(allocator, month_names[@as(usize, daysToYmd(daysFromDatetime(s[i])).month) - 1]);
    }
}

fn monthDiff(start_days: i32, end_days: i32) i64 {
    const s = daysToYmd(start_days);
    const e = daysToYmd(end_days);
    var months = (@as(i64, e.year) - s.year) * 12 + (@as(i64, e.month) - s.month);
    if (months > 0 and e.day < s.day) months -= 1;
    if (months < 0 and e.day > s.day) months += 1;
    return months;
}

fn diffDate(unit: DateUnit, start_days: i32, end_days: i32) i64 {
    const days = @as(i64, end_days) - start_days;
    return switch (unit) {
        .day => days,
        .week => @divTrunc(days, 7),
        .month => monthDiff(start_days, end_days),
        .quarter => @divTrunc(monthDiff(start_days, end_days), 3),
        .year => @divTrunc(monthDiff(start_days, end_days), 12),
        .second => days * 86_400,
        .minute => days * 1_440,
        .hour => days * 24,
    };
}

fn diffDatetime(unit: DateUnit, start_us: i64, end_us: i64) i64 {
    const delta = end_us -| start_us;
    const days = @as(i64, daysFromDatetime(end_us)) - daysFromDatetime(start_us);
    return switch (unit) {
        .second => @divTrunc(delta, 1_000_000),
        .minute => @divTrunc(delta, 60 * 1_000_000),
        .hour => @divTrunc(delta, 3_600 * 1_000_000),
        .day => days,
        .week => @divTrunc(days, 7),
        .month => monthDiff(daysFromDatetime(start_us), daysFromDatetime(end_us)),
        .quarter => @divTrunc(monthDiff(daysFromDatetime(start_us), daysFromDatetime(end_us)), 3),
        .year => @divTrunc(monthDiff(daysFromDatetime(start_us), daysFromDatetime(end_us)), 12),
    };
}

pub fn dateDiffDateKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const unit = try parseDateUnit(stringViewOf(args[0]).rowBytes(0));
    const start = args[1].data.date;
    const end = args[2].data.date;
    for (start[0..row_count], end[0..row_count]) |s, e| try out.data.bigint.append(allocator, diffDate(unit, s, e));
}

pub fn dateDiffDatetimeKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const unit = try parseDateUnit(stringViewOf(args[0]).rowBytes(0));
    const start = args[1].data.datetime;
    const end = args[2].data.datetime;
    for (start[0..row_count], end[0..row_count]) |s, e| try out.data.bigint.append(allocator, diffDatetime(unit, s, e));
}

/// A TIMESTAMPDIFF unit: MySQL's, with or without its SQL_TSI_ prefix, or
/// any spelling `parseDateUnit` accepts.
const DiffUnit = enum { microsecond, second, minute, hour, day, week, month, quarter, year };

fn parseDiffUnit(text: []const u8) error{ComputeUnsupportedExpr}!DiffUnit {
    const unit = if (std.ascii.startsWithIgnoreCase(text, "sql_tsi_")) text["sql_tsi_".len..] else text;
    if (std.ascii.eqlIgnoreCase(unit, "microsecond") or std.ascii.eqlIgnoreCase(unit, "microseconds")) return .microsecond;
    return switch (try parseDateUnit(unit)) {
        inline else => |u| @field(DiffUnit, @tagName(u)),
    };
}

/// TIMESTAMPDIFF(unit, start, end) as MySQL computes it: the whole units
/// elapsed from `start` to `end`, negative when `end` is earlier.
fn timestampDiff(unit: DiffUnit, start: i64, end: i64) i64 {
    const delta = end -| start;
    const seconds = @divTrunc(delta, std.time.us_per_s);
    return switch (unit) {
        .microsecond => delta,
        .second => seconds,
        .minute => @divTrunc(seconds, 60),
        .hour => @divTrunc(seconds, 3_600),
        .day => @divTrunc(seconds, 86_400),
        .week => @divTrunc(seconds, 7 * 86_400),
        .month => elapsedMonths(start, end),
        .quarter => @divTrunc(elapsedMonths(start, end), 3),
        .year => @divTrunc(elapsedMonths(start, end), 12),
    };
}

/// Whole months from `start` to `end`: a month counts once the later
/// value's day of the month and time of day reach the earlier one's.
fn elapsedMonths(start: i64, end: i64) i64 {
    const earlier = @min(start, end);
    const later = @max(start, end);
    const b = daysToYmd(daysFromDatetime(earlier));
    const e = daysToYmd(daysFromDatetime(later));
    var months = (@as(i64, e.year) - b.year) * 12 + (@as(i64, e.month) - b.month);
    const time_before = @mod(later, std.time.us_per_day) < @mod(earlier, std.time.us_per_day);
    if (e.day < b.day or (e.day == b.day and time_before)) months -= 1;
    return if (end < start) -months else months;
}

pub fn timestampDiffKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const unit = try parseDiffUnit(stringViewOf(args[0]).rowBytes(0));
    const start = args[1].data.datetime;
    const end = args[2].data.datetime;
    for (start[0..row_count], end[0..row_count]) |s, e| try out.data.bigint.append(allocator, timestampDiff(unit, s, e));
}

fn addUnitToDate(unit: DateUnit, days: i32, n: i32) i32 {
    return switch (unit) {
        .day => days + n,
        .week => days + n * 7,
        .month => addMonths(days, n),
        .quarter => addMonths(days, n * 3),
        .year => addMonths(days, n * 12),
        .hour => daysFromDatetime(@as(i64, days) * std.time.us_per_day + @as(i64, n) * std.time.us_per_hour),
        .minute => daysFromDatetime(@as(i64, days) * std.time.us_per_day + @as(i64, n) * std.time.us_per_min),
        .second => daysFromDatetime(@as(i64, days) * std.time.us_per_day + @as(i64, n) * std.time.us_per_s),
    };
}

fn addUnitToDatetime(unit: DateUnit, micros: i64, n: i32) i64 {
    return switch (unit) {
        .second => micros + @as(i64, n) * std.time.us_per_s,
        .minute => micros + @as(i64, n) * std.time.us_per_min,
        .hour => micros + @as(i64, n) * std.time.us_per_hour,
        .day => micros + @as(i64, n) * std.time.us_per_day,
        .week => micros + @as(i64, n) * 7 * std.time.us_per_day,
        .month, .quarter, .year => blk: {
            const days = daysFromDatetime(micros);
            const time_of_day = @mod(micros, std.time.us_per_day);
            const months = switch (unit) {
                .year => n * 12,
                .quarter => n * 3,
                else => n,
            };
            break :blk @as(i64, addMonths(days, months)) * std.time.us_per_day + time_of_day;
        },
    };
}

pub fn timestampAddDateKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const unit = try parseDateUnit(stringViewOf(args[0]).rowBytes(0));
    const ns = args[1].data.int;
    const dates = args[2].data.date;
    var i: usize = 0;
    while (i < row_count) : (i += 1) try out.data.date.append(allocator, addUnitToDate(unit, dates[i], ns[i]));
}

pub fn timestampAddDatetimeKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const unit = try parseDateUnit(stringViewOf(args[0]).rowBytes(0));
    const ns = args[1].data.int;
    const dts = args[2].data.datetime;
    var i: usize = 0;
    while (i < row_count) : (i += 1) try out.data.datetime.append(allocator, addUnitToDatetime(unit, dts[i], ns[i]));
}

// ---------------------------------------------------------------------------
// date_format: MySQL's specifiers (scalar_fn_datefmt.zig). Per-row format
// strings are allowed; each row re-reads its format, which costs a few
// hundred ns.
// ---------------------------------------------------------------------------

fn dateFormatRow(
    allocator: Allocator,
    out: *ColumnStore,
    fmt: []const u8,
    days: i32,
    micros_into_day: i64,
) !void {
    var buf: std.ArrayList(u8) = .empty;
    defer buf.deinit(allocator);
    try datefmt.format(allocator, &buf, fmt, days, micros_into_day);
    try stringStoreOf(out).appendValue(allocator, buf.items);
}

pub fn dateFormatDatetimeKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const dts = args[0].data.datetime;
    const fmt_sv = stringViewOf(args[1]);
    var i: usize = 0;
    while (i < row_count) : (i += 1) {
        const micros = dts[i];
        const days = daysFromDatetime(micros);
        const micros_into_day = @mod(micros, std.time.us_per_day);
        try dateFormatRow(allocator, out, fmt_sv.rowBytes(i), days, micros_into_day);
    }
}

pub fn dateFormatDateKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const ds = args[0].data.date;
    const fmt_sv = stringViewOf(args[1]);
    var i: usize = 0;
    while (i < row_count) : (i += 1) {
        try dateFormatRow(allocator, out, fmt_sv.rowBytes(i), ds[i], 0);
    }
}

pub fn dateToStringKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const ds = args[0].data.date;
    const ss = stringStoreOf(out);
    var buf: [32]u8 = undefined;
    for (ds[0..row_count]) |d| try ss.appendValue(allocator, try common.formatDate(&buf, d));
}

pub fn datetimeToStringKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const dts = args[0].data.datetime;
    const ss = stringStoreOf(out);
    var buf: [48]u8 = undefined;
    for (dts[0..row_count]) |dt| try ss.appendValue(allocator, try common.formatDateTime(&buf, dt));
}

test "date_trunc: vectorized quantum truncation matches @divFloor reference" {
    const us: i64 = 1_000_000;
    // Lengths spanning the vector-tail boundary; sign-mixed inputs (pre-epoch
    // negatives stress @mod's sign vs the @divFloor*q reference).
    const lengths = [_]usize{ 0, 1, 3, 7, 8, 15, 16, 17, 64, 1000 };
    inline for (.{ us, 60 * us, 3600 * us, 86_400 * us }) |q| {
        for (lengths) |len| {
            const s = try std.testing.allocator.alloc(i64, len);
            defer std.testing.allocator.free(s);
            const dst = try std.testing.allocator.alloc(i64, len);
            defer std.testing.allocator.free(dst);
            for (s, 0..) |*v, idx| {
                const x: i64 = @intCast(idx);
                v.* = (x *% 7919 -% 5000) *% 137; // varied, spans negatives
            }
            truncToQuantum(q, s, dst);
            for (s, dst) |v, d| try std.testing.expectEqual(@divFloor(v, q) * q, d);
        }
    }
}

test "date_trunc: month/year land on the first of the unit" {
    const us: i64 = 1_000_000;
    const day = 86_400 * us;
    // 2013-07-14 12:34:56 UTC = 15900 days since epoch + time-of-day.
    const d_2013_07_14: i64 = 15900;
    const v = d_2013_07_14 * day + (12 * 3600 + 34 * 60 + 56) * us + 789_000;
    // month → 2013-07-01 00:00:00; year → 2013-01-01 00:00:00.
    const d_2013_07_01 = common.ymdToDays(2013, 7, 1);
    const d_2013_01_01 = common.ymdToDays(2013, 1, 1);
    try std.testing.expectEqual(@as(i64, d_2013_07_01) * day, truncCalendar(v, .month));
    try std.testing.expectEqual(@as(i64, d_2013_07_01) * day, truncCalendar(v, .quarter));
    try std.testing.expectEqual(@as(i64, d_2013_01_01) * day, truncCalendar(v, .year));
    // 2013-07-14 was a Sunday; its week starts Monday 2013-07-08.
    try std.testing.expectEqual(@as(i64, common.ymdToDays(2013, 7, 8)) * day, truncCalendar(v, .week));
}

test "date unit parse is case-insensitive and rejects unknown units" {
    try std.testing.expectEqual(DateUnit.minute, try parseDateUnit("MiNuTe"));
    try std.testing.expectEqual(DateUnit.year, try parseDateUnit("YEAR"));
    try std.testing.expectEqual(DateUnit.quarter, try parseDateUnit("Quarters"));
    try std.testing.expectEqual(DateUnit.week, try parseDateUnit("wk"));
    try std.testing.expectError(error.ComputeUnsupportedExpr, parseDateUnit("fortnight"));
}

test "TIMESTAMPDIFF counts whole units as MySQL does" {
    const dt = struct {
        fn at(y: i32, mo: u32, d: u32, h: i64, mi: i64, s: i64) i64 {
            return @as(i64, common.ymdToDays(y, mo, d)) * std.time.us_per_day + ((h * 60 + mi) * 60 + s) * std.time.us_per_s;
        }
    }.at;
    // Every expected value is MySQL 8.4's.
    const cases = .{
        .{ "MONTH", dt(2026, 1, 31, 10, 0, 0), dt(2026, 2, 28, 9, 0, 0), 0 },
        .{ "MONTH", dt(2026, 1, 15, 10, 0, 0), dt(2026, 2, 15, 10, 0, 0), 1 },
        .{ "MONTH", dt(2026, 1, 15, 10, 0, 0), dt(2026, 2, 15, 9, 59, 59), 0 },
        .{ "MONTH", dt(2026, 2, 15, 9, 0, 0), dt(2026, 1, 15, 10, 0, 0), 0 },
        .{ "MONTH", dt(2026, 3, 31, 0, 0, 0), dt(2026, 2, 28, 0, 0, 0), -1 },
        .{ "YEAR", dt(2024, 2, 29, 0, 0, 0), dt(2025, 2, 28, 0, 0, 0), 0 },
        .{ "QUARTER", dt(2026, 1, 1, 0, 0, 0), dt(2026, 12, 31, 23, 59, 59), 3 },
        .{ "WEEK", dt(2026, 1, 1, 0, 0, 0), dt(2025, 12, 17, 0, 0, 1), -2 },
        .{ "MICROSECOND", dt(2026, 1, 1, 0, 0, 0), dt(2026, 1, 1, 0, 0, 1) + 500_000, 1_500_000 },
        .{ "SECOND", dt(1970, 1, 1, 0, 0, 0), dt(2100, 1, 1, 0, 0, 0), 4_102_444_800 },
        .{ "DAY", dt(2026, 1, 1, 12, 0, 0), dt(2026, 1, 3, 11, 59, 59), 1 },
        .{ "SQL_TSI_HOUR", dt(2026, 1, 1, 0, 0, 0), dt(2026, 1, 2, 0, 0, 0), 24 },
        .{ "MINUTE", dt(2026, 1, 1, 0, 0, 0), dt(2025, 12, 31, 23, 58, 30), -1 },
    };
    inline for (cases) |c| try std.testing.expectEqual(@as(i64, c[3]), timestampDiff(try parseDiffUnit(c[0]), c[1], c[2]));
    try std.testing.expectError(error.ComputeUnsupportedExpr, parseDiffUnit("SQL_TSI_FORTNIGHT"));
}

test "periods, day numbers and zone offsets follow MySQL" {
    const t = std.testing;
    // Every expected value is MySQL 8.4's.
    inline for (.{ .{ 202601, 13, 202702 }, .{ 6901, 1, 206902 }, .{ 7001, -1, 196912 }, .{ 1, -1, 199912 }, .{ 9912, 1, 200001 } }) |c| {
        try t.expectEqual(@as(u64, c[2]), monthsPeriod(periodMonths(c[0]) +% @as(u64, @bitCast(@as(i64, c[1])))));
    }
    try t.expectEqual(@as(u64, 313), periodMonths(202601) -% periodMonths(199912));
    inline for (.{ 0, -5, 202600, 202613 }) |p| try t.expect(!validPeriod(p));
    inline for (.{ .{ 0, 1, 1, 1 }, .{ 0, 3, 1, 60 }, .{ 1970, 1, 1, 719_528 }, .{ 2026, 9, 26, 740_250 }, .{ 1, 1, 1, 366 } }) |c| {
        try t.expectEqual(@as(?i64, c[3]), dayNumber(common.ymdToDays(c[0], c[1], c[2])));
    }
    try t.expectEqual(@as(?i64, null), dayNumber(common.ymdToDays(0, 2, 29)));
    inline for (.{ .{ "+14:00", 50_400 }, .{ "-13:59", -50_340 }, .{ "+5:30", 19_800 }, .{ "+05:3", 18_180 }, .{ "utc", 0 } }) |c| {
        try t.expectEqual(@as(?i64, c[1]), zoneOffsetSeconds(c[0]));
    }
    inline for (.{ "+14:01", "-14:00", "+0530", "+05:60", "+1:00x", "Europe/Paris" }) |z| try t.expectEqual(@as(?i64, null), zoneOffsetSeconds(z));
}
