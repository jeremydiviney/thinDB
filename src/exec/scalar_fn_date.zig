//! Date / datetime scalar kernels. Includes the per-component extractors
//! (year, month, day, hour, ...) for both DATE and DATETIME, calendar
//! arithmetic (datediff, date_add, date_sub), epoch conversion
//! (unix_timestamp, from_unixtime), MySQL-style calendar helpers
//! (dayofweek, dayofyear, quarter, last_day), and date_format.

const std = @import("std");
const Allocator = std.mem.Allocator;

const common = @import("scalar_fn_common.zig");
const ColumnView = common.ColumnView;
const ColumnStore = common.ColumnStore;
const stringViewOf = common.stringViewOf;
const stringStoreOf = common.stringStoreOf;
const daysToYmd = common.daysToYmd;
const microsToHms = common.microsToHms;
const daysFromDatetime = common.daysFromDatetime;

// ---------------------------------------------------------------------------
// Component extractors (year/month/day for date + datetime; hour/minute/sec
// for datetime).
// ---------------------------------------------------------------------------

pub fn yearFromDateKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const s = args[0].data.date;
    var i: usize = 0;
    while (i < row_count) : (i += 1) {
        const y: i32 = if (daysToYmd(s[i])) |ymd| @intCast(ymd.year) else 0;
        try out.data.int.append(allocator, y);
    }
}

pub fn yearFromDatetimeKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const s = args[0].data.datetime;
    var i: usize = 0;
    while (i < row_count) : (i += 1) {
        const y: i32 = if (daysToYmd(daysFromDatetime(s[i]))) |ymd| @intCast(ymd.year) else 0;
        try out.data.int.append(allocator, y);
    }
}

pub fn monthFromDateKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const s = args[0].data.date;
    var i: usize = 0;
    while (i < row_count) : (i += 1) {
        const m: i32 = if (daysToYmd(s[i])) |ymd| @intCast(ymd.month) else 0;
        try out.data.int.append(allocator, m);
    }
}

pub fn monthFromDatetimeKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const s = args[0].data.datetime;
    var i: usize = 0;
    while (i < row_count) : (i += 1) {
        const m: i32 = if (daysToYmd(daysFromDatetime(s[i]))) |ymd| @intCast(ymd.month) else 0;
        try out.data.int.append(allocator, m);
    }
}

pub fn dayFromDateKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const s = args[0].data.date;
    var i: usize = 0;
    while (i < row_count) : (i += 1) {
        const d: i32 = if (daysToYmd(s[i])) |ymd| @intCast(ymd.day) else 0;
        try out.data.int.append(allocator, d);
    }
}

pub fn dayFromDatetimeKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const s = args[0].data.datetime;
    var i: usize = 0;
    while (i < row_count) : (i += 1) {
        const d: i32 = if (daysToYmd(daysFromDatetime(s[i]))) |ymd| @intCast(ymd.day) else 0;
        try out.data.int.append(allocator, d);
    }
}

pub fn hourKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const s = args[0].data.datetime;
    var i: usize = 0;
    while (i < row_count) : (i += 1) {
        const h: i32 = if (microsToHms(s[i])) |hms| @intCast(hms.hour) else 0;
        try out.data.int.append(allocator, h);
    }
}

pub fn minuteKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const s = args[0].data.datetime;
    var i: usize = 0;
    while (i < row_count) : (i += 1) {
        const m: i32 = if (microsToHms(s[i])) |hms| @intCast(hms.minute) else 0;
        try out.data.int.append(allocator, m);
    }
}

pub fn secondKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const s = args[0].data.datetime;
    var i: usize = 0;
    while (i < row_count) : (i += 1) {
        const sec: i32 = if (microsToHms(s[i])) |hms| @intCast(hms.second) else 0;
        try out.data.int.append(allocator, sec);
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

pub fn makedateKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const years = args[0].data.int;
    const day_of_years = args[1].data.int;
    var i: usize = 0;
    while (i < row_count) : (i += 1) {
        const day_of_year = day_of_years[i];
        if (day_of_year <= 0) {
            try out.data.date.append(allocator, 0);
            continue;
        }
        const first_day = common.ymdToDays(years[i], 1, 1);
        try out.data.date.append(allocator, first_day + day_of_year - 1);
    }
}

fn addMonths(days: i32, n_months: i32) i32 {
    const ymd = daysToYmd(days) orelse return days;
    // Compute (year, month_0_indexed) zero-based math, then re-bias.
    const total_m0: i32 = @as(i32, @intCast(ymd.year)) * 12 + (@as(i32, ymd.month) - 1) + n_months;
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

/// DATE_TRUNC(unit, datetime) → datetime truncated down to the unit
/// boundary. `unit` is a constant string ('second'/'minute'/'hour'/
/// 'day'/'month'/'year'); an unrecognized unit passes the value through.
pub fn dateTruncKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    if (row_count == 0) return;
    // The unit is the same for every row, so identify it ONCE — the per-row
    // hot loop is then a branch-free arithmetic truncation, not six repeated
    // case-insensitive string compares.
    const unit = parseTruncUnit(stringViewOf(args[0]).rowBytes(0));
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
        // Month/year boundaries aren't fixed-width — scalar calendar math.
        .month, .year => for (dst, s) |*d, v| {
            d.* = truncCalendar(v, unit == .year);
        },
        .other => @memcpy(dst, s),
    }
}

const TruncUnit = enum { second, minute, hour, day, month, year, other };

fn parseTruncUnit(unit: []const u8) TruncUnit {
    const table = .{
        .{ "second", TruncUnit.second }, .{ "minute", TruncUnit.minute },
        .{ "hour", TruncUnit.hour },     .{ "day", TruncUnit.day },
        .{ "month", TruncUnit.month },   .{ "year", TruncUnit.year },
    };
    inline for (table) |e| if (std.ascii.eqlIgnoreCase(unit, e[0])) return e[1];
    return .other;
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

/// Truncate to the start of the month (or year) — calendar-relative, so it
/// can't be expressed as a fixed micro-quantum. Unparseable values pass through.
fn truncCalendar(v: i64, to_year: bool) i64 {
    const us: i64 = 1_000_000;
    const ymd = daysToYmd(daysFromDatetime(v)) orelse return v;
    const month: u32 = if (to_year) 1 else @intCast(ymd.month);
    const td = common.ymdToDays(@intCast(ymd.year), month, 1);
    return @as(i64, td) * 86_400 * us;
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
    if (days < 0) return 0;
    const u_days: u47 = @intCast(days);
    const epoch_day = std.time.epoch.EpochDay{ .day = u_days };
    const year_day = epoch_day.calculateYearDay();
    return @as(i32, year_day.day) + 1;
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
    const ymd = daysToYmd(days) orelse return 0;
    return @divTrunc(@as(i32, ymd.month) - 1, 3) + 1;
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

/// LAST_DAY: return the date corresponding to the last day of the given
/// month. Days-since-epoch backed; pre-epoch returns 0.
fn lastDayFromDays(days: i32) i32 {
    const ymd = daysToYmd(days) orelse return 0;
    const last = daysInMonth(ymd.year, ymd.month);
    return days - @as(i32, ymd.day - 1) + @as(i32, last - 1);
}

fn daysInMonth(yr: u16, mo: u4) u5 {
    return switch (mo) {
        1, 3, 5, 7, 8, 10, 12 => 31,
        4, 6, 9, 11 => 30,
        2 => if (isLeapYear(yr)) 29 else 28,
        else => 0,
    };
}

fn isLeapYear(yr: u16) bool {
    if (yr % 400 == 0) return true;
    if (yr % 100 == 0) return false;
    return yr % 4 == 0;
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

const day_names = [_][]const u8{ "Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday" };
const month_names = [_][]const u8{ "January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December" };

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
        const ymd = daysToYmd(s[i]) orelse {
            try ss.appendValue(allocator, "");
            continue;
        };
        try ss.appendValue(allocator, month_names[@as(usize, ymd.month) - 1]);
    }
}

pub fn monthnameFromDatetimeKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const s = args[0].data.datetime;
    const ss = stringStoreOf(out);
    var i: usize = 0;
    while (i < row_count) : (i += 1) {
        const ymd = daysToYmd(daysFromDatetime(s[i])) orelse {
            try ss.appendValue(allocator, "");
            continue;
        };
        try ss.appendValue(allocator, month_names[@as(usize, ymd.month) - 1]);
    }
}

const DiffUnit = enum { second, minute, hour, day, month, year, other };

fn parseDiffUnit(unit: []const u8) DiffUnit {
    if (std.ascii.eqlIgnoreCase(unit, "second") or std.ascii.eqlIgnoreCase(unit, "seconds") or std.ascii.eqlIgnoreCase(unit, "ss")) return .second;
    if (std.ascii.eqlIgnoreCase(unit, "minute") or std.ascii.eqlIgnoreCase(unit, "minutes") or std.ascii.eqlIgnoreCase(unit, "mi")) return .minute;
    if (std.ascii.eqlIgnoreCase(unit, "hour") or std.ascii.eqlIgnoreCase(unit, "hours") or std.ascii.eqlIgnoreCase(unit, "hh")) return .hour;
    if (std.ascii.eqlIgnoreCase(unit, "day") or std.ascii.eqlIgnoreCase(unit, "days") or std.ascii.eqlIgnoreCase(unit, "dd")) return .day;
    if (std.ascii.eqlIgnoreCase(unit, "month") or std.ascii.eqlIgnoreCase(unit, "months") or std.ascii.eqlIgnoreCase(unit, "mm")) return .month;
    if (std.ascii.eqlIgnoreCase(unit, "year") or std.ascii.eqlIgnoreCase(unit, "years") or std.ascii.eqlIgnoreCase(unit, "yy")) return .year;
    return .other;
}

fn monthDiff(start_days: i32, end_days: i32) i32 {
    const s = daysToYmd(start_days) orelse return 0;
    const e = daysToYmd(end_days) orelse return 0;
    var months = (@as(i32, e.year) - @as(i32, s.year)) * 12 + (@as(i32, e.month) - @as(i32, s.month));
    if (months > 0 and e.day < s.day) months -= 1;
    if (months < 0 and e.day > s.day) months += 1;
    return months;
}

fn diffDate(unit: DiffUnit, start_days: i32, end_days: i32) i32 {
    return switch (unit) {
        .day => end_days - start_days,
        .month => monthDiff(start_days, end_days),
        .year => @divTrunc(monthDiff(start_days, end_days), 12),
        .second => (end_days - start_days) * 86_400,
        .minute => (end_days - start_days) * 1_440,
        .hour => (end_days - start_days) * 24,
        .other => 0,
    };
}

fn diffDatetime(unit: DiffUnit, start_us: i64, end_us: i64) i32 {
    const delta = end_us - start_us;
    const v: i64 = switch (unit) {
        .second => @divTrunc(delta, 1_000_000),
        .minute => @divTrunc(delta, 60 * 1_000_000),
        .hour => @divTrunc(delta, 3_600 * 1_000_000),
        .day => @as(i64, daysFromDatetime(end_us) - daysFromDatetime(start_us)),
        .month => monthDiff(daysFromDatetime(start_us), daysFromDatetime(end_us)),
        .year => @divTrunc(monthDiff(daysFromDatetime(start_us), daysFromDatetime(end_us)), 12),
        .other => 0,
    };
    return std.math.lossyCast(i32, v);
}

pub fn dateDiffDateKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const unit = parseDiffUnit(stringViewOf(args[0]).rowBytes(0));
    const start = args[1].data.date;
    const end = args[2].data.date;
    var i: usize = 0;
    while (i < row_count) : (i += 1) try out.data.int.append(allocator, diffDate(unit, start[i], end[i]));
}

pub fn dateDiffDatetimeKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const unit = parseDiffUnit(stringViewOf(args[0]).rowBytes(0));
    const start = args[1].data.datetime;
    const end = args[2].data.datetime;
    var i: usize = 0;
    while (i < row_count) : (i += 1) try out.data.int.append(allocator, diffDatetime(unit, start[i], end[i]));
}

fn addUnitToDate(unit: DiffUnit, days: i32, n: i32) i32 {
    return switch (unit) {
        .day => days + n,
        .month => addMonths(days, n),
        .year => addMonths(days, n * 12),
        .hour => daysFromDatetime(@as(i64, days) * std.time.us_per_day + @as(i64, n) * std.time.us_per_hour),
        .minute => daysFromDatetime(@as(i64, days) * std.time.us_per_day + @as(i64, n) * std.time.us_per_min),
        .second => daysFromDatetime(@as(i64, days) * std.time.us_per_day + @as(i64, n) * std.time.us_per_s),
        .other => days,
    };
}

fn addUnitToDatetime(unit: DiffUnit, micros: i64, n: i32) i64 {
    return switch (unit) {
        .second => micros + @as(i64, n) * std.time.us_per_s,
        .minute => micros + @as(i64, n) * std.time.us_per_min,
        .hour => micros + @as(i64, n) * std.time.us_per_hour,
        .day => micros + @as(i64, n) * std.time.us_per_day,
        .month, .year => blk: {
            const days = daysFromDatetime(micros);
            const time_of_day = @mod(micros, std.time.us_per_day);
            const months = if (unit == .year) n * 12 else n;
            break :blk @as(i64, addMonths(days, months)) * std.time.us_per_day + time_of_day;
        },
        .other => micros,
    };
}

pub fn timestampAddDateKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const unit = parseDiffUnit(stringViewOf(args[0]).rowBytes(0));
    const ns = args[1].data.int;
    const dates = args[2].data.date;
    var i: usize = 0;
    while (i < row_count) : (i += 1) try out.data.date.append(allocator, addUnitToDate(unit, dates[i], ns[i]));
}

pub fn timestampAddDatetimeKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const unit = parseDiffUnit(stringViewOf(args[0]).rowBytes(0));
    const ns = args[1].data.int;
    const dts = args[2].data.datetime;
    var i: usize = 0;
    while (i < row_count) : (i += 1) try out.data.datetime.append(allocator, addUnitToDatetime(unit, dts[i], ns[i]));
}

// ---------------------------------------------------------------------------
// date_format — MySQL-style strftime subset. Recognised specifiers:
//   %Y  4-digit year   %y  2-digit year (last two digits)
//   %m  month 01-12    %d  day 01-31
//   %H  hour 00-23     %i  minute 00-59  %s  second 00-59
//   %%  literal '%'
// Other %X sequences pass through with the '%' stripped (matches MySQL's
// "unknown specifier" behavior); bare text is copied verbatim.
//
// Per-row format strings are allowed but rare — most callers pass a literal
// format. We don't precompile (would require constant folding); each row
// re-parses, which is fine at ~few hundred ns per row.
// ---------------------------------------------------------------------------

fn appendDigits2(buf: *std.ArrayList(u8), aa: Allocator, v: u64) !void {
    try buf.append(aa, @intCast('0' + (v / 10) % 10));
    try buf.append(aa, @intCast('0' + (v % 10)));
}

fn appendDigits4(buf: *std.ArrayList(u8), aa: Allocator, v: u64) !void {
    try buf.append(aa, @intCast('0' + (v / 1000) % 10));
    try buf.append(aa, @intCast('0' + (v / 100) % 10));
    try buf.append(aa, @intCast('0' + (v / 10) % 10));
    try buf.append(aa, @intCast('0' + (v % 10)));
}

fn dateFormatRow(
    allocator: Allocator,
    out: *ColumnStore,
    fmt: []const u8,
    days: i32,
    micros_into_day: i64,
) !void {
    const ymd_opt = daysToYmd(days);
    var hh: u32 = 0;
    var mm: u32 = 0;
    var ss_v: u32 = 0;
    if (micros_into_day >= 0) {
        const total_secs = @divTrunc(micros_into_day, 1_000_000);
        hh = @intCast(@divTrunc(total_secs, 3600));
        mm = @intCast(@mod(@divTrunc(total_secs, 60), 60));
        ss_v = @intCast(@mod(total_secs, 60));
    }

    var buf: std.ArrayList(u8) = .empty;
    defer buf.deinit(allocator);

    var i: usize = 0;
    while (i < fmt.len) {
        if (fmt[i] != '%') {
            try buf.append(allocator, fmt[i]);
            i += 1;
            continue;
        }
        if (i + 1 >= fmt.len) {
            try buf.append(allocator, '%');
            i += 1;
            continue;
        }
        const spec = fmt[i + 1];
        i += 2;
        switch (spec) {
            'Y' => if (ymd_opt) |ymd| try appendDigits4(&buf, allocator, ymd.year) else try buf.appendSlice(allocator, "0000"),
            'y' => if (ymd_opt) |ymd| try appendDigits2(&buf, allocator, @as(u64, ymd.year) % 100) else try buf.appendSlice(allocator, "00"),
            'm' => if (ymd_opt) |ymd| try appendDigits2(&buf, allocator, ymd.month) else try buf.appendSlice(allocator, "00"),
            'd' => if (ymd_opt) |ymd| try appendDigits2(&buf, allocator, ymd.day) else try buf.appendSlice(allocator, "00"),
            'H' => try appendDigits2(&buf, allocator, hh),
            'i' => try appendDigits2(&buf, allocator, mm),
            's' => try appendDigits2(&buf, allocator, ss_v),
            '%' => try buf.append(allocator, '%'),
            else => try buf.append(allocator, spec), // unknown: pass through stripped
        }
    }

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
    try std.testing.expectEqual(@as(i64, d_2013_07_01) * day, truncCalendar(v, false));
    try std.testing.expectEqual(@as(i64, d_2013_01_01) * day, truncCalendar(v, true));
}

test "date_trunc: unit parse is case-insensitive, unknown → other" {
    try std.testing.expectEqual(TruncUnit.minute, parseTruncUnit("MiNuTe"));
    try std.testing.expectEqual(TruncUnit.year, parseTruncUnit("YEAR"));
    try std.testing.expectEqual(TruncUnit.other, parseTruncUnit("fortnight"));
}
