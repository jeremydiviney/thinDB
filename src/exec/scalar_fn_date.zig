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

/// DATE_TRUNC(unit, datetime) → datetime truncated down to the unit
/// boundary. `unit` is a constant string ('second'/'minute'/'hour'/
/// 'day'/'month'/'year'); an unrecognized unit passes the value through.
pub fn dateTruncKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    if (row_count == 0) return;
    const unit = stringViewOf(args[0]).rowBytes(0);
    const s = args[1].data.datetime;
    var i: usize = 0;
    while (i < row_count) : (i += 1) {
        try out.data.datetime.append(allocator, truncDatetime(s[i], unit));
    }
}

fn truncDatetime(v: i64, unit: []const u8) i64 {
    const us: i64 = 1_000_000;
    if (std.ascii.eqlIgnoreCase(unit, "second")) return @divFloor(v, us) * us;
    if (std.ascii.eqlIgnoreCase(unit, "minute")) return @divFloor(v, 60 * us) * (60 * us);
    if (std.ascii.eqlIgnoreCase(unit, "hour")) return @divFloor(v, 3600 * us) * (3600 * us);
    if (std.ascii.eqlIgnoreCase(unit, "day")) return @divFloor(v, 86_400 * us) * (86_400 * us);
    if (std.ascii.eqlIgnoreCase(unit, "month") or std.ascii.eqlIgnoreCase(unit, "year")) {
        const ymd = daysToYmd(daysFromDatetime(v)) orelse return v;
        const month: u32 = if (std.ascii.eqlIgnoreCase(unit, "year")) 1 else @intCast(ymd.month);
        const td = common.ymdToDays(@intCast(ymd.year), month, 1);
        return @as(i64, td) * 86_400 * us;
    }
    return v;
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
