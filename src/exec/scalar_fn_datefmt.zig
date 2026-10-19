//! MySQL's date format specifiers, shared by DATE_FORMAT (formatting) and
//! STR_TO_DATE (parsing), and MySQL's week numbering (sql_time.cc
//! calc_week), which %U %u %V %v %X %x and WEEK / YEARWEEK / WEEKOFYEAR read.
//! Also StarRocks' other formats: Joda-Time patterns (JODATIME_FORMAT,
//! STR_TO_JODATIME) and Teradata's (TO_TERA_DATE, TO_TERA_TIMESTAMP).

const std = @import("std");
const Allocator = std.mem.Allocator;

const common = @import("scalar_fn_common.zig");
const time = @import("scalar_fn_time.zig");
const ColumnView = common.ColumnView;
const ColumnStore = common.ColumnStore;
const stringViewOf = common.stringViewOf;

const Kernel = *const fn (allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) anyerror!void;

pub const day_names = [_][]const u8{ "Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday" };
pub const month_names = [_][]const u8{ "January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December" };

// ---------------------------------------------------------------------------
// Week numbering
// ---------------------------------------------------------------------------

/// calc_week's behaviour bits, in WEEK()'s `mode` bit order.
const WeekBehaviour = packed struct(u3) {
    monday_first: bool = false,
    week_year: bool = false,
    first_weekday: bool = false,
};

const YearWeek = struct { year: i32, week: i32 };

/// WEEK(d, mode)'s behaviour for any integer mode (only the low three bits
/// count, as in MySQL).
fn weekBehaviour(mode: i64) WeekBehaviour {
    var b: WeekBehaviour = @bitCast(@as(u3, @intCast(@mod(mode, 8))));
    if (!b.monday_first) b.first_weekday = !b.first_weekday;
    return b;
}

/// Weekday of `days`: 0 = Monday, or 0 = Sunday when `sunday_first`.
fn weekdayOf(days: i32, sunday_first: bool) i32 {
    // 1970-01-01 was a Thursday.
    return @mod(days + 3 + @as(i32, @intFromBool(sunday_first)), 7);
}

fn daysInYear(year: i32) i32 {
    return if (common.isLeapYear(year)) 366 else 365;
}

/// Whether a year whose January 1 falls on `weekday` opens with days that
/// belong to the week before its week 1.
fn opensWithPartialWeek(b: WeekBehaviour, weekday: i32) bool {
    return if (b.first_weekday) weekday != 0 else weekday >= 4;
}

/// The week `days` falls in under `b`, and the year that week belongs to,
/// which differs from the date's own year only under `week_year`.
fn calcWeek(days: i32, b: WeekBehaviour) YearWeek {
    const ymd = common.daysToYmd(days);
    var year = ymd.year;
    var first_day = common.ymdToDays(year, 1, 1);
    var weekday = weekdayOf(first_day, !b.monday_first);
    var week_year = b.week_year;
    if (ymd.month == 1 and ymd.day <= 7 - weekday) {
        if (!week_year and opensWithPartialWeek(b, weekday)) return .{ .year = year, .week = 0 };
        week_year = true;
        year -= 1;
        const len = daysInYear(year);
        first_day -= len;
        weekday = @mod(weekday + 53 * 7 - len, 7);
    }
    const offset = if (opensWithPartialWeek(b, weekday))
        days - (first_day + 7 - weekday)
    else
        days - (first_day - weekday);
    if (week_year and offset >= 52 * 7) {
        const next_weekday = @mod(weekday + daysInYear(year), 7);
        if (!opensWithPartialWeek(b, next_weekday)) return .{ .year = year + 1, .week = 1 };
    }
    return .{ .year = year, .week = @divFloor(offset, 7) + 1 };
}

pub fn week(days: i32, mode: i64) i32 {
    return calcWeek(days, weekBehaviour(mode)).week;
}

pub fn yearWeek(days: i32, mode: i64) i32 {
    var b = weekBehaviour(mode);
    b.week_year = true;
    const yw = calcWeek(days, b);
    return yw.year * 100 + yw.week;
}

const Temporal = enum { date, datetime };

fn daysAt(v: ColumnView, i: usize, comptime temporal: Temporal) i32 {
    return switch (temporal) {
        .date => v.data.date[i],
        .datetime => common.daysFromDatetime(v.data.datetime[i]),
    };
}

/// WEEK(d[, mode]); the mode defaults to 0, MySQL's default_week_format.
pub fn weekKernel(comptime temporal: Temporal) Kernel {
    return struct {
        fn kernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
            for (0..row_count) |i| {
                const mode: i64 = if (args.len > 1) args[1].data.int[i] else 0;
                try out.data.int.append(allocator, week(daysAt(args[0], i, temporal), mode));
            }
        }
    }.kernel;
}

pub fn yearWeekKernel(comptime temporal: Temporal) Kernel {
    return struct {
        fn kernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
            for (0..row_count) |i| {
                const mode: i64 = if (args.len > 1) args[1].data.int[i] else 0;
                try out.data.int.append(allocator, yearWeek(daysAt(args[0], i, temporal), mode));
            }
        }
    }.kernel;
}

/// WEEKOFYEAR(d): the ISO week, WEEK(d, 3).
pub fn weekOfYearKernel(comptime temporal: Temporal) Kernel {
    return struct {
        fn kernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
            for (0..row_count) |i| try out.data.int.append(allocator, week(daysAt(args[0], i, temporal), 3));
        }
    }.kernel;
}

/// WEEKDAY(d): 0 = Monday ... 6 = Sunday.
pub fn weekdayKernel(comptime temporal: Temporal) Kernel {
    return struct {
        fn kernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
            for (0..row_count) |i| try out.data.int.append(allocator, weekdayOf(daysAt(args[0], i, temporal), false));
        }
    }.kernel;
}

/// DAYOFWEEK_ISO(d): 1 = Monday ... 7 = Sunday.
pub fn dayofweekIsoKernel(comptime temporal: Temporal) Kernel {
    return struct {
        fn kernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
            for (0..row_count) |i| try out.data.int.append(allocator, weekdayOf(daysAt(args[0], i, temporal), false) + 1);
        }
    }.kernel;
}

/// The weekday, 0 = Monday, that NEXT_DAY and PREVIOUS_DAY read from a
/// name: its first two letters, its first three, or all of it, in any case.
fn weekdayNamed(name: []const u8) ?i32 {
    for (0..7) |monday_based| {
        const full = day_names[(monday_based + 1) % 7];
        if (std.ascii.eqlIgnoreCase(name, full) or
            std.ascii.eqlIgnoreCase(name, full[0..2]) or
            std.ascii.eqlIgnoreCase(name, full[0..3])) return @intCast(monday_based);
    }
    return null;
}

/// NEXT_DAY(d, weekday) and PREVIOUS_DAY(d, weekday), as StarRocks: the
/// first DATE after (or before) d's day that falls on the named weekday, so
/// never d's day itself. Text is read as a DATETIME, so one whose time of
/// day isn't valid is NULL. A result outside years 0-9999 is NULL, and a
/// name that isn't a weekday fails the query.
pub fn stepToWeekdayKernel(comptime source: enum { date, datetime, text }, comptime direction: enum { next, previous }) Kernel {
    return struct {
        fn kernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
            const base = out.data.rowCount();
            const names = stringViewOf(args[1]);
            for (0..row_count) |i| {
                const target: ?i32 = if (args[1].isValid(i)) weekdayNamed(names.rowBytes(i)) orelse return error.InvalidWeekdayName else null;
                const start: ?i32 = if (target == null or !args[0].isValid(i)) null else switch (source) {
                    .date => args[0].data.date[i],
                    .datetime => common.daysFromDatetime(args[0].data.datetime[i]),
                    .text => if (common.textToDatetime(stringViewOf(args[0]).rowBytes(i))) |micros| common.daysFromDatetime(micros) else null,
                };
                const days: ?i32 = if (start) |from| blk: {
                    const weekday = weekdayOf(from, false);
                    const step: i64 = switch (direction) {
                        .next => @mod(target.? - weekday - 1, 7) + 1,
                        .previous => -(@mod(weekday - target.? - 1, 7) + 1),
                    };
                    const moved = @as(i64, from) + step;
                    break :blk if (moved < common.FIRST_DATE_DAYS or moved > common.LAST_DATE_DAYS) null else @intCast(moved);
                } else null;
                try out.data.date.append(allocator, days orelse 0);
                try out.appendValidBit(allocator, base + i, days != null);
            }
        }
    }.kernel;
}

/// TO_ISO8601(x): a DATE as `YYYY-MM-DD`, a DATETIME as
/// `YYYY-MM-DDThh:mm:ss.ffffff`, always with six fraction digits.
pub fn toIso8601Kernel(comptime temporal: Temporal) Kernel {
    return struct {
        fn kernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
            const ss = common.stringStoreOf(out);
            var day_buf: [10]u8 = undefined;
            var buf: [32]u8 = undefined;
            for (0..row_count) |i| {
                const ymd = common.daysToYmd(daysAt(args[0], i, temporal));
                const day = std.fmt.bufPrint(&day_buf, "{d:0>4}-{d:0>2}-{d:0>2}", .{ @as(u32, @intCast(ymd.year)), ymd.month, ymd.day }) catch unreachable;
                const text = switch (temporal) {
                    .date => day,
                    .datetime => blk: {
                        const micros = args[0].data.datetime[i];
                        const hms = common.microsToHms(micros);
                        const fraction: u32 = @intCast(@mod(micros, std.time.us_per_s));
                        break :blk std.fmt.bufPrint(&buf, "{s}T{d:0>2}:{d:0>2}:{d:0>2}.{d:0>6}", .{ day, hms.hour, hms.minute, hms.second, fraction }) catch unreachable;
                    },
                };
                try ss.appendValue(allocator, text);
            }
        }
    }.kernel;
}

pub fn microsecondKernel(comptime temporal: Temporal) Kernel {
    return struct {
        fn kernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
            for (0..row_count) |i| {
                const micro: i32 = switch (temporal) {
                    .date => 0,
                    .datetime => @intCast(@mod(args[0].data.datetime[i], std.time.us_per_s)),
                };
                try out.data.int.append(allocator, micro);
            }
        }
    }.kernel;
}

// ---------------------------------------------------------------------------
// DATE_FORMAT
// ---------------------------------------------------------------------------

fn appendPadded(buf: *std.ArrayList(u8), allocator: Allocator, value: i64, width: usize) !void {
    try buf.print(allocator, "{d:0>[1]}", .{ @as(u64, @intCast(@max(value, 0))), width });
}

fn dayOrdinalSuffix(day: u32) []const u8 {
    if (day / 10 == 1) return "th";
    return switch (day % 10) {
        1 => "st",
        2 => "nd",
        3 => "rd",
        else => "th",
    };
}

/// The MySQL format string DATE_FORMAT and FROM_UNIXTIME render `fmt` by.
/// StarRocks also takes three Java-style patterns, spelled exactly so, and
/// gives NULL for an empty format, so an empty format has none.
pub fn mysqlFormat(fmt: []const u8) ?[]const u8 {
    if (fmt.len == 0) return null;
    const java_patterns = [_]struct { java: []const u8, mysql: []const u8 }{
        .{ .java = "yyyy-MM-dd", .mysql = "%Y-%m-%d" },
        .{ .java = "yyyy-MM-dd HH:mm:ss", .mysql = "%Y-%m-%d %H:%i:%s" },
        .{ .java = "yyyyMMdd", .mysql = "%Y%m%d" },
    };
    for (java_patterns) |p| if (std.mem.eql(u8, fmt, p.java)) return p.mysql;
    return fmt;
}

/// Appends `days` + `micros_into_day` rendered under a MySQL format string.
/// An unknown specifier prints its letter, as MySQL does.
pub fn format(allocator: Allocator, buf: *std.ArrayList(u8), fmt: []const u8, days: i32, micros_into_day: i64) !void {
    const ymd = common.daysToYmd(days);
    const hms = common.microsToHms(micros_into_day);
    // DATE's range is years 0000-9999; arithmetic can step outside it.
    const year: i64 = std.math.clamp(ymd.year, 0, 9999);
    const hour12: i64 = if (hms.hour % 12 == 0) 12 else hms.hour % 12;
    const am_pm: []const u8 = if (hms.hour < 12) "AM" else "PM";
    const weekday: usize = @intCast(weekdayOf(days, true));
    const month_name = month_names[@as(usize, ymd.month) - 1];

    var i: usize = 0;
    while (i < fmt.len) : (i += 1) {
        if (fmt[i] != '%' or i + 1 == fmt.len) {
            try buf.append(allocator, fmt[i]);
            continue;
        }
        i += 1;
        switch (fmt[i]) {
            'a' => try buf.appendSlice(allocator, day_names[weekday][0..3]),
            'b' => try buf.appendSlice(allocator, month_name[0..3]),
            'c' => try appendPadded(buf, allocator, ymd.month, 1),
            'D' => {
                try appendPadded(buf, allocator, ymd.day, 1);
                try buf.appendSlice(allocator, dayOrdinalSuffix(ymd.day));
            },
            'd' => try appendPadded(buf, allocator, ymd.day, 2),
            'e' => try appendPadded(buf, allocator, ymd.day, 1),
            'f' => try appendPadded(buf, allocator, @mod(micros_into_day, std.time.us_per_s), 6),
            'H' => try appendPadded(buf, allocator, hms.hour, 2),
            'h', 'I' => try appendPadded(buf, allocator, hour12, 2),
            'i' => try appendPadded(buf, allocator, hms.minute, 2),
            'j' => try appendPadded(buf, allocator, days - common.ymdToDays(ymd.year, 1, 1) + 1, 3),
            'k' => try appendPadded(buf, allocator, hms.hour, 1),
            'l' => try appendPadded(buf, allocator, hour12, 1),
            'M' => try buf.appendSlice(allocator, month_name),
            'm' => try appendPadded(buf, allocator, ymd.month, 2),
            'p' => try buf.appendSlice(allocator, am_pm),
            'r' => try buf.print(allocator, "{d:0>2}:{d:0>2}:{d:0>2} {s}", .{ @as(u64, @intCast(hour12)), hms.minute, hms.second, am_pm }),
            'S', 's' => try appendPadded(buf, allocator, hms.second, 2),
            'T' => try buf.print(allocator, "{d:0>2}:{d:0>2}:{d:0>2}", .{ hms.hour, hms.minute, hms.second }),
            'U' => try appendPadded(buf, allocator, calcWeek(days, .{ .first_weekday = true }).week, 2),
            'u' => try appendPadded(buf, allocator, calcWeek(days, .{ .monday_first = true }).week, 2),
            'V' => try appendPadded(buf, allocator, calcWeek(days, .{ .week_year = true, .first_weekday = true }).week, 2),
            'v' => try appendPadded(buf, allocator, calcWeek(days, .{ .week_year = true, .monday_first = true }).week, 2),
            'W' => try buf.appendSlice(allocator, day_names[weekday]),
            'w' => try appendPadded(buf, allocator, @intCast(weekday), 1),
            'X' => try appendPadded(buf, allocator, std.math.clamp(calcWeek(days, .{ .week_year = true, .first_weekday = true }).year, 0, 9999), 4),
            'x' => try appendPadded(buf, allocator, std.math.clamp(calcWeek(days, .{ .week_year = true, .monday_first = true }).year, 0, 9999), 4),
            'Y' => try appendPadded(buf, allocator, year, 4),
            'y' => try appendPadded(buf, allocator, @mod(year, 100), 2),
            else => |c| try buf.append(allocator, c),
        }
    }
}

// ---------------------------------------------------------------------------
// STR_TO_DATE
// ---------------------------------------------------------------------------

/// Whether STR_TO_DATE under this format yields a time of day, so its
/// result is a DATETIME rather than a DATE (MySQL's
/// get_date_time_result_type).
pub fn formatHasTimePart(fmt: []const u8) bool {
    var i: usize = 0;
    while (i + 1 < fmt.len) : (i += 1) {
        if (fmt[i] != '%') continue;
        i += 1;
        if (std.mem.indexOfScalar(u8, "HISThiklrsf", fmt[i]) != null) return true;
    }
    return false;
}

/// Whether a format names a year, month, week or weekday. A format with a
/// time of day and none of these makes STR_TO_DATE a TIME, with any day of
/// the month (`%d`, `%e`, `%D`) counted in hours, as MySQL does.
pub fn formatHasDatePart(fmt: []const u8) bool {
    var i: usize = 0;
    while (i + 1 < fmt.len) : (i += 1) {
        if (fmt[i] != '%') continue;
        i += 1;
        if (std.mem.indexOfScalar(u8, "yYmcMbjuUvVxXwWa", fmt[i]) != null) return true;
    }
    return false;
}

const ParseState = struct {
    year: i32 = 0,
    month: i32 = 0,
    day: i32 = 0,
    hour: i32 = 0,
    minute: i32 = 0,
    second: i32 = 0,
    micro: i32 = 0,
    twelve_hour: bool = false,
    pm_offset: i32 = 0,
    yearday: i32 = 0,
    /// 1 = Monday ... 7 = Sunday; 0 when the text names no weekday.
    weekday: i32 = 0,
    week_number: i32 = -1,
    sunday_first_week: bool = false,
    strict_week: bool = false,
    strict_week_year: i32 = -1,
    strict_week_year_sunday: bool = false,
};

fn isSpace(c: u8) bool {
    return c == ' ' or c == '\t' or c == '\n' or c == '\r' or c == 0x0b or c == 0x0c;
}

fn isPunctuation(c: u8) bool {
    return std.ascii.isPrint(c) and !std.ascii.isAlphanumeric(c) and c != ' ';
}

/// Up to `max_digits` decimal digits at `pos`; null when there are none.
fn readNumber(text: []const u8, pos: *usize, max_digits: usize) ?struct { value: i32, digits: usize } {
    const start = pos.*;
    var value: i32 = 0;
    while (pos.* < text.len and pos.* - start < max_digits and std.ascii.isDigit(text[pos.*])) : (pos.* += 1) {
        value = value * 10 + (text[pos.*] - '0');
    }
    const digits = pos.* - start;
    return if (digits == 0) null else .{ .value = value, .digits = digits };
}

/// The 1-based index of the name the alphabetic word at `pos` spells, in
/// full or as an unambiguous prefix, ignoring case; `offset` shifts the
/// names so that index 1 lands where MySQL's lists start.
fn readName(text: []const u8, pos: *usize, names: []const []const u8, abbreviated: bool) ?usize {
    const start = pos.*;
    while (pos.* < text.len and std.ascii.isAlphabetic(text[pos.*])) pos.* += 1;
    const word = text[start..pos.*];
    if (word.len == 0) return null;
    var found: ?usize = null;
    for (names, 0..) |full, idx| {
        const name = if (abbreviated) full[0..3] else full;
        if (word.len > name.len or !std.ascii.eqlIgnoreCase(word, name[0..word.len])) continue;
        if (word.len == name.len) return idx;
        if (found != null) return null;
        found = idx;
    }
    return found;
}

/// Walks `fmt` against `text` from `pos`, as MySQL's extract_date_time does:
/// whitespace in the text before every item is skipped, whitespace in the
/// format is ignored, other format characters must match, and the walk ends
/// quietly when either runs out.
fn scan(state: *ParseState, text: []const u8, pos: *usize, fmt: []const u8) bool {
    var f: usize = 0;
    while (f < fmt.len and pos.* < text.len) : (f += 1) {
        while (pos.* < text.len and isSpace(text[pos.*])) pos.* += 1;
        if (pos.* == text.len) break;
        if (fmt[f] != '%' or f + 1 == fmt.len) {
            if (isSpace(fmt[f])) continue;
            if (text[pos.*] != fmt[f]) return false;
            pos.* += 1;
            continue;
        }
        f += 1;
        switch (fmt[f]) {
            'Y' => {
                const n = readNumber(text, pos, 4) orelse return false;
                state.year = if (n.digits <= 2) twoDigitYear(n.value) else n.value;
            },
            'y' => state.year = twoDigitYear((readNumber(text, pos, 2) orelse return false).value),
            'm', 'c' => state.month = (readNumber(text, pos, 2) orelse return false).value,
            'M' => state.month = @intCast(1 + (readName(text, pos, &month_names, false) orelse return false)),
            'b' => state.month = @intCast(1 + (readName(text, pos, &month_names, true) orelse return false)),
            'd', 'e' => state.day = (readNumber(text, pos, 2) orelse return false).value,
            'D' => {
                state.day = (readNumber(text, pos, 2) orelse return false).value;
                pos.* = @min(pos.* + 2, text.len);
            },
            'h', 'I', 'l' => {
                state.twelve_hour = true;
                state.hour = (readNumber(text, pos, 2) orelse return false).value;
            },
            'H', 'k' => state.hour = (readNumber(text, pos, 2) orelse return false).value,
            'i' => state.minute = (readNumber(text, pos, 2) orelse return false).value,
            's', 'S' => state.second = (readNumber(text, pos, 2) orelse return false).value,
            'f' => {
                const n = readNumber(text, pos, 6) orelse return false;
                state.micro = n.value * std.math.pow(i32, 10, @intCast(6 - n.digits));
            },
            'p' => {
                if (text.len - pos.* < 2 or !state.twelve_hour) return false;
                const word = text[pos.*..][0..2];
                if (std.ascii.eqlIgnoreCase(word, "PM")) {
                    state.pm_offset = 12;
                } else if (!std.ascii.eqlIgnoreCase(word, "AM")) return false;
                pos.* += 2;
            },
            'W' => state.weekday = mondayBased(readName(text, pos, &day_names, false) orelse return false),
            'a' => state.weekday = mondayBased(readName(text, pos, &day_names, true) orelse return false),
            'w' => {
                const n = readNumber(text, pos, 1) orelse return false;
                if (n.value >= 7) return false;
                state.weekday = if (n.value == 0) 7 else n.value;
            },
            'j' => state.yearday = (readNumber(text, pos, 3) orelse return false).value,
            'U', 'u', 'V', 'v' => {
                state.sunday_first_week = fmt[f] == 'U' or fmt[f] == 'V';
                state.strict_week = fmt[f] == 'V' or fmt[f] == 'v';
                const n = readNumber(text, pos, 2) orelse return false;
                if ((state.strict_week and n.value == 0) or n.value > 53) return false;
                state.week_number = n.value;
            },
            'X', 'x' => {
                state.strict_week_year_sunday = fmt[f] == 'X';
                state.strict_week_year = (readNumber(text, pos, 4) orelse return false).value;
            },
            'r' => if (!scan(state, text, pos, "%I:%i:%S %p")) return false,
            'T' => if (!scan(state, text, pos, "%H:%i:%S")) return false,
            '.' => {
                while (pos.* < text.len and isPunctuation(text[pos.*])) pos.* += 1;
            },
            '@' => {
                while (pos.* < text.len and std.ascii.isAlphabetic(text[pos.*])) pos.* += 1;
            },
            '#' => {
                while (pos.* < text.len and std.ascii.isDigit(text[pos.*])) pos.* += 1;
            },
            '%' => {
                if (text[pos.*] != '%') return false;
                pos.* += 1;
            },
            else => return false,
        }
    }
    return true;
}

fn twoDigitYear(value: i32) i32 {
    return value + @as(i32, if (value < 70) 2000 else 1900);
}

/// Sunday-first name index to MySQL's 1 = Monday ... 7 = Sunday.
fn mondayBased(sunday_first_index: usize) i32 {
    return if (sunday_first_index == 0) 7 else @intCast(sunday_first_index);
}

/// The DATETIME that `text` spells under `fmt`, or null where it does not
/// match or names no real date (a zero month or day, February 30, ...),
/// which MySQL answers with NULL too. Text left over after the format ends
/// is ignored, as MySQL ignores it with a warning.
pub fn parse(text: []const u8, fmt: []const u8) ?i64 {
    var state = scanClock(text, fmt) orelse return null;
    var days: ?i32 = null;
    if (state.yearday > 0) days = common.ymdToDays(state.year, 1, 1) + state.yearday - 1;
    if (state.week_number >= 0 and state.weekday != 0) {
        if (state.strict_week) {
            if (state.strict_week_year < 0 or state.strict_week_year_sunday != state.sunday_first_week) return null;
        } else if (state.strict_week_year >= 0) return null;
        const year = if (state.strict_week) state.strict_week_year else state.year;
        const first_day = common.ymdToDays(year, 1, 1);
        const first_weekday = weekdayOf(first_day, state.sunday_first_week);
        const week_start = (state.week_number - 1) * 7;
        days = if (state.sunday_first_week)
            first_day + (if (first_weekday == 0) @as(i32, 0) else 7) - first_weekday + week_start + @mod(state.weekday, 7)
        else
            first_day + (if (first_weekday <= 3) @as(i32, 0) else 7) - first_weekday + week_start + state.weekday - 1;
    }
    if (days) |d| {
        const ymd = common.daysToYmd(d);
        state.year = ymd.year;
        state.month = ymd.month;
        state.day = ymd.day;
    }

    if (state.year < 0 or state.year > 9999 or state.month < 1 or state.month > 12 or state.day < 1) return null;
    if (state.day > common.lastDayOfMonth(state.year, @intCast(state.month))) return null;
    const date = common.ymdToDays(state.year, @intCast(state.month), @intCast(state.day));
    const seconds = (@as(i64, state.hour) * 60 + state.minute) * 60 + state.second;
    return @as(i64, date) * std.time.us_per_day + seconds * std.time.us_per_s + state.micro;
}

/// `text` walked under `fmt` with its time of day checked: hours 0-23
/// (1-12 before a 12-hour clock's AM/PM applies), minutes and seconds 0-59.
fn scanClock(text: []const u8, fmt: []const u8) ?ParseState {
    var state: ParseState = .{};
    var pos: usize = 0;
    if (!scan(&state, text, &pos, fmt)) return null;
    if (state.twelve_hour) {
        if (state.hour < 1 or state.hour > 12) return null;
        state.hour = @mod(state.hour, 12) + state.pm_offset;
    }
    if (state.hour > 23 or state.minute > 59 or state.second > 59) return null;
    return state;
}

/// The TIME, in microseconds, that `text` spells under a format with no
/// date part (`formatHasDatePart`): its day of the month counts 24 hours
/// each. MySQL leaves such a sum past 838:59:59 unclamped, and so does this.
pub fn parseClock(text: []const u8, fmt: []const u8) ?i64 {
    const state = scanClock(text, fmt) orelse return null;
    const hours = @as(i64, state.day) * 24 + state.hour;
    const seconds = (hours * 60 + state.minute) * 60 + state.second;
    return seconds * std.time.us_per_s + state.micro;
}

/// STR_TO_DATE(text, format) under a constant format with no date part: a
/// TIME's text, with six fraction digits when the format reads them (`%f`).
pub fn strToTimeKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const base = out.data.rowCount();
    const text = stringViewOf(args[0]);
    const fmt = stringViewOf(args[1]);
    const ss = common.stringStoreOf(out);
    var buf: [48]u8 = undefined;
    for (0..row_count) |i| {
        const micros = if (args[0].isValid(i) and args[1].isValid(i)) parseClock(text.rowBytes(i), fmt.rowBytes(i)) else null;
        const fsp: u8 = if (std.mem.indexOf(u8, fmt.rowBytes(i), "%f") != null) 6 else 0;
        try ss.appendValue(allocator, if (micros) |m| try time.formatTime(&buf, m, fsp) else "");
        try out.appendValidBit(allocator, base + i, micros != null);
    }
}

/// GET_FORMAT(kind, standard): the format string MySQL names for a DATE,
/// TIME or DATETIME (TIMESTAMP) in the USA, JIS, ISO, EUR or INTERNAL
/// standard; null for any other pair.
pub fn getFormat(kind: []const u8, standard: []const u8) ?[]const u8 {
    const kinds = [_][]const u8{ "date", "time", "datetime" };
    const table = [_]struct { []const u8, [3][]const u8 }{
        .{ "usa", .{ "%m.%d.%Y", "%h:%i:%s %p", "%Y-%m-%d %H.%i.%s" } },
        .{ "jis", .{ "%Y-%m-%d", "%H:%i:%s", "%Y-%m-%d %H:%i:%s" } },
        .{ "iso", .{ "%Y-%m-%d", "%H:%i:%s", "%Y-%m-%d %H:%i:%s" } },
        .{ "eur", .{ "%d.%m.%Y", "%H.%i.%s", "%Y-%m-%d %H.%i.%s" } },
        .{ "internal", .{ "%Y%m%d", "%H%i%s", "%Y%m%d%H%i%s" } },
    };
    const k = for (kinds, 0..) |name, idx| {
        if (std.ascii.eqlIgnoreCase(kind, name)) break idx;
    } else if (std.ascii.eqlIgnoreCase(kind, "timestamp")) 2 else return null;
    for (table) |row| if (std.ascii.eqlIgnoreCase(standard, row[0])) return row[1][k];
    return null;
}

pub fn getFormatKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const base = out.data.rowCount();
    const kind = stringViewOf(args[0]);
    const standard = stringViewOf(args[1]);
    const ss = common.stringStoreOf(out);
    for (0..row_count) |i| {
        const f = if (args[0].isValid(i) and args[1].isValid(i)) getFormat(kind.rowBytes(i), standard.rowBytes(i)) else null;
        try ss.appendValue(allocator, f orelse "");
        try out.appendValidBit(allocator, base + i, f != null);
    }
}

/// STR_TO_DATE(text, format) as a DATETIME; the parser narrows it to a DATE
/// when a constant format names no time of day.
pub fn strToDateKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const base = out.data.rowCount();
    const text = stringViewOf(args[0]);
    const fmt = stringViewOf(args[1]);
    for (0..row_count) |i| {
        const micros = if (args[0].isValid(i) and args[1].isValid(i)) parse(text.rowBytes(i), fmt.rowBytes(i)) else null;
        try out.data.datetime.append(allocator, micros orelse 0);
        try out.appendValidBit(allocator, base + i, micros != null);
    }
}

// ---------------------------------------------------------------------------
// Joda-Time patterns: JODATIME_FORMAT, STR_TO_JODATIME
// ---------------------------------------------------------------------------

/// The letters a Joda-Time pattern gives meaning to. Any other letter is
/// literal text, as StarRocks prints it.
const JODA_LETTERS = "GCYxweEyDMdaKhHkmsSzZ";

/// A run of one pattern letter, or, where `letter` is 0, literal text:
/// `JodaPattern.literals[start..][0..count]`.
const JodaToken = struct { letter: u8, count: u32, start: u32 = 0 };

/// A Joda pattern split into tokens. A kernel keeps one and compiles again
/// only when a row's pattern differs from the one before.
const JodaPattern = struct {
    source: std.ArrayList(u8) = .empty,
    tokens: std.ArrayList(JodaToken) = .empty,
    literals: std.ArrayList(u8) = .empty,
    compiled: bool = false,

    fn deinit(p: *JodaPattern, allocator: Allocator) void {
        p.source.deinit(allocator);
        p.tokens.deinit(allocator);
        p.literals.deinit(allocator);
    }

    /// Text in single quotes is literal, and `''` is one quote, in quotes or
    /// out. An unclosed quote runs to the end, as in StarRocks.
    fn compile(p: *JodaPattern, allocator: Allocator, pattern: []const u8) !void {
        if (p.compiled and std.mem.eql(u8, p.source.items, pattern)) return;
        p.compiled = false;
        p.source.clearRetainingCapacity();
        try p.source.appendSlice(allocator, pattern);
        p.tokens.clearRetainingCapacity();
        p.literals.clearRetainingCapacity();
        var i: usize = 0;
        while (i < pattern.len) {
            const c = pattern[i];
            if (c == '\'') {
                i += 1;
                if (i < pattern.len and pattern[i] == '\'') {
                    try p.appendLiteral(allocator, "'");
                    i += 1;
                    continue;
                }
                while (i < pattern.len) {
                    if (pattern[i] != '\'') {
                        try p.appendLiteral(allocator, pattern[i .. i + 1]);
                        i += 1;
                    } else if (i + 1 < pattern.len and pattern[i + 1] == '\'') {
                        try p.appendLiteral(allocator, "'");
                        i += 2;
                    } else {
                        i += 1;
                        break;
                    }
                }
                continue;
            }
            if (std.mem.indexOfScalar(u8, JODA_LETTERS, c) == null) {
                try p.appendLiteral(allocator, pattern[i .. i + 1]);
                i += 1;
                continue;
            }
            var end = i + 1;
            while (end < pattern.len and pattern[end] == c) end += 1;
            try p.tokens.append(allocator, .{ .letter = c, .count = @intCast(end - i) });
            i = end;
        }
        p.compiled = true;
    }

    /// Literal text joins the literal token before it, whose bytes end the
    /// literals so far.
    fn appendLiteral(p: *JodaPattern, allocator: Allocator, text: []const u8) !void {
        const tokens = p.tokens.items;
        if (tokens.len > 0 and tokens[tokens.len - 1].letter == 0) {
            tokens[tokens.len - 1].count += @intCast(text.len);
        } else {
            try p.tokens.append(allocator, .{ .letter = 0, .count = @intCast(text.len), .start = @intCast(p.literals.items.len) });
        }
        try p.literals.appendSlice(allocator, text);
    }

    fn literal(p: *const JodaPattern, t: JodaToken) []const u8 {
        return p.literals.items[t.start..][0..t.count];
    }
};

/// A name in full for four or more pattern letters, else its first three.
fn jodaName(name: []const u8, count: usize) []const u8 {
    return if (count >= 4) name else name[0..3];
}

/// A year under `yy` is its last two digits; under any other count it is
/// whole, padded to the count.
fn appendJodaYear(buf: *std.ArrayList(u8), allocator: Allocator, year: i32, count: usize) !void {
    if (count == 2) return appendPadded(buf, allocator, @mod(year, 100), 2);
    try appendPadded(buf, allocator, year, count);
}

/// Appends `days` + `micros_into_day` rendered under a Joda pattern. A
/// number is padded to its letter count. A DATETIME carries no zone, so a
/// zone renders as UTC.
fn formatJoda(allocator: Allocator, buf: *std.ArrayList(u8), p: *const JodaPattern, days: i32, micros_into_day: i64) !void {
    const ymd = common.daysToYmd(days);
    const hms = common.microsToHms(micros_into_day);
    const iso = calcWeek(days, .{ .monday_first = true, .week_year = true });
    for (p.tokens.items) |t| {
        const n: usize = t.count;
        switch (t.letter) {
            0 => try buf.appendSlice(allocator, p.literal(t)),
            'G' => try buf.appendSlice(allocator, if (ymd.year > 0) "AD" else "BC"),
            'C' => try appendPadded(buf, allocator, @divFloor(ymd.year, 100), n),
            'Y', 'y' => try appendJodaYear(buf, allocator, ymd.year, n),
            'x' => try appendJodaYear(buf, allocator, iso.year, n),
            'w' => try appendPadded(buf, allocator, iso.week, n),
            'e' => try appendPadded(buf, allocator, weekdayOf(days, false) + 1, n),
            'E' => try buf.appendSlice(allocator, jodaName(day_names[@intCast(weekdayOf(days, true))], n)),
            'D' => try appendPadded(buf, allocator, days - common.ymdToDays(ymd.year, 1, 1) + 1, n),
            'M' => if (n >= 3)
                try buf.appendSlice(allocator, jodaName(month_names[@as(usize, ymd.month) - 1], n))
            else
                try appendPadded(buf, allocator, ymd.month, n),
            'd' => try appendPadded(buf, allocator, ymd.day, n),
            'a' => try buf.appendSlice(allocator, if (hms.hour < 12) "AM" else "PM"),
            'K' => try appendPadded(buf, allocator, hms.hour % 12, n),
            'h' => try appendPadded(buf, allocator, if (hms.hour % 12 == 0) 12 else hms.hour % 12, n),
            'H' => try appendPadded(buf, allocator, hms.hour, n),
            'k' => try appendPadded(buf, allocator, if (hms.hour == 0) 24 else hms.hour, n),
            'm' => try appendPadded(buf, allocator, hms.minute, n),
            's' => try appendPadded(buf, allocator, hms.second, n),
            'S' => {
                var digits: [6]u8 = undefined;
                _ = std.fmt.bufPrint(&digits, "{d:0>6}", .{@as(u32, @intCast(@mod(micros_into_day, std.time.us_per_s)))}) catch unreachable;
                try buf.appendSlice(allocator, digits[0..@min(n, 6)]);
                if (n > 6) try buf.appendNTimes(allocator, '0', n - 6);
            },
            'z' => try buf.appendSlice(allocator, "UTC"),
            'Z' => try buf.appendSlice(allocator, switch (n) {
                1 => "+0000",
                2 => "+00:00",
                else => "UTC",
            }),
            // `compile` makes tokens only of literal text and JODA_LETTERS.
            else => unreachable,
        }
    }
}

fn isJodaNumber(t: JodaToken) bool {
    return switch (t.letter) {
        0, 'G', 'E', 'a', 'z', 'Z' => false,
        'M' => t.count < 3,
        else => true,
    };
}

/// The DATETIME `text` spells under a Joda pattern, or null where the text
/// doesn't match all of the pattern or names no real time. A number
/// followed straight by another reads exactly its letter count of digits,
/// as in Joda (`yyyyMMdd`); any other reads as many as its field can hold.
/// Fields the pattern doesn't name are those of 1970-01-01 00:00:00. A
/// two-digit `yy` year below 70 is in the 2000s, as in MySQL. A zone or a
/// century can't be read, so they give NULL.
fn parseJoda(p: *const JodaPattern, text: []const u8) ?i64 {
    var year: i32 = 1970;
    var month: i32 = 1;
    var day: i32 = 1;
    var day_of_year: ?i32 = null;
    var week_year: ?i32 = null;
    var week_number: ?i32 = null;
    var weekday: i32 = 1;
    var hour: i32 = 0;
    var half_day_hour = false;
    var pm = false;
    var minute: i32 = 0;
    var second: i32 = 0;
    var micro: i32 = 0;
    var pos: usize = 0;
    const tokens = p.tokens.items;
    for (tokens, 0..) |t, ti| {
        switch (t.letter) {
            0 => {
                const lit = p.literal(t);
                if (text.len - pos < lit.len or !std.ascii.eqlIgnoreCase(text[pos..][0..lit.len], lit)) return null;
                pos += lit.len;
                continue;
            },
            'E' => {
                _ = readName(text, &pos, &day_names, t.count < 4) orelse return null;
                continue;
            },
            'a', 'G' => {
                const options: [2][]const u8 = if (t.letter == 'a') .{ "AM", "PM" } else .{ "AD", "BC" };
                if (text.len - pos < 2) return null;
                const word = text[pos..][0..2];
                if (std.ascii.eqlIgnoreCase(word, options[1])) {
                    if (t.letter == 'a') pm = true else return null;
                } else if (!std.ascii.eqlIgnoreCase(word, options[0])) return null;
                pos += 2;
                continue;
            },
            'M' => if (t.count >= 3) {
                month = @intCast((readName(text, &pos, &month_names, t.count == 3) orelse return null) + 1);
                continue;
            },
            'C', 'z', 'Z' => return null,
            else => {},
        }
        const followed_by_number = ti + 1 < tokens.len and isJodaNumber(tokens[ti + 1]);
        const max_digits: usize = if (followed_by_number) t.count else switch (t.letter) {
            'y', 'Y', 'x', 'S' => 9,
            'D' => 3,
            'e' => 1,
            else => 2,
        };
        const number = readNumber(text, &pos, max_digits) orelse return null;
        const v = number.value;
        switch (t.letter) {
            'y', 'Y' => year = if (t.count == 2 and number.digits == 2) twoDigitYear(v) else v,
            'x' => week_year = if (t.count == 2 and number.digits == 2) twoDigitYear(v) else v,
            'w' => week_number = v,
            'e' => weekday = v,
            'D' => day_of_year = v,
            'M' => month = v,
            'd' => day = v,
            'H' => hour = v,
            'k' => hour = if (v == 24) 0 else if (v >= 1) v else return null,
            'K' => {
                if (v > 11) return null;
                hour = v;
                half_day_hour = true;
            },
            'h' => {
                if (v < 1 or v > 12) return null;
                hour = @mod(v, 12);
                half_day_hour = true;
            },
            'm' => minute = v,
            's' => second = v,
            'S' => micro = if (number.digits <= 6)
                v * std.math.pow(i32, 10, @intCast(6 - number.digits))
            else
                @divTrunc(v, std.math.pow(i32, 10, @intCast(number.digits - 6))),
            else => unreachable,
        }
    }
    if (pos != text.len) return null;
    if (half_day_hour and pm) hour += 12;

    var days: i32 = undefined;
    if (week_year != null or week_number != null) {
        const w = week_number orelse 1;
        if (w < 1 or w > 53 or weekday < 1 or weekday > 7) return null;
        const wy = week_year orelse year;
        if (wy < 0 or wy > 9999) return null;
        const jan4 = common.ymdToDays(wy, 1, 4);
        days = jan4 - weekdayOf(jan4, false) + (w - 1) * 7 + weekday - 1;
        if (calcWeek(days, .{ .monday_first = true, .week_year = true }).year != wy) return null;
    } else {
        if (year < 0 or year > 9999) return null;
        if (day_of_year) |doy| {
            if (doy < 1 or doy > daysInYear(year)) return null;
            days = common.ymdToDays(year, 1, 1) + doy - 1;
        } else {
            if (month < 1 or month > 12 or day < 1) return null;
            if (day > common.lastDayOfMonth(year, @intCast(month))) return null;
            days = common.ymdToDays(year, @intCast(month), @intCast(day));
        }
    }
    if (hour > 23 or minute > 59 or second > 59) return null;
    const seconds = (@as(i64, hour) * 60 + minute) * 60 + second;
    return @as(i64, days) * std.time.us_per_day + seconds * std.time.us_per_s + micro;
}

/// JODATIME_FORMAT(x, pattern), and Trino's FORMAT_DATETIME: x rendered
/// under a Joda-Time pattern (`formatJoda`).
pub fn jodaFormatKernel(comptime temporal: Temporal) Kernel {
    return struct {
        fn kernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
            const base = out.data.rowCount();
            const patterns = stringViewOf(args[1]);
            const ss = common.stringStoreOf(out);
            var pattern: JodaPattern = .{};
            defer pattern.deinit(allocator);
            var buf: std.ArrayList(u8) = .empty;
            defer buf.deinit(allocator);
            for (0..row_count) |i| {
                const valid = args[0].isValid(i) and args[1].isValid(i);
                buf.clearRetainingCapacity();
                if (valid) {
                    try pattern.compile(allocator, patterns.rowBytes(i));
                    const micros: i64 = switch (temporal) {
                        .date => 0,
                        .datetime => @mod(args[0].data.datetime[i], std.time.us_per_day),
                    };
                    try formatJoda(allocator, &buf, &pattern, daysAt(args[0], i, temporal), micros);
                }
                try ss.appendValue(allocator, buf.items);
                try out.appendValidBit(allocator, base + i, valid);
            }
        }
    }.kernel;
}

/// STR_TO_JODATIME(text, pattern), and Trino's PARSE_DATETIME: the DATETIME
/// `text` spells under a Joda-Time pattern (`parseJoda`).
pub fn strToJodaKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const base = out.data.rowCount();
    const texts = stringViewOf(args[0]);
    const patterns = stringViewOf(args[1]);
    var pattern: JodaPattern = .{};
    defer pattern.deinit(allocator);
    for (0..row_count) |i| {
        var micros: ?i64 = null;
        if (args[0].isValid(i) and args[1].isValid(i)) {
            try pattern.compile(allocator, patterns.rowBytes(i));
            micros = parseJoda(&pattern, texts.rowBytes(i));
        }
        try out.data.datetime.append(allocator, micros orelse 0);
        try out.appendValidBit(allocator, base + i, micros != null);
    }
}

// ---------------------------------------------------------------------------
// Teradata formats: TO_TERA_DATE, TO_TERA_TIMESTAMP
// ---------------------------------------------------------------------------

const TeraElement = enum { year4, year2, month, day, hour24, hour, minute, second, am, pm };

const TeraEntry = struct { text: []const u8, element: TeraElement };

/// A Teradata format's elements, each before any shorter one it starts with.
const TERA_ELEMENTS = [_]TeraEntry{
    .{ .text = "yyyy", .element = .year4 },
    .{ .text = "yy", .element = .year2 },
    .{ .text = "mm", .element = .month },
    .{ .text = "dd", .element = .day },
    .{ .text = "hh24", .element = .hour24 },
    .{ .text = "hh", .element = .hour },
    .{ .text = "mi", .element = .minute },
    .{ .text = "ss", .element = .second },
    .{ .text = "am", .element = .am },
    .{ .text = "pm", .element = .pm },
};

const TERA_SEPARATORS = "-/,.;: ";

/// The element `fmt` spells at `pos`, or null for a separator.
fn teraElementAt(fmt: []const u8, pos: usize) error{InvalidTeradataFormat}!?TeraEntry {
    if (std.mem.indexOfScalar(u8, TERA_SEPARATORS, fmt[pos]) != null) return null;
    for (TERA_ELEMENTS) |e| if (std.mem.startsWith(u8, fmt[pos..], e.text)) return e;
    return error.InvalidTeradataFormat;
}

/// The DATETIME `text` spells under a Teradata format, or null where it
/// doesn't. Elements are lowercase. A number reads up to its element's
/// width of digits, at least one, and a separator must match itself. `am`
/// or `pm` must match the text, in any case, and makes `hh` a 12-hour
/// hour; text past the format's end is ignored. Fields the format doesn't
/// name are those of 1970-01-01 00:00:00, and `yy` reads as in MySQL.
pub fn parseTeradata(text: []const u8, fmt: []const u8) error{InvalidTeradataFormat}!?i64 {
    var f: usize = 0;
    while (f < fmt.len) {
        const e = try teraElementAt(fmt, f) orelse {
            f += 1;
            continue;
        };
        f += e.text.len;
    }

    var year: i32 = 1970;
    var month: i32 = 1;
    var day: i32 = 1;
    var hour: i32 = 0;
    var half_day: ?bool = null;
    var minute: i32 = 0;
    var second: i32 = 0;
    var pos: usize = 0;
    f = 0;
    while (f < fmt.len) {
        const e = try teraElementAt(fmt, f) orelse {
            if (pos >= text.len or text[pos] != fmt[f]) return null;
            pos += 1;
            f += 1;
            continue;
        };
        f += e.text.len;
        switch (e.element) {
            .am, .pm => {
                if (text.len - pos < 2 or !std.ascii.eqlIgnoreCase(text[pos..][0..2], e.text)) return null;
                pos += 2;
                half_day = e.element == .pm;
                continue;
            },
            else => {},
        }
        const width: usize = if (e.element == .year4) 4 else 2;
        const v = (readNumber(text, &pos, width) orelse return null).value;
        switch (e.element) {
            .year4 => year = v,
            .year2 => year = twoDigitYear(v),
            .month => month = v,
            .day => day = v,
            .hour24, .hour => hour = v,
            .minute => minute = v,
            .second => second = v,
            .am, .pm => unreachable,
        }
    }
    if (half_day) |pm| {
        if (hour < 1 or hour > 12) return null;
        hour = @mod(hour, 12) + @as(i32, if (pm) 12 else 0);
    }
    if (month < 1 or month > 12 or day < 1 or day > common.lastDayOfMonth(year, @intCast(month))) return null;
    if (hour > 23 or minute > 59 or second > 59) return null;
    const seconds = (@as(i64, hour) * 60 + minute) * 60 + second;
    return @as(i64, common.ymdToDays(year, @intCast(month), @intCast(day))) * std.time.us_per_day + seconds * std.time.us_per_s;
}

/// TO_TERA_DATE(text, format) and TO_TERA_TIMESTAMP(text, format)
/// (`parseTeradata`).
pub fn teradataKernel(comptime temporal: Temporal) Kernel {
    return struct {
        fn kernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
            const base = out.data.rowCount();
            const texts = stringViewOf(args[0]);
            const formats = stringViewOf(args[1]);
            for (0..row_count) |i| {
                const micros = if (args[0].isValid(i) and args[1].isValid(i)) try parseTeradata(texts.rowBytes(i), formats.rowBytes(i)) else null;
                switch (temporal) {
                    .date => try out.data.date.append(allocator, if (micros) |m| common.daysFromDatetime(m) else 0),
                    .datetime => try out.data.datetime.append(allocator, micros orelse 0),
                }
                try out.appendValidBit(allocator, base + i, micros != null);
            }
        }
    }.kernel;
}

test "week numbering matches MySQL's modes across year boundaries" {
    // MySQL 8 WEEK(d, 0..7) and YEARWEEK(d, 0..7).
    const cases = .{
        .{ 2024, 1, 1, [8]i32{ 0, 1, 53, 1, 1, 1, 1, 1 }, [8]i32{ 202353, 202401, 202353, 202401, 202401, 202401, 202401, 202401 } },
        .{ 2021, 1, 3, [8]i32{ 1, 0, 1, 53, 1, 0, 1, 52 }, [8]i32{ 202101, 202053, 202101, 202053, 202101, 202052, 202101, 202052 } },
        .{ 2020, 12, 31, [8]i32{ 52, 53, 52, 53, 53, 52, 53, 52 }, [8]i32{ 202052, 202053, 202052, 202053, 202053, 202052, 202053, 202052 } },
        .{ 1965, 3, 5, [8]i32{ 9, 9, 9, 9, 9, 9, 9, 9 }, [8]i32{ 196509, 196509, 196509, 196509, 196509, 196509, 196509, 196509 } },
    };
    inline for (cases) |c| {
        const days = common.ymdToDays(c[0], c[1], c[2]);
        for (0..8) |mode| {
            std.testing.expectEqual(c[3][mode], week(days, @intCast(mode))) catch |err| {
                std.debug.print("WEEK({d}-{d}-{d}, {d})\n", .{ c[0], c[1], c[2], mode });
                return err;
            };
            std.testing.expectEqual(c[4][mode], yearWeek(days, @intCast(mode))) catch |err| {
                std.debug.print("YEARWEEK({d}-{d}-{d}, {d})\n", .{ c[0], c[1], c[2], mode });
                return err;
            };
        }
    }
}
