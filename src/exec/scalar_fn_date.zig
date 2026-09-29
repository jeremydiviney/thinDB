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

const Temporal = enum { date, datetime };

/// DATE_ADD over a DATE or DATETIME by `n` days, weeks, months, quarters or
/// years (DATE_SUB negates `n`), keeping the time of day; a month step clamps
/// the day to the destination month (`2024-01-31 + 1 month → 2024-02-29`).
/// A result outside years 0-9999 is NULL, as in StarRocks.
fn AddUnit(comptime temporal: Temporal, comptime unit: DateUnit, comptime negate: bool) type {
    return struct {
        fn kernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
            const base = out.data.rowCount();
            const values = @field(args[0].data, @tagName(temporal));
            const ns = args[1].data.int;
            for (0..row_count) |i| {
                // Saturating: a count of days or more past INT's range moves
                // any date outside years 0-9999 either way.
                const n = if (negate) 0 -| ns[i] else ns[i];
                const moved = if (args[0].isValid(i) and args[1].isValid(i)) switch (temporal) {
                    .date => addUnitToDate(unit, values[i], n),
                    .datetime => addUnitToDatetime(unit, values[i], n),
                } else null;
                try @field(out.data, @tagName(temporal)).append(allocator, moved orelse 0);
                try out.appendValidBit(allocator, base + i, moved != null);
            }
        }
    };
}

pub const dateAddDaysKernel = AddUnit(.date, .day, false).kernel;
pub const dateSubDaysKernel = AddUnit(.date, .day, true).kernel;
pub const dateAddWeeksKernel = AddUnit(.date, .week, false).kernel;
pub const dateAddMonthsKernel = AddUnit(.date, .month, false).kernel;
pub const dateAddQuartersKernel = AddUnit(.date, .quarter, false).kernel;
pub const dateAddYearsKernel = AddUnit(.date, .year, false).kernel;
pub const datetimeAddDaysKernel = AddUnit(.datetime, .day, false).kernel;
pub const datetimeSubDaysKernel = AddUnit(.datetime, .day, true).kernel;
pub const datetimeAddWeeksKernel = AddUnit(.datetime, .week, false).kernel;
pub const datetimeAddMonthsKernel = AddUnit(.datetime, .month, false).kernel;
pub const datetimeAddQuartersKernel = AddUnit(.datetime, .quarter, false).kernel;
pub const datetimeAddYearsKernel = AddUnit(.datetime, .year, false).kernel;

/// A DATETIME moved by a count of `step_micros`-long steps. The count is an
/// INT, as in StarRocks: one past INT's range is NULL, and so is a result
/// outside years 0-9999.
fn DatetimeAddSteps(comptime step_micros: i64) type {
    return struct {
        fn kernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
            const base = out.data.rowCount();
            const dts = args[0].data.datetime;
            const ns = args[1].data.bigint;
            for (0..row_count) |i| {
                const count: ?i32 = if (args[0].isValid(i) and args[1].isValid(i)) std.math.cast(i32, ns[i]) else null;
                const moved = if (count) |n| datetimeInRange(dts[i] +| @as(i64, n) * step_micros) else null;
                try out.data.datetime.append(allocator, moved orelse 0);
                try out.appendValidBit(allocator, base + i, moved != null);
            }
        }
    };
}

pub const datetimeAddHoursKernel = DatetimeAddSteps(std.time.us_per_hour).kernel;
pub const datetimeAddMinutesKernel = DatetimeAddSteps(std.time.us_per_min).kernel;
pub const datetimeAddSecondsKernel = DatetimeAddSteps(std.time.us_per_s).kernel;
pub const datetimeAddMicrosKernel = DatetimeAddSteps(1).kernel;

/// The DATE `days` names, or null outside years 0-9999.
fn dateInRange(days: i64) ?i32 {
    if (days < common.FIRST_DATE_DAYS or days > common.LAST_DATE_DAYS) return null;
    return @intCast(days);
}

/// The DATETIME `micros` names, or null outside years 0-9999.
fn datetimeInRange(micros: i64) ?i64 {
    if (micros < common.FIRST_DATETIME_MICROS or micros > common.LAST_DATETIME_MICROS) return null;
    return micros;
}

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
    if (days > common.LAST_DATE_DAYS) return null;
    return @intCast(days);
}

/// The day number of 1970-01-01 (TO_DAYS): days since 0000-01-01.
const DAY_NUMBER_OF_EPOCH: i64 = -@as(i64, common.FIRST_DATE_DAYS);

/// The day number (TO_DAYS) of a date: days since 0000-01-01, in the one
/// calendar every date reads (`common.validDate`), where year 0 has 366
/// days, as StarRocks counts them. MySQL counts year 0 as 365 days, so its
/// numbers for 0000-01-01 through 0000-02-28 are one higher. Null before
/// year 0.
fn dayNumber(days: i32) ?i64 {
    const number = @as(i64, days) + DAY_NUMBER_OF_EPOCH;
    return if (number < 0) null else number;
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

/// FROM_DAYS: the date of a day number (`dayNumber`). Numbers before
/// 0000-01-01 or past 9999-12-31 are NULL. StarRocks gives its zero date
/// before 0000-01-01, and MySQL before 0001-01-01; a DATE can't hold it.
pub fn fromDaysKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const base = out.data.rowCount();
    const numbers = args[0].data.bigint;
    for (0..row_count) |i| {
        const n = numbers[i];
        const valid = args[0].isValid(i) and n >= 0 and n <= common.LAST_DATE_DAYS + DAY_NUMBER_OF_EPOCH;
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

/// A DATE moved by `n_months`, its day clamped to the destination month's
/// last, or null when the destination leaves years 0-9999.
fn addMonths(days: i32, n_months: i64) ?i32 {
    const ymd = daysToYmd(days);
    const months_since_year_0 = @as(i64, ymd.year) * 12 + (@as(i64, ymd.month) - 1) + n_months;
    const whole_years = @divFloor(months_since_year_0, 12);
    if (whole_years < 0 or whole_years > 9999) return null;
    const year: i32 = @intCast(whole_years);
    const month: u32 = @intCast(@mod(months_since_year_0, 12) + 1);
    return common.ymdToDays(year, month, @min(@as(u32, ymd.day), common.lastDayOfMonth(year, month)));
}

/// UNIX_TIMESTAMP: whole seconds since 1970-01-01 00:00:00 UTC. A time
/// before 1970 is 0, as in StarRocks and MySQL. Every DATETIME through
/// 9999-12-31 23:59:59 has its count; StarRocks gives 0 past 9999-12-31
/// 07:59:59, a time-zone guard band thinDB doesn't copy.
pub fn unixTimestampKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    for (args[0].data.datetime[0..row_count]) |micros| {
        const seconds: i64 = @max(@divFloor(micros, std.time.us_per_s), 0);
        try out.data.bigint.append(allocator, seconds);
    }
}

const LAST_UNIX_SECOND: i64 = @divFloor(common.LAST_DATETIME_MICROS, std.time.us_per_s);

/// The moment `count` seconds after 1970-01-01 00:00:00 UTC, or null for a
/// negative count, as in StarRocks and MySQL, or for one past 9999-12-31
/// 23:59:59, the end of the DATETIME range. StarRocks stops 16 hours
/// earlier, at 9999-12-31 07:59:59, a time-zone guard band; thinDB keeps one
/// calendar end for every function instead.
fn unixMoment(count: i64) ?i64 {
    if (count < 0 or count > LAST_UNIX_SECOND) return null;
    return count * std.time.us_per_s;
}

fn unixMomentAt(counts: ColumnView, row: usize) ?i64 {
    return if (counts.isValid(row)) unixMoment(counts.data.bigint[row]) else null;
}

/// FROM_UNIXTIME(n): the DATETIME `unixMoment` gives. A count that isn't an
/// integer is read as CAST reads it (`scalar_fn.readsArgsAsCast`).
pub fn fromUnixtimeKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const base = out.data.rowCount();
    for (0..row_count) |i| {
        const moment = unixMomentAt(args[0], i);
        try out.data.datetime.append(allocator, moment orelse 0);
        try out.appendValidBit(allocator, base + i, moment != null);
    }
}

/// FROM_UNIXTIME(n, format): that moment as DATE_FORMAT renders it.
pub fn fromUnixtimeFormatKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const base = out.data.rowCount();
    const formats = stringViewOf(args[1]);
    var buf: std.ArrayList(u8) = .empty;
    defer buf.deinit(allocator);
    for (0..row_count) |i| {
        const fmt = if (args[1].isValid(i)) formats.rowBytes(i) else null;
        try appendFormatted(allocator, out, &buf, base + i, unixMomentAt(args[0], i), fmt);
    }
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

/// Each row of `args[0]` read by `read` into the `field` column of `out`,
/// NULL where `read` gives null.
fn readRows(
    comptime field: []const u8,
    comptime read: anytype,
    allocator: Allocator,
    args: []const ColumnView,
    out: *ColumnStore,
    row_count: usize,
) !void {
    if (row_count == 0) return;
    const base = out.data.rowCount();
    for (0..row_count) |i| {
        const value = if (!args[0].isValid(i)) null else switch (@typeInfo(@TypeOf(read)).@"fn".params[0].type.?) {
            []const u8 => read(stringViewOf(args[0]).rowBytes(i)),
            i64 => read(args[0].data.bigint[i]),
            f64 => read(args[0].data.double[i]),
            else => @compileError("readRows reads text, BIGINT or DOUBLE"),
        };
        try @field(out.data, field).append(allocator, value orelse 0);
        try out.appendValidBit(allocator, base + i, value != null);
    }
}

/// The day of text read as a DATETIME: what DATE(text) gives in StarRocks,
/// where a time of day that isn't valid makes the whole text NULL.
fn textDatetimeDay(s: []const u8) ?i32 {
    return daysFromDatetime(common.textToDatetime(s) orelse return null);
}

/// CAST(text AS DATE). Text that isn't a date is NULL.
pub fn stringToDateKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    try readRows("date", common.textToDate, allocator, args, out, row_count);
}

/// CAST(text AS DATETIME). Text that isn't a datetime is NULL.
pub fn stringToDatetimeKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    try readRows("datetime", common.textToDatetime, allocator, args, out, row_count);
}

/// DATE(text): the text read as a DATETIME, then its day.
pub fn stringDatetimeDayKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    try readRows("date", textDatetimeDay, allocator, args, out, row_count);
}

/// CAST(n AS DATE) and DATE(n). A number that isn't a date is NULL.
pub fn bigintToDateKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    try readRows("date", common.numberToDate, allocator, args, out, row_count);
}

/// CAST(x AS DATE) and DATE(x) of a double.
pub fn doubleToDateKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    try readRows("date", common.doubleToDate, allocator, args, out, row_count);
}

/// CAST(n AS DATETIME). A number that isn't a datetime is NULL.
pub fn bigintToDatetimeKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    try readRows("datetime", common.numberToDatetime, allocator, args, out, row_count);
}

/// CAST(x AS DATETIME) of a double.
pub fn doubleToDatetimeKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    try readRows("datetime", common.doubleToDatetime, allocator, args, out, row_count);
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

/// A DATE moved by `n` of `unit`, or null outside years 0-9999. A sub-day
/// unit moves the date's midnight and keeps the day it lands on.
fn addUnitToDate(unit: DateUnit, days: i32, n: i32) ?i32 {
    return switch (unit) {
        .day => dateInRange(@as(i64, days) + n),
        .week => dateInRange(@as(i64, days) + @as(i64, n) * 7),
        .month => addMonths(days, n),
        .quarter => addMonths(days, @as(i64, n) * 3),
        .year => addMonths(days, @as(i64, n) * 12),
        .hour, .minute, .second => daysFromDatetime(addUnitToDatetime(unit, @as(i64, days) * std.time.us_per_day, n) orelse return null),
    };
}

/// A DATETIME moved by `n` of `unit`, keeping the time of day for a unit of
/// a day or more, or null outside years 0-9999.
fn addUnitToDatetime(unit: DateUnit, micros: i64, n: i32) ?i64 {
    return switch (unit) {
        .second => datetimeInRange(micros +| @as(i64, n) * std.time.us_per_s),
        .minute => datetimeInRange(micros +| @as(i64, n) * std.time.us_per_min),
        .hour => datetimeInRange(micros +| @as(i64, n) * std.time.us_per_hour),
        .day, .week, .month, .quarter, .year => {
            const days = addUnitToDate(unit, daysFromDatetime(micros), n) orelse return null;
            return @as(i64, days) * std.time.us_per_day + @mod(micros, std.time.us_per_day);
        },
    };
}

/// TIMESTAMPADD(unit, n, x) with its unit given as text; the parser turns a
/// unit word into an interval. A result outside years 0-9999 is NULL.
fn TimestampAdd(comptime temporal: Temporal) type {
    return struct {
        fn kernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
            const unit = try parseDateUnit(stringViewOf(args[0]).rowBytes(0));
            const base = out.data.rowCount();
            const ns = args[1].data.int;
            const values = @field(args[2].data, @tagName(temporal));
            for (0..row_count) |i| {
                const moved = if (args[1].isValid(i) and args[2].isValid(i)) switch (temporal) {
                    .date => addUnitToDate(unit, values[i], ns[i]),
                    .datetime => addUnitToDatetime(unit, values[i], ns[i]),
                } else null;
                try @field(out.data, @tagName(temporal)).append(allocator, moved orelse 0);
                try out.appendValidBit(allocator, base + i, moved != null);
            }
        }
    };
}

pub const timestampAddDateKernel = TimestampAdd(.date).kernel;
pub const timestampAddDatetimeKernel = TimestampAdd(.datetime).kernel;

// ---------------------------------------------------------------------------
// date_format: MySQL's specifiers (scalar_fn_datefmt.zig). Per-row format
// strings are allowed; each row re-reads its format, which costs a few
// hundred ns.
// ---------------------------------------------------------------------------

/// Appends `micros` rendered under `fmt` (`datefmt.mysqlFormat`) as output
/// row `row`: NULL when either is NULL or the format is empty, as in
/// StarRocks. `buf` is scratch reused across rows.
fn appendFormatted(allocator: Allocator, out: *ColumnStore, buf: *std.ArrayList(u8), row: usize, micros: ?i64, fmt: ?[]const u8) !void {
    const mysql_fmt = if (fmt) |f| datefmt.mysqlFormat(f) else null;
    if (micros == null or mysql_fmt == null) {
        try stringStoreOf(out).appendValue(allocator, "");
        return out.appendValidBit(allocator, row, false);
    }
    buf.clearRetainingCapacity();
    try datefmt.format(allocator, buf, mysql_fmt.?, daysFromDatetime(micros.?), @mod(micros.?, std.time.us_per_day));
    try stringStoreOf(out).appendValue(allocator, buf.items);
    try out.appendValidBit(allocator, row, true);
}

fn DateFormat(comptime temporal: Temporal) type {
    return struct {
        fn kernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
            const base = out.data.rowCount();
            const values = @field(args[0].data, @tagName(temporal));
            const formats = stringViewOf(args[1]);
            var buf: std.ArrayList(u8) = .empty;
            defer buf.deinit(allocator);
            for (0..row_count) |i| {
                const micros: ?i64 = if (!args[0].isValid(i)) null else switch (temporal) {
                    .date => @as(i64, values[i]) * std.time.us_per_day,
                    .datetime => values[i],
                };
                const fmt = if (args[1].isValid(i)) formats.rowBytes(i) else null;
                try appendFormatted(allocator, out, &buf, base + i, micros, fmt);
            }
        }
    };
}

pub const dateFormatDatetimeKernel = DateFormat(.datetime).kernel;
pub const dateFormatDateKernel = DateFormat(.date).kernel;

/// A DATE as its number `YYYYMMDD` (`CAST(d AS SIGNED)`, `d + 0`).
pub fn dateToBigintKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    for (args[0].data.date[0..row_count]) |d| try out.data.bigint.append(allocator, common.dateNumber(d));
}

/// A DATETIME as its number `YYYYMMDDHHMMSS`, rounded to the second first,
/// as MySQL rounds it: 23:59:59.5 is the next day at 000000.
pub fn datetimeToBigintKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const us_per_s = std.time.us_per_s;
    for (args[0].data.datetime[0..row_count]) |dt| {
        const fraction = @mod(dt, us_per_s);
        const seconds = dt - fraction + @as(i64, if (fraction >= us_per_s / 2) us_per_s else 0);
        try out.data.bigint.append(allocator, @intCast(@divExact(common.datetimeNumber(seconds).m, us_per_s)));
    }
}

pub fn dateToDoubleKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    for (args[0].data.date[0..row_count]) |d| try out.data.double.append(allocator, @floatFromInt(common.dateNumber(d)));
}

/// A DATETIME as its number `YYYYMMDDHHMMSS.ffffff`.
pub fn datetimeToDoubleKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    for (args[0].data.datetime[0..row_count]) |dt| {
        const n = common.datetimeNumber(dt).m;
        const whole: f64 = @floatFromInt(@divFloor(n, std.time.us_per_s));
        const fraction: f64 = @floatFromInt(@mod(n, std.time.us_per_s));
        try out.data.double.append(allocator, whole + fraction / std.time.us_per_s);
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

test "date arithmetic that leaves years 0-9999 is null, whatever its count" {
    const day = common.ymdToDays;
    const dt = struct {
        fn at(y: i32, mo: u32, d: u32, h: i64, mi: i64, s: i64) i64 {
            return @as(i64, common.ymdToDays(y, mo, d)) * std.time.us_per_day + ((h * 60 + mi) * 60 + s) * std.time.us_per_s;
        }
    }.at;
    const max = std.math.maxInt(i32);
    const min = std.math.minInt(i32);
    // Every expected value is StarRocks 4.0's.
    const dates = .{
        .{ DateUnit.day, day(9999, 12, 30), 1, day(9999, 12, 31) },
        .{ DateUnit.day, day(9999, 12, 31), 1, null },
        .{ DateUnit.day, day(0, 1, 1), -1, null },
        .{ DateUnit.day, day(2026, 1, 1), max, null },
        .{ DateUnit.day, day(2026, 1, 1), min, null },
        .{ DateUnit.week, day(9999, 12, 24), 1, day(9999, 12, 31) },
        .{ DateUnit.week, day(9999, 12, 25), 1, null },
        .{ DateUnit.month, day(9999, 11, 30), 1, day(9999, 12, 30) },
        .{ DateUnit.month, day(9999, 12, 15), 1, null },
        .{ DateUnit.month, day(0, 1, 31), -1, null },
        .{ DateUnit.month, day(2026, 1, 1), max, null },
        .{ DateUnit.quarter, day(2026, 1, 1), min, null },
        .{ DateUnit.year, day(9998, 2, 28), 1, day(9999, 2, 28) },
        .{ DateUnit.year, day(0, 2, 29), 1, day(1, 2, 28) },
        .{ DateUnit.year, day(9999, 1, 1), 1, null },
        // A year count times 12 once wrapped INT, landing on 2025-01-01.
        .{ DateUnit.year, day(2026, 1, 1), max, null },
        .{ DateUnit.hour, day(9999, 12, 31), 23, day(9999, 12, 31) },
        .{ DateUnit.hour, day(9999, 12, 31), 24, null },
        .{ DateUnit.second, day(0, 1, 1), -1, null },
    };
    inline for (dates) |c| try std.testing.expectEqual(@as(?i32, c[3]), addUnitToDate(c[0], c[1], c[2]));
    const datetimes = .{
        .{ DateUnit.second, dt(9999, 12, 31, 23, 59, 58), 1, dt(9999, 12, 31, 23, 59, 59) },
        .{ DateUnit.second, dt(9999, 12, 31, 23, 59, 59), 1, null },
        .{ DateUnit.hour, dt(2026, 1, 1, 0, 0, 0), max, null },
        .{ DateUnit.day, dt(9999, 12, 30, 10, 0, 0), 1, dt(9999, 12, 31, 10, 0, 0) },
        .{ DateUnit.day, dt(0, 1, 1, 10, 0, 0), -1, null },
        // A day count in microseconds once wrapped BIGINT into the year 36096.
        .{ DateUnit.day, dt(2026, 1, 1, 0, 0, 0), max, null },
        .{ DateUnit.month, dt(9999, 12, 15, 10, 0, 0), 1, null },
        .{ DateUnit.year, dt(2026, 1, 1, 0, 0, 0), min, null },
    };
    inline for (datetimes) |c| try std.testing.expectEqual(@as(?i64, c[3]), addUnitToDatetime(c[0], c[1], c[2]));
}

test "periods, day numbers and zone offsets follow MySQL, and year 0 StarRocks" {
    const t = std.testing;
    // Every expected value is MySQL 8.4's, except day numbers before
    // 0000-03-01, which are StarRocks'.
    inline for (.{ .{ 202601, 13, 202702 }, .{ 6901, 1, 206902 }, .{ 7001, -1, 196912 }, .{ 1, -1, 199912 }, .{ 9912, 1, 200001 } }) |c| {
        try t.expectEqual(@as(u64, c[2]), monthsPeriod(periodMonths(c[0]) +% @as(u64, @bitCast(@as(i64, c[1])))));
    }
    try t.expectEqual(@as(u64, 313), periodMonths(202601) -% periodMonths(199912));
    inline for (.{ 0, -5, 202600, 202613 }) |p| try t.expect(!validPeriod(p));
    inline for (.{ .{ 0, 1, 1, 0 }, .{ 0, 2, 29, 59 }, .{ 0, 3, 1, 60 }, .{ 1970, 1, 1, 719_528 }, .{ 2026, 9, 26, 740_250 }, .{ 1, 1, 1, 366 } }) |c| {
        try t.expectEqual(@as(?i64, c[3]), dayNumber(common.ymdToDays(c[0], c[1], c[2])));
    }
    try t.expectEqual(@as(?i64, null), dayNumber(common.ymdToDays(-1, 12, 31)));
    inline for (.{ .{ "+14:00", 50_400 }, .{ "-13:59", -50_340 }, .{ "+5:30", 19_800 }, .{ "+05:3", 18_180 }, .{ "utc", 0 } }) |c| {
        try t.expectEqual(@as(?i64, c[1]), zoneOffsetSeconds(c[0]));
    }
    inline for (.{ "+14:01", "-14:00", "+0530", "+05:60", "+1:00x", "Europe/Paris" }) |z| try t.expectEqual(@as(?i64, null), zoneOffsetSeconds(z));
}
