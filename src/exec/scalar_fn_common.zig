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

/// Days-since-epoch → (year, month, day), for every day before the epoch
/// too. Hinnant's civil_from_days — the exact O(1) inverse of `ymdToDays`.
/// The std `EpochDay` path scans year by year from 1970 (~56 iterations for
/// a 2020s date), which dominates every date-part kernel
/// (YEAR/MONTH/DAY/quarter/last_day/…) on hot columns.
pub fn daysToYmd(days: i32) struct { year: i32, month: u4, day: u5 } {
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

/// (hour, minute, second) of a datetime's time of day, before the epoch too.
pub fn microsToHms(micros: i64) struct { hour: u5, minute: u6, second: u6 } {
    const secs: u32 = @intCast(@divFloor(@mod(micros, std.time.us_per_day), std.time.us_per_s));
    return .{
        .hour = @intCast(secs / 3600),
        .minute = @intCast(secs / 60 % 60),
        .second = @intCast(secs % 60),
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

/// A DATE as MySQL reads it where a number is wanted: `YYYYMMDD`.
pub fn dateNumber(days: i32) i64 {
    const ymd = daysToYmd(days);
    return (@as(i64, ymd.year) * 100 + ymd.month) * 100 + ymd.day;
}

/// A DATETIME as MySQL reads it where a number is wanted,
/// `YYYYMMDDHHMMSS.ffffff`: the mantissa at scale 6.
pub fn datetimeNumber(micros: i64) ScaledInt {
    const hms = microsToHms(micros);
    const clock = (@as(i64, hms.hour) * 100 + hms.minute) * 100 + hms.second;
    const whole = @as(i128, dateNumber(daysFromDatetime(micros))) * 1_000_000 + clock;
    return .{ .m = whole * std.time.us_per_s + @mod(micros, std.time.us_per_s), .s = 6 };
}

/// The first and last days a DATE holds, 0000-01-01 and 9999-12-31, as
/// days since the epoch.
pub const FIRST_DATE_DAYS: i32 = -719_528;
pub const LAST_DATE_DAYS: i32 = 2_932_896;

/// The first and last microseconds a DATETIME holds, 0000-01-01 00:00:00
/// and 9999-12-31 23:59:59.999999.
pub const FIRST_DATETIME_MICROS: i64 = @as(i64, FIRST_DATE_DAYS) * std.time.us_per_day;
pub const LAST_DATETIME_MICROS: i64 = (@as(i64, LAST_DATE_DAYS) + 1) * std.time.us_per_day - 1;

/// Whether (year, month, day) names a day a DATE holds: a year from 0 to
/// 9999, and a day within its month in the proleptic Gregorian calendar,
/// where year 0 is a leap year, as StarRocks reads it. MySQL's year 0 has
/// no February 29.
pub fn validDate(year: i32, month: u32, day: u32) bool {
    return year >= 0 and year <= 9999 and month >= 1 and month <= 12 and day >= 1 and day <= lastDayOfMonth(year, month);
}

/// Parse a `YYYY-MM-DD` date string to days-since-epoch. Accepts a trailing
/// time component (so a datetime string parses as its date part). Errors on a
/// malformed prefix or a day that doesn't exist (`2026-02-30`) — callers use
/// that to fall back / reject.
pub fn parseDateString(s: []const u8) !i32 {
    if (s.len < 10) return error.Invalid;
    if (s[4] != '-' or s[7] != '-') return error.Invalid;
    const year = try std.fmt.parseInt(i32, s[0..4], 10);
    const month = try std.fmt.parseInt(u32, s[5..7], 10);
    const day = try std.fmt.parseInt(u32, s[8..10], 10);
    if (!validDate(year, month, day)) return error.Invalid;
    return ymdToDays(year, month, day);
}

/// Parse a datetime string to micros-since-epoch. Accepted forms:
///   `YYYY-MM-DD`                      (midnight)
///   `YYYY-MM-DD[ T]HH:MM:SS`
///   `YYYY-MM-DD[ T]HH:MM:SS.f{1,6}`   (fraction right-padded to micros)
/// with an optional trailing `Z` (values are UTC-naive; an explicit UTC
/// marker is a no-op). Anything else after the recognized prefix errors —
/// a truncating parse would make `=` comparisons silently miss µs-precision
/// rows (the original #148 bug).
pub fn parseDateTimeString(s: []const u8) !i64 {
    const days = try parseDateString(s);

    var idx: usize = 10;
    var day_micros: i64 = 0;
    if (idx < s.len and (s[idx] == ' ' or s[idx] == 'T')) {
        if (s.len < idx + 9) return error.Invalid;
        if (s[idx + 3] != ':' or s[idx + 6] != ':') return error.Invalid;
        const hour = try std.fmt.parseInt(u32, s[idx + 1 .. idx + 3], 10);
        const minute = try std.fmt.parseInt(u32, s[idx + 4 .. idx + 6], 10);
        const second = try std.fmt.parseInt(u32, s[idx + 7 .. idx + 9], 10);
        if (hour > 23 or minute > 59 or second > 59) return error.Invalid;
        idx += 9;
        var micros: i64 = 0;
        if (idx < s.len and s[idx] == '.') {
            idx += 1;
            var digits: usize = 0;
            while (idx < s.len and digits < 6 and s[idx] >= '0' and s[idx] <= '9') : (idx += 1) {
                micros = micros * 10 + (s[idx] - '0');
                digits += 1;
            }
            if (digits == 0) return error.Invalid;
            while (digits < 6) : (digits += 1) micros *= 10;
        }
        day_micros = (@as(i64, hour) * 3600 + @as(i64, minute) * 60 + @as(i64, second)) * 1_000_000 + micros;
    }
    if (idx < s.len and s[idx] == 'Z') idx += 1;
    if (idx != s.len) return error.Invalid;
    return @as(i64, days) * 86_400_000_000 + day_micros;
}

/// Text as a DATE, as StarRocks' `CAST(text AS DATE)` reads a column. Ten
/// characters, once space is trimmed, are read as `plainDate`, and any other
/// text field by field (`scanMoment`). Only the date must be valid: a time
/// of day after it is read but not checked, so `'2026-01-01 25:00:00'` is
/// 2026-01-01. Null when the text isn't a date.
pub fn textToDate(s: []const u8) ?i32 {
    const trimmed = trimDateSpace(s);
    if (trimmed.len == 10) if (plainDate(trimmed)) |m| return m.days();
    return (scanMoment(s) orelse return null).days();
}

/// Text as a DATETIME, as StarRocks' `CAST(text AS DATETIME)` reads a
/// column: a `plainDate` of ten characters is midnight, a `plainDatetime`
/// whose values are valid is read as such, and any other text is read field
/// by field (`scanMoment`). The date and the time of day must both be
/// valid. Null when the text isn't a datetime.
pub fn textToDatetime(s: []const u8) ?i64 {
    const trimmed = trimDateSpace(s);
    if (trimmed.len == 10) if (plainDate(trimmed)) |m| return m.micros();
    if (plainDatetime(trimmed)) |m| if (m.micros()) |us| return us;
    return (scanMoment(s) orelse return null).micros();
}

/// A number as a DATETIME, as StarRocks casts one (`numberDigits`). Null
/// when the number isn't a datetime.
pub fn numberToDatetime(n: i64) ?i64 {
    const digits = numberDigits(n) orelse return null;
    const moment: Moment = .{
        .year = @intCast(@divTrunc(digits, 10_000_000_000)),
        .month = @intCast(@mod(@divTrunc(digits, 100_000_000), 100)),
        .day = @intCast(@mod(@divTrunc(digits, 1_000_000), 100)),
        .hour = @intCast(@mod(@divTrunc(digits, 10_000), 100)),
        .minute = @intCast(@mod(@divTrunc(digits, 100), 100)),
        .second = @intCast(@mod(digits, 100)),
    };
    return moment.micros();
}

/// A number as a DATE: the day of `numberToDatetime`, so its time of day
/// must be valid too (`20260101240000` is null), as in StarRocks.
pub fn numberToDate(n: i64) ?i32 {
    return daysFromDatetime(numberToDatetime(n) orelse return null);
}

/// A double as a DATETIME: its whole part, truncated toward zero, read as
/// `numberToDatetime` reads a number.
pub fn doubleToDatetime(x: f64) ?i64 {
    if (!(@abs(x) < 1e15)) return null;
    return numberToDatetime(@intFromFloat(@trunc(x)));
}

/// A double as a DATE: the day of `doubleToDatetime`.
pub fn doubleToDate(x: f64) ?i32 {
    return daysFromDatetime(doubleToDatetime(x) orelse return null);
}

/// A number's digits as `YYYYMMDDhhmmss`. The number is read by its size:
/// `YYMMDD`, `YYYYMMDD`, `YYMMDDhhmmss` or `YYYYMMDDhhmmss`, where a
/// two-digit year below 70 is in the 2000s and any other in the 1900s. A
/// number between those sizes, or one that no size fits, isn't a date.
fn numberDigits(n: i64) ?i64 {
    if (n >= 101 and n <= 691_231) return (n + 20_000_000) * 1_000_000;
    if (n >= 700_101 and n <= 991_231) return (n + 19_000_000) * 1_000_000;
    if (n >= 10_000_101 and n <= 99_991_231) return n * 1_000_000;
    if (n >= 101_000_000 and n <= 691_231_235_959) return n + 20_000_000_000_000;
    if (n >= 700_101_000_000 and n <= 991_231_235_959) return n + 19_000_000_000_000;
    if (n > 991_231_235_959 and n <= 99_999_999_999_999) return n;
    return null;
}

/// A date and time of day read from text or a number, before its range is
/// checked.
const Moment = struct {
    year: u32,
    month: u32,
    day: u32,
    hour: u32 = 0,
    minute: u32 = 0,
    second: u32 = 0,
    microsecond: u32 = 0,

    fn days(self: Moment) ?i32 {
        const year: i32 = @intCast(self.year);
        if (!validDate(year, self.month, self.day)) return null;
        return ymdToDays(year, self.month, self.day);
    }

    fn micros(self: Moment) ?i64 {
        if (self.hour > 23 or self.minute > 59 or self.second > 59) return null;
        const clock = (@as(i64, self.hour) * 60 + self.minute) * 60 + self.second;
        return @as(i64, self.days() orelse return null) * std.time.us_per_day + clock * std.time.us_per_s + self.microsecond;
    }
};

/// Space around a date: ' ' and the control characters \t \n \v \f \r.
fn isDateSpace(c: u8) bool {
    return c == ' ' or (c >= '\t' and c <= '\r');
}

/// A printable ASCII character that is neither a letter, a digit nor a space.
fn isDatePunct(c: u8) bool {
    return c > ' ' and c < 0x7f and !std.ascii.isAlphanumeric(c);
}

fn trimDateSpace(s: []const u8) []const u8 {
    var start: usize = 0;
    var end = s.len;
    while (start < end and isDateSpace(s[start])) start += 1;
    while (end > start and isDateSpace(s[end - 1])) end -= 1;
    return s[start..end];
}

fn allDigits(s: []const u8) bool {
    for (s) |c| if (!std.ascii.isDigit(c)) return false;
    return true;
}

fn digitsValue(s: []const u8) u32 {
    var value: u32 = 0;
    for (s) |c| value = value * 10 + (c - '0');
    return value;
}

/// `YYYY?MM?DD` at the start of `t`, where each `?` is any one character
/// but a digit. Its values aren't checked.
fn plainDate(t: []const u8) ?Moment {
    if (t.len < 10 or !allDigits(t[0..4]) or std.ascii.isDigit(t[4]) or
        !allDigits(t[5..7]) or std.ascii.isDigit(t[7]) or !allDigits(t[8..10])) return null;
    return .{ .year = digitsValue(t[0..4]), .month = digitsValue(t[5..7]), .day = digitsValue(t[8..10]) };
}

/// A plain date followed by `hh?mm?ss`, either right after a `T` (19
/// characters in all) or as the last eight characters after a run of
/// space.
fn plainDatetime(t: []const u8) ?Moment {
    if (t.len < 19 or !std.ascii.isDigit(t[9])) return null;
    const clock = if (t.len == 19 and t[10] == 'T' and std.ascii.isDigit(t[11])) t[11..] else blk: {
        for (t[10 .. t.len - 8]) |c| if (!isDateSpace(c)) return null;
        break :blk t[t.len - 8 ..];
    };
    if (!allDigits(clock[0..2]) or std.ascii.isDigit(clock[2]) or
        !allDigits(clock[3..5]) or std.ascii.isDigit(clock[5]) or !allDigits(clock[6..8])) return null;
    var moment = plainDate(t) orelse return null;
    moment.hour = digitsValue(clock[0..2]);
    moment.minute = digitsValue(clock[3..5]);
    moment.second = digitsValue(clock[6..8]);
    return moment;
}

/// Text read as up to seven numeric fields: year, month, day, hour, minute,
/// second and a fraction of a second. At least the first three must be
/// there. Leading space is skipped; reading stops at the first character
/// that can't continue a field, and the rest of the text is ignored.
///
/// - When the leading run of digits (a `T` may sit among them) reaches the
///   end of the text or a `.`, fields have fixed widths: a four-digit year
///   when the run is 4, 8 or at least 14 characters long and a two-digit one
///   otherwise, then two digits a field. Otherwise each field is a whole run
///   of digits, and a field over 999999 isn't a date.
/// - Fields are separated by punctuation. Space may also separate the day
///   from the time, but nowhere else in the date or the time; a letter ends
///   the reading.
/// - A `T` right after the day starts the time.
/// - Only a `.` after the seconds starts the fraction, whose first six
///   digits are read.
/// - A two-digit year below 70 is in the 2000s, and any other in the 1900s.
fn scanMoment(s: []const u8) ?Moment {
    var i: usize = 0;
    while (i < s.len and isDateSpace(s[i])) i += 1;
    if (i == s.len or !std.ascii.isDigit(s[i])) return null;

    var run_end = i;
    while (run_end < s.len and (std.ascii.isDigit(s[run_end]) or s[run_end] == 'T')) run_end += 1;
    const fixed_widths = run_end == s.len or s[run_end] == '.';
    const run_len = run_end - i;
    const short_year = !(run_len == 4 or run_len == 8 or run_len >= 14);

    var fields = [_]u32{0} ** 7;
    var widths = [_]usize{0} ** 7;
    var count: usize = 0;
    while (count < fields.len and i < s.len and std.ascii.isDigit(s[i])) {
        const max_width: usize = if (count == 6) 6 else if (!fixed_widths) std.math.maxInt(usize) else if (count == 0 and !short_year) 4 else 2;
        const start = i;
        var value: u32 = 0;
        while (i < s.len and std.ascii.isDigit(s[i]) and i - start < max_width) : (i += 1) {
            value = value * 10 + (s[i] - '0');
            if (value > 999_999) return null;
        }
        fields[count] = value;
        widths[count] = i - start;
        count += 1;
        if (i == s.len or count == fields.len) break;
        if (count == 3 and s[i] == 'T') {
            i += 1;
            continue;
        }
        if (count == 6) {
            if (s[i] != '.') break;
            i += 1;
            continue;
        }
        while (i < s.len and (isDatePunct(s[i]) or isDateSpace(s[i]))) : (i += 1) {
            if (isDateSpace(s[i]) and count != 3) return null;
        }
    }
    if (count < 3) return null;

    const two_digit_year = if (fixed_widths) short_year else widths[0] == 2;
    var fraction = fields[6];
    for (widths[6]..6) |_| fraction *= 10;
    return .{
        .year = if (!two_digit_year) fields[0] else if (fields[0] < 70) fields[0] + 2000 else fields[0] + 1900,
        .month = fields[1],
        .day = fields[2],
        .hour = fields[3],
        .minute = fields[4],
        .second = fields[5],
        .microsecond = fraction,
    };
}

pub const TEXT_SPACE = " \t\r\n";

/// A decimal value: mantissa `m` at scale `s`, i.e. m / 10^s.
pub const ScaledInt = struct { m: i128, s: u8 };

/// A number read from text: exact when it is plain decimal digits a DECIMAL
/// holds, a double otherwise.
pub const TextNumber = union(enum) {
    exact: ScaledInt,
    float: f64,
};

/// Text read as a number, the way StarRocks casts it: surrounding spaces
/// ignored, plain decimal digits kept exact, an exponent form read as a
/// double; anything else is not a number.
pub fn textNumber(raw: []const u8) ?TextNumber {
    const text = std.mem.trim(u8, raw, TEXT_SPACE);
    var i: usize = 0;
    const negative = i < text.len and text[i] == '-';
    if (i < text.len and (text[i] == '-' or text[i] == '+')) i += 1;
    var m: i128 = 0;
    var digits: usize = 0;
    var scale: u8 = 0;
    var seen_point = false;
    var exact = true;
    while (i < text.len) : (i += 1) {
        const c = text[i];
        if (c == '.' and !seen_point) {
            seen_point = true;
        } else if (c >= '0' and c <= '9') {
            digits += 1;
            if (digits > 38) {
                exact = false;
                continue;
            }
            m = m * 10 + (c - '0');
            if (seen_point) scale += 1;
        } else break;
    }
    if (digits == 0) return null;
    if (i == text.len and exact) return .{ .exact = .{ .m = if (negative) -m else m, .s = scale } };
    for (text[i..]) |c| switch (c) {
        '0'...'9', '.', 'e', 'E', '+', '-' => {},
        else => return null,
    };
    const f = std.fmt.parseFloat(f64, text) catch return null;
    return if (std.math.isFinite(f)) .{ .float = f } else null;
}

/// 10^38: every DECIMAL value's magnitude is below it.
const DECIMAL_LIMIT: u128 = std.math.powi(u128, 10, 38) catch unreachable;

/// A double as the decimal it prints as: the shortest digits that read back
/// as the same double. MySQL converts a DOUBLE to a DECIMAL through these
/// digits, so `1.005e0` is 1.005 rather than the binary value just below
/// it. Null when it isn't finite or needs more than 38 digits before the
/// point; digits past scale 38 round away.
pub fn floatDigits(x: f64) ?ScaledInt {
    if (!std.math.isFinite(x)) return null;
    const float_fmt = std.fmt.float;
    const d = float_fmt.binaryToDecimal(u64, @bitCast(x), std.math.floatMantissaBits(f64), std.math.floatExponentBits(f64), false, &float_fmt.Backend64_TablesFull);
    const m: i128 = if (d.sign) -@as(i128, d.mantissa) else d.mantissa;
    if (d.exponent >= 0) {
        const unit = std.math.powi(i128, 10, @intCast(d.exponent)) catch return null;
        const whole = std.math.mul(i128, m, unit) catch return null;
        if (@abs(whole) >= DECIMAL_LIMIT) return null;
        return .{ .m = whole, .s = 0 };
    }
    const s: u32 = @intCast(-d.exponent);
    if (s <= 38) return .{ .m = m, .s = @intCast(s) };
    const unit = std.math.powi(i128, 10, s - 38) catch return .{ .m = 0, .s = 38 };
    const q = @divTrunc(m, unit);
    const rounds_away = @abs(@rem(m, unit)) * 2 >= @abs(unit);
    return .{ .m = if (!rounds_away) q else if (m < 0) q - 1 else q + 1, .s = 38 };
}

/// The longest text `floatText` writes: a sign, `0.`, 14 zeros and 17 digits.
pub const FLOAT_TEXT_MAX = 40;

pub const FloatTextStyle = enum {
    /// A DOUBLE or FLOAT value as text: `100`, `-0`, `1e15`.
    plain,
    /// A double inside JSON text, where a positional whole number keeps a
    /// fraction: `100.0`, `0.0`.
    json,
};

/// A float as MySQL writes it as text: its shortest round-trip digits,
/// positional while the decimal point falls at most 15 places after the
/// first digit (or inside the digits) and at most 14 zeros before it, and
/// `d.ddde[-]x` otherwise, so `1e14` is `100000000000000` but `1e15` is
/// `1e15`. A FLOAT keeps its own shortest digits where MySQL rounds it to
/// six, as StarRocks and DuckDB do. A value that isn't finite writes as
/// `inf`, `-inf` or `nan`.
pub fn floatText(buf: *[FLOAT_TEXT_MAX]u8, x: anytype, style: FloatTextStyle) []const u8 {
    const F = @TypeOf(x);
    var w: std.Io.Writer = .fixed(buf);
    writeFloatText(&w, F, x, style) catch unreachable;
    return w.buffered();
}

fn writeFloatText(w: *std.Io.Writer, comptime F: type, x: F, style: FloatTextStyle) std.Io.Writer.Error!void {
    if (std.math.isNan(x)) return w.writeAll("nan");
    if (std.math.isInf(x)) return w.writeAll(if (x > 0) "inf" else "-inf");
    if (std.math.signbit(x)) try w.writeByte('-');
    if (x == 0) return w.writeAll(if (style == .json) "0.0" else "0");
    const Bits = @Int(.unsigned, @bitSizeOf(F));
    const float_fmt = std.fmt.float;
    const d = float_fmt.binaryToDecimal(u64, @as(Bits, @bitCast(x)), std.math.floatMantissaBits(F), std.math.floatExponentBits(F), false, &float_fmt.Backend64_TablesFull);
    var digit_buf: [24]u8 = undefined;
    const all_digits = std.fmt.bufPrint(&digit_buf, "{d}", .{d.mantissa}) catch unreachable;
    const point: i32 = @as(i32, @intCast(all_digits.len)) + d.exponent;
    const digits = std.mem.trimEnd(u8, all_digits, "0");
    const len: i32 = @intCast(digits.len);
    if (point >= -14 and (point <= 15 or len > point)) {
        if (point <= 0) {
            try w.writeAll("0.");
            try w.splatByteAll('0', @intCast(-point));
            try w.writeAll(digits);
        } else if (point >= len) {
            try w.writeAll(digits);
            try w.splatByteAll('0', @intCast(point - len));
            if (style == .json) try w.writeAll(".0");
        } else {
            const whole: usize = @intCast(point);
            try w.writeAll(digits[0..whole]);
            try w.writeByte('.');
            try w.writeAll(digits[whole..]);
        }
        return;
    }
    try w.writeByte(digits[0]);
    if (digits.len > 1) {
        try w.writeByte('.');
        try w.writeAll(digits[1..]);
    }
    try w.print("e{d}", .{point - 1});
}

/// Text as a DOUBLE: any number `textNumber` reads, correctly rounded.
pub fn textDouble(raw: []const u8) ?f64 {
    return switch (textNumber(raw) orelse return null) {
        .float => |f| f,
        .exact => std.fmt.parseFloat(f64, std.mem.trim(u8, raw, TEXT_SPACE)) catch null,
    };
}

/// A double where MySQL reads it as an integer argument (`ELT(2.5e0, ...)`,
/// `INET_NTOA(1.5e0)`): rounded half to even, as C's rint rounds it.
pub fn roundHalfEven(x: f64) f64 {
    return if (@abs(x - @trunc(x)) == 0.5) 2 * @round(x / 2) else @round(x);
}

/// A double where MySQL reads it as a BIGINT (`HEX(2.5e0)`): rounded half
/// to even, and clamped to the BIGINT range.
pub fn doubleAsBigint(x: f64) i64 {
    if (std.math.isNan(x)) return 0;
    const r = roundHalfEven(x);
    if (r >= 0x1p63) return std.math.maxInt(i64);
    if (r < -0x1p63) return std.math.minInt(i64);
    return @intFromFloat(r);
}

/// HEX of a number, as MySQL prints it: the uppercase digits of the
/// BIGINT's 64 bits, so a negative value shows its two's complement.
pub fn integerHex(buf: *[16]u8, v: i64) []const u8 {
    return std.fmt.bufPrint(buf, "{X}", .{@as(u64, @bitCast(v))}) catch unreachable;
}

/// A wide integer where MySQL's HEX reads a BIGINT: a value from 2^63 to
/// 2^64 - 1 keeps its low 64 bits, as MySQL types an integer literal that
/// large BIGINT UNSIGNED; any other value past BIGINT is clamped to it.
pub fn wideIntegerAsBigint(v: i128) i64 {
    if (v > std.math.maxInt(i64)) {
        if (std.math.cast(u64, v)) |u| return @bitCast(u);
    }
    return std.math.cast(i64, v) orelse if (v < 0) std.math.minInt(i64) else std.math.maxInt(i64);
}

/// Text as an integer, the way StarRocks casts it to one: surrounding
/// spaces ignored, an optional sign, then digits only. A fraction, an
/// exponent or a value past i128 is not an integer.
pub fn textInteger(raw: []const u8) ?i128 {
    const text = std.mem.trim(u8, raw, TEXT_SPACE);
    const negative = text.len > 0 and text[0] == '-';
    const digits = if (text.len > 0 and (text[0] == '-' or text[0] == '+')) text[1..] else text;
    if (digits.len == 0) return null;
    var v: i128 = 0;
    for (digits) |c| {
        if (c < '0' or c > '9') return null;
        const d: i128 = c - '0';
        v = std.math.mul(i128, v, 10) catch return null;
        v = (if (negative) std.math.sub(i128, v, d) else std.math.add(i128, v, d)) catch return null;
    }
    return v;
}

/// Text as a BOOLEAN, the way StarRocks casts it: `true` or `false` in any
/// case, or an INT (`textInteger`), which is true when nonzero. A fraction,
/// an exponent or an integer past INT is no BOOLEAN.
pub fn textBoolean(raw: []const u8) ?bool {
    const text = std.mem.trim(u8, raw, TEXT_SPACE);
    if (std.ascii.eqlIgnoreCase(text, "true")) return true;
    if (std.ascii.eqlIgnoreCase(text, "false")) return false;
    const n = std.math.cast(i32, textInteger(text) orelse return null) orelse return null;
    return n != 0;
}

/// Text where MySQL expects a DOUBLE and no CAST was written: the longest
/// number the text starts with after any whitespace, its exponent kept only
/// when digits follow the `e`; 0 when it starts with none (`'3abc'` is 3,
/// `'abc'` is 0). Past the double range it is the largest finite double of
/// its sign.
pub fn leadingDouble(raw: []const u8) f64 {
    var i: usize = 0;
    while (i < raw.len and std.ascii.isWhitespace(raw[i])) i += 1;
    const start = i;
    if (i < raw.len and (raw[i] == '-' or raw[i] == '+')) i += 1;
    var digits: usize = 0;
    while (i < raw.len and std.ascii.isDigit(raw[i])) : (i += 1) digits += 1;
    if (i < raw.len and raw[i] == '.') {
        i += 1;
        while (i < raw.len and std.ascii.isDigit(raw[i])) : (i += 1) digits += 1;
    }
    if (digits == 0) return 0;
    if (i < raw.len and (raw[i] == 'e' or raw[i] == 'E')) {
        var j = i + 1;
        if (j < raw.len and (raw[j] == '-' or raw[j] == '+')) j += 1;
        const exponent_start = j;
        while (j < raw.len and std.ascii.isDigit(raw[j])) j += 1;
        if (j > exponent_start) i = j;
    }
    const x = std.fmt.parseFloat(f64, raw[start..i]) catch return 0;
    if (std.math.isInf(x)) return std.math.copysign(std.math.floatMax(f64), x);
    return x;
}

/// Text where MySQL expects an integer and no CAST was written: after any
/// spaces and tabs, an optional sign and the digits that follow, so a
/// fraction or an exponent is cut off (`'2.7'` is 2, `'1e3'` is 1) and text
/// with no leading digits is 0. As in MySQL, a magnitude past 64 bits
/// saturates and a positive one past BIGINT reads as its unsigned bits.
pub fn leadingInteger(raw: []const u8) i64 {
    var i: usize = 0;
    while (i < raw.len and (raw[i] == ' ' or raw[i] == '\t')) i += 1;
    const negative = i < raw.len and raw[i] == '-';
    if (i < raw.len and (raw[i] == '-' or raw[i] == '+')) i += 1;
    var magnitude: u64 = 0;
    while (i < raw.len and std.ascii.isDigit(raw[i])) : (i += 1) {
        magnitude = std.math.mul(u64, magnitude, 10) catch std.math.maxInt(u64);
        magnitude = std.math.add(u64, magnitude, raw[i] - '0') catch std.math.maxInt(u64);
    }
    if (!negative) return @bitCast(magnitude);
    if (magnitude >= @as(u64, 1) << 63) return std.math.minInt(i64);
    return -@as(i64, @intCast(magnitude));
}

test "leading numbers: what MySQL reads where a number is expected" {
    const t = std.testing;
    const doubles = .{
        .{ "3abc", 3.0 },    .{ "abc", 0.0 }, .{ "", 0.0 },     .{ " 12 ", 12.0 },
        .{ "\n2", 2.0 },     .{ "+3", 3.0 },  .{ "0x10", 0.0 }, .{ ".5", 0.5 },
        .{ "5.", 5.0 },      .{ "1e", 1.0 },  .{ "1e+", 1.0 },  .{ "-.5e1x", -5.0 },
        .{ "1.5e2", 150.0 }, .{ "inf", 0.0 }, .{ "-", 0.0 },    .{ ".", 0.0 },
        .{ "1_0", 1.0 },
    };
    inline for (doubles) |c| try t.expectEqual(@as(f64, c[1]), leadingDouble(c[0]));
    try t.expectEqual(std.math.floatMax(f64), leadingDouble("1e400"));
    try t.expectEqual(-std.math.floatMax(f64), leadingDouble("-1e400"));

    const integers = .{
        .{ "3", 3 },                                        .{ "2.7", 2 },                                    .{ " 3", 3 },
        .{ "\t2", 2 },                                      .{ "\n2", 0 },                                    .{ "3abc", 3 },
        .{ "abc", 0 },                                      .{ "1e1", 1 },                                    .{ "-1", -1 },
        .{ "+4", 4 },                                       .{ "", 0 },                                       .{ "-9223372036854775808", std.math.minInt(i64) },
        .{ "9223372036854775807", std.math.maxInt(i64) },   .{ "9223372036854775808", std.math.minInt(i64) }, .{ "99999999999999999999", -1 },
        .{ "-99999999999999999999", std.math.minInt(i64) },
    };
    inline for (integers) |c| try t.expectEqual(@as(i64, c[1]), leadingInteger(c[0]));
}

test "text as a number: what StarRocks casts, and nothing else" {
    const t = std.testing;
    const exact = .{
        .{ "12", ScaledInt{ .m = 12, .s = 0 } },
        .{ " -1.50 ", ScaledInt{ .m = -150, .s = 2 } },
        .{ "+.5", ScaledInt{ .m = 5, .s = 1 } },
        .{ "5.", ScaledInt{ .m = 5, .s = 0 } },
    };
    inline for (exact) |c| try t.expectEqual(TextNumber{ .exact = c[1] }, textNumber(c[0]).?);
    try t.expectEqual(TextNumber{ .float = 1500.0 }, textNumber("1.5e3").?);
    inline for (.{ "", "abc", "12abc", "-", ".", "1e999", "inf", "NaN", "0x10", "1_0", "1 2" }) |bad| try t.expect(textNumber(bad) == null);

    try t.expectEqual(@as(?f64, 0.1), textDouble(" 0.1"));
    try t.expectEqual(@as(?f64, 100.0), textDouble("1e2"));
    try t.expect(textDouble("1.5x") == null);

    try t.expectEqual(@as(?i128, 5), textInteger("+5"));
    try t.expectEqual(@as(?i128, -7), textInteger(" -007 "));
    try t.expectEqual(@as(?i128, std.math.minInt(i128)), textInteger("-170141183460469231731687303715884105728"));
    inline for (.{ "", "-", "12.0", "1.7", "12abc", "1e3", "1 2", "170141183460469231731687303715884105728" }) |bad| try t.expect(textInteger(bad) == null);

    try t.expectEqual(@as(?bool, true), textBoolean(" TRUE "));
    try t.expectEqual(@as(?bool, false), textBoolean("False"));
    try t.expectEqual(@as(?bool, true), textBoolean("\t-2147483648\n"));
    try t.expectEqual(@as(?bool, false), textBoolean("+00"));
    inline for (.{ "x", "", "0.0", "0.5", "1e2", "2147483648", "- 1", "1 1" }) |bad| try t.expect(textBoolean(bad) == null);
}

test "floatDigits: a double's shortest digits" {
    const t = std.testing;
    const cases = .{
        .{ 1.005, ScaledInt{ .m = 1005, .s = 3 } },
        .{ -2.675, ScaledInt{ .m = -2675, .s = 3 } },
        .{ @as(f64, 0.1) + @as(f64, 0.2), ScaledInt{ .m = 30000000000000004, .s = 17 } },
        .{ 100.0, ScaledInt{ .m = 100, .s = 0 } },
        .{ 0.0, ScaledInt{ .m = 0, .s = 0 } },
        .{ 1.5e20, ScaledInt{ .m = 150000000000000000000, .s = 0 } },
        .{ 1.5e-40, ScaledInt{ .m = 0, .s = 38 } },
        .{ 5.0e-38, ScaledInt{ .m = 5, .s = 38 } },
    };
    inline for (cases) |c| try t.expectEqual(c[1], floatDigits(c[0]).?);
    inline for (.{ 1e38, -1e39, std.math.inf(f64), std.math.nan(f64) }) |bad| try t.expect(floatDigits(bad) == null);
}

test "floatText: doubles and floats as MySQL 8.4 writes them" {
    const t = std.testing;
    const cases = .{
        .{ @as(f64, 1e100), "1e100" },
        .{ @as(f64, 1e15), "1e15" },
        .{ @as(f64, 1e14), "100000000000000" },
        .{ @as(f64, 1e16), "1e16" },
        .{ @as(f64, 1.5e17), "1.5e17" },
        .{ @as(f64, 1e15) + 0.5, "1000000000000000.5" },
        .{ @as(f64, 123456789012345678.0), "1.2345678901234568e17" },
        .{ @as(f64, 1234567890123456.7), "1234567890123456.8" },
        .{ @as(f64, 9.223372036854776e18), "9.223372036854776e18" },
        .{ @as(f64, 1.5e-16), "1.5e-16" },
        .{ @as(f64, 1e-15), "0.000000000000001" },
        .{ @as(f64, -2.5e-5), "-0.000025" },
        .{ @as(f64, 0.1) + @as(f64, 0.2), "0.30000000000000004" },
        .{ @as(f64, 100.0), "100" },
        .{ @as(f64, 0.0), "0" },
        .{ @as(f64, -0.0), "-0" },
        .{ @as(f64, 1.7976931348623157e308), "1.7976931348623157e308" },
        .{ @as(f64, 5e-324), "5e-324" },
        .{ std.math.inf(f64), "inf" },
        .{ -std.math.inf(f64), "-inf" },
        .{ @as(f32, 0.1), "0.1" },
        .{ @as(f32, 3.4e38), "3.4e38" },
        .{ @as(f32, 1234567.0), "1234567" },
        .{ @as(f32, 1e-10), "0.0000000001" },
    };
    var buf: [FLOAT_TEXT_MAX]u8 = undefined;
    inline for (cases) |c| try t.expectEqualStrings(c[1], floatText(&buf, c[0], .plain));
    try t.expectEqualStrings("100.0", floatText(&buf, @as(f64, 100), .json));
    try t.expectEqualStrings("0.0", floatText(&buf, @as(f64, 0), .json));
    try t.expectEqualStrings("1e15", floatText(&buf, @as(f64, 1e15), .json));
}

test "parseDateTimeString: fractions, date-only, Z, rejects" {
    try std.testing.expectEqual(@as(i64, 1783663005455833), try parseDateTimeString("2026-07-10 05:56:45.455833"));
    try std.testing.expectEqual(@as(i64, 1783663005455000), try parseDateTimeString("2026-07-10 05:56:45.455"));
    try std.testing.expectEqual(@as(i64, 1783663005000000), try parseDateTimeString("2026-07-10 05:56:45"));
    try std.testing.expectEqual(@as(i64, 1783641600000000), try parseDateTimeString("2026-07-10"));
    try std.testing.expectEqual(@as(i64, 1783663005455833), try parseDateTimeString("2026-07-10T05:56:45.455833Z"));
    try std.testing.expectError(error.Invalid, parseDateTimeString("2026-07-10 05:56"));
    try std.testing.expectError(error.Invalid, parseDateTimeString("2026-07-10 05:56:45."));
    try std.testing.expectError(error.Invalid, parseDateTimeString("2026-07-10 05:56:45.455833x"));
    try std.testing.expectError(error.Invalid, parseDateTimeString("2026-07-10x"));
    try std.testing.expectError(error.Invalid, parseDateTimeString("1783663005455833"));
}

test "a date's day must exist in its month, in years 0 to 9999" {
    const cases = .{
        .{ "0000-01-01", true },
        .{ "9999-12-31", true },
        .{ "-001-01-01", false },
        .{ "-999-12-31", false },
        .{ "2026-02-28", true },
        .{ "2026-02-29", false },
        .{ "2026-02-30", false },
        .{ "2026-04-31", false },
        .{ "2026-12-31", true },
        .{ "2024-02-29", true },
        .{ "2000-02-29", true },
        .{ "1900-02-29", false },
        .{ "0000-02-29", true },
        .{ "0100-02-29", false },
        .{ "2026-13-01", false },
        .{ "2026-00-10", false },
        .{ "2026-01-00", false },
    };
    inline for (cases) |c| {
        const valid = if (parseDateString(c[0])) |_| true else |_| false;
        try std.testing.expectEqual(c[1], valid);
    }
    try std.testing.expectError(error.Invalid, parseDateTimeString("2026-02-30 10:00:00"));
}

test "a date or datetime as MySQL's YYYYMMDD[HHMMSS] number" {
    try std.testing.expectEqual(@as(i64, 20260927), dateNumber(try parseDateString("2026-09-27")));
    try std.testing.expectEqual(@as(i64, 19691231), dateNumber(-1));
    try std.testing.expectEqual(ScaledInt{ .m = 20260927123456500000, .s = 6 }, datetimeNumber(try parseDateTimeString("2026-09-27 12:34:56.5")));
    try std.testing.expectEqual(ScaledInt{ .m = 19691231235959250000, .s = 6 }, datetimeNumber(try parseDateTimeString("1969-12-31 23:59:59.25")));
}

/// The canonical text of a DATE (`YYYY-MM-DD`), as the wire and `CAST(d AS CHAR)` print it.
pub fn formatDate(buf: []u8, days_since_epoch: i32) ![]const u8 {
    const ymd = daysToYmd(days_since_epoch);
    return std.fmt.bufPrint(buf, "{d:0>4}-{d:0>2}-{d:0>2}", .{ @as(u32, @intCast(ymd.year)), ymd.month, ymd.day });
}

/// The canonical text of a DATETIME: fractional seconds only when nonzero.
pub fn formatDateTime(buf: []u8, micros_since_epoch: i64) ![]const u8 {
    const ymd = daysToYmd(daysFromDatetime(micros_since_epoch));
    const hms = microsToHms(micros_since_epoch);
    // Zig 0.16's `{d:0>N}` prints a leading `+` for a signed value.
    const year: u32 = @intCast(ymd.year);
    const micros: u32 = @intCast(@mod(micros_since_epoch, std.time.us_per_s));
    if (micros == 0)
        return std.fmt.bufPrint(buf, "{d:0>4}-{d:0>2}-{d:0>2} {d:0>2}:{d:0>2}:{d:0>2}", .{ year, ymd.month, ymd.day, hms.hour, hms.minute, hms.second });
    return std.fmt.bufPrint(buf, "{d:0>4}-{d:0>2}-{d:0>2} {d:0>2}:{d:0>2}:{d:0>2}.{d:0>6}", .{ year, ymd.month, ymd.day, hms.hour, hms.minute, hms.second, micros });
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

test "daysToYmd round-trips ymdToDays and matches std's calendar" {
    // Walk every day from 1970-01-01 through 9999-12-31 beside a calendar
    // advanced a day at a time by std's month lengths; daysToYmd must agree
    // with it and be the exact inverse of ymdToDays. (std's EpochDay scans
    // year by year for each day, which took most of a minute here.)
    var year: std.time.epoch.Year = 1970;
    var month: u4 = 1;
    var day: u5 = 1;
    var days: i32 = 0;
    const last = ymdToDays(9999, 12, 31);
    while (days <= last) : (days += 1) {
        const ymd = daysToYmd(days);
        try std.testing.expectEqual(days, ymdToDays(ymd.year, ymd.month, ymd.day));
        try std.testing.expectEqual(@as(i32, year), ymd.year);
        try std.testing.expectEqual(month, ymd.month);
        try std.testing.expectEqual(day, ymd.day);

        if (day < std.time.epoch.getDaysInMonth(year, @enumFromInt(month))) {
            day += 1;
        } else if (month < 12) {
            day = 1;
            month += 1;
        } else {
            day = 1;
            month = 1;
            year += 1;
        }
    }
}

test "every date from 0000-01-01 to 9999-12-31 prints as the day it parses from (issue #393)" {
    // Year 0 is a leap year: 0000-01-01 is 366 days before 0001-01-01 and
    // 719,528 before the epoch, as StarRocks counts them.
    try std.testing.expectEqual(@as(i32, -719_528), ymdToDays(0, 1, 1));
    try std.testing.expectEqual(@as(i32, -719_469), ymdToDays(0, 2, 29));
    try std.testing.expectEqual(@as(i32, -719_162), ymdToDays(1, 1, 1));
    var buf: [32]u8 = undefined;
    var days = ymdToDays(0, 1, 1);
    const last = ymdToDays(9999, 12, 31);
    while (days <= last) : (days += 1) {
        const text = try formatDate(&buf, days);
        const back = parseDateString(text) catch |err| {
            std.debug.print("{d} prints as {s}\n", .{ days, text });
            return err;
        };
        try std.testing.expectEqual(days, back);
    }
    const cases = .{
        .{ "0000-01-01 00:00:00", "0000-01-01 00:00:00" },
        .{ "0000-02-28 23:59:59.5", "0000-02-28 23:59:59.500000" },
        .{ "0000-02-29 12:00:00", "0000-02-29 12:00:00" },
        .{ "0000-12-31 23:59:59.000001", "0000-12-31 23:59:59.000001" },
        .{ "1969-12-31 23:59:59.5", "1969-12-31 23:59:59.500000" },
        .{ "1900-06-15 10:20:30.25", "1900-06-15 10:20:30.250000" },
    };
    inline for (cases) |c| try std.testing.expectEqualStrings(c[1], try formatDateTime(&buf, try parseDateTimeString(c[0])));
}

test "daysToYmd and microsToHms before the epoch" {
    var days = ymdToDays(0, 1, 1);
    while (days < 0) : (days += 1) {
        const ymd = daysToYmd(days);
        try std.testing.expectEqual(days, ymdToDays(ymd.year, ymd.month, ymd.day));
    }
    const d = daysToYmd(ymdToDays(1965, 3, 5));
    try std.testing.expectEqual(@as(i32, 1965), d.year);
    try std.testing.expectEqual(@as(u4, 3), d.month);
    try std.testing.expectEqual(@as(u5, 5), d.day);
    const t = microsToHms((@as(i64, ymdToDays(1965, 3, 5)) * 86_400 + 10 * 3600 + 7 * 60 + 9) * std.time.us_per_s + 250);
    try std.testing.expectEqual(@as(u5, 10), t.hour);
    try std.testing.expectEqual(@as(u6, 7), t.minute);
    try std.testing.expectEqual(@as(u6, 9), t.second);
}

/// Expected `?[]const u8` against a read date or datetime, compared as the
/// text StarRocks prints.
const ExpectRead = struct {
    fn date(expected: ?[]const u8, got: ?i32) !void {
        var buf: [32]u8 = undefined;
        const text: ?[]const u8 = if (got) |days| try formatDate(&buf, days) else null;
        try expectOptionalText(expected, text);
    }

    fn datetime(expected: ?[]const u8, got: ?i64) !void {
        var buf: [32]u8 = undefined;
        const text: ?[]const u8 = if (got) |micros| try formatDateTime(&buf, micros) else null;
        try expectOptionalText(expected, text);
    }

    fn expectOptionalText(expected: ?[]const u8, got: ?[]const u8) !void {
        if (expected == null or got == null) return std.testing.expectEqual(expected == null, got == null);
        try std.testing.expectEqualStrings(expected.?, got.?);
    }
};

test "text reads as a date as StarRocks casts a column: CAST AS DATE, DATE(), CAST AS DATETIME (issue #399)" {
    // StarRocks 4.0's results, with the text wrapped in IF(RAND() < 2, x, NULL)
    // so that its backend casts it: the frontend folds a constant differently.
    const Case = struct { text: []const u8, cast_date: ?[]const u8, date_fn: ?[]const u8, cast_datetime: ?[]const u8 };
    const cases = [_]Case{
        .{ .text = "2026-01-01", .cast_date = "2026-01-01", .date_fn = "2026-01-01", .cast_datetime = "2026-01-01 00:00:00" },
        .{ .text = "2026-1-1", .cast_date = "2026-01-01", .date_fn = "2026-01-01", .cast_datetime = "2026-01-01 00:00:00" },
        .{ .text = "2026-01-1", .cast_date = "2026-01-01", .date_fn = "2026-01-01", .cast_datetime = "2026-01-01 00:00:00" },
        .{ .text = "20260101", .cast_date = "2026-01-01", .date_fn = "2026-01-01", .cast_datetime = "2026-01-01 00:00:00" },
        .{ .text = "260101", .cast_date = "2026-01-01", .date_fn = "2026-01-01", .cast_datetime = "2026-01-01 00:00:00" },
        .{ .text = "2026/01/01", .cast_date = "2026-01-01", .date_fn = "2026-01-01", .cast_datetime = "2026-01-01 00:00:00" },
        .{ .text = "2026.01.01", .cast_date = "2026-01-01", .date_fn = "2026-01-01", .cast_datetime = "2026-01-01 00:00:00" },
        .{ .text = "2026:01:01", .cast_date = "2026-01-01", .date_fn = "2026-01-01", .cast_datetime = "2026-01-01 00:00:00" },
        .{ .text = "2026 01 01", .cast_date = "2026-01-01", .date_fn = "2026-01-01", .cast_datetime = "2026-01-01 00:00:00" },
        .{ .text = "2026-01-01 10:00:00", .cast_date = "2026-01-01", .date_fn = "2026-01-01", .cast_datetime = "2026-01-01 10:00:00" },
        .{ .text = "2026-01-01T10:00:00", .cast_date = "2026-01-01", .date_fn = "2026-01-01", .cast_datetime = "2026-01-01 10:00:00" },
        .{ .text = "2026-01-01 garbage", .cast_date = "2026-01-01", .date_fn = "2026-01-01", .cast_datetime = "2026-01-01 00:00:00" },
        .{ .text = " 2026-01-01", .cast_date = "2026-01-01", .date_fn = "2026-01-01", .cast_datetime = "2026-01-01 00:00:00" },
        .{ .text = "2026-01-01 ", .cast_date = "2026-01-01", .date_fn = "2026-01-01", .cast_datetime = "2026-01-01 00:00:00" },
        .{ .text = "26-01-01", .cast_date = "2026-01-01", .date_fn = "2026-01-01", .cast_datetime = "2026-01-01 00:00:00" },
        .{ .text = "6-1-1", .cast_date = "0006-01-01", .date_fn = "0006-01-01", .cast_datetime = "0006-01-01 00:00:00" },
        .{ .text = "0-1-1", .cast_date = "0000-01-01", .date_fn = "0000-01-01", .cast_datetime = "0000-01-01 00:00:00" },
        .{ .text = "00000101", .cast_date = "0000-01-01", .date_fn = "0000-01-01", .cast_datetime = "0000-01-01 00:00:00" },
        .{ .text = "99991231", .cast_date = "9999-12-31", .date_fn = "9999-12-31", .cast_datetime = "9999-12-31 00:00:00" },
        .{ .text = "100000101", .cast_date = null, .date_fn = null, .cast_datetime = null },
        .{ .text = "2026-13-01", .cast_date = null, .date_fn = null, .cast_datetime = null },
        .{ .text = "2026-02-30", .cast_date = null, .date_fn = null, .cast_datetime = null },
        .{ .text = "2026-00-01", .cast_date = null, .date_fn = null, .cast_datetime = null },
        .{ .text = "0000-00-00", .cast_date = null, .date_fn = null, .cast_datetime = null },
        .{ .text = "2026", .cast_date = null, .date_fn = null, .cast_datetime = null },
        .{ .text = "202601", .cast_date = null, .date_fn = null, .cast_datetime = null },
        .{ .text = "2026-01", .cast_date = null, .date_fn = null, .cast_datetime = null },
        .{ .text = "+2026-01-01", .cast_date = null, .date_fn = null, .cast_datetime = null },
        .{ .text = "1_23-01-01", .cast_date = null, .date_fn = null, .cast_datetime = null },
        .{ .text = "-001-01-01", .cast_date = null, .date_fn = null, .cast_datetime = null },
        .{ .text = "2026--01-01", .cast_date = "2026-01-01", .date_fn = "2026-01-01", .cast_datetime = "2026-01-01 00:00:00" },
        .{ .text = "2026-01-01x", .cast_date = "2026-01-01", .date_fn = "2026-01-01", .cast_datetime = "2026-01-01 00:00:00" },
        .{ .text = "20260101103000", .cast_date = "2026-01-01", .date_fn = "2026-01-01", .cast_datetime = "2026-01-01 10:30:00" },
        .{ .text = "20260101 103000", .cast_date = null, .date_fn = null, .cast_datetime = null },
        .{ .text = "2026-01-01 10", .cast_date = "2026-01-01", .date_fn = "2026-01-01", .cast_datetime = "2026-01-01 10:00:00" },
        .{ .text = "2026-1-1 1:2:3", .cast_date = "2026-01-01", .date_fn = "2026-01-01", .cast_datetime = "2026-01-01 01:02:03" },
        .{ .text = "2026-01-01 25:00:00", .cast_date = "2026-01-01", .date_fn = null, .cast_datetime = null },
        .{ .text = "2026-01-01 10:00:00.123", .cast_date = "2026-01-01", .date_fn = "2026-01-01", .cast_datetime = "2026-01-01 10:00:00.123000" },
        .{ .text = "12026-01-01", .cast_date = null, .date_fn = null, .cast_datetime = null },
        .{ .text = "2026-001-01", .cast_date = "2026-01-01", .date_fn = "2026-01-01", .cast_datetime = "2026-01-01 00:00:00" },
        .{ .text = "2026-01-001", .cast_date = "2026-01-01", .date_fn = "2026-01-01", .cast_datetime = "2026-01-01 00:00:00" },
        .{ .text = "x2026-01-01", .cast_date = null, .date_fn = null, .cast_datetime = null },
        .{ .text = "", .cast_date = null, .date_fn = null, .cast_datetime = null },
        .{ .text = " ", .cast_date = null, .date_fn = null, .cast_datetime = null },
        .{ .text = "2026/1/1", .cast_date = "2026-01-01", .date_fn = "2026-01-01", .cast_datetime = "2026-01-01 00:00:00" },
        .{ .text = "2026-01-01Z", .cast_date = "2026-01-01", .date_fn = "2026-01-01", .cast_datetime = "2026-01-01 00:00:00" },
        .{ .text = "2026-01-01T", .cast_date = "2026-01-01", .date_fn = "2026-01-01", .cast_datetime = "2026-01-01 00:00:00" },
        .{ .text = "26-1-1", .cast_date = "2026-01-01", .date_fn = "2026-01-01", .cast_datetime = "2026-01-01 00:00:00" },
        .{ .text = "69-01-01", .cast_date = "2069-01-01", .date_fn = "2069-01-01", .cast_datetime = "2069-01-01 00:00:00" },
        .{ .text = "70-01-01", .cast_date = "1970-01-01", .date_fn = "1970-01-01", .cast_datetime = "1970-01-01 00:00:00" },
        .{ .text = "0069-01-01", .cast_date = "0069-01-01", .date_fn = "0069-01-01", .cast_datetime = "0069-01-01 00:00:00" },
        .{ .text = "690101", .cast_date = "2069-01-01", .date_fn = "2069-01-01", .cast_datetime = "2069-01-01 00:00:00" },
        .{ .text = "700101", .cast_date = "1970-01-01", .date_fn = "1970-01-01", .cast_datetime = "1970-01-01 00:00:00" },
        .{ .text = "0101", .cast_date = null, .date_fn = null, .cast_datetime = null },
        .{ .text = "101", .cast_date = null, .date_fn = null, .cast_datetime = null },
        .{ .text = "1231", .cast_date = null, .date_fn = null, .cast_datetime = null },
        .{ .text = "2026-1", .cast_date = null, .date_fn = null, .cast_datetime = null },
        .{ .text = "2026-01-01 00:00:00 UTC", .cast_date = "2026-01-01", .date_fn = "2026-01-01", .cast_datetime = "2026-01-01 00:00:00" },
        .{ .text = "2026-01-01 10:61:00", .cast_date = "2026-01-01", .date_fn = null, .cast_datetime = null },
        .{ .text = "2026-01-01 10:00:61", .cast_date = "2026-01-01", .date_fn = null, .cast_datetime = null },
        .{ .text = "2026-01-01 1:2", .cast_date = "2026-01-01", .date_fn = "2026-01-01", .cast_datetime = "2026-01-01 01:02:00" },
        .{ .text = "2026-01-01  10:00:00", .cast_date = "2026-01-01", .date_fn = "2026-01-01", .cast_datetime = "2026-01-01 10:00:00" },
        .{ .text = "2026_01_01", .cast_date = "2026-01-01", .date_fn = "2026-01-01", .cast_datetime = "2026-01-01 00:00:00" },
        .{ .text = "2026-01-01-", .cast_date = "2026-01-01", .date_fn = "2026-01-01", .cast_datetime = "2026-01-01 00:00:00" },
        .{ .text = "2026-1-01T1:2:3", .cast_date = "2026-01-01", .date_fn = "2026-01-01", .cast_datetime = "2026-01-01 01:02:03" },
        .{ .text = "260101103000", .cast_date = "2026-01-01", .date_fn = "2026-01-01", .cast_datetime = "2026-01-01 10:30:00" },
        .{ .text = "2601011030", .cast_date = "2026-01-01", .date_fn = "2026-01-01", .cast_datetime = "2026-01-01 10:30:00" },
        .{ .text = "2026010110", .cast_date = null, .date_fn = null, .cast_datetime = null },
        .{ .text = "2026-01-01 10:00:00.1234567", .cast_date = "2026-01-01", .date_fn = "2026-01-01", .cast_datetime = "2026-01-01 10:00:00.123456" },
        .{ .text = "9999-12-31 23:59:59.9999999", .cast_date = "9999-12-31", .date_fn = "9999-12-31", .cast_datetime = "9999-12-31 23:59:59.999999" },
        .{ .text = "0000-01-01", .cast_date = "0000-01-01", .date_fn = "0000-01-01", .cast_datetime = "0000-01-01 00:00:00" },
        .{ .text = "0001-01-01", .cast_date = "0001-01-01", .date_fn = "0001-01-01", .cast_datetime = "0001-01-01 00:00:00" },
        .{ .text = "2026-01 01", .cast_date = "2026-01-01", .date_fn = "2026-01-01", .cast_datetime = "2026-01-01 00:00:00" },
        .{ .text = "2026 01-01", .cast_date = "2026-01-01", .date_fn = "2026-01-01", .cast_datetime = "2026-01-01 00:00:00" },
        .{ .text = "2026-01-01 10 30 00", .cast_date = null, .date_fn = "2026-01-01", .cast_datetime = "2026-01-01 10:30:00" },
        .{ .text = "2026-01-01 10:30 00", .cast_date = null, .date_fn = "2026-01-01", .cast_datetime = "2026-01-01 10:30:00" },
        .{ .text = "2026-01-01 10: 30:00", .cast_date = null, .date_fn = null, .cast_datetime = null },
        .{ .text = "2026-01-01 10 :30:00", .cast_date = null, .date_fn = null, .cast_datetime = null },
        .{ .text = "9901011", .cast_date = "1999-01-01", .date_fn = "1999-01-01", .cast_datetime = "1999-01-01 01:00:00" },
        .{ .text = "99010112", .cast_date = "9901-01-12", .date_fn = "9901-01-12", .cast_datetime = "9901-01-12 00:00:00" },
        .{ .text = "990101123", .cast_date = "1999-01-01", .date_fn = "1999-01-01", .cast_datetime = "1999-01-01 12:03:00" },
        .{ .text = "9901011230", .cast_date = "1999-01-01", .date_fn = "1999-01-01", .cast_datetime = "1999-01-01 12:30:00" },
        .{ .text = "99010112304", .cast_date = "1999-01-01", .date_fn = "1999-01-01", .cast_datetime = "1999-01-01 12:30:04" },
        .{ .text = "990101123045", .cast_date = "1999-01-01", .date_fn = "1999-01-01", .cast_datetime = "1999-01-01 12:30:45" },
        .{ .text = "9901011230451", .cast_date = "1999-01-01", .date_fn = "1999-01-01", .cast_datetime = "1999-01-01 12:30:45" },
        .{ .text = "202601011030001", .cast_date = "2026-01-01", .date_fn = "2026-01-01", .cast_datetime = "2026-01-01 10:30:00" },
        .{ .text = "20260101103000.5", .cast_date = "2026-01-01", .date_fn = "2026-01-01", .cast_datetime = "2026-01-01 10:30:00.500000" },
        .{ .text = "260101103000.5", .cast_date = "2026-01-01", .date_fn = "2026-01-01", .cast_datetime = "2026-01-01 10:30:00.500000" },
        .{ .text = "20260101.5", .cast_date = "2026-01-01", .date_fn = "2026-01-01", .cast_datetime = "2026-01-01 05:00:00" },
        .{ .text = "260101.5", .cast_date = "2026-01-01", .date_fn = "2026-01-01", .cast_datetime = "2026-01-01 05:00:00" },
        .{ .text = "2026010110.5", .cast_date = null, .date_fn = null, .cast_datetime = null },
        .{ .text = "0026-01-01", .cast_date = "0026-01-01", .date_fn = "0026-01-01", .cast_datetime = "0026-01-01 00:00:00" },
        .{ .text = "00-01-01", .cast_date = "2000-01-01", .date_fn = "2000-01-01", .cast_datetime = "2000-01-01 00:00:00" },
        .{ .text = "000101", .cast_date = "2000-01-01", .date_fn = "2000-01-01", .cast_datetime = "2000-01-01 00:00:00" },
        .{ .text = "000000", .cast_date = null, .date_fn = null, .cast_datetime = null },
        .{ .text = "00000000", .cast_date = null, .date_fn = null, .cast_datetime = null },
        .{ .text = "2026-01-01 24:00:00", .cast_date = "2026-01-01", .date_fn = null, .cast_datetime = null },
        .{ .text = "2026-01-01 10:30:00.", .cast_date = "2026-01-01", .date_fn = "2026-01-01", .cast_datetime = "2026-01-01 10:30:00" },
        .{ .text = "2026-01-01 10:30:00.abc", .cast_date = "2026-01-01", .date_fn = "2026-01-01", .cast_datetime = "2026-01-01 10:30:00" },
        .{ .text = "2026-01-01 10:30:00:123", .cast_date = "2026-01-01", .date_fn = "2026-01-01", .cast_datetime = "2026-01-01 10:30:00" },
        .{ .text = "2026-01-01 10.30.00", .cast_date = "2026-01-01", .date_fn = "2026-01-01", .cast_datetime = "2026-01-01 10:30:00" },
        .{ .text = "2026-01-01T10", .cast_date = "2026-01-01", .date_fn = "2026-01-01", .cast_datetime = "2026-01-01 10:00:00" },
        .{ .text = "20260101T103000", .cast_date = "2026-01-01", .date_fn = "2026-01-01", .cast_datetime = "2026-01-01 10:30:00" },
        .{ .text = "20260101T10:30:00", .cast_date = null, .date_fn = null, .cast_datetime = null },
        .{ .text = "2026-01-01 10:30:00.123456789", .cast_date = "2026-01-01", .date_fn = "2026-01-01", .cast_datetime = "2026-01-01 10:30:00.123456" },
        .{ .text = "2026-01-01 1:2:3.4", .cast_date = "2026-01-01", .date_fn = "2026-01-01", .cast_datetime = "2026-01-01 01:02:03.400000" },
        .{ .text = "2026-01-0000001", .cast_date = "2026-01-01", .date_fn = "2026-01-01", .cast_datetime = "2026-01-01 00:00:00" },
        .{ .text = "2026-01-1000000", .cast_date = null, .date_fn = null, .cast_datetime = null },
        .{ .text = "2026-01-01 10:30:00 5", .cast_date = "2026-01-01", .date_fn = "2026-01-01", .cast_datetime = "2026-01-01 10:30:00" },
        .{ .text = "2026-01-01 10:30:00.5 x", .cast_date = "2026-01-01", .date_fn = "2026-01-01", .cast_datetime = "2026-01-01 10:30:00.500000" },
        .{ .text = "20260101253000", .cast_date = "2026-01-01", .date_fn = null, .cast_datetime = null },
        .{ .text = "2026-01-01 garbage:99", .cast_date = "2026-01-01", .date_fn = "2026-01-01", .cast_datetime = "2026-01-01 00:00:00" },
        .{ .text = "2026-01-01 99", .cast_date = "2026-01-01", .date_fn = null, .cast_datetime = null },
        .{ .text = "2026-01-01 10:99", .cast_date = "2026-01-01", .date_fn = null, .cast_datetime = null },
        .{ .text = "2026-1-1 25", .cast_date = "2026-01-01", .date_fn = null, .cast_datetime = null },
        .{ .text = "2026-01-01 -10:00:00", .cast_date = "2026-01-01", .date_fn = "2026-01-01", .cast_datetime = "2026-01-01 10:00:00" },
        .{ .text = "2026-01-01 10:30:00.999999999", .cast_date = "2026-01-01", .date_fn = "2026-01-01", .cast_datetime = "2026-01-01 10:30:00.999999" },
        .{ .text = "2026-01-01 23:59:59.9999995", .cast_date = "2026-01-01", .date_fn = "2026-01-01", .cast_datetime = "2026-01-01 23:59:59.999999" },
        .{ .text = "2026-01-01+10:00:00", .cast_date = "2026-01-01", .date_fn = "2026-01-01", .cast_datetime = "2026-01-01 10:00:00" },
        .{ .text = "2026-01-01/10/30/00", .cast_date = "2026-01-01", .date_fn = "2026-01-01", .cast_datetime = "2026-01-01 10:30:00" },
        .{ .text = "2026-01-01T10:30:00Z", .cast_date = "2026-01-01", .date_fn = "2026-01-01", .cast_datetime = "2026-01-01 10:30:00" },
        .{ .text = "2026-01-01T10:30:00+08:00", .cast_date = "2026-01-01", .date_fn = "2026-01-01", .cast_datetime = "2026-01-01 10:30:00" },
        .{ .text = "2026-01-01 10:30:00 +08:00", .cast_date = "2026-01-01", .date_fn = "2026-01-01", .cast_datetime = "2026-01-01 10:30:00" },
        .{ .text = "2026-02-29", .cast_date = null, .date_fn = null, .cast_datetime = null },
        .{ .text = "2024-02-29", .cast_date = "2024-02-29", .date_fn = "2024-02-29", .cast_datetime = "2024-02-29 00:00:00" },
        .{ .text = "1900-02-29", .cast_date = null, .date_fn = null, .cast_datetime = null },
        .{ .text = "2000-02-29", .cast_date = "2000-02-29", .date_fn = "2000-02-29", .cast_datetime = "2000-02-29 00:00:00" },
        .{ .text = "9999-12-31 23:59:59", .cast_date = "9999-12-31", .date_fn = "9999-12-31", .cast_datetime = "9999-12-31 23:59:59" },
        .{ .text = "9999-12-31 24:00:00", .cast_date = "9999-12-31", .date_fn = null, .cast_datetime = null },
        .{ .text = "10000-01-01", .cast_date = null, .date_fn = null, .cast_datetime = null },
        .{ .text = "2026-01-01 00:00:00.000001", .cast_date = "2026-01-01", .date_fn = "2026-01-01", .cast_datetime = "2026-01-01 00:00:00.000001" },
        .{ .text = "2026-1-1 0:0:0", .cast_date = "2026-01-01", .date_fn = "2026-01-01", .cast_datetime = "2026-01-01 00:00:00" },
        .{ .text = "2026-01-01 000000", .cast_date = "2026-01-01", .date_fn = "2026-01-01", .cast_datetime = "2026-01-01 00:00:00" },
        .{ .text = "2026-01-01 103000", .cast_date = "2026-01-01", .date_fn = null, .cast_datetime = null },
        .{ .text = "2026-01-01 1030", .cast_date = "2026-01-01", .date_fn = null, .cast_datetime = null },
        .{ .text = "2026-01-01T103000", .cast_date = "2026-01-01", .date_fn = null, .cast_datetime = null },
        .{ .text = "20260101 10:30:00", .cast_date = null, .date_fn = null, .cast_datetime = null },
        .{ .text = "26-01-01 10:30", .cast_date = "2026-01-01", .date_fn = "2026-01-01", .cast_datetime = "2026-01-01 10:30:00" },
        .{ .text = "69-12-31 23:59:59", .cast_date = "2069-12-31", .date_fn = "2069-12-31", .cast_datetime = "2069-12-31 23:59:59" },
        .{ .text = "1-1-1", .cast_date = "0001-01-01", .date_fn = "0001-01-01", .cast_datetime = "0001-01-01 00:00:00" },
        .{ .text = "01-1-1", .cast_date = "2001-01-01", .date_fn = "2001-01-01", .cast_datetime = "2001-01-01 00:00:00" },
        .{ .text = "001-1-1", .cast_date = "0001-01-01", .date_fn = "0001-01-01", .cast_datetime = "0001-01-01 00:00:00" },
        .{ .text = "0001-1-1", .cast_date = "0001-01-01", .date_fn = "0001-01-01", .cast_datetime = "0001-01-01 00:00:00" },
        .{ .text = "99-1-1", .cast_date = "1999-01-01", .date_fn = "1999-01-01", .cast_datetime = "1999-01-01 00:00:00" },
        .{ .text = "100-1-1", .cast_date = "0100-01-01", .date_fn = "0100-01-01", .cast_datetime = "0100-01-01 00:00:00" },
        .{ .text = "999-1-1", .cast_date = "0999-01-01", .date_fn = "0999-01-01", .cast_datetime = "0999-01-01 00:00:00" },
        .{ .text = "12-12-12", .cast_date = "2012-12-12", .date_fn = "2012-12-12", .cast_datetime = "2012-12-12 00:00:00" },
        .{ .text = "123-12-12", .cast_date = "0123-12-12", .date_fn = "0123-12-12", .cast_datetime = "0123-12-12 00:00:00" },
        .{ .text = "2026-01-01 10:30:00.1", .cast_date = "2026-01-01", .date_fn = "2026-01-01", .cast_datetime = "2026-01-01 10:30:00.100000" },
        .{ .text = "2026-01-01 10:30:00.12", .cast_date = "2026-01-01", .date_fn = "2026-01-01", .cast_datetime = "2026-01-01 10:30:00.120000" },
        .{ .text = "2026-01-01 10:30:00.000", .cast_date = "2026-01-01", .date_fn = "2026-01-01", .cast_datetime = "2026-01-01 10:30:00" },
        .{ .text = "2026-01-01 10:30:0.5", .cast_date = "2026-01-01", .date_fn = "2026-01-01", .cast_datetime = "2026-01-01 10:30:00.500000" },
        .{ .text = "2026-01-01 1:2:3.456789", .cast_date = "2026-01-01", .date_fn = "2026-01-01", .cast_datetime = "2026-01-01 01:02:03.456789" },
        .{ .text = "2026-01-01 10:30:60", .cast_date = "2026-01-01", .date_fn = null, .cast_datetime = null },
        .{ .text = "2026-01-01 10:60:00", .cast_date = "2026-01-01", .date_fn = null, .cast_datetime = null },
        .{ .text = "2026-01-32", .cast_date = null, .date_fn = null, .cast_datetime = null },
        .{ .text = "2026-04-31", .cast_date = null, .date_fn = null, .cast_datetime = null },
        .{ .text = "2026-12-31", .cast_date = "2026-12-31", .date_fn = "2026-12-31", .cast_datetime = "2026-12-31 00:00:00" },
        .{ .text = "2026--1-1", .cast_date = "2026-01-01", .date_fn = "2026-01-01", .cast_datetime = "2026-01-01 00:00:00" },
        .{ .text = "2026-01-01T10:30", .cast_date = "2026-01-01", .date_fn = "2026-01-01", .cast_datetime = "2026-01-01 10:30:00" },
        .{ .text = "2026-01-01T", .cast_date = "2026-01-01", .date_fn = "2026-01-01", .cast_datetime = "2026-01-01 00:00:00" },
        .{ .text = "2026-01-01TT10", .cast_date = "2026-01-01", .date_fn = "2026-01-01", .cast_datetime = "2026-01-01 00:00:00" },
        .{ .text = "2026-01-01 10:30:00.5.5", .cast_date = "2026-01-01", .date_fn = "2026-01-01", .cast_datetime = "2026-01-01 10:30:00.500000" },
        .{ .text = "  2026-01-01  ", .cast_date = "2026-01-01", .date_fn = "2026-01-01", .cast_datetime = "2026-01-01 00:00:00" },
        .{ .text = "2026-01-01\t", .cast_date = "2026-01-01", .date_fn = "2026-01-01", .cast_datetime = "2026-01-01 00:00:00" },
        .{ .text = "1", .cast_date = null, .date_fn = null, .cast_datetime = null },
        .{ .text = "12", .cast_date = null, .date_fn = null, .cast_datetime = null },
        .{ .text = "123456789012345678", .cast_date = null, .date_fn = null, .cast_datetime = null },
        .{ .text = "20261", .cast_date = null, .date_fn = null, .cast_datetime = null },
        .{ .text = "2026011", .cast_date = null, .date_fn = null, .cast_datetime = null },
        .{ .text = "20260101 ", .cast_date = null, .date_fn = null, .cast_datetime = null },
        .{ .text = " 20260101", .cast_date = "2026-01-01", .date_fn = "2026-01-01", .cast_datetime = "2026-01-01 00:00:00" },
        .{ .text = "2026-01-01 10:30:00 PM", .cast_date = "2026-01-01", .date_fn = "2026-01-01", .cast_datetime = "2026-01-01 10:30:00" },
    };
    for (cases) |c| {
        errdefer std.debug.print("text: '{s}'\n", .{c.text});
        try ExpectRead.date(c.cast_date, textToDate(c.text));
        const datetime = textToDatetime(c.text);
        try ExpectRead.date(c.date_fn, if (datetime) |micros| daysFromDatetime(micros) else null);
        try ExpectRead.datetime(c.cast_datetime, datetime);
    }
    // StarRocks reads '0000-02-29' as NULL. thinDB's calendar makes year 0 a
    // leap year (DESIGN.md §3.1), so the day is there.
    try ExpectRead.date("0000-02-29", textToDate("0000-02-29"));
    try ExpectRead.datetime("0000-02-29 10:00:00", textToDatetime("0000-02-29 10:00:00"));
}

test "numbers read as a date as StarRocks casts a column (issue #399)" {
    // DATE(n) gives what CAST(n AS DATE) does for every one of these.
    const Case = struct { number: i64, cast_date: ?[]const u8, cast_datetime: ?[]const u8 };
    const cases = [_]Case{
        .{ .number = 20260101, .cast_date = "2026-01-01", .cast_datetime = "2026-01-01 00:00:00" },
        .{ .number = 260101, .cast_date = "2026-01-01", .cast_datetime = "2026-01-01 00:00:00" },
        .{ .number = 20260101103000, .cast_date = "2026-01-01", .cast_datetime = "2026-01-01 10:30:00" },
        .{ .number = 0, .cast_date = null, .cast_datetime = null },
        .{ .number = 101, .cast_date = "2000-01-01", .cast_datetime = "2000-01-01 00:00:00" },
        .{ .number = 2026, .cast_date = null, .cast_datetime = null },
        .{ .number = 20261301, .cast_date = null, .cast_datetime = null },
        .{ .number = -20260101, .cast_date = null, .cast_datetime = null },
        .{ .number = 99991231, .cast_date = "9999-12-31", .cast_datetime = "9999-12-31 00:00:00" },
        .{ .number = 100000101, .cast_date = null, .cast_datetime = null },
        .{ .number = 1231, .cast_date = "2000-12-31", .cast_datetime = "2000-12-31 00:00:00" },
        .{ .number = 70101, .cast_date = "2007-01-01", .cast_datetime = "2007-01-01 00:00:00" },
        .{ .number = 691231, .cast_date = "2069-12-31", .cast_datetime = "2069-12-31 00:00:00" },
        .{ .number = 20260230, .cast_date = null, .cast_datetime = null },
        .{ .number = 260101103000, .cast_date = "2026-01-01", .cast_datetime = "2026-01-01 10:30:00" },
        .{ .number = 2601011030, .cast_date = null, .cast_datetime = null },
        .{ .number = 20260101000000, .cast_date = "2026-01-01", .cast_datetime = "2026-01-01 00:00:00" },
        .{ .number = 20260101235959, .cast_date = "2026-01-01", .cast_datetime = "2026-01-01 23:59:59" },
        .{ .number = 20260101240000, .cast_date = null, .cast_datetime = null },
        .{ .number = 991231, .cast_date = "1999-12-31", .cast_datetime = "1999-12-31 00:00:00" },
        .{ .number = 991231235959, .cast_date = "1999-12-31", .cast_datetime = "1999-12-31 23:59:59" },
        .{ .number = 19991231, .cast_date = "1999-12-31", .cast_datetime = "1999-12-31 00:00:00" },
        .{ .number = 10000101, .cast_date = "1000-01-01", .cast_datetime = "1000-01-01 00:00:00" },
        .{ .number = 101000000, .cast_date = "2000-01-01", .cast_datetime = "2000-01-01 00:00:00" },
        .{ .number = 1000000, .cast_date = null, .cast_datetime = null },
        .{ .number = 10101, .cast_date = "2001-01-01", .cast_datetime = "2001-01-01 00:00:00" },
        .{ .number = 100, .cast_date = null, .cast_datetime = null },
        .{ .number = 99, .cast_date = null, .cast_datetime = null },
        .{ .number = 1, .cast_date = null, .cast_datetime = null },
        .{ .number = 9991231, .cast_date = null, .cast_datetime = null },
        .{ .number = 99999999999999, .cast_date = null, .cast_datetime = null },
        .{ .number = 100000000000000, .cast_date = null, .cast_datetime = null },
        .{ .number = -1, .cast_date = null, .cast_datetime = null },
        .{ .number = 1000101, .cast_date = null, .cast_datetime = null },
        .{ .number = 69, .cast_date = null, .cast_datetime = null },
        .{ .number = 70, .cast_date = null, .cast_datetime = null },
        .{ .number = 691231235959, .cast_date = "2069-12-31", .cast_datetime = "2069-12-31 23:59:59" },
        .{ .number = 700101000000, .cast_date = "1970-01-01", .cast_datetime = "1970-01-01 00:00:00" },
        .{ .number = 5000101000000, .cast_date = "0500-01-01", .cast_datetime = "0500-01-01 00:00:00" },
        .{ .number = 991231235960, .cast_date = null, .cast_datetime = null },
        .{ .number = 1000101000000, .cast_date = "0100-01-01", .cast_datetime = "0100-01-01 00:00:00" },
        .{ .number = 9991231235959, .cast_date = "0999-12-31", .cast_datetime = "0999-12-31 23:59:59" },
        .{ .number = 10000101000000, .cast_date = "1000-01-01", .cast_datetime = "1000-01-01 00:00:00" },
    };
    for (cases) |c| {
        errdefer std.debug.print("number: {d}\n", .{c.number});
        try ExpectRead.date(c.cast_date, numberToDate(c.number));
        try ExpectRead.datetime(c.cast_datetime, numberToDatetime(c.number));
        try ExpectRead.date(c.cast_date, doubleToDate(@floatFromInt(c.number)));
        try ExpectRead.datetime(c.cast_datetime, doubleToDatetime(@floatFromInt(c.number)));
    }
    // A double's fraction is dropped, toward zero.
    try ExpectRead.datetime("2026-01-01 00:00:00", doubleToDatetime(20260101.9));
    try ExpectRead.datetime("2026-01-01 23:59:59", doubleToDatetime(20260101235959.99));
    try ExpectRead.datetime("1999-12-31 23:59:59", doubleToDatetime(991231235959.5));
    try ExpectRead.date(null, doubleToDate(100.9));
    try ExpectRead.date(null, doubleToDate(-0.5));
    try ExpectRead.date(null, doubleToDate(1e20));
    try ExpectRead.date(null, doubleToDate(std.math.nan(f64)));
}
