//! MySQL's TIME functions. thinDB has no TIME column type: a TIME value is
//! its text, `[-]HH:MM:SS[.ffffff]` with up to 838 hours, as a TIME column
//! holds it. Text reads the way MySQL's str_to_time reads it, and results
//! print with as many fraction digits as MySQL shows.

const std = @import("std");
const Allocator = std.mem.Allocator;

const types = @import("../types.zig");
const Type = types.Type;
const common = @import("scalar_fn_common.zig");
const dec = @import("scalar_fn_decimal.zig");
const ColumnView = common.ColumnView;
const ColumnStore = common.ColumnStore;

const Kernel = *const fn (allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) anyerror!void;

const US_PER_S = std.time.us_per_s;
const US_PER_DAY = std.time.us_per_day;

/// 838:59:59, the largest TIME.
pub const MAX_TIME_MICROS: i64 = (838 * 3600 + 59 * 60 + 59) * US_PER_S;

/// Microseconds and the fraction digits they show.
pub const Micros = struct { value: i64, fsp: u8 };

pub const Temporal = union(enum) {
    /// Signed, within ±838:59:59.
    time: Micros,
    /// Since the epoch.
    datetime: Micros,

    fn micros(self: Temporal) Micros {
        return switch (self) {
            inline else => |m| m,
        };
    }
};

fn isSpace(c: u8) bool {
    return c == ' ' or c == '\t' or c == '\n' or c == '\r' or c == 0x0b or c == 0x0c;
}

fn isPunct(c: u8) bool {
    return std.ascii.isPrint(c) and !std.ascii.isAlphanumeric(c) and c != ' ';
}

fn pow10(n: u8) u64 {
    return std.math.powi(u64, 10, n) catch 1;
}

/// Fraction digits as MySQL reads them: six count, a seventh rounds the
/// sixth half up, and any more are skipped.
const Fraction = struct { micros: i64 = 0, digits: u8 = 0, round_up: bool = false };

fn readFraction(s: []const u8, pos: *usize) Fraction {
    var f: Fraction = .{};
    var seen: usize = 0;
    while (pos.* < s.len and std.ascii.isDigit(s[pos.*])) : (pos.* += 1) {
        const d = s[pos.*] - '0';
        if (seen < 6) f.micros = f.micros * 10 + d else if (seen == 6) f.round_up = d >= 5;
        seen += 1;
    }
    f.digits = @intCast(@min(seen, 6));
    f.micros *= @intCast(pow10(6 - f.digits));
    return f;
}

const DatetimeScan = union(enum) { not_datetime, invalid, valid: Micros };

/// Text as MySQL's str_to_datetime reads a date or a datetime: fields with
/// any punctuation between them (`2026-9-6 1:2:3`), a two-digit year taken
/// as 1970-2069, or digits only (`YYYYMMDD[HHMMSS]`, `YYMMDD[HHMMSS]`), with
/// text after the last field ignored. With `datetime_only`, as str_to_time
/// reads a long string, delimited text must hold a space to be a datetime.
/// `s` starts at its first non-space.
fn scanDatetime(s: []const u8, datetime_only: bool) DatetimeScan {
    var digits: usize = 0;
    while (digits < s.len and std.ascii.isDigit(s[digits])) digits += 1;
    var fields = [6]u32{ 0, 0, 0, 0, 0, 0 };
    var count: usize = 0;
    var year_len: usize = 0;
    var frac: Fraction = .{};
    var pos: usize = 0;
    if (digits == s.len or s[digits] == '.') {
        year_len = if (digits == 4 or digits == 8 or digits >= 14) 4 else 2;
        while (count < 6 and pos < digits) : (count += 1) {
            const width = @min(if (count == 0) year_len else 2, digits - pos);
            fields[count] = std.fmt.parseInt(u32, s[pos..][0..width], 10) catch return .invalid;
            pos += width;
        }
        if (pos < digits) return .invalid;
        if (count == 6 and pos < s.len and s[pos] == '.') {
            pos += 1;
            frac = readFraction(s, &pos);
        }
    } else {
        var found_delimiter = false;
        var found_space = false;
        while (count < 6 and pos < s.len and std.ascii.isDigit(s[pos])) {
            const start = pos;
            var v: u32 = 0;
            while (pos < s.len and std.ascii.isDigit(s[pos])) : (pos += 1) {
                v = v * 10 + (s[pos] - '0');
                if (v > 999_999) return .not_datetime;
            }
            if (count == 0) year_len = pos - start;
            fields[count] = v;
            count += 1;
            if (pos == s.len) break;
            if (count == 3 and s[pos] == 'T') {
                pos += 1;
                continue;
            }
            if (count == 6) {
                if (s[pos] == '.') {
                    pos += 1;
                    frac = readFraction(s, &pos);
                }
                break;
            }
            // MySQL allows a space only between the date and the time.
            while (pos < s.len and (isPunct(s[pos]) or isSpace(s[pos]))) : (pos += 1) {
                if (isSpace(s[pos])) {
                    if (count != 3) return .not_datetime;
                    found_space = true;
                }
                found_delimiter = true;
            }
        }
        if (datetime_only and found_delimiter and !found_space) return .not_datetime;
    }
    if (count == 0 or year_len == 0) return .not_datetime;
    if (count < 3) return .invalid;

    const zero_date = fields[0] == 0 and fields[1] == 0 and fields[2] == 0;
    var year: i32 = @intCast(fields[0]);
    if (year_len == 2 and !zero_date) year += if (year < 70) 2000 else 1900;
    const month = fields[1];
    const day = fields[2];
    if (year > 9999 or !common.validDate(year, month, day) or fields[3] > 23 or fields[4] > 59 or fields[5] > 59) return .invalid;
    const seconds = (@as(i64, fields[3]) * 60 + fields[4]) * 60 + fields[5];
    const micros = @as(i64, common.ymdToDays(year, month, day)) * US_PER_DAY + seconds * US_PER_S + frac.micros;
    return .{ .valid = .{ .value = micros + @intFromBool(frac.round_up), .fsp = frac.digits } };
}

/// Text as a DATE or DATETIME the way MySQL reads it for a date argument
/// (`scanDatetime`): null when it is neither.
pub fn parseDatetime(raw: []const u8) ?Micros {
    var pos: usize = 0;
    while (pos < raw.len and isSpace(raw[pos])) pos += 1;
    return switch (scanDatetime(raw[pos..], false)) {
        .valid => |m| m,
        .not_datetime, .invalid => null,
    };
}

/// Digits at `pos`; null past MySQL's 32-bit field limit.
fn readField(s: []const u8, pos: *usize) ?u64 {
    var v: u64 = 0;
    while (pos.* < s.len and std.ascii.isDigit(s[pos.*])) : (pos.* += 1) {
        v = v * 10 + (s[pos.*] - '0');
        if (v > std.math.maxInt(u32)) return null;
    }
    return v;
}

/// `out.len` fields separated by ':', each read while a ':' and a digit follow.
fn readClockFields(s: []const u8, pos: *usize, out: []u64) ?void {
    for (out, 0..) |*slot, i| {
        slot.* = readField(s, pos) orelse return null;
        if (i + 1 == out.len or s.len - pos.* < 2 or s[pos.*] != ':' or !std.ascii.isDigit(s[pos.* + 1])) return;
        pos.* += 1;
    }
}

/// Text as MySQL's str_to_time reads it: a DATETIME when it is at least 12
/// characters and reads as one (`scanDatetime`), else a TIME spelled
/// `[-][D ]H[:M[:S]][.f]` or as the digits `HHMMSS`, with text after it
/// ignored. Minutes or seconds past 59 are no time; hours clamp to
/// 838:59:59.
pub fn parseTime(raw: []const u8) ?Temporal {
    var pos: usize = 0;
    while (pos < raw.len and isSpace(raw[pos])) pos += 1;
    const negative = pos < raw.len and raw[pos] == '-';
    if (negative) {
        pos += 1;
        while (pos < raw.len and isSpace(raw[pos])) pos += 1;
    }
    const s = raw[pos..];
    if (s.len == 0 or !std.ascii.isDigit(s[0])) return null;
    if (s.len >= 12) switch (scanDatetime(s, true)) {
        .valid => |m| return .{ .datetime = m },
        .invalid => return null,
        .not_datetime => {},
    };

    var p: usize = 0;
    const lead = readField(s, &p) orelse return null;
    const end_of_days = p;
    while (p < s.len and isSpace(s[p])) p += 1;
    // days, hours, minutes, seconds
    var parts = [4]u64{ 0, 0, 0, 0 };
    if (s.len - p > 1 and p != end_of_days and std.ascii.isDigit(s[p])) {
        parts[0] = lead;
        readClockFields(s, &p, parts[1..]) orelse return null;
    } else if (s.len - p > 1 and s[p] == ':' and std.ascii.isDigit(s[p + 1])) {
        parts[1] = lead;
        p += 1;
        readClockFields(s, &p, parts[2..]) orelse return null;
    } else {
        parts[1] = lead / 10000;
        parts[2] = lead / 100 % 100;
        parts[3] = lead % 100;
    }
    var frac: Fraction = .{};
    if (s.len - p >= 2 and s[p] == '.' and std.ascii.isDigit(s[p + 1])) {
        p += 1;
        frac = readFraction(s, &p);
    }
    // An exponent means the text is a number, not a TIME.
    if (s.len - p > 1 and (s[p] == 'e' or s[p] == 'E') and (std.ascii.isDigit(s[p + 1]) or
        ((s[p + 1] == '-' or s[p + 1] == '+') and s.len - p > 2 and std.ascii.isDigit(s[p + 2])))) return null;
    if (parts[2] > 59 or parts[3] > 59) return null;
    const hours = parts[0] * 24 + parts[1];
    var micros: i64 = MAX_TIME_MICROS + 1;
    if (hours <= 838) micros = @intCast(((hours * 60 + parts[2]) * 60 + parts[3]) * US_PER_S + @as(u64, @intCast(frac.micros)));
    // Past the range the fraction is dropped, as MySQL clamps before rounding.
    micros = if (micros > MAX_TIME_MICROS) MAX_TIME_MICROS else @min(micros + @intFromBool(frac.round_up), MAX_TIME_MICROS);
    return .{ .time = .{ .value = if (negative) -micros else micros, .fsp = frac.digits } };
}

/// `[-]HH:MM:SS` with `fsp` fraction digits, as MySQL prints a TIME.
pub fn formatTime(buf: []u8, micros: i64, fsp: u8) ![]const u8 {
    const magnitude = @abs(micros);
    const secs = magnitude / US_PER_S;
    const sign: []const u8 = if (micros < 0) "-" else "";
    const clock = try std.fmt.bufPrint(buf, "{s}{d:0>2}:{d:0>2}:{d:0>2}", .{ sign, secs / 3600, secs / 60 % 60, secs % 60 });
    if (fsp == 0) return clock;
    const frac = try std.fmt.bufPrint(buf[clock.len..], ".{d:0>[1]}", .{ magnitude % US_PER_S / pow10(6 - fsp), fsp });
    return buf[0 .. clock.len + frac.len];
}

/// A DATETIME's text with `fsp` (0 or 6) fraction digits.
fn formatDatetime(buf: []u8, micros: i64, fsp: u8) ![]const u8 {
    const frac = @mod(micros, US_PER_S);
    const whole = try common.formatDateTime(buf, micros - frac);
    if (fsp == 0) return whole;
    const tail = try std.fmt.bufPrint(buf[whole.len..], ".{d:0>6}", .{@as(u64, @intCast(frac))});
    return buf[0 .. whole.len + tail.len];
}

/// The wall clock's time of day as CURTIME(fsp) shows it: truncated to
/// `fsp` digits.
pub fn clockTime(buf: []u8, now_micros: i64, fsp: u8) ![]const u8 {
    const time_of_day = @mod(now_micros, US_PER_DAY);
    return formatTime(buf, time_of_day - @mod(time_of_day, @as(i64, @intCast(pow10(6 - fsp)))), fsp);
}

/// The fraction digits a DATETIME shows: six when it has a fraction, as
/// thinDB prints DATETIMEs.
fn datetimeFsp(micros: i64) u8 {
    return if (@mod(micros, US_PER_S) != 0) 6 else 0;
}

/// Row `i` of a DATE, DATETIME or text argument as a TIME or a DATETIME;
/// null for text that is neither.
pub fn temporalAt(v: ColumnView, i: usize) ?Temporal {
    return switch (v.data) {
        .date => |d| .{ .datetime = .{ .value = @as(i64, d[i]) * US_PER_DAY, .fsp = 0 } },
        .datetime => |d| .{ .datetime = .{ .value = d[i], .fsp = datetimeFsp(d[i]) } },
        else => parseTime(common.stringViewOf(v).rowBytes(i)),
    };
}

/// A TIME function's arguments, each read as the function reads it (`at`).
const TimeArgs = struct {
    views: []const ColumnView,
    /// Each argument's type, when a number is among them.
    types: ?[]const Type = null,

    /// Row `i` of argument `k`: a number as `numberTemporal` reads it, by
    /// its digits rather than its text, and anything else as `temporalAt`
    /// reads it.
    fn at(self: TimeArgs, k: usize, i: usize) ?Temporal {
        const v = self.views[k];
        if (!v.isValid(i)) return null;
        if (self.types) |ts| if (!(ts[k].isString() or ts[k].isTemporal())) {
            return numberTemporal(dec.exactAt(v, ts[k], i) orelse return null, numericFsp(ts[k]));
        };
        return temporalAt(v, i);
    }
};

const TimeRows = fn (allocator: Allocator, args: TimeArgs, out: *ColumnStore, row_count: usize) anyerror!void;

/// A TIME function's kernel over text, DATEs and DATETIMEs.
fn textKernel(comptime rows: TimeRows) Kernel {
    return struct {
        fn kernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) anyerror!void {
            return rows(allocator, .{ .views = args }, out, row_count);
        }
    }.kernel;
}

/// A TIME function's kernel when a number is among its arguments.
fn numberKernel(comptime rows: TimeRows) common.TypedKernelFn {
    return struct {
        fn kernel(allocator: Allocator, arg_types: []const Type, out_type: Type, args: []const ColumnView, out: *ColumnStore, row_count: usize) anyerror!void {
            _ = out_type;
            return rows(allocator, .{ .views = args, .types = arg_types }, out, row_count);
        }
    }.kernel;
}

/// A TIME function's kernel and result type when a number is among its
/// arguments (`arg_types`), which reads by its digits: `HOUR(8390000)` is
/// NULL, as 839 hours is no TIME, where `HOUR('8390000')` clamps to 838.
/// Null for a function that takes no TIME.
pub fn numberArgFn(name: []const u8, arg_types: []const Type) ?struct { kernel: common.TypedKernelFn, return_type: Type } {
    const eql = std.ascii.eqlIgnoreCase;
    if (arg_types.len == 1) {
        inline for (std.meta.fields(ClockPart)) |f| {
            if (eql(name, f.name)) return .{ .kernel = numberKernel(clockPartRows(@enumFromInt(f.value))), .return_type = .int };
        }
        if (eql(name, "time_to_sec")) return .{ .kernel = numberKernel(timeToSecRows), .return_type = .bigint };
        if (eql(name, "time")) return .{ .kernel = numberKernel(timeRows), .return_type = .string };
    }
    if (arg_types.len == 2) {
        if (eql(name, "timediff")) return .{ .kernel = numberKernel(timediffRows), .return_type = .string };
        const moved: Type = if (arg_types[0] == .datetime) .datetime else .string;
        if (eql(name, "addtime")) return .{ .kernel = numberKernel(addTimeRows(1)), .return_type = moved };
        if (eql(name, "subtime")) return .{ .kernel = numberKernel(addTimeRows(-1)), .return_type = moved };
    }
    return null;
}

fn clampTime(micros: i64) i64 {
    return std.math.clamp(micros, -MAX_TIME_MICROS, MAX_TIME_MICROS);
}

fn appendText(allocator: Allocator, out: *ColumnStore, base: usize, i: usize, text: ?[]const u8) !void {
    try common.stringStoreOf(out).appendValue(allocator, text orelse "");
    try out.appendValidBit(allocator, base + i, text != null);
}

/// TIME_TO_SEC: a TIME's whole seconds, keeping its sign, or a DATETIME's
/// seconds into its day.
pub const timeToSecKernel = textKernel(timeToSecRows);

fn timeToSecRows(allocator: Allocator, args: TimeArgs, out: *ColumnStore, row_count: usize) anyerror!void {
    const base = out.data.rowCount();
    for (0..row_count) |i| {
        const t = args.at(0, i);
        const secs: i64 = if (t) |v| switch (v) {
            .time => |x| @divTrunc(x.value, US_PER_S),
            .datetime => |x| @divFloor(@mod(x.value, US_PER_DAY), US_PER_S),
        } else 0;
        try out.data.bigint.append(allocator, secs);
        try out.appendValidBit(allocator, base + i, t != null);
    }
}

pub const ClockPart = enum { hour, minute, second, microsecond };

/// HOUR, MINUTE, SECOND or MICROSECOND of text: a TIME's magnitude
/// (`HOUR('-838:00:00')` is 838) or a DATETIME's time of day.
pub fn clockPartKernel(comptime part: ClockPart) Kernel {
    return textKernel(clockPartRows(part));
}

fn clockPartRows(comptime part: ClockPart) TimeRows {
    return struct {
        fn rows(allocator: Allocator, args: TimeArgs, out: *ColumnStore, row_count: usize) anyerror!void {
            const base = out.data.rowCount();
            for (0..row_count) |i| {
                const t = args.at(0, i);
                const magnitude: u64 = if (t) |v| switch (v) {
                    .time => |x| @abs(x.value),
                    .datetime => |x| @intCast(@mod(x.value, US_PER_DAY)),
                } else 0;
                const secs = magnitude / US_PER_S;
                const value = switch (part) {
                    .hour => secs / 3600,
                    .minute => secs / 60 % 60,
                    .second => secs % 60,
                    .microsecond => magnitude % US_PER_S,
                };
                try out.data.int.append(allocator, @intCast(value));
                try out.appendValidBit(allocator, base + i, t != null);
            }
        }
    }.rows;
}

/// TIME(x): a TIME as itself, a DATETIME's time of day.
pub const timeKernel = textKernel(timeRows);

fn timeRows(allocator: Allocator, args: TimeArgs, out: *ColumnStore, row_count: usize) anyerror!void {
    const base = out.data.rowCount();
    var buf: [48]u8 = undefined;
    for (0..row_count) |i| {
        const value: ?Micros = if (args.at(0, i)) |v| switch (v) {
            .time => |x| x,
            .datetime => |x| .{ .value = @mod(x.value, US_PER_DAY), .fsp = x.fsp },
        } else null;
        try appendText(allocator, out, base, i, if (value) |x| try formatTime(&buf, x.value, x.fsp) else null);
    }
}

/// The deepest fraction CAST(x AS TIME(n)) takes.
pub const MAX_FSP: u8 = 6;

/// CAST(x AS TIME(fsp)): text, a DATE or a DATETIME as `temporalAt` reads it
/// (a DATETIME gives its time of day), a number as `numberTemporal` reads it,
/// rounded half away from zero to `fsp` digits and shown with exactly that
/// many. A time of day can round up to 24:00:00, as in MySQL.
pub fn castTimeKernel(comptime fsp: u8) common.TypedKernelFn {
    return struct {
        fn kernel(allocator: Allocator, arg_types: []const Type, out_type: Type, args: []const ColumnView, out: *ColumnStore, row_count: usize) anyerror!void {
            _ = out_type;
            const base = out.data.rowCount();
            const read: TimeArgs = .{ .views = args, .types = arg_types };
            var buf: [48]u8 = undefined;
            for (0..row_count) |i| {
                const micros: ?i64 = if (read.at(0, i)) |v| switch (v) {
                    .time => |x| x.value,
                    .datetime => |x| @mod(x.value, US_PER_DAY),
                } else null;
                try appendText(allocator, out, base, i, if (micros) |m| try formatTime(&buf, roundTime(m, fsp), fsp) else null);
            }
        }
    }.kernel;
}

/// `micros` rounded half away from zero to `fsp` fraction digits, within
/// ±838:59:59.
fn roundTime(micros: i64, fsp: u8) i64 {
    const unit: u64 = pow10(MAX_FSP - fsp);
    const rounded = (@abs(micros) + unit / 2) / unit * unit;
    const magnitude: i64 = @intCast(@min(rounded, @as(u64, MAX_TIME_MICROS)));
    return if (micros < 0) -magnitude else magnitude;
}

/// A number as MySQL reads it for a TIME, showing `fsp` fraction digits:
/// `[-]HHMMSS[.f]` with minutes and seconds within 59, or from 10^10 up a
/// `[YY]YYMMDDHHMMSS` DATETIME. Past 838:59:59 otherwise it is no TIME,
/// unlike text, which clamps. The fraction rounds to the microsecond after
/// the fields are checked, so 59.9999999 is 00:01:00, and a TIME it carries
/// past 838:59:59 clamps.
fn numberTemporal(n: common.ScaledInt, fsp: u8) ?Temporal {
    const scale = dec.pow10(n.s);
    const magnitude: i128 = @intCast(@abs(n.m));
    const whole = @divTrunc(magnitude, scale);
    const fraction: i64 = @intCast(dec.rescale(@mod(magnitude, scale), n.s, MAX_FSP) orelse return null);
    if (whole > 8_385_959) {
        if (n.m < 0 or whole < 10_000_000_000) return null;
        var buf: [40]u8 = undefined;
        const digits = std.fmt.bufPrint(&buf, "{d}", .{whole}) catch return null;
        return switch (scanDatetime(digits, false)) {
            .valid => |m| .{ .datetime = .{ .value = m.value + fraction, .fsp = fsp } },
            .not_datetime, .invalid => null,
        };
    }
    const w: i64 = @intCast(whole);
    const minute = @mod(@divTrunc(w, 100), 100);
    const second = @mod(w, 100);
    if (minute > 59 or second > 59) return null;
    const micros = @min(((@divTrunc(w, 10_000) * 60 + minute) * 60 + second) * US_PER_S + fraction, MAX_TIME_MICROS);
    return .{ .time = .{ .value = if (n.m < 0) -micros else micros, .fsp = fsp } };
}

/// A datetime's fields, which need not name a day: MySQL compares a
/// number with a DATE or DATETIME by these (`numberDatetimeFields`).
pub const DatetimeFields = struct {
    year: i32,
    month: u32,
    day: u32,
    hour: u32,
    minute: u32,
    second: u32,
    /// The fraction, rounded to the microsecond: up to a whole second.
    micros: i64,
};

/// A number as MySQL's number_to_datetime reads one to compare with a DATE
/// or DATETIME: `YYMMDD` (years 2000-2069 up to 691231, 1970-1999 from
/// 700101), `YYYYMMDD`, `YYMMDDhhmmss` or `YYYYMMDDhhmmss` by its size,
/// its fraction the microseconds. A zero month or day, or a day past its
/// month's end, still reads (`20260900`); null for a negative number, one
/// between those forms' ranges, a month past 12, a day past 31 or a time of
/// day past 23:59:59.
pub fn numberDatetimeFields(n: common.ScaledInt) ?DatetimeFields {
    if (n.m < 0) return null;
    const scale = dec.pow10(n.s);
    const whole = @divTrunc(n.m, scale);
    const fraction: i64 = @intCast(dec.rescale(@mod(n.m, scale), n.s, MAX_FSP) orelse return null);
    const clock: bool, const short_year: bool = if (whole < 101)
        return null
    else if (whole <= 991_231)
        .{ false, true }
    else if (whole < 10_000_101)
        return null
    else if (whole <= 99_991_231)
        .{ false, false }
    else if (whole < 101_000_000)
        return null
    else if (whole <= 991_231_235_959)
        .{ true, true }
    else if (whole < 10_000_101_000_000 or whole > 99_991_231_235_959)
        return null
    else
        .{ true, false };
    const hms: u32 = if (clock) @intCast(@mod(whole, 1_000_000)) else 0;
    const ymd: u32 = @intCast(if (clock) @divTrunc(whole, 1_000_000) else whole);
    var year: i32 = @intCast(ymd / 10_000);
    if (short_year) year += if (year < 70) 2000 else 1900;
    const f: DatetimeFields = .{
        .year = year,
        .month = ymd / 100 % 100,
        .day = ymd % 100,
        .hour = hms / 10_000,
        .minute = hms / 100 % 100,
        .second = hms % 100,
        .micros = fraction,
    };
    if (f.month > 12 or f.day > 31 or f.hour > 23 or f.minute > 59 or f.second > 59) return null;
    return f;
}

/// TIMEDIFF(a, b): a - b as a TIME, for two TIMEs or two DATETIMEs; one of
/// each is NULL, and so is a DATE beside a DATETIME, as MySQL has them. It
/// shows the larger of the two fraction digit counts.
pub const timediffKernel = textKernel(timediffRows);

fn timediffRows(allocator: Allocator, args: TimeArgs, out: *ColumnStore, row_count: usize) anyerror!void {
    const base = out.data.rowCount();
    var buf: [48]u8 = undefined;
    const views = args.views;
    const date_beside_datetime = (views[0].data == .date and views[1].data == .datetime) or
        (views[0].data == .datetime and views[1].data == .date);
    for (0..row_count) |i| {
        const diff = if (date_beside_datetime) null else timeDiff(args.at(0, i), args.at(1, i));
        try appendText(allocator, out, base, i, if (diff) |d| try formatTime(&buf, d.value, d.fsp) else null);
    }
}

fn timeDiff(a: ?Temporal, b: ?Temporal) ?Micros {
    const x = a orelse return null;
    const y = b orelse return null;
    if (std.meta.activeTag(x) != std.meta.activeTag(y)) return null;
    return .{ .value = clampTime(x.micros().value -| y.micros().value), .fsp = @max(x.micros().fsp, y.micros().fsp) };
}

/// DATETIMEs a sum can land on: years 0 to 9999.
const MIN_DATETIME_MICROS: i64 = @as(i64, -719_528) * US_PER_DAY;
const MAX_DATETIME_MICROS: i64 = @as(i64, 2_932_897) * US_PER_DAY - 1;

/// ADDTIME (`sign` 1) or SUBTIME (-1): a TIME or DATETIME moved by a TIME;
/// NULL when the second argument is not a TIME or the sum leaves years
/// 0-9999. A DATETIME-typed first argument gives a DATETIME; text gives
/// text, with six fraction digits when either argument has a fraction.
pub fn addTimeKernel(comptime sign: i64) Kernel {
    return textKernel(addTimeRows(sign));
}

fn addTimeRows(comptime sign: i64) TimeRows {
    return struct {
        fn rows(allocator: Allocator, args: TimeArgs, out: *ColumnStore, row_count: usize) anyerror!void {
            const base = out.data.rowCount();
            var buf: [48]u8 = undefined;
            for (0..row_count) |i| {
                const sum = addTime(sign, args.at(0, i), args.at(1, i));
                if (out.data == .datetime) {
                    const dt: ?i64 = if (sum) |s| switch (s) {
                        .datetime => |x| x.value,
                        .time => null,
                    } else null;
                    try out.data.datetime.append(allocator, dt orelse 0);
                    try out.appendValidBit(allocator, base + i, dt != null);
                    continue;
                }
                const text: ?[]const u8 = if (sum) |s| switch (s) {
                    .time => |x| try formatTime(&buf, x.value, x.fsp),
                    .datetime => |x| try formatDatetime(&buf, x.value, x.fsp),
                } else null;
                try appendText(allocator, out, base, i, text);
            }
        }
    }.rows;
}

fn addTime(sign: i64, a: ?Temporal, b: ?Temporal) ?Temporal {
    const x = a orelse return null;
    const delta = switch (b orelse return null) {
        .time => |t| t.value,
        .datetime => return null,
    };
    const has_fraction = @mod(x.micros().value, US_PER_S) != 0 or @mod(delta, US_PER_S) != 0;
    const fsp: u8 = if (has_fraction) 6 else 0;
    return switch (x) {
        .time => |t| .{ .time = .{ .value = clampTime(t.value + sign * delta), .fsp = fsp } },
        .datetime => |t| blk: {
            const moved = t.value +| sign * delta;
            if (moved < MIN_DATETIME_MICROS or moved > MAX_DATETIME_MICROS) break :blk null;
            break :blk .{ .datetime = .{ .value = moved, .fsp = fsp } };
        },
    };
}

/// The fraction digits a number of type `t` shows as a TIME's seconds, as
/// MySQL derives them: none for an integer, the scale (at most 6) for a
/// decimal, six for a double or text.
fn numericFsp(t: Type) u8 {
    if (t.decimalSpec()) |spec| return @min(spec.s, 6);
    if (t.isInteger() or t == .boolean) return 0;
    return 6;
}

/// Row `i` of a numeric or text argument as an exact decimal; null for text
/// that isn't a number and a double past DECIMAL's range.
fn exactNumberAt(v: ColumnView, t: Type, i: usize) ?common.ScaledInt {
    if (!v.isValid(i)) return null;
    if (t.isString()) return switch (common.textNumber(common.stringViewOf(v).rowBytes(i)) orelse return null) {
        .exact => |d| d,
        .float => |f| common.floatDigits(f),
    };
    return dec.exactAt(v, t, i);
}

/// SEC_TO_TIME(n): `n` seconds as a TIME, rounded to the microsecond and
/// clamped to ±838:59:59.
pub fn secToTimeKernel(allocator: Allocator, arg_types: []const Type, out_type: Type, args: []const ColumnView, out: *ColumnStore, row_count: usize) anyerror!void {
    _ = out_type;
    const base = out.data.rowCount();
    const fsp = numericFsp(arg_types[0]);
    var buf: [48]u8 = undefined;
    for (0..row_count) |i| {
        const micros: ?i64 = if (exactNumberAt(args[0], arg_types[0], i)) |n| clampedMicros(n) else null;
        try appendText(allocator, out, base, i, if (micros) |m| try formatTime(&buf, m, fsp) else null);
    }
}

fn clampedMicros(seconds: common.ScaledInt) i64 {
    const us = dec.rescale(seconds.m, seconds.s, 6) orelse return if (seconds.m < 0) -MAX_TIME_MICROS else MAX_TIME_MICROS;
    return @intCast(std.math.clamp(us, -MAX_TIME_MICROS, MAX_TIME_MICROS));
}

/// MAKETIME(hour, minute, second): hour and minute round to integers, the
/// seconds keep their fraction. A minute outside 0-59, or seconds that are
/// negative or reach 60, give NULL; hours clamp to ±838:59:59.
pub fn makeTimeKernel(allocator: Allocator, arg_types: []const Type, out_type: Type, args: []const ColumnView, out: *ColumnStore, row_count: usize) anyerror!void {
    _ = out_type;
    const base = out.data.rowCount();
    const fsp = numericFsp(arg_types[2]);
    var buf: [48]u8 = undefined;
    for (0..row_count) |i| {
        const hour = exactNumberAt(args[0], arg_types[0], i);
        const minute = exactNumberAt(args[1], arg_types[1], i);
        const second = exactNumberAt(args[2], arg_types[2], i);
        const micros: ?i64 = if (hour != null and minute != null and second != null) makeTime(hour.?, minute.?, second.?) else null;
        try appendText(allocator, out, base, i, if (micros) |m| try formatTime(&buf, m, fsp) else null);
    }
}

fn makeTime(hour: common.ScaledInt, minute: common.ScaledInt, second: common.ScaledInt) ?i64 {
    const h = dec.rescale(hour.m, hour.s, 0) orelse return null;
    const m = dec.rescale(minute.m, minute.s, 0) orelse return null;
    if (m < 0 or m > 59 or second.m < 0 or @divTrunc(second.m, dec.pow10(second.s)) > 59) return null;
    if (h < -838 or h > 838) return if (h < 0) -MAX_TIME_MICROS else MAX_TIME_MICROS;
    const sec_micros: i64 = @intCast(dec.rescale(second.m, second.s, 6) orelse return null);
    const hours: i64 = @intCast(@abs(h));
    const micros = @min((hours * 3600 + @as(i64, @intCast(m)) * 60) * US_PER_S + sec_micros, MAX_TIME_MICROS);
    return if (h < 0) -micros else micros;
}

/// EXTRACT's compound units below the day. A TIME's days fold into its
/// hours and its sign applies to the whole value; a DATETIME's day is its
/// day of the month.
pub const ClockUnit = enum {
    day_hour,
    day_minute,
    day_second,
    day_microsecond,
    hour_minute,
    hour_second,
    hour_microsecond,
    minute_second,
    minute_microsecond,
    second_microsecond,
};

const ClockFields = struct { day: i64, hour: i64, minute: i64, second: i64, micro: i64, sign: i64 };

fn clockFields(t: Temporal) ClockFields {
    const day: i64, const magnitude: u64, const sign: i64 = switch (t) {
        .time => |x| .{ 0, @abs(x.value), if (x.value < 0) -1 else 1 },
        .datetime => |x| .{ common.daysToYmd(common.daysFromDatetime(x.value)).day, @intCast(@mod(x.value, US_PER_DAY)), 1 },
    };
    const secs: i64 = @intCast(magnitude / US_PER_S);
    return .{
        .day = day,
        .hour = @divTrunc(secs, 3600),
        .minute = @mod(@divTrunc(secs, 60), 60),
        .second = @mod(secs, 60),
        .micro = @intCast(magnitude % US_PER_S),
        .sign = sign,
    };
}

fn clockUnitValue(unit: ClockUnit, f: ClockFields) i64 {
    const hms = f.hour * 10_000 + f.minute * 100 + f.second;
    return f.sign * switch (unit) {
        .day_hour => f.day * 100 + f.hour,
        .day_minute => f.day * 10_000 + f.hour * 100 + f.minute,
        .day_second => f.day * 1_000_000 + hms,
        .day_microsecond => (f.day * 1_000_000 + hms) * US_PER_S + f.micro,
        .hour_minute => f.hour * 100 + f.minute,
        .hour_second => hms,
        .hour_microsecond => hms * US_PER_S + f.micro,
        .minute_second => f.minute * 100 + f.second,
        .minute_microsecond => (f.minute * 100 + f.second) * US_PER_S + f.micro,
        .second_microsecond => f.second * US_PER_S + f.micro,
    };
}

/// EXTRACT(unit FROM x) for a compound unit below the day, over a DATE,
/// DATETIME or text argument.
pub fn extractClockKernel(comptime unit: ClockUnit) Kernel {
    return struct {
        fn kernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
            const base = out.data.rowCount();
            const read: TimeArgs = .{ .views = args };
            for (0..row_count) |i| {
                const t = read.at(0, i);
                try out.data.bigint.append(allocator, if (t) |v| clockUnitValue(unit, clockFields(v)) else 0);
                try out.appendValidBit(allocator, base + i, t != null);
            }
        }
    }.kernel;
}

/// EXTRACT(YEAR_MONTH FROM x): `YYYYMM`. Text reads as a date.
pub fn extractYearMonthKernel(allocator: Allocator, args: []const ColumnView, out: *ColumnStore, row_count: usize) !void {
    const base = out.data.rowCount();
    for (0..row_count) |i| {
        const days: ?i32 = if (!args[0].isValid(i)) null else switch (args[0].data) {
            .date => |d| d[i],
            .datetime => |d| common.daysFromDatetime(d[i]),
            else => if (parseDatetime(common.stringViewOf(args[0]).rowBytes(i))) |m| common.daysFromDatetime(m.value) else null,
        };
        const ymd = common.daysToYmd(days orelse 0);
        try out.data.bigint.append(allocator, @as(i64, ymd.year) * 100 + ymd.month);
        try out.appendValidBit(allocator, base + i, days != null);
    }
}

test "text reads as a TIME the way MySQL's str_to_time reads it" {
    const t = std.testing;
    // TIME_TO_SEC over each text, from MySQL 8.4.
    const seconds = .{
        .{ "01:01:01", 3661 },           .{ "-01:01:01", -3661 },        .{ "838:59:59", 3020399 },
        .{ "839:00:00", 3020399 },       .{ "10:00", 36000 },            .{ "1005", 605 },
        .{ "12", 12 },                   .{ "100503", 36303 },           .{ "1 10:00:00", 122400 },
        .{ "10:00abc", 36000 },          .{ "10:5", 36300 },             .{ "1:2:3", 3723 },
        .{ "  10:00:00  ", 36000 },      .{ "10.5", 10 },                .{ "1 10", 122400 },
        .{ "1 10:30", 124200 },          .{ "100:00:00", 360000 },       .{ "2026-09-26", 1226 },
        .{ "20260926100503", 36303 },    .{ "100:00:00.123", 360000 },   .{ "- 10:00:00", -36000 },
        .{ "10:00:00.9999999", 36001 },  .{ "2026-09-26 10:00", 36000 }, .{ "2026-09-26T10:00:00", 1226 },
        .{ "26-09-26 10:00:00", 36000 }, .{ "2026-9-6 1:2:3", 3723 },    .{ "10:00:00 PM", 36000 },
        .{ "34 22:59:59", 3020399 },     .{ "35 00:00:00", 3020399 },    .{ "2026-09-26 10:05:03.5", 36303 },
    };
    inline for (seconds) |c| {
        const got = parseTime(c[0]) orelse {
            std.debug.print("text: {s}\n", .{c[0]});
            return error.TestUnexpectedResult;
        };
        const secs: i64 = switch (got) {
            .time => |x| @divTrunc(x.value, US_PER_S),
            .datetime => |x| @divFloor(@mod(x.value, US_PER_DAY), US_PER_S),
        };
        t.expectEqual(@as(i64, c[1]), secs) catch |err| {
            std.debug.print("text: {s}\n", .{c[0]});
            return err;
        };
    }
    inline for (.{ "10:60:00", "abc", "", "10:00:60", "2026-13-01 10:00:00", "2026-09-26 25:00:00", "--10:00:00", "4294967296", "2026-02-30 10:00:00", "0100-02-29 10:00:00", "1e5" }) |bad| {
        t.expect(parseTime(bad) == null) catch |err| {
            std.debug.print("text: {s}\n", .{bad});
            return err;
        };
    }
}

test "TIME text prints its sign, hours past 24 and its fraction digits" {
    var buf: [48]u8 = undefined;
    const t = std.testing;
    try t.expectEqualStrings("01:01:01", try formatTime(&buf, 3661 * US_PER_S, 0));
    try t.expectEqualStrings("-838:59:59", try formatTime(&buf, -MAX_TIME_MICROS, 0));
    try t.expectEqualStrings("-00:00:00.5", try formatTime(&buf, -500_000, 1));
    try t.expectEqualStrings("25:01:01.000002", try formatTime(&buf, 90061 * US_PER_S + 2, 6));
    const half = parseTime("10:00:00.50").?.time;
    try t.expectEqual(@as(u8, 2), half.fsp);
    try t.expectEqualStrings("10:00:00.50", try formatTime(&buf, half.value, half.fsp));
    const now: i64 = 20_000 * US_PER_DAY + (10 * 3600 + 5 * 60 + 3) * US_PER_S + 499_999;
    try t.expectEqualStrings("10:05:03.49", try clockTime(&buf, now, 2));
    try t.expectEqualStrings("10:05:03", try clockTime(&buf, now, 0));
}

test "EXTRACT's clock units follow MySQL's day, hour and sign rules" {
    const t = std.testing;
    const cases = .{
        .{ ClockUnit.day_hour, "1 10:05:03", 34 },
        .{ ClockUnit.hour_second, "-10:05:03", -100503 },
        .{ ClockUnit.day_second, "100:05:03", 1000503 },
        .{ ClockUnit.hour_minute, "100:05:03", 10005 },
        .{ ClockUnit.second_microsecond, "-00:00:01.5", -1500000 },
        .{ ClockUnit.day_microsecond, "-838:59:59.5", -8385959000000 },
        .{ ClockUnit.day_hour, "2026-09-26 10:05:03.123456", 2610 },
        .{ ClockUnit.day_microsecond, "2026-09-26 10:05:03.123456", 26100503123456 },
        .{ ClockUnit.minute_microsecond, "2026-09-26 10:05:03.123456", 503123456 },
        .{ ClockUnit.day_minute, "2026-09-26", 20 },
        .{ ClockUnit.day_second, "2026-09-26", 2026 },
    };
    inline for (cases) |c| try t.expectEqual(@as(i64, c[2]), clockUnitValue(c[0], clockFields(parseTime(c[1]).?)));
}

test "a number reads as datetime fields the way MySQL's number_to_datetime reads it" {
    const S = common.ScaledInt;
    const F = DatetimeFields;
    const cases = .{
        .{ S{ .m = 20260926, .s = 0 }, @as(?F, .{ .year = 2026, .month = 9, .day = 26, .hour = 0, .minute = 0, .second = 0, .micros = 0 }) },
        .{ S{ .m = 260926, .s = 0 }, @as(?F, .{ .year = 2026, .month = 9, .day = 26, .hour = 0, .minute = 0, .second = 0, .micros = 0 }) },
        .{ S{ .m = 101, .s = 0 }, @as(?F, .{ .year = 2000, .month = 1, .day = 1, .hour = 0, .minute = 0, .second = 0, .micros = 0 }) },
        .{ S{ .m = 991231, .s = 0 }, @as(?F, .{ .year = 1999, .month = 12, .day = 31, .hour = 0, .minute = 0, .second = 0, .micros = 0 }) },
        .{ S{ .m = 202609261005035, .s = 1 }, @as(?F, .{ .year = 2026, .month = 9, .day = 26, .hour = 10, .minute = 5, .second = 3, .micros = 500_000 }) },
        .{ S{ .m = 260926100503, .s = 0 }, @as(?F, .{ .year = 2026, .month = 9, .day = 26, .hour = 10, .minute = 5, .second = 3, .micros = 0 }) },
        .{ S{ .m = 10000000000, .s = 0 }, @as(?F, .{ .year = 2001, .month = 0, .day = 0, .hour = 0, .minute = 0, .second = 0, .micros = 0 }) },
        .{ S{ .m = 20260230, .s = 0 }, @as(?F, .{ .year = 2026, .month = 2, .day = 30, .hour = 0, .minute = 0, .second = 0, .micros = 0 }) },
        .{ S{ .m = 100, .s = 0 }, @as(?F, null) },
        .{ S{ .m = 2026, .s = 0 }, @as(?F, null) },
        .{ S{ .m = 9991231, .s = 0 }, @as(?F, null) },
        .{ S{ .m = 20261301, .s = 0 }, @as(?F, null) },
        .{ S{ .m = 20260932, .s = 0 }, @as(?F, null) },
        .{ S{ .m = 2026092610050, .s = 0 }, @as(?F, null) },
        .{ S{ .m = 20260926250000, .s = 0 }, @as(?F, null) },
        .{ S{ .m = -20260926, .s = 0 }, @as(?F, null) },
    };
    inline for (cases) |c| try std.testing.expectEqual(c[1], numberDatetimeFields(c[0]));
}

test "a number reads as a TIME by its HHMMSS digits, or as a DATETIME" {
    const S = common.ScaledInt;
    const clock = (10 * 3600 + 5 * 60 + 3) * US_PER_S;
    const time_of_day = struct {
        fn of(t: ?Temporal) ?i64 {
            return switch (t orelse return null) {
                .time => |x| x.value,
                .datetime => |x| @mod(x.value, US_PER_DAY),
            };
        }
    }.of;
    const cases = .{
        .{ S{ .m = 100503, .s = 0 }, @as(?i64, clock) },
        .{ S{ .m = -1005035, .s = 1 }, @as(?i64, -(clock + 500_000)) },
        .{ S{ .m = 106000, .s = 0 }, @as(?i64, null) },
        .{ S{ .m = 8390000, .s = 0 }, @as(?i64, null) },
        .{ S{ .m = -8390000, .s = 0 }, @as(?i64, null) },
        .{ S{ .m = 8385959, .s = 0 }, @as(?i64, MAX_TIME_MICROS) },
        .{ S{ .m = 83859599999999, .s = 7 }, @as(?i64, MAX_TIME_MICROS) },
        .{ S{ .m = 20260926100503, .s = 0 }, @as(?i64, clock) },
        .{ S{ .m = 20260230100503, .s = 0 }, @as(?i64, null) },
        .{ S{ .m = 599999999, .s = 7 }, @as(?i64, 60 * US_PER_S) },
    };
    inline for (cases) |c| try std.testing.expectEqual(c[1], time_of_day(numberTemporal(c[0], 0)));
    try std.testing.expect(numberTemporal(.{ .m = 20260926100503, .s = 0 }, 0).? == .datetime);
}
