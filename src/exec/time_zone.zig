//! Time zones for CONVERT_TZ and FROM_UNIXTIME(n, format, zone). A zone's
//! text names a fixed offset or a tz database zone, read from its TZif file
//! (RFC 8536): the offset changes the file lists, then the POSIX TZ rule in
//! its footer for every time after them.
//!
//! The files are the host's, not a database's, so a parsed zone is cached
//! once per process and shared by every database in it; it never changes
//! once read. The directory is `TZDIR` when that is set, else
//! /usr/share/zoneinfo, as cctz (and so StarRocks) reads it. Windows has no
//! system zone files: there a named zone is known only under `TZDIR`.

const std = @import("std");
const builtin = @import("builtin");
const Allocator = std.mem.Allocator;
const common = @import("scalar_fn_common.zig");

extern "c" fn getenv(name: [*:0]const u8) ?[*:0]const u8;

pub const Zone = union(enum) {
    /// Seconds east of UTC.
    fixed: i32,
    named: *const TzData,

    pub const utc: Zone = .{ .fixed = 0 };

    /// Seconds east of UTC in effect at the instant `utc_seconds`.
    pub fn offsetAt(zone: Zone, utc_seconds: i64) i64 {
        return switch (zone) {
            .fixed => |offset| offset,
            .named => |data| data.offsetAt(utc_seconds),
        };
    }

    /// The instant (seconds since the epoch) whose local time here is
    /// `local_seconds`. A local time a change skips or repeats reads with the
    /// offset in effect before that change, as StarRocks (cctz's `pre`)
    /// reads it: 02:30 on a spring-forward day in New York is 07:30 UTC, and
    /// 01:30 on a fall-back day is its first occurrence, 05:30 UTC.
    pub fn localToUtc(zone: Zone, local_seconds: i64) i64 {
        return switch (zone) {
            .fixed => |offset| local_seconds - offset,
            .named => |data| data.localToUtc(local_seconds),
        };
    }
};

/// The zone `text` names, or null when thinDB doesn't know it:
/// - `SYSTEM` or `UTC` in any case, as MySQL reads them (thinDB's clock runs
///   in UTC); `Z`; `CST`, which StarRocks reads as +08:00;
/// - an offset: MySQL's `+H:MM` from -13:59 to +14:00, or one of
///   `+h`, `+hh`, `+hh:mm`, `+hhmm`, `+hh:mm:ss`, `+hhmmss` up to 18 hours,
///   bare or after `UTC`, `GMT` or `UT`, the forms StarRocks reads; or
///   cctz's own `Fixed/UTC+hh:mm:ss`, up to 24 hours;
/// - a tz database name such as `America/New_York`, read from its file. A
///   name is one or more `/`-separated parts of letters, digits, `_`, `-`,
///   `+` and `.`, none empty or starting with `.`, so it can't leave the
///   zone directory.
pub fn resolve(text: []const u8) error{OutOfMemory}!?Zone {
    if (fixedOffset(text)) |offset| return .{ .fixed = offset };
    if (!isZoneName(text)) return null;
    const data = (try lookupNamed(text)) orelse return null;
    return .{ .named = data };
}

/// `resolve` for a kernel's rows: a row naming the previous row's zone
/// reuses it, so a constant zone resolves once per batch.
pub const Memo = struct {
    text: []const u8 = "",
    zone: ?Zone = null,
    filled: bool = false,

    pub fn zoneOf(memo: *Memo, text: []const u8) error{OutOfMemory}!?Zone {
        if (memo.filled and std.mem.eql(u8, memo.text, text)) return memo.zone;
        memo.zone = try resolve(text);
        memo.text = text;
        memo.filled = true;
        return memo.zone;
    }
};

// ---------------------------------------------------------------------------
// Fixed offsets
// ---------------------------------------------------------------------------

fn fixedOffset(text: []const u8) ?i32 {
    if (std.ascii.eqlIgnoreCase(text, "SYSTEM") or std.ascii.eqlIgnoreCase(text, "UTC")) return 0;
    if (std.mem.eql(u8, text, "Z")) return 0;
    if (std.mem.eql(u8, text, "CST")) return 8 * std.time.s_per_hour;
    if (std.mem.startsWith(u8, text, CCTZ_FIXED_PREFIX)) return cctzFixedOffset(text[CCTZ_FIXED_PREFIX.len..]);
    inline for (.{ "UTC", "GMT", "UT" }) |prefix| {
        if (text.len > prefix.len and std.mem.startsWith(u8, text, prefix) and isSign(text[prefix.len])) {
            return isoOffset(text[prefix.len..]);
        }
    }
    if (text.len == 0 or !isSign(text[0])) return null;
    return mysqlOffset(text) orelse isoOffset(text);
}

fn isSign(c: u8) bool {
    return c == '+' or c == '-';
}

/// MySQL's offset: `+H:MM` or `+HH:MM` (a one-digit minute reads as that
/// many minutes), from -13:59 to +14:00.
fn mysqlOffset(text: []const u8) ?i32 {
    if (text.len < 4) return null;
    var pos: usize = 1;
    var hours: i32 = 0;
    while (pos < text.len and std.ascii.isDigit(text[pos]) and hours < 100) : (pos += 1) hours = hours * 10 + (text[pos] - '0');
    if (pos + 1 >= text.len or text[pos] != ':') return null;
    pos += 1;
    var minutes: i32 = 0;
    while (pos < text.len and std.ascii.isDigit(text[pos]) and minutes < 100) : (pos += 1) minutes = minutes * 10 + (text[pos] - '0');
    if (pos != text.len or minutes > 59) return null;
    const offset = (hours * 60 + minutes) * (if (text[0] == '-') @as(i32, -60) else 60);
    if (offset < -(13 * 3600 + 59 * 60) or offset > 14 * 3600) return null;
    return offset;
}

/// The offsets Java's `ZoneOffset.of` reads, as StarRocks does: `+h`,
/// `+hh`, `+hh:mm`, `+hhmm`, `+hh:mm:ss` or `+hhmmss` (or `-`), minutes and
/// seconds below 60, up to 18 hours.
fn isoOffset(text: []const u8) ?i32 {
    const body = text[1..];
    const Fields = struct { h: []const u8, m: []const u8 = "", s: []const u8 = "" };
    const fields: Fields = switch (body.len) {
        1, 2 => .{ .h = body },
        4 => .{ .h = body[0..2], .m = body[2..4] },
        5 => if (body[2] == ':') .{ .h = body[0..2], .m = body[3..5] } else return null,
        6 => .{ .h = body[0..2], .m = body[2..4], .s = body[4..6] },
        8 => if (body[2] == ':' and body[5] == ':') .{ .h = body[0..2], .m = body[3..5], .s = body[6..8] } else return null,
        else => return null,
    };
    const h = decimalDigits(fields.h) orelse return null;
    const m = decimalDigits(fields.m) orelse return null;
    const s = decimalDigits(fields.s) orelse return null;
    if (m > 59 or s > 59) return null;
    const total = (h * 60 + m) * 60 + s;
    if (total > 18 * std.time.s_per_hour) return null;
    return if (text[0] == '-') -total else total;
}

const CCTZ_FIXED_PREFIX = "Fixed/UTC";

fn cctzFixedOffset(body: []const u8) ?i32 {
    if (body.len != 9 or !isSign(body[0]) or body[3] != ':' or body[6] != ':') return null;
    const h = decimalDigits(body[1..3]) orelse return null;
    const m = decimalDigits(body[4..6]) orelse return null;
    const s = decimalDigits(body[7..9]) orelse return null;
    if (m > 59 or s > 59) return null;
    const total = (h * 60 + m) * 60 + s;
    if (total > 24 * std.time.s_per_hour) return null;
    return if (body[0] == '-') -total else total;
}

/// The value of a run of ASCII digits; 0 for none.
fn decimalDigits(text: []const u8) ?i32 {
    var value: i32 = 0;
    for (text) |c| {
        if (!std.ascii.isDigit(c)) return null;
        value = value * 10 + (c - '0');
    }
    return value;
}

// ---------------------------------------------------------------------------
// Zones read from TZif files
// ---------------------------------------------------------------------------

/// A change of offset: at the UTC instant `at`, `before` gives way to `after`.
const Shift = struct {
    at: i64,
    before: i32,
    after: i32,

    /// The first local second past the local times that read with `before`:
    /// the ones a change skips or repeats read with it too.
    fn localLimit(shift: Shift) i64 {
        return shift.at + @max(shift.before, shift.after);
    }
};

pub const TzData = struct {
    /// Ascending by `at`.
    shifts: []const Shift,
    /// `shifts[i].localLimit()`, raised to the largest so far so that it
    /// ascends as a search needs.
    local_limits: []const i64,
    /// The offset before the first shift, time type 0's (RFC 8536 §3.2).
    initial: i32,
    /// The footer's rule for the times after the last shift; null keeps the
    /// last shift's offset.
    rule: ?PosixRule,

    fn offsetAt(data: *const TzData, instant: i64) i64 {
        if (data.shifts.len == 0) return if (data.rule) |rule| rule.offsetAt(instant) else data.initial;
        if (instant < data.shifts[0].at) return data.initial;
        const last = data.shifts[data.shifts.len - 1];
        if (instant > last.at) return if (data.rule) |rule| rule.offsetAt(instant) else last.after;
        var lo: usize = 0;
        var hi: usize = data.shifts.len;
        while (hi - lo > 1) {
            const mid = lo + (hi - lo) / 2;
            if (data.shifts[mid].at <= instant) lo = mid else hi = mid;
        }
        return data.shifts[lo].after;
    }

    fn localToUtc(data: *const TzData, local: i64) i64 {
        var lo: usize = 0;
        var hi: usize = data.local_limits.len;
        while (lo < hi) {
            const mid = lo + (hi - lo) / 2;
            if (data.local_limits[mid] > local) hi = mid else lo = mid + 1;
        }
        if (lo < data.shifts.len) return local - data.shifts[lo].before;
        if (data.rule) |rule| return rule.localToUtc(local);
        return local - (if (data.shifts.len == 0) data.initial else data.shifts[data.shifts.len - 1].after);
    }

    fn destroy(data: *const TzData, allocator: Allocator) void {
        allocator.free(data.shifts);
        allocator.free(data.local_limits);
        allocator.destroy(data);
    }
};

/// A TZif file's zone, or null when it isn't one thinDB reads: malformed,
/// carrying leap seconds (the `right/` zones, which cctz refuses too), or
/// ending in a footer that isn't a POSIX TZ rule.
fn parseTzif(allocator: Allocator, bytes: []const u8) error{OutOfMemory}!?*const TzData {
    var reader: std.Io.Reader = .fixed(bytes);
    var tz = std.Tz.parse(allocator, &reader) catch |err| switch (err) {
        error.OutOfMemory => return error.OutOfMemory,
        else => return null,
    };
    defer tz.deinit();
    if (tz.leapseconds.len > 0) return null;
    var rule: ?PosixRule = null;
    if (tz.footer) |footer| rule = parsePosixRule(footer) orelse return null;

    const shifts = try allocator.alloc(Shift, tz.transitions.len);
    errdefer allocator.free(shifts);
    const limits = try allocator.alloc(i64, tz.transitions.len);
    errdefer allocator.free(limits);
    const initial = tz.timetypes[0].offset;
    var before = initial;
    var limit: i64 = std.math.minInt(i64);
    for (tz.transitions, shifts, limits) |transition, *shift, *local_limit| {
        shift.* = .{ .at = transition.ts, .before = before, .after = transition.timetype.offset };
        limit = @max(limit, shift.localLimit());
        local_limit.* = limit;
        before = shift.after;
    }
    const data = try allocator.create(TzData);
    data.* = .{ .shifts = shifts, .local_limits = limits, .initial = initial, .rule = rule };
    return data;
}

// ---------------------------------------------------------------------------
// POSIX TZ rules (TZif footers)
// ---------------------------------------------------------------------------

/// A POSIX TZ string's rule, such as `EST5EDT,M3.2.0,M11.1.0`: a standard
/// offset and, optionally, a daylight offset with the yearly days and local
/// times it starts and ends.
pub const PosixRule = struct {
    /// Seconds east of UTC (POSIX writes them west).
    std_offset: i32,
    dst: ?Daylight = null,

    const Daylight = struct {
        offset: i32,
        start: DayRule,
        /// Seconds after local standard midnight.
        start_time: i32,
        end: DayRule,
        /// Seconds after local daylight midnight.
        end_time: i32,
    };

    fn startAt(rule: PosixRule, daylight: Daylight, year: i32) i64 {
        return daylight.start.day(year) * std.time.s_per_day + daylight.start_time - rule.std_offset;
    }

    fn endAt(daylight: Daylight, year: i32) i64 {
        return daylight.end.day(year) * std.time.s_per_day + daylight.end_time - daylight.offset;
    }

    fn offsetAt(rule: PosixRule, instant: i64) i64 {
        const daylight = rule.dst orelse return rule.std_offset;
        const year = yearOf(instant + rule.std_offset);
        const start = rule.startAt(daylight, year);
        const end = endAt(daylight, year);
        const in_daylight = if (start < end) instant >= start and instant < end else !(instant >= end and instant < start);
        return if (in_daylight) daylight.offset else rule.std_offset;
    }

    fn localToUtc(rule: PosixRule, local: i64) i64 {
        const daylight = rule.dst orelse return local - rule.std_offset;
        const year = yearOf(local);
        var shifts: [6]Shift = undefined;
        for (0..3) |k| {
            const y = year - 1 + @as(i32, @intCast(k));
            shifts[2 * k] = .{ .at = rule.startAt(daylight, y), .before = rule.std_offset, .after = daylight.offset };
            shifts[2 * k + 1] = .{ .at = endAt(daylight, y), .before = daylight.offset, .after = rule.std_offset };
        }
        std.sort.insertion(Shift, &shifts, {}, struct {
            fn earlier(_: void, a: Shift, b: Shift) bool {
                return a.at < b.at;
            }
        }.earlier);
        var limit: i64 = std.math.minInt(i64);
        for (shifts) |shift| {
            limit = @max(limit, shift.localLimit());
            if (local < limit) return local - shift.before;
        }
        return local - shifts[shifts.len - 1].after;
    }
};

/// A day of the year in a POSIX TZ rule.
const DayRule = union(enum) {
    /// `Jn`: day n of 1-365, never counting February 29.
    julian: u16,
    /// `n`: day n of 0-365, counting February 29.
    day_of_year: u16,
    /// `Mm.w.d`: weekday d (0 is Sunday) of week w (5 is the last) of month m.
    month_week_day: struct { month: u8, week: u8, weekday: u8 },

    /// Days since 1970-01-01.
    fn day(rule: DayRule, year: i32) i64 {
        const jan1: i64 = common.ymdToDays(year, 1, 1);
        switch (rule) {
            .julian => |n| return jan1 + n - 1 + @intFromBool(n >= 60 and common.isLeapYear(year)),
            .day_of_year => |n| return jan1 + n,
            .month_week_day => |mwd| {
                const first: i64 = common.ymdToDays(year, mwd.month, 1);
                const first_weekday = @mod(first + 4, 7);
                var offset = @mod(@as(i64, mwd.weekday) - first_weekday, 7) + (@as(i64, mwd.week) - 1) * 7;
                while (offset >= common.lastDayOfMonth(year, mwd.month)) offset -= 7;
                return first + offset;
            },
        }
    }
};

/// The year a local time (seconds since the epoch) falls in.
fn yearOf(local_seconds: i64) i32 {
    const days = std.math.clamp(@divFloor(local_seconds, std.time.s_per_day), common.FIRST_DATE_DAYS - 366, common.LAST_DATE_DAYS + 366);
    return common.daysToYmd(@intCast(days)).year;
}

/// `text` as a POSIX TZ rule (POSIX.1-2017 §8.3, with RFC 8536's version 3
/// extensions: rule times from -167 to 167 hours), or null. A daylight
/// name without rules takes the US rules, as glibc's default does.
pub fn parsePosixRule(text: []const u8) ?PosixRule {
    var p: RuleParser = .{ .text = text };
    if (!p.skipName()) return null;
    const std_west = p.clock(24) orelse return null;
    var rule: PosixRule = .{ .std_offset = -std_west };
    if (p.done()) return rule;
    if (!p.skipName()) return null;
    var daylight: PosixRule.Daylight = .{
        .offset = rule.std_offset + std.time.s_per_hour,
        .start = .{ .month_week_day = .{ .month = 3, .week = 2, .weekday = 0 } },
        .start_time = 2 * std.time.s_per_hour,
        .end = .{ .month_week_day = .{ .month = 11, .week = 1, .weekday = 0 } },
        .end_time = 2 * std.time.s_per_hour,
    };
    if (!p.done() and p.text[p.pos] != ',') daylight.offset = -(p.clock(24) orelse return null);
    if (!p.done()) {
        if (!p.eat(',')) return null;
        daylight.start = p.dayRule() orelse return null;
        if (p.eat('/')) daylight.start_time = p.clock(167) orelse return null;
        if (!p.eat(',')) return null;
        daylight.end = p.dayRule() orelse return null;
        if (p.eat('/')) daylight.end_time = p.clock(167) orelse return null;
        if (!p.done()) return null;
    }
    rule.dst = daylight;
    return rule;
}

const RuleParser = struct {
    text: []const u8,
    pos: usize = 0,

    fn done(p: *const RuleParser) bool {
        return p.pos == p.text.len;
    }

    fn eat(p: *RuleParser, c: u8) bool {
        if (p.done() or p.text[p.pos] != c) return false;
        p.pos += 1;
        return true;
    }

    /// A zone abbreviation: three or more letters, or `<...>` quoting
    /// letters, digits, `+` and `-`.
    fn skipName(p: *RuleParser) bool {
        const start = p.pos;
        if (p.eat('<')) {
            while (!p.done() and (std.ascii.isAlphanumeric(p.text[p.pos]) or isSign(p.text[p.pos]))) p.pos += 1;
            return p.pos > start + 1 and p.eat('>');
        }
        while (!p.done() and std.ascii.isAlphabetic(p.text[p.pos])) p.pos += 1;
        return p.pos - start >= 3;
    }

    fn number(p: *RuleParser, max_digits: usize) ?i32 {
        const start = p.pos;
        var value: i32 = 0;
        while (!p.done() and std.ascii.isDigit(p.text[p.pos]) and p.pos - start < max_digits) : (p.pos += 1) {
            value = value * 10 + (p.text[p.pos] - '0');
        }
        return if (p.pos == start) null else value;
    }

    /// `[+-]h[:mm[:ss]]` as signed seconds, hours up to `max_hours`.
    fn clock(p: *RuleParser, max_hours: i32) ?i32 {
        const negative = p.eat('-');
        if (!negative) _ = p.eat('+');
        const hours = p.number(3) orelse return null;
        var minutes: i32 = 0;
        var seconds: i32 = 0;
        if (p.eat(':')) {
            minutes = p.number(2) orelse return null;
            if (p.eat(':')) seconds = p.number(2) orelse return null;
        }
        if (hours > max_hours or minutes > 59 or seconds > 59) return null;
        const total = (hours * 60 + minutes) * 60 + seconds;
        return if (negative) -total else total;
    }

    fn dayRule(p: *RuleParser) ?DayRule {
        if (p.eat('J')) {
            const n = p.number(3) orelse return null;
            return if (n >= 1 and n <= 365) .{ .julian = @intCast(n) } else null;
        }
        if (p.eat('M')) {
            const month = p.number(2) orelse return null;
            if (!p.eat('.')) return null;
            const week = p.number(1) orelse return null;
            if (!p.eat('.')) return null;
            const weekday = p.number(1) orelse return null;
            if (month < 1 or month > 12 or week < 1 or week > 5 or weekday > 6) return null;
            return .{ .month_week_day = .{ .month = @intCast(month), .week = @intCast(week), .weekday = @intCast(weekday) } };
        }
        const n = p.number(3) orelse return null;
        return if (n <= 365) .{ .day_of_year = @intCast(n) } else null;
    }
};

// ---------------------------------------------------------------------------
// The process's zone cache
// ---------------------------------------------------------------------------

/// A zone file is a few KiB; anything past this isn't one.
const MAX_ZONE_FILE_BYTES = 1 << 20;
const MAX_ZONE_NAME_BYTES = 255;
/// Names found to be no zone stay cached up to this many, so a column of
/// distinct unknown names can't grow the cache without bound.
const MAX_UNKNOWN_NAMES = 4096;

/// Process-wide on purpose: zones are the host's files, read once and never
/// changed, so every database shares them. Held only for map lookups and
/// inserts; files are read outside it.
const Cache = struct {
    lock: std.atomic.Mutex = .unlocked,
    zones: std.StringHashMapUnmanaged(?*const TzData) = .empty,
    unknown: usize = 0,

    fn acquire(cache: *Cache) void {
        while (!cache.lock.tryLock()) std.atomic.spinLoopHint();
    }
};

var zone_cache: Cache = .{};
const cache_allocator = std.heap.smp_allocator;

fn isZoneName(text: []const u8) bool {
    if (text.len == 0 or text.len > MAX_ZONE_NAME_BYTES) return false;
    var parts = std.mem.splitScalar(u8, text, '/');
    while (parts.next()) |part| {
        if (part.len == 0 or part[0] == '.') return false;
        for (part) |c| {
            if (!(std.ascii.isAlphanumeric(c) or c == '_' or c == '-' or c == '+' or c == '.')) return false;
        }
    }
    return true;
}

fn lookupNamed(name: []const u8) error{OutOfMemory}!?*const TzData {
    zone_cache.acquire();
    if (zone_cache.zones.get(name)) |known| {
        zone_cache.lock.unlock();
        return known;
    }
    zone_cache.lock.unlock();

    const loaded = try loadZone(name);
    errdefer if (loaded) |data| data.destroy(cache_allocator);

    zone_cache.acquire();
    defer zone_cache.lock.unlock();
    if (zone_cache.zones.get(name)) |known| {
        if (loaded) |data| data.destroy(cache_allocator);
        return known;
    }
    if (loaded == null and zone_cache.unknown >= MAX_UNKNOWN_NAMES) return null;
    const key = try cache_allocator.dupe(u8, name);
    errdefer cache_allocator.free(key);
    try zone_cache.zones.put(cache_allocator, key, loaded);
    if (loaded == null) zone_cache.unknown += 1;
    return loaded;
}

fn zoneDirectory() ?[]const u8 {
    if (getenv("TZDIR")) |dir| {
        const path = std.mem.span(dir);
        if (path.len > 0) return path;
    }
    return if (builtin.os.tag == .windows) null else "/usr/share/zoneinfo";
}

fn loadZone(name: []const u8) error{OutOfMemory}!?*const TzData {
    const dir = zoneDirectory() orelse return null;
    const path = try std.fmt.allocPrint(cache_allocator, "{s}/{s}", .{ dir, name });
    defer cache_allocator.free(path);
    // Kernels carry no Io. A private single-threaded instance only opens and
    // reads one file here, which needs no shared state.
    var threaded: std.Io.Threaded = .init_single_threaded;
    const bytes = std.Io.Dir.cwd().readFileAlloc(threaded.io(), path, cache_allocator, .limited(MAX_ZONE_FILE_BYTES)) catch |err| switch (err) {
        error.OutOfMemory => return error.OutOfMemory,
        else => return null,
    };
    defer cache_allocator.free(bytes);
    return parseTzif(cache_allocator, bytes);
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

const t = std.testing;

test "time zone: fixed offsets as MySQL and StarRocks read them" {
    const hour = std.time.s_per_hour;
    const cases = .{
        .{ "SYSTEM", 0 },                    .{ "utc", 0 },                         .{ "UTC", 0 },
        .{ "Z", 0 },                         .{ "CST", 8 * hour },                  .{ "+14:00", 14 * hour },
        .{ "-13:59", -50_340 },              .{ "+5:30", 19_800 },                  .{ "+05:3", 18_180 },
        .{ "+14:01", 50_460 },               .{ "-14:00", -14 * hour },             .{ "+0530", 19_800 },
        .{ "+8", 8 * hour },                 .{ "+08", 8 * hour },                  .{ "-0", 0 },
        .{ "+080000", 8 * hour },            .{ "+08:00:30", 28_830 },              .{ "+18:00", 18 * hour },
        .{ "-18:00", -18 * hour },           .{ "+17:59", 64_740 },                 .{ "UTC+8", 8 * hour },
        .{ "UTC-8", -8 * hour },             .{ "GMT+18", 18 * hour },              .{ "UT+8", 8 * hour },
        .{ "UTC+08:00", 8 * hour },          .{ "UTC+0", 0 },                       .{ "+08:0030", 30_600 },
        .{ "Fixed/UTC+08:00:00", 8 * hour }, .{ "Fixed/UTC-24:00:00", -24 * hour },
    };
    inline for (cases) |c| try t.expectEqual(@as(?i32, c[1]), fixedOffset(c[0]));
    const refused = .{
        "",         "z",         "+",        "-",            "+05:60",          "+1:00x",             "+18:01",             "-18:01",
        "+19:00",   "+23:59",    "+24:00",   "08:00",        "+08-00",          "+123",               "+12345",             "+0860",
        "+0800:00", "+08:00:60", "UTC+8:00", "UTC+",         "UTC0",            "GMT+19",             "utc+8",              "cst",
        " +08:00",  "+08:00 ",   "++08:00",  "Europe/Paris", "Fixed/UTC+08:00", "Fixed/UTC+24:00:01", "Fixed/UTC+08:60:00", "Fixed/UTC08:00:00",
    };
    inline for (refused) |z| try t.expectEqual(@as(?i32, null), fixedOffset(z));
}

test "time zone: names stay inside the zone directory" {
    inline for (.{ "America/New_York", "Etc/GMT+5", "America/Port-au-Prince", "UTC", "posixrules" }) |name| {
        try t.expect(isZoneName(name));
    }
    inline for (.{ "", "/etc/passwd", "../x", "Asia/../Asia/Shanghai", "./Asia/Shanghai", "Asia//Shanghai", "Asia/Shanghai/", ".hidden", "a\\b", "a b", "a:b" }) |name| {
        try t.expect(!isZoneName(name));
    }
}

fn utcSeconds(year: i32, month: u32, day: u32, hour: i64, minute: i64) i64 {
    return @as(i64, common.ymdToDays(year, month, day)) * std.time.s_per_day + hour * 3600 + minute * 60;
}

test "time zone: POSIX rules place daylight time in both hemispheres" {
    const hour = std.time.s_per_hour;
    const new_york = parsePosixRule("EST5EDT,M3.2.0,M11.1.0").?;
    // 2040's changes: 11 March 07:00 UTC and 4 November 06:00 UTC.
    try t.expectEqual(@as(i64, -5 * hour), new_york.offsetAt(utcSeconds(2040, 3, 11, 6, 59)));
    try t.expectEqual(@as(i64, -4 * hour), new_york.offsetAt(utcSeconds(2040, 3, 11, 7, 0)));
    try t.expectEqual(@as(i64, -4 * hour), new_york.offsetAt(utcSeconds(2040, 11, 4, 5, 59)));
    try t.expectEqual(@as(i64, -5 * hour), new_york.offsetAt(utcSeconds(2040, 11, 4, 6, 0)));
    // Skipped 02:30 reads as standard time; repeated 01:30 as its first
    // occurrence, in daylight time.
    try t.expectEqual(utcSeconds(2040, 3, 11, 7, 30), new_york.localToUtc(utcSeconds(2040, 3, 11, 2, 30)));
    try t.expectEqual(utcSeconds(2040, 11, 4, 5, 30), new_york.localToUtc(utcSeconds(2040, 11, 4, 1, 30)));
    try t.expectEqual(utcSeconds(2040, 11, 4, 7, 0), new_york.localToUtc(utcSeconds(2040, 11, 4, 2, 0)));

    const lord_howe = parsePosixRule("<+1030>-10:30<+11>-11,M10.1.0,M4.1.0").?;
    try t.expectEqual(@as(i64, 11 * hour), lord_howe.offsetAt(utcSeconds(2040, 1, 15, 12, 0)));
    try t.expectEqual(@as(i64, 10 * hour + 1800), lord_howe.offsetAt(utcSeconds(2040, 7, 1, 12, 0)));

    // Dublin's daylight offset is its winter one.
    const dublin = parsePosixRule("IST-1GMT0,M10.5.0,M3.5.0/1").?;
    try t.expectEqual(@as(i64, 0), dublin.offsetAt(utcSeconds(2040, 1, 15, 12, 0)));
    try t.expectEqual(@as(i64, hour), dublin.offsetAt(utcSeconds(2040, 7, 1, 12, 0)));

    // Version 3 rule times below zero.
    const nuuk = parsePosixRule("<-02>2<-01>,M3.5.0/-1,M10.5.0/0").?;
    try t.expectEqual(@as(i64, -2 * hour), nuuk.offsetAt(utcSeconds(2040, 1, 15, 12, 0)));
    try t.expectEqual(@as(i64, -hour), nuuk.offsetAt(utcSeconds(2040, 7, 1, 12, 0)));

    try t.expectEqual(@as(i64, 8 * hour), parsePosixRule("CST-8").?.offsetAt(0));
    try t.expectEqual(@as(i64, 5 * hour + 45 * 60), parsePosixRule("<+0545>-5:45").?.offsetAt(0));
    const julian = parsePosixRule("AAA0BBB,J60/0,J61/0").?;
    try t.expectEqual(@as(i64, hour), julian.offsetAt(utcSeconds(2040, 3, 1, 12, 0)));
    try t.expectEqual(@as(i64, 0), julian.offsetAt(utcSeconds(2040, 2, 29, 12, 0)));

    inline for (.{ "", "E5", "EST", "EST5EDT,M3.2.0", "EST5EDT,M13.1.0,M11.1.0", "EST5EDT,M3.2.0,M11.1.0x", "<>5", "EST25" }) |bad| {
        try t.expectEqual(@as(?PosixRule, null), parsePosixRule(bad));
    }
}

test "time zone: a zone file gives its changes, then its footer's rule" {
    if (getenv("TZDIR") == null) return error.SkipZigTest;
    const hour = std.time.s_per_hour;
    const new_york = (try resolve("America/New_York")).?;
    try t.expectEqual(@as(i64, -5 * hour), new_york.offsetAt(0));
    try t.expectEqual(@as(i64, -5 * hour), new_york.offsetAt(1_772_953_199));
    try t.expectEqual(@as(i64, -4 * hour), new_york.offsetAt(1_772_953_200));
    try t.expectEqual(@as(i64, -4 * hour), new_york.offsetAt(1_793_512_799));
    try t.expectEqual(@as(i64, -5 * hour), new_york.offsetAt(1_793_512_800));
    try t.expectEqual(@as(i64, -4 * hour), new_york.offsetAt(4_118_126_400));
    // Local mean time before 1883: -4:56:02.
    try t.expectEqual(@as(i64, -17_762), new_york.offsetAt(utcSeconds(1800, 1, 1, 0, 0)));
    try t.expectEqual(utcSeconds(2026, 3, 8, 7, 30), new_york.localToUtc(utcSeconds(2026, 3, 8, 2, 30)));
    try t.expectEqual(utcSeconds(2026, 11, 1, 5, 30), new_york.localToUtc(utcSeconds(2026, 11, 1, 1, 30)));
    try t.expectEqual(utcSeconds(2040, 7, 1, 12, 0), new_york.localToUtc(utcSeconds(2040, 7, 1, 8, 0)));

    const shanghai = (try resolve("Asia/Shanghai")).?;
    try t.expectEqual(@as(i64, 8 * hour), shanghai.offsetAt(0));
    try t.expectEqual(new_york.named, (try resolve("America/New_York")).?.named);

    inline for (.{ "America", "America/Nowhere", "Mars/Olympus_Mons" }) |unknown| {
        try t.expectEqual(@as(?Zone, null), try resolve(unknown));
    }
}

test "time zone: threads resolving at once share one parse" {
    if (getenv("TZDIR") == null) return error.SkipZigTest;
    const Resolver = struct {
        fn run(out: *?Zone, failed: *bool) void {
            out.* = resolve("Australia/Lord_Howe") catch {
                failed.* = true;
                return;
            };
        }
    };
    var zones: [8]?Zone = @splat(null);
    var failed: [8]bool = @splat(false);
    var threads: [8]std.Thread = undefined;
    for (&threads, &zones, &failed) |*thread, *zone, *fail| thread.* = try std.Thread.spawn(.{}, Resolver.run, .{ zone, fail });
    for (threads) |thread| thread.join();
    for (zones, failed) |zone, fail| {
        try t.expect(!fail);
        try t.expectEqual(zones[0].?.named, zone.?.named);
    }
}

test "time zone: the memo resolves a repeated zone once" {
    var memo: Memo = .{};
    try t.expectEqual(@as(?Zone, .{ .fixed = 3600 }), try memo.zoneOf("+01:00"));
    try t.expectEqual(@as(?Zone, .{ .fixed = 3600 }), try memo.zoneOf("+01:00"));
    try t.expectEqual(@as(?Zone, null), try memo.zoneOf("+99:00"));
    try t.expectEqual(@as(?Zone, .{ .fixed = 0 }), try memo.zoneOf("UTC"));
}
