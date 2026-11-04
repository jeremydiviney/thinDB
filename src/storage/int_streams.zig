//! Integer-stream transforms (DESIGN.md §4.3). Before a column block is
//! compressed, each integer array in its payload may be rewritten so the
//! codec finds more to squeeze: as offsets from a frame of reference or as
//! successive differences, divided by the values' common factor, stored at
//! the narrowest byte width that holds them, and optionally transposed into
//! byte planes. An array keeps whichever form a fast trial on its first
//! values compresses smallest, its original form included. The reader
//! restores the exact payload right after decompressing it, so the block
//! cache and every decoder past it see what the encoder wrote.
//!
//! Stored bytes of a block whose header sets `ColumnBlockFlags.int_streams`:
//!
//!   the directory, uncompressed:
//!   [stream_count: u32]
//!   per rewritten array, `entry_size` bytes:
//!     [offset: u32]        its byte offset in the payload
//!     [count: u32]         values
//!     [width: u8]          bytes per value in the payload: 1, 2, 3, 4 or 8
//!     [method: u8]         `Method`, plus `shuffled_bit` for byte planes
//!     [stored_width: u8]   bytes per stored value: 0, 1, 2, 3, 4 or 8,
//!                          never more than `width`
//!     [pad: u8]
//!     [base: u64][factor: u64][first: u64]
//!   the codec frame of the stored payload: the payload, each rewritten array
//!   replaced by its stored values
//!
//! Every array is stored no wider than it was, so the stored payload is never
//! longer than the payload, by as much as the directory says. The reader
//! decompresses it into the tail of the payload's own buffer and restores
//! front to back in place: what it writes never overtakes what it has yet to
//! read. Keeping the directory out of the frame keeps that true for an array
//! that only changes form (byte planes at its own width).
//!
//! Arithmetic is modulo 2^64 and every value is written back truncated to
//! `width` bytes, so restoring is exact for any input (a stored value
//! truncated to `width` bytes too):
//!   .plain   value_i = stored_i
//!   .frame   value_i = base + stored_i * factor
//!   .delta   value_0 = first,
//!            value_i = value_(i-1) + base + stored_(i-1) * factor
//!            (count - 1 stored values)

const std = @import("std");
const Allocator = std.mem.Allocator;

const format = @import("format.zig");
const compression = @import("compression.zig");

/// An integer array inside a block payload.
pub const Stream = struct {
    offset: u32,
    count: u32,
    /// Bytes per little-endian value: 1, 2, 3, 4 or 8.
    width: u8,
    /// Two's-complement values: a frame of reference starts at their signed
    /// minimum.
    signed: bool = false,
};

pub const Method = enum(u8) { plain = 0, frame = 1, delta = 2 };
pub const shuffled_bit: u8 = 0x80;
pub const entry_size: usize = 36;

/// Values a trial compresses per candidate form: enough to rank forms as
/// well as a trial on the whole array, at a fraction of its cost.
const trial_values: usize = 8192;
/// Arrays shorter than this stay as they are: their entry would eat most of
/// what a rewrite saves.
const min_values: u32 = 64;
const trial_level: c_int = 1;

const Form = struct {
    method: Method,
    shuffled: bool = false,
    stored_width: u8,
    base: u64 = 0,
    factor: u64 = 1,
    first: u64 = 0,
};

const Rewrite = struct { stream: Stream, form: Form };

/// Appends to `directory` and `out` the directory and stored payload of
/// `payload` with each of `streams` (in payload order, not overlapping)
/// rewritten in the form a trial compresses smallest, and returns true; or
/// returns false, leaving both as they were, when every array is smallest as
/// it is.
pub fn transform(
    allocator: Allocator,
    compressor: *compression.Compressor,
    payload: []const u8,
    streams: []const Stream,
    directory: *std.ArrayList(u8),
    out: *std.ArrayList(u8),
) !bool {
    var rewrites: std.ArrayList(Rewrite) = .empty;
    defer rewrites.deinit(allocator);
    const trial = try allocator.alloc(u8, trial_values * 8);
    defer allocator.free(trial);
    const trial_out = try allocator.alloc(u8, compression.compressBound(trial.len));
    defer allocator.free(trial_out);
    const window = try allocator.alloc(u64, 2 * trial_values);
    defer allocator.free(window);

    for (streams) |s| {
        if (s.count < min_values) continue;
        const bytes = payload[s.offset..][0 .. @as(usize, s.count) * s.width];
        const n: usize = @min(s.count, trial_values);
        loadValues(bytes, s.width, s.signed, window[0..n]);
        const choice = try pick(compressor, window[0..n], bytes[0 .. n * s.width], s, window[trial_values..], trial, trial_out) orelse continue;
        try rewrites.append(allocator, .{ .stream = s, .form = choice });
    }
    if (rewrites.items.len == 0) return false;

    const directory_start = directory.items.len;
    errdefer directory.shrinkRetainingCapacity(directory_start);
    const start = out.items.len;
    errdefer out.shrinkRetainingCapacity(start);
    try directory.ensureUnusedCapacity(allocator, 4 + rewrites.items.len * entry_size);
    appendU32Assume(directory, @intCast(rewrites.items.len));
    const entries_at = directory.items.len;
    directory.appendNTimesAssumeCapacity(0, rewrites.items.len * entry_size);

    var at: usize = 0;
    for (rewrites.items, 0..) |*r, i| {
        const s = r.stream;
        try out.appendSlice(allocator, payload[at..s.offset]);
        const values = try allocator.alloc(u64, s.count);
        defer allocator.free(values);
        loadValues(payload[s.offset..][0 .. @as(usize, s.count) * s.width], s.width, s.signed, values);
        // The trial picked the form on the first values; its parameters
        // come from them all. Every rewritten array has `min_values`, so
        // each method fits.
        const shuffled = r.form.shuffled;
        r.form = fit(values, r.form.method, s).?;
        r.form.shuffled = shuffled;
        try appendStored(allocator, out, values, r.form);
        writeEntry(directory.items[entries_at + i * entry_size ..][0..entry_size], s, r.form);
        at = s.offset + @as(usize, s.count) * s.width;
    }
    try out.appendSlice(allocator, payload[at..]);
    return true;
}

/// The directory heading a block's stored bytes.
pub const Directory = struct {
    /// `stream_count`, then the entries.
    bytes: []const u8,
    /// Bytes of the stored payload in the codec frame after the directory.
    stored_len: usize,
};

/// Reads the directory at the start of `stored`, the stored bytes of a
/// payload of `payload_len` bytes.
pub fn readDirectory(stored: []const u8, payload_len: usize) !Directory {
    const corrupt = format.Error.CorruptColumnBlockHeader;
    if (stored.len < 4) return corrupt;
    const n = format.readU32(stored[0..4]);
    if (n > (stored.len - 4) / entry_size) return corrupt;
    const bytes = stored[0 .. 4 + @as(usize, n) * entry_size];
    var shed: usize = 0;
    for (0..n) |i| {
        const e = try readEntry(bytes[4 + i * entry_size ..][0..entry_size]);
        shed += @as(usize, e.stream.count) * e.stream.width - storedCount(e.stream.count, e.form.method) * e.form.stored_width;
    }
    if (shed > payload_len) return corrupt;
    return .{ .bytes = bytes, .stored_len = payload_len - shed };
}

/// Rebuilds the payload filling `buf` in place from its stored payload,
/// which fills `buf[buf.len - directory.stored_len ..]`. `allocator` holds a
/// byte-plane array while it is restored.
pub fn restore(allocator: Allocator, directory: Directory, buf: []u8) !void {
    const corrupt = format.Error.CorruptColumnBlockHeader;
    if (directory.stored_len > buf.len) return corrupt;
    var planes: []u8 = &.{};
    defer allocator.free(planes);
    var src: usize = buf.len - directory.stored_len;
    var dst: usize = 0;
    const n = (directory.bytes.len - 4) / entry_size;
    for (0..n) |i| {
        const e = try readEntry(directory.bytes[4 + i * entry_size ..][0..entry_size]);
        const len = @as(usize, e.stream.count) * e.stream.width;
        if (e.stream.offset < dst or e.stream.offset + len > buf.len) return corrupt;
        const gap = e.stream.offset - dst;
        if (src + gap > buf.len) return corrupt;
        @memmove(buf[dst..e.stream.offset], buf[src..][0..gap]);
        src += gap;
        dst = e.stream.offset;
        const array_len = storedCount(e.stream.count, e.form.method) * e.form.stored_width;
        if (src + array_len > buf.len) return corrupt;
        var in: []const u8 = buf[src..][0..array_len];
        if (e.form.shuffled) {
            // Each byte plane is read across the whole array, ahead of what
            // the restore writes, so the planes are moved out first.
            if (planes.len < array_len) {
                allocator.free(planes);
                planes = &.{};
                planes = try allocator.alloc(u8, array_len);
            }
            @memcpy(planes[0..array_len], in);
            in = planes[0..array_len];
        }
        decode(in, e.stream, e.form, buf[dst..][0..len]);
        src += array_len;
        dst += len;
    }
    // The bytes past the last array are already in place.
    if (src != dst) return corrupt;
}

fn storedCount(count: u32, method: Method) usize {
    return if (method == .delta) count - 1 else count;
}

/// The form a trial over `values` (the array's first values, `raw` their
/// bytes) compresses smallest, or null when that is the array as it is.
fn pick(
    compressor: *compression.Compressor,
    values: []const u64,
    raw: []const u8,
    s: Stream,
    stored: []u64,
    trial: []u8,
    trial_out: []u8,
) !?Form {
    var best_size = try compressor.compressedSize(trial_out, raw, trial_level);
    var best: ?Form = null;
    for ([_]Method{ .plain, .frame, .delta }) |method| {
        var form = fit(values, method, s) orelse continue;
        const kept = storedValues(values, form, stored);
        for ([_]bool{ false, true }) |shuffled| {
            if (method == .plain and !shuffled) continue;
            if (shuffled and form.stored_width < 2) continue;
            form.shuffled = shuffled;
            const len = writeStored(trial, kept, form);
            const size = entry_size + try compressor.compressedSize(trial_out, trial[0..len], trial_level);
            if (size < best_size) {
                best_size = size;
                best = form;
            }
        }
    }
    return best;
}

/// `method`'s parameters over `values`, or null when it can't apply.
fn fit(values: []const u64, method: Method, s: Stream) ?Form {
    switch (method) {
        .plain => return .{ .method = .plain, .stored_width = s.width },
        .frame => {
            var base = values[0];
            for (values[1..]) |v| {
                const lower = if (s.signed) @as(i64, @bitCast(v)) < @as(i64, @bitCast(base)) else v < base;
                if (lower) base = v;
            }
            const spread = spreadOf(values, base, 0);
            return .{ .method = .frame, .stored_width = @min(narrowWidth(spread.max), s.width), .base = base, .factor = spread.factor };
        },
        .delta => {
            if (values.len < 2) return null;
            var base: i64 = std.math.maxInt(i64);
            var prev = values[0];
            for (values[1..]) |v| {
                base = @min(base, @as(i64, @bitCast(v -% prev)));
                prev = v;
            }
            const spread = spreadOf(values, @bitCast(base), 1);
            return .{ .method = .delta, .stored_width = @min(narrowWidth(spread.max), s.width), .base = @bitCast(base), .factor = spread.factor, .first = values[0] };
        },
    }
}

const Spread = struct { max: u64, factor: u64 };

/// The largest offset from `base` and the offsets' common factor, over
/// `values` themselves (`order` 0) or their successive differences (1).
fn spreadOf(values: []const u64, base: u64, comptime order: u1) Spread {
    var max: u64 = 0;
    var g: u64 = 0;
    for (order..values.len) |i| {
        const x = if (order == 0) values[i] else values[i] -% values[i - 1];
        const off = x -% base;
        max = @max(max, off);
        if (off == 0 or g == 1) continue;
        if (g == 0) g = off else if (off % g != 0) g = std.math.gcd(g, off);
    }
    const factor = @max(g, 1);
    return .{ .max = max / factor, .factor = factor };
}

fn narrowWidth(max: u64) u8 {
    if (max == 0) return 0;
    if (max <= std.math.maxInt(u8)) return 1;
    if (max <= std.math.maxInt(u16)) return 2;
    if (max <= std.math.maxInt(u32)) return 4;
    return 8;
}

/// `values` as `form` stores them, into `out`; returns the stored slice.
fn storedValues(values: []const u64, form: Form, out: []u64) []const u64 {
    switch (form.method) {
        .plain => {
            @memcpy(out[0..values.len], values);
            return out[0..values.len];
        },
        .frame => {
            for (values, out[0..values.len]) |v, *o| o.* = (v -% form.base) / form.factor;
            return out[0..values.len];
        },
        .delta => {
            for (values[1..], values[0 .. values.len - 1], out[0 .. values.len - 1]) |v, prev, *o| {
                o.* = ((v -% prev) -% form.base) / form.factor;
            }
            return out[0 .. values.len - 1];
        },
    }
}

/// Writes `stored` at `form.stored_width` into `out`, in byte planes when
/// shuffled; returns the bytes written.
fn writeStored(out: []u8, stored: []const u64, form: Form) usize {
    const w = form.stored_width;
    if (form.shuffled) {
        for (0..w) |b| {
            const plane = out[b * stored.len ..][0..stored.len];
            const shift: u6 = @intCast(b * 8);
            for (stored, plane) |v, *o| o.* = @truncate(v >> shift);
        }
    } else {
        for (stored, 0..) |v, i| {
            var b8: [8]u8 = undefined;
            std.mem.writeInt(u64, &b8, v, .little);
            @memcpy(out[i * w ..][0..w], b8[0..w]);
        }
    }
    return stored.len * w;
}

/// Appends `values` in `form` to `out`, reusing `values` as scratch.
fn appendStored(allocator: Allocator, out: *std.ArrayList(u8), values: []u64, form: Form) !void {
    const n = storedCount(@intCast(values.len), form.method);
    switch (form.method) {
        .plain => {},
        .frame => for (values) |*v| {
            v.* = (v.* -% form.base) / form.factor;
        },
        .delta => {
            for (0..n) |i| values[i] = ((values[i + 1] -% values[i]) -% form.base) / form.factor;
        },
    }
    const bytes = try out.addManyAsSlice(allocator, n * form.stored_width);
    _ = writeStored(bytes, values[0..n], form);
}

fn writeEntry(out: *[entry_size]u8, s: Stream, form: Form) void {
    format.writeU32(out[0..4], s.offset);
    format.writeU32(out[4..8], s.count);
    out[8] = s.width;
    out[9] = @backingInt(form.method) | (if (form.shuffled) shuffled_bit else 0);
    out[10] = form.stored_width;
    out[11] = 0;
    std.mem.writeInt(u64, out[12..20], form.base, .little);
    std.mem.writeInt(u64, out[20..28], form.factor, .little);
    std.mem.writeInt(u64, out[28..36], form.first, .little);
}

const Entry = struct { stream: Stream, form: Form };

fn readEntry(in: *const [entry_size]u8) !Entry {
    const corrupt = format.Error.CorruptColumnBlockHeader;
    const width = in[8];
    const stored_width = in[10];
    const method = std.enums.fromInt(Method, in[9] & ~shuffled_bit) orelse return corrupt;
    const count = format.readU32(in[4..8]);
    switch (width) {
        1, 2, 3, 4, 8 => {},
        else => return corrupt,
    }
    switch (stored_width) {
        0, 1, 2, 3, 4, 8 => {},
        else => return corrupt,
    }
    if (stored_width > width) return corrupt;
    if (method == .plain and stored_width != width) return corrupt;
    if (method == .delta and count == 0) return corrupt;
    return .{
        .stream = .{ .offset = format.readU32(in[0..4]), .count = count, .width = width },
        .form = .{
            .method = method,
            .shuffled = in[9] & shuffled_bit != 0,
            .stored_width = stored_width,
            .base = std.mem.readInt(u64, in[12..20], .little),
            .factor = std.mem.readInt(u64, in[20..28], .little),
            .first = std.mem.readInt(u64, in[28..36], .little),
        },
    };
}

/// Restores one array: `in` holds its stored values, `out` its payload bytes.
fn decode(in: []const u8, s: Stream, form: Form, out: []u8) void {
    const chunk = 1024;
    var buf: [chunk]u64 = undefined;
    const n = storedCount(s.count, form.method);
    var prev = form.first;
    var dst: usize = 0;
    if (form.method == .delta) {
        storeValues(out, s.width, 0, &.{prev});
        dst = 1;
    }
    var k: usize = 0;
    while (k < n) {
        const len: usize = @min(chunk, n - k);
        loadStored(in, form, n, k, buf[0..len]);
        switch (form.method) {
            .plain => {},
            .frame => for (buf[0..len]) |*v| {
                v.* = form.base +% v.* *% form.factor;
            },
            .delta => for (buf[0..len]) |*v| {
                prev = prev +% (form.base +% v.* *% form.factor);
                v.* = prev;
            },
        }
        storeValues(out, s.width, dst, buf[0..len]);
        dst += len;
        k += len;
    }
}

/// Stored values `first..first + out.len` of `n`, widened to u64.
fn loadStored(in: []const u8, form: Form, n: usize, first: usize, out: []u64) void {
    switch (form.stored_width) {
        0 => @memset(out, 0),
        inline 1, 2, 3, 4, 8 => |w| {
            if (form.shuffled) {
                @memset(out, 0);
                inline for (0..w) |b| {
                    const plane = in[b * n + first ..][0..out.len];
                    for (plane, out) |byte, *o| o.* |= @as(u64, byte) << (b * 8);
                }
            } else {
                const T = @Int(.unsigned, w * 8);
                for (out, first..) |*o, i| o.* = std.mem.readInt(T, in[i * w ..][0..w], .little);
            }
        },
        else => unreachable,
    }
}

/// Writes `values`, each truncated to `width` bytes, as payload values
/// `first..` of `out`.
fn storeValues(out: []u8, width: u8, first: usize, values: []const u64) void {
    switch (width) {
        inline 1, 2, 3, 4, 8 => |w| {
            const T = @Int(.unsigned, w * 8);
            for (values, first..) |v, i| std.mem.writeInt(T, out[i * w ..][0..w], @truncate(v), .little);
        },
        else => unreachable,
    }
}

/// Payload values widened to u64: sign-extended when `signed`, else zero.
fn loadValues(bytes: []const u8, width: u8, signed: bool, out: []u64) void {
    switch (width) {
        inline 1, 2, 3, 4, 8 => |w| {
            const U = @Int(.unsigned, w * 8);
            const I = @Int(.signed, w * 8);
            for (out, 0..) |*o, i| {
                const u = std.mem.readInt(U, bytes[i * w ..][0..w], .little);
                o.* = if (signed) @bitCast(@as(i64, @as(I, @bitCast(u)))) else u;
            }
        },
        else => unreachable,
    }
}

fn appendU32Assume(out: *std.ArrayList(u8), v: u32) void {
    var b4: [4]u8 = undefined;
    format.writeU32(&b4, v);
    out.appendSliceAssumeCapacity(&b4);
}

const Shape = enum { timestamps, constant, multiples, negative, ranks, wide_tail, random };

fn shapeValues(shape: Shape, values: []u64, random: std.Random) void {
    for (values, 0..) |*v, i| v.* = switch (shape) {
        .timestamps => 1_700_000_000_000 + (i + random.uintLessThan(u64, 3)) * 1000,
        .constant => 42,
        .multiples => 1_000_000 + random.uintLessThan(u64, 1000) * 7,
        .negative => @bitCast(random.intRangeAtMost(i64, -500, -300)),
        .ranks => i * 3 + random.uintLessThan(u64, 3),
        .wide_tail => if (i < trial_values) i else i * 1_000_003,
        .random => random.int(u64),
    };
}

/// Transforms `payload`, checks the stored form restores it exactly, and
/// returns whether anything was rewritten.
fn expectRoundTrip(payload: []const u8, streams: []const Stream) !bool {
    const allocator = std.testing.allocator;
    var compressor = try compression.Compressor.init();
    defer compressor.deinit();
    var directory: std.ArrayList(u8) = .empty;
    defer directory.deinit(allocator);
    var stored: std.ArrayList(u8) = .empty;
    defer stored.deinit(allocator);
    const rewritten = try transform(allocator, &compressor, payload, streams, &directory, &stored);
    if (!rewritten) {
        try std.testing.expectEqual(@as(usize, 0), directory.items.len);
        try std.testing.expectEqual(@as(usize, 0), stored.items.len);
        return false;
    }
    const out = try restoreCopy(directory.items, stored.items, payload.len);
    defer allocator.free(out);
    try std.testing.expectEqualSlices(u8, payload, out);
    return true;
}

/// `stored` restored into a fresh buffer of `len` bytes, as a reader does.
fn restoreCopy(directory_bytes: []const u8, stored: []const u8, len: usize) ![]u8 {
    const allocator = std.testing.allocator;
    const directory = try readDirectory(directory_bytes, len);
    if (stored.len != directory.stored_len) return format.Error.CorruptColumnBlockHeader;
    const out = try allocator.alloc(u8, len);
    errdefer allocator.free(out);
    @memcpy(out[len - stored.len ..], stored);
    try restore(allocator, directory, out);
    return out;
}

test "int_streams restores every shape and width exactly; keeps arrays a rewrite can't shrink" {
    const allocator = std.testing.allocator;
    var prng = std.Random.DefaultPrng.init(0x5eed);
    const count = 20_000;
    const head = 13;
    const tail = 7;
    const cases = .{
        .{ .shape = Shape.timestamps, .width = 8, .signed = true, .rewritten = @as(?bool, true) },
        .{ .shape = Shape.constant, .width = 4, .signed = false, .rewritten = @as(?bool, false) },
        .{ .shape = Shape.multiples, .width = 4, .signed = false, .rewritten = @as(?bool, null) },
        .{ .shape = Shape.negative, .width = 4, .signed = true, .rewritten = @as(?bool, null) },
        .{ .shape = Shape.negative, .width = 2, .signed = true, .rewritten = @as(?bool, null) },
        .{ .shape = Shape.ranks, .width = 3, .signed = false, .rewritten = @as(?bool, true) },
        .{ .shape = Shape.wide_tail, .width = 8, .signed = false, .rewritten = @as(?bool, null) },
        .{ .shape = Shape.random, .width = 8, .signed = false, .rewritten = @as(?bool, false) },
    };
    inline for (cases) |c| {
        const values = try allocator.alloc(u64, count);
        defer allocator.free(values);
        shapeValues(c.shape, values, prng.random());
        const payload = try allocator.alloc(u8, head + count * c.width + tail);
        defer allocator.free(payload);
        prng.random().bytes(payload[0..head]);
        storeValues(payload[head..], c.width, 0, values);
        prng.random().bytes(payload[payload.len - tail ..]);
        const rewritten = try expectRoundTrip(payload, &.{.{ .offset = head, .count = count, .width = c.width, .signed = c.signed }});
        if (c.rewritten) |want| try std.testing.expectEqual(want, rewritten);
    }
}

test "int_streams rewrites several arrays around the bytes between them" {
    const allocator = std.testing.allocator;
    var prng = std.Random.DefaultPrng.init(7);
    const count = 3000;
    const offsets_at = 5;
    const gap = 11;
    const lengths_at = offsets_at + (count + 1) * 4 + gap;
    const payload = try allocator.alloc(u8, lengths_at + count * 2 + 3);
    defer allocator.free(payload);
    prng.random().bytes(payload);
    var acc: u64 = 0;
    for (0..count + 1) |i| {
        storeValues(payload[offsets_at..], 4, i, &.{acc});
        acc += 20 + prng.random().uintLessThan(u64, 40);
    }
    for (0..count) |i| storeValues(payload[lengths_at..], 2, i, &.{prng.random().uintLessThan(u64, 9)});
    const streams = [_]Stream{
        .{ .offset = offsets_at, .count = count + 1, .width = 4 },
        .{ .offset = lengths_at, .count = count, .width = 2 },
    };
    try std.testing.expect(try expectRoundTrip(payload, &streams));
}

test "int_streams leaves arrays below min_values as they are" {
    var payload: [4 + 10 * 8]u8 = undefined;
    for (0..10) |i| storeValues(payload[4..], 8, i, &.{1_000_000 + i});
    @memset(payload[0..4], 0);
    try std.testing.expect(!try expectRoundTrip(&payload, &.{.{ .offset = 4, .count = 10, .width = 8 }}));
}

test "int_streams restore rejects truncated and malformed stored forms" {
    const allocator = std.testing.allocator;
    var compressor = try compression.Compressor.init();
    defer compressor.deinit();
    const count = 1000;
    var payload: [count * 8]u8 = undefined;
    // Not all one difference: a zero-width stored payload has no byte to cut.
    for (0..count) |i| storeValues(&payload, 8, i, &.{1_700_000_000 + i * 60 + i % 7});
    var directory: std.ArrayList(u8) = .empty;
    defer directory.deinit(allocator);
    var stored: std.ArrayList(u8) = .empty;
    defer stored.deinit(allocator);
    try std.testing.expect(try transform(allocator, &compressor, &payload, &.{.{ .offset = 0, .count = count, .width = 8 }}, &directory, &stored));
    const corrupt = format.Error.CorruptColumnBlockHeader;
    try std.testing.expectError(corrupt, restoreCopy(directory.items, stored.items[0 .. stored.items.len - 1], payload.len));
    try std.testing.expectError(corrupt, restoreCopy(directory.items[0 .. directory.items.len - 1], stored.items, payload.len));
    try std.testing.expectError(corrupt, restoreCopy(directory.items[0..2], stored.items, payload.len));
    try std.testing.expectError(corrupt, restoreCopy(directory.items, stored.items, 16));
    directory.items[4 + 8] = 5;
    try std.testing.expectError(corrupt, restoreCopy(directory.items, stored.items, payload.len));
}
