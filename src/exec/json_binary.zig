//! JSONB — thinDB's binary JSON representation (Phase 2). A JSON document is
//! parsed once into a compact, self-describing byte tree so that path
//! extraction is a pointer-walk instead of a re-parse of the text.
//!
//! A value is self-identifying: its first byte is a type tag in 0..=8, which
//! can never begin valid JSON *text* (which starts with whitespace or `{ [ "
//! - digit t f n`, all >= 0x09). Kernels use `looksBinary` to accept either
//! form, so the at-rest storage format can be flipped from text to JSONB
//! independently.
//!
//! Layout (all integers little-endian, offsets relative to the value's own
//! first byte so any sub-value slice is a valid standalone document):
//!
//!   null  : [0]
//!   false : [1]
//!   true  : [2]
//!   int   : [3][i64]
//!   double: [4][f64]
//!   string: [5][u32 len][len bytes]           (raw, unescaped UTF-8)
//!   array : [6][u32 byte_len][u32 count]
//!           [u32 elem_off * count][elements...]
//!   object: [7][u32 byte_len][u32 count]
//!           [Entry{u32 key_off,u32 key_len,u32 val_off} * count]
//!           [key bytes...][values...]          (entries sorted by key bytes)
//!   number: [8][NumberKind][u32 len][ASCII]  (exact: a DECIMAL, or an
//!           integer past i64 up to u64)
//!
//! Object keys are sorted (bytewise) at encode time, giving a canonical form
//! and O(log n) member lookup by binary search. Text prints the way MySQL
//! prints a JSON value: `, ` and `: ` separators, members shorter key first.

const std = @import("std");
const Allocator = std.mem.Allocator;

pub const Tag = enum(u8) {
    null = 0,
    false = 1,
    true = 2,
    int = 3,
    double = 4,
    string = 5,
    array = 6,
    object = 7,
    number = 8,
};

/// What an exact `number` is, which JSON_TYPE reports.
pub const NumberKind = enum(u8) {
    decimal = 0,
    unsigned = 1,
};

pub const Error = error{JsonInvalid} || Allocator.Error;

/// True if `bytes` is (the start of) a JSONB value rather than JSON text.
pub inline fn looksBinary(bytes: []const u8) bool {
    return bytes.len > 0 and bytes[0] <= @intFromEnum(Tag.number);
}

fn readU32(b: []const u8, at: usize) u32 {
    return std.mem.readInt(u32, b[at..][0..4], .little);
}

fn writeU32(list: *std.ArrayList(u8), aa: Allocator, v: u32) !void {
    var buf: [4]u8 = undefined;
    std.mem.writeInt(u32, &buf, v, .little);
    try list.appendSlice(aa, buf[0..]);
}

// ---------------------------------------------------------------------------
// JSONB writers
// ---------------------------------------------------------------------------

pub fn appendNull(aa: Allocator, out: *std.ArrayList(u8)) Allocator.Error!void {
    try out.append(aa, @intFromEnum(Tag.null));
}

pub fn appendBool(aa: Allocator, out: *std.ArrayList(u8), v: bool) Allocator.Error!void {
    try out.append(aa, @intFromEnum(if (v) Tag.true else Tag.false));
}

pub fn appendInt(aa: Allocator, out: *std.ArrayList(u8), v: i64) Allocator.Error!void {
    var buf: [9]u8 = undefined;
    buf[0] = @intFromEnum(Tag.int);
    std.mem.writeInt(i64, buf[1..9], v, .little);
    try out.appendSlice(aa, &buf);
}

pub fn appendDouble(aa: Allocator, out: *std.ArrayList(u8), v: f64) Allocator.Error!void {
    var buf: [9]u8 = undefined;
    buf[0] = @intFromEnum(Tag.double);
    std.mem.writeInt(u64, buf[1..9], @bitCast(v), .little);
    try out.appendSlice(aa, &buf);
}

pub fn appendString(aa: Allocator, out: *std.ArrayList(u8), s: []const u8) Allocator.Error!void {
    try out.append(aa, @intFromEnum(Tag.string));
    try writeU32(out, aa, @intCast(s.len));
    try out.appendSlice(aa, s);
}

/// An exact number from its canonical digits (`-12.50`, `18446744073709551615`).
pub fn appendNumber(aa: Allocator, out: *std.ArrayList(u8), kind: NumberKind, digits: []const u8) Allocator.Error!void {
    try out.appendSlice(aa, &.{ @intFromEnum(Tag.number), @intFromEnum(kind) });
    try writeU32(out, aa, @intCast(digits.len));
    try out.appendSlice(aa, digits);
}

/// An array of the JSONB values packed back to back in `elems`, value `i`
/// starting at byte `offsets[i]`.
pub fn appendArray(aa: Allocator, out: *std.ArrayList(u8), elems: []const u8, offsets: []const u32) Allocator.Error!void {
    const start = out.items.len;
    try out.append(aa, @intFromEnum(Tag.array));
    const bytelen_pos = out.items.len;
    try writeU32(out, aa, 0);
    const count: u32 = @intCast(offsets.len);
    try writeU32(out, aa, count);
    const elems_base: u32 = @intCast(out.items.len + count * 4 - start);
    for (offsets) |o| try writeU32(out, aa, elems_base + o);
    try out.appendSlice(aa, elems);
    std.mem.writeInt(u32, out.items[bytelen_pos..][0..4], @intCast(out.items.len - start), .little);
}

pub const Member = struct { key: []const u8, val: []const u8 };

fn memberKeyLess(_: void, a: Member, b: Member) bool {
    return std.mem.lessThan(u8, a.key, b.key);
}

/// An object of `members`, which it reorders in place. A repeated key keeps
/// its last value, as MySQL does.
pub fn appendObject(aa: Allocator, out: *std.ArrayList(u8), members: []Member) Allocator.Error!void {
    std.mem.sort(Member, members, {}, memberKeyLess);
    var kept: usize = 0;
    for (members) |m| {
        if (kept > 0 and std.mem.eql(u8, members[kept - 1].key, m.key)) {
            members[kept - 1] = m;
            continue;
        }
        members[kept] = m;
        kept += 1;
    }
    const unique = members[0..kept];
    const start = out.items.len;
    try out.append(aa, @intFromEnum(Tag.object));
    const bytelen_pos = out.items.len;
    try writeU32(out, aa, 0);
    try writeU32(out, aa, @intCast(unique.len));
    const entries_pos = out.items.len;
    try out.appendNTimes(aa, 0, unique.len * 12);
    for (unique, 0..) |m, i| {
        const entry = entries_pos + i * 12;
        std.mem.writeInt(u32, out.items[entry..][0..4], @intCast(out.items.len - start), .little);
        std.mem.writeInt(u32, out.items[entry + 4 ..][0..4], @intCast(m.key.len), .little);
        try out.appendSlice(aa, m.key);
    }
    for (unique, 0..) |m, i| {
        const entry = entries_pos + i * 12;
        std.mem.writeInt(u32, out.items[entry + 8 ..][0..4], @intCast(out.items.len - start), .little);
        try out.appendSlice(aa, m.val);
    }
    std.mem.writeInt(u32, out.items[bytelen_pos..][0..4], @intCast(out.items.len - start), .little);
}

// ---------------------------------------------------------------------------
// Text → JSONB encoder
// ---------------------------------------------------------------------------

const Parser = struct {
    src: []const u8,
    pos: usize = 0,
    aa: Allocator,

    fn ws(self: *Parser) void {
        while (self.pos < self.src.len) : (self.pos += 1) {
            switch (self.src[self.pos]) {
                ' ', '\t', '\n', '\r' => {},
                else => break,
            }
        }
    }

    fn peek(self: *Parser) ?u8 {
        return if (self.pos < self.src.len) self.src[self.pos] else null;
    }

    fn value(self: *Parser, out: *std.ArrayList(u8)) Error!void {
        self.ws();
        const c = self.peek() orelse return Error.JsonInvalid;
        switch (c) {
            '{' => try self.object(out),
            '[' => try self.array(out),
            '"' => {
                try out.append(self.aa, @intFromEnum(Tag.string));
                try self.stringBody(out);
            },
            't' => {
                try self.literal("true");
                try out.append(self.aa, @intFromEnum(Tag.true));
            },
            'f' => {
                try self.literal("false");
                try out.append(self.aa, @intFromEnum(Tag.false));
            },
            'n' => {
                try self.literal("null");
                try out.append(self.aa, @intFromEnum(Tag.null));
            },
            '-', '0'...'9' => try self.number(out),
            else => return Error.JsonInvalid,
        }
    }

    fn literal(self: *Parser, comptime lit: []const u8) Error!void {
        if (self.pos + lit.len > self.src.len) return Error.JsonInvalid;
        if (!std.mem.eql(u8, self.src[self.pos .. self.pos + lit.len], lit)) return Error.JsonInvalid;
        self.pos += lit.len;
    }

    /// Decode a JSON string starting at the opening quote and append its raw
    /// bytes as `[u32 len][bytes]` (tag already written by the caller).
    fn stringBody(self: *Parser, out: *std.ArrayList(u8)) Error!void {
        std.debug.assert(self.src[self.pos] == '"');
        self.pos += 1;
        const len_pos = out.items.len;
        try writeU32(out, self.aa, 0); // placeholder
        const start = out.items.len;
        while (self.pos < self.src.len) {
            const c = self.src[self.pos];
            if (c == '"') {
                self.pos += 1;
                const n: u32 = @intCast(out.items.len - start);
                std.mem.writeInt(u32, out.items[len_pos..][0..4], n, .little);
                return;
            }
            if (c == '\\') {
                self.pos += 1;
                if (self.pos >= self.src.len) return Error.JsonInvalid;
                const e = self.src[self.pos];
                self.pos += 1;
                switch (e) {
                    '"' => try out.append(self.aa, '"'),
                    '\\' => try out.append(self.aa, '\\'),
                    '/' => try out.append(self.aa, '/'),
                    'b' => try out.append(self.aa, 0x08),
                    'f' => try out.append(self.aa, 0x0c),
                    'n' => try out.append(self.aa, '\n'),
                    'r' => try out.append(self.aa, '\r'),
                    't' => try out.append(self.aa, '\t'),
                    'u' => {
                        if (self.pos + 4 > self.src.len) return Error.JsonInvalid;
                        const cp = std.fmt.parseInt(u21, self.src[self.pos .. self.pos + 4], 16) catch
                            return Error.JsonInvalid;
                        self.pos += 4;
                        var buf: [4]u8 = undefined;
                        const nn = std.unicode.utf8Encode(cp, &buf) catch return Error.JsonInvalid;
                        try out.appendSlice(self.aa, buf[0..nn]);
                    },
                    else => return Error.JsonInvalid,
                }
                continue;
            }
            try out.append(self.aa, c);
            self.pos += 1;
        }
        return Error.JsonInvalid;
    }

    fn number(self: *Parser, out: *std.ArrayList(u8)) Error!void {
        const start = self.pos;
        var is_float = false;
        if (self.peek() == @as(u8, '-')) self.pos += 1;
        while (self.pos < self.src.len and self.src[self.pos] >= '0' and self.src[self.pos] <= '9') self.pos += 1;
        if (self.pos < self.src.len and self.src[self.pos] == '.') {
            is_float = true;
            self.pos += 1;
            while (self.pos < self.src.len and self.src[self.pos] >= '0' and self.src[self.pos] <= '9') self.pos += 1;
        }
        if (self.pos < self.src.len and (self.src[self.pos] == 'e' or self.src[self.pos] == 'E')) {
            is_float = true;
            self.pos += 1;
            if (self.pos < self.src.len and (self.src[self.pos] == '+' or self.src[self.pos] == '-')) self.pos += 1;
            while (self.pos < self.src.len and self.src[self.pos] >= '0' and self.src[self.pos] <= '9') self.pos += 1;
        }
        const text = self.src[start..self.pos];
        if (text.len == 0) return Error.JsonInvalid;
        if (!is_float) {
            if (std.fmt.parseInt(i64, text, 10)) |iv| return appendInt(self.aa, out, iv) else |_| {}
            if (std.fmt.parseInt(u64, text, 10)) |_| return appendNumber(self.aa, out, .unsigned, text) else |_| {}
        }
        const fv = std.fmt.parseFloat(f64, text) catch return Error.JsonInvalid;
        if (!std.math.isFinite(fv)) return Error.JsonInvalid;
        try appendDouble(self.aa, out, fv);
    }

    fn array(self: *Parser, out: *std.ArrayList(u8)) Error!void {
        self.pos += 1; // '['
        var elems: std.ArrayList(u8) = .empty;
        defer elems.deinit(self.aa);
        var offs: std.ArrayList(u32) = .empty;
        defer offs.deinit(self.aa);
        self.ws();
        if (self.peek() == @as(u8, ']')) {
            self.pos += 1;
        } else {
            while (true) {
                try offs.append(self.aa, @intCast(elems.items.len));
                try self.value(&elems);
                self.ws();
                const c = self.peek() orelse return Error.JsonInvalid;
                if (c == ',') {
                    self.pos += 1;
                    continue;
                }
                if (c == ']') {
                    self.pos += 1;
                    break;
                }
                return Error.JsonInvalid;
            }
        }
        try appendArray(self.aa, out, elems.items, offs.items);
    }

    fn object(self: *Parser, out: *std.ArrayList(u8)) Error!void {
        self.pos += 1; // '{'
        const KV = struct { key: []u8, val: []u8 };
        var kvs: std.ArrayList(KV) = .empty;
        defer {
            for (kvs.items) |kv| {
                self.aa.free(kv.key);
                self.aa.free(kv.val);
            }
            kvs.deinit(self.aa);
        }
        self.ws();
        if (self.peek() == @as(u8, '}')) {
            self.pos += 1;
        } else {
            while (true) {
                self.ws();
                if (self.peek() != @as(u8, '"')) return Error.JsonInvalid;
                var key_buf: std.ArrayList(u8) = .empty;
                errdefer key_buf.deinit(self.aa);
                // Decode the key into raw bytes (reuse stringBody, then strip
                // the len prefix it writes).
                try key_buf.append(self.aa, 0); // dummy tag slot for stringBody math
                _ = key_buf.pop();
                try self.decodeStringInto(&key_buf);
                self.ws();
                if (self.peek() != @as(u8, ':')) return Error.JsonInvalid;
                self.pos += 1;
                var val_buf: std.ArrayList(u8) = .empty;
                errdefer val_buf.deinit(self.aa);
                try self.value(&val_buf);
                try kvs.append(self.aa, .{
                    .key = try key_buf.toOwnedSlice(self.aa),
                    .val = try val_buf.toOwnedSlice(self.aa),
                });
                self.ws();
                const c = self.peek() orelse return Error.JsonInvalid;
                if (c == ',') {
                    self.pos += 1;
                    continue;
                }
                if (c == '}') {
                    self.pos += 1;
                    break;
                }
                return Error.JsonInvalid;
            }
        }
        const members = try self.aa.alloc(Member, kvs.items.len);
        defer self.aa.free(members);
        for (kvs.items, members) |kv, *m| m.* = .{ .key = kv.key, .val = kv.val };
        try appendObject(self.aa, out, members);
    }

    /// Decode a JSON string (at the opening quote) into `out` as raw bytes,
    /// no length prefix.
    fn decodeStringInto(self: *Parser, out: *std.ArrayList(u8)) Error!void {
        std.debug.assert(self.src[self.pos] == '"');
        self.pos += 1;
        while (self.pos < self.src.len) {
            const c = self.src[self.pos];
            if (c == '"') {
                self.pos += 1;
                return;
            }
            if (c == '\\') {
                self.pos += 1;
                if (self.pos >= self.src.len) return Error.JsonInvalid;
                const e = self.src[self.pos];
                self.pos += 1;
                switch (e) {
                    '"' => try out.append(self.aa, '"'),
                    '\\' => try out.append(self.aa, '\\'),
                    '/' => try out.append(self.aa, '/'),
                    'b' => try out.append(self.aa, 0x08),
                    'f' => try out.append(self.aa, 0x0c),
                    'n' => try out.append(self.aa, '\n'),
                    'r' => try out.append(self.aa, '\r'),
                    't' => try out.append(self.aa, '\t'),
                    'u' => {
                        if (self.pos + 4 > self.src.len) return Error.JsonInvalid;
                        const cp = std.fmt.parseInt(u21, self.src[self.pos .. self.pos + 4], 16) catch
                            return Error.JsonInvalid;
                        self.pos += 4;
                        var buf: [4]u8 = undefined;
                        const nn = std.unicode.utf8Encode(cp, &buf) catch return Error.JsonInvalid;
                        try out.appendSlice(self.aa, buf[0..nn]);
                    },
                    else => return Error.JsonInvalid,
                }
                continue;
            }
            try out.append(self.aa, c);
            self.pos += 1;
        }
        return Error.JsonInvalid;
    }
};

/// Parse JSON text into a freshly-allocated JSONB byte slice. Caller owns it.
pub fn encodeFromText(aa: Allocator, text: []const u8) Error![]u8 {
    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(aa);
    var p = Parser{ .src = text, .aa = aa };
    try p.value(&out);
    p.ws();
    if (p.pos != text.len) return Error.JsonInvalid;
    return out.toOwnedSlice(aa);
}

/// Normalize a stored JSON value to JSONB. If it already is JSONB, return a
/// borrowed slice (`owned = false`); otherwise encode the text (`owned =
/// true`, caller frees).
pub const Normalized = struct { bytes: []const u8, owned: bool };

pub fn normalize(aa: Allocator, bytes: []const u8) Error!Normalized {
    if (looksBinary(bytes)) return .{ .bytes = bytes, .owned = false };
    return .{ .bytes = try encodeFromText(aa, bytes), .owned = true };
}

// ---------------------------------------------------------------------------
// Navigation over JSONB
// ---------------------------------------------------------------------------

pub fn tagOf(v: []const u8) Tag {
    return @enumFromInt(v[0]);
}

/// Byte length of the value starting at `v[0]`.
pub fn valueLen(v: []const u8) usize {
    return switch (tagOf(v)) {
        .null, .false, .true => 1,
        .int, .double => 9,
        .string => 1 + 4 + readU32(v, 1),
        .array, .object => readU32(v, 1),
        .number => 6 + readU32(v, 2),
    };
}

/// Byte length of the value starting at `v[0]`, or null when `v` is too short
/// to hold it or its tag isn't one.
pub fn checkedValueLen(v: []const u8) ?usize {
    if (v.len == 0 or v[0] > @intFromEnum(Tag.number)) return null;
    const header: usize = switch (tagOf(v)) {
        .null, .false, .true => 1,
        .int, .double => 9,
        .string, .array, .object => 5,
        .number => 6,
    };
    if (v.len < header) return null;
    const len = valueLen(v);
    return if (len >= header and len <= v.len) len else null;
}

/// True when `v` is exactly one JSONB value whose every offset and length
/// stays inside it: bytes from outside the engine are safe to walk.
pub fn wellFormed(v: []const u8) bool {
    const len = checkedValueLen(v) orelse return false;
    if (len != v.len) return false;
    switch (tagOf(v)) {
        .number => return v[1] <= @intFromEnum(NumberKind.unsigned),
        .array, .object => {
            if (len < 9) return false;
            const count = readU32(v, 5);
            const entry_size: usize = if (tagOf(v) == .array) 4 else 12;
            if (count > (len - 9) / entry_size) return false;
            const body_start = 9 + count * entry_size;
            var i: u32 = 0;
            while (i < count) : (i += 1) {
                const e = 9 + i * entry_size;
                if (tagOf(v) == .object) {
                    const koff = readU32(v, e);
                    if (koff > len or readU32(v, e + 4) > len - koff) return false;
                }
                // Past the entry table, so each child is shorter than `v`.
                const off = readU32(v, e + entry_size - 4);
                if (off < body_start or off >= len) return false;
                const child_len = checkedValueLen(v[off..]) orelse return false;
                if (!wellFormed(v[off..][0..child_len])) return false;
            }
            return true;
        },
        else => return true,
    }
}

fn numberKind(v: []const u8) NumberKind {
    return @enumFromInt(v[1]);
}

fn numberDigits(v: []const u8) []const u8 {
    return v[6..][0..readU32(v, 2)];
}

fn memberKey(obj: []const u8, i: u32) []const u8 {
    const e = 9 + i * 12;
    return obj[readU32(obj, e)..][0..readU32(obj, e + 4)];
}

fn memberValue(obj: []const u8, i: u32) []const u8 {
    const voff = readU32(obj, 9 + i * 12 + 8);
    return obj[voff..][0..valueLen(obj[voff..])];
}

fn printKeyLess(obj: []const u8, a: u32, b: u32) bool {
    const ka = memberKey(obj, a);
    const kb = memberKey(obj, b);
    if (ka.len != kb.len) return ka.len < kb.len;
    return std.mem.lessThan(u8, ka, kb);
}

/// An object's member indexes in the order MySQL prints them and lists them
/// in JSON_KEYS: shorter keys first, equal lengths bytewise. Caller frees.
fn printOrder(aa: Allocator, obj: []const u8) Allocator.Error![]u32 {
    const order = try aa.alloc(u32, readU32(obj, 5));
    for (order, 0..) |*o, i| o.* = @intCast(i);
    std.mem.sort(u32, order, obj, printKeyLess);
    return order;
}

/// Member lookup on an object value by key. Returns the child value slice.
pub fn member(obj: []const u8, key: []const u8) ?[]const u8 {
    if (tagOf(obj) != .object) return null;
    const count = readU32(obj, 5);
    const entries = 9;
    // Binary search: keys stored sorted bytewise.
    var lo: u32 = 0;
    var hi: u32 = count;
    while (lo < hi) {
        const mid = lo + (hi - lo) / 2;
        const e = entries + mid * 12;
        const koff = readU32(obj, e);
        const klen = readU32(obj, e + 4);
        const k = obj[koff .. koff + klen];
        switch (std.mem.order(u8, k, key)) {
            .lt => lo = mid + 1,
            .gt => hi = mid,
            .eq => {
                const voff = readU32(obj, e + 8);
                return obj[voff..][0..valueLen(obj[voff..])];
            },
        }
    }
    return null;
}

/// Element lookup on an array value by index.
pub fn element(arr: []const u8, idx: usize) ?[]const u8 {
    if (tagOf(arr) != .array) return null;
    const count = readU32(arr, 5);
    if (idx >= count) return null;
    const table = 9;
    const off = readU32(arr, table + @as(u32, @intCast(idx)) * 4);
    return arr[off..][0..valueLen(arr[off..])];
}

pub const Step = union(enum) { member: []const u8, index: usize };

pub fn navigate(root: []const u8, steps: []const Step) ?[]const u8 {
    var cur = root;
    for (steps) |s| {
        cur = switch (s) {
            .member => |m| member(cur, m) orelse return null,
            .index => |i| element(cur, i) orelse return null,
        };
    }
    return cur;
}

pub fn arrayLen(v: []const u8) ?usize {
    return switch (tagOf(v)) {
        .array, .object => readU32(v, 5),
        else => null,
    };
}

pub fn typeName(v: []const u8) []const u8 {
    return switch (tagOf(v)) {
        .null => "NULL",
        .false, .true => "BOOLEAN",
        .int => "INTEGER",
        .double => "DOUBLE",
        .string => "STRING",
        .array => "ARRAY",
        .object => "OBJECT",
        .number => switch (numberKind(v)) {
            .decimal => "DECIMAL",
            .unsigned => "UNSIGNED INTEGER",
        },
    };
}

/// True if `bytes` is a valid JSON document (text) or already JSONB.
pub fn isValid(aa: Allocator, bytes: []const u8) bool {
    if (looksBinary(bytes)) return true;
    const b = encodeFromText(aa, bytes) catch return false;
    aa.free(b);
    return true;
}

// ---------------------------------------------------------------------------
// Containment (JSON_CONTAINS) over JSONB
// ---------------------------------------------------------------------------

/// MySQL JSON_CONTAINS: is `cand` structurally contained in `target`?
pub fn contains(target: []const u8, cand: []const u8) bool {
    switch (tagOf(cand)) {
        .object => {
            if (tagOf(target) != .object) return false;
            const count = readU32(cand, 5);
            var i: u32 = 0;
            while (i < count) : (i += 1) {
                const e = 9 + i * 12;
                const koff = readU32(cand, e);
                const klen = readU32(cand, e + 4);
                const voff = readU32(cand, e + 8);
                const cv = cand[voff..][0..valueLen(cand[voff..])];
                const tv = member(target, cand[koff .. koff + klen]) orelse return false;
                if (!contains(tv, cv)) return false;
            }
            return true;
        },
        .array => {
            if (tagOf(target) != .array) return false;
            const count = readU32(cand, 5);
            var i: u32 = 0;
            while (i < count) : (i += 1) {
                const off = readU32(cand, 9 + i * 4);
                const ce = cand[off..][0..valueLen(cand[off..])];
                if (!arrayContains(target, ce)) return false;
            }
            return true;
        },
        else => {
            if (tagOf(target) == .array) return arrayContains(target, cand);
            return std.mem.eql(u8, target, cand);
        },
    }
}

fn arrayContains(target_arr: []const u8, cand: []const u8) bool {
    if (tagOf(target_arr) != .array) return false;
    const count = readU32(target_arr, 5);
    var i: u32 = 0;
    while (i < count) : (i += 1) {
        const off = readU32(target_arr, 9 + i * 4);
        const x = target_arr[off..][0..valueLen(target_arr[off..])];
        if (contains(x, cand)) return true;
    }
    return false;
}

/// Build a freshly-allocated JSONB array of an object's keys (each a JSON
/// string) in MySQL's order, or null if `obj` is not an object. Caller owns
/// the slice.
pub fn keysArray(aa: Allocator, obj: []const u8) Allocator.Error!?[]u8 {
    if (tagOf(obj) != .object) return null;
    const order = try printOrder(aa, obj);
    defer aa.free(order);
    var elems: std.ArrayList(u8) = .empty;
    defer elems.deinit(aa);
    const offsets = try aa.alloc(u32, order.len);
    defer aa.free(offsets);
    for (order, offsets) |i, *off| {
        off.* = @intCast(elems.items.len);
        try appendString(aa, &elems, memberKey(obj, i));
    }
    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(aa);
    try appendArray(aa, &out, elems.items, offsets);
    return try out.toOwnedSlice(aa);
}

// ---------------------------------------------------------------------------
// JSONB → text serializer
// ---------------------------------------------------------------------------

fn appendEscaped(aa: Allocator, out: *std.ArrayList(u8), s: []const u8) !void {
    try out.append(aa, '"');
    for (s) |c| {
        switch (c) {
            '"' => try out.appendSlice(aa, "\\\""),
            '\\' => try out.appendSlice(aa, "\\\\"),
            '\n' => try out.appendSlice(aa, "\\n"),
            '\r' => try out.appendSlice(aa, "\\r"),
            '\t' => try out.appendSlice(aa, "\\t"),
            0x08 => try out.appendSlice(aa, "\\b"),
            0x0c => try out.appendSlice(aa, "\\f"),
            else => {
                if (c < 0x20) {
                    var buf: [6]u8 = undefined;
                    _ = std.fmt.bufPrint(&buf, "\\u{x:0>4}", .{c}) catch unreachable;
                    try out.appendSlice(aa, buf[0..6]);
                } else {
                    try out.append(aa, c);
                }
            },
        }
    }
    try out.append(aa, '"');
}

/// Serialize a JSONB value to JSON text into `out`, spelled as MySQL prints
/// it.
pub fn toText(aa: Allocator, out: *std.ArrayList(u8), v: []const u8) Allocator.Error!void {
    switch (tagOf(v)) {
        .null => try out.appendSlice(aa, "null"),
        .false => try out.appendSlice(aa, "false"),
        .true => try out.appendSlice(aa, "true"),
        .int => {
            const iv = std.mem.readInt(i64, v[1..][0..8], .little);
            var buf: [24]u8 = undefined;
            try out.appendSlice(aa, std.fmt.bufPrint(&buf, "{d}", .{iv}) catch unreachable);
        },
        .double => try appendDoubleText(aa, out, @bitCast(std.mem.readInt(u64, v[1..][0..8], .little))),
        .number => try out.appendSlice(aa, numberDigits(v)),
        .string => {
            const len = readU32(v, 1);
            try appendEscaped(aa, out, v[5 .. 5 + len]);
        },
        .array => {
            try out.append(aa, '[');
            const count = readU32(v, 5);
            const table = 9;
            var i: u32 = 0;
            while (i < count) : (i += 1) {
                if (i != 0) try out.appendSlice(aa, ", ");
                const off = readU32(v, table + i * 4);
                try toText(aa, out, v[off..][0..valueLen(v[off..])]);
            }
            try out.append(aa, ']');
        },
        .object => {
            const order = try printOrder(aa, v);
            defer aa.free(order);
            try out.append(aa, '{');
            for (order, 0..) |member_index, n| {
                if (n != 0) try out.appendSlice(aa, ", ");
                try appendEscaped(aa, out, memberKey(v, member_index));
                try out.appendSlice(aa, ": ");
                try toText(aa, out, memberValue(v, member_index));
            }
            try out.append(aa, '}');
        },
    }
}

/// A double as MySQL prints one in JSON: its shortest round-trip digits,
/// positional when the decimal exponent is in -14..15 (or the digits reach
/// past the point), otherwise `d.ddde[-]x`; a positional integer gains `.0`.
fn appendDoubleText(aa: Allocator, out: *std.ArrayList(u8), x: f64) Allocator.Error!void {
    if (x == 0) return out.appendSlice(aa, if (std.math.signbit(x)) "-0.0" else "0.0");
    const float_fmt = std.fmt.float;
    const d = float_fmt.binaryToDecimal(u64, @bitCast(x), std.math.floatMantissaBits(f64), std.math.floatExponentBits(f64), false, &float_fmt.Backend64_TablesFull);
    var digit_buf: [24]u8 = undefined;
    const all_digits = std.fmt.bufPrint(&digit_buf, "{d}", .{d.mantissa}) catch unreachable;
    const point: i32 = @as(i32, @intCast(all_digits.len)) + d.exponent;
    const digits = std.mem.trimEnd(u8, all_digits, "0");
    const len: i32 = @intCast(digits.len);
    if (d.sign) try out.append(aa, '-');
    if (point >= -14 and (point <= 15 or len > point)) {
        if (point <= 0) {
            try out.appendSlice(aa, "0.");
            try out.appendNTimes(aa, '0', @intCast(-point));
            try out.appendSlice(aa, digits);
        } else if (point >= len) {
            try out.appendSlice(aa, digits);
            try out.appendNTimes(aa, '0', @intCast(point - len));
            try out.appendSlice(aa, ".0");
        } else {
            const whole: usize = @intCast(point);
            try out.appendSlice(aa, digits[0..whole]);
            try out.append(aa, '.');
            try out.appendSlice(aa, digits[whole..]);
        }
        return;
    }
    try out.append(aa, digits[0]);
    if (digits.len > 1) {
        try out.append(aa, '.');
        try out.appendSlice(aa, digits[1..]);
    }
    var exp_buf: [8]u8 = undefined;
    try out.appendSlice(aa, std.fmt.bufPrint(&exp_buf, "e{d}", .{point - 1}) catch unreachable);
}

/// Append the unquoted scalar form (JSON_UNQUOTE) of a JSONB value: a string
/// yields its raw bytes; everything else yields its canonical text.
pub fn appendUnquoted(aa: Allocator, out: *std.ArrayList(u8), v: []const u8) Allocator.Error!void {
    if (tagOf(v) == .string) {
        const len = readU32(v, 1);
        try out.appendSlice(aa, v[5 .. 5 + len]);
        return;
    }
    try toText(aa, out, v);
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

fn roundtrip(aa: Allocator, text: []const u8, expect: []const u8) !void {
    const b = try encodeFromText(aa, text);
    defer aa.free(b);
    try std.testing.expect(looksBinary(b));
    var out: std.ArrayList(u8) = .empty;
    defer out.deinit(aa);
    try toText(aa, &out, b);
    try std.testing.expectEqualStrings(expect, out.items);
}

test "encode/serialize round-trip prints as MySQL does" {
    const aa = std.testing.allocator;
    const cases = .{
        .{ "  42 ", "42" },
        .{ "-7", "-7" },
        .{ "3.5", "3.5" },
        .{ "true", "true" },
        .{ "null", "null" },
        .{ "\"a\\nb\"", "\"a\\nb\"" },
        .{ "[1,2,3]", "[1, 2, 3]" },
        .{ "[]", "[]" },
        .{ "{}", "{}" },
        .{ "{\"b\":1,\"a\":2}", "{\"a\": 2, \"b\": 1}" },
        .{ "{\"aa\":1,\"b\":2,\"c\":3,\"a\":4}", "{\"a\": 4, \"b\": 2, \"c\": 3, \"aa\": 1}" },
        .{ "{\"z\": [1, {\"y\": 2}], \"a\": \"x\"}", "{\"a\": \"x\", \"z\": [1, {\"y\": 2}]}" },
        .{ "{\"a\":1,\"a\":2,\"b\":3,\"a\":5}", "{\"a\": 5, \"b\": 3}" },
        .{ "18446744073709551615", "18446744073709551615" },
        .{ "-9223372036854775808", "-9223372036854775808" },
        .{ "18446744073709551616", "1.8446744073709552e19" },
    };
    inline for (cases) |c| try roundtrip(aa, c[0], c[1]);
}

test "doubles print as MySQL prints them in JSON" {
    const aa = std.testing.allocator;
    const cases = .{
        .{ 1.5e-15, "0.0000000000000015" },
        .{ 1.5e-16, "1.5e-16" },
        .{ 1e14, "100000000000000.0" },
        .{ 1e15, "1e15" },
        .{ 100.0, "100.0" },
        .{ 0.1, "0.1" },
        .{ -0.0, "-0.0" },
        .{ 0.0, "0.0" },
        .{ -1.5, "-1.5" },
        .{ -1e20, "-1e20" },
        .{ 123456789.0, "123456789.0" },
        .{ 1234567890123456789.0, "1.2345678901234568e18" },
        .{ 12345678901234567.0, "1.2345678901234568e16" },
        .{ 1.7976931348623157e308, "1.7976931348623157e308" },
        .{ 5e-324, "5e-324" },
        .{ 1e-7, "0.0000001" },
    };
    var out: std.ArrayList(u8) = .empty;
    defer out.deinit(aa);
    inline for (cases) |c| {
        out.clearRetainingCapacity();
        try appendDoubleText(aa, &out, c[0]);
        try std.testing.expectEqualStrings(c[1], out.items);
    }
}

test "exact numbers keep their digits and report their JSON type" {
    const aa = std.testing.allocator;
    var doc: std.ArrayList(u8) = .empty;
    defer doc.deinit(aa);
    try appendNumber(aa, &doc, .decimal, "1.50");
    try std.testing.expect(looksBinary(doc.items));
    try std.testing.expectEqual(doc.items.len, valueLen(doc.items));
    try std.testing.expectEqualStrings("DECIMAL", typeName(doc.items));
    var text: std.ArrayList(u8) = .empty;
    defer text.deinit(aa);
    try toText(aa, &text, doc.items);
    try std.testing.expectEqualStrings("1.50", text.items);

    const unsigned = try encodeFromText(aa, "18446744073709551615");
    defer aa.free(unsigned);
    try std.testing.expectEqualStrings("UNSIGNED INTEGER", typeName(unsigned));
    try std.testing.expectError(Error.JsonInvalid, encodeFromText(aa, "1e999"));
}

test "JSON_KEYS lists keys shorter first" {
    const aa = std.testing.allocator;
    const obj = try encodeFromText(aa, "{\"bb\":1,\"a\":2,\"ccc\":3,\"b\":4}");
    defer aa.free(obj);
    const keys = (try keysArray(aa, obj)).?;
    defer aa.free(keys);
    var text: std.ArrayList(u8) = .empty;
    defer text.deinit(aa);
    try toText(aa, &text, keys);
    try std.testing.expectEqualStrings("[\"a\", \"b\", \"bb\", \"ccc\"]", text.items);
}

test "navigate via binary offsets" {
    const aa = std.testing.allocator;
    const b = try encodeFromText(aa, "{\"a\": {\"b\": [10, 20, 30]}, \"n\": \"jr\"}");
    defer aa.free(b);

    const p1 = [_]Step{ .{ .member = "a" }, .{ .member = "b" }, .{ .index = 1 } };
    const v1 = navigate(b, &p1).?;
    try std.testing.expectEqual(Tag.int, tagOf(v1));
    var out: std.ArrayList(u8) = .empty;
    defer out.deinit(aa);
    try toText(aa, &out, v1);
    try std.testing.expectEqualStrings("20", out.items);

    const p2 = [_]Step{.{ .member = "n" }};
    out.clearRetainingCapacity();
    try appendUnquoted(aa, &out, navigate(b, &p2).?);
    try std.testing.expectEqualStrings("jr", out.items);

    const p3 = [_]Step{.{ .member = "missing" }};
    try std.testing.expect(navigate(b, &p3) == null);
}

test "invalid text is rejected" {
    const aa = std.testing.allocator;
    try std.testing.expectError(Error.JsonInvalid, encodeFromText(aa, "{\"a\":}"));
    try std.testing.expectError(Error.JsonInvalid, encodeFromText(aa, "[1,2"));
    try std.testing.expectError(Error.JsonInvalid, encodeFromText(aa, "nul"));
    try std.testing.expectError(Error.JsonInvalid, encodeFromText(aa, "1 2"));
}

test "wellFormed accepts encoded values and rejects corrupt offsets" {
    const aa = std.testing.allocator;
    const texts = .{ "null", "-3", "2.5", "\"x\"", "[]", "[1, [2, {\"a\": null}]]", "{\"k\": [1], \"kk\": \"v\"}", "18446744073709551615" };
    inline for (texts) |t| {
        const b = try encodeFromText(aa, t);
        defer aa.free(b);
        try std.testing.expect(wellFormed(b));
        try std.testing.expectEqual(b.len, checkedValueLen(b).?);
        try std.testing.expect(!wellFormed(b[0 .. b.len - 1]));
    }

    const arr = try encodeFromText(aa, "[1, 2]");
    defer aa.free(arr);
    const bad = try aa.dupe(u8, arr);
    defer aa.free(bad);
    std.mem.writeInt(u32, bad[9..13], 0, .little);
    try std.testing.expect(!wellFormed(bad));
    @memcpy(bad, arr);
    std.mem.writeInt(u32, bad[9..13], @intCast(bad.len), .little);
    try std.testing.expect(!wellFormed(bad));
    @memcpy(bad, arr);
    std.mem.writeInt(u32, bad[5..9], 1000, .little);
    try std.testing.expect(!wellFormed(bad));
    try std.testing.expect(checkedValueLen(&.{9}) == null);
    try std.testing.expect(checkedValueLen("") == null);
}

test "looksBinary discriminates text vs binary" {
    try std.testing.expect(!looksBinary("{\"a\":1}"));
    try std.testing.expect(!looksBinary("42"));
    try std.testing.expect(!looksBinary("\"x\""));
    try std.testing.expect(!looksBinary("-5"));
    const aa = std.testing.allocator;
    const b = try encodeFromText(aa, "{}");
    defer aa.free(b);
    try std.testing.expect(looksBinary(b));
}
