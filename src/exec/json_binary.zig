//! JSONB — thinDB's binary JSON representation (Phase 2). A JSON document is
//! parsed once into a compact, self-describing byte tree so that path
//! extraction is a pointer-walk instead of a re-parse of the text.
//!
//! A value is self-identifying: its first byte is a type tag in 0..=7, which
//! can never begin valid JSON *text* (which starts with `{ [ " - digit t f
//! n`, all >= 0x22). Kernels use `looksBinary` to accept either form, so the
//! at-rest storage format can be flipped from text to JSONB independently.
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
//!
//! Object keys are sorted (bytewise) at encode time, giving a canonical form
//! and O(log n) member lookup by binary search.

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
};

pub const Error = error{JsonInvalid} || Allocator.Error;

/// True if `bytes` is (the start of) a JSONB value rather than JSON text.
pub inline fn looksBinary(bytes: []const u8) bool {
    return bytes.len > 0 and bytes[0] <= @intFromEnum(Tag.object);
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
            if (std.fmt.parseInt(i64, text, 10)) |iv| {
                try out.append(self.aa, @intFromEnum(Tag.int));
                var buf: [8]u8 = undefined;
                std.mem.writeInt(i64, &buf, iv, .little);
                try out.appendSlice(self.aa, buf[0..]);
                return;
            } else |_| {}
        }
        const fv = std.fmt.parseFloat(f64, text) catch return Error.JsonInvalid;
        try out.append(self.aa, @intFromEnum(Tag.double));
        var buf: [8]u8 = undefined;
        std.mem.writeInt(u64, &buf, @bitCast(fv), .little);
        try out.appendSlice(self.aa, buf[0..]);
    }

    fn array(self: *Parser, out: *std.ArrayList(u8)) Error!void {
        self.pos += 1; // '['
        const start = out.items.len;
        try out.append(self.aa, @intFromEnum(Tag.array));
        const bytelen_pos = out.items.len;
        try writeU32(out, self.aa, 0);
        // Encode elements into a scratch list, tracking their relative offsets.
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
        const count: u32 = @intCast(offs.items.len);
        try writeU32(out, self.aa, count);
        const table_bytes = count * 4;
        const elems_base: u32 = @intCast((out.items.len + table_bytes) - start);
        for (offs.items) |o| try writeU32(out, self.aa, elems_base + o);
        try out.appendSlice(self.aa, elems.items);
        const total: u32 = @intCast(out.items.len - start);
        std.mem.writeInt(u32, out.items[bytelen_pos..][0..4], total, .little);
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
        // Canonicalize: sort by key bytes; last write wins on duplicates.
        std.mem.sort(KV, kvs.items, {}, struct {
            fn lt(_: void, a: KV, b: KV) bool {
                return std.mem.lessThan(u8, a.key, b.key);
            }
        }.lt);

        const start = out.items.len;
        try out.append(self.aa, @intFromEnum(Tag.object));
        const bytelen_pos = out.items.len;
        try writeU32(out, self.aa, 0);
        const count: u32 = @intCast(kvs.items.len);
        try writeU32(out, self.aa, count);
        const entries_pos = out.items.len;
        for (0..count) |_| {
            try writeU32(out, self.aa, 0); // key_off
            try writeU32(out, self.aa, 0); // key_len
            try writeU32(out, self.aa, 0); // val_off
        }
        // Key bytes region, then value region.
        var key_offs: [*]u32 = undefined;
        _ = &key_offs;
        for (kvs.items, 0..) |kv, i| {
            const koff: u32 = @intCast(out.items.len - start);
            try out.appendSlice(self.aa, kv.key);
            const entry = entries_pos + i * 12;
            std.mem.writeInt(u32, out.items[entry..][0..4], koff, .little);
            std.mem.writeInt(u32, out.items[entry + 4 ..][0..4], @intCast(kv.key.len), .little);
        }
        for (kvs.items, 0..) |kv, i| {
            const voff: u32 = @intCast(out.items.len - start);
            try out.appendSlice(self.aa, kv.val);
            const entry = entries_pos + i * 12;
            std.mem.writeInt(u32, out.items[entry + 8 ..][0..4], voff, .little);
        }
        const total: u32 = @intCast(out.items.len - start);
        std.mem.writeInt(u32, out.items[bytelen_pos..][0..4], total, .little);
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
    };
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
/// string), or null if `obj` is not an object. Caller owns the slice.
pub fn keysArray(aa: Allocator, obj: []const u8) Allocator.Error!?[]u8 {
    if (tagOf(obj) != .object) return null;
    const count = readU32(obj, 5);
    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(aa);
    const start = out.items.len;
    try out.append(aa, @intFromEnum(Tag.array));
    const bytelen_pos = out.items.len;
    try writeU32(&out, aa, 0);
    try writeU32(&out, aa, count);
    const table_pos = out.items.len;
    for (0..count) |_| try writeU32(&out, aa, 0);
    var i: u32 = 0;
    while (i < count) : (i += 1) {
        const off: u32 = @intCast(out.items.len - start);
        std.mem.writeInt(u32, out.items[table_pos + i * 4 ..][0..4], off, .little);
        const e = 9 + i * 12;
        const koff = readU32(obj, e);
        const klen = readU32(obj, e + 4);
        try out.append(aa, @intFromEnum(Tag.string));
        try writeU32(&out, aa, klen);
        try out.appendSlice(aa, obj[koff .. koff + klen]);
    }
    const total: u32 = @intCast(out.items.len - start);
    std.mem.writeInt(u32, out.items[bytelen_pos..][0..4], total, .little);
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

/// Serialize a JSONB value to canonical JSON text into `out`.
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
        .double => {
            const fv: f64 = @bitCast(std.mem.readInt(u64, v[1..][0..8], .little));
            var buf: [40]u8 = undefined;
            try out.appendSlice(aa, std.fmt.bufPrint(&buf, "{d}", .{fv}) catch unreachable);
        },
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
                if (i != 0) try out.append(aa, ',');
                const off = readU32(v, table + i * 4);
                try toText(aa, out, v[off..][0..valueLen(v[off..])]);
            }
            try out.append(aa, ']');
        },
        .object => {
            try out.append(aa, '{');
            const count = readU32(v, 5);
            const entries = 9;
            var i: u32 = 0;
            while (i < count) : (i += 1) {
                if (i != 0) try out.append(aa, ',');
                const e = entries + i * 12;
                const koff = readU32(v, e);
                const klen = readU32(v, e + 4);
                try appendEscaped(aa, out, v[koff .. koff + klen]);
                try out.append(aa, ':');
                const voff = readU32(v, e + 8);
                try toText(aa, out, v[voff..][0..valueLen(v[voff..])]);
            }
            try out.append(aa, '}');
        },
    }
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

test "encode/serialize round-trip (canonical, keys sorted)" {
    const aa = std.testing.allocator;
    try roundtrip(aa, "  42 ", "42");
    try roundtrip(aa, "-7", "-7");
    try roundtrip(aa, "3.5", "3.5");
    try roundtrip(aa, "true", "true");
    try roundtrip(aa, "null", "null");
    try roundtrip(aa, "\"a\\nb\"", "\"a\\nb\"");
    try roundtrip(aa, "[1, 2, 3]", "[1,2,3]");
    // keys canonicalized to sorted order
    try roundtrip(aa, "{\"b\": 1, \"a\": 2}", "{\"a\":2,\"b\":1}");
    try roundtrip(aa, "{\"z\": [1, {\"y\": 2}], \"a\": \"x\"}", "{\"a\":\"x\",\"z\":[1,{\"y\":2}]}");
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
