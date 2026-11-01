//! Type system for v0.1: INT, BIGINT, BOOLEAN, VARCHAR(N), STRING.
//! Type is the schema-side description; Value is the runtime tagged datum.

const std = @import("std");

/// Which client wire/SQL flavor a statement is being parsed and served for.
/// `.neutral` is the embedded/native path (permissive, ANSI-leaning); the
/// wire servers pin `.mysql` / `.postgres` so parsing can enforce per-flavor
/// syntax. Lives here (leaf module) so the lexer/parser and the high-level
/// api/Session can share one definition without an import cycle.
pub const Dialect = enum { neutral, mysql, postgres };

pub const TypeTag = enum(u8) {
    int = 1,
    bigint = 2,
    boolean = 3,
    varchar = 4,
    string = 5,
    float = 6,
    double = 7,
    /// Days since 1970-01-01 (UTC), stored as i32.
    date = 8,
    /// Microseconds since 1970-01-01T00:00:00 UTC (timezone-naive),
    /// stored as i64.
    datetime = 9,
    tinyint = 10,
    smallint = 11,
    largeint = 12,
    char = 13,
    /// Fixed-point decimal with i64 backing (precision <= 18).
    decimal64 = 14,
    /// Fixed-point decimal with i128 backing (19 <= precision <= 38).
    decimal128 = 15,
    /// 128-bit unsigned identifier. Storage: u128 little-endian. Ordering
    /// is the natural unsigned numeric order (matches lexicographic over
    /// the canonical 8-4-4-4-12 hex form). Insertion accepts u128 or
    /// `Uuid` (logical wrapper); inserts of textual UUIDs are surfaced
    /// later via the uuid_from_string scalar function.
    uuid = 16,
    /// JSON document. Physically stored as UTF-8 text (raw JSON) — behaves
    /// like a string for storage, encoding, and the wire — but carries a
    /// distinct logical identity so the `->` / `->>` operators and the
    /// JSON_* scalar functions can type-check against it, and so wire
    /// metadata reports it as JSON. Phase 1 keeps the text representation;
    /// a binary JSONB form and columnar shredding are later phases.
    json = 17,
    /// ARRAY, MAP or STRUCT. The payload's `Nested` gives the element types.
    /// Physically each value is one variable-length cell in the
    /// self-contained encoding of `exec/nested.zig`, so storage, the WAL and
    /// row movement treat it as bytes, the way they treat a string.
    nested = 18,
    /// A set of unsigned 64-bit integers (`exec/bitmap.zig`). Physically each
    /// value is one variable-length cell in StarRocks' serialized form, so
    /// storage, the WAL and row movement treat it as bytes, as they treat a
    /// nested value.
    bitmap = 19,
    /// Fixed-point decimal with i256 backing (39 <= precision <= 76).
    decimal256 = 20,
};

/// The shape of a nested type. Interned (`internNested`): the process keeps
/// one instance per distinct shape, so a `Type` stays a plain value that is
/// copied freely and compares by pointer.
pub const Nested = union(enum) {
    /// Element type. Elements are always nullable.
    array: Type,
    map: MapTypes,
    /// Fields in order. Names are unique, compared case-insensitively;
    /// values are always nullable.
    @"struct": []const StructField,
};

/// A STRUCT field.
pub const StructField = struct {
    name: []const u8,
    type: Type,
};

/// A MAP's key and value types. Keys are unique within a map; values are
/// always nullable.
pub const MapTypes = struct {
    key: Type,
    value: Type,
};

/// Process-wide on purpose, like the time zone cache: a shape is immutable
/// and means the same in every database, and interning it keeps `Type`
/// copyable with no owner to outlive. Grows by one entry per distinct shape
/// a statement spells, and is never freed.
const NestedPool = struct {
    lock: std.atomic.Mutex = .unlocked,
    by_key: std.StringHashMapUnmanaged(*const Nested) = .empty,

    fn acquire(pool: *NestedPool) void {
        while (!pool.lock.tryLock()) std.atomic.spinLoopHint();
    }
};

var nested_pool: NestedPool = .{};
const nested_allocator = std.heap.smp_allocator;

/// The interned instance of `shape`. Its element types are interned already,
/// so two shapes are equal exactly when their shallow bytes, field names
/// included, are.
pub fn internNested(shape: Nested) error{OutOfMemory}!*const Nested {
    var fallback_buffer: [256]u8 = undefined;
    var fallback: std.heap.BufferFirstAllocator = .init(&fallback_buffer, nested_allocator);
    const key_allocator = fallback.allocator();
    var key: std.ArrayList(u8) = .empty;
    defer key.deinit(key_allocator);
    try appendNestedKey(key_allocator, &key, shape);
    nested_pool.acquire();
    defer nested_pool.lock.unlock();
    if (nested_pool.by_key.get(key.items)) |known| return known;
    const owned_key = try nested_allocator.dupe(u8, key.items);
    errdefer nested_allocator.free(owned_key);
    const instance = try nested_allocator.create(Nested);
    errdefer nested_allocator.destroy(instance);
    instance.* = try ownedShape(shape);
    errdefer freeShape(instance.*);
    try nested_pool.by_key.put(nested_allocator, owned_key, instance);
    return instance;
}

fn appendNestedKey(allocator: std.mem.Allocator, key: *std.ArrayList(u8), shape: Nested) !void {
    try key.append(allocator, @backingInt(std.meta.activeTag(shape)));
    switch (shape) {
        .array => |element| try appendTypeKey(allocator, key, element),
        .map => |m| {
            try appendTypeKey(allocator, key, m.key);
            try appendTypeKey(allocator, key, m.value);
        },
        .@"struct" => |fields| for (fields) |f| {
            var len: [4]u8 = undefined;
            std.mem.writeInt(u32, &len, @intCast(f.name.len), .little);
            try key.appendSlice(allocator, &len);
            try key.appendSlice(allocator, f.name);
            try appendTypeKey(allocator, key, f.type);
        },
    }
}

fn appendTypeKey(allocator: std.mem.Allocator, key: *std.ArrayList(u8), t: Type) !void {
    try key.append(allocator, @backingInt(std.meta.activeTag(t)));
    const payload: u64 = switch (t) {
        .varchar, .char => |n| n,
        .decimal64, .decimal128, .decimal256 => |spec| (@as(u64, spec.p) << 8) | spec.s,
        .nested => |n| @intFromPtr(n),
        else => 0,
    };
    var buf: [8]u8 = undefined;
    std.mem.writeInt(u64, &buf, payload, .little);
    try key.appendSlice(allocator, &buf);
}

/// `shape` with its fields and their names copied into the pool, as the
/// caller's may not outlive the call.
fn ownedShape(shape: Nested) !Nested {
    const fields = switch (shape) {
        .@"struct" => |f| f,
        else => return shape,
    };
    const owned = try nested_allocator.alloc(StructField, fields.len);
    var named: usize = 0;
    errdefer {
        for (owned[0..named]) |f| nested_allocator.free(f.name);
        nested_allocator.free(owned);
    }
    for (fields, owned) |f, *o| {
        o.* = .{ .name = try nested_allocator.dupe(u8, f.name), .type = f.type };
        named += 1;
    }
    return .{ .@"struct" = owned };
}

fn freeShape(shape: Nested) void {
    const fields = switch (shape) {
        .@"struct" => |f| f,
        else => return,
    };
    for (fields) |f| nested_allocator.free(f.name);
    nested_allocator.free(fields);
}

/// `ARRAY<element>`.
pub fn arrayOf(element: Type) error{OutOfMemory}!Type {
    return .{ .nested = try internNested(.{ .array = element }) };
}

/// `MAP<key, value>`.
pub fn mapOf(key: Type, value: Type) error{OutOfMemory}!Type {
    return .{ .nested = try internNested(.{ .map = .{ .key = key, .value = value } }) };
}

/// `STRUCT<fields>`. The caller keeps `fields`; the type holds a copy.
pub fn structOf(fields: []const StructField) error{OutOfMemory}!Type {
    return .{ .nested = try internNested(.{ .@"struct" = fields }) };
}

/// The index of the field `name` names, matched case-insensitively.
pub fn structFieldIndex(fields: []const StructField, name: []const u8) ?usize {
    for (fields, 0..) |f, i| if (std.ascii.eqlIgnoreCase(f.name, name)) return i;
    return null;
}

/// The canonical text of `t` (`int`, `varchar(20)`, `decimal(10,2)`,
/// `array<array<int>>`, `map<string,int>`, `struct<a int,b string>`; a
/// field name that is no plain identifier in backticks), which `parseTypeText` reads
/// back. Where a type leaves the process (schema files, the IR wire, CAST's
/// function name) it travels as this text, since a nested shape is a pointer.
pub fn appendTypeText(allocator: std.mem.Allocator, out: *std.ArrayList(u8), t: Type) std.mem.Allocator.Error!void {
    var buf: [32]u8 = undefined;
    switch (t) {
        .varchar => |n| try out.appendSlice(allocator, std.fmt.bufPrint(&buf, "varchar({d})", .{n}) catch unreachable),
        .char => |n| try out.appendSlice(allocator, std.fmt.bufPrint(&buf, "char({d})", .{n}) catch unreachable),
        .decimal64, .decimal128, .decimal256 => |spec| try out.appendSlice(allocator, std.fmt.bufPrint(&buf, "decimal({d},{d})", .{ spec.p, spec.s }) catch unreachable),
        .nested => |n| switch (n.*) {
            .array => |element| {
                try out.appendSlice(allocator, "array<");
                try appendTypeText(allocator, out, element);
                try out.append(allocator, '>');
            },
            .map => |m| {
                try out.appendSlice(allocator, "map<");
                try appendTypeText(allocator, out, m.key);
                try out.append(allocator, ',');
                try appendTypeText(allocator, out, m.value);
                try out.append(allocator, '>');
            },
            .@"struct" => |fields| {
                try out.appendSlice(allocator, "struct<");
                for (fields, 0..) |f, i| {
                    if (i > 0) try out.append(allocator, ',');
                    try appendFieldNameText(allocator, out, f.name);
                    try out.append(allocator, ' ');
                    try appendTypeText(allocator, out, f.type);
                }
                try out.append(allocator, '>');
            },
        },
        else => try out.appendSlice(allocator, @tagName(t)),
    }
}

/// A STRUCT field name as type text spells it: as is when plain, else in
/// backticks.
pub fn appendFieldNameText(allocator: std.mem.Allocator, out: *std.ArrayList(u8), name: []const u8) std.mem.Allocator.Error!void {
    if (isPlainFieldName(name)) return out.appendSlice(allocator, name);
    try out.append(allocator, '`');
    try out.appendSlice(allocator, name);
    try out.append(allocator, '`');
}

pub const TypeTextError = error{InvalidTypeText} || std.mem.Allocator.Error;

/// Read comma-joined field names as `appendFieldNameText` writes them.
pub fn parseFieldNamesText(allocator: std.mem.Allocator, text: []const u8) TypeTextError![]const []const u8 {
    var names: std.ArrayList([]const u8) = .empty;
    errdefer names.deinit(allocator);
    var pos: usize = 0;
    while (true) {
        try names.append(allocator, try typeTextFieldName(text, &pos));
        if (pos == text.len) return names.toOwnedSlice(allocator);
        try expectTypeChar(text, &pos, ',');
    }
}

/// Read the canonical text `appendTypeText` writes; the whole text must be
/// one type.
pub fn parseTypeText(text: []const u8) TypeTextError!Type {
    var pos: usize = 0;
    const t = try parseTypeTextAt(text, &pos);
    if (pos != text.len) return error.InvalidTypeText;
    return t;
}

fn parseTypeTextAt(text: []const u8, pos: *usize) TypeTextError!Type {
    const start = pos.*;
    while (pos.* < text.len and (std.ascii.isAlphanumeric(text[pos.*]) or text[pos.*] == '_')) pos.* += 1;
    const name = text[start..pos.*];
    if (std.mem.eql(u8, name, "array")) {
        try expectTypeChar(text, pos, '<');
        const element = try parseTypeTextAt(text, pos);
        try expectTypeChar(text, pos, '>');
        return arrayOf(element);
    }
    if (std.mem.eql(u8, name, "map")) {
        try expectTypeChar(text, pos, '<');
        const key = try parseTypeTextAt(text, pos);
        try expectTypeChar(text, pos, ',');
        const value = try parseTypeTextAt(text, pos);
        try expectTypeChar(text, pos, '>');
        return mapOf(key, value);
    }
    if (std.mem.eql(u8, name, "struct")) {
        try expectTypeChar(text, pos, '<');
        var fallback_buffer: [16 * @sizeOf(StructField)]u8 = undefined;
        var fallback: std.heap.BufferFirstAllocator = .init(&fallback_buffer, nested_allocator);
        const allocator = fallback.allocator();
        var fields: std.ArrayList(StructField) = .empty;
        defer fields.deinit(allocator);
        while (true) {
            const field_name = try typeTextFieldName(text, pos);
            try expectTypeChar(text, pos, ' ');
            try fields.append(allocator, .{ .name = field_name, .type = try parseTypeTextAt(text, pos) });
            if (pos.* < text.len and text[pos.*] == ',') {
                pos.* += 1;
                continue;
            }
            try expectTypeChar(text, pos, '>');
            return structOf(fields.items);
        }
    }
    if (std.mem.eql(u8, name, "varchar") or std.mem.eql(u8, name, "char")) {
        try expectTypeChar(text, pos, '(');
        const n = try typeTextInt(u32, text, pos);
        try expectTypeChar(text, pos, ')');
        return if (name[0] == 'v') .{ .varchar = n } else .{ .char = n };
    }
    if (std.mem.eql(u8, name, "decimal")) {
        try expectTypeChar(text, pos, '(');
        const p = try typeTextInt(u8, text, pos);
        try expectTypeChar(text, pos, ',');
        const s = try typeTextInt(u8, text, pos);
        try expectTypeChar(text, pos, ')');
        if (p == 0 or p > max_decimal_precision or s > p) return error.InvalidTypeText;
        return decimalOf(.{ .p = p, .s = s });
    }
    inline for (@typeInfo(Type).@"union".field_names, @typeInfo(Type).@"union".field_types) |field_name, field_type| {
        if (field_type == void and std.mem.eql(u8, name, field_name)) return @unionInit(Type, field_name, {});
    }
    return error.InvalidTypeText;
}

/// Whether a STRUCT field name reads back unquoted in type text.
fn isPlainFieldName(name: []const u8) bool {
    if (name.len == 0) return false;
    for (name) |c| if (!std.ascii.isAlphanumeric(c) and c != '_') return false;
    return true;
}

fn typeTextFieldName(text: []const u8, pos: *usize) TypeTextError![]const u8 {
    if (pos.* < text.len and text[pos.*] == '`') {
        const start = pos.* + 1;
        const end = std.mem.indexOfScalarPos(u8, text, start, '`') orelse return error.InvalidTypeText;
        pos.* = end + 1;
        return text[start..end];
    }
    const start = pos.*;
    while (pos.* < text.len and (std.ascii.isAlphanumeric(text[pos.*]) or text[pos.*] == '_')) pos.* += 1;
    if (pos.* == start) return error.InvalidTypeText;
    return text[start..pos.*];
}

fn expectTypeChar(text: []const u8, pos: *usize, c: u8) TypeTextError!void {
    if (pos.* >= text.len or text[pos.*] != c) return error.InvalidTypeText;
    pos.* += 1;
}

fn typeTextInt(comptime T: type, text: []const u8, pos: *usize) TypeTextError!T {
    const start = pos.*;
    while (pos.* < text.len and std.ascii.isDigit(text[pos.*])) pos.* += 1;
    return std.fmt.parseInt(T, text[start..pos.*], 10) catch error.InvalidTypeText;
}

/// Precision/scale carried by every decimal type variant.
pub const DecimalSpec = struct {
    /// Total number of significant digits. Range [1, 76].
    p: u8,
    /// Digits after the decimal point. Range [0, p].
    s: u8,
};

/// The most digits a DECIMAL holds, in its i256 backing.
pub const max_decimal_precision: u8 = 76;
/// The most digits the i128 backing holds.
pub const max_decimal128_precision: u8 = 38;

/// The decimal type of `spec`, in the smallest backing that holds its
/// precision.
pub fn decimalOf(spec: DecimalSpec) Type {
    if (spec.p <= 18) return .{ .decimal64 = spec };
    if (spec.p <= max_decimal128_precision) return .{ .decimal128 = spec };
    return .{ .decimal256 = spec };
}

/// `v` as the nearest f64, for an integer of any width to 256 bits. LLVM
/// lowers no 256-bit integer-to-float conversion on AArch64, so a wider one
/// converts the top 64 bits of its magnitude, the bits below folded into a
/// sticky bit so it rounds as the conversion itself would.
pub inline fn floatOfInt(v: anytype) f64 {
    const T = @TypeOf(v);
    if (@typeInfo(T) == .comptime_int or @bitSizeOf(T) <= 128) return @floatFromInt(v);
    const magnitude: u256 = @abs(@as(i256, v));
    const f = if (magnitude >> 128 == 0) @as(f64, @floatFromInt(@as(u128, @intCast(magnitude)))) else wide: {
        const shift: u8 = @intCast(192 - @as(u16, @clz(magnitude)));
        var top: u64 = @truncate(magnitude >> shift);
        if (magnitude & ((@as(u256, 1) << shift) - 1) != 0) top |= 1;
        break :wide std.math.ldexp(@as(f64, @floatFromInt(top)), shift);
    };
    return if (v < 0) -f else f;
}

/// Build a `Type` for a DECIMAL(p, s). Picks the smallest backing that
/// holds `p` digits.
pub fn decimal(comptime p: u8, comptime s: u8) Type {
    comptime {
        if (p == 0 or p > max_decimal_precision) @compileError("DECIMAL precision must be in 1..=76");
        if (s > p) @compileError("DECIMAL scale must be <= precision");
    }
    return decimalOf(.{ .p = p, .s = s });
}

pub const Type = union(TypeTag) {
    int,
    bigint,
    boolean,
    varchar: u32, // declared max length (not enforced in v0.1)
    string,
    float,
    double,
    date,
    datetime,
    tinyint,
    smallint,
    largeint,
    /// CHAR(N) — declared max length N, storage identical to VARCHAR.
    /// Per DuckDB convention: no blank-padding, N is metadata only.
    char: u32,
    decimal64: DecimalSpec,
    decimal128: DecimalSpec,
    uuid,
    json,
    nested: *const Nested,
    bitmap,
    decimal256: DecimalSpec,

    pub fn fixedSize(self: Type) ?usize {
        return switch (self) {
            .int => @sizeOf(i32),
            .bigint => @sizeOf(i64),
            .boolean => @sizeOf(u8),
            .float => @sizeOf(f32),
            .double => @sizeOf(f64),
            .date => @sizeOf(i32),
            .datetime => @sizeOf(i64),
            .tinyint => @sizeOf(i8),
            .smallint => @sizeOf(i16),
            .largeint => @sizeOf(i128),
            .decimal64 => @sizeOf(i64),
            .decimal128 => @sizeOf(i128),
            .decimal256 => @sizeOf(i256),
            .uuid => @sizeOf(u128),
            .varchar, .string, .char, .json, .nested, .bitmap => null,
        };
    }

    /// Stored as one variable-length cell per row: the string family, JSON,
    /// the nested types and BITMAP. Storage, row movement and the WAL key off
    /// this; `isString` is the narrower logical question.
    pub fn isVarlen(self: Type) bool {
        return self.isString() or self == .nested or self == .bitmap;
    }

    pub fn isNested(self: Type) bool {
        return self == .nested;
    }

    /// The element type of an ARRAY, or null for any other type.
    pub fn arrayElement(self: Type) ?Type {
        return switch (self) {
            .nested => |n| switch (n.*) {
                .array => |element| element,
                else => null,
            },
            else => null,
        };
    }

    /// The key and value types of a MAP, or null for any other type.
    pub fn mapTypes(self: Type) ?MapTypes {
        return switch (self) {
            .nested => |n| switch (n.*) {
                .map => |m| m,
                else => null,
            },
            else => null,
        };
    }

    /// The fields of a STRUCT, or null for any other type.
    pub fn structFields(self: Type) ?[]const StructField {
        return switch (self) {
            .nested => |n| switch (n.*) {
                .@"struct" => |fields| fields,
                else => null,
            },
            else => null,
        };
    }

    /// JSON and the VARCHAR family share one physical representation:
    /// variable-length UTF-8 bytes. Storage, encoding, and the wire all key
    /// off `isString`, so JSON flows through them unchanged. Use `isJson`
    /// where the *logical* distinction matters (operators, wire type code).
    pub fn isString(self: Type) bool {
        return switch (self) {
            .varchar, .string, .char, .json => true,
            else => false,
        };
    }

    pub fn isJson(self: Type) bool {
        return self == .json;
    }

    pub fn isFloat(self: Type) bool {
        return switch (self) {
            .float, .double => true,
            else => false,
        };
    }

    pub fn isTemporal(self: Type) bool {
        return switch (self) {
            .date, .datetime => true,
            else => false,
        };
    }

    pub fn isInteger(self: Type) bool {
        return switch (self) {
            .tinyint, .smallint, .int, .bigint, .largeint => true,
            else => false,
        };
    }

    pub fn isDecimal(self: Type) bool {
        return switch (self) {
            .decimal64, .decimal128, .decimal256 => true,
            else => false,
        };
    }

    /// Returns the (p, s) of a decimal column, or null if not a decimal.
    pub fn decimalSpec(self: Type) ?DecimalSpec {
        return switch (self) {
            .decimal64, .decimal128, .decimal256 => |spec| spec,
            else => null,
        };
    }

    pub fn matchesZigType(self: Type, comptime T: type) bool {
        return switch (self) {
            .int => T == i32 or T == comptime_int,
            .bigint => T == i64 or T == comptime_int,
            .boolean => T == bool,
            .float => T == f32 or T == comptime_float,
            .double => T == f64 or T == comptime_float,
            .date => T == Date or T == i32 or T == comptime_int,
            .datetime => T == DateTime or T == i64 or T == comptime_int,
            .tinyint => T == i8 or T == comptime_int,
            .smallint => T == i16 or T == comptime_int,
            .largeint => T == i128 or T == comptime_int,
            .decimal64 => T == i64 or T == comptime_int,
            .decimal128 => T == i128 or T == comptime_int,
            .decimal256 => T == i256 or T == comptime_int,
            .uuid => T == Uuid or T == u128 or T == comptime_int,
            .varchar, .string, .char, .json, .nested, .bitmap => isStringLikeType(T),
        };
    }
};

/// Total order for floats with NaN sorting LAST (greater than every finite value
/// and ±inf), so float ORDER BY / MIN / MAX are deterministic — matching
/// Postgres/DuckDB. `std.math.order` leaves NaN unordered (`.eq` to everything),
/// which makes sorts nondeterministic; this replaces it at every float-compare
/// site. Accepts f32 or f64.
pub fn floatOrder(a: anytype, b: @TypeOf(a)) std.math.Order {
    const an = std.math.isNan(a);
    const bn = std.math.isNan(b);
    if (an or bn) {
        if (an and bn) return .eq;
        return if (an) .gt else .lt; // NaN is the largest
    }
    return std.math.order(a, b);
}

/// The one representative of a float's equality class: -0.0 and +0.0 are one
/// value and so is every NaN, as in `floatOrder` (#82). A site that hashes,
/// dedupes or matches float keys by their bits canonicalizes first, or equal
/// keys land in different groups, join buckets or Bloom slots.
pub fn canonicalFloat(v: anytype) @TypeOf(v) {
    if (std.math.isNan(v)) return std.math.nan(@TypeOf(v));
    return if (v == 0) 0 else v;
}

pub fn canonicalFloatBits(v: anytype) @Int(.unsigned, @bitSizeOf(@TypeOf(v))) {
    return @bitCast(canonicalFloat(v));
}

/// Logical wrapper around a stored `u128` UUID. Mirrors `Date`/`DateTime`
/// — gives `@as(Uuid, ...)` ergonomics for insert without colliding with
/// LARGEINT columns. `.value` accesses the raw u128. UUIDs are
/// big-endian when serialized to the canonical 8-4-4-4-12 hex form, but
/// stored as u128 in native byte order — the formatter converts.
pub const Uuid = enum(u128) {
    _,
    pub fn fromU128(v: u128) Uuid {
        return @fromBackingInt(@intCast(v));
    }
    pub fn value(self: Uuid) u128 {
        return @backingInt(self);
    }
};

/// Logical wrapper around a stored `i32` of days since the epoch. Provided
/// so insertion can be unambiguous (`@as(Date, ...)`) without colliding
/// with INT columns. `.value` accesses the raw days count.
pub const Date = enum(i32) {
    _,
    pub fn fromDays(d: i32) Date {
        return @fromBackingInt(@intCast(d));
    }
    pub fn days(self: Date) i32 {
        return @backingInt(self);
    }
};

/// Logical wrapper around a stored `i64` of microseconds since the epoch.
pub const DateTime = enum(i64) {
    _,
    pub fn fromMicros(us: i64) DateTime {
        return @fromBackingInt(@intCast(us));
    }
    pub fn micros(self: DateTime) i64 {
        return @backingInt(self);
    }
};

/// Returns true if values of type `T` can be coerced to `[]const u8` for
/// insertion into a VARCHAR/STRING column. Handles `[]const u8`, `[]u8`, and
/// string-literal types (`*const [N:0]u8`).
pub fn isStringLikeType(comptime T: type) bool {
    if (T == []const u8 or T == []u8) return true;
    const info = @typeInfo(T);
    if (info == .pointer) {
        const p = info.pointer;
        switch (p.size) {
            .slice => return p.child == u8,
            .one => {
                const child_info = @typeInfo(p.child);
                return child_info == .array and child_info.array.child == u8;
            },
            else => return false,
        }
    }
    return false;
}

/// Runtime value used for filter literals and inserts originating from
/// untyped sources. Insert from a typed struct uses comptime reflection
/// to skip the boxing.
pub const ValueTag = enum(u8) {
    int = 1,
    bigint = 2,
    boolean = 3,
    text = 4, // covers VARCHAR, STRING, and CHAR
    float = 5,
    double = 6,
    date = 7,
    datetime = 8,
    tinyint = 9,
    smallint = 10,
    largeint = 11,
    decimal64 = 12,
    decimal128 = 13,
    uuid = 14,
    decimal256 = 15,

    pub fn fromType(t: Type) ValueTag {
        return switch (t) {
            .int => .int,
            .bigint => .bigint,
            .boolean => .boolean,
            .varchar, .string, .char, .json, .nested, .bitmap => .text,
            .float => .float,
            .double => .double,
            .date => .date,
            .datetime => .datetime,
            .tinyint => .tinyint,
            .smallint => .smallint,
            .largeint => .largeint,
            .decimal64 => .decimal64,
            .decimal128 => .decimal128,
            .decimal256 => .decimal256,
            .uuid => .uuid,
        };
    }
};

pub const Value = union(ValueTag) {
    int: i32,
    bigint: i64,
    boolean: bool,
    text: []const u8,
    float: f32,
    double: f64,
    date: i32,
    datetime: i64,
    tinyint: i8,
    smallint: i16,
    largeint: i128,
    decimal64: i64,
    decimal128: i128,
    uuid: u128,
    decimal256: i256,

    pub fn compare(self: Value, other: Value) std.math.Order {
        std.debug.assert(std.meta.activeTag(self) == std.meta.activeTag(other));
        return switch (self) {
            .int => |a| std.math.order(a, other.int),
            .bigint => |a| std.math.order(a, other.bigint),
            .boolean => |a| std.math.order(@as(u8, @intFromBool(a)), @as(u8, @intFromBool(other.boolean))),
            .float => |a| floatOrder(a, other.float),
            .double => |a| floatOrder(a, other.double),
            .date => |a| std.math.order(a, other.date),
            .datetime => |a| std.math.order(a, other.datetime),
            .tinyint => |a| std.math.order(a, other.tinyint),
            .smallint => |a| std.math.order(a, other.smallint),
            .largeint => |a| std.math.order(a, other.largeint),
            .decimal64 => |a| std.math.order(a, other.decimal64),
            .decimal128 => |a| std.math.order(a, other.decimal128),
            .decimal256 => |a| std.math.order(a, other.decimal256),
            .uuid => |a| std.math.order(a, other.uuid),
            .text => |a| switch (std.mem.order(u8, a, other.text)) {
                .lt => .lt,
                .gt => .gt,
                .eq => .eq,
            },
        };
    }

    /// Same tag and same payload (`.text` by bytes). Floats compare with
    /// `==`, so NaN never equals itself.
    pub fn eql(self: Value, other: Value) bool {
        if (std.meta.activeTag(self) != std.meta.activeTag(other)) return false;
        return switch (self) {
            .text => |a| std.mem.eql(u8, a, other.text),
            inline else => |a, tag| a == @field(other, @tagName(tag)),
        };
    }
};

/// Where ALTER TABLE ... ADD COLUMN puts the new column: `FIRST`,
/// `AFTER name`, or last when the statement names no position.
pub const ColumnPosition = union(enum) {
    last,
    first,
    after: []const u8,
};

pub const Column = struct {
    name: []const u8,
    type: Type,
    /// True if NULL values are permitted in this column. When true, segment
    /// blocks and memtable buffers carry a validity bitmap alongside the
    /// data. Default `false` (NOT NULL) matches the original v0.1 contract.
    nullable: bool = false,
    /// Optional default-value expression for this column. Today only
    /// literal values are accepted (`DEFAULT 0`, `DEFAULT 'unset'`,
    /// `DEFAULT TRUE`). Used by the INSERT path when the user omits
    /// this column from their column list. The value's tag must match
    /// the column type (the parser + compile path enforce this).
    default_value: ?Value = null,
    /// `DEFAULT CURRENT_TIMESTAMP`: INSERT fills an omitted column with
    /// the wall-clock time. Datetime columns only; exclusive with
    /// `default_value`.
    default_now: bool = false,
    /// `ON UPDATE CURRENT_TIMESTAMP`: an UPDATE, or the update branch of
    /// an upsert, that changes a row without assigning this column sets it
    /// to the statement's timestamp. Datetime columns only.
    on_update_now: bool = false,
    /// MySQL-style AUTO_INCREMENT attribute. At most one column per
    /// table; type must be an integer width. The owning Table holds
    /// a monotonic counter in its manifest; INSERT fills omitted /
    /// NULL inserts with the counter, and bumps it past any explicit
    /// caller-supplied value.
    auto_increment: bool = false,
};

pub const TableSchemaError = error{
    DuplicateColumn,
    OrderKeyColumnMissing,
    EmptyColumns,
    EmptyOrderKey,
};

/// Per-table block compression for the general (post-encoding) payload bytes.
/// A storage policy, not part of the table's identity: blocks self-describe
/// their compression on disk, so it's excluded from the schema fingerprint and
/// `schemasEqual`. Applied by flush and compaction; changing it on an existing
/// table re-compresses lazily as segments get rewritten.
pub const TableCompression = enum(u8) {
    /// Encodings only; payload bytes stored raw.
    none = 0,
    /// zstd-3 on every block; the cache holds decompressed bytes. Fastest hot
    /// reads, largest resident set, best ratio.
    zstd = 1,
    /// LZ4HC on every block (decode ~4 GB/s); every block is decompressed
    /// once at cache fill.
    lz4 = 2,
    /// `.lz4` everywhere, except large string blocks are FSST-encoded: the
    /// cache holds the FSST form (per-string random access — kernels with
    /// per-row lanes decode only the rows they touch), and the FSST bytes are
    /// themselves LZ4'd on disk.
    lz4_fsst = 3,
};

pub const default_table_compression: TableCompression = .lz4;

pub const TableSchema = struct {
    columns: []const Column,
    order_key: []const []const u8,
    unique: bool,
    compression: TableCompression = default_table_compression,

    /// Validate basic invariants. Does not allocate.
    pub fn validate(self: TableSchema) TableSchemaError!void {
        if (self.columns.len == 0) return TableSchemaError.EmptyColumns;
        if (self.order_key.len == 0) return TableSchemaError.EmptyOrderKey;

        for (self.columns, 0..) |c, i| {
            for (self.columns[0..i]) |prior| {
                if (columnNameEql(c.name, prior.name)) return TableSchemaError.DuplicateColumn;
            }
        }

        for (self.order_key, 0..) |key, i| {
            if (self.columnIndex(key) == null) return TableSchemaError.OrderKeyColumnMissing;
            // A repeat sorts nothing the first use didn't, and a unique key
            // can't hold one column twice; MySQL and StarRocks refuse it too.
            if (columnNameIn(self.order_key[0..i], key)) return TableSchemaError.DuplicateColumn;
        }
    }

    pub fn columnIndex(self: TableSchema, name: []const u8) ?usize {
        for (self.columns, 0..) |c, i| {
            if (columnNameEql(c.name, name)) return i;
        }
        return null;
    }

    pub fn column(self: TableSchema, name: []const u8) ?Column {
        const idx = self.columnIndex(name) orelse return null;
        return self.columns[idx];
    }
};

/// Case-insensitive compare for user-facing identifier lookup. The SQL
/// lexer already lowercases unquoted identifiers, but schemas created
/// through the Zig API (or via quoted DDL) keep their original case —
/// every lookup site needs to bridge that, or `WatchID` vs `watchid`
/// silently 404s.
pub fn columnNameEql(a: []const u8, b: []const u8) bool {
    return std.ascii.eqlIgnoreCase(a, b);
}

fn columnNameIn(names: []const []const u8, name: []const u8) bool {
    for (names) |n| if (columnNameEql(n, name)) return true;
    return false;
}

/// Rename each name an earlier one in `names` already uses to `name_N`, the
/// smallest N no name claims: a result's columns bind by name, so a repeat
/// keeps its first use (DuckDB's naming, as the parser gives repeated SELECT
/// items). Returns the names it allocated, for the caller to free once the
/// names are copied.
pub fn dedupeColumnNames(allocator: std.mem.Allocator, names: [][]const u8) std.mem.Allocator.Error![][]u8 {
    var renamed: std.ArrayList([]u8) = .empty;
    errdefer {
        for (renamed.items) |r| allocator.free(r);
        renamed.deinit(allocator);
    }
    for (names, 0..) |*name, i| {
        if (!columnNameIn(names[0..i], name.*)) continue;
        var n: usize = 1;
        while (true) : (n += 1) {
            try renamed.ensureUnusedCapacity(allocator, 1);
            const candidate = try std.fmt.allocPrint(allocator, "{s}_{d}", .{ name.*, n });
            if (columnNameIn(names, candidate)) {
                allocator.free(candidate);
                continue;
            }
            renamed.appendAssumeCapacity(candidate);
            name.* = candidate;
            break;
        }
    }
    return renamed.toOwnedSlice(allocator);
}

pub const QualifiedName = struct {
    qualifier: []const u8,
    bare: []const u8,
};

/// Split a qualified name (`alias.col`, or `db.table.col`) into the
/// qualifier and the bare column. The one rule for where a qualifier ends:
/// every lookup, pruning pass and result-name presenter that tolerates
/// qualifiers splits through here.
///
/// Qualifiers are identifiers but column names need not be: a literal or
/// expression column is named `0.5` or `mul(x, 1.5)`, and an alias
/// qualifies it as `s.0.5` or `s.mul(x, 1.5)`. So the qualifier is the run
/// of leading identifier segments, and the bare column is everything after
/// it, dots included. When that leaves a bare column that isn't an
/// identifier, the qualifier is the first segment alone: an alias qualifies
/// with one, and a column named by its text may itself open with one
/// (`d.t.a + 1` is column `t.a + 1` of `d`). A name with no leading
/// identifier segment (`0.5`, `add(x, 1.5)`, a JSON path) splits only when
/// its last segment is an identifier, which keeps quoted qualifiers like
/// `` `my tbl`.x `` working.
pub fn splitQualifiedName(name: []const u8) ?QualifiedName {
    var bare_start: usize = 0;
    var first_dot: usize = 0;
    while (std.mem.indexOfScalarPos(u8, name, bare_start, '.')) |dot| {
        if (!isPlainIdentifier(name[bare_start..dot])) break;
        if (bare_start == 0) first_dot = dot;
        bare_start = dot + 1;
    }
    if (bare_start > 0 and bare_start < name.len) {
        if (isPlainIdentifier(name[bare_start..])) return .{ .qualifier = name[0 .. bare_start - 1], .bare = name[bare_start..] };
        return .{ .qualifier = name[0..first_dot], .bare = name[first_dot + 1 ..] };
    }
    const dot = std.mem.lastIndexOfScalar(u8, name, '.') orelse return null;
    if (dot == 0 or !isPlainIdentifier(name[dot + 1 ..])) return null;
    return .{ .qualifier = name[0..dot], .bare = name[dot + 1 ..] };
}

/// `name` split the way a client sees it: as `qualifier.column` only when
/// the column is an identifier. A result named by its text, like `t.price *
/// t.qty` or `e.a IS NULL`, stays whole.
pub fn splitResultName(name: []const u8) ?QualifiedName {
    const split = splitQualifiedName(name) orelse return null;
    return if (isPlainIdentifier(split.bare)) split else null;
}

/// The bare column of a possibly qualified name (`t.x` → `x`, `s.0.5` →
/// `0.5`, `0.5` → `0.5`).
pub fn unqualifiedName(name: []const u8) []const u8 {
    return if (splitQualifiedName(name)) |split| split.bare else name;
}

pub fn isPlainIdentifier(s: []const u8) bool {
    if (s.len == 0 or (!std.ascii.isAlphabetic(s[0]) and s[0] != '_')) return false;
    for (s) |ch| {
        if (!std.ascii.isAlphanumeric(ch) and ch != '_' and ch != '$') return false;
    }
    return true;
}

/// Resolve a column reference (possibly qualified `alias.col`) against a
/// flat column slice. Used by every user-facing lookup site so the
/// parser can leave qualifiers intact without each operator carrying
/// its own match logic.
///
/// Match order:
///   1. Exact match on the full string.
///   2. If `name` is qualified (`splitQualifiedName`), retry exact match
///      against its bare column. Lets bare-table users keep writing
///      `t.col` against an unaliased scan whose schema only has `col`.
///   3. Else search for any qualified column whose bare column is
///      `name` — accepted only when exactly one column matches. Lets
///      bare `col` resolve against an aliased schema (`a.col`).
pub fn findColumn(columns: []const Column, name: []const u8) ?usize {
    for (columns, 0..) |c, i| {
        if (columnNameEql(c.name, name)) return i;
    }
    if (splitQualifiedName(name)) |split| {
        for (columns, 0..) |c, i| {
            if (columnNameEql(c.name, split.bare)) return i;
        }
    }
    // A name that only looks qualified (`t.a + 1`, a column named by its
    // text) is still the bare column of an alias-qualified one.
    var match: ?usize = null;
    for (columns, 0..) |c, i| {
        const split = splitQualifiedName(c.name) orelse continue;
        if (columnNameEql(split.bare, name)) {
            if (match != null) return null; // ambiguous
            match = i;
        }
    }
    return match;
}

/// Whether an exclude of `excluded` drops `columns[index]`: a column any
/// name spells exactly, or the one a name resolves to as a reference does.
/// A hidden column staged below an aliased input surfaces alias-qualified
/// (`l.__join_on_left_0`), and its bare name must still drop it.
pub fn columnExcluded(columns: []const Column, index: usize, excluded: []const []const u8) bool {
    for (excluded) |name| {
        if (columnNameEql(columns[index].name, name)) return true;
        if (findColumn(columns, name) == index) return true;
    }
    return false;
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

test "floatOfInt rounds an i256 to the nearest double, ties to even" {
    const two_200: i256 = @as(i256, 1) << 200;
    const cases = .{
        .{ @as(i256, 12345), 12345.0 },
        .{ two_200 + 1, std.math.ldexp(@as(f64, 1), 200) },
        .{ two_200 + (@as(i256, 1) << 147), std.math.ldexp(@as(f64, 1), 200) },
        .{ two_200 + (@as(i256, 1) << 147) + 1, std.math.ldexp(@as(f64, 1), 200) + std.math.ldexp(@as(f64, 1), 148) },
        .{ -(two_200 + (@as(i256, 1) << 147) + 1), -(std.math.ldexp(@as(f64, 1), 200) + std.math.ldexp(@as(f64, 1), 148)) },
        .{ (@as(i256, 3) << 200) + (@as(i256, 1) << 148), 3 * std.math.ldexp(@as(f64, 1), 200) },
        .{ @as(i256, std.math.maxInt(i256)), std.math.ldexp(@as(f64, 1), 255) },
        .{ @as(i256, std.math.minInt(i256)), -std.math.ldexp(@as(f64, 1), 255) },
    };
    inline for (cases) |c| try std.testing.expectEqual(@as(f64, c[1]), floatOfInt(c[0]));
    // The digits read back as a double round correctly too.
    var prng = std.Random.DefaultPrng.init(0x256);
    const random = prng.random();
    var buf: [80]u8 = undefined;
    for (0..2000) |_| {
        const raw: i256 = @bitCast([4]u64{ random.int(u64), random.int(u64), random.int(u64), random.int(u64) });
        const v = raw >> random.intRangeAtMost(u8, 0, 254);
        const digits = try std.fmt.bufPrint(&buf, "{d}", .{v});
        try std.testing.expectEqual(try std.fmt.parseFloat(f64, digits), floatOfInt(v));
    }
}

test "Type.fixedSize returns correct widths" {
    try std.testing.expectEqual(@as(?usize, 4), (Type{ .int = {} }).fixedSize());
    try std.testing.expectEqual(@as(?usize, 8), (Type{ .bigint = {} }).fixedSize());
    try std.testing.expectEqual(@as(?usize, 1), (Type{ .boolean = {} }).fixedSize());
    try std.testing.expectEqual(@as(?usize, null), (Type{ .string = {} }).fixedSize());
    try std.testing.expectEqual(@as(?usize, null), (Type{ .varchar = 32 }).fixedSize());
}

test "Type.matchesZigType" {
    try std.testing.expect((Type{ .int = {} }).matchesZigType(i32));
    try std.testing.expect(!(Type{ .int = {} }).matchesZigType(i64));
    try std.testing.expect((Type{ .bigint = {} }).matchesZigType(i64));
    try std.testing.expect((Type{ .boolean = {} }).matchesZigType(bool));
    try std.testing.expect((Type{ .string = {} }).matchesZigType([]const u8));
    try std.testing.expect((Type{ .varchar = 32 }).matchesZigType([]const u8));
}

test "ValueTag.fromType maps varchar+string to text" {
    try std.testing.expectEqual(ValueTag.int, ValueTag.fromType(.int));
    try std.testing.expectEqual(ValueTag.text, ValueTag.fromType(.string));
    try std.testing.expectEqual(ValueTag.text, ValueTag.fromType(.{ .varchar = 16 }));
}

test "Value.compare on int" {
    const a = Value{ .int = 5 };
    const b = Value{ .int = 10 };
    try std.testing.expectEqual(std.math.Order.lt, a.compare(b));
    try std.testing.expectEqual(std.math.Order.gt, b.compare(a));
    try std.testing.expectEqual(std.math.Order.eq, a.compare(a));
}

test "Value.compare on text" {
    const a = Value{ .text = "apple" };
    const b = Value{ .text = "banana" };
    try std.testing.expectEqual(std.math.Order.lt, a.compare(b));
}

test "TableSchema.validate accepts a well-formed schema" {
    const schema = TableSchema{
        .columns = &.{
            .{ .name = "id", .type = .bigint },
            .{ .name = "name", .type = .string },
            .{ .name = "active", .type = .boolean },
        },
        .order_key = &.{"id"},
        .unique = true,
    };
    try schema.validate();
    try std.testing.expectEqual(@as(?usize, 0), schema.columnIndex("id"));
    try std.testing.expectEqual(@as(?usize, 2), schema.columnIndex("active"));
    try std.testing.expectEqual(@as(?usize, null), schema.columnIndex("missing"));
}

test "TableSchema.validate rejects duplicate columns" {
    const schema = TableSchema{
        .columns = &.{
            .{ .name = "id", .type = .bigint },
            .{ .name = "id", .type = .int },
        },
        .order_key = &.{"id"},
        .unique = false,
    };
    try std.testing.expectError(TableSchemaError.DuplicateColumn, schema.validate());
}

test "TableSchema.validate rejects order key that isn't a column" {
    const schema = TableSchema{
        .columns = &.{.{ .name = "id", .type = .bigint }},
        .order_key = &.{"missing"},
        .unique = false,
    };
    try std.testing.expectError(TableSchemaError.OrderKeyColumnMissing, schema.validate());
}

test "TableSchema.validate rejects an order key that names a column twice" {
    const schema = TableSchema{
        .columns = &.{ .{ .name = "k1", .type = .int }, .{ .name = "k2", .type = .int } },
        .order_key = &.{ "k2", "k1", "K2" },
        .unique = false,
    };
    try std.testing.expectError(TableSchemaError.DuplicateColumn, schema.validate());
}

test "TableSchema.validate rejects empty columns" {
    const schema = TableSchema{
        .columns = &.{},
        .order_key = &.{"id"},
        .unique = false,
    };
    try std.testing.expectError(TableSchemaError.EmptyColumns, schema.validate());
}

test "floatOrder sorts NaN last and is otherwise numeric" {
    const order = std.math.Order;
    try std.testing.expectEqual(order.lt, floatOrder(@as(f64, 1.0), 2.0));
    try std.testing.expectEqual(order.gt, floatOrder(@as(f64, 2.0), 1.0));
    try std.testing.expectEqual(order.eq, floatOrder(@as(f64, 1.0), 1.0));
    // ±inf order intact.
    try std.testing.expectEqual(order.lt, floatOrder(-std.math.inf(f64), std.math.inf(f64)));
    // NaN is greater than every finite value and ±inf, in either argument slot.
    const nan = std.math.nan(f64);
    try std.testing.expectEqual(order.gt, floatOrder(nan, std.math.inf(f64)));
    try std.testing.expectEqual(order.lt, floatOrder(std.math.inf(f64), nan));
    try std.testing.expectEqual(order.eq, floatOrder(nan, nan));
    // Works for f32 too.
    try std.testing.expectEqual(order.lt, floatOrder(@as(f32, 1.0), 2.0));
    try std.testing.expectEqual(order.gt, floatOrder(std.math.nan(f32), @as(f32, 9.0)));
}

test "splitQualifiedName ends the qualifier at the leading identifier segments" {
    const cases = .{
        .{ "e.id", "e", "id" },
        .{ "main.t.ID", "main.t", "ID" },
        .{ "s.0.5", "s", "0.5" },
        .{ "s.mul(x, 1.5)", "s", "mul(x, 1.5)" },
        .{ "main.t.add(x, 1.5)", "main", "t.add(x, 1.5)" },
        .{ "d.t.a + 1", "d", "t.a + 1" },
        .{ "d.a.id", "d.a", "id" },
        .{ "e.1", "e", "1" },
        .{ "t.my col", "t", "my col" },
        .{ "my tbl.x", "my tbl", "x" },
    };
    inline for (cases) |c| {
        const split = splitQualifiedName(c[0]).?;
        try std.testing.expectEqualStrings(c[1], split.qualifier);
        try std.testing.expectEqualStrings(c[2], split.bare);
    }
    inline for (.{ "id", "1.5", "-0.0025", "add(x, 1.5)", "UPPER(t.s)", "'a.b'", "payload->'a.b'", ".id", "e." }) |name| {
        try std.testing.expect(splitQualifiedName(name) == null);
        try std.testing.expectEqualStrings(name, unqualifiedName(name));
    }
}

test "splitResultName splits only a qualified identifier" {
    const cases = .{
        .{ "e.id", "e", "id" },
        .{ "main.t.ID", "main.t", "ID" },
        .{ "my tbl.x", "my tbl", "x" },
    };
    inline for (cases) |c| {
        const split = splitResultName(c[0]).?;
        try std.testing.expectEqualStrings(c[1], split.qualifier);
        try std.testing.expectEqualStrings(c[2], split.bare);
    }
    inline for (.{ "id", "0.5", "a.b c", "t.price * t.qty", "e.a IS NULL", "s.mul(x, 1.5)", "s.0.5" }) |name| {
        try std.testing.expect(splitResultName(name) == null);
    }
}

test "findColumn resolves names that contain dots" {
    const derived = [_]Column{
        .{ .name = "0.5", .type = .{ .double = {} } },
        .{ .name = "1.5", .type = .{ .double = {} } },
        .{ .name = "5", .type = .{ .double = {} } },
    };
    try std.testing.expectEqual(@as(?usize, 1), findColumn(&derived, "1.5"));
    try std.testing.expectEqual(@as(?usize, 0), findColumn(&derived, "s.0.5"));
    const aliased = [_]Column{
        .{ .name = "s.0.5", .type = .{ .double = {} } },
        .{ .name = "s.mul(x, 1.5)", .type = .{ .double = {} } },
        .{ .name = "s.x", .type = .{ .double = {} } },
    };
    try std.testing.expectEqual(@as(?usize, 0), findColumn(&aliased, "0.5"));
    try std.testing.expectEqual(@as(?usize, 1), findColumn(&aliased, "mul(x, 1.5)"));
    try std.testing.expectEqual(@as(?usize, 2), findColumn(&aliased, "x"));
    try std.testing.expectEqual(@as(?usize, null), findColumn(&aliased, "5"));
    const text_named = [_]Column{
        .{ .name = "d.t.a + 1", .type = .{ .double = {} } },
        .{ .name = "d.id", .type = .{ .double = {} } },
    };
    try std.testing.expectEqual(@as(?usize, 0), findColumn(&text_named, "t.a + 1"));
    try std.testing.expectEqual(@as(?usize, 0), findColumn(&text_named, "d.t.a + 1"));
    try std.testing.expectEqual(@as(?usize, null), findColumn(&text_named, "e.id"));
}

test "nested type text round-trips, and shapes intern to one instance" {
    const a = std.testing.allocator;
    const map_type = try mapOf(.{ .varchar = 8 }, try arrayOf(.int));
    const texts = .{ "array<array<int>>", "map<varchar(8),array<int>>", "map<int,map<string,decimal(10,2)>>", "struct<a int,`b c` array<struct<x string>>>" };
    inline for (texts) |text| {
        const t = try parseTypeText(text);
        var out: std.ArrayList(u8) = .empty;
        defer out.deinit(a);
        try appendTypeText(a, &out, t);
        try std.testing.expectEqualStrings(text, out.items);
    }
    try std.testing.expect(std.meta.eql(map_type, try parseTypeText("map<varchar(8),array<int>>")));
    try std.testing.expect(!std.meta.eql(map_type, try mapOf(.{ .varchar = 9 }, try arrayOf(.int))));
    try std.testing.expectEqual(Type.int, map_type.mapTypes().?.value.arrayElement().?);
    try std.testing.expect(map_type.arrayElement() == null);
    try std.testing.expectError(error.InvalidTypeText, parseTypeText("map<int>"));

    var name_buf = "a".*;
    const fields = [_]StructField{.{ .name = &name_buf, .type = .int }};
    const struct_type = try structOf(&fields);
    name_buf[0] = 'z';
    try std.testing.expectEqualStrings("a", struct_type.structFields().?[0].name);
    try std.testing.expect(std.meta.eql(struct_type, try parseTypeText("struct<a int>")));
    try std.testing.expect(!std.meta.eql(struct_type, try parseTypeText("struct<b int>")));
    try std.testing.expectEqual(@as(?usize, 0), structFieldIndex(struct_type.structFields().?, "A"));
    try std.testing.expectError(error.InvalidTypeText, parseTypeText("struct<>"));
}
