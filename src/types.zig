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
};

/// Precision/scale carried by both decimal type variants.
pub const DecimalSpec = struct {
    /// Total number of significant digits. Range [1, 38].
    p: u8,
    /// Digits after the decimal point. Range [0, p].
    s: u8,
};

/// Build a `Type` for a DECIMAL(p, s). Picks the smallest backing that
/// holds `p` digits.
pub fn decimal(comptime p: u8, comptime s: u8) Type {
    comptime {
        if (p == 0 or p > 38) @compileError("DECIMAL precision must be in 1..=38");
        if (s > p) @compileError("DECIMAL scale must be <= precision");
    }
    return if (p <= 18)
        .{ .decimal64 = .{ .p = p, .s = s } }
    else
        .{ .decimal128 = .{ .p = p, .s = s } };
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
            .uuid => @sizeOf(u128),
            .varchar, .string, .char => null,
        };
    }

    pub fn isString(self: Type) bool {
        return switch (self) {
            .varchar, .string, .char => true,
            else => false,
        };
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
            .decimal64, .decimal128 => true,
            else => false,
        };
    }

    /// Returns the (p, s) of a decimal column, or null if not a decimal.
    pub fn decimalSpec(self: Type) ?DecimalSpec {
        return switch (self) {
            .decimal64 => |spec| spec,
            .decimal128 => |spec| spec,
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
            .uuid => T == Uuid or T == u128 or T == comptime_int,
            .varchar, .string, .char => isStringLikeType(T),
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

/// Logical wrapper around a stored `u128` UUID. Mirrors `Date`/`DateTime`
/// — gives `@as(Uuid, ...)` ergonomics for insert without colliding with
/// LARGEINT columns. `.value` accesses the raw u128. UUIDs are
/// big-endian when serialized to the canonical 8-4-4-4-12 hex form, but
/// stored as u128 in native byte order — the formatter converts.
pub const Uuid = enum(u128) {
    _,
    pub fn fromU128(v: u128) Uuid {
        return @enumFromInt(v);
    }
    pub fn value(self: Uuid) u128 {
        return @intFromEnum(self);
    }
};

/// Logical wrapper around a stored `i32` of days since the epoch. Provided
/// so insertion can be unambiguous (`@as(Date, ...)`) without colliding
/// with INT columns. `.value` accesses the raw days count.
pub const Date = enum(i32) {
    _,
    pub fn fromDays(d: i32) Date {
        return @enumFromInt(d);
    }
    pub fn days(self: Date) i32 {
        return @intFromEnum(self);
    }
};

/// Logical wrapper around a stored `i64` of microseconds since the epoch.
pub const DateTime = enum(i64) {
    _,
    pub fn fromMicros(us: i64) DateTime {
        return @enumFromInt(us);
    }
    pub fn micros(self: DateTime) i64 {
        return @intFromEnum(self);
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

    pub fn fromType(t: Type) ValueTag {
        return switch (t) {
            .int => .int,
            .bigint => .bigint,
            .boolean => .boolean,
            .varchar, .string, .char => .text,
            .float => .float,
            .double => .double,
            .date => .date,
            .datetime => .datetime,
            .tinyint => .tinyint,
            .smallint => .smallint,
            .largeint => .largeint,
            .decimal64 => .decimal64,
            .decimal128 => .decimal128,
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

    pub fn compare(self: Value, other: Value) std.math.Order {
        std.debug.assert(std.meta.activeTag(self) == std.meta.activeTag(other));
        return switch (self) {
            .int => |a| std.math.order(a, other.int),
            .bigint => |a| std.math.order(a, other.bigint),
            .boolean => |a| std.math.order(@as(u8, @intFromBool(a)), @as(u8, @intFromBool(other.boolean))),
            .float => |a| std.math.order(a, other.float),
            .double => |a| std.math.order(a, other.double),
            .date => |a| std.math.order(a, other.date),
            .datetime => |a| std.math.order(a, other.datetime),
            .tinyint => |a| std.math.order(a, other.tinyint),
            .smallint => |a| std.math.order(a, other.smallint),
            .largeint => |a| std.math.order(a, other.largeint),
            .decimal64 => |a| std.math.order(a, other.decimal64),
            .decimal128 => |a| std.math.order(a, other.decimal128),
            .uuid => |a| std.math.order(a, other.uuid),
            .text => |a| switch (std.mem.order(u8, a, other.text)) {
                .lt => .lt,
                .gt => .gt,
                .eq => .eq,
            },
        };
    }
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
    /// LZ4HC on every block (decode ~4 GB/s). Large raw string blocks
    /// additionally stay compressed IN CACHE and decompress per access into
    /// recycled scratch — a much smaller resident set for string-heavy data.
    lz4 = 2,
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

        for (self.order_key) |key| {
            if (self.columnIndex(key) == null) return TableSchemaError.OrderKeyColumnMissing;
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

/// Resolve a column reference (possibly qualified `alias.col`) against a
/// flat column slice. Used by every user-facing lookup site so the
/// parser can leave qualifiers intact without each operator carrying
/// its own match logic.
///
/// Match order:
///   1. Exact match on the full string.
///   2. If `name` contains a `.`, retry exact match against the suffix
///      after the last dot. Lets bare-table users keep writing `t.col`
///      against an unaliased scan whose schema only has `col`.
///   3. Else (no dot in `name`) search for any column whose name ends
///      in `.name` — accepted only when exactly one column matches.
///      Lets bare `col` resolve against an aliased schema (`a.col`).
pub fn findColumn(columns: []const Column, name: []const u8) ?usize {
    for (columns, 0..) |c, i| {
        if (columnNameEql(c.name, name)) return i;
    }
    if (std.mem.lastIndexOfScalar(u8, name, '.')) |dot| {
        const tail = name[dot + 1 ..];
        for (columns, 0..) |c, i| {
            if (columnNameEql(c.name, tail)) return i;
        }
        return null;
    }
    var match: ?usize = null;
    for (columns, 0..) |c, i| {
        const d = std.mem.lastIndexOfScalar(u8, c.name, '.') orelse continue;
        if (columnNameEql(c.name[d + 1 ..], name)) {
            if (match != null) return null; // ambiguous
            match = i;
        }
    }
    return match;
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

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
