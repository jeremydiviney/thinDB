//! Type system for v0.1: INT, BIGINT, BOOLEAN, VARCHAR(N), STRING.
//! Type is the schema-side description; Value is the runtime tagged datum.

const std = @import("std");

pub const TypeTag = enum(u8) {
    int = 1,
    bigint = 2,
    boolean = 3,
    varchar = 4,
    string = 5,
    float = 6,
    double = 7,
};

pub const Type = union(TypeTag) {
    int,
    bigint,
    boolean,
    varchar: u32, // declared max length (not enforced in v0.1)
    string,
    float,
    double,

    pub fn fixedSize(self: Type) ?usize {
        return switch (self) {
            .int => @sizeOf(i32),
            .bigint => @sizeOf(i64),
            .boolean => @sizeOf(u8),
            .float => @sizeOf(f32),
            .double => @sizeOf(f64),
            .varchar, .string => null,
        };
    }

    pub fn isString(self: Type) bool {
        return switch (self) {
            .varchar, .string => true,
            else => false,
        };
    }

    pub fn isFloat(self: Type) bool {
        return switch (self) {
            .float, .double => true,
            else => false,
        };
    }

    pub fn matchesZigType(self: Type, comptime T: type) bool {
        return switch (self) {
            .int => T == i32 or T == comptime_int,
            .bigint => T == i64 or T == comptime_int,
            .boolean => T == bool,
            .float => T == f32 or T == comptime_float,
            .double => T == f64 or T == comptime_float,
            .varchar, .string => isStringLikeType(T),
        };
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
    text = 4, // covers both VARCHAR and STRING
    float = 5,
    double = 6,

    pub fn fromType(t: Type) ValueTag {
        return switch (t) {
            .int => .int,
            .bigint => .bigint,
            .boolean => .boolean,
            .varchar, .string => .text,
            .float => .float,
            .double => .double,
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

    pub fn compare(self: Value, other: Value) std.math.Order {
        std.debug.assert(std.meta.activeTag(self) == std.meta.activeTag(other));
        return switch (self) {
            .int => |a| std.math.order(a, other.int),
            .bigint => |a| std.math.order(a, other.bigint),
            .boolean => |a| std.math.order(@as(u8, @intFromBool(a)), @as(u8, @intFromBool(other.boolean))),
            .float => |a| std.math.order(a, other.float),
            .double => |a| std.math.order(a, other.double),
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
};

pub const SchemaError = error{
    DuplicateColumn,
    OrderKeyColumnMissing,
    EmptyColumns,
    EmptyOrderKey,
};

pub const Schema = struct {
    columns: []const Column,
    order_key: []const []const u8,
    unique: bool,

    /// Validate basic invariants. Does not allocate.
    pub fn validate(self: Schema) SchemaError!void {
        if (self.columns.len == 0) return SchemaError.EmptyColumns;
        if (self.order_key.len == 0) return SchemaError.EmptyOrderKey;

        for (self.columns, 0..) |c, i| {
            for (self.columns[0..i]) |prior| {
                if (std.mem.eql(u8, c.name, prior.name)) return SchemaError.DuplicateColumn;
            }
        }

        for (self.order_key) |key| {
            if (self.columnIndex(key) == null) return SchemaError.OrderKeyColumnMissing;
        }
    }

    pub fn columnIndex(self: Schema, name: []const u8) ?usize {
        for (self.columns, 0..) |c, i| {
            if (std.mem.eql(u8, c.name, name)) return i;
        }
        return null;
    }

    pub fn column(self: Schema, name: []const u8) ?Column {
        const idx = self.columnIndex(name) orelse return null;
        return self.columns[idx];
    }
};

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

test "Schema.validate accepts a well-formed schema" {
    const schema = Schema{
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

test "Schema.validate rejects duplicate columns" {
    const schema = Schema{
        .columns = &.{
            .{ .name = "id", .type = .bigint },
            .{ .name = "id", .type = .int },
        },
        .order_key = &.{"id"},
        .unique = false,
    };
    try std.testing.expectError(SchemaError.DuplicateColumn, schema.validate());
}

test "Schema.validate rejects order key that isn't a column" {
    const schema = Schema{
        .columns = &.{.{ .name = "id", .type = .bigint }},
        .order_key = &.{"missing"},
        .unique = false,
    };
    try std.testing.expectError(SchemaError.OrderKeyColumnMissing, schema.validate());
}

test "Schema.validate rejects empty columns" {
    const schema = Schema{
        .columns = &.{},
        .order_key = &.{"id"},
        .unique = false,
    };
    try std.testing.expectError(SchemaError.EmptyColumns, schema.validate());
}
