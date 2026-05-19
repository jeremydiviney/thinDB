//! Per-table schema persistence (<table>/schema.bin).
//!
//! Format (v2, binary, little-endian):
//!
//!   "tDBC" (4)
//!   version u16
//!   flags u16
//!   column_count u32
//!   For each column:
//!     name_len u32, name bytes
//!     type_tag u8
//!     nullable u8     (added v2; 0 = NOT NULL, 1 = nullable)
//!     type_extra u32  (varchar N or 0)
//!   order_key_count u32
//!   For each order_key name:
//!     name_len u32, name bytes
//!   unique u8
//!   "tDBC" (4)

const std = @import("std");
const Io = std.Io;
const Allocator = std.mem.Allocator;

const types = @import("../types.zig");
const Type = types.Type;
const TypeTag = types.TypeTag;
const Column = types.Column;
const TableSchema = types.TableSchema;

const format = @import("format.zig");

pub const schema_magic: [4]u8 = .{ 't', 'D', 'B', 'C' };
pub const schema_version: u16 = 2;
pub const schema_filename = "schema.bin";

pub const Error = error{
    SchemaTooSmall,
    SchemaBadMagic,
    SchemaBadTrailerMagic,
    SchemaUnsupportedVersion,
    SchemaCorrupt,
    SchemaMismatch,
    SchemaRequired,
};

/// Owns the memory backing a Schema. Holds all column names and order key
/// names in an arena; `view()` returns a borrowed Schema that's valid until
/// `deinit()`.
pub const SchemaOwner = struct {
    arena: std.heap.ArenaAllocator,
    columns: []Column,
    order_key: [][]const u8,
    unique: bool,

    pub fn view(self: *const SchemaOwner) TableSchema {
        return .{
            .columns = self.columns,
            .order_key = self.order_key,
            .unique = self.unique,
        };
    }

    pub fn deinit(self: *SchemaOwner) void {
        self.arena.deinit();
        self.* = undefined;
    }

    /// Deep-copy an existing Schema into a fresh arena-owned SchemaOwner.
    pub fn clone(parent_allocator: Allocator, src: TableSchema) !SchemaOwner {
        var arena = std.heap.ArenaAllocator.init(parent_allocator);
        errdefer arena.deinit();
        const aa = arena.allocator();

        const columns = try aa.alloc(Column, src.columns.len);
        for (src.columns, 0..) |c, i| {
            columns[i] = .{
                .name = try aa.dupe(u8, c.name),
                .type = c.type,
                .nullable = c.nullable,
            };
        }

        const order_key = try aa.alloc([]const u8, src.order_key.len);
        for (src.order_key, 0..) |k, i| {
            order_key[i] = try aa.dupe(u8, k);
        }

        return .{
            .arena = arena,
            .columns = columns,
            .order_key = order_key,
            .unique = src.unique,
        };
    }
};

pub fn writeSchema(io: Io, dir: Io.Dir, schema: TableSchema, scratch: Allocator) !void {
    var buf: std.ArrayList(u8) = .empty;
    defer buf.deinit(scratch);

    try buf.appendSlice(scratch, &schema_magic);
    try appendU16(scratch, &buf, schema_version);
    try appendU16(scratch, &buf, 0); // flags

    try appendU32(scratch, &buf, @intCast(schema.columns.len));
    for (schema.columns) |c| {
        try appendU32(scratch, &buf, @intCast(c.name.len));
        try buf.appendSlice(scratch, c.name);
        const tag: u8 = @intFromEnum(@as(TypeTag, c.type));
        try buf.append(scratch, tag);
        try buf.append(scratch, @intFromBool(c.nullable));
        const extra: u32 = switch (c.type) {
            .varchar => |n| n,
            .char => |n| n,
            .decimal64 => |spec| (@as(u32, spec.p) << 8) | @as(u32, spec.s),
            .decimal128 => |spec| (@as(u32, spec.p) << 8) | @as(u32, spec.s),
            else => 0,
        };
        try appendU32(scratch, &buf, extra);
    }

    try appendU32(scratch, &buf, @intCast(schema.order_key.len));
    for (schema.order_key) |k| {
        try appendU32(scratch, &buf, @intCast(k.len));
        try buf.appendSlice(scratch, k);
    }

    try buf.append(scratch, @intFromBool(schema.unique));
    try buf.appendSlice(scratch, &schema_magic);

    try dir.writeFile(io, .{ .sub_path = schema_filename, .data = buf.items });
}

pub fn readSchema(allocator: Allocator, io: Io, dir: Io.Dir) !SchemaOwner {
    const bytes = try dir.readFileAlloc(io, schema_filename, allocator, .unlimited);
    defer allocator.free(bytes);

    if (bytes.len < 4 + 2 + 2 + 4 + 4 + 1 + 4) return Error.SchemaTooSmall;
    if (!std.mem.eql(u8, bytes[0..4], &schema_magic)) return Error.SchemaBadMagic;

    const version = format.readU16(bytes[4..6]);
    if (version != schema_version) return Error.SchemaUnsupportedVersion;

    var arena = std.heap.ArenaAllocator.init(allocator);
    errdefer arena.deinit();
    const aa = arena.allocator();

    var cursor: usize = 8;
    if (cursor + 4 > bytes.len) return Error.SchemaCorrupt;
    const col_count = format.readU32(bytes[cursor .. cursor + 4]);
    cursor += 4;

    const columns = try aa.alloc(Column, col_count);
    var i: u32 = 0;
    while (i < col_count) : (i += 1) {
        if (cursor + 4 > bytes.len) return Error.SchemaCorrupt;
        const name_len = format.readU32(bytes[cursor .. cursor + 4]);
        cursor += 4;
        if (cursor + name_len > bytes.len) return Error.SchemaCorrupt;
        const name = try aa.dupe(u8, bytes[cursor .. cursor + name_len]);
        cursor += name_len;

        if (cursor + 1 + 1 + 4 > bytes.len) return Error.SchemaCorrupt;
        const tag_byte = bytes[cursor];
        cursor += 1;
        if (tag_byte < 1 or tag_byte > 16) return Error.SchemaCorrupt;
        const tag: TypeTag = @enumFromInt(tag_byte);
        const nullable = bytes[cursor] != 0;
        cursor += 1;
        const extra = format.readU32(bytes[cursor .. cursor + 4]);
        cursor += 4;

        const t: Type = switch (tag) {
            .int => .int,
            .bigint => .bigint,
            .boolean => .boolean,
            .varchar => .{ .varchar = extra },
            .string => .string,
            .float => .float,
            .double => .double,
            .date => .date,
            .datetime => .datetime,
            .tinyint => .tinyint,
            .smallint => .smallint,
            .largeint => .largeint,
            .char => .{ .char = extra },
            // Decimal extra: high byte = precision, low byte = scale.
            .decimal64 => .{ .decimal64 = .{ .p = @intCast((extra >> 8) & 0xff), .s = @intCast(extra & 0xff) } },
            .decimal128 => .{ .decimal128 = .{ .p = @intCast((extra >> 8) & 0xff), .s = @intCast(extra & 0xff) } },
            .uuid => .uuid,
        };
        columns[i] = .{ .name = name, .type = t, .nullable = nullable };
    }

    if (cursor + 4 > bytes.len) return Error.SchemaCorrupt;
    const ok_count = format.readU32(bytes[cursor .. cursor + 4]);
    cursor += 4;

    const order_key = try aa.alloc([]const u8, ok_count);
    var j: u32 = 0;
    while (j < ok_count) : (j += 1) {
        if (cursor + 4 > bytes.len) return Error.SchemaCorrupt;
        const name_len = format.readU32(bytes[cursor .. cursor + 4]);
        cursor += 4;
        if (cursor + name_len > bytes.len) return Error.SchemaCorrupt;
        order_key[j] = try aa.dupe(u8, bytes[cursor .. cursor + name_len]);
        cursor += name_len;
    }

    if (cursor + 1 + 4 > bytes.len) return Error.SchemaCorrupt;
    const unique = bytes[cursor] != 0;
    cursor += 1;
    if (!std.mem.eql(u8, bytes[cursor .. cursor + 4], &schema_magic)) {
        return Error.SchemaBadTrailerMagic;
    }

    return .{
        .arena = arena,
        .columns = columns,
        .order_key = order_key,
        .unique = unique,
    };
}

/// True iff two schemas are structurally identical: same column count, same
/// column name+type at each position, same order_key, same unique flag.
pub fn schemasEqual(a: TableSchema, b: TableSchema) bool {
    if (a.columns.len != b.columns.len) return false;
    if (a.order_key.len != b.order_key.len) return false;
    if (a.unique != b.unique) return false;

    for (a.columns, b.columns) |ac, bc| {
        if (!std.mem.eql(u8, ac.name, bc.name)) return false;
        if (std.meta.activeTag(ac.type) != std.meta.activeTag(bc.type)) return false;
        if (ac.nullable != bc.nullable) return false;
        switch (ac.type) {
            .varchar => |n| if (n != bc.type.varchar) return false,
            .char => |n| if (n != bc.type.char) return false,
            .decimal64 => |spec| if (spec.p != bc.type.decimal64.p or spec.s != bc.type.decimal64.s) return false,
            .decimal128 => |spec| if (spec.p != bc.type.decimal128.p or spec.s != bc.type.decimal128.s) return false,
            else => {},
        }
    }
    for (a.order_key, b.order_key) |ak, bk| {
        if (!std.mem.eql(u8, ak, bk)) return false;
    }
    return true;
}

// ---------- helpers ------------------------------------------------------

const appendU16 = format.appendU16;
const appendU32 = format.appendU32;

// ---------- tests --------------------------------------------------------

test "round-trip schema (simple)" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const schema = TableSchema{
        .columns = &.{
            .{ .name = "id", .type = .bigint },
            .{ .name = "qty", .type = .int },
            .{ .name = "active", .type = .boolean },
            .{ .name = "tag", .type = .{ .varchar = 32 } },
            .{ .name = "note", .type = .string },
        },
        .order_key = &.{"id"},
        .unique = true,
    };

    try writeSchema(io, tmp.dir, schema, allocator);

    var owner = try readSchema(allocator, io, tmp.dir);
    defer owner.deinit();

    try std.testing.expect(schemasEqual(schema, owner.view()));
}

test "round-trip schema (composite order key)" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const schema = TableSchema{
        .columns = &.{
            .{ .name = "user_id", .type = .bigint },
            .{ .name = "ts", .type = .bigint },
            .{ .name = "event", .type = .string },
        },
        .order_key = &.{ "user_id", "ts" },
        .unique = false,
    };

    try writeSchema(io, tmp.dir, schema, allocator);

    var owner = try readSchema(allocator, io, tmp.dir);
    defer owner.deinit();

    try std.testing.expect(schemasEqual(schema, owner.view()));
    try std.testing.expectEqualStrings("user_id", owner.view().order_key[0]);
    try std.testing.expectEqualStrings("ts", owner.view().order_key[1]);
}

test "schemasEqual detects type mismatch" {
    const a = TableSchema{
        .columns = &.{.{ .name = "id", .type = .bigint }},
        .order_key = &.{"id"},
        .unique = false,
    };
    const b = TableSchema{
        .columns = &.{.{ .name = "id", .type = .int }},
        .order_key = &.{"id"},
        .unique = false,
    };
    try std.testing.expect(!schemasEqual(a, b));
}

test "SchemaOwner.clone deep-copies into a fresh arena" {
    const allocator = std.testing.allocator;
    const src = TableSchema{
        .columns = &.{
            .{ .name = "id", .type = .bigint },
            .{ .name = "tag", .type = .string },
        },
        .order_key = &.{"id"},
        .unique = true,
    };
    var owner = try SchemaOwner.clone(allocator, src);
    defer owner.deinit();

    try std.testing.expect(schemasEqual(src, owner.view()));
    try std.testing.expectEqualStrings("id", owner.view().columns[0].name);
}
