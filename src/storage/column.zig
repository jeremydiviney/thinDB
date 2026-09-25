//! ColumnView — type-tagged view over a column's raw data. Used as input
//! to the segment writer and (owned variant) as output from the reader.

const std = @import("std");
const types = @import("../types.zig");
const TypeTag = types.TypeTag;

pub const StringView = struct {
    /// offsets[i] is the byte position of row i's first byte.
    /// offsets[row_count] is the total byte length.
    /// Length: row_count + 1.
    offsets: []const u32,
    bytes: []const u8,

    pub fn rowCount(self: StringView) usize {
        std.debug.assert(self.offsets.len > 0);
        return self.offsets.len - 1;
    }

    pub fn rowBytes(self: StringView, row: usize) []const u8 {
        const start = self.offsets[row];
        const end = self.offsets[row + 1];
        return self.bytes[start..end];
    }
};

/// Borrowed view of a single column's data. May optionally carry a validity
/// bitmap (`nulls`) for nullable columns. Bitmap convention: bit set (1) =
/// valid (has value); bit clear (0) = NULL. `nulls == null` means the column
/// is not nullable — treat every row as valid.
pub const ColumnView = struct {
    data: ValueView,
    nulls: ?[]const u8 = null,

    pub fn rowCount(self: ColumnView) usize {
        return self.data.rowCount();
    }

    /// True if row `row` is non-null. Returns `true` whenever `nulls` is
    /// `null` (column not declared nullable).
    pub fn isValid(self: ColumnView, row: usize) bool {
        return isValidBit(self.nulls, row);
    }

    /// True when each of the first `rows` rows is NULL. Such a column carries
    /// no type of its own (a bare `NULL` literal reaches the engine with a
    /// placeholder type), so a store of any type may take it as NULLs.
    pub fn allNull(self: ColumnView, rows: usize) bool {
        const bm = self.nulls orelse return false;
        for (0..rows) |row| if (isValidBit(bm, row)) return false;
        return true;
    }

    /// True when any of the first `rows` rows is NULL.
    pub fn anyNull(self: ColumnView, rows: usize) bool {
        const bm = self.nulls orelse return false;
        for (0..rows) |row| if (!isValidBit(bm, row)) return true;
        return false;
    }

    /// Append row `row`'s value as self-delimiting bytes: equal values give
    /// equal bytes, so a concatenation over columns keys a row or a tuple.
    pub fn appendValueBytes(self: ColumnView, allocator: std.mem.Allocator, buf: *std.ArrayList(u8), row: u32) !void {
        switch (self.data) {
            .int, .date => |s| try appendLittle(i32, allocator, buf, s[row]),
            .bigint, .datetime, .decimal64 => |s| try appendLittle(i64, allocator, buf, s[row]),
            .boolean => |s| try buf.append(allocator, s[row]),
            .varchar, .string, .char, .json => |sv| {
                const bytes = sv.rowBytes(row);
                try appendLittle(u32, allocator, buf, @intCast(bytes.len));
                try buf.appendSlice(allocator, bytes);
            },
            .float => |s| try appendLittle(u32, allocator, buf, types.canonicalFloatBits(s[row])),
            .double => |s| try appendLittle(u64, allocator, buf, types.canonicalFloatBits(s[row])),
            .tinyint => |s| try buf.append(allocator, @bitCast(s[row])),
            .smallint => |s| try appendLittle(i16, allocator, buf, s[row]),
            .largeint, .decimal128 => |s| try appendLittle(i128, allocator, buf, s[row]),
            .uuid => |s| try appendLittle(u128, allocator, buf, s[row]),
        }
    }
};

fn appendLittle(comptime T: type, allocator: std.mem.Allocator, buf: *std.ArrayList(u8), value: T) !void {
    var bytes: [@sizeOf(T)]u8 = undefined;
    std.mem.writeInt(T, &bytes, value, .little);
    try buf.appendSlice(allocator, &bytes);
}

pub const ValueView = union(TypeTag) {
    int: []const i32,
    bigint: []const i64,
    boolean: []const u8,
    varchar: StringView,
    string: StringView,
    float: []const f32,
    double: []const f64,
    date: []const i32,
    datetime: []const i64,
    tinyint: []const i8,
    smallint: []const i16,
    largeint: []const i128,
    char: StringView,
    decimal64: []const i64,
    decimal128: []const i128,
    uuid: []const u128,
    json: StringView,

    pub fn rowCount(self: ValueView) usize {
        return switch (self) {
            .int => |s| s.len,
            .bigint => |s| s.len,
            .boolean => |s| s.len,
            .varchar => |s| s.rowCount(),
            .string => |s| s.rowCount(),
            .float => |s| s.len,
            .double => |s| s.len,
            .date => |s| s.len,
            .datetime => |s| s.len,
            .tinyint => |s| s.len,
            .smallint => |s| s.len,
            .largeint => |s| s.len,
            .char => |s| s.rowCount(),
            .decimal64 => |s| s.len,
            .decimal128 => |s| s.len,
            .uuid => |s| s.len,
            .json => |s| s.rowCount(),
        };
    }
};

/// Validity-bit lookup. `bitmap == null` always returns `true` (column is
/// not nullable; every row is implicitly valid).
pub inline fn isValidBit(bitmap: ?[]const u8, row: usize) bool {
    const bm = bitmap orelse return true;
    return (bm[row >> 3] & (@as(u8, 1) << @intCast(row & 7))) != 0;
}

pub inline fn setValidBit(bitmap: []u8, row: usize, valid: bool) void {
    const byte_idx = row >> 3;
    const bit: u3 = @intCast(row & 7);
    if (valid) {
        bitmap[byte_idx] |= (@as(u8, 1) << bit);
    } else {
        bitmap[byte_idx] &= ~(@as(u8, 1) << bit);
    }
}

/// Required bitmap byte length for a column of `row_count` rows.
pub inline fn bitmapBytes(row_count: usize) usize {
    return (row_count + 7) / 8;
}

/// Owned counterpart of ColumnView — allocated on read, freed via deinit.
pub const OwnedStringColumn = struct {
    offsets: []u32,
    bytes: []u8,

    pub fn deinit(self: *OwnedStringColumn, allocator: std.mem.Allocator) void {
        allocator.free(self.offsets);
        allocator.free(self.bytes);
        self.* = undefined;
    }

    pub fn view(self: OwnedStringColumn) StringView {
        return .{ .offsets = self.offsets, .bytes = self.bytes };
    }
};

/// Owned column data; mirrors `ColumnView` in shape but holds heap-allocated
/// buffers. `nulls` is null when the column is not nullable.
pub const OwnedColumn = struct {
    data: OwnedData,
    nulls: ?[]u8 = null,

    pub fn rowCount(self: OwnedColumn) usize {
        return self.data.rowCount();
    }

    pub fn view(self: OwnedColumn) ColumnView {
        return .{ .data = self.data.view(), .nulls = self.nulls };
    }

    pub fn deinit(self: *OwnedColumn, allocator: std.mem.Allocator) void {
        self.data.deinit(allocator);
        if (self.nulls) |n| allocator.free(n);
        self.* = undefined;
    }
};

pub const OwnedData = union(TypeTag) {
    int: []i32,
    bigint: []i64,
    boolean: []u8,
    varchar: OwnedStringColumn,
    string: OwnedStringColumn,
    float: []f32,
    double: []f64,
    date: []i32,
    datetime: []i64,
    tinyint: []i8,
    smallint: []i16,
    largeint: []i128,
    char: OwnedStringColumn,
    decimal64: []i64,
    decimal128: []i128,
    uuid: []u128,
    json: OwnedStringColumn,

    pub fn rowCount(self: OwnedData) usize {
        return switch (self) {
            .int => |s| s.len,
            .bigint => |s| s.len,
            .boolean => |s| s.len,
            .varchar => |s| s.offsets.len - 1,
            .string => |s| s.offsets.len - 1,
            .float => |s| s.len,
            .double => |s| s.len,
            .date => |s| s.len,
            .datetime => |s| s.len,
            .tinyint => |s| s.len,
            .smallint => |s| s.len,
            .largeint => |s| s.len,
            .char => |s| s.offsets.len - 1,
            .decimal64 => |s| s.len,
            .decimal128 => |s| s.len,
            .uuid => |s| s.len,
            .json => |s| s.offsets.len - 1,
        };
    }

    pub fn view(self: OwnedData) ValueView {
        return switch (self) {
            .int => |s| .{ .int = s },
            .bigint => |s| .{ .bigint = s },
            .boolean => |s| .{ .boolean = s },
            .varchar => |s| .{ .varchar = s.view() },
            .string => |s| .{ .string = s.view() },
            .float => |s| .{ .float = s },
            .double => |s| .{ .double = s },
            .date => |s| .{ .date = s },
            .datetime => |s| .{ .datetime = s },
            .tinyint => |s| .{ .tinyint = s },
            .smallint => |s| .{ .smallint = s },
            .largeint => |s| .{ .largeint = s },
            .char => |s| .{ .char = s.view() },
            .decimal64 => |s| .{ .decimal64 = s },
            .decimal128 => |s| .{ .decimal128 = s },
            .uuid => |s| .{ .uuid = s },
            .json => |s| .{ .json = s.view() },
        };
    }

    pub fn deinit(self: *OwnedData, allocator: std.mem.Allocator) void {
        switch (self.*) {
            .int => |s| allocator.free(s),
            .bigint => |s| allocator.free(s),
            .boolean => |s| allocator.free(s),
            .varchar => |*s| s.deinit(allocator),
            .string => |*s| s.deinit(allocator),
            .float => |s| allocator.free(s),
            .double => |s| allocator.free(s),
            .date => |s| allocator.free(s),
            .datetime => |s| allocator.free(s),
            .tinyint => |s| allocator.free(s),
            .smallint => |s| allocator.free(s),
            .largeint => |s| allocator.free(s),
            .char => |*s| s.deinit(allocator),
            .decimal64 => |s| allocator.free(s),
            .decimal128 => |s| allocator.free(s),
            .uuid => |s| allocator.free(s),
            .json => |*s| s.deinit(allocator),
        }
        self.* = undefined;
    }
};

test "StringView returns row slices" {
    const offsets = [_]u32{ 0, 3, 8, 8, 13 };
    const bytes = "foobarba" ++ "z!quu";
    const v = StringView{ .offsets = &offsets, .bytes = bytes };
    try std.testing.expectEqual(@as(usize, 4), v.rowCount());
    try std.testing.expectEqualStrings("foo", v.rowBytes(0));
    try std.testing.expectEqualStrings("barba", v.rowBytes(1));
    try std.testing.expectEqualStrings("", v.rowBytes(2));
    try std.testing.expectEqualStrings("z!quu", v.rowBytes(3));
}

test "ColumnView.rowCount across variants" {
    const ints = [_]i32{ 1, 2, 3 };
    try std.testing.expectEqual(@as(usize, 3), (ColumnView{ .data = .{ .int = &ints } }).rowCount());

    const bigs = [_]i64{ 10, 20 };
    try std.testing.expectEqual(@as(usize, 2), (ColumnView{ .data = .{ .bigint = &bigs } }).rowCount());

    const bools = [_]u8{ 1, 0, 1, 1 };
    try std.testing.expectEqual(@as(usize, 4), (ColumnView{ .data = .{ .boolean = &bools } }).rowCount());

    const offsets = [_]u32{ 0, 5, 10 };
    const text_bytes = "helloworld";
    const sv = StringView{ .offsets = &offsets, .bytes = text_bytes };
    try std.testing.expectEqual(@as(usize, 2), (ColumnView{ .data = .{ .string = sv } }).rowCount());
}

test "isValidBit reads the validity bitmap" {
    const bm = [_]u8{ 0b1010_1101, 0b0000_0001 };
    // bit 0 = 1 → valid; bit 1 = 0 → null; bit 2 = 1; bit 3 = 1; bit 4 = 0; ...
    try std.testing.expect(isValidBit(&bm, 0));
    try std.testing.expect(!isValidBit(&bm, 1));
    try std.testing.expect(isValidBit(&bm, 2));
    try std.testing.expect(isValidBit(&bm, 3));
    try std.testing.expect(!isValidBit(&bm, 4));
    // Byte 1, bit 0 → valid.
    try std.testing.expect(isValidBit(&bm, 8));
    // nulls == null → always valid.
    try std.testing.expect(isValidBit(null, 999));
}
