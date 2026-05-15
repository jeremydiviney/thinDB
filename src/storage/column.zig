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
};

pub const ValueView = union(TypeTag) {
    int: []const i32,
    bigint: []const i64,
    boolean: []const u8,
    varchar: StringView,
    string: StringView,

    pub fn rowCount(self: ValueView) usize {
        return switch (self) {
            .int => |s| s.len,
            .bigint => |s| s.len,
            .boolean => |s| s.len,
            .varchar => |s| s.rowCount(),
            .string => |s| s.rowCount(),
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

    pub fn rowCount(self: OwnedData) usize {
        return switch (self) {
            .int => |s| s.len,
            .bigint => |s| s.len,
            .boolean => |s| s.len,
            .varchar => |s| s.offsets.len - 1,
            .string => |s| s.offsets.len - 1,
        };
    }

    pub fn view(self: OwnedData) ValueView {
        return switch (self) {
            .int => |s| .{ .int = s },
            .bigint => |s| .{ .bigint = s },
            .boolean => |s| .{ .boolean = s },
            .varchar => |s| .{ .varchar = s.view() },
            .string => |s| .{ .string = s.view() },
        };
    }

    pub fn deinit(self: *OwnedData, allocator: std.mem.Allocator) void {
        switch (self.*) {
            .int => |s| allocator.free(s),
            .bigint => |s| allocator.free(s),
            .boolean => |s| allocator.free(s),
            .varchar => |*s| s.deinit(allocator),
            .string => |*s| s.deinit(allocator),
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
