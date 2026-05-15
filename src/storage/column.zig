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

pub const ColumnView = union(TypeTag) {
    int: []const i32,
    bigint: []const i64,
    boolean: []const u8,
    varchar: StringView,
    string: StringView,

    pub fn rowCount(self: ColumnView) usize {
        return switch (self) {
            .int => |s| s.len,
            .bigint => |s| s.len,
            .boolean => |s| s.len,
            .varchar => |s| s.rowCount(),
            .string => |s| s.rowCount(),
        };
    }
};

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

pub const OwnedColumn = union(TypeTag) {
    int: []i32,
    bigint: []i64,
    boolean: []u8,
    varchar: OwnedStringColumn,
    string: OwnedStringColumn,

    pub fn rowCount(self: OwnedColumn) usize {
        return switch (self) {
            .int => |s| s.len,
            .bigint => |s| s.len,
            .boolean => |s| s.len,
            .varchar => |s| s.offsets.len - 1,
            .string => |s| s.offsets.len - 1,
        };
    }

    pub fn view(self: OwnedColumn) ColumnView {
        return switch (self) {
            .int => |s| .{ .int = s },
            .bigint => |s| .{ .bigint = s },
            .boolean => |s| .{ .boolean = s },
            .varchar => |s| .{ .varchar = s.view() },
            .string => |s| .{ .string = s.view() },
        };
    }

    pub fn deinit(self: *OwnedColumn, allocator: std.mem.Allocator) void {
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
    try std.testing.expectEqual(@as(usize, 3), (ColumnView{ .int = &ints }).rowCount());

    const bigs = [_]i64{ 10, 20 };
    try std.testing.expectEqual(@as(usize, 2), (ColumnView{ .bigint = &bigs }).rowCount());

    const bools = [_]u8{ 1, 0, 1, 1 };
    try std.testing.expectEqual(@as(usize, 4), (ColumnView{ .boolean = &bools }).rowCount());

    const offsets = [_]u32{ 0, 5, 10 };
    const text_bytes = "helloworld";
    const sv = StringView{ .offsets = &offsets, .bytes = text_bytes };
    try std.testing.expectEqual(@as(usize, 2), (ColumnView{ .string = sv }).rowCount());
}
