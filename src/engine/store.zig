//! Memtable backing storage: per-column buffers (StringStore, DataStore,
//! ColumnStore) and the validity-bit machinery. Decoupled from Memtable
//! itself so transform helpers and the memtable can evolve independently.

const std = @import("std");
const Allocator = std.mem.Allocator;

const types = @import("../types.zig");
const Type = types.Type;
const TypeTag = types.TypeTag;

const storage = @import("../storage/storage.zig");
const ColumnView = storage.ColumnView;
const ValueView = storage.column.ValueView;
const StringView = storage.StringView;

/// Variable-width string column buffer. `offsets` is invariant length
/// `row_count + 1` with `offsets[0] == 0` and `offsets[row_count]` = total bytes.
pub const StringStore = struct {
    offsets: std.ArrayList(u32),
    bytes: std.ArrayList(u8),

    pub fn init(allocator: Allocator) Allocator.Error!StringStore {
        return initCapacity(allocator, 0, 0);
    }

    /// Initialize with pre-reserved capacity. Required for snapshot isolation:
    /// once a reader pins a slice into `offsets`/`bytes`, an `append` that
    /// triggers realloc would invalidate the reader's pointer. By reserving
    /// enough capacity up-front, appends never realloc.
    pub fn initCapacity(allocator: Allocator, rows_cap: usize, bytes_cap: usize) Allocator.Error!StringStore {
        var offsets: std.ArrayList(u32) = .empty;
        errdefer offsets.deinit(allocator);
        try offsets.ensureTotalCapacity(allocator, rows_cap + 1);
        try offsets.append(allocator, 0);
        var bytes: std.ArrayList(u8) = .empty;
        errdefer bytes.deinit(allocator);
        try bytes.ensureTotalCapacity(allocator, bytes_cap);
        return .{ .offsets = offsets, .bytes = bytes };
    }

    pub fn deinit(self: *StringStore, allocator: Allocator) void {
        self.offsets.deinit(allocator);
        self.bytes.deinit(allocator);
        self.* = undefined;
    }

    pub fn appendValue(self: *StringStore, allocator: Allocator, slice: []const u8) !void {
        try self.bytes.appendSlice(allocator, slice);
        try self.offsets.append(allocator, @intCast(self.bytes.items.len));
    }

    pub fn rowCount(self: StringStore) usize {
        return self.offsets.items.len - 1;
    }

    pub fn view(self: StringStore) StringView {
        return .{ .offsets = self.offsets.items, .bytes = self.bytes.items };
    }

    pub fn clear(self: *StringStore) void {
        self.offsets.clearRetainingCapacity();
        self.bytes.clearRetainingCapacity();
        self.offsets.appendAssumeCapacity(0);
    }
};

pub const ColumnStore = struct {
    data: DataStore,
    /// Validity bitmap (1 = valid, 0 = null). Present iff the column is
    /// nullable. Grown alongside the data so `data.rowCount()` rows always
    /// have a bit available.
    nulls: ?std.ArrayList(u8) = null,

    pub fn init(allocator: Allocator, t: Type, nullable: bool) Allocator.Error!ColumnStore {
        return initCapacity(allocator, t, nullable, 0, 0);
    }

    pub fn initCapacity(
        allocator: Allocator,
        t: Type,
        nullable: bool,
        rows_cap: usize,
        bytes_cap: usize,
    ) Allocator.Error!ColumnStore {
        var nulls_opt: ?std.ArrayList(u8) = if (nullable) blk: {
            var nb: std.ArrayList(u8) = .empty;
            errdefer nb.deinit(allocator);
            try nb.ensureTotalCapacity(allocator, (rows_cap + 7) >> 3);
            break :blk nb;
        } else null;
        errdefer if (nulls_opt) |*n| n.deinit(allocator);
        return .{
            .data = try DataStore.initCapacity(allocator, t, rows_cap, bytes_cap),
            .nulls = nulls_opt,
        };
    }

    pub fn deinit(self: *ColumnStore, allocator: Allocator) void {
        self.data.deinit(allocator);
        if (self.nulls) |*n| n.deinit(allocator);
        self.* = undefined;
    }

    pub fn rowCount(self: ColumnStore) usize {
        return self.data.rowCount();
    }

    pub fn view(self: ColumnStore) ColumnView {
        return .{
            .data = self.data.view(),
            .nulls = if (self.nulls) |n| n.items else null,
        };
    }

    pub fn clear(self: *ColumnStore) void {
        self.data.clear();
        if (self.nulls) |*n| n.clearRetainingCapacity();
    }

    /// Append a single validity bit for the row at index `row` (= current row
    /// count BEFORE this call's data append). Grows the bitmap byte by byte
    /// as needed. No-op on non-nullable columns.
    pub fn appendValidBit(self: *ColumnStore, allocator: Allocator, row: usize, valid: bool) !void {
        const nulls = self.nullsPtr() orelse return;
        const byte_idx = row >> 3;
        if (byte_idx >= nulls.items.len) {
            const need = byte_idx + 1 - nulls.items.len;
            try nulls.appendNTimes(allocator, 0, need);
        }
        const bit: u3 = @intCast(row & 7);
        if (valid) {
            nulls.items[byte_idx] |= (@as(u8, 1) << bit);
        } else {
            nulls.items[byte_idx] &= ~(@as(u8, 1) << bit);
        }
    }

    fn nullsPtr(self: *ColumnStore) ?*std.ArrayList(u8) {
        if (self.nulls) |_| return &self.nulls.?;
        return null;
    }
};

pub const DataStore = union(TypeTag) {
    int: std.ArrayList(i32),
    bigint: std.ArrayList(i64),
    boolean: std.ArrayList(u8),
    varchar: StringStore,
    string: StringStore,
    float: std.ArrayList(f32),
    double: std.ArrayList(f64),
    date: std.ArrayList(i32),
    datetime: std.ArrayList(i64),
    tinyint: std.ArrayList(i8),
    smallint: std.ArrayList(i16),
    largeint: std.ArrayList(i128),
    char: StringStore,
    decimal64: std.ArrayList(i64),
    decimal128: std.ArrayList(i128),
    uuid: std.ArrayList(u128),

    pub fn init(allocator: Allocator, t: Type) Allocator.Error!DataStore {
        return initCapacity(allocator, t, 0, 0);
    }

    pub fn initCapacity(
        allocator: Allocator,
        t: Type,
        rows_cap: usize,
        bytes_cap: usize,
    ) Allocator.Error!DataStore {
        return switch (t) {
            .int => .{ .int = try ensuredCapList(i32, allocator, rows_cap) },
            .bigint => .{ .bigint = try ensuredCapList(i64, allocator, rows_cap) },
            .boolean => .{ .boolean = try ensuredCapList(u8, allocator, rows_cap) },
            .varchar => .{ .varchar = try StringStore.initCapacity(allocator, rows_cap, bytes_cap) },
            .string => .{ .string = try StringStore.initCapacity(allocator, rows_cap, bytes_cap) },
            .float => .{ .float = try ensuredCapList(f32, allocator, rows_cap) },
            .double => .{ .double = try ensuredCapList(f64, allocator, rows_cap) },
            .date => .{ .date = try ensuredCapList(i32, allocator, rows_cap) },
            .datetime => .{ .datetime = try ensuredCapList(i64, allocator, rows_cap) },
            .tinyint => .{ .tinyint = try ensuredCapList(i8, allocator, rows_cap) },
            .smallint => .{ .smallint = try ensuredCapList(i16, allocator, rows_cap) },
            .largeint => .{ .largeint = try ensuredCapList(i128, allocator, rows_cap) },
            .char => .{ .char = try StringStore.initCapacity(allocator, rows_cap, bytes_cap) },
            .decimal64 => .{ .decimal64 = try ensuredCapList(i64, allocator, rows_cap) },
            .decimal128 => .{ .decimal128 = try ensuredCapList(i128, allocator, rows_cap) },
            .uuid => .{ .uuid = try ensuredCapList(u128, allocator, rows_cap) },
        };
    }

    fn ensuredCapList(comptime T: type, allocator: Allocator, cap: usize) !std.ArrayList(T) {
        var list: std.ArrayList(T) = .empty;
        if (cap > 0) try list.ensureTotalCapacity(allocator, cap);
        return list;
    }

    pub fn deinit(self: *DataStore, allocator: Allocator) void {
        switch (self.*) {
            .int => |*list| list.deinit(allocator),
            .bigint => |*list| list.deinit(allocator),
            .boolean => |*list| list.deinit(allocator),
            .varchar => |*ss| ss.deinit(allocator),
            .string => |*ss| ss.deinit(allocator),
            .float => |*list| list.deinit(allocator),
            .double => |*list| list.deinit(allocator),
            .date => |*list| list.deinit(allocator),
            .datetime => |*list| list.deinit(allocator),
            .tinyint => |*list| list.deinit(allocator),
            .smallint => |*list| list.deinit(allocator),
            .largeint => |*list| list.deinit(allocator),
            .char => |*ss| ss.deinit(allocator),
            .decimal64 => |*list| list.deinit(allocator),
            .decimal128 => |*list| list.deinit(allocator),
            .uuid => |*list| list.deinit(allocator),
        }
        self.* = undefined;
    }

    pub fn rowCount(self: DataStore) usize {
        return switch (self) {
            .int => |l| l.items.len,
            .bigint => |l| l.items.len,
            .boolean => |l| l.items.len,
            .varchar => |s| s.rowCount(),
            .string => |s| s.rowCount(),
            .float => |l| l.items.len,
            .double => |l| l.items.len,
            .date => |l| l.items.len,
            .datetime => |l| l.items.len,
            .tinyint => |l| l.items.len,
            .smallint => |l| l.items.len,
            .largeint => |l| l.items.len,
            .char => |s| s.rowCount(),
            .decimal64 => |l| l.items.len,
            .decimal128 => |l| l.items.len,
            .uuid => |l| l.items.len,
        };
    }

    pub fn view(self: DataStore) ValueView {
        return switch (self) {
            .int => |l| .{ .int = l.items },
            .bigint => |l| .{ .bigint = l.items },
            .boolean => |l| .{ .boolean = l.items },
            .varchar => |s| .{ .varchar = s.view() },
            .string => |s| .{ .string = s.view() },
            .float => |l| .{ .float = l.items },
            .double => |l| .{ .double = l.items },
            .date => |l| .{ .date = l.items },
            .datetime => |l| .{ .datetime = l.items },
            .tinyint => |l| .{ .tinyint = l.items },
            .smallint => |l| .{ .smallint = l.items },
            .largeint => |l| .{ .largeint = l.items },
            .char => |s| .{ .char = s.view() },
            .decimal64 => |l| .{ .decimal64 = l.items },
            .decimal128 => |l| .{ .decimal128 = l.items },
            .uuid => |l| .{ .uuid = l.items },
        };
    }

    pub fn clear(self: *DataStore) void {
        switch (self.*) {
            .int => |*l| l.clearRetainingCapacity(),
            .bigint => |*l| l.clearRetainingCapacity(),
            .boolean => |*l| l.clearRetainingCapacity(),
            .varchar => |*s| s.clear(),
            .string => |*s| s.clear(),
            .float => |*l| l.clearRetainingCapacity(),
            .double => |*l| l.clearRetainingCapacity(),
            .date => |*l| l.clearRetainingCapacity(),
            .datetime => |*l| l.clearRetainingCapacity(),
            .tinyint => |*l| l.clearRetainingCapacity(),
            .smallint => |*l| l.clearRetainingCapacity(),
            .largeint => |*l| l.clearRetainingCapacity(),
            .char => |*s| s.clear(),
            .decimal64 => |*l| l.clearRetainingCapacity(),
            .decimal128 => |*l| l.clearRetainingCapacity(),
            .uuid => |*l| l.clearRetainingCapacity(),
        }
    }

    /// Append a placeholder/null value (zero for ints, false for bool,
    /// empty for strings). Used when the row's actual value is NULL — the
    /// data slot still has to be filled to keep row indices aligned.
    pub fn appendNullPlaceholder(self: *DataStore, allocator: Allocator) !void {
        switch (self.*) {
            .int => |*l| try l.append(allocator, 0),
            .bigint => |*l| try l.append(allocator, 0),
            .boolean => |*l| try l.append(allocator, 0),
            .varchar => |*s| try s.appendValue(allocator, ""),
            .string => |*s| try s.appendValue(allocator, ""),
            .float => |*l| try l.append(allocator, 0.0),
            .double => |*l| try l.append(allocator, 0.0),
            .date => |*l| try l.append(allocator, 0),
            .datetime => |*l| try l.append(allocator, 0),
            .tinyint => |*l| try l.append(allocator, 0),
            .smallint => |*l| try l.append(allocator, 0),
            .largeint => |*l| try l.append(allocator, 0),
            .char => |*s| try s.appendValue(allocator, ""),
            .decimal64 => |*l| try l.append(allocator, 0),
            .decimal128 => |*l| try l.append(allocator, 0),
            .uuid => |*l| try l.append(allocator, 0),
        }
    }
};
