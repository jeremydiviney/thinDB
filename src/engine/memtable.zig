//! Memtable — column-oriented in-memory buffer of pending writes.
//! On flush, exposes ColumnViews into its internal buffers for the segment
//! writer to consume, then can be `clear`ed for the next batch.

const std = @import("std");
const Allocator = std.mem.Allocator;

const types = @import("../types.zig");
const Type = types.Type;
const Schema = types.Schema;
const TypeTag = types.TypeTag;

const storage = @import("../storage/storage.zig");
const ColumnView = storage.ColumnView;
const ValueView = storage.column.ValueView;
const StringView = storage.StringView;

pub const Error = error{
    ColumnNotFound,
    TypeMismatch,
    MissingColumn,
};

/// Variable-width string column buffer. `offsets` is invariant length
/// `row_count + 1` with `offsets[0] == 0` and `offsets[row_count]` = total bytes.
pub const StringStore = struct {
    offsets: std.ArrayList(u32),
    bytes: std.ArrayList(u8),

    pub fn init(allocator: Allocator) Allocator.Error!StringStore {
        var offsets: std.ArrayList(u32) = .empty;
        errdefer offsets.deinit(allocator);
        try offsets.append(allocator, 0);
        return .{ .offsets = offsets, .bytes = .empty };
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
        return .{
            .data = try DataStore.init(allocator, t),
            .nulls = if (nullable) std.ArrayList(u8).empty else null,
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

    pub fn init(allocator: Allocator, t: Type) Allocator.Error!DataStore {
        return switch (t) {
            .int => .{ .int = .empty },
            .bigint => .{ .bigint = .empty },
            .boolean => .{ .boolean = .empty },
            .varchar => .{ .varchar = try StringStore.init(allocator) },
            .string => .{ .string = try StringStore.init(allocator) },
        };
    }

    pub fn deinit(self: *DataStore, allocator: Allocator) void {
        switch (self.*) {
            .int => |*list| list.deinit(allocator),
            .bigint => |*list| list.deinit(allocator),
            .boolean => |*list| list.deinit(allocator),
            .varchar => |*ss| ss.deinit(allocator),
            .string => |*ss| ss.deinit(allocator),
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
        };
    }

    pub fn view(self: DataStore) ValueView {
        return switch (self) {
            .int => |l| .{ .int = l.items },
            .bigint => |l| .{ .bigint = l.items },
            .boolean => |l| .{ .boolean = l.items },
            .varchar => |s| .{ .varchar = s.view() },
            .string => |s| .{ .string = s.view() },
        };
    }

    pub fn clear(self: *DataStore) void {
        switch (self.*) {
            .int => |*l| l.clearRetainingCapacity(),
            .bigint => |*l| l.clearRetainingCapacity(),
            .boolean => |*l| l.clearRetainingCapacity(),
            .varchar => |*s| s.clear(),
            .string => |*s| s.clear(),
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
        }
    }
};

pub const Memtable = struct {
    allocator: Allocator,
    schema: Schema,
    columns: []ColumnStore,
    row_count: u64 = 0,

    pub fn init(allocator: Allocator, schema: Schema) !Memtable {
        var columns = try allocator.alloc(ColumnStore, schema.columns.len);
        errdefer allocator.free(columns);

        var initialized: usize = 0;
        errdefer {
            for (columns[0..initialized]) |*c| c.deinit(allocator);
        }
        for (schema.columns, 0..) |c, i| {
            columns[i] = try ColumnStore.init(allocator, c.type, c.nullable);
            initialized += 1;
        }

        return .{ .allocator = allocator, .schema = schema, .columns = columns };
    }

    pub fn deinit(self: *Memtable) void {
        for (self.columns) |*c| c.deinit(self.allocator);
        self.allocator.free(self.columns);
        self.* = undefined;
    }

    pub fn isEmpty(self: Memtable) bool {
        return self.row_count == 0;
    }

    /// Approximate in-memory size of the column buffers in bytes. Used by
    /// auto-flush size triggers. Doesn't include ArrayList overhead or any
    /// auxiliary indexes — just the data.
    pub fn byteSize(self: Memtable) usize {
        var total: usize = 0;
        for (self.columns) |col| {
            total += switch (col.data) {
                .int => |l| l.items.len * @sizeOf(i32),
                .bigint => |l| l.items.len * @sizeOf(i64),
                .boolean => |l| l.items.len,
                .varchar => |s| s.offsets.items.len * @sizeOf(u32) + s.bytes.items.len,
                .string => |s| s.offsets.items.len * @sizeOf(u32) + s.bytes.items.len,
            };
            if (col.nulls) |n| total += n.items.len;
        }
        return total;
    }

    pub fn clear(self: *Memtable) void {
        for (self.columns) |*c| c.clear();
        self.row_count = 0;
    }

    /// Replace the memtable's column buffers, keeping only rows where
    /// `keep[i] == true`. Returns the number of rows kept. If all rows are
    /// kept, returns without rebuilding.
    pub fn retainRows(self: *Memtable, keep: []const bool) !usize {
        std.debug.assert(keep.len == @as(usize, @intCast(self.row_count)));
        var kept: usize = 0;
        for (keep) |m| if (m) {
            kept += 1;
        };
        if (kept == keep.len) return kept;

        var new_columns = try self.allocator.alloc(ColumnStore, self.columns.len);
        errdefer self.allocator.free(new_columns);
        var inited: usize = 0;
        errdefer for (new_columns[0..inited]) |*c| c.deinit(self.allocator);
        for (self.schema.columns, 0..) |sch_col, ci| {
            new_columns[ci] = try ColumnStore.init(self.allocator, sch_col.type, sch_col.nullable);
            inited += 1;
        }

        for (self.columns, 0..) |src, ci| {
            try appendMaskedColumn(self.allocator, src.view(), keep, &new_columns[ci]);
        }

        for (self.columns) |*c| c.deinit(self.allocator);
        self.allocator.free(self.columns);
        self.columns = new_columns;
        self.row_count = kept;
        return kept;
    }

    pub fn views(self: Memtable, allocator: Allocator) ![]ColumnView {
        const out = try allocator.alloc(ColumnView, self.columns.len);
        for (self.columns, 0..) |c, i| out[i] = c.view();
        return out;
    }

    /// Build an order-key-sorted snapshot of the current memtable. Allocates
    /// fresh ColumnStores for the sorted output; caller calls `.deinit()`.
    /// `order_key_indices` lists column indices in the order to sort by
    /// (composite keys compared lexicographically).
    pub fn buildSortedSnapshot(
        self: Memtable,
        allocator: Allocator,
        order_key_indices: []const usize,
    ) !SortedSnapshot {
        const n: usize = @intCast(self.row_count);

        const perm = try allocator.alloc(u32, n);
        defer allocator.free(perm);
        for (perm, 0..) |*p, i| p.* = @intCast(i);

        const Ctx = struct {
            mt: *const Memtable,
            keys: []const usize,

            pub fn lessThan(ctx: @This(), a: u32, b: u32) bool {
                for (ctx.keys) |ci| {
                    const ord = compareInColumn(ctx.mt.columns[ci], a, b);
                    if (ord == .lt) return true;
                    if (ord == .gt) return false;
                }
                return false;
            }
        };

        std.sort.heap(u32, perm, Ctx{ .mt = &self, .keys = order_key_indices }, Ctx.lessThan);

        const sorted_columns = try allocator.alloc(ColumnStore, self.columns.len);
        errdefer allocator.free(sorted_columns);
        var inited: usize = 0;
        errdefer for (sorted_columns[0..inited]) |*c| c.deinit(allocator);

        for (self.columns, 0..) |src, ci| {
            sorted_columns[ci] = try applyPermutation(allocator, src, perm);
            inited += 1;
        }

        const view_buf = try allocator.alloc(ColumnView, sorted_columns.len);
        errdefer allocator.free(view_buf);
        for (sorted_columns, 0..) |c, ci| view_buf[ci] = c.view();

        return .{
            .allocator = allocator,
            .columns = sorted_columns,
            .views = view_buf,
            .row_count = self.row_count,
        };
    }
    /// Append a slice/array/tuple of row structs. Each row must have one field
    /// per schema column. Field types are validated against schema column types
    /// via `Type.matchesZigType`.
    pub fn insertRows(self: *Memtable, rows: anytype) !void {
        const Rows = @TypeOf(rows);
        const rows_info = @typeInfo(Rows);

        switch (rows_info) {
            .pointer => |p| switch (p.size) {
                .slice => {
                    for (rows) |row| try self.insertOneRow(row);
                },
                .one => {
                    const child_info = @typeInfo(p.child);
                    if (comptime child_info == .@"struct" and child_info.@"struct".is_tuple) {
                        // &.{ .{...}, .{...} } syntax — iterate tuple fields by comptime name
                        inline for (child_info.@"struct".fields) |tf| {
                            try self.insertOneRow(@field(rows, tf.name));
                        }
                    } else if (comptime child_info == .array) {
                        for (rows) |row| try self.insertOneRow(row);
                    } else {
                        @compileError("expected pointer to array or tuple of structs, got " ++ @typeName(Rows));
                    }
                },
                else => @compileError("expected slice or pointer-to-array, got " ++ @typeName(Rows)),
            },
            .array => {
                for (rows) |row| try self.insertOneRow(row);
            },
            else => @compileError("expected slice/array/tuple of row structs, got " ++ @typeName(Rows)),
        }
    }

    fn insertOneRow(self: *Memtable, row: anytype) !void {
        const R = @TypeOf(row);
        const info = @typeInfo(R);
        if (comptime info != .@"struct") {
            @compileError("expected struct row, got " ++ @typeName(R));
        }
        const fields = info.@"struct".fields;

        // Validate fields against schema.
        inline for (fields) |field| {
            const col_idx = self.schema.columnIndex(field.name) orelse return Error.ColumnNotFound;
            const sch_col = self.schema.columns[col_idx];
            const InnerT = comptime innerType(field.type);
            if (comptime isOptionalType(field.type)) {
                if (!sch_col.nullable) return Error.TypeMismatch;
            }
            if (!sch_col.type.matchesZigType(InnerT)) {
                return Error.TypeMismatch;
            }
        }
        // Validate every schema column has a value in the row.
        for (self.schema.columns) |col| {
            var found = false;
            inline for (fields) |field| {
                if (std.mem.eql(u8, field.name, col.name)) found = true;
            }
            if (!found) return Error.MissingColumn;
        }

        // Append.
        inline for (fields) |field| {
            const col_idx = self.schema.columnIndex(field.name).?;
            const value = @field(row, field.name);
            if (comptime isOptionalType(field.type)) {
                if (value) |v| {
                    try self.appendValue(col_idx, @TypeOf(v), v);
                } else {
                    try self.appendNullToColumn(col_idx);
                }
            } else {
                try self.appendValue(col_idx, field.type, value);
            }
        }
        self.row_count += 1;
    }

    fn isOptionalType(comptime T: type) bool {
        return @typeInfo(T) == .optional;
    }

    fn innerType(comptime T: type) type {
        return switch (@typeInfo(T)) {
            .optional => |o| o.child,
            else => T,
        };
    }

    pub fn appendValue(self: *Memtable, col_idx: usize, comptime V: type, value: V) !void {
        if (comptime V == i32 or V == comptime_int) {
            try self.appendInt32Like(col_idx, V, value);
        } else if (comptime V == i64) {
            try self.appendInt64(col_idx, value);
        } else if (comptime V == bool) {
            try self.appendBoolean(col_idx, value);
        } else if (comptime types.isStringLikeType(V)) {
            try self.appendString(col_idx, asConstSlice(value));
        } else {
            @compileError("memtable.appendValue: unsupported value type " ++ @typeName(V));
        }
    }

    fn appendInt32Like(self: *Memtable, col_idx: usize, comptime V: type, value: V) !void {
        const col = &self.columns[col_idx];
        switch (col.data) {
            .int => |*list| try list.append(self.allocator, @as(i32, value)),
            .bigint => |*list| try list.append(self.allocator, @as(i64, value)),
            else => return Error.TypeMismatch,
        }
        try col.appendValidBit(self.allocator, col.data.rowCount() - 1, true);
    }

    fn appendInt64(self: *Memtable, col_idx: usize, value: i64) !void {
        const col = &self.columns[col_idx];
        switch (col.data) {
            .bigint => |*list| try list.append(self.allocator, value),
            else => return Error.TypeMismatch,
        }
        try col.appendValidBit(self.allocator, col.data.rowCount() - 1, true);
    }

    fn appendBoolean(self: *Memtable, col_idx: usize, value: bool) !void {
        const col = &self.columns[col_idx];
        switch (col.data) {
            .boolean => |*list| try list.append(self.allocator, @intFromBool(value)),
            else => return Error.TypeMismatch,
        }
        try col.appendValidBit(self.allocator, col.data.rowCount() - 1, true);
    }

    fn appendString(self: *Memtable, col_idx: usize, value: []const u8) !void {
        const col = &self.columns[col_idx];
        switch (col.data) {
            .varchar => |*ss| try ss.appendValue(self.allocator, value),
            .string => |*ss| try ss.appendValue(self.allocator, value),
            else => return Error.TypeMismatch,
        }
        try col.appendValidBit(self.allocator, col.data.rowCount() - 1, true);
    }

    /// Append a NULL slot. The schema column at `col_idx` must be nullable.
    /// Writes a placeholder into `data` (zero / empty) and clears the
    /// validity bit.
    pub fn appendNullToColumn(self: *Memtable, col_idx: usize) !void {
        const col = &self.columns[col_idx];
        if (col.nulls == null) return Error.TypeMismatch;
        try col.data.appendNullPlaceholder(self.allocator);
        try col.appendValidBit(self.allocator, col.data.rowCount() - 1, false);
    }
};

/// Compare row `a` vs row `b` within a single column. Lexicographic order
/// for strings, numeric order for everything else. Used by sort kernels.
pub fn compareInColumnPub(col: ColumnStore, a: u32, b: u32) std.math.Order {
    return compareInColumn(col, a, b);
}

/// Append every row of `view` (including its validity bits if `view.nulls`
/// is non-null) to `out`. Types must match.
pub fn appendAllColumn(
    allocator: Allocator,
    view: ColumnView,
    out: *ColumnStore,
) !void {
    const dst_start = out.data.rowCount();
    switch (view.data) {
        .int => |s| switch (out.data) {
            .int => |*list| try list.appendSlice(allocator, s),
            else => unreachable,
        },
        .bigint => |s| switch (out.data) {
            .bigint => |*list| try list.appendSlice(allocator, s),
            else => unreachable,
        },
        .boolean => |s| switch (out.data) {
            .boolean => |*list| try list.appendSlice(allocator, s),
            else => unreachable,
        },
        .varchar => |sv| switch (out.data) {
            .varchar => |*ss| {
                for (0..sv.rowCount()) |i| try ss.appendValue(allocator, sv.rowBytes(i));
            },
            else => unreachable,
        },
        .string => |sv| switch (out.data) {
            .string => |*ss| {
                for (0..sv.rowCount()) |i| try ss.appendValue(allocator, sv.rowBytes(i));
            },
            else => unreachable,
        },
    }
    // Carry validity bits across.
    if (out.nulls != null) {
        const n = view.data.rowCount();
        for (0..n) |i| {
            const valid = storage.column.isValidBit(view.nulls, i);
            try out.appendValidBit(allocator, dst_start + i, valid);
        }
    }
}

/// Append rows from `view` to `out`, picking by the given indices into `view`.
/// Used by Sort to materialize batches in permutation order. Validity bits
/// are carried across via `view.nulls`.
pub fn appendByIndices(
    allocator: Allocator,
    view: ColumnView,
    indices: []const u32,
    out: *ColumnStore,
) !void {
    const dst_start = out.data.rowCount();
    switch (view.data) {
        .int => |s| switch (out.data) {
            .int => |*list| {
                try list.ensureUnusedCapacity(allocator, indices.len);
                for (indices) |idx| list.appendAssumeCapacity(s[idx]);
            },
            else => unreachable,
        },
        .bigint => |s| switch (out.data) {
            .bigint => |*list| {
                try list.ensureUnusedCapacity(allocator, indices.len);
                for (indices) |idx| list.appendAssumeCapacity(s[idx]);
            },
            else => unreachable,
        },
        .boolean => |s| switch (out.data) {
            .boolean => |*list| {
                try list.ensureUnusedCapacity(allocator, indices.len);
                for (indices) |idx| list.appendAssumeCapacity(s[idx]);
            },
            else => unreachable,
        },
        .varchar => |sv| switch (out.data) {
            .varchar => |*ss| {
                for (indices) |idx| try ss.appendValue(allocator, sv.rowBytes(idx));
            },
            else => unreachable,
        },
        .string => |sv| switch (out.data) {
            .string => |*ss| {
                for (indices) |idx| try ss.appendValue(allocator, sv.rowBytes(idx));
            },
            else => unreachable,
        },
    }
    if (out.nulls != null) {
        for (indices, 0..) |src_idx, j| {
            const valid = storage.column.isValidBit(view.nulls, src_idx);
            try out.appendValidBit(allocator, dst_start + j, valid);
        }
    }
}

pub fn appendMaskedColumn(
    allocator: Allocator,
    view: ColumnView,
    mask: []const bool,
    out: *ColumnStore,
) !void {
    const dst_start = out.data.rowCount();
    switch (view.data) {
        .int => |s| switch (out.data) {
            .int => |*list| for (s, mask) |v, m| {
                if (m) try list.append(allocator, v);
            },
            else => unreachable,
        },
        .bigint => |s| switch (out.data) {
            .bigint => |*list| for (s, mask) |v, m| {
                if (m) try list.append(allocator, v);
            },
            else => unreachable,
        },
        .boolean => |s| switch (out.data) {
            .boolean => |*list| for (s, mask) |v, m| {
                if (m) try list.append(allocator, v);
            },
            else => unreachable,
        },
        .varchar => |sv| switch (out.data) {
            .varchar => |*ss| for (mask, 0..) |m, row| {
                if (m) try ss.appendValue(allocator, sv.rowBytes(row));
            },
            else => unreachable,
        },
        .string => |sv| switch (out.data) {
            .string => |*ss| for (mask, 0..) |m, row| {
                if (m) try ss.appendValue(allocator, sv.rowBytes(row));
            },
            else => unreachable,
        },
    }
    if (out.nulls != null) {
        var j: usize = 0;
        for (mask, 0..) |m, src_row| {
            if (!m) continue;
            const valid = storage.column.isValidBit(view.nulls, src_row);
            try out.appendValidBit(allocator, dst_start + j, valid);
            j += 1;
        }
    }
}

/// Owns the buffers produced by `Memtable.buildSortedSnapshot`. `views`
/// borrows from `columns`.
pub const SortedSnapshot = struct {
    allocator: Allocator,
    columns: []ColumnStore,
    views: []ColumnView,
    row_count: u64,

    pub fn deinit(self: *SortedSnapshot) void {
        for (self.columns) |*c| c.deinit(self.allocator);
        self.allocator.free(self.columns);
        self.allocator.free(self.views);
        self.* = undefined;
    }
};

fn applyPermutation(
    allocator: Allocator,
    src: ColumnStore,
    perm: []const u32,
) !ColumnStore {
    const dst_data: DataStore = switch (src.data) {
        .int => |l| blk: {
            var dst: std.ArrayList(i32) = try .initCapacity(allocator, perm.len);
            errdefer dst.deinit(allocator);
            for (perm) |p| dst.appendAssumeCapacity(l.items[p]);
            break :blk DataStore{ .int = dst };
        },
        .bigint => |l| blk: {
            var dst: std.ArrayList(i64) = try .initCapacity(allocator, perm.len);
            errdefer dst.deinit(allocator);
            for (perm) |p| dst.appendAssumeCapacity(l.items[p]);
            break :blk DataStore{ .bigint = dst };
        },
        .boolean => |l| blk: {
            var dst: std.ArrayList(u8) = try .initCapacity(allocator, perm.len);
            errdefer dst.deinit(allocator);
            for (perm) |p| dst.appendAssumeCapacity(l.items[p]);
            break :blk DataStore{ .boolean = dst };
        },
        .varchar => |s| blk: {
            var dst = try StringStore.init(allocator);
            errdefer dst.deinit(allocator);
            for (perm) |p| try dst.appendValue(allocator, s.view().rowBytes(p));
            break :blk DataStore{ .varchar = dst };
        },
        .string => |s| blk: {
            var dst = try StringStore.init(allocator);
            errdefer dst.deinit(allocator);
            for (perm) |p| try dst.appendValue(allocator, s.view().rowBytes(p));
            break :blk DataStore{ .string = dst };
        },
    };

    var nulls: ?std.ArrayList(u8) = null;
    if (src.nulls) |src_bits| {
        var b: std.ArrayList(u8) = try .initCapacity(allocator, storage.column.bitmapBytes(perm.len));
        errdefer b.deinit(allocator);
        try b.appendNTimes(allocator, 0, storage.column.bitmapBytes(perm.len));
        for (perm, 0..) |src_row, dst_row| {
            if (storage.column.isValidBit(src_bits.items, src_row)) {
                const byte_idx = dst_row >> 3;
                const bit: u3 = @intCast(dst_row & 7);
                b.items[byte_idx] |= (@as(u8, 1) << bit);
            }
        }
        nulls = b;
    }

    return .{ .data = dst_data, .nulls = nulls };
}

fn compareInColumn(col: ColumnStore, a: u32, b: u32) std.math.Order {
    return switch (col.data) {
        .int => |l| std.math.order(l.items[a], l.items[b]),
        .bigint => |l| std.math.order(l.items[a], l.items[b]),
        .boolean => |l| std.math.order(l.items[a], l.items[b]),
        .varchar => |s| std.mem.order(u8, s.view().rowBytes(a), s.view().rowBytes(b)),
        .string => |s| std.mem.order(u8, s.view().rowBytes(a), s.view().rowBytes(b)),
    };
}

fn asConstSlice(v: anytype) []const u8 {
    const V = @TypeOf(v);
    if (V == []const u8) return v;
    if (V == []u8) return v;
    const info = @typeInfo(V);
    if (info == .pointer) {
        switch (info.pointer.size) {
            .slice => return v,
            .one => return v[0..],
            else => @compileError("asConstSlice unexpected pointer size"),
        }
    }
    @compileError("asConstSlice unsupported type " ++ @typeName(V));
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

test "memtable insertRows accumulates and exposes ColumnViews" {
    const allocator = std.testing.allocator;
    const schema = Schema{
        .columns = &.{
            .{ .name = "id", .type = .bigint },
            .{ .name = "qty", .type = .int },
            .{ .name = "active", .type = .boolean },
            .{ .name = "tag", .type = .string },
        },
        .order_key = &.{"id"},
        .unique = true,
    };

    var mt = try Memtable.init(allocator, schema);
    defer mt.deinit();

    try mt.insertRows(&.{
        .{ .id = @as(i64, 1), .qty = @as(i32, 10), .active = true, .tag = "alpha" },
        .{ .id = @as(i64, 2), .qty = @as(i32, 20), .active = false, .tag = "beta" },
        .{ .id = @as(i64, 3), .qty = @as(i32, 30), .active = true, .tag = "" },
    });

    try std.testing.expectEqual(@as(u64, 3), mt.row_count);

    const v = try mt.views(allocator);
    defer allocator.free(v);

    try std.testing.expectEqualSlices(i64, &[_]i64{ 1, 2, 3 }, v[0].data.bigint);
    try std.testing.expectEqualSlices(i32, &[_]i32{ 10, 20, 30 }, v[1].data.int);
    try std.testing.expectEqualSlices(u8, &[_]u8{ 1, 0, 1 }, v[2].data.boolean);
    try std.testing.expectEqualStrings("alpha", v[3].data.string.rowBytes(0));
    try std.testing.expectEqualStrings("beta", v[3].data.string.rowBytes(1));
    try std.testing.expectEqualStrings("", v[3].data.string.rowBytes(2));
}

test "memtable insertRows rejects missing column" {
    const allocator = std.testing.allocator;
    const schema = Schema{
        .columns = &.{
            .{ .name = "id", .type = .bigint },
            .{ .name = "qty", .type = .int },
        },
        .order_key = &.{"id"},
        .unique = false,
    };
    var mt = try Memtable.init(allocator, schema);
    defer mt.deinit();

    const rows = &[_]struct { id: i64 }{.{ .id = 1 }};
    try std.testing.expectError(Error.MissingColumn, mt.insertRows(rows));
}

test "memtable insertRows rejects unknown column" {
    const allocator = std.testing.allocator;
    const schema = Schema{
        .columns = &.{.{ .name = "id", .type = .bigint }},
        .order_key = &.{"id"},
        .unique = false,
    };
    var mt = try Memtable.init(allocator, schema);
    defer mt.deinit();

    const rows = &[_]struct { id: i64, bogus: i32 }{.{ .id = 1, .bogus = 99 }};
    try std.testing.expectError(Error.ColumnNotFound, mt.insertRows(rows));
}

test "memtable clear resets row count but preserves capacity" {
    const allocator = std.testing.allocator;
    const schema = Schema{
        .columns = &.{
            .{ .name = "id", .type = .bigint },
            .{ .name = "tag", .type = .string },
        },
        .order_key = &.{"id"},
        .unique = false,
    };
    var mt = try Memtable.init(allocator, schema);
    defer mt.deinit();

    try mt.insertRows(&.{
        .{ .id = @as(i64, 1), .tag = "a" },
        .{ .id = @as(i64, 2), .tag = "bb" },
    });
    try std.testing.expectEqual(@as(u64, 2), mt.row_count);

    mt.clear();
    try std.testing.expectEqual(@as(u64, 0), mt.row_count);
    try std.testing.expectEqual(@as(usize, 0), mt.columns[1].rowCount());

    try mt.insertRows(&.{.{ .id = @as(i64, 3), .tag = "ccc" }});
    try std.testing.expectEqual(@as(u64, 1), mt.row_count);
    try std.testing.expectEqualStrings("ccc", mt.columns[1].view().data.string.rowBytes(0));
}
