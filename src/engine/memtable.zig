//! Memtable — column-oriented in-memory buffer of pending writes.
//! On flush, exposes ColumnViews into its internal buffers for the segment
//! writer to consume, then can be `clear`ed for the next batch.

const std = @import("std");
const Allocator = std.mem.Allocator;

const types = @import("../types.zig");
const Type = types.Type;
const Schema = types.Schema;

const storage = @import("../storage/storage.zig");
const ColumnView = storage.ColumnView;

const store = @import("store.zig");
pub const StringStore = store.StringStore;
pub const ColumnStore = store.ColumnStore;
pub const DataStore = store.DataStore;

const transform = @import("transform.zig");
pub const appendAllColumn = transform.appendAllColumn;
pub const appendByIndices = transform.appendByIndices;
pub const appendMaskedColumn = transform.appendMaskedColumn;
pub const compareInColumn = transform.compareInColumn;

pub const Error = error{
    ColumnNotFound,
    TypeMismatch,
    MissingColumn,
};

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

pub const Memtable = struct {
    allocator: Allocator,
    schema: Schema,
    columns: []ColumnStore,
    row_count: u64 = 0,

    /// Reference count for snapshot-isolated reads. Starts at 1 (the Table's
    /// own reference). Each scan that captures this memtable calls `acquire`
    /// to bump it; `release` drops the count and frees the memtable when
    /// the count reaches 0 AND it has been retired.
    refcount: std.atomic.Value(u32) = .init(1),
    /// Set to true by `retire()` once a flush/delete has installed a fresh
    /// memtable as the table's active one. A retired memtable is read-only
    /// from then on — readers iterating it remain safe because we never
    /// mutate columns after retirement.
    retired: std.atomic.Value(bool) = .init(false),

    /// Heap-allocate a Memtable with refcount = 1. Use this for any Memtable
    /// owned by a Table (the value-returning `init` is kept for unit tests
    /// that own the struct on the stack).
    pub fn create(allocator: Allocator, schema: Schema) !*Memtable {
        return createCapacity(allocator, schema, 0, 0);
    }

    /// Like `create` but pre-reserves per-column buffer capacity. Used by
    /// Table to size memtables to ~`auto_flush_rows` so concurrent inserts
    /// never trigger an ArrayList realloc — which would invalidate the slice
    /// pointers a snapshot-pinned scan is iterating.
    pub fn createCapacity(
        allocator: Allocator,
        schema: Schema,
        rows_cap: usize,
        bytes_cap: usize,
    ) !*Memtable {
        const self = try allocator.create(Memtable);
        errdefer allocator.destroy(self);
        self.* = try initCapacity(allocator, schema, rows_cap, bytes_cap);
        return self;
    }

    pub fn init(allocator: Allocator, schema: Schema) !Memtable {
        return initCapacity(allocator, schema, 0, 0);
    }

    pub fn initCapacity(
        allocator: Allocator,
        schema: Schema,
        rows_cap: usize,
        bytes_cap: usize,
    ) !Memtable {
        var columns = try allocator.alloc(ColumnStore, schema.columns.len);
        errdefer allocator.free(columns);

        var initialized: usize = 0;
        errdefer {
            for (columns[0..initialized]) |*c| c.deinit(allocator);
        }
        for (schema.columns, 0..) |c, i| {
            columns[i] = try ColumnStore.initCapacity(allocator, c.type, c.nullable, rows_cap, bytes_cap);
            initialized += 1;
        }

        return .{ .allocator = allocator, .schema = schema, .columns = columns };
    }

    pub fn deinit(self: *Memtable) void {
        for (self.columns) |*c| c.deinit(self.allocator);
        self.allocator.free(self.columns);
        self.* = undefined;
    }

    /// Snapshot pin: increments refcount so this memtable's column buffers
    /// stay alive even after a flush/delete swaps it out as the table's
    /// active memtable. Pair with `release`.
    pub fn acquire(self: *Memtable) void {
        _ = self.refcount.fetchAdd(1, .monotonic);
    }

    /// Release a snapshot pin. If this drops the refcount to 0 AND the
    /// memtable has been retired, frees the heap allocation.
    pub fn release(self: *Memtable) void {
        const prev = self.refcount.fetchSub(1, .acq_rel);
        if (prev == 1) {
            // We held the last reference. Safe to free.
            const allocator = self.allocator;
            self.deinit();
            allocator.destroy(self);
        }
    }

    /// Mark this memtable as retired (no longer the table's active one).
    /// Caller must still call `release` to drop the table's own reference.
    pub fn retire(self: *Memtable) void {
        self.retired.store(true, .release);
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
                .float => |l| l.items.len * @sizeOf(f32),
                .double => |l| l.items.len * @sizeOf(f64),
                .date => |l| l.items.len * @sizeOf(i32),
                .datetime => |l| l.items.len * @sizeOf(i64),
                .tinyint => |l| l.items.len,
                .smallint => |l| l.items.len * @sizeOf(i16),
                .largeint => |l| l.items.len * @sizeOf(i128),
                .char => |s| s.offsets.items.len * @sizeOf(u32) + s.bytes.items.len,
                .decimal64 => |l| l.items.len * @sizeOf(i64),
                .decimal128 => |l| l.items.len * @sizeOf(i128),
                .uuid => |l| l.items.len * @sizeOf(u128),
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
    ///
    /// NOTE: in-place mutation — unsafe to call when other threads may be
    /// reading this memtable. Callers that need snapshot isolation should
    /// use `cloneWithRetainedRows` and swap the pointer instead.
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
            try transform.appendMaskedColumn(self.allocator, src.view(), keep, &new_columns[ci]);
        }

        for (self.columns) |*c| c.deinit(self.allocator);
        self.allocator.free(self.columns);
        self.columns = new_columns;
        self.row_count = kept;
        return kept;
    }

    /// Build a fresh heap-allocated Memtable containing only the rows from
    /// `self` where `keep[i] == true`. Returns null if `keep` is all-true
    /// (caller should skip the swap). Used by the snapshot-isolated delete
    /// path: instead of mutating in place, we build a new memtable and the
    /// caller atomically swaps the table's pointer + retires the old.
    pub fn cloneWithRetainedRows(self: *const Memtable, allocator: Allocator, keep: []const bool) !?*Memtable {
        std.debug.assert(keep.len == @as(usize, @intCast(self.row_count)));
        var kept: usize = 0;
        for (keep) |m| if (m) {
            kept += 1;
        };
        if (kept == keep.len) return null;

        const new = try Memtable.create(allocator, self.schema);
        errdefer new.release();

        for (self.columns, 0..) |src, ci| {
            try transform.appendMaskedColumn(allocator, src.view(), keep, &new.columns[ci]);
        }
        new.row_count = kept;
        return new;
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
                    const ord = transform.compareInColumn(ctx.mt.columns[ci], a, b);
                    if (ord == .lt) return true;
                    if (ord == .gt) return false;
                }
                return false;
            }
        };

        std.sort.pdq(u32, perm, Ctx{ .mt = &self, .keys = order_key_indices }, Ctx.lessThan);

        const sorted_columns = try allocator.alloc(ColumnStore, self.columns.len);
        errdefer allocator.free(sorted_columns);
        var inited: usize = 0;
        errdefer for (sorted_columns[0..inited]) |*c| c.deinit(allocator);

        for (self.columns, 0..) |src, ci| {
            sorted_columns[ci] = try transform.applyPermutation(allocator, src, perm);
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

    /// Append a wire-decoded columnar batch into the memtable. Each batch
    /// column is matched to the memtable schema by NAME (column order
    /// across the wire may differ); every memtable schema column must
    /// have a matching batch column with the same type tag.
    ///
    /// Used by the TCP write path: the client encodes rows to the wire
    /// batch format, the server hands the decoded batch here for bulk
    /// per-column append. Avoids the row-major detour that
    /// `insertOneRow` takes.
    pub fn insertColumnarBatch(
        self: *Memtable,
        batch_schema: []const types.Column,
        column_views: []const ColumnView,
        row_count: usize,
    ) !void {
        // Build batch_to_memtable map: for each schema column, which batch
        // column is it? Also enforces "every schema column present".
        // Stack-allocated upper bound — schemas this wide don't happen in v1.
        var batch_idx_for_schema: [256]usize = undefined;
        if (self.schema.columns.len > batch_idx_for_schema.len) return Error.MissingColumn;

        for (self.schema.columns, 0..) |sch_col, si| {
            var found: ?usize = null;
            for (batch_schema, 0..) |bc, bi| {
                if (std.mem.eql(u8, bc.name, sch_col.name)) {
                    found = bi;
                    break;
                }
            }
            const bi = found orelse return Error.MissingColumn;
            // Type tag must match. Width-info inside the type (varchar N,
            // decimal precision) is metadata; the storage layout only cares
            // about the tag. The string-like family (.varchar/.string/.char)
            // is treated as one — clients without schema knowledge ship
            // `[]const u8` as the generic .string tag, server lands it in
            // whichever string column the schema declares.
            const sch_tag = @as(types.TypeTag, sch_col.type);
            const batch_tag = @as(types.TypeTag, batch_schema[bi].type);
            const ok = sch_tag == batch_tag or
                (isStringTag(sch_tag) and isStringTag(batch_tag));
            if (!ok) return Error.TypeMismatch;
            // Wire-side nullable=true into a NOT-NULL schema column is a
            // hazard (some incoming rows may be NULL); reject so the
            // caller fixes their schema.
            if (batch_schema[bi].nullable and !sch_col.nullable) return Error.TypeMismatch;
            batch_idx_for_schema[si] = bi;
        }

        // Append each column. v1: no rollback on partial-column-append
        // failure — the only realistic failure is OOM, which is terminal
        // in practice. If that limitation matters later, swap in a snapshot/
        // restore of every column's ArrayList lengths.
        for (self.schema.columns, 0..) |sch_col, si| {
            const view = column_views[batch_idx_for_schema[si]];
            try self.appendColumnFromView(si, sch_col, view, row_count);
        }
        self.row_count += @intCast(row_count);
    }

    fn appendColumnFromView(
        self: *Memtable,
        col_idx: usize,
        sch_col: types.Column,
        view: ColumnView,
        row_count: usize,
    ) !void {
        const col = &self.columns[col_idx];
        switch (view.data) {
            .int => |s| switch (col.data) {
                .int => |*list| try list.appendSlice(self.allocator, s[0..row_count]),
                else => return Error.TypeMismatch,
            },
            .bigint => |s| switch (col.data) {
                .bigint => |*list| try list.appendSlice(self.allocator, s[0..row_count]),
                else => return Error.TypeMismatch,
            },
            .boolean => |s| switch (col.data) {
                .boolean => |*list| try list.appendSlice(self.allocator, s[0..row_count]),
                else => return Error.TypeMismatch,
            },
            .float => |s| switch (col.data) {
                .float => |*list| try list.appendSlice(self.allocator, s[0..row_count]),
                else => return Error.TypeMismatch,
            },
            .double => |s| switch (col.data) {
                .double => |*list| try list.appendSlice(self.allocator, s[0..row_count]),
                else => return Error.TypeMismatch,
            },
            .date => |s| switch (col.data) {
                .date => |*list| try list.appendSlice(self.allocator, s[0..row_count]),
                else => return Error.TypeMismatch,
            },
            .datetime => |s| switch (col.data) {
                .datetime => |*list| try list.appendSlice(self.allocator, s[0..row_count]),
                else => return Error.TypeMismatch,
            },
            .tinyint => |s| switch (col.data) {
                .tinyint => |*list| try list.appendSlice(self.allocator, s[0..row_count]),
                else => return Error.TypeMismatch,
            },
            .smallint => |s| switch (col.data) {
                .smallint => |*list| try list.appendSlice(self.allocator, s[0..row_count]),
                else => return Error.TypeMismatch,
            },
            .largeint => |s| switch (col.data) {
                .largeint => |*list| try list.appendSlice(self.allocator, s[0..row_count]),
                else => return Error.TypeMismatch,
            },
            .decimal64 => |s| switch (col.data) {
                .decimal64 => |*list| try list.appendSlice(self.allocator, s[0..row_count]),
                else => return Error.TypeMismatch,
            },
            .decimal128 => |s| switch (col.data) {
                .decimal128 => |*list| try list.appendSlice(self.allocator, s[0..row_count]),
                else => return Error.TypeMismatch,
            },
            .uuid => |s| switch (col.data) {
                .uuid => |*list| try list.appendSlice(self.allocator, s[0..row_count]),
                else => return Error.TypeMismatch,
            },
            .varchar, .string, .char => |sv| {
                // Honor the wire's null bitmap if present. appendString
                // always writes valid=true, so for null rows we have to
                // route through appendNullToColumn instead.
                var i: usize = 0;
                while (i < row_count) : (i += 1) {
                    const valid = if (view.nulls) |bm|
                        @import("../storage/column.zig").isValidBit(bm, i)
                    else
                        true;
                    if (valid) {
                        try self.appendString(col_idx, sv.rowBytes(i));
                    } else {
                        try self.appendNullToColumn(col_idx);
                    }
                }
                _ = sch_col;
                return;
            },
        }

        // Fixed-width path: update the validity bitmap for the appended rows.
        if (col.nulls != null) {
            const new_row_count = col.data.rowCount();
            var i: usize = 0;
            while (i < row_count) : (i += 1) {
                const row = new_row_count - row_count + i;
                const valid = if (view.nulls) |bm|
                    @import("../storage/column.zig").isValidBit(bm, i)
                else
                    true;
                try col.appendValidBit(self.allocator, row, valid);
            }
        }
        _ = sch_col;
    }

    fn isStringTag(t: types.TypeTag) bool {
        return switch (t) {
            .varchar, .string, .char => true,
            else => false,
        };
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
        if (comptime V == types.Date) {
            try self.appendDate(col_idx, value.days());
        } else if (comptime V == types.DateTime) {
            try self.appendDateTime(col_idx, value.micros());
        } else if (comptime V == comptime_int) {
            try self.appendComptimeInt(col_idx, value);
        } else if (comptime V == i8) {
            try self.appendInt8(col_idx, value);
        } else if (comptime V == i16) {
            try self.appendInt16(col_idx, value);
        } else if (comptime V == i32) {
            try self.appendInt32(col_idx, value);
        } else if (comptime V == i64) {
            try self.appendInt64(col_idx, value);
        } else if (comptime V == i128) {
            try self.appendInt128(col_idx, value);
        } else if (comptime V == u128) {
            try self.appendUuid(col_idx, value);
        } else if (comptime V == types.Uuid) {
            try self.appendUuid(col_idx, value.value());
        } else if (comptime V == bool) {
            try self.appendBoolean(col_idx, value);
        } else if (comptime V == f32 or V == comptime_float) {
            try self.appendFloat32Like(col_idx, V, value);
        } else if (comptime V == f64) {
            try self.appendFloat64(col_idx, value);
        } else if (comptime types.isStringLikeType(V)) {
            try self.appendString(col_idx, asConstSlice(value));
        } else {
            @compileError("memtable.appendValue: unsupported value type " ++ @typeName(V));
        }
    }

    /// `comptime_int` literals can flow into any integer-shaped column;
    /// the comptime cast verifies the literal fits.
    fn appendComptimeInt(self: *Memtable, col_idx: usize, comptime value: comptime_int) !void {
        const col = &self.columns[col_idx];
        switch (col.data) {
            .tinyint => |*list| try list.append(self.allocator, @as(i8, value)),
            .smallint => |*list| try list.append(self.allocator, @as(i16, value)),
            .int => |*list| try list.append(self.allocator, @as(i32, value)),
            .bigint => |*list| try list.append(self.allocator, @as(i64, value)),
            .largeint => |*list| try list.append(self.allocator, @as(i128, value)),
            .date => |*list| try list.append(self.allocator, @as(i32, value)),
            .datetime => |*list| try list.append(self.allocator, @as(i64, value)),
            .decimal64 => |*list| try list.append(self.allocator, @as(i64, value)),
            .decimal128 => |*list| try list.append(self.allocator, @as(i128, value)),
            .uuid => |*list| try list.append(self.allocator, @as(u128, value)),
            else => return Error.TypeMismatch,
        }
        if (col.nulls != null) try col.appendValidBit(self.allocator, col.data.rowCount() - 1, true);
    }

    /// Runtime i32 may only flow into columns where it fits without narrowing.
    fn appendInt32(self: *Memtable, col_idx: usize, value: i32) !void {
        const col = &self.columns[col_idx];
        switch (col.data) {
            .int => |*list| try list.append(self.allocator, value),
            .bigint => |*list| try list.append(self.allocator, value),
            .largeint => |*list| try list.append(self.allocator, value),
            .date => |*list| try list.append(self.allocator, value),
            .datetime => |*list| try list.append(self.allocator, value),
            else => return Error.TypeMismatch,
        }
        if (col.nulls != null) try col.appendValidBit(self.allocator, col.data.rowCount() - 1, true);
    }

    fn appendInt8(self: *Memtable, col_idx: usize, value: i8) !void {
        const col = &self.columns[col_idx];
        switch (col.data) {
            .tinyint => |*list| try list.append(self.allocator, value),
            else => return Error.TypeMismatch,
        }
        if (col.nulls != null) try col.appendValidBit(self.allocator, col.data.rowCount() - 1, true);
    }

    fn appendInt16(self: *Memtable, col_idx: usize, value: i16) !void {
        const col = &self.columns[col_idx];
        switch (col.data) {
            .smallint => |*list| try list.append(self.allocator, value),
            else => return Error.TypeMismatch,
        }
        if (col.nulls != null) try col.appendValidBit(self.allocator, col.data.rowCount() - 1, true);
    }

    fn appendInt128(self: *Memtable, col_idx: usize, value: i128) !void {
        const col = &self.columns[col_idx];
        switch (col.data) {
            .largeint => |*list| try list.append(self.allocator, value),
            .decimal128 => |*list| try list.append(self.allocator, value),
            else => return Error.TypeMismatch,
        }
        if (col.nulls != null) try col.appendValidBit(self.allocator, col.data.rowCount() - 1, true);
    }

    fn appendUuid(self: *Memtable, col_idx: usize, value: u128) !void {
        const col = &self.columns[col_idx];
        switch (col.data) {
            .uuid => |*list| try list.append(self.allocator, value),
            else => return Error.TypeMismatch,
        }
        if (col.nulls != null) try col.appendValidBit(self.allocator, col.data.rowCount() - 1, true);
    }

    fn appendDate(self: *Memtable, col_idx: usize, days: i32) !void {
        const col = &self.columns[col_idx];
        switch (col.data) {
            .date => |*list| try list.append(self.allocator, days),
            else => return Error.TypeMismatch,
        }
        if (col.nulls != null) try col.appendValidBit(self.allocator, col.data.rowCount() - 1, true);
    }

    fn appendDateTime(self: *Memtable, col_idx: usize, micros: i64) !void {
        const col = &self.columns[col_idx];
        switch (col.data) {
            .datetime => |*list| try list.append(self.allocator, micros),
            else => return Error.TypeMismatch,
        }
        if (col.nulls != null) try col.appendValidBit(self.allocator, col.data.rowCount() - 1, true);
    }

    fn appendFloat32Like(self: *Memtable, col_idx: usize, comptime V: type, value: V) !void {
        const col = &self.columns[col_idx];
        switch (col.data) {
            .float => |*list| try list.append(self.allocator, @as(f32, value)),
            .double => |*list| try list.append(self.allocator, @as(f64, value)),
            else => return Error.TypeMismatch,
        }
        if (col.nulls != null) try col.appendValidBit(self.allocator, col.data.rowCount() - 1, true);
    }

    fn appendFloat64(self: *Memtable, col_idx: usize, value: f64) !void {
        const col = &self.columns[col_idx];
        switch (col.data) {
            .double => |*list| try list.append(self.allocator, value),
            else => return Error.TypeMismatch,
        }
        if (col.nulls != null) try col.appendValidBit(self.allocator, col.data.rowCount() - 1, true);
    }

    fn appendInt64(self: *Memtable, col_idx: usize, value: i64) !void {
        const col = &self.columns[col_idx];
        switch (col.data) {
            .bigint => |*list| try list.append(self.allocator, value),
            .datetime => |*list| try list.append(self.allocator, value),
            .largeint => |*list| try list.append(self.allocator, @as(i128, value)),
            .decimal64 => |*list| try list.append(self.allocator, value),
            .decimal128 => |*list| try list.append(self.allocator, @as(i128, value)),
            else => return Error.TypeMismatch,
        }
        if (col.nulls != null) try col.appendValidBit(self.allocator, col.data.rowCount() - 1, true);
    }

    fn appendBoolean(self: *Memtable, col_idx: usize, value: bool) !void {
        const col = &self.columns[col_idx];
        switch (col.data) {
            .boolean => |*list| try list.append(self.allocator, @intFromBool(value)),
            else => return Error.TypeMismatch,
        }
        if (col.nulls != null) try col.appendValidBit(self.allocator, col.data.rowCount() - 1, true);
    }

    fn appendString(self: *Memtable, col_idx: usize, value: []const u8) !void {
        const col = &self.columns[col_idx];
        switch (col.data) {
            .varchar => |*ss| try ss.appendValue(self.allocator, value),
            .string => |*ss| try ss.appendValue(self.allocator, value),
            .char => |*ss| try ss.appendValue(self.allocator, value),
            else => return Error.TypeMismatch,
        }
        if (col.nulls != null) try col.appendValidBit(self.allocator, col.data.rowCount() - 1, true);
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

test {
    _ = store;
    _ = transform;
}
