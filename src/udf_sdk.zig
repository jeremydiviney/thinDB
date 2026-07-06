//! Comptime SDK for table-valued UDFs — the ergonomic layer users write
//! against. A function is a file (or struct) with three declarations:
//!
//!     pub const spec = tdb.TableFnSpec{ .name = "running_total", .execution = .partitioned };
//!     pub const Input = struct { id: i64, g: i32, amt: i64 };
//!     pub const Output = struct { id: i64, running: i64 };
//!     pub fn process(ctx: *tdb.Ctx, p: tdb.Partition(Input), out: *tdb.Writer(Output)) !void { ... }
//!
//! Row types are plain Zig structs: field name = column name, declaration
//! order = column order, Zig type maps to the thinDB type (`i64`→bigint,
//! `i32`→int, `f64`→double, `[]const u8`→string, `tdb.Date`→date, `?T`→
//! nullable T). `Database.registerTableFn(@import("file.zig"))` reflects
//! over the declarations and generates the raw `udf.TableUdf` descriptor +
//! trampoline — the engine layer (exec/table_fn.zig) never sees the sugar.
//!
//! Two access styles over the same borrowed column memory, freely mixable:
//! columnar (`p.col(.amt)` → `[]const i64`, zero-copy) and row-struct
//! (`p.iter()` / `p.at(i)` assemble an `Input` value on the fly). Emission
//! is a typed struct literal: `try out.row(.{ .id = ..., .running = ... })`
//! — a missing/extra/mistyped field is a Zig compile error, so a function
//! that compiles cannot emit a wrong-shaped row.

const std = @import("std");
const Allocator = std.mem.Allocator;

const types = @import("types.zig");
const storage = @import("storage/storage.zig");
const ColumnView = storage.ColumnView;
const column_mod = @import("storage/column.zig");
const StringView = column_mod.StringView;
const udf = @import("udf.zig");

pub const TvfExecution = udf.TvfExecution;

pub const TableFnSpec = struct {
    name: []const u8,
    execution: TvfExecution = .either,
};

/// Per-partition context. `arena` is freed after each process() call —
/// scratch allocations cannot leak.
pub const Ctx = struct {
    arena: Allocator,
    user_data: ?*anyopaque = null,
};

/// Days since 1970-01-01 (UTC). enum(i32) so a raw date column reinterprets
/// as `[]const Date` with zero copies.
pub const Date = enum(i32) {
    _,

    pub fn days(self: Date) i32 {
        return @intFromEnum(self);
    }
    pub fn fromDays(d: i32) Date {
        return @enumFromInt(d);
    }
    pub fn eq(self: Date, other: Date) bool {
        return @intFromEnum(self) == @intFromEnum(other);
    }
    pub fn lt(self: Date, other: Date) bool {
        return @intFromEnum(self) < @intFromEnum(other);
    }
    pub fn lte(self: Date, other: Date) bool {
        return @intFromEnum(self) <= @intFromEnum(other);
    }

    pub const Ymd = struct { y: i32, m: u8, d: u8 };

    /// Howard Hinnant's civil-from-days algorithm.
    pub fn ymd(self: Date) Ymd {
        var z: i64 = @intFromEnum(self);
        z += 719468;
        const era: i64 = @divFloor(z, 146097);
        const doe: u32 = @intCast(z - era * 146097);
        const yoe: u32 = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
        const y: i64 = @as(i64, yoe) + era * 400;
        const doy: u32 = doe - (365 * yoe + yoe / 4 - yoe / 100);
        const mp: u32 = (5 * doy + 2) / 153;
        const d: u32 = doy - (153 * mp + 2) / 5 + 1;
        const m: u32 = if (mp < 10) mp + 3 else mp - 9;
        return .{ .y = @intCast(if (m <= 2) y + 1 else y), .m = @intCast(m), .d = @intCast(d) };
    }

    pub fn fromYmd(v: Ymd) Date {
        const y: i64 = if (v.m <= 2) v.y - 1 else v.y;
        const era: i64 = @divFloor(y, 400);
        const yoe: u32 = @intCast(y - era * 400);
        const mp: u32 = if (v.m > 2) v.m - 3 else v.m + 9;
        const doy: u32 = (153 * mp + 2) / 5 + v.d - 1;
        const doe: u32 = yoe * 365 + yoe / 4 - yoe / 100 + doy;
        return @enumFromInt(@as(i32, @intCast(era * 146097 + doe - 719468)));
    }

    pub fn addMonths(self: Date, n: i32) Date {
        const v = self.ymd();
        const total: i32 = (v.y * 12 + (@as(i32, v.m) - 1)) + n;
        const y: i32 = @divFloor(total, 12);
        const m: u8 = @intCast(@mod(total, 12) + 1);
        const dim = daysInMonth(y, m);
        return fromYmd(.{ .y = y, .m = m, .d = @min(v.d, dim) });
    }

    fn daysInMonth(y: i32, m: u8) u8 {
        const leap = (@mod(y, 4) == 0 and @mod(y, 100) != 0) or @mod(y, 400) == 0;
        return switch (m) {
            1, 3, 5, 7, 8, 10, 12 => 31,
            4, 6, 9, 11 => 30,
            2 => if (leap) 29 else 28,
            else => unreachable,
        };
    }
};

/// Microseconds since 1970-01-01T00:00:00 UTC (timezone-naive). enum(i64)
/// so a raw datetime column reinterprets as `[]const DateTime`.
pub const DateTime = enum(i64) {
    _,

    pub fn micros(self: DateTime) i64 {
        return @intFromEnum(self);
    }
    pub fn fromMicros(us: i64) DateTime {
        return @enumFromInt(us);
    }
    pub fn eq(self: DateTime, other: DateTime) bool {
        return @intFromEnum(self) == @intFromEnum(other);
    }
    pub fn lt(self: DateTime, other: DateTime) bool {
        return @intFromEnum(self) < @intFromEnum(other);
    }
    pub fn lte(self: DateTime, other: DateTime) bool {
        return @intFromEnum(self) <= @intFromEnum(other);
    }
    pub fn date(self: DateTime) Date {
        return Date.fromDays(@intCast(@divFloor(@intFromEnum(self), std.time.us_per_day)));
    }
};

/// Nullable column accessor: raw `values` + validity for hot loops, or
/// `get(i)` for per-row `?T` access.
pub fn Opt(comptime T: type) type {
    return struct {
        values: []const T,
        valid: ?[]const u8,

        pub fn get(self: @This(), i: usize) ?T {
            if (!column_mod.isValidBit(self.valid, i)) return null;
            return self.values[i];
        }
        pub fn isNull(self: @This(), i: usize) bool {
            return !column_mod.isValidBit(self.valid, i);
        }
    };
}

/// String column accessor (var-width data can't be a plain slice).
pub const Strings = struct {
    view: StringView,

    pub fn get(self: Strings, i: usize) []const u8 {
        return self.view.rowBytes(i);
    }
};

/// Nullable string column accessor.
pub const OptStrings = struct {
    view: StringView,
    valid: ?[]const u8,

    pub fn get(self: OptStrings, i: usize) ?[]const u8 {
        if (!column_mod.isValidBit(self.valid, i)) return null;
        return self.view.rowBytes(i);
    }
    pub fn isNull(self: OptStrings, i: usize) bool {
        return !column_mod.isValidBit(self.valid, i);
    }
};

/// The thinDB column type for a Zig field type; a compile error names the
/// offending type when there is no mapping.
fn columnTypeFor(comptime T: type) types.Type {
    return switch (T) {
        i8 => .tinyint,
        i16 => .smallint,
        i32 => .int,
        i64 => .bigint,
        i128 => .largeint,
        f32 => .float,
        f64 => .double,
        bool => .boolean,
        u128 => .uuid,
        []const u8 => .string,
        Date => .date,
        DateTime => .datetime,
        else => @compileError("thinDB table UDF: no column type mapping for '" ++
            @typeName(T) ++ "' (use i8/i16/i32/i64/i128, f32/f64, bool, u128, " ++
            "[]const u8, tdb.Date, tdb.DateTime, or ?T of those)"),
    };
}

fn unwrapOptional(comptime T: type) struct { child: type, nullable: bool } {
    return switch (@typeInfo(T)) {
        .optional => |o| .{ .child = o.child, .nullable = true },
        else => .{ .child = T, .nullable = false },
    };
}

/// Comptime schema for a row struct: field name = column name, declaration
/// order = column order.
fn schemaFor(comptime Row: type) [@typeInfo(Row).@"struct".fields.len]types.Column {
    const fields = @typeInfo(Row).@"struct".fields;
    var cols: [fields.len]types.Column = undefined;
    for (fields, 0..) |f, i| {
        const u = unwrapOptional(f.type);
        cols[i] = .{
            .name = f.name,
            .type = columnTypeFor(u.child),
            .nullable = u.nullable,
        };
    }
    return cols;
}

/// What `p.col(.field)` returns for a given Zig field type.
fn ColAccessor(comptime FieldT: type) type {
    const u = unwrapOptional(FieldT);
    if (u.child == []const u8) {
        return if (u.nullable) OptStrings else Strings;
    }
    return if (u.nullable) Opt(u.child) else []const u.child;
}

/// The raw view slice for a scalar column, reinterpreted to the SDK type
/// (Date/DateTime are layout-identical enum wrappers over i32/i64).
fn scalarSlice(comptime T: type, view: ColumnView) []const T {
    return switch (T) {
        i8 => view.data.tinyint,
        i16 => view.data.smallint,
        i32 => view.data.int,
        i64 => view.data.bigint,
        i128 => view.data.largeint,
        f32 => view.data.float,
        f64 => view.data.double,
        u128 => view.data.uuid,
        Date => @ptrCast(view.data.date),
        DateTime => @ptrCast(view.data.datetime),
        bool => @compileError("bool columns: use p.colBool(.field) — storage is u8"),
        else => unreachable,
    };
}

fn stringViewOf(view: ColumnView) StringView {
    return switch (view.data) {
        .varchar, .string, .char => |s| s,
        else => unreachable,
    };
}

/// Typed, zero-copy access to one partition of the declared input. Borrowed
/// for the duration of the process() call; rows are ordered per the call
/// site's ORDER BY.
pub fn Partition(comptime Input: type) type {
    const FieldEnum = std.meta.FieldEnum(Input);
    return struct {
        raw: *const udf.TvfPartition,
        len: usize,

        const Self = @This();

        fn fieldType(comptime f: FieldEnum) type {
            return @typeInfo(Input).@"struct".fields[@intFromEnum(f)].type;
        }

        /// Columnar accessor: `[]const T` for scalars (zero-copy), `Strings`
        /// for text, `Opt(T)` / `OptStrings` for nullable fields.
        pub fn col(self: Self, comptime f: FieldEnum) ColAccessor(fieldType(f)) {
            const view = self.raw.columns[@intFromEnum(f)];
            const u = unwrapOptional(fieldType(f));
            if (u.child == []const u8) {
                if (u.nullable) return .{ .view = stringViewOf(view), .valid = view.nulls };
                return .{ .view = stringViewOf(view) };
            }
            if (u.nullable) return .{ .values = scalarSlice(u.child, view), .valid = view.nulls };
            return scalarSlice(u.child, view);
        }

        /// Bool columns (u8 storage): per-row accessor.
        pub fn colBool(self: Self, comptime f: FieldEnum, i: usize) bool {
            return self.raw.columns[@intFromEnum(f)].data.boolean[i] != 0;
        }

        /// One row assembled as an `Input` value. Strings are borrowed.
        pub fn at(self: Self, i: usize) Input {
            var row: Input = undefined;
            inline for (@typeInfo(Input).@"struct".fields, 0..) |fld, ci| {
                const view = self.raw.columns[ci];
                const u = unwrapOptional(fld.type);
                if (u.nullable) {
                    @field(row, fld.name) = if (!view.isValid(i))
                        null
                    else
                        fieldValueAt(u.child, view, i);
                } else {
                    @field(row, fld.name) = fieldValueAt(u.child, view, i);
                }
            }
            return row;
        }

        fn fieldValueAt(comptime T: type, view: ColumnView, i: usize) T {
            if (T == []const u8) return stringViewOf(view).rowBytes(i);
            if (T == bool) return view.data.boolean[i] != 0;
            return scalarSlice(T, view)[i];
        }

        pub const RowIter = struct {
            p: Self,
            i: usize = 0,

            pub fn next(self: *RowIter) ?Input {
                if (self.i >= self.p.len) return null;
                defer self.i += 1;
                return self.p.at(self.i);
            }
        };

        /// Row-struct iteration: assembles `Input` values on the fly from
        /// the column slices — convenience path; `col()` is the fast path.
        pub fn iter(self: Self) RowIter {
            return .{ .p = self };
        }

        /// Partition-key value: the field's value on the partition's first
        /// row (constant within the partition when the call site partitions
        /// by it). Meaningful only for PARTITION BY columns.
        pub fn key(self: Self, comptime f: FieldEnum) fieldType(f) {
            std.debug.assert(self.len > 0);
            return fieldValueAt(unwrapOptional(fieldType(f)).child, self.raw.columns[@intFromEnum(f)], 0);
        }
    };
}

/// Typed row emission into the declared output. `row(.{...})` is checked
/// field-for-field at compile time — wrong shape cannot compile.
pub fn Writer(comptime Output: type) type {
    return struct {
        raw: *udf.TvfOutput,

        const Self = @This();

        pub fn row(self: *Self, r: Output) !void {
            const alloc = self.raw.allocator;
            inline for (@typeInfo(Output).@"struct".fields, 0..) |fld, ci| {
                const store = self.raw.columns[ci];
                const u = unwrapOptional(fld.type);
                const v = @field(r, fld.name);
                if (u.nullable) {
                    if (v) |val| {
                        try appendScalar(u.child, store, alloc, val);
                        try store.appendValidBit(alloc, store.rowCount() - 1, true);
                    } else {
                        try store.appendNulls(alloc, 1);
                    }
                } else {
                    try appendScalar(u.child, store, alloc, v);
                    if (store.nulls != null) {
                        try store.appendValidBit(alloc, store.rowCount() - 1, true);
                    }
                }
            }
        }

        fn appendScalar(comptime T: type, store: anytype, alloc: Allocator, v: T) !void {
            switch (T) {
                i8 => try store.data.tinyint.append(alloc, v),
                i16 => try store.data.smallint.append(alloc, v),
                i32 => try store.data.int.append(alloc, v),
                i64 => try store.data.bigint.append(alloc, v),
                i128 => try store.data.largeint.append(alloc, v),
                f32 => try store.data.float.append(alloc, v),
                f64 => try store.data.double.append(alloc, v),
                u128 => try store.data.uuid.append(alloc, v),
                bool => try store.data.boolean.append(alloc, @intFromBool(v)),
                Date => try store.data.date.append(alloc, v.days()),
                DateTime => try store.data.datetime.append(alloc, v.micros()),
                []const u8 => switch (store.data) {
                    .varchar, .string, .char => |*s| try s.appendValue(alloc, v),
                    else => unreachable,
                },
                else => comptime unreachable,
            }
        }
    };
}

/// Reflect a function module (`spec` + `Input` + `Output` + `process`) into
/// the raw engine descriptor. The schemas live in comptime statics; the
/// registry copies them at registration.
pub fn descriptorFor(comptime Mod: type) udf.TableUdf {
    const S = struct {
        const input = schemaFor(Mod.Input);
        const output = schemaFor(Mod.Output);

        fn trampoline(
            raw_ctx: *const udf.TvfContext,
            part: *const udf.TvfPartition,
            out: *udf.TvfOutput,
        ) anyerror!void {
            var ctx = Ctx{ .arena = raw_ctx.arena, .user_data = raw_ctx.user_data };
            const p = Partition(Mod.Input){ .raw = part, .len = part.row_count };
            var w = Writer(Mod.Output){ .raw = out };
            try Mod.process(&ctx, p, &w);
        }
    };
    return .{
        .name = Mod.spec.name,
        .input_schema = &S.input,
        .output_schema = &S.output,
        .execution = Mod.spec.execution,
        .process = S.trampoline,
    };
}
