//! ARRAY and MAP values. An array is one self-contained cell of a
//! variable-length column:
//!
//!   class     u8   how elements are stored and ordered (`ElemClass`)
//!   count     u32  element count, little-endian
//!   validity  ceil(count / 8) bytes; bit i set = element i is not NULL,
//!                  as in a column's bitmap
//!   elements  fixed classes: count values of the class's width,
//!                  little-endian, zero where the element is NULL;
//!             variable classes: count + 1 u32 offsets into the bytes that
//!                  follow, then those bytes; a NULL element is empty
//!
//! A map is its keys and its values as two arrays of one length:
//!
//!   tag       u8   `MAP_TAG`, which no element class takes
//!   keys_len  u32  byte length of the keys array, little-endian
//!   keys      an array of the key type
//!   values    an array of the value type
//!
//! Keys are unique: building a map (`appendMap`) keeps the last entry of a
//! repeated key, where that entry stands.
//!
//! The encoding is canonical: floats are stored canonicalized and NULL slots
//! are zero or empty, so equal values have equal bytes. Grouping, DISTINCT,
//! joins and hashing compare bytes. Ordering decodes (`order`), and needs no
//! type since the leading byte says how to compare.
//!
//! Kernels work on whole batches: `flatten` turns a column of arrays into one
//! flat column of their elements plus per-row offsets, existing kernels run
//! over that column, and `encodeColumn` cuts the result back into arrays.

const std = @import("std");
const Allocator = std.mem.Allocator;

const types = @import("../types.zig");
const Type = types.Type;

const storage = @import("../storage/storage.zig");
const ColumnView = storage.ColumnView;

const store = @import("../engine/store.zig");
const ColumnStore = store.ColumnStore;

const common = @import("scalar_fn_common.zig");
const dec = @import("scalar_fn_decimal.zig");
const json_binary = @import("json_binary.zig");

pub const ElemClass = enum(u8) {
    i8 = 1,
    i16 = 2,
    i32 = 3,
    i64 = 4,
    i128 = 5,
    u128 = 6,
    f32 = 7,
    f64 = 8,
    boolean = 9,
    bytes = 10,
    nested = 11,

    pub fn of(t: Type) ElemClass {
        return switch (t) {
            .tinyint => .i8,
            .smallint => .i16,
            .int, .date => .i32,
            .bigint, .datetime, .decimal64 => .i64,
            .largeint, .decimal128 => .i128,
            .uuid => .u128,
            .float => .f32,
            .double => .f64,
            .boolean => .boolean,
            .varchar, .string, .char, .json => .bytes,
            .nested => .nested,
        };
    }

    pub fn width(c: ElemClass) ?usize {
        return switch (c) {
            .i8, .boolean => 1,
            .i16 => 2,
            .i32, .f32 => 4,
            .i64, .f64 => 8,
            .i128, .u128 => 16,
            .bytes, .nested => null,
        };
    }
};

const HEADER_BYTES = 5;

pub const MAP_TAG: u8 = 0x40;
const MAP_HEADER_BYTES = 5;

pub fn validityBytes(count: usize) usize {
    return (count + 7) >> 3;
}

/// A decoded view over one array value's bytes.
pub const Array = struct {
    class: ElemClass,
    count: u32,
    validity: []const u8,
    body: []const u8,

    /// `bytes` must be well-formed (`isWellFormed`): every value a column
    /// holds was built by `appendEncoded` or checked on the way in.
    pub fn decode(bytes: []const u8) Array {
        const count = std.mem.readInt(u32, bytes[1..5], .little);
        const vb = validityBytes(count);
        return .{
            .class = @enumFromInt(bytes[0]),
            .count = count,
            .validity = bytes[HEADER_BYTES..][0..vb],
            .body = bytes[HEADER_BYTES + vb ..],
        };
    }

    pub fn isValid(self: Array, i: usize) bool {
        return (self.validity[i >> 3] >> @intCast(i & 7)) & 1 == 1;
    }

    pub fn element(self: Array, i: usize) []const u8 {
        if (self.class.width()) |w| return self.body[i * w ..][0..w];
        const data = self.body[4 * (@as(usize, self.count) + 1) ..];
        const start = std.mem.readInt(u32, self.body[4 * i ..][0..4], .little);
        const end = std.mem.readInt(u32, self.body[4 * (i + 1) ..][0..4], .little);
        return data[start..end];
    }
};

/// A decoded view over one map value's bytes: two arrays of one count.
pub const Map = struct {
    keys: []const u8,
    values: []const u8,

    pub fn decode(bytes: []const u8) Map {
        const keys_len = std.mem.readInt(u32, bytes[1..5], .little);
        return .{
            .keys = bytes[MAP_HEADER_BYTES..][0..keys_len],
            .values = bytes[MAP_HEADER_BYTES + keys_len ..],
        };
    }

    pub fn count(self: Map) u32 {
        return Array.decode(self.keys).count;
    }
};

/// Whether `bytes` is a value of the nested type `t`, with every length and
/// offset in bounds, nested elements included.
pub fn isWellFormed(bytes: []const u8, t: Type) bool {
    if (t.mapTypes()) |m| {
        if (bytes.len < MAP_HEADER_BYTES or bytes[0] != MAP_TAG) return false;
        const keys_len = std.mem.readInt(u32, bytes[1..5], .little);
        if (bytes.len - MAP_HEADER_BYTES < keys_len) return false;
        const map = Map.decode(bytes);
        if (!arrayWellFormed(map.keys, m.key) or !arrayWellFormed(map.values, m.value)) return false;
        return Array.decode(map.keys).count == Array.decode(map.values).count;
    }
    return arrayWellFormed(bytes, t.arrayElement() orelse return false);
}

fn arrayWellFormed(bytes: []const u8, element: Type) bool {
    if (bytes.len < HEADER_BYTES) return false;
    const class = ElemClass.of(element);
    if (bytes[0] != @intFromEnum(class)) return false;
    const count: usize = std.mem.readInt(u32, bytes[1..5], .little);
    const vb = validityBytes(count);
    if (bytes.len - HEADER_BYTES < vb) return false;
    const body = bytes[HEADER_BYTES + vb ..];
    if (class.width()) |w| return body.len == count * w;
    if (body.len < 4 * (count + 1)) return false;
    const data = body[4 * (count + 1) ..];
    var prev: u32 = 0;
    for (0..count + 1) |i| {
        const off = std.mem.readInt(u32, body[4 * i ..][0..4], .little);
        if (off < prev or off > data.len) return false;
        prev = off;
    }
    if (prev != data.len) return false;
    if (class == .nested) {
        const a = Array.decode(bytes);
        for (0..count) |i| {
            if (a.isValid(i) and !isWellFormed(a.element(i), element)) return false;
        }
    }
    return true;
}

/// Append one array to `out`: rows [start, end) of `elements`, a flat view
/// of the element type.
pub fn appendEncoded(allocator: Allocator, out: *std.ArrayList(u8), class: ElemClass, elements: ColumnView, start: usize, end: usize) !void {
    try appendEncodedRows(allocator, out, class, elements, RowRange{ .start = start, .end = end });
}

const RowRange = struct {
    start: usize,
    end: usize,

    fn len(self: RowRange) usize {
        return self.end - self.start;
    }

    fn at(self: RowRange, i: usize) usize {
        return self.start + i;
    }
};

const PickedRows = struct {
    rows: []const u32,

    fn len(self: PickedRows) usize {
        return self.rows.len;
    }

    fn at(self: PickedRows, i: usize) usize {
        return self.rows[i];
    }
};

/// Append one array of the rows of `elements` that `rows`, a `RowRange` or
/// `PickedRows`, names.
fn appendEncodedRows(allocator: Allocator, out: *std.ArrayList(u8), class: ElemClass, elements: ColumnView, rows: anytype) !void {
    const count = rows.len();
    try out.append(allocator, @intFromEnum(class));
    var count_buf: [4]u8 = undefined;
    std.mem.writeInt(u32, &count_buf, @intCast(count), .little);
    try out.appendSlice(allocator, &count_buf);
    const vb_at = out.items.len;
    try out.appendNTimes(allocator, 0, validityBytes(count));
    for (0..count) |i| {
        if (elements.isValid(rows.at(i))) out.items[vb_at + (i >> 3)] |= @as(u8, 1) << @intCast(i & 7);
    }
    if (class.width()) |w| {
        const at = out.items.len;
        try out.appendNTimes(allocator, 0, count * w);
        for (0..count) |i| {
            if (!elements.isValid(rows.at(i))) continue;
            writeFixed(out.items[at + i * w ..][0..w], elements, rows.at(i));
        }
        return;
    }
    const offsets_at = out.items.len;
    try out.appendNTimes(allocator, 0, 4 * (count + 1));
    const data_at = out.items.len;
    for (0..count) |i| {
        if (elements.isValid(rows.at(i))) try out.appendSlice(allocator, varBytes(elements, rows.at(i)));
        std.mem.writeInt(u32, out.items[offsets_at + 4 * (i + 1) ..][0..4], @intCast(out.items.len - data_at), .little);
    }
}

/// Append one map of `map_types`: the entries of rows [start, end) of
/// `keys` and `values`, flat columns of the key and value types, with a
/// repeated key's last entry kept where it stands. `scratch` is working
/// space a caller reuses across calls.
pub fn appendMap(
    allocator: Allocator,
    out: *std.ArrayList(u8),
    map_types: types.MapTypes,
    keys: ColumnView,
    values: ColumnView,
    start: usize,
    end: usize,
    scratch: *std.ArrayList(u32),
) !void {
    try uniqueKeyRows(allocator, ViewKeys{ .view = keys }, start, end, scratch);
    try out.append(allocator, MAP_TAG);
    const len_at = out.items.len;
    try out.appendNTimes(allocator, 0, 4);
    const keys_at = out.items.len;
    const picked: PickedRows = .{ .rows = scratch.items };
    try appendEncodedRows(allocator, out, ElemClass.of(map_types.key), keys, picked);
    std.mem.writeInt(u32, out.items[len_at..][0..4], @intCast(out.items.len - keys_at), .little);
    try appendEncodedRows(allocator, out, ElemClass.of(map_types.value), values, picked);
}

/// Up to this many entries, a map finds repeated keys by comparing every
/// pair; past it, by sorting.
const PAIRWISE_REPEATS_MAX = 16;

/// The rows of [start, end) of `keys` that no later row repeats, in order.
/// `keys` is a `ViewKeys` or `ArrayKeys`.
fn uniqueKeyRows(allocator: Allocator, keys: anytype, start: usize, end: usize, rows: *std.ArrayList(u32)) !void {
    rows.clearRetainingCapacity();
    if (end - start <= PAIRWISE_REPEATS_MAX) {
        outer: for (start..end) |i| {
            for (i + 1..end) |j| if (keys.order(i, j) == .eq) continue :outer;
            try rows.append(allocator, @intCast(i));
        }
        return;
    }
    for (start..end) |i| try rows.append(allocator, @intCast(i));
    // A stable sort keeps one key's rows in order, so each run ends at its
    // last.
    std.sort.block(u32, rows.items, keys, struct {
        fn lessThan(k: @TypeOf(keys), x: u32, y: u32) bool {
            return k.order(x, y) == .lt;
        }
    }.lessThan);
    var kept: usize = 0;
    for (rows.items, 0..) |row, i| {
        if (i + 1 < rows.items.len and keys.order(row, rows.items[i + 1]) == .eq) continue;
        rows.items[kept] = row;
        kept += 1;
    }
    rows.shrinkRetainingCapacity(kept);
    std.mem.sort(u32, rows.items, {}, std.sort.asc(u32));
}

/// Keys as rows of a flat column.
const ViewKeys = struct {
    view: ColumnView,

    /// Two rows in element order, NULL first.
    fn order(self: ViewKeys, i: usize, j: usize) std.math.Order {
        const iv = self.view.isValid(i);
        const jv = self.view.isValid(j);
        if (!iv or !jv) return if (iv == jv) .eq else if (iv) .gt else .lt;
        var bi: [16]u8 = undefined;
        var bj: [16]u8 = undefined;
        return elementOrder(classOfView(self.view), elementBytes(self.view, i, &bi), elementBytes(self.view, j, &bj));
    }
};

/// Keys as the elements of an encoded array.
const ArrayKeys = struct {
    array: Array,

    fn order(self: ArrayKeys, i: usize, j: usize) std.math.Order {
        const iv = self.array.isValid(i);
        const jv = self.array.isValid(j);
        if (!iv or !jv) return if (iv == jv) .eq else if (iv) .gt else .lt;
        return elementOrder(self.array.class, self.array.element(i), self.array.element(j));
    }
};

/// Append the map of every entry of `parts`, maps of one type, one map
/// after another: a key in several keeps its last entry, where it stands.
pub fn appendMapConcat(allocator: Allocator, out: *std.ArrayList(u8), parts: []const []const u8) !void {
    const key_parts = try allocator.alloc([]const u8, parts.len);
    defer allocator.free(key_parts);
    const value_parts = try allocator.alloc([]const u8, parts.len);
    defer allocator.free(value_parts);
    for (parts, key_parts, value_parts) |p, *k, *v| {
        const map = Map.decode(p);
        k.* = map.keys;
        v.* = map.values;
    }
    var keys_buf: std.ArrayList(u8) = .empty;
    defer keys_buf.deinit(allocator);
    try appendConcat(allocator, &keys_buf, Array.decode(key_parts[0]).class, key_parts);
    var values_buf: std.ArrayList(u8) = .empty;
    defer values_buf.deinit(allocator);
    try appendConcat(allocator, &values_buf, Array.decode(value_parts[0]).class, value_parts);
    const keys = Array.decode(keys_buf.items);
    var picked: std.ArrayList(u32) = .empty;
    defer picked.deinit(allocator);
    try uniqueKeyRows(allocator, ArrayKeys{ .array = keys }, 0, keys.count, &picked);
    try out.append(allocator, MAP_TAG);
    const len_at = out.items.len;
    try out.appendNTimes(allocator, 0, 4);
    const keys_at = out.items.len;
    try appendPickedElements(allocator, out, keys, picked.items);
    std.mem.writeInt(u32, out.items[len_at..][0..4], @intCast(out.items.len - keys_at), .little);
    try appendPickedElements(allocator, out, Array.decode(values_buf.items), picked.items);
}

/// Append the array of `a`'s elements at `picked`, in that order.
fn appendPickedElements(allocator: Allocator, out: *std.ArrayList(u8), a: Array, picked: []const u32) !void {
    try out.append(allocator, @intFromEnum(a.class));
    var count_buf: [4]u8 = undefined;
    std.mem.writeInt(u32, &count_buf, @intCast(picked.len), .little);
    try out.appendSlice(allocator, &count_buf);
    const vb_at = out.items.len;
    try out.appendNTimes(allocator, 0, validityBytes(picked.len));
    for (picked, 0..) |p, i| {
        if (a.isValid(p)) out.items[vb_at + (i >> 3)] |= @as(u8, 1) << @intCast(i & 7);
    }
    if (a.class.width() != null) {
        for (picked) |p| try out.appendSlice(allocator, a.element(p));
        return;
    }
    const offsets_at = out.items.len;
    try out.appendNTimes(allocator, 0, 4 * (picked.len + 1));
    const data_at = out.items.len;
    for (picked, 0..) |p, i| {
        try out.appendSlice(allocator, a.element(p));
        std.mem.writeInt(u32, out.items[offsets_at + 4 * (i + 1) ..][0..4], @intCast(out.items.len - data_at), .little);
    }
}

fn classOfView(v: ColumnView) ElemClass {
    return switch (v.data) {
        .tinyint => .i8,
        .smallint => .i16,
        .int, .date => .i32,
        .bigint, .datetime, .decimal64 => .i64,
        .largeint, .decimal128 => .i128,
        .uuid => .u128,
        .float => .f32,
        .double => .f64,
        .boolean => .boolean,
        .varchar, .string, .char, .json => .bytes,
        .nested => .nested,
    };
}

/// Row `row` of `v` as an array stores it as an element.
fn elementBytes(v: ColumnView, row: usize, buf: *[16]u8) []const u8 {
    const w = classOfView(v).width() orelse return varBytes(v, row);
    writeFixed(buf[0..w], v, row);
    return buf[0..w];
}

fn varBytes(v: ColumnView, row: usize) []const u8 {
    return switch (v.data) {
        .varchar, .string, .char, .json, .nested => |sv| sv.rowBytes(row),
        else => unreachable,
    };
}

fn writeFixed(dst: []u8, v: ColumnView, row: usize) void {
    switch (v.data) {
        .boolean => |s| dst[0] = @intFromBool(s[row] != 0),
        .float => |s| std.mem.writeInt(u32, dst[0..4], types.canonicalFloatBits(s[row]), .little),
        .double => |s| std.mem.writeInt(u64, dst[0..8], types.canonicalFloatBits(s[row]), .little),
        .varchar, .string, .char, .json, .nested => unreachable,
        inline else => |s| {
            const T = std.meta.Elem(@TypeOf(s));
            std.mem.writeInt(T, dst[0..@sizeOf(T)], s[row], .little);
        },
    }
}

/// Append the array of `parts`' elements, one array after another, all of
/// element class `class`.
pub fn appendConcat(allocator: Allocator, out: *std.ArrayList(u8), class: ElemClass, parts: []const []const u8) !void {
    var count: usize = 0;
    for (parts) |p| count += Array.decode(p).count;
    try out.append(allocator, @intFromEnum(class));
    var count_buf: [4]u8 = undefined;
    std.mem.writeInt(u32, &count_buf, @intCast(count), .little);
    try out.appendSlice(allocator, &count_buf);
    const vb_at = out.items.len;
    try out.appendNTimes(allocator, 0, validityBytes(count));
    var at: usize = 0;
    for (parts) |p| {
        const a = Array.decode(p);
        for (0..a.count) |i| {
            if (a.isValid(i)) out.items[vb_at + (at >> 3)] |= @as(u8, 1) << @intCast(at & 7);
            at += 1;
        }
    }
    if (class.width() != null) {
        for (parts) |p| try out.appendSlice(allocator, Array.decode(p).body);
        return;
    }
    const offsets_at = out.items.len;
    try out.appendNTimes(allocator, 0, 4 * (count + 1));
    const data_at = out.items.len;
    at = 0;
    for (parts) |p| {
        const a = Array.decode(p);
        for (0..a.count) |i| {
            try out.appendSlice(allocator, a.element(i));
            at += 1;
            std.mem.writeInt(u32, out.items[offsets_at + 4 * at ..][0..4], @intCast(out.items.len - data_at), .little);
        }
    }
}

/// Build an ARRAY column of `rows` rows: row r holds elements
/// [offsets[r], offsets[r + 1]) of `elements`, unless `row_validity` clears
/// its bit, which makes it NULL.
pub fn encodeColumn(
    allocator: Allocator,
    out: *ColumnStore,
    class: ElemClass,
    elements: ColumnView,
    offsets: []const u32,
    row_validity: ?[]const u8,
    rows: usize,
) !void {
    var cell: std.ArrayList(u8) = .empty;
    defer cell.deinit(allocator);
    const ss = &out.data.nested;
    for (0..rows) |r| {
        const valid = storage.column.isValidBit(row_validity, r);
        const at = out.rowCount();
        if (valid) {
            cell.clearRetainingCapacity();
            try appendEncoded(allocator, &cell, class, elements, offsets[r], offsets[r + 1]);
            try ss.appendValue(allocator, cell.items);
        } else {
            try ss.appendValue(allocator, "");
        }
        try out.appendValidBit(allocator, at, valid);
    }
}

/// The elements of a column of arrays, as one flat column, with row r's
/// elements at [offsets[r], offsets[r + 1]). A NULL row has none.
pub const Flat = struct {
    elements: ColumnStore,
    offsets: std.ArrayList(u32),

    pub fn deinit(self: *Flat, allocator: Allocator) void {
        self.elements.deinit(allocator);
        self.offsets.deinit(allocator);
        self.* = undefined;
    }
};

pub fn flatten(allocator: Allocator, element: Type, arrays: ColumnView, rows: usize) !Flat {
    var flat: Flat = .{ .elements = try ColumnStore.init(allocator, element, true), .offsets = .empty };
    errdefer flat.deinit(allocator);
    try flat.offsets.ensureTotalCapacity(allocator, rows + 1);
    flat.offsets.appendAssumeCapacity(0);
    const sv = arrays.data.nested;
    for (0..rows) |r| {
        if (arrays.isValid(r)) {
            const a = Array.decode(sv.rowBytes(r));
            for (0..a.count) |i| {
                if (a.isValid(i)) try appendRaw(allocator, &flat.elements, a.element(i)) else try flat.elements.appendNulls(allocator, 1);
            }
        }
        flat.offsets.appendAssumeCapacity(@intCast(flat.elements.rowCount()));
    }
    return flat;
}

/// The entries of a column of maps, as flat key and value columns, with row
/// r's entries at [offsets[r], offsets[r + 1]). A NULL row has none.
pub const FlatMap = struct {
    keys: ColumnStore,
    values: ColumnStore,
    offsets: std.ArrayList(u32),

    pub fn deinit(self: *FlatMap, allocator: Allocator) void {
        self.keys.deinit(allocator);
        self.values.deinit(allocator);
        self.offsets.deinit(allocator);
        self.* = undefined;
    }
};

pub fn flattenMap(allocator: Allocator, map_types: types.MapTypes, maps: ColumnView, rows: usize) !FlatMap {
    var keys = try ColumnStore.init(allocator, map_types.key, true);
    errdefer keys.deinit(allocator);
    var values = try ColumnStore.init(allocator, map_types.value, true);
    errdefer values.deinit(allocator);
    var flat: FlatMap = .{ .keys = keys, .values = values, .offsets = .empty };
    errdefer flat.offsets.deinit(allocator);
    try flat.offsets.ensureTotalCapacity(allocator, rows + 1);
    flat.offsets.appendAssumeCapacity(0);
    const sv = maps.data.nested;
    for (0..rows) |r| {
        if (maps.isValid(r)) {
            const map = Map.decode(sv.rowBytes(r));
            try appendElements(allocator, &flat.keys, Array.decode(map.keys));
            try appendElements(allocator, &flat.values, Array.decode(map.values));
        }
        flat.offsets.appendAssumeCapacity(@intCast(flat.keys.rowCount()));
    }
    return flat;
}

fn appendElements(allocator: Allocator, out: *ColumnStore, a: Array) !void {
    for (0..a.count) |i| {
        if (a.isValid(i)) try appendRaw(allocator, out, a.element(i)) else try out.appendNulls(allocator, 1);
    }
}

/// Build a MAP column of `rows` rows: row r holds the entries
/// [offsets[r], offsets[r + 1]) of `keys` and `values` (`appendMap`),
/// unless `row_validity` clears its bit, which makes it NULL.
pub fn encodeMapColumn(
    allocator: Allocator,
    out: *ColumnStore,
    map_types: types.MapTypes,
    keys: ColumnView,
    values: ColumnView,
    offsets: []const u32,
    row_validity: ?[]const u8,
    rows: usize,
) !void {
    var cell: std.ArrayList(u8) = .empty;
    defer cell.deinit(allocator);
    var scratch: std.ArrayList(u32) = .empty;
    defer scratch.deinit(allocator);
    const ss = &out.data.nested;
    for (0..rows) |r| {
        const valid = storage.column.isValidBit(row_validity, r);
        const at = out.rowCount();
        if (valid) {
            cell.clearRetainingCapacity();
            try appendMap(allocator, &cell, map_types, keys, values, offsets[r], offsets[r + 1], &scratch);
            try ss.appendValue(allocator, cell.items);
        } else {
            try ss.appendValue(allocator, "");
        }
        try out.appendValidBit(allocator, at, valid);
    }
}

/// Append one element's stored bytes to a store of the element type.
pub fn appendRaw(allocator: Allocator, out: *ColumnStore, raw: []const u8) !void {
    const at = out.rowCount();
    switch (out.data) {
        .varchar, .string, .char, .json, .nested => |*ss| try ss.appendValue(allocator, raw),
        .boolean => |*l| try l.append(allocator, raw[0]),
        .float => |*l| try l.append(allocator, @bitCast(std.mem.readInt(u32, raw[0..4], .little))),
        .double => |*l| try l.append(allocator, @bitCast(std.mem.readInt(u64, raw[0..8], .little))),
        inline else => |*l| {
            const T = std.meta.Elem(@TypeOf(l.items));
            try l.append(allocator, std.mem.readInt(T, raw[0..@sizeOf(T)], .little));
        },
    }
    try out.appendValidBit(allocator, at, true);
}

/// Total order over values of one nested type. Arrays order element by
/// element, a NULL element before any value, then the shorter array first;
/// maps by their keys, then their values.
pub fn order(a_bytes: []const u8, b_bytes: []const u8) std.math.Order {
    // A NULL row's cell is empty; callers order NULLs themselves, but the
    // comparison must not read past it.
    if (a_bytes.len < HEADER_BYTES or b_bytes.len < HEADER_BYTES) return std.math.order(a_bytes.len, b_bytes.len);
    if (a_bytes[0] != b_bytes[0]) return std.math.order(a_bytes[0], b_bytes[0]);
    if (a_bytes[0] == MAP_TAG) {
        const a = Map.decode(a_bytes);
        const b = Map.decode(b_bytes);
        const by_keys = order(a.keys, b.keys);
        return if (by_keys != .eq) by_keys else order(a.values, b.values);
    }
    const a = Array.decode(a_bytes);
    const b = Array.decode(b_bytes);
    for (0..@min(a.count, b.count)) |i| {
        const av = a.isValid(i);
        const bv = b.isValid(i);
        if (!av or !bv) {
            if (av == bv) continue;
            return if (av) .gt else .lt;
        }
        const o = elementOrder(a.class, a.element(i), b.element(i));
        if (o != .eq) return o;
    }
    return std.math.order(a.count, b.count);
}

fn elementOrder(class: ElemClass, x: []const u8, y: []const u8) std.math.Order {
    return switch (class) {
        .i8 => std.math.order(@as(i8, @bitCast(x[0])), @as(i8, @bitCast(y[0]))),
        .boolean => std.math.order(x[0], y[0]),
        .i16 => std.math.order(std.mem.readInt(i16, x[0..2], .little), std.mem.readInt(i16, y[0..2], .little)),
        .i32 => std.math.order(std.mem.readInt(i32, x[0..4], .little), std.mem.readInt(i32, y[0..4], .little)),
        .i64 => std.math.order(std.mem.readInt(i64, x[0..8], .little), std.mem.readInt(i64, y[0..8], .little)),
        .i128 => std.math.order(std.mem.readInt(i128, x[0..16], .little), std.mem.readInt(i128, y[0..16], .little)),
        .u128 => std.math.order(std.mem.readInt(u128, x[0..16], .little), std.mem.readInt(u128, y[0..16], .little)),
        .f32 => types.floatOrder(@as(f32, @bitCast(std.mem.readInt(u32, x[0..4], .little))), @as(f32, @bitCast(std.mem.readInt(u32, y[0..4], .little)))),
        .f64 => types.floatOrder(@as(f64, @bitCast(std.mem.readInt(u64, x[0..8], .little))), @as(f64, @bitCast(std.mem.readInt(u64, y[0..8], .little)))),
        .bytes => std.mem.order(u8, x, y),
        .nested => order(x, y),
    };
}

pub const TextError = Allocator.Error || error{NoSpaceLeft};

pub const TextStyle = enum {
    /// `[1,2,null]`, `["a","b"]`: StarRocks' rendering.
    starrocks,
    /// `{1,2,NULL}`, `{a,"b c"}`: PostgreSQL's array literal.
    postgres,
};

/// Append the text of one value of the nested type `t` to `out`. A map is
/// `{k:v,...}` in either style, as StarRocks writes it.
pub fn appendText(allocator: Allocator, out: *std.ArrayList(u8), t: Type, bytes: []const u8, style: TextStyle) TextError!void {
    if (t.mapTypes()) |m| {
        const map = Map.decode(bytes);
        const keys = Array.decode(map.keys);
        const values = Array.decode(map.values);
        try out.append(allocator, '{');
        for (0..keys.count) |i| {
            if (i > 0) try out.append(allocator, ',');
            try appendSlotText(allocator, out, m.key, keys, i, style);
            try out.append(allocator, ':');
            try appendSlotText(allocator, out, m.value, values, i, style);
        }
        return out.append(allocator, '}');
    }
    const element = t.arrayElement().?;
    const a = Array.decode(bytes);
    try out.append(allocator, if (style == .starrocks) '[' else '{');
    for (0..a.count) |i| {
        if (i > 0) try out.append(allocator, ',');
        if (!a.isValid(i)) {
            try out.appendSlice(allocator, if (style == .starrocks) "null" else "NULL");
            continue;
        }
        try appendElementText(allocator, out, element, a.element(i), style);
    }
    try out.append(allocator, if (style == .starrocks) ']' else '}');
}

fn appendSlotText(allocator: Allocator, out: *std.ArrayList(u8), t: Type, a: Array, i: usize, style: TextStyle) TextError!void {
    if (!a.isValid(i)) return out.appendSlice(allocator, if (style == .starrocks) "null" else "NULL");
    try appendElementText(allocator, out, t, a.element(i), style);
}

fn appendElementText(allocator: Allocator, out: *std.ArrayList(u8), t: Type, raw: []const u8, style: TextStyle) TextError!void {
    var buf: [64]u8 = undefined;
    switch (t) {
        .nested => {
            // A map element of a PostgreSQL array is a quoted string; an
            // array element nests in its own braces.
            if (style == .starrocks or t.mapTypes() == null) return appendText(allocator, out, t, raw, style);
            var text: std.ArrayList(u8) = .empty;
            defer text.deinit(allocator);
            try appendText(allocator, &text, t, raw, style);
            try appendQuoted(allocator, out, text.items, style);
        },
        .json => {
            var text: std.ArrayList(u8) = .empty;
            defer text.deinit(allocator);
            if (json_binary.looksBinary(raw)) try json_binary.toText(allocator, &text, raw) else try text.appendSlice(allocator, raw);
            if (style == .starrocks) try out.appendSlice(allocator, text.items) else try appendQuoted(allocator, out, text.items, style);
        },
        .varchar, .string, .char => try appendQuoted(allocator, out, raw, style),
        .boolean => try out.appendSlice(allocator, if (style == .postgres) (if (raw[0] != 0) "t" else "f") else (if (raw[0] != 0) "1" else "0")),
        .date => try appendQuoted(allocator, out, try common.formatDate(&buf, std.mem.readInt(i32, raw[0..4], .little)), style),
        .datetime => try appendQuoted(allocator, out, try common.formatDateTime(&buf, std.mem.readInt(i64, raw[0..8], .little)), style),
        .uuid => {
            var be: [16]u8 = undefined;
            std.mem.writeInt(u128, &be, std.mem.readInt(u128, raw[0..16], .little), .big);
            const hex = std.fmt.bytesToHex(be, .lower);
            const text = try std.fmt.bufPrint(&buf, "{s}-{s}-{s}-{s}-{s}", .{ hex[0..8], hex[8..12], hex[12..16], hex[16..20], hex[20..32] });
            try appendQuoted(allocator, out, text, style);
        },
        .float => {
            var fbuf: [common.FLOAT_TEXT_MAX]u8 = undefined;
            try out.appendSlice(allocator, common.floatText(&fbuf, @as(f32, @bitCast(std.mem.readInt(u32, raw[0..4], .little))), .plain));
        },
        .double => {
            var fbuf: [common.FLOAT_TEXT_MAX]u8 = undefined;
            try out.appendSlice(allocator, common.floatText(&fbuf, @as(f64, @bitCast(std.mem.readInt(u64, raw[0..8], .little))), .plain));
        },
        .decimal64 => |spec| try out.appendSlice(allocator, dec.formatDecimal(&buf, std.mem.readInt(i64, raw[0..8], .little), spec.s)),
        .decimal128 => |spec| try out.appendSlice(allocator, dec.formatDecimal(&buf, std.mem.readInt(i128, raw[0..16], .little), spec.s)),
        .tinyint => try out.appendSlice(allocator, try std.fmt.bufPrint(&buf, "{d}", .{@as(i8, @bitCast(raw[0]))})),
        .smallint => try out.appendSlice(allocator, try std.fmt.bufPrint(&buf, "{d}", .{std.mem.readInt(i16, raw[0..2], .little)})),
        .int => try out.appendSlice(allocator, try std.fmt.bufPrint(&buf, "{d}", .{std.mem.readInt(i32, raw[0..4], .little)})),
        .bigint => try out.appendSlice(allocator, try std.fmt.bufPrint(&buf, "{d}", .{std.mem.readInt(i64, raw[0..8], .little)})),
        .largeint => try out.appendSlice(allocator, try std.fmt.bufPrint(&buf, "{d}", .{std.mem.readInt(i128, raw[0..16], .little)})),
    }
}

fn appendQuoted(allocator: Allocator, out: *std.ArrayList(u8), text: []const u8, style: TextStyle) !void {
    if (style == .postgres and !postgresNeedsQuotes(text)) return out.appendSlice(allocator, text);
    try out.append(allocator, '"');
    for (text) |c| {
        if (c == '"' or c == '\\') try out.append(allocator, '\\');
        try out.append(allocator, c);
    }
    try out.append(allocator, '"');
}

fn postgresNeedsQuotes(text: []const u8) bool {
    if (text.len == 0 or std.ascii.eqlIgnoreCase(text, "null")) return true;
    for (text) |c| switch (c) {
        '{', '}', ',', '"', '\\', ' ', '\t', '\n', '\r' => return true,
        else => {},
    };
    return false;
}

pub const ParseError = error{InvalidNestedText} || Allocator.Error;

/// Encode the value of the nested type `t` that `text` spells: an array as
/// StarRocks' `[1, 2, null]` (strings in single or double quotes) or
/// PostgreSQL's `{1,2,NULL}`, a map as StarRocks' `{"a":1,"b":null}`. An
/// element that doesn't read as its type is NULL, as a CAST of its text is;
/// text of another shape fails.
pub fn parseText(allocator: Allocator, t: Type, text: []const u8, out: *std.ArrayList(u8)) ParseError!void {
    var reader: TextReader = .{ .text = text };
    reader.skipSpace();
    try reader.parseValue(allocator, t, out);
    reader.skipSpace();
    if (reader.pos != text.len) return error.InvalidNestedText;
}

const TextReader = struct {
    text: []const u8,
    pos: usize = 0,

    fn skipSpace(r: *TextReader) void {
        while (r.pos < r.text.len and std.ascii.isWhitespace(r.text[r.pos])) r.pos += 1;
    }

    fn parseValue(r: *TextReader, allocator: Allocator, t: Type, out: *std.ArrayList(u8)) ParseError!void {
        if (t.mapTypes()) |m| return r.parseMap(allocator, m, out);
        return r.parseArray(allocator, t.arrayElement() orelse return error.InvalidNestedText, out);
    }

    fn parseMap(r: *TextReader, allocator: Allocator, m: types.MapTypes, out: *std.ArrayList(u8)) ParseError!void {
        if (r.pos >= r.text.len or r.text[r.pos] != '{') return error.InvalidNestedText;
        r.pos += 1;
        var keys = try ColumnStore.init(allocator, m.key, true);
        defer keys.deinit(allocator);
        var values = try ColumnStore.init(allocator, m.value, true);
        defer values.deinit(allocator);
        var scratch: std.ArrayList(u8) = .empty;
        defer scratch.deinit(allocator);
        r.skipSpace();
        if (r.pos < r.text.len and r.text[r.pos] == '}') {
            r.pos += 1;
        } else while (true) {
            try r.parseSlot(allocator, &keys, m.key, ':', &scratch);
            r.skipSpace();
            if (r.pos >= r.text.len or r.text[r.pos] != ':') return error.InvalidNestedText;
            r.pos += 1;
            try r.parseSlot(allocator, &values, m.value, '}', &scratch);
            r.skipSpace();
            if (r.pos >= r.text.len) return error.InvalidNestedText;
            if (r.text[r.pos] == ',') {
                r.pos += 1;
                continue;
            }
            if (r.text[r.pos] != '}') return error.InvalidNestedText;
            r.pos += 1;
            break;
        }
        var rows: std.ArrayList(u32) = .empty;
        defer rows.deinit(allocator);
        try appendMap(allocator, out, m, keys.view(), values.view(), 0, keys.rowCount(), &rows);
    }

    /// One element of `t` onto `slots`, the text before `close` or a comma.
    fn parseSlot(r: *TextReader, allocator: Allocator, slots: *ColumnStore, t: Type, close: u8, scratch: *std.ArrayList(u8)) ParseError!void {
        r.skipSpace();
        if (r.pos >= r.text.len) return error.InvalidNestedText;
        if (t == .nested) {
            if (r.bareWordIs("null")) return slots.appendNulls(allocator, 1);
            scratch.clearRetainingCapacity();
            try r.parseValue(allocator, t, scratch);
            return appendRaw(allocator, slots, scratch.items);
        }
        const token = try r.scalarToken(allocator, close, scratch) orelse return slots.appendNulls(allocator, 1);
        if (!try appendTextElement(allocator, slots, t, token)) try slots.appendNulls(allocator, 1);
    }

    fn parseArray(r: *TextReader, allocator: Allocator, element: Type, out: *std.ArrayList(u8)) ParseError!void {
        if (r.pos >= r.text.len) return error.InvalidNestedText;
        const close: u8 = switch (r.text[r.pos]) {
            '[' => ']',
            '{' => '}',
            else => return error.InvalidNestedText,
        };
        r.pos += 1;
        var elements = try ColumnStore.init(allocator, element, true);
        defer elements.deinit(allocator);
        var scratch: std.ArrayList(u8) = .empty;
        defer scratch.deinit(allocator);
        r.skipSpace();
        if (r.pos < r.text.len and r.text[r.pos] == close) {
            r.pos += 1;
        } else while (true) {
            try r.parseSlot(allocator, &elements, element, close, &scratch);
            r.skipSpace();
            if (r.pos >= r.text.len) return error.InvalidNestedText;
            if (r.text[r.pos] == ',') {
                r.pos += 1;
                continue;
            }
            if (r.text[r.pos] != close) return error.InvalidNestedText;
            r.pos += 1;
            break;
        }
        try appendEncoded(allocator, out, ElemClass.of(element), elements.view(), 0, elements.rowCount());
    }

    fn bareWordIs(r: *TextReader, word: []const u8) bool {
        if (r.text.len - r.pos < word.len or !std.ascii.eqlIgnoreCase(r.text[r.pos..][0..word.len], word)) return false;
        const after = r.pos + word.len;
        if (after < r.text.len and (std.ascii.isAlphanumeric(r.text[after]) or r.text[after] == '_')) return false;
        r.pos = after;
        return true;
    }

    /// The next element's text, unquoted, or null for an unquoted NULL.
    /// It ends at a comma or `close` outside brackets.
    fn scalarToken(r: *TextReader, allocator: Allocator, close: u8, scratch: *std.ArrayList(u8)) ParseError!?[]const u8 {
        const c = r.text[r.pos];
        if (c == '"' or c == '\'') {
            r.pos += 1;
            scratch.clearRetainingCapacity();
            while (r.pos < r.text.len and r.text[r.pos] != c) : (r.pos += 1) {
                if (r.text[r.pos] == '\\' and r.pos + 1 < r.text.len) r.pos += 1;
                try scratch.append(allocator, r.text[r.pos]);
            }
            if (r.pos >= r.text.len) return error.InvalidNestedText;
            r.pos += 1;
            return scratch.items;
        }
        const start = r.pos;
        var depth: usize = 0;
        while (r.pos < r.text.len) : (r.pos += 1) {
            const ch = r.text[r.pos];
            if (ch == '{' or ch == '[') depth += 1;
            if (ch == '}' or ch == ']') {
                if (depth == 0) break;
                depth -= 1;
            }
            if (depth == 0 and (ch == ',' or ch == close)) break;
        }
        const token = std.mem.trim(u8, r.text[start..r.pos], " \t\r\n");
        if (token.len == 0) return error.InvalidNestedText;
        if (std.ascii.eqlIgnoreCase(token, "null")) return null;
        return token;
    }
};

/// Append the element `text` reads as; false when it reads as no value of
/// `t`, which makes the element NULL.
fn appendTextElement(allocator: Allocator, out: *ColumnStore, t: Type, text: []const u8) !bool {
    const at = out.rowCount();
    switch (out.data) {
        .varchar, .string, .char => |*ss| try ss.appendValue(allocator, text),
        .json => |*ss| {
            const enc = json_binary.encodeFromText(allocator, text) catch return false;
            defer allocator.free(enc);
            try ss.appendValue(allocator, enc);
        },
        .nested => return false,
        .boolean => |*l| try l.append(allocator, @intFromBool(common.textBoolean(text) orelse return false)),
        .float => |*l| try l.append(allocator, @floatCast(common.textDouble(text) orelse return false)),
        .double => |*l| try l.append(allocator, common.textDouble(text) orelse return false),
        .date => |*l| try l.append(allocator, common.textToDate(text) orelse return false),
        .datetime => |*l| try l.append(allocator, common.textToDatetime(text) orelse return false),
        .decimal64 => |*l| try l.append(allocator, std.math.cast(i64, dec.textConstantMantissa(text, t.decimalSpec().?) orelse return false) orelse return false),
        .decimal128 => |*l| try l.append(allocator, dec.textConstantMantissa(text, t.decimalSpec().?) orelse return false),
        .uuid => |*l| try l.append(allocator, parseUuid(text) orelse return false),
        inline .tinyint, .smallint, .int, .bigint, .largeint => |*l| {
            const T = std.meta.Elem(@TypeOf(l.items));
            try l.append(allocator, std.math.cast(T, common.textInteger(text) orelse return false) orelse return false);
        },
    }
    try out.appendValidBit(allocator, at, true);
    return true;
}

fn parseUuid(text: []const u8) ?u128 {
    var v: u128 = 0;
    var digits: usize = 0;
    for (text) |c| {
        if (c == '-') continue;
        const d = std.fmt.charToDigit(c, 16) catch return null;
        v = (v << 4) | d;
        digits += 1;
    }
    return if (digits == 32) v else null;
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

const testing = std.testing;

fn encodeText(allocator: Allocator, t: Type, text: []const u8) ![]u8 {
    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(allocator);
    try parseText(allocator, t, text, &out);
    return out.toOwnedSlice(allocator);
}

fn renderText(allocator: Allocator, t: Type, bytes: []const u8, style: TextStyle) ![]u8 {
    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(allocator);
    try appendText(allocator, &out, t, bytes, style);
    return out.toOwnedSlice(allocator);
}

test "nested: text round-trips through the encoding" {
    const a = testing.allocator;
    const int_array = try types.arrayOf(.int);
    const cases = .{
        .{ int_array, "[1, 2, null, -3]", "[1,2,null,-3]", "{1,2,NULL,-3}" },
        .{ int_array, "[]", "[]", "{}" },
        .{ try types.arrayOf(.string), "['a', \"b c\", null, '']", "[\"a\",\"b c\",null,\"\"]", "{a,\"b c\",NULL,\"\"}" },
        .{ try types.arrayOf(.date), "['2024-01-01', null]", "[\"2024-01-01\",null]", "{2024-01-01,NULL}" },
        .{ try types.arrayOf(.double), "[1.5, -0.0]", "[1.5,0]", "{1.5,0}" },
        .{ try types.arrayOf(.{ .decimal64 = .{ .p = 10, .s = 2 } }), "[1.1, 2]", "[1.10,2.00]", "{1.10,2.00}" },
        .{ try types.arrayOf(int_array), "[[1, 2], null, []]", "[[1,2],null,[]]", "{{1,2},NULL,{}}" },
        .{ int_array, "{1,2,3}", "[1,2,3]", "{1,2,3}" },
    };
    inline for (cases) |c| {
        const bytes = try encodeText(a, c[0], c[1]);
        defer a.free(bytes);
        try testing.expect(isWellFormed(bytes, c[0]));
        const sr = try renderText(a, c[0], bytes, .starrocks);
        defer a.free(sr);
        try testing.expectEqualStrings(c[2], sr);
        const pg = try renderText(a, c[0], bytes, .postgres);
        defer a.free(pg);
        try testing.expectEqualStrings(c[3], pg);
    }
}

test "nested: unreadable elements are NULL, malformed text fails" {
    const a = testing.allocator;
    const int_array = try types.arrayOf(.int);
    const bytes = try encodeText(a, int_array, "[1, x, 3]");
    defer a.free(bytes);
    const text = try renderText(a, int_array, bytes, .starrocks);
    defer a.free(text);
    try testing.expectEqualStrings("[1,null,3]", text);
    inline for (.{ "", "1,2", "[1,2", "[1,,2]", "[1] x" }) |bad| {
        var out: std.ArrayList(u8) = .empty;
        defer out.deinit(a);
        try testing.expectError(error.InvalidNestedText, parseText(a, int_array, bad, &out));
    }
}

test "nested: equal arrays have equal bytes, and order is element-wise" {
    const a = testing.allocator;
    const t = try types.arrayOf(.double);
    const zero = try encodeText(a, t, "[0.0, 1]");
    defer a.free(zero);
    const neg_zero = try encodeText(a, t, "[-0.0, 1]");
    defer a.free(neg_zero);
    try testing.expectEqualSlices(u8, zero, neg_zero);

    const it = try types.arrayOf(.int);
    const texts = [_][]const u8{ "[]", "[null]", "[-5]", "[1, null]", "[1, 2]", "[1, 2, 0]", "[2]" };
    var encoded: [texts.len][]u8 = undefined;
    for (texts, 0..) |s, i| encoded[i] = try encodeText(a, it, s);
    defer for (encoded) |e| a.free(e);
    for (0..texts.len) |i| for (0..texts.len) |j| {
        try testing.expectEqual(std.math.order(i, j), order(encoded[i], encoded[j]));
    };
}

test "nested: map text round-trips, and a repeated key keeps its last entry" {
    const a = testing.allocator;
    const by_name = try types.mapOf(.string, .int);
    const cases = .{
        .{ by_name, "{'a': 1, \"b\": null}", "{\"a\":1,\"b\":null}" },
        .{ by_name, "{}", "{}" },
        .{ by_name, "{'a': 1, 'b': 2, 'a': 3}", "{\"b\":2,\"a\":3}" },
        .{ try types.mapOf(.int, try types.arrayOf(.double)), "{1: [1.5, null], 2: null}", "{1:[1.5,null],2:null}" },
        .{ try types.arrayOf(try types.mapOf(.int, .string)), "[{1: 'x'}, null, {}]", "[{1:\"x\"},null,{}]" },
    };
    inline for (cases) |c| {
        const bytes = try encodeText(a, c[0], c[1]);
        defer a.free(bytes);
        try testing.expect(isWellFormed(bytes, c[0]));
        const sr = try renderText(a, c[0], bytes, .starrocks);
        defer a.free(sr);
        try testing.expectEqualStrings(c[2], sr);
    }
    inline for (.{ "{'a' 1}", "{'a': 1", "['a']", "{'a': 1,}" }) |bad| {
        var out: std.ArrayList(u8) = .empty;
        defer out.deinit(a);
        try testing.expectError(error.InvalidNestedText, parseText(a, by_name, bad, &out));
    }
}

test "nested: maps past the pairwise limit drop repeated keys by sorting" {
    const a = testing.allocator;
    const t = try types.mapOf(.int, .int);
    var text: std.ArrayList(u8) = .empty;
    defer text.deinit(a);
    try text.append(a, '{');
    for (0..40) |i| {
        if (i > 0) try text.append(a, ',');
        try text.print(a, "{d}:{d}", .{ (i * 7) % 20, i });
    }
    try text.append(a, '}');
    const bytes = try encodeText(a, t, text.items);
    defer a.free(bytes);
    const map = Map.decode(bytes);
    try testing.expectEqual(@as(u32, 20), map.count());
    const keys = Array.decode(map.keys);
    const values = Array.decode(map.values);
    for (0..keys.count) |i| {
        const k = std.mem.readInt(i32, keys.element(i)[0..4], .little);
        const v = std.mem.readInt(i32, values.element(i)[0..4], .little);
        try testing.expectEqual(@as(i32, @intCast((@as(usize, @intCast(v)) * 7) % 20)), k);
        try testing.expect(v >= 20);
    }
}

test "nested: concatenated maps keep each key's last entry" {
    const a = testing.allocator;
    const t = try types.mapOf(.string, .int);
    const first = try encodeText(a, t, "{'a': 1, 'b': 2}");
    defer a.free(first);
    const second = try encodeText(a, t, "{'c': null, 'a': 3}");
    defer a.free(second);
    var out: std.ArrayList(u8) = .empty;
    defer out.deinit(a);
    try appendMapConcat(a, &out, &.{ first, second });
    try testing.expect(isWellFormed(out.items, t));
    const text = try renderText(a, t, out.items, .starrocks);
    defer a.free(text);
    try testing.expectEqualStrings("{\"b\":2,\"c\":null,\"a\":3}", text);
}

test "nested: flatten and encodeColumn are inverses" {
    const a = testing.allocator;
    const t = try types.arrayOf(.string);
    var col = try ColumnStore.init(a, t, true);
    defer col.deinit(a);
    for ([_]?[]const u8{ "['x', null]", null, "[]", "['y', 'z']" }) |text| {
        const at = col.rowCount();
        if (text) |s| {
            const bytes = try encodeText(a, t, s);
            defer a.free(bytes);
            try col.data.nested.appendValue(a, bytes);
        } else try col.data.nested.appendValue(a, "");
        try col.appendValidBit(a, at, text != null);
    }
    var flat = try flatten(a, .string, col.view(), 4);
    defer flat.deinit(a);
    try testing.expectEqualSlices(u32, &.{ 0, 2, 2, 2, 4 }, flat.offsets.items);
    try testing.expectEqual(@as(usize, 4), flat.elements.rowCount());

    var back = try ColumnStore.init(a, t, true);
    defer back.deinit(a);
    try encodeColumn(a, &back, .bytes, flat.elements.view(), flat.offsets.items, col.view().nulls, 4);
    for (0..4) |r| {
        try testing.expectEqual(col.view().isValid(r), back.view().isValid(r));
        try testing.expectEqualSlices(u8, col.data.nested.view().rowBytes(r), back.data.nested.view().rowBytes(r));
    }
}
