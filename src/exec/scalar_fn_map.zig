//! MAP functions: the constructor a map literal lowers to
//! (`map_from_arrays`), CAST to maps, subscripts, and StarRocks' map
//! functions. Like the array functions, kernels flatten a column of maps
//! into flat key and value columns (`nested.flattenMap`), convert those with
//! the kernels CAST runs, and cut the result back into maps (`MapBuilder`),
//! which drops repeated keys.

const std = @import("std");
const Allocator = std.mem.Allocator;

const types = @import("../types.zig");
const Type = types.Type;

const storage = @import("../storage/storage.zig");
const ColumnView = storage.ColumnView;

const store = @import("../engine/store.zig");
const ColumnStore = store.ColumnStore;

const scalar_fn = @import("scalar_fn.zig");
const ResolvedOverload = scalar_fn.ResolvedOverload;
const cast = @import("cast.zig");
const array = @import("scalar_fn_array.zig");
const overload = array.overload;
const castable = array.castable;
const AsType = array.AsType;
const nested = @import("nested.zig");
const Error = @import("exec.zig").Error;

/// Names that resolve here, for `scalar_fn.nameResolvable`.
const NAMES = [_][]const u8{
    scalar_fn.MAP_FROM_ARRAYS_FN, "map_size", "map_keys", "map_values", "map_concat", "distinct_map_keys", "map_filter",
};

pub fn isMapFnName(name: []const u8) bool {
    for (NAMES) |n| if (eql(name, n)) return true;
    return false;
}

fn eql(a: []const u8, b: []const u8) bool {
    return std.ascii.eqlIgnoreCase(a, b);
}

/// The overload of a map function, or null when `name` is none or its
/// arguments don't fit it.
pub fn resolveMap(aa: Allocator, name: []const u8, arg_types: []const Type) Allocator.Error!?ResolvedOverload {
    if (std.mem.startsWith(u8, name, scalar_fn.NESTED_CAST_PREFIX)) return resolveCastToMap(aa, name, arg_types);
    if (eql(name, scalar_fn.MAP_FROM_ARRAYS_FN)) {
        if (arg_types.len != 2) return null;
        const key = arg_types[0].arrayElement() orelse return null;
        const value = arg_types[1].arrayElement() orelse return null;
        // StarRocks takes no nested type as a key.
        if (key.isNested()) return null;
        return overload(aa, name, arg_types, try types.mapOf(key, value), fromArraysKernel);
    }
    if (eql(name, "map_concat")) {
        if (arg_types.len == 0) return null;
        const t = cast.commonTypeOf(arg_types) orelse return null;
        if (t.mapTypes() == null) return null;
        for (arg_types) |a| if (!try castable(aa, a, t)) return null;
        return overload(aa, name, arg_types, t, concatKernel);
    }
    if (arg_types.len == 0) return null;
    const m = arg_types[0].mapTypes() orelse return null;
    if (arg_types.len == 1) {
        if (eql(name, "map_size") or eql(name, "cardinality")) return overload(aa, name, arg_types, .int, sizeKernel);
        if (eql(name, "map_keys")) return overload(aa, name, arg_types, try types.arrayOf(m.key), keysKernel);
        if (eql(name, "map_values")) return overload(aa, name, arg_types, try types.arrayOf(m.value), valuesKernel);
        // Maps hold no repeated keys, so there are none to drop.
        if (eql(name, "distinct_map_keys")) return overload(aa, name, arg_types, arg_types[0], identityKernel);
        return null;
    }
    if (eql(name, "map_filter") and arg_types.len == 2) {
        const keep = arg_types[1].arrayElement() orelse return null;
        if (!try castable(aa, keep, .boolean)) return null;
        return overload(aa, name, arg_types, arg_types[0], filterKernel);
    }
    if (eql(name, scalar_fn.ELEMENT_AT_FN) and arg_types.len == 2) {
        const c = array.comparedType(m.key, arg_types[1]) orelse return null;
        if (!try castable(aa, m.key, c) or !try castable(aa, arg_types[1], c)) return null;
        return overload(aa, name, arg_types, m.value, elementAtKernel);
    }
    return null;
}

/// CAST to a map: of another map, its keys and values each as their CAST
/// converts them, keys that meet merging; of JSON, the map its object
/// holds (`nested.fromJson`); of text, read as the map it spells
/// (`nested.parseText`).
fn resolveCastToMap(aa: Allocator, name: []const u8, arg_types: []const Type) !?ResolvedOverload {
    if (arg_types.len != 1) return null;
    const target = types.parseTypeText(name[scalar_fn.NESTED_CAST_PREFIX.len..]) catch |err| switch (err) {
        error.OutOfMemory => return error.OutOfMemory,
        error.InvalidTypeText => return null,
    };
    const to = target.mapTypes() orelse return null;
    const from = arg_types[0];
    if (from.mapTypes()) |m| {
        if (!try castable(aa, m.key, to.key) or !try castable(aa, m.value, to.value)) return null;
        return overload(aa, name, arg_types, target, castMapKernel);
    }
    if (from == .json) return overload(aa, name, arg_types, target, array.jsonNestedKernel);
    if (from.isString()) return overload(aa, name, arg_types, target, array.parseNestedKernel);
    return null;
}

// ---------------------------------------------------------------------------
// Batch plumbing
// ---------------------------------------------------------------------------

/// Maps of `map_types` built one row at a time: append a row's entries, then
/// close it with `endRow`.
const MapBuilder = struct {
    map_types: types.MapTypes,
    keys: ColumnStore,
    values: ColumnStore,
    offsets: std.ArrayList(u32) = .empty,
    validity: std.ArrayList(u8) = .empty,
    rows: usize = 0,

    fn init(allocator: Allocator, map_types: types.MapTypes) !MapBuilder {
        var keys = try ColumnStore.init(allocator, map_types.key, true);
        errdefer keys.deinit(allocator);
        var values = try ColumnStore.init(allocator, map_types.value, true);
        errdefer values.deinit(allocator);
        var b: MapBuilder = .{ .map_types = map_types, .keys = keys, .values = values };
        try b.offsets.append(allocator, 0);
        return b;
    }

    fn deinit(self: *MapBuilder, allocator: Allocator) void {
        self.keys.deinit(allocator);
        self.values.deinit(allocator);
        self.offsets.deinit(allocator);
        self.validity.deinit(allocator);
    }

    fn appendEntries(self: *MapBuilder, allocator: Allocator, keys: ColumnView, key_start: usize, values: ColumnView, value_start: usize, count: usize) !void {
        try store.appendViewRange(allocator, &self.keys, keys, key_start, key_start + count);
        try store.appendViewRange(allocator, &self.values, values, value_start, value_start + count);
    }

    fn endRow(self: *MapBuilder, allocator: Allocator, valid: bool) !void {
        try self.offsets.append(allocator, @intCast(self.keys.rowCount()));
        if (self.rows & 7 == 0) try self.validity.append(allocator, 0);
        if (valid) self.validity.items[self.rows >> 3] |= @as(u8, 1) << @intCast(self.rows & 7);
        self.rows += 1;
    }

    fn finish(self: *MapBuilder, allocator: Allocator, out: *ColumnStore) !void {
        try nested.encodeMapColumn(allocator, out, self.map_types, self.keys.view(), self.values.view(), self.offsets.items, self.validity.items, self.rows);
    }
};

/// One map argument flattened, its keys and values as the types `to` names.
const FlatMapAs = struct {
    flat: nested.FlatMap,
    keys: AsType,
    values: AsType,

    fn init(allocator: Allocator, arg_type: Type, arg: ColumnView, to: types.MapTypes, rows: usize) !FlatMapAs {
        const from = arg_type.mapTypes().?;
        var flat = try nested.flattenMap(allocator, from, arg, rows);
        errdefer flat.deinit(allocator);
        var keys = try AsType.init(allocator, flat.keys.view(), from.key, to.key, flat.keys.rowCount());
        errdefer keys.deinit(allocator);
        const values = try AsType.init(allocator, flat.values.view(), from.value, to.value, flat.values.rowCount());
        return .{ .flat = flat, .keys = keys, .values = values };
    }

    fn deinit(self: *FlatMapAs, allocator: Allocator) void {
        self.values.deinit(allocator);
        self.keys.deinit(allocator);
        self.flat.deinit(allocator);
    }

    fn start(self: *const FlatMapAs, row: usize) usize {
        return self.flat.offsets.items[row];
    }

    fn end(self: *const FlatMapAs, row: usize) usize {
        return self.flat.offsets.items[row + 1];
    }
};

fn mapAt(v: ColumnView, row: usize) ?nested.Map {
    if (!v.isValid(row)) return null;
    return nested.Map.decode(v.data.nested.rowBytes(row));
}

// ---------------------------------------------------------------------------
// Kernels
// ---------------------------------------------------------------------------

/// MAP_FROM_ARRAYS(keys, values): the i-th key mapped to the i-th value;
/// NULL when either array is. Arrays of different lengths are an error.
fn fromArraysKernel(allocator: Allocator, arg_types: []const Type, out_type: Type, args: []const ColumnView, out: *ColumnStore, rows: usize) anyerror!void {
    _ = arg_types;
    const m = out_type.mapTypes().?;
    var keys = try nested.flatten(allocator, m.key, args[0], rows);
    defer keys.deinit(allocator);
    var values = try nested.flatten(allocator, m.value, args[1], rows);
    defer values.deinit(allocator);
    var b = try MapBuilder.init(allocator, m);
    defer b.deinit(allocator);
    for (0..rows) |r| {
        const valid = args[0].isValid(r) and args[1].isValid(r);
        if (valid) {
            const ks = keys.offsets.items[r];
            const count = keys.offsets.items[r + 1] - ks;
            const vs = values.offsets.items[r];
            if (values.offsets.items[r + 1] - vs != count) return Error.MapArrayLengthMismatch;
            try b.appendEntries(allocator, keys.elements.view(), ks, values.elements.view(), vs, count);
        }
        try b.endRow(allocator, valid);
    }
    try b.finish(allocator, out);
}

fn castMapKernel(allocator: Allocator, arg_types: []const Type, out_type: Type, args: []const ColumnView, out: *ColumnStore, rows: usize) anyerror!void {
    const to = out_type.mapTypes().?;
    var fm = try FlatMapAs.init(allocator, arg_types[0], args[0], to, rows);
    defer fm.deinit(allocator);
    try nested.encodeMapColumn(allocator, out, to, fm.keys.view, fm.values.view, fm.flat.offsets.items, args[0].nulls, rows);
}

fn sizeKernel(allocator: Allocator, arg_types: []const Type, out_type: Type, args: []const ColumnView, out: *ColumnStore, rows: usize) anyerror!void {
    _ = arg_types;
    _ = out_type;
    for (0..rows) |r| {
        const map = mapAt(args[0], r) orelse {
            try out.appendNulls(allocator, 1);
            continue;
        };
        try array.appendValue(allocator, out, "int", @as(i32, @intCast(map.count())));
    }
}

fn keysKernel(allocator: Allocator, arg_types: []const Type, out_type: Type, args: []const ColumnView, out: *ColumnStore, rows: usize) anyerror!void {
    try partKernel(allocator, arg_types, out_type, args, out, rows, .keys);
}

fn valuesKernel(allocator: Allocator, arg_types: []const Type, out_type: Type, args: []const ColumnView, out: *ColumnStore, rows: usize) anyerror!void {
    try partKernel(allocator, arg_types, out_type, args, out, rows, .values);
}

/// MAP_KEYS, MAP_VALUES: a map stores each part as an array already.
fn partKernel(allocator: Allocator, arg_types: []const Type, out_type: Type, args: []const ColumnView, out: *ColumnStore, rows: usize, comptime part: enum { keys, values }) !void {
    _ = arg_types;
    _ = out_type;
    const ss = &out.data.nested;
    for (0..rows) |r| {
        const at = out.rowCount();
        const map = mapAt(args[0], r);
        try ss.appendValue(allocator, if (map) |m| @field(m, @tagName(part)) else "");
        try out.appendValidBit(allocator, at, map != null);
    }
}

fn identityKernel(allocator: Allocator, arg_types: []const Type, out_type: Type, args: []const ColumnView, out: *ColumnStore, rows: usize) anyerror!void {
    _ = arg_types;
    _ = out_type;
    try store.appendViewRange(allocator, out, args[0], 0, rows);
}

/// `m[k]`, ELEMENT_AT(m, k): the value `k` maps to, NULL when it maps none.
fn elementAtKernel(allocator: Allocator, arg_types: []const Type, out_type: Type, args: []const ColumnView, out: *ColumnStore, rows: usize) anyerror!void {
    _ = out_type;
    const m = arg_types[0].mapTypes().?;
    const c = array.comparedType(m.key, arg_types[1]) orelse return Error.ComputeNoSuchOverload;
    var fm = try FlatMapAs.init(allocator, arg_types[0], args[0], .{ .key = c, .value = m.value }, rows);
    defer fm.deinit(allocator);
    var key = try AsType.init(allocator, args[1], arg_types[1], c, rows);
    defer key.deinit(allocator);
    for (0..rows) |r| {
        const found = if (args[0].isValid(r)) array.findIn(fm.keys.view, fm.start(r), fm.end(r), key.view, r) else null;
        if (found) |i| try store.appendViewRange(allocator, out, fm.values.view, i, i + 1) else try out.appendNulls(allocator, 1);
    }
}

/// MAP_FILTER(m, keep): the entries whose place in `keep` holds true; none
/// past `keep`'s end, and none when `keep` is NULL.
fn filterKernel(allocator: Allocator, arg_types: []const Type, out_type: Type, args: []const ColumnView, out: *ColumnStore, rows: usize) anyerror!void {
    const m = out_type.mapTypes().?;
    var fm = try FlatMapAs.init(allocator, arg_types[0], args[0], m, rows);
    defer fm.deinit(allocator);
    const keep_element = arg_types[1].arrayElement().?;
    var keep = try nested.flatten(allocator, keep_element, args[1], rows);
    defer keep.deinit(allocator);
    var flags = try AsType.init(allocator, keep.elements.view(), keep_element, .boolean, keep.elements.rowCount());
    defer flags.deinit(allocator);
    var b = try MapBuilder.init(allocator, m);
    defer b.deinit(allocator);
    for (0..rows) |r| {
        const ks = keep.offsets.items[r];
        const n = @min(fm.end(r) - fm.start(r), keep.offsets.items[r + 1] - ks);
        for (0..n) |i| {
            if (!flags.view.isValid(ks + i) or flags.view.data.boolean[ks + i] == 0) continue;
            try b.appendEntries(allocator, fm.keys.view, fm.start(r) + i, fm.values.view, fm.start(r) + i, 1);
        }
        try b.endRow(allocator, args[0].isValid(r));
    }
    try b.finish(allocator, out);
}

/// MAP_CONCAT: every map's entries, a key in several taking its value from
/// the last; a NULL map adds none, and the result is NULL only when every
/// map is.
fn concatKernel(allocator: Allocator, arg_types: []const Type, out_type: Type, args: []const ColumnView, out: *ColumnStore, rows: usize) anyerror!void {
    const to = out_type.mapTypes().?;
    const maps = try allocator.alloc(FlatMapAs, args.len);
    defer allocator.free(maps);
    var built: usize = 0;
    defer for (maps[0..built]) |*fm| fm.deinit(allocator);
    for (maps, arg_types, args) |*fm, t, a| {
        fm.* = try FlatMapAs.init(allocator, t, a, to, rows);
        built += 1;
    }
    var b = try MapBuilder.init(allocator, to);
    defer b.deinit(allocator);
    for (0..rows) |r| {
        var any = false;
        for (maps, args) |*fm, a| {
            if (!a.isValid(r)) continue;
            any = true;
            try b.appendEntries(allocator, fm.keys.view, fm.start(r), fm.values.view, fm.start(r), fm.end(r) - fm.start(r));
        }
        try b.endRow(allocator, any);
    }
    try b.finish(allocator, out);
}

test "map resolution: types" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const aa = arena.allocator();
    const ints = try types.arrayOf(.int);
    const texts = try types.arrayOf(.string);
    const text_to_int = try types.mapOf(.string, .int);

    const built = (try resolveMap(aa, scalar_fn.MAP_FROM_ARRAYS_FN, &.{ texts, ints })).?;
    try std.testing.expect(std.meta.eql(text_to_int, built.func.return_type));
    try std.testing.expect(try resolveMap(aa, scalar_fn.MAP_FROM_ARRAYS_FN, &.{ try types.arrayOf(ints), ints }) == null);
    try std.testing.expectEqual(Type.int, (try resolveMap(aa, "map_size", &.{text_to_int})).?.func.return_type);
    try std.testing.expect(std.meta.eql(texts, (try resolveMap(aa, "map_keys", &.{text_to_int})).?.func.return_type));
    try std.testing.expectEqual(Type.int, (try resolveMap(aa, "element_at", &.{ text_to_int, .string })).?.func.return_type);
    const widened = (try resolveMap(aa, "map_concat", &.{ text_to_int, try types.mapOf(.string, .bigint) })).?;
    try std.testing.expect(std.meta.eql(try types.mapOf(.string, .bigint), widened.func.return_type));
    try std.testing.expect(try resolveMap(aa, "map_size", &.{ints}) == null);
}
