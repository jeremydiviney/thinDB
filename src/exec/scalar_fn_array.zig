//! ARRAY functions: the constructor an array literal lowers to, CAST to and
//! from arrays, subscripts, and StarRocks' array functions. Kernels run on
//! whole batches: an argument's arrays flatten into one column of their
//! elements (`nested.flatten`), conversions run over that column with the
//! kernels CAST runs, and results are cut back into arrays
//! (`ArrayBuilder`). Every kernel writes its own validity.

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
const TypedKernel = scalar_fn.TypedKernel;
const cast = @import("cast.zig");
const common = @import("scalar_fn_common.zig");
const string = @import("scalar_fn_string.zig");
const nested = @import("nested.zig");
const json_binary = @import("json_binary.zig");
const Error = @import("exec.zig").Error;

/// Names that resolve here, for `scalar_fn.nameResolvable`.
const NAMES = [_][]const u8{
    scalar_fn.ARRAY_VALUE_FN, "split",              "array_generate",    "array_repeat",
    "array_length",           "cardinality",        "array_distinct",    "array_sort",
    "array_reverse",          "array_min",          "array_max",         "array_sum",
    "array_avg",              "array_flatten",      "array_difference",  "element_at",
    "array_slice",            "array_join",         "array_contains",    "array_position",
    "array_remove",           "array_append",       "array_concat",      "arrays_overlap",
    "array_contains_all",     "array_contains_seq", "array_intersect",   scalar_fn.ARRAY_ORDER_FN,
    "array_filter",           "any_match",          "all_match",         "array_sortby",
    "array_map",              "regexp_extract_all", "cosine_similarity", "cosine_similarity_norm",
    "l2_distance",
};

pub fn isArrayFnName(name: []const u8) bool {
    if (std.mem.startsWith(u8, name, scalar_fn.NESTED_CAST_PREFIX)) return true;
    for (NAMES) |n| if (eql(name, n)) return true;
    return false;
}

fn eql(a: []const u8, b: []const u8) bool {
    return std.ascii.eqlIgnoreCase(a, b);
}

pub fn isText(t: Type) bool {
    return t.isString() and t != .json;
}

pub fn overload(aa: Allocator, name: []const u8, arg_types: []const Type, return_type: Type, kernel: TypedKernel) !?ResolvedOverload {
    return .{
        .func = .{
            .name = name,
            .arg_types = try aa.dupe(Type, arg_types),
            .return_type = return_type,
            .typed_kernel = kernel,
            .null_strategy = .kernel_managed,
        },
        .arg_casts = null,
    };
}

/// The overload of an array function, or null when `name` is none or its
/// arguments don't fit it.
pub fn resolveArray(aa: Allocator, name: []const u8, arg_types: []const Type) Allocator.Error!?ResolvedOverload {
    if (std.mem.startsWith(u8, name, scalar_fn.NESTED_CAST_PREFIX)) return resolveCastToArray(aa, name, arg_types);
    if (eql(name, scalar_fn.ARRAY_VALUE_FN)) return resolveArrayValue(aa, name, arg_types);
    if (eql(name, "split")) {
        if (arg_types.len != 2 or !isText(arg_types[0]) or !isText(arg_types[1])) return null;
        return overload(aa, name, arg_types, try types.arrayOf(.string), splitKernel);
    }
    if (eql(name, "regexp_extract_all")) {
        if (arg_types.len != 3 or !isText(arg_types[0]) or !isText(arg_types[1]) or !arg_types[2].isInteger()) return null;
        return overload(aa, name, arg_types, try types.arrayOf(.string), regexpExtractAllKernel);
    }
    if (eql(name, "array_generate")) {
        if (arg_types.len < 2 or arg_types.len > 3) return null;
        for (arg_types) |t| if (!t.isInteger()) return null;
        return overload(aa, name, arg_types, try types.arrayOf(.bigint), generateKernel);
    }
    if (eql(name, "array_repeat")) {
        if (arg_types.len != 2 or !arg_types[1].isInteger()) return null;
        return overload(aa, name, arg_types, try types.arrayOf(arg_types[0]), repeatKernel);
    }
    if (try resolveNestedConditional(aa, name, arg_types)) |ov| return ov;
    if (arg_types.len == 0) return null;
    if (eql(name, "to_string") and arg_types.len == 1 and arg_types[0].isNested()) return overload(aa, name, arg_types, .string, toStringKernel);
    if (eql(name, "to_json") and arg_types.len == 1 and arg_types[0].isNested()) return overload(aa, name, arg_types, .json, toJsonKernel);
    if (eql(name, scalar_fn.ARRAY_ORDER_FN)) {
        if (arg_types.len != 2) return null;
        const t = orderedType(arg_types[0], arg_types[1]) orelse return null;
        if (!t.isNested()) return null;
        if (!try castable(aa, arg_types[0], t) or !try castable(aa, arg_types[1], t)) return null;
        return overload(aa, name, arg_types, .int, orderKernel);
    }
    const array = arg_types[0];
    const element = array.arrayElement() orelse return null;
    if (arg_types.len == 1) {
        if (eql(name, "array_length") or eql(name, "cardinality")) return overload(aa, name, arg_types, .int, lengthKernel);
        if (eql(name, "array_distinct")) return overload(aa, name, arg_types, array, distinctKernel);
        if (eql(name, "array_sort") and element != .json) return overload(aa, name, arg_types, array, sortKernel);
        if (eql(name, "reverse") or eql(name, "array_reverse")) return overload(aa, name, arg_types, array, reverseKernel);
        if (eql(name, "array_min") and element != .json) return overload(aa, name, arg_types, element, minKernel);
        if (eql(name, "array_max") and element != .json) return overload(aa, name, arg_types, element, maxKernel);
        if (eql(name, "array_sum") or eql(name, "array_avg") or eql(name, "array_difference")) {
            const sum = sumType(element) orelse return null;
            if (!try castable(aa, element, sum)) return null;
            if (eql(name, "array_sum")) return overload(aa, name, arg_types, sum, sumKernel);
            if (eql(name, "array_avg")) return overload(aa, name, arg_types, avgType(element).?, avgKernel);
            return overload(aa, name, arg_types, try types.arrayOf(sum), differenceKernel);
        }
        if (eql(name, "array_flatten")) {
            if (element.arrayElement() == null) return null;
            return overload(aa, name, arg_types, element, flattenKernel);
        }
        if (eql(name, "array_concat") or eql(name, "array_intersect")) return overload(aa, name, arg_types, array, if (eql(name, "array_concat")) concatKernel else intersectKernel);
        if (eql(name, "any_match") or eql(name, "all_match")) {
            if (!try castable(aa, element, .boolean)) return null;
            return overload(aa, name, arg_types, .boolean, if (eql(name, "any_match")) anyMatchKernel else allMatchKernel);
        }
        return null;
    }
    if (eql(name, "cosine_similarity") or eql(name, "cosine_similarity_norm") or eql(name, "l2_distance")) {
        if (arg_types.len != 2) return null;
        for (arg_types) |t| {
            const e = t.arrayElement() orelse return null;
            if (!(e.isInteger() or e.isDecimal() or e.isFloat() or e == .boolean)) return null;
        }
        const kernel = if (eql(name, "l2_distance"))
            VectorKernel(.l2_distance)
        else if (eql(name, "cosine_similarity"))
            VectorKernel(.cosine_similarity)
        else
            VectorKernel(.inner_product);
        return overload(aa, name, arg_types, .double, kernel);
    }
    if (eql(name, "array_filter")) {
        if (arg_types.len != 2) return null;
        const keep = arg_types[1].arrayElement() orelse return null;
        if (!try castable(aa, keep, .boolean)) return null;
        return overload(aa, name, arg_types, array, filterKernel);
    }
    if (eql(name, "array_sortby")) {
        for (arg_types[1..]) |t| if (t.arrayElement() == null) return null;
        return overload(aa, name, arg_types, array, sortByKernel);
    }
    if (eql(name, scalar_fn.ELEMENT_AT_FN)) {
        if (arg_types.len != 2 or !arg_types[1].isInteger()) return null;
        return overload(aa, name, arg_types, element, elementAtKernel);
    }
    if (eql(name, "array_slice")) {
        if (arg_types.len > 3) return null;
        for (arg_types[1..]) |t| if (!t.isInteger()) return null;
        return overload(aa, name, arg_types, array, sliceKernel);
    }
    if (eql(name, "array_join")) {
        if (arg_types.len > 3) return null;
        for (arg_types[1..]) |t| if (!isText(t)) return null;
        if (!try castable(aa, element, .string)) return null;
        return overload(aa, name, arg_types, .string, joinKernel);
    }
    if (isValueFn(name)) {
        if (arg_types.len != 2) return null;
        const c = comparedType(element, arg_types[1]) orelse return null;
        if (!try castable(aa, element, c) or !try castable(aa, arg_types[1], c)) return null;
        if (eql(name, "array_contains")) return overload(aa, name, arg_types, .boolean, containsKernel);
        if (eql(name, "array_position")) return overload(aa, name, arg_types, .int, positionKernel);
        if (eql(name, "array_remove")) return overload(aa, name, arg_types, array, removeKernel);
        if (eql(name, "array_append")) return overload(aa, name, arg_types, try types.arrayOf(c), appendKernel);
        return null;
    }
    const c = commonElementOf(arg_types) orelse return null;
    for (arg_types) |t| if (!try castable(aa, t.arrayElement().?, c)) return null;
    if (eql(name, "array_concat")) return overload(aa, name, arg_types, try types.arrayOf(c), concatKernel);
    if (eql(name, "array_intersect")) return overload(aa, name, arg_types, try types.arrayOf(c), intersectKernel);
    if (arg_types.len != 2) return null;
    if (eql(name, "arrays_overlap")) return overload(aa, name, arg_types, .boolean, overlapKernel);
    if (eql(name, "array_contains_all")) return overload(aa, name, arg_types, .boolean, containsAllKernel);
    if (eql(name, "array_contains_seq")) return overload(aa, name, arg_types, .boolean, containsSeqKernel);
    return null;
}

/// IF, IFNULL and COALESCE over arrays and maps: the branches meet at their
/// common type, which must be nested; the scalar overloads take the rest.
fn resolveNestedConditional(aa: Allocator, name: []const u8, arg_types: []const Type) !?ResolvedOverload {
    const is_if = eql(name, "if");
    if (!is_if and !eql(name, "ifnull") and !eql(name, "coalesce")) return null;
    if (is_if and (arg_types.len != 3 or arg_types[0] != .boolean)) return null;
    if (eql(name, "ifnull") and arg_types.len != 2) return null;
    const branches = if (is_if) arg_types[1..] else arg_types;
    if (branches.len == 0) return null;
    const t = cast.commonTypeOf(branches) orelse return null;
    if (!t.isNested()) return null;
    for (branches) |b| if (!try castable(aa, b, t)) return null;
    return overload(aa, name, arg_types, t, if (is_if) ifKernel else coalesceKernel);
}

/// Functions of an array and one value compared with its elements.
fn isValueFn(name: []const u8) bool {
    inline for (.{ "array_contains", "array_position", "array_remove", "array_append" }) |n| if (eql(name, n)) return true;
    return false;
}

/// `[a, b, ...]`: its elements meet at their common type, which every one
/// must already share a representation with; any other mix resolves once
/// Compute converts them (`scalar_fn.resultValueArgsStart`). No elements
/// make an empty ARRAY<BOOLEAN>, StarRocks' type for `[]`.
fn resolveArrayValue(aa: Allocator, name: []const u8, arg_types: []const Type) !?ResolvedOverload {
    if (arg_types.len == 0) return overload(aa, name, arg_types, try types.arrayOf(.boolean), arrayValueKernel);
    const element = cast.commonTypeOf(arg_types) orelse return null;
    for (arg_types) |t| {
        if (!cast.sameRepresentation(t, element) or (t == .json) != (element == .json)) return null;
    }
    return overload(aa, name, arg_types, try types.arrayOf(element), arrayValueKernel);
}

/// CAST to an array (`scalar_fn.NESTED_CAST_PREFIX`): of another array,
/// element by element as each element's CAST converts it; of JSON, the
/// array it holds (`nested.fromJson`); of text, read as the array it
/// spells (`nested.parseText`).
fn resolveCastToArray(aa: Allocator, name: []const u8, arg_types: []const Type) !?ResolvedOverload {
    if (arg_types.len != 1) return null;
    const target = types.parseTypeText(name[scalar_fn.NESTED_CAST_PREFIX.len..]) catch |err| switch (err) {
        error.OutOfMemory => return error.OutOfMemory,
        error.InvalidTypeText => return null,
    };
    const target_element = target.arrayElement() orelse return null;
    const from = arg_types[0];
    if (from.arrayElement()) |from_element| {
        if (!try castable(aa, from_element, target_element)) return null;
        return overload(aa, name, arg_types, target, castArrayKernel);
    }
    if (from == .json) return overload(aa, name, arg_types, target, jsonNestedKernel);
    if (from.isString()) return overload(aa, name, arg_types, target, parseNestedKernel);
    return null;
}

/// Whether CAST(x AS to) is defined for an x of `from`.
pub fn castable(aa: Allocator, from: Type, to: Type) !bool {
    if (cast.sameRepresentation(from, to)) return true;
    const name = try scalar_fn.castFnName(aa, to) orelse return false;
    return try scalar_fn.resolve(aa, name, &.{from}) != null;
}

/// The type an element of `element` and a value of `t` compare and combine
/// in: text beside elements that aren't reads as the element type, as CAST
/// reads it (so a NULL literal, whose placeholder type is text, takes the
/// element type); otherwise their common type.
pub fn comparedType(element: Type, t: Type) ?Type {
    if (isText(t) and !isText(element)) return element;
    return cast.commonType(element, t);
}

/// The common type of every argument's elements; null when one isn't an
/// array or two never meet.
fn commonElementOf(arg_types: []const Type) ?Type {
    var c = arg_types[0].arrayElement() orelse return null;
    for (arg_types[1..]) |t| c = cast.commonType(c, t.arrayElement() orelse return null) orelse return null;
    return c;
}

/// SUM over an array's elements: integers as BIGINT (LARGEINT stays one),
/// floats as DOUBLE, a DECIMAL at 38 digits of its own scale.
fn sumType(element: Type) ?Type {
    if (element == .largeint) return .largeint;
    if (element.isInteger() or element == .boolean) return .bigint;
    if (element.isFloat()) return .double;
    if (element.decimalSpec()) |spec| return .{ .decimal128 = .{ .p = 38, .s = spec.s } };
    return null;
}

/// AVG over an array's elements: a DOUBLE, or for a DECIMAL six more
/// fraction digits, as StarRocks gives them.
fn avgType(element: Type) ?Type {
    if (element.isInteger() or element == .boolean or element.isFloat()) return .double;
    if (element.decimalSpec()) |spec| return .{ .decimal128 = .{ .p = 38, .s = @min(spec.s + AVG_EXTRA_SCALE, 38) } };
    return null;
}

const AVG_EXTRA_SCALE = 6;

// ---------------------------------------------------------------------------
// Batch plumbing
// ---------------------------------------------------------------------------

/// `src`, `rows` values of `from`, as CAST(src AS to) makes them.
fn castColumn(allocator: Allocator, src: ColumnView, from: Type, to: Type, rows: usize) !ColumnStore {
    var arena_state = std.heap.ArenaAllocator.init(allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();
    const name = try scalar_fn.castFnName(arena, to) orelse return Error.ComputeNoSuchOverload;
    const ov = try scalar_fn.resolve(arena, name, &.{from}) orelse return Error.ComputeNoSuchOverload;
    var arg = src;
    var arg_buf: ?ColumnStore = null;
    defer if (arg_buf) |*b| b.deinit(allocator);
    if (ov.arg_casts) |casts| if (casts[0]) |k| {
        arg_buf = try ColumnStore.init(allocator, ov.func.arg_types[0], true);
        try k(allocator, &.{src}, &arg_buf.?, rows);
        arg = arg_buf.?.view();
    };
    var out = try ColumnStore.init(allocator, ov.func.return_type, true);
    errdefer out.deinit(allocator);
    const args = [_]ColumnView{arg};
    if (ov.func.typed_kernel) |tk| {
        try tk(allocator, &.{from}, ov.func.return_type, &args, &out, rows);
    } else if (ov.func.kernel) |k| {
        try k(allocator, &args, &out, rows);
    } else return Error.ComputeNoSuchOverload;
    if (ov.func.null_strategy != .kernel_managed) try out.appendValidityRange(allocator, 0, arg.nulls, rows);
    return out;
}

/// A column of `rows` values as type `to`: the column itself where it
/// already is one, else its CAST, which this owns.
pub const AsType = struct {
    view: ColumnView,
    owned: ?ColumnStore = null,

    pub fn init(allocator: Allocator, src: ColumnView, from: Type, to: Type, rows: usize) !AsType {
        if (cast.sameRepresentation(from, to)) return .{ .view = src };
        const converted = try castColumn(allocator, src, from, to, rows);
        return .{ .view = converted.view(), .owned = converted };
    }

    pub fn deinit(self: *AsType, allocator: Allocator) void {
        if (self.owned) |*o| o.deinit(allocator);
    }
};

/// One array argument flattened, its elements as type `to`.
const FlatAs = struct {
    flat: nested.Flat,
    elements: AsType,

    fn init(allocator: Allocator, arg_type: Type, arg: ColumnView, to: Type, rows: usize) !FlatAs {
        const element = arg_type.arrayElement().?;
        var flat = try nested.flatten(allocator, element, arg, rows);
        errdefer flat.deinit(allocator);
        const elements = try AsType.init(allocator, flat.elements.view(), element, to, flat.elements.rowCount());
        return .{ .flat = flat, .elements = elements };
    }

    fn deinit(self: *FlatAs, allocator: Allocator) void {
        self.elements.deinit(allocator);
        self.flat.deinit(allocator);
    }

    fn start(self: *const FlatAs, row: usize) usize {
        return self.flat.offsets.items[row];
    }

    fn end(self: *const FlatAs, row: usize) usize {
        return self.flat.offsets.items[row + 1];
    }
};

/// Arrays of `element` built one row at a time: append a row's elements,
/// then close it with `endRow`.
const ArrayBuilder = struct {
    element: Type,
    elements: ColumnStore,
    offsets: std.ArrayList(u32) = .empty,
    validity: std.ArrayList(u8) = .empty,
    rows: usize = 0,

    fn init(allocator: Allocator, element: Type) !ArrayBuilder {
        var b: ArrayBuilder = .{ .element = element, .elements = try ColumnStore.init(allocator, element, true) };
        errdefer b.elements.deinit(allocator);
        try b.offsets.append(allocator, 0);
        return b;
    }

    fn deinit(self: *ArrayBuilder, allocator: Allocator) void {
        self.elements.deinit(allocator);
        self.offsets.deinit(allocator);
        self.validity.deinit(allocator);
    }

    fn appendRange(self: *ArrayBuilder, allocator: Allocator, v: ColumnView, start: usize, end: usize) !void {
        try store.appendViewRange(allocator, &self.elements, v, start, end);
    }

    fn appendOne(self: *ArrayBuilder, allocator: Allocator, v: ColumnView, i: usize) !void {
        try store.appendViewRange(allocator, &self.elements, v, i, i + 1);
    }

    fn endRow(self: *ArrayBuilder, allocator: Allocator, valid: bool) !void {
        try self.offsets.append(allocator, @intCast(self.elements.rowCount()));
        if (self.rows & 7 == 0) try self.validity.append(allocator, 0);
        if (valid) self.validity.items[self.rows >> 3] |= @as(u8, 1) << @intCast(self.rows & 7);
        self.rows += 1;
    }

    fn finish(self: *ArrayBuilder, allocator: Allocator, out: *ColumnStore) !void {
        try nested.encodeColumn(allocator, out, nested.ElemClass.of(self.element), self.elements.view(), self.offsets.items, self.validity.items, self.rows);
    }
};

fn arrayAt(v: ColumnView, row: usize) ?nested.Array {
    if (!v.isValid(row)) return null;
    return nested.Array.decode(v.data.nested.rowBytes(row));
}

fn intAt(v: ColumnView, row: usize) ?i64 {
    if (!v.isValid(row)) return null;
    return switch (v.data) {
        inline .tinyint, .smallint, .int, .bigint => |s| s[row],
        .largeint => |s| std.math.cast(i64, s[row]) orelse if (s[row] < 0) std.math.minInt(i64) else std.math.maxInt(i64),
        else => null,
    };
}

pub fn appendValue(allocator: Allocator, out: *ColumnStore, comptime field: []const u8, value: anytype) !void {
    try @field(out.data, field).append(allocator, value);
    try out.appendValidBit(allocator, out.rowCount() - 1, true);
}

fn appendText(allocator: Allocator, out: *ColumnStore, text: []const u8) !void {
    try common.stringStoreOf(out).appendValue(allocator, text);
    try out.appendValidBit(allocator, out.rowCount() - 1, true);
}

/// The stored bytes of row `i` of `v`: equal exactly when the values are,
/// once floats are canonical.
fn cellBytes(v: ColumnView, i: usize) []const u8 {
    return switch (v.data) {
        .varchar, .string, .char, .json, .nested => |sv| sv.rowBytes(i),
        inline else => |s| std.mem.asBytes(&s[i]),
    };
}

/// Two cells of one type: NULL equals NULL here, as StarRocks' array
/// functions match a NULL element.
fn cellsEqual(a: ColumnView, i: usize, b: ColumnView, j: usize) bool {
    const av = a.isValid(i);
    const bv = b.isValid(j);
    if (!av or !bv) return av == bv;
    return switch (a.data) {
        .float => |s| types.canonicalFloatBits(s[i]) == types.canonicalFloatBits(b.data.float[j]),
        .double => |s| types.canonicalFloatBits(s[i]) == types.canonicalFloatBits(b.data.double[j]),
        else => std.mem.eql(u8, cellBytes(a, i), cellBytes(b, j)),
    };
}

/// Two cells of one column in sort order, NULL first.
fn cellOrder(v: ColumnView, i: usize, j: usize) std.math.Order {
    const iv = v.isValid(i);
    const jv = v.isValid(j);
    if (!iv or !jv) return if (iv == jv) .eq else if (iv) .gt else .lt;
    return switch (v.data) {
        .varchar, .string, .char, .json => |sv| std.mem.order(u8, sv.rowBytes(i), sv.rowBytes(j)),
        .nested => |sv| nested.order(sv.rowBytes(i), sv.rowBytes(j)),
        .float => |s| types.floatOrder(s[i], s[j]),
        .double => |s| types.floatOrder(s[i], s[j]),
        inline else => |s| std.math.order(s[i], s[j]),
    };
}

/// The first of `v`'s rows [start, end) equal to row `j` of `x`.
pub fn findIn(v: ColumnView, start: usize, end: usize, x: ColumnView, j: usize) ?usize {
    for (start..end) |i| if (cellsEqual(v, i, x, j)) return i;
    return null;
}

// ---------------------------------------------------------------------------
// Construction and CAST
// ---------------------------------------------------------------------------

fn arrayValueKernel(allocator: Allocator, arg_types: []const Type, out_type: Type, args: []const ColumnView, out: *ColumnStore, rows: usize) anyerror!void {
    _ = arg_types;
    var b = try ArrayBuilder.init(allocator, out_type.arrayElement().?);
    defer b.deinit(allocator);
    for (0..rows) |r| {
        for (args) |a| try b.appendOne(allocator, a, r);
        try b.endRow(allocator, true);
    }
    try b.finish(allocator, out);
}

fn castArrayKernel(allocator: Allocator, arg_types: []const Type, out_type: Type, args: []const ColumnView, out: *ColumnStore, rows: usize) anyerror!void {
    const target = out_type.arrayElement().?;
    var fa = try FlatAs.init(allocator, arg_types[0], args[0], target, rows);
    defer fa.deinit(allocator);
    try nested.encodeColumn(allocator, out, nested.ElemClass.of(target), fa.elements.view, fa.flat.offsets.items, args[0].nulls, rows);
}

/// Text read as the nested value it spells; NULL where it spells none.
pub fn parseNestedKernel(allocator: Allocator, arg_types: []const Type, out_type: Type, args: []const ColumnView, out: *ColumnStore, rows: usize) anyerror!void {
    _ = arg_types;
    const sv = common.stringViewOf(args[0]);
    var cell: std.ArrayList(u8) = .empty;
    defer cell.deinit(allocator);
    const ss = &out.data.nested;
    for (0..rows) |r| {
        const at = out.rowCount();
        const valid = args[0].isValid(r) and blk: {
            cell.clearRetainingCapacity();
            nested.parseText(allocator, out_type, sv.rowBytes(r), &cell) catch |err| switch (err) {
                error.OutOfMemory => return err,
                error.InvalidNestedText => break :blk false,
            };
            break :blk true;
        };
        try ss.appendValue(allocator, if (valid) cell.items else "");
        try out.appendValidBit(allocator, at, valid);
    }
}

/// JSON read as the nested value it holds (`nested.fromJson`); NULL where
/// it holds none.
pub fn jsonNestedKernel(allocator: Allocator, arg_types: []const Type, out_type: Type, args: []const ColumnView, out: *ColumnStore, rows: usize) anyerror!void {
    _ = arg_types;
    const sv = common.stringViewOf(args[0]);
    var cell: std.ArrayList(u8) = .empty;
    defer cell.deinit(allocator);
    const ss = &out.data.nested;
    for (0..rows) |r| {
        const at = out.rowCount();
        const valid = args[0].isValid(r) and blk: {
            const doc = json_binary.normalize(allocator, sv.rowBytes(r)) catch |err| switch (err) {
                error.OutOfMemory => return err,
                error.JsonInvalid => break :blk false,
            };
            defer if (doc.owned) allocator.free(doc.bytes);
            cell.clearRetainingCapacity();
            nested.fromJson(allocator, out_type, doc.bytes, &cell) catch |err| switch (err) {
                error.OutOfMemory => return err,
                error.JsonShape => break :blk false,
            };
            break :blk true;
        };
        try ss.appendValue(allocator, if (valid) cell.items else "");
        try out.appendValidBit(allocator, at, valid);
    }
}

/// IF(c, a, b): `a` where `c` is true, else `b` (a NULL `c` is false).
fn ifKernel(allocator: Allocator, arg_types: []const Type, out_type: Type, args: []const ColumnView, out: *ColumnStore, rows: usize) anyerror!void {
    var a = try AsType.init(allocator, args[1], arg_types[1], out_type, rows);
    defer a.deinit(allocator);
    var b = try AsType.init(allocator, args[2], arg_types[2], out_type, rows);
    defer b.deinit(allocator);
    for (0..rows) |r| {
        const chosen = if (args[0].isValid(r) and args[0].data.boolean[r] != 0) a.view else b.view;
        try appendNestedCell(allocator, out, chosen, r);
    }
}

/// IFNULL and COALESCE: each row's first non-NULL argument.
fn coalesceKernel(allocator: Allocator, arg_types: []const Type, out_type: Type, args: []const ColumnView, out: *ColumnStore, rows: usize) anyerror!void {
    const as = try allocator.alloc(AsType, args.len);
    defer allocator.free(as);
    var ready: usize = 0;
    defer for (as[0..ready]) |*x| x.deinit(allocator);
    for (as, args, arg_types) |*x, a, t| {
        x.* = try AsType.init(allocator, a, t, out_type, rows);
        ready += 1;
    }
    for (0..rows) |r| {
        const chosen = for (as) |x| {
            if (x.view.isValid(r)) break x.view;
        } else as[as.len - 1].view;
        try appendNestedCell(allocator, out, chosen, r);
    }
}

/// Append row `r` of a nested `v` to `out`, NULL as NULL.
fn appendNestedCell(allocator: Allocator, out: *ColumnStore, v: ColumnView, r: usize) !void {
    const at = out.rowCount();
    const valid = v.isValid(r);
    try out.data.nested.appendValue(allocator, if (valid) v.data.nested.rowBytes(r) else "");
    try out.appendValidBit(allocator, at, valid);
}

fn toStringKernel(allocator: Allocator, arg_types: []const Type, out_type: Type, args: []const ColumnView, out: *ColumnStore, rows: usize) anyerror!void {
    _ = out_type;
    var text: std.ArrayList(u8) = .empty;
    defer text.deinit(allocator);
    for (0..rows) |r| {
        if (!args[0].isValid(r)) {
            try out.appendNulls(allocator, 1);
            continue;
        }
        text.clearRetainingCapacity();
        try nested.appendText(allocator, &text, arg_types[0], args[0].data.nested.rowBytes(r), .starrocks);
        try appendText(allocator, out, text.items);
    }
}

/// An array, map or struct as JSON (`nested.appendJson`); NULL where JSON
/// can't hold it (a NaN element, a NULL map key).
fn toJsonKernel(allocator: Allocator, arg_types: []const Type, out_type: Type, args: []const ColumnView, out: *ColumnStore, rows: usize) anyerror!void {
    _ = out_type;
    var doc: std.ArrayList(u8) = .empty;
    defer doc.deinit(allocator);
    for (0..rows) |r| {
        if (!args[0].isValid(r)) {
            try out.appendNulls(allocator, 1);
            continue;
        }
        doc.clearRetainingCapacity();
        nested.appendJson(allocator, &doc, arg_types[0], args[0].data.nested.rowBytes(r)) catch |err| switch (err) {
            error.NotJson => {
                try out.appendNulls(allocator, 1);
                continue;
            },
            else => |e| return e,
        };
        try appendText(allocator, out, doc.items);
    }
}

fn splitKernel(allocator: Allocator, arg_types: []const Type, out_type: Type, args: []const ColumnView, out: *ColumnStore, rows: usize) anyerror!void {
    _ = arg_types;
    _ = out_type;
    var b = try ArrayBuilder.init(allocator, .string);
    defer b.deinit(allocator);
    const text = common.stringViewOf(args[0]);
    const sep = common.stringViewOf(args[1]);
    for (0..rows) |r| {
        if (!args[0].isValid(r) or !args[1].isValid(r)) {
            try b.endRow(allocator, false);
            continue;
        }
        const s = text.rowBytes(r);
        const d = sep.rowBytes(r);
        if (d.len == 0) {
            // An empty separator splits between characters.
            var i: usize = 0;
            while (i < s.len) {
                const n = @min(std.unicode.utf8ByteSequenceLength(s[i]) catch 1, s.len - i);
                try appendText(allocator, &b.elements, s[i..][0..n]);
                i += n;
            }
        } else {
            var it = std.mem.splitSequence(u8, s, d);
            while (it.next()) |part| try appendText(allocator, &b.elements, part);
        }
        try b.endRow(allocator, true);
    }
    try b.finish(allocator, out);
}

/// REGEXP_EXTRACT_ALL(s, pattern, group): capture group `group` of every
/// match (`string.regexGroup`), each search resuming where the last match
/// ended; empty when the pattern has no such group.
fn regexpExtractAllKernel(allocator: Allocator, arg_types: []const Type, out_type: Type, args: []const ColumnView, out: *ColumnStore, rows: usize) anyerror!void {
    _ = arg_types;
    _ = out_type;
    var b = try ArrayBuilder.init(allocator, .string);
    defer b.deinit(allocator);
    var call = string.RegexCall.init(allocator, args[1], null);
    defer call.deinit();
    const text = common.stringViewOf(args[0]);
    for (0..rows) |r| {
        const valid = try call.at(r) != null and args[0].isValid(r) and args[2].isValid(r);
        if (valid) {
            const s = text.rowBytes(r);
            const group = intAt(args[2], r).?;
            var pos: usize = 0;
            while (try call.next(s, &pos)) |slots| try appendText(allocator, &b.elements, string.regexGroup(s, slots, group) orelse break);
        }
        try b.endRow(allocator, valid);
    }
    try b.finish(allocator, out);
}

/// ARRAY_GENERATE(start, end[, step]): start, start + step, ... up to and
/// including end. The step defaults to 1, or -1 when end is below start; a
/// step away from end, or of 0, gives an empty array.
fn generateKernel(allocator: Allocator, arg_types: []const Type, out_type: Type, args: []const ColumnView, out: *ColumnStore, rows: usize) anyerror!void {
    _ = arg_types;
    _ = out_type;
    var b = try ArrayBuilder.init(allocator, .bigint);
    defer b.deinit(allocator);
    for (0..rows) |r| {
        const start = intAt(args[0], r);
        const end = intAt(args[1], r);
        const step = if (args.len == 3) intAt(args[2], r) else if (start != null and end != null and end.? < start.?) @as(?i64, -1) else 1;
        if (start == null or end == null or step == null) {
            try b.endRow(allocator, false);
            continue;
        }
        const s: i128 = step.?;
        var x: i128 = start.?;
        while (s != 0 and ((s > 0 and x <= end.?) or (s < 0 and x >= end.?))) : (x += s) {
            try appendValue(allocator, &b.elements, "bigint", @as(i64, @intCast(x)));
        }
        try b.endRow(allocator, true);
    }
    try b.finish(allocator, out);
}

fn repeatKernel(allocator: Allocator, arg_types: []const Type, out_type: Type, args: []const ColumnView, out: *ColumnStore, rows: usize) anyerror!void {
    _ = arg_types;
    var b = try ArrayBuilder.init(allocator, out_type.arrayElement().?);
    defer b.deinit(allocator);
    for (0..rows) |r| {
        const n = intAt(args[1], r) orelse {
            try b.endRow(allocator, false);
            continue;
        };
        for (0..@intCast(@max(n, 0))) |_| try b.appendOne(allocator, args[0], r);
        try b.endRow(allocator, true);
    }
    try b.finish(allocator, out);
}

// ---------------------------------------------------------------------------
// One array
// ---------------------------------------------------------------------------

fn lengthKernel(allocator: Allocator, arg_types: []const Type, out_type: Type, args: []const ColumnView, out: *ColumnStore, rows: usize) anyerror!void {
    _ = arg_types;
    _ = out_type;
    for (0..rows) |r| {
        const a = arrayAt(args[0], r) orelse {
            try out.appendNulls(allocator, 1);
            continue;
        };
        try appendValue(allocator, out, "int", @as(i32, @intCast(a.count)));
    }
}

/// `arr[i]`: the 1-based i-th element, NULL past either end.
fn elementAtKernel(allocator: Allocator, arg_types: []const Type, out_type: Type, args: []const ColumnView, out: *ColumnStore, rows: usize) anyerror!void {
    _ = arg_types;
    _ = out_type;
    for (0..rows) |r| {
        const a = arrayAt(args[0], r);
        const i = intAt(args[1], r);
        if (a == null or i == null or i.? < 1 or i.? > a.?.count or !a.?.isValid(@intCast(i.? - 1))) {
            try out.appendNulls(allocator, 1);
            continue;
        }
        try nested.appendRaw(allocator, out, a.?.element(@intCast(i.? - 1)));
    }
}

/// ARRAY_SLICE(arr, offset[, length]): from the 1-based offset, or counted
/// from the end when negative, `length` elements or to the end.
fn sliceKernel(allocator: Allocator, arg_types: []const Type, out_type: Type, args: []const ColumnView, out: *ColumnStore, rows: usize) anyerror!void {
    _ = out_type;
    const element = arg_types[0].arrayElement().?;
    var fa = try FlatAs.init(allocator, arg_types[0], args[0], element, rows);
    defer fa.deinit(allocator);
    var b = try ArrayBuilder.init(allocator, element);
    defer b.deinit(allocator);
    for (0..rows) |r| {
        const offset = intAt(args[1], r);
        const length: ?i64 = if (args.len == 3) intAt(args[2], r) else std.math.maxInt(i64);
        if (!args[0].isValid(r) or offset == null or length == null) {
            try b.endRow(allocator, false);
            continue;
        }
        const s = fa.start(r);
        const n: i64 = @intCast(fa.end(r) - s);
        const first: i64 = if (offset.? > 0) offset.? - 1 else if (offset.? < 0) @max(n + offset.?, 0) else n;
        const last: i64 = @min(n, first +| @max(length.?, 0));
        if (first < last) try b.appendRange(allocator, fa.elements.view, s + @as(usize, @intCast(first)), s + @as(usize, @intCast(last)));
        try b.endRow(allocator, true);
    }
    try b.finish(allocator, out);
}

fn reverseKernel(allocator: Allocator, arg_types: []const Type, out_type: Type, args: []const ColumnView, out: *ColumnStore, rows: usize) anyerror!void {
    _ = out_type;
    const element = arg_types[0].arrayElement().?;
    var fa = try FlatAs.init(allocator, arg_types[0], args[0], element, rows);
    defer fa.deinit(allocator);
    var b = try ArrayBuilder.init(allocator, element);
    defer b.deinit(allocator);
    for (0..rows) |r| {
        var i = fa.end(r);
        while (i > fa.start(r)) {
            i -= 1;
            try b.appendOne(allocator, fa.elements.view, i);
        }
        try b.endRow(allocator, args[0].isValid(r));
    }
    try b.finish(allocator, out);
}

/// ARRAY_DISTINCT: each value's first occurrence, in order; NULL once.
fn distinctKernel(allocator: Allocator, arg_types: []const Type, out_type: Type, args: []const ColumnView, out: *ColumnStore, rows: usize) anyerror!void {
    _ = out_type;
    const element = arg_types[0].arrayElement().?;
    var fa = try FlatAs.init(allocator, arg_types[0], args[0], element, rows);
    defer fa.deinit(allocator);
    var b = try ArrayBuilder.init(allocator, element);
    defer b.deinit(allocator);
    var seen: std.StringHashMapUnmanaged(void) = .empty;
    defer seen.deinit(allocator);
    const v = fa.elements.view;
    for (0..rows) |r| {
        seen.clearRetainingCapacity();
        var saw_null = false;
        for (fa.start(r)..fa.end(r)) |i| {
            if (!v.isValid(i)) {
                if (saw_null) continue;
                saw_null = true;
            } else if ((try seen.getOrPut(allocator, cellBytes(v, i))).found_existing) continue;
            try b.appendOne(allocator, v, i);
        }
        try b.endRow(allocator, args[0].isValid(r));
    }
    try b.finish(allocator, out);
}

/// ARRAY_SORT: ascending, NULLs first.
fn sortKernel(allocator: Allocator, arg_types: []const Type, out_type: Type, args: []const ColumnView, out: *ColumnStore, rows: usize) anyerror!void {
    _ = out_type;
    const element = arg_types[0].arrayElement().?;
    var fa = try FlatAs.init(allocator, arg_types[0], args[0], element, rows);
    defer fa.deinit(allocator);
    var b = try ArrayBuilder.init(allocator, element);
    defer b.deinit(allocator);
    var order: std.ArrayList(usize) = .empty;
    defer order.deinit(allocator);
    const v = fa.elements.view;
    for (0..rows) |r| {
        order.clearRetainingCapacity();
        for (fa.start(r)..fa.end(r)) |i| try order.append(allocator, i);
        std.sort.block(usize, order.items, v, struct {
            fn lessThan(view: ColumnView, x: usize, y: usize) bool {
                return cellOrder(view, x, y) == .lt;
            }
        }.lessThan);
        for (order.items) |i| try b.appendOne(allocator, v, i);
        try b.endRow(allocator, args[0].isValid(r));
    }
    try b.finish(allocator, out);
}

/// ARRAY_FILTER(arr, keep): the elements whose place in `keep` holds true;
/// none past `keep`'s end, and none when `keep` is NULL.
fn filterKernel(allocator: Allocator, arg_types: []const Type, out_type: Type, args: []const ColumnView, out: *ColumnStore, rows: usize) anyerror!void {
    _ = out_type;
    const element = arg_types[0].arrayElement().?;
    var fa = try FlatAs.init(allocator, arg_types[0], args[0], element, rows);
    defer fa.deinit(allocator);
    var keep = try FlatAs.init(allocator, arg_types[1], args[1], .boolean, rows);
    defer keep.deinit(allocator);
    var b = try ArrayBuilder.init(allocator, element);
    defer b.deinit(allocator);
    const flags = keep.elements.view;
    for (0..rows) |r| {
        const n = @min(fa.end(r) - fa.start(r), keep.end(r) - keep.start(r));
        for (0..n) |i| {
            const k = keep.start(r) + i;
            if (flags.isValid(k) and flags.data.boolean[k] != 0) try b.appendOne(allocator, fa.elements.view, fa.start(r) + i);
        }
        try b.endRow(allocator, args[0].isValid(r));
    }
    try b.finish(allocator, out);
}

fn anyMatchKernel(allocator: Allocator, arg_types: []const Type, out_type: Type, args: []const ColumnView, out: *ColumnStore, rows: usize) anyerror!void {
    try matchKernel(allocator, arg_types, out_type, args, out, rows, true);
}

fn allMatchKernel(allocator: Allocator, arg_types: []const Type, out_type: Type, args: []const ColumnView, out: *ColumnStore, rows: usize) anyerror!void {
    try matchKernel(allocator, arg_types, out_type, args, out, rows, false);
}

/// ANY_MATCH (`decides` true): whether an element is true. ALL_MATCH
/// (`decides` false): whether none is false. An element that decides
/// neither way, a NULL, makes the answer NULL when no other decides it.
fn matchKernel(allocator: Allocator, arg_types: []const Type, out_type: Type, args: []const ColumnView, out: *ColumnStore, rows: usize, comptime decides: bool) !void {
    _ = out_type;
    var fa = try FlatAs.init(allocator, arg_types[0], args[0], .boolean, rows);
    defer fa.deinit(allocator);
    const v = fa.elements.view;
    for (0..rows) |r| {
        if (!args[0].isValid(r)) {
            try out.appendNulls(allocator, 1);
            continue;
        }
        var saw_null = false;
        const decided = for (fa.start(r)..fa.end(r)) |i| {
            if (!v.isValid(i)) {
                saw_null = true;
            } else if ((v.data.boolean[i] != 0) == decides) break true;
        } else false;
        if (!decided and saw_null) {
            try out.appendNulls(allocator, 1);
            continue;
        }
        try appendValue(allocator, out, "boolean", @intFromBool(decided == decides));
    }
}

/// ARRAY_SORTBY(arr, keys...): `arr` in the order of its elements' keys,
/// compared one key array after another, NULLs first; ties keep their
/// order. A NULL key array is passed over; the others must be as long as
/// `arr`.
fn sortByKernel(allocator: Allocator, arg_types: []const Type, out_type: Type, args: []const ColumnView, out: *ColumnStore, rows: usize) anyerror!void {
    _ = out_type;
    const element = arg_types[0].arrayElement().?;
    var fa = try FlatAs.init(allocator, arg_types[0], args[0], element, rows);
    defer fa.deinit(allocator);
    const keys = try allocator.alloc(FlatAs, args.len - 1);
    var built: usize = 0;
    defer {
        for (keys[0..built]) |*k| k.deinit(allocator);
        allocator.free(keys);
    }
    for (keys, arg_types[1..], args[1..]) |*k, t, a| {
        k.* = try FlatAs.init(allocator, t, a, t.arrayElement().?, rows);
        built += 1;
    }
    const SortKey = struct { view: ColumnView, start: usize };
    const row_keys = try allocator.alloc(SortKey, keys.len);
    defer allocator.free(row_keys);
    var b = try ArrayBuilder.init(allocator, element);
    defer b.deinit(allocator);
    var order: std.ArrayList(usize) = .empty;
    defer order.deinit(allocator);
    for (0..rows) |r| {
        if (!args[0].isValid(r)) {
            try b.endRow(allocator, false);
            continue;
        }
        const s = fa.start(r);
        const n = fa.end(r) - s;
        var used: usize = 0;
        for (keys, args[1..]) |*k, a| {
            if (!a.isValid(r)) continue;
            if (k.end(r) - k.start(r) != n) return Error.ArrayLengthMismatch;
            row_keys[used] = .{ .view = k.elements.view, .start = k.start(r) };
            used += 1;
        }
        order.clearRetainingCapacity();
        for (0..n) |i| try order.append(allocator, i);
        std.sort.block(usize, order.items, row_keys[0..used], struct {
            fn lessThan(ks: []const SortKey, x: usize, y: usize) bool {
                for (ks) |k| switch (cellOrder(k.view, k.start + x, k.start + y)) {
                    .lt => return true,
                    .gt => return false,
                    .eq => {},
                };
                return false;
            }
        }.lessThan);
        for (order.items) |i| try b.appendOne(allocator, fa.elements.view, s + i);
        try b.endRow(allocator, true);
    }
    try b.finish(allocator, out);
}

fn minKernel(allocator: Allocator, arg_types: []const Type, out_type: Type, args: []const ColumnView, out: *ColumnStore, rows: usize) anyerror!void {
    try extremeKernel(allocator, arg_types, out_type, args, out, rows, .lt);
}

fn maxKernel(allocator: Allocator, arg_types: []const Type, out_type: Type, args: []const ColumnView, out: *ColumnStore, rows: usize) anyerror!void {
    try extremeKernel(allocator, arg_types, out_type, args, out, rows, .gt);
}

/// The least (`.lt`) or greatest (`.gt`) element that isn't NULL; NULL when
/// there is none.
fn extremeKernel(allocator: Allocator, arg_types: []const Type, out_type: Type, args: []const ColumnView, out: *ColumnStore, rows: usize, comptime wins: std.math.Order) !void {
    _ = out_type;
    const element = arg_types[0].arrayElement().?;
    var fa = try FlatAs.init(allocator, arg_types[0], args[0], element, rows);
    defer fa.deinit(allocator);
    const v = fa.elements.view;
    for (0..rows) |r| {
        var best: ?usize = null;
        for (fa.start(r)..fa.end(r)) |i| {
            if (!v.isValid(i)) continue;
            if (best == null or cellOrder(v, i, best.?) == wins) best = i;
        }
        if (best) |i| try store.appendViewRange(allocator, out, v, i, i + 1) else try out.appendNulls(allocator, 1);
    }
}

fn sumKernel(allocator: Allocator, arg_types: []const Type, out_type: Type, args: []const ColumnView, out: *ColumnStore, rows: usize) anyerror!void {
    var fa = try FlatAs.init(allocator, arg_types[0], args[0], out_type, rows);
    defer fa.deinit(allocator);
    const v = fa.elements.view;
    switch (v.data) {
        inline .bigint, .largeint, .double, .decimal128 => |s, tag| for (0..rows) |r| {
            const T = std.meta.Elem(@TypeOf(s));
            var sum: T = 0;
            var any = false;
            for (fa.start(r)..fa.end(r)) |i| {
                if (!v.isValid(i)) continue;
                sum = if (T == f64) sum + s[i] else sum +% s[i];
                any = true;
            }
            if (any) try appendValue(allocator, out, @tagName(tag), sum) else try out.appendNulls(allocator, 1);
        },
        else => return Error.ComputeNoSuchOverload,
    }
}

const VectorMeasure = enum { cosine_similarity, inner_product, l2_distance };

/// COSINE_SIMILARITY(a, b), COSINE_SIMILARITY_NORM(a, b) (the inner product,
/// for vectors already of length 1) and L2_DISTANCE(a, b) (the squared
/// distance), as StarRocks defines them, over equal-length numeric arrays in
/// double precision. A NULL array or element gives NULL, as does a cosine
/// with a zero vector; arrays of different lengths are `ArrayLengthMismatch`.
fn VectorKernel(comptime measure: VectorMeasure) TypedKernel {
    return struct {
        fn kernel(allocator: Allocator, arg_types: []const Type, out_type: Type, args: []const ColumnView, out: *ColumnStore, rows: usize) anyerror!void {
            _ = out_type;
            var a = try FlatAs.init(allocator, arg_types[0], args[0], .double, rows);
            defer a.deinit(allocator);
            var b = try FlatAs.init(allocator, arg_types[1], args[1], .double, rows);
            defer b.deinit(allocator);
            const av = a.elements.view;
            const bv = b.elements.view;
            for (0..rows) |r| {
                if (!args[0].isValid(r) or !args[1].isValid(r)) {
                    try out.appendNulls(allocator, 1);
                    continue;
                }
                const as = a.start(r);
                const bs = b.start(r);
                const n = a.end(r) - as;
                if (b.end(r) - bs != n) return Error.ArrayLengthMismatch;
                var dot: f64 = 0;
                var a_norm: f64 = 0;
                var b_norm: f64 = 0;
                var any_null = false;
                for (0..n) |i| {
                    if (!av.isValid(as + i) or !bv.isValid(bs + i)) {
                        any_null = true;
                        break;
                    }
                    const x = av.data.double[as + i];
                    const y = bv.data.double[bs + i];
                    switch (measure) {
                        .l2_distance => dot += (x - y) * (x - y),
                        .inner_product => dot += x * y,
                        .cosine_similarity => {
                            dot += x * y;
                            a_norm += x * x;
                            b_norm += y * y;
                        },
                    }
                }
                const zero_vector = measure == .cosine_similarity and (a_norm == 0 or b_norm == 0);
                if (any_null or zero_vector) {
                    try out.appendNulls(allocator, 1);
                    continue;
                }
                const value = if (measure == .cosine_similarity) dot / (@sqrt(a_norm) * @sqrt(b_norm)) else dot;
                try appendValue(allocator, out, "double", value);
            }
        }
    }.kernel;
}

/// ARRAY_AVG: the mean of the elements that aren't NULL, a DECIMAL's
/// rounded half away from zero at its six extra digits.
fn avgKernel(allocator: Allocator, arg_types: []const Type, out_type: Type, args: []const ColumnView, out: *ColumnStore, rows: usize) anyerror!void {
    const element = arg_types[0].arrayElement().?;
    const sum_type = sumType(element).?;
    var fa = try FlatAs.init(allocator, arg_types[0], args[0], if (sum_type.isDecimal()) sum_type else .double, rows);
    defer fa.deinit(allocator);
    const v = fa.elements.view;
    for (0..rows) |r| {
        var count: usize = 0;
        for (fa.start(r)..fa.end(r)) |i| count += @intFromBool(v.isValid(i));
        if (count == 0) {
            try out.appendNulls(allocator, 1);
            continue;
        }
        switch (v.data) {
            .double => |s| {
                var sum: f64 = 0;
                for (fa.start(r)..fa.end(r)) |i| {
                    if (v.isValid(i)) sum += s[i];
                }
                try appendValue(allocator, out, "double", sum / @as(f64, @floatFromInt(count)));
            },
            .decimal128 => |s| {
                var sum: i256 = 0;
                for (fa.start(r)..fa.end(r)) |i| {
                    if (v.isValid(i)) sum += s[i];
                }
                const extra = out_type.decimalSpec().?.s - sum_type.decimalSpec().?.s;
                const scaled = sum * std.math.pow(i256, 10, extra);
                const n: i256 = @intCast(count);
                const q = @divTrunc(scaled, n);
                const rem = @rem(scaled, n);
                const rounded = if (2 * @abs(rem) >= n) (if (scaled < 0) q - 1 else q + 1) else q;
                try appendValue(allocator, out, "decimal128", @as(i128, @truncate(rounded)));
            },
            else => return Error.ComputeNoSuchOverload,
        }
    }
}

/// ARRAY_DIFFERENCE: each element less the one before it, the first 0;
/// NULL beside a NULL.
fn differenceKernel(allocator: Allocator, arg_types: []const Type, out_type: Type, args: []const ColumnView, out: *ColumnStore, rows: usize) anyerror!void {
    const element = out_type.arrayElement().?;
    var fa = try FlatAs.init(allocator, arg_types[0], args[0], element, rows);
    defer fa.deinit(allocator);
    var b = try ArrayBuilder.init(allocator, element);
    defer b.deinit(allocator);
    const v = fa.elements.view;
    switch (v.data) {
        inline .bigint, .largeint, .double, .decimal128 => |s, tag| for (0..rows) |r| {
            const start = fa.start(r);
            for (start..fa.end(r)) |i| {
                if (!v.isValid(i) or (i > start and !v.isValid(i - 1))) {
                    try b.elements.appendNulls(allocator, 1);
                } else {
                    const T = std.meta.Elem(@TypeOf(s));
                    const d: T = if (i == start) 0 else if (T == f64) s[i] - s[i - 1] else s[i] -% s[i - 1];
                    try appendValue(allocator, &b.elements, @tagName(tag), d);
                }
            }
            try b.endRow(allocator, args[0].isValid(r));
        },
        else => return Error.ComputeNoSuchOverload,
    }
    try b.finish(allocator, out);
}

/// ARRAY_FLATTEN: an array of arrays as one array of their elements, a
/// NULL inner array adding none.
fn flattenKernel(allocator: Allocator, arg_types: []const Type, out_type: Type, args: []const ColumnView, out: *ColumnStore, rows: usize) anyerror!void {
    const inner_type = arg_types[0].arrayElement().?;
    var outer = try nested.flatten(allocator, inner_type, args[0], rows);
    defer outer.deinit(allocator);
    var inner = try nested.flatten(allocator, inner_type.arrayElement().?, outer.elements.view(), outer.elements.rowCount());
    defer inner.deinit(allocator);
    const offsets = try allocator.alloc(u32, rows + 1);
    defer allocator.free(offsets);
    for (offsets, outer.offsets.items) |*o, at| o.* = inner.offsets.items[at];
    try nested.encodeColumn(allocator, out, nested.ElemClass.of(out_type.arrayElement().?), inner.elements.view(), offsets, args[0].nulls, rows);
}

/// ARRAY_JOIN(arr, sep[, null_text]): the elements as text between `sep`,
/// a NULL element as `null_text` or skipped without it.
fn joinKernel(allocator: Allocator, arg_types: []const Type, out_type: Type, args: []const ColumnView, out: *ColumnStore, rows: usize) anyerror!void {
    _ = out_type;
    var fa = try FlatAs.init(allocator, arg_types[0], args[0], .string, rows);
    defer fa.deinit(allocator);
    const v = fa.elements.view;
    const sep = common.stringViewOf(args[1]);
    var text: std.ArrayList(u8) = .empty;
    defer text.deinit(allocator);
    for (0..rows) |r| {
        if (!args[0].isValid(r) or !args[1].isValid(r)) {
            try out.appendNulls(allocator, 1);
            continue;
        }
        const null_text: ?[]const u8 = if (args.len == 3 and args[2].isValid(r)) common.stringViewOf(args[2]).rowBytes(r) else null;
        text.clearRetainingCapacity();
        var first = true;
        for (fa.start(r)..fa.end(r)) |i| {
            const part = if (v.isValid(i)) cellBytes(v, i) else null_text orelse continue;
            if (!first) try text.appendSlice(allocator, sep.rowBytes(r));
            first = false;
            try text.appendSlice(allocator, part);
        }
        try appendText(allocator, out, text.items);
    }
}

// ---------------------------------------------------------------------------
// An array and a value
// ---------------------------------------------------------------------------

/// An array argument's elements and a value argument, both as the type they
/// compare in (`comparedType`).
const ArrayAndValue = struct {
    fa: FlatAs,
    value: AsType,

    fn init(allocator: Allocator, arg_types: []const Type, args: []const ColumnView, rows: usize) !ArrayAndValue {
        const c = comparedType(arg_types[0].arrayElement().?, arg_types[1]) orelse return Error.ComputeNoSuchOverload;
        var fa = try FlatAs.init(allocator, arg_types[0], args[0], c, rows);
        errdefer fa.deinit(allocator);
        return .{ .fa = fa, .value = try AsType.init(allocator, args[1], arg_types[1], c, rows) };
    }

    fn deinit(self: *ArrayAndValue, allocator: Allocator) void {
        self.value.deinit(allocator);
        self.fa.deinit(allocator);
    }

    fn find(self: *const ArrayAndValue, row: usize) ?usize {
        return findIn(self.fa.elements.view, self.fa.start(row), self.fa.end(row), self.value.view, row);
    }
};

fn containsKernel(allocator: Allocator, arg_types: []const Type, out_type: Type, args: []const ColumnView, out: *ColumnStore, rows: usize) anyerror!void {
    _ = out_type;
    var av = try ArrayAndValue.init(allocator, arg_types, args, rows);
    defer av.deinit(allocator);
    for (0..rows) |r| {
        if (!args[0].isValid(r)) {
            try out.appendNulls(allocator, 1);
            continue;
        }
        try appendValue(allocator, out, "boolean", @intFromBool(av.find(r) != null));
    }
}

/// ARRAY_POSITION: the 1-based index of the first element equal to the
/// value, 0 when none is.
fn positionKernel(allocator: Allocator, arg_types: []const Type, out_type: Type, args: []const ColumnView, out: *ColumnStore, rows: usize) anyerror!void {
    _ = out_type;
    var av = try ArrayAndValue.init(allocator, arg_types, args, rows);
    defer av.deinit(allocator);
    for (0..rows) |r| {
        if (!args[0].isValid(r)) {
            try out.appendNulls(allocator, 1);
            continue;
        }
        const at: i32 = if (av.find(r)) |i| @intCast(i - av.fa.start(r) + 1) else 0;
        try appendValue(allocator, out, "int", at);
    }
}

/// ARRAY_REMOVE: the elements not equal to the value, in their own type.
fn removeKernel(allocator: Allocator, arg_types: []const Type, out_type: Type, args: []const ColumnView, out: *ColumnStore, rows: usize) anyerror!void {
    _ = out_type;
    var av = try ArrayAndValue.init(allocator, arg_types, args, rows);
    defer av.deinit(allocator);
    const element = arg_types[0].arrayElement().?;
    var b = try ArrayBuilder.init(allocator, element);
    defer b.deinit(allocator);
    const original = av.fa.flat.elements.view();
    for (0..rows) |r| {
        for (av.fa.start(r)..av.fa.end(r)) |i| {
            if (!cellsEqual(av.fa.elements.view, i, av.value.view, r)) try b.appendOne(allocator, original, i);
        }
        try b.endRow(allocator, args[0].isValid(r));
    }
    try b.finish(allocator, out);
}

fn appendKernel(allocator: Allocator, arg_types: []const Type, out_type: Type, args: []const ColumnView, out: *ColumnStore, rows: usize) anyerror!void {
    var av = try ArrayAndValue.init(allocator, arg_types, args, rows);
    defer av.deinit(allocator);
    var b = try ArrayBuilder.init(allocator, out_type.arrayElement().?);
    defer b.deinit(allocator);
    for (0..rows) |r| {
        if (args[0].isValid(r)) {
            try b.appendRange(allocator, av.fa.elements.view, av.fa.start(r), av.fa.end(r));
            try b.appendOne(allocator, av.value.view, r);
        }
        try b.endRow(allocator, args[0].isValid(r));
    }
    try b.finish(allocator, out);
}

// ---------------------------------------------------------------------------
// Several arrays
// ---------------------------------------------------------------------------

/// Every argument's arrays, their elements as their common type
/// (`commonElementOf`).
const Arrays = struct {
    items: []FlatAs,

    fn init(allocator: Allocator, arg_types: []const Type, args: []const ColumnView, rows: usize) !Arrays {
        const c = commonElementOf(arg_types) orelse return Error.ComputeNoSuchOverload;
        const items = try allocator.alloc(FlatAs, args.len);
        var built: usize = 0;
        errdefer {
            for (items[0..built]) |*fa| fa.deinit(allocator);
            allocator.free(items);
        }
        for (items, arg_types, args) |*fa, t, a| {
            fa.* = try FlatAs.init(allocator, t, a, c, rows);
            built += 1;
        }
        return .{ .items = items };
    }

    fn deinit(self: *Arrays, allocator: Allocator) void {
        for (self.items) |*fa| fa.deinit(allocator);
        allocator.free(self.items);
    }
};

fn allValid(args: []const ColumnView, row: usize) bool {
    for (args) |a| if (!a.isValid(row)) return false;
    return true;
}

/// ARRAY_CONCAT: the arrays one after another; NULL when any is.
fn concatKernel(allocator: Allocator, arg_types: []const Type, out_type: Type, args: []const ColumnView, out: *ColumnStore, rows: usize) anyerror!void {
    var arrays = try Arrays.init(allocator, arg_types, args, rows);
    defer arrays.deinit(allocator);
    var b = try ArrayBuilder.init(allocator, out_type.arrayElement().?);
    defer b.deinit(allocator);
    for (0..rows) |r| {
        const valid = allValid(args, r);
        if (valid) for (arrays.items) |*fa| try b.appendRange(allocator, fa.elements.view, fa.start(r), fa.end(r));
        try b.endRow(allocator, valid);
    }
    try b.finish(allocator, out);
}

/// ARRAY_INTERSECT: the distinct elements of the first array that every
/// other array holds too.
fn intersectKernel(allocator: Allocator, arg_types: []const Type, out_type: Type, args: []const ColumnView, out: *ColumnStore, rows: usize) anyerror!void {
    var arrays = try Arrays.init(allocator, arg_types, args, rows);
    defer arrays.deinit(allocator);
    var b = try ArrayBuilder.init(allocator, out_type.arrayElement().?);
    defer b.deinit(allocator);
    const first = &arrays.items[0];
    const v = first.elements.view;
    for (0..rows) |r| {
        const valid = allValid(args, r);
        if (valid) {
            const kept_from = b.elements.rowCount();
            next: for (first.start(r)..first.end(r)) |i| {
                if (findIn(b.elements.view(), kept_from, b.elements.rowCount(), v, i) != null) continue;
                for (arrays.items[1..]) |*other| {
                    if (findIn(other.elements.view, other.start(r), other.end(r), v, i) == null) continue :next;
                }
                try b.appendOne(allocator, v, i);
            }
        }
        try b.endRow(allocator, valid);
    }
    try b.finish(allocator, out);
}

fn overlapKernel(allocator: Allocator, arg_types: []const Type, out_type: Type, args: []const ColumnView, out: *ColumnStore, rows: usize) anyerror!void {
    try pairKernel(allocator, arg_types, out_type, args, out, rows, .overlap);
}

fn containsAllKernel(allocator: Allocator, arg_types: []const Type, out_type: Type, args: []const ColumnView, out: *ColumnStore, rows: usize) anyerror!void {
    try pairKernel(allocator, arg_types, out_type, args, out, rows, .contains_all);
}

fn containsSeqKernel(allocator: Allocator, arg_types: []const Type, out_type: Type, args: []const ColumnView, out: *ColumnStore, rows: usize) anyerror!void {
    try pairKernel(allocator, arg_types, out_type, args, out, rows, .contains_seq);
}

const PairTest = enum {
    /// Some element of the first is in the second.
    overlap,
    /// Every element of the second is in the first.
    contains_all,
    /// The second's elements appear in the first, in order and adjacent.
    contains_seq,
};

fn pairKernel(allocator: Allocator, arg_types: []const Type, out_type: Type, args: []const ColumnView, out: *ColumnStore, rows: usize, comptime which: PairTest) !void {
    _ = out_type;
    var arrays = try Arrays.init(allocator, arg_types, args, rows);
    defer arrays.deinit(allocator);
    const a = &arrays.items[0];
    const b = &arrays.items[1];
    const av = a.elements.view;
    const bv = b.elements.view;
    for (0..rows) |r| {
        if (!allValid(args, r)) {
            try out.appendNulls(allocator, 1);
            continue;
        }
        const holds = switch (which) {
            .overlap => blk: {
                for (a.start(r)..a.end(r)) |i| if (findIn(bv, b.start(r), b.end(r), av, i) != null) break :blk true;
                break :blk false;
            },
            .contains_all => blk: {
                for (b.start(r)..b.end(r)) |j| if (findIn(av, a.start(r), a.end(r), bv, j) == null) break :blk false;
                break :blk true;
            },
            .contains_seq => blk: {
                const n = b.end(r) - b.start(r);
                if (n > a.end(r) - a.start(r)) break :blk false;
                outer: for (a.start(r)..a.end(r) - n + 1) |i| {
                    for (0..n) |k| if (!cellsEqual(av, i + k, bv, b.start(r) + k)) continue :outer;
                    break :blk true;
                }
                break :blk false;
            },
        };
        try appendValue(allocator, out, "boolean", @intFromBool(holds));
    }
}

/// The type two arrays compare as: their common type, or the array's beside
/// text, which reads as one.
fn orderedType(a: Type, b: Type) ?Type {
    if (isText(b)) return if (a.isNested()) a else null;
    if (isText(a)) return if (b.isNested()) b else null;
    return cast.commonType(a, b);
}

/// ARRAY_ORDER: -1, 0 or 1 as the first nested value sorts before, with or
/// after the second, both as `orderedType` (`nested.order`).
fn orderKernel(allocator: Allocator, arg_types: []const Type, out_type: Type, args: []const ColumnView, out: *ColumnStore, rows: usize) anyerror!void {
    _ = out_type;
    const t = orderedType(arg_types[0], arg_types[1]) orelse return Error.ComputeNoSuchOverload;
    var a = try AsType.init(allocator, args[0], arg_types[0], t, rows);
    defer a.deinit(allocator);
    var b = try AsType.init(allocator, args[1], arg_types[1], t, rows);
    defer b.deinit(allocator);
    for (0..rows) |r| {
        if (!a.view.isValid(r) or !b.view.isValid(r)) {
            try out.appendNulls(allocator, 1);
            continue;
        }
        const order: i32 = switch (nested.order(cellBytes(a.view, r), cellBytes(b.view, r))) {
            .lt => -1,
            .eq => 0,
            .gt => 1,
        };
        try appendValue(allocator, out, "int", order);
    }
}

test "array resolution: types and NULL placeholders" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const aa = arena.allocator();
    const ints = try types.arrayOf(.int);
    const texts = try types.arrayOf(.string);

    const value = (try resolveArray(aa, scalar_fn.ARRAY_VALUE_FN, &.{ .int, .int })).?;
    try std.testing.expect(std.meta.eql(ints, value.func.return_type));
    try std.testing.expect(try resolveArray(aa, scalar_fn.ARRAY_VALUE_FN, &.{ .int, .string }) == null);

    try std.testing.expectEqual(Type.int, (try resolveArray(aa, "array_length", &.{ints})).?.func.return_type);
    try std.testing.expect(std.meta.eql(ints, (try resolveArray(aa, "array_append", &.{ ints, .string })).?.func.return_type));
    try std.testing.expect(std.meta.eql(texts, (try resolveArray(aa, "array_concat", &.{ texts, ints })).?.func.return_type));
    try std.testing.expect(try resolveArray(aa, "element_at", &.{ ints, .string }) == null);
    try std.testing.expect(try resolveArray(aa, "array_length", &.{.string}) == null);
}
