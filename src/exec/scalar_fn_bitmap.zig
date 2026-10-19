//! BITMAP functions over `bitmap.zig`'s sets, as StarRocks names them:
//! building a bitmap from a value, text, an array or StarRocks' bytes,
//! reading one back, set algebra, and the subsets StarRocks cuts by value
//! or by rank. A stored bitmap is in the one layout `Bitmap.serialize`
//! writes for its set, so a kernel that keeps a bitmap whole copies its
//! bytes.

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
const common = @import("scalar_fn_common.zig");
const array = @import("scalar_fn_array.zig");
const overload = array.overload;
const string = @import("scalar_fn_string.zig");
const nested = @import("nested.zig");
const bitmap = @import("bitmap.zig");
const Bitmap = bitmap.Bitmap;
const Error = @import("exec.zig").Error;

/// Names that resolve here, for `scalar_fn.nameResolvable`.
const NAMES = [_][]const u8{
    "to_bitmap",          "bitmap_empty",           "bitmap_from_string",  "bitmap_to_string", "bitmap_count",
    "bitmap_min",         "bitmap_max",             "bitmap_contains",     "bitmap_has_any",   "bitmap_or",
    "bitmap_and",         "bitmap_xor",             "bitmap_andnot",       "bitmap_remove",    "bitmap_to_array",
    "array_to_bitmap",    "bitmap_subset_in_range", "bitmap_subset_limit", "sub_bitmap",       "bitmap_to_binary",
    "bitmap_from_binary", "bitmap_to_base64",       "base64_to_bitmap",    "bitmap_hash",
};

pub fn isBitmapFnName(name: []const u8) bool {
    for (NAMES) |n| if (eql(name, n)) return true;
    return false;
}

fn eql(a: []const u8, b: []const u8) bool {
    return std.ascii.eqlIgnoreCase(a, b);
}

/// The types whose values `to_bitmap` and `bitmap_agg` take as members.
pub fn isMemberType(t: Type) bool {
    return t.isInteger() or t == .boolean or array.isText(t);
}

/// Row `r` of `v` as a bitmap member: null when it is NULL or no unsigned
/// 64-bit integer — negative, too large, or text that spells none.
pub fn memberAt(v: ColumnView, r: usize) ?u64 {
    if (!v.isValid(r)) return null;
    return switch (v.data) {
        inline .tinyint, .smallint, .int, .bigint, .largeint => |s| std.math.cast(u64, s[r]),
        .boolean => |s| @intFromBool(s[r] != 0),
        .varchar, .string, .char => |sv| bitmap.parseMember(sv.rowBytes(r)),
        else => null,
    };
}

/// The overload of a bitmap function, or null when `name` is none or its
/// arguments don't fit it.
pub fn resolveBitmap(aa: Allocator, name: []const u8, arg_types: []const Type) Allocator.Error!?ResolvedOverload {
    if (arg_types.len == 0) {
        if (eql(name, "bitmap_empty")) return overload(aa, name, arg_types, .bitmap, emptyKernel);
        return null;
    }
    const first = arg_types[0];
    if (arg_types.len == 1) {
        if (eql(name, "to_bitmap")) return if (isMemberType(first)) overload(aa, name, arg_types, .bitmap, toBitmapKernel) else null;
        // StarRocks hashes a number as the text CAST gives it.
        if (eql(name, "bitmap_hash")) return if (array.isText(first) or first.isInteger()) overload(aa, name, arg_types, .bitmap, hashKernel) else null;
        if (eql(name, "array_to_bitmap")) {
            const element = first.arrayElement() orelse return null;
            return if (isMemberType(element)) overload(aa, name, arg_types, .bitmap, fromArrayKernel) else null;
        }
        if (array.isText(first)) {
            if (eql(name, "bitmap_from_string")) return overload(aa, name, arg_types, .bitmap, fromStringKernel);
            if (eql(name, "bitmap_from_binary")) return overload(aa, name, arg_types, .bitmap, fromBinaryKernel);
            if (eql(name, "base64_to_bitmap")) return overload(aa, name, arg_types, .bitmap, fromBase64Kernel);
            return null;
        }
        if (first != .bitmap) return null;
        if (eql(name, "bitmap_to_string")) return overload(aa, name, arg_types, .string, toStringKernel);
        if (eql(name, "bitmap_count")) return overload(aa, name, arg_types, .bigint, countKernel);
        // A member can pass BIGINT's range, as in StarRocks.
        if (eql(name, "bitmap_min")) return overload(aa, name, arg_types, .largeint, extremeKernel(.min));
        if (eql(name, "bitmap_max")) return overload(aa, name, arg_types, .largeint, extremeKernel(.max));
        if (eql(name, "bitmap_to_array")) return overload(aa, name, arg_types, try types.arrayOf(.bigint), toArrayKernel);
        if (eql(name, "bitmap_to_binary")) return overload(aa, name, arg_types, .string, toBinaryKernel);
        if (eql(name, "bitmap_to_base64")) return overload(aa, name, arg_types, .string, toBase64Kernel);
        return null;
    }
    if (first != .bitmap) return null;
    if (arg_types.len == 2) {
        const second = arg_types[1];
        if (second == .bitmap) {
            if (eql(name, "bitmap_or")) return overload(aa, name, arg_types, .bitmap, combineKernel(.@"or"));
            if (eql(name, "bitmap_and")) return overload(aa, name, arg_types, .bitmap, combineKernel(.@"and"));
            if (eql(name, "bitmap_xor")) return overload(aa, name, arg_types, .bitmap, combineKernel(.xor));
            if (eql(name, "bitmap_andnot")) return overload(aa, name, arg_types, .bitmap, combineKernel(.and_not));
            if (eql(name, "bitmap_has_any")) return overload(aa, name, arg_types, .boolean, hasAnyKernel);
            return null;
        }
        if (!second.isInteger()) return null;
        if (eql(name, "bitmap_contains")) return overload(aa, name, arg_types, .boolean, containsKernel);
        if (eql(name, "bitmap_remove")) return overload(aa, name, arg_types, .bitmap, removeKernel);
        return null;
    }
    if (arg_types.len == 3 and arg_types[1].isInteger() and arg_types[2].isInteger()) {
        if (eql(name, "bitmap_subset_in_range")) return overload(aa, name, arg_types, .bitmap, subsetKernel(.in_range));
        if (eql(name, "bitmap_subset_limit")) return overload(aa, name, arg_types, .bitmap, subsetKernel(.limit));
        if (eql(name, "sub_bitmap")) return overload(aa, name, arg_types, .bitmap, subsetKernel(.ranks));
    }
    return null;
}

// ---------------------------------------------------------------------------
// Reading and writing cells
// ---------------------------------------------------------------------------

/// Row `r` of a BITMAP column, or null when it is NULL. Stored bitmaps were
/// read on the way in, so one that doesn't read now is no bitmap at all.
fn bitmapAt(allocator: Allocator, v: ColumnView, r: usize) !?Bitmap {
    if (!v.isValid(r)) return null;
    return Bitmap.deserialize(allocator, v.data.bitmap.rowBytes(r)) catch |err| switch (err) {
        error.BitmapInvalid => return Error.TypeMismatch,
        error.OutOfMemory => return error.OutOfMemory,
    };
}

fn intAt(v: ColumnView, r: usize) ?i128 {
    if (!v.isValid(r)) return null;
    return switch (v.data) {
        inline .tinyint, .smallint, .int, .bigint, .largeint => |s| s[r],
        else => null,
    };
}

fn appendBytes(allocator: Allocator, out: *ColumnStore, bytes: []const u8) !void {
    try out.data.bitmap.appendValue(allocator, bytes);
    try out.appendValidBit(allocator, out.rowCount() - 1, true);
}

fn appendText(allocator: Allocator, out: *ColumnStore, text: []const u8) !void {
    try common.stringStoreOf(out).appendValue(allocator, text);
    try out.appendValidBit(allocator, out.rowCount() - 1, true);
}

/// Bitmaps appended to an output column, each serialized through one
/// reused buffer.
const BitmapOut = struct {
    out: *ColumnStore,
    buf: std.ArrayList(u8) = .empty,

    fn deinit(w: *BitmapOut, allocator: Allocator) void {
        w.buf.deinit(allocator);
    }

    fn append(w: *BitmapOut, allocator: Allocator, bm: *const Bitmap) !void {
        w.buf.clearRetainingCapacity();
        try bm.serialize(allocator, &w.buf);
        try appendBytes(allocator, w.out, w.buf.items);
    }

    fn appendSingle(w: *BitmapOut, allocator: Allocator, v: u64) !void {
        w.buf.clearRetainingCapacity();
        try bitmap.appendSingle(allocator, &w.buf, v);
        try appendBytes(allocator, w.out, w.buf.items);
    }

    /// `bytes`, a bitmap from outside, in the layout this engine stores;
    /// NULL when it is no bitmap.
    fn appendForeign(w: *BitmapOut, allocator: Allocator, bytes: []const u8) !void {
        w.buf.clearRetainingCapacity();
        bitmap.canonicalize(allocator, bytes, &w.buf) catch |err| switch (err) {
            error.BitmapInvalid => return w.out.appendNulls(allocator, 1),
            error.OutOfMemory => return error.OutOfMemory,
        };
        try appendBytes(allocator, w.out, w.buf.items);
    }
};

// ---------------------------------------------------------------------------
// Kernels
// ---------------------------------------------------------------------------

fn emptyKernel(allocator: Allocator, arg_types: []const Type, out_type: Type, args: []const ColumnView, out: *ColumnStore, rows: usize) anyerror!void {
    _ = arg_types;
    _ = out_type;
    _ = args;
    for (0..rows) |_| try appendBytes(allocator, out, &bitmap.EMPTY_BYTES);
}

/// TO_BITMAP(x): the set {x}; NULL when x is no member (`memberAt`).
fn toBitmapKernel(allocator: Allocator, arg_types: []const Type, out_type: Type, args: []const ColumnView, out: *ColumnStore, rows: usize) anyerror!void {
    _ = arg_types;
    _ = out_type;
    var w: BitmapOut = .{ .out = out };
    defer w.deinit(allocator);
    for (0..rows) |r| {
        const v = memberAt(args[0], r) orelse {
            try out.appendNulls(allocator, 1);
            continue;
        };
        try w.appendSingle(allocator, v);
    }
}

/// BITMAP_HASH(x): the set holding x's 32-bit MurmurHash3, as StarRocks
/// seeds it.
fn hashKernel(allocator: Allocator, arg_types: []const Type, out_type: Type, args: []const ColumnView, out: *ColumnStore, rows: usize) anyerror!void {
    _ = arg_types;
    _ = out_type;
    var w: BitmapOut = .{ .out = out };
    defer w.deinit(allocator);
    var digits: [48]u8 = undefined;
    for (0..rows) |r| {
        if (!args[0].isValid(r)) {
            try out.appendNulls(allocator, 1);
            continue;
        }
        const text = switch (args[0].data) {
            inline .tinyint, .smallint, .int, .bigint, .largeint => |s| std.fmt.bufPrint(&digits, "{d}", .{s[r]}) catch unreachable,
            else => common.stringViewOf(args[0]).rowBytes(r),
        };
        try w.appendSingle(allocator, std.hash.Murmur3_32.hashWithSeed(text, string.MURMUR3_32_SEED));
    }
}

/// BITMAP_FROM_STRING(text): the set a comma-separated list spells; NULL
/// when an item is no member (`Bitmap.parseText`).
fn fromStringKernel(allocator: Allocator, arg_types: []const Type, out_type: Type, args: []const ColumnView, out: *ColumnStore, rows: usize) anyerror!void {
    _ = arg_types;
    _ = out_type;
    var w: BitmapOut = .{ .out = out };
    defer w.deinit(allocator);
    const sv = common.stringViewOf(args[0]);
    for (0..rows) |r| {
        var parsed = (if (args[0].isValid(r)) try Bitmap.parseText(allocator, sv.rowBytes(r)) else null) orelse {
            try out.appendNulls(allocator, 1);
            continue;
        };
        defer parsed.deinit(allocator);
        try w.append(allocator, &parsed);
    }
}

/// BITMAP_FROM_BINARY(bytes): the bitmap StarRocks' bytes hold, in any of
/// its layouts; NULL when they hold none.
fn fromBinaryKernel(allocator: Allocator, arg_types: []const Type, out_type: Type, args: []const ColumnView, out: *ColumnStore, rows: usize) anyerror!void {
    _ = arg_types;
    _ = out_type;
    var w: BitmapOut = .{ .out = out };
    defer w.deinit(allocator);
    const sv = common.stringViewOf(args[0]);
    for (0..rows) |r| {
        if (!args[0].isValid(r)) {
            try out.appendNulls(allocator, 1);
            continue;
        }
        try w.appendForeign(allocator, sv.rowBytes(r));
    }
}

/// BASE64_TO_BITMAP(text): BITMAP_FROM_BINARY of the bytes padded base64
/// spells; NULL when it spells none.
fn fromBase64Kernel(allocator: Allocator, arg_types: []const Type, out_type: Type, args: []const ColumnView, out: *ColumnStore, rows: usize) anyerror!void {
    _ = arg_types;
    _ = out_type;
    var w: BitmapOut = .{ .out = out };
    defer w.deinit(allocator);
    var bytes: std.ArrayList(u8) = .empty;
    defer bytes.deinit(allocator);
    const dec = std.base64.standard.Decoder;
    const sv = common.stringViewOf(args[0]);
    for (0..rows) |r| {
        if (!args[0].isValid(r)) {
            try out.appendNulls(allocator, 1);
            continue;
        }
        const text = sv.rowBytes(r);
        const n = dec.calcSizeForSlice(text) catch {
            try out.appendNulls(allocator, 1);
            continue;
        };
        try bytes.resize(allocator, n);
        dec.decode(bytes.items, text) catch {
            try out.appendNulls(allocator, 1);
            continue;
        };
        try w.appendForeign(allocator, bytes.items);
    }
}

/// ARRAY_TO_BITMAP(a): the set of a's elements, skipping those that are
/// no member, as BITMAP_AGG skips them.
fn fromArrayKernel(allocator: Allocator, arg_types: []const Type, out_type: Type, args: []const ColumnView, out: *ColumnStore, rows: usize) anyerror!void {
    _ = out_type;
    var flat = try nested.flatten(allocator, arg_types[0].arrayElement().?, args[0], rows);
    defer flat.deinit(allocator);
    const elements = flat.elements.view();
    var w: BitmapOut = .{ .out = out };
    defer w.deinit(allocator);
    var values: std.ArrayList(u64) = .empty;
    defer values.deinit(allocator);
    for (0..rows) |r| {
        if (!args[0].isValid(r)) {
            try out.appendNulls(allocator, 1);
            continue;
        }
        values.clearRetainingCapacity();
        for (flat.offsets.items[r]..flat.offsets.items[r + 1]) |i| {
            if (memberAt(elements, i)) |v| try values.append(allocator, v);
        }
        var set = try Bitmap.fromValues(allocator, values.items);
        defer set.deinit(allocator);
        try w.append(allocator, &set);
    }
}

/// BITMAP_TO_STRING(bm): the members ascending, comma-separated; '' for
/// the empty set.
fn toStringKernel(allocator: Allocator, arg_types: []const Type, out_type: Type, args: []const ColumnView, out: *ColumnStore, rows: usize) anyerror!void {
    _ = arg_types;
    _ = out_type;
    var text: std.ArrayList(u8) = .empty;
    defer text.deinit(allocator);
    for (0..rows) |r| {
        var bm = try bitmapAt(allocator, args[0], r) orelse {
            try out.appendNulls(allocator, 1);
            continue;
        };
        defer bm.deinit(allocator);
        text.clearRetainingCapacity();
        try bm.appendText(allocator, &text);
        try appendText(allocator, out, text.items);
    }
}

/// BITMAP_COUNT(bm): the number of members; 0 for NULL, as in StarRocks.
fn countKernel(allocator: Allocator, arg_types: []const Type, out_type: Type, args: []const ColumnView, out: *ColumnStore, rows: usize) anyerror!void {
    _ = arg_types;
    _ = out_type;
    for (0..rows) |r| {
        const n: u64 = if (args[0].isValid(r))
            bitmap.cardinalityOf(args[0].data.bitmap.rowBytes(r)) catch return Error.TypeMismatch
        else
            0;
        try array.appendValue(allocator, out, "bigint", std.math.cast(i64, n) orelse return Error.ValueOutOfRange);
    }
}

/// BITMAP_MIN(bm), BITMAP_MAX(bm): the least or greatest member; NULL for
/// the empty set.
fn extremeKernel(comptime which: enum { min, max }) TypedKernel {
    return struct {
        fn kernel(allocator: Allocator, arg_types: []const Type, out_type: Type, args: []const ColumnView, out: *ColumnStore, rows: usize) anyerror!void {
            _ = arg_types;
            _ = out_type;
            for (0..rows) |r| {
                var bm = try bitmapAt(allocator, args[0], r) orelse {
                    try out.appendNulls(allocator, 1);
                    continue;
                };
                defer bm.deinit(allocator);
                const v = (if (which == .min) bm.min() else bm.max()) orelse {
                    try out.appendNulls(allocator, 1);
                    continue;
                };
                try array.appendValue(allocator, out, "largeint", @as(i128, v));
            }
        }
    }.kernel;
}

/// BITMAP_TO_ARRAY(bm): the members ascending, as BIGINTs. A member past
/// BIGINT's range is an error rather than a wrapped negative.
fn toArrayKernel(allocator: Allocator, arg_types: []const Type, out_type: Type, args: []const ColumnView, out: *ColumnStore, rows: usize) anyerror!void {
    _ = arg_types;
    _ = out_type;
    var elements = try ColumnStore.init(allocator, .bigint, true);
    defer elements.deinit(allocator);
    var offsets: std.ArrayList(u32) = .empty;
    defer offsets.deinit(allocator);
    var validity: std.ArrayList(u8) = .empty;
    defer validity.deinit(allocator);
    try offsets.append(allocator, 0);
    try validity.appendNTimes(allocator, 0, (rows + 7) / 8);
    for (0..rows) |r| {
        if (try bitmapAt(allocator, args[0], r)) |found| {
            var bm = found;
            defer bm.deinit(allocator);
            validity.items[r >> 3] |= @as(u8, 1) << @intCast(r & 7);
            var it = bm.iterator();
            while (it.next()) |v| try array.appendValue(allocator, &elements, "bigint", std.math.cast(i64, v) orelse return Error.ValueOutOfRange);
        }
        try offsets.append(allocator, @intCast(elements.rowCount()));
    }
    try nested.encodeColumn(allocator, out, nested.ElemClass.of(.bigint), elements.view(), offsets.items, validity.items, rows);
}

/// BITMAP_TO_BINARY(bm): StarRocks' bytes for the bitmap.
fn toBinaryKernel(allocator: Allocator, arg_types: []const Type, out_type: Type, args: []const ColumnView, out: *ColumnStore, rows: usize) anyerror!void {
    _ = arg_types;
    _ = out_type;
    for (0..rows) |r| {
        if (!args[0].isValid(r)) {
            try out.appendNulls(allocator, 1);
            continue;
        }
        try appendText(allocator, out, args[0].data.bitmap.rowBytes(r));
    }
}

/// BITMAP_TO_BASE64(bm): BITMAP_TO_BINARY's bytes in padded base64.
fn toBase64Kernel(allocator: Allocator, arg_types: []const Type, out_type: Type, args: []const ColumnView, out: *ColumnStore, rows: usize) anyerror!void {
    _ = arg_types;
    _ = out_type;
    const enc = std.base64.standard.Encoder;
    var text: std.ArrayList(u8) = .empty;
    defer text.deinit(allocator);
    for (0..rows) |r| {
        if (!args[0].isValid(r)) {
            try out.appendNulls(allocator, 1);
            continue;
        }
        const bytes = args[0].data.bitmap.rowBytes(r);
        try text.resize(allocator, enc.calcSize(bytes.len));
        try appendText(allocator, out, enc.encode(text.items, bytes));
    }
}

/// BITMAP_OR, BITMAP_AND, BITMAP_XOR, BITMAP_ANDNOT of two bitmaps.
fn combineKernel(comptime op: bitmap.Op) TypedKernel {
    return struct {
        fn kernel(allocator: Allocator, arg_types: []const Type, out_type: Type, args: []const ColumnView, out: *ColumnStore, rows: usize) anyerror!void {
            _ = arg_types;
            _ = out_type;
            var w: BitmapOut = .{ .out = out };
            defer w.deinit(allocator);
            for (0..rows) |r| {
                if (!args[0].isValid(r) or !args[1].isValid(r)) {
                    try out.appendNulls(allocator, 1);
                    continue;
                }
                var a = (try bitmapAt(allocator, args[0], r)).?;
                defer a.deinit(allocator);
                var b = (try bitmapAt(allocator, args[1], r)).?;
                defer b.deinit(allocator);
                var result = try Bitmap.combine(allocator, op, &a, &b);
                defer result.deinit(allocator);
                try w.append(allocator, &result);
            }
        }
    }.kernel;
}

/// BITMAP_HAS_ANY(a, b): whether the two share a member.
fn hasAnyKernel(allocator: Allocator, arg_types: []const Type, out_type: Type, args: []const ColumnView, out: *ColumnStore, rows: usize) anyerror!void {
    _ = arg_types;
    _ = out_type;
    for (0..rows) |r| {
        if (!args[0].isValid(r) or !args[1].isValid(r)) {
            try out.appendNulls(allocator, 1);
            continue;
        }
        var a = (try bitmapAt(allocator, args[0], r)).?;
        defer a.deinit(allocator);
        var b = (try bitmapAt(allocator, args[1], r)).?;
        defer b.deinit(allocator);
        try array.appendValue(allocator, out, "boolean", @intFromBool(a.intersects(&b)));
    }
}

/// BITMAP_CONTAINS(bm, x): whether x is a member.
fn containsKernel(allocator: Allocator, arg_types: []const Type, out_type: Type, args: []const ColumnView, out: *ColumnStore, rows: usize) anyerror!void {
    _ = arg_types;
    _ = out_type;
    for (0..rows) |r| {
        const x = intAt(args[1], r) orelse {
            try out.appendNulls(allocator, 1);
            continue;
        };
        var bm = try bitmapAt(allocator, args[0], r) orelse {
            try out.appendNulls(allocator, 1);
            continue;
        };
        defer bm.deinit(allocator);
        const holds = if (std.math.cast(u64, x)) |v| bm.contains(v) else false;
        try array.appendValue(allocator, out, "boolean", @intFromBool(holds));
    }
}

/// BITMAP_REMOVE(bm, x): the bitmap without x.
fn removeKernel(allocator: Allocator, arg_types: []const Type, out_type: Type, args: []const ColumnView, out: *ColumnStore, rows: usize) anyerror!void {
    _ = arg_types;
    _ = out_type;
    var w: BitmapOut = .{ .out = out };
    defer w.deinit(allocator);
    for (0..rows) |r| {
        const x = intAt(args[1], r) orelse {
            try out.appendNulls(allocator, 1);
            continue;
        };
        var bm = try bitmapAt(allocator, args[0], r) orelse {
            try out.appendNulls(allocator, 1);
            continue;
        };
        defer bm.deinit(allocator);
        if (std.math.cast(u64, x)) |v| try bm.remove(allocator, v);
        try w.append(allocator, &bm);
    }
}

const Cut = enum { in_range, limit, ranks };

/// BITMAP_SUBSET_IN_RANGE(bm, start, end), BITMAP_SUBSET_LIMIT(bm, start,
/// limit) and SUB_BITMAP(bm, offset, len): the members `rankWindow` keeps;
/// NULL when that is none, as in StarRocks.
fn subsetKernel(comptime cut: Cut) TypedKernel {
    return struct {
        fn kernel(allocator: Allocator, arg_types: []const Type, out_type: Type, args: []const ColumnView, out: *ColumnStore, rows: usize) anyerror!void {
            _ = arg_types;
            _ = out_type;
            var w: BitmapOut = .{ .out = out };
            defer w.deinit(allocator);
            for (0..rows) |r| {
                const a = intAt(args[1], r);
                const b = intAt(args[2], r);
                if (a == null or b == null or !args[0].isValid(r)) {
                    try out.appendNulls(allocator, 1);
                    continue;
                }
                var bm = (try bitmapAt(allocator, args[0], r)).?;
                defer bm.deinit(allocator);
                const window = rankWindow(cut, &bm, a.?, b.?) orelse {
                    try out.appendNulls(allocator, 1);
                    continue;
                };
                var part = try bm.rankRange(allocator, window.from, window.to);
                defer part.deinit(allocator);
                if (part.cardinality() == 0) {
                    try out.appendNulls(allocator, 1);
                    continue;
                }
                try w.append(allocator, &part);
            }
        }
    }.kernel;
}

const RankWindow = struct { from: u64, to: u64 };

/// The 0-based ranks [from, to) of the members a subset function keeps:
/// those in [start, end) for `.in_range`; the first `limit` from `start`
/// up, or for a negative limit the last ones up to `start`, for `.limit`
/// (a negative start reads as 0 for both); `len` members from rank
/// `offset`, counted from the end when negative, for `.ranks`.
fn rankWindow(cut: Cut, bm: *const Bitmap, a: i128, b: i128) ?RankWindow {
    switch (cut) {
        .in_range => {
            const start = @max(a, 0);
            if (b <= start) return null;
            return .{ .from = countBelow(bm, start), .to = countBelow(bm, b) };
        },
        .limit => {
            const start = @max(a, 0);
            if (b >= 0) {
                const from = countBelow(bm, start);
                return .{ .from = from, .to = from +| saturatedU64(b) };
            }
            const to = countBelow(bm, start +| 1);
            return .{ .from = to -| saturatedU64(@abs(b)), .to = to };
        },
        .ranks => {
            if (b <= 0) return null;
            const offset = if (a < 0) a + bm.cardinality() else a;
            if (offset < 0) return null;
            const from = saturatedU64(offset);
            return .{ .from = from, .to = from +| saturatedU64(b) };
        },
    }
}

/// The number of members below `v`.
fn countBelow(bm: *const Bitmap, v: i128) u64 {
    if (v <= 0) return 0;
    const at = std.math.cast(u64, v - 1) orelse return bm.cardinality();
    return bm.rank(at);
}

fn saturatedU64(v: anytype) u64 {
    return std.math.cast(u64, v) orelse if (v < 0) 0 else std.math.maxInt(u64);
}
