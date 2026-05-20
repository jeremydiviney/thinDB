//! Per-type encode/decode helpers for the WAL byte format. Split out of
//! wal.zig so the WalWriter / replay loop stay readable. Everything here
//! is pure functions over byte buffers — no I/O.
//!
//! Used by wal.zig:
//!   - WalWriter.appendInsert  -> encodeColumnRange + writePacked helpers
//!   - WalWriter.appendDelete  -> encodeValue
//!   - replay() apply step     -> applyInsertRecord, applyDeleteRecord

const std = @import("std");
const Allocator = std.mem.Allocator;

const types = @import("../types.zig");
const Type = types.Type;
const TypeTag = types.TypeTag;
const ValueTag = types.ValueTag;
const Value = types.Value;

const storage = @import("../storage/storage.zig");
const format = storage.format;
const ColumnView = storage.ColumnView;
const column_mod = storage.column;

const store = @import("store.zig");
const ColumnStore = store.ColumnStore;

const memtable_mod = @import("memtable.zig");
const Memtable = memtable_mod.Memtable;

const wal = @import("wal.zig");
const Error = wal.Error;

pub fn applyInsertRecord(allocator: Allocator, payload: []const u8, mt: *Memtable) !void {
    if (payload.len < 4) return Error.WalCorrupt;
    const row_count = format.readU32(payload[0..4]);
    var cursor: usize = 4;
    for (mt.columns, mt.schema.columns) |*col, schema_col| {
        cursor = try decodeColumnRange(allocator, payload, cursor, row_count, col, schema_col);
    }
    mt.row_count += row_count;
}

pub fn applyDeleteRecord(allocator: Allocator, payload: []const u8, mt: *Memtable) !void {
    _ = allocator;
    if (payload.len < 4) return Error.WalCorrupt;
    var cursor: usize = 0;
    const col_name_len = format.readU32(payload[cursor .. cursor + 4]);
    cursor += 4;
    if (cursor + col_name_len > payload.len) return Error.WalCorrupt;
    const col_name = payload[cursor .. cursor + col_name_len];
    cursor += col_name_len;

    if (cursor + 1 + 1 > payload.len) return Error.WalCorrupt;
    const op_byte = payload[cursor];
    cursor += 1;
    const value_tag_byte = payload[cursor];
    cursor += 1;

    const col_idx = mt.schema.columnIndex(col_name) orelse return Error.WalCorrupt;
    const value = try decodeValue(@enumFromInt(value_tag_byte), payload, &cursor);

    // Apply: rebuild memtable keeping rows that DON'T match.
    const n: usize = @intCast(mt.row_count);
    if (n == 0) return;
    const keep = try mt.allocator.alloc(bool, n);
    defer mt.allocator.free(keep);
    const view = mt.columns[col_idx].view();
    for (0..n) |i| keep[i] = !predicateMatches(view, @intCast(i), op_byte, value);
    _ = try mt.retainRows(keep);
}

fn predicateMatches(view: ColumnView, row: u32, op_byte: u8, val: Value) bool {
    // Reuse the api.comparison kernel by inlining it here to avoid the
    // engine→api cycle.
    return switch (view.data) {
        .int => |s| cmpI(i32, s[row], val.int, op_byte),
        .bigint => |s| cmpI(i64, s[row], val.bigint, op_byte),
        .boolean => |s| cmpI(u8, s[row], @intFromBool(val.boolean), op_byte),
        .float => |s| cmpF(f32, s[row], val.float, op_byte),
        .double => |s| cmpF(f64, s[row], val.double, op_byte),
        .date => |s| cmpI(i32, s[row], val.date, op_byte),
        .datetime => |s| cmpI(i64, s[row], val.datetime, op_byte),
        .tinyint => |s| cmpI(i8, s[row], val.tinyint, op_byte),
        .smallint => |s| cmpI(i16, s[row], val.smallint, op_byte),
        .largeint => |s| cmpI(i128, s[row], val.largeint, op_byte),
        .decimal64 => |s| cmpI(i64, s[row], val.decimal64, op_byte),
        .decimal128 => |s| cmpI(i128, s[row], val.decimal128, op_byte),
        .uuid => |s| cmpI(u128, s[row], val.uuid, op_byte),
        .varchar => |sv| cmpStr(sv.rowBytes(row), val.text, op_byte),
        .string => |sv| cmpStr(sv.rowBytes(row), val.text, op_byte),
        .char => |sv| cmpStr(sv.rowBytes(row), val.text, op_byte),
    };
}

fn cmpI(comptime T: type, a: T, b: T, op: u8) bool {
    return switch (op) {
        0 => a == b, // eq
        1 => a != b, // neq
        2 => a < b, // lt
        3 => a <= b, // lte
        4 => a > b, // gt
        5 => a >= b, // gte
        else => false,
    };
}
fn cmpF(comptime T: type, a: T, b: T, op: u8) bool {
    return switch (op) {
        0 => a == b,
        1 => a != b,
        2 => a < b,
        3 => a <= b,
        4 => a > b,
        5 => a >= b,
        else => false,
    };
}
fn cmpStr(a: []const u8, b: []const u8, op: u8) bool {
    const eq = std.mem.eql(u8, a, b);
    return switch (op) {
        0 => eq,
        1 => !eq,
        else => false,
    };
}

// ---------------------------------------------------------------------------
// Per-column encode / decode for insert records
// ---------------------------------------------------------------------------

pub fn encodeColumnRange(
    allocator: Allocator,
    out: *std.ArrayList(u8),
    col: ColumnStore,
    schema_col: types.Column,
    from: usize,
    to: usize,
) !void {
    const n = to - from;
    // Optional null-bitmap prefix if column is nullable.
    if (schema_col.nullable) {
        const byte_len = column_mod.bitmapBytes(n);
        const start = out.items.len;
        try out.appendNTimes(allocator, 0, byte_len);
        const slice = out.items[start .. start + byte_len];
        if (col.nulls) |bits| {
            for (0..n) |i| {
                if (column_mod.isValidBit(bits.items, from + i)) {
                    column_mod.setValidBit(slice, i, true);
                }
            }
        } else {
            // All valid.
            for (0..n) |i| column_mod.setValidBit(slice, i, true);
        }
    }

    const view = col.view();
    switch (view.data) {
        .int => |s| try writePackedI32(allocator, out, s[from..to]),
        .bigint => |s| try writePackedI64(allocator, out, s[from..to]),
        .boolean => |s| try out.appendSlice(allocator, s[from..to]),
        .float => |s| try writePackedBytes(allocator, out, std.mem.sliceAsBytes(s[from..to])),
        .double => |s| try writePackedBytes(allocator, out, std.mem.sliceAsBytes(s[from..to])),
        .date => |s| try writePackedI32(allocator, out, s[from..to]),
        .datetime => |s| try writePackedI64(allocator, out, s[from..to]),
        .tinyint => |s| try out.appendSlice(allocator, std.mem.sliceAsBytes(s[from..to])),
        .smallint => |s| try writePackedI16(allocator, out, s[from..to]),
        .largeint => |s| try writePackedI128(allocator, out, s[from..to]),
        .decimal64 => |s| try writePackedI64(allocator, out, s[from..to]),
        .decimal128 => |s| try writePackedI128(allocator, out, s[from..to]),
        .uuid => |s| try writePackedBytes(allocator, out, std.mem.sliceAsBytes(s[from..to])),
        .varchar, .string, .char => |sv| try writePackedStrings(allocator, out, sv, from, to),
    }
}

fn decodeColumnRange(
    allocator: Allocator,
    payload: []const u8,
    cursor_in: usize,
    n: u32,
    col: *ColumnStore,
    schema_col: types.Column,
) !usize {
    var cursor = cursor_in;
    var nulls: ?[]const u8 = null;
    if (schema_col.nullable) {
        const byte_len = column_mod.bitmapBytes(n);
        if (cursor + byte_len > payload.len) return Error.WalCorrupt;
        nulls = payload[cursor .. cursor + byte_len];
        cursor += byte_len;
    }

    switch (schema_col.type) {
        .int, .date => {
            const want = @as(usize, n) * 4;
            if (cursor + want > payload.len) return Error.WalCorrupt;
            for (0..n) |i| {
                const v = format.readI32(payload[cursor + i * 4 ..][0..4]);
                try appendOne(allocator, col, schema_col, .{ .i32 = v }, nulls, i);
            }
            cursor += want;
        },
        .bigint, .datetime, .decimal64 => {
            const want = @as(usize, n) * 8;
            if (cursor + want > payload.len) return Error.WalCorrupt;
            for (0..n) |i| {
                const v = format.readI64(payload[cursor + i * 8 ..][0..8]);
                try appendOne(allocator, col, schema_col, .{ .i64 = v }, nulls, i);
            }
            cursor += want;
        },
        .boolean => {
            const want = @as(usize, n);
            if (cursor + want > payload.len) return Error.WalCorrupt;
            for (0..n) |i| {
                try appendOne(allocator, col, schema_col, .{ .u8 = payload[cursor + i] }, nulls, i);
            }
            cursor += want;
        },
        .tinyint => {
            const want = @as(usize, n);
            if (cursor + want > payload.len) return Error.WalCorrupt;
            for (0..n) |i| {
                try appendOne(allocator, col, schema_col, .{ .i8 = @bitCast(payload[cursor + i]) }, nulls, i);
            }
            cursor += want;
        },
        .smallint => {
            const want = @as(usize, n) * 2;
            if (cursor + want > payload.len) return Error.WalCorrupt;
            for (0..n) |i| {
                const v = std.mem.readInt(i16, payload[cursor + i * 2 ..][0..2], .little);
                try appendOne(allocator, col, schema_col, .{ .i16 = v }, nulls, i);
            }
            cursor += want;
        },
        .largeint, .decimal128 => {
            const want = @as(usize, n) * 16;
            if (cursor + want > payload.len) return Error.WalCorrupt;
            for (0..n) |i| {
                const v = std.mem.readInt(i128, payload[cursor + i * 16 ..][0..16], .little);
                try appendOne(allocator, col, schema_col, .{ .i128 = v }, nulls, i);
            }
            cursor += want;
        },
        .uuid => {
            const want = @as(usize, n) * 16;
            if (cursor + want > payload.len) return Error.WalCorrupt;
            for (0..n) |i| {
                const v = std.mem.readInt(u128, payload[cursor + i * 16 ..][0..16], .little);
                try appendOne(allocator, col, schema_col, .{ .u128 = v }, nulls, i);
            }
            cursor += want;
        },
        .float => {
            const want = @as(usize, n) * 4;
            if (cursor + want > payload.len) return Error.WalCorrupt;
            for (0..n) |i| {
                const v = format.readF32(payload[cursor + i * 4 ..][0..4]);
                try appendOne(allocator, col, schema_col, .{ .f32 = v }, nulls, i);
            }
            cursor += want;
        },
        .double => {
            const want = @as(usize, n) * 8;
            if (cursor + want > payload.len) return Error.WalCorrupt;
            for (0..n) |i| {
                const v = format.readF64(payload[cursor + i * 8 ..][0..8]);
                try appendOne(allocator, col, schema_col, .{ .f64 = v }, nulls, i);
            }
            cursor += want;
        },
        .varchar, .string, .char => {
            for (0..n) |i| {
                if (cursor + 4 > payload.len) return Error.WalCorrupt;
                const len = format.readU32(payload[cursor .. cursor + 4]);
                cursor += 4;
                if (cursor + len > payload.len) return Error.WalCorrupt;
                const bytes = payload[cursor .. cursor + len];
                cursor += len;
                try appendOne(allocator, col, schema_col, .{ .bytes = bytes }, nulls, i);
            }
        },
    }
    return cursor;
}

const TypedValue = union(enum) {
    i8: i8,
    i16: i16,
    i32: i32,
    i64: i64,
    i128: i128,
    u8: u8,
    u128: u128,
    f32: f32,
    f64: f64,
    bytes: []const u8,
};

fn appendOne(
    allocator: Allocator,
    col: *ColumnStore,
    schema_col: types.Column,
    v: TypedValue,
    nulls: ?[]const u8,
    i: usize,
) !void {
    const is_valid = if (nulls) |bm| column_mod.isValidBit(bm, i) else true;
    if (schema_col.nullable and !is_valid) {
        try col.data.appendNullPlaceholder(allocator);
        try col.appendValidBit(allocator, col.data.rowCount() - 1, false);
        return;
    }
    switch (col.data) {
        .int => |*l| try l.append(allocator, v.i32),
        .bigint => |*l| try l.append(allocator, v.i64),
        .boolean => |*l| try l.append(allocator, v.u8),
        .tinyint => |*l| try l.append(allocator, v.i8),
        .smallint => |*l| try l.append(allocator, v.i16),
        .largeint => |*l| try l.append(allocator, v.i128),
        .float => |*l| try l.append(allocator, v.f32),
        .double => |*l| try l.append(allocator, v.f64),
        .date => |*l| try l.append(allocator, v.i32),
        .datetime => |*l| try l.append(allocator, v.i64),
        .decimal64 => |*l| try l.append(allocator, v.i64),
        .decimal128 => |*l| try l.append(allocator, v.i128),
        .uuid => |*l| try l.append(allocator, v.u128),
        .varchar => |*ss| try ss.appendValue(allocator, v.bytes),
        .string => |*ss| try ss.appendValue(allocator, v.bytes),
        .char => |*ss| try ss.appendValue(allocator, v.bytes),
    }
    if (col.nulls != null) {
        try col.appendValidBit(allocator, col.data.rowCount() - 1, true);
    }
}

fn writePackedI16(allocator: Allocator, out: *std.ArrayList(u8), slice: []const i16) !void {
    try out.ensureUnusedCapacity(allocator, slice.len * 2);
    for (slice) |v| {
        var b: [2]u8 = undefined;
        std.mem.writeInt(i16, &b, v, .little);
        out.appendSliceAssumeCapacity(&b);
    }
}
fn writePackedI32(allocator: Allocator, out: *std.ArrayList(u8), slice: []const i32) !void {
    try out.ensureUnusedCapacity(allocator, slice.len * 4);
    for (slice) |v| {
        var b: [4]u8 = undefined;
        format.writeI32(&b, v);
        out.appendSliceAssumeCapacity(&b);
    }
}
fn writePackedI64(allocator: Allocator, out: *std.ArrayList(u8), slice: []const i64) !void {
    try out.ensureUnusedCapacity(allocator, slice.len * 8);
    for (slice) |v| {
        var b: [8]u8 = undefined;
        format.writeI64(&b, v);
        out.appendSliceAssumeCapacity(&b);
    }
}
fn writePackedI128(allocator: Allocator, out: *std.ArrayList(u8), slice: []const i128) !void {
    try out.ensureUnusedCapacity(allocator, slice.len * 16);
    for (slice) |v| {
        var b: [16]u8 = undefined;
        std.mem.writeInt(i128, &b, v, .little);
        out.appendSliceAssumeCapacity(&b);
    }
}
fn writePackedBytes(allocator: Allocator, out: *std.ArrayList(u8), bytes: []const u8) !void {
    try out.appendSlice(allocator, bytes);
}
fn writePackedStrings(
    allocator: Allocator,
    out: *std.ArrayList(u8),
    sv: storage.StringView,
    from: usize,
    to: usize,
) !void {
    for (from..to) |i| {
        const bytes = sv.rowBytes(i);
        var b4: [4]u8 = undefined;
        format.writeU32(&b4, @intCast(bytes.len));
        try out.appendSlice(allocator, &b4);
        try out.appendSlice(allocator, bytes);
    }
}

pub fn encodeValue(allocator: Allocator, out: *std.ArrayList(u8), v: Value) !void {
    var b: [16]u8 = undefined;
    switch (v) {
        .int => |x| {
            format.writeI32(b[0..4], x);
            try out.appendSlice(allocator, b[0..4]);
        },
        .bigint => |x| {
            format.writeI64(b[0..8], x);
            try out.appendSlice(allocator, b[0..8]);
        },
        .boolean => |x| try out.append(allocator, @intFromBool(x)),
        .float => |x| {
            format.writeF32(b[0..4], x);
            try out.appendSlice(allocator, b[0..4]);
        },
        .double => |x| {
            format.writeF64(b[0..8], x);
            try out.appendSlice(allocator, b[0..8]);
        },
        .date => |x| {
            format.writeI32(b[0..4], x);
            try out.appendSlice(allocator, b[0..4]);
        },
        .datetime => |x| {
            format.writeI64(b[0..8], x);
            try out.appendSlice(allocator, b[0..8]);
        },
        .tinyint => |x| try out.append(allocator, @bitCast(x)),
        .smallint => |x| {
            std.mem.writeInt(i16, b[0..2], x, .little);
            try out.appendSlice(allocator, b[0..2]);
        },
        .largeint => |x| {
            std.mem.writeInt(i128, b[0..16], x, .little);
            try out.appendSlice(allocator, &b);
        },
        .decimal64 => |x| {
            format.writeI64(b[0..8], x);
            try out.appendSlice(allocator, b[0..8]);
        },
        .decimal128 => |x| {
            std.mem.writeInt(i128, b[0..16], x, .little);
            try out.appendSlice(allocator, &b);
        },
        .uuid => |x| {
            std.mem.writeInt(u128, b[0..16], x, .little);
            try out.appendSlice(allocator, &b);
        },
        .text => |s| {
            format.writeU32(b[0..4], @intCast(s.len));
            try out.appendSlice(allocator, b[0..4]);
            try out.appendSlice(allocator, s);
        },
    }
}

pub fn decodeValuePub(tag: ValueTag, payload: []const u8, cursor: *usize) !Value {
    return decodeValue(tag, payload, cursor);
}

fn decodeValue(tag: ValueTag, payload: []const u8, cursor: *usize) !Value {
    const c = cursor.*;
    return switch (tag) {
        .int => blk: {
            if (c + 4 > payload.len) return Error.WalCorrupt;
            const v = format.readI32(payload[c..][0..4]);
            cursor.* = c + 4;
            break :blk Value{ .int = v };
        },
        .bigint => blk: {
            if (c + 8 > payload.len) return Error.WalCorrupt;
            const v = format.readI64(payload[c..][0..8]);
            cursor.* = c + 8;
            break :blk Value{ .bigint = v };
        },
        .boolean => blk: {
            if (c + 1 > payload.len) return Error.WalCorrupt;
            cursor.* = c + 1;
            break :blk Value{ .boolean = payload[c] != 0 };
        },
        .float => blk: {
            if (c + 4 > payload.len) return Error.WalCorrupt;
            const v = format.readF32(payload[c..][0..4]);
            cursor.* = c + 4;
            break :blk Value{ .float = v };
        },
        .double => blk: {
            if (c + 8 > payload.len) return Error.WalCorrupt;
            const v = format.readF64(payload[c..][0..8]);
            cursor.* = c + 8;
            break :blk Value{ .double = v };
        },
        .date => blk: {
            if (c + 4 > payload.len) return Error.WalCorrupt;
            const v = format.readI32(payload[c..][0..4]);
            cursor.* = c + 4;
            break :blk Value{ .date = v };
        },
        .datetime => blk: {
            if (c + 8 > payload.len) return Error.WalCorrupt;
            const v = format.readI64(payload[c..][0..8]);
            cursor.* = c + 8;
            break :blk Value{ .datetime = v };
        },
        .tinyint => blk: {
            if (c + 1 > payload.len) return Error.WalCorrupt;
            const v: i8 = @bitCast(payload[c]);
            cursor.* = c + 1;
            break :blk Value{ .tinyint = v };
        },
        .smallint => blk: {
            if (c + 2 > payload.len) return Error.WalCorrupt;
            const v = std.mem.readInt(i16, payload[c..][0..2], .little);
            cursor.* = c + 2;
            break :blk Value{ .smallint = v };
        },
        .largeint => blk: {
            if (c + 16 > payload.len) return Error.WalCorrupt;
            const v = std.mem.readInt(i128, payload[c..][0..16], .little);
            cursor.* = c + 16;
            break :blk Value{ .largeint = v };
        },
        .decimal64 => blk: {
            if (c + 8 > payload.len) return Error.WalCorrupt;
            const v = format.readI64(payload[c..][0..8]);
            cursor.* = c + 8;
            break :blk Value{ .decimal64 = v };
        },
        .decimal128 => blk: {
            if (c + 16 > payload.len) return Error.WalCorrupt;
            const v = std.mem.readInt(i128, payload[c..][0..16], .little);
            cursor.* = c + 16;
            break :blk Value{ .decimal128 = v };
        },
        .uuid => blk: {
            if (c + 16 > payload.len) return Error.WalCorrupt;
            const v = std.mem.readInt(u128, payload[c..][0..16], .little);
            cursor.* = c + 16;
            break :blk Value{ .uuid = v };
        },
        .text => blk: {
            if (c + 4 > payload.len) return Error.WalCorrupt;
            const len = format.readU32(payload[c..][0..4]);
            if (c + 4 + len > payload.len) return Error.WalCorrupt;
            cursor.* = c + 4 + len;
            break :blk Value{ .text = payload[c + 4 .. c + 4 + len] };
        },
    };
}

// ---------------------------------------------------------------------------
// Rich-predicate (PredicateExpr) encode / decode for delete_expr WAL records.
//
// Mirrors the IR's `encodePredicate` tag numbering so the byte layout is
// stable across the codebase; the engine layer can't import IR (would
// cycle), so we duplicate the small encoder/decoder here.
//
// Tag (u8):
//   0  = leaf            [op u8][col_len u32][col bytes][value_tag u8][value bytes]
//   1  = is_null         [col_len u32][col bytes]
//   2  = is_not_null     [col_len u32][col bytes]
//   3  = and             [n u32][child0][child1]...
//   4  = or              [n u32][child0][child1]...
//   5  = not             [child]
//   6  = like            [col_len u32][col bytes][pat_len u32][pat bytes]
//   7  = leaf_col_col    [op u8][left_len u32][left bytes][right_len u32][right bytes]
//   8  = always          [b u8]   (0 = false, non-zero = true)
//   9  = in_set          [col_len u32][col bytes][negate u8][value_tag u8][n_values u32][values...]
//
// Unresolved or runtime-only variants (scalar_subquery / exists_subquery /
// in_subquery / correlated_* / leaf_var / var_ref) error out with
// WalPredicateUnsupported — caller can choose to skip WAL logging and
// proceed with the mutation.
// ---------------------------------------------------------------------------

const PredTagWal = enum(u8) {
    leaf = 0,
    is_null = 1,
    is_not_null = 2,
    p_and = 3,
    p_or = 4,
    p_not = 5,
    like = 6,
    leaf_col_col = 7,
    always = 8,
    in_set = 9,
};

pub fn encodePredicateExpr(allocator: Allocator, out: *std.ArrayList(u8), expr: anytype) !void {
    var b4: [4]u8 = undefined;
    switch (expr) {
        .leaf => |lf| {
            try out.append(allocator, @intFromEnum(PredTagWal.leaf));
            try out.append(allocator, @intFromEnum(lf.op));
            format.writeU32(&b4, @intCast(lf.col.len));
            try out.appendSlice(allocator, &b4);
            try out.appendSlice(allocator, lf.col);
            try out.append(allocator, @intFromEnum(@as(ValueTag, lf.val)));
            try encodeValue(allocator, out, lf.val);
        },
        .leaf_col_col => |lc| {
            try out.append(allocator, @intFromEnum(PredTagWal.leaf_col_col));
            try out.append(allocator, @intFromEnum(lc.op));
            format.writeU32(&b4, @intCast(lc.left.len));
            try out.appendSlice(allocator, &b4);
            try out.appendSlice(allocator, lc.left);
            format.writeU32(&b4, @intCast(lc.right.len));
            try out.appendSlice(allocator, &b4);
            try out.appendSlice(allocator, lc.right);
        },
        .is_null => |col| {
            try out.append(allocator, @intFromEnum(PredTagWal.is_null));
            format.writeU32(&b4, @intCast(col.len));
            try out.appendSlice(allocator, &b4);
            try out.appendSlice(allocator, col);
        },
        .is_not_null => |col| {
            try out.append(allocator, @intFromEnum(PredTagWal.is_not_null));
            format.writeU32(&b4, @intCast(col.len));
            try out.appendSlice(allocator, &b4);
            try out.appendSlice(allocator, col);
        },
        .like => |lp| {
            try out.append(allocator, @intFromEnum(PredTagWal.like));
            format.writeU32(&b4, @intCast(lp.col.len));
            try out.appendSlice(allocator, &b4);
            try out.appendSlice(allocator, lp.col);
            format.writeU32(&b4, @intCast(lp.pattern.len));
            try out.appendSlice(allocator, &b4);
            try out.appendSlice(allocator, lp.pattern);
        },
        .@"and" => |kids| {
            try out.append(allocator, @intFromEnum(PredTagWal.p_and));
            format.writeU32(&b4, @intCast(kids.len));
            try out.appendSlice(allocator, &b4);
            for (kids) |k| try encodePredicateExpr(allocator, out, k);
        },
        .@"or" => |kids| {
            try out.append(allocator, @intFromEnum(PredTagWal.p_or));
            format.writeU32(&b4, @intCast(kids.len));
            try out.appendSlice(allocator, &b4);
            for (kids) |k| try encodePredicateExpr(allocator, out, k);
        },
        .not => |child| {
            try out.append(allocator, @intFromEnum(PredTagWal.p_not));
            try encodePredicateExpr(allocator, out, child.*);
        },
        .always => |b| {
            try out.append(allocator, @intFromEnum(PredTagWal.always));
            try out.append(allocator, if (b) @as(u8, 1) else 0);
        },
        .in_set => |s| {
            try out.append(allocator, @intFromEnum(PredTagWal.in_set));
            format.writeU32(&b4, @intCast(s.col.len));
            try out.appendSlice(allocator, &b4);
            try out.appendSlice(allocator, s.col);
            try out.append(allocator, if (s.negate) @as(u8, 1) else 0);
            // All set values share the same type tag — use [0]'s.
            // The set is guaranteed non-empty at this point (parser
            // requires at least one value).
            if (s.values.len == 0) return Error.WalPredicateUnsupported;
            const tag: ValueTag = std.meta.activeTag(s.values[0]);
            try out.append(allocator, @intFromEnum(tag));
            format.writeU32(&b4, @intCast(s.values.len));
            try out.appendSlice(allocator, &b4);
            for (s.values) |v| try encodeValue(allocator, out, v);
        },
        // Resolving / runtime forms can't be WAL-logged. Skip.
        .scalar_subquery,
        .exists_subquery,
        .in_subquery,
        .correlated_set,
        .correlated_scalar,
        .correlated_range,
        .leaf_var,
        => return Error.WalPredicateUnsupported,
    }
}

/// Apply a `delete_expr` WAL record to the recovered memtable. Decodes
/// the predicate then runs `evaluatePredicateOnMemtable` to drop the
/// matching rows. Segments are durable independently via per-segment
/// tombstone files — replay only fixes up the memtable side.
pub fn applyDeleteExprRecord(allocator: Allocator, payload: []const u8, mt: *Memtable) !void {
    _ = allocator;
    var cursor: usize = 0;
    const expr = try decodeOwnedPredicate(payload, &cursor, mt.allocator);
    defer freeOwnedPredicate(mt.allocator, expr);

    const n: usize = @intCast(mt.row_count);
    if (n == 0) return;
    const keep = try mt.allocator.alloc(bool, n);
    defer mt.allocator.free(keep);
    for (0..n) |i| keep[i] = !evaluatePredicateOnRow(mt, @intCast(i), expr);
    _ = try mt.retainRows(keep);
}

/// Memtable-replay-only predicate node. Owned by the caller's
/// allocator — strings + child arrays are dup'd / heap-alloc'd.
/// Distinct from `exec.predicate.PredicateExpr` because engine
/// can't import exec.
const OwnedPredicate = union(PredTagWal) {
    leaf: OwnedLeaf,
    is_null: []u8,
    is_not_null: []u8,
    p_and: []OwnedPredicate,
    p_or: []OwnedPredicate,
    p_not: *OwnedPredicate,
    like: OwnedLike,
    leaf_col_col: OwnedColCol,
    always: bool,
    in_set: OwnedInSet,
};

const OwnedLeaf = struct {
    col: []u8,
    op: u8,
    val: Value, // .text values borrow into the payload — see decoder note
};

const OwnedLike = struct {
    col: []u8,
    pattern: []u8,
};

const OwnedColCol = struct {
    left: []u8,
    op: u8,
    right: []u8,
};

const OwnedInSet = struct {
    col: []u8,
    negate: bool,
    values: []Value,
};

fn decodeOwnedPredicate(payload: []const u8, cursor: *usize, alloc: Allocator) !OwnedPredicate {
    if (cursor.* + 1 > payload.len) return Error.WalCorrupt;
    const tag_byte = payload[cursor.*];
    cursor.* += 1;
    if (tag_byte > @intFromEnum(PredTagWal.in_set)) return Error.WalUnknownRecord;
    const tag: PredTagWal = @enumFromInt(tag_byte);
    return switch (tag) {
        .leaf => blk: {
            if (cursor.* + 1 > payload.len) return Error.WalCorrupt;
            const op_byte = payload[cursor.*];
            cursor.* += 1;
            const col = try readOwnedBytes(payload, cursor, alloc);
            if (cursor.* + 1 > payload.len) return Error.WalCorrupt;
            const val_tag_byte = payload[cursor.*];
            cursor.* += 1;
            const val = try decodeValue(@enumFromInt(val_tag_byte), payload, cursor);
            break :blk OwnedPredicate{ .leaf = .{ .col = col, .op = op_byte, .val = val } };
        },
        .leaf_col_col => blk: {
            if (cursor.* + 1 > payload.len) return Error.WalCorrupt;
            const op_byte = payload[cursor.*];
            cursor.* += 1;
            const left = try readOwnedBytes(payload, cursor, alloc);
            const right = try readOwnedBytes(payload, cursor, alloc);
            break :blk OwnedPredicate{ .leaf_col_col = .{ .left = left, .op = op_byte, .right = right } };
        },
        .is_null => OwnedPredicate{ .is_null = try readOwnedBytes(payload, cursor, alloc) },
        .is_not_null => OwnedPredicate{ .is_not_null = try readOwnedBytes(payload, cursor, alloc) },
        .like => blk: {
            const col = try readOwnedBytes(payload, cursor, alloc);
            const pat = try readOwnedBytes(payload, cursor, alloc);
            break :blk OwnedPredicate{ .like = .{ .col = col, .pattern = pat } };
        },
        .p_and, .p_or => blk: {
            if (cursor.* + 4 > payload.len) return Error.WalCorrupt;
            const n = format.readU32(payload[cursor.*..][0..4]);
            cursor.* += 4;
            const kids = try alloc.alloc(OwnedPredicate, n);
            for (kids) |*k| k.* = try decodeOwnedPredicate(payload, cursor, alloc);
            break :blk if (tag == .p_and) OwnedPredicate{ .p_and = kids } else OwnedPredicate{ .p_or = kids };
        },
        .p_not => blk: {
            const child = try alloc.create(OwnedPredicate);
            child.* = try decodeOwnedPredicate(payload, cursor, alloc);
            break :blk OwnedPredicate{ .p_not = child };
        },
        .always => blk: {
            if (cursor.* + 1 > payload.len) return Error.WalCorrupt;
            const b = payload[cursor.*] != 0;
            cursor.* += 1;
            break :blk OwnedPredicate{ .always = b };
        },
        .in_set => blk: {
            const col = try readOwnedBytes(payload, cursor, alloc);
            if (cursor.* + 1 + 1 + 4 > payload.len) return Error.WalCorrupt;
            const negate = payload[cursor.*] != 0;
            cursor.* += 1;
            const val_tag_byte = payload[cursor.*];
            cursor.* += 1;
            const n = format.readU32(payload[cursor.*..][0..4]);
            cursor.* += 4;
            const vals = try alloc.alloc(Value, n);
            for (vals) |*v| v.* = try decodeValue(@enumFromInt(val_tag_byte), payload, cursor);
            break :blk OwnedPredicate{ .in_set = .{ .col = col, .negate = negate, .values = vals } };
        },
    };
}

fn freeOwnedPredicate(alloc: Allocator, p: OwnedPredicate) void {
    switch (p) {
        .leaf => |lf| alloc.free(lf.col),
        .leaf_col_col => |lc| {
            alloc.free(lc.left);
            alloc.free(lc.right);
        },
        .is_null, .is_not_null => |col| alloc.free(col),
        .like => |lp| {
            alloc.free(lp.col);
            alloc.free(lp.pattern);
        },
        .p_and, .p_or => |kids| {
            for (kids) |k| freeOwnedPredicate(alloc, k);
            alloc.free(kids);
        },
        .p_not => |child| {
            freeOwnedPredicate(alloc, child.*);
            alloc.destroy(child);
        },
        .always => {},
        .in_set => |s| {
            alloc.free(s.col);
            alloc.free(s.values);
        },
    }
}

fn readOwnedBytes(payload: []const u8, cursor: *usize, alloc: Allocator) ![]u8 {
    if (cursor.* + 4 > payload.len) return Error.WalCorrupt;
    const len = format.readU32(payload[cursor.*..][0..4]);
    cursor.* += 4;
    if (cursor.* + len > payload.len) return Error.WalCorrupt;
    const dup = try alloc.alloc(u8, len);
    @memcpy(dup, payload[cursor.* .. cursor.* + len]);
    cursor.* += len;
    return dup;
}

fn evaluatePredicateOnRow(mt: *Memtable, row: u32, p: OwnedPredicate) bool {
    return switch (p) {
        .leaf => |lf| blk: {
            const idx = mt.schema.columnIndex(lf.col) orelse break :blk false;
            const view = mt.columns[idx].view();
            if (!view.isValid(row)) break :blk false;
            break :blk predicateMatches(view, row, lf.op, lf.val);
        },
        .leaf_col_col => |lc| blk: {
            const li = mt.schema.columnIndex(lc.left) orelse break :blk false;
            const ri = mt.schema.columnIndex(lc.right) orelse break :blk false;
            const lv = mt.columns[li].view();
            const rv = mt.columns[ri].view();
            if (!lv.isValid(row) or !rv.isValid(row)) break :blk false;
            break :blk colColMatches(lv, rv, row, lc.op);
        },
        .is_null => |col| blk: {
            const idx = mt.schema.columnIndex(col) orelse break :blk false;
            break :blk !mt.columns[idx].view().isValid(row);
        },
        .is_not_null => |col| blk: {
            const idx = mt.schema.columnIndex(col) orelse break :blk false;
            break :blk mt.columns[idx].view().isValid(row);
        },
        .like => |lp| blk: {
            const idx = mt.schema.columnIndex(lp.col) orelse break :blk false;
            const view = mt.columns[idx].view();
            if (!view.isValid(row)) break :blk false;
            const cell = switch (view.data) {
                .varchar => |sv| sv.rowBytes(row),
                .string => |sv| sv.rowBytes(row),
                .char => |sv| sv.rowBytes(row),
                else => break :blk false,
            };
            break :blk likeMatch(cell, lp.pattern);
        },
        .p_and => |kids| {
            for (kids) |k| {
                if (!evaluatePredicateOnRow(mt, row, k)) return false;
            }
            return true;
        },
        .p_or => |kids| {
            for (kids) |k| {
                if (evaluatePredicateOnRow(mt, row, k)) return true;
            }
            return false;
        },
        .p_not => |child| !evaluatePredicateOnRow(mt, row, child.*),
        .always => |b| b,
        .in_set => |s| blk: {
            const idx = mt.schema.columnIndex(s.col) orelse break :blk false;
            const view = mt.columns[idx].view();
            if (!view.isValid(row)) {
                // NULL never matches; for IN false, for NOT IN false (dialect).
                break :blk false;
            }
            var found = false;
            for (s.values) |v| {
                if (predicateMatches(view, row, 0, v)) { // op=0 → eq
                    found = true;
                    break;
                }
            }
            break :blk if (s.negate) !found else found;
        },
    };
}

fn colColMatches(lv: ColumnView, rv: ColumnView, row: u32, op: u8) bool {
    return switch (lv.data) {
        .int => switch (rv.data) {
            .int => cmpI(i32, lv.data.int[row], rv.data.int[row], op),
            else => false,
        },
        .bigint => switch (rv.data) {
            .bigint => cmpI(i64, lv.data.bigint[row], rv.data.bigint[row], op),
            else => false,
        },
        .smallint => switch (rv.data) {
            .smallint => cmpI(i16, lv.data.smallint[row], rv.data.smallint[row], op),
            else => false,
        },
        .tinyint => switch (rv.data) {
            .tinyint => cmpI(i8, lv.data.tinyint[row], rv.data.tinyint[row], op),
            else => false,
        },
        .largeint => switch (rv.data) {
            .largeint => cmpI(i128, lv.data.largeint[row], rv.data.largeint[row], op),
            else => false,
        },
        .float => switch (rv.data) {
            .float => cmpF(f32, lv.data.float[row], rv.data.float[row], op),
            else => false,
        },
        .double => switch (rv.data) {
            .double => cmpF(f64, lv.data.double[row], rv.data.double[row], op),
            else => false,
        },
        .date => switch (rv.data) {
            .date => cmpI(i32, lv.data.date[row], rv.data.date[row], op),
            else => false,
        },
        .datetime => switch (rv.data) {
            .datetime => cmpI(i64, lv.data.datetime[row], rv.data.datetime[row], op),
            else => false,
        },
        .decimal64 => switch (rv.data) {
            .decimal64 => cmpI(i64, lv.data.decimal64[row], rv.data.decimal64[row], op),
            else => false,
        },
        .decimal128 => switch (rv.data) {
            .decimal128 => cmpI(i128, lv.data.decimal128[row], rv.data.decimal128[row], op),
            else => false,
        },
        .uuid => switch (rv.data) {
            .uuid => cmpI(u128, lv.data.uuid[row], rv.data.uuid[row], op),
            else => false,
        },
        .boolean => switch (rv.data) {
            .boolean => cmpI(u8, lv.data.boolean[row], rv.data.boolean[row], op),
            else => false,
        },
        .varchar, .string, .char => switch (rv.data) {
            .varchar => |sv| cmpStr(rowStringBytes(lv, row), sv.rowBytes(row), op),
            .string => |sv| cmpStr(rowStringBytes(lv, row), sv.rowBytes(row), op),
            .char => |sv| cmpStr(rowStringBytes(lv, row), sv.rowBytes(row), op),
            else => false,
        },
    };
}

fn rowStringBytes(view: ColumnView, row: u32) []const u8 {
    return switch (view.data) {
        .varchar => |sv| sv.rowBytes(row),
        .string => |sv| sv.rowBytes(row),
        .char => |sv| sv.rowBytes(row),
        else => &[_]u8{},
    };
}

/// Recursive LIKE matcher — `%` matches any run, `_` matches one byte.
/// Local copy to avoid the engine→exec import.
fn likeMatch(text: []const u8, pattern: []const u8) bool {
    var ti: usize = 0;
    var pi: usize = 0;
    var star_ti: ?usize = null;
    var star_pi: usize = 0;
    while (ti < text.len) {
        if (pi < pattern.len and pattern[pi] == '%') {
            star_pi = pi;
            star_ti = ti;
            pi += 1;
        } else if (pi < pattern.len and (pattern[pi] == '_' or pattern[pi] == text[ti])) {
            pi += 1;
            ti += 1;
        } else if (star_ti) |sti| {
            pi = star_pi + 1;
            ti = sti + 1;
            star_ti = sti + 1;
        } else return false;
    }
    while (pi < pattern.len and pattern[pi] == '%') pi += 1;
    return pi == pattern.len;
}

test {
    // Pulls in nothing yet; tests for end-to-end behavior live in
    // tests/integration/roundtrip.zig under "wal:" prefixed tests.
}
