//! Per-row comparison helpers shared by delete and upsert. Encodes a
//! ColumnView row's value into a byte slice (for compound-key hashing) or
//! evaluates a single PredicateOp against it.

const std = @import("std");
const Allocator = std.mem.Allocator;

const storage = @import("../storage/storage.zig");
const exec = @import("../exec/exec.zig");

pub fn cmpU32(target: u32, item: u32) std.math.Order {
    return std.math.order(target, item);
}

pub fn cmpVal(comptime T: type, a: T, b: T, op: exec.PredicateOp) bool {
    return switch (op) {
        .eq => a == b,
        .neq => a != b,
        .lt => a < b,
        .lte => a <= b,
        .gt => a > b,
        .gte => a >= b,
    };
}

pub fn cmpStr(a: []const u8, b: []const u8, op: exec.PredicateOp) bool {
    const eq = std.mem.eql(u8, a, b);
    return switch (op) {
        .eq => eq,
        .neq => !eq,
        else => unreachable, // pre-validated upstream
    };
}

/// Single-row predicate evaluation against a column view.
pub fn evalRow(view: storage.ColumnView, row: u32, pred: exec.Predicate) bool {
    return switch (view.data) {
        .int => |s| cmpVal(i32, s[row], pred.val.int, pred.op),
        .bigint => |s| cmpVal(i64, s[row], pred.val.bigint, pred.op),
        .boolean => |s| cmpVal(u8, s[row], @intFromBool(pred.val.boolean), pred.op),
        .varchar => |sv| cmpStr(sv.rowBytes(row), pred.val.text, pred.op),
        .string => |sv| cmpStr(sv.rowBytes(row), pred.val.text, pred.op),
        .float => |s| cmpVal(f32, s[row], pred.val.float, pred.op),
        .double => |s| cmpVal(f64, s[row], pred.val.double, pred.op),
        .date => |s| cmpVal(i32, s[row], pred.val.date, pred.op),
        .datetime => |s| cmpVal(i64, s[row], pred.val.datetime, pred.op),
        .tinyint => |s| cmpVal(i8, s[row], pred.val.tinyint, pred.op),
        .smallint => |s| cmpVal(i16, s[row], pred.val.smallint, pred.op),
        .largeint => |s| cmpVal(i128, s[row], pred.val.largeint, pred.op),
        .char => |sv| cmpStr(sv.rowBytes(row), pred.val.text, pred.op),
        .decimal64 => |s| cmpVal(i64, s[row], pred.val.decimal64, pred.op),
        .decimal128 => |s| cmpVal(i128, s[row], pred.val.decimal128, pred.op),
        .uuid => |s| cmpVal(u128, s[row], pred.val.uuid, pred.op),
    };
}

/// Append the bytes of `row`'s value in `view` to `buf`. Layout per type:
///   - INT (i32):    4 bytes LE        - BIGINT (i64): 8 bytes LE
///   - TINYINT (i8): 1 byte             - SMALLINT (i16): 2 bytes LE
///   - LARGEINT (i128): 16 bytes LE     - BOOLEAN (u8): 1 byte
///   - FLOAT (f32):  4 bytes LE         - DOUBLE (f64): 8 bytes LE
///   - DATE (i32):   4 bytes LE         - DATETIME (i64): 8 bytes LE
///   - VARCHAR/STRING/CHAR: 4-byte LE length + bytes
pub fn appendColumnValueBytes(
    aa: Allocator,
    buf: *std.ArrayList(u8),
    view: storage.ColumnView,
    row: u32,
) !void {
    switch (view.data) {
        .int => |s| try storage.format.appendI32(aa, buf, s[row]),
        .bigint => |s| try storage.format.appendI64(aa, buf, s[row]),
        .boolean => |s| try buf.append(aa, s[row]),
        .varchar => |sv| {
            const bytes = sv.rowBytes(row);
            try storage.format.appendU32(aa, buf, @intCast(bytes.len));
            try buf.appendSlice(aa, bytes);
        },
        .string => |sv| {
            const bytes = sv.rowBytes(row);
            try storage.format.appendU32(aa, buf, @intCast(bytes.len));
            try buf.appendSlice(aa, bytes);
        },
        .float => |s| {
            var b: [4]u8 = undefined;
            storage.format.writeF32(&b, s[row]);
            try buf.appendSlice(aa, &b);
        },
        .double => |s| {
            var b: [8]u8 = undefined;
            storage.format.writeF64(&b, s[row]);
            try buf.appendSlice(aa, &b);
        },
        .date => |s| try storage.format.appendI32(aa, buf, s[row]),
        .datetime => |s| try storage.format.appendI64(aa, buf, s[row]),
        .tinyint => |s| try buf.append(aa, @bitCast(s[row])),
        .smallint => |s| {
            var b: [2]u8 = undefined;
            std.mem.writeInt(i16, &b, s[row], .little);
            try buf.appendSlice(aa, &b);
        },
        .largeint => |s| {
            var b: [16]u8 = undefined;
            std.mem.writeInt(i128, &b, s[row], .little);
            try buf.appendSlice(aa, &b);
        },
        .char => |sv| {
            const bytes = sv.rowBytes(row);
            try storage.format.appendU32(aa, buf, @intCast(bytes.len));
            try buf.appendSlice(aa, bytes);
        },
        .decimal64 => |s| try storage.format.appendI64(aa, buf, s[row]),
        .decimal128 => |s| {
            var b: [16]u8 = undefined;
            std.mem.writeInt(i128, &b, s[row], .little);
            try buf.appendSlice(aa, &b);
        },
        .uuid => |s| {
            var b: [16]u8 = undefined;
            std.mem.writeInt(u128, &b, s[row], .little);
            try buf.appendSlice(aa, &b);
        },
    }
}
