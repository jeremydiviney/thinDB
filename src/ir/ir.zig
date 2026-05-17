//! Operator IR — the binary wire format clients send to the server.
//!
//! Each query is a single operator tree, encoded as a tagged tree: each
//! operator carries its upstream encoded immediately after the operator's
//! own payload. Decoding is recursive — read the tag, decode the op-
//! specific payload, then (for non-source operators) recursively decode
//! the upstream.
//!
//! Scope (walking skeleton): Scan + Limit only. More operators land as
//! the client API grows. The format is versioned so additions are safe.
//!
//! Wire format:
//!
//!   [header — 8 bytes]
//!     magic  "tDBQ"          4
//!     version u16            2
//!     flags u16              2 (reserved)
//!
//!   [op tree — recursive]
//!     tag u8                 1
//!     op-specific payload    N
//!     (for non-source ops) upstream op tree (recursive)

const std = @import("std");
const Allocator = std.mem.Allocator;

const types = @import("../types.zig");
const Value = types.Value;
const ValueTag = types.ValueTag;

const exec_predicate = @import("../exec/predicate.zig");
const Predicate = exec_predicate.Predicate;
const PredicateExpr = exec_predicate.PredicateExpr;
const PredicateOp = exec_predicate.PredicateOp;

pub const magic: [4]u8 = .{ 't', 'D', 'B', 'Q' };
pub const version: u16 = 1;
pub const header_size: usize = 8;

pub const Error = error{
    IrBadMagic,
    IrUnsupportedVersion,
    IrTooSmall,
    IrUnknownOp,
    IrCorrupt,
};

pub const OpTag = enum(u8) {
    scan = 0,
    limit = 1,
    /// Whitelist projection: keep only these columns, in the listed order.
    select = 2,
    /// Anti-projection: drop these columns. Strict pipeline semantics —
    /// after Exclude, downstream operators cannot reference the dropped
    /// columns. (Server enforces this via the existing per-operator
    /// schema-lookup error path.)
    exclude = 3,
    /// Predicate filter. `.where` and `.filter` on the client both encode
    /// to this tag — `.where` is the SQL-flavored canonical spelling.
    filter = 4,
};

/// In-memory operator tree, built by the client query-builder and decoded
/// by the server dispatcher. Tagged union: each variant carries an
/// optional `upstream` (null for sources like Scan).
pub const Op = union(OpTag) {
    scan: Scan,
    limit: Limit,
    select: Project,
    exclude: Project,
    filter: Filter,

    pub const Scan = struct {
        /// Table name. Borrowed from the encoded buffer on decode; owned
        /// by the client on encode. Lifetime matches the surrounding Op.
        table_name: []const u8,
    };

    pub const Limit = struct {
        n: u64,
        upstream: *Op,
    };

    pub const Project = struct {
        /// Column names. Shared variant for both .select (keep) and
        /// .exclude (drop) — disambiguated by the outer Op tag.
        columns: []const []const u8,
        upstream: *Op,
    };

    pub const Filter = struct {
        /// Decoded predicate tree. Strings (column names + text values) +
        /// children arrays + `not` child pointer are all allocated into
        /// the decoder's allocator; freed via `freeDecodedPredicate`.
        predicate: PredicateExpr,
        upstream: *Op,
    };

    /// Free any allocations made by `decode`. No-op for client-built trees
    /// whose strings come from caller storage.
    pub fn deinitDecoded(self: *Op, allocator: Allocator) void {
        switch (self.*) {
            .scan => {},
            .limit => |l| {
                l.upstream.deinitDecoded(allocator);
                allocator.destroy(l.upstream);
            },
            .select => |p| freeProject(p, allocator),
            .exclude => |p| freeProject(p, allocator),
            .filter => |f| {
                freeDecodedPredicate(f.predicate, allocator);
                f.upstream.deinitDecoded(allocator);
                allocator.destroy(f.upstream);
            },
        }
    }
};

fn freeProject(p: Op.Project, allocator: Allocator) void {
    // p.columns is a freshly-allocated slice of slices; the individual
    // string slices are borrowed from the encoded buffer (not owned).
    // Only free the outer slice.
    allocator.free(p.columns);
    p.upstream.deinitDecoded(allocator);
    allocator.destroy(p.upstream);
}

// ---------------------------------------------------------------------------
// Encode
// ---------------------------------------------------------------------------

/// Serialize `root` into `out` (header + tree). Caller owns `out`.
pub fn encode(allocator: Allocator, out: *std.ArrayList(u8), root: Op) !void {
    try out.appendSlice(allocator, &magic);
    try appendU16(allocator, out, version);
    try appendU16(allocator, out, 0); // flags
    try encodeOp(allocator, out, root);
}

const EncodeError = Allocator.Error;

fn encodeOp(allocator: Allocator, out: *std.ArrayList(u8), op: Op) EncodeError!void {
    try out.append(allocator, @intFromEnum(@as(OpTag, op)));
    switch (op) {
        .scan => |s| {
            try appendU32(allocator, out, @intCast(s.table_name.len));
            try out.appendSlice(allocator, s.table_name);
        },
        .limit => |l| {
            try appendU64(allocator, out, l.n);
            try encodeOp(allocator, out, l.upstream.*);
        },
        .select => |p| try encodeProject(allocator, out, p),
        .exclude => |p| try encodeProject(allocator, out, p),
        .filter => |f| try encodeFilter(allocator, out, f),
    }
}

fn encodeProject(allocator: Allocator, out: *std.ArrayList(u8), p: Op.Project) EncodeError!void {
    try appendU32(allocator, out, @intCast(p.columns.len));
    for (p.columns) |c| {
        try appendU32(allocator, out, @intCast(c.len));
        try out.appendSlice(allocator, c);
    }
    try encodeOp(allocator, out, p.upstream.*);
}

fn encodeFilter(allocator: Allocator, out: *std.ArrayList(u8), f: Op.Filter) EncodeError!void {
    try encodePredicate(allocator, out, f.predicate);
    try encodeOp(allocator, out, f.upstream.*);
}

// ---------------------------------------------------------------------------
// Predicate encoding — mirrors exec.predicate.PredicateExpr shape.
//
//   Tag (u8):
//     0 = leaf            [op u8][col_len u32][col bytes][value]
//     1 = is_null         [col_len u32][col bytes]
//     2 = is_not_null     [col_len u32][col bytes]
//     3 = and             [n u32][child0][child1]...
//     4 = or              [n u32][child0][child1]...
//     5 = not             [child]
//
//   Value (after value_tag u8): per-type bytes.
//     int       i32 LE       (4)
//     bigint    i64 LE       (8)
//     boolean   u8           (1)
//     text      u32 len + bytes
//     float     f32 LE       (4)
//     double    f64 LE       (8)
//     date      i32 LE       (4)
//     datetime  i64 LE       (8)
//     tinyint   i8           (1)
//     smallint  i16 LE       (2)
//     largeint  i128 LE      (16)
//     decimal64 i64 LE       (8)
//     decimal128 i128 LE     (16)
// ---------------------------------------------------------------------------

const PredTag = enum(u8) {
    leaf = 0,
    is_null = 1,
    is_not_null = 2,
    p_and = 3,
    p_or = 4,
    p_not = 5,
};

fn encodePredicate(allocator: Allocator, out: *std.ArrayList(u8), expr: PredicateExpr) EncodeError!void {
    switch (expr) {
        .leaf => |p| {
            try out.append(allocator, @intFromEnum(PredTag.leaf));
            try out.append(allocator, @intFromEnum(p.op));
            try appendU32(allocator, out, @intCast(p.col.len));
            try out.appendSlice(allocator, p.col);
            try encodeValue(allocator, out, p.val);
        },
        .is_null => |col| {
            try out.append(allocator, @intFromEnum(PredTag.is_null));
            try appendU32(allocator, out, @intCast(col.len));
            try out.appendSlice(allocator, col);
        },
        .is_not_null => |col| {
            try out.append(allocator, @intFromEnum(PredTag.is_not_null));
            try appendU32(allocator, out, @intCast(col.len));
            try out.appendSlice(allocator, col);
        },
        .@"and" => |children| {
            try out.append(allocator, @intFromEnum(PredTag.p_and));
            try appendU32(allocator, out, @intCast(children.len));
            for (children) |c| try encodePredicate(allocator, out, c);
        },
        .@"or" => |children| {
            try out.append(allocator, @intFromEnum(PredTag.p_or));
            try appendU32(allocator, out, @intCast(children.len));
            for (children) |c| try encodePredicate(allocator, out, c);
        },
        .not => |child| {
            try out.append(allocator, @intFromEnum(PredTag.p_not));
            try encodePredicate(allocator, out, child.*);
        },
    }
}

fn encodeValue(allocator: Allocator, out: *std.ArrayList(u8), v: Value) EncodeError!void {
    try out.append(allocator, @intFromEnum(@as(ValueTag, v)));
    var b: [16]u8 = undefined;
    switch (v) {
        .int => |x| {
            std.mem.writeInt(i32, b[0..4], x, .little);
            try out.appendSlice(allocator, b[0..4]);
        },
        .bigint => |x| {
            std.mem.writeInt(i64, b[0..8], x, .little);
            try out.appendSlice(allocator, b[0..8]);
        },
        .boolean => |x| try out.append(allocator, @intFromBool(x)),
        .text => |s| {
            try appendU32(allocator, out, @intCast(s.len));
            try out.appendSlice(allocator, s);
        },
        .float => |x| {
            std.mem.writeInt(u32, b[0..4], @bitCast(x), .little);
            try out.appendSlice(allocator, b[0..4]);
        },
        .double => |x| {
            std.mem.writeInt(u64, b[0..8], @bitCast(x), .little);
            try out.appendSlice(allocator, b[0..8]);
        },
        .date => |x| {
            std.mem.writeInt(i32, b[0..4], x, .little);
            try out.appendSlice(allocator, b[0..4]);
        },
        .datetime => |x| {
            std.mem.writeInt(i64, b[0..8], x, .little);
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
            std.mem.writeInt(i64, b[0..8], x, .little);
            try out.appendSlice(allocator, b[0..8]);
        },
        .decimal128 => |x| {
            std.mem.writeInt(i128, b[0..16], x, .little);
            try out.appendSlice(allocator, &b);
        },
    }
}

// ---------------------------------------------------------------------------
// Decode
// ---------------------------------------------------------------------------

/// Parse `bytes` into an Op tree. Returns the root op; caller calls
/// `root.deinitDecoded(allocator)` when done. String fields (table_name)
/// are borrowed slices into `bytes` — keep it alive until the tree is
/// no longer needed.
pub fn decode(allocator: Allocator, bytes: []const u8) !Op {
    if (bytes.len < header_size) return Error.IrTooSmall;
    if (!std.mem.eql(u8, bytes[0..4], &magic)) return Error.IrBadMagic;
    const ver = readU16(bytes[4..6]);
    if (ver != version) return Error.IrUnsupportedVersion;
    var cursor: usize = header_size;
    return try decodeOp(allocator, bytes, &cursor);
}

const DecodeError = Error || Allocator.Error;

fn decodeOp(allocator: Allocator, bytes: []const u8, cursor: *usize) DecodeError!Op {
    if (cursor.* + 1 > bytes.len) return Error.IrCorrupt;
    const tag_byte = bytes[cursor.*];
    cursor.* += 1;
    if (tag_byte > @intFromEnum(OpTag.filter)) return Error.IrUnknownOp;
    const tag: OpTag = @enumFromInt(tag_byte);

    return switch (tag) {
        .scan => blk: {
            if (cursor.* + 4 > bytes.len) return Error.IrCorrupt;
            const name_len = readU32(bytes[cursor.* .. cursor.* + 4]);
            cursor.* += 4;
            if (cursor.* + name_len > bytes.len) return Error.IrCorrupt;
            const name = bytes[cursor.* .. cursor.* + name_len];
            cursor.* += name_len;
            break :blk Op{ .scan = .{ .table_name = name } };
        },
        .limit => blk: {
            if (cursor.* + 8 > bytes.len) return Error.IrCorrupt;
            const n = readU64(bytes[cursor.* .. cursor.* + 8]);
            cursor.* += 8;
            const upstream = try allocator.create(Op);
            errdefer allocator.destroy(upstream);
            upstream.* = try decodeOp(allocator, bytes, cursor);
            break :blk Op{ .limit = .{ .n = n, .upstream = upstream } };
        },
        .select, .exclude => blk: {
            const project = try decodeProject(allocator, bytes, cursor);
            break :blk if (tag == .select) Op{ .select = project } else Op{ .exclude = project };
        },
        .filter => blk: {
            const pred = try decodePredicate(allocator, bytes, cursor);
            errdefer freeDecodedPredicate(pred, allocator);
            const upstream = try allocator.create(Op);
            errdefer allocator.destroy(upstream);
            upstream.* = try decodeOp(allocator, bytes, cursor);
            break :blk Op{ .filter = .{ .predicate = pred, .upstream = upstream } };
        },
    };
}

fn decodePredicate(allocator: Allocator, bytes: []const u8, cursor: *usize) DecodeError!PredicateExpr {
    if (cursor.* + 1 > bytes.len) return Error.IrCorrupt;
    const tag_byte = bytes[cursor.*];
    cursor.* += 1;
    if (tag_byte > @intFromEnum(PredTag.p_not)) return Error.IrCorrupt;
    const tag: PredTag = @enumFromInt(tag_byte);

    return switch (tag) {
        .leaf => blk: {
            if (cursor.* + 1 > bytes.len) return Error.IrCorrupt;
            const op_byte = bytes[cursor.*];
            cursor.* += 1;
            if (op_byte > @intFromEnum(PredicateOp.gte)) return Error.IrCorrupt;
            const op: PredicateOp = @enumFromInt(op_byte);
            const col = try readString(bytes, cursor);
            const val = try decodeValue(bytes, cursor);
            break :blk PredicateExpr{ .leaf = .{ .col = col, .op = op, .val = val } };
        },
        .is_null => blk: {
            const col = try readString(bytes, cursor);
            break :blk PredicateExpr{ .is_null = col };
        },
        .is_not_null => blk: {
            const col = try readString(bytes, cursor);
            break :blk PredicateExpr{ .is_not_null = col };
        },
        .p_and, .p_or => blk: {
            if (cursor.* + 4 > bytes.len) return Error.IrCorrupt;
            const n = readU32(bytes[cursor.* .. cursor.* + 4]);
            cursor.* += 4;
            const children = try allocator.alloc(PredicateExpr, n);
            errdefer {
                // On failure mid-decode, free what we built.
                var freed: u32 = 0;
                while (freed < n) : (freed += 1) {
                    // Only free initialized entries; rest are uninitialized.
                }
                allocator.free(children);
            }
            var i: u32 = 0;
            while (i < n) : (i += 1) {
                children[i] = try decodePredicate(allocator, bytes, cursor);
            }
            break :blk if (tag == .p_and) PredicateExpr{ .@"and" = children } else PredicateExpr{ .@"or" = children };
        },
        .p_not => blk: {
            const child = try allocator.create(PredicateExpr);
            errdefer allocator.destroy(child);
            child.* = try decodePredicate(allocator, bytes, cursor);
            break :blk PredicateExpr{ .not = child };
        },
    };
}

fn readString(bytes: []const u8, cursor: *usize) DecodeError![]const u8 {
    if (cursor.* + 4 > bytes.len) return Error.IrCorrupt;
    const len = readU32(bytes[cursor.* .. cursor.* + 4]);
    cursor.* += 4;
    if (cursor.* + len > bytes.len) return Error.IrCorrupt;
    const s = bytes[cursor.* .. cursor.* + len];
    cursor.* += len;
    return s;
}

fn decodeValue(bytes: []const u8, cursor: *usize) DecodeError!Value {
    if (cursor.* + 1 > bytes.len) return Error.IrCorrupt;
    const tag_byte = bytes[cursor.*];
    cursor.* += 1;
    const tag: ValueTag = @enumFromInt(tag_byte);
    const c = cursor.*;
    return switch (tag) {
        .int => blk: {
            if (c + 4 > bytes.len) return Error.IrCorrupt;
            cursor.* = c + 4;
            break :blk Value{ .int = std.mem.readInt(i32, bytes[c..][0..4], .little) };
        },
        .bigint => blk: {
            if (c + 8 > bytes.len) return Error.IrCorrupt;
            cursor.* = c + 8;
            break :blk Value{ .bigint = std.mem.readInt(i64, bytes[c..][0..8], .little) };
        },
        .boolean => blk: {
            if (c + 1 > bytes.len) return Error.IrCorrupt;
            cursor.* = c + 1;
            break :blk Value{ .boolean = bytes[c] != 0 };
        },
        .text => blk: {
            if (c + 4 > bytes.len) return Error.IrCorrupt;
            const len = readU32(bytes[c..][0..4]);
            if (c + 4 + len > bytes.len) return Error.IrCorrupt;
            cursor.* = c + 4 + len;
            break :blk Value{ .text = bytes[c + 4 .. c + 4 + len] };
        },
        .float => blk: {
            if (c + 4 > bytes.len) return Error.IrCorrupt;
            cursor.* = c + 4;
            const raw = std.mem.readInt(u32, bytes[c..][0..4], .little);
            break :blk Value{ .float = @bitCast(raw) };
        },
        .double => blk: {
            if (c + 8 > bytes.len) return Error.IrCorrupt;
            cursor.* = c + 8;
            const raw = std.mem.readInt(u64, bytes[c..][0..8], .little);
            break :blk Value{ .double = @bitCast(raw) };
        },
        .date => blk: {
            if (c + 4 > bytes.len) return Error.IrCorrupt;
            cursor.* = c + 4;
            break :blk Value{ .date = std.mem.readInt(i32, bytes[c..][0..4], .little) };
        },
        .datetime => blk: {
            if (c + 8 > bytes.len) return Error.IrCorrupt;
            cursor.* = c + 8;
            break :blk Value{ .datetime = std.mem.readInt(i64, bytes[c..][0..8], .little) };
        },
        .tinyint => blk: {
            if (c + 1 > bytes.len) return Error.IrCorrupt;
            cursor.* = c + 1;
            break :blk Value{ .tinyint = @bitCast(bytes[c]) };
        },
        .smallint => blk: {
            if (c + 2 > bytes.len) return Error.IrCorrupt;
            cursor.* = c + 2;
            break :blk Value{ .smallint = std.mem.readInt(i16, bytes[c..][0..2], .little) };
        },
        .largeint => blk: {
            if (c + 16 > bytes.len) return Error.IrCorrupt;
            cursor.* = c + 16;
            break :blk Value{ .largeint = std.mem.readInt(i128, bytes[c..][0..16], .little) };
        },
        .decimal64 => blk: {
            if (c + 8 > bytes.len) return Error.IrCorrupt;
            cursor.* = c + 8;
            break :blk Value{ .decimal64 = std.mem.readInt(i64, bytes[c..][0..8], .little) };
        },
        .decimal128 => blk: {
            if (c + 16 > bytes.len) return Error.IrCorrupt;
            cursor.* = c + 16;
            break :blk Value{ .decimal128 = std.mem.readInt(i128, bytes[c..][0..16], .little) };
        },
    };
}

fn freeDecodedPredicate(expr: PredicateExpr, allocator: Allocator) void {
    switch (expr) {
        .leaf, .is_null, .is_not_null => {},
        .@"and", .@"or" => |children| {
            for (children) |c| freeDecodedPredicate(c, allocator);
            allocator.free(children);
        },
        .not => |child| {
            freeDecodedPredicate(child.*, allocator);
            allocator.destroy(child);
        },
    }
}

fn decodeProject(allocator: Allocator, bytes: []const u8, cursor: *usize) DecodeError!Op.Project {
    if (cursor.* + 4 > bytes.len) return Error.IrCorrupt;
    const n_cols = readU32(bytes[cursor.* .. cursor.* + 4]);
    cursor.* += 4;

    const cols = try allocator.alloc([]const u8, n_cols);
    errdefer allocator.free(cols);
    for (cols) |*c| {
        if (cursor.* + 4 > bytes.len) return Error.IrCorrupt;
        const col_len = readU32(bytes[cursor.* .. cursor.* + 4]);
        cursor.* += 4;
        if (cursor.* + col_len > bytes.len) return Error.IrCorrupt;
        c.* = bytes[cursor.* .. cursor.* + col_len];
        cursor.* += col_len;
    }

    const upstream = try allocator.create(Op);
    errdefer allocator.destroy(upstream);
    upstream.* = try decodeOp(allocator, bytes, cursor);
    return .{ .columns = cols, .upstream = upstream };
}

// ---------------------------------------------------------------------------
// Little-endian helpers (lifted from storage/format.zig style)
// ---------------------------------------------------------------------------

fn appendU16(allocator: Allocator, out: *std.ArrayList(u8), v: u16) !void {
    var b: [2]u8 = undefined;
    std.mem.writeInt(u16, &b, v, .little);
    try out.appendSlice(allocator, &b);
}

fn appendU32(allocator: Allocator, out: *std.ArrayList(u8), v: u32) !void {
    var b: [4]u8 = undefined;
    std.mem.writeInt(u32, &b, v, .little);
    try out.appendSlice(allocator, &b);
}

fn appendU64(allocator: Allocator, out: *std.ArrayList(u8), v: u64) !void {
    var b: [8]u8 = undefined;
    std.mem.writeInt(u64, &b, v, .little);
    try out.appendSlice(allocator, &b);
}

fn readU16(b: []const u8) u16 {
    return std.mem.readInt(u16, b[0..2], .little);
}

fn readU32(b: []const u8) u32 {
    return std.mem.readInt(u32, b[0..4], .little);
}

fn readU64(b: []const u8) u64 {
    return std.mem.readInt(u64, b[0..8], .little);
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

test "ir: scan + limit round-trips through encode/decode" {
    const allocator = std.testing.allocator;

    var limit_upstream_storage: Op = .{ .scan = .{ .table_name = "orders" } };
    const root: Op = .{ .limit = .{ .n = 42, .upstream = &limit_upstream_storage } };

    var buf: std.ArrayList(u8) = .empty;
    defer buf.deinit(allocator);
    try encode(allocator, &buf, root);

    var decoded = try decode(allocator, buf.items);
    defer decoded.deinitDecoded(allocator);

    try std.testing.expect(decoded == .limit);
    try std.testing.expectEqual(@as(u64, 42), decoded.limit.n);
    try std.testing.expect(decoded.limit.upstream.* == .scan);
    try std.testing.expectEqualStrings("orders", decoded.limit.upstream.scan.table_name);
}

test "ir: decode rejects bad magic" {
    const allocator = std.testing.allocator;
    const bad = [_]u8{ 'X', 'X', 'X', 'X', 1, 0, 0, 0 };
    try std.testing.expectError(Error.IrBadMagic, decode(allocator, &bad));
}

test "ir: decode rejects unsupported version" {
    const allocator = std.testing.allocator;
    const bad = [_]u8{ 't', 'D', 'B', 'Q', 99, 0, 0, 0 };
    try std.testing.expectError(Error.IrUnsupportedVersion, decode(allocator, &bad));
}

test "ir: decode rejects truncated input" {
    const allocator = std.testing.allocator;
    const short = [_]u8{ 't', 'D', 'B' };
    try std.testing.expectError(Error.IrTooSmall, decode(allocator, &short));
}

test "ir: select round-trips with multiple columns" {
    const allocator = std.testing.allocator;

    var scan_storage: Op = .{ .scan = .{ .table_name = "orders" } };
    const cols = [_][]const u8{ "id", "qty", "tag" };
    const root: Op = .{ .select = .{ .columns = &cols, .upstream = &scan_storage } };

    var buf: std.ArrayList(u8) = .empty;
    defer buf.deinit(allocator);
    try encode(allocator, &buf, root);

    var decoded = try decode(allocator, buf.items);
    defer decoded.deinitDecoded(allocator);

    try std.testing.expect(decoded == .select);
    try std.testing.expectEqual(@as(usize, 3), decoded.select.columns.len);
    try std.testing.expectEqualStrings("id", decoded.select.columns[0]);
    try std.testing.expectEqualStrings("qty", decoded.select.columns[1]);
    try std.testing.expectEqualStrings("tag", decoded.select.columns[2]);
    try std.testing.expect(decoded.select.upstream.* == .scan);
    try std.testing.expectEqualStrings("orders", decoded.select.upstream.scan.table_name);
}

test "ir: exclude round-trips and is distinguishable from select" {
    const allocator = std.testing.allocator;

    var scan_storage: Op = .{ .scan = .{ .table_name = "t" } };
    const cols = [_][]const u8{"secret"};
    const root: Op = .{ .exclude = .{ .columns = &cols, .upstream = &scan_storage } };

    var buf: std.ArrayList(u8) = .empty;
    defer buf.deinit(allocator);
    try encode(allocator, &buf, root);

    var decoded = try decode(allocator, buf.items);
    defer decoded.deinitDecoded(allocator);

    try std.testing.expect(decoded == .exclude);
    try std.testing.expectEqualStrings("secret", decoded.exclude.columns[0]);
}
