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
};

/// In-memory operator tree, built by the client query-builder and decoded
/// by the server dispatcher. Tagged union: each variant carries an
/// optional `upstream` (null for sources like Scan).
pub const Op = union(OpTag) {
    scan: Scan,
    limit: Limit,
    select: Project,
    exclude: Project,

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
    if (tag_byte > @intFromEnum(OpTag.exclude)) return Error.IrUnknownOp;
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
    };
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
