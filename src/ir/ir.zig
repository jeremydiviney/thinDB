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

const exec_sort = @import("../exec/sort.zig");
pub const SortSpec = exec_sort.SortSpec;

const exec_aggregate = @import("../exec/aggregate.zig");
pub const AggFunc = exec_aggregate.AggFunc;
pub const AggSpec = exec_aggregate.AggSpec;

const exec_compute = @import("../exec/compute.zig");
pub const Derived = exec_compute.Derived;

const exec_join = @import("../exec/join.zig");
pub const JoinSpec = exec_join.Spec;
pub const JoinKeyPair = exec_join.KeyPair;
pub const JoinRangePredicate = exec_join.RangePredicate;
pub const JoinAlgorithm = exec_join.Algorithm;
pub const JoinType = exec_join.JoinType;

const exec_expr = @import("../exec/expr.zig");
pub const Expr = exec_expr.Expr;

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
    /// Multi-column ORDER BY with per-key ASC/DESC.
    order_by = 5,
    /// GROUP BY (with empty group_cols, acts as a global aggregate).
    group_by = 6,
    /// Derived columns via scalar functions.
    compute = 7,
    /// Inner / outer / range / opaque join. Two upstreams.
    join = 8,
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
    order_by: OrderBy,
    group_by: GroupBy,
    compute: Compute,
    join: Join,

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

    pub const OrderBy = struct {
        /// Sort keys + per-key ASC/DESC.
        specs: []const SortSpec,
        upstream: *Op,
    };

    pub const GroupBy = struct {
        /// Group-by column names. Empty slice = global aggregate (one
        /// output row over the whole input).
        group_cols: []const []const u8,
        /// Aggregate specs (func + col + output name).
        aggs: []const AggSpec,
        upstream: *Op,
    };

    pub const Compute = struct {
        /// Derived columns to append. Each carries an output name +
        /// an Expr tree (col_ref / lit / call).
        derived: []const Derived,
        upstream: *Op,
    };

    pub const Join = struct {
        /// Join algorithm choice. `.auto` lets compile() route via
        /// stats; explicit values pin the operator.
        algorithm: JoinAlgorithm,
        join_type: JoinType,
        /// Equi-join key pairs (left_col, right_col). Empty when the
        /// shape is pure-range or pure-opaque-predicate.
        on: []const JoinKeyPair,
        /// Inequality predicates AND'd onto the equi join.
        ranges: []const JoinRangePredicate,
        /// Optional post-join filter (Predicate over the joined schema).
        extra_predicate: ?PredicateExpr,
        skew_ratio_threshold: f32,
        skew_absolute_threshold: u32,
        skew_sample_interval: u32,
        /// Two upstreams. Both compile to executable Queries before the
        /// join operator is built.
        left: *Op,
        right: *Op,
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
            .order_by => |o| {
                allocator.free(o.specs);
                o.upstream.deinitDecoded(allocator);
                allocator.destroy(o.upstream);
            },
            .group_by => |g| {
                allocator.free(g.group_cols);
                allocator.free(g.aggs);
                g.upstream.deinitDecoded(allocator);
                allocator.destroy(g.upstream);
            },
            .compute => |c| {
                for (c.derived) |d| freeDecodedExpr(d.expr, allocator);
                allocator.free(c.derived);
                c.upstream.deinitDecoded(allocator);
                allocator.destroy(c.upstream);
            },
            .join => |j| {
                if (j.on.len > 0) allocator.free(j.on);
                if (j.ranges.len > 0) allocator.free(j.ranges);
                if (j.extra_predicate) |pred| freeDecodedPredicate(pred, allocator);
                j.left.deinitDecoded(allocator);
                allocator.destroy(j.left);
                j.right.deinitDecoded(allocator);
                allocator.destroy(j.right);
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
        .order_by => |o| try encodeOrderBy(allocator, out, o),
        .group_by => |g| try encodeGroupBy(allocator, out, g),
        .compute => |c| try encodeCompute(allocator, out, c),
        .join => |j| try encodeJoin(allocator, out, j),
    }
}

fn encodeOrderBy(allocator: Allocator, out: *std.ArrayList(u8), o: Op.OrderBy) EncodeError!void {
    try appendU32(allocator, out, @intCast(o.specs.len));
    for (o.specs) |s| {
        try appendU32(allocator, out, @intCast(s.col.len));
        try out.appendSlice(allocator, s.col);
        try out.append(allocator, @intFromBool(s.desc));
    }
    try encodeOp(allocator, out, o.upstream.*);
}

fn encodeGroupBy(allocator: Allocator, out: *std.ArrayList(u8), g: Op.GroupBy) EncodeError!void {
    try appendU32(allocator, out, @intCast(g.group_cols.len));
    for (g.group_cols) |c| {
        try appendU32(allocator, out, @intCast(c.len));
        try out.appendSlice(allocator, c);
    }
    try appendU32(allocator, out, @intCast(g.aggs.len));
    for (g.aggs) |a| {
        // func (u8)
        try out.append(allocator, @intFromEnum(a.func));
        // optional column: 0 = null (COUNT(*)), 1 = present
        if (a.col) |c| {
            try out.append(allocator, 1);
            try appendU32(allocator, out, @intCast(c.len));
            try out.appendSlice(allocator, c);
        } else {
            try out.append(allocator, 0);
        }
        // alias (always present — server enforces this on the existing API)
        try appendU32(allocator, out, @intCast(a.as.len));
        try out.appendSlice(allocator, a.as);
        // params tag (u8) + payload (matches decoder)
        switch (a.params) {
            .none => try out.append(allocator, 0),
            .percentile => |p| {
                try out.append(allocator, 1);
                const bits: u64 = @bitCast(p);
                var b: [8]u8 = undefined;
                std.mem.writeInt(u64, &b, bits, .little);
                try out.appendSlice(allocator, &b);
            },
            .separator => |sep| {
                try out.append(allocator, 2);
                try appendU32(allocator, out, @intCast(sep.len));
                try out.appendSlice(allocator, sep);
            },
        }
    }
    try encodeOp(allocator, out, g.upstream.*);
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

fn encodeCompute(allocator: Allocator, out: *std.ArrayList(u8), c: Op.Compute) EncodeError!void {
    try appendU32(allocator, out, @intCast(c.derived.len));
    for (c.derived) |d| {
        try appendU32(allocator, out, @intCast(d.name.len));
        try out.appendSlice(allocator, d.name);
        try encodeExpr(allocator, out, d.expr);
    }
    try encodeOp(allocator, out, c.upstream.*);
}

fn encodeJoin(allocator: Allocator, out: *std.ArrayList(u8), j: Op.Join) EncodeError!void {
    try out.append(allocator, @intFromEnum(j.algorithm));
    try out.append(allocator, @intFromEnum(j.join_type));
    // ON pairs
    try appendU32(allocator, out, @intCast(j.on.len));
    for (j.on) |kp| {
        try appendU32(allocator, out, @intCast(kp.left.len));
        try out.appendSlice(allocator, kp.left);
        try appendU32(allocator, out, @intCast(kp.right.len));
        try out.appendSlice(allocator, kp.right);
    }
    // Ranges
    try appendU32(allocator, out, @intCast(j.ranges.len));
    for (j.ranges) |rg| {
        try appendU32(allocator, out, @intCast(rg.left.len));
        try out.appendSlice(allocator, rg.left);
        try appendU32(allocator, out, @intCast(rg.right.len));
        try out.appendSlice(allocator, rg.right);
        try out.append(allocator, @intFromEnum(rg.op));
    }
    // Optional extra predicate (post-join filter)
    if (j.extra_predicate) |pred| {
        try out.append(allocator, 1);
        try encodePredicate(allocator, out, pred);
    } else {
        try out.append(allocator, 0);
    }
    // Skew detection knobs (f32 ratio + u32 absolute + u32 interval)
    {
        const bits: u32 = @bitCast(j.skew_ratio_threshold);
        var b: [4]u8 = undefined;
        std.mem.writeInt(u32, &b, bits, .little);
        try out.appendSlice(allocator, &b);
    }
    try appendU32(allocator, out, j.skew_absolute_threshold);
    try appendU32(allocator, out, j.skew_sample_interval);
    // Two upstreams
    try encodeOp(allocator, out, j.left.*);
    try encodeOp(allocator, out, j.right.*);
}

const ExprTag = enum(u8) { col_ref = 0, lit = 1, call = 2 };

/// Wire-encode an Expr tree (col_ref / lit / call). Recursive — mirrors
/// the recursive Predicate encoding above.
pub fn encodeExpr(allocator: Allocator, out: *std.ArrayList(u8), e: Expr) EncodeError!void {
    switch (e) {
        .col_ref => |name| {
            try out.append(allocator, @intFromEnum(ExprTag.col_ref));
            try appendU32(allocator, out, @intCast(name.len));
            try out.appendSlice(allocator, name);
        },
        .lit => |v| {
            try out.append(allocator, @intFromEnum(ExprTag.lit));
            try encodeValue(allocator, out, v);
        },
        .call => |c| {
            try out.append(allocator, @intFromEnum(ExprTag.call));
            try appendU32(allocator, out, @intCast(c.fn_name.len));
            try out.appendSlice(allocator, c.fn_name);
            try appendU32(allocator, out, @intCast(c.args.len));
            for (c.args) |child| try encodeExpr(allocator, out, child);
        },
    }
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

pub fn encodePredicate(allocator: Allocator, out: *std.ArrayList(u8), expr: PredicateExpr) EncodeError!void {
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

pub fn encodeValue(allocator: Allocator, out: *std.ArrayList(u8), v: Value) EncodeError!void {
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
        .uuid => |x| {
            std.mem.writeInt(u128, b[0..16], x, .little);
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
    if (tag_byte > @intFromEnum(OpTag.join)) return Error.IrUnknownOp;
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
        .order_by => blk: {
            if (cursor.* + 4 > bytes.len) return Error.IrCorrupt;
            const n = readU32(bytes[cursor.* .. cursor.* + 4]);
            cursor.* += 4;
            const specs = try allocator.alloc(SortSpec, n);
            errdefer allocator.free(specs);
            for (specs) |*s| {
                const col = try readString(bytes, cursor);
                if (cursor.* + 1 > bytes.len) return Error.IrCorrupt;
                const desc = bytes[cursor.*] != 0;
                cursor.* += 1;
                s.* = .{ .col = col, .desc = desc };
            }
            const upstream = try allocator.create(Op);
            errdefer allocator.destroy(upstream);
            upstream.* = try decodeOp(allocator, bytes, cursor);
            break :blk Op{ .order_by = .{ .specs = specs, .upstream = upstream } };
        },
        .group_by => blk: {
            // group_cols
            if (cursor.* + 4 > bytes.len) return Error.IrCorrupt;
            const n_groups = readU32(bytes[cursor.* .. cursor.* + 4]);
            cursor.* += 4;
            const group_cols = try allocator.alloc([]const u8, n_groups);
            errdefer allocator.free(group_cols);
            for (group_cols) |*c| c.* = try readString(bytes, cursor);

            // aggs
            if (cursor.* + 4 > bytes.len) return Error.IrCorrupt;
            const n_aggs = readU32(bytes[cursor.* .. cursor.* + 4]);
            cursor.* += 4;
            const aggs = try allocator.alloc(AggSpec, n_aggs);
            errdefer allocator.free(aggs);
            for (aggs) |*a| {
                if (cursor.* + 1 > bytes.len) return Error.IrCorrupt;
                const func_byte = bytes[cursor.*];
                cursor.* += 1;
                if (func_byte > @intFromEnum(AggFunc.group_concat)) return Error.IrCorrupt;
                const func: AggFunc = @enumFromInt(func_byte);
                if (cursor.* + 1 > bytes.len) return Error.IrCorrupt;
                const has_col = bytes[cursor.*];
                cursor.* += 1;
                const col: ?[]const u8 = if (has_col != 0) try readString(bytes, cursor) else null;
                const as = try readString(bytes, cursor);
                // AggParams tag: 0=none, 1=percentile (f64 payload),
                // 2=separator (string payload). Older encoders omit
                // it; we treat absence as .none for back-compat with
                // pre-params IR.
                if (cursor.* + 1 > bytes.len) return Error.IrCorrupt;
                const params_tag = bytes[cursor.*];
                cursor.* += 1;
                const params: exec_aggregate.AggParams = switch (params_tag) {
                    0 => .none,
                    1 => blk2: {
                        if (cursor.* + 8 > bytes.len) return Error.IrCorrupt;
                        const bits = std.mem.readInt(u64, bytes[cursor.*..][0..8], .little);
                        cursor.* += 8;
                        break :blk2 .{ .percentile = @as(f64, @bitCast(bits)) };
                    },
                    2 => .{ .separator = try readString(bytes, cursor) },
                    else => return Error.IrCorrupt,
                };
                a.* = .{ .func = func, .col = col, .as = as, .params = params };
            }

            const upstream = try allocator.create(Op);
            errdefer allocator.destroy(upstream);
            upstream.* = try decodeOp(allocator, bytes, cursor);
            break :blk Op{ .group_by = .{
                .group_cols = group_cols,
                .aggs = aggs,
                .upstream = upstream,
            } };
        },
        .compute => blk: {
            if (cursor.* + 4 > bytes.len) return Error.IrCorrupt;
            const n = readU32(bytes[cursor.* .. cursor.* + 4]);
            cursor.* += 4;
            const derived = try allocator.alloc(Derived, n);
            errdefer allocator.free(derived);
            for (derived) |*d| {
                const name = try readString(bytes, cursor);
                const expr = try decodeExpr(allocator, bytes, cursor);
                d.* = .{ .name = name, .expr = expr };
            }
            const upstream = try allocator.create(Op);
            errdefer allocator.destroy(upstream);
            upstream.* = try decodeOp(allocator, bytes, cursor);
            break :blk Op{ .compute = .{ .derived = derived, .upstream = upstream } };
        },
        .join => blk: {
            if (cursor.* + 2 > bytes.len) return Error.IrCorrupt;
            const algo_byte = bytes[cursor.*];
            const jtype_byte = bytes[cursor.* + 1];
            cursor.* += 2;
            if (algo_byte > @intFromEnum(JoinAlgorithm.range_sweep)) return Error.IrCorrupt;
            if (jtype_byte > @intFromEnum(JoinType.full)) return Error.IrCorrupt;
            const algo: JoinAlgorithm = @enumFromInt(algo_byte);
            const jtype: JoinType = @enumFromInt(jtype_byte);

            if (cursor.* + 4 > bytes.len) return Error.IrCorrupt;
            const n_on = readU32(bytes[cursor.* .. cursor.* + 4]);
            cursor.* += 4;
            const on = if (n_on == 0)
                @as([]JoinKeyPair, &.{})
            else
                try allocator.alloc(JoinKeyPair, n_on);
            errdefer if (n_on > 0) allocator.free(on);
            for (on) |*kp| {
                const l = try readString(bytes, cursor);
                const r = try readString(bytes, cursor);
                kp.* = .{ .left = l, .right = r };
            }

            if (cursor.* + 4 > bytes.len) return Error.IrCorrupt;
            const n_ranges = readU32(bytes[cursor.* .. cursor.* + 4]);
            cursor.* += 4;
            const ranges = if (n_ranges == 0)
                @as([]JoinRangePredicate, &.{})
            else
                try allocator.alloc(JoinRangePredicate, n_ranges);
            errdefer if (n_ranges > 0) allocator.free(ranges);
            for (ranges) |*rg| {
                const l = try readString(bytes, cursor);
                const r = try readString(bytes, cursor);
                if (cursor.* + 1 > bytes.len) return Error.IrCorrupt;
                const op_byte = bytes[cursor.*];
                cursor.* += 1;
                if (op_byte > @intFromEnum(PredicateOp.gte)) return Error.IrCorrupt;
                rg.* = .{ .left = l, .right = r, .op = @enumFromInt(op_byte) };
            }

            if (cursor.* + 1 > bytes.len) return Error.IrCorrupt;
            const has_pred = bytes[cursor.*];
            cursor.* += 1;
            const extra_predicate: ?PredicateExpr = if (has_pred != 0)
                try decodePredicate(allocator, bytes, cursor)
            else
                null;
            errdefer if (extra_predicate) |p| freeDecodedPredicate(p, allocator);

            if (cursor.* + 12 > bytes.len) return Error.IrCorrupt;
            const ratio_bits = std.mem.readInt(u32, bytes[cursor.*..][0..4], .little);
            const skew_ratio: f32 = @bitCast(ratio_bits);
            cursor.* += 4;
            const skew_abs = readU32(bytes[cursor.* .. cursor.* + 4]);
            cursor.* += 4;
            const skew_interval = readU32(bytes[cursor.* .. cursor.* + 4]);
            cursor.* += 4;

            const left_up = try allocator.create(Op);
            errdefer allocator.destroy(left_up);
            left_up.* = try decodeOp(allocator, bytes, cursor);
            errdefer left_up.deinitDecoded(allocator);
            const right_up = try allocator.create(Op);
            errdefer allocator.destroy(right_up);
            right_up.* = try decodeOp(allocator, bytes, cursor);

            break :blk Op{ .join = .{
                .algorithm = algo,
                .join_type = jtype,
                .on = on,
                .ranges = ranges,
                .extra_predicate = extra_predicate,
                .skew_ratio_threshold = skew_ratio,
                .skew_absolute_threshold = skew_abs,
                .skew_sample_interval = skew_interval,
                .left = left_up,
                .right = right_up,
            } };
        },
    };
}

/// Decode an Expr tree (mirror of encodeExpr). All strings + sub-slices
/// are allocated into `allocator` and must be released via
/// `freeDecodedExpr`.
pub fn decodeExpr(allocator: Allocator, bytes: []const u8, cursor: *usize) DecodeError!Expr {
    if (cursor.* + 1 > bytes.len) return Error.IrCorrupt;
    const tag_byte = bytes[cursor.*];
    cursor.* += 1;
    if (tag_byte > @intFromEnum(ExprTag.call)) return Error.IrCorrupt;
    const tag: ExprTag = @enumFromInt(tag_byte);
    return switch (tag) {
        .col_ref => Expr{ .col_ref = try readString(bytes, cursor) },
        .lit => Expr{ .lit = try decodeValue(bytes, cursor) },
        .call => blk: {
            const name = try readString(bytes, cursor);
            if (cursor.* + 4 > bytes.len) return Error.IrCorrupt;
            const n = readU32(bytes[cursor.* .. cursor.* + 4]);
            cursor.* += 4;
            const args = try allocator.alloc(Expr, n);
            errdefer allocator.free(args);
            for (args) |*a| a.* = try decodeExpr(allocator, bytes, cursor);
            break :blk Expr{ .call = .{ .fn_name = name, .args = args } };
        },
    };
}

pub fn freeDecodedExpr(e: Expr, allocator: Allocator) void {
    switch (e) {
        .col_ref, .lit => {},
        .call => |c| {
            for (c.args) |child| freeDecodedExpr(child, allocator);
            allocator.free(c.args);
        },
    }
}

pub fn decodePredicate(allocator: Allocator, bytes: []const u8, cursor: *usize) DecodeError!PredicateExpr {
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

pub fn decodeValue(bytes: []const u8, cursor: *usize) DecodeError!Value {
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
        .uuid => blk: {
            if (c + 16 > bytes.len) return Error.IrCorrupt;
            cursor.* = c + 16;
            break :blk Value{ .uuid = std.mem.readInt(u128, bytes[c..][0..16], .little) };
        },
    };
}

pub fn freeDecodedPredicate(expr: PredicateExpr, allocator: Allocator) void {
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

// ---------------------------------------------------------------------------
// EXPLAIN — render an Op tree as indented text. Binary ops (Join) recurse
// into both branches so the output reflects the full plan shape.
//
// Output style: one line per operator, two-space indent per depth level,
// each line "OpName <key=value …>" with the most useful spec fields. The
// goal is debuggability, not a stable machine-readable schema — call
// formats are intentionally lightweight.
// ---------------------------------------------------------------------------

pub fn explain(allocator: Allocator, out: *std.ArrayList(u8), root: Op) !void {
    try explainOp(allocator, out, root, 0);
}

fn explainOp(allocator: Allocator, out: *std.ArrayList(u8), op: Op, depth: usize) !void {
    try writeIndent(allocator, out, depth);
    switch (op) {
        .scan => |s| try writeAll(allocator, out, "Scan ", s.table_name, "\n"),
        .limit => |l| {
            var buf: [48]u8 = undefined;
            const s = try std.fmt.bufPrint(&buf, "Limit n={d}\n", .{l.n});
            try out.appendSlice(allocator, s);
            try explainOp(allocator, out, l.upstream.*, depth + 1);
        },
        .select => |p| {
            try out.appendSlice(allocator, "Select [");
            try writeJoinedNames(allocator, out, p.columns);
            try out.appendSlice(allocator, "]\n");
            try explainOp(allocator, out, p.upstream.*, depth + 1);
        },
        .exclude => |p| {
            try out.appendSlice(allocator, "Exclude [");
            try writeJoinedNames(allocator, out, p.columns);
            try out.appendSlice(allocator, "]\n");
            try explainOp(allocator, out, p.upstream.*, depth + 1);
        },
        .filter => |f| {
            try out.appendSlice(allocator, "Filter (");
            try explainPredicate(allocator, out, f.predicate);
            try out.appendSlice(allocator, ")\n");
            try explainOp(allocator, out, f.upstream.*, depth + 1);
        },
        .order_by => |o| {
            try out.appendSlice(allocator, "OrderBy [");
            for (o.specs, 0..) |s, i| {
                if (i > 0) try out.appendSlice(allocator, ", ");
                try out.appendSlice(allocator, s.col);
                try out.appendSlice(allocator, if (s.desc) " DESC" else " ASC");
            }
            try out.appendSlice(allocator, "]\n");
            try explainOp(allocator, out, o.upstream.*, depth + 1);
        },
        .group_by => |g| {
            try out.appendSlice(allocator, "GroupBy keys=[");
            try writeJoinedNames(allocator, out, g.group_cols);
            try out.appendSlice(allocator, "] aggs=[");
            for (g.aggs, 0..) |a, i| {
                if (i > 0) try out.appendSlice(allocator, ", ");
                try out.appendSlice(allocator, @tagName(a.func));
                try out.append(allocator, '(');
                if (a.col) |c| try out.appendSlice(allocator, c) else try out.append(allocator, '*');
                try out.appendSlice(allocator, ") AS ");
                try out.appendSlice(allocator, a.as);
            }
            try out.appendSlice(allocator, "]\n");
            try explainOp(allocator, out, g.upstream.*, depth + 1);
        },
        .compute => |c| {
            try out.appendSlice(allocator, "Compute [");
            for (c.derived, 0..) |d, i| {
                if (i > 0) try out.appendSlice(allocator, ", ");
                try out.appendSlice(allocator, d.name);
                try out.appendSlice(allocator, " := ");
                try explainExpr(allocator, out, d.expr);
            }
            try out.appendSlice(allocator, "]\n");
            try explainOp(allocator, out, c.upstream.*, depth + 1);
        },
        .join => |j| {
            var buf: [128]u8 = undefined;
            const s = try std.fmt.bufPrint(
                &buf,
                "Join algorithm={s} type={s} on=[",
                .{ @tagName(j.algorithm), @tagName(j.join_type) },
            );
            try out.appendSlice(allocator, s);
            for (j.on, 0..) |kp, i| {
                if (i > 0) try out.appendSlice(allocator, ", ");
                try out.appendSlice(allocator, kp.left);
                try out.append(allocator, '=');
                try out.appendSlice(allocator, kp.right);
            }
            try out.append(allocator, ']');
            if (j.ranges.len > 0) {
                try out.appendSlice(allocator, " ranges=[");
                for (j.ranges, 0..) |rg, i| {
                    if (i > 0) try out.appendSlice(allocator, ", ");
                    try out.appendSlice(allocator, rg.left);
                    try out.append(allocator, ' ');
                    try out.appendSlice(allocator, opSymbol(rg.op));
                    try out.append(allocator, ' ');
                    try out.appendSlice(allocator, rg.right);
                }
                try out.append(allocator, ']');
            }
            if (j.extra_predicate) |pred| {
                try out.appendSlice(allocator, " extra=(");
                try explainPredicate(allocator, out, pred);
                try out.append(allocator, ')');
            }
            try out.append(allocator, '\n');
            // Render LEFT side first (depth + 1), then RIGHT — readers expect
            // a top-down left-to-right reading order on the page.
            try explainOp(allocator, out, j.left.*, depth + 1);
            try explainOp(allocator, out, j.right.*, depth + 1);
        },
    }
}

fn writeIndent(allocator: Allocator, out: *std.ArrayList(u8), depth: usize) !void {
    var i: usize = 0;
    while (i < depth) : (i += 1) try out.appendSlice(allocator, "  ");
}

fn writeAll(allocator: Allocator, out: *std.ArrayList(u8), a: []const u8, b: []const u8, c: []const u8) !void {
    try out.appendSlice(allocator, a);
    try out.appendSlice(allocator, b);
    try out.appendSlice(allocator, c);
}

fn writeJoinedNames(allocator: Allocator, out: *std.ArrayList(u8), names: []const []const u8) !void {
    for (names, 0..) |n, i| {
        if (i > 0) try out.appendSlice(allocator, ", ");
        try out.appendSlice(allocator, n);
    }
}

fn opSymbol(op: PredicateOp) []const u8 {
    return switch (op) {
        .eq => "=",
        .neq => "!=",
        .lt => "<",
        .lte => "<=",
        .gt => ">",
        .gte => ">=",
    };
}

fn explainExpr(allocator: Allocator, out: *std.ArrayList(u8), e: Expr) anyerror!void {
    switch (e) {
        .col_ref => |name| try out.appendSlice(allocator, name),
        .lit => |v| try writeValue(allocator, out, v),
        .call => |c| {
            try out.appendSlice(allocator, c.fn_name);
            try out.append(allocator, '(');
            for (c.args, 0..) |arg, i| {
                if (i > 0) try out.appendSlice(allocator, ", ");
                try explainExpr(allocator, out, arg);
            }
            try out.append(allocator, ')');
        },
    }
}

fn explainPredicate(allocator: Allocator, out: *std.ArrayList(u8), p: PredicateExpr) anyerror!void {
    switch (p) {
        .leaf => |l| {
            try out.appendSlice(allocator, l.col);
            try out.append(allocator, ' ');
            try out.appendSlice(allocator, opSymbol(l.op));
            try out.append(allocator, ' ');
            try writeValue(allocator, out, l.val);
        },
        .is_null => |col| try writeAll(allocator, out, col, " IS NULL", ""),
        .is_not_null => |col| try writeAll(allocator, out, col, " IS NOT NULL", ""),
        .@"and" => |children| try joinPredicates(allocator, out, children, " AND "),
        .@"or" => |children| try joinPredicates(allocator, out, children, " OR "),
        .not => |child| {
            try out.appendSlice(allocator, "NOT (");
            try explainPredicate(allocator, out, child.*);
            try out.append(allocator, ')');
        },
    }
}

fn joinPredicates(allocator: Allocator, out: *std.ArrayList(u8), children: []const PredicateExpr, sep: []const u8) anyerror!void {
    for (children, 0..) |c, i| {
        if (i > 0) try out.appendSlice(allocator, sep);
        try explainPredicate(allocator, out, c);
    }
}

fn writeValue(allocator: Allocator, out: *std.ArrayList(u8), v: Value) anyerror!void {
    var buf: [64]u8 = undefined;
    switch (v) {
        .int => |x| try out.appendSlice(allocator, try std.fmt.bufPrint(&buf, "{d}", .{x})),
        .bigint => |x| try out.appendSlice(allocator, try std.fmt.bufPrint(&buf, "{d}", .{x})),
        .smallint => |x| try out.appendSlice(allocator, try std.fmt.bufPrint(&buf, "{d}", .{x})),
        .tinyint => |x| try out.appendSlice(allocator, try std.fmt.bufPrint(&buf, "{d}", .{x})),
        .largeint => |x| try out.appendSlice(allocator, try std.fmt.bufPrint(&buf, "{d}", .{x})),
        .float => |x| try out.appendSlice(allocator, try std.fmt.bufPrint(&buf, "{d}", .{x})),
        .double => |x| try out.appendSlice(allocator, try std.fmt.bufPrint(&buf, "{d}", .{x})),
        .boolean => |x| try out.appendSlice(allocator, if (x) "true" else "false"),
        .text => |s| {
            try out.append(allocator, '\'');
            try out.appendSlice(allocator, s);
            try out.append(allocator, '\'');
        },
        .date => |x| try out.appendSlice(allocator, try std.fmt.bufPrint(&buf, "date({d})", .{x})),
        .datetime => |x| try out.appendSlice(allocator, try std.fmt.bufPrint(&buf, "datetime({d})", .{x})),
        .decimal64 => |x| try out.appendSlice(allocator, try std.fmt.bufPrint(&buf, "decimal64({d})", .{x})),
        .decimal128 => |x| try out.appendSlice(allocator, try std.fmt.bufPrint(&buf, "decimal128({d})", .{x})),
        .uuid => |x| try out.appendSlice(allocator, try std.fmt.bufPrint(&buf, "uuid({d})", .{x})),
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

test "ir: compute round-trips with a call expr over a col_ref" {
    const allocator = std.testing.allocator;

    var scan_storage: Op = .{ .scan = .{ .table_name = "users" } };
    const arg = Expr{ .col_ref = "name" };
    const args = [_]Expr{arg};
    const expr_call = Expr{ .call = .{ .fn_name = "upper", .args = &args } };
    const derived = [_]Derived{.{ .name = "name_upper", .expr = expr_call }};
    const root: Op = .{ .compute = .{ .derived = &derived, .upstream = &scan_storage } };

    var buf: std.ArrayList(u8) = .empty;
    defer buf.deinit(allocator);
    try encode(allocator, &buf, root);

    var decoded = try decode(allocator, buf.items);
    defer decoded.deinitDecoded(allocator);

    try std.testing.expect(decoded == .compute);
    try std.testing.expectEqual(@as(usize, 1), decoded.compute.derived.len);
    try std.testing.expectEqualStrings("name_upper", decoded.compute.derived[0].name);
    try std.testing.expect(decoded.compute.derived[0].expr == .call);
    try std.testing.expectEqualStrings("upper", decoded.compute.derived[0].expr.call.fn_name);
    try std.testing.expectEqual(@as(usize, 1), decoded.compute.derived[0].expr.call.args.len);
    try std.testing.expect(decoded.compute.derived[0].expr.call.args[0] == .col_ref);
    try std.testing.expectEqualStrings("name", decoded.compute.derived[0].expr.call.args[0].col_ref);
}

test "ir: join round-trips with on + range + extra_predicate + skew" {
    const allocator = std.testing.allocator;

    var left_scan: Op = .{ .scan = .{ .table_name = "orders" } };
    var right_scan: Op = .{ .scan = .{ .table_name = "items" } };
    const on_pairs = [_]JoinKeyPair{.{ .left = "item_id", .right = "id" }};
    const ranges = [_]JoinRangePredicate{.{ .left = "qty", .right = "min_qty", .op = .gte }};
    // Post-join filter on a column that exists in the joined output.
    const extra = PredicateExpr{ .leaf = .{ .col = "price", .op = .lt, .val = .{ .double = 100.0 } } };
    const root: Op = .{ .join = .{
        .algorithm = .auto,
        .join_type = .inner,
        .on = &on_pairs,
        .ranges = &ranges,
        .extra_predicate = extra,
        .skew_ratio_threshold = 0.5,
        .skew_absolute_threshold = 50_000,
        .skew_sample_interval = 10,
        .left = &left_scan,
        .right = &right_scan,
    } };

    var buf: std.ArrayList(u8) = .empty;
    defer buf.deinit(allocator);
    try encode(allocator, &buf, root);

    var decoded = try decode(allocator, buf.items);
    defer decoded.deinitDecoded(allocator);

    try std.testing.expect(decoded == .join);
    try std.testing.expectEqual(JoinAlgorithm.auto, decoded.join.algorithm);
    try std.testing.expectEqual(JoinType.inner, decoded.join.join_type);
    try std.testing.expectEqual(@as(usize, 1), decoded.join.on.len);
    try std.testing.expectEqualStrings("item_id", decoded.join.on[0].left);
    try std.testing.expectEqualStrings("id", decoded.join.on[0].right);
    try std.testing.expectEqual(@as(usize, 1), decoded.join.ranges.len);
    try std.testing.expectEqual(PredicateOp.gte, decoded.join.ranges[0].op);
    try std.testing.expect(decoded.join.extra_predicate != null);
    try std.testing.expectEqual(@as(f32, 0.5), decoded.join.skew_ratio_threshold);
    try std.testing.expectEqual(@as(u32, 50_000), decoded.join.skew_absolute_threshold);
    try std.testing.expect(decoded.join.left.* == .scan);
    try std.testing.expectEqualStrings("orders", decoded.join.left.scan.table_name);
    try std.testing.expect(decoded.join.right.* == .scan);
    try std.testing.expectEqualStrings("items", decoded.join.right.scan.table_name);
}

test "ir: join with no on/ranges and no extra_predicate (pure-NLJ shape)" {
    const allocator = std.testing.allocator;

    var left_scan: Op = .{ .scan = .{ .table_name = "a" } };
    var right_scan: Op = .{ .scan = .{ .table_name = "b" } };
    const root: Op = .{ .join = .{
        .algorithm = .nested_loop,
        .join_type = .inner,
        .on = &.{},
        .ranges = &.{},
        .extra_predicate = null,
        .skew_ratio_threshold = 0.0,
        .skew_absolute_threshold = 0,
        .skew_sample_interval = 10,
        .left = &left_scan,
        .right = &right_scan,
    } };

    var buf: std.ArrayList(u8) = .empty;
    defer buf.deinit(allocator);
    try encode(allocator, &buf, root);

    var decoded = try decode(allocator, buf.items);
    defer decoded.deinitDecoded(allocator);

    try std.testing.expect(decoded == .join);
    try std.testing.expectEqual(JoinAlgorithm.nested_loop, decoded.join.algorithm);
    try std.testing.expectEqual(@as(usize, 0), decoded.join.on.len);
    try std.testing.expectEqual(@as(usize, 0), decoded.join.ranges.len);
    try std.testing.expect(decoded.join.extra_predicate == null);
}
