//! Window-function parsing — extracted from parser.zig for readability.
//!
//! Layout:
//!   - `ParsedWindowCall` type (referenced from `parser.ProjItem.kind.window`)
//!   - Free helpers for spec equality, validation, and cloning
//!   - Free parser fns (taking `anytype` for the Parser pointer to avoid
//!     a circular import with parser.zig — the only argument any caller
//!     ever passes is `*parser.Parser`)
//!
//! `buildWindowOp` stays in parser.zig because it consumes `ProjItem`,
//! which is the natural place for it; everything else lives here so the
//! window grammar is one file you can read top-to-bottom.

const std = @import("std");
const Allocator = std.mem.Allocator;

const ir = @import("../ir/ir.zig");
const SortSpec = @import("../exec/sort.zig").SortSpec;

/// Inline-spec representation of a window call collected from the SELECT
/// projection. The spec is either inline (`OVER (...)`) or a named-window
/// reference (`OVER name`). Named references resolve at the IR-build step
/// after the WINDOW clause has been parsed; that's what lets a query use
/// the standard placement (WINDOW after GROUP BY, before ORDER BY) and
/// still have earlier OVER references find the spec.
pub const ParsedWindowCall = struct {
    func: ir.WindowFunc,
    args: []const ir.Expr,
    ignore_nulls: bool,
    spec_kind: SpecKind,
};

pub const SpecKind = union(enum) {
    inline_spec: ir.WindowSpec,
    named: []const u8,
};

// ---------------------------------------------------------------------------
// Module-level helpers (no Parser dep)
// ---------------------------------------------------------------------------

/// True for window functions whose name is also a regular aggregate
/// (e.g. `sum`, `avg`, `count`). When OVER is absent the parser falls
/// through to the aggregate-call path; with OVER they become aggregate
/// windows.
pub fn isAggregateAlsoFunc(f: ir.WindowFunc) bool {
    return switch (f) {
        .sum, .avg, .count, .min, .max => true,
        else => false,
    };
}

/// Per-function argument-shape validation. The operator (Phase 2) needs
/// args in a well-known shape per function, so the parser rejects
/// nonsensical calls (e.g. `RANK(x)`, `LAG()` with no args). Generic
/// error so callers can map it to their own ParseError variant.
pub fn validateWindowCall(
    func: ir.WindowFunc,
    args: []const ir.Expr,
    ignore_nulls: bool,
) error{InvalidShape}!void {
    switch (func) {
        // Ranking — zero args. IGNORE NULLS not meaningful.
        .row_number, .rank, .dense_rank => {
            if (args.len != 0) return error.InvalidShape;
            if (ignore_nulls) return error.InvalidShape;
        },
        // Value access. IGNORE NULLS is meaningful and honored by the
        // operator.
        .lag, .lead => {
            if (args.len < 1 or args.len > 3) return error.InvalidShape;
        },
        .first_value, .last_value => {
            if (args.len != 1) return error.InvalidShape;
        },
        .nth_value => {
            if (args.len != 2) return error.InvalidShape;
        },
        // Aggregate windows — one column-ref arg.
        .sum, .avg, .count, .min, .max => {
            if (args.len != 1) return error.InvalidShape;
            if (ignore_nulls) return error.InvalidShape;
        },
        // Tier 2 ranking-style — take one integer-literal arg (NTILE)
        // or no args (PERCENT_RANK, CUME_DIST). IGNORE NULLS isn't
        // meaningful for any of these.
        .ntile => {
            if (args.len != 1) return error.InvalidShape;
            if (ignore_nulls) return error.InvalidShape;
        },
        .cume_dist, .percent_rank => {
            if (args.len != 0) return error.InvalidShape;
            if (ignore_nulls) return error.InvalidShape;
        },
    }
}

/// Deep-copy a window spec into `arena`. Used when a function references
/// a named window — every call needs its own copy so the deduplication
/// pass can mutate / compare cleanly without aliasing the dictionary's
/// stored entry.
pub fn cloneWindowSpec(arena: Allocator, src: ir.WindowSpec) !ir.WindowSpec {
    const pb = try arena.alloc([]const u8, src.partition_by.len);
    for (src.partition_by, pb) |s, *dst| dst.* = try arena.dupe(u8, s);
    const ob = try arena.alloc(SortSpec, src.order_by.len);
    for (src.order_by, ob) |s, *dst| {
        dst.* = .{ .col = try arena.dupe(u8, s.col), .desc = s.desc };
    }
    return .{ .partition_by = pb, .order_by = ob, .frame = src.frame };
}

/// Structural equality on two window specs — used to dedupe specs
/// across all window calls in a single SELECT so the eventual operator
/// sorts once per spec and runs all of that spec's calls in one pass.
pub fn windowSpecsEqual(a: ir.WindowSpec, b: ir.WindowSpec) bool {
    if (a.partition_by.len != b.partition_by.len) return false;
    if (a.order_by.len != b.order_by.len) return false;
    for (a.partition_by, b.partition_by) |x, y|
        if (!std.mem.eql(u8, x, y)) return false;
    for (a.order_by, b.order_by) |x, y| {
        if (!std.mem.eql(u8, x.col, y.col)) return false;
        if (x.desc != y.desc) return false;
    }
    if (a.frame.kind != b.frame.kind) return false;
    if (!frameBoundsEqual(a.frame.start, b.frame.start)) return false;
    if (!frameBoundsEqual(a.frame.end, b.frame.end)) return false;
    return true;
}

pub fn frameBoundsEqual(a: ir.FrameBound, b: ir.FrameBound) bool {
    if (std.meta.activeTag(a) != std.meta.activeTag(b)) return false;
    return switch (a) {
        .unbounded_preceding, .current_row, .unbounded_following => true,
        .preceding => |n| n == b.preceding,
        .following => |n| n == b.following,
    };
}

// ---------------------------------------------------------------------------
// Parser-driven free functions
//
// All take the Parser via `anytype` to avoid a circular import with
// parser.zig. Each one is the natural extraction of a method that
// previously lived on the Parser struct.
// ---------------------------------------------------------------------------

/// Consume an optional `IGNORE NULLS` / `RESPECT NULLS` modifier
/// between a function's closing `)` and its `OVER` keyword. Returns
/// `true` only for `IGNORE NULLS`. `RESPECT NULLS` is consumed and
/// ignored — it's the SQL-standard default and the operator behaves
/// the same either way.
pub fn parseIgnoreNulls(p: anytype) !bool {
    const PE = @TypeOf(p.*).Err;
    if (p.cur.tag == .kw_ignore) {
        try p.advance();
        if (p.cur.tag != .kw_nulls) return PE.SqlExpectedKeyword;
        try p.advance();
        return true;
    }
    if (p.cur.tag == .kw_respect) {
        try p.advance();
        if (p.cur.tag != .kw_nulls) return PE.SqlExpectedKeyword;
        try p.advance();
        return false;
    }
    return false;
}

/// Parse what follows `OVER`. Two forms:
///   - `OVER name` — reference to a window declared in the trailing
///     `WINDOW name AS (...)` clause. Resolution is deferred to
///     `buildWindowOp` so the standard placement (WINDOW after GROUP
///     BY, before ORDER BY) works.
///   - `OVER (...)` — inline spec.
pub fn parseWindowSpecOrRef(p: anytype) !SpecKind {
    if (p.cur.tag == .identifier) {
        const name = try p.arena.dupe(u8, p.cur.text);
        try p.advance();
        return .{ .named = name };
    }
    const spec = try parseWindowSpec(p);
    return .{ .inline_spec = spec };
}

/// Parse a parenthesized window spec: `(PARTITION BY ... ORDER BY ...
/// [ROWS|RANGE|GROUPS BETWEEN ... AND ...])`. All three sub-clauses
/// are optional; absence yields the SQL-standard defaults (no
/// partitioning, no ordering, frame chosen by whether ORDER BY is
/// present).
pub fn parseWindowSpec(p: anytype) !ir.WindowSpec {
    try p.expect(.lparen);

    var partition_by: []const []const u8 = &.{};
    if (p.cur.tag == .kw_partition) {
        try p.advance();
        try p.expect(.kw_by);
        partition_by = try parsePartitionByList(p);
    }

    var order_by: []const SortSpec = &.{};
    if (p.cur.tag == .kw_order) {
        try p.advance();
        try p.expect(.kw_by);
        // Window ORDER BY references columns, not projection outputs, so
        // no projection context is needed for scalar-expr binding.
        order_by = try p.parseOrderBy(&.{});
    }

    var frame: ir.Frame = if (order_by.len == 0)
        ir.Frame.default_no_order
    else
        ir.Frame.default_with_order;

    if (p.cur.tag == .kw_rows or
        p.cur.tag == .kw_range or
        p.cur.tag == .kw_groups)
    {
        frame = try parseFrameClause(p);
    }

    try p.expect(.rparen);
    return .{ .partition_by = partition_by, .order_by = order_by, .frame = frame };
}

fn parsePartitionByList(p: anytype) ![]const []const u8 {
    var items: std.ArrayList([]const u8) = .empty;
    defer items.deinit(p.arena);

    while (true) {
        const expr = try p.parseAddSub();
        const name = switch (expr) {
            .col_ref => |c| c,
            else => try p.materializeWindowPartitionExpr(expr),
        };
        try items.append(p.arena, name);
        if (p.cur.tag != .comma) break;
        try p.advance();
    }
    return try items.toOwnedSlice(p.arena);
}

/// Parse `ROWS|RANGE|GROUPS [BETWEEN start AND end | bound]`. Without
/// `BETWEEN`, the parsed bound becomes the `start` and the end
/// defaults to `CURRENT ROW`.
pub fn parseFrameClause(p: anytype) !ir.Frame {
    const PE = @TypeOf(p.*).Err;
    const kind: ir.FrameKind = switch (p.cur.tag) {
        .kw_rows => .rows,
        .kw_range => .range,
        .kw_groups => .groups,
        else => return PE.SqlExpectedKeyword,
    };
    try p.advance();

    if (p.cur.tag == .kw_between) {
        try p.advance();
        const start = try parseFrameBound(p);
        try p.expect(.kw_and);
        const end = try parseFrameBound(p);
        return .{ .kind = kind, .start = start, .end = end };
    }

    const start = try parseFrameBound(p);
    return .{ .kind = kind, .start = start, .end = .current_row };
}

/// Parse a single frame bound. Accepted forms:
///   UNBOUNDED PRECEDING
///   UNBOUNDED FOLLOWING
///   N PRECEDING
///   N FOLLOWING
///   CURRENT ROW
pub fn parseFrameBound(p: anytype) !ir.FrameBound {
    const PE = @TypeOf(p.*).Err;
    if (p.cur.tag == .kw_unbounded) {
        try p.advance();
        if (p.cur.tag == .kw_preceding) {
            try p.advance();
            return .unbounded_preceding;
        }
        if (p.cur.tag == .kw_following) {
            try p.advance();
            return .unbounded_following;
        }
        return PE.SqlExpectedKeyword;
    }
    if (p.cur.tag == .kw_current) {
        try p.advance();
        if (p.cur.tag != .kw_row) return PE.SqlExpectedKeyword;
        try p.advance();
        return .current_row;
    }
    if (p.cur.tag == .integer) {
        const n = p.cur.value.integer;
        if (n < 0) return PE.SqlExpectedValue;
        try p.advance();
        if (p.cur.tag == .kw_preceding) {
            try p.advance();
            return ir.FrameBound{ .preceding = @intCast(n) };
        }
        if (p.cur.tag == .kw_following) {
            try p.advance();
            return ir.FrameBound{ .following = @intCast(n) };
        }
        return PE.SqlExpectedKeyword;
    }
    return PE.SqlExpectedKeyword;
}

/// Parse the trailing `WINDOW name AS (spec), name AS (spec), ...`
/// clause. Populates `p.named_windows`; called after GROUP BY / HAVING
/// and before ORDER BY per the SQL standard. Each spec is fully
/// resolved at definition time (no forward references).
pub fn parseWindowClause(p: anytype) !void {
    const PE = @TypeOf(p.*).Err;
    try p.advance(); // consume WINDOW
    while (true) {
        if (p.cur.tag != .identifier) return PE.SqlExpectedIdent;
        const name = try p.arena.dupe(u8, p.cur.text);
        try p.advance();
        if (p.cur.tag != .kw_as) return PE.SqlExpectedKeyword;
        try p.advance();
        const spec = try parseWindowSpec(p);
        const existing = try p.named_windows.fetchPut(p.arena, name, spec);
        if (existing != null) return PE.SqlInvalidProjection;
        if (p.cur.tag != .comma) break;
        try p.advance();
    }
}

/// Build the default output name for a window call. Display only —
/// wire layers and downstream operators reference by alias.
pub fn defaultName(arena: Allocator, func_name: []const u8, args: []const ir.Expr) ![]const u8 {
    var buf: std.ArrayList(u8) = .empty;
    defer buf.deinit(arena);
    try buf.appendSlice(arena, func_name);
    try buf.append(arena, '(');
    for (args, 0..) |a, i| {
        if (i > 0) try buf.appendSlice(arena, ", ");
        switch (a) {
            .col_ref => |c| try buf.appendSlice(arena, c),
            .lit => try buf.appendSlice(arena, "?"),
            .null_lit => try buf.appendSlice(arena, "NULL"),
            .call => |c| {
                try buf.appendSlice(arena, c.fn_name);
                try buf.appendSlice(arena, "(...)");
            },
            .case => try buf.appendSlice(arena, "case"),
            .scalar_subquery => try buf.appendSlice(arena, "subquery"),
            .exists_subquery => try buf.appendSlice(arena, "exists"),
            .var_ref => |name| {
                try buf.append(arena, '@');
                try buf.appendSlice(arena, name);
            },
        }
    }
    try buf.append(arena, ')');
    return try buf.toOwnedSlice(arena);
}
