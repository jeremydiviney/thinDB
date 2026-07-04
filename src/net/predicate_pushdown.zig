//! Pre-execution predicate pushdown for joins.
//!
//! Rewrites `Filter(pred, Join(L, R))` by moving each top-level WHERE conjunct
//! that references columns from exactly ONE preserved join input down onto that
//! input, so the source is narrowed BEFORE the join runs. A conjunct that
//! empties a side turns the whole join into a no-op; one that merely narrows it
//! shrinks the build/probe input.
//!
//! This is a plan rewrite (allowed — see DESIGN.md "no RUNTIME optimization"),
//! run once after subquery resolution and before any handler compiles the tree,
//! so every execution path (generic, silo, staged-CTE, …) benefits uniformly.
//!
//! Safety — thinDB has no runtime reordering, so correctness is everything:
//!   - Columns resolve by SUFFIX (last dotted segment — see `types.findColumn`).
//!     A conjunct pushes to a side only when every one of its column suffixes
//!     appears in that side's EXACT output-column set AND in NEITHER the other
//!     side's set. Anything we can't pin to one side stays above the join.
//!   - Each side's column set is inferred exactly from the IR (projections,
//!     computes, windows, group-bys, unions) down to base scans (catalog). If
//!     any node can't be enumerated exactly (star projection, file scan, …) the
//!     side is treated as opaque and nothing crosses it.
//!   - Only PRESERVED-side predicates push: either side of INNER, the left of
//!     LEFT, the right of RIGHT. Pushing a nullable-side WHERE predicate under
//!     an outer join would change its semantics, so we never do.
//!   - Conjuncts containing a subquery / correlated node are left in place.

const std = @import("std");
const Allocator = std.mem.Allocator;

const ir = @import("../ir/ir.zig");
const types = @import("../types.zig");
const PredicateExpr = @import("../exec/predicate.zig").PredicateExpr;
const local = @import("local.zig");
const api = @import("../api/api.zig");

const Ctx = struct {
    arena: Allocator,
    catalog: ?*api.Catalog,
    session: api.Session,
};

var trace_push: bool = false;

extern "c" fn getenv(name: [*:0]const u8) ?[*:0]const u8;

/// Rewrite the whole op tree in place. `arena` owns any new Filter nodes and
/// predicate slices (the compile-time node arena).
pub fn pushJoinFilters(arena: Allocator, catalog: ?*api.Catalog, session: api.Session, op: *ir.Op) anyerror!void {
    trace_push = getenv("THINDB_TRACE_PUSHDOWN") != null;
    const ctx = Ctx{ .arena = arena, .catalog = catalog, .session = session };
    try walk(ctx, op);
}

fn walk(ctx: Ctx, op: *ir.Op) anyerror!void {
    // Bottom-up: optimize children first, then try to push at this node.
    switch (op.*) {
        .scan, .single_row, .file_scan, .ddl, .show, .insert, .copy, .set_var, .delete_op, .update_op => {},
        .limit => |l| try walk(ctx, @constCast(l.upstream)),
        .select, .exclude => |p| try walk(ctx, @constCast(p.upstream)),
        .order_by => |o| try walk(ctx, @constCast(o.upstream)),
        .group_by => |g| try walk(ctx, @constCast(g.upstream)),
        .compute => |c| try walk(ctx, @constCast(c.upstream)),
        .materialize => |m| try walk(ctx, @constCast(m.upstream)),
        .window => |w| try walk(ctx, @constCast(w.upstream)),
        .alias => |a| try walk(ctx, @constCast(a.upstream)),
        .explain => |e| try walk(ctx, e.inner),
        .create_table_as => |c| try walk(ctx, @constCast(c.source)),
        .insert_select => |i| try walk(ctx, @constCast(i.source)),
        .batch => |b| for (b.statements) |s| try walk(ctx, @constCast(s)),
        .set_union => |u| {
            try walk(ctx, @constCast(u.left));
            try walk(ctx, @constCast(u.right));
        },
        .join => |j| {
            try walk(ctx, @constCast(j.left));
            try walk(ctx, @constCast(j.right));
        },
        .filter => |f| {
            try walk(ctx, @constCast(f.upstream));
            if (trace_push) std.debug.print("[ppd] filter over {s}\n", .{@tagName(f.upstream.*)});
            try sinkThroughShapers(ctx, op);
            if (op.* == .filter and op.filter.upstream.* == .join) try pushFilterIntoJoin(ctx, op);
        },
    }
}

/// Sink filter conjuncts through row-preserving shaper nodes so they land
/// directly above the join/scan they belong to — `filter(compute(join))`
/// becomes `compute(filter(join))` and the join push then fires. A conjunct
/// may cross a `.compute` only when it references none of the derived names
/// (below the compute it would read the pre-compute value of a shadowed
/// column). It may cross an `.exclude` only when its columns are disjoint
/// from the excluded names (below the exclude a dropped qualified column
/// could make a suffix reference ambiguous). Rewrites `op` in place, then
/// cascades on the sunk filter.
fn sinkThroughShapers(ctx: Ctx, op: *ir.Op) anyerror!void {
    if (op.* != .filter) return;
    const up = @constCast(op.filter.upstream);
    switch (up.*) {
        .compute, .exclude => {},
        else => return,
    }

    var movable: std.ArrayListUnmanaged(PredicateExpr) = .empty;
    var stay: std.ArrayListUnmanaged(PredicateExpr) = .empty;

    const conjuncts = try splitConjuncts(ctx.arena, op.filter.predicate);
    for (conjuncts) |c| {
        if (conjunctCrossesShaper(ctx.arena, c, up)) {
            try movable.append(ctx.arena, c);
        } else {
            try stay.append(ctx.arena, c);
        }
    }
    if (movable.items.len == 0) return;
    if (trace_push) std.debug.print("[ppd]   sink through {s}: moved={d} stayed={d}\n", .{ @tagName(up.*), movable.items.len, stay.items.len });

    const nf = try ctx.arena.create(ir.Op);
    nf.* = .{ .filter = .{
        .predicate = try combine(ctx.arena, movable.items),
        .upstream = switch (up.*) {
            .compute => |c| c.upstream,
            .exclude => |p| p.upstream,
            else => unreachable,
        },
    } };
    switch (up.*) {
        .compute => up.compute.upstream = nf,
        .exclude => up.exclude.upstream = nf,
        else => unreachable,
    }
    if (stay.items.len > 0) {
        op.filter.predicate = try combine(ctx.arena, stay.items);
    } else {
        // Every conjunct sank — the parent filter dissolves into the shaper.
        op.* = up.*;
    }
    try sinkThroughShapers(ctx, nf);
    if (nf.* == .filter and nf.filter.upstream.* == .join) try pushFilterIntoJoin(ctx, nf);
}

fn conjunctCrossesShaper(arena: Allocator, c: PredicateExpr, shaper: *const ir.Op) bool {
    var cols: std.ArrayListUnmanaged([]const u8) = .empty;
    if (!collectPredCols(arena, c, &cols)) return false;
    if (cols.items.len == 0) return false; // constant — nothing gained by moving
    const blocked: []const []const u8 = switch (shaper.*) {
        .compute => |cp| blk: {
            var names: std.ArrayListUnmanaged([]const u8) = .empty;
            for (cp.derived) |d| names.append(arena, d.name) catch return false;
            break :blk names.items;
        },
        .exclude => |p| p.columns,
        else => return false,
    };
    for (cols.items) |col| {
        const s = suffix(col);
        for (blocked) |b| {
            if (types.columnNameEql(suffix(b), s)) return false;
        }
    }
    return true;
}

fn leftPreserved(jt: ir.JoinType) bool {
    return jt == .inner or jt == .left;
}
fn rightPreserved(jt: ir.JoinType) bool {
    return jt == .inner or jt == .right;
}

/// `op.* == .filter` and its upstream is a `.join`. Split the predicate and
/// relocate single-side conjuncts onto the matching preserved side.
fn pushFilterIntoJoin(ctx: Ctx, op: *ir.Op) anyerror!void {
    const join_op = @constCast(op.filter.upstream);
    const jt = join_op.join.join_type;

    var left_cols: std.ArrayListUnmanaged([]const u8) = .empty;
    var right_cols: std.ArrayListUnmanaged([]const u8) = .empty;
    const left_ok = collectColumns(ctx, join_op.join.left, &left_cols) catch false;
    const right_ok = collectColumns(ctx, join_op.join.right, &right_cols) catch false;
    if (trace_push) std.debug.print("[ppd]   join jt={s} left_ok={} ({d} cols, child={s}) right_ok={} ({d} cols, child={s})\n", .{
        @tagName(jt), left_ok, left_cols.items.len, @tagName(join_op.join.left.*), right_ok, right_cols.items.len, @tagName(join_op.join.right.*),
    });
    if (!left_ok and !right_ok) return; // can't reason about either side

    var to_left: std.ArrayListUnmanaged(PredicateExpr) = .empty;
    var to_right: std.ArrayListUnmanaged(PredicateExpr) = .empty;
    var stay: std.ArrayListUnmanaged(PredicateExpr) = .empty;

    const conjuncts = try splitConjuncts(ctx.arena, op.filter.predicate);
    for (conjuncts) |c| {
        const side = classify(ctx.arena, c, left_cols.items, right_cols.items, left_ok, right_ok, jt);
        if (trace_push) std.debug.print("[ppd]   conjunct tag={s} -> {s}\n", .{ @tagName(c), @tagName(side) });
        switch (side) {
            .left => try to_left.append(ctx.arena, c),
            .right => try to_right.append(ctx.arena, c),
            .stay => try stay.append(ctx.arena, c),
        }
    }

    if (to_left.items.len == 0 and to_right.items.len == 0) return; // nothing moved

    if (to_left.items.len > 0) {
        const nf = try ctx.arena.create(ir.Op);
        nf.* = .{ .filter = .{ .predicate = try combine(ctx.arena, to_left.items), .upstream = join_op.join.left } };
        join_op.join.left = nf;
        try walk(ctx, nf); // cascade deeper if that side is itself a join
    }
    if (to_right.items.len > 0) {
        const nf = try ctx.arena.create(ir.Op);
        nf.* = .{ .filter = .{ .predicate = try combine(ctx.arena, to_right.items), .upstream = join_op.join.right } };
        join_op.join.right = nf;
        try walk(ctx, nf);
    }

    if (stay.items.len > 0) {
        op.* = .{ .filter = .{ .predicate = try combine(ctx.arena, stay.items), .upstream = join_op } };
    } else {
        // Every conjunct moved down — the parent filter dissolves into the join.
        op.* = join_op.*;
    }
}

const Side = enum { left, right, stay };

fn classify(
    arena: Allocator,
    pred: PredicateExpr,
    left_cols: []const []const u8,
    right_cols: []const []const u8,
    left_ok: bool,
    right_ok: bool,
    jt: ir.JoinType,
) Side {
    var cols: std.ArrayListUnmanaged([]const u8) = .empty;
    if (!collectPredCols(arena, pred, &cols)) return .stay; // subquery / unpushable
    if (cols.items.len == 0) return .stay; // constant predicate

    // A conjunct may push to a side only when EVERY column suffix lives in that
    // side's set and NONE lives in the other — exact, both sides enumerable.
    if (!left_ok or !right_ok) return .stay;
    var all_left = true;
    var all_right = true;
    for (cols.items) |col| {
        const s = suffix(col);
        const in_l = contains(left_cols, s);
        const in_r = contains(right_cols, s);
        if (!in_l or in_r) all_left = false;
        if (!in_r or in_l) all_right = false;
    }
    if (all_left and leftPreserved(jt)) return .left;
    if (all_right and rightPreserved(jt)) return .right;
    return .stay;
}

/// Append the EXACT set of output column suffixes of `op`. Returns false the
/// moment any node can't be enumerated exactly — the caller then treats the
/// side as opaque (never a partial set, which would make the cross-side
/// exclusion test unsound).
fn collectColumns(ctx: Ctx, op: *const ir.Op, out: *std.ArrayListUnmanaged([]const u8)) anyerror!bool {
    switch (op.*) {
        .scan => |s| {
            const cat = ctx.catalog orelse return false;
            const t = local.resolveTable(cat, ctx.session, s.table) catch return false;
            for (t.schema.columns) |col| try out.append(ctx.arena, suffix(col.name));
            return true;
        },
        .alias => |a| return collectColumns(ctx, a.upstream, out),
        .materialize => |m| return collectColumns(ctx, m.upstream, out),
        .limit => |l| return collectColumns(ctx, l.upstream, out),
        .order_by => |o| return collectColumns(ctx, o.upstream, out),
        .filter => |f| return collectColumns(ctx, f.upstream, out),
        .select => |p| {
            for (p.columns, 0..) |nm, i| {
                if (isStar(nm)) return false; // can't enumerate without the upstream schema
                const out_name = if (p.outputs) |o| (if (i < o.len) (o[i] orelse nm) else nm) else nm;
                try out.append(ctx.arena, suffix(out_name));
            }
            return true;
        },
        .exclude => |p| {
            if (!try collectColumns(ctx, p.upstream, out)) return false;
            for (p.columns) |nm| removeSuffix(out, suffix(nm));
            return true;
        },
        .compute => |c| {
            if (!try collectColumns(ctx, c.upstream, out)) return false;
            for (c.derived) |d| try out.append(ctx.arena, suffix(d.name));
            return true;
        },
        .window => |w| {
            if (!try collectColumns(ctx, w.upstream, out)) return false;
            for (w.calls) |call| try out.append(ctx.arena, suffix(call.output_name));
            return true;
        },
        .group_by => |g| {
            for (g.group_cols) |nm| try out.append(ctx.arena, suffix(nm));
            for (g.aggs) |a| try out.append(ctx.arena, suffix(a.as));
            return true;
        },
        // UNION output column names come from the left arm (both arms agree on
        // arity; see set_union.zig).
        .set_union => |u| return collectColumns(ctx, u.left, out),
        .join => |j| {
            const l = try collectColumns(ctx, j.left, out);
            const r = try collectColumns(ctx, j.right, out);
            return l and r;
        },
        // single_row / file_scan / statement ops: not safely enumerable here.
        else => return false,
    }
}

fn splitConjuncts(arena: Allocator, pred: PredicateExpr) ![]const PredicateExpr {
    return switch (pred) {
        .@"and" => |kids| kids,
        else => try arena.dupe(PredicateExpr, &[_]PredicateExpr{pred}),
    };
}

fn combine(arena: Allocator, conjuncts: []const PredicateExpr) !PredicateExpr {
    if (conjuncts.len == 1) return conjuncts[0];
    return .{ .@"and" = try arena.dupe(PredicateExpr, conjuncts) };
}

/// Last dotted segment of `name` — the form `types.findColumn` matches on.
fn suffix(name: []const u8) []const u8 {
    if (std.mem.lastIndexOfScalar(u8, name, '.')) |dot| return name[dot + 1 ..];
    return name;
}

fn isStar(name: []const u8) bool {
    return std.mem.eql(u8, name, "*") or std.mem.endsWith(u8, name, ".*");
}

fn contains(names: []const []const u8, s: []const u8) bool {
    for (names) |n| if (types.columnNameEql(n, s)) return true;
    return false;
}

fn removeSuffix(out: *std.ArrayListUnmanaged([]const u8), s: []const u8) void {
    var i: usize = 0;
    while (i < out.items.len) {
        if (types.columnNameEql(out.items[i], s)) {
            _ = out.swapRemove(i);
        } else i += 1;
    }
}

/// Append every column referenced by `pred`. Returns false if the predicate
/// holds a subquery / correlated node (never safe to relocate blindly).
fn collectPredCols(arena: Allocator, pred: PredicateExpr, out: *std.ArrayListUnmanaged([]const u8)) bool {
    switch (pred) {
        .leaf => |l| out.append(arena, l.col) catch return false,
        .leaf_col_col => |c| {
            out.append(arena, c.left) catch return false;
            out.append(arena, c.right) catch return false;
        },
        .day_leaf => |l| out.append(arena, l.col) catch return false,
        .is_null, .is_not_null => |name| out.append(arena, name) catch return false,
        .like => |lk| out.append(arena, lk.col) catch return false,
        .in_set => |s| out.append(arena, s.col) catch return false,
        .leaf_var => |v| out.append(arena, v.col) catch return false,
        .always, .unknown => {},
        .@"and", .@"or" => |kids| for (kids) |k| {
            if (!collectPredCols(arena, k, out)) return false;
        },
        .not => |child| return collectPredCols(arena, child.*, out),
        .scalar_subquery, .exists_subquery, .in_subquery, .correlated_set, .correlated_scalar, .correlated_range => return false,
    }
    return true;
}

const testing = std.testing;

fn testSelect(cols: []const []const u8, upstream: *ir.Op) ir.Op {
    return .{ .select = .{ .columns = cols, .upstream = upstream } };
}

fn testJoin(jt: ir.JoinType, left: *ir.Op, right: *ir.Op) ir.Op {
    return .{ .join = .{
        .algorithm = .auto,
        .join_type = jt,
        .on = &.{},
        .ranges = &.{},
        .extra_predicate = null,
        .skew_ratio_threshold = 0,
        .skew_absolute_threshold = 0,
        .skew_sample_interval = 0,
        .left = left,
        .right = right,
    } };
}

test "predicate pushdown: single-side WHERE relocates below an inner join" {
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();

    var dummy: ir.Op = .single_row;
    var left = testSelect(&.{ "l.a", "l.b" }, &dummy);
    var right = testSelect(&.{ "r.c", "r.d" }, &dummy);
    var join = testJoin(.inner, &left, &right);
    var op = ir.Op{ .filter = .{ .predicate = .{ .is_not_null = "l.a" }, .upstream = &join } };

    try pushJoinFilters(a, null, .{}, &op);

    // Every conjunct moved down, so the parent filter dissolves into the join.
    try testing.expect(op == .join);
    // The left input is now wrapped in the pushed filter; the right is untouched.
    try testing.expect(op.join.left.* == .filter);
    try testing.expectEqualStrings("l.a", op.join.left.*.filter.predicate.is_not_null);
    try testing.expect(op.join.right.* == .select);
}

test "predicate pushdown: an AND splits per side, cross-side conjunct stays" {
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();

    var dummy: ir.Op = .single_row;
    var left = testSelect(&.{ "l.a", "l.b" }, &dummy);
    var right = testSelect(&.{ "r.c", "r.d" }, &dummy);
    var join = testJoin(.inner, &left, &right);

    // l.a IS NOT NULL  AND  r.c IS NOT NULL  AND  (l.b = r.d)  [cross-side → stays]
    const conjuncts = [_]PredicateExpr{
        .{ .is_not_null = "l.a" },
        .{ .is_not_null = "r.c" },
        .{ .leaf_col_col = .{ .left = "l.b", .right = "r.d", .op = .eq } },
    };
    var op = ir.Op{ .filter = .{ .predicate = .{ .@"and" = &conjuncts }, .upstream = &join } };

    try pushJoinFilters(a, null, .{}, &op);

    // The cross-side conjunct can't be attributed to one side, so a Filter
    // remains above the join; both single-side conjuncts pushed down.
    try testing.expect(op == .filter);
    try testing.expect(op.filter.upstream.* == .join);
    try testing.expect(op.filter.upstream.join.left.* == .filter);
    try testing.expectEqualStrings("l.a", op.filter.upstream.join.left.*.filter.predicate.is_not_null);
    try testing.expect(op.filter.upstream.join.right.* == .filter);
    try testing.expectEqualStrings("r.c", op.filter.upstream.join.right.*.filter.predicate.is_not_null);
}

test "predicate pushdown: filter sinks through a compute onto the join side" {
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();

    var dummy: ir.Op = .single_row;
    var left = testSelect(&.{ "l.a", "l.b" }, &dummy);
    var right = testSelect(&.{ "r.c", "r.d" }, &dummy);
    var join = testJoin(.inner, &left, &right);
    const derived = [_]ir.Derived{.{ .name = "gap", .expr = .{ .col_ref = "l.a" } }};
    var comp = ir.Op{ .compute = .{ .derived = &derived, .upstream = &join } };
    // `l.a IS NOT NULL` references no derived name → sinks below the compute
    // and then onto the join's left input.
    var op = ir.Op{ .filter = .{ .predicate = .{ .is_not_null = "l.a" }, .upstream = &comp } };

    try pushJoinFilters(a, null, .{}, &op);

    try testing.expect(op == .compute);
    try testing.expect(op.compute.upstream.* == .join);
    try testing.expect(op.compute.upstream.join.left.* == .filter);
    try testing.expectEqualStrings("l.a", op.compute.upstream.join.left.*.filter.predicate.is_not_null);
}

test "predicate pushdown: conjunct on a derived column stays above the compute" {
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();

    var dummy: ir.Op = .single_row;
    var left = testSelect(&.{ "l.a", "l.b" }, &dummy);
    var right = testSelect(&.{ "r.c", "r.d" }, &dummy);
    var join = testJoin(.inner, &left, &right);
    const derived = [_]ir.Derived{.{ .name = "gap", .expr = .{ .col_ref = "l.a" } }};
    var comp = ir.Op{ .compute = .{ .derived = &derived, .upstream = &join } };
    // `gap IS NOT NULL` (derived) must stay; `l.b IS NOT NULL` sinks.
    const conjuncts = [_]PredicateExpr{
        .{ .is_not_null = "gap" },
        .{ .is_not_null = "l.b" },
    };
    var op = ir.Op{ .filter = .{ .predicate = .{ .@"and" = &conjuncts }, .upstream = &comp } };

    try pushJoinFilters(a, null, .{}, &op);

    try testing.expect(op == .filter);
    try testing.expectEqualStrings("gap", op.filter.predicate.is_not_null);
    try testing.expect(op.filter.upstream.* == .compute);
    try testing.expect(op.filter.upstream.compute.upstream.* == .join);
    try testing.expect(op.filter.upstream.compute.upstream.join.left.* == .filter);
    try testing.expectEqualStrings("l.b", op.filter.upstream.compute.upstream.join.left.*.filter.predicate.is_not_null);
}

test "predicate pushdown: exclude of a same-suffix column blocks the sink" {
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();

    var dummy: ir.Op = .single_row;
    var left = testSelect(&.{ "l.a", "l.b" }, &dummy);
    var right = testSelect(&.{ "r.c", "r.a" }, &dummy);
    var join = testJoin(.inner, &left, &right);
    // Below the exclude BOTH `l.a` and `r.a` exist — a bare `a` reference
    // sunk below could resolve to the wrong one, so it must stay.
    var excl = ir.Op{ .exclude = .{ .columns = &.{"r.a"}, .upstream = &join } };
    var op = ir.Op{ .filter = .{ .predicate = .{ .is_not_null = "a" }, .upstream = &excl } };

    try pushJoinFilters(a, null, .{}, &op);

    try testing.expect(op == .filter);
    try testing.expect(op.filter.upstream.* == .exclude);
    try testing.expect(op.filter.upstream.exclude.upstream.* == .join);
}

test "predicate pushdown: nullable-side predicate never crosses an outer join" {
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();

    var dummy: ir.Op = .single_row;
    var left = testSelect(&.{ "l.a", "l.b" }, &dummy);
    var right = testSelect(&.{ "r.c", "r.d" }, &dummy);
    var join = testJoin(.left, &left, &right);
    // Predicate on the RIGHT (nullable) side of a LEFT join — must NOT push.
    var op = ir.Op{ .filter = .{ .predicate = .{ .is_not_null = "r.c" }, .upstream = &join } };

    try pushJoinFilters(a, null, .{}, &op);

    try testing.expect(op == .filter);
    try testing.expect(op.filter.upstream.* == .join);
    try testing.expect(op.filter.upstream.join.right.* == .select);
}
