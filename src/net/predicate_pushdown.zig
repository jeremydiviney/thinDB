//! Pre-execution pushdown passes: join filters and union computes.
//!
//! Rewrites `Filter(pred, Join(L, R))` by moving each top-level WHERE conjunct
//! that references columns from exactly ONE preserved join input down onto that
//! input, so the source is narrowed BEFORE the join runs. A conjunct that
//! empties a side turns the whole join into a no-op; one that merely narrows it
//! shrinks the build/probe input.
//!
//! Also rewrites `Compute(set_union(A, B))` → `set_union(Compute(A),
//! Compute(B))` (see `pushComputeThroughUnions`): per-row derived columns
//! commute with UNION ALL's bag concatenation, and a stage-backed arm can then
//! parallelise the evaluation via the terminal compute push where the union'd
//! operator cannot.
//!
//! And rewrites `GroupBy(A × M)` whose aggregates read A alone and ignore
//! duplicates into a product of per-side aggregates (see
//! `pushAggregatesThroughCrossJoins`).
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
const Predicate = @import("../exec/predicate.zig").Predicate;
const expr_mod = @import("../exec/expr.zig");
const local = @import("local.zig");
const api = @import("../api/api.zig");

const Ctx = struct {
    arena: Allocator,
    catalog: ?*api.Catalog,
    session: api.Session,
    /// A star projection lists its whole upstream rather than leaving the
    /// node opaque: a superset of its columns, for passes that tolerate one.
    expand_stars: bool = false,
    /// The inputs each join had when its key constants last transferred.
    transferred: ?*std.AutoHashMapUnmanaged(*const ir.Op, [2]*const ir.Op) = null,
};

var trace_push: bool = false;

extern "c" fn getenv(name: [*:0]const u8) ?[*:0]const u8;

/// Rewrite the whole op tree in place. `arena` owns any new Filter nodes and
/// predicate slices (the compile-time node arena).
pub fn pushJoinFilters(arena: Allocator, catalog: ?*api.Catalog, session: api.Session, op: *ir.Op) anyerror!void {
    trace_push = getenv("THINDB_TRACE_PUSHDOWN") != null;
    var transferred: std.AutoHashMapUnmanaged(*const ir.Op, [2]*const ir.Op) = .empty;
    const ctx = Ctx{ .arena = arena, .catalog = catalog, .session = session, .transferred = &transferred };
    try walk(ctx, op);
    // Every pin has reached the inputs it can by now: a key pinned on both
    // inputs is only provable once the walk has finished moving filters.
    var seen: std.AutoHashMapUnmanaged(*const ir.Op, void) = .empty;
    try dropPinnedKeys(ctx, op, false, &seen);
}

fn walk(ctx: Ctx, op: *ir.Op) anyerror!void {
    // Bottom-up: optimize children first, then try to push at this node.
    switch (op.*) {
        .scan, .single_row, .file_scan, .ddl, .show, .insert, .copy, .set_var, .admin => {},
        .delete_op => |d| if (d.source) |s| try walk(ctx, s),
        .update_op => |u| if (u.source) |s| try walk(ctx, s),
        .limit => |l| try walk(ctx, @constCast(l.upstream)),
        .select, .exclude => |p| try walk(ctx, @constCast(p.upstream)),
        .order_by => |o| try walk(ctx, @constCast(o.upstream)),
        .group_by => |g| try walk(ctx, @constCast(g.upstream)),
        .compute => |c| try walk(ctx, @constCast(c.upstream)),
        .materialize => |m| try walk(ctx, @constCast(m.upstream)),
        .table_fn => |t| for (t.inputs) |inp| try walk(ctx, inp),
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
            try transferKeyConstants(ctx, op, &.{});
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
    const conjuncts = try splitConjuncts(ctx.arena, op.filter.predicate);
    try transferKeyConstants(ctx, join_op, conjuncts);

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
        const nf = try filterOver(ctx, join_op.join.left, to_left.items);
        join_op.join.left = nf;
        try walk(ctx, nf); // cascade deeper if that side is itself a join
    }
    if (to_right.items.len > 0) {
        const nf = try filterOver(ctx, join_op.join.right, to_right.items);
        join_op.join.right = nf;
        try walk(ctx, nf);
    }
    // The walk below this join ran before these conjuncts reached its inputs.
    try transferKeyConstants(ctx, join_op, &.{});

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
                if (isStar(nm)) {
                    if (!ctx.expand_stars or !try collectColumns(ctx, p.upstream, out)) return false;
                    continue;
                }
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

/// `conjuncts` over `input`, ANDed into the filter `input` already is: a
/// scan block reads one filter over its source, not a stack of them.
fn filterOver(ctx: Ctx, input: *ir.Op, conjuncts: []const PredicateExpr) !*ir.Op {
    const nf = try ctx.arena.create(ir.Op);
    nf.* = if (input.* == .filter) .{ .filter = .{
        .predicate = try combine(ctx.arena, try std.mem.concat(ctx.arena, PredicateExpr, &.{ try splitConjuncts(ctx.arena, input.filter.predicate), conjuncts })),
        .upstream = input.filter.upstream,
    } } else .{ .filter = .{ .predicate = try combine(ctx.arena, conjuncts), .upstream = input } };
    return nf;
}

fn combine(arena: Allocator, conjuncts: []const PredicateExpr) !PredicateExpr {
    if (conjuncts.len == 1) return conjuncts[0];
    return .{ .@"and" = try arena.dupe(PredicateExpr, conjuncts) };
}

/// Bare column of a name — the form `types.findColumn` matches on.
const suffix = types.unqualifiedName;

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
        .in_set, .text_as_number_set => |s| out.append(arena, s.col) catch return false,
        .text_as_number => |l| out.append(arena, l.col) catch return false,
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

// ---------------------------------------------------------------------------
// Join-key constant transfer
// ---------------------------------------------------------------------------

/// Copy an equality that pins one input's join key onto the other input's
/// key: `l.k = 7` below the left input of `ON l.k = r.k` adds `r.k = 7` over
/// the right input, which then prunes like any written filter. A row of the
/// receiving input whose key differs could never match, so dropping it
/// changes nothing on either side of an INNER join or on the nullable side
/// of an outer join; a preserved side never receives one from below.
///
/// `above` is the filter right over the join. Every row it keeps has its
/// pinned key, so it may give to either input under any join type: a row
/// it would drop only ever matched rows the filter drops too, and only
/// ever spared rows from null extension that the filter drops anyway.
/// Null-safe keys stay out: `NULL <=> NULL` matches rows the constant would
/// drop.
fn transferKeyConstants(ctx: Ctx, op: *ir.Op, above: []const PredicateExpr) anyerror!void {
    const j = &op.join;
    const to_left_ok = j.join_type == .inner or j.join_type == .right;
    const to_right_ok = j.join_type == .inner or j.join_type == .left;
    // The walk revisits a shared CTE body once per reference, and a body's
    // inputs only change by a new node on the edge: unchanged inputs hold
    // the facts they held last time, which have already transferred.
    const last = if (ctx.transferred) |seen| seen.get(op) else null;
    const unchanged = if (last) |inputs| inputs[0] == j.left and inputs[1] == j.right else false;
    const from_below = (to_left_ok or to_right_ok) and !unchanged;
    if (!from_below and above.len == 0) return;

    var to_left: std.ArrayListUnmanaged(PredicateExpr) = .empty;
    var to_right: std.ArrayListUnmanaged(PredicateExpr) = .empty;
    for (j.on) |pair| {
        if (pair.null_safe) continue;
        const l_name = visibleSource(j.left, pair.left);
        const r_name = visibleSource(j.right, pair.right);
        if (!from_below and !equatesColumn(above, l_name) and !equatesColumn(above, r_name)) continue;
        const l = try keyFacts(ctx, j.left, pair.left) orelse continue;
        const r = try keyFacts(ctx, j.right, pair.right) orelse continue;
        if (!sameKeyType(l.col_type, r.col_type)) continue;
        if (r.pin == null) if ((if (to_right_ok) l.pin else null) orelse try pinAbove(ctx, j, above, l_name, .left, l.col_type)) |pin| {
            var derived = pin;
            derived.col = r_name;
            try to_right.append(ctx.arena, .{ .leaf = derived });
        };
        if (l.pin == null) if ((if (to_left_ok) r.pin else null) orelse try pinAbove(ctx, j, above, r_name, .right, r.col_type)) |pin| {
            var derived = pin;
            derived.col = l_name;
            try to_left.append(ctx.arena, .{ .leaf = derived });
        };
    }
    if (trace_push and to_left.items.len + to_right.items.len > 0) std.debug.print("[ppd]   key constants: to_left={d} to_right={d}\n", .{ to_left.items.len, to_right.items.len });
    if (to_left.items.len > 0) {
        const nf = try filterOver(ctx, j.left, to_left.items);
        j.left = nf;
        try walk(ctx, nf);
    }
    if (to_right.items.len > 0) {
        const nf = try filterOver(ctx, j.right, to_right.items);
        j.right = nf;
        try walk(ctx, nf);
    }
    if (ctx.transferred) |seen| try seen.put(ctx.arena, op, .{ j.left, j.right });
}

/// Walk the tree, each shared body once, dropping pinned key pairs from its
/// joins. A KEYED BY body keeps its pairs: the region recognizer routes and
/// filters its joins by them.
fn dropPinnedKeys(ctx: Ctx, op: *ir.Op, in_region: bool, seen: *std.AutoHashMapUnmanaged(*const ir.Op, void)) anyerror!void {
    var region = in_region;
    if (op.* == .materialize) {
        if ((try seen.getOrPut(ctx.arena, op)).found_existing) return;
        if (op.materialize.region_keys != null) region = true;
    }
    switch (op.*) {
        inline else => |payload| {
            const Payload = @TypeOf(payload);
            if (@typeInfo(Payload) != .@"struct") return;
            inline for (@typeInfo(Payload).@"struct".fields) |field| {
                const child = @field(payload, field.name);
                if (field.type == *ir.Op) {
                    try dropPinnedKeys(ctx, child, region, seen);
                } else if (field.type == ?*ir.Op) {
                    if (child) |c| try dropPinnedKeys(ctx, c, region, seen);
                } else if (field.type == []const *ir.Op) {
                    for (child) |c| try dropPinnedKeys(ctx, c, region, seen);
                }
            }
        },
    }
    if (op.* == .join and !region) try dropJoinPinnedKeys(ctx, &op.join);
}

/// Drop the ON pairs whose two keys every input row pins to one same
/// literal. Such a pair holds for every pair of rows, so testing it only
/// hashes and compares a constant. One pair always stays, so the join keeps
/// its keyed form.
fn dropJoinPinnedKeys(ctx: Ctx, j: *ir.Op.Join) anyerror!void {
    if (j.on.len < 2) return;
    var kept: std.ArrayListUnmanaged(ir.JoinKeyPair) = .empty;
    for (j.on, 0..) |pair, i| {
        const last_left = kept.items.len == 0 and i == j.on.len - 1;
        if (!last_left and !pair.null_safe and try pinnedAlike(ctx, j, pair)) continue;
        try kept.append(ctx.arena, pair);
    }
    if (kept.items.len == j.on.len) return;
    if (trace_push) std.debug.print("[ppd]   pinned keys dropped: {d} of {d}\n", .{ j.on.len - kept.items.len, j.on.len });
    j.on = kept.items;
}

fn pinnedAlike(ctx: Ctx, j: *const ir.Op.Join, pair: ir.JoinKeyPair) anyerror!bool {
    const l = try keyFacts(ctx, j.left, pair.left) orelse return false;
    const r = try keyFacts(ctx, j.right, pair.right) orelse return false;
    const l_pin = l.pin orelse return false;
    const r_pin = r.pin orelse return false;
    return sameKeyType(l.col_type, r.col_type) and l_pin.val.eql(r_pin.val);
}

/// A conjunct of `above` that pins column `name` of `j`'s `side` input.
fn pinAbove(ctx: Ctx, j: *const ir.Op.Join, above: []const PredicateExpr, name: []const u8, side: Side, col_type: types.Type) anyerror!?Predicate {
    const pin = pinOf(above, name, col_type) orelse return null;
    const pin_side = try joinSideOf(ctx, j, pin.col) orelse return null;
    return if (pin_side == side) pin else null;
}

/// What `keyFacts` proves about one column of an input.
const KeyFacts = struct {
    /// The base column's declared type.
    col_type: types.Type,
    /// A `col = literal` every row of the input satisfies, or NULL fails.
    pin: ?Predicate,
};

/// Trace column `name` of `op` down to the table column it reads unchanged,
/// collecting its type and any equality with a literal applied on the way.
/// Null when the column is computed, aggregated, or can't be traced exactly.
/// Filters, projections and renames, computes and windows (which add columns
/// beside it), excludes, order by, limit, materialize and group-by keys pass a
/// column through. A join passes the columns of its sides whose rows it
/// keeps as they are: both sides of INNER, the left of LEFT, the right of
/// RIGHT.
fn keyFacts(ctx: Ctx, op: *const ir.Op, name: []const u8) anyerror!?KeyFacts {
    switch (op.*) {
        .scan => |s| {
            if (types.splitQualifiedName(name)) |split| if (!namesQualifier(op, split.qualifier)) return null;
            const cat = ctx.catalog orelse return null;
            const t = local.resolveTable(cat, ctx.session, s.table) catch return null;
            const idx = types.findColumn(t.schema.columns, name) orelse return null;
            return .{ .col_type = t.schema.columns[idx].type, .pin = null };
        },
        .alias => |a| {
            const bare = if (types.splitQualifiedName(name)) |split| blk: {
                if (!types.columnNameEql(split.qualifier, a.alias)) return null;
                break :blk split.bare;
            } else name;
            return keyFacts(ctx, a.upstream, bare);
        },
        .filter => |f| {
            var facts = try keyFacts(ctx, f.upstream, name) orelse return null;
            if (facts.pin == null) facts.pin = pinOf(try splitConjuncts(ctx.arena, f.predicate), name, facts.col_type);
            return facts;
        },
        .order_by => |o| return keyFacts(ctx, o.upstream, name),
        .limit => |l| return keyFacts(ctx, l.upstream, name),
        .materialize => |m| return keyFacts(ctx, m.upstream, name),
        .select => |p| {
            var star = false;
            for (p.columns, 0..) |nm, i| {
                if (isStar(nm)) {
                    star = true;
                    continue;
                }
                const out_name = if (p.outputs) |o| (if (i < o.len) (o[i] orelse nm) else nm) else nm;
                if (sameColumn(out_name, name)) return keyFacts(ctx, p.upstream, nm);
            }
            return if (star) keyFacts(ctx, p.upstream, name) else null;
        },
        .compute => |c| {
            for (c.derived) |d| if (sameColumn(d.name, name)) {
                return keyFacts(ctx, c.upstream, copiedColumn(c, d) orelse return null);
            };
            return keyFacts(ctx, c.upstream, name);
        },
        .window => |w| {
            for (w.calls) |call| if (sameColumn(call.output_name, name)) return null;
            return keyFacts(ctx, w.upstream, name);
        },
        .exclude => |p| {
            for (p.columns) |nm| if (sameColumn(nm, name)) return null;
            return keyFacts(ctx, p.upstream, name);
        },
        .group_by => |g| {
            for (g.group_cols) |nm| if (sameColumn(nm, name)) return keyFacts(ctx, g.upstream, nm);
            return null;
        },
        .join => |j| {
            const side = try joinSideOf(ctx, &j, name) orelse return null;
            const kept = switch (side) {
                .left => j.join_type == .inner or j.join_type == .left,
                .right => j.join_type == .inner or j.join_type == .right,
                .stay => false,
            };
            if (!kept) return null;
            return keyFacts(ctx, if (side == .left) j.left else j.right, name);
        },
        else => return null,
    }
}

/// The column a derived column copies unchanged, when the compute keeps
/// that column visible beside it: the parser's `__join_on_right_N` keys.
fn copiedColumn(c: ir.Op.Compute, d: ir.Derived) ?[]const u8 {
    if (d.expr != .col_ref) return null;
    for (c.derived) |other| if (sameColumn(other.name, d.expr.col_ref)) return null;
    return d.expr.col_ref;
}

/// A name for column `name` of `op` that a filter over `op` can sink
/// through it with: the source of a copied key rather than the copy.
fn visibleSource(op: *const ir.Op, name: []const u8) []const u8 {
    if (op.* != .compute) return name;
    for (op.compute.derived) |d| if (sameColumn(d.name, name)) return copiedColumn(op.compute, d) orelse name;
    return name;
}

/// The input of `j` that provides column `name`: the one its qualifier names,
/// or for a bare name the one input that has it. `.stay` when neither or both.
fn joinSideOf(ctx: Ctx, j: *const ir.Op.Join, name: []const u8) anyerror!?Side {
    if (types.splitQualifiedName(name)) |split| {
        const in_l = namesQualifier(j.left, split.qualifier);
        const in_r = namesQualifier(j.right, split.qualifier);
        if (in_l == in_r) return null;
        return if (in_l) .left else .right;
    }
    var left_cols: std.ArrayListUnmanaged([]const u8) = .empty;
    var right_cols: std.ArrayListUnmanaged([]const u8) = .empty;
    if (!try collectColumns(ctx, j.left, &left_cols) or !try collectColumns(ctx, j.right, &right_cols)) return null;
    const in_l = contains(left_cols.items, name);
    const in_r = contains(right_cols.items, name);
    if (in_l == in_r) return null;
    return if (in_l) .left else .right;
}

/// Whether `qualifier.col` names a column of `op`'s output: an alias over it
/// or, without one, a table it scans.
fn namesQualifier(op: *const ir.Op, qualifier: []const u8) bool {
    return switch (op.*) {
        .alias => |a| types.columnNameEql(a.alias, qualifier),
        .scan => |s| types.columnNameEql(s.alias orelse s.table.name, qualifier),
        .join => |j| namesQualifier(j.left, qualifier) or namesQualifier(j.right, qualifier),
        .filter => |f| namesQualifier(f.upstream, qualifier),
        .order_by => |o| namesQualifier(o.upstream, qualifier),
        .limit => |l| namesQualifier(l.upstream, qualifier),
        .materialize => |m| namesQualifier(m.upstream, qualifier),
        .compute => |c| namesQualifier(c.upstream, qualifier),
        .window => |w| namesQualifier(w.upstream, qualifier),
        .exclude => |p| namesQualifier(p.upstream, qualifier),
        else => false,
    };
}

/// Two spellings of one column: equal bare names, and equal qualifiers when
/// both have one.
fn sameColumn(a: []const u8, b: []const u8) bool {
    const sa = types.splitQualifiedName(a);
    const sb = types.splitQualifiedName(b);
    if (sa != null and sb != null and !types.columnNameEql(sa.?.qualifier, sb.?.qualifier)) return false;
    return types.columnNameEql(suffix(a), suffix(b));
}

/// The first of `conjuncts`, or of an AND nested among them, that pins
/// `name` to a literal its type stores exactly one way.
fn pinOf(conjuncts: []const PredicateExpr, name: []const u8, col_type: types.Type) ?Predicate {
    for (conjuncts) |c| {
        if (c == .@"and") if (pinOf(c.@"and", name, col_type)) |p| return p;
        if (c != .leaf) continue;
        const p = c.leaf;
        if (p.op != .eq or p.as_boolean or !sameColumn(p.col, name)) continue;
        if (literalPinsColumn(col_type, p.val)) return p;
    }
    return null;
}

/// Whether a conjunct of `conjuncts`, or of an AND nested among them, is an
/// equality on `name`.
fn equatesColumn(conjuncts: []const PredicateExpr, name: []const u8) bool {
    for (conjuncts) |c| switch (c) {
        .@"and" => |kids| if (equatesColumn(kids, name)) return true,
        .leaf => |p| if (p.op == .eq and sameColumn(p.col, name)) return true,
        else => {},
    };
    return false;
}

/// Whether `col = lit` admits exactly one stored value, which an equi join
/// matches only to that same value: integers against an integer literal,
/// text against text, dates and datetimes against whatever they read. Float
/// equality (NaN, -0.0), text read as a number, and the rest stay out.
fn literalPinsColumn(col_type: types.Type, lit: types.Value) bool {
    return switch (col_type) {
        .tinyint, .smallint, .int, .bigint, .largeint => switch (lit) {
            .tinyint, .smallint, .int, .bigint, .largeint => true,
            else => false,
        },
        .varchar, .string, .char => lit == .text,
        .date, .datetime => switch (lit) {
            .text, .date, .datetime => true,
            else => false,
        },
        else => false,
    };
}

/// Key columns a pinned literal means the same thing on: the same type, or
/// VARCHAR and STRING, which both compare their bytes. CHAR pairs only with
/// CHAR, whose padding the other text types don't share.
fn sameKeyType(a: types.Type, b: types.Type) bool {
    const text_a = a == .varchar or a == .string;
    const text_b = b == .varchar or b == .string;
    if (text_a or text_b) return text_a and text_b;
    return std.meta.activeTag(a) == std.meta.activeTag(b);
}

// ---------------------------------------------------------------------------
// Compute push through UNION ALL
// ---------------------------------------------------------------------------

/// Rewrite `Compute(set_union(A, B))` → `set_union(Compute(A), Compute(B))`,
/// also through a single-consumer `.materialize` wrapper (the CTE-ref shape
/// `Compute(materialize(set_union(..)))`). Per-row derived columns commute
/// with UNION ALL's bag concatenation; after the split each arm's compute can
/// terminal-push into that arm's parallel pipeline, where the union'd
/// operator has nothing to fuse into.
///
/// Guards:
///   - UNION ALL only: a distinct union dedups on the union's own columns —
///     appending derived columns before the dedup would change it.
///   - Both arms expose IDENTICAL column-name suffix lists (exact, catalog-
///     resolved, positional): the derived expressions were written against
///     the union's (= left arm's) names and must resolve identically inside
///     the right arm.
///   - The `.materialize` wrapper must have exactly one consumer — rewriting
///     inside a shared CTE body would leak the derived columns to its other
///     consumers.
pub fn pushComputeThroughUnions(arena: Allocator, catalog: ?*api.Catalog, session: api.Session, op: *ir.Op) anyerror!void {
    trace_push = getenv("THINDB_TRACE_PUSHDOWN") != null;
    const ctx = Ctx{ .arena = arena, .catalog = catalog, .session = session };
    var mat_refs: std.AutoHashMapUnmanaged(*const ir.Op, u32) = .empty;
    try countMatRefs(ctx.arena, op, &mat_refs);
    var visited: std.AutoHashMapUnmanaged(*const ir.Op, void) = .empty;
    try walkComputeUnions(ctx, op, &mat_refs, &visited);
}

/// Count how many parent edges reference each `.materialize` node — the same
/// shared-node convention the staged compiler uses. Each body walks once.
fn countMatRefs(arena: Allocator, op: *const ir.Op, map: *std.AutoHashMapUnmanaged(*const ir.Op, u32)) anyerror!void {
    if (op.* == .materialize) {
        const gop = try map.getOrPut(arena, op);
        if (gop.found_existing) {
            gop.value_ptr.* += 1;
            return;
        }
        gop.value_ptr.* = 1;
        return countMatRefs(arena, op.materialize.upstream, map);
    }
    switch (op.*) {
        .scan, .single_row, .file_scan, .ddl, .show, .insert, .copy, .set_var, .admin => {},
        .delete_op => |d| if (d.source) |s| try countMatRefs(arena, s, map),
        .update_op => |u| if (u.source) |s| try countMatRefs(arena, s, map),
        .limit => |l| try countMatRefs(arena, l.upstream, map),
        .select, .exclude => |p| try countMatRefs(arena, p.upstream, map),
        .order_by => |o| try countMatRefs(arena, o.upstream, map),
        .group_by => |g| try countMatRefs(arena, g.upstream, map),
        .compute => |c| try countMatRefs(arena, c.upstream, map),
        .table_fn => |t| for (t.inputs) |inp| try countMatRefs(arena, inp, map),
        .window => |w| try countMatRefs(arena, w.upstream, map),
        .alias => |a| try countMatRefs(arena, a.upstream, map),
        .filter => |f| try countMatRefs(arena, f.upstream, map),
        .explain => |e| try countMatRefs(arena, e.inner, map),
        .create_table_as => |c| try countMatRefs(arena, c.source, map),
        .insert_select => |i| try countMatRefs(arena, i.source, map),
        .batch => |b| for (b.statements) |s| try countMatRefs(arena, s, map),
        .set_union => |u| {
            try countMatRefs(arena, u.left, map);
            try countMatRefs(arena, u.right, map);
        },
        .join => |j| {
            try countMatRefs(arena, j.left, map);
            try countMatRefs(arena, j.right, map);
        },
        .materialize => unreachable,
    }
}

fn walkComputeUnions(
    ctx: Ctx,
    op: *ir.Op,
    mat_refs: *const std.AutoHashMapUnmanaged(*const ir.Op, u32),
    visited: *std.AutoHashMapUnmanaged(*const ir.Op, void),
) anyerror!void {
    switch (op.*) {
        .scan, .single_row, .file_scan, .ddl, .show, .insert, .copy, .set_var, .admin => {},
        .delete_op => |d| if (d.source) |s| try walkComputeUnions(ctx, s, mat_refs, visited),
        .update_op => |u| if (u.source) |s| try walkComputeUnions(ctx, s, mat_refs, visited),
        .limit => |l| try walkComputeUnions(ctx, @constCast(l.upstream), mat_refs, visited),
        .select, .exclude => |p| try walkComputeUnions(ctx, @constCast(p.upstream), mat_refs, visited),
        .order_by => |o| try walkComputeUnions(ctx, @constCast(o.upstream), mat_refs, visited),
        .group_by => |g| try walkComputeUnions(ctx, @constCast(g.upstream), mat_refs, visited),
        .table_fn => |t| for (t.inputs) |inp| try walkComputeUnions(ctx, inp, mat_refs, visited),
        .window => |w| try walkComputeUnions(ctx, @constCast(w.upstream), mat_refs, visited),
        .alias => |a| try walkComputeUnions(ctx, @constCast(a.upstream), mat_refs, visited),
        .filter => |f| try walkComputeUnions(ctx, @constCast(f.upstream), mat_refs, visited),
        .explain => |e| try walkComputeUnions(ctx, e.inner, mat_refs, visited),
        .create_table_as => |c| try walkComputeUnions(ctx, @constCast(c.source), mat_refs, visited),
        .insert_select => |i| try walkComputeUnions(ctx, @constCast(i.source), mat_refs, visited),
        .batch => |b| for (b.statements) |s| try walkComputeUnions(ctx, @constCast(s), mat_refs, visited),
        .materialize => |m| {
            // Shared bodies walk once.
            const gop = try visited.getOrPut(ctx.arena, op);
            if (!gop.found_existing) try walkComputeUnions(ctx, @constCast(m.upstream), mat_refs, visited);
        },
        .set_union => |u| {
            try walkComputeUnions(ctx, @constCast(u.left), mat_refs, visited);
            try walkComputeUnions(ctx, @constCast(u.right), mat_refs, visited);
        },
        .join => |j| {
            try walkComputeUnions(ctx, @constCast(j.left), mat_refs, visited);
            try walkComputeUnions(ctx, @constCast(j.right), mat_refs, visited);
        },
        .compute => |c| {
            try walkComputeUnions(ctx, @constCast(c.upstream), mat_refs, visited);
            try trySplitComputeOverUnion(ctx, op, mat_refs);
        },
    }
}

fn trySplitComputeOverUnion(
    ctx: Ctx,
    op: *ir.Op,
    mat_refs: *const std.AutoHashMapUnmanaged(*const ir.Op, u32),
) anyerror!void {
    const up = @constCast(op.compute.upstream);
    const union_op: *ir.Op = switch (up.*) {
        .set_union => up,
        .materialize => |m| blk: {
            // A recursive CTE's arms are its iteration driver's plan: an arm
            // that gained a column would no longer line up with the others.
            if (m.recursion != null) return;
            if ((mat_refs.get(up) orelse 0) != 1) return;
            if (m.upstream.* != .set_union) return;
            break :blk @constCast(m.upstream);
        },
        else => return,
    };
    const u = &union_op.set_union;
    if (!u.all) return;

    // Both arms must enumerate exactly and expose the same names positionally.
    var left_cols: std.ArrayListUnmanaged([]const u8) = .empty;
    var right_cols: std.ArrayListUnmanaged([]const u8) = .empty;
    const left_ok = collectColumns(ctx, u.left, &left_cols) catch false;
    const right_ok = collectColumns(ctx, u.right, &right_cols) catch false;
    if (!left_ok or !right_ok) return;
    if (left_cols.items.len != right_cols.items.len) return;
    for (left_cols.items, right_cols.items) |l, r| {
        if (!types.columnNameEql(l, r)) return;
    }
    if (trace_push) std.debug.print("[ppd]   compute({d} derived) split into union arms\n", .{op.compute.derived.len});

    const lc = try ctx.arena.create(ir.Op);
    lc.* = .{ .compute = .{ .derived = op.compute.derived, .upstream = u.left } };
    const rc = try ctx.arena.create(ir.Op);
    rc.* = .{ .compute = .{ .derived = op.compute.derived, .upstream = u.right } };
    u.left = lc;
    u.right = rc;
    // The parent compute dissolves into whatever it sat on (the union, or
    // the single-consumer materialize whose body now carries the computes).
    op.* = up.*;

    // A nested union inside an arm splits further.
    try trySplitComputeOverUnion(ctx, lc, mat_refs);
    try trySplitComputeOverUnion(ctx, rc, mat_refs);
}

// ---------------------------------------------------------------------------
// Aggregate push through a cross join
// ---------------------------------------------------------------------------

/// Rewrite `GroupBy{KA ∪ KM}(A × M)` → `Project(GroupBy{KA}(A) × GroupBy{KM}(M))`
/// when every aggregate reads A alone and ignores duplicate inputs (MIN, MAX,
/// ANY_VALUE, COUNT(DISTINCT), …). Group (ka, km) of the product holds group
/// ka's A rows once per M row keyed km: the same values repeated, so such an
/// aggregate equals its value over group ka of A, and the group exists exactly
/// when ka occurs in A and km in M. The product then joins |A| + |M|
/// aggregated rows rather than aggregating |A|·|M| — the month-spine shape
/// that fans each entity out over a calendar only to collapse it again.
///
/// Guards:
///   - A pure cross join: no keys, ranges or predicates.
///   - Both key sets non-empty: an empty KA would turn the product's
///     zero groups over an empty A into a global aggregate's one row.
///   - Every key, aggregate input and derived reference classifies to one
///     side: its suffix is in that side's column set and not the other's.
///     A star projection lists its whole upstream here — a superset, which
///     only ever leaves a name unclassified or unresolvable on both sides.
///   - A Compute between the join and the aggregate (computed keys or
///     aggregate inputs) splits per side, each derived column reading one.
///   - A star-free Project above the aggregate reads its output by name,
///     through nothing but name-reading nodes. The Project the rewrite adds
///     names a key as a SELECT does, bare (`e.k` outputs `k`), where the
///     aggregate keeps its input's name (`e.k`), and the aggregate's names
///     and column order are the result when nothing above re-projects them.
/// The added Project restores the aggregate's output columns in order and
/// drops the hidden count the key-only side carries.
pub fn pushAggregatesThroughCrossJoins(arena: Allocator, catalog: ?*api.Catalog, session: api.Session, op: *ir.Op) anyerror!void {
    trace_push = getenv("THINDB_TRACE_PUSHDOWN") != null;
    const ctx = Ctx{ .arena = arena, .catalog = catalog, .session = session, .expand_stars = true };
    var visited: std.AutoHashMapUnmanaged(*const ir.Op, void) = .empty;
    try walkCrossAggregates(ctx, op, &visited, false);
}

/// `projected`: a star-free Project above `op` reads its output by name.
fn walkCrossAggregates(ctx: Ctx, op: *ir.Op, visited: *std.AutoHashMapUnmanaged(*const ir.Op, void), projected: bool) anyerror!void {
    switch (op.*) {
        .scan, .single_row, .file_scan, .ddl, .show, .insert, .copy, .set_var, .admin => {},
        .delete_op => |d| if (d.source) |s| try walkCrossAggregates(ctx, s, visited, false),
        .update_op => |u| if (u.source) |s| try walkCrossAggregates(ctx, s, visited, false),
        .select => |p| {
            const named = for (p.columns) |c| {
                if (isStar(c)) break false;
            } else true;
            try walkCrossAggregates(ctx, @constCast(p.upstream), visited, named);
        },
        .limit => |l| try walkCrossAggregates(ctx, @constCast(l.upstream), visited, projected),
        .order_by => |o| try walkCrossAggregates(ctx, @constCast(o.upstream), visited, projected),
        .compute => |c| try walkCrossAggregates(ctx, @constCast(c.upstream), visited, projected),
        .window => |w| try walkCrossAggregates(ctx, @constCast(w.upstream), visited, projected),
        .filter => |f| try walkCrossAggregates(ctx, @constCast(f.upstream), visited, projected),
        .exclude => |p| try walkCrossAggregates(ctx, @constCast(p.upstream), visited, false),
        .table_fn => |t| for (t.inputs) |inp| try walkCrossAggregates(ctx, inp, visited, false),
        .alias => |a| try walkCrossAggregates(ctx, @constCast(a.upstream), visited, false),
        .explain => |e| try walkCrossAggregates(ctx, e.inner, visited, false),
        .create_table_as => |c| try walkCrossAggregates(ctx, @constCast(c.source), visited, false),
        .insert_select => |i| try walkCrossAggregates(ctx, @constCast(i.source), visited, false),
        .batch => |b| for (b.statements) |s| try walkCrossAggregates(ctx, @constCast(s), visited, false),
        .materialize => |m| {
            // A recursive CTE's arms belong to its iteration driver.
            if (m.recursion != null) return;
            const gop = try visited.getOrPut(ctx.arena, op);
            if (!gop.found_existing) try walkCrossAggregates(ctx, @constCast(m.upstream), visited, false);
        },
        .set_union => |u| {
            try walkCrossAggregates(ctx, @constCast(u.left), visited, false);
            try walkCrossAggregates(ctx, @constCast(u.right), visited, false);
        },
        .join => |j| {
            try walkCrossAggregates(ctx, @constCast(j.left), visited, false);
            try walkCrossAggregates(ctx, @constCast(j.right), visited, false);
        },
        .group_by => |g| {
            try walkCrossAggregates(ctx, @constCast(g.upstream), visited, false);
            if (projected) try trySplitAggregateOverCrossJoin(ctx, op);
        },
    }
}

const ColumnSets = [2]std.ArrayListUnmanaged([]const u8);

fn trySplitAggregateOverCrossJoin(ctx: Ctx, op: *ir.Op) anyerror!void {
    const g = op.group_by;
    if (g.group_cols.len == 0 or g.top_k != null or g.emit_limit != null) return;
    for (g.aggs) |a| if (!ignoresDuplicates(a)) return;
    var join_op = g.upstream;
    const pre: []const ir.Derived = if (join_op.* == .compute) blk: {
        const derived = join_op.compute.derived;
        join_op = join_op.compute.upstream;
        break :blk derived;
    } else &.{};
    if (join_op.* != .join) return;
    const j = join_op.join;
    if (j.join_type != .inner or j.on.len != 0 or j.ranges.len != 0 or j.extra_predicate != null or j.residual != null) return;

    var sides: ColumnSets = .{ .empty, .empty };
    if (!(collectColumns(ctx, j.left, &sides[0]) catch false)) return;
    if (!(collectColumns(ctx, j.right, &sides[1]) catch false)) return;

    var side_derived: [2]std.ArrayListUnmanaged(ir.Derived) = .{ .empty, .empty };
    for (pre) |d| {
        var refs: std.ArrayListUnmanaged([]const u8) = .empty;
        if (!try collectExprRefs(ctx.arena, d.expr, &refs)) return;
        var side: ?usize = null;
        for (refs.items) |name| {
            const s = sideOf(&sides, name) orelse return;
            if (side != null and side.? != s) return;
            side = s;
        }
        // A constant column has no side of its own to follow.
        const s = side orelse return;
        if (contains(sides[1 - s].items, suffix(d.name))) return;
        try side_derived[s].append(ctx.arena, d);
        try sides[s].append(ctx.arena, suffix(d.name));
    }

    var agg_side: ?usize = null;
    for (g.aggs) |a| {
        for ([_]?[]const u8{ a.col, a.arg2_col }) |maybe_name| {
            const name = maybe_name orelse continue;
            const s = sideOf(&sides, name) orelse return;
            if (agg_side != null and agg_side.? != s) return;
            agg_side = s;
        }
    }
    const a_side = agg_side orelse return;

    var keys: [2]std.ArrayListUnmanaged([]const u8) = .{ .empty, .empty };
    for (g.group_cols) |name| {
        const s = sideOf(&sides, name) orelse return;
        try keys[s].append(ctx.arena, name);
    }
    if (keys[0].items.len == 0 or keys[1].items.len == 0) return;
    if (trace_push) std.debug.print("[ppd]   group by {d}+{d} keys split over cross join, {d} aggregates on the {s} side\n", .{
        keys[0].items.len, keys[1].items.len, g.aggs.len, if (a_side == 0) "left" else "right",
    });

    const inputs = [2]*ir.Op{ j.left, j.right };
    var grouped: [2]*ir.Op = undefined;
    for (&grouped, inputs, side_derived, keys, 0..) |*dst, input, derived, side_keys, s| {
        var upstream = input;
        if (derived.items.len > 0) {
            upstream = try ctx.arena.create(ir.Op);
            upstream.* = .{ .compute = .{ .derived = derived.items, .upstream = input } };
        }
        dst.* = try ctx.arena.create(ir.Op);
        dst.*.* = .{ .group_by = .{
            .group_cols = side_keys.items,
            .aggs = if (s == a_side) g.aggs else &key_only_aggs,
            .upstream = upstream,
        } };
    }
    const product = try ctx.arena.create(ir.Op);
    product.* = join_op.*;
    product.join.left = grouped[0];
    product.join.right = grouped[1];

    const columns = try ctx.arena.alloc([]const u8, g.group_cols.len + g.aggs.len);
    @memcpy(columns[0..g.group_cols.len], g.group_cols);
    for (columns[g.group_cols.len..], g.aggs) |*dst, a| dst.* = a.as;
    op.* = .{ .select = .{ .columns = columns, .upstream = product } };
    // A chain of cross joins splits one product at a time.
    try trySplitAggregateOverCrossJoin(ctx, grouped[a_side]);
}

/// A grouped core needs one aggregate; the key-only side's count is dropped
/// by the Project above the product.
const key_only_aggs = [_]ir.AggSpec{.{ .func = .count, .as = "__cross_key_rows" }};

/// The side whose column set alone holds `name`'s suffix.
fn sideOf(sides: *const ColumnSets, name: []const u8) ?usize {
    const s = suffix(name);
    const in_left = contains(sides[0].items, s);
    const in_right = contains(sides[1].items, s);
    if (in_left == in_right) return null;
    return if (in_left) 0 else 1;
}

/// An aggregate whose value over a bag equals its value over the bag's
/// distinct rows: repeating every input row leaves it unchanged.
fn ignoresDuplicates(a: ir.AggSpec) bool {
    if (a.col == null or a.udf_arg_cols.len != 0) return false;
    return switch (a.func) {
        .min, .max, .any_value, .bool_and, .bool_or, .bit_and, .bit_or, .unsigned_bit_and, .unsigned_bit_or, .max_by, .max_by_key, .count_distinct, .sum_distinct, .avg_distinct => true,
        .count, .sum, .avg, .count_if, .first, .last, .bit_xor, .unsigned_bit_xor, .stddev_pop, .stddev_samp, .var_pop, .var_samp, .percentile, .group_concat, .udf => false,
    };
}

/// Append every column `e` reads. Returns false for a subquery or session
/// variable, whose reads no side owns.
fn collectExprRefs(arena: Allocator, e: ir.Expr, out: *std.ArrayListUnmanaged([]const u8)) !bool {
    switch (e) {
        .col_ref => |name| try out.append(arena, name),
        .lit, .null_lit => {},
        .call => |c| for (c.args) |arg| {
            if (!try collectExprRefs(arena, arg, out)) return false;
        },
        .case => |c| {
            for (c.operands) |o| {
                if (!try collectExprRefs(arena, o.expr, out)) return false;
            }
            for (c.branches) |b| {
                try expr_mod.collectCaseConditionRefs(arena, out, c, b.cond);
                if (!try collectExprRefs(arena, b.then, out)) return false;
            }
            if (c.else_branch) |eb| return collectExprRefs(arena, eb.*, out);
        },
        .scalar_subquery, .exists_subquery, .var_ref => return false,
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

test "predicate pushdown: a conjunct pushed onto a filtered side joins that filter" {
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();

    var dummy: ir.Op = .single_row;
    var left_cols = testSelect(&.{ "l.a", "l.b" }, &dummy);
    var left = ir.Op{ .filter = .{ .predicate = .{ .is_not_null = "l.b" }, .upstream = &left_cols } };
    var right = testSelect(&.{ "r.c", "r.d" }, &dummy);
    var join = testJoin(.left, &left, &right);
    const conjuncts = [_]PredicateExpr{ .{ .is_not_null = "l.a" }, .{ .is_null = "r.c" } };
    var op = ir.Op{ .filter = .{ .predicate = .{ .@"and" = &conjuncts }, .upstream = &join } };

    try pushJoinFilters(a, null, .{}, &op);

    try testing.expect(op == .filter);
    const pushed = op.filter.upstream.join.left.*;
    try testing.expect(pushed == .filter);
    try testing.expect(pushed.filter.upstream == &left_cols);
    try testing.expectEqual(@as(usize, 2), pushed.filter.predicate.@"and".len);
    try testing.expectEqualStrings("l.b", pushed.filter.predicate.@"and"[0].is_not_null);
    try testing.expectEqualStrings("l.a", pushed.filter.predicate.@"and"[1].is_not_null);
    try testing.expectEqualStrings("l.b", left.filter.predicate.is_not_null);
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

test "compute push: splits over UNION ALL with matching arm names" {
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();

    var dummy: ir.Op = .single_row;
    var left = testSelect(&.{ "t.a", "t.b" }, &dummy);
    var right = testSelect(&.{ "u.a", "u.b" }, &dummy);
    var un = ir.Op{ .set_union = .{ .left = &left, .right = &right, .all = true } };
    const derived = [_]ir.Derived{.{ .name = "d", .expr = .{ .col_ref = "a" } }};
    var op = ir.Op{ .compute = .{ .derived = &derived, .upstream = &un } };

    try pushComputeThroughUnions(a, null, .{}, &op);

    try testing.expect(op == .set_union);
    try testing.expect(op.set_union.left.* == .compute);
    try testing.expect(op.set_union.right.* == .compute);
    try testing.expect(op.set_union.left.compute.upstream.* == .select);
    try testing.expectEqual(@as(usize, 1), op.set_union.right.compute.derived.len);
}

test "compute push: splits through a single-consumer materialize wrapper" {
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();

    var dummy: ir.Op = .single_row;
    var left = testSelect(&.{"t.a"}, &dummy);
    var right = testSelect(&.{"u.a"}, &dummy);
    var un = ir.Op{ .set_union = .{ .left = &left, .right = &right, .all = true } };
    var mat = ir.Op{ .materialize = .{ .upstream = &un } };
    const derived = [_]ir.Derived{.{ .name = "d", .expr = .{ .col_ref = "a" } }};
    var op = ir.Op{ .compute = .{ .derived = &derived, .upstream = &mat } };

    try pushComputeThroughUnions(a, null, .{}, &op);

    try testing.expect(op == .materialize);
    try testing.expect(op.materialize.upstream.* == .set_union);
    try testing.expect(op.materialize.upstream.set_union.left.* == .compute);
    try testing.expect(op.materialize.upstream.set_union.right.* == .compute);
}

test "compute push: mismatched arm names block the split" {
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();

    var dummy: ir.Op = .single_row;
    var left = testSelect(&.{ "t.a", "t.b" }, &dummy);
    var right = testSelect(&.{ "u.a", "u.c" }, &dummy);
    var un = ir.Op{ .set_union = .{ .left = &left, .right = &right, .all = true } };
    const derived = [_]ir.Derived{.{ .name = "d", .expr = .{ .col_ref = "a" } }};
    var op = ir.Op{ .compute = .{ .derived = &derived, .upstream = &un } };

    try pushComputeThroughUnions(a, null, .{}, &op);

    try testing.expect(op == .compute);
    try testing.expect(op.compute.upstream.* == .set_union);
    try testing.expect(op.compute.upstream.set_union.left.* == .select);
}

test "compute push: shared materialize blocks the split" {
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();

    var dummy: ir.Op = .single_row;
    var left = testSelect(&.{"t.a"}, &dummy);
    var right = testSelect(&.{"u.a"}, &dummy);
    var un = ir.Op{ .set_union = .{ .left = &left, .right = &right, .all = true } };
    var mat = ir.Op{ .materialize = .{ .upstream = &un } };
    const derived = [_]ir.Derived{.{ .name = "d", .expr = .{ .col_ref = "a" } }};
    var comp = ir.Op{ .compute = .{ .derived = &derived, .upstream = &mat } };
    // Second consumer of the same materialize node.
    var other = ir.Op{ .filter = .{ .predicate = .{ .is_not_null = "a" }, .upstream = &mat } };
    var root = ir.Op{ .join = .{
        .algorithm = .auto,
        .join_type = .inner,
        .on = &.{},
        .ranges = &.{},
        .extra_predicate = null,
        .skew_ratio_threshold = 0,
        .skew_absolute_threshold = 0,
        .skew_sample_interval = 0,
        .left = &comp,
        .right = &other,
    } };

    try pushComputeThroughUnions(a, null, .{}, &root);

    try testing.expect(comp == .compute);
    try testing.expect(mat.materialize.upstream.* == .set_union);
    try testing.expect(mat.materialize.upstream.set_union.left.* == .select);
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

fn testCrossGroupBy(keys: []const []const u8, aggs: []const ir.AggSpec, upstream: *ir.Op) ir.Op {
    return .{ .group_by = .{ .group_cols = keys, .aggs = aggs, .upstream = upstream } };
}

test "cross aggregate push: duplicate-insensitive aggregates group each side before the product" {
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();

    var dummy: ir.Op = .single_row;
    var left = testSelect(&.{ "l.a", "l.b" }, &dummy);
    var right = testSelect(&.{"r.c"}, &dummy);
    var join = testJoin(.inner, &left, &right);
    const aggs = [_]ir.AggSpec{
        .{ .func = .max, .col = "l.b", .as = "mb" },
        .{ .func = .count_distinct, .col = "l.b", .as = "nb" },
    };
    var op = testCrossGroupBy(&.{ "l.a", "r.c" }, &aggs, &join);
    var top = testSelect(&.{ "nb", "l.a", "r.c", "mb" }, &op);

    try pushAggregatesThroughCrossJoins(a, null, .{}, &top);

    try testing.expect(op == .select);
    const cols = op.select.columns;
    try testing.expectEqual(@as(usize, 4), cols.len);
    inline for (.{ "l.a", "r.c", "mb", "nb" }, 0..) |want, i| try testing.expectEqualStrings(want, cols[i]);
    const product = op.select.upstream.join;
    try testing.expect(product.left.* == .group_by and product.right.* == .group_by);
    try testing.expectEqualStrings("l.a", product.left.group_by.group_cols[0]);
    try testing.expectEqual(@as(usize, 2), product.left.group_by.aggs.len);
    try testing.expectEqualStrings("r.c", product.right.group_by.group_cols[0]);
    try testing.expect(product.left.group_by.upstream == &left);
    try testing.expect(product.right.group_by.upstream == &right);
}

test "cross aggregate push: a pre-aggregate compute splits onto the sides it reads" {
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();

    var dummy: ir.Op = .single_row;
    var left = testSelect(&.{ "l.a", "l.b" }, &dummy);
    var right = testSelect(&.{"r.c"}, &dummy);
    var join = testJoin(.inner, &left, &right);
    const lower_args = [_]ir.Expr{.{ .col_ref = "l.a" }};
    const next_args = [_]ir.Expr{ .{ .col_ref = "r.c" }, .{ .col_ref = "r.c" } };
    const derived = [_]ir.Derived{
        .{ .name = "la", .expr = .{ .call = .{ .fn_name = "lower", .args = &lower_args } } },
        .{ .name = "cc", .expr = .{ .call = .{ .fn_name = "add", .args = &next_args } } },
    };
    var pre = ir.Op{ .compute = .{ .derived = &derived, .upstream = &join } };
    const aggs = [_]ir.AggSpec{.{ .func = .min, .col = "l.b", .as = "lo" }};
    var op = testCrossGroupBy(&.{ "la", "cc" }, &aggs, &pre);
    const post_args = [_]ir.Expr{ .{ .col_ref = "lo" }, .{ .col_ref = "cc" } };
    const post = [_]ir.Derived{.{ .name = "shifted", .expr = .{ .call = .{ .fn_name = "add", .args = &post_args } } }};
    var above = ir.Op{ .compute = .{ .derived = &post, .upstream = &op } };
    var top = testSelect(&.{ "la", "shifted" }, &above);

    try pushAggregatesThroughCrossJoins(a, null, .{}, &top);

    try testing.expect(op == .select);
    const product = op.select.upstream.join;
    const left_compute = product.left.group_by.upstream.compute;
    const right_compute = product.right.group_by.upstream.compute;
    try testing.expectEqualStrings("la", left_compute.derived[0].name);
    try testing.expectEqualStrings("cc", right_compute.derived[0].name);
    try testing.expect(left_compute.upstream == &left);
    try testing.expect(right_compute.upstream == &right);
}

test "cross aggregate push: shapes the product changes stay as written" {
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();

    var dummy: ir.Op = .single_row;
    var left = testSelect(&.{ "l.a", "l.b" }, &dummy);
    var right = testSelect(&.{ "r.c", "r.d" }, &dummy);
    var cross = testJoin(.inner, &left, &right);
    var keyed = testJoin(.inner, &left, &right);
    keyed.join.on = &.{.{ .left = "l.a", .right = "r.c" }};
    var outer = testJoin(.left, &left, &right);

    const max_b = [_]ir.AggSpec{.{ .func = .max, .col = "l.b", .as = "x" }};
    const sum_b = [_]ir.AggSpec{.{ .func = .sum, .col = "l.b", .as = "x" }};
    const count_rows = [_]ir.AggSpec{.{ .func = .count, .as = "x" }};
    const both_sides = [_]ir.AggSpec{ .{ .func = .max, .col = "l.b", .as = "x" }, .{ .func = .max, .col = "r.d", .as = "y" } };
    const cases = [_]struct { keys: []const []const u8, aggs: []const ir.AggSpec, join: *ir.Op }{
        .{ .keys = &.{ "l.a", "r.c" }, .aggs = &sum_b, .join = &cross },
        .{ .keys = &.{ "l.a", "r.c" }, .aggs = &count_rows, .join = &cross },
        .{ .keys = &.{ "l.a", "r.c" }, .aggs = &both_sides, .join = &cross },
        .{ .keys = &.{"l.a"}, .aggs = &max_b, .join = &cross },
        .{ .keys = &.{ "l.a", "r.c" }, .aggs = &max_b, .join = &keyed },
        .{ .keys = &.{ "l.a", "r.c" }, .aggs = &max_b, .join = &outer },
        .{ .keys = &.{ "l.a", "zz" }, .aggs = &max_b, .join = &cross },
    };
    for (cases) |c| {
        var op = testCrossGroupBy(c.keys, c.aggs, c.join);
        var top = testSelect(&.{"x"}, &op);
        try pushAggregatesThroughCrossJoins(a, null, .{}, &top);
        try testing.expect(op == .group_by);
        try testing.expect(op.group_by.upstream == c.join);
    }
}

test "cross aggregate push: an aggregate whose own names reach the result stays as written" {
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();

    var dummy: ir.Op = .single_row;
    var left = testSelect(&.{ "l.a", "l.b" }, &dummy);
    var right = testSelect(&.{"r.c"}, &dummy);
    var join = testJoin(.inner, &left, &right);
    const aggs = [_]ir.AggSpec{.{ .func = .max, .col = "l.b", .as = "mb" }};

    var bare = testCrossGroupBy(&.{ "l.a", "r.c" }, &aggs, &join);
    try pushAggregatesThroughCrossJoins(a, null, .{}, &bare);
    try testing.expect(bare == .group_by);

    var starred = testCrossGroupBy(&.{ "l.a", "r.c" }, &aggs, &join);
    var star = testSelect(&.{"*"}, &starred);
    try pushAggregatesThroughCrossJoins(a, null, .{}, &star);
    try testing.expect(starred == .group_by);

    var aliased = testCrossGroupBy(&.{ "l.a", "r.c" }, &aggs, &join);
    var derived_table = ir.Op{ .alias = .{ .alias = "g", .upstream = &aliased } };
    var outer = testSelect(&.{ "g.mb", "g.a" }, &derived_table);
    try pushAggregatesThroughCrossJoins(a, null, .{}, &outer);
    try testing.expect(aliased == .group_by);
}

test "cross aggregate push: a chain of cross joins splits at every product" {
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();

    var dummy: ir.Op = .single_row;
    var x = testSelect(&.{ "x.a", "x.v" }, &dummy);
    var y = testSelect(&.{"y.b"}, &dummy);
    var z = testSelect(&.{"z.c"}, &dummy);
    var inner = testJoin(.inner, &x, &y);
    var outer = testJoin(.inner, &inner, &z);
    const aggs = [_]ir.AggSpec{.{ .func = .max, .col = "x.v", .as = "mv" }};
    var op = testCrossGroupBy(&.{ "x.a", "y.b", "z.c" }, &aggs, &outer);
    var top = testSelect(&.{ "mv", "x.a", "y.b", "z.c" }, &op);

    try pushAggregatesThroughCrossJoins(a, null, .{}, &top);

    try testing.expect(op == .select);
    const left = op.select.upstream.join.left;
    try testing.expect(left.* == .select);
    const inner_product = left.select.upstream.join;
    try testing.expect(inner_product.left.group_by.upstream == &x);
    try testing.expect(inner_product.right.group_by.upstream == &y);
    try testing.expect(op.select.upstream.join.right.group_by.upstream == &z);
}
