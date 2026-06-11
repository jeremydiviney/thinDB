//! Staged compilation for plans with structure nodes — materialize
//! boundaries (CTEs and FROM-clause subqueries) and joins — on the V2 path.
//!
//! Every CTE/subquery boundary materializes (see exec/mat_stage.zig for the
//! execution model). Compilation walks the IR's materialize nodes bottom-up:
//! each distinct node becomes a Stage whose body compiles as its own query
//! block — table-sourced blocks run the regular V2 handlers at full
//! parallelism; blocks reading an upstream stage build a generic operator
//! pipeline over a MatScan leaf. The root block compiles last and is wrapped
//! in a StagedRoot that owns the stage set.
//!
//! Joins are pipeline operators, not boundaries: each child compiles as its
//! own block (table / stage / nested join) and `Query.join` routes the
//! algorithm (hash / sort-merge / nested-loop / range-sweep) from the
//! children's stats. The build side drains into the join's own hash table;
//! the probe side and everything above the join stay streaming.
//!
//! Two references to the same node share one stage (MATERIALIZED / default);
//! NOT MATERIALIZED gives each reference its own node — and therefore its own
//! regenerated stage (the parser encodes this distinction).

const std = @import("std");
const Allocator = std.mem.Allocator;

const ir = @import("../ir/ir.zig");
const exec = @import("../exec/exec.zig");
const engine_v2 = @import("../exec/engine_v2.zig");
const mat_stage = @import("../exec/mat_stage.zig");
const local = @import("local.zig");
const types = @import("../types.zig");

const PredicateExpr = exec.predicate.PredicateExpr;

const StageMap = std.AutoHashMapUnmanaged(*const ir.Op, *mat_stage.Stage);

/// True when the plan needs the staged compiler: any materialize boundary
/// (CTE / FROM-subquery), join, or window node anywhere in the tree.
pub fn needsStaging(op: *const ir.Op) bool {
    return switch (op.*) {
        .materialize, .join, .window => true,
        .select => |p| needsStaging(p.upstream),
        .exclude => |p| needsStaging(p.upstream),
        .filter => |f| needsStaging(f.upstream),
        .order_by => |o| needsStaging(o.upstream),
        .group_by => |g| needsStaging(g.upstream),
        .compute => |c| needsStaging(c.upstream),
        .alias => |a| needsStaging(a.upstream),
        .limit => |l| needsStaging(l.upstream),
        .set_union => |u| needsStaging(u.left) or needsStaging(u.right),
        else => false,
    };
}

pub fn compileStaged(input: engine_v2.CompileInput, root: *const ir.Op) anyerror!exec.Query {
    const set = try mat_stage.StageSet.create(input.allocator);
    errdefer set.deinit();
    var map: StageMap = .empty;
    defer map.deinit(input.allocator);
    var refs: MatRefCounts = .empty;
    defer refs.deinit(input.allocator);
    try countMatRefs(input.allocator, root, &refs);
    try collectStages(input, root, set, &map, &refs);
    const inner = try compileBlock(input, root, &map);
    return mat_stage.StagedRoot.create(input.allocator, inner, set);
}

const MatRefCounts = std.AutoHashMapUnmanaged(*const ir.Op, u32);

/// Count how many places in the tree reference each materialize node. A
/// node referenced once gets no stage — its body compiles inline at the
/// use site (materializing only to re-read once is a pure copy tax; this
/// is every FROM-subquery and every NOT MATERIALIZED reference). Shared
/// nodes (a default/MATERIALIZED CTE referenced 2+ times) keep one stage.
/// A shared node's subtree is walked once, mirroring collectStages.
fn countMatRefs(allocator: Allocator, op: *const ir.Op, refs: *MatRefCounts) anyerror!void {
    switch (op.*) {
        .materialize => |m| {
            const gop = try refs.getOrPut(allocator, op);
            if (gop.found_existing) {
                gop.value_ptr.* += 1;
                return;
            }
            gop.value_ptr.* = 1;
            try countMatRefs(allocator, m.upstream, refs);
        },
        .select => |p| try countMatRefs(allocator, p.upstream, refs),
        .exclude => |p| try countMatRefs(allocator, p.upstream, refs),
        .filter => |f| try countMatRefs(allocator, f.upstream, refs),
        .order_by => |o| try countMatRefs(allocator, o.upstream, refs),
        .group_by => |g| try countMatRefs(allocator, g.upstream, refs),
        .compute => |c| try countMatRefs(allocator, c.upstream, refs),
        .alias => |a| try countMatRefs(allocator, a.upstream, refs),
        .limit => |l| try countMatRefs(allocator, l.upstream, refs),
        .window => |w| try countMatRefs(allocator, w.upstream, refs),
        .join => |j| {
            try countMatRefs(allocator, j.left, refs);
            try countMatRefs(allocator, j.right, refs);
        },
        .set_union => |u| {
            try countMatRefs(allocator, u.left, refs);
            try countMatRefs(allocator, u.right, refs);
        },
        else => {},
    }
}

/// Post-order walk: a stage's own upstream stages exist (and are compiled)
/// before the stage's body compiles, so its MatScan leaves can bind.
fn collectStages(
    input: engine_v2.CompileInput,
    op: *const ir.Op,
    set: *mat_stage.StageSet,
    map: *StageMap,
    refs: *const MatRefCounts,
) anyerror!void {
    switch (op.*) {
        .materialize => |m| {
            if (map.contains(op)) return; // shared CTE: one stage, many readers
            try collectStages(input, m.upstream, set, map, refs);
            // Single reference → no stage; the body compiles inline at the
            // use site (buildGenericBlock's .materialize arm). Inner shared
            // nodes were still collected by the recursion above.
            if ((refs.get(op) orelse 1) <= 1) return;
            const q = try compileBlock(input, m.upstream, map);
            const stage = try set.addStage(q);
            try map.put(input.allocator, op, stage);
        },
        .select => |p| try collectStages(input, p.upstream, set, map, refs),
        .exclude => |p| try collectStages(input, p.upstream, set, map, refs),
        .filter => |f| try collectStages(input, f.upstream, set, map, refs),
        .order_by => |o| try collectStages(input, o.upstream, set, map, refs),
        .group_by => |g| try collectStages(input, g.upstream, set, map, refs),
        .compute => |c| try collectStages(input, c.upstream, set, map, refs),
        .alias => |a| try collectStages(input, a.upstream, set, map, refs),
        .limit => |l| try collectStages(input, l.upstream, set, map, refs),
        .window => |w| try collectStages(input, w.upstream, set, map, refs),
        .join => |j| {
            try collectStages(input, j.left, set, map, refs);
            try collectStages(input, j.right, set, map, refs);
        },
        .set_union => |u| {
            try collectStages(input, u.left, set, map, refs);
            try collectStages(input, u.right, set, map, refs);
        },
        else => {},
    }
}

const BlockSource = enum { table, mat, join, window, unsupported };

/// A query block is a linear pipeline; its source is whatever the upstream
/// chain bottoms out at. A window node is a block boundary like a join:
/// everything below it compiles as its own block (a table-backed input gets
/// the full parallel V2 handlers — with no top-end fused, since the window
/// must see every row), and the operators above it run generically over the
/// window's output.
fn blockSource(op: *const ir.Op) BlockSource {
    var cur = op;
    while (true) {
        switch (cur.*) {
            .scan, .file_scan, .single_row => return .table,
            .materialize => return .mat,
            .join => return .join,
            .window => return .window,
            .select => |p| cur = p.upstream,
            .exclude => |p| cur = p.upstream,
            .filter => |f| cur = f.upstream,
            .order_by => |o| cur = o.upstream,
            .group_by => |g| cur = g.upstream,
            .compute => |c| cur = c.upstream,
            .alias => |a| cur = a.upstream,
            .limit => |l| cur = l.upstream,
            else => return .unsupported,
        }
    }
}

fn compileBlock(input: engine_v2.CompileInput, op: *const ir.Op, map: *StageMap) anyerror!exec.Query {
    return switch (blockSource(op)) {
        // Table-backed block: the regular V2 handlers, full parallelism.
        .table => engine_v2.compileSelectBlock(input, op),
        // Stage-, join-, or window-backed block: generic operators over
        // MatScan / Join / Window leaves. The heavy inputs were already
        // produced by upstream stage handlers or stream in from table-backed
        // child blocks.
        .mat, .join, .window => buildGenericBlock(input, op, map, op),
        .unsupported => error.UnsupportedQueryShape,
    };
}

/// A join child is a FROM-clause table reference: a (possibly aliased) table
/// scan, a CTE/subquery reference, or a nested join. An aliased table scan
/// compiles with the alias stripped (the V2 matchers decline aliased scans)
/// and re-qualifies its output names through AliasRename so ON pairs and
/// self-joins disambiguate — the same shape the legacy engine builds.
fn compileJoinChild(input: engine_v2.CompileInput, op: *const ir.Op, map: *StageMap) anyerror!exec.Query {
    if (op.* == .scan) {
        if (op.scan.alias) |alias| {
            const stripped = try input.node_arena.create(ir.Op);
            stripped.* = op.*;
            stripped.scan.alias = null;
            var q = try engine_v2.compileSelectBlock(input, stripped);
            errdefer q.deinit();
            return exec.AliasRename.create(input.allocator, q, alias);
        }
    }
    return compileBlock(input, op, map);
}

fn buildGenericBlock(input: engine_v2.CompileInput, op: *const ir.Op, map: *StageMap, block_root: *const ir.Op) anyerror!exec.Query {
    switch (op.*) {
        .materialize => |m| {
            // In the map = shared (staged once, read here). Absent = single
            // reference: stream the body inline — no stage copy + re-read,
            // and a table-backed body keeps its full V2 handlers.
            if (map.get(op)) |stage| return mat_stage.MatScan.create(input.allocator, stage);
            return compileBlock(input, m.upstream, map);
        },
        .alias => |a| {
            var up = try buildGenericBlock(input, a.upstream, map, block_root);
            errdefer up.deinit();
            return exec.AliasRename.create(input.allocator, up, a.alias);
        },
        .filter => |f| {
            if (f.upstream.* == .join) return compileFilteredJoin(input, f.predicate, f.upstream, map);
            var up = try buildGenericBlock(input, f.upstream, map, block_root);
            errdefer up.deinit();
            return up.filter(f.predicate);
        },
        .select => |s| {
            var up = try buildGenericBlock(input, s.upstream, map, block_root);
            errdefer up.deinit();
            return local.compileSelectProject(input.allocator, up, s);
        },
        .exclude => |e| {
            var up = try buildGenericBlock(input, e.upstream, map, block_root);
            errdefer up.deinit();
            const remaining = try local.complementColumns(input.allocator, up.outputSchema(), e.columns);
            defer input.allocator.free(remaining);
            return up.project(remaining);
        },
        .compute => |c| {
            var up = try buildGenericBlock(input, c.upstream, map, block_root);
            errdefer up.deinit();
            return up.compute(c.derived);
        },
        .order_by => |o| {
            var up = try buildGenericBlock(input, o.upstream, map, block_root);
            errdefer up.deinit();
            return up.orderBy(o.specs);
        },
        .limit => |l| {
            var up = try buildGenericBlock(input, l.upstream, map, block_root);
            errdefer up.deinit();
            return up.limitOffset(@intCast(l.n), @intCast(l.offset));
        },
        .group_by => |g| {
            for (g.aggs) |a| if (a.func == .udf) return error.UnsupportedQueryShape;
            var up = try buildGenericBlock(input, g.upstream, map, block_root);
            errdefer up.deinit();
            // The same strategy routing as the table path: a stage that ends
            // sorted on the group keys streams; a proven-over-budget or
            // unknown-cardinality input sorts then streams; only a proven-
            // small key space takes the hash aggregate.
            return local.routeGroupBy(
                input.allocator,
                &up,
                g.group_cols,
                g.aggs,
                g.top_k,
                g.emit_limit,
                input.db.config.query_memory_budget,
            );
        },
        .window => |w| {
            // The below-window input compiles as its own block: table-backed
            // inputs route through the regular V2 handlers (full parallelism,
            // all-rows/all-groups emission — the window is a barrier and must
            // see everything), stage/join inputs build their generic
            // pipelines. The blocking Window operator accumulates that
            // block's output, evaluates, and emits in input order with the
            // call columns appended.
            var up = try compileBlock(input, w.upstream, map);
            errdefer up.deinit();
            // Prune the window's input to the columns the window or
            // anything above it references: the below block otherwise
            // emits its fused-filter columns too (often a wide string),
            // which would ride the blocking accumulate/sort/emit for
            // nothing. Skipped when the shape above isn't fully understood.
            if (local.windowInputNames(input.allocator, block_root, op)) |names| {
                defer input.allocator.free(names);
                const schema = up.outputSchema();
                var kept = try std.ArrayListUnmanaged([]const u8).initCapacity(input.allocator, schema.len);
                defer kept.deinit(input.allocator);
                for (schema) |col| {
                    for (names) |nm| {
                        if (@import("../types.zig").columnNameEql(col.name, nm)) {
                            kept.appendAssumeCapacity(col.name);
                            break;
                        }
                    }
                }
                if (kept.items.len > 0 and kept.items.len < schema.len) {
                    up = try up.project(kept.items);
                }
            }
            return up.window(w.specs, w.calls, input.db.config.max_dop);
        },
        .join => |j| {
            var left = try compileJoinChild(input, j.left, map);
            errdefer left.deinit();
            const right = try compileJoinChild(input, j.right, map);
            return left.join(right, joinSpecOf(j));
        },
        else => return error.UnsupportedQueryShape,
    }
}

fn joinSpecOf(j: anytype) ir.JoinSpec {
    return .{
        .join_type = j.join_type,
        .algorithm = j.algorithm,
        .on = j.on,
        .ranges = j.ranges,
        .extra_predicate = j.extra_predicate,
        .skew_ratio_threshold = j.skew_ratio_threshold,
        .skew_absolute_threshold = j.skew_absolute_threshold,
        .skew_sample_interval = j.skew_sample_interval,
    };
}

// ---------------------------------------------------------------------------
// WHERE-above-JOIN: basic predicate pushdown into the join inputs.
// ---------------------------------------------------------------------------

const ConjunctSide = enum { left, right, mixed };

/// Compile `filter(pred) -> join(left, right)` splitting the top-level AND
/// conjuncts by which join input their columns resolve to. Single-side
/// conjuncts become filters ON that input — the Filter registers row-group
/// prune hints and offers itself for scan fusion, so only surviving rows are
/// materialized (build side) or streamed (probe side). Pushing below the
/// join is only sound where filtered rows can't resurface as null-extended
/// output: INNER pushes both sides, LEFT only left-side conjuncts, RIGHT
/// only right-side, FULL nothing. Cross-side / unresolvable / subquery
/// conjuncts stay in a residual filter above the join (WHERE semantics).
fn compileFilteredJoin(
    input: engine_v2.CompileInput,
    pred: PredicateExpr,
    join_op: *const ir.Op,
    map: *StageMap,
) anyerror!exec.Query {
    const j = join_op.join;
    const allocator = input.allocator;

    var left = try compileJoinChild(input, j.left, map);
    var left_owned = true;
    errdefer if (left_owned) left.deinit();
    var right = try compileJoinChild(input, j.right, map);
    var right_owned = true;
    errdefer if (right_owned) right.deinit();

    var conjuncts: std.ArrayListUnmanaged(PredicateExpr) = .empty;
    defer conjuncts.deinit(allocator);
    try flattenConjuncts(allocator, pred, &conjuncts);

    var left_push: std.ArrayListUnmanaged(PredicateExpr) = .empty;
    defer left_push.deinit(allocator);
    var right_push: std.ArrayListUnmanaged(PredicateExpr) = .empty;
    defer right_push.deinit(allocator);
    var residual: std.ArrayListUnmanaged(PredicateExpr) = .empty;
    defer residual.deinit(allocator);

    const can_left = j.join_type == .inner or j.join_type == .left;
    const can_right = j.join_type == .inner or j.join_type == .right;
    const left_schema = left.outputSchema();
    const right_schema = right.outputSchema();

    for (conjuncts.items) |c| {
        switch (conjunctSide(c, left_schema, right_schema)) {
            .left => try (if (can_left) &left_push else &residual).append(allocator, c),
            .right => try (if (can_right) &right_push else &residual).append(allocator, c),
            .mixed => try residual.append(allocator, c),
        }
    }

    if (left_push.items.len > 0)
        left = try left.filter(try combineConjuncts(input.node_arena, left_push.items));
    if (right_push.items.len > 0)
        right = try right.filter(try combineConjuncts(input.node_arena, right_push.items));

    left_owned = false;
    right_owned = false;
    var joined = try left.join(right, joinSpecOf(j));
    if (residual.items.len == 0) return joined;
    errdefer joined.deinit();
    return joined.filter(try combineConjuncts(input.node_arena, residual.items));
}

fn flattenConjuncts(
    allocator: std.mem.Allocator,
    e: PredicateExpr,
    out: *std.ArrayListUnmanaged(PredicateExpr),
) anyerror!void {
    switch (e) {
        .@"and" => |children| for (children) |c| try flattenConjuncts(allocator, c, out),
        else => try out.append(allocator, e),
    }
}

/// Conjuncts pushed below the join outlive the Filter that borrows them, so
/// the combined AND node's child slice lives in the statement's node arena.
fn combineConjuncts(arena: Allocator, items: []const PredicateExpr) !PredicateExpr {
    if (items.len == 1) return items[0];
    const children = try arena.alloc(PredicateExpr, items.len);
    @memcpy(children, items);
    return .{ .@"and" = children };
}

fn conjunctSide(c: PredicateExpr, left_schema: []const types.Column, right_schema: []const types.Column) ConjunctSide {
    var side: ?ConjunctSide = null;
    if (!walkConjunctCols(c, left_schema, right_schema, &side)) return .mixed;
    return side orelse .mixed;
}

/// Accumulate the side every column of `e` resolves to. Returns false to
/// keep the conjunct above the join: subquery / correlated / variable
/// markers, a column resolving to both or neither side, or sides mixing.
fn walkConjunctCols(
    e: PredicateExpr,
    ls: []const types.Column,
    rs: []const types.Column,
    side: *?ConjunctSide,
) bool {
    switch (e) {
        .leaf => |lf| return noteCol(lf.col, ls, rs, side),
        .leaf_col_col => |lc| return noteCol(lc.left, ls, rs, side) and noteCol(lc.right, ls, rs, side),
        .is_null, .is_not_null => |col| return noteCol(col, ls, rs, side),
        .like => |lp| return noteCol(lp.col, ls, rs, side),
        .in_set => |s| return noteCol(s.col, ls, rs, side),
        .@"and", .@"or" => |children| {
            for (children) |ch| if (!walkConjunctCols(ch, ls, rs, side)) return false;
            return true;
        },
        .not => |child| return walkConjunctCols(child.*, ls, rs, side),
        .always => return true,
        else => return false,
    }
}

fn noteCol(name: []const u8, ls: []const types.Column, rs: []const types.Column, side: *?ConjunctSide) bool {
    const in_left = types.findColumn(ls, name) != null;
    const in_right = types.findColumn(rs, name) != null;
    var s: ConjunctSide = undefined;
    if (in_left and in_right) {
        // findColumn's qualified-name tail matching can hit BOTH sides
        // (`h.RegionID` tail-matches a bare `RegionID` on the other
        // side). An exact-name match on exactly one side disambiguates;
        // anything else is genuinely ambiguous — keep above the join.
        const exact_left = exactCol(ls, name);
        const exact_right = exactCol(rs, name);
        if (exact_left == exact_right) return false;
        s = if (exact_left) .left else .right;
    } else if (in_left) {
        s = .left;
    } else if (in_right) {
        s = .right;
    } else return false;
    if (side.*) |prev| {
        if (prev != s) return false;
    } else side.* = s;
    return true;
}

fn exactCol(schema: []const types.Column, name: []const u8) bool {
    for (schema) |c| if (types.columnNameEql(c.name, name)) return true;
    return false;
}
