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
const group_route = @import("../exec/group_route.zig");
const partitioned_aggregate = @import("../exec/partitioned_aggregate.zig");
const mat_stage = @import("../exec/mat_stage.zig");
const window_op = @import("../exec/window.zig");
const local = @import("local.zig");
const pgcat = @import("pg_catalog.zig");
const types = @import("../types.zig");
const storage_column = @import("../storage/column.zig");
const engine_mod = @import("../engine/engine.zig");
const core_scheduler = @import("../util/core_scheduler.zig");
const join_mod = @import("../exec/join.zig");

const PredicateExpr = exec.predicate.PredicateExpr;

const StageMap = std.AutoHashMapUnmanaged(*const ir.Op, *mat_stage.Stage);

/// True when the plan needs the staged compiler: any materialize boundary
/// (CTE / FROM-subquery), join, window, or set-union node anywhere in the
/// tree (each union side compiles as its own block). Non-table leaves
/// (FROM-less SELECT, CSV/JSON file scans) also compile here — generic
/// operators over the leaf, no table-backed V2 handler applies.
pub fn needsStaging(op: *const ir.Op) bool {
    return switch (op.*) {
        .materialize, .join, .window, .set_union, .single_row, .file_scan => true,
        .select => |p| needsStaging(p.upstream),
        .exclude => |p| needsStaging(p.upstream),
        .filter => |f| needsStaging(f.upstream),
        .order_by => |o| needsStaging(o.upstream),
        .group_by => |g| needsStaging(g.upstream),
        .compute => |c| needsStaging(c.upstream),
        .alias => |a| needsStaging(a.upstream),
        .limit => |l| needsStaging(l.upstream),
        else => false,
    };
}

/// `stage_count_out` (optional) receives the number of shared stages the
/// plan compiled to — the V2 analogue of the legacy `ctx.materialized`
/// count, used by introspection-style tests.
pub fn compileStaged(input: engine_v2.CompileInput, root: *const ir.Op, stage_count_out: ?*u32) anyerror!exec.Query {
    const set = try mat_stage.StageSet.create(input.allocator);
    errdefer set.deinit();
    var map: StageMap = .empty;
    defer map.deinit(input.allocator);

    var enc_arena = std.heap.ArenaAllocator.init(input.allocator);
    defer enc_arena.deinit();
    var cse: MatCse = .{ .enc_arena = enc_arena.allocator() };
    defer {
        cse.refs.deinit(input.allocator);
        cse.canon.deinit(input.allocator);
    }
    try countMatRefs(input.allocator, root, &cse);
    wrapWindowsInMaterialize(input.node_arena, @constCast(root), &cse) catch {};
    try collectStages(input, root, set, &map, &cse, null, null);
    if (stage_count_out) |out| out.* = @intCast(set.stages.items.len);
    const inner = try compileBlock(input, root, &map);
    set.releaseCompilePins();
    return mat_stage.StagedRoot.create(input.allocator, inner, set);
}

/// Pre-execution rewrite (window-output-as-stage): wrap every `.window`
/// node in a synthetic forced `.materialize` so the staged compiler gives
/// it a real stage. The stage adopts the window's materialized buffers
/// zero-copy (Stage.adopt_window), and everything ABOVE the window then
/// compiles as a chain over a stage — the existing parallel buffer-scan
/// machinery — instead of pulling the window's emit serially. A window
/// that is already a materialize body double-wraps harmlessly: the outer
/// (single-ref) node inlines and reads the inner forced stage.
fn wrapWindowsInMaterialize(node_arena: Allocator, op: *ir.Op, cse: *const MatCse) anyerror!void {
    switch (op.*) {
        .select => |*p| try wrapWindowChild(node_arena, &p.upstream, cse),
        .exclude => |*p| try wrapWindowChild(node_arena, &p.upstream, cse),
        .filter => |*f| try wrapWindowChild(node_arena, &f.upstream, cse),
        .order_by => |*o| try wrapWindowChild(node_arena, &o.upstream, cse),
        .group_by => |*g| try wrapWindowChild(node_arena, &g.upstream, cse),
        .compute => |*c| try wrapWindowChild(node_arena, &c.upstream, cse),
        .alias => |*a| try wrapWindowChild(node_arena, &a.upstream, cse),
        .limit => |*l| try wrapWindowChild(node_arena, &l.upstream, cse),
        .window => |*w| try wrapWindowChild(node_arena, &w.upstream, cse),
        .materialize => |*m| {
            // A CTE that will stage anyway (multi-ref or AS MATERIALIZED)
            // and whose body IS the window adopts it directly through its
            // own stage — an inner wrap would only add a copy layer. A
            // single-ref (inlining) CTE body still wants the wrap.
            const rep = cse.canon.get(op) orelse op;
            const stages_anyway = (cse.refs.get(rep) orelse 1) > 1 or op.materialize.forced;
            if (stages_anyway and m.upstream.* == .window) {
                try wrapWindowsInMaterialize(node_arena, m.upstream, cse);
            } else {
                try wrapWindowChild(node_arena, &m.upstream, cse);
            }
        },
        .join => |*j| {
            try wrapWindowChild(node_arena, &j.left, cse);
            try wrapWindowChild(node_arena, &j.right, cse);
        },
        .set_union => |*u| {
            try wrapWindowChild(node_arena, &u.left, cse);
            try wrapWindowChild(node_arena, &u.right, cse);
        },
        else => {},
    }
}

fn wrapWindowChild(node_arena: Allocator, child: **ir.Op, cse: *const MatCse) anyerror!void {
    const c = child.*;
    if (c.* == .window) {
        const wrap = try node_arena.create(ir.Op);
        wrap.* = .{ .materialize = .{ .upstream = c, .forced = true } };
        child.* = wrap;
    }
    try wrapWindowsInMaterialize(node_arena, c, cse);
}

const MatRefCounts = std.AutoHashMapUnmanaged(*const ir.Op, u32);

/// Reference counting + structural CSE state for materialize nodes.
/// `canon` maps a duplicate node (a FROM-subquery whose IR encodes
/// byte-identically to an earlier one) to its representative; `refs`
/// counts references per CANONICAL node. Encoding scratch and the
/// bytes→node table live in `enc_arena` (freed after stage collection).
const MatCse = struct {
    refs: MatRefCounts = .empty,
    canon: std.AutoHashMapUnmanaged(*const ir.Op, *const ir.Op) = .empty,
    bodies: std.StringHashMapUnmanaged(*const ir.Op) = .empty,
    enc_arena: Allocator,
};

/// Count how many places in the tree reference each materialize node,
/// merging structurally identical bodies (byte-equal IR encodings) into
/// one canonical node first — so two copies of the same FROM-subquery
/// (a self-join over identical subqueries) count as TWO references to
/// ONE node and share a stage instead of scanning twice. A canonical
/// node referenced once gets no stage — its body compiles inline at the
/// use site (materializing only to re-read once is a pure copy tax).
/// A shared node's subtree is walked once, mirroring collectStages.
fn countMatRefs(allocator: Allocator, op: *const ir.Op, cse: *MatCse) anyerror!void {
    switch (op.*) {
        .materialize => |m| {
            const rep: *const ir.Op = blk: {
                var buf: std.ArrayList(u8) = .empty;
                ir.encode(cse.enc_arena, &buf, op.*) catch break :blk op; // unencodable: no CSE
                const gop = try cse.bodies.getOrPut(cse.enc_arena, buf.items);
                if (gop.found_existing) break :blk gop.value_ptr.*;
                gop.value_ptr.* = op;
                break :blk op;
            };
            if (rep != op) try cse.canon.put(allocator, op, rep);
            const gop = try cse.refs.getOrPut(allocator, rep);
            if (gop.found_existing) {
                gop.value_ptr.* += 1;
                return;
            }
            gop.value_ptr.* = 1;
            try countMatRefs(allocator, m.upstream, cse);
        },
        .select => |p| try countMatRefs(allocator, p.upstream, cse),
        .exclude => |p| try countMatRefs(allocator, p.upstream, cse),
        .filter => |f| try countMatRefs(allocator, f.upstream, cse),
        .order_by => |o| try countMatRefs(allocator, o.upstream, cse),
        .group_by => |g| try countMatRefs(allocator, g.upstream, cse),
        .compute => |c| try countMatRefs(allocator, c.upstream, cse),
        .alias => |a| try countMatRefs(allocator, a.upstream, cse),
        .limit => |l| try countMatRefs(allocator, l.upstream, cse),
        .window => |w| try countMatRefs(allocator, w.upstream, cse),
        .join => |j| {
            try countMatRefs(allocator, j.left, cse);
            try countMatRefs(allocator, j.right, cse);
        },
        .set_union => |u| {
            try countMatRefs(allocator, u.left, cse);
            try countMatRefs(allocator, u.right, cse);
        },
        else => {},
    }
}

/// Post-order walk: a stage's own upstream stages exist (and are compiled)
/// before the stage's body compiles, so its MatScan leaves can bind.
const PrivSet = std.AutoHashMapUnmanaged(*const ir.Op, void);

/// Count canonical materialize references WITHIN one subtree (read-only
/// against the statement-wide CSE). A node whose in-body count equals its
/// statement-wide count is PRIVATE to that body: no consumer outside the
/// separable block reads it, so it needn't exist as a shared stage — each
/// slice computes its own 1/N-sized copy.
/// Conservative IR-level check: does this block's OUTPUT provably carry
/// every named column? Walks projection-transparent ops; a .select decides
/// (its columns/outputs ARE the projection); a .compute adds derived names
/// and keeps looking for the rest. Anything else (group_by, window, join,
/// star-less shapes) = unknown → false. Used to decide whether a multi-ref
/// private block can be pre-partitioned by the slice key at all.
fn irBlockCarriesCols(op: *const ir.Op, cols: []const []const u8) bool {
    for (cols) |c| {
        if (!irBlockCarriesCol(op, c)) return false;
    }
    return true;
}

/// True when the block is a plain projection chain over a table scan —
/// select/compute/filter/alias/limit/order_by only. This is the shape the
/// scan-once pre-partition is proven on; blocks rooted in group_by/window/
/// join/union stay shared (draining a parallel-aggregate-rooted block
/// through the router crashes — bug filed, dodge until fixed).
fn irBlockIsSimpleTableChain(op: *const ir.Op) bool {
    var cur = op;
    while (true) switch (cur.*) {
        .scan => return true,
        .select => |p| cur = p.upstream,
        .exclude => |p| cur = p.upstream,
        .filter => |f| cur = f.upstream,
        .compute => |c| cur = c.upstream,
        .alias => |a| cur = a.upstream,
        .limit => |l| cur = l.upstream,
        .order_by => |o| cur = o.upstream,
        else => return false,
    };
}

fn irBlockCarriesCol(op: *const ir.Op, col: []const u8) bool {
    var cur = op;
    while (true) switch (cur.*) {
        .select => |p| {
            if (p.outputs) |outs| {
                for (p.columns, outs) |src, out_opt| {
                    const name = out_opt orelse src;
                    if (types.columnNameEql(name, col)) return true;
                }
            } else for (p.columns) |src| {
                if (types.columnNameEql(src, col)) return true;
            }
            return false; // the projection decides — col not among it
        },
        .compute => |c| {
            for (c.derived) |d| {
                if (types.columnNameEql(d.name, col)) return true;
            }
            cur = c.upstream;
        },
        .filter => |f| cur = f.upstream,
        .order_by => |o| cur = o.upstream,
        .alias => |a| cur = a.upstream,
        .limit => |l| cur = l.upstream,
        else => return false, // unknown projection: assume absent
    };
}

fn countBodyMatRefs(
    allocator: Allocator,
    op: *const ir.Op,
    cse: *const MatCse,
    out: *MatRefCounts,
) anyerror!void {
    switch (op.*) {
        .materialize => |m| {
            const rep = cse.canon.get(op) orelse op;
            const gop = try out.getOrPut(allocator, rep);
            if (gop.found_existing) {
                gop.value_ptr.* += 1;
                return;
            }
            gop.value_ptr.* = 1;
            try countBodyMatRefs(allocator, m.upstream, cse, out);
        },
        .select => |p| try countBodyMatRefs(allocator, p.upstream, cse, out),
        .exclude => |p| try countBodyMatRefs(allocator, p.upstream, cse, out),
        .filter => |f| try countBodyMatRefs(allocator, f.upstream, cse, out),
        .order_by => |o| try countBodyMatRefs(allocator, o.upstream, cse, out),
        .group_by => |g| try countBodyMatRefs(allocator, g.upstream, cse, out),
        .compute => |c| try countBodyMatRefs(allocator, c.upstream, cse, out),
        .alias => |a| try countBodyMatRefs(allocator, a.upstream, cse, out),
        .limit => |l| try countBodyMatRefs(allocator, l.upstream, cse, out),
        .window => |w| try countBodyMatRefs(allocator, w.upstream, cse, out),
        .join => |j| {
            try countBodyMatRefs(allocator, j.left, cse, out);
            try countBodyMatRefs(allocator, j.right, cse, out);
        },
        .set_union => |u| {
            try countBodyMatRefs(allocator, u.left, cse, out);
            try countBodyMatRefs(allocator, u.right, cse, out);
        },
        else => {},
    }
}

fn collectStages(
    input: engine_v2.CompileInput,
    op: *const ir.Op,
    set: *mat_stage.StageSet,
    map: *StageMap,
    cse: *const MatCse,
    inherited: ?ir.SeparableSpec,
    priv: ?*const PrivSet,
) anyerror!void {
    switch (op.*) {
        .materialize => {
            // Stage decisions run on the CANONICAL node; a structural
            // duplicate gets an alias entry pointing at the shared stage
            // so buildGenericBlock resolves either pointer.
            const rep = cse.canon.get(op) orelse op;
            if (map.get(rep)) |stage| {
                if (rep != op) try map.put(input.allocator, op, stage);
                return; // shared CTE / duplicate: one stage, many readers
            }
            // PRIVATE to an enclosing separable block: no shared stage —
            // each slice pipeline computes its own 1/N-sized copy inside
            // its per-slice StageSet. Still recurse: a deeper node with
            // consumers OUTSIDE the block is shared and stages normally.
            if (priv) |p| {
                if (p.contains(rep)) {
                    try collectStages(input, rep.materialize.upstream, set, map, cse, inherited, priv);
                    return;
                }
            }
            // A SEPARABLE declaration covers the block's own internals:
            // synthetic window wraps created inside a marked body inherit
            // the spec (windows partitioned by the key are per-key by the
            // author's assertion). Only window wraps inherit — a shared
            // sub-CTE first encountered here may have consumers OUTSIDE the
            // marked block, where no separability was asserted.
            const descend = rep.materialize.separable orelse inherited;
            // A separable block's private upstream closure stays out of the
            // global stage set entirely — the flow-through shape: each slice
            // runs the whole private chain end-to-end on its key range.
            var body_priv: ?PrivSet = null;
            defer if (body_priv) |*bp| bp.deinit(input.allocator);
            const child_priv: ?*const PrivSet = blk: {
                if (rep.materialize.separable == null or input.dop_cap != null) break :blk priv;
                var body_refs: MatRefCounts = .empty;
                defer body_refs.deinit(input.allocator);
                try countBodyMatRefs(input.allocator, rep.materialize.upstream, cse, &body_refs);
                var bp: PrivSet = .empty;
                errdefer bp.deinit(input.allocator);
                const sep_cols = rep.materialize.separable.?.cols;
                var it = body_refs.iterator();
                while (it.next()) |e| {
                    const total = cse.refs.get(e.key_ptr.*) orelse 1;
                    if (e.value_ptr.* < total) continue;
                    // A multi-ref block stays PRIVATE only when the scan-once
                    // pre-partition can actually route it: table-backed AND
                    // its output provably carries the slice key. Everything
                    // else stays SHARED — a normal stage (computed once with
                    // the full machinery) that every slice reads through its
                    // range-filtered MatScan; never recomputed per slice.
                    // A multi-ref block whose output CARRIES the slice key
                    // stays PRIVATE: per-slice it costs ~1/N (the range
                    // predicate constrains it), and simple table chains
                    // additionally get the scan-once pre-partition. A
                    // key-less multi-ref block is DEMOTED to a normal
                    // shared stage — computed once with the full machinery
                    // and read by every slice — because recomputing it per
                    // slice costs N x its full price.
                    const carries = irBlockCarriesCols(e.key_ptr.*.materialize.upstream, sep_cols);
                    if (getenv("THINDB_TRACE_SEP") != null and total >= 2) {
                        std.debug.print("[sep] priv-decide {*}: refs={d} carries={} -> {s}\n", .{ e.key_ptr.*, total, carries, if (carries) "PRIVATE" else "DEMOTE-SHARED" });
                    }
                    if (total >= 2 and !carries) continue;
                    try bp.put(input.allocator, e.key_ptr.*, {});
                }
                body_priv = bp;
                break :blk &body_priv.?;
            };
            try collectStages(input, rep.materialize.upstream, set, map, cse, descend, child_priv);
            // Single reference → no stage; the body compiles inline at the
            // use site (buildGenericBlock's .materialize arm) — including
            // UNION bodies, which stream arm-then-arm through exec.SetUnion
            // (they previously staged for an exact downstream row count; the
            // summed-arm estimate proved cheaper than a full buffer copy).
            // Inner shared nodes were still collected by the recursion above.
            // An explicit `AS MATERIALIZED` CTE stages regardless — the user
            // demanded a real buffer (and the budget charge that comes with
            // it).
            const single_ref = (cse.refs.get(rep) orelse 1) <= 1;
            // Profiling overrides that stage normally-inlined single-ref CTEs so
            // `--profile-ops` emits a distinct `[cte]` line per block (both
            // correctness-neutral, both add the copy tax inlining avoids):
            //   THINDB_PROFILE_FORCE_STAGE   — EVERY CTE (incl. pure streaming).
            //   THINDB_PROFILE_STAGE_BARRIERS — only CTEs whose own operations
            //     already form a barrier (GROUP BY / ORDER BY / WINDOW / JOIN /
            //     UNION) — those buffer internally anyway, so staging them is
            //     close to the real execution; thin streaming CTEs stay fused.
            const prof_stage = forceStageAll() or
                (stageBarriersOnly() and bodyFormsBarrier(rep.materialize.upstream));
            // Inside a SEPARABLE slice (dop_cap set), the synthetic window
            // wraps lose their purpose: they exist so DOP>1 chains can
            // parallel-scan a window's buffered output, but a slice has ONE
            // serial consumer — staging just adds a full write + re-read of
            // memory the 12 concurrent slices then fight over. Inline them:
            // the Window op streams straight into its consumer.
            const inline_slice_window = input.dop_cap != null and
                rep.materialize.upstream.* == .window and single_ref;
            if (inline_slice_window) return;
            if ((single_ref or noStage()) and !rep.materialize.forced and !prof_stage) return;
            const c0 = if (exec.prof.enabled) exec.prof.nowTicks() else 0;
            var q = try compileBlock(input, rep.materialize.upstream, map);
            // A window-rooted body hands its buffers to the stage zero-copy
            // (Stage.adopt_window); output pruning would wrap a Project over
            // the window and break the root identity — and with adoption the
            // copy it would save no longer exists.
            const win_root = exec.queryAs(window_op.Window, q);
            if (win_root == null) q = pruneStageColumns(input, q);
            const stage = try set.addStage(q, input.accountant);
            stage.slice_local = input.dop_cap != null;
            if (stage.slice_local and getenv("THINDB_TRACE_SEP") != null and stage.id < 20) {
                var buf: std.ArrayList(u8) = .empty;
                defer buf.deinit(input.allocator);
                var qq = q;
                qq.explain(&buf, input.allocator, 0) catch {};
                const first = if (std.mem.indexOfScalar(u8, buf.items, '\n')) |nl| buf.items[0..nl] else buf.items;
                std.debug.print("[sep] slice-stage#{d} = {s}\n", .{ stage.id, first });
            }
            stage.adopt_window = win_root;
            if (win_root) |wr| {
                if (wr.borrow_src) |src| {
                    src.registerUse();
                    stage.pinned_upstream = src;
                }
            }
            if (exec.prof.enabled) stage.setup_ticks = exec.prof.nowTicks() - c0;
            const eff: ?ir.SeparableSpec = rep.materialize.separable orelse
                (if (rep.materialize.upstream.* == .window) inherited else null);
            // dop_cap set = we ARE a slice pipeline's stage collection —
            // never nest a fan-out inside a slice.
            if (eff != null and input.dop_cap == null) {
                const spec = eff.?;
                stage.sliced_fill = try makeSlicedFill(input, spec, rep.materialize.upstream, map, cse);
                // Slice concat only preserves order when the leading sort key
                // IS the slice key; drop the claim otherwise so no downstream
                // rider trusts an interleaved order.
                const keep_sort = spec.cols.len == 1 and stage.sort_state.keys.len > 0 and
                    types.columnNameEql(stage.sort_state.keys[0], spec.cols[0]);
                if (!keep_sort) stage.sort_state = .{};
            }
            try map.put(input.allocator, rep, stage);
            if (rep != op) try map.put(input.allocator, op, stage);
        },
        .select => |p| try collectStages(input, p.upstream, set, map, cse, inherited, priv),
        .exclude => |p| try collectStages(input, p.upstream, set, map, cse, inherited, priv),
        .filter => |f| try collectStages(input, f.upstream, set, map, cse, inherited, priv),
        .order_by => |o| try collectStages(input, o.upstream, set, map, cse, inherited, priv),
        .group_by => |g| try collectStages(input, g.upstream, set, map, cse, inherited, priv),
        .compute => |c| try collectStages(input, c.upstream, set, map, cse, inherited, priv),
        .alias => |a| try collectStages(input, a.upstream, set, map, cse, inherited, priv),
        .limit => |l| try collectStages(input, l.upstream, set, map, cse, inherited, priv),
        .window => |w| try collectStages(input, w.upstream, set, map, cse, inherited, priv),
        .join => |j| {
            try collectStages(input, j.left, set, map, cse, inherited, priv);
            try collectStages(input, j.right, set, map, cse, inherited, priv);
        },
        .set_union => |u| {
            try collectStages(input, u.left, set, map, cse, inherited, priv);
            try collectStages(input, u.right, set, map, cse, inherited, priv);
        },
        else => {},
    }
}

const BlockSource = enum { table, leaf, mat, join, window, set_union, unsupported };

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
            .scan => return .table,
            .file_scan, .single_row => return .leaf,
            .materialize => return .mat,
            .join => return .join,
            .window => return .window,
            .set_union => return .set_union,
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

threadlocal var compile_block_depth: usize = 0;

fn compileBlock(input: engine_v2.CompileInput, op: *const ir.Op, map: *StageMap) anyerror!exec.Query {
    // Recursion guard: a compile that nests blocks this deep is either a
    // pathological plan or a walk cycle — fail the query instead of
    // silently smashing the stack (Windows kills without a trace). The
    // dump past the threshold shows the repeating node pattern.
    compile_block_depth += 1;
    defer compile_block_depth -= 1;
    if (compile_block_depth > 5000) {
        if (compile_block_depth < 5030) {
            std.debug.print("[depth] compileBlock {d}: {s} {*}\n", .{ compile_block_depth, @tagName(op.*), op });
            return error.UnsupportedQueryShape;
        }
        return error.UnsupportedQueryShape;
    }
    return switch (blockSource(op)) {
        // Table-backed block: the regular V2 handlers, full parallelism.
        // pg_catalog virtual tables are in-memory metadata batches, not
        // columnar scans — they build generically like the other leaves.
        .table => if (blockScanIsPgCatalog(input, op))
            buildGenericBlock(input, op, map, op)
        else
            wrapSlicePred(input, try engine_v2.compileSelectBlock(input, op)),
        // Stage-, join-, window-, union- or non-table-leaf-backed block:
        // generic operators over MatScan / Join / Window / SetUnion /
        // SingleRow / FileScan leaves. The heavy inputs were already produced
        // by upstream stage handlers or stream in from table-backed child
        // blocks; single-row and file leaves are not perf shapes.
        .leaf, .mat, .join, .window, .set_union => buildGenericBlock(input, op, map, op),
        .unsupported => error.UnsupportedQueryShape,
    };
}

/// A join child is a FROM-clause table reference: a (possibly aliased) table
/// scan, a CTE/subquery reference, or a nested join. An aliased table scan
/// compiles with the alias stripped (the V2 matchers decline aliased scans)
/// and re-qualifies its output names through AliasRename so ON pairs and
/// self-joins disambiguate — the same shape the legacy engine builds.
fn compileJoinChild(input: engine_v2.CompileInput, op: *const ir.Op, map: *StageMap, is_probe: bool) anyerror!exec.Query {
    if (op.* == .scan) {
        if (try tryPgCatalogLeaf(input, op.scan)) |q| return q;
        if (op.scan.alias) |alias| {
            const stripped = try input.node_arena.create(ir.Op);
            stripped.* = op.*;
            stripped.scan.alias = null;
            var q = try engine_v2.compileSelectBlock(input, stripped);
            errdefer q.deinit();
            return exec.AliasRename.create(input.allocator, q, alias);
        }
    }
    // A single-table child whose aliased scan is wrapped (e.g. a derived ON
    // key `lower(l.tag)` puts a Compute above the scan): the alias is no
    // longer the direct child, so strip it from the bottom scan, compile the
    // whole block bare, and re-qualify its output through one AliasRename.
    // A multi-table (join-bottomed) child returns null here — its leaf scans
    // qualify themselves through their own compileJoinChild calls.
    if (singleAliasedScanAlias(op)) |alias| {
        const stripped = try cloneStrippedScanAlias(input.node_arena, op);
        var q = try compileBlock(input, stripped, map);
        errdefer q.deinit();
        return exec.AliasRename.create(input.allocator, q, alias);
    }
    // A probe-side join child that is a streaming chain over one stage —
    // even with no per-row work, and seeing through single-ref inline
    // materialize wrappers — compiles as a DEFERRED parallel buffer scan:
    // the join fuses its probe into the stripe workers (and everything
    // above chains into the same pipeline) instead of probing serially off
    // a MatScan. The deferred leaf materializes at first pull (normal DAG
    // order) and releases its stage use at drain exhaustion, so its memory
    // profile matches the MatScan it replaces.
    if (input.effectiveDop() > 1 and is_probe and
        streamingChainOverStage(op, map, false, true))
    {
        return buildFusedStreamOverStage(input, op, map, true);
    }
    return compileBlock(input, op, map);
}

/// If `op` is a single-table block whose bottom is an aliased scan, return
/// that alias; null when it bottoms out in a join (multi-table) or an
/// unaliased / non-scan leaf.
fn singleAliasedScanAlias(op: *const ir.Op) ?[]const u8 {
    var cur = op;
    while (true) {
        switch (cur.*) {
            .scan => |s| return s.alias,
            .compute => |c| cur = c.upstream,
            .filter => |f| cur = f.upstream,
            else => return null,
        }
    }
}

/// Clone the single-upstream wrapper chain above an aliased scan, nulling the
/// scan's alias so the block compiles bare; the caller re-qualifies the whole
/// output through one AliasRename. Never mutates the shared IR node.
fn cloneStrippedScanAlias(arena: std.mem.Allocator, op: *const ir.Op) !*ir.Op {
    const out = try arena.create(ir.Op);
    out.* = op.*;
    switch (op.*) {
        .scan => out.scan.alias = null,
        .compute => |c| out.compute.upstream = try cloneStrippedScanAlias(arena, c.upstream),
        .filter => |f| out.filter.upstream = try cloneStrippedScanAlias(arena, f.upstream),
        else => unreachable,
    }
    return out;
}

/// Build the in-memory PgCatalogSource leaf (alias-renamed if the scan is
/// aliased) when `s` names a pg_catalog virtual table — PG/neutral dialects
/// only, MySQL resolves the same name as a real table. Null = real scan.
fn tryPgCatalogLeaf(input: engine_v2.CompileInput, s: anytype) !?exec.Query {
    if (input.session.dialect == .mysql) return null;
    const vt = pgcat.match(s.table) orelse return null;
    const catalog = local.catalogFor(input.db) orelse return local.Error.DatabaseNotFound;
    const base = try pgcat.build(input.allocator, catalog, input.session, vt);
    if (s.alias) |alias| {
        errdefer @constCast(&base).deinit();
        return try exec.AliasRename.create(input.allocator, base, alias);
    }
    return base;
}

/// Whether the block's bottom scan is a pg_catalog virtual table (the walk
/// mirrors blockSource; only called for `.table`-sourced blocks).
fn blockScanIsPgCatalog(input: engine_v2.CompileInput, op: *const ir.Op) bool {
    if (input.session.dialect == .mysql) return false;
    var cur = op;
    while (true) {
        switch (cur.*) {
            .scan => |s| return pgcat.match(s.table) != null,
            .select => |p| cur = p.upstream,
            .exclude => |p| cur = p.upstream,
            .filter => |f| cur = f.upstream,
            .order_by => |o| cur = o.upstream,
            .group_by => |g| cur = g.upstream,
            .compute => |c| cur = c.upstream,
            .alias => |a| cur = a.upstream,
            .limit => |l| cur = l.upstream,
            else => return false,
        }
    }
}

/// Collect the materialized stages a generic block reads from (directly or
/// through inline joins/unions/single-ref bodies). Filling these before the
/// block's GROUP BY routes gives the router EXACT realized row counts instead
/// of pre-run estimates. Deduplicated; `out` is caller-owned.
fn collectBlockStages(
    allocator: Allocator,
    op: *const ir.Op,
    map: *StageMap,
    out: *std.ArrayListUnmanaged(*mat_stage.Stage),
) anyerror!void {
    switch (op.*) {
        .materialize => |m| {
            if (map.get(op)) |stage| {
                for (out.items) |s| if (s == stage) return;
                try out.append(allocator, stage);
            } else try collectBlockStages(allocator, m.upstream, map, out);
        },
        .select => |p| try collectBlockStages(allocator, p.upstream, map, out),
        .exclude => |p| try collectBlockStages(allocator, p.upstream, map, out),
        .filter => |f| try collectBlockStages(allocator, f.upstream, map, out),
        .order_by => |o| try collectBlockStages(allocator, o.upstream, map, out),
        .group_by => |g| try collectBlockStages(allocator, g.upstream, map, out),
        .compute => |c| try collectBlockStages(allocator, c.upstream, map, out),
        .alias => |a| try collectBlockStages(allocator, a.upstream, map, out),
        .limit => |l| try collectBlockStages(allocator, l.upstream, map, out),
        .window => |w| try collectBlockStages(allocator, w.upstream, map, out),
        .join => |j| {
            try collectBlockStages(allocator, j.left, map, out);
            try collectBlockStages(allocator, j.right, map, out);
        },
        .set_union => |u| {
            try collectBlockStages(allocator, u.left, map, out);
            try collectBlockStages(allocator, u.right, map, out);
        },
        else => {},
    }
}

/// Defers the GROUP BY hash-vs-sort decision to first `next()`: it `ensureRun`s
/// the materialized stages its input reads, so `routeGroupBy` sees the realized
/// row count (not a pre-run estimate) — the right call for deep CTE chains
/// where an estimate compounds badly. EXPLAIN never calls `next()`, so the plan
/// stays cheap to print and the stages aren't run.
const AdaptiveGroupBy = struct {
    allocator: Allocator,
    up: exec.Query,
    stages: []*mat_stage.Stage,
    group_cols: []const []const u8,
    aggs: []const ir.AggSpec,
    top_k: ?ir.Op.TopK,
    emit_limit: ?u32,
    budget: usize,
    max_dop: usize,
    output_schema: []types.Column,
    chosen: ?exec.Query = null,
    routed: bool = false,

    fn create(
        allocator: Allocator,
        up: exec.Query,
        stages: []*mat_stage.Stage,
        group_cols: []const []const u8,
        aggs: []const ir.AggSpec,
        top_k: ?ir.Op.TopK,
        emit_limit: ?u32,
        budget: usize,
        max_dop: usize,
    ) !exec.Query {
        const schema = try exec.aggregate_op.outputSchemaFor(allocator, up.outputSchema(), group_cols, aggs);
        errdefer allocator.free(schema);
        const self = try allocator.create(AdaptiveGroupBy);
        self.* = .{
            .allocator = allocator,
            .up = up,
            .stages = stages,
            .group_cols = group_cols,
            .aggs = aggs,
            .top_k = top_k,
            .emit_limit = emit_limit,
            .budget = budget,
            .max_dop = max_dop,
            .output_schema = schema,
        };
        return exec.makeQuery(allocator, self);
    }

    fn ensureRouted(self: *AdaptiveGroupBy) !void {
        if (self.routed) return;
        self.routed = true;
        for (self.stages) |s| try s.ensureRun();
        // A plain (no top-k / no limit) GROUP BY over a buffer with a realized
        // row count worth threading: try the specialized serial-beating paths
        // (sorted-stream, radix) first, and if both decline — the string-key /
        // MAX_BY / ANY_VALUE shapes the radix path can't carry — partition the
        // input across cores instead of falling to the serial hash aggregate.
        if (self.group_cols.len > 0 and self.max_dop > 1 and self.top_k == null and self.emit_limit == null and
            self.up.stats().upper_rows >= partitioned_aggregate.MIN_ROWS_FOR_PARALLEL and
            getenv("THINDB_NO_PARALLEL_GROUP") == null)
        {
            if (try group_route.routeStreamGroupBy(self.allocator, &self.up, self.group_cols, self.aggs, self.budget)) |q| {
                self.chosen = q;
                return;
            }
            if (try group_route.routeRadixGroupBy(self.up, self.group_cols, self.aggs, self.top_k, self.emit_limit)) |q| {
                self.chosen = q;
                return;
            }
            self.chosen = try partitioned_aggregate.PartitionedAggregate.create(
                self.allocator,
                self.up,
                self.group_cols,
                self.aggs,
                self.max_dop,
            );
            return;
        }
        self.chosen = try group_route.routeGroupBy(
            self.allocator,
            &self.up,
            self.group_cols,
            self.aggs,
            self.top_k,
            self.emit_limit,
            self.budget,
        );
    }

    pub fn next(self: *AdaptiveGroupBy) !?exec.Batch {
        try self.ensureRouted();
        return self.chosen.?.next();
    }

    pub fn outputSchema(self: *AdaptiveGroupBy) []const types.Column {
        return self.output_schema;
    }

    pub fn addPrune(_: *AdaptiveGroupBy, _: exec.Predicate) !void {}

    pub fn stats(self: *AdaptiveGroupBy) exec.PipelineStats {
        if (self.chosen) |c| return c.stats();
        return .{ .upper_rows = self.up.stats().upper_rows };
    }

    pub fn accountant(self: *AdaptiveGroupBy) ?*exec.memory.MemoryAccountant {
        if (self.chosen) |c| return c.accountant();
        return self.up.accountant();
    }

    pub fn explain(self: *AdaptiveGroupBy, out: *std.ArrayList(u8), alloc: Allocator, depth: usize) anyerror!void {
        if (self.chosen) |c| return c.explain(out, alloc, depth);
        try exec.explainLine(out, alloc, depth, "AdaptiveGroupBy (routes at runtime)");
        try self.up.explain(out, alloc, depth + 1);
    }

    pub fn deinit(self: *AdaptiveGroupBy) void {
        if (self.chosen) |*c| c.deinit() else self.up.deinit();
        self.allocator.free(self.stages);
        self.allocator.free(self.output_schema);
        self.allocator.destroy(self);
    }
};

/// When a GROUP BY reads exactly one materialized stage with nothing between
/// (a bare `FROM <cte>`), build the input as a parallel buffer scan instead of
/// the serial MatScan so `routeJoinPartialGroupBy` can fuse the partial
/// aggregate into the stripe workers. The ParallelScan counts as ONE stage use
/// (parity with MatScan.create's bump), so it cleanly REPLACES the serial leaf
/// — never built alongside it. Returns null for any other shape (the caller
/// falls back to the serial `buildGenericBlock`).
fn tryStageParallelScan(input: engine_v2.CompileInput, op: *const ir.Op, map: *StageMap, deferred: bool) anyerror!?exec.Query {
    if (input.effectiveDop() <= 1) return null;
    const stage = switch (op.*) {
        .materialize => map.get(op) orelse return null,
        else => return null,
    };
    if (deferred) {
        return try wrapSlicePred(input, try exec.ParallelScan.createOverStageDeferred(
            input.allocator,
            input.db.allocator,
            stage,
            input.accountant,
            input.effectiveDop(),
        ));
    }
    return try wrapSlicePred(input, try exec.ParallelScan.createOverStage(
        input.allocator,
        input.db.allocator,
        stage,
        input.accountant,
        input.effectiveDop(),
    ));
}

/// SEPARABLE slice predicate: wrap a freshly compiled LEAF (a base-table
/// block or a shared-stage read) in the slice filter when its schema carries
/// every slice column. The Filter fuses into the scan below where possible
/// (zone maps on tables); leaves without the key — dimensions — pass through
/// whole, by design.
fn wrapSlicePred(input: engine_v2.CompileInput, q: exec.Query) anyerror!exec.Query {
    const pred = input.slice_pred orelse return q;
    const schema = q.outputSchema();
    for (input.slice_cols) |c| {
        if (types.findColumn(schema, c) == null) return q;
    }
    // A per-slice stage's content is already range-restricted; re-filtering
    // its reads would be a pure serial pass over rows that all survive.
    if (exec.queryAs(mat_stage.MatScan, q)) |ms| {
        if (ms.stage.slice_local) return q;
    }
    // Sliced-result chunk skip: when this leaf reads a stage that was itself
    // slice-adopted, hand the range straight to the scan so it skips
    // disjoint chunks wholesale — the boundary between two sliced stages
    // then costs ~1/N of a buffer scan instead of a full one.
    if (exec.queryAs(mat_stage.MatScan, q)) |ms| {
        if (input.slice_cols.len == 1) applySliceSkip(ms, input.slice_cols[0], pred);
    }
    var qq = q;
    errdefer qq.deinit();
    const out = try qq.filter(pred);
    if (getenv("THINDB_TRACE_SEP") != null) {
        const fused = if (exec.queryAs(exec.Filter, out)) |f| f.fused else false;
        var buf: std.ArrayList(u8) = .empty;
        defer buf.deinit(input.allocator);
        var inner = qq;
        inner.explain(&buf, input.allocator, 0) catch {};
        const first = if (std.mem.indexOfScalar(u8, buf.items, '\n')) |nl| buf.items[0..nl] else buf.items;
        std.debug.print("[sep] wrap: fused={} root='{s}'\n", .{ fused, first });
        if (!fused) std.debug.print("[sep] unfused pipeline:\n{s}\n", .{buf.items});
    }
    return out;
}

/// Recognize exactly the three predicate shapes the sliced fill builds and
/// forward them as a chunk-skip hint. Anything else stays hint-less (the
/// Filter above is always the correctness authority).
fn applySliceSkip(ms: *mat_stage.MatScan, col: []const u8, pred: PredicateExpr) void {
    switch (pred) {
        .leaf => |l| {
            if (l.op == .gt and types.columnNameEql(l.col, col)) ms.setSliceSkip(col, l.val, null, false);
        },
        .@"and" => |arms| {
            if (arms.len != 2 or arms[0] != .leaf or arms[1] != .leaf) return;
            const a = arms[0].leaf;
            const b = arms[1].leaf;
            if (a.op == .gt and b.op == .lte and
                types.columnNameEql(a.col, col) and types.columnNameEql(b.col, col))
            {
                ms.setSliceSkip(col, a.val, b.val, false);
            }
        },
        .@"or" => |arms| {
            if (arms.len != 2 or arms[0] != .leaf or arms[1] != .is_null) return;
            const a = arms[0].leaf;
            if (a.op == .lte and types.columnNameEql(a.col, col) and
                types.columnNameEql(arms[1].is_null, col))
            {
                ms.setSliceSkip(col, null, a.val, true);
            }
        },
        else => {},
    }
}

// ---------------------------------------------------------------------------
// SEPARABLE BY — sliced stage fill.
//
// The declared block's stage fills by compiling its body N times, each with a
// disjoint key-range predicate applied at its leaves (wrapSlicePred above) at
// internal DOP 1, running the N pipelines on their own threads, and adopting
// their outputs in slice order. Shared upstream stages are computed ONCE,
// unsliced, and range-filtered per slice — equivalent under the separability
// contract (a per-key block commutes with a key filter) and strictly less
// work. The body IR is shared read-only; the predicate travels as a compile
// parameter, never an IR mutation.
// ---------------------------------------------------------------------------

const SlicedFillCtx = struct {
    /// Captured compile input — the allocator/session/arena pointers inside
    /// live for the whole query, so a value copy stays valid at fill time.
    input: engine_v2.CompileInput,
    spec: ir.SeparableSpec,
    body: *const ir.Op,
    /// Snapshot of the SHARED stage map at collect time (post-order:
    /// everything shared the body references is already in it; the block's
    /// private closure is deliberately absent — each slice stages its own
    /// 1/N-sized copies). Backing memory in `arena`; slices only read it.
    map: StageMap,
    /// Read-only CSE snapshots so each slice's stage collection makes the
    /// same canonicalization / single-ref decisions the outer plan did.
    canon: std.AutoHashMapUnmanaged(*const ir.Op, *const ir.Op),
    refs: MatRefCounts,
    arena: std.heap.ArenaAllocator,
};

fn makeSlicedFill(
    input: engine_v2.CompileInput,
    spec: ir.SeparableSpec,
    body: *const ir.Op,
    map: *const StageMap,
    cse: *const MatCse,
) !mat_stage.Stage.SlicedFill {
    const ctx = try input.allocator.create(SlicedFillCtx);
    errdefer input.allocator.destroy(ctx);
    ctx.* = .{
        .input = input,
        .spec = spec,
        .body = body,
        .map = .empty,
        .canon = .empty,
        .refs = .empty,
        .arena = std.heap.ArenaAllocator.init(input.allocator),
    };
    errdefer ctx.arena.deinit();
    const aa = ctx.arena.allocator();
    var it = @constCast(map).iterator();
    while (it.next()) |e| try ctx.map.put(aa, e.key_ptr.*, e.value_ptr.*);
    var cit = @constCast(&cse.canon).iterator();
    while (cit.next()) |e| try ctx.canon.put(aa, e.key_ptr.*, e.value_ptr.*);
    var rit = @constCast(&cse.refs).iterator();
    while (rit.next()) |e| try ctx.refs.put(aa, e.key_ptr.*, e.value_ptr.*);
    return .{ .ctx = ctx, .run = slicedFillRun, .drop = slicedFillDrop };
}

fn slicedFillDrop(ctx_op: *anyopaque) void {
    const ctx: *SlicedFillCtx = @ptrCast(@alignCast(ctx_op));
    const alloc = ctx.input.allocator;
    ctx.arena.deinit();
    alloc.destroy(ctx);
}

/// Every shared stage the body reads — the slice pipelines' stage leaves.
/// Recursion mirrors collectStages; a materialize NOT in the map is a
/// single-ref body that will inline into the slice pipelines, so its own
/// subtree's inputs count too.
fn collectBodyInputStages(
    op: *const ir.Op,
    map: *const StageMap,
    alloc: Allocator,
    out: *std.ArrayListUnmanaged(*mat_stage.Stage),
) !void {
    switch (op.*) {
        .materialize => |m| {
            if (map.get(op)) |st| {
                for (out.items) |s| if (s == st) return;
                try out.append(alloc, st);
                return;
            }
            try collectBodyInputStages(m.upstream, map, alloc, out);
        },
        .select => |p| try collectBodyInputStages(p.upstream, map, alloc, out),
        .exclude => |p| try collectBodyInputStages(p.upstream, map, alloc, out),
        .filter => |f| try collectBodyInputStages(f.upstream, map, alloc, out),
        .order_by => |o| try collectBodyInputStages(o.upstream, map, alloc, out),
        .group_by => |g| try collectBodyInputStages(g.upstream, map, alloc, out),
        .compute => |c| try collectBodyInputStages(c.upstream, map, alloc, out),
        .alias => |a| try collectBodyInputStages(a.upstream, map, alloc, out),
        .limit => |l| try collectBodyInputStages(l.upstream, map, alloc, out),
        .window => |w| try collectBodyInputStages(w.upstream, map, alloc, out),
        .join => |j| {
            try collectBodyInputStages(j.left, map, alloc, out);
            try collectBodyInputStages(j.right, map, alloc, out);
        },
        .set_union => |u| {
            try collectBodyInputStages(u.left, map, alloc, out);
            try collectBodyInputStages(u.right, map, alloc, out);
        },
        else => {},
    }
}

fn sliceChunkView(chunk: *const mat_stage.MaterializedResult.Chunk, ci: usize) storage_column.ColumnView {
    if (chunk.views.len > 0) return chunk.views[ci];
    return chunk.cols[ci].view();
}

fn sliceValueAt(v: storage_column.ColumnView, row: usize) ?types.Value {
    return switch (v.data) {
        .int => |s| .{ .int = s[row] },
        .bigint => |s| .{ .bigint = s[row] },
        .tinyint => |s| .{ .tinyint = s[row] },
        .smallint => |s| .{ .smallint = s[row] },
        .date => |s| .{ .date = s[row] },
        .datetime => |s| .{ .datetime = s[row] },
        .float => |s| .{ .float = s[row] },
        .double => |s| .{ .double = s[row] },
        .decimal64 => |s| .{ .decimal64 = s[row] },
        .largeint => |s| .{ .largeint = s[row] },
        .varchar, .string, .char => |s| .{ .text = s.bytes[s.offsets[row]..s.offsets[row + 1]] },
        else => null,
    };
}

fn sliceValueLess(_: void, a: types.Value, b: types.Value) bool {
    return a.compare(b) == .lt;
}

// --- Composite (multi-column) slice keys: tuples ordered lexicographically.

fn tupleLexCompare(a: []const types.Value, b: []const types.Value) std.math.Order {
    for (a, b) |va, vb| {
        const ord = va.compare(vb);
        if (ord != .eq) return ord;
    }
    return .eq;
}

fn tupleLexLess(_: void, a: []types.Value, b: []types.Value) bool {
    return tupleLexCompare(a, b) == .lt;
}

/// Lexicographic tuple comparison as a predicate tree:
///   (k0 OP' b0) OR (k0 = b0 AND k1 OP' b1) OR ... — strict OP' on every
/// component except the LAST, which uses `last_op` (.lte gives tuple <=,
/// .gt gives tuple >). Single-column keys reduce to a bare leaf, keeping
/// the exact shapes the chunk-skip recognizer already matches.
fn tupleCmpPred(
    aa: Allocator,
    cols: []const []const u8,
    bound: []const types.Value,
    last_op: exec.predicate.PredicateOp,
) !PredicateExpr {
    const k = cols.len;
    const strict: exec.predicate.PredicateOp = if (last_op == .lte or last_op == .lt) .lt else .gt;
    if (k == 1) return .{ .leaf = .{ .col = cols[0], .op = last_op, .val = bound[0] } };
    const arms = try aa.alloc(PredicateExpr, k);
    for (0..k) |i| {
        const op = if (i == k - 1) last_op else strict;
        if (i == 0) {
            arms[0] = .{ .leaf = .{ .col = cols[0], .op = op, .val = bound[0] } };
        } else {
            const sub = try aa.alloc(PredicateExpr, i + 1);
            for (0..i) |j| sub[j] = .{ .leaf = .{ .col = cols[j], .op = .eq, .val = bound[j] } };
            sub[i] = .{ .leaf = .{ .col = cols[i], .op = op, .val = bound[i] } };
            arms[i] = .{ .@"and" = sub };
        }
    }
    return .{ .@"or" = arms };
}

/// OR of IS NULL over the key columns — a NULL in ANY component routes the
/// row to slice 0 (value comparisons are 2VL and would drop it otherwise).
fn tupleAnyNullPred(aa: Allocator, cols: []const []const u8) !PredicateExpr {
    if (cols.len == 1) return .{ .is_null = cols[0] };
    const arms = try aa.alloc(PredicateExpr, cols.len);
    for (cols, arms) |c, *a| a.* = .{ .is_null = c };
    return .{ .@"or" = arms };
}

/// First tuple boundary >= t wins its slice; any-NULL tuples were routed by
/// the caller.
fn sliceIndexForTuple(t: []const types.Value, bounds: []const []const types.Value) usize {
    var lo: usize = 0;
    var hi: usize = bounds.len;
    while (lo < hi) {
        const mid = lo + (hi - lo) / 2;
        if (tupleLexCompare(t, bounds[mid]) == .gt) lo = mid + 1 else hi = mid;
    }
    return lo;
}

fn dupeSliceValue(arena: Allocator, v: types.Value) !types.Value {
    return switch (v) {
        .text => |t| .{ .text = try arena.dupe(u8, t) },
        else => v,
    };
}

/// Stride-sample the slice key TUPLE from the first input stage that carries
/// every key column. Returns a lexicographically SORTED sample (tuples
/// arena-owned) or null when no input qualifies / a component type isn't
/// range-partitionable. Tuples containing a NULL are skipped — they route to
/// slice 0 regardless of bounds.
fn sampleSliceColumn(ctx: *SlicedFillCtx, cols: []const []const u8, inputs: []const *mat_stage.Stage, target: usize) !?[][]types.Value {
    const aa = ctx.arena.allocator();
    const cis = try aa.alloc(usize, cols.len);
    next_input: for (inputs) |s| {
        for (cols, cis) |col, *ci| {
            ci.* = for (s.schema, 0..) |sc, i| {
                if (types.columnNameEql(sc.name, col)) break i;
            } else continue :next_input;
        }
        const r = s.result orelse continue;
        if (r.total_rows == 0) continue;
        const stride: usize = @intCast(@max(1, r.total_rows / target));
        var vals: std.ArrayListUnmanaged([]types.Value) = .empty;
        var tick: usize = 0;
        const views = try aa.alloc(storage_column.ColumnView, cols.len);
        for (r.chunks.items) |*ch| {
            for (cis, views) |ci, *v| v.* = sliceChunkView(ch, ci);
            var row: usize = 0;
            rows: while (row < ch.rows) : (row += 1) {
                tick += 1;
                if (tick % stride != 0) continue;
                const tuple = try aa.alloc(types.Value, cols.len);
                for (views, tuple) |v, *slot| {
                    if (!v.isValid(row)) continue :rows; // NULL keys route to slice 0
                    const val = sliceValueAt(v, row) orelse continue :next_input; // unpartitionable type
                    slot.* = try dupeSliceValue(aa, val);
                }
                try vals.append(aa, tuple);
            }
        }
        if (vals.items.len < 2) continue;
        std.mem.sort([]types.Value, vals.items, {}, tupleLexLess);
        return vals.items;
    }
    return null;
}

/// Base-table leaves under the body (recursing through private, not-in-map
/// materialize nodes; shared stages stop the walk — they were already
/// offered to the stage sampler).
fn collectScanLeaves(
    op: *const ir.Op,
    map: *const StageMap,
    alloc: Allocator,
    out: *std.ArrayListUnmanaged(ir.TableRef),
) !void {
    switch (op.*) {
        .scan => |s| {
            for (out.items) |ref| {
                if (std.mem.eql(u8, ref.name, s.table.name)) return;
            }
            try out.append(alloc, s.table);
        },
        .materialize => |m| {
            if (map.get(op) != null) return;
            try collectScanLeaves(m.upstream, map, alloc, out);
        },
        .select => |p| try collectScanLeaves(p.upstream, map, alloc, out),
        .exclude => |p| try collectScanLeaves(p.upstream, map, alloc, out),
        .filter => |f| try collectScanLeaves(f.upstream, map, alloc, out),
        .order_by => |o| try collectScanLeaves(o.upstream, map, alloc, out),
        .group_by => |g| try collectScanLeaves(g.upstream, map, alloc, out),
        .compute => |c| try collectScanLeaves(c.upstream, map, alloc, out),
        .alias => |a| try collectScanLeaves(a.upstream, map, alloc, out),
        .limit => |l| try collectScanLeaves(l.upstream, map, alloc, out),
        .window => |w| try collectScanLeaves(w.upstream, map, alloc, out),
        .join => |j| {
            try collectScanLeaves(j.left, map, alloc, out);
            try collectScanLeaves(j.right, map, alloc, out);
        },
        .set_union => |u| {
            try collectScanLeaves(u.left, map, alloc, out);
            try collectScanLeaves(u.right, map, alloc, out);
        },
        else => {},
    }
}

/// Bounds fallback when no shared stage carries the slice column: stride-
/// sample it straight off a base table under the body via a synthetic
/// single-column scan. Tables without the column just fail the compile and
/// the next candidate is tried. Only physical columns sample this way — a
/// derived slice key (an expression alias) has no table to sample, and the
/// fill declines with the physical-column hint in the trace.
fn sampleTableColumn(ctx: *SlicedFillCtx, cols: []const []const u8, target: usize) !?[][]types.Value {
    const aa = ctx.arena.allocator();
    var tables: std.ArrayListUnmanaged(ir.TableRef) = .empty;
    defer tables.deinit(aa);
    try collectScanLeaves(ctx.body, &ctx.map, aa, &tables);
    outer: for (tables.items) |ref| {
        const scan = try aa.create(ir.Op);
        scan.* = .{ .scan = .{ .table = ref, .alias = null } };
        const sel = try aa.create(ir.Op);
        sel.* = .{ .select = .{ .columns = cols, .upstream = scan } };
        var sin = ctx.input;
        sin.accountant = null;
        var q = engine_v2.compileSelectBlock(sin, sel) catch continue;
        const upper = q.stats().upper_rows;
        if (upper == 0 or upper == std.math.maxInt(u64)) {
            q.deinit();
            continue;
        }
        const stride: usize = @intCast(@max(1, upper / target));
        var vals: std.ArrayListUnmanaged([]types.Value) = .empty;
        var tick: usize = 0;
        while (true) {
            const b = q.next() catch {
                q.deinit();
                continue :outer;
            };
            const batch = b orelse break;
            var row: usize = 0;
            rows: while (row < batch.row_count) : (row += 1) {
                tick += 1;
                if (tick % stride != 0) continue;
                const tuple = try aa.alloc(types.Value, cols.len);
                for (batch.values[0..cols.len], tuple) |v, *slot| {
                    if (!v.isValid(row)) continue :rows; // NULL keys route to slice 0
                    const val = sliceValueAt(v, row) orelse {
                        q.deinit();
                        continue :outer;
                    };
                    slot.* = try dupeSliceValue(aa, val);
                }
                try vals.append(aa, tuple);
            }
        }
        q.deinit();
        if (vals.items.len < 2) continue;
        std.mem.sort([]types.Value, vals.items, {}, tupleLexLess);
        return vals.items;
    }
    return null;
}

/// SCAN-ONCE pre-partition: a private multi-ref TABLE-BACKED block would be
/// recompiled and rescanned by every slice — instead compile it ONCE at full
/// parallelism, route its output rows by slice-key range into N per-slice
/// buffers, and hand each slice a pre-filled slice_local stage for that node.
/// The table is then touched by exactly one well-behaved parallel scan.
const PrePart = struct {
    node: *const ir.Op,
    stages: []*mat_stage.Stage,
    /// True when every slot in `stages` is the SAME stage — a multi-ref
    /// block whose output lacks the slice key: unsliceable, so computed
    /// once and read-shared by every slice (release/deinit exactly once).
    shared: bool = false,
};

fn collectPrePartCandidates(
    ctx: *SlicedFillCtx,
    op: *const ir.Op,
    alloc: Allocator,
    out: *std.ArrayListUnmanaged(*const ir.Op),
) !void {
    switch (op.*) {
        .materialize => |m| {
            const rep = ctx.canon.get(op) orelse op;
            if (ctx.map.get(rep) != null) return; // shared: already staged once
            const multi = (ctx.refs.get(rep) orelse 1) >= 2 or rep.materialize.forced;
            if (multi and irBlockIsSimpleTableChain(rep.materialize.upstream)) {
                for (out.items) |e| if (e == rep) return;
                try out.append(alloc, rep);
                return;
            }
            try collectPrePartCandidates(ctx, m.upstream, alloc, out);
        },
        .select => |p| try collectPrePartCandidates(ctx, p.upstream, alloc, out),
        .exclude => |p| try collectPrePartCandidates(ctx, p.upstream, alloc, out),
        .filter => |f| try collectPrePartCandidates(ctx, f.upstream, alloc, out),
        .order_by => |o| try collectPrePartCandidates(ctx, o.upstream, alloc, out),
        .group_by => |g| try collectPrePartCandidates(ctx, g.upstream, alloc, out),
        .compute => |c| try collectPrePartCandidates(ctx, c.upstream, alloc, out),
        .alias => |a| try collectPrePartCandidates(ctx, a.upstream, alloc, out),
        .limit => |l| try collectPrePartCandidates(ctx, l.upstream, alloc, out),
        .window => |w| try collectPrePartCandidates(ctx, w.upstream, alloc, out),
        .join => |j| {
            try collectPrePartCandidates(ctx, j.left, alloc, out);
            try collectPrePartCandidates(ctx, j.right, alloc, out);
        },
        .set_union => |u| {
            try collectPrePartCandidates(ctx, u.left, alloc, out);
            try collectPrePartCandidates(ctx, u.right, alloc, out);
        },
        else => {},
    }
}

/// Column-strip gather pool for the pre-partition router: the drain thread
/// publishes one batch at a time (generation counter); each worker copies
/// its column strip into every slice's sink. All (column, slice) cells and
/// their per-column arenas are disjoint across workers, so no locks. Spin
/// waits are fine — a batch's gather is ~ms and the pool lives for one
/// drain.
const GatherPool = struct {
    sinks: []mat_stage.ContigSink,
    idx_lists: []std.ArrayListUnmanaged(u32),
    ncols: usize,
    batch: ?*const exec.Batch = null,
    gen: std.atomic.Value(usize) = .init(0),
    done: std.atomic.Value(usize) = .init(0),
    stop: std.atomic.Value(bool) = .init(false),
    err: std.atomic.Value(bool) = .init(false),

    fn dispatch(self: *GatherPool, batch: *const exec.Batch) void {
        self.batch = batch;
        self.done.store(0, .release);
        _ = self.gen.fetchAdd(1, .release);
    }

    fn wait(self: *GatherPool) void {
        while (self.done.load(.acquire) < self.ncols) std.atomic.spinLoopHint();
    }

    fn worker(self: *GatherPool, tid: usize, nthreads: usize) void {
        var lease = core_scheduler.global().tryAcquire();
        defer lease.release();
        var seen: usize = 0;
        while (true) {
            while (self.gen.load(.acquire) == seen) {
                if (self.stop.load(.acquire)) return;
                std.atomic.spinLoopHint();
            }
            seen = self.gen.load(.acquire);
            if (self.stop.load(.acquire)) return;
            const batch = self.batch.?;
            var c = tid;
            while (c < self.ncols) : (c += nthreads) {
                for (self.sinks, self.idx_lists) |*sk, l| {
                    if (l.items.len == 0) continue;
                    engine_mod.transform.appendByIndices(
                        sk.arenas[c].allocator(),
                        batch.values[c],
                        l.items,
                        &sk.stores[c],
                    ) catch {
                        self.err.store(true, .release);
                    };
                }
                _ = self.done.fetchAdd(1, .release);
            }
            // Columns not divisible by nthreads: strips cover all of them —
            // done counts one per column, wait() releases at ncols.
        }
    }
};

/// The most common window PARTITION BY column set under the body. Sorting a
/// pre-partitioned slice buffer by these keys (once, at full parallelism,
/// before routing — the router is stable) lets every same-partition window
/// inside the slices take the grouped fast path: no global sort per window,
/// just tiny within-partition orderings.
fn commonWindowPartitionSet(
    ctx: *SlicedFillCtx,
    op: *const ir.Op,
    alloc: Allocator,
    sets: *std.ArrayListUnmanaged([]const []const u8),
    counts: *std.ArrayListUnmanaged(usize),
) !void {
    switch (op.*) {
        .window => |w| {
            for (w.specs) |spec| {
                if (spec.partition_by.len == 0) continue;
                found: {
                    for (sets.items, counts.items) |s, *c| {
                        if (s.len != spec.partition_by.len) continue;
                        for (spec.partition_by) |p| {
                            var hit = false;
                            for (s) |name| {
                                if (types.columnNameEql(name, p)) {
                                    hit = true;
                                    break;
                                }
                            }
                            if (!hit) break :found;
                        }
                        c.* += 1;
                        break :found;
                    }
                    try sets.append(alloc, spec.partition_by);
                    try counts.append(alloc, 1);
                }
            }
            try commonWindowPartitionSet(ctx, w.upstream, alloc, sets, counts);
        },
        .materialize => |m| {
            if (ctx.map.get(op) != null) return; // shared: fills elsewhere
            try commonWindowPartitionSet(ctx, m.upstream, alloc, sets, counts);
        },
        .select => |p| try commonWindowPartitionSet(ctx, p.upstream, alloc, sets, counts),
        .exclude => |p| try commonWindowPartitionSet(ctx, p.upstream, alloc, sets, counts),
        .filter => |f| try commonWindowPartitionSet(ctx, f.upstream, alloc, sets, counts),
        .order_by => |o| try commonWindowPartitionSet(ctx, o.upstream, alloc, sets, counts),
        .group_by => |g| try commonWindowPartitionSet(ctx, g.upstream, alloc, sets, counts),
        .compute => |c| try commonWindowPartitionSet(ctx, c.upstream, alloc, sets, counts),
        .alias => |a| try commonWindowPartitionSet(ctx, a.upstream, alloc, sets, counts),
        .limit => |l| try commonWindowPartitionSet(ctx, l.upstream, alloc, sets, counts),
        .join => |j| {
            try commonWindowPartitionSet(ctx, j.left, alloc, sets, counts);
            try commonWindowPartitionSet(ctx, j.right, alloc, sets, counts);
        },
        .set_union => |u| {
            try commonWindowPartitionSet(ctx, u.left, alloc, sets, counts);
            try commonWindowPartitionSet(ctx, u.right, alloc, sets, counts);
        },
        else => {},
    }
}

/// Parallel per-slice partition-grouping pass: sort ONE slice buffer by the
/// dominant window partition keys (honest ascending value order — the claim
/// feeds sorted-stream consumers too, not just window grouping). Runs on its
/// own thread per slice; reads the routed sink non-destructively and gathers
/// into a fresh conn-thread-created sink, so a failure falls back to the
/// unsorted buffer.
const GroupKI = struct { hi: u64, idx: u32 };

const GroupSortCtx = struct {
    cols: []const engine_mod.ColumnStore,
    keys: []const usize,

    pub fn less(ctx: @This(), a: GroupKI, b: GroupKI) bool {
        if (a.hi != b.hi) return a.hi < b.hi;
        for (ctx.keys) |ci| {
            const ord = engine_mod.transform.compareInColumnNullsFirst(ctx.cols[ci], a.idx, b.idx);
            if (ord == .lt) return true;
            if (ord == .gt) return false;
        }
        return a.idx < b.idx;
    }
};

const GroupClaim = struct {
    sinks: []mat_stage.ContigSink,
    fresh: []mat_stage.ContigSink,
    kidx: []const usize,
    errs: []?anyerror,
    next: std.atomic.Value(usize) = .init(0),

    fn worker(self: *GroupClaim) void {
        while (true) {
            const i = self.next.fetchAdd(1, .monotonic);
            if (i >= self.sinks.len) return;
            groupSliceSink(&self.sinks[i], &self.fresh[i], self.kidx, &self.errs[i]);
        }
    }
};

fn groupSliceSink(
    sink: *mat_stage.ContigSink,
    fresh: *mat_stage.ContigSink,
    key_idxs: []const usize,
    err_out: *?anyerror,
) void {
    const n: usize = @intCast(sink.rows);
    if (n == 0) return;
    var lease = core_scheduler.global().tryAcquire();
    defer lease.release();
    var arena = std.heap.ArenaAllocator.init(std.heap.c_allocator);
    defer arena.deinit();
    const aa = arena.allocator();
    const kis = aa.alloc(GroupKI, n) catch |e| {
        err_out.* = e;
        return;
    };
    for (kis, 0..) |*ki, i| {
        const row: u32 = @intCast(i);
        ki.* = .{ .hi = window_op.orderPrefix(sink.stores[key_idxs[0]], row, false), .idx = row };
    }
    std.sort.pdq(GroupKI, kis, GroupSortCtx{ .cols = sink.stores, .keys = key_idxs }, GroupSortCtx.less);
    const perm = aa.alloc(u32, n) catch |e| {
        err_out.* = e;
        return;
    };
    for (perm, kis) |*p, ki| p.* = ki.idx;
    for (sink.stores, 0..) |*st, ci| {
        engine_mod.transform.appendByIndices(fresh.arenas[ci].allocator(), st.view(), perm, &fresh.stores[ci]) catch |e| {
            err_out.* = e;
            return;
        };
    }
    fresh.rows = sink.rows;
}

/// Window-chain pairing (SEPARABLE slices): the .window IR node reachable
/// from `op` through GROUPING-PRESERVING ops only — compute/filter/alias/
/// exclude keep partitions intact; a select with renames could shadow a
/// partition name (decline); an in-map materialize is a stage boundary
/// (decline); everything else (joins, unions, group-bys) reorders.
const ChainBelow = struct {
    win: *const ir.Op,
    joins: [6]*const ir.Op,
    n_joins: usize,
};

fn windowChainBelow(op: *const ir.Op, map: *const StageMap) ?ChainBelow {
    var res: ChainBelow = .{ .win = undefined, .joins = undefined, .n_joins = 0 };
    var cur = op;
    while (true) switch (cur.*) {
        .compute => |c| cur = c.upstream,
        .filter => |f| cur = f.upstream,
        .alias => |a| cur = a.upstream,
        .exclude => |p| cur = p.upstream,
        .select => |p| cur = p.upstream, // rename shadowing re-checked per key below
        .materialize => |m| {
            if (map.get(cur) != null) return null;
            cur = m.upstream;
        },
        .join => |j| {
            // Continue through the LEFT side — probe by convention for
            // LEFT/typical INNER joins; the pairing VERIFIES each join's
            // compiled build_is_left before trusting the traversal.
            if (res.n_joins == res.joins.len) return null;
            res.joins[res.n_joins] = cur;
            res.n_joins += 1;
            cur = j.left;
        },
        .window => {
            res.win = cur;
            return res;
        },
        else => return null,
    };
}

/// A select along the pairing chain may rename columns — safe UNLESS a
/// partition-set name is renamed or shadowed (the rider's name would then
/// bind a different column than the emitter grouped by).
fn chainRenamesPartitionKey(op: *const ir.Op, below: *const ir.Op, keys: []const []const u8) bool {
    var cur = op;
    while (cur != below) switch (cur.*) {
        .compute => |c| {
            // A derived column REPLACING a partition-key name changes its
            // values — the emitter's grouping no longer describes it.
            for (c.derived) |d| {
                for (keys) |k| {
                    if (types.columnNameEql(d.name, k)) return true;
                }
            }
            cur = c.upstream;
        },
        .filter => |f| cur = f.upstream,
        .alias => |a| cur = a.upstream,
        .exclude => |p| cur = p.upstream,
        .select => |p| {
            if (p.outputs) |outs| {
                for (p.columns, outs) |src, out_opt| {
                    const out = out_opt orelse continue;
                    if (types.columnNameEql(out, src)) continue;
                    for (keys) |k| {
                        if (types.columnNameEql(out, k) or types.columnNameEql(src, k)) return true;
                    }
                }
            }
            cur = p.upstream;
        },
        .materialize => |m| cur = m.upstream,
        .join => |j| cur = j.left, // right-side shadowing checked at pairing
        else => return true, // unexpected — decline
    };
    return false;
}

fn partitionNameSetsEqual(pa: []const []const u8, pb: []const []const u8) bool {
    if (pa.len != pb.len or pa.len == 0) return false;
    for (pa) |ka| {
        var hit = false;
        for (pb) |kb| {
            if (types.columnNameEql(ka, kb)) {
                hit = true;
                break;
            }
        }
        if (!hit) return false;
    }
    return true;
}

fn windowPartitionSetsEqual(a: *const ir.Op, b: *const ir.Op) bool {
    return partitionNameSetsEqual(a.window.specs[0].partition_by, b.window.specs[0].partition_by);
}

/// Trace helper: the op tag where the pairing chain walk gave up.
fn windowChainStopTag(op: *const ir.Op, map: *const StageMap) []const u8 {
    var cur = op;
    while (true) switch (cur.*) {
        .compute => |c| cur = c.upstream,
        .filter => |f| cur = f.upstream,
        .alias => |a| cur = a.upstream,
        .exclude => |p| cur = p.upstream,
        .select => |p| cur = p.upstream,
        .materialize => |m| {
            if (map.get(cur) != null) return "materialize(staged)";
            cur = m.upstream;
        },
        .window => return "window",
        else => return @tagName(cur.*),
    };
}

/// The rider only needs GROUPING, so its specs may differ in ORDER BY —
/// but every spec must partition by the same set for one op-level
/// assume_grouped to be valid.
fn irAllSpecsSamePartition(node: *const ir.Op) bool {
    const specs = node.window.specs;
    if (specs.len == 0 or specs[0].partition_by.len == 0) return false;
    for (specs[1..]) |s| {
        if (!partitionNameSetsEqual(specs[0].partition_by, s.partition_by)) return false;
    }
    return true;
}

/// First boundary >= v wins its slice; NULL keys were routed by the caller.
fn sliceIndexForValue(v: types.Value, bounds: []const types.Value) usize {
    var lo: usize = 0;
    var hi: usize = bounds.len;
    while (lo < hi) {
        const mid = lo + (hi - lo) / 2;
        if (v.compare(bounds[mid]) == .gt) lo = mid + 1 else hi = mid;
    }
    return lo;
}

fn prePartition(
    ctx: *SlicedFillCtx,
    stage: *mat_stage.Stage,
    cols: []const []const u8,
    bounds: []const []const types.Value,
    trace: bool,
) ![]PrePart {
    const alloc = ctx.input.allocator;
    const aa = ctx.arena.allocator();
    const total = bounds.len + 1;
    var cands: std.ArrayListUnmanaged(*const ir.Op) = .empty;
    defer cands.deinit(alloc);
    try collectPrePartCandidates(ctx, ctx.body, alloc, &cands);
    if (trace) std.debug.print("[sep] prePartition: {d} candidates\n", .{cands.items.len});

    // The dominant window PARTITION BY set under the body: sorting the
    // partitioner's output by it (once, full DOP, before the stable router)
    // delivers every slice buffer partition-grouped, so the windows above
    // take the grouped fast path instead of one full sort each.
    var psets: std.ArrayListUnmanaged([]const []const u8) = .empty;
    defer psets.deinit(alloc);
    var pcounts: std.ArrayListUnmanaged(usize) = .empty;
    defer pcounts.deinit(alloc);
    try commonWindowPartitionSet(ctx, ctx.body, alloc, &psets, &pcounts);
    if (trace) std.debug.print("[sep] prePartition: window sets collected ({d})\n", .{psets.items.len});
    var part_set: []const []const u8 = &.{};
    var part_best: usize = 0;
    for (psets.items, pcounts.items) |s, c| {
        if (c > part_best) {
            part_best = c;
            part_set = s;
        }
    }

    var parts: std.ArrayListUnmanaged(PrePart) = .empty;
    errdefer parts.deinit(alloc);
    next_cand: for (cands.items) |node| {
        if (trace) std.debug.print("[sep] prePartition: compiling candidate {*}\n", .{node});
        var q = compileBlock(ctx.input, node.materialize.upstream, @constCast(&ctx.map)) catch {
            if (trace) std.debug.print("[sep] prePartition: candidate compile FAILED\n", .{});
            continue;
        };
        if (trace) std.debug.print("[sep] prePartition: candidate compiled\n", .{});
        q = pruneStageColumns(ctx.input, q);
        // Buffers get partition-GROUPED after routing (a parallel per-slice
        // pass — grouping only needs digest order, not a semantic sort), so
        // the windows above take the grouped fast path. Record the keys now;
        // the group pass + claim happen once the sinks are built.
        var sorted_keys: []const []const u8 = &.{};
        if (part_set.len > 0) apply: {
            for (part_set) |k| {
                if (types.findColumn(q.outputSchema(), k) == null) break :apply;
            }
            const key_names = try aa.alloc([]const u8, part_set.len);
            for (part_set, key_names) |k, *kn| kn.* = try aa.dupe(u8, k);
            sorted_keys = key_names;
        }
        const raw_schema = q.outputSchema();
        const cis = try aa.alloc(usize, cols.len);
        const found: bool = blk: {
            for (cols, cis) |col, *ci| {
                ci.* = for (raw_schema, 0..) |sc, i| {
                    if (types.columnNameEql(sc.name, col)) break i;
                } else break :blk false;
            }
            break :blk true;
        };
        // Deep-copy the schema (ctx arena) — the compiled pipeline dies after
        // this drain, but the pre-filled stages' consumers keep referring.
        const schema = try aa.alloc(types.Column, raw_schema.len);
        for (raw_schema, schema) |src, *dst| {
            dst.* = src;
            dst.name = try aa.dupe(u8, src.name);
        }

        if (!found) {
            q.deinit();
            continue;
        }

        const sinks = try alloc.alloc(mat_stage.ContigSink, total);
        var made: usize = 0;
        errdefer {
            for (sinks[0..made]) |*sk| sk.deinit();
            alloc.free(sinks);
        }
        while (made < total) : (made += 1) {
            sinks[made] = try mat_stage.ContigSink.init(stage.allocator, schema, 1 << 16);
        }

        const idx_lists = try alloc.alloc(std.ArrayListUnmanaged(u32), total);
        defer {
            for (idx_lists) |*l| l.deinit(alloc);
            alloc.free(idx_lists);
        }
        @memset(idx_lists, .empty);

        // Gather pool: the drain stays serial (batch views are only valid
        // until the next pull), but within a batch the copies parallelize
        // across COLUMN STRIPS — thread t owns columns [t::T) across every
        // slice, so all (column, slice) cells (and their per-column arenas)
        // are disjoint. A per-batch generation counter + done count is the
        // whole synchronization; workers spin briefly (batches are ~ms).
        var gather = GatherPool{
            .sinks = sinks,
            .idx_lists = idx_lists,
            .ncols = schema.len,
        };
        const n_gather: usize = @min(4, schema.len);
        var gather_threads: [4]?std.Thread = .{ null, null, null, null };
        var pool_ok = true;
        for (0..n_gather) |t| {
            gather_threads[t] = std.Thread.spawn(.{}, GatherPool.worker, .{ &gather, t, n_gather }) catch {
                pool_ok = false; // a missing strip would hang wait(): go serial
                break;
            };
        }
        defer {
            gather.stop.store(true, .release);
            _ = gather.gen.fetchAdd(1, .release);
            for (gather_threads) |gt| if (gt) |th| th.join();
        }

        var route_err: ?anyerror = null;
        const key_views = try aa.alloc(storage_column.ColumnView, cols.len);
        const row_tuple = try aa.alloc(types.Value, cols.len);
        while (true) {
            const b = q.next() catch |e| {
                route_err = e;
                break;
            };
            const batch = b orelse break;
            for (idx_lists) |*l| l.clearRetainingCapacity();
            for (cis, key_views) |kci, *kv| kv.* = batch.values[kci];
            var row: usize = 0;
            while (row < batch.row_count) : (row += 1) {
                const si = blk: {
                    for (key_views, row_tuple) |v, *slot| {
                        if (!v.isValid(row)) break :blk 0; // NULL keys ride slice 0
                        slot.* = sliceValueAt(v, row) orelse {
                            route_err = error.UnsupportedQueryShape;
                            break :blk 0;
                        };
                    }
                    break :blk sliceIndexForTuple(row_tuple, bounds);
                };
                idx_lists[si].append(alloc, @intCast(row)) catch |e| {
                    route_err = e;
                    break;
                };
            }
            if (route_err != null) break;
            if (pool_ok) {
                gather.dispatch(&batch);
                gather.wait();
                if (gather.err.load(.acquire)) {
                    route_err = error.OutOfMemory;
                    break;
                }
                for (sinks, idx_lists) |*sk, l| sk.rows += l.items.len;
            } else {
                for (sinks, idx_lists) |*sk, l| {
                    if (l.items.len == 0) continue;
                    sk.appendIndices(batch, l.items) catch |e| {
                        route_err = e;
                        break;
                    };
                }
                if (route_err != null) break;
            }
        }
        q.deinit();
        if (route_err != null) {
            for (sinks) |*sk| sk.deinit();
            alloc.free(sinks);
            continue :next_cand;
        }

        // Parallel partition-grouping pass over the routed sinks (one thread
        // per slice; see groupSliceSink). Any failure falls back to the
        // ungrouped buffers — the windows then just sort as before.
        var grouped = false;
        if (sorted_keys.len > 0) group: {
            const kidx = try alloc.alloc(usize, sorted_keys.len);
            defer alloc.free(kidx);
            for (sorted_keys, kidx) |k, *ki| {
                ki.* = for (schema, 0..) |sc, i| {
                    if (types.columnNameEql(sc.name, k)) break i;
                } else break :group;
            }
            const fresh = try alloc.alloc(mat_stage.ContigSink, total);
            var fmade: usize = 0;
            errdefer {
                for (fresh[0..fmade]) |*sk| sk.deinit();
                alloc.free(fresh);
            }
            while (fmade < total) : (fmade += 1) {
                fresh[fmade] = try mat_stage.ContigSink.init(stage.allocator, schema, 1 << 16);
            }
            const gerrs = try alloc.alloc(?anyerror, total);
            defer alloc.free(gerrs);
            @memset(gerrs, null);
            var gclaim = GroupClaim{ .sinks = sinks, .fresh = fresh, .kidx = kidx, .errs = gerrs };
            const g_workers: usize = @min(ctx.input.effectiveDop(), total);
            const gthreads = try alloc.alloc(?std.Thread, g_workers);
            defer alloc.free(gthreads);
            for (gthreads) |*gt| {
                gt.* = std.Thread.spawn(.{}, GroupClaim.worker, .{&gclaim}) catch null;
            }
            var any_spawned = false;
            for (gthreads) |gt| {
                if (gt != null) any_spawned = true;
            }
            if (!any_spawned) GroupClaim.worker(&gclaim);
            for (gthreads) |gt| if (gt) |th| th.join();
            var all_ok = true;
            for (gerrs) |e| {
                if (e != null) all_ok = false;
            }
            if (gclaim.next.load(.acquire) < total) all_ok = false; // a strip died mid-queue
            if (!all_ok) {
                for (fresh) |*sk| sk.deinit();
                alloc.free(fresh);
                break :group;
            }
            for (sinks, fresh) |*old, nw| {
                old.deinit();
                old.* = nw;
            }
            alloc.free(fresh);
            grouped = true;
            if (trace) std.debug.print("[sep] stage#{d} grouped {d} buffers by {d} window partition keys\n", .{ stage.id, total, sorted_keys.len });
        }

        // Wrap each sink as a pre-filled slice_local stage the slice compile
        // resolves through its map — no recompiled scan, no re-filter.
        const stages = try alloc.alloc(*mat_stage.Stage, total);
        var built: usize = 0;
        errdefer {
            for (stages[0..built]) |st| st.deinit();
            alloc.free(stages);
        }
        var rows_total: u64 = 0;
        while (built < total) : (built += 1) {
            const sres = try stage.allocator.create(mat_stage.MaterializedResult);
            errdefer stage.allocator.destroy(sres);
            sres.* = .{ .allocator = stage.allocator, .schema = schema };
            try sres.adoptContiguous(sinks[built].take(), sinks[built].rows);
            rows_total += sres.total_rows;
            const st = try stage.allocator.create(mat_stage.Stage);
            st.* = .{
                .allocator = stage.allocator,
                .query = undefined,
                .query_alive = false,
                .schema = schema,
                .stats_upper_rows = sres.total_rows,
                // Grouped buffer (honest ascending value order): claim it so
                // the windows above prove partition-grouping and skip their
                // full sorts. THINDB_SEP_NOCLAIM suppresses the claim for
                // layout-vs-routing diagnostics.
                .sort_state = if (grouped and getenv("THINDB_SEP_NOCLAIM") == null)
                    .{ .keys = sorted_keys, .descs = &.{}, .global = true }
                else
                    .{},
                .col_stats = &.{},
                .result = sres,
                // Guard use: released by the fill AFTER every slice joined,
                // so a slice's own release can never free another's input.
                .uses_total = .init(1),
                .slice_local = true,
            };
            stages[built] = st;
        }
        alloc.free(sinks);
        if (trace) std.debug.print("[sep] stage#{d} pre-partitioned block once: {d} rows -> {d} buffers\n", .{ stage.id, rows_total, total });
        try parts.append(alloc, .{ .node = node, .stages = stages });
    }
    return parts.toOwnedSlice(alloc);
}

fn slicedFillRun(ctx_op: *anyopaque, stage: *mat_stage.Stage, res: *mat_stage.MaterializedResult) anyerror!bool {
    const ctx: *SlicedFillCtx = @ptrCast(@alignCast(ctx_op));
    const trace = getenv("THINDB_TRACE_SEP") != null;
    const alloc = ctx.input.allocator;
    var ph = exec.prof.nowTicks();
    // A downstream borrower expects ONE contiguous store per column; a
    // sliced fill adopts N parts. Keep the borrow optimization instead.
    if (stage.want_contiguous) {
        if (trace) std.debug.print("[sep] stage#{d} decline: contiguous borrower registered\n", .{stage.id});
        return false;
    }
    const cols = ctx.spec.cols;

    // Materialize the body's shared inputs now (driving thread — ensureRun
    // is not re-entrant) and hold a guard use on each: a slice releasing its
    // reads must never be the "last consumer" while another slice is still
    // compiling against the same stage.
    var inputs: std.ArrayListUnmanaged(*mat_stage.Stage) = .empty;
    defer inputs.deinit(alloc);
    try collectBodyInputStages(ctx.body, &ctx.map, alloc, &inputs);
    for (inputs.items) |s| try s.ensureRun();
    for (inputs.items) |s| s.registerUse();
    defer for (inputs.items) |s| s.releaseUse();

    if (trace) {
        std.debug.print("[sep] phase inputs: {d:.0}ms\n", .{exec.prof.ticksToMs(exec.prof.nowTicks() - ph)});
        ph = exec.prof.nowTicks();
    }
    // THINDB_SEP_SLICES > dop = COHORT mode: many small slices, each
    // pipeline's whole working set cache-sized, run by the dop-wide
    // claiming pool. Measured on the rollforward: cache locality is real
    // but PER-SLICE COMPILATION dominates past ~2x dop (the 39-CTE plan
    // costs ~0.5s to compile x N slices) — the default stays at dop until
    // slice plans can be compiled once and re-parameterized per range.
    var n_slices: usize = ctx.input.effectiveDop();
    if (getenv("THINDB_SEP_SLICES")) |sv| {
        n_slices = std.fmt.parseInt(usize, std.mem.span(sv), 10) catch n_slices;
    }
    if (n_slices < 2) {
        if (trace) std.debug.print("[sep] stage#{d} decline: n_slices={d}\n", .{ stage.id, n_slices });
        return false;
    }
    const sample_target = @max(@as(usize, 4096), n_slices * 16);

    const samples = (try sampleSliceColumn(ctx, cols, inputs.items, sample_target)) orelse
        (try sampleTableColumn(ctx, cols, sample_target)) orelse
    {
        if (trace) std.debug.print("[sep] stage#{d} decline: no input stage or base table carries '{s}' (derived keys need a physical column, e.g. a stored hash)\n", .{ stage.id, cols[0] });
        return false;
    };

    // Boundaries = N-tiles of the lexicographically sorted tuple sample,
    // deduped: heavy skew collapses neighboring tiles, and fewer, fuller
    // slices beat empty ones.
    const aa = ctx.arena.allocator();
    var bounds: std.ArrayListUnmanaged([]types.Value) = .empty;
    var j: usize = 1;
    while (j < n_slices) : (j += 1) {
        const b = samples[samples.len * j / n_slices];
        if (bounds.items.len == 0 or tupleLexCompare(bounds.items[bounds.items.len - 1], b) == .lt) {
            try bounds.append(aa, b);
        }
    }
    if (bounds.items.len == 0) {
        if (trace) std.debug.print("[sep] stage#{d} decline: degenerate bounds (constant key)\n", .{stage.id});
        return false;
    }
    const nb = bounds.items.len;
    const total = nb + 1;

    // Per-slice lexicographic range predicates. A NULL in ANY key component
    // matches no range — slice 0 takes those via the any-null arm, and the
    // other slices guard against non-leading NULLs sneaking through a
    // leading-column comparison (a leading NULL already fails every branch).
    const non_leading_guard: ?PredicateExpr = if (cols.len > 1) blk: {
        const guards = try aa.alloc(PredicateExpr, cols.len - 1);
        for (cols[1..], guards) |c, *g| g.* = .{ .is_not_null = c };
        break :blk if (guards.len == 1) guards[0] else .{ .@"and" = guards };
    } else null;
    const preds = try aa.alloc(PredicateExpr, total);
    for (preds, 0..) |*p, i| {
        if (i == 0) {
            const arms = try aa.alloc(PredicateExpr, 2);
            arms[0] = try tupleCmpPred(aa, cols, bounds.items[0], .lte);
            arms[1] = try tupleAnyNullPred(aa, cols);
            p.* = .{ .@"or" = arms };
        } else if (i == nb) {
            const gt = try tupleCmpPred(aa, cols, bounds.items[nb - 1], .gt);
            p.* = if (non_leading_guard) |g| blk: {
                const arms = try aa.alloc(PredicateExpr, 2);
                arms[0] = gt;
                arms[1] = g;
                break :blk .{ .@"and" = arms };
            } else gt;
        } else {
            const n_arms: usize = if (non_leading_guard != null) 3 else 2;
            const arms = try aa.alloc(PredicateExpr, n_arms);
            arms[0] = try tupleCmpPred(aa, cols, bounds.items[i - 1], .gt);
            arms[1] = try tupleCmpPred(aa, cols, bounds.items[i], .lte);
            if (non_leading_guard) |g| arms[2] = g;
            p.* = .{ .@"and" = arms };
        }
    }
    if (trace) std.debug.print("[sep] stage#{d} slicing on {d} key col(s) ('{s}'...): {d} slices ({d} sampled)\n", .{ stage.id, cols.len, cols[0], total, samples.len });

    // Scan-once: private table-backed multi-ref blocks fill N per-slice
    // buffers from ONE full-DOP scan instead of being rescanned per slice.
    if (trace) {
        std.debug.print("[sep] phase bounds: {d:.0}ms\n", .{exec.prof.ticksToMs(exec.prof.nowTicks() - ph)});
        ph = exec.prof.nowTicks();
    }
    const pre_parts = try prePartition(ctx, stage, cols, bounds.items, trace);
    if (trace) {
        std.debug.print("[sep] phase pre-partition: {d:.0}ms\n", .{exec.prof.ticksToMs(exec.prof.nowTicks() - ph)});
        ph = exec.prof.nowTicks();
    }
    defer {
        for (pre_parts) |pp| {
            if (pp.shared) {
                // Every slot is the same stage — guard + free exactly once.
                pp.stages[0].releaseUse();
                pp.stages[0].deinit();
            } else for (pp.stages) |st| {
                st.releaseUse(); // drop the guard; frees when slices are done
                st.deinit();
            }
            alloc.free(pp.stages);
        }
        alloc.free(pre_parts);
    }

    const workers = try alloc.alloc(std.Thread, total);
    defer alloc.free(workers);
    const errs = try alloc.alloc(?anyerror, total);
    defer alloc.free(errs);
    @memset(errs, null);

    // Sinks are created HERE: init allocates from the stage allocator, which
    // is only proven for driving-thread use. Slice threads touch only their
    // own sink's c_allocator-backed arenas.
    const row_hint: usize = @intCast(@min(stage.stats_upper_rows, 1 << 22) / total + 1);
    const sinks = try alloc.alloc(mat_stage.ContigSink, total);
    {
        var made: usize = 0;
        while (made < total) : (made += 1) {
            sinks[made] = mat_stage.ContigSink.init(stage.allocator, stage.schema, row_hint) catch |e| {
                for (sinks[0..made]) |*sk| sk.deinit();
                alloc.free(sinks);
                return e;
            };
        }
    }

    // Claiming pool: dop workers eat the slice queue. With cohort-sized
    // slices this IS the cache-friendly execution: each worker's live
    // pipeline holds one small slice end-to-end, so dop concurrent
    // working sets share L3 instead of fighting over DRAM.
    // THINDB_SEP_SEQUENTIAL runs one worker — separates per-slice
    // pipeline cost from concurrency effects.
    const sequential = getenv("THINDB_SEP_SEQUENTIAL") != null;
    const pool_workers: usize = if (sequential) 1 else @min(ctx.input.effectiveDop(), total);
    var claim = SliceClaim{
        .ctx = ctx,
        .stage = stage,
        .pre_parts = pre_parts,
        .preds = preds,
        .sinks = sinks,
        .errs = errs,
    };
    var spawned: usize = 0;
    for (workers[0..pool_workers]) |*th| {
        // Each slice compiles the WHOLE private closure as one block; deep
        // CTE chains overflow the 16 MiB default stack (reserve-only cost).
        th.* = std.Thread.spawn(.{ .stack_size = 1024 << 20 }, SliceClaim.worker, .{&claim}) catch break;
        spawned += 1;
    }
    if (spawned == 0) SliceClaim.worker(&claim); // spawn failed: run inline
    for (workers[0..spawned]) |th| th.join();
    if (trace) {
        std.debug.print("[sep] phase slices: {d:.0}ms\n", .{exec.prof.ticksToMs(exec.prof.nowTicks() - ph)});
        ph = exec.prof.nowTicks();
    }

    var first_err: ?anyerror = null;
    for (errs, 0..) |e, i| {
        if (e) |err| {
            if (trace) std.debug.print("[sep] slice {d} FAILED: {s}\n", .{ i, @errorName(err) });
            if (first_err == null) first_err = err;
        }
    }
    if (first_err) |e| {
        for (sinks) |*sk| sk.deinit();
        alloc.free(sinks);
        return e;
    }
    // Chunk-range metadata (and the MatScan skip it feeds) is single-key
    // only; composite keys concat without ranges — the slice predicates
    // remain the correctness authority either way.
    if (cols.len == 1) {
        const flat = try aa.alloc(types.Value, bounds.items.len);
        for (bounds.items, flat) |t, *f| f.* = t[0];
        try res.adoptSlices(sinks, .{ .col = cols[0], .bounds = flat });
    } else {
        try res.adoptSlices(sinks, null);
    }
    alloc.free(sinks);
    if (trace) std.debug.print("[sep] stage#{d} done: rows={d}\n", .{ stage.id, res.total_rows });
    return true;
}

/// Shared claim state for the slice pool: workers fetch slice indices from
/// the atomic cursor and run each to completion.
const SliceClaim = struct {
    ctx: *SlicedFillCtx,
    stage: *mat_stage.Stage,
    pre_parts: []const PrePart,
    preds: []const PredicateExpr,
    sinks: []mat_stage.ContigSink,
    errs: []?anyerror,
    next: std.atomic.Value(usize) = .init(0),

    fn worker(self: *SliceClaim) void {
        while (true) {
            const i = self.next.fetchAdd(1, .monotonic);
            if (i >= self.preds.len) return;
            sliceWorker(self.ctx, self.stage, self.pre_parts, i, self.preds[i], &self.sinks[i], &self.errs[i]);
        }
    }
};

fn sliceWorker(
    ctx: *SlicedFillCtx,
    stage: *mat_stage.Stage,
    pre_parts: []const PrePart,
    slice_idx: usize,
    pred: PredicateExpr,
    sink: *mat_stage.ContigSink,
    err_out: *?anyerror,
) void {
    const trace = getenv("THINDB_TRACE_SEP") != null;
    const t0 = exec.prof.nowTicks();
    defer if (trace) std.debug.print("[sep]   slice stage#{d} rows={d} {d:.0}ms\n", .{
        stage.id, sink.rows, exec.prof.ticksToMs(exec.prof.nowTicks() - t0),
    });
    // Lease a core from the process-global scheduler so the N single-
    // threaded slice pipelines land on N DISTINCT physical cores instead of
    // wherever the OS migrates them (two slices sharing one core's
    // hyperthreads while others idle). tryAcquire — never block.
    var lease = core_scheduler.global().tryAcquire();
    defer lease.release();
    var arena = std.heap.ArenaAllocator.init(std.heap.c_allocator);
    defer arena.deinit();
    const aa = arena.allocator();
    var sin = ctx.input;
    sin.allocator = aa;
    sin.node_arena = aa;
    sin.accountant = null; // result bytes charged by ensureRun post-join
    sin.dop_cap = 1;
    sin.slice_pred = pred;
    sin.slice_cols = ctx.spec.cols;
    var win_registry: std.AutoHashMapUnmanaged(*const anyopaque, *anyopaque) = .empty;
    sin.win_registry = &win_registry;
    // Two-level staging: shared stages resolve through the outer snapshot
    // (pre-seeded below, so collectStages early-returns on them); the
    // block's PRIVATE closure is absent from it and stages HERE, per slice
    // — each slice runs the whole private chain end-to-end on its key
    // range, 1/N-sized stages included. Same collector, same CSE
    // decisions, own StageSet.
    var slice_map: StageMap = .empty;
    var mit = @constCast(&ctx.map).iterator();
    while (mit.next()) |e| slice_map.put(aa, e.key_ptr.*, e.value_ptr.*) catch |err| {
        err_out.* = err;
        return;
    };
    // Pre-partitioned blocks resolve to this slice's pre-filled buffer —
    // the collector early-returns on them, so no recompiled scan.
    for (pre_parts) |pp| slice_map.put(aa, pp.node, pp.stages[slice_idx]) catch |err| {
        err_out.* = err;
        return;
    };
    const slice_set = mat_stage.StageSet.create(aa) catch |e| {
        err_out.* = e;
        return;
    };
    const slice_cse: MatCse = .{
        .enc_arena = aa,
        .refs = ctx.refs,
        .canon = ctx.canon,
        .bodies = .empty,
    };
    collectStages(sin, ctx.body, slice_set, &slice_map, &slice_cse, null, null) catch |e| {
        err_out.* = e;
        slice_set.deinit();
        return;
    };
    const inner = compileBlock(sin, ctx.body, &slice_map) catch |e| {
        err_out.* = e;
        slice_set.deinit();
        return;
    };
    slice_set.releaseCompilePins();
    var q = mat_stage.StagedRoot.create(aa, inner, slice_set) catch |e| {
        err_out.* = e;
        var qq = inner;
        qq.deinit();
        slice_set.deinit();
        return;
    };
    // The slice's output must line up with the stage schema the sink was
    // built on — re-apply the canonical body's column pruning when the raw
    // output is wider (same projection the staged compiler applied).
    if (!sliceSchemaMatches(q.outputSchema(), stage.schema)) {
        const names = arena.allocator().alloc([]const u8, stage.schema.len) catch |e| {
            q.deinit();
            err_out.* = e;
            return;
        };
        for (stage.schema, names) |sc, *nm| nm.* = sc.name;
        q = q.project(names) catch |e| {
            err_out.* = e;
            return;
        };
    }
    while (true) {
        const batch = q.next() catch |e| {
            err_out.* = e;
            q.deinit();
            return;
        };
        if (batch == null) break;
        sink.append(batch.?) catch |e| {
            err_out.* = e;
            q.deinit();
            return;
        };
    }
    q.deinit();
}

fn sliceSchemaMatches(a: []const types.Column, b: []const types.Column) bool {
    if (a.len != b.len) return false;
    for (a, b) |ca, cb| {
        if (!types.columnNameEql(ca.name, cb.name)) return false;
    }
    return true;
}

/// A pure row-streaming chain — compute / filter / select / exclude / alias —
/// crossing CTE boundaries with NO blocking op, bottoming at exactly one
/// materialized stage, carrying real per-row work (a compute or filter). Such a
/// chain fuses end-to-end into a parallel BUFFER scan that streams (round mode,
/// no full-result copy): the derived columns / predicates evaluate on `max_dop`
/// cores instead of the serial MatScan drain. Gated to multi-chunk stages so
/// the round-pool overhead is amortized.
///
/// `require_work = false` accepts a work-free chain too — a JOIN child wants
/// the parallel leaf even bare: a round-mode ParallelScan probe side lets the
/// join fuse its probe into the stripe workers (and a build side drains in
/// parallel), where a serial MatScan pins the whole probe on one thread.
fn streamingChainOverStage(op: *const ir.Op, map: *StageMap, require_work: bool, see_through: bool) bool {
    var has_work = false;
    var cur = op;
    while (true) {
        switch (cur.*) {
            .materialize => |m| {
                if (map.get(cur)) |stage| {
                    return (has_work or !require_work) and stage.stats_upper_rows > mat_stage.chunk_rows;
                }
                // Single-ref: the body compiles inline at this spot anyway,
                // so a see-through walk continues into it — a window/forced
                // stage inside is then reachable as the chain's parallel
                // source. Only the root join tail opts in: each match is an
                // EAGER compile-time barrier that pins its stage for the
                // whole query, so matching broadly runs every stage
                // simultaneously and blows the memory budget.
                if (!see_through) return false;
                cur = m.upstream;
            },
            .compute => |c| {
                has_work = true;
                cur = c.upstream;
            },
            .filter => |f| {
                has_work = true;
                cur = f.upstream;
            },
            .select => |p| cur = p.upstream,
            .exclude => |e| cur = e.upstream,
            .alias => |a| cur = a.upstream,
            else => return false,
        }
    }
}

/// Build a streaming chain over one materialized stage as a streaming PARALLEL
/// buffer scan: `createOverStage` at the leaf, filter/compute fused into the
/// stripe workers (round mode emits batch-at-a-time, no full-result copy).
/// Mirrors `buildGenericBlock`'s streaming arms but pushes the fusable compute
/// down via `computeDerivedFused`. Precondition: `streamingChainOverStage`.
fn buildFusedStreamOverStage(input: engine_v2.CompileInput, op: *const ir.Op, map: *StageMap, deferred: bool) anyerror!exec.Query {
    switch (op.*) {
        .materialize => |m| {
            if (map.get(op) != null) return (try tryStageParallelScan(input, op, map, deferred)) orelse error.UnsupportedQueryShape;
            return buildFusedStreamOverStage(input, m.upstream, map, deferred);
        },
        .compute => |c| {
            var up = try buildFusedStreamOverStage(input, c.upstream, map, deferred);
            errdefer up.deinit();
            return engine_v2.computeDerivedFused(input.allocator, up, c.derived, input.udf_registry);
        },
        .filter => |f| {
            var up = try buildFusedStreamOverStage(input, f.upstream, map, deferred);
            errdefer up.deinit();
            return up.filter(f.predicate);
        },
        .select => |s| {
            var up = try buildFusedStreamOverStage(input, s.upstream, map, deferred);
            errdefer up.deinit();
            return local.compileSelectProject(input.allocator, up, s);
        },
        .exclude => |e| {
            var up = try buildFusedStreamOverStage(input, e.upstream, map, deferred);
            errdefer up.deinit();
            const remaining = try local.complementColumns(input.allocator, up.outputSchema(), e.columns);
            defer input.allocator.free(remaining);
            return up.project(remaining);
        },
        .alias => |a| {
            var up = try buildFusedStreamOverStage(input, a.upstream, map, deferred);
            errdefer up.deinit();
            return exec.AliasRename.create(input.allocator, up, a.alias);
        },
        else => unreachable,
    }
}

/// A UNION ALL arm that is a streaming chain over a stage — even one buried
/// inside single-ref inline bodies — compiles as a DEFERRED parallel scan:
/// its chain work (the computes an IR union-split pushed into the arm)
/// evaluates in the stripe workers via the terminal compute push. Safe to
/// scope broadly here because SetUnion never forwards aggregate fusion to
/// its inputs, so the deferred leaf's tryFuseAggregate decline can't demote
/// any consumer (the reason this route is NOT in the generic gate below).
fn compileUnionArm(input: engine_v2.CompileInput, op: *const ir.Op, map: *StageMap) anyerror!exec.Query {
    if (input.effectiveDop() > 1 and !input.force_ordered and
        !streamingChainOverStage(op, map, true, false) and
        streamingChainOverStage(op, map, true, true))
    {
        return buildFusedStreamOverStage(input, op, map, true);
    }
    return compileBlock(input, op, map);
}

fn buildGenericBlock(input: engine_v2.CompileInput, op: *const ir.Op, map: *StageMap, block_root: *const ir.Op) anyerror!exec.Query {
    if (input.effectiveDop() > 1 and !input.force_ordered and streamingChainOverStage(op, map, true, false)) return buildFusedStreamOverStage(input, op, map, false);
    switch (op.*) {
        .scan => |s| {
            // Only pg_catalog virtual scans route here; real-table blocks
            // compile through the V2 handlers in compileBlock /
            // compileJoinChild.
            return (try tryPgCatalogLeaf(input, s)) orelse error.UnsupportedQueryShape;
        },
        .single_row => return local.SingleRowSource.create(input.allocator),
        .file_scan => |f| {
            const base = try exec.fileScan(input.allocator, input.db.io, input.db.config.file_scan_access, f, null);
            if (f.alias) |alias| {
                errdefer @constCast(&base).deinit();
                return exec.AliasRename.create(input.allocator, base, alias);
            }
            return base;
        },
        .materialize => |m| {
            // In the map = shared (staged once, read here). Absent = single
            // reference: stream the body inline — no stage copy + re-read,
            // and a table-backed body keeps its full V2 handlers.
            if (map.get(op)) |stage| return wrapSlicePred(input, try mat_stage.MatScan.create(input.allocator, stage));
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
            return engine_v2.computeSelfPushed(up, c.derived, input.udf_registry);
        },
        .order_by => |o| {
            var up = try buildGenericBlock(input, o.upstream, map, block_root);
            errdefer up.deinit();
            return up.orderBy(o.specs);
        },
        .limit => |l| {
            // Fuse ORDER BY + LIMIT into a bounded top-k (keep limit+offset rows
            // in a heap) instead of a full Sort then Limit — the same fusion the
            // table path already does. Over a multi-million-row buffer a full
            // sort for a small LIMIT is the dominant cost. Row-wise layers
            // (projection / rename / compute) between the LIMIT and the ORDER
            // BY commute with the bound: look through them, fuse the top-k
            // below, and re-apply them above — they then run on `n` rows
            // instead of the full sorted input.
            var layers: std.ArrayListUnmanaged(*const ir.Op) = .empty;
            defer layers.deinit(input.allocator);
            var cur: *const ir.Op = l.upstream;
            while (true) {
                switch (cur.*) {
                    .select, .exclude => |p| {
                        try layers.append(input.allocator, cur);
                        cur = p.upstream;
                    },
                    .compute => |c2| {
                        try layers.append(input.allocator, cur);
                        cur = c2.upstream;
                    },
                    .alias => |a| {
                        try layers.append(input.allocator, cur);
                        cur = a.upstream;
                    },
                    else => break,
                }
            }
            if (cur.* == .order_by) {
                const o = cur.order_by;
                var up = try buildGenericBlock(input, o.upstream, map, block_root);
                errdefer up.deinit();
                up = try up.topN(o.specs, @intCast(l.n), @intCast(l.offset));
                // Re-apply the looked-through layers innermost-first.
                var i = layers.items.len;
                while (i > 0) {
                    i -= 1;
                    up = switch (layers.items[i].*) {
                        .select => |s| try local.compileSelectProject(input.allocator, up, s),
                        .exclude => |e| blk: {
                            const remaining = try local.complementColumns(input.allocator, up.outputSchema(), e.columns);
                            defer input.allocator.free(remaining);
                            break :blk try up.project(remaining);
                        },
                        .compute => |c2| try engine_v2.computeSelfPushed(up, c2.derived, input.udf_registry),
                        .alias => |a| try exec.AliasRename.create(input.allocator, up, a.alias),
                        else => unreachable,
                    };
                }
                return up;
            }
            var up = try buildGenericBlock(input, l.upstream, map, block_root);
            errdefer up.deinit();
            return up.limitOffset(@intCast(l.n), @intCast(l.offset));
        },
        .group_by => |g| {
            for (g.aggs) |a| if (a.func == .udf) return error.UnsupportedQueryShape;
            var up = (try tryStageParallelScan(input, g.upstream, map, false)) orelse
                try buildGenericBlock(input, g.upstream, map, block_root);
            errdefer up.deinit();
            // Probe-fused join below: aggregate the joined batches inside
            // the scan workers (partial per chunk, serial combine here)
            // instead of hashing the full join output on this thread.
            if (try group_route.routeJoinPartialGroupBy(input.node_arena, &up, g.group_cols, g.aggs, g.top_k, g.emit_limit)) |q| return q;
            // When the input reads materialized stages, defer the hash-vs-sort
            // decision to runtime: priming those stages yields exact realized
            // row counts, which beat compile-time estimates down a deep CTE
            // chain. With no stages to prime, route now on the (already good)
            // estimate. A stage that ends sorted on the group keys streams; a
            // proven-over-budget / unknown-cardinality input sorts then
            // streams; only a proven-small key space takes the hash aggregate.
            var stages: std.ArrayListUnmanaged(*mat_stage.Stage) = .empty;
            defer stages.deinit(input.allocator);
            try collectBlockStages(input.allocator, g.upstream, map, &stages);
            if (stages.items.len > 0) {
                const owned = try input.allocator.dupe(*mat_stage.Stage, stages.items);
                errdefer input.allocator.free(owned);
                return AdaptiveGroupBy.create(
                    input.allocator,
                    up,
                    owned,
                    g.group_cols,
                    g.aggs,
                    g.top_k,
                    g.emit_limit,
                    input.db.config.query_memory_budget,
                    input.effectiveDop(),
                );
            }
            return group_route.routeGroupBy(
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
            // Ride-the-order: when a same-key window stage feeds this one
            // through an order-preserving row-wise chain, mark the source
            // to adopt its stage in spec order (must happen BEFORE the
            // upstream compile — an eager chain-over-stage barrier there
            // runs the source), compile the chain with parallel seams
            // suppressed, and skip this window's sort.
            var ride = false;
            var eff_input = input;
            var ride_keys: ir.WindowSpec = undefined;
            var borrow_stage: ?*mat_stage.Stage = null;
            if (input.effectiveDop() > 1) {
                const rider_keys: ?ir.WindowSpec = if (sameKeysAllSpecs(w.specs)) w.specs[0] else null;
                if (rideSource(map, w.upstream, rider_keys)) |src| {
                    if (src.covered) {
                        src.win.?.emit_sorted = true;
                        ride = true;
                        ride_keys = src.keys.?;
                    }
                    if (!src.has_filter) {
                        borrow_stage = src.stage;
                        if (src.win == null) src.stage.want_contiguous = true;
                    }
                    // Both riding and borrowing need the chain to deliver
                    // rows in the source's order (borrowing additionally
                    // 1:1) — suppress order-scrambling parallel seams.
                    if (ride or borrow_stage != null) eff_input.force_ordered = true;
                }
            }
            var up = try compileBlock(eff_input, w.upstream, map);
            errdefer up.deinit();
            // Prune the window's input to the columns the window or
            // anything above it references: the below block otherwise
            // emits its fused-filter columns too (often a wide string),
            // which would ride the blocking accumulate/sort/emit for
            // nothing. Skipped when the shape above isn't fully understood.
            // A synthetic window stage (block_root == op, the wrap from
            // wrapWindowsInMaterialize) has its consumers in OTHER blocks;
            // there the tree-wide referenced-name set stands in for the
            // invisible layers above (null = unaccountable shape somewhere
            // → keep everything).
            const bare_stage_body = block_root == op;
            const global_refs: ?[]const []const u8 = if (bare_stage_body) input.prune_names else null;
            if (local.windowInputNames(input.allocator, block_root, op)) |names| {
                defer input.allocator.free(names);
                if (!bare_stage_body or global_refs != null) {
                    const schema = up.outputSchema();
                    var kept = try std.ArrayListUnmanaged([]const u8).initCapacity(input.allocator, schema.len);
                    defer kept.deinit(input.allocator);
                    outer: for (schema) |col| {
                        for (names) |nm| {
                            if (columnRefMatchesName(col.name, nm)) {
                                kept.appendAssumeCapacity(col.name);
                                continue :outer;
                            }
                        }
                        if (global_refs) |refs| for (refs) |nm| {
                            if (columnRefMatchesName(col.name, nm)) {
                                kept.appendAssumeCapacity(col.name);
                                continue :outer;
                            }
                        };
                    }
                    if (kept.items.len > 0 and kept.items.len < schema.len) {
                        up = try up.project(kept.items);
                    }
                }
            }
            const wq = try up.window(w.specs, w.calls, input.effectiveDop());
            // SEPARABLE slice window-chain pairing: when this window's IR
            // chain reaches an upstream window with the SAME partition set
            // through grouping-preserving ops, that window emits in its
            // sorted order (one gather pass) and this one is compile-proven
            // grouped — skipping its full sort. Chains cascade: a grouped
            // window retains its own perm and hands the order onward.
            if (input.win_registry) |reg| pair: {
                const wtrace = getenv("THINDB_TRACE_SEP") != null;
                const win = exec.queryAs(window_op.Window, wq) orelse break :pair;
                reg.put(input.allocator, @ptrCast(op), @ptrCast(win)) catch break :pair;
                const chain = windowChainBelow(w.upstream, map) orelse {
                    if (wtrace) std.debug.print("[sep] pair decline: no window below (stopped at {s})\n", .{windowChainStopTag(w.upstream, map)});
                    break :pair;
                };
                const below = chain.win;
                const prev_opaque = reg.get(@ptrCast(below)) orelse {
                    if (wtrace) std.debug.print("[sep] pair decline: below window not in registry\n", .{});
                    break :pair;
                };
                const prev: *window_op.Window = @ptrCast(@alignCast(prev_opaque));
                if (!prev.singleSortGroup()) {
                    if (wtrace) std.debug.print("[sep] pair decline: emitter multi-sort-group\n", .{});
                    break :pair; // emitter needs ONE perm
                }
                if (!irAllSpecsSamePartition(op)) {
                    if (wtrace) std.debug.print("[sep] pair decline: rider specs differ in partition\n", .{});
                    break :pair; // rider: grouping only, orders may differ
                }
                if (!windowPartitionSetsEqual(below, op)) {
                    if (wtrace) std.debug.print("[sep] pair decline: partition sets differ\n", .{});
                    break :pair;
                }
                if (chainRenamesPartitionKey(w.upstream, below, op.window.specs[0].partition_by)) {
                    if (wtrace) std.debug.print("[sep] pair decline: chain renames a partition key\n", .{});
                    break :pair;
                }
                // Every join the chain crossed must provably preserve
                // left-side grouping: probe = left (fixed at create), no
                // skew handoff (SMJ reorders), INNER/LEFT only, and no
                // right-side output column shadowing a partition key.
                for (chain.joins[0..chain.n_joins]) |jn| {
                    const jopq = reg.get(@ptrCast(jn)) orelse {
                        if (wtrace) std.debug.print("[sep] pair decline: chain join not registered\n", .{});
                        break :pair;
                    };
                    const jop: *join_mod.Join = @ptrCast(@alignCast(jopq));
                    if (jop.build_is_left) {
                        if (wtrace) std.debug.print("[sep] pair decline: join probes right side\n", .{});
                        break :pair;
                    }
                    if (jop.skew_detector != null or jop.skew_smj != null) {
                        if (wtrace) std.debug.print("[sep] pair decline: join may hand off to skew SMJ\n", .{});
                        break :pair;
                    }
                    if (jop.join_type != .left and jop.join_type != .inner) {
                        if (wtrace) std.debug.print("[sep] pair decline: join type\n", .{});
                        break :pair;
                    }
                    for (jop.output_schema[jop.left_col_count..]) |c| {
                        for (op.window.specs[0].partition_by) |k| {
                            if (types.columnNameEql(c.name, k)) {
                                if (wtrace) std.debug.print("[sep] pair decline: join right side shadows a partition key\n", .{});
                                break :pair;
                            }
                        }
                    }
                }
                prev.emit_sorted = true;
                prev.emit_sorted_stream = true;
                win.assume_grouped = true;
                if (wtrace) {
                    std.debug.print("[sep] window pair: upstream emits sorted, this one rides grouped\n", .{});
                }
            }
            if (ride or borrow_stage != null) {
                if (exec.queryAs(window_op.Window, wq)) |win| {
                    if (ride) {
                        win.assume_sorted = true;
                        win.inherited_order = ride_keys;
                    }
                    if (borrow_stage) |src_stage| {
                        const in_schema = win.input_schema;
                        const bm = try input.node_arena.alloc(?usize, in_schema.len);
                        var any = false;
                        for (in_schema, bm) |col, *slot| {
                            slot.* = borrowIdxFor(map, w.upstream, col.name, src_stage);
                            if (slot.* != null) any = true;
                        }
                        if (any) {
                            win.borrow_src = src_stage;
                            win.borrow_map = bm;
                        }
                    }
                }
            }
            return wq;
        },
        .join => |j| {
            // INNER picks its build side from realized stats at exec, so
            // EITHER child may end up the probe — both take the parallel
            // probe route (a deferred leaf that becomes the build side just
            // drains in parallel; small dims decline via the stage row gate).
            var left = try compileJoinChild(input, j.left, map, j.join_type == .left or j.join_type == .inner);
            errdefer left.deinit();
            const right = try compileJoinChild(input, j.right, map, j.join_type == .right or j.join_type == .inner);
            const jq = try left.join(right, joinSpecOf(j));
            // Window-chain pairing: register the compiled hash join so a
            // downstream window can VERIFY (at pairing time) that this
            // join's probe is the left side — probe-order emission then
            // preserves left-side grouping through the join.
            if (input.win_registry) |reg| {
                if (exec.queryAs(join_mod.Join, jq)) |jop| {
                    reg.put(input.allocator, @ptrCast(op), @ptrCast(jop)) catch {};
                }
            }
            return jq;
        },
        .set_union => |u| {
            // SetUnion.create validates schema compatibility and does NOT
            // consume its inputs on error — both sides need errdefers (the
            // width-mismatch path is exercised by tests).
            var left = try compileUnionArm(input, u.left, map);
            errdefer left.deinit();
            var right = try compileUnionArm(input, u.right, map);
            errdefer right.deinit();
            return exec.SetUnion.create(input.allocator, left, right, u.all);
        },
        else => return error.UnsupportedQueryShape,
    }
}

/// Ride-the-order eligibility: every spec of the (potential rider) window
/// shares one (PARTITION BY, ORDER BY) key set — a single permutation
/// describes the op.
fn sameKeysAllSpecs(specs: []const ir.WindowSpec) bool {
    if (specs.len == 0) return false;
    for (specs[1..]) |sp| {
        if (!sameSpecKeys(specs[0], sp)) return false;
    }
    return true;
}

fn sameSpecKeys(a: ir.WindowSpec, b: ir.WindowSpec) bool {
    if (a.partition_by.len != b.partition_by.len or a.order_by.len != b.order_by.len) return false;
    for (a.partition_by, b.partition_by) |x, y| {
        if (!columnRefMatchesName(x, y)) return false;
    }
    for (a.order_by, b.order_by) |x, y| {
        if (!columnRefMatchesName(x.col, y.col) or x.desc != y.desc) return false;
    }
    return true;
}

/// Walk a rider window's upstream chain looking for a same-key window
/// stage whose adopted buffers can arrive here order-preserved. Crossable:
/// filters (drop rows, keep order), computes (append columns — must not
/// redefine a key name), explicit column projections that pass every key
/// through unrenamed, and single-ref (inlined) materialize boundaries.
/// Everything else — unions, joins, group-bys, sorts, multi-ref stages
/// (their rematerialization pull isn't order-preserving) — stops the walk.
const RideSrc = struct {
    win: ?*window_op.Window,
    stage: *mat_stage.Stage,
    /// The source's effective emit order, when it has a single usable one.
    keys: ?ir.WindowSpec,
    /// keys covers the rider's spec → the rider can skip its sort.
    covered: bool,
    /// A filter sits between source and rider: rows still ordered (riding
    /// fine) but no longer 1:1 with the source (borrowing impossible).
    has_filter: bool,
};

/// Rider coverage: the source's emitted order satisfies the rider when the
/// partition lists match exactly and the rider's ORDER BY is a (possibly
/// empty) prefix of the source's — partition-only riders need adjacency
/// only, and full-key riders need the same order.
fn riderCoveredBy(src: ir.WindowSpec, rider: ir.WindowSpec) bool {
    if (src.partition_by.len != rider.partition_by.len) return false;
    for (src.partition_by, rider.partition_by) |x, y| {
        if (!columnRefMatchesName(x, y)) return false;
    }
    if (rider.order_by.len > src.order_by.len) return false;
    for (rider.order_by, src.order_by[0..rider.order_by.len]) |x, y| {
        if (!columnRefMatchesName(x.col, y.col) or x.desc != y.desc) return false;
    }
    return true;
}

fn rideSource(map: *StageMap, op: *const ir.Op, keys: ?ir.WindowSpec) ?RideSrc {
    var cur = op;
    var has_filter = false;
    while (true) {
        switch (cur.*) {
            .filter => |f| {
                has_filter = true;
                cur = f.upstream;
            },
            .compute => |c| {
                if (keys) |k| for (c.derived) |d| {
                    if (nameIsSpecKey(d.name, k)) return null;
                };
                cur = c.upstream;
            },
            .exclude => |e| {
                if (keys) |k| for (e.columns) |col| {
                    if (nameIsSpecKey(col, k)) return null;
                };
                cur = e.upstream;
            },
            .select => |sel| {
                if (keys) |k| if (!selectPassesKeys(sel, k)) return null;
                cur = sel.upstream;
            },
            .materialize => |m| {
                if (map.get(cur)) |stage| {
                    const win = stage.adopt_window orelse {
                        // Not a window stage — no order to ride, but a
                        // filterless chain can still BORROW its columns:
                        // ask it to materialize contiguous.
                        return .{ .win = null, .stage = stage, .keys = null, .covered = false, .has_filter = has_filter };
                    };
                    const src_keys = win.effectiveEmitKeys();
                    const covered = if (keys) |k|
                        (if (src_keys) |sk| riderCoveredBy(sk, k) else false)
                    else
                        false;
                    return .{ .win = win, .stage = stage, .keys = src_keys, .covered = covered, .has_filter = has_filter };
                }
                // single-ref: the body compiles inline at this use site —
                // no rematerialization between us and it.
                cur = m.upstream;
            },
            else => return null,
        }
    }
}

/// Second walk of the ride chain for ONE window-input column name: does it
/// pass through untransformed all the way to the source stage, and at which
/// source column? Null = compute-derived / renamed / dropped → the window
/// accumulates it normally.
fn borrowIdxFor(map: *StageMap, op: *const ir.Op, name: []const u8, src_stage: *mat_stage.Stage) ?usize {
    var cur = op;
    while (true) {
        switch (cur.*) {
            .filter => |f| cur = f.upstream,
            .compute => |c| {
                for (c.derived) |d| {
                    if (columnRefMatchesName(d.name, name)) return null;
                }
                cur = c.upstream;
            },
            .exclude => |e| cur = e.upstream,
            .select => |sel| {
                if (!projectionKeepsName(sel, name)) return null;
                cur = sel.upstream;
            },
            .materialize => {
                if (map.get(cur)) |stage| {
                    if (stage != src_stage) return null;
                    for (stage.schema, 0..) |col, i| {
                        if (columnRefMatchesName(col.name, name)) return i;
                    }
                    return null;
                }
                cur = cur.materialize.upstream;
            },
            else => return null,
        }
    }
}

fn nameIsSpecKey(name: []const u8, keys: ir.WindowSpec) bool {
    for (keys.partition_by) |k| {
        if (columnRefMatchesName(name, k)) return true;
    }
    for (keys.order_by) |k| {
        if (columnRefMatchesName(name, k.col)) return true;
    }
    return false;
}

/// Every spec key must appear in the projection unrenamed (star entries and
/// key renames bail — over-caution is correctness here).
fn selectPassesKeys(sel: ir.Op.Project, keys: ir.WindowSpec) bool {
    for (keys.partition_by) |k| {
        if (!projectionKeepsName(sel, k)) return false;
    }
    for (keys.order_by) |k| {
        if (!projectionKeepsName(sel, k.col)) return false;
    }
    return true;
}

fn projectionKeepsName(sel: ir.Op.Project, key: []const u8) bool {
    for (sel.columns, 0..) |col, i| {
        if (std.mem.eql(u8, col, "*")) return true; // star passes everything through
        if (!columnRefMatchesName(col, key)) continue;
        // found the key — renamed?
        if (sel.outputs) |outs| {
            if (outs[i]) |alias| {
                if (!columnRefMatchesName(alias, key)) return false;
            }
        }
        return true;
    }
    return false;
}

/// Drop stage output columns no consumer can reference. With a non-null
/// flat referenced-name set (analyzeProjection over the whole tree — null
/// means an unaccountable shape like `SELECT *` exists somewhere), a stage
/// column matching no referenced name is dead in EVERY consumer, so
/// materializing it only burns buffer memory + copy time. Matching is
/// suffix-aware both ways (qualified stage columns vs qualified refs) —
/// over-keeping is safe, over-pruning is not. Best-effort: any hiccup
/// (name collision in the projection, OOM) keeps the unpruned stage.
fn pruneStageColumns(input: engine_v2.CompileInput, q: exec.Query) exec.Query {
    const refs = input.prune_names orelse return q;
    const schema = q.outputSchema();
    var keep: std.ArrayListUnmanaged([]const u8) = .empty;
    defer keep.deinit(input.allocator);
    outer: for (schema) |col| {
        for (refs) |r| {
            if (columnRefMatchesName(col.name, r)) {
                keep.append(input.allocator, col.name) catch return q;
                continue :outer;
            }
        }
    }
    if (keep.items.len == 0 or keep.items.len == schema.len) return q;
    return q.project(keep.items) catch q;
}

fn columnRefMatchesName(column_name: []const u8, ref_name: []const u8) bool {
    if (types.columnNameEql(column_name, ref_name)) return true;
    if (std.mem.lastIndexOfScalar(u8, ref_name, '.')) |dot| {
        if (types.columnNameEql(column_name, ref_name[dot + 1 ..])) return true;
    }
    if (std.mem.lastIndexOfScalar(u8, column_name, '.')) |dot| {
        if (types.columnNameEql(column_name[dot + 1 ..], ref_name)) return true;
    }
    return false;
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

    var left = try compileJoinChild(input, j.left, map, j.join_type == .left or j.join_type == .inner);
    var left_owned = true;
    errdefer if (left_owned) left.deinit();
    var right = try compileJoinChild(input, j.right, map, j.join_type == .right or j.join_type == .inner);
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

extern "c" fn getenv(name: [*:0]const u8) ?[*:0]const u8;

/// Profiling override: when `THINDB_PROFILE_FORCE_STAGE` is set, materialize
/// every CTE (disable single-ref inlining) so each one shows as its own stage.
fn forceStageAll() bool {
    return getenv("THINDB_PROFILE_FORCE_STAGE") != null;
}

/// Profiling override: when `THINDB_PROFILE_STAGE_BARRIERS` is set, materialize
/// only single-ref CTEs whose own operations already form a barrier.
fn stageBarriersOnly() bool {
    return getenv("THINDB_PROFILE_STAGE_BARRIERS") != null;
}

/// Experiment override: when `THINDB_NO_STAGE` is set, never materialize a
/// multi-ref CTE — inline (recompute) its body at every use site instead of
/// staging it once and re-reading. Measures the cost of the barrier
/// materialize-then-rescan vs. independent streamed recomputation. Forced
/// (`AS MATERIALIZED`) and union-bodied CTEs still stage (correctness).
fn noStage() bool {
    return getenv("THINDB_NO_STAGE") != null;
}

/// True when this CTE body's top-level operations include a blocking
/// (pipeline-breaking) operator — GROUP BY, ORDER BY, WINDOW, JOIN, or a set
/// op — which buffers its whole input before emitting. Peels streaming wrappers
/// (select/filter/compute/…); stops at a leaf or a nested materialize boundary
/// (that's a different CTE's barrier, not this one's).
fn bodyFormsBarrier(op: *const ir.Op) bool {
    var cur = op;
    while (true) {
        switch (cur.*) {
            .group_by, .order_by, .window, .join, .set_union => return true,
            .select => |p| cur = p.upstream,
            .exclude => |p| cur = p.upstream,
            .filter => |f| cur = f.upstream,
            .compute => |c| cur = c.upstream,
            .alias => |a| cur = a.upstream,
            .limit => |l| cur = l.upstream,
            else => return false,
        }
    }
}
