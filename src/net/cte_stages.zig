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
const join_mod = @import("../exec/join.zig");
const udf = @import("../udf.zig");

const PredicateExpr = exec.predicate.PredicateExpr;

const StageMap = std.AutoHashMapUnmanaged(*const ir.Op, *mat_stage.Stage);

/// True when the plan needs the staged compiler: any materialize boundary
/// (CTE / FROM-subquery), join, window, or set-union node anywhere in the
/// tree (each union side compiles as its own block). Non-table leaves
/// (FROM-less SELECT, CSV/JSON file scans) also compile here — generic
/// operators over the leaf, no table-backed V2 handler applies.
pub fn needsStaging(op: *const ir.Op) bool {
    return switch (op.*) {
        .materialize, .join, .window, .set_union, .single_row, .file_scan, .table_fn => true,
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
pub fn compileStaged(input_in: engine_v2.CompileInput, root: *const ir.Op, stage_count_out: ?*u32) anyerror!exec.Query {
    // Compiled-join registry (IR node → operator): lets a rider that
    // crossed a join through rideSource verify the compiled operator's
    // order guarantees post-compile. Compile-lifetime only.
    var join_registry: std.AutoHashMapUnmanaged(*const anyopaque, *anyopaque) = .empty;
    defer join_registry.deinit(input_in.allocator);
    var input = input_in;
    if (input.win_registry == null) input.win_registry = &join_registry;

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
    const t_count_mat_refs = exec.prof.nowTicks();
    try countMatRefs(input.allocator, root, &cse);
    exec.prof.addPhase("compile.count_mat_refs", @intCast(exec.prof.nowTicks() - t_count_mat_refs));
    // Keyed pipeline regions (`WITH KEYED BY (...)`): a declared block
    // pre-registers its stage here, BEFORE the window-wrap rewrite mutates
    // nodes below its anchor; collectStages' map hit then stops the walk at
    // the anchor and compileBlock reads the region output as an ordinary
    // stage. The declaration is a hard contract — verification/compile
    // failures are query errors, never a silent fall-back.
    const t_region_declared = exec.prof.nowTicks();
    const declared = try @import("region_rollforward.zig").compileDeclared(input, root);
    exec.prof.addPhase("compile.region_declared", @intCast(exec.prof.nowTicks() - t_region_declared));
    if (declared) |rec| {
        const stage = try set.addStage(rec.query, input.accountant);
        const rep = cse.canon.get(rec.anchor) orelse rec.anchor;
        try map.put(input.allocator, rep, stage);
        if (rep != rec.anchor) try map.put(input.allocator, rec.anchor, stage);
        std.debug.print("[region] region engaged\n", .{});
    }
    wrapWindowsInMaterialize(input.node_arena, @constCast(root), &cse, null) catch {};
    const t_collect_stages = exec.prof.nowTicks();
    try collectStages(input, root, set, &map, &cse);
    exec.prof.addPhase("compile.collect_stages", @intCast(exec.prof.nowTicks() - t_collect_stages));
    if (stage_count_out) |out| out.* = @intCast(set.stages.items.len);
    const t_final_block = exec.prof.nowTicks();
    const inner = try compileBlock(input, root, &map);
    exec.prof.addPhase("compile.final_block", @intCast(exec.prof.nowTicks() - t_final_block));
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
/// `scope` is the nearest enclosing CTE's name; the synthetic stages this
/// creates carry it ("<cte>/window") so the `[cte]` profile lines stay
/// attributable.
fn wrapWindowsInMaterialize(node_arena: Allocator, op: *ir.Op, cse: *const MatCse, scope: ?[]const u8) anyerror!void {
    switch (op.*) {
        .select => |*p| try wrapWindowChild(node_arena, &p.upstream, cse, scope),
        .exclude => |*p| try wrapWindowChild(node_arena, &p.upstream, cse, scope),
        .filter => |*f| try wrapWindowChild(node_arena, &f.upstream, cse, scope),
        .order_by => |*o| try wrapWindowChild(node_arena, &o.upstream, cse, scope),
        .group_by => |*g| try wrapWindowChild(node_arena, &g.upstream, cse, scope),
        .compute => |*c| try wrapWindowChild(node_arena, &c.upstream, cse, scope),
        .alias => |*a| try wrapWindowChild(node_arena, &a.upstream, cse, scope),
        .limit => |*l| try wrapWindowChild(node_arena, &l.upstream, cse, scope),
        .window => |*w| try wrapWindowChild(node_arena, &w.upstream, cse, scope),
        .table_fn => |*t| {
            // A multi-input TVF drains every input serially on its own
            // thread (executeMulti). An input whose body does real
            // streaming work (union / join / aggregate) would execute that
            // work single-threaded inside the drain — stage it so it fills
            // through the parallel buffer-scan seams and the drain becomes
            // a buffer read (and, filterless, a zero-copy borrow).
            if (t.inputs.len > 1) {
                const wrapped = try node_arena.alloc(*ir.Op, t.inputs.len);
                for (t.inputs, wrapped) |inp, *slot| {
                    slot.* = inp;
                    if (tvfInputDoesWork(inp, cse)) {
                        const wrap = try node_arena.create(ir.Op);
                        wrap.* = .{ .materialize = .{ .upstream = inp, .forced = true, .name = try wrapName(node_arena, scope, "tvf-input") } };
                        slot.* = wrap;
                    }
                }
                t.inputs = wrapped;
            }
            for (t.inputs) |inp| try wrapWindowsInMaterialize(node_arena, inp, cse, scope);
        },
        .materialize => |*m| {
            // A CTE that will stage anyway (multi-ref or AS MATERIALIZED)
            // and whose body IS the window adopts it directly through its
            // own stage — an inner wrap would only add a copy layer. A
            // single-ref (inlining) CTE body still wants the wrap.
            const rep = cse.canon.get(op) orelse op;
            const stages_anyway = (cse.refs.get(rep) orelse 1) > 1 or op.materialize.forced;
            const inner_scope = m.name orelse scope;
            if (stages_anyway and (m.upstream.* == .window or m.upstream.* == .table_fn)) {
                try wrapWindowsInMaterialize(node_arena, m.upstream, cse, inner_scope);
            } else {
                try wrapWindowChild(node_arena, &m.upstream, cse, inner_scope);
            }
        },
        .join => |*j| {
            try wrapWindowChild(node_arena, &j.left, cse, scope);
            try wrapWindowChild(node_arena, &j.right, cse, scope);
        },
        .set_union => |*u| {
            try wrapWindowChild(node_arena, &u.left, cse, scope);
            try wrapWindowChild(node_arena, &u.right, cse, scope);
        },
        else => {},
    }
}

fn wrapName(node_arena: Allocator, scope: ?[]const u8, kind: []const u8) !?[]const u8 {
    const cte = scope orelse return null;
    return try std.fmt.allocPrint(node_arena, "{s}/{s}", .{ cte, kind });
}

/// Does a TVF input's body, peeled of thin projection wrappers, bottom out
/// in an operation that streams real work (union / join / aggregate)?
/// Windows and TVFs are excluded — wrapWindowChild stages those itself —
/// and a chain ending at a shared (multi-ref / forced) materialize already
/// has a stage the borrow walk will find.
fn tvfInputDoesWork(op: *const ir.Op, cse: *const MatCse) bool {
    var cur = op;
    while (true) switch (cur.*) {
        .select => |s| cur = s.upstream,
        .compute => |c| cur = c.upstream,
        .exclude => |e| cur = e.upstream,
        .filter => |f| cur = f.upstream,
        .alias => |al| cur = al.upstream,
        .materialize => |m| {
            const rep = cse.canon.get(cur) orelse cur;
            if ((cse.refs.get(rep) orelse 1) > 1 or cur.materialize.forced) return false;
            cur = m.upstream;
        },
        .set_union, .join, .group_by => return true,
        else => return false,
    };
}

fn wrapWindowChild(node_arena: Allocator, child: **ir.Op, cse: *const MatCse, scope: ?[]const u8) anyerror!void {
    const c = child.*;
    // table_fn gets the same treatment as window: its output is already a
    // fully materialized buffer, and without a stage everything above it
    // (joins, computes, downstream window accumulation) degrades to one
    // serial chain on the connection thread — the stage restores the
    // parallel buffer-scan seams.
    if (c.* == .window or c.* == .table_fn) {
        const wrap = try node_arena.create(ir.Op);
        const kind: []const u8 = if (c.* == .window) "window" else "tvf";
        wrap.* = .{ .materialize = .{ .upstream = c, .forced = true, .name = try wrapName(node_arena, scope, kind) } };
        child.* = wrap;
    }
    try wrapWindowsInMaterialize(node_arena, c, cse, scope);
}

const MatRefCounts = std.AutoHashMapUnmanaged(*const ir.Op, u32);

/// Reference counting + structural CSE state for materialize nodes.
/// `canon` maps an opted-in duplicate node (a FROM-subquery whose IR encodes
/// byte-identically to an earlier one) to its representative; `refs` counts
/// references per CANONICAL node. Encoding scratch and the bytes→node table
/// live in `enc_arena` (freed after stage collection).
const MatCse = struct {
    refs: MatRefCounts = .empty,
    canon: std.AutoHashMapUnmanaged(*const ir.Op, *const ir.Op) = .empty,
    bodies: std.StringHashMapUnmanaged(*const ir.Op) = .empty,
    enc_arena: Allocator,
};

/// Count how many places in the tree reference each materialize node,
/// merging opted-in structurally identical bodies (byte-equal IR encodings)
/// into one canonical node first — so two copies of the same FROM-subquery
/// (a self-join over identical subqueries) count as TWO references to
/// ONE node and share a stage instead of scanning twice. A canonical
/// node referenced once gets no stage — its body compiles inline at the
/// use site (materializing only to re-read once is a pure copy tax).
/// A shared node's subtree is walked once, mirroring collectStages.
fn countMatRefs(allocator: Allocator, op: *const ir.Op, cse: *MatCse) anyerror!void {
    switch (op.*) {
        .materialize => |m| {
            const rep: *const ir.Op = if (!m.structural_cse) op else blk: {
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
        .table_fn => |t| for (t.inputs) |inp| try countMatRefs(allocator, inp, cse),
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
fn collectStages(
    input: engine_v2.CompileInput,
    op: *const ir.Op,
    set: *mat_stage.StageSet,
    map: *StageMap,
    cse: *const MatCse,
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
            try collectStages(input, rep.materialize.upstream, set, map, cse);
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
            if ((single_ref or noStage()) and !rep.materialize.forced and !prof_stage) return;
            const c0 = if (exec.prof.enabled) exec.prof.nowTicks() else 0;
            const child0 = if (exec.prof.enabled) exec.prof.cteChildTicks() else 0;
            var q = try compileBlock(input, rep.materialize.upstream, map);
            // A window-rooted body hands its buffers to the stage zero-copy
            // (Stage.adopt_window); output pruning would wrap a Project over
            // the window and break the root identity — and with adoption the
            // copy it would save no longer exists.
            const win_root: ?*window_op.Window = if (exec.queryAs(window_op.Window, q)) |w| (if (w.adoptable()) w else null) else null;
            // A TVF-rooted body hands its output stores to the stage the
            // same way (adopt_table_fn) — and like the window case, pruning
            // would wrap a Project over the root and break its identity.
            const tvf_root = if (win_root == null) exec.queryAs(exec.table_fn.TableFnExec, q) else null;
            if (getenv("THINDB_TRACE_FUSE") != null and win_root == null and tvf_root == null) {
                var buf: std.ArrayList(u8) = .empty;
                defer buf.deinit(input.allocator);
                q.explain(&buf, input.allocator, 0) catch {};
                const first = if (std.mem.indexOfScalar(u8, buf.items, '\n')) |nl| buf.items[0..nl] else buf.items;
                std.debug.print("[stage-noadopt] root={s}\n", .{first});
            }
            const t_prune = exec.prof.nowTicks();
            if (win_root == null and tvf_root == null) q = pruneStageColumns(input, q);
            exec.prof.addPhase("compile.stage.prune", @intCast(exec.prof.nowTicks() - t_prune));
            const t_add = exec.prof.nowTicks();
            const stage = try set.addStage(q, input.accountant);
            exec.prof.addPhase("compile.stage.add", @intCast(exec.prof.nowTicks() - t_add));
            stage.name = rep.materialize.name orelse "";
            stage.adopt_window = win_root;
            stage.adopt_table_fn = tvf_root;
            if (win_root) |wr| {
                if (wr.borrow_src) |src| {
                    src.registerUse();
                    stage.pinned_upstream = src;
                }
            }
            // Compile-time seams (eager stage scans) run upstream stages inside
            // this block's compile; those charge cte_child_ticks, so net them
            // out and setup stays the block's own compile time.
            if (exec.prof.enabled) {
                const nested: i64 = @intCast(exec.prof.cteChildTicks() - child0);
                stage.setup_ticks = (exec.prof.nowTicks() - c0) - nested;
            }
            try map.put(input.allocator, rep, stage);
            if (rep != op) try map.put(input.allocator, op, stage);
        },
        .select => |p| try collectStages(input, p.upstream, set, map, cse),
        .exclude => |p| try collectStages(input, p.upstream, set, map, cse),
        .filter => |f| try collectStages(input, f.upstream, set, map, cse),
        .order_by => |o| try collectStages(input, o.upstream, set, map, cse),
        .group_by => |g| try collectStages(input, g.upstream, set, map, cse),
        .compute => |c| try collectStages(input, c.upstream, set, map, cse),
        .alias => |a| try collectStages(input, a.upstream, set, map, cse),
        .limit => |l| try collectStages(input, l.upstream, set, map, cse),
        .window => |w| try collectStages(input, w.upstream, set, map, cse),
        .table_fn => |t| for (t.inputs) |inp| try collectStages(input, inp, set, map, cse),
        .join => |j| {
            try collectStages(input, j.left, set, map, cse);
            try collectStages(input, j.right, set, map, cse);
        },
        .set_union => |u| {
            try collectStages(input, u.left, set, map, cse);
            try collectStages(input, u.right, set, map, cse);
        },
        else => {},
    }
}

const BlockSource = enum { table, leaf, mat, join, window, set_union, table_fn, unsupported };

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
            .table_fn => return .table_fn,
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

pub fn compileBlock(input: engine_v2.CompileInput, op: *const ir.Op, map: *StageMap) anyerror!exec.Query {
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
        else blk: {
            const t_leaf = exec.prof.nowTicks();
            defer exec.prof.addPhase("compile.select_block", @intCast(exec.prof.nowTicks() - t_leaf));
            break :blk engine_v2.compileSelectBlock(input, op);
        },
        // Stage-, join-, window-, union- or non-table-leaf-backed block:
        // generic operators over MatScan / Join / Window / SetUnion /
        // SingleRow / FileScan leaves. The heavy inputs were already produced
        // by upstream stage handlers or stream in from table-backed child
        // blocks; single-row and file leaves are not perf shapes.
        .leaf, .mat, .join, .window, .set_union, .table_fn => buildGenericBlock(input, op, map, op),
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
            const t_leaf2 = exec.prof.nowTicks();
            var q = try engine_v2.compileSelectBlock(input, stripped);
            exec.prof.addPhase("compile.select_block", @intCast(exec.prof.nowTicks() - t_leaf2));
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
    // A force_ordered chain (a downstream consumer rides the source order)
    // keeps the probe serial: the deferred parallel buffer scan feeds the
    // join in stripe-completion order.
    if (input.effectiveDop() > 1 and is_probe and !input.force_ordered and
        streamingChainOverStage(op, map, false, true))
    {
        return buildFusedStreamOverStage(input, op, map, .deferred);
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
    /// Thread-safe allocator for parallel routes (global reduce workers).
    worker_alloc: Allocator,
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
        worker_alloc: Allocator,
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
            .worker_alloc = worker_alloc,
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
        const trace_gb = getenv("THINDB_TRACE_GBROUTE") != null;
        if (trace_gb) {
            std.debug.print("[gbroute-adaptive] keys={d} aggs=", .{self.group_cols.len});
            for (self.aggs) |a| std.debug.print("{s},", .{@tagName(a.func)});
            std.debug.print(
                " upper_rows={d} max_dop={d} top_k={} emit_limit={} stages={d}\n",
                .{ self.up.stats().upper_rows, self.max_dop, self.top_k != null, self.emit_limit != null, self.stages.len },
            );
        }
        // A GLOBAL aggregate (no keys) over a big primed buffer: fold partials
        // on `max_dop` workers sharing the buffer reads instead of draining
        // the whole input serially on this thread. Declines itself (falling
        // through to the serial path) when the input's batch data isn't
        // stable or an aggregate isn't two-phase combinable.
        if (self.group_cols.len == 0 and self.top_k == null and self.emit_limit == null and
            exec.force_group_by == .auto and getenv("THINDB_NO_PARALLEL_GROUP") == null)
        {
            if (try group_route.routeGlobalReduce(self.allocator, self.worker_alloc, &self.up, self.aggs, self.max_dop)) |q| {
                if (trace_gb) std.debug.print("[gbroute-adaptive]   -> global reduce\n", .{});
                self.chosen = q;
                return;
            }
        }
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
                if (trace_gb) std.debug.print("[gbroute-adaptive]   -> stream\n", .{});
                self.chosen = q;
                return;
            }
            if (try group_route.routeRadixGroupBy(self.up, self.group_cols, self.aggs, self.top_k, self.emit_limit)) |q| {
                if (trace_gb) std.debug.print("[gbroute-adaptive]   -> radix\n", .{});
                self.chosen = q;
                return;
            }
            if (trace_gb) std.debug.print("[gbroute-adaptive]   -> partitioned\n", .{});
            self.chosen = try partitioned_aggregate.PartitionedAggregate.create(
                self.allocator,
                self.up,
                self.group_cols,
                self.aggs,
                self.max_dop,
            );
            return;
        }
        if (trace_gb) std.debug.print("[gbroute-adaptive]   -> serial routeGroupBy (gate failed)\n", .{});
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
/// How a stage-backed parallel scan seam materializes: `.eager` runs the
/// stage at compile (round/materialize modes), `.deferred` lazily on first
/// pull, `.ordered` eagerly with original-row-order emission (the only mode
/// legal under `force_ordered` — ride/borrow chains).
const StageScanMode = enum { eager, deferred, ordered };

/// Stage-parallel GROUP BY input for WRAPPED stage refs: peel row-wise
/// layers (compute / select / exclude / alias / filter) off the upstream
/// down to a bare materialize ref, build the parallel buffer scan over that
/// stage, and re-apply the layers — computes through the worker-fusion
/// offer, filters through the scan offer — so their per-row work runs in
/// the scan stripes instead of a serial chain on the aggregate's pull path
/// (a GROUP BY whose SELECT derives columns paid its whole scalar bill,
/// ~270ms on a 3.6M-row block, on the connection thread). Returns null on
/// a bare ref (the plain tryStageParallelScan path handles it) or any
/// non-row-wise layer.
fn tryStageParallelChain(input: engine_v2.CompileInput, op: *const ir.Op, map: *StageMap) anyerror!?exec.Query {
    var layers: std.ArrayListUnmanaged(*const ir.Op) = .empty;
    defer layers.deinit(input.allocator);
    var cur: *const ir.Op = op;
    var crossed_wrapper = false;
    while (true) {
        switch (cur.*) {
            // A mapped materialize is the stage leaf we scan; an unmapped one
            // is a single-ref inline CTE wrapper: walk through it and scan
            // DEFERRED below (ParallelScan's pending-compute machinery). The
            // budget blowout that once kept this opt-in was the materialize-
            // drain stage pin, released in runMaterialize since the drain
            // deep-copies. THINDB_NO_CHAIN_WRAPPERS=1 restores the decline.
            .materialize => |m| {
                if (map.get(cur) != null) break;
                if (getenv("THINDB_NO_CHAIN_WRAPPERS") != null) return null;
                crossed_wrapper = true;
                cur = m.upstream;
            },
            .compute => |c| {
                try layers.append(input.allocator, cur);
                cur = c.upstream;
            },
            .select => |s| {
                try layers.append(input.allocator, cur);
                cur = s.upstream;
            },
            .exclude => |e| {
                try layers.append(input.allocator, cur);
                cur = e.upstream;
            },
            .alias => |a| {
                try layers.append(input.allocator, cur);
                cur = a.upstream;
            },
            .filter => |f| {
                if (f.upstream.* == .join) return null;
                try layers.append(input.allocator, cur);
                cur = f.upstream;
            },
            else => return null,
        }
    }
    if (layers.items.len == 0) return null;
    const trace_chain = getenv("THINDB_TRACE_FUSE") != null;
    const mode: StageScanMode = if (crossed_wrapper) .deferred else .eager;
    var q = (try tryStageParallelScan(input, cur, map, mode)) orelse {
        if (trace_chain) std.debug.print("[chain] pscan-over-stage DECLINED (layers={d})\n", .{layers.items.len});
        return null;
    };
    if (trace_chain) {
        std.debug.print("[chain] engaged: layers={d} [", .{layers.items.len});
        var li = layers.items.len;
        while (li > 0) {
            li -= 1;
            std.debug.print(" {s}", .{@tagName(layers.items[li].*)});
        }
        std.debug.print(" ]\n", .{});
    }
    errdefer q.deinit();
    var i = layers.items.len;
    while (i > 0) {
        i -= 1;
        q = switch (layers.items[i].*) {
            .compute => |c| try engine_v2.computeDerivedFused(input.allocator, q, c.derived, input.udf_registry),
            .select => |s| try local.compileSelectProject(input.allocator, q, s),
            .exclude => |e| blk: {
                const remaining = try local.complementColumns(input.allocator, q.outputSchema(), e.columns);
                defer input.allocator.free(remaining);
                break :blk try q.project(remaining);
            },
            .alias => |a| try exec.AliasRename.create(input.allocator, q, a.alias),
            .filter => |f| try q.filter(f.predicate),
            else => unreachable,
        };
    }
    return q;
}

fn tryStageParallelScan(input: engine_v2.CompileInput, op: *const ir.Op, map: *StageMap, mode: StageScanMode) anyerror!?exec.Query {
    if (input.effectiveDop() <= 1) return null;
    const stage = switch (op.*) {
        .materialize => map.get(op) orelse return null,
        else => return null,
    };
    return try switch (mode) {
        .deferred => exec.ParallelScan.createOverStageDeferred(
            input.allocator,
            input.db.allocator,
            stage,
            input.accountant,
            input.effectiveDop(),
        ),
        .eager => exec.ParallelScan.createOverStage(
            input.allocator,
            input.db.allocator,
            stage,
            input.accountant,
            input.effectiveDop(),
        ),
        .ordered => exec.ParallelScan.createOverStageOrdered(
            input.allocator,
            input.db.allocator,
            stage,
            input.accountant,
            input.effectiveDop(),
        ),
    };
}

/// Window-chain pairing: the .window IR node reachable
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
fn buildFusedStreamOverStage(input: engine_v2.CompileInput, op: *const ir.Op, map: *StageMap, mode: StageScanMode) anyerror!exec.Query {
    switch (op.*) {
        .materialize => |m| {
            if (map.get(op) != null) return (try tryStageParallelScan(input, op, map, mode)) orelse error.UnsupportedQueryShape;
            return buildFusedStreamOverStage(input, m.upstream, map, mode);
        },
        .compute => |c| {
            var up = try buildFusedStreamOverStage(input, c.upstream, map, mode);
            errdefer up.deinit();
            return engine_v2.computeDerivedFused(input.allocator, up, c.derived, input.udf_registry);
        },
        .filter => |f| {
            var up = try buildFusedStreamOverStage(input, f.upstream, map, mode);
            errdefer up.deinit();
            return up.filter(f.predicate);
        },
        .select => |s| {
            var up = try buildFusedStreamOverStage(input, s.upstream, map, mode);
            errdefer up.deinit();
            return local.compileSelectProject(input.allocator, up, s);
        },
        .exclude => |e| {
            var up = try buildFusedStreamOverStage(input, e.upstream, map, mode);
            errdefer up.deinit();
            const remaining = try local.complementColumns(input.allocator, up.outputSchema(), e.columns);
            defer input.allocator.free(remaining);
            return up.project(remaining);
        },
        .alias => |a| {
            var up = try buildFusedStreamOverStage(input, a.upstream, map, mode);
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
        return buildFusedStreamOverStage(input, op, map, .deferred);
    }
    return compileBlock(input, op, map);
}

fn buildGenericBlock(input: engine_v2.CompileInput, op: *const ir.Op, map: *StageMap, block_root: *const ir.Op) anyerror!exec.Query {
    if (input.effectiveDop() > 1 and (streamingChainOverStage(op, map, true, false) or
        // Ordered only: window-input chains usually reach the previous
        // window's forced stage through a single-ref CTE wrapper — walk
        // through it (buildFusedStreamOverStage recurses the same way).
        // The eager compile-time pin this costs is one the borrow chain
        // holds on that stage anyway.
        (input.force_ordered and streamingChainOverStage(op, map, true, true))))
    {
        if (!input.force_ordered) return buildFusedStreamOverStage(input, op, map, .eager);
        // ride/borrow chains demand source row order — the ORDERED stage
        // scan provides it while the chain work still evaluates in the
        // stripe workers. Too-large stages (or the escape hatch) decline
        // back to the serial chain.
        if (getenv("THINDB_NO_ORDERED_PSCAN") == null) {
            if (buildFusedStreamOverStage(input, op, map, .ordered)) |q| {
                if (getenv("THINDB_TRACE_OPSCAN") != null) std.debug.print("[opscan] ordered seam ENGAGED\n", .{});
                return q;
            } else |err| switch (err) {
                error.UnsupportedQueryShape => {
                    if (getenv("THINDB_TRACE_OPSCAN") != null) std.debug.print("[opscan] ordered seam DECLINED (shape)\n", .{});
                },
                else => return err,
            }
        }
    } else if (input.force_ordered and input.effectiveDop() > 1 and getenv("THINDB_TRACE_OPSCAN") != null) {
        var cur = op;
        var depth: usize = 0;
        const stop: []const u8 = blk: while (depth < 32) : (depth += 1) {
            switch (cur.*) {
                .materialize => {
                    if (map.get(cur)) |stage| {
                        break :blk if (stage.stats_upper_rows <= mat_stage.chunk_rows) "mat-small-stats" else "mat-ok?";
                    }
                    break :blk "mat-single-ref";
                },
                .compute => |c| cur = c.upstream,
                .filter => |f| cur = f.upstream,
                .select => |p| cur = p.upstream,
                .exclude => |e| cur = e.upstream,
                .alias => |a| cur = a.upstream,
                else => break :blk @tagName(cur.*),
            }
        } else "deep";
        std.debug.print("[opscan] force_ordered chain declined: root={s} stop={s}\n", .{ @tagName(op.*), stop });
    }
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
            if (map.get(op)) |stage| {
                const t_ms = exec.prof.nowTicks();
                defer exec.prof.addPhase("compile.op.matscan", @intCast(exec.prof.nowTicks() - t_ms));
                return mat_stage.MatScan.create(input.allocator, stage);
            }
            return compileBlock(input, m.upstream, map);
        },
        .table_fn => |t| {
            const registry = input.udf_registry orelse return error.UnsupportedQueryShape;
            const entry = registry.tableByName(t.name) orelse return error.UnsupportedQueryShape;
            // Input ride/borrow detection (single input over a stage-backed
            // chain) — the window rider's machinery aimed at the TVF's
            // (PARTITION BY ++ ORDER BY) keys. Covered source order ⇒ the
            // operator skips its input sort; a filterless chain ⇒ columns
            // it passes through untransformed are borrowed zero-copy from
            // the stage's contiguous result instead of drain-copied. Both
            // require the compiled chain to deliver rows in source order
            // (force_ordered), exactly like the window ride/borrow.
            var tvf_ordered = false;
            const n_tin = t.inputs.len;
            const tvf_borrow_srcs = try input.node_arena.alloc(?*mat_stage.Stage, n_tin);
            @memset(tvf_borrow_srcs, null);
            const tvf_borrow_maps = try input.node_arena.alloc([]const ?usize, n_tin);
            @memset(tvf_borrow_maps, &.{});
            var any_borrow = false;
            var eff_input = input;
            var tvf_crossed: [4]*const ir.Op = undefined;
            var tvf_n_crossed: u8 = 0;
            for (t.inputs, 0..) |t_in, i| {
                // Ride keys are a single-input notion (the multi path forms
                // runs per input from its own sort); borrow applies to all.
                const keys: ?ir.WindowSpec = if (n_tin == 1 and t.partition_by.len + t.order_by.len > 0)
                    .{ .partition_by = t.partition_by, .order_by = t.order_by, .frame = ir.Frame.default_no_order }
                else
                    null;
                const src = rideSource(map, t_in, keys) orelse continue;
                if (n_tin == 1 and src.covered) {
                    // A TVF source (win == null) already emits in its
                    // advertised order — nothing to mark.
                    if (src.win) |sw| sw.emit_sorted = true;
                    tvf_ordered = true;
                    tvf_crossed = src.joins;
                    tvf_n_crossed = src.n_joins;
                }
                if (!src.has_filter) {
                    const decl = entry.input_schemas[i];
                    const bm = try input.node_arena.alloc(?usize, decl.len);
                    var any = false;
                    for (decl, bm) |col, *slot| {
                        slot.* = borrowIdxFor(map, t_in, col.name, src.stage);
                        if (slot.* != null) any = true;
                    }
                    if (any) {
                        tvf_borrow_srcs[i] = src.stage;
                        tvf_borrow_maps[i] = bm;
                        any_borrow = true;
                        if (src.win == null and src.stage.adopt_table_fn == null) {
                            src.stage.want_contiguous = true;
                            src.stage.fill_dop = input.effectiveDop();
                        }
                    }
                }
                if (getenv("THINDB_TVF_TRACE") != null) {
                    var k: usize = 0;
                    for (tvf_borrow_maps[i]) |m| {
                        if (m != null) k += 1;
                    }
                    std.debug.print("[tvf] compile {s}: input{d} ride={} borrow={d}/{d}\n", .{
                        t.name, i, tvf_ordered, k, entry.input_schemas[i].len,
                    });
                }
            }
            if (tvf_ordered or any_borrow) eff_input.force_ordered = true;
            var ups: std.ArrayList(exec.Query) = .empty;
            defer ups.deinit(input.allocator);
            errdefer for (ups.items) |*u| u.deinit();
            for (t.inputs) |inp| {
                try ups.append(input.allocator, try compileBlock(eff_input, inp, map));
            }
            // A ride that crossed LEFT joins holds only if the compiled
            // operators kept the probe-order pin; sort on any doubt.
            if (tvf_ordered and tvf_n_crossed > 0 and
                !verifyCrossedJoins(input, tvf_crossed[0..tvf_n_crossed], .{
                    .partition_by = t.partition_by,
                    .order_by = t.order_by,
                    .frame = ir.Frame.default_no_order,
                }))
            {
                tvf_ordered = false;
            }
            const q = try exec.table_fn.TableFnExec.create(input.allocator, ups.items, entry, t.args, t.partition_by, t.order_by, input.effectiveDop());
            if (exec.queryAs(exec.table_fn.TableFnExec, q)) |tf| {
                tf.input_ordered = tvf_ordered;
                if (n_tin == 1) {
                    tf.borrow_src = tvf_borrow_srcs[0];
                    tf.borrow_map = tvf_borrow_maps[0];
                } else {
                    tf.multi_borrow_srcs = tvf_borrow_srcs;
                    tf.multi_borrow_maps = tvf_borrow_maps;
                }
                // The compiled input chain releases its stage use on
                // EXHAUSTION (end of the drain pull), which can free the
                // borrowed stores while the operator still reads them.
                // Hold one use for the operator's lifetime; released in
                // TableFnExec.deinit.
                for (tvf_borrow_srcs) |maybe| {
                    if (maybe) |src| src.registerUse();
                }
                tf.advertised_keys = try tvfEmitKeys(input.node_arena, entry, t);
                if (tf.advertised_keys != null and getenv("THINDB_TVF_TRACE") != null) {
                    std.debug.print("[tvf] compile {s}: advertises output order\n", .{t.name});
                }
            }
            if (t.alias) |a| {
                errdefer @constCast(&q).deinit();
                return exec.AliasRename.create(input.allocator, q, a);
            }
            return q;
        },
        .alias => |a| {
            var up = try buildGenericBlock(input, a.upstream, map, block_root);
            errdefer up.deinit();
            const t_op = exec.prof.nowTicks();
            defer exec.prof.addPhase("compile.op.alias", @intCast(exec.prof.nowTicks() - t_op));
            return exec.AliasRename.create(input.allocator, up, a.alias);
        },
        .filter => |f| {
            if (f.upstream.* == .join) {
                const t_op = exec.prof.nowTicks();
                defer exec.prof.addPhase("compile.op.filtered_join_incl", @intCast(exec.prof.nowTicks() - t_op));
                return compileFilteredJoin(input, f.predicate, f.upstream, map);
            }
            var up = try buildGenericBlock(input, f.upstream, map, block_root);
            errdefer up.deinit();
            const t_op = exec.prof.nowTicks();
            defer exec.prof.addPhase("compile.op.filter", @intCast(exec.prof.nowTicks() - t_op));
            return up.filter(f.predicate);
        },
        .select => |s| {
            var up = try buildGenericBlock(input, s.upstream, map, block_root);
            errdefer up.deinit();
            const t_op = exec.prof.nowTicks();
            defer exec.prof.addPhase("compile.op.select", @intCast(exec.prof.nowTicks() - t_op));
            return local.compileSelectProject(input.allocator, up, s);
        },
        .exclude => |e| {
            var up = try buildGenericBlock(input, e.upstream, map, block_root);
            errdefer up.deinit();
            const t_op = exec.prof.nowTicks();
            defer exec.prof.addPhase("compile.op.exclude", @intCast(exec.prof.nowTicks() - t_op));
            const remaining = try local.complementColumns(input.allocator, up.outputSchema(), e.columns);
            defer input.allocator.free(remaining);
            return up.project(remaining);
        },
        .compute => |c| {
            var up = try buildGenericBlock(input, c.upstream, map, block_root);
            errdefer up.deinit();
            // Offer the row-local subset into the parallel-scan workers first
            // (same split the V2 table handlers and join arms use); the
            // non-fusable remainder — and the whole list when the upstream
            // declines — still takes the serial layer + terminal self-push.
            const t_op = exec.prof.nowTicks();
            defer exec.prof.addPhase("compile.op.compute", @intCast(exec.prof.nowTicks() - t_op));
            return engine_v2.computeDerivedFused(input.allocator, up, c.derived, input.udf_registry);
        },
        .order_by => |o| {
            var up = try buildGenericBlock(input, o.upstream, map, block_root);
            errdefer up.deinit();
            const t_op = exec.prof.nowTicks();
            defer exec.prof.addPhase("compile.op.order_by", @intCast(exec.prof.nowTicks() - t_op));
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
                const t_topn = exec.prof.nowTicks();
                up = try up.topN(o.specs, @intCast(l.n), @intCast(l.offset));
                exec.prof.addPhase("compile.op.topn", @intCast(exec.prof.nowTicks() - t_topn));
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
            var up = (try tryStageParallelChain(input, g.upstream, map)) orelse
                (try tryStageParallelScan(input, g.upstream, map, .eager)) orelse
                try buildGenericBlock(input, g.upstream, map, block_root);
            errdefer up.deinit();
            const t_op = exec.prof.nowTicks();
            defer exec.prof.addPhase("compile.op.group_by", @intCast(exec.prof.nowTicks() - t_op));
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
                    input.db.allocator,
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
            if (getenv("THINDB_TRACE_GBROUTE") != null) {
                std.debug.print("[gbroute-compile] no stages beneath group_by: keys={d} aggs=", .{g.group_cols.len});
                for (g.aggs) |a| std.debug.print("{s},", .{@tagName(a.func)});
                std.debug.print(" upper_rows={d}\n", .{up.stats().upper_rows});
            }
            return group_route.routeGroupByDop(
                input.allocator,
                input.db.allocator,
                &up,
                g.group_cols,
                g.aggs,
                g.top_k,
                g.emit_limit,
                input.db.config.query_memory_budget,
                input.effectiveDop(),
            );
        },
        .window => |w| {
            const t_op = exec.prof.nowTicks();
            defer exec.prof.addPhase("compile.op.window_incl", @intCast(exec.prof.nowTicks() - t_op));
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
            var crossed_joins: [4]*const ir.Op = undefined;
            var n_crossed: u8 = 0;
            if (input.effectiveDop() > 1) {
                const rider_keys: ?ir.WindowSpec = if (sameKeysAllSpecs(w.specs)) w.specs[0] else null;
                if (rideSource(map, w.upstream, rider_keys)) |src| {
                    if (src.covered) {
                        // A TVF source (win == null) already emits in its
                        // advertised order — nothing to mark.
                        if (src.win) |sw| sw.emit_sorted = true;
                        ride = true;
                        ride_keys = src.keys.?;
                        crossed_joins = src.joins;
                        n_crossed = src.n_joins;
                    }
                    if (!src.has_filter) {
                        borrow_stage = src.stage;
                        if (src.win == null) {
                            src.stage.want_contiguous = true;
                            src.stage.fill_dop = input.effectiveDop();
                        }
                    }
                    // Both riding and borrowing need the chain to deliver
                    // rows in the source's order (borrowing additionally
                    // 1:1) — suppress order-scrambling parallel seams.
                    if (ride or borrow_stage != null) eff_input.force_ordered = true;
                }
            }
            const t_up = exec.prof.nowTicks();
            var up = try compileBlock(eff_input, w.upstream, map);
            errdefer up.deinit();
            exec.prof.addPhase("compile.win.up_incl", @intCast(exec.prof.nowTicks() - t_up));
            // A ride that crossed LEFT joins holds only if the compiled
            // operators kept the probe-order pin; sort on any doubt.
            if (ride and n_crossed > 0 and
                !verifyCrossedJoins(input, crossed_joins[0..n_crossed], ride_keys))
            {
                ride = false;
            }
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
            // Window-chain pairing: when this window's IR chain reaches an
            // upstream window with the SAME partition set through
            // grouping-preserving ops, that window emits in its sorted
            // order (one gather pass) and this one is compile-proven
            // grouped — skipping its full sort. Chains cascade: a grouped
            // window retains its own perm and hands the order onward.
            if (input.win_registry) |reg| pair: {
                const win = exec.queryAs(window_op.Window, wq) orelse break :pair;
                reg.put(input.allocator, @ptrCast(op), @ptrCast(win)) catch break :pair;
                const chain = windowChainBelow(w.upstream, map) orelse break :pair;
                const below = chain.win;
                const prev_opaque = reg.get(@ptrCast(below)) orelse break :pair;
                const prev: *window_op.Window = @ptrCast(@alignCast(prev_opaque));
                // emitter needs ONE perm
                if (!prev.singleSortGroup()) break :pair;
                // rider: grouping only, orders may differ
                if (!irAllSpecsSamePartition(op)) break :pair;
                if (!windowPartitionSetsEqual(below, op)) break :pair;
                if (chainRenamesPartitionKey(w.upstream, below, op.window.specs[0].partition_by)) break :pair;
                // Every join the chain crossed must provably preserve
                // left-side grouping: probe = left (fixed at create), no
                // skew handoff (SMJ reorders), INNER/LEFT only, and no
                // right-side output column shadowing a partition key.
                for (chain.joins[0..chain.n_joins]) |jn| {
                    const jopq = reg.get(@ptrCast(jn)) orelse break :pair;
                    const jop: *join_mod.Join = @ptrCast(@alignCast(jopq));
                    if (jop.build_is_left) break :pair;
                    if (jop.skew_detector != null or jop.skew_smj != null) break :pair;
                    if (jop.join_type != .left and jop.join_type != .inner) break :pair;
                    for (jop.output_schema[jop.left_col_count..]) |c| {
                        for (op.window.specs[0].partition_by) |k| {
                            if (types.columnNameEql(c.name, k)) break :pair;
                        }
                    }
                }
                prev.emit_sorted = true;
                prev.emit_sorted_stream = true;
                win.assume_grouped = true;
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
                            // Operator-lifetime pin (released in Window.deinit):
                            // without it, a ROOT (non-staged) borrowing window
                            // can outlive the source result — the chain's own
                            // use releases at drain exhaustion, before the
                            // window evaluates over the borrowed stores.
                            src_stage.registerUse();
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
            markJoinBuildContiguous(input, j.join_type, left, right);
            const t_join = exec.prof.nowTicks();
            const jq = try left.join(right, joinSpecOf(j, input.force_ordered));
            exec.prof.addPhase("compile.op.join", @intCast(exec.prof.nowTicks() - t_join));
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
            // Pre-push lossless widening casts into the arms so the exec
            // union is cast-free: rebatched()'s serial per-batch kernels
            // disappear, and a join's probe sink can forward into BOTH arms
            // (a cast arm otherwise pins the union's serial lane — on the
            // production rollforward workload that lane carried the heavy 3.2M-row arm).
            // The cast Compute fuses into the arm's stripe workers, or rides
            // the ChainForward terminal push when a probe later chains
            // through it; worst case it evaluates where rebatched would
            // have, so this can't regress.
            const t_op = exec.prof.nowTicks();
            defer exec.prof.addPhase("compile.op.union", @intCast(exec.prof.nowTicks() - t_op));
            try unifyUnionArmTypes(input, &left, &right);
            return exec.SetUnion.create(input.allocator, left, right, u.all);
        },
        else => return error.UnsupportedQueryShape,
    }
}

/// Integer-family widening rank; null for anything outside the family.
fn intRank(t: types.Type) ?u8 {
    return switch (t) {
        .tinyint => 0,
        .smallint => 1,
        .int => 2,
        .bigint => 3,
        .largeint => 4,
        else => null,
    };
}

/// The scalar conversion fn implementing a lossless widening to `t`.
fn wideningFnName(t: types.Type) ?[]const u8 {
    return switch (t) {
        .smallint => "to_smallint",
        .int => "to_int",
        .bigint => "to_bigint",
        .largeint => "to_largeint",
        .double => "to_double",
        .datetime => "to_datetime",
        else => null,
    };
}

/// The common type both union arms should widen to — ONLY when the pair is
/// a lossless, unambiguous widening (int-family → the wider member;
/// float/double → double; date/datetime → datetime, a date being its own
/// midnight). Everything else returns null and stays on the exec union's
/// own cast kernels, exactly as before.
fn unionWideningType(l: types.Type, r: types.Type) ?types.Type {
    if (std.meta.activeTag(l) == std.meta.activeTag(r)) return null; // no cast needed
    if (intRank(l)) |lr| {
        if (intRank(r)) |rr| return if (lr > rr) l else r;
        return null;
    }
    const lf = l == .float or l == .double;
    const rf = r == .float or r == .double;
    if (lf and rf) return .double;
    const ld = l == .date or l == .datetime;
    const rd = r == .date or r == .datetime;
    if (ld and rd) return .datetime;
    return null;
}

/// Rewrite each arm so cast-needing columns are widened INSIDE the arm
/// (fused into its scan workers where the chain allows) instead of by the
/// union's serial per-batch kernels. Only the safe widening subset — see
/// `unionWideningType`. Arm ownership transfers into the wrapping Compute
/// on success; on ANY failure the arms are left exactly as passed in.
fn unifyUnionArmTypes(input: engine_v2.CompileInput, left: *exec.Query, right: *exec.Query) !void {
    const ls = left.outputSchema();
    const rs = right.outputSchema();
    if (ls.len != rs.len) return; // SetUnion.create reports the real error
    var l_derived: std.ArrayListUnmanaged(ir.Derived) = .empty;
    var r_derived: std.ArrayListUnmanaged(ir.Derived) = .empty;
    const trace_jf = getenv("THINDB_TRACE_JOINFUSE") != null;
    for (ls, rs) |lc, rc| {
        const common = unionWideningType(lc.type, rc.type) orelse {
            if (trace_jf and std.meta.activeTag(lc.type) != std.meta.activeTag(rc.type)) {
                std.debug.print("[jf]   union widen skip: {s} {s} vs {s}\n", .{ lc.name, @tagName(lc.type), @tagName(rc.type) });
            }
            continue;
        };
        const fn_name = wideningFnName(common) orelse continue;
        if (std.meta.activeTag(lc.type) != std.meta.activeTag(common)) {
            try l_derived.append(input.node_arena, try castDerived(input.node_arena, lc.name, fn_name));
        }
        if (std.meta.activeTag(rc.type) != std.meta.activeTag(common)) {
            try r_derived.append(input.node_arena, try castDerived(input.node_arena, rc.name, fn_name));
        }
    }
    if (l_derived.items.len > 0) {
        left.* = try engine_v2.computeDerivedFused(input.allocator, left.*, l_derived.items, input.udf_registry);
    }
    if (r_derived.items.len > 0) {
        right.* = try engine_v2.computeDerivedFused(input.allocator, right.*, r_derived.items, input.udf_registry);
    }
}

/// `name = to_T(name)` — replaces the arm's own output slot in place (a
/// derived whose name matches an upstream column replaces that slot, order
/// preserved). Everything lives in the statement's node arena: the Compute
/// keeps a shallow dupe of the derived list, so the expr internals must
/// outlive compile.
fn castDerived(arena: std.mem.Allocator, col_name: []const u8, fn_name: []const u8) !ir.Derived {
    const name = try arena.dupe(u8, col_name);
    const args = try arena.alloc(ir.Expr, 1);
    args[0] = .{ .col_ref = name };
    return .{ .name = name, .expr = .{ .call = .{ .fn_name = fn_name, .args = args } } };
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
    /// LEFT equi-joins the walk crossed (probe-order emission pinned by
    /// preserve_left_order under force_ordered). A rider that skips its
    /// sort must verify each compiled join post-compile — probe = left,
    /// no skew handoff, and no kept right-side column shadowing a ride
    /// key — and fall back to sorting on any doubt.
    joins: [4]*const ir.Op = undefined,
    n_joins: u8 = 0,
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

/// The output-order advertisement a TVF call can make, in OUTPUT-schema
/// column names: partition keys adjacent (partition ORDER unspecified —
/// digest-major, exactly like window emissions), ordered within each
/// partition by the order keys. Two ways to earn it:
///   - `ordered_output` (kernel-asserted, any row shape): the call keys
///     must exist in the output schema by name — the kernel promises those
///     columns carry the partition's key values / the emit order.
///   - row_aligned with EVERY call key pass-through: provable — output row
///     i of a run is input row i, and the pass-through columns are permuted
///     copies of the key inputs. Advertised under the pass-through OUTPUT
///     names.
/// Anything else (row-generating without the flag, keys not in the output,
/// multi-input) advertises nothing.
fn tvfEmitKeys(node_arena: Allocator, entry: *const udf.TableEntry, t: ir.Op.TableFn) !?ir.WindowSpec {
    if (t.inputs.len != 1) return null;
    if (t.partition_by.len + t.order_by.len == 0) return null;
    if (entry.ordered_output) {
        for (t.partition_by) |k| {
            if (types.findColumn(entry.output_schema, k) == null) return null;
        }
        for (t.order_by) |s| {
            if (types.findColumn(entry.output_schema, s.col) == null) return null;
        }
        return .{ .partition_by = t.partition_by, .order_by = t.order_by, .frame = ir.Frame.default_no_order };
    }
    if (!entry.row_aligned) return null;
    const decl = entry.input_schemas[0];
    const parts = try node_arena.alloc([]const u8, t.partition_by.len);
    for (t.partition_by, parts) |k, *slot| {
        slot.* = passOutNameFor(entry, decl, k) orelse return null;
    }
    const ords = try node_arena.alloc(ir.SortSpec, t.order_by.len);
    for (t.order_by, ords) |s, *slot| {
        slot.* = .{
            .col = passOutNameFor(entry, decl, s.col) orelse return null,
            .desc = s.desc,
        };
    }
    return .{ .partition_by = parts, .order_by = ords, .frame = ir.Frame.default_no_order };
}

/// The OUTPUT column name a pass-through pair gives input column `name`,
/// or null when the column isn't pass-through.
fn passOutNameFor(entry: *const udf.TableEntry, decl: []const types.Column, name: []const u8) ?[]const u8 {
    const in_idx = types.findColumn(decl, name) orelse return null;
    for (entry.passthrough) |pp| {
        if (pp.in_idx == in_idx) return entry.output_schema[pp.out_idx].name;
    }
    return null;
}

test "tvfEmitKeys: advertisement conditions" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const aa = arena.allocator();

    const stub = struct {
        fn process(_: *const udf.TvfContext, _: []const udf.TvfPartition, _: *udf.TvfOutput) anyerror!void {}
    };
    const in0 = [_]types.Column{
        .{ .name = "k", .type = .{ .int = {} }, .nullable = false },
        .{ .name = "o", .type = .{ .bigint = {} }, .nullable = false },
        .{ .name = "v", .type = .{ .bigint = {} }, .nullable = false },
    };
    const out_renamed = [_]types.Column{
        .{ .name = "k_out", .type = .{ .int = {} }, .nullable = false },
        .{ .name = "o_out", .type = .{ .bigint = {} }, .nullable = false },
        .{ .name = "r", .type = .{ .bigint = {} }, .nullable = false },
    };
    var entry = udf.TableEntry{
        .name = "f",
        .input_schemas = &.{&in0},
        .output_schema = &out_renamed,
        .execution = .either,
        .arg_types = &.{},
        .row_aligned = true,
        .ordered_output = false,
        .passthrough = &.{ .{ .out_idx = 0, .in_idx = 0 }, .{ .out_idx = 1, .in_idx = 1 } },
        .kernel_input_cols = 3,
        .broadcast_inputs = &.{},
        .process = stub.process,
        .user_data = null,
    };
    var dummy_op: ir.Op = undefined;
    var call = ir.Op.TableFn{
        .name = "f",
        .inputs = &.{&dummy_op},
        .partition_by = &.{"k"},
        .order_by = &.{.{ .col = "o", .desc = true }},
    };

    // Provable path: row-aligned + all keys pass-through → OUTPUT names.
    {
        const spec = (try tvfEmitKeys(aa, &entry, call)).?;
        try std.testing.expectEqualStrings("k_out", spec.partition_by[0]);
        try std.testing.expectEqualStrings("o_out", spec.order_by[0].col);
        try std.testing.expect(spec.order_by[0].desc);
    }
    // A key without a pass-through pair kills the advertisement.
    entry.passthrough = &.{.{ .out_idx = 0, .in_idx = 0 }};
    try std.testing.expectEqual(@as(?ir.WindowSpec, null), try tvfEmitKeys(aa, &entry, call));
    // Not row-aligned (row-generating) without the flag: nothing.
    entry.row_aligned = false;
    entry.passthrough = &.{};
    try std.testing.expectEqual(@as(?ir.WindowSpec, null), try tvfEmitKeys(aa, &entry, call));
    // Kernel-asserted flag: advertised under the CALL names, which must
    // exist in the output schema.
    entry.ordered_output = true;
    const out_named = [_]types.Column{
        .{ .name = "k", .type = .{ .int = {} }, .nullable = false },
        .{ .name = "o", .type = .{ .bigint = {} }, .nullable = false },
    };
    entry.output_schema = &out_named;
    {
        const spec = (try tvfEmitKeys(aa, &entry, call)).?;
        try std.testing.expectEqualStrings("k", spec.partition_by[0]);
        try std.testing.expectEqualStrings("o", spec.order_by[0].col);
    }
    const out_missing = [_]types.Column{
        .{ .name = "k", .type = .{ .int = {} }, .nullable = false },
    };
    entry.output_schema = &out_missing;
    try std.testing.expectEqual(@as(?ir.WindowSpec, null), try tvfEmitKeys(aa, &entry, call));
    // Multi-input and keyless calls never advertise.
    entry.output_schema = &out_named;
    call.inputs = &.{ &dummy_op, &dummy_op };
    try std.testing.expectEqual(@as(?ir.WindowSpec, null), try tvfEmitKeys(aa, &entry, call));
    call.inputs = &.{&dummy_op};
    call.partition_by = &.{};
    call.order_by = &.{};
    try std.testing.expectEqual(@as(?ir.WindowSpec, null), try tvfEmitKeys(aa, &entry, call));
}

test "riderCoveredBy: partition equality + order-prefix with matching desc" {
    const mk = struct {
        fn spec(part: []const []const u8, ord: []const ir.SortSpec) ir.WindowSpec {
            return .{ .partition_by = part, .order_by = ord, .frame = ir.Frame.default_no_order };
        }
    };
    const src = mk.spec(&.{ "p", "d" }, &.{ .{ .col = "m" }, .{ .col = "x" } });
    // Covered: exact keys, any order-prefix (including empty — adjacency only).
    try std.testing.expect(riderCoveredBy(src, mk.spec(&.{ "p", "d" }, &.{ .{ .col = "m" }, .{ .col = "x" } })));
    try std.testing.expect(riderCoveredBy(src, mk.spec(&.{ "p", "d" }, &.{.{ .col = "m" }})));
    try std.testing.expect(riderCoveredBy(src, mk.spec(&.{ "p", "d" }, &.{})));
    // Not covered: partition count/name/position mismatch, order keys past
    // the source's, a desc flip, or a non-prefix order key.
    try std.testing.expect(!riderCoveredBy(src, mk.spec(&.{"p"}, &.{.{ .col = "m" }})));
    try std.testing.expect(!riderCoveredBy(src, mk.spec(&.{ "d", "p" }, &.{.{ .col = "m" }})));
    try std.testing.expect(!riderCoveredBy(src, mk.spec(&.{ "p", "d" }, &.{ .{ .col = "m" }, .{ .col = "x" }, .{ .col = "y" } })));
    try std.testing.expect(!riderCoveredBy(src, mk.spec(&.{ "p", "d" }, &.{.{ .col = "m", .desc = true }})));
    try std.testing.expect(!riderCoveredBy(src, mk.spec(&.{ "p", "d" }, &.{.{ .col = "x" }})));
}

fn rideSource(map: *StageMap, op: *const ir.Op, keys: ?ir.WindowSpec) ?RideSrc {
    var cur = op;
    var has_filter = false;
    var joins: [4]*const ir.Op = undefined;
    var n_joins: u8 = 0;
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
            .join => |j| {
                // A LEFT equi-join preserves its left side's ORDER (partition
                // keys stay adjacent, within-partition order survives): the
                // hash operator pins probe = left, and under force_ordered
                // the compile pins the hash algorithm, disables the skew
                // re-route, and keeps the probe serial (joinSpecOf /
                // compileJoinChild). Duplicate output names are rejected at
                // join create, so a key name binds unambiguously to the left
                // column. Rows may be dropped or duplicated, so borrowing
                // must not survive — same contract as .filter. Range /
                // opaque / empty-on shapes route to other algorithms with no
                // order guarantee; only clean equi left joins walk through.
                if (j.join_type != .left) return null;
                if (j.on.len == 0 or j.ranges.len != 0) return null;
                if (j.algorithm != .auto and j.algorithm != .hash) return null;
                // A/B + safety hatch: rides never cross joins when set.
                if (getenv("THINDB_NO_JOIN_RIDE") != null) return null;
                if (n_joins == joins.len) return null;
                joins[n_joins] = cur;
                n_joins += 1;
                has_filter = true;
                cur = j.left;
            },
            .materialize => |m| {
                if (map.get(cur)) |stage| {
                    if (stage.adopt_window) |win| {
                        const src_keys = win.effectiveEmitKeys();
                        const covered = if (keys) |k|
                            (if (src_keys) |sk| riderCoveredBy(sk, k) else false)
                        else
                            false;
                        return .{ .win = win, .stage = stage, .keys = src_keys, .covered = covered, .has_filter = has_filter, .joins = joins, .n_joins = n_joins };
                    }
                    // A TVF stage advertising its output order (partition
                    // keys adjacent, ordered within) is ridable exactly
                    // like a window stage; its adopted stores are already
                    // in that order, so there is no emit_sorted to mark.
                    if (stage.adopt_table_fn) |tf| {
                        if (tf.advertised_keys) |sk| {
                            const covered = if (keys) |k| riderCoveredBy(sk, k) else false;
                            return .{ .win = null, .stage = stage, .keys = sk, .covered = covered, .has_filter = has_filter, .joins = joins, .n_joins = n_joins };
                        }
                    }
                    // No order to ride, but a filterless chain can still
                    // BORROW its columns: ask it to materialize contiguous.
                    return .{ .win = null, .stage = stage, .keys = null, .covered = false, .has_filter = has_filter, .joins = joins, .n_joins = n_joins };
                }
                // single-ref: the body compiles inline at this use site —
                // no rematerialization between us and it.
                cur = m.upstream;
            },
            else => return null,
        }
    }
}

/// Post-compile check for a ride that crossed LEFT joins: every crossed
/// join's compiled operator must probe its left side with no chance of a
/// skew handoff (both pinned by preserve_left_order under force_ordered —
/// this re-verifies the pin held), and no KEPT right-side output column may
/// shadow a ride key: a spec key that suffix-matches a right column could
/// resolve there, and left-side order says nothing about right values.
/// Any join missing from the registry (or no registry) → sort instead.
fn verifyCrossedJoins(input: engine_v2.CompileInput, joins: []const *const ir.Op, keys: ir.WindowSpec) bool {
    const reg = input.win_registry orelse return false;
    for (joins) |jn| {
        const jopq = reg.get(@ptrCast(jn)) orelse return false;
        const jop: *join_mod.Join = @ptrCast(@alignCast(jopq));
        if (jop.build_is_left) return false;
        if (jop.skew_detector != null or jop.skew_smj != null) return false;
        for (jop.output_schema[jop.left_col_count..]) |c| {
            if (nameIsSpecKey(c.name, keys)) return false;
        }
    }
    return true;
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

/// A hash join whose build side is a stage reads the stage's columns in
/// place and shares one FastTable per key set (Join.bindSharedStageBuild);
/// that needs the result contiguous, so ask for that layout before the
/// stage runs. A stage that already ran eagerly just gets copied once.
fn markJoinBuildContiguous(input: engine_v2.CompileInput, join_type: exec.JoinType, left: exec.Query, right: exec.Query) void {
    const build = if (exec.Join.buildIsLeft(join_type, left, right)) left else right;
    var src = (mat_stage.stageBehind(input.allocator, build) catch return) orelse return;
    defer src.deinit(input.allocator);
    const stage = src.stage;
    if (stage.result != null or stage.adopt_window != null or stage.adopt_table_fn != null) return;
    stage.want_contiguous = true;
    stage.fill_dop = @max(stage.fill_dop, input.effectiveDop());
}

fn joinSpecOf(j: anytype, force_ordered: bool) ir.JoinSpec {
    return .{
        .join_type = j.join_type,
        .algorithm = j.algorithm,
        .on = j.on,
        .ranges = j.ranges,
        .extra_predicate = j.extra_predicate,
        .skew_ratio_threshold = j.skew_ratio_threshold,
        .skew_absolute_threshold = j.skew_absolute_threshold,
        .skew_sample_interval = j.skew_sample_interval,
        // Inside a force_ordered chain (a downstream window/TVF rides the
        // source order) a left join must emit its left side in input order.
        .preserve_left_order = force_ordered and j.join_type == .left,
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
    markJoinBuildContiguous(input, j.join_type, left, right);
    var joined = try left.join(right, joinSpecOf(j, input.force_ordered));
    if (input.win_registry) |reg| {
        if (exec.queryAs(join_mod.Join, joined)) |jop| {
            reg.put(input.allocator, @ptrCast(join_op), @ptrCast(jop)) catch {};
        }
    }
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
