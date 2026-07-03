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
    try collectStages(input, root, set, &map, &cse);
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
            var q = try compileBlock(input, rep.materialize.upstream, map);
            // A window-rooted body hands its buffers to the stage zero-copy
            // (Stage.adopt_window); output pruning would wrap a Project over
            // the window and break the root identity — and with adoption the
            // copy it would save no longer exists.
            const win_root = exec.queryAs(window_op.Window, q);
            if (win_root == null) q = pruneStageColumns(input, q);
            const stage = try set.addStage(q, input.accountant);
            stage.adopt_window = win_root;
            if (win_root) |wr| {
                if (wr.borrow_src) |src| {
                    src.registerUse();
                    stage.pinned_upstream = src;
                }
            }
            if (exec.prof.enabled) stage.setup_ticks = exec.prof.nowTicks() - c0;
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

fn compileBlock(input: engine_v2.CompileInput, op: *const ir.Op, map: *StageMap) anyerror!exec.Query {
    return switch (blockSource(op)) {
        // Table-backed block: the regular V2 handlers, full parallelism.
        // pg_catalog virtual tables are in-memory metadata batches, not
        // columnar scans — they build generically like the other leaves.
        .table => if (blockScanIsPgCatalog(input, op))
            buildGenericBlock(input, op, map, op)
        else
            engine_v2.compileSelectBlock(input, op),
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
fn compileJoinChild(input: engine_v2.CompileInput, op: *const ir.Op, map: *StageMap) anyerror!exec.Query {
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
    // A join child that is a streaming chain over one stage — even with no
    // per-row work — compiles as a parallel buffer scan: as the probe side
    // it lets the join fuse its probe into the stripe workers instead of
    // probing serially off a MatScan; as the build side it drains faster.
    if (input.db.config.max_dop > 1 and streamingChainOverStage(op, map, false)) {
        return buildFusedStreamOverStage(input, op, map);
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
fn tryStageParallelScan(input: engine_v2.CompileInput, op: *const ir.Op, map: *StageMap) anyerror!?exec.Query {
    if (input.db.config.max_dop <= 1) return null;
    const stage = switch (op.*) {
        .materialize => map.get(op) orelse return null,
        else => return null,
    };
    return try exec.ParallelScan.createOverStage(
        input.allocator,
        input.db.allocator,
        stage,
        input.accountant,
        input.db.config.max_dop,
    );
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
fn streamingChainOverStage(op: *const ir.Op, map: *StageMap, require_work: bool) bool {
    var has_work = false;
    var cur = op;
    while (true) {
        switch (cur.*) {
            .materialize => {
                const stage = map.get(cur) orelse return false;
                return (has_work or !require_work) and stage.stats_upper_rows > mat_stage.chunk_rows;
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
fn buildFusedStreamOverStage(input: engine_v2.CompileInput, op: *const ir.Op, map: *StageMap) anyerror!exec.Query {
    switch (op.*) {
        .materialize => return (try tryStageParallelScan(input, op, map)) orelse error.UnsupportedQueryShape,
        .compute => |c| {
            var up = try buildFusedStreamOverStage(input, c.upstream, map);
            errdefer up.deinit();
            return engine_v2.computeDerivedFused(input.allocator, up, c.derived, input.udf_registry);
        },
        .filter => |f| {
            var up = try buildFusedStreamOverStage(input, f.upstream, map);
            errdefer up.deinit();
            return up.filter(f.predicate);
        },
        .select => |s| {
            var up = try buildFusedStreamOverStage(input, s.upstream, map);
            errdefer up.deinit();
            return local.compileSelectProject(input.allocator, up, s);
        },
        .exclude => |e| {
            var up = try buildFusedStreamOverStage(input, e.upstream, map);
            errdefer up.deinit();
            const remaining = try local.complementColumns(input.allocator, up.outputSchema(), e.columns);
            defer input.allocator.free(remaining);
            return up.project(remaining);
        },
        .alias => |a| {
            var up = try buildFusedStreamOverStage(input, a.upstream, map);
            errdefer up.deinit();
            return exec.AliasRename.create(input.allocator, up, a.alias);
        },
        else => unreachable,
    }
}

fn buildGenericBlock(input: engine_v2.CompileInput, op: *const ir.Op, map: *StageMap, block_root: *const ir.Op) anyerror!exec.Query {
    if (input.db.config.max_dop > 1 and !input.force_ordered and streamingChainOverStage(op, map, true)) return buildFusedStreamOverStage(input, op, map);
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
            return up.computeWithRegistry(c.derived, input.udf_registry);
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
                        .compute => |c2| try up.computeWithRegistry(c2.derived, input.udf_registry),
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
            var up = (try tryStageParallelScan(input, g.upstream, map)) orelse
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
                    input.db.config.max_dop,
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
            if (input.db.config.max_dop > 1) {
                const rider_keys: ?ir.WindowSpec = if (sameKeysAllSpecs(w.specs)) w.specs[0] else null;
                if (rideSource(map, w.upstream, rider_keys)) |src| {
                    if (src.covered) {
                        src.win.emit_sorted = true;
                        ride = true;
                        ride_keys = src.keys.?;
                    }
                    if (!src.has_filter) borrow_stage = src.stage;
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
            const wq = try up.window(w.specs, w.calls, input.db.config.max_dop);
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
            var left = try compileJoinChild(input, j.left, map);
            errdefer left.deinit();
            const right = try compileJoinChild(input, j.right, map);
            return left.join(right, joinSpecOf(j));
        },
        .set_union => |u| {
            // SetUnion.create validates schema compatibility and does NOT
            // consume its inputs on error — both sides need errdefers (the
            // width-mismatch path is exercised by tests).
            var left = try compileBlock(input, u.left, map);
            errdefer left.deinit();
            var right = try compileBlock(input, u.right, map);
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
    win: *window_op.Window,
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
                    const win = stage.adopt_window orelse return null;
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
