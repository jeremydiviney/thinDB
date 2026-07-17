//! Keyed-pipeline-region recognizer for the wayroll rollforward shape
//! (task #184 P0, REGION_PLAN.md §7). Behind THINDB_REGION=1.
//!
//! Matches the post-pass IR of the 15-CTE rollforward query (ground truth:
//! the four rf_* TVF calls chained over one invoice scan, keyed end-to-end
//! by customerNumberLC) and compiles the whole subtree into ONE region
//! program: exchange-scatter the scan by wyhash(customerNumberLC), then per
//! shard run estimates-union → currency kernel → computes → in-group ranks
//! → keyed group-agg → gap-fill → up/down chain → broadcast probes → final
//! ranks, with zero stage materializations. The region's output becomes an
//! ordinary Stage (per-shard chunks adopted zero-copy); the keyed GROUP BY
//! above it and the rest of the query compile normally.
//!
//! Matching is strict: any structural deviation declines (returns null) and
//! the staged compiler proceeds untouched. Two semantic preconditions are
//! verified by EXECUTING small subtrees at compile time (same precedent as
//! scalar-subquery resolution): the customer-totals build side must be
//! EMPTY (its four LEFT joins then reduce to typed NULL columns — exact),
//! and the broadcast lookups (division / external_plan / currency rates)
//! are drained into in-memory maps/partitions.
//!
//! Name discipline: the region frame only ever APPENDS columns (no
//! pruning), so the engine's suffix-based column resolution would misbind
//! refs like `$amount` against stale entry columns. The recognizer instead
//! maintains the engine-visible name map itself (types.findColumn rules,
//! updated per select/alias/group node) and deep-clones every captured
//! expression, rewriting each col_ref to the unique frame-column name it
//! resolves to at that point.

const std = @import("std");
const Allocator = std.mem.Allocator;

const ir = @import("../ir/ir.zig");
const exec = @import("../exec/exec.zig");
const engine_v2 = @import("../exec/engine_v2.zig");
const region = @import("../exec/region_exec.zig");
const mat_stage = @import("../exec/mat_stage.zig");
const compute_mod = @import("../exec/compute.zig");
const expr_mod = @import("../exec/expr.zig");
const predicate_mod = @import("../exec/predicate.zig");
const types = @import("../types.zig");
const udf_mod = @import("../udf.zig");
const cte_stages = @import("cte_stages.zig");

const Column = types.Column;
const Value = types.Value;
const ColumnStore = @import("../engine/store.zig").ColumnStore;
const ColumnView = @import("../storage/storage.zig").ColumnView;
const Expr = expr_mod.Expr;
const PredicateExpr = predicate_mod.PredicateExpr;
const Derived = compute_mod.Derived;
const Scan = exec.Scan;

const StageMap = std.AutoHashMapUnmanaged(*const ir.Op, *mat_stage.Stage);

extern "c" fn getenv(name: [*:0]const u8) ?[*:0]const u8;

const NoMatch = error.RegionNoMatch;

pub const Recognized = struct {
    anchor: *const ir.Op,
    query: exec.Query,
};

/// Entry point (called from compileStaged behind THINDB_REGION=1). Returns
/// the region query + its anchor materialize node on success; null when the
/// plan doesn't match. Never fails the compile: every error except OOM is a
/// decline.
pub fn tryRecognize(input: engine_v2.CompileInput, root: *const ir.Op) ?Recognized {
    var cur = root;
    var depth: usize = 0;
    while (depth < 16) : (depth += 1) {
        switch (cur.*) {
            .materialize => |m| {
                if (recognizeAt(input, cur)) |q| {
                    return .{ .anchor = cur, .query = q };
                } else |e| {
                    if (e == error.OutOfMemory) return null;
                    if (getenv("THINDB_REGION_TRACE") != null) {
                        std.debug.print("[region] decline at materialize {*}: {s}\n", .{ cur, @errorName(e) });
                        if (@errorReturnTrace()) |t| {
                            const n = @min(t.index, t.instruction_addresses.len);
                            const st = std.debug.StackTrace{
                                .return_addresses = t.instruction_addresses[0..n],
                                .skipped = .none,
                            };
                            std.debug.dumpStackTrace(&st);
                        }
                    }
                }
                cur = m.upstream;
            },
            .select => |p| cur = p.upstream,
            .exclude => |p| cur = p.upstream,
            .filter => |f| cur = f.upstream,
            .group_by => |g| cur = g.upstream,
            .compute => |c| cur = c.upstream,
            .limit => |l| cur = l.upstream,
            .order_by => |o| cur = o.upstream,
            else => return null,
        }
    }
    return null;
}

// ---------------------------------------------------------------------------
// Spine walk: strict top-down capture of every node in the rollforward shape.
// ---------------------------------------------------------------------------

const Spine = struct {
    s14: *const ir.Op.Project,
    w15: *const ir.WindowOp,
    c16: []const Derived,
    s18: *const ir.Op.Project,
    c19: []const Derived,
    c20: []const Derived,
    j21: *const ir.Op.Join, // pc_last
    j22: *const ir.Op.Join, // pc_current
    x23: *const ir.Op.Project,
    j24: *const ir.Op.Join, // ctl
    j26: *const ir.Op.Join, // ctc
    s29: *const ir.Op.Project,
    s31: *const ir.Op.Project,
    j32: *const ir.Op.Join, // pc plan probe
    j33: *const ir.Op.Join, // division
    s36: *const ir.Op.Project,
    tf37: *const ir.Op.TableFn, // rf_updown_chain
    s38: *const ir.Op.Project,
    c39: []const Derived,
    s41: *const ir.Op.Project,
    tf42: *const ir.Op.TableFn, // rf_gap_fill
    s45: *const ir.Op.Project,
    w46: *const ir.WindowOp,
    s48: *const ir.Op.Project,
    c49: []const Derived,
    g50: *const ir.Op.GroupBy,
    c51: []const Derived,
    s53: *const ir.Op.Project,
    w54: *const ir.WindowOp,
    c55: []const Derived,
    s57: *const ir.Op.Project,
    tf58: *const ir.Op.TableFn, // rf_currency_convert
    s59: *const ir.Op.Project,
    u61: *const ir.SetUnion,
    base_mat: *const ir.Op, // shared materialize of the invoice base
    s63: *const ir.Op.Project,
    entry_derived: []const Derived, // c64
    f65: PredicateExpr,
    scan66: *const ir.Op.Scan,
    s68: *const ir.Op.Project,
    tf69: *const ir.Op.TableFn, // rf_estimates
    f71: PredicateExpr, // estimates date window
};

const Cursor = struct {
    cur: *const ir.Op,

    fn mat(w: *Cursor) !void {
        if (w.cur.* != .materialize) return NoMatch;
        w.cur = w.cur.materialize.upstream;
    }
    fn sel(w: *Cursor) !*const ir.Op.Project {
        if (w.cur.* != .select) return NoMatch;
        const p = &w.cur.select;
        w.cur = p.upstream;
        return p;
    }
    fn excl(w: *Cursor) !*const ir.Op.Project {
        if (w.cur.* != .exclude) return NoMatch;
        const p = &w.cur.exclude;
        w.cur = p.upstream;
        return p;
    }
    fn comp(w: *Cursor) ![]const Derived {
        if (w.cur.* != .compute) return NoMatch;
        const c = &w.cur.compute;
        w.cur = c.upstream;
        return c.derived;
    }
    fn win(w: *Cursor) !*const ir.WindowOp {
        if (w.cur.* != .window) return NoMatch;
        const ww = &w.cur.window;
        w.cur = ww.upstream;
        return ww;
    }
    fn joinLeft(w: *Cursor) !*const ir.Op.Join {
        if (w.cur.* != .join) return NoMatch;
        const j = &w.cur.join;
        if (j.join_type != .left) return NoMatch;
        w.cur = j.left;
        return j;
    }
    fn joinInner(w: *Cursor) !*const ir.Op.Join {
        if (w.cur.* != .join) return NoMatch;
        const j = &w.cur.join;
        if (j.join_type != .inner) return NoMatch;
        w.cur = j.left;
        return j;
    }
    fn alias(w: *Cursor) ![]const u8 {
        if (w.cur.* != .alias) return NoMatch;
        const a = &w.cur.alias;
        w.cur = a.upstream;
        return a.alias;
    }
    fn tvf(w: *Cursor, name: []const u8) !*const ir.Op.TableFn {
        if (w.cur.* != .table_fn) return NoMatch;
        const t = &w.cur.table_fn;
        if (!std.ascii.eqlIgnoreCase(t.name, name)) return NoMatch;
        if (t.inputs.len < 1) return NoMatch;
        w.cur = t.inputs[0];
        return t;
    }
    fn filt(w: *Cursor) !PredicateExpr {
        if (w.cur.* != .filter) return NoMatch;
        const f = &w.cur.filter;
        w.cur = f.upstream;
        return f.predicate;
    }
};

fn walkSpine(root_mat: *const ir.Op) !Spine {
    if (root_mat.* != .materialize) return NoMatch;
    var w = Cursor{ .cur = root_mat.materialize.upstream };
    var sp: Spine = undefined;

    sp.s14 = try w.sel();
    sp.w15 = try w.win();
    sp.c16 = try w.comp();
    try w.mat();
    sp.s18 = try w.sel();
    sp.c19 = try w.comp();
    sp.c20 = try w.comp();
    sp.j21 = try w.joinLeft();
    sp.j22 = try w.joinLeft();
    sp.x23 = try w.excl();
    sp.j24 = try w.joinLeft();
    _ = try w.comp(); // __join_on_left_3 (join-internal; build side is empty)
    sp.j26 = try w.joinLeft();
    if (!std.ascii.eqlIgnoreCase(try w.alias(), "r")) return NoMatch;
    try w.mat();
    sp.s29 = try w.sel();
    try w.mat();
    sp.s31 = try w.sel();
    sp.j32 = try w.joinLeft();
    sp.j33 = try w.joinInner();
    if (!std.ascii.eqlIgnoreCase(try w.alias(), "r")) return NoMatch;
    try w.mat();
    sp.s36 = try w.sel();
    sp.tf37 = try w.tvf("rf_updown_chain");
    sp.s38 = try w.sel();
    sp.c39 = try w.comp();
    try w.mat();
    sp.s41 = try w.sel();
    sp.tf42 = try w.tvf("rf_gap_fill");
    _ = try w.sel(); // s43: identity projection over the agg stage
    try w.mat();
    sp.s45 = try w.sel();
    sp.w46 = try w.win();
    try w.mat();
    sp.s48 = try w.sel();
    sp.c49 = try w.comp();
    if (w.cur.* != .group_by) return NoMatch;
    sp.g50 = &w.cur.group_by;
    w.cur = sp.g50.upstream;
    sp.c51 = try w.comp();
    try w.mat();
    sp.s53 = try w.sel();
    sp.w54 = try w.win();
    sp.c55 = try w.comp();
    try w.mat();
    sp.s57 = try w.sel();
    sp.tf58 = try w.tvf("rf_currency_convert");
    if (sp.tf58.inputs.len != 3) return NoMatch;
    sp.s59 = try w.sel();
    try w.mat();
    if (w.cur.* != .set_union) return NoMatch;
    sp.u61 = &w.cur.set_union;
    if (!sp.u61.all) return NoMatch;

    // Base arm: materialize -> select -> compute(entry) -> filter -> scan.
    sp.base_mat = sp.u61.left;
    var wb = Cursor{ .cur = sp.base_mat };
    try wb.mat();
    sp.s63 = try wb.sel();
    sp.entry_derived = try wb.comp();
    sp.f65 = try wb.filt();
    if (wb.cur.* != .scan) return NoMatch;
    sp.scan66 = &wb.cur.scan;

    // Estimates arm: materialize -> select -> rf_estimates(select -> filter
    // -> SAME base materialize node).
    var we = Cursor{ .cur = sp.u61.right };
    try we.mat();
    sp.s68 = try we.sel();
    sp.tf69 = try we.tvf("rf_estimates");
    _ = try we.sel(); // s70
    sp.f71 = try we.filt();
    if (we.cur != sp.base_mat) return NoMatch;

    // Entry compute shape: two string NULL literals + LOWER(customerNumber).
    if (sp.entry_derived.len != 3) return NoMatch;
    var lower_seen = false;
    for (sp.entry_derived) |d| {
        switch (d.expr) {
            .null_lit => |t| if (t != .string) return NoMatch,
            .call => |c| {
                if (!std.ascii.eqlIgnoreCase(c.fn_name, "LOWER") or c.args.len != 1) return NoMatch;
                if (c.args[0] != .col_ref) return NoMatch;
                lower_seen = true;
            },
            else => return NoMatch,
        }
    }
    if (!lower_seen) return NoMatch;
    return sp;
}

// ---------------------------------------------------------------------------
// Frame + visible-name map (types.findColumn rules over recognizer state).
// ---------------------------------------------------------------------------

const VisEntry = struct { name: []const u8, idx: usize };

const FrameB = struct {
    a: Allocator, // ctx arena
    cols: std.ArrayListUnmanaged(Column) = .empty,
    vis: std.ArrayListUnmanaged(VisEntry) = .empty,
    next_id: usize = 0,

    fn canonName(fb: *FrameB, hint: []const u8) ![]const u8 {
        const buf = try std.fmt.allocPrint(fb.a, "__rg{d}_{s}", .{ fb.next_id, hint });
        fb.next_id += 1;
        for (buf) |*ch| {
            if (ch.* == '.') ch.* = '_';
        }
        return buf;
    }

    /// Append a frame column under a canonical unique name; returns its idx.
    fn addCol(fb: *FrameB, hint: []const u8, t: types.Type, nullable: bool) !usize {
        const idx = fb.cols.items.len;
        try fb.cols.append(fb.a, .{ .name = try fb.canonName(hint), .type = t, .nullable = nullable });
        return idx;
    }

    /// Append a frame column keeping a REAL (entry) name — caller must
    /// guarantee uniqueness across the whole frame.
    fn addColNamed(fb: *FrameB, name: []const u8, t: types.Type, nullable: bool) !usize {
        const idx = fb.cols.items.len;
        try fb.cols.append(fb.a, .{ .name = try fb.a.dupe(u8, name), .type = t, .nullable = nullable });
        return idx;
    }

    fn setVis(fb: *FrameB, name: []const u8, idx: usize) !void {
        for (fb.vis.items) |*e| {
            if (std.ascii.eqlIgnoreCase(e.name, name)) {
                e.idx = idx;
                return;
            }
        }
        try fb.vis.append(fb.a, .{ .name = try fb.a.dupe(u8, name), .idx = idx });
    }

    fn removeVis(fb: *FrameB, name: []const u8) void {
        var i: usize = 0;
        while (i < fb.vis.items.len) {
            if (visRefMatches(fb.vis.items[i].name, name)) {
                _ = fb.vis.swapRemove(i);
                return;
            }
            i += 1;
        }
    }

    /// types.findColumn semantics over the visible map.
    fn resolve(fb: *const FrameB, name: []const u8) ?VisEntry {
        for (fb.vis.items) |e| {
            if (std.ascii.eqlIgnoreCase(e.name, name)) return e;
        }
        if (std.mem.lastIndexOfScalar(u8, name, '.')) |dot| {
            const tail = name[dot + 1 ..];
            for (fb.vis.items) |e| {
                if (std.ascii.eqlIgnoreCase(e.name, tail)) return e;
            }
            return null;
        }
        var match: ?VisEntry = null;
        for (fb.vis.items) |e| {
            const d = std.mem.lastIndexOfScalar(u8, e.name, '.') orelse continue;
            if (std.ascii.eqlIgnoreCase(e.name[d + 1 ..], name)) {
                if (match != null) return null; // ambiguous
                match = e;
            }
        }
        return match;
    }
};

fn visRefMatches(vis_name: []const u8, ref: []const u8) bool {
    if (std.ascii.eqlIgnoreCase(vis_name, ref)) return true;
    if (std.mem.lastIndexOfScalar(u8, ref, '.')) |dot| {
        return std.ascii.eqlIgnoreCase(vis_name, ref[dot + 1 ..]);
    }
    return false;
}

// ---------------------------------------------------------------------------
// Recognizer context: owns everything the compiled program borrows. Attached
// to the RegionExecOp and freed at query teardown.
// ---------------------------------------------------------------------------

const NullSide = struct {
    alias: []const u8,
    schema: []const Column, // arena copy of the compiled right-side schema
};

const Ctx = struct {
    gpa: Allocator,
    arena: std.heap.ArenaAllocator,
    prog: region.Program = undefined,
    prog_built: bool = false,

    fn destroyErased(p: *anyopaque) void {
        const self: *Ctx = @ptrCast(@alignCast(p));
        const gpa = self.gpa;
        if (self.prog_built) self.prog.deinit();
        var arena = self.arena;
        gpa.destroy(self);
        arena.deinit();
    }
};

const Builder = struct {
    input: engine_v2.CompileInput,
    ctx: *Ctx,
    a: Allocator, // ctx arena allocator
    fb: FrameB,
    ops: std.ArrayListUnmanaged(region.RegionOp) = .empty,
    /// Range-key SQL names (resolved on demand at each partition check).
    key_names: [3][]const u8 = .{ "projectId", "customerNumberLC", "divisionId" },
    /// LEFT-join sides proven all-NULL; refs against them append typed NULL
    /// columns on demand.
    null_sides: std.ArrayListUnmanaged(NullSide) = .empty,
    pending_nulls: std.ArrayListUnmanaged(Derived) = .empty,
    /// Probe-side projectId literal (from the scan filter) for the
    /// external_plan map build.
    project_lit: ?Value = null,

    fn resolveIdx(b: *Builder, name: []const u8) !usize {
        if (b.fb.resolve(name)) |e| return e.idx;
        return b.tryNullAppend(name);
    }

    /// A ref that doesn't resolve may target one of the proven-empty join
    /// sides: append a typed NULL frame column for it.
    fn tryNullAppend(b: *Builder, name: []const u8) !usize {
        errdefer if (getenv("THINDB_REGION_TRACE") != null) {
            std.debug.print("[region] unresolved ref '{s}' (vis: ", .{name});
            for (b.fb.vis.items) |e| std.debug.print("{s} ", .{e.name});
            std.debug.print(")\n", .{});
        };
        const dot = std.mem.indexOfScalar(u8, name, '.') orelse return NoMatch;
        const prefix = name[0..dot];
        for (b.null_sides.items) |ns| {
            if (!std.ascii.eqlIgnoreCase(ns.alias, prefix)) continue;
            const ci = types.findColumn(ns.schema, name) orelse return NoMatch;
            const col = ns.schema[ci];
            const idx = try b.fb.addCol(name, col.type, true);
            try b.pending_nulls.append(b.a, .{
                .name = b.fb.cols.items[idx].name,
                .expr = .{ .null_lit = col.type },
            });
            try b.fb.setVis(try visKeyFor(b.a, ns.alias, col.name), idx);
            return idx;
        }
        return NoMatch;
    }

    fn flushPending(b: *Builder) !void {
        if (b.pending_nulls.items.len == 0) return;
        const derived = try b.a.dupe(Derived, b.pending_nulls.items);
        b.pending_nulls.clearRetainingCapacity();
        try b.ops.append(b.a, .{ .compute = .{ .derived = derived } });
    }

    fn rangeKeyIdxs(b: *Builder) ![3]usize {
        var out: [3]usize = undefined;
        for (b.key_names, 0..) |n, i| {
            out[i] = (b.fb.resolve(n) orelse return NoMatch).idx;
        }
        return out;
    }

    fn partitionMatchesRangeKeys(b: *Builder, part: []const []const u8) !bool {
        if (part.len != 3) return false;
        const keys = try b.rangeKeyIdxs();
        var seen = [3]bool{ false, false, false };
        for (part) |p| {
            const idx = (b.fb.resolve(p) orelse return NoMatch).idx;
            var found = false;
            for (keys, 0..) |k, i| {
                if (k == idx and !seen[i]) {
                    seen[i] = true;
                    found = true;
                    break;
                }
            }
            if (!found) return false;
        }
        return seen[0] and seen[1] and seen[2];
    }

    // ---- expression cloning (col_ref rewrite to canonical frame names) ----

    fn cloneValue(b: *Builder, v: Value) !Value {
        return switch (v) {
            .text => |s| .{ .text = try b.a.dupe(u8, s) },
            else => v,
        };
    }

    fn cloneExpr(b: *Builder, e: Expr) anyerror!Expr {
        return switch (e) {
            .col_ref => |name| blk: {
                const idx = try b.resolveIdx(name);
                break :blk .{ .col_ref = b.fb.cols.items[idx].name };
            },
            .lit => |v| .{ .lit = try b.cloneValue(v) },
            .null_lit => |t| .{ .null_lit = t },
            .call => |c| blk: {
                const args = try b.a.alloc(Expr, c.args.len);
                for (c.args, args) |src, *dst| dst.* = try b.cloneExpr(src);
                break :blk .{ .call = .{ .fn_name = try b.a.dupe(u8, c.fn_name), .args = args } };
            },
            .case => |c| blk: {
                const branches = try b.a.alloc(Expr.Branch, c.branches.len);
                for (c.branches, branches) |src, *dst| {
                    dst.* = .{ .cond = try b.clonePred(src.cond), .then = try b.cloneExpr(src.then) };
                }
                var else_branch: ?*const Expr = null;
                if (c.else_branch) |eb| {
                    const p = try b.a.create(Expr);
                    p.* = try b.cloneExpr(eb.*);
                    else_branch = p;
                }
                break :blk .{ .case = .{ .branches = branches, .else_branch = else_branch } };
            },
            else => NoMatch,
        };
    }

    fn clonePred(b: *Builder, p: PredicateExpr) anyerror!PredicateExpr {
        return switch (p) {
            .leaf => |l| .{ .leaf = try b.cloneLeaf(l) },
            .day_leaf => |l| .{ .day_leaf = try b.cloneLeaf(l) },
            .leaf_col_col => |cc| blk: {
                const li = try b.resolveIdx(cc.left);
                const ri = try b.resolveIdx(cc.right);
                break :blk .{ .leaf_col_col = .{
                    .left = b.fb.cols.items[li].name,
                    .op = cc.op,
                    .right = b.fb.cols.items[ri].name,
                } };
            },
            .is_null => |name| blk: {
                const idx = try b.resolveIdx(name);
                break :blk .{ .is_null = b.fb.cols.items[idx].name };
            },
            .is_not_null => |name| blk: {
                const idx = try b.resolveIdx(name);
                break :blk .{ .is_not_null = b.fb.cols.items[idx].name };
            },
            .like => |l| blk: {
                const idx = try b.resolveIdx(l.col);
                break :blk .{ .like = .{
                    .col = b.fb.cols.items[idx].name,
                    .pattern = try b.a.dupe(u8, l.pattern),
                } };
            },
            .@"and" => |kids| blk: {
                const out = try b.a.alloc(PredicateExpr, kids.len);
                for (kids, out) |src, *dst| dst.* = try b.clonePred(src);
                break :blk .{ .@"and" = out };
            },
            .@"or" => |kids| blk: {
                const out = try b.a.alloc(PredicateExpr, kids.len);
                for (kids, out) |src, *dst| dst.* = try b.clonePred(src);
                break :blk .{ .@"or" = out };
            },
            .not => |child| blk: {
                const out = try b.a.create(PredicateExpr);
                out.* = try b.clonePred(child.*);
                break :blk .{ .not = out };
            },
            .always => |v| .{ .always = v },
            .in_set => |s| blk: {
                const idx = try b.resolveIdx(s.col);
                const vals = try b.a.alloc(Value, s.values.len);
                for (s.values, vals) |src, *dst| dst.* = try b.cloneValue(src);
                break :blk .{ .in_set = .{
                    .col = b.fb.cols.items[idx].name,
                    .values = vals,
                    .negate = s.negate,
                } };
            },
            else => NoMatch,
        };
    }

    fn cloneLeaf(b: *Builder, l: predicate_mod.Predicate) !predicate_mod.Predicate {
        const idx = try b.resolveIdx(l.col);
        return .{ .col = b.fb.cols.items[idx].name, .op = l.op, .val = try b.cloneValue(l.val) };
    }

    // ---- structural op appenders -----------------------------------------

    /// Clone `derived` against the current frame and append one compute op;
    /// each output becomes a fresh canonical frame column shadowing its
    /// visible name. Output TYPES come from a throwaway engine Compute over
    /// the frame schema — the same resolution the runtime instances use, so
    /// later type-driven decisions (sum int vs float, int-family checks)
    /// can never disagree with execution.
    fn pushCompute(b: *Builder, derived: []const Derived) !void {
        // Clone FIRST (may append pending null columns), flush those, then
        // append this op — execution order preserved.
        const out = try b.a.alloc(Derived, derived.len);
        for (derived, out) |src, *dst| {
            const expr = try b.cloneExpr(src.expr);
            dst.* = .{ .name = try b.fb.canonName(src.name), .expr = expr };
        }
        try b.flushPending();

        const typed = region.computeOutputSchema(
            b.input.allocator,
            b.a,
            b.fb.cols.items,
            out,
            b.input.udf_registry,
        ) catch return NoMatch;
        if (typed.len != b.fb.cols.items.len + derived.len) return NoMatch;
        try b.ops.append(b.a, .{ .compute = .{ .derived = out } });
        for (derived, typed[b.fb.cols.items.len..]) |src, col| {
            const idx = b.fb.cols.items.len;
            try b.fb.cols.append(b.a, col);
            try b.fb.setVis(src.name, idx);
        }
    }

    fn applySelect(b: *Builder, p: *const ir.Op.Project) !void {
        var new_vis: std.ArrayListUnmanaged(VisEntry) = .empty;
        for (p.columns, 0..) |col, i| {
            if (b.fb.resolve(col) == null) _ = try b.tryNullAppend(col);
            const e = b.fb.resolve(col) orelse return NoMatch;
            var out_name: []const u8 = e.name;
            if (p.outputs) |outs| {
                if (i < outs.len) {
                    if (outs[i]) |o| out_name = o;
                }
            }
            try new_vis.append(b.a, .{ .name = try b.a.dupe(u8, out_name), .idx = e.idx });
        }
        try b.flushPending();
        b.fb.vis = new_vis;
    }

    fn applyAlias(b: *Builder, alias: []const u8) !void {
        // Requalify on the LAST name segment (idempotent): the SQL's refs
        // address alias.col regardless of how many qualifier layers the
        // upstream names accumulated, and any resulting ambiguity declines
        // through the resolver rather than misbinding.
        for (b.fb.vis.items) |*e| {
            e.name = try std.fmt.allocPrint(b.a, "{s}.{s}", .{ alias, lastSegment(e.name) });
        }
    }
};

fn visKeyFor(a: Allocator, alias: []const u8, col_name: []const u8) ![]const u8 {
    // The compiled right side already qualifies names ("ctc.amount"); keep
    // them verbatim, otherwise qualify with the alias.
    if (std.mem.indexOfScalar(u8, col_name, '.') != null) return a.dupe(u8, col_name);
    return std.fmt.allocPrint(a, "{s}.{s}", .{ alias, col_name });
}

// ---------------------------------------------------------------------------
// Compile-time subtree execution (broadcasts + emptiness probes).
// ---------------------------------------------------------------------------

const DrainedBlock = struct {
    schema: []const Column, // arena copy
    rows: usize,
    /// Arena stores per schema column (empty when drain=false).
    stores: []ColumnStore,
};

fn compileAndDrain(b: *Builder, node: *const ir.Op, drain: bool) !DrainedBlock {
    var local_map: StageMap = .empty;
    defer local_map.deinit(b.input.allocator);
    var q = cte_stages.compileBlock(b.input, node, &local_map) catch return NoMatch;
    defer q.deinit();

    const src_schema = q.outputSchema();
    const schema = try b.a.alloc(Column, src_schema.len);
    for (src_schema, schema) |src, *dst| {
        dst.* = src;
        dst.name = try b.a.dupe(u8, src.name);
    }

    var stores: []ColumnStore = &.{};
    var rows: usize = 0;
    if (drain) {
        stores = try b.a.alloc(ColumnStore, schema.len);
        for (stores, schema) |*s, col| s.* = try ColumnStore.init(b.a, col.type, true);
        while (q.next() catch return NoMatch) |batch| {
            for (stores, 0..) |*s, ci| {
                try appendViewAll(b.a, s, batch.values[ci], batch.row_count);
            }
            rows += batch.row_count;
        }
    }
    return .{ .schema = schema, .rows = rows, .stores = stores };
}

fn appendViewAll(a: Allocator, store: *ColumnStore, v: ColumnView, n: usize) !void {
    for (0..n) |i| {
        if (!v.isValid(i)) {
            try store.appendNulls(a, 1);
            continue;
        }
        switch (v.data) {
            .tinyint => |s| try store.data.tinyint.append(a, s[i]),
            .smallint => |s| try store.data.smallint.append(a, s[i]),
            .int => |s| try store.data.int.append(a, s[i]),
            .bigint => |s| try store.data.bigint.append(a, s[i]),
            .date => |s| try store.data.date.append(a, s[i]),
            .datetime => |s| try store.data.datetime.append(a, s[i]),
            .float => |s| try store.data.float.append(a, s[i]),
            .double => |s| try store.data.double.append(a, s[i]),
            .varchar, .string, .char, .json => |s| switch (store.data) {
                .varchar, .string, .char, .json => |*d| try d.appendValue(a, s.rowBytes(i)),
                else => return NoMatch,
            },
            else => return NoMatch,
        }
        if (store.nulls != null) try store.appendValidBit(a, store.rowCount() - 1, true);
    }
}

fn i64At(v: ColumnView, i: usize) ?i64 {
    if (!v.isValid(i)) return null;
    return switch (v.data) {
        .tinyint => |s| s[i],
        .smallint => |s| s[i],
        .int => |s| s[i],
        .bigint => |s| s[i],
        .date => |s| s[i],
        .datetime => |s| s[i],
        else => null,
    };
}

// ---------------------------------------------------------------------------
// The full recognize + build pass.
// ---------------------------------------------------------------------------

fn recognizeAt(input: engine_v2.CompileInput, anchor: *const ir.Op) anyerror!exec.Query {
    const sp = try walkSpine(anchor);

    const registry = input.udf_registry orelse return NoMatch;
    const ent_est = registry.tableByName("rf_estimates") orelse return NoMatch;
    const ent_cur = registry.tableByName("rf_currency_convert") orelse return NoMatch;
    const ent_gap = registry.tableByName("rf_gap_fill") orelse return NoMatch;
    const ent_ud = registry.tableByName("rf_updown_chain") orelse return NoMatch;
    if (getenv("THINDB_REGION_TRACE") != null) {
        for ([_]*const udf_mod.TableEntry{ ent_est, ent_cur, ent_gap, ent_ud }) |e| {
            std.debug.print("[region] entry {s}: in0={d} kic={d} pass={d} out={d} bcast={d}\n", .{
                e.name, e.input_schemas[0].len, e.kernel_input_cols, e.passthrough.len, e.output_schema.len, e.broadcast_inputs.len,
            });
        }
    }
    // Union-append / replace kernels must write EVERY output themselves: no
    // pass-through pairs and no carry split (kic 0 or == full input width).
    if (!kernelReadsAll(ent_gap) or ent_gap.passthrough.len != 0) return NoMatch;
    if (!kernelReadsAll(ent_est) or ent_est.passthrough.len != 0) return NoMatch;
    if (ent_cur.input_schemas.len != 3 or ent_cur.broadcast_inputs.len != 2) return NoMatch;

    const gpa = input.allocator;
    const ctx = try gpa.create(Ctx);
    ctx.* = .{ .gpa = gpa, .arena = std.heap.ArenaAllocator.init(gpa) };
    errdefer Ctx.destroyErased(ctx);
    const a = ctx.arena.allocator();

    var b = Builder{ .input = input, .ctx = ctx, .a = a, .fb = .{ .a = a } };

    // ---- scan filter: probe-side projectId literal + prune leaves --------
    var prune_leaves: std.ArrayListUnmanaged(predicate_mod.Predicate) = .empty;
    try collectAndLeaves(&b, sp.f65, &prune_leaves);
    for (prune_leaves.items) |l| {
        if (std.ascii.eqlIgnoreCase(l.col, "projectId") and l.op == .eq) {
            b.project_lit = l.val;
        }
    }
    if (b.project_lit == null) return NoMatch; // merge_on + plan-map packing need it

    // ---- sources: chunked fused-filter scans (rf_custom recipe) ----------
    const table = input.db.openTable(sp.scan66.table.name, .{}) catch return NoMatch;

    // Scan projection = the base select's items minus the entry-computed
    // names (they exist only after the entry compute).
    var scan_cols: std.ArrayListUnmanaged([]const u8) = .empty;
    outer: for (sp.s63.columns) |col| {
        for (sp.entry_derived) |d| {
            if (std.ascii.eqlIgnoreCase(d.name, col)) continue :outer;
        }
        try scan_cols.append(a, col);
    }

    const dop = input.effectiveDop();
    const n_threads = @max(dop, 1);
    const n_shards: usize = blk: {
        if (getenv("THINDB_REGION_SHARDS")) |v| {
            break :blk std.fmt.parseInt(usize, std.mem.span(v), 10) catch 64;
        }
        break :blk 64;
    };

    table.ddl_lock.lockSharedUncancelable(table.io);
    defer table.ddl_lock.unlockShared(table.io);
    const snap = try Scan.captureSnapshotAlloc(table, gpa);
    defer gpa.free(snap.segments);
    var pin_held = true;
    defer if (pin_held) snap.memtable_snap.release();

    var total_rgs: usize = 0;
    var total_rows: u64 = snap.memtable_row_count;
    const seg_start = try a.alloc(usize, snap.segment_count + 1);
    for (snap.segments, 0..) |e, i| {
        seg_start[i] = total_rgs;
        total_rgs += e.row_group_count;
        total_rows += e.row_count;
    }
    seg_start[snap.segment_count] = total_rgs;
    const n_chunks = @max(n_threads, @min(n_threads * 4, @max(total_rgs, 1)));

    const sources = try gpa.alloc(exec.Query, n_chunks);
    var built: usize = 0;
    var sources_owned = true; // RegionExecOp takes them over on create
    errdefer if (sources_owned) {
        for (sources[0..built]) |*q| q.deinit();
        gpa.free(sources);
    };
    for (0..n_chunks) |i| {
        const lo = i * total_rgs / n_chunks;
        const hi = if (i == n_chunks - 1) total_rgs else (i + 1) * total_rgs / n_chunks;
        // emit_loc: the physical row locator rides through the exchange as
        // the FINAL consolidation sort key, so (invoiceId, date) ties inside
        // a group keep the table's physical order — the same order the
        // engine's staged path presents to the estimates kernel, whose
        // representative-row picks are input-order-sensitive ("first row
        // wins"). Without it those picks are scatter-arrival nondeterministic.
        const s = Scan.allocWithProjectionLoc(gpa, table, input.accountant, scan_cols.items, true, snap) catch return NoMatch;
        sources[i] = exec.makeQuery(gpa, s);
        built += 1;
        const start = flatToCoord(lo, seg_start, snap.segment_count);
        const end = flatToCoord(hi, seg_start, snap.segment_count);
        s.setRange(start.seg, start.rg, end.seg, end.rg, i == n_chunks - 1);
        for (prune_leaves.items) |l| s.addPrune(l) catch {};
        const fused = s.tryFuseFilter(sp.f65) catch return NoMatch;
        if (!fused) return NoMatch;
    }
    snap.memtable_snap.release();
    pin_held = false;

    // ---- entry schema = scan output ++ entry-computed columns ------------
    const scan_schema = sources[0].outputSchema();
    const entry_schema = try a.alloc(Column, scan_schema.len + sp.entry_derived.len);
    for (scan_schema, entry_schema[0..scan_schema.len]) |src, *dst| {
        dst.* = src;
        dst.name = try a.dupe(u8, src.name);
    }
    // The __rowloc tie-break column: nullable so kernel-generated rows
    // (which have no physical location) NULL-pad it.
    const rowloc_entry = scan_schema.len - 1;
    entry_schema[rowloc_entry].nullable = true;
    for (sp.entry_derived, entry_schema[scan_schema.len..]) |d, *dst| {
        dst.* = .{ .name = try a.dupe(u8, d.name), .type = .string, .nullable = true };
    }
    for (entry_schema, 0..) |col, i| {
        _ = try b.fb.addColNamed(col.name, col.type, col.nullable);
        try b.fb.setVis(col.name, i);
    }

    const pj_entry = (b.fb.resolve("projectId") orelse return NoMatch).idx;
    const lc_entry = (b.fb.resolve("customerNumberLC") orelse return NoMatch).idx;
    const div_entry = (b.fb.resolve("divisionId") orelse return NoMatch).idx;

    // Consolidation contract: prefix (projectId, customerNumberLC,
    // divisionId) — lc before div so equal-(project,lc) ranges are ADJACENT
    // (the final window merges across divisions via merge_on) — then the
    // estimates order (invoiceId, date) inside each range.
    if (sp.tf69.order_by.len != 2) return NoMatch;
    for (sp.tf69.order_by) |ob| if (ob.desc) return NoMatch;
    const est_o0 = (b.fb.resolve(sp.tf69.order_by[0].col) orelse return NoMatch).idx;
    const est_o1 = (b.fb.resolve(sp.tf69.order_by[1].col) orelse return NoMatch).idx;
    const sort_cols = try a.alloc(region.OrderCol, 6);
    sort_cols[0] = .{ .col = pj_entry, .kind = try orderKind(entry_schema[pj_entry].type) };
    sort_cols[1] = .{ .col = lc_entry, .kind = try orderKind(entry_schema[lc_entry].type) };
    sort_cols[2] = .{ .col = div_entry, .kind = try orderKind(entry_schema[div_entry].type) };
    sort_cols[3] = .{ .col = est_o0, .kind = try orderKind(entry_schema[est_o0].type) };
    sort_cols[4] = .{ .col = est_o1, .kind = try orderKind(entry_schema[est_o1].type) };
    sort_cols[5] = .{ .col = rowloc_entry, .kind = .int64 };

    // ---- visible map at the estimates input ------------------------------
    try b.applySelect(sp.s63);

    // ---- op 0: rf_estimates union-append (fused consolidation tail) ------
    if (!try b.partitionMatchesRangeKeys(sp.tf69.partition_by)) return NoMatch;
    const est_inputs = try tvfInputs(&b, ent_est, ent_est.input_schemas[0].len);
    const est_out = try a.alloc(Column, est_inputs.len);
    for (est_out, est_inputs) |*o, ci| o.* = b.fb.cols.items[ci];
    const est_filter = try dateWindow(sp.f71, &b);
    try b.ops.append(a, .{ .tvf_grouped = .{
        .spec = .{
            .process = ent_est.process,
            .user_data = ent_est.user_data,
            .args = try cloneArgs(&b, sp.tf69.args),
            .inputs = est_inputs,
            .out = est_out,
        },
        .union_append = true,
        .input_filter = .{ .col = est_filter.col, .lo = est_filter.lo, .hi = est_filter.hi },
    } });
    try b.applySelect(sp.s59);

    // ---- broadcast partitions for rf_currency (rates + plans) ------------
    const rates_blk = try compileAndDrain(&b, sp.tf58.inputs[1], true);
    const plans_blk = try compileAndDrain(&b, sp.tf58.inputs[2], true);
    const rates_part = try blockPartition(&b, ent_cur.input_schemas[1], rates_blk);
    const plans_part = try blockPartition(&b, ent_cur.input_schemas[2], plans_blk);
    const extra_parts = try a.alloc(udf_mod.TvfPartition, 2);
    extra_parts[0] = rates_part;
    extra_parts[1] = plans_part;

    // ---- op 1: rf_currency aligned over the whole shard ------------------
    // (kernel groups internally by its useDivision argument — call-site
    // partition granularity is irrelevant to its output values).
    try pushAlignedTvf(&b, ent_cur, extra_parts, sp.tf58.args, false);
    try b.applySelect(sp.s57);

    // ---- computes + in-group ranks + keyed aggregation -------------------
    try b.pushCompute(sp.c55);
    try pushRanksFromWindow(&b, sp.w54, null);
    try b.applySelect(sp.s53);
    try b.pushCompute(sp.c51);
    try pushGroupAgg(&b, sp.g50);
    try b.pushCompute(sp.c49);
    try b.applySelect(sp.s48);
    try pushFillLast(&b, sp.w46);
    try b.applySelect(sp.s45);

    // ---- rf_gap_fill: per-range replace ----------------------------------
    if (!try b.partitionMatchesRangeKeys(sp.tf42.partition_by)) return NoMatch;
    try checkMonthOrder(&b, sp.tf42.order_by);
    {
        const inputs = try tvfInputs(&b, ent_gap, ent_gap.input_schemas[0].len);
        const out = try a.alloc(Column, ent_gap.output_schema.len);
        for (ent_gap.output_schema, out) |src, *dst| {
            dst.* = .{ .name = try b.fb.canonName(src.name), .type = src.type, .nullable = true };
        }
        try b.flushPending();
        try b.ops.append(a, .{ .tvf_grouped = .{ .spec = .{
            .process = ent_gap.process,
            .user_data = ent_gap.user_data,
            .args = try cloneArgs(&b, sp.tf42.args),
            .inputs = inputs,
            .out = out,
        } } });
        // Frame REPLACED by the kernel output.
        b.fb.cols.clearRetainingCapacity();
        b.fb.vis.clearRetainingCapacity();
        for (ent_gap.output_schema, out) |src, o| {
            const idx = b.fb.cols.items.len;
            try b.fb.cols.append(a, o);
            _ = src;
            try b.fb.setVis(ent_gap.output_schema[idx].name, idx);
        }
    }
    try b.applySelect(sp.s41);
    try b.pushCompute(sp.c39);
    try b.applySelect(sp.s38);

    // ---- rf_updown_chain: per-range aligned append -----------------------
    if (!try b.partitionMatchesRangeKeys(sp.tf37.partition_by)) return NoMatch;
    try checkMonthOrder(&b, sp.tf37.order_by);
    try pushAlignedTvf(&b, ent_ud, &.{}, sp.tf37.args, true);
    try b.applySelect(sp.s36);
    try b.applyAlias("r");

    // ---- division INNER probe --------------------------------------------
    {
        if (sp.j33.on.len != 1 or sp.j33.extra_predicate != null or sp.j33.ranges.len != 0) return NoMatch;
        const div_blk = try compileAndDrain(&b, sp.j33.right, true);
        const key_ci = types.findColumn(div_blk.schema, sp.j33.on[0].right) orelse return NoMatch;
        const map = try a.create(region.KeyMap);
        map.* = .empty;
        if (div_blk.rows > 0) {
            const kv = div_blk.stores[key_ci].view();
            for (0..div_blk.rows) |i| {
                if (i64At(kv, i)) |k| try map.put(a, k, @intCast(i));
            }
        }
        const probe = try b.resolveIdx(sp.j33.on[0].left);
        try b.flushPending();
        try b.ops.append(a, .{ .hash_probe = .{
            .probe = probe,
            .map = map,
            .payload = &.{},
            .inner = true,
        } });
    }

    // ---- external_plan LEFT probe (packed icid<<32 + id key) -------------
    {
        if (sp.j32.on.len != 3 or sp.j32.extra_predicate != null or sp.j32.ranges.len != 0) return NoMatch;
        const plan_blk = try compileAndDrain(&b, sp.j32.right, true);
        var left_pj: ?[]const u8 = null;
        var left_ic: ?[]const u8 = null;
        var left_id: ?[]const u8 = null;
        var right_pj: ?usize = null;
        var right_ic: ?usize = null;
        var right_id: ?usize = null;
        for (sp.j32.on) |pair| {
            const tail = lastSegment(pair.left);
            const rci = types.findColumn(plan_blk.schema, pair.right) orelse return NoMatch;
            if (std.ascii.eqlIgnoreCase(tail, "projectId")) {
                left_pj = pair.left;
                right_pj = rci;
            } else if (std.ascii.eqlIgnoreCase(tail, "integrationConfigId")) {
                left_ic = pair.left;
                right_ic = rci;
            } else if (std.ascii.eqlIgnoreCase(tail, "planId")) {
                left_id = pair.left;
                right_id = rci;
            } else return NoMatch;
        }
        if (left_pj == null or left_ic == null or left_id == null) return NoMatch;

        const map = try a.create(region.KeyMap);
        map.* = .empty;
        const pay_store = try a.create(ColumnStore);
        pay_store.* = try ColumnStore.init(a, .string, true);
        // Build rows: skip NULL keys (SQL join can't match them); a project
        // mismatch vs the probe-side literal can never match either.
        const probe_pj: i64 = valueI64(b.project_lit.?) orelse return NoMatch;
        // Payload: the single right-side column the projection above
        // consumes (everything else it selects is r.* / computed).
        const ext_ci = blk: {
            for (sp.s31.columns) |col| {
                const dot = std.mem.indexOfScalar(u8, col, '.') orelse continue;
                if (std.ascii.eqlIgnoreCase(col[0..dot], "r")) continue;
                break :blk types.findColumn(plan_blk.schema, col) orelse return NoMatch;
            }
            return NoMatch;
        };
        if (plan_blk.rows > 0) {
            const pjv = plan_blk.stores[right_pj.?].view();
            const icv = plan_blk.stores[right_ic.?].view();
            const idv = plan_blk.stores[right_id.?].view();
            const extv = plan_blk.stores[ext_ci].view();
            var kept: u32 = 0;
            for (0..plan_blk.rows) |i| {
                const pj = i64At(pjv, i) orelse continue;
                if (pj != probe_pj) continue;
                const ic = i64At(icv, i) orelse continue;
                const id = i64At(idv, i) orelse continue;
                const key = ic * 4294967296 + id;
                const gop = try map.getOrPut(a, key);
                if (gop.found_existing) return NoMatch; // dup key would change LEFT-join row counts
                gop.value_ptr.* = kept;
                if (extv.isValid(i)) {
                    switch (extv.data) {
                        .varchar, .string, .char, .json => |s| {
                            try pay_store.data.string.appendValue(a, s.rowBytes(i));
                            try pay_store.appendValidBit(a, pay_store.rowCount() - 1, true);
                        },
                        else => return NoMatch,
                    }
                } else {
                    try pay_store.appendNulls(a, 1);
                }
                kept += 1;
            }
        }

        // Probe key compute: to_bigint(icid) * 2^32 + to_bigint(planId);
        // NULL on either side propagates → probe miss, matching SQL.
        const ic_idx = try b.resolveIdx(left_ic.?);
        const id_idx = try b.resolveIdx(left_id.?);
        const mul_args = try a.alloc(Expr, 2);
        mul_args[0] = .{ .call = .{ .fn_name = "to_bigint", .args = try dupExpr(a, .{ .col_ref = b.fb.cols.items[ic_idx].name }) } };
        mul_args[1] = .{ .lit = .{ .bigint = 4294967296 } };
        const add_args = try a.alloc(Expr, 2);
        add_args[0] = .{ .call = .{ .fn_name = "mul", .args = mul_args } };
        add_args[1] = .{ .call = .{ .fn_name = "to_bigint", .args = try dupExpr(a, .{ .col_ref = b.fb.cols.items[id_idx].name }) } };
        const key_idx = try b.fb.addCol("plan_key", .bigint, true);
        const key_derived = try a.alloc(Derived, 1);
        key_derived[0] = .{ .name = b.fb.cols.items[key_idx].name, .expr = .{ .call = .{ .fn_name = "add", .args = add_args } } };
        try b.flushPending();
        try b.ops.append(a, .{ .compute = .{ .derived = key_derived } });

        const ext_name = plan_blk.schema[ext_ci].name;
        const payload = try a.alloc(region.Payload, 1);
        const pay_idx = try b.fb.addCol(ext_name, .string, true);
        payload[0] = .{ .name = b.fb.cols.items[pay_idx].name, .view = pay_store.view(), .out_type = .string };
        try b.ops.append(a, .{ .hash_probe = .{
            .probe = key_idx,
            .map = map,
            .payload = payload,
            .inner = false,
        } });
        try b.fb.setVis(ext_name, pay_idx);
    }

    try b.applySelect(sp.s31);
    try b.applySelect(sp.s29);
    try b.applyAlias("r");

    // ---- customer-totals cluster: prove empty, then all four LEFT joins
    // collapse to typed NULL columns (appended lazily on first reference).
    {
        const ctc_alias = try rightAliasName(sp.j26.right);
        const ctl_alias = try rightAliasName(sp.j24.right);
        const pcc_alias = try rightAliasName(sp.j22.right);
        const pcl_alias = try rightAliasName(sp.j21.right);

        const ctc_blk = try compileAndDrain(&b, sp.j26.right, true);
        if (ctc_blk.rows != 0) return NoMatch;
        const ctl_blk = try compileAndDrain(&b, sp.j24.right, true);
        if (ctl_blk.rows != 0) return NoMatch;
        const pcc_blk = try compileAndDrain(&b, sp.j22.right, false);
        const pcl_blk = try compileAndDrain(&b, sp.j21.right, false);

        // pc_current / pc_last are NOT empty tables — their joins only
        // collapse because a join key comes from the empty ctc/ctl side
        // (all-NULL ⇒ never matches). Require that structurally.
        if (!joinKeyTouchesAlias(sp.j22, ctc_alias) and !joinKeyTouchesAlias(sp.j22, ctl_alias)) return NoMatch;
        if (!joinKeyTouchesAlias(sp.j21, ctc_alias) and !joinKeyTouchesAlias(sp.j21, ctl_alias)) return NoMatch;

        try b.null_sides.append(a, .{ .alias = ctc_alias, .schema = ctc_blk.schema });
        try b.null_sides.append(a, .{ .alias = ctl_alias, .schema = ctl_blk.schema });
        try b.null_sides.append(a, .{ .alias = pcc_alias, .schema = pcc_blk.schema });
        try b.null_sides.append(a, .{ .alias = pcl_alias, .schema = pcl_blk.schema });
    }

    // exclude (join-internal computed key): drop from vis if present.
    for (sp.x23.columns) |col| b.fb.removeVis(col);

    try b.pushCompute(sp.c20);
    try b.pushCompute(sp.c19);
    try b.applySelect(sp.s18);
    try b.pushCompute(sp.c16);

    // ---- final ranks: partition (projectId, customerNumberLC) — coarser
    // than the range keys; adjacent equal-lc ranges merge (projectId is a
    // scan-filter constant, verified above).
    {
        const wc = sp.w15;
        if (wc.calls.len != 1 or wc.calls[0].func != .row_number) return NoMatch;
        const spec = wc.specs[wc.calls[0].spec_idx];
        if (spec.partition_by.len != 2) return NoMatch;
        const keys = try b.rangeKeyIdxs();
        var part_idx: [2]usize = undefined;
        for (spec.partition_by, 0..) |p, i| {
            part_idx[i] = (b.fb.resolve(p) orelse return NoMatch).idx;
        }
        const pj_idx = keys[0];
        const lc_idx = keys[1];
        const part_ok = (part_idx[0] == pj_idx and part_idx[1] == lc_idx) or
            (part_idx[0] == lc_idx and part_idx[1] == pj_idx);
        if (!part_ok) return NoMatch;
        const order = try cloneOrder(&b, spec.order_by);
        try b.flushPending();
        const rank_idx = try b.fb.addCol(wc.calls[0].output_name, .bigint, false);
        try b.ops.append(a, .{ .ranks = .{
            .name = b.fb.cols.items[rank_idx].name,
            .order = order,
            .merge_on = lc_idx,
        } });
        try b.fb.setVis(wc.calls[0].output_name, rank_idx);
    }

    // ---- emit per the projection above the final window ------------------
    const emit_cols = try a.alloc(usize, sp.s14.columns.len);
    const emit_names = try a.alloc([]const u8, sp.s14.columns.len);
    for (sp.s14.columns, 0..) |col, i| {
        const e = b.fb.resolve(col) orelse return NoMatch;
        emit_cols[i] = e.idx;
        var out_name: []const u8 = e.name;
        if (sp.s14.outputs) |outs| {
            if (i < outs.len) {
                if (outs[i]) |o| out_name = o;
            }
        }
        emit_names[i] = try a.dupe(u8, out_name);
    }
    try b.flushPending();
    try b.ops.append(a, .{ .emit = .{ .cols = emit_cols } });

    // ---- compile the program ---------------------------------------------
    ctx.prog = region.Program.build(gpa, entry_schema, b.ops.items, registry) catch return NoMatch;
    ctx.prog_built = true;
    // Emit-column NAMES for the stage schema: the program derives them from
    // the frame (canonical); patch to the SQL-visible names the group-by
    // above resolves against.
    if (ctx.prog.output_schema.len != emit_names.len) return NoMatch;
    const patched = try a.alloc(Column, ctx.prog.output_schema.len);
    for (ctx.prog.output_schema, emit_names, patched) |src, name, *dst| {
        dst.* = src;
        dst.name = name;
    }
    ctx.prog.output_schema = patched;

    const opts = region.DriverOpts{
        .n_threads = n_threads,
        .n_shards = n_shards,
        .key_col = lc_entry,
        .sort_cols = sort_cols,
        .group_prefix = 3,
    };

    var q = try region.RegionExecOp.create(
        gpa,
        entry_schema,
        sources,
        sp.entry_derived,
        &ctx.prog,
        opts,
        null,
        total_rows * 2,
    );
    sources_owned = false; // the query owns sources (and, below, the ctx)
    const op = exec.queryAs(region.RegionExecOp, q) orelse {
        q.deinit();
        return NoMatch;
    };
    op.setOwnedCtx(ctx, Ctx.destroyErased);
    return q;
}

// ---------------------------------------------------------------------------
// Helpers for the build pass.
// ---------------------------------------------------------------------------

fn collectAndLeaves(b: *Builder, p: PredicateExpr, out: *std.ArrayListUnmanaged(predicate_mod.Predicate)) !void {
    switch (p) {
        .leaf => |l| try out.append(b.a, l),
        .@"and" => |kids| for (kids) |k| try collectAndLeaves(b, k, out),
        else => {},
    }
}

const Coord = struct { seg: usize, rg: usize };

fn flatToCoord(flat: usize, seg_start: []const usize, n_segs: usize) Coord {
    var s: usize = 0;
    while (s < n_segs and seg_start[s + 1] <= flat) s += 1;
    return .{ .seg = s, .rg = flat - seg_start[@min(s, n_segs)] };
}

fn orderKind(t: types.Type) !region.OrderKind {
    return switch (t) {
        .tinyint, .smallint, .int, .date => .int32,
        .bigint, .datetime => .int64,
        .varchar, .string, .char => .string,
        else => NoMatch,
    };
}

fn lastSegment(name: []const u8) []const u8 {
    if (std.mem.lastIndexOfScalar(u8, name, '.')) |d| return name[d + 1 ..];
    return name;
}

fn valueI64(v: Value) ?i64 {
    return switch (v) {
        .tinyint => |x| x,
        .smallint => |x| x,
        .int => |x| x,
        .bigint => |x| x,
        .date => |x| x,
        .datetime => |x| x,
        else => null,
    };
}

fn dupExpr(a: Allocator, e: Expr) ![]Expr {
    const s = try a.alloc(Expr, 1);
    s[0] = e;
    return s;
}

fn cloneArgs(b: *Builder, args: []const ?Value) ![]const ?Value {
    const out = try b.a.alloc(?Value, args.len);
    for (args, out) |src, *dst| {
        dst.* = if (src) |v| try b.cloneValue(v) else null;
    }
    return out;
}

/// Resolve the kernel's declared input-0 columns (first `count`) against
/// the current visible map.
fn tvfInputs(b: *Builder, ent: *const udf_mod.TableEntry, count: usize) ![]const usize {
    if (count > ent.input_schemas[0].len) return NoMatch;
    const inputs = try b.a.alloc(usize, count);
    for (ent.input_schemas[0][0..count], inputs) |col, *dst| {
        dst.* = (b.fb.resolve(col.name) orelse return NoMatch).idx;
    }
    return inputs;
}

const DateWindow = struct { col: usize, lo: i64, hi: i64 };

fn dateWindow(p: PredicateExpr, b: *Builder) !DateWindow {
    if (p != .@"and" or p.@"and".len != 2) return NoMatch;
    var col: ?usize = null;
    var lo: ?i64 = null;
    var hi: ?i64 = null;
    for (p.@"and") |k| {
        if (k != .leaf) return NoMatch;
        const l = k.leaf;
        const idx = (b.fb.resolve(l.col) orelse return NoMatch).idx;
        if (col != null and col.? != idx) return NoMatch;
        col = idx;
        const v = valueI64(l.val) orelse return NoMatch;
        switch (l.op) {
            .gte => lo = v,
            .lte => hi = v,
            else => return NoMatch,
        }
    }
    return .{ .col = col orelse return NoMatch, .lo = lo orelse return NoMatch, .hi = hi orelse return NoMatch };
}

/// Build a broadcast TvfPartition from a drained block, columns mapped to
/// the kernel's declared broadcast-input schema by name.
fn blockPartition(b: *Builder, want: []const Column, blk: DrainedBlock) !udf_mod.TvfPartition {
    const views = try b.a.alloc(ColumnView, want.len);
    for (want, views) |col, *v| {
        const ci = types.findColumn(blk.schema, col.name) orelse return NoMatch;
        if (!std.meta.eql(blk.schema[ci].type, col.type)) return NoMatch;
        v.* = blk.stores[ci].view();
    }
    return .{ .columns = views, .row_count = blk.rows, .keys = &.{} };
}

/// Row-aligned passthrough kernel: inputs = the declared kernel-visible
/// input columns; out = the non-passthrough (computed) outputs. Appends
/// the computed columns; passthrough outputs re-map to their frame sources
/// in the visible map.
fn pushAlignedTvf(
    b: *Builder,
    ent: *const udf_mod.TableEntry,
    extra_parts: []const udf_mod.TvfPartition,
    args: []const ?Value,
    per_range: bool,
) !void {
    const a = b.a;
    const kic: usize = if (ent.kernel_input_cols == 0) ent.input_schemas[0].len else ent.kernel_input_cols;
    const inputs = try tvfInputs(b, ent, kic);

    var is_pass = try a.alloc(bool, ent.output_schema.len);
    @memset(is_pass, false);
    for (ent.passthrough) |pp| is_pass[pp.out_idx] = true;

    var n_out: usize = 0;
    for (is_pass) |x| {
        if (!x) n_out += 1;
    }
    if (n_out == 0) return NoMatch;
    const out = try a.alloc(Column, n_out);
    var oi: usize = 0;
    for (ent.output_schema, is_pass) |col, pass| {
        if (pass) continue;
        out[oi] = .{ .name = try b.fb.canonName(col.name), .type = col.type, .nullable = true };
        oi += 1;
    }

    try b.flushPending();
    const base = b.fb.cols.items.len;
    if (per_range) {
        try b.ops.append(a, .{ .tvf_grouped = .{
            .spec = .{
                .process = ent.process,
                .user_data = ent.user_data,
                .args = try cloneArgs(b, args),
                .inputs = inputs,
                .extra_parts = extra_parts,
                .out = out,
            },
            .aligned_append = true,
        } });
    } else {
        try b.ops.append(a, .{ .tvf_aligned = .{
            .process = ent.process,
            .user_data = ent.user_data,
            .args = try cloneArgs(b, args),
            .inputs = inputs,
            .extra_parts = extra_parts,
            .out = out,
        } });
    }

    oi = 0;
    for (ent.output_schema, is_pass) |col, pass| {
        if (pass) continue;
        const idx = base + oi;
        try b.fb.cols.append(a, out[oi]);
        try b.fb.setVis(col.name, idx);
        oi += 1;
    }
    for (ent.passthrough) |pp| {
        if (pp.in_idx >= ent.input_schemas[0].len) return NoMatch;
        const src_name = ent.input_schemas[0][pp.in_idx].name;
        const src = b.fb.resolve(src_name) orelse return NoMatch;
        try b.fb.setVis(ent.output_schema[pp.out_idx].name, src.idx);
    }
}

fn cloneOrder(b: *Builder, specs: []const ir.SortSpec) ![]const region.OrderBy {
    const out = try b.a.alloc(region.OrderBy, specs.len);
    for (specs, out) |src, *dst| {
        dst.* = .{ .col = try b.resolveIdx(src.col), .desc = src.desc };
    }
    return out;
}

/// Every ROW_NUMBER call of a window node whose partition equals the range
/// keys becomes one ranks op.
fn pushRanksFromWindow(b: *Builder, w: *const ir.WindowOp, merge_on: ?usize) !void {
    for (w.calls) |call| {
        if (call.func != .row_number or call.args.len != 0) return NoMatch;
        const spec = w.specs[call.spec_idx];
        if (!try b.partitionMatchesRangeKeys(spec.partition_by)) return NoMatch;
        const order = try cloneOrder(b, spec.order_by);
        try b.flushPending();
        const idx = try b.fb.addCol(call.output_name, .bigint, false);
        try b.ops.append(b.a, .{ .ranks = .{
            .name = b.fb.cols.items[idx].name,
            .order = order,
            .merge_on = merge_on,
        } });
        try b.fb.setVis(call.output_name, idx);
    }
}

/// LAST_VALUE(col) with an unbounded-following frame over the range keys →
/// fill_last; a current-row frame is per-row identity (vis re-point only).
fn pushFillLast(b: *Builder, w: *const ir.WindowOp) !void {
    for (w.calls) |call| {
        if (call.func != .last_value or call.args.len != 1) return NoMatch;
        if (call.args[0] != .col_ref) return NoMatch;
        const spec = w.specs[call.spec_idx];
        if (!try b.partitionMatchesRangeKeys(spec.partition_by)) return NoMatch;
        try checkMonthOrder(b, spec.order_by);
        const src = try b.resolveIdx(call.args[0].col_ref);
        if (spec.frame.end != .unbounded_following) {
            try b.fb.setVis(call.output_name, src);
            continue;
        }
        try b.flushPending();
        const idx = try b.fb.addCol(call.output_name, b.fb.cols.items[src].type, true);
        try b.ops.append(b.a, .{ .fill_last = .{ .name = b.fb.cols.items[idx].name, .src = src } });
        try b.fb.setVis(call.output_name, idx);
    }
}

/// The keyed aggregation: group keys = range keys + int-family subkeys; the
/// region emits sub-groups in subkey-ascending order per range — exactly the
/// month order every downstream op requires.
fn pushGroupAgg(b: *Builder, g: *const ir.Op.GroupBy) !void {
    const a = b.a;
    const keys = try b.rangeKeyIdxs();
    var subkeys: std.ArrayListUnmanaged(usize) = .empty;
    var key_covered = [3]bool{ false, false, false };
    for (g.group_cols) |gc| {
        const idx = (b.fb.resolve(gc) orelse return NoMatch).idx;
        var is_range_key = false;
        for (keys, 0..) |k, i| {
            if (k == idx) {
                key_covered[i] = true;
                is_range_key = true;
                break;
            }
        }
        if (!is_range_key) try subkeys.append(a, idx);
    }
    if (!key_covered[0] or !key_covered[1] or !key_covered[2]) return NoMatch;
    if (subkeys.items.len != 1) return NoMatch; // rollforward shape: month only

    var out: std.ArrayListUnmanaged(region.AggOut) = .empty;
    var new_vis: std.ArrayListUnmanaged(VisEntry) = .empty;

    // Group keys first (constant within their sub-group → .first).
    for (g.group_cols) |gc| {
        const e = b.fb.resolve(gc) orelse return NoMatch;
        try out.append(a, .{ .name = try nameFor(b, gc), .kind = .{ .first = e.idx } });
        try new_vis.append(a, .{ .name = try a.dupe(u8, gc), .idx = new_vis.items.len });
    }
    for (g.aggs) |spec| {
        const kind: @FieldType(region.AggOut, "kind") = switch (spec.func) {
            .any_value => .{ .first = try b.resolveIdx(spec.col orelse return NoMatch) },
            .max_by => .{ .max_by = .{
                .val = try b.resolveIdx(spec.col orelse return NoMatch),
                .ord = try b.resolveIdx(spec.arg2_col orelse return NoMatch),
            } },
            .max => .{ .max_int = try intFamilyIdx(b, spec.col orelse return NoMatch) },
            .min => .{ .min_int = try intFamilyIdx(b, spec.col orelse return NoMatch) },
            .sum => blk: {
                const idx = try b.resolveIdx(spec.col orelse return NoMatch);
                const t = b.fb.cols.items[idx].type;
                break :blk switch (t) {
                    .tinyint, .smallint, .int, .bigint, .date, .datetime => .{ .sum_int = idx },
                    .float, .double => .{ .sum_float = idx },
                    else => return NoMatch,
                };
            },
            else => return NoMatch,
        };
        try out.append(a, .{ .name = try nameFor(b, spec.as), .kind = kind });
        try new_vis.append(a, .{ .name = try a.dupe(u8, spec.as), .idx = new_vis.items.len });
    }

    try b.flushPending();
    try b.ops.append(a, .{ .group_agg = .{
        .subkeys = try a.dupe(usize, subkeys.items),
        .out = try a.dupe(region.AggOut, out.items),
    } });

    // Frame replaced by the aggregation output. Types are re-derived by
    // Program.build; our copy mirrors names only (placeholder types for
    // .first/.max_by columns whose sources we know).
    const in_cols = b.fb.cols.items;
    var new_cols: std.ArrayListUnmanaged(Column) = .empty;
    for (out.items) |o| {
        const t: types.Type = switch (o.kind) {
            .first => |c| in_cols[c].type,
            .max_by => |mb| in_cols[mb.val].type,
            .min_int, .max_int => |c| in_cols[c].type,
            .sum_int => .bigint,
            .sum_float => .double,
        };
        try new_cols.append(a, .{ .name = o.name, .type = t, .nullable = true });
    }
    b.fb.cols = new_cols;
    b.fb.vis = new_vis;
}

fn nameFor(b: *Builder, hint: []const u8) ![]const u8 {
    return b.fb.canonName(hint);
}

fn intFamilyIdx(b: *Builder, name: []const u8) !usize {
    const idx = try b.resolveIdx(name);
    return switch (b.fb.cols.items[idx].type) {
        .tinyint, .smallint, .int, .bigint, .date, .datetime => idx,
        else => NoMatch,
    };
}

/// Downstream per-range order requirement: single ascending key that is the
/// aggregation's month subkey (the region emits sub-groups month-ascending,
/// so the order already holds — this just verifies the SQL asked for it).
fn checkMonthOrder(b: *Builder, specs: []const ir.SortSpec) !void {
    if (specs.len != 1 or specs[0].desc) return NoMatch;
    _ = try b.resolveIdx(specs[0].col);
}

fn kernelReadsAll(ent: *const udf_mod.TableEntry) bool {
    return ent.kernel_input_cols == 0 or ent.kernel_input_cols == ent.input_schemas[0].len;
}

fn rightAliasName(op: *const ir.Op) ![]const u8 {
    var cur = op;
    var depth: usize = 0;
    while (depth < 8) : (depth += 1) {
        switch (cur.*) {
            .alias => |al| return al.alias,
            .compute => |c| cur = c.upstream,
            .select => |p| cur = p.upstream,
            else => return NoMatch,
        }
    }
    return NoMatch;
}

fn joinKeyTouchesAlias(j: *const ir.Op.Join, alias: []const u8) bool {
    for (j.on) |pair| {
        if (std.mem.indexOfScalar(u8, pair.left, '.')) |d| {
            if (std.ascii.eqlIgnoreCase(pair.left[0..d], alias)) return true;
        }
    }
    return false;
}
