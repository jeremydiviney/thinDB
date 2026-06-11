//! Staged compilation for plans with materialize boundaries (CTEs and
//! FROM-clause subqueries) on the V2 path.
//!
//! Every boundary materializes (see exec/mat_stage.zig for the execution
//! model). Compilation walks the IR's materialize nodes bottom-up: each
//! distinct node becomes a Stage whose body compiles as its own query block —
//! table-sourced blocks run the regular V2 handlers at full parallelism;
//! blocks reading an upstream stage build a generic operator pipeline over a
//! MatScan leaf. The root block compiles last and is wrapped in a StagedRoot
//! that owns the stage set.
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

const StageMap = std.AutoHashMapUnmanaged(*const ir.Op, *mat_stage.Stage);

/// True when the plan contains at least one materialize boundary anywhere.
pub fn containsMaterialize(op: *const ir.Op) bool {
    return switch (op.*) {
        .materialize => true,
        .select => |p| containsMaterialize(p.upstream),
        .exclude => |p| containsMaterialize(p.upstream),
        .filter => |f| containsMaterialize(f.upstream),
        .order_by => |o| containsMaterialize(o.upstream),
        .group_by => |g| containsMaterialize(g.upstream),
        .compute => |c| containsMaterialize(c.upstream),
        .alias => |a| containsMaterialize(a.upstream),
        .limit => |l| containsMaterialize(l.upstream),
        .window => |w| containsMaterialize(w.upstream),
        .join => |j| containsMaterialize(j.left) or containsMaterialize(j.right),
        .set_union => |u| containsMaterialize(u.left) or containsMaterialize(u.right),
        else => false,
    };
}

pub fn compileStaged(input: engine_v2.CompileInput, root: *const ir.Op) anyerror!exec.Query {
    const set = try mat_stage.StageSet.create(input.allocator);
    errdefer set.deinit();
    var map: StageMap = .empty;
    defer map.deinit(input.allocator);
    try collectStages(input, root, set, &map);
    const inner = try compileBlock(input, root, &map);
    return mat_stage.StagedRoot.create(input.allocator, inner, set);
}

/// Post-order walk: a stage's own upstream stages exist (and are compiled)
/// before the stage's body compiles, so its MatScan leaves can bind.
fn collectStages(
    input: engine_v2.CompileInput,
    op: *const ir.Op,
    set: *mat_stage.StageSet,
    map: *StageMap,
) anyerror!void {
    switch (op.*) {
        .materialize => |m| {
            if (map.contains(op)) return; // shared CTE: one stage, many readers
            try collectStages(input, m.upstream, set, map);
            const q = try compileBlock(input, m.upstream, map);
            const stage = try set.addStage(q);
            try map.put(input.allocator, op, stage);
        },
        .select => |p| try collectStages(input, p.upstream, set, map),
        .exclude => |p| try collectStages(input, p.upstream, set, map),
        .filter => |f| try collectStages(input, f.upstream, set, map),
        .order_by => |o| try collectStages(input, o.upstream, set, map),
        .group_by => |g| try collectStages(input, g.upstream, set, map),
        .compute => |c| try collectStages(input, c.upstream, set, map),
        .alias => |a| try collectStages(input, a.upstream, set, map),
        .limit => |l| try collectStages(input, l.upstream, set, map),
        .window => |w| try collectStages(input, w.upstream, set, map),
        .join => |j| {
            try collectStages(input, j.left, set, map);
            try collectStages(input, j.right, set, map);
        },
        .set_union => |u| {
            try collectStages(input, u.left, set, map);
            try collectStages(input, u.right, set, map);
        },
        else => {},
    }
}

const BlockSource = enum { table, mat, unsupported };

/// A query block is a linear pipeline; its source is whatever the upstream
/// chain bottoms out at.
fn blockSource(op: *const ir.Op) BlockSource {
    var cur = op;
    while (true) {
        switch (cur.*) {
            .scan, .file_scan, .single_row => return .table,
            .materialize => return .mat,
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
        // Stage-backed block: generic operators over the MatScan leaf. The
        // heavy input was already produced by the upstream stage's handler;
        // this pipeline reads (typically small) in-memory chunks.
        .mat => buildMatBlock(input, op, map),
        .unsupported => error.UnsupportedQueryShape,
    };
}

fn buildMatBlock(input: engine_v2.CompileInput, op: *const ir.Op, map: *StageMap) anyerror!exec.Query {
    switch (op.*) {
        .materialize => {
            const stage = map.get(op) orelse return error.UnsupportedQueryShape;
            return mat_stage.MatScan.create(input.allocator, stage);
        },
        .alias => |a| {
            var up = try buildMatBlock(input, a.upstream, map);
            errdefer up.deinit();
            return exec.AliasRename.create(input.allocator, up, a.alias);
        },
        .filter => |f| {
            var up = try buildMatBlock(input, f.upstream, map);
            errdefer up.deinit();
            return up.filter(f.predicate);
        },
        .select => |s| {
            var up = try buildMatBlock(input, s.upstream, map);
            errdefer up.deinit();
            return local.compileSelectProject(input.allocator, up, s);
        },
        .exclude => |e| {
            var up = try buildMatBlock(input, e.upstream, map);
            errdefer up.deinit();
            const remaining = try local.complementColumns(input.allocator, up.outputSchema(), e.columns);
            defer input.allocator.free(remaining);
            return up.project(remaining);
        },
        .compute => |c| {
            var up = try buildMatBlock(input, c.upstream, map);
            errdefer up.deinit();
            return up.compute(c.derived);
        },
        .order_by => |o| {
            var up = try buildMatBlock(input, o.upstream, map);
            errdefer up.deinit();
            return up.orderBy(o.specs);
        },
        .limit => |l| {
            var up = try buildMatBlock(input, l.upstream, map);
            errdefer up.deinit();
            return up.limitOffset(@intCast(l.n), @intCast(l.offset));
        },
        .group_by => |g| {
            for (g.aggs) |a| if (a.func == .udf) return error.UnsupportedQueryShape;
            var up = try buildMatBlock(input, g.upstream, map);
            errdefer up.deinit();
            return up.groupByTopK(g.group_cols, g.aggs, g.top_k, g.emit_limit);
        },
        else => return error.UnsupportedQueryShape,
    }
}
