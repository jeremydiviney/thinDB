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
        .materialize => {
            const stage = map.get(op) orelse return error.UnsupportedQueryShape;
            return mat_stage.MatScan.create(input.allocator, stage);
        },
        .alias => |a| {
            var up = try buildGenericBlock(input, a.upstream, map, block_root);
            errdefer up.deinit();
            return exec.AliasRename.create(input.allocator, up, a.alias);
        },
        .filter => |f| {
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
            const spec: ir.JoinSpec = .{
                .join_type = j.join_type,
                .algorithm = j.algorithm,
                .on = j.on,
                .ranges = j.ranges,
                .extra_predicate = j.extra_predicate,
                .skew_ratio_threshold = j.skew_ratio_threshold,
                .skew_absolute_threshold = j.skew_absolute_threshold,
                .skew_sample_interval = j.skew_sample_interval,
            };
            return left.join(right, spec);
        },
        else => return error.UnsupportedQueryShape,
    }
}
