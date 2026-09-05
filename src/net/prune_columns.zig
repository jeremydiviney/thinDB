//! Pre-execution dead-column elimination.
//!
//! Generated CTE stacks re-list every column at every level, so a column can
//! ride 3M+ rows through ten materialized stages after its last real use
//! (a production rollforward workload carries seven dead string columns through the
//! whole window tail). The flat projection analysis (`analyzeProjection`)
//! can't catch this: each CTE's own select list "references" the column and
//! keeps it alive forever.
//!
//! This pass propagates a NEEDED set of output names top-down from the root:
//! a select item, window call, compute derived, or aggregate whose output
//! nothing above consumes is deleted. A window whose every call dies is
//! unlinked entirely. Everything is conservative: any shape the walk doesn't
//! fully understand (star projections mixed with items, `exclude`,
//! unresolved subquery markers, table functions) degrades to keep-all for
//! that subtree — never a wrong drop.
//!
//! Shared CTEs (`materialize` reached from several consumers) accumulate the
//! UNION of their consumers' needs, iterated to fixpoint before any rewrite.
//! UNION ALL is positional: its arms are narrowed only when both end in
//! star-free selects with identical output-name sequences (then the same
//! keep-set preserves alignment); both-materialize arms additionally have
//! their need-sets equalized so an outside consumer of one arm can't skew it.
//!
//! Like the other plan rewrites (const_fold, predicate_pushdown) this runs
//! once pre-compile and mutates the tree in place — allowed, DESIGN.md
//! forbids RUNTIME optimization only.

const std = @import("std");
const Allocator = std.mem.Allocator;
const builtin = @import("builtin");

const ir = @import("../ir/ir.zig");
const types = @import("../types.zig");
const PredicateExpr = @import("../exec/predicate.zig").PredicateExpr;

fn trace() bool {
    if (builtin.is_test) return false;
    return std.c.getenv("THINDB_TRACE_PRUNE") != null;
}

fn lastSegment(name: []const u8) []const u8 {
    if (std.mem.lastIndexOfScalar(u8, name, '.')) |dot| return name[dot + 1 ..];
    return name;
}

fn lower(arena: Allocator, s: []const u8) ?[]const u8 {
    const buf = arena.alloc(u8, s.len) catch return null;
    return std.ascii.lowerString(buf, s);
}

/// Case-insensitive name set with qualified-name tolerance: `p.amount` and
/// `amount` match each other in both directions (over-matching only ever
/// keeps more).
const NameSet = struct {
    map: std.StringHashMapUnmanaged(void) = .empty,

    fn add(self: *NameSet, arena: Allocator, name: []const u8) bool {
        var grew = false;
        grew = self.addOne(arena, name) or grew;
        const seg = lastSegment(name);
        if (seg.ptr != name.ptr or seg.len != name.len) grew = self.addOne(arena, seg) or grew;
        return grew;
    }

    fn addOne(self: *NameSet, arena: Allocator, name: []const u8) bool {
        const lc = lower(arena, name) orelse return false;
        const gop = self.map.getOrPut(arena, lc) catch return false;
        return !gop.found_existing;
    }

    fn has(self: *const NameSet, arena: Allocator, name: []const u8) bool {
        const lc = lower(arena, name) orelse return true; // OOM: pretend needed
        if (self.map.contains(lc)) return true;
        const seg = lastSegment(name);
        if (seg.ptr == name.ptr and seg.len == name.len) return false;
        const lcs = lower(arena, seg) orelse return true;
        return self.map.contains(lcs);
    }

    fn addAll(self: *NameSet, arena: Allocator, other: *const NameSet) bool {
        var grew = false;
        var it = other.map.keyIterator();
        while (it.next()) |k| {
            const gop = self.map.getOrPut(arena, k.*) catch return grew;
            if (!gop.found_existing) grew = true;
        }
        return grew;
    }
};

const MatNeed = struct {
    set: NameSet = .{},
    all: bool = false,
};

const Mode = enum { collect, mutate };

const Ctx = struct {
    arena: Allocator,
    mats: std.AutoHashMapUnmanaged(*const ir.Op, *MatNeed) = .empty,
    /// Per-iteration guard: each materialize body walks once per pass so a
    /// diamond of shared CTEs stays linear (late-added names propagate on
    /// the next fixpoint iteration instead).
    visited: std.AutoHashMapUnmanaged(*const ir.Op, void) = .empty,
    changed: bool = false,
    mode: Mode = .collect,
    /// Trace only: why the current needed==null flow originated.
    null_reason: []const u8 = "root",

    fn matNeed(self: *Ctx, op: *const ir.Op) ?*MatNeed {
        const gop = self.mats.getOrPut(self.arena, op) catch return null;
        if (!gop.found_existing) {
            const mn = self.arena.create(MatNeed) catch return null;
            mn.* = .{};
            gop.value_ptr.* = mn;
            self.changed = true;
        }
        return gop.value_ptr.*;
    }
};

/// Entry point. `arena` must outlive the compiled plan (the per-query node
/// arena). On any allocation failure the pass silently degrades — the tree
/// stays correct, just less narrowed.
pub fn pruneDeadColumns(arena: Allocator, root: *ir.Op) void {
    var ctx = Ctx{ .arena = arena };
    // Fixpoint: shared-CTE need sets grow monotonically. The cap covers
    // any realistic CTE nesting depth; on non-convergence every mat body
    // falls back to keep-all (walk once more with mats forced wide).
    var iter: usize = 0;
    while (iter < 12) : (iter += 1) {
        ctx.changed = false;
        ctx.visited.clearRetainingCapacity();
        walkRoot(&ctx, root);
        if (!ctx.changed) break;
    }
    if (iter == 12) {
        var it = ctx.mats.valueIterator();
        while (it.next()) |mn| mn.*.all = true;
    }
    ctx.mode = .mutate;
    ctx.visited.clearRetainingCapacity();
    walkRoot(&ctx, root);
}

fn walkRoot(ctx: *Ctx, op: *ir.Op) void {
    switch (op.*) {
        .explain => |e| walkRoot(ctx, @constCast(e.inner)),
        .create_table_as => |c| walk(ctx, @constCast(c.source), null),
        .insert_select => |i| walk(ctx, @constCast(i.source), null),
        .batch => |b| for (b.statements) |s| walkRoot(ctx, @constCast(s)),
        .ddl, .show, .insert, .copy, .set_var, .delete_op, .update_op => {},
        else => walk(ctx, op, null),
    }
}

/// `needed == null` means every output of `op` is (or must be treated as)
/// required. Shaper ops (star-free select, group_by) reset precision: their
/// upstream need is derivable from their own item lists regardless.
fn walk(ctx: *Ctx, op: *ir.Op, needed: ?*const NameSet) void {
    switch (op.*) {
        .scan, .single_row, .file_scan, .ddl, .show, .insert, .copy, .set_var, .delete_op, .update_op => {},
        .explain, .create_table_as, .insert_select, .batch => walkRoot(ctx, op),
        .limit => |l| walk(ctx, l.upstream, needed),
        .alias => |a| walk(ctx, a.upstream, needed),
        .exclude => |p| {
            // Anti-projection: output = upstream minus the listed names.
            // Upstream needs whatever the consumers need, plus the listed
            // names themselves so the drop has something to drop.
            if (needed == null) {
                walk(ctx, p.upstream, null);
                return;
            }
            var child = NameSet{};
            _ = child.addAll(ctx.arena, needed.?);
            for (p.columns) |nm| _ = child.add(ctx.arena, nm);
            walk(ctx, p.upstream, &child);
        },
        .table_fn => |t| {
            ctx.null_reason = "table_fn";
            for (t.inputs) |inp| walk(ctx, @constCast(inp), null);
        },

        .select => |*p| {
            // Pure star = identity projection: the need flows through
            // untouched (this is what lets union arm CTEs behind
            // `SELECT * FROM arm` narrow).
            if (p.columns.len == 1 and std.mem.eql(u8, p.columns[0], "*")) {
                walk(ctx, p.upstream, needed);
                return;
            }
            for (p.columns) |nm| {
                if (std.mem.eql(u8, nm, "*") or isAliasStar(nm)) {
                    // Mixed star + items: expansion order semantics are the
                    // compiler's business — keep everything below.
                    ctx.null_reason = "select-star-mix";
                    if (trace()) std.debug.print("[prune] bail select: mixed star\n", .{});
                    walk(ctx, p.upstream, null);
                    return;
                }
            }
            // Star-free: the trailing-skip count only shapes `*` expansion,
            // and it describes an upstream this pass may shrink (dead derived
            // and window columns), so it must not survive as a bound on the
            // narrowed schema.
            if (ctx.mode == .mutate) p.star_skip_trailing = 0;
            if (p.replace_on_collision != null) {
                // The parser marks every expression/renamed item replace-
                // capable, but replacement only ever fires when two items
                // share a final output name. Unique names (the ubiquitous
                // case) make the policy inert — narrowing stays safe.
                var seen = NameSet{};
                for (p.columns, 0..) |src, i| {
                    const out = if (p.outputs) |outs| (outs[i] orelse src) else src;
                    const final = lastSegment(out);
                    if (seen.has(ctx.arena, final)) {
                        ctx.null_reason = "select-dup-output";
                        if (trace()) std.debug.print("[prune] bail select: duplicate output '{s}'\n", .{final});
                        walk(ctx, p.upstream, null);
                        return;
                    }
                    _ = seen.add(ctx.arena, final);
                }
            }
            var child = NameSet{};
            var keep = std.ArrayListUnmanaged(bool).initCapacity(ctx.arena, p.columns.len) catch {
                walk(ctx, p.upstream, null);
                return;
            };
            var kept: usize = 0;
            for (p.columns, 0..) |src, i| {
                const out = if (p.outputs) |outs| (outs[i] orelse src) else src;
                const k = needed == null or needed.?.has(ctx.arena, out);
                keep.appendAssumeCapacity(k);
                if (k) {
                    kept += 1;
                    _ = child.add(ctx.arena, src);
                }
            }
            if (kept == 0) {
                keep.items[0] = true;
                _ = child.add(ctx.arena, p.columns[0]);
                kept = 1;
            }
            if (ctx.mode == .mutate and trace()) {
                std.debug.print("[prune] select {d}/{d} kept; first='{s}'", .{ kept, p.columns.len, p.columns[0] });
                if (kept < p.columns.len) {
                    std.debug.print(" dropped:", .{});
                    for (p.columns, 0..) |src, i| {
                        if (!keep.items[i]) std.debug.print(" {s}", .{lastSegment(src)});
                    }
                }
                std.debug.print("\n", .{});
            }
            if (ctx.mode == .mutate and kept < p.columns.len) {
                const cols = ctx.arena.alloc([]const u8, kept) catch {
                    walk(ctx, p.upstream, &child);
                    return;
                };
                const outs: ?[]?[]const u8 = if (p.outputs != null)
                    (ctx.arena.alloc(?[]const u8, kept) catch null)
                else
                    null;
                if (p.outputs != null and outs == null) {
                    walk(ctx, p.upstream, &child);
                    return;
                }
                const roc: ?[]bool = if (p.replace_on_collision != null)
                    (ctx.arena.alloc(bool, kept) catch null)
                else
                    null;
                if (p.replace_on_collision != null and roc == null) {
                    walk(ctx, p.upstream, &child);
                    return;
                }
                var w: usize = 0;
                for (p.columns, 0..) |src, i| {
                    if (!keep.items[i]) continue;
                    cols[w] = src;
                    if (outs) |o| o[w] = p.outputs.?[i];
                    if (roc) |r| r[w] = p.replace_on_collision.?[i];
                    w += 1;
                }
                p.columns = cols;
                if (outs) |o| p.outputs = o;
                if (roc) |r| p.replace_on_collision = r;
            }
            walk(ctx, p.upstream, &child);
        },

        .filter => |f| {
            if (needed == null) {
                walk(ctx, f.upstream, null);
                return;
            }
            var child = NameSet{};
            _ = child.addAll(ctx.arena, needed.?);
            if (!collectPredicate(ctx.arena, f.predicate, &child)) {
                ctx.null_reason = "filter-pred";
                if (trace()) std.debug.print("[prune] bail filter: opaque predicate\n", .{});
                walk(ctx, f.upstream, null);
                return;
            }
            walk(ctx, f.upstream, &child);
        },

        .order_by => |o| {
            if (needed == null) {
                walk(ctx, o.upstream, null);
                return;
            }
            var child = NameSet{};
            _ = child.addAll(ctx.arena, needed.?);
            for (o.specs) |sp| _ = child.add(ctx.arena, sp.col);
            walk(ctx, o.upstream, &child);
        },

        .group_by => |*g| {
            // Shaper: upstream need derives from kept keys + agg args even
            // with an unknown parent need.
            var child = NameSet{};
            for (g.group_cols) |nm| _ = child.add(ctx.arena, nm);
            var keep = std.ArrayListUnmanaged(bool).initCapacity(ctx.arena, g.aggs.len) catch {
                walk(ctx, g.upstream, null);
                return;
            };
            var kept: usize = 0;
            for (g.aggs) |a| {
                const k = needed == null or g.top_k != null or needed.?.has(ctx.arena, a.as);
                keep.appendAssumeCapacity(k);
                if (!k) continue;
                kept += 1;
                if (a.col) |c| _ = child.add(ctx.arena, c);
                if (a.arg2_col) |c| _ = child.add(ctx.arena, c);
                for (a.udf_arg_cols) |c| _ = child.add(ctx.arena, c);
            }
            // The grouped engines want at least one aggregate (DISTINCT
            // lowers with a hidden COUNT(*)); never drop down to zero.
            if (kept == 0 and g.aggs.len > 0) {
                keep.items[0] = true;
                const a = g.aggs[0];
                if (a.col) |c| _ = child.add(ctx.arena, c);
                if (a.arg2_col) |c| _ = child.add(ctx.arena, c);
                for (a.udf_arg_cols) |c| _ = child.add(ctx.arena, c);
                kept = 1;
            }
            if (ctx.mode == .mutate and kept < g.aggs.len) {
                if (ctx.arena.alloc(ir.AggSpec, kept)) |aggs| {
                    var w: usize = 0;
                    for (g.aggs, 0..) |a, i| {
                        if (!keep.items[i]) continue;
                        aggs[w] = a;
                        w += 1;
                    }
                    g.aggs = aggs;
                } else |_| {}
            }
            walk(ctx, g.upstream, &child);
        },

        .compute => |*c| {
            if (needed == null) {
                walk(ctx, c.upstream, null);
                return;
            }
            var child = NameSet{};
            _ = child.addAll(ctx.arena, needed.?);
            var keep = std.ArrayListUnmanaged(bool).initCapacity(ctx.arena, c.derived.len) catch {
                walk(ctx, c.upstream, null);
                return;
            };
            keep.appendNTimesAssumeCapacity(false, c.derived.len);
            var kept: usize = 0;
            // Right-to-left: a later derived may reference an earlier
            // sibling, so a kept item revives the siblings it reads.
            var i: usize = c.derived.len;
            while (i > 0) {
                i -= 1;
                const d = c.derived[i];
                if (!child.has(ctx.arena, d.name)) continue;
                keep.items[i] = true;
                kept += 1;
                if (!collectExpr(ctx.arena, d.expr, &child)) {
                    ctx.null_reason = "compute-expr";
                    if (trace()) std.debug.print("[prune] bail compute: opaque expr in '{s}'\n", .{d.name});
                    walk(ctx, c.upstream, null);
                    return;
                }
            }
            if (ctx.mode == .mutate and kept < c.derived.len) {
                if (kept == 0) {
                    // Every derived is dead: splice the node out entirely
                    // (same as the all-calls-dead window case) — an empty
                    // Compute is not a legal operator (ComputeNoColumns).
                    if (trace()) std.debug.print("[prune] compute spliced out (all derived dead)\n", .{});
                    op.* = c.upstream.*;
                    walk(ctx, op, needed);
                    return;
                }
                if (ctx.arena.alloc(ir.Derived, kept)) |ds| {
                    var w: usize = 0;
                    for (c.derived, 0..) |d, j| {
                        if (!keep.items[j]) continue;
                        ds[w] = d;
                        w += 1;
                    }
                    c.derived = ds;
                } else |_| {}
            }
            walk(ctx, c.upstream, &child);
        },

        .window => |*win| {
            if (needed == null) {
                walk(ctx, win.upstream, null);
                return;
            }
            var child = NameSet{};
            _ = child.addAll(ctx.arena, needed.?);
            var keep = std.ArrayListUnmanaged(bool).initCapacity(ctx.arena, win.calls.len) catch {
                walk(ctx, win.upstream, null);
                return;
            };
            var kept: usize = 0;
            for (win.calls) |call| {
                const k = needed.?.has(ctx.arena, call.output_name);
                keep.appendAssumeCapacity(k);
                if (!k) continue;
                kept += 1;
                const sp = win.specs[call.spec_idx];
                for (sp.partition_by) |nm| _ = child.add(ctx.arena, nm);
                for (sp.order_by) |so| _ = child.add(ctx.arena, so.col);
                for (call.args) |a| {
                    if (!collectExpr(ctx.arena, a, &child)) {
                        ctx.null_reason = "window-arg";
                        if (trace()) std.debug.print("[prune] bail window: opaque arg in '{s}'\n", .{call.output_name});
                        walk(ctx, win.upstream, null);
                        return;
                    }
                }
            }
            if (ctx.mode == .mutate and trace()) std.debug.print("[prune] window {d}/{d} calls kept\n", .{ kept, win.calls.len });
            if (ctx.mode == .mutate and kept == 0) {
                // Every call is dead: unlink the whole window (its sorts,
                // its buffer, its stage barrier).
                const up = win.upstream;
                walk(ctx, up, &child);
                op.* = up.*;
                return;
            }
            if (ctx.mode == .mutate and kept < win.calls.len) {
                narrowWindow(ctx.arena, win, keep.items, kept);
            }
            walk(ctx, win.upstream, &child);
        },

        .join => |j| {
            if (needed == null) {
                walk(ctx, j.left, null);
                walk(ctx, j.right, null);
                return;
            }
            var child = NameSet{};
            _ = child.addAll(ctx.arena, needed.?);
            for (j.on) |kp| {
                _ = child.add(ctx.arena, kp.left);
                _ = child.add(ctx.arena, kp.right);
            }
            for (j.ranges) |rp| {
                _ = child.add(ctx.arena, rp.left);
                _ = child.add(ctx.arena, rp.right);
            }
            var ok = true;
            if (j.extra_predicate) |p| ok = collectPredicate(ctx.arena, p, &child);
            if (!ok) ctx.null_reason = "join-pred";
            if (!ok and trace()) std.debug.print("[prune] bail join: opaque extra predicate\n", .{});
            walk(ctx, j.left, if (ok) &child else null);
            walk(ctx, j.right, if (ok) &child else null);
        },

        .set_union => |u| {
            var child: ?*const NameSet = needed;
            if (needed != null) {
                const ln = armSelectNames(ctx.arena, u.left);
                const rn = armSelectNames(ctx.arena, u.right);
                const lmat = armMat(u.left);
                const rmat = armMat(u.right);
                // Positional semantics: narrowing is safe only when both
                // arms provably keep identical column subsets. Same output
                // sequence + same needed set gives that — unless exactly
                // one arm sits on a shared CTE whose other consumers could
                // widen it unilaterally.
                var aligned = ln != null and rn != null and sameNameSeq(ln.?, rn.?) and
                    ((lmat == null) == (rmat == null));
                if (aligned and lmat != null and ctx.mode == .collect) {
                    // Equalize the two arm CTEs' need sets so a third
                    // consumer of one arm can't skew the positional match.
                    const lm = ctx.matNeed(lmat.?);
                    const rm = ctx.matNeed(rmat.?);
                    if (lm != null and rm != null) {
                        if (lm.?.all != rm.?.all) {
                            lm.?.all = true;
                            rm.?.all = true;
                            ctx.changed = true;
                        }
                        if (lm.?.set.addAll(ctx.arena, &rm.?.set)) ctx.changed = true;
                        if (rm.?.set.addAll(ctx.arena, &lm.?.set)) ctx.changed = true;
                    } else aligned = false;
                }
                if (!aligned) {
                    child = null;
                    ctx.null_reason = "union-unaligned";
                    if (trace() and ctx.mode == .collect) {
                        std.debug.print(
                            "[prune] bail union: L={s} R={s} ln={} rn={} lmat={} rmat={}\n",
                            .{ @tagName(u.left.*), @tagName(u.right.*), ln != null, rn != null, lmat != null, rmat != null },
                        );
                        if (ln != null and rn != null) {
                            const n = @min(ln.?.len, rn.?.len);
                            if (ln.?.len != rn.?.len) std.debug.print("[prune]   len {d} vs {d}\n", .{ ln.?.len, rn.?.len });
                            for (0..n) |i| {
                                if (!types.columnNameEql(lastSegment(ln.?[i]), lastSegment(rn.?[i])))
                                    std.debug.print("[prune]   @{d}: '{s}' vs '{s}'\n", .{ i, ln.?[i], rn.?[i] });
                            }
                        }
                    }
                }
            }
            walk(ctx, u.left, child);
            walk(ctx, u.right, child);
        },

        .materialize => |m| {
            const mn = ctx.matNeed(op) orelse {
                walk(ctx, m.upstream, null);
                return;
            };
            if (needed == null) {
                if (!mn.all) {
                    mn.all = true;
                    ctx.changed = true;
                    if (trace()) std.debug.print("[prune] mat marked ALL (reason={s})", .{ctx.null_reason});
                }
            } else if (!mn.all) {
                var it = needed.?.map.keyIterator();
                while (it.next()) |k| {
                    const gop = mn.set.map.getOrPut(ctx.arena, k.*) catch break;
                    if (!gop.found_existing) ctx.changed = true;
                }
            }
            // One body visit per pass: shared consumers accumulate first,
            // the fixpoint loop carries growth inward on the next round.
            if (ctx.visited.contains(op)) return;
            ctx.visited.put(ctx.arena, op, {}) catch return;
            if (ctx.mode == .mutate and trace()) std.debug.print(
                "[prune] mat body={s} all={} needed={d}\n",
                .{ @tagName(m.upstream.*), mn.all, mn.set.map.count() },
            );
            walk(ctx, m.upstream, if (mn.all) null else &mn.set);
        },
    }
}

fn isAliasStar(name: []const u8) bool {
    return name.len > 2 and name[name.len - 2] == '.' and name[name.len - 1] == '*';
}

fn narrowWindow(arena: Allocator, win: *ir.WindowOp, keep: []const bool, kept: usize) void {
    const calls = arena.alloc(ir.WindowCall, kept) catch return;
    // Compact the spec table to the ones kept calls reference.
    const spec_map = arena.alloc(?u32, win.specs.len) catch return;
    @memset(spec_map, null);
    var n_specs: u32 = 0;
    var w: usize = 0;
    for (win.calls, 0..) |call, i| {
        if (!keep[i]) continue;
        if (spec_map[call.spec_idx] == null) {
            spec_map[call.spec_idx] = n_specs;
            n_specs += 1;
        }
        calls[w] = call;
        calls[w].spec_idx = spec_map[call.spec_idx].?;
        w += 1;
    }
    const specs = arena.alloc(ir.WindowSpec, n_specs) catch return;
    for (spec_map, 0..) |maybe, old| {
        if (maybe) |new| specs[new] = win.specs[old];
    }
    win.calls = calls;
    win.specs = specs;
}

/// OUTPUT-name sequence of a union arm, reached through pass-through
/// plumbing (a materialize boundary, and per-arm Computes pushed down by
/// pushComputeThroughUnions — their derived append to the output). Null =
/// shape not understood (star-mixed select, group_by-rooted arm, ...) —
/// caller keeps everything.
fn armSelectNames(arena: Allocator, op: *const ir.Op) ?[]const []const u8 {
    switch (op.*) {
        .select => |p| {
            for (p.columns) |nm| {
                if (std.mem.eql(u8, nm, "*") or isAliasStar(nm)) {
                    // A pure star arm is a pass-through: look through it.
                    if (p.columns.len == 1) return armSelectNames(arena, p.upstream);
                    return null;
                }
            }
            if (p.outputs == null) return p.columns;
            const outs = arena.alloc([]const u8, p.columns.len) catch return null;
            for (p.columns, 0..) |src, i| outs[i] = p.outputs.?[i] orelse src;
            return outs;
        },
        .compute => |c| {
            const base = armSelectNames(arena, c.upstream) orelse return null;
            const outs = arena.alloc([]const u8, base.len + c.derived.len) catch return null;
            @memcpy(outs[0..base.len], base);
            for (c.derived, 0..) |d, i| outs[base.len + i] = d.name;
            return outs;
        },
        .alias => |a| return armSelectNames(arena, a.upstream),
        .limit => |l| return armSelectNames(arena, l.upstream),
        .filter => |f| return armSelectNames(arena, f.upstream),
        .order_by => |o| return armSelectNames(arena, o.upstream),
        .materialize => |m| return armSelectNames(arena, m.upstream),
        else => return null,
    }
}

/// The shared-CTE node a union arm bottoms out on (through the same
/// pass-through plumbing armSelectNames walks), or null for a self-contained
/// arm. Alignment of a mat-backed arm requires equalizing that CTE's need
/// set with its sibling arm's.
fn armMat(op: *const ir.Op) ?*const ir.Op {
    switch (op.*) {
        .materialize => return op,
        .select => |p| {
            if (p.columns.len == 1 and std.mem.eql(u8, p.columns[0], "*")) return armMat(p.upstream);
            return null; // an explicit select re-shapes: the mat below can widen freely
        },
        .compute => |c| return armMat(c.upstream),
        .alias => |a| return armMat(a.upstream),
        .limit => |l| return armMat(l.upstream),
        .filter => |f| return armMat(f.upstream),
        .order_by => |o| return armMat(o.upstream),
        else => return null,
    }
}

fn sameNameSeq(a: []const []const u8, b: []const []const u8) bool {
    if (a.len != b.len) return false;
    for (a, b) |x, y| {
        if (!types.columnNameEql(lastSegment(x), lastSegment(y))) return false;
    }
    return true;
}

/// Add every column the predicate reads. False = predicate contains a
/// marker this pass must not reason about (unresolved subquery / session
/// var) — caller degrades to keep-all.
fn collectPredicate(arena: Allocator, p: PredicateExpr, out: *NameSet) bool {
    switch (p) {
        .leaf => |lf| _ = out.add(arena, lf.col),
        .day_leaf => |lf| _ = out.add(arena, lf.col),
        .leaf_col_col => |lc| {
            _ = out.add(arena, lc.left);
            _ = out.add(arena, lc.right);
        },
        .is_null, .is_not_null => |col| _ = out.add(arena, col),
        .like => |lp| _ = out.add(arena, lp.col),
        .in_set => |s| _ = out.add(arena, s.col),
        .@"and", .@"or" => |children| for (children) |ch| {
            if (!collectPredicate(arena, ch, out)) return false;
        },
        .not => |child| return collectPredicate(arena, child.*, out),
        .always, .unknown => {},
        .correlated_set => |s| for (s.outer_cols) |nm| {
            _ = out.add(arena, nm);
        },
        .correlated_scalar => |s| {
            _ = out.add(arena, s.outer_compared);
            for (s.outer_keys) |nm| _ = out.add(arena, nm);
        },
        .correlated_range => |s| {
            _ = out.add(arena, s.outer_range_col);
            if (s.outer_range_col_upper) |upper| _ = out.add(arena, upper);
            for (s.outer_keys) |nm| _ = out.add(arena, nm);
        },
        // The variable / inner-query side is a VALUE; the compared column
        // is right here. (Scalar/exists markers name no column — bail.)
        .leaf_var => |v| _ = out.add(arena, v.col),
        .in_subquery => |s| _ = out.add(arena, s.col),
        .scalar_subquery, .exists_subquery => return false,
    }
    return true;
}

fn collectExpr(arena: Allocator, e: ir.Expr, out: *NameSet) bool {
    switch (e) {
        .col_ref => |nm| _ = out.add(arena, nm),
        .lit, .null_lit => {},
        .call => |call| for (call.args) |a| {
            if (!collectExpr(arena, a, out)) return false;
        },
        .case => |cs| {
            for (cs.branches) |br| {
                if (!collectPredicate(arena, br.cond, out)) return false;
                if (!collectExpr(arena, br.then, out)) return false;
            }
            if (cs.else_branch) |eb| return collectExpr(arena, eb.*, out);
        },
        .scalar_subquery, .exists_subquery, .var_ref => return false,
    }
    return true;
}

// ---------------------------------------------------------------------------

const testing = std.testing;

fn scanOp() ir.Op {
    return .{ .scan = .{ .table = .{ .name = "t" } } };
}

test "select items feeding nothing above are dropped; sources stay live" {
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    var base = scanOp();
    var inner = ir.Op{ .select = .{
        .columns = &.{ "a", "b", "c" },
        .upstream = &base,
    } };
    var outer = ir.Op{ .select = .{
        .columns = &.{"a"},
        .upstream = &inner,
    } };
    pruneDeadColumns(arena, &outer);
    try testing.expectEqual(@as(usize, 1), outer.select.columns.len);
    try testing.expectEqual(@as(usize, 1), inner.select.columns.len);
    try testing.expectEqualStrings("a", inner.select.columns[0]);
}

test "window with only dead calls is unlinked; live calls keep their spec cols" {
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    var base = scanOp();
    var inner = ir.Op{ .select = .{ .columns = &.{ "k", "v" }, .upstream = &base } };
    const specs = [_]ir.WindowSpec{.{
        .partition_by = &.{"k"},
        .order_by = &.{},
        .frame = ir.Frame.default_no_order,
    }};
    const calls = [_]ir.WindowCall{.{
        .spec_idx = 0,
        .func = .last_value,
        .args = &.{.{ .col_ref = "v" }},
        .ignore_nulls = false,
        .output_name = "lv",
    }};
    var win = ir.Op{ .window = .{ .specs = &specs, .calls = &calls, .upstream = &inner } };
    var outer = ir.Op{ .select = .{ .columns = &.{"k"}, .upstream = &win } };
    pruneDeadColumns(arena, &outer);
    // "lv" is dead -> the window disappears and the inner select narrows to k.
    try testing.expect(win == .select);
    try testing.expectEqual(@as(usize, 1), inner.select.columns.len);
    try testing.expectEqualStrings("k", inner.select.columns[0]);
}

test "live window call keeps its args and partition keys upstream" {
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    var base = scanOp();
    var inner = ir.Op{ .select = .{ .columns = &.{ "k", "v", "z" }, .upstream = &base } };
    const specs = [_]ir.WindowSpec{.{
        .partition_by = &.{"k"},
        .order_by = &.{},
        .frame = ir.Frame.default_no_order,
    }};
    const calls = [_]ir.WindowCall{.{
        .spec_idx = 0,
        .func = .last_value,
        .args = &.{.{ .col_ref = "v" }},
        .ignore_nulls = false,
        .output_name = "lv",
    }};
    var win = ir.Op{ .window = .{ .specs = &specs, .calls = &calls, .upstream = &inner } };
    var outer = ir.Op{ .select = .{ .columns = &.{"lv"}, .upstream = &win } };
    pruneDeadColumns(arena, &outer);
    try testing.expect(win == .window);
    // z is dead; k (partition) and v (arg) survive.
    try testing.expectEqual(@as(usize, 2), inner.select.columns.len);
    try testing.expectEqualStrings("k", inner.select.columns[0]);
    try testing.expectEqualStrings("v", inner.select.columns[1]);
}

test "group_by drops unread aggregates but never group keys or the last agg" {
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    var base = scanOp();
    var inner = ir.Op{ .select = .{ .columns = &.{ "k", "x", "y" }, .upstream = &base } };
    const aggs = [_]ir.AggSpec{
        .{ .func = .sum, .col = "x", .as = "sx" },
        .{ .func = .sum, .col = "y", .as = "sy" },
    };
    var g = ir.Op{ .group_by = .{ .group_cols = &.{"k"}, .aggs = &aggs, .upstream = &inner } };
    var outer = ir.Op{ .select = .{ .columns = &.{ "k", "sx" }, .upstream = &g } };
    pruneDeadColumns(arena, &outer);
    try testing.expectEqual(@as(usize, 1), g.group_by.aggs.len);
    try testing.expectEqualStrings("sx", g.group_by.aggs[0].as);
    // y fed only the dropped aggregate.
    try testing.expectEqual(@as(usize, 2), inner.select.columns.len);
    try testing.expectEqualStrings("k", inner.select.columns[0]);
    try testing.expectEqualStrings("x", inner.select.columns[1]);
}

test "shared materialize accumulates the union of its consumers' needs" {
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    var base = scanOp();
    var body = ir.Op{ .select = .{ .columns = &.{ "a", "b", "c" }, .upstream = &base } };
    var mat = ir.Op{ .materialize = .{ .upstream = &body } };
    var reader_a = ir.Op{ .select = .{ .columns = &.{"a"}, .upstream = &mat } };
    var reader_b = ir.Op{ .select = .{ .columns = &.{"b"}, .upstream = &mat } };
    var joined = ir.Op{ .join = .{
        .join_type = .inner,
        .algorithm = .auto,
        .on = &.{},
        .ranges = &.{},
        .extra_predicate = null,
        .skew_ratio_threshold = 0,
        .skew_absolute_threshold = 0,
        .skew_sample_interval = 1,
        .left = &reader_a,
        .right = &reader_b,
    } };
    var outer = ir.Op{ .select = .{ .columns = &.{ "a", "b" }, .upstream = &joined } };
    pruneDeadColumns(arena, &outer);
    // c dies; a AND b survive (union of both readers).
    try testing.expectEqual(@as(usize, 2), body.select.columns.len);
    try testing.expectEqualStrings("a", body.select.columns[0]);
    try testing.expectEqualStrings("b", body.select.columns[1]);
}

test "union arms with mismatched select sequences are left whole" {
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    var base_l = scanOp();
    var left = ir.Op{ .select = .{ .columns = &.{ "a", "b" }, .upstream = &base_l } };
    var base_r = scanOp();
    var right = ir.Op{ .select = .{ .columns = &.{ "b", "a" }, .upstream = &base_r } };
    var u = ir.Op{ .set_union = .{ .left = &left, .right = &right, .all = true } };
    var outer = ir.Op{ .select = .{ .columns = &.{"a"}, .upstream = &u } };
    pruneDeadColumns(arena, &outer);
    try testing.expectEqual(@as(usize, 2), left.select.columns.len);
    try testing.expectEqual(@as(usize, 2), right.select.columns.len);
}

test "aligned union arms narrow identically" {
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    var base_l = scanOp();
    var left = ir.Op{ .select = .{ .columns = &.{ "a", "b" }, .upstream = &base_l } };
    var base_r = scanOp();
    var right = ir.Op{ .select = .{ .columns = &.{ "a", "b" }, .upstream = &base_r } };
    var u = ir.Op{ .set_union = .{ .left = &left, .right = &right, .all = true } };
    var outer = ir.Op{ .select = .{ .columns = &.{"a"}, .upstream = &u } };
    pruneDeadColumns(arena, &outer);
    try testing.expectEqual(@as(usize, 1), left.select.columns.len);
    try testing.expectEqual(@as(usize, 1), right.select.columns.len);
    try testing.expectEqualStrings("a", left.select.columns[0]);
    try testing.expectEqualStrings("a", right.select.columns[0]);
}

test "compute keeps a derived that a kept sibling references" {
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    var base = scanOp();
    var inner = ir.Op{ .select = .{ .columns = &.{"x"}, .upstream = &base } };
    const derived = [_]ir.Derived{
        .{ .name = "d1", .expr = .{ .col_ref = "x" } },
        .{ .name = "dead", .expr = .{ .col_ref = "x" } },
        .{ .name = "d2", .expr = .{ .col_ref = "d1" } },
    };
    var cmp = ir.Op{ .compute = .{ .derived = &derived, .upstream = &inner } };
    var outer = ir.Op{ .select = .{ .columns = &.{"d2"}, .upstream = &cmp } };
    pruneDeadColumns(arena, &outer);
    try testing.expectEqual(@as(usize, 2), cmp.compute.derived.len);
    try testing.expectEqualStrings("d1", cmp.compute.derived[0].name);
    try testing.expectEqualStrings("d2", cmp.compute.derived[1].name);
}

test "compute with every derived dead is spliced out (never an empty Compute)" {
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    var base = scanOp();
    var inner = ir.Op{ .select = .{ .columns = &.{"x"}, .upstream = &base } };
    const derived = [_]ir.Derived{
        .{ .name = "dead1", .expr = .{ .col_ref = "x" } },
        .{ .name = "dead2", .expr = .{ .col_ref = "x" } },
    };
    var cmp = ir.Op{ .compute = .{ .derived = &derived, .upstream = &inner } };
    var outer = ir.Op{ .select = .{ .columns = &.{"x"}, .upstream = &cmp } };
    pruneDeadColumns(arena, &outer);
    // The node itself must have become the select below it — an empty
    // Compute is not a legal operator (ComputeNoColumns at compile).
    try testing.expect(cmp == .select);
    try testing.expectEqual(@as(usize, 1), cmp.select.columns.len);
    try testing.expectEqualStrings("x", cmp.select.columns[0]);
}

test "a narrowed star-free select drops its trailing star-skip count" {
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    var base = scanOp();
    const derived = [_]ir.Derived{
        .{ .name = "h", .expr = .{ .col_ref = "v" } },
        .{ .name = "h2", .expr = .{ .col_ref = "z" } },
    };
    var cmp = ir.Op{ .compute = .{ .derived = &derived, .upstream = &base } };
    const specs = [_]ir.WindowSpec{.{
        .partition_by = &.{"k"},
        .order_by = &.{},
        .frame = ir.Frame.default_no_order,
    }};
    const calls = [_]ir.WindowCall{.{
        .spec_idx = 0,
        .func = .row_number,
        .args = &.{},
        .ignore_nulls = false,
        .output_name = "rn",
    }};
    var win = ir.Op{ .window = .{ .specs = &specs, .calls = &calls, .upstream = &cmp } };
    // The parser counted the two derived + one window output as hidden
    // trailing columns; after pruning only `rn` (and the scan) remain.
    var inner = ir.Op{ .select = .{ .columns = &.{ "k", "h", "h2", "rn" }, .star_skip_trailing = 3, .upstream = &win } };
    var outer = ir.Op{ .select = .{ .columns = &.{"rn"}, .upstream = &inner } };
    pruneDeadColumns(arena, &outer);
    try testing.expectEqual(@as(usize, 1), inner.select.columns.len);
    try testing.expectEqual(@as(u32, 0), inner.select.star_skip_trailing);
    try testing.expect(cmp == .scan);
}
