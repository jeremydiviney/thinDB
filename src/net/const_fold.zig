//! Pre-execution dead-branch elimination.
//!
//! The parser already folds literal-vs-literal comparisons to
//! `PredicateExpr.always` (so `WHERE 'USD' != 'USD'` arrives as a
//! constant-false filter). This pass propagates that knowledge through the
//! plan shape:
//!
//!   - `Filter(always-true)` disappears (`WHERE 1=1` template padding).
//!   - A UNION ALL arm that provably yields zero rows is dropped, so the
//!     surviving arm compiles alone. Everything below the dead arm — joins,
//!     buffer reads, filter evaluation — never runs, and the staged
//!     compiler's reference counting (which runs on the rewritten tree)
//!     de-stages any CTE that was only shared with the dead arm.
//!
//! Emptiness is judged conservatively: only node kinds whose output is
//! *structurally* empty over an empty input propagate; anything unknown
//! (global aggregates, scans, …) stops the walk. Like the join filter
//! pushdown, this is a plan rewrite (allowed — DESIGN.md forbids RUNTIME
//! optimization), run once before any handler compiles the tree.

const std = @import("std");

const ir = @import("../ir/ir.zig");
const PredicateExpr = @import("../exec/predicate.zig").PredicateExpr;

/// Constant value of a predicate, if statically known. Conservative: any
/// node kind other than boolean combinators over `.always` is unknown.
fn predConst(pe: PredicateExpr) ?bool {
    switch (pe) {
        .always => |b| return b,
        .@"and" => |children| {
            var all_true = true;
            for (children) |c| {
                const v = predConst(c) orelse {
                    all_true = false;
                    continue;
                };
                if (!v) return false;
                // v == true contributes nothing; keep scanning.
            }
            return if (all_true) true else null;
        },
        .@"or" => |children| {
            var all_false = true;
            for (children) |c| {
                const v = predConst(c) orelse {
                    all_false = false;
                    continue;
                };
                if (v) return true;
            }
            return if (all_false) false else null;
        },
        .not => |child| {
            const v = predConst(child.*) orelse return null;
            return !v;
        },
        else => return null,
    }
}

/// True iff `op` PROVABLY yields zero rows. False means "unknown" — never
/// "known non-empty".
fn subtreeIsEmpty(op: *const ir.Op) bool {
    return switch (op.*) {
        .filter => |f| (predConst(f.predicate) orelse true) == false or subtreeIsEmpty(f.upstream),
        .select, .exclude => |p| subtreeIsEmpty(p.upstream),
        .compute => |c| subtreeIsEmpty(c.upstream),
        .alias => |a| subtreeIsEmpty(a.upstream),
        .window => |w| subtreeIsEmpty(w.upstream),
        .order_by => |o| subtreeIsEmpty(o.upstream),
        .materialize => |m| subtreeIsEmpty(m.upstream),
        .limit => |l| l.n == 0 or subtreeIsEmpty(l.upstream),
        // Grouped aggregate over empty input emits no groups. A GLOBAL
        // aggregate (no keys) emits exactly one row — never empty.
        .group_by => |g| g.group_cols.len > 0 and subtreeIsEmpty(g.upstream),
        .join => |j| switch (j.join_type) {
            .inner => subtreeIsEmpty(j.left) or subtreeIsEmpty(j.right),
            .left => subtreeIsEmpty(j.left),
            .right => subtreeIsEmpty(j.right),
            .full => subtreeIsEmpty(j.left) and subtreeIsEmpty(j.right),
        },
        .set_union => |u| subtreeIsEmpty(u.left) and subtreeIsEmpty(u.right),
        else => false,
    };
}

/// Rewrite the op tree in place: prune dead UNION ALL arms, drop
/// always-true filters.
pub fn foldDeadBranches(op: *ir.Op) void {
    // Bottom-up: children first, so a union arm that is itself a pruned
    // union is judged in final form.
    switch (op.*) {
        .scan, .single_row, .file_scan, .ddl, .show, .insert, .copy, .set_var, .delete_op, .update_op => {},
        .limit => |l| foldDeadBranches(l.upstream),
        .select, .exclude => |p| foldDeadBranches(p.upstream),
        .order_by => |o| foldDeadBranches(o.upstream),
        .group_by => |g| foldDeadBranches(g.upstream),
        .compute => |c| foldDeadBranches(c.upstream),
        .materialize => |m| foldDeadBranches(m.upstream),
        .table_fn => |t| foldDeadBranches(t.input),
        .window => |w| foldDeadBranches(w.upstream),
        .alias => |a| foldDeadBranches(a.upstream),
        .explain => |e| foldDeadBranches(@constCast(e.inner)),
        .create_table_as => |c| foldDeadBranches(@constCast(c.source)),
        .insert_select => |i| foldDeadBranches(@constCast(i.source)),
        .batch => |b| for (b.statements) |s| foldDeadBranches(@constCast(s)),
        .filter => |f| {
            foldDeadBranches(f.upstream);
            // WHERE TRUE is pure plumbing — unlink it. (Constant-FALSE stays:
            // subtreeIsEmpty reads it in place, and a filter that kills every
            // row is cheap to run when it isn't inside a prunable union arm.)
            if ((predConst(f.predicate) orelse false) == true) op.* = f.upstream.*;
        },
        .join => |j| {
            foldDeadBranches(j.left);
            foldDeadBranches(j.right);
        },
        .set_union => |u| {
            foldDeadBranches(u.left);
            foldDeadBranches(u.right);
            // UNION ALL only: dropping an arm of a future UNION DISTINCT
            // would skip the dedup of the surviving arm.
            if (!u.all) return;
            // Dropping the RIGHT arm is always output-name-safe (union
            // column names follow the left arm). Dropping the LEFT arm
            // hands naming to the right arm — safe in the overwhelming
            // CTE-stack case where both arms project identical aliases,
            // and both arms already had to be column-compatible for the
            // union to compile at all.
            if (subtreeIsEmpty(u.right) and !subtreeIsEmpty(u.left)) {
                op.* = u.left.*;
            } else if (subtreeIsEmpty(u.left) and !subtreeIsEmpty(u.right)) {
                op.* = u.right.*;
            }
        },
    }
}

// ---------------------------------------------------------------------------

const testing = std.testing;

fn scanOp() ir.Op {
    return .{ .scan = .{ .table = .{ .name = "t" } } };
}

fn testJoin(jt: ir.JoinType, left: *ir.Op, right: *ir.Op) ir.Op {
    return .{ .join = .{
        .join_type = jt,
        .algorithm = .auto,
        .on = &.{},
        .ranges = &.{},
        .extra_predicate = null,
        .skew_ratio_threshold = 0,
        .skew_absolute_threshold = 0,
        .skew_sample_interval = 1,
        .left = left,
        .right = right,
    } };
}

test "predConst folds boolean combinators over .always" {
    const f = PredicateExpr{ .always = false };
    const t = PredicateExpr{ .always = true };
    try testing.expectEqual(@as(?bool, false), predConst(f));
    try testing.expectEqual(@as(?bool, true), predConst(t));
    try testing.expectEqual(@as(?bool, false), predConst(.{ .@"and" = &.{ t, f } }));
    try testing.expectEqual(@as(?bool, true), predConst(.{ .@"or" = &.{ f, t } }));
    try testing.expectEqual(@as(?bool, true), predConst(.{ .not = &f }));
    const unknown = PredicateExpr{ .is_null = "c" };
    try testing.expectEqual(@as(?bool, null), predConst(unknown));
    // AND with an unknown child and a false child is still false.
    try testing.expectEqual(@as(?bool, false), predConst(.{ .@"and" = &.{ unknown, f } }));
    try testing.expectEqual(@as(?bool, null), predConst(.{ .@"and" = &.{ unknown, t } }));
}

test "dead union arm is pruned (right, then left)" {
    var base_r = scanOp();
    var dead_filter = ir.Op{ .filter = .{ .predicate = .{ .always = false }, .upstream = &base_r } };
    var live = scanOp();
    var u = ir.Op{ .set_union = .{ .left = &live, .right = &dead_filter, .all = true } };
    foldDeadBranches(&u);
    try testing.expect(u == .scan);

    var base_l2 = scanOp();
    var dead2 = ir.Op{ .filter = .{ .predicate = .{ .always = false }, .upstream = &base_l2 } };
    var base_r2 = scanOp();
    var union2 = ir.Op{ .set_union = .{ .left = &dead2, .right = &base_r2, .all = true } };
    foldDeadBranches(&union2);
    try testing.expect(union2 == .scan);
}

test "always-true filter is unlinked; false filter stays put outside unions" {
    var base = scanOp();
    var wrapped = ir.Op{ .filter = .{ .predicate = .{ .always = true }, .upstream = &base } };
    foldDeadBranches(&wrapped);
    try testing.expect(wrapped == .scan);

    var base2 = scanOp();
    var false_f = ir.Op{ .filter = .{ .predicate = .{ .always = false }, .upstream = &base2 } };
    foldDeadBranches(&false_f);
    try testing.expect(false_f == .filter);
}

test "emptiness propagates through pass-through nodes and inner joins" {
    var base = scanOp();
    var dead = ir.Op{ .filter = .{ .predicate = .{ .always = false }, .upstream = &base } };
    var proj = ir.Op{ .select = .{ .columns = &.{"a"}, .upstream = &dead } };
    try testing.expect(subtreeIsEmpty(&proj));

    var other = scanOp();
    var j = testJoin(.inner, &proj, &other);
    try testing.expect(subtreeIsEmpty(&j));
    // LEFT join with only the RIGHT side dead is NOT empty.
    var j2 = testJoin(.left, &other, &proj);
    try testing.expect(!subtreeIsEmpty(&j2));
    // Global aggregate over empty input still emits one row.
    var g = ir.Op{ .group_by = .{ .group_cols = &.{}, .aggs = &.{}, .upstream = &proj } };
    try testing.expect(!subtreeIsEmpty(&g));
    var g2 = ir.Op{ .group_by = .{ .group_cols = &.{"a"}, .aggs = &.{}, .upstream = &proj } };
    try testing.expect(subtreeIsEmpty(&g2));
}
