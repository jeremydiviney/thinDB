//! Partition-key subtree analysis — Phase 1 of partition-parallel execution.
//!
//! Bottom-up over the IR: for each node compute the MAXIMAL key set K (column
//! names in that node's output namespace) such that rows with different
//! K-values never interact anywhere in the subtree. Where such a K exists,
//! the whole subtree could hash-partition on K once and run per-partition on
//! independent cores — every window sort becomes a partition-local sort, and
//! the stage barriers between chained CTEs disappear inside each partition.
//!
//! Interaction rules (K only ever shrinks; TOP at the leaves):
//!   window    K := K ∩ every spec's PARTITION BY (a window partition is then
//!             entirely inside one K-group). Any global spec ⇒ BOTTOM.
//!   group_by  K := K ∩ group_cols. Global aggregate ⇒ BOTTOM.
//!   order_by  BOTTOM (a global sort interleaves everything). A top-level
//!             ORDER BY could be merge-finalized later; conservative for now.
//!   join      K flows from the PRESERVED side (probe); the other side is
//!             assumed broadcast (all wayroll joins are small dimensions —
//!             Phase 2 verifies with stats). FULL joins ⇒ BOTTOM.
//!   union     K := name-meet of both arms (the rollforward arms share
//!             names; positional mapping is a Phase-2 refinement).
//!   row-local (select/compute/filter/…) pass K through, tracking renames
//!             and dropping replaced/projected-away key columns. Any SUBSET
//!             of a valid K is itself valid (coarser groups), so shrinking
//!             is always sound — it just costs partition-balance cardinality.
//!
//! Phase 1 is REPORT-ONLY: `THINDB_TRACE_PARTKEYS` prints each maximal
//! K-subtree (where K dies at the parent, or the root) with the surviving
//! keys and how many windows/aggregates the subtree covers. No plan changes.
//!
//! Phase 2 (`THINDB_AUTO_SEP=1`, A/B-gated): mark the topmost qualifying
//! materialize node(s) with a synthetic SEPARABLE spec — the exact
//! annotation `SEPARABLE BY (cols)` SQL syntax would carry — so the staged
//! compiler's sliced-fill machinery partitions the whole subtree: bounds
//! sampled from staged inputs, N range-disjoint slice pipelines, private
//! table-backed blocks pre-partitioned by ONE full-DOP scan. The fill
//! declines gracefully (degenerate bounds, contiguous borrowers), so a bad
//! mark costs at most a forced stage buffer.
//!
//! A/B verdict (2026-07-11, AirDNA agg, live 32M-row data): value-exact
//! (identical result hash) but 2.3x SLOWER than mono (13.8s vs 5.9s at
//! min_keys=3; 3.4x at min_keys=1 where project+division collapsed to 2
//! slices). Two structural losses: (a) slices run dop_cap=1 pipelines that
//! bypass every fused parallel operator the mono path has (probe fusion,
//! parallel windows, radix agg); (b) range slices on customer keys are
//! whale-skewed — the largest slice carried 51% of rows, so the phase
//! equals its slowest slice. Stays default-off until slice pipelines can
//! be compiled once and re-parameterized per range, or an exchange feeds
//! fused operators instead of serial slice plans.

const std = @import("std");
const Allocator = std.mem.Allocator;

const ir = @import("../ir/ir.zig");
const types = @import("../types.zig");

const getenv_pk = @extern(*const fn (name: [*:0]const u8) callconv(.c) ?[*:0]const u8, .{ .name = "getenv", .library_name = "c" });

/// Analysis result for one node. `keys == null` is TOP (leaf: any key
/// works); an empty slice is BOTTOM (no partitioning possible).
const Info = struct {
    keys: ?[]const []const u8,
    windows: u32 = 0,
    groups: u32 = 0,
    ops: u32 = 0,

    fn bottom(self: Info) Info {
        return .{ .keys = &.{}, .windows = self.windows, .groups = self.groups, .ops = self.ops };
    }
    fn isBottom(self: Info) bool {
        return if (self.keys) |k| k.len == 0 else false;
    }
};

pub fn report(arena: Allocator, root: *const ir.Op) void {
    if (getenv_pk("THINDB_TRACE_PARTKEYS") == null) return;
    var memo: std.AutoHashMapUnmanaged(*const ir.Op, Info) = .empty;
    const info = analyze(arena, root, &memo) catch return;
    if (info.keys) |k| {
        if (k.len > 0) emit("root", root, info);
    } else if (info.windows + info.groups > 0) {
        emit("root(top)", root, info);
    }
}

pub const AutoSepOpts = struct {
    /// Minimum surviving keys at the mark site. 1 accepts the coarse top
    /// boundary (e.g. project+division after the final rollup); 3 forces
    /// the finer per-customer placement on the wayroll shape.
    min_keys: usize = 1,
    /// Minimum windows+groups the subtree must cover — a single window is
    /// already well-parallelized by the window operator itself; slicing
    /// earns its stage tax on CHAINS.
    min_cover: u32 = 3,
};

/// Phase 2 — auto-exchange. Walk top-down; at the first (topmost)
/// materialize whose subtree analysis yields >= min_keys surviving keys
/// covering >= min_cover windows+groups, attach the synthetic SEPARABLE
/// spec and stop descending (never nest fan-outs). Siblings (join arms,
/// union arms) are still walked — disjoint subtrees mark independently.
pub fn applyAutoSeparable(arena: Allocator, root: *ir.Op) void {
    if (getenv_pk("THINDB_AUTO_SEP") == null) return;
    const opts = AutoSepOpts{
        .min_keys = envInt(usize, "THINDB_AUTO_SEP_MIN_KEYS", 1),
        .min_cover = envInt(u32, "THINDB_AUTO_SEP_MIN_COVER", 3),
    };
    var memo: std.AutoHashMapUnmanaged(*const ir.Op, Info) = .empty;
    _ = autoSepMark(arena, root, opts, &memo) catch {};
}

fn envInt(comptime T: type, name: [*:0]const u8, default: T) T {
    const v = getenv_pk(name) orelse return default;
    return std.fmt.parseInt(T, std.mem.span(v), 10) catch default;
}

fn autoSepMark(
    arena: Allocator,
    op: *ir.Op,
    opts: AutoSepOpts,
    memo: *std.AutoHashMapUnmanaged(*const ir.Op, Info),
) anyerror!u32 {
    switch (op.*) {
        .materialize => |*m| {
            // A manual SEPARABLE BY rules its block; don't second-guess or
            // nest inside it.
            if (m.separable != null) return 0;
            const info = try analyze(arena, op, memo);
            if (info.keys) |k| {
                if (k.len >= opts.min_keys and info.windows + info.groups >= opts.min_cover) {
                    m.separable = .{ .cols = k };
                    m.forced = true;
                    if (getenv_pk("THINDB_TRACE_PARTKEYS") != null) emit("auto-sep", op, info);
                    return 1;
                }
            }
            return autoSepMark(arena, m.upstream, opts, memo);
        },
        .filter => |f| return autoSepMark(arena, f.upstream, opts, memo),
        .limit => |l| return autoSepMark(arena, l.upstream, opts, memo),
        .alias => |a| return autoSepMark(arena, a.upstream, opts, memo),
        .order_by => |o| return autoSepMark(arena, o.upstream, opts, memo),
        .window => |w| return autoSepMark(arena, w.upstream, opts, memo),
        .group_by => |g| return autoSepMark(arena, g.upstream, opts, memo),
        .compute => |c| return autoSepMark(arena, c.upstream, opts, memo),
        .select => |p| return autoSepMark(arena, p.upstream, opts, memo),
        .exclude => |p| return autoSepMark(arena, p.upstream, opts, memo),
        .join => |j| {
            const l = try autoSepMark(arena, j.left, opts, memo);
            const r = try autoSepMark(arena, j.right, opts, memo);
            return l + r;
        },
        .set_union => |u| {
            const l = try autoSepMark(arena, u.left, opts, memo);
            const r = try autoSepMark(arena, u.right, opts, memo);
            return l + r;
        },
        else => return 0,
    }
}

fn emit(where: []const u8, op: *const ir.Op, info: Info) void {
    std.debug.print("[partkeys] subtree at {s} root={s} windows={d} groups={d} ops={d} keys=", .{
        where,
        @tagName(op.*),
        info.windows,
        info.groups,
        info.ops,
    });
    if (info.keys) |k| {
        for (k, 0..) |c, i| std.debug.print("{s}{s}", .{ if (i > 0) "," else "", c });
        std.debug.print("\n", .{});
    } else {
        std.debug.print("<any>\n", .{});
    }
}

/// Qualified-name tolerant match: `estimate_candidates.projectId` meets
/// `projectId` (SQL refs mix qualified and bare forms freely across CTEs).
fn nameMatches(a: []const u8, b: []const u8) bool {
    if (types.columnNameEql(a, b)) return true;
    if (std.mem.lastIndexOfScalar(u8, a, '.')) |dot| {
        if (types.columnNameEql(a[dot + 1 ..], b)) return true;
    }
    if (std.mem.lastIndexOfScalar(u8, b, '.')) |dot| {
        if (types.columnNameEql(a, b[dot + 1 ..])) return true;
    }
    return false;
}

fn bareName(n: []const u8) []const u8 {
    return if (std.mem.lastIndexOfScalar(u8, n, '.')) |dot| n[dot + 1 ..] else n;
}

/// Intersect `keys` (null = TOP) with name set `with`, in the arena. Keys
/// are held in BARE form throughout (SQL refs mix `cia.projectId` and
/// `projectId` freely across CTE hops; the qualifier carries no identity
/// here because K only ever narrows from one lineage).
fn meet(arena: Allocator, keys: ?[]const []const u8, with: []const []const u8) ![]const []const u8 {
    const base = keys orelse {
        const out = try arena.alloc([]const u8, with.len);
        for (with, out) |w, *o| o.* = bareName(w);
        return out;
    };
    var out: std.ArrayListUnmanaged([]const u8) = .empty;
    for (base) |b| {
        for (with) |w| {
            if (nameMatches(b, w)) {
                try out.append(arena, bareName(b));
                break;
            }
        }
    }
    return out.items;
}

fn removeKey(arena: Allocator, keys: []const []const u8, name: []const u8) ![]const []const u8 {
    var out: std.ArrayListUnmanaged([]const u8) = .empty;
    for (keys) |k| {
        if (!types.columnNameEql(k, name)) try out.append(arena, k);
    }
    return out.items;
}

/// A child subtree ends here (its K dies at this parent): report it if it
/// covered anything worth partitioning.
fn boundary(child_op: *const ir.Op, child: Info) void {
    if (child.isBottom()) return; // already reported deeper
    const covered = child.windows + child.groups;
    if (covered == 0) return;
    if (child.keys != null and child.keys.?.len == 0) return;
    emit("boundary", child_op, child);
}

fn analyze(arena: Allocator, op: *const ir.Op, memo: *std.AutoHashMapUnmanaged(*const ir.Op, Info)) !Info {
    if (memo.get(op)) |hit| return hit;
    const info = analyzeInner(arena, op, memo) catch |e| return e;
    try memo.put(arena, op, info);
    return info;
}

fn analyzeInner(arena: Allocator, op: *const ir.Op, memo: *std.AutoHashMapUnmanaged(*const ir.Op, Info)) anyerror!Info {
    switch (op.*) {
        .scan, .single_row, .file_scan => return .{ .keys = null, .ops = 1 },

        .filter => |f| {
            var i = try analyze(arena, f.upstream, memo);
            i.ops += 1;
            return i;
        },
        .limit => |l| {
            var i = try analyze(arena, l.upstream, memo);
            i.ops += 1;
            return i;
        },
        .alias => |a| {
            var i = try analyze(arena, a.upstream, memo);
            i.ops += 1;
            return i;
        },
        .materialize => |m| {
            var i = try analyze(arena, m.upstream, memo);
            i.ops += 1;
            return i;
        },

        .order_by => |o| {
            const child = try analyze(arena, o.upstream, memo);
            boundary(o.upstream, child);
            return .{ .keys = &.{}, .windows = child.windows, .groups = child.groups, .ops = child.ops + 1 };
        },

        .window => |w| {
            const child = try analyze(arena, w.upstream, memo);
            var i = child;
            i.ops += 1;
            i.windows += 1;
            for (w.specs) |sp| {
                if (sp.partition_by.len == 0) {
                    boundary(w.upstream, child);
                    return i.bottom();
                }
                i.keys = try meet(arena, i.keys, sp.partition_by);
            }
            if (i.isBottom() and !child.isBottom()) boundary(w.upstream, child);
            return i;
        },

        .group_by => |g| {
            const child = try analyze(arena, g.upstream, memo);
            var i = child;
            i.ops += 1;
            i.groups += 1;
            if (g.group_cols.len == 0) {
                boundary(g.upstream, child);
                return i.bottom();
            }
            i.keys = try meet(arena, i.keys, g.group_cols);
            if (i.isBottom() and !child.isBottom()) boundary(g.upstream, child);
            return i;
        },

        .compute => |c| {
            var i = try analyze(arena, c.upstream, memo);
            i.ops += 1;
            if (i.keys) |k| {
                var keys = k;
                for (c.derived) |d| {
                    // A derived that is a BARE ref of a key column carries the
                    // key value unchanged — a rename/copy preserves grouping.
                    // Anything else replacing a key column destroys it.
                    if (d.expr == .col_ref and nameMatches(d.expr.col_ref, d.name)) continue;
                    const before = keys.len;
                    keys = try removeKey(arena, keys, d.name);
                    if (keys.len == 0 and before > 0 and getenv_pk("THINDB_TRACE_PARTKEYS") != null) {
                        std.debug.print("[partkeys] key killed at compute: derived '{s}' replaced the last key\n", .{d.name});
                    }
                }
                i.keys = keys;
            }
            return i;
        },

        .select => |p| {
            var i = try analyze(arena, p.upstream, memo);
            i.ops += 1;
            const keys = i.keys orelse return i;
            // Pure star passes everything through unchanged.
            if (p.columns.len == 1 and std.mem.eql(u8, p.columns[0], "*")) return i;
            var out: std.ArrayListUnmanaged([]const u8) = .empty;
            var star = false;
            for (p.columns) |src| {
                if (std.mem.eql(u8, src, "*") or std.mem.endsWith(u8, src, ".*")) star = true;
            }
            for (keys) |kcol| {
                var kept = false;
                for (p.columns, 0..) |src, idx| {
                    if (std.mem.eql(u8, src, "*") or std.mem.endsWith(u8, src, ".*")) continue;
                    if (!nameMatches(src, kcol)) continue;
                    const out_name = if (p.outputs) |outs| (outs[idx] orelse src) else src;
                    try out.append(arena, bareName(out_name));
                    kept = true;
                    break;
                }
                // A star item passes unlisted columns through untouched.
                if (!kept and star) {
                    try out.append(arena, kcol);
                    kept = true;
                }
                if (!kept and keys.len > 0 and getenv_pk("THINDB_TRACE_PARTKEYS") != null) {
                    std.debug.print("[partkeys] key dropped at select: '{s}' not projected\n", .{kcol});
                }
            }
            i.keys = out.items;
            return i;
        },

        .exclude => |p| {
            var i = try analyze(arena, p.upstream, memo);
            i.ops += 1;
            if (i.keys) |k| {
                var keys = k;
                for (p.columns) |c| keys = try removeKey(arena, keys, c);
                i.keys = keys;
            }
            return i;
        },

        .join => |j| {
            const l = try analyze(arena, j.left, memo);
            const r = try analyze(arena, j.right, memo);
            const preserved: Info, const other_op: *const ir.Op, const other: Info = switch (j.join_type) {
                .left, .inner => .{ l, j.right, r },
                .right => .{ r, j.left, l },
                .full => {
                    boundary(j.left, l);
                    boundary(j.right, r);
                    return .{ .keys = &.{}, .windows = l.windows + r.windows, .groups = l.groups + r.groups, .ops = l.ops + r.ops + 1 };
                },
            };
            // The non-preserved side is assumed broadcast (Phase 2 verifies
            // size); its own K-subtree, if any, ends here.
            boundary(other_op, other);
            return .{
                .keys = preserved.keys,
                .windows = l.windows + r.windows,
                .groups = l.groups + r.groups,
                .ops = l.ops + r.ops + 1,
            };
        },

        .set_union => |u| {
            const l = try analyze(arena, u.left, memo);
            const r = try analyze(arena, u.right, memo);
            var i = Info{
                .keys = undefined,
                .windows = l.windows + r.windows,
                .groups = l.groups + r.groups,
                .ops = l.ops + r.ops + 1,
            };
            // Name-meet: keep keys valid in BOTH arms under the same name.
            if (l.keys == null) {
                i.keys = r.keys;
            } else if (r.keys == null) {
                i.keys = l.keys;
            } else {
                i.keys = try meet(arena, l.keys, r.keys.?);
                if (i.isBottom()) {
                    boundary(u.left, l);
                    boundary(u.right, r);
                }
            }
            return i;
        },

        else => {
            // DDL / DML / show / explain wrappers and unknown shapes: no
            // partitioning claim through them.
            return .{ .keys = &.{}, .ops = 1 };
        },
    }
}

// ---------------------------------------------------------------------------

const testing = std.testing;

fn scanOp() ir.Op {
    return .{ .scan = .{ .table = .{ .name = "t" } } };
}

test "window chain preserves the shared partition key; final global agg is the boundary" {
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    var base = scanOp();
    const part = [_][]const u8{ "proj", "cust" };
    const spec1 = [_]ir.WindowSpec{.{ .partition_by = &part, .order_by = &.{}, .frame = ir.Frame.default_no_order }};
    var w1 = ir.Op{ .window = .{ .specs = &spec1, .calls = &.{}, .upstream = &base } };
    var w2 = ir.Op{ .window = .{ .specs = &spec1, .calls = &.{}, .upstream = &w1 } };
    const gcols = [_][]const u8{ "proj", "cust" };
    var g = ir.Op{ .group_by = .{ .group_cols = &gcols, .aggs = &.{}, .upstream = &w2 } };

    var memo: std.AutoHashMapUnmanaged(*const ir.Op, Info) = .empty;
    const info = try analyze(arena, &g, &memo);
    try testing.expect(info.keys != null);
    try testing.expectEqual(@as(usize, 2), info.keys.?.len);
    try testing.expectEqual(@as(u32, 2), info.windows);
    try testing.expectEqual(@as(u32, 1), info.groups);
}

test "narrower group keys shrink K; global aggregate kills it" {
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    var base = scanOp();
    const part = [_][]const u8{ "proj", "cust" };
    const spec1 = [_]ir.WindowSpec{.{ .partition_by = &part, .order_by = &.{}, .frame = ir.Frame.default_no_order }};
    var w1 = ir.Op{ .window = .{ .specs = &spec1, .calls = &.{}, .upstream = &base } };
    const gcols = [_][]const u8{"proj"};
    var g = ir.Op{ .group_by = .{ .group_cols = &gcols, .aggs = &.{}, .upstream = &w1 } };
    var g2 = ir.Op{ .group_by = .{ .group_cols = &.{}, .aggs = &.{}, .upstream = &g } };

    var memo: std.AutoHashMapUnmanaged(*const ir.Op, Info) = .empty;
    const gi = try analyze(arena, &g, &memo);
    try testing.expectEqual(@as(usize, 1), gi.keys.?.len);
    try testing.expectEqualStrings("proj", gi.keys.?[0]);

    var memo2: std.AutoHashMapUnmanaged(*const ir.Op, Info) = .empty;
    const ti = try analyze(arena, &g2, &memo2);
    try testing.expect(ti.isBottom());
}

test "autoSepMark: topmost qualifying materialize gets the spec, thin ones don't" {
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    // materialize( group_by( window( window( scan )))) — covers 3, keys 2.
    var base = scanOp();
    const part = [_][]const u8{ "proj", "cust" };
    const spec1 = [_]ir.WindowSpec{.{ .partition_by = &part, .order_by = &.{}, .frame = ir.Frame.default_no_order }};
    var w1 = ir.Op{ .window = .{ .specs = &spec1, .calls = &.{}, .upstream = &base } };
    var w2 = ir.Op{ .window = .{ .specs = &spec1, .calls = &.{}, .upstream = &w1 } };
    const gcols = [_][]const u8{ "proj", "cust" };
    var g = ir.Op{ .group_by = .{ .group_cols = &gcols, .aggs = &.{}, .upstream = &w2 } };
    var mat = ir.Op{ .materialize = .{ .upstream = &g } };
    var sel_cols = [_][]const u8{"*"};
    var root = ir.Op{ .select = .{ .columns = &sel_cols, .upstream = &mat } };

    var memo: std.AutoHashMapUnmanaged(*const ir.Op, Info) = .empty;
    const marked = try autoSepMark(arena, &root, .{}, &memo);
    try testing.expectEqual(@as(u32, 1), marked);
    try testing.expect(mat.materialize.separable != null);
    try testing.expect(mat.materialize.forced);
    const cols = mat.materialize.separable.?.cols;
    try testing.expectEqual(@as(usize, 2), cols.len);
    try testing.expectEqualStrings("proj", cols[0]);
    try testing.expectEqualStrings("cust", cols[1]);

    // A single-window block stays unmarked (cover 1 < min_cover 3): the
    // window operator already parallelizes itself.
    var base2 = scanOp();
    var w_only = ir.Op{ .window = .{ .specs = &spec1, .calls = &.{}, .upstream = &base2 } };
    var mat2 = ir.Op{ .materialize = .{ .upstream = &w_only } };
    var memo2: std.AutoHashMapUnmanaged(*const ir.Op, Info) = .empty;
    const marked2 = try autoSepMark(arena, &mat2, .{}, &memo2);
    try testing.expectEqual(@as(u32, 0), marked2);
    try testing.expect(mat2.materialize.separable == null);

    // min_keys=3 skips the 2-key top block and finds nothing deeper here.
    var g3 = ir.Op{ .group_by = .{ .group_cols = &gcols, .aggs = &.{}, .upstream = &w2 } };
    var mat3 = ir.Op{ .materialize = .{ .upstream = &g3 } };
    var memo3: std.AutoHashMapUnmanaged(*const ir.Op, Info) = .empty;
    const marked3 = try autoSepMark(arena, &mat3, .{ .min_keys = 3 }, &memo3);
    try testing.expectEqual(@as(u32, 0), marked3);
    try testing.expect(mat3.materialize.separable == null);
}

test "select rename tracks the key; compute replacing it drops it" {
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    var base = scanOp();
    const part = [_][]const u8{"cust"};
    const spec1 = [_]ir.WindowSpec{.{ .partition_by = &part, .order_by = &.{}, .frame = ir.Frame.default_no_order }};
    var w = ir.Op{ .window = .{ .specs = &spec1, .calls = &.{}, .upstream = &base } };

    const cols = [_][]const u8{"cust"};
    const outs = [_]?[]const u8{"customer"};
    var sel = ir.Op{ .select = .{ .columns = &cols, .outputs = &outs, .upstream = &w } };
    var memo: std.AutoHashMapUnmanaged(*const ir.Op, Info) = .empty;
    const si = try analyze(arena, &sel, &memo);
    try testing.expectEqual(@as(usize, 1), si.keys.?.len);
    try testing.expectEqualStrings("customer", si.keys.?[0]);

    // A bare-ref rename of the key onto itself PRESERVES it (a copy keeps
    // grouping): K survives.
    const copy = [_]ir.Derived{.{ .name = "customer", .expr = .{ .col_ref = "customer" } }};
    var cmp_copy = ir.Op{ .compute = .{ .derived = &copy, .upstream = &sel } };
    var memo_c: std.AutoHashMapUnmanaged(*const ir.Op, Info) = .empty;
    const ci_copy = try analyze(arena, &cmp_copy, &memo_c);
    try testing.expectEqual(@as(usize, 1), ci_copy.keys.?.len);
    try testing.expectEqualStrings("customer", ci_copy.keys.?[0]);

    // A non-trivial expr replacing the key column DESTROYS it.
    const args = [_]ir.Expr{.{ .col_ref = "customer" }};
    const replaced = [_]ir.Derived{.{ .name = "customer", .expr = .{ .call = .{ .fn_name = "lower", .args = &args } } }};
    var cmp = ir.Op{ .compute = .{ .derived = &replaced, .upstream = &sel } };
    var memo2: std.AutoHashMapUnmanaged(*const ir.Op, Info) = .empty;
    const ci = try analyze(arena, &cmp, &memo2);
    try testing.expect(ci.isBottom());
}
