//! Filter operator. Walks the upstream batch-by-batch, evaluates a
//! `PredicateExpr` per row, and emits batches containing only matching
//! rows. Pushes leaves of top-level ANDs down to the upstream so Scan can
//! prune row groups via min/max stats.

const std = @import("std");
const Allocator = std.mem.Allocator;

const types = @import("../types.zig");
const Column = types.Column;

const storage = @import("../storage/storage.zig");
const ColumnView = storage.ColumnView;

const engine = @import("../engine/engine.zig");
const ColumnStore = engine.ColumnStore;

const exec = @import("exec.zig");
const Query = exec.Query;
const Batch = exec.Batch;
const makeQuery = exec.makeQuery;

const predicate = @import("predicate.zig");
const Predicate = predicate.Predicate;
const PredicateExpr = predicate.PredicateExpr;

pub const Filter = struct {
    allocator: Allocator,
    upstream: Query,
    expr: PredicateExpr,
    schema: []const Column,

    /// Upstream per-column stats tightened by this filter's predicate
    /// (proven upper bounds only). Empty when the upstream carries no
    /// full per-column array. Cached at create; borrowed by `stats()`.
    cached_stats: []const exec.ColStat = &.{},

    /// Per-column accumulator. Mirrors the memtable's storage shape so the
    /// same view() helper applies.
    filtered: []ColumnStore,
    views: []ColumnView,

    /// When true, the upstream Scan accepted this predicate and applies it
    /// itself (scan-side in-place filter), returning already-compacted owned
    /// survivors. This Filter then forwards upstream batches verbatim — no
    /// re-evaluation, no copy. The borrow over cache bytes lives entirely
    /// inside the Scan's `next()`, never reaching here.
    fused: bool = false,

    /// New conjunct slices synthesized by the plan-time simplification pass
    /// (one per AND level that lost a conjunct). Allocated from `allocator`,
    /// freed in `deinit`. Empty when nothing was rewritten.
    rewritten_children: std.ArrayListUnmanaged([]PredicateExpr) = .empty,

    pub fn create(allocator: Allocator, upstream: Query, expr: PredicateExpr) !Query {
        const schema = upstream.outputSchema();

        // Validate first so integer-literal widening lands before the
        // simplification pass reads the literal as i128, then prove-and-prune
        // top-level AND conjuncts against the upstream's per-column min/max.
        var validated = expr;
        try predicate.validateExpr(&validated, schema);

        var rewritten: std.ArrayListUnmanaged([]PredicateExpr) = .empty;
        errdefer rewritten.deinit(allocator);
        const simplified = try simplifyPredicate(
            allocator,
            &rewritten,
            validated,
            schema,
            upstream.stats().column_stats,
        );

        // All conjuncts provably true ⇒ the Filter is a no-op. Drop it so the
        // columns it referenced are never scanned on its behalf; the upstream
        // becomes the query node directly.
        if (simplified == .always and simplified.always) {
            rewritten.deinit(allocator);
            return upstream;
        }

        const views = try allocator.alloc(ColumnView, schema.len);
        errdefer allocator.free(views);

        const filtered = try allocator.alloc(ColumnStore, schema.len);
        errdefer allocator.free(filtered);
        var inited: usize = 0;
        errdefer for (filtered[0..inited]) |*c| c.deinit(allocator);
        for (schema, 0..) |col, i| {
            filtered[i] = try ColumnStore.init(allocator, col.type, col.nullable);
            inited += 1;
        }

        const self = try allocator.create(Filter);
        errdefer allocator.destroy(self);

        self.* = .{
            .allocator = allocator,
            .upstream = upstream,
            .expr = simplified,
            .schema = schema,
            .filtered = filtered,
            .views = views,
            .rewritten_children = rewritten,
        };

        // Push surviving leaves through top-level ANDs down to Scan for
        // row-group prune. An always-false filter prunes nothing extra.
        var up = self.upstream;
        predicate.pushExprDown(&up, self.expr) catch |err| switch (err) {
            error.ColumnNotFound => {},
            else => return err,
        };

        // Tighten the upstream per-column stats with this predicate's proven
        // bounds (eq pins a column to one value, ranges clamp min/max, etc.).
        // Computed after `validateExpr` so any integer-literal widening is
        // reflected in `self.expr`. The Scan-fusion offer below doesn't change
        // the OUTPUT this Filter represents, so the tightening holds either way.
        self.cached_stats = try tightenStats(allocator, self.upstream.stats(), schema, self.expr);
        errdefer if (self.cached_stats.len > 0) allocator.free(@constCast(self.cached_stats));

        // Offer the full (validated) predicate to the upstream Scan for in-place
        // filtering. If it accepts, it emits compacted owned survivors and this
        // Filter degrades to a pass-through. The expr it borrows is `self.expr`,
        // which outlives the query (owned by this Filter).
        self.fused = self.upstream.tryFuseFilter(self.expr) catch false;

        return makeQuery(allocator, self);
    }

    pub fn deinit(self: *Filter) void {
        var up = self.upstream;
        up.deinit();
        if (self.cached_stats.len > 0) self.allocator.free(@constCast(self.cached_stats));
        for (self.rewritten_children.items) |slice| self.allocator.free(slice);
        self.rewritten_children.deinit(self.allocator);
        for (self.filtered) |*c| c.deinit(self.allocator);
        self.allocator.free(self.filtered);
        self.allocator.free(self.views);
        const allocator = self.allocator;
        allocator.destroy(self);
    }

    pub fn outputSchema(self: *Filter) []const Column {
        return self.schema;
    }

    pub fn addPrune(self: *Filter, pred: Predicate) !void {
        return self.upstream.addPrune(pred);
    }

    /// Filter only restricts rows — `upper_rows` is unchanged (a filter is
    /// only provably ≤ input; we don't estimate a reduction). Sort state
    /// preserved (Filter doesn't reorder). Per-column stats are tightened by
    /// the predicate's proven bounds (see `tightenStats`), then capped at
    /// `upper_rows`.
    pub fn stats(self: *Filter) exec.PipelineStats {
        const up = self.upstream.stats();
        return .{
            .upper_rows = up.upper_rows,
            .sort_state = up.sort_state,
            .column_stats = if (self.cached_stats.len > 0) self.cached_stats else up.column_stats,
        };
    }

    pub fn accountant(self: *Filter) ?*exec.memory.MemoryAccountant {
        return self.upstream.accountant();
    }

    pub fn explain(self: *Filter, out: *std.ArrayList(u8), allocator: std.mem.Allocator, depth: usize) !void {
        try exec.explainLine(out, allocator, depth, "Filter");
        try self.upstream.explain(out, allocator, depth + 1);
    }

    pub fn next(self: *Filter) !?Batch {
        // Fused: the Scan already applied the predicate and returns compacted
        // owned survivors. Forward verbatim.
        if (self.fused) return self.upstream.next();

        while (true) {
            const upstream_batch = (try self.upstream.next()) orelse return null;

            const n = upstream_batch.row_count;
            const mask = try self.allocator.alloc(bool, n);
            defer self.allocator.free(mask);
            try predicate.evaluateExprGuided(self.allocator, self.expr, self.schema, upstream_batch, mask, null);

            var matched: usize = 0;
            for (mask) |m| if (m) {
                matched += 1;
            };
            if (matched == 0) continue;

            for (self.filtered) |*c| c.clear();
            for (upstream_batch.values, 0..) |view, ci| {
                try engine.memtable.appendMaskedColumn(self.allocator, view, mask, &self.filtered[ci]);
            }
            for (self.filtered, 0..) |c, ci| self.views[ci] = c.view();

            return Batch{ .schema = self.schema, .values = self.views, .row_count = matched };
        }
    }
};

/// Tighten the upstream per-column stats with the proven bounds a filter
/// predicate guarantees. Only top-level AND conjuncts contribute (an OR/NOT
/// branch proves nothing about any single column). For each contributing
/// leaf the column's bound shrinks; `upper_rows` (the row ceiling) is left to
/// the caller, then every column's ndv is capped at it.
///
/// Returns `&.{}` (caller falls back to upstream's array) when the upstream
/// doesn't carry a full per-column array — fabricating one would change the
/// "empty ⇒ no info" contract downstream consumers rely on. Caller owns the
/// returned slice.
fn tightenStats(
    allocator: Allocator,
    up: exec.PipelineStats,
    schema: []const Column,
    expr: PredicateExpr,
) ![]const exec.ColStat {
    if (up.column_stats.len != schema.len) return &.{};

    const out = try allocator.alloc(exec.ColStat, schema.len);
    errdefer allocator.free(out);
    @memcpy(out, up.column_stats);

    applyConjuncts(out, schema, expr);
    exec.capColStats(out, up.upper_rows);
    return out;
}

/// Walk top-level AND conjuncts, tightening `stats` for each `.leaf` /
/// `.in_set` whose column resolves in `schema`.
fn applyConjuncts(stats: []exec.ColStat, schema: []const Column, expr: PredicateExpr) void {
    switch (expr) {
        .@"and" => |children| for (children) |c| applyConjuncts(stats, schema, c),
        .leaf => |p| applyLeaf(stats, schema, p),
        .in_set => |s| applyInSet(stats, schema, s),
        else => {},
    }
}

fn applyLeaf(stats: []exec.ColStat, schema: []const Column, p: Predicate) void {
    const idx = types.findColumn(schema, p.col) orelse return;
    const has_range = predicate.typeHasRange(schema[idx].type);
    const v: ?i128 = predicate.valueToRangeI128(p.val);
    var s = &stats[idx];
    switch (p.op) {
        // `col = v`: exactly one surviving value.
        .eq => {
            s.ndv = .{ .exact = 1 };
            if (has_range) if (v) |val| {
                s.min = val;
                s.max = val;
            };
        },
        // `col <> v`: at most one distinct value is removed.
        .neq => switch (s.ndv) {
            .exact => |n| s.ndv = .{ .exact = @max(1, n) - 1 },
            .unknown => {},
        },
        // `col < v` / `col <= v`: clamp the upper end.
        .lt, .lte => if (has_range) if (v) |val| {
            s.max = if (s.max) |m| @min(m, val) else val;
        },
        // `col > v` / `col >= v`: clamp the lower end.
        .gt, .gte => if (has_range) if (v) |val| {
            s.min = if (s.min) |m| @max(m, val) else val;
        },
    }
}

/// `col IN (k literals)`: ndv ≤ k; min/max clamp the prior range to the
/// span of the literal set (intersection of bounds).
fn applyInSet(stats: []exec.ColStat, schema: []const Column, in: predicate.InSet) void {
    if (in.negate) return; // NOT IN proves no single-column bound
    const idx = types.findColumn(schema, in.col) orelse return;
    const k: u32 = if (in.values.len > std.math.maxInt(u32)) std.math.maxInt(u32) else @intCast(in.values.len);
    var s = &stats[idx];
    switch (s.ndv) {
        .exact => |n| s.ndv = .{ .exact = @min(n, k) },
        .unknown => s.ndv = .{ .exact = k },
    }
    if (!predicate.typeHasRange(schema[idx].type)) return;
    var set_min: ?i128 = null;
    var set_max: ?i128 = null;
    for (in.values) |val| {
        const iv = predicate.valueToRangeI128(val) orelse continue;
        set_min = if (set_min) |m| @min(m, iv) else iv;
        set_max = if (set_max) |m| @max(m, iv) else iv;
    }
    if (set_min) |lo| s.min = if (s.min) |m| @max(m, lo) else lo;
    if (set_max) |hi| s.max = if (s.max) |m| @min(m, hi) else hi;
}

/// Plan-time predicate simplification against the upstream's proven per-column
/// min/max. Only fires when a column carries BOTH a known `min` and `max`
/// (e.g. `EventDate` ∈ ['2013-07-01','2013-07-31'] across every ClickBench
/// row). Three outcomes per top-level AND conjunct that is a range/comparison
/// leaf:
///
///   - ALWAYS-TRUE → drop the conjunct. A range predicate excludes NULLs, so
///     dropping it would wrongly admit NULL rows — gated on the column being
///     non-nullable. When every conjunct drops the AND collapses to
///     `.always = true` (caller drops the whole Filter).
///   - ALWAYS-FALSE → the whole AND is `.always = false` (0 rows). Safe
///     regardless of nullability: no row, null or not, passes.
///   - otherwise → the leaf is kept verbatim.
///
/// `.@"or"` / `.not` children are left untouched (an always-true inside an OR
/// flips the OR's truth — different logic, out of scope). `eq` in range, `neq`,
/// LIKE, col-col, subquery forms, and is_null pass through. Returns the
/// (possibly rebuilt) expr; `.always` carries a proven constant outcome.
fn simplifyPredicate(
    allocator: Allocator,
    rewritten: *std.ArrayListUnmanaged([]PredicateExpr),
    expr: PredicateExpr,
    schema: []const Column,
    stats: []const exec.ColStat,
) !PredicateExpr {
    switch (expr) {
        .leaf => |p| return simplifyLeaf(p, schema, stats),
        .@"and" => |children| {
            var survivors: std.ArrayListUnmanaged(PredicateExpr) = .empty;
            errdefer survivors.deinit(allocator);
            for (children) |child| {
                const s = try simplifyPredicate(allocator, rewritten, child, schema, stats);
                switch (s) {
                    .always => |b| {
                        if (!b) {
                            survivors.deinit(allocator);
                            return .{ .always = false };
                        }
                        // always-true conjunct contributes nothing — drop it.
                    },
                    else => try survivors.append(allocator, s),
                }
            }
            if (survivors.items.len == 0) {
                survivors.deinit(allocator);
                return .{ .always = true };
            }
            if (survivors.items.len == children.len) {
                // Nothing dropped — reuse the borrowed children slice.
                survivors.deinit(allocator);
                return expr;
            }
            if (survivors.items.len == 1) {
                const only = survivors.items[0];
                survivors.deinit(allocator);
                return only;
            }
            const owned = try survivors.toOwnedSlice(allocator);
            errdefer allocator.free(owned);
            try rewritten.append(allocator, owned);
            return .{ .@"and" = owned };
        },
        else => return expr,
    }
}

/// Evaluate one comparison leaf against `[cmin, cmax]`. Returns
/// `.always = true` (provably matches every row → droppable),
/// `.always = false` (provably matches none → empty), or the leaf unchanged.
fn simplifyLeaf(p: Predicate, schema: []const Column, stats: []const exec.ColStat) PredicateExpr {
    const leaf: PredicateExpr = .{ .leaf = p };
    if (stats.len != schema.len) return leaf;
    const idx = types.findColumn(schema, p.col) orelse return leaf;
    if (!predicate.typeHasRange(schema[idx].type)) return leaf;
    const cmin = stats[idx].min orelse return leaf;
    const cmax = stats[idx].max orelse return leaf;
    const v = predicate.valueToRangeI128(p.val) orelse return leaf;
    const non_nullable = !schema[idx].nullable;

    switch (p.op) {
        // `col >= v` — false iff v > cmax; true iff v <= cmin.
        .gte => {
            if (v > cmax) return .{ .always = false };
            if (non_nullable and v <= cmin) return .{ .always = true };
        },
        // `col > v` — false iff v >= cmax; true iff v < cmin.
        .gt => {
            if (v >= cmax) return .{ .always = false };
            if (non_nullable and v < cmin) return .{ .always = true };
        },
        // `col <= v` — false iff v < cmin; true iff v >= cmax.
        .lte => {
            if (v < cmin) return .{ .always = false };
            if (non_nullable and v >= cmax) return .{ .always = true };
        },
        // `col < v` — false iff v <= cmin; true iff v > cmax.
        .lt => {
            if (v <= cmin) return .{ .always = false };
            if (non_nullable and v > cmax) return .{ .always = true };
        },
        // `col = v` — false iff v outside [cmin, cmax]. In-range eq is never
        // provably always-true (the column may hold other values too).
        .eq => {
            if (v < cmin or v > cmax) return .{ .always = false };
        },
        // `col <> v` proves nothing always-false from range alone.
        .neq => {},
    }
    return leaf;
}
