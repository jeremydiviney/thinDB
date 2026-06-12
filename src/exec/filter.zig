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

    /// Second ping-pong materialization set for the compacting filter path: an
    /// intermediate compaction writes survivors of the current (possibly
    /// already-compacted) batch into a set distinct from the one it reads, so
    /// successive compactions can alternate. Same shape as `filtered`/`views`,
    /// but lazily allocated on the first compaction that needs it — fused and
    /// non-compacting filters never pay for it.
    filtered_b: []ColumnStore = &.{},
    views_b: []ColumnView = &.{},

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
        if (self.filtered_b.len > 0) {
            for (self.filtered_b) |*c| c.deinit(self.allocator);
            self.allocator.free(self.filtered_b);
            self.allocator.free(self.views_b);
        }
        const allocator = self.allocator;
        allocator.destroy(self);
    }

    pub fn outputSchema(self: *Filter) []const Column {
        // A fused Filter is a pass-through and emits its upstream's batches
        // verbatim — so it must report the upstream's CURRENT schema, which may
        // have grown columns if a projection Compute fused down through us after
        // construction. (Non-fused: our own schema == upstream's, but read it
        // live anyway.) Without this the cached schema goes stale and a fused
        // Compute's new columns become "unknown" to downstream operators.
        if (self.fused) return self.upstream.outputSchema();
        return self.schema;
    }

    pub fn addPrune(self: *Filter, pred: Predicate) !void {
        return self.upstream.addPrune(pred);
    }

    /// Forward coded-key setup to the upstream Scan — but only when this Filter
    /// FUSED its predicate into the Scan. A fused Filter is a passthrough: the
    /// Scan filters + emits coded survivors itself (its filtered path is
    /// coding-aware). A non-fused Filter evaluates rows here and would have to
    /// compact the code sidecar in lockstep (it can't), so it declines — the
    /// GROUP BY gate then keeps the normal materialized-string path.
    pub fn setDictCodeColumn(self: *Filter, name: []const u8, dict: *exec.GlobalDict) bool {
        if (!self.fused) return false;
        return self.upstream.setDictCodeColumn(name, dict);
    }

    pub fn canCodeColumn(self: *Filter, name: []const u8) bool {
        if (!self.fused) return false;
        return self.upstream.canCodeColumn(name);
    }

    pub fn clearDictCodeColumns(self: *Filter) void {
        if (self.fused) self.upstream.clearDictCodeColumns();
    }

    /// Forward the consumer's emit-projection upstream ONLY when this Filter is a
    /// pass-through (its predicate already ran in the Scan). A non-fused Filter
    /// still evaluates its predicate over the upstream batch at runtime, so it
    /// needs those columns materialized — swallow the projection to keep them.
    pub fn setEmitProjection(self: *Filter, keep: []const []const u8) !void {
        if (self.fused) try self.upstream.setEmitProjection(keep);
    }

    /// Forward a partial-aggregate fusion upstream only when this Filter is a
    /// fused pass-through (its predicate already runs in the scan); a non-fused
    /// Filter must evaluate rows itself, so the aggregate can't push below it.
    pub fn tryFuseAggregate(self: *Filter, group_cols: []const []const u8, aggs: []const exec.AggSpec) !bool {
        if (!self.fused) return false;
        return self.upstream.tryFuseAggregate(group_cols, aggs);
    }

    pub fn tryLeaseGroupBy(self: *Filter, group_cols: []const []const u8, aggs: []const exec.AggSpec, top_k: ?@import("../ir/ir.zig").Op.TopK, emit_limit: ?u32, dop: usize) !?exec.Query {
        if (!self.fused) return null;
        return self.upstream.tryLeaseGroupBy(group_cols, aggs, top_k, emit_limit, dop);
    }

    /// Forward a projection-Compute fusion to the upstream, but only when this
    /// Filter is itself fused (a pass-through). A non-fused Filter still
    /// evaluates rows here, so a Compute above it must stay a separate operator.
    /// A fused Filter is a pass-through, so a join-probe offer continues to
    /// the scan (the batches this Filter forwards become joined batches —
    /// its own predicate already runs inside the scan). An UNFUSED Filter
    /// must decline: its mask evaluates against the probe-side schema and
    /// would be applied to already-joined rows.
    pub fn tryFuseProbe(self: *Filter, sink: exec.ProbeSink) !bool {
        if (!self.fused) return false;
        return self.upstream.tryFuseProbe(sink);
    }

    pub fn tryFuseCompute(self: *Filter, derived: []const @import("compute.zig").Derived) !bool {
        if (!self.fused) return false;
        const ok = try self.upstream.tryFuseCompute(derived);
        // The fused derived columns grow the upstream's output schema, so our
        // pre-fusion `cached_stats` (tightened for the OLD, narrower schema) is
        // now stale + too short — a derived GROUP BY key would index past its
        // end and look unknown, mis-sizing the hash table. Drop it; `stats()`
        // then forwards the fresh upstream stats (which carry the fused
        // columns' propagated NDV). Losing the predicate's min/max tightening
        // here is safe: untightened bounds are still valid upper bounds.
        if (ok and self.cached_stats.len > 0) {
            self.allocator.free(@constCast(self.cached_stats));
            self.cached_stats = &.{};
        }
        return ok;
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
        // Proven-empty predicate (stats simplification folded it to
        // `.always = false`): no row can match, so emit nothing without
        // draining the upstream scan. Covers the non-fused path; the fused
        // path short-circuits in the Scan itself.
        if (self.expr == .always and !self.expr.always) return null;

        // Fused: the Scan already applied the predicate and returns compacted
        // owned survivors. Forward verbatim.
        if (self.fused) return self.upstream.next();

        // A top-level AND of ≥2 conjuncts can pay to compact survivors mid-way
        // so the remaining conjuncts scan only the survivors. Anything else
        // (single leaf, OR, NOT) has nothing to compact between.
        switch (self.expr) {
            .@"and" => |children| if (children.len >= 2) return self.nextCompacting(children),
            else => {},
        }

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

    /// Compacting evaluation of an ordered AND of conjuncts. Evaluates one
    /// conjunct at a time against the current (possibly already-compacted)
    /// batch; whenever `shouldCompact` fires it materializes the survivors of
    /// every output column densely, so the remaining conjuncts scan only the
    /// survivors. Identical results to the single-pass path (AND is
    /// commutative; materialization only relocates rows, never changes them).
    fn nextCompacting(self: *Filter, conjuncts: []const PredicateExpr) !?Batch {
        const w = self.schema.len;
        while (true) {
            const upstream_batch = (try self.upstream.next()) orelse return null;
            const n = upstream_batch.row_count;

            // mask + scratch sized to the (largest) upstream batch; we operate
            // on the [0..P] prefix as the working set shrinks on compaction.
            const mask = try self.allocator.alloc(bool, n);
            defer self.allocator.free(mask);
            const scratch = try self.allocator.alloc(bool, n);
            defer self.allocator.free(scratch);

            var cur = Batch{ .schema = self.schema, .values = upstream_batch.values, .row_count = n };
            // `which` names the buffer set free to receive the next compaction.
            var which: u1 = 0;

            try predicate.evaluateExprGuided(self.allocator, conjuncts[0], self.schema, cur, mask[0..n], null);
            var m = popcount(mask[0..cur.row_count]);

            var i: usize = 1;
            while (m > 0 and i < conjuncts.len) : (i += 1) {
                if (shouldCompact(cur.row_count, m, conjuncts.len - i, w)) {
                    cur = try self.compactInto(which, cur, mask[0..cur.row_count], m);
                    which ^= 1;
                    @memset(mask[0..m], true);
                }
                const p = cur.row_count;
                try predicate.evaluateExprGuided(self.allocator, conjuncts[i], self.schema, cur, scratch[0..p], mask[0..p]);
                for (mask[0..p], scratch[0..p]) |*a, b| a.* = a.* and b;
                m = popcount(mask[0..p]);
            }
            if (m == 0) continue;

            const out = try self.compactInto(which, cur, mask[0..cur.row_count], m);
            return out;
        }
    }

    /// Materialize the `survivors` rows of `src` (those set in `mask`) into the
    /// `which` ping-pong buffer set, returning a Batch over that set.
    fn compactInto(self: *Filter, which: u1, src: Batch, mask: []const bool, survivors: usize) !Batch {
        if (which == 1 and self.filtered_b.len == 0) try self.initBufferB();
        const cols = if (which == 0) self.filtered else self.filtered_b;
        const vs = if (which == 0) self.views else self.views_b;
        for (cols) |*c| c.clear();
        for (src.values, 0..) |view, ci| {
            try engine.memtable.appendMaskedColumn(self.allocator, view, mask, &cols[ci]);
        }
        for (cols, 0..) |c, ci| vs[ci] = c.view();
        return Batch{ .schema = self.schema, .values = vs, .row_count = survivors };
    }

    /// Allocate the second ping-pong buffer set on first use.
    fn initBufferB(self: *Filter) !void {
        const fb = try self.allocator.alloc(ColumnStore, self.schema.len);
        errdefer self.allocator.free(fb);
        var inited: usize = 0;
        errdefer for (fb[0..inited]) |*c| c.deinit(self.allocator);
        for (self.schema, 0..) |col, i| {
            fb[i] = try ColumnStore.init(self.allocator, col.type, col.nullable);
            inited += 1;
        }
        const vb = try self.allocator.alloc(ColumnView, self.schema.len);
        self.filtered_b = fb;
        self.views_b = vb;
    }
};

fn popcount(mask: []const bool) usize {
    var c: usize = 0;
    for (mask) |m| c += @intFromBool(m);
    return c;
}

// Compaction cost model (ns/row, relative — only ratios matter). Calibrated
// from a microbench on Zen 5: a remaining conjunct costs ~`W_PRED` per row to
// SIMD-scan + combine; materializing a survivor costs ~`G_COPY` per output
// column; the survivor-count scan costs ~`G_SCAN`. Compaction at survival σ
// over a P-row buffer pays when the rescans it saves on the eliminated rows
// outweigh the extra full-width materialize:
//   (1 − σ)·remaining·W_PRED  >  σ·W·G_COPY + G_SCAN.
const W_PRED: f64 = 0.40;
const G_COPY: f64 = 0.20;
const G_SCAN: f64 = 0.18;
// Below this many rows the per-batch fixed costs dominate any per-row saving —
// never compact a trivially small working set.
const COMPACT_MIN_ROWS: usize = 1024;

fn shouldCompact(physical: usize, survivors: usize, remaining: usize, width: usize) bool {
    if (physical < COMPACT_MIN_ROWS) return false;
    const sigma = @as(f64, @floatFromInt(survivors)) / @as(f64, @floatFromInt(physical));
    const saved = (1.0 - sigma) * @as(f64, @floatFromInt(remaining)) * W_PRED;
    const cost = sigma * @as(f64, @floatFromInt(width)) * G_COPY + G_SCAN;
    return saved > cost;
}

test "shouldCompact gate" {
    // Below the row floor: never, regardless of how favourable the ratio.
    try std.testing.expect(!shouldCompact(500, 1, 4, 2));
    // High survival: the gather costs more than the rescans it saves.
    try std.testing.expect(!shouldCompact(10000, 9500, 2, 4));
    // Low survival + remaining work: pays.
    try std.testing.expect(shouldCompact(10000, 200, 2, 4));
    // More remaining conjuncts lowers the bar (monotone in `remaining`).
    try std.testing.expect(!shouldCompact(10000, 4000, 1, 4));
    try std.testing.expect(shouldCompact(10000, 4000, 4, 4));
    // Wider output raises the bar (monotone in `width`).
    try std.testing.expect(shouldCompact(10000, 2500, 2, 2));
    try std.testing.expect(!shouldCompact(10000, 2500, 2, 16));
}

test "orderPredicate flattens parser-nested AND/OR chains" {
    const allocator = std.testing.allocator;
    const schema = [_]Column{
        .{ .name = "s", .type = .{ .varchar = 64 } },
        .{ .name = "t", .type = .{ .varchar = 64 } },
    };
    const stats = [_]exec.ColStat{ .{}, .{} };

    const leaf: PredicateExpr = .{ .leaf = .{ .col = "s", .op = .neq, .val = .{ .text = "" } } };
    const lk: PredicateExpr = .{ .like = .{ .col = "t", .pattern = "%g%" } };
    var lk2: PredicateExpr = .{ .like = .{ .col = "s", .pattern = "%h%" } };
    const nlk: PredicateExpr = .{ .not = &lk2 };

    // `a AND b AND c` parses left-nested: AND(AND(a, b), c).
    const inner = [_]PredicateExpr{ leaf, lk };
    const outer = [_]PredicateExpr{ .{ .@"and" = &inner }, nlk };

    var rewritten: std.ArrayListUnmanaged([]PredicateExpr) = .empty;
    defer {
        for (rewritten.items) |s| allocator.free(s);
        rewritten.deinit(allocator);
    }
    const ordered = try orderPredicate(allocator, &rewritten, .{ .@"and" = &outer }, &schema, &stats);
    try std.testing.expectEqual(@as(usize, 3), ordered.@"and".len);
    for (ordered.@"and") |c| try std.testing.expect(c != .@"and");

    const or_outer = [_]PredicateExpr{ .{ .@"or" = &inner }, nlk };
    const or_ordered = try orderPredicate(allocator, &rewritten, .{ .@"or" = &or_outer }, &schema, &stats);
    try std.testing.expectEqual(@as(usize, 3), or_ordered.@"or".len);
    for (or_ordered.@"or") |c| try std.testing.expect(c != .@"or");
}

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
/// `.@"or"` recurses with the inverted rules: an always-FALSE disjunct drops
/// (contributes nothing — safe regardless of nullability), an always-TRUE
/// disjunct collapses the whole OR to `.always = true` (reached only via the
/// non-nullable-gated path in `simplifyLeaf`, so admitting every row is safe),
/// all-dropped → `.always = false`, and survivors are ordered cheapest +
/// most-likely-true first. `.not` children are left untouched. `eq` in range,
/// `neq`, LIKE, col-col, subquery forms, and is_null pass through. Returns the
/// (possibly rebuilt) expr; `.always` carries a proven constant outcome.
/// Reorder a validated predicate's AND/OR conjuncts cheapest + most-selective
/// first and fold any conjunct proven always-true/false by `stats` (per-schema
/// column). Commutative — never changes results. Child arrays allocated for
/// reordered nodes are appended to `rewritten` (caller owns + frees them).
/// Pass an all-`.unknown` `stats` to order by kernel cost alone, no folding.
/// Shared by the legacy Filter operator and the fused-scan path.
pub fn orderPredicate(
    allocator: Allocator,
    rewritten: *std.ArrayListUnmanaged([]PredicateExpr),
    expr: PredicateExpr,
    schema: []const Column,
    stats: []const exec.ColStat,
) !PredicateExpr {
    return simplifyPredicate(allocator, rewritten, expr, schema, stats);
}

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
                    // Splice a nested AND's conjuncts into this one: the SQL
                    // parser left-nests `a AND b AND c`, and downstream fast
                    // paths (the scan's guided filter) only engage on a flat
                    // conjunct list. The child was simplified recursively, so
                    // one splice level fully flattens.
                    .@"and" => |inner| try survivors.appendSlice(allocator, inner),
                    else => try survivors.append(allocator, s),
                }
            }
            if (survivors.items.len == 0) {
                survivors.deinit(allocator);
                return .{ .always = true };
            }
            if (survivors.items.len == 1) {
                const only = survivors.items[0];
                survivors.deinit(allocator);
                return only;
            }
            // Order conjuncts cheap+selective first, expensive last (commutative,
            // so results are unchanged) — owns the slice since the borrowed
            // `children` can't be reordered in place.
            std.mem.sort(PredicateExpr, survivors.items, ConjunctSortCtx{ .schema = schema, .stats = stats }, ConjunctSortCtx.lessThan);
            const owned = try survivors.toOwnedSlice(allocator);
            errdefer allocator.free(owned);
            try rewritten.append(allocator, owned);
            return .{ .@"and" = owned };
        },
        .@"or" => |children| {
            var survivors: std.ArrayListUnmanaged(PredicateExpr) = .empty;
            errdefer survivors.deinit(allocator);
            for (children) |child| {
                const s = try simplifyPredicate(allocator, rewritten, child, schema, stats);
                switch (s) {
                    .always => |b| {
                        if (b) {
                            // An always-true disjunct satisfies the whole OR. Only
                            // the #297 non-nullable-gated path produces always-true,
                            // so admitting every row here is safe.
                            survivors.deinit(allocator);
                            return .{ .always = true };
                        }
                        // always-false disjunct contributes nothing — drop it.
                    },
                    // Mirror of the AND splice: flatten parser-nested ORs.
                    .@"or" => |inner| try survivors.appendSlice(allocator, inner),
                    else => try survivors.append(allocator, s),
                }
            }
            if (survivors.items.len == 0) {
                survivors.deinit(allocator);
                return .{ .always = false };
            }
            if (survivors.items.len == 1) {
                const only = survivors.items[0];
                survivors.deinit(allocator);
                return only;
            }
            // Order disjuncts cheapest first, and within a cost class the
            // most-likely-TRUE first so the OR short-circuits early (the
            // evaluator skips already-satisfied rows in later disjuncts).
            std.mem.sort(PredicateExpr, survivors.items, DisjunctSortCtx{ .schema = schema, .stats = stats }, DisjunctSortCtx.lessThan);
            const owned = try survivors.toOwnedSlice(allocator);
            errdefer allocator.free(owned);
            try rewritten.append(allocator, owned);
            return .{ .@"or" = owned };
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

/// Plan-time ordering key for a top-level AND conjunct. We evaluate
/// cheap+selective conjuncts first so the running alive-mask is tightest by the
/// time the expensive ones (LIKE/regex/subquery) run — those alone skip dead
/// rows (cheap int/range kernels SIMD-scan the whole batch regardless of order).
/// Sort by `cost` ascending, then `sel` (pass-fraction) ascending. Selectivity
/// is an NDV/min-max seed (a guess, not a histogram); it only affects order, so
/// a wrong guess costs nothing but a suboptimal evaluation order.
const ConjunctKey = struct { cost: u8, sel: f64 };

fn conjunctKey(expr: PredicateExpr, schema: []const Column, stats: []const exec.ColStat) ConjunctKey {
    return switch (expr) {
        .leaf => |p| leafKey(p, schema, stats),
        .is_null => .{ .cost = 0, .sel = 0.05 },
        .is_not_null => .{ .cost = 0, .sel = 0.95 },
        .leaf_col_col => .{ .cost = 1, .sel = 0.5 },
        .in_set => |s| .{ .cost = 1, .sel = colSelectivity(s.col, schema, stats, @floatFromInt(@max(s.values.len, 1))) },
        .like => .{ .cost = 2, .sel = 0.10 },
        .not => .{ .cost = 2, .sel = 0.5 },
        // Roll a nested group's children up so its real cost/selectivity orders
        // it against its siblings: cost is the costliest child; an AND multiplies
        // pass-fractions, an OR combines them as 1 - ∏(1 - childᵢ).
        .@"and" => |kids| blk: {
            var cost: u8 = 0;
            var sel: f64 = 1.0;
            for (kids) |k| {
                const kk = conjunctKey(k, schema, stats);
                cost = @max(cost, kk.cost);
                sel *= kk.sel;
            }
            break :blk .{ .cost = cost, .sel = std.math.clamp(sel, 0.0, 1.0) };
        },
        .@"or" => |kids| blk: {
            var cost: u8 = 0;
            var none: f64 = 1.0;
            for (kids) |k| {
                const kk = conjunctKey(k, schema, stats);
                cost = @max(cost, kk.cost);
                none *= 1.0 - kk.sel;
            }
            break :blk .{ .cost = cost, .sel = std.math.clamp(1.0 - none, 0.0, 1.0) };
        },
        .scalar_subquery, .exists_subquery, .in_subquery, .correlated_set, .correlated_scalar, .correlated_range => .{ .cost = 3, .sel = 0.5 },
        .always => |b| .{ .cost = 0, .sel = if (b) 1.0 else 0.0 },
        .unknown => .{ .cost = 0, .sel = 0.0 },
        .leaf_var => .{ .cost = 0, .sel = 0.10 },
    };
}

fn leafKey(p: Predicate, schema: []const Column, stats: []const exec.ColStat) ConjunctKey {
    const idx = types.findColumn(schema, p.col) orelse return .{ .cost = 1, .sel = 0.5 };
    // int/date/bool/decimal compares are a cheap SIMD pass; string is memcmp.
    const cost: u8 = if (predicate.typeHasRange(schema[idx].type)) 0 else 1;
    const ndv: f64 = if (idx < stats.len) switch (stats[idx].ndv) {
        .exact => |n| @floatFromInt(@max(n, 1)),
        .unknown => 0,
    } else 0;
    const sel: f64 = switch (p.op) {
        .eq => if (ndv > 0) 1.0 / ndv else 0.10,
        .neq => if (ndv > 0) 1.0 - 1.0 / ndv else 0.90,
        .lt, .lte, .gt, .gte => rangeSelectivity(p, stats, idx),
    };
    return .{ .cost = cost, .sel = sel };
}

/// Uniform-distribution interpolation of a range predicate's pass-fraction from
/// the column's [min,max]. In f64 (lossy but overflow-free; this only orders).
fn rangeSelectivity(p: Predicate, stats: []const exec.ColStat, idx: usize) f64 {
    if (idx >= stats.len) return 0.33;
    const cmin = stats[idx].min orelse return 0.33;
    const cmax = stats[idx].max orelse return 0.33;
    const v = predicate.valueToRangeI128(p.val) orelse return 0.33;
    const fmin: f64 = @floatFromInt(cmin);
    const fmax: f64 = @floatFromInt(cmax);
    if (fmax <= fmin) return 0.5;
    const frac = std.math.clamp((@as(f64, @floatFromInt(v)) - fmin) / (fmax - fmin), 0.0, 1.0);
    return switch (p.op) {
        .lt, .lte => frac,
        .gt, .gte => 1.0 - frac,
        else => 0.33,
    };
}

fn colSelectivity(col: []const u8, schema: []const Column, stats: []const exec.ColStat, k: f64) f64 {
    const idx = types.findColumn(schema, col) orelse return 0.5;
    if (idx >= stats.len) return 0.5;
    return switch (stats[idx].ndv) {
        .exact => |n| std.math.clamp(k / @as(f64, @floatFromInt(@max(n, 1))), 0.0, 1.0),
        .unknown => 0.30,
    };
}

const ConjunctSortCtx = struct {
    schema: []const Column,
    stats: []const exec.ColStat,
    fn lessThan(ctx: ConjunctSortCtx, a: PredicateExpr, b: PredicateExpr) bool {
        const ka = conjunctKey(a, ctx.schema, ctx.stats);
        const kb = conjunctKey(b, ctx.schema, ctx.stats);
        if (ka.cost != kb.cost) return ka.cost < kb.cost;
        return ka.sel < kb.sel;
    }
};

/// Disjunct ordering: cheapest first, then within a cost class the
/// most-likely-TRUE (highest pass-fraction) first, so the OR is satisfied
/// early and the evaluator's short-circuit skips already-true rows in the
/// costlier later disjuncts. OR is commutative, so order never changes results.
const DisjunctSortCtx = struct {
    schema: []const Column,
    stats: []const exec.ColStat,
    fn lessThan(ctx: DisjunctSortCtx, a: PredicateExpr, b: PredicateExpr) bool {
        const ka = conjunctKey(a, ctx.schema, ctx.stats);
        const kb = conjunctKey(b, ctx.schema, ctx.stats);
        if (ka.cost != kb.cost) return ka.cost < kb.cost;
        return ka.sel > kb.sel;
    }
};
