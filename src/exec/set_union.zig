//! UNION ALL — concatenate the row streams from two upstream Queries.
//!
//! v1 supports UNION ALL only. UNION (distinct) would land as a hash-
//! dedup operator on top of this; deferred.
//!
//! Schema unification: both sides must declare the same number of
//! columns with matching type tags at each position. Column *names*
//! may differ — the output adopts the left side's names. Nullability
//! is the union (nullable if either side is).

const std = @import("std");
const Allocator = std.mem.Allocator;

const types = @import("../types.zig");
const Column = types.Column;
const Type = types.Type;
const TypeTag = types.TypeTag;

const storage = @import("../storage/storage.zig");
const ColumnView = storage.ColumnView;
const store = @import("../engine/store.zig");
const ColumnStore = store.ColumnStore;

const exec = @import("exec.zig");
const Query = exec.Query;
const Batch = exec.Batch;
const Error = exec.Error;
const makeQuery = exec.makeQuery;
const cast = @import("cast.zig");
const CastKernel = cast.CastKernel;

const predicate = @import("predicate.zig");
const Predicate = predicate.Predicate;

pub const SetUnion = struct {
    allocator: Allocator,
    left: Query,
    right: Query,
    /// Set to false once the left side runs dry; subsequent next()
    /// calls drain from the right.
    left_exhausted: bool,
    output_schema: []Column,
    left_casts: []?CastKernel,
    right_casts: []?CastKernel,
    left_cast_cols: []ColumnStore,
    right_cast_cols: []ColumnStore,
    views: []ColumnView,
    /// True for UNION ALL; reserved for the future distinct variant.
    /// v1 only constructs `all = true`.
    all: bool,
    /// Per-output-column stats merged from both arms at create. Empty when
    /// neither arm carried information. See `exec.unionColStats`.
    cached_stats: []const exec.ColStat = &.{},
    /// A join's probe sink forwarded through this union (tryFuseProbe): a
    /// fused arm's batches arrive already joined and pass through; a
    /// declined arm's raw batches are probed HERE through the sink on the
    /// calling thread. The arms drain strictly sequentially, so reusing the
    /// sink's chunk-0 scratch for the serial arm never races a worker.
    probe_sink: ?exec.ProbeSink = null,
    left_fused: bool = false,
    right_fused: bool = false,
    /// Remap scratch for the serial-arm probe when the sink carries a
    /// probe_map (a narrowing Project above the join's probe side). A fused
    /// arm's scan applies the map itself; the serial path applies it here.
    probe_map_views: []ColumnView = &.{},

    pub fn create(allocator: Allocator, left: Query, right: Query, all: bool) !Query {
        const left_schema = left.outputSchema();
        const right_schema = right.outputSchema();
        if (left_schema.len != right_schema.len) return Error.TypeMismatch;

        const out_schema = try allocator.alloc(Column, left_schema.len);
        errdefer allocator.free(out_schema);

        const left_casts = try allocator.alloc(?CastKernel, left_schema.len);
        errdefer allocator.free(left_casts);
        const right_casts = try allocator.alloc(?CastKernel, right_schema.len);
        errdefer allocator.free(right_casts);

        const left_cast_cols = try allocator.alloc(ColumnStore, left_schema.len);
        errdefer allocator.free(left_cast_cols);
        var left_cols_inited: usize = 0;
        errdefer for (left_cast_cols[0..left_cols_inited]) |*c| c.deinit(allocator);

        const right_cast_cols = try allocator.alloc(ColumnStore, right_schema.len);
        errdefer allocator.free(right_cast_cols);
        var right_cols_inited: usize = 0;
        errdefer for (right_cast_cols[0..right_cols_inited]) |*c| c.deinit(allocator);

        const views = try allocator.alloc(ColumnView, left_schema.len);
        errdefer allocator.free(views);

        for (left_schema, right_schema, out_schema, left_casts, right_casts, left_cast_cols, right_cast_cols) |l, r, *o, *lk, *rk, *lc, *rc| {
            const out_type = commonUnionType(l.type, r.type) orelse return Error.TypeMismatch;
            const out_nullable = l.nullable or r.nullable;
            o.* = .{ .name = l.name, .type = out_type, .nullable = out_nullable };

            lk.* = castFor(l.type, out_type);
            if (lk.* == null and !sameRepr(l.type, out_type)) return Error.TypeMismatch;
            rk.* = castFor(r.type, out_type);
            if (rk.* == null and !sameRepr(r.type, out_type)) return Error.TypeMismatch;

            lc.* = try ColumnStore.init(allocator, out_type, out_nullable);
            left_cols_inited += 1;
            rc.* = try ColumnStore.init(allocator, out_type, out_nullable);
            right_cols_inited += 1;
        }

        const cached_stats = try exec.unionColStats(allocator, left, right, out_schema.len);
        errdefer if (cached_stats.len > 0) allocator.free(@constCast(cached_stats));

        const self = try allocator.create(SetUnion);
        errdefer allocator.destroy(self);
        self.* = .{
            .allocator = allocator,
            .left = left,
            .right = right,
            .left_exhausted = false,
            .output_schema = out_schema,
            .left_casts = left_casts,
            .right_casts = right_casts,
            .left_cast_cols = left_cast_cols,
            .right_cast_cols = right_cast_cols,
            .views = views,
            .all = all,
            .cached_stats = cached_stats,
        };
        return makeQuery(allocator, self);
    }

    pub fn deinit(self: *SetUnion) void {
        var l = self.left;
        l.deinit();
        var r = self.right;
        r.deinit();
        for (self.left_cast_cols) |*c| c.deinit(self.allocator);
        for (self.right_cast_cols) |*c| c.deinit(self.allocator);
        self.allocator.free(self.output_schema);
        self.allocator.free(self.left_casts);
        self.allocator.free(self.right_casts);
        self.allocator.free(self.left_cast_cols);
        self.allocator.free(self.right_cast_cols);
        self.allocator.free(self.views);
        if (self.cached_stats.len > 0) self.allocator.free(@constCast(self.cached_stats));
        if (self.probe_map_views.len > 0) self.allocator.free(self.probe_map_views);
        const allocator = self.allocator;
        allocator.destroy(self);
    }

    pub fn outputSchema(self: *SetUnion) []const Column {
        // Probe-fused: every emitted batch (worker-joined or serially probed
        // below) carries the JOIN's schema — same live-forward rule as a
        // fused Project/Filter.
        if (self.probe_sink) |sink| return sink.out_schema;
        return self.output_schema;
    }

    /// Forward a join's probe-sink offer into the arms so the probe runs in
    /// their parallel-scan workers instead of serially above this union.
    /// Requires cast-free arms (raw arm batches must be positionally and
    /// representationally valid probe input — the sink's compiled indices
    /// were resolved against our output schema) and no probe_map (each arm
    /// would need its own remap space). Accepts when EITHER arm accepts: a
    /// declined arm still joins correctly — next() probes its raw batches
    /// through the sink on the calling thread, which is exactly the serial
    /// work the caller would otherwise do itself. The sink's bind hooks are
    /// grow-only, so the two accepting scans binding different chunk counts
    /// compose (larger space wins; arms drain sequentially, never racing).
    pub fn tryFuseProbe(self: *SetUnion, sink: exec.ProbeSink) !bool {
        if (self.probe_sink != null) return false;
        for (self.left_casts) |k| if (k != null) return false;
        for (self.right_casts) |k| if (k != null) return false;
        const l = try self.left.tryFuseProbe(sink);
        const r = try self.right.tryFuseProbe(sink);
        if (!l and !r) return false;
        // A fused arm's scan applies sink.probe_map itself; the serial path
        // in nextFused needs its own remap scratch.
        if (sink.probe_map) |m| {
            if (!(l and r)) self.probe_map_views = try self.allocator.alloc(ColumnView, m.len);
        }
        self.probe_sink = sink;
        self.left_fused = l;
        self.right_fused = r;
        return true;
    }

    pub fn addPrune(_: *SetUnion, _: Predicate) !void {
        // Pushdown across UNION would need to clone the predicate down
        // each side; not worth the complication for v1.
    }

    pub fn stats(self: *SetUnion) exec.PipelineStats {
        const l = self.left.stats();
        const r = self.right.stats();
        return .{
            .upper_rows = l.upper_rows +| r.upper_rows,
            .sort_state = .{ .keys = &.{}, .global = false },
            // Fused: cached_stats index the pre-fusion union schema, not the
            // join output the batches now carry.
            .column_stats = if (self.probe_sink != null) &.{} else self.cached_stats,
        };
    }

    pub fn accountant(self: *SetUnion) ?*exec.memory.MemoryAccountant {
        return self.left.accountant() orelse self.right.accountant();
    }

    pub fn explain(self: *SetUnion, out: *std.ArrayList(u8), allocator: std.mem.Allocator, depth: usize) !void {
        try exec.explainLine(out, allocator, depth, if (self.all) "UnionAll" else "Union");
        try self.left.explain(out, allocator, depth + 1);
        try self.right.explain(out, allocator, depth + 1);
    }

    pub fn next(self: *SetUnion) !?Batch {
        if (self.probe_sink) |sink| return self.nextFused(sink);
        if (!self.left_exhausted) {
            if (try self.left.next()) |b| {
                return try rebatched(self, b, self.left_casts, self.left_cast_cols);
            }
            self.left_exhausted = true;
        }
        if (try self.right.next()) |b| {
            return try rebatched(self, b, self.right_casts, self.right_cast_cols);
        }
        return null;
    }

    /// Fused drain: a fused arm's batches are already joined — forward. A
    /// declined arm's raw batches probe through the sink here, looping past
    /// batches the join fully missed on (process → null / zero rows), same
    /// contract the scan workers follow.
    fn nextFused(self: *SetUnion, sink: exec.ProbeSink) !?Batch {
        if (!self.left_exhausted) {
            while (try self.left.next()) |b| {
                if (self.left_fused) return b;
                if (b.row_count == 0) continue;
                if (try sink.process(sink.ctx, 0, self.remapped(sink, b))) |jb| {
                    if (jb.row_count > 0) return jb;
                }
            }
            self.left_exhausted = true;
        }
        while (try self.right.next()) |b| {
            if (self.right_fused) return b;
            if (b.row_count == 0) continue;
            if (try sink.process(sink.ctx, 0, self.remapped(sink, b))) |jb| {
                if (jb.row_count > 0) return jb;
            }
        }
        return null;
    }

    fn remapped(self: *SetUnion, sink: exec.ProbeSink, batch: Batch) Batch {
        const m = sink.probe_map orelse return batch;
        for (m, self.probe_map_views) |src, *v| v.* = batch.values[src];
        return .{ .schema = batch.schema, .values = self.probe_map_views, .row_count = batch.row_count };
    }

    fn rebatched(self: *SetUnion, src: Batch, kernels: []const ?CastKernel, cast_cols: []ColumnStore) !Batch {
        for (src.values, kernels, cast_cols, self.views) |v, k, *cast_col, *out| {
            if (k) |kernel| {
                cast_col.clear();
                const args = [_]ColumnView{v};
                try kernel(self.allocator, &args, cast_col, src.row_count);
                out.* = cast_col.view();
            } else {
                out.* = v;
            }
        }
        return .{
            .schema = self.output_schema,
            .values = self.views,
            .row_count = src.row_count,
        };
    }
};

fn typeTag(t: Type) TypeTag {
    return std.meta.activeTag(t);
}

/// `.string`, `.varchar(n)`, and `.char(n)` share one physical StringView
/// representation, so unioning across them needs no cast — only a common tag.
fn stringFamily(t: TypeTag) bool {
    return t == .string or t == .varchar or t == .char;
}

/// True when `a` and `b` occupy the same physical column representation, so a
/// value of one can flow into an output column typed as the other uncast.
fn sameRepr(a: Type, b: Type) bool {
    const at = typeTag(a);
    const bt = typeTag(b);
    return at == bt or (stringFamily(at) and stringFamily(bt));
}

fn commonUnionType(left: Type, right: Type) ?Type {
    const lt = typeTag(left);
    const rt = typeTag(right);
    if (lt == rt) return left;
    // A `VARCHAR(n)` base column unioned with a `.string` expression result
    // (CONCAT/LOWER/…) reconciles to plain string — same representation.
    if (stringFamily(lt) and stringFamily(rt)) return Type{ .string = {} };
    if (cast.castCost(lt, rt) != null) return right;
    if (cast.castCost(rt, lt) != null) return left;
    return null;
}

fn castFor(from: Type, to: Type) ?CastKernel {
    if (sameRepr(from, to)) return null;
    return cast.kernelFor(typeTag(from), typeTag(to));
}
