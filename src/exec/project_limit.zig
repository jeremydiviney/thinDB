//! Project (column subset / reorder) and Limit (row cap) operators. Both
//! are zero-copy pass-throughs: Project rearranges ColumnView pointers,
//! Limit slices the views to truncate. They share a file because each is
//! small and they have nearly identical lifecycles.

const std = @import("std");
const Allocator = std.mem.Allocator;

const types = @import("../types.zig");
const Column = types.Column;

const storage = @import("../storage/storage.zig");
const ColumnView = storage.ColumnView;

const exec = @import("exec.zig");
const Query = exec.Query;
const Batch = exec.Batch;
const Error = exec.Error;
const makeQuery = exec.makeQuery;

const predicate = @import("predicate.zig");
const Predicate = predicate.Predicate;

pub const Project = struct {
    allocator: Allocator,
    upstream: Query,
    output_schema: []Column,
    owned_names: ?[][]u8 = null,
    column_map: []usize, // output_idx → upstream_idx
    views: []ColumnView,
    /// Join probe fused below (identity projection only): batches arrive
    /// already joined; next() passes them through unmapped.
    probe_fused: bool = false,
    /// Upstream per-column stats remapped to the projected columns (a
    /// passed-through column keeps its ndv + min/max). Empty when the
    /// upstream has none. Cached at create.
    cached_stats: []const exec.ColStat = &.{},
    /// Composed probe remap owned by this projection (an incoming
    /// sink.probe_map composed with column_map in tryFuseProbe).
    owned_probe_map: ?[]usize = null,

    pub fn create(allocator: Allocator, upstream: Query, names: []const []const u8) !Query {
        return createNamed(allocator, upstream, names, null);
    }

    pub fn createNamed(
        allocator: Allocator,
        upstream: Query,
        names: []const []const u8,
        output_names: ?[]const []const u8,
    ) !Query {
        if (output_names) |outs| {
            if (outs.len != names.len) return Error.TypeMismatch;
        }
        const up_schema = upstream.outputSchema();
        const out_schema = try allocator.alloc(Column, names.len);
        errdefer allocator.free(out_schema);
        const column_map = try allocator.alloc(usize, names.len);
        errdefer allocator.free(column_map);
        const views = try allocator.alloc(ColumnView, names.len);
        errdefer allocator.free(views);

        for (names, 0..) |name, i| {
            const idx = types.findColumn(up_schema, name) orelse return Error.ColumnNotFound;
            column_map[i] = idx;
            out_schema[i] = up_schema[idx];
            if (output_names) |outs| out_schema[i].name = outs[i];
            for (out_schema[0..i]) |prior| {
                if (types.columnNameEql(prior.name, out_schema[i].name)) return Error.ComputeNameCollision;
            }
        }
        // Identity elision: keeps every upstream column in place with its
        // exact label — the operator would be a pure pull-through frame, so
        // don't build it. Labels compare by bytes, not columnNameEql: a
        // case-only "rename" is a real relabel on the wire.
        const identity = blk: {
            if (names.len != up_schema.len) break :blk false;
            for (column_map, 0..) |src, i| {
                if (src != i) break :blk false;
            }
            if (output_names) |outs| {
                for (outs, 0..) |o, i| {
                    if (!std.mem.eql(u8, o, up_schema[i].name)) break :blk false;
                }
            }
            break :blk true;
        };
        if (identity) {
            allocator.free(views);
            allocator.free(column_map);
            allocator.free(out_schema);
            return upstream;
        }

        const owned_names: ?[][]u8 = if (output_names != null) try cloneOutputNames(allocator, out_schema) else null;
        errdefer if (owned_names) |owned| freeOutputNames(allocator, owned);

        // Remap per-column stats to the projected column order.
        const up = upstream.stats();
        const cached_stats: []const exec.ColStat = if (up.column_stats.len == 0) &.{} else blk: {
            const cc = try allocator.alloc(exec.ColStat, names.len);
            for (column_map, cc) |src, *out| out.* = if (src < up.column_stats.len) up.column_stats[src] else .{ .ndv = .unknown };
            exec.capColStats(cc, up.upper_rows);
            break :blk cc;
        };
        errdefer if (cached_stats.len > 0) allocator.free(@constCast(cached_stats));

        const self = try allocator.create(Project);
        errdefer allocator.destroy(self);
        self.* = .{
            .allocator = allocator,
            .upstream = upstream,
            .output_schema = out_schema,
            .owned_names = owned_names,
            .column_map = column_map,
            .views = views,
            .cached_stats = cached_stats,
        };
        return makeQuery(allocator, self);
    }

    pub fn deinit(self: *Project) void {
        var up = self.upstream;
        up.deinit();
        if (self.owned_names) |owned| freeOutputNames(self.allocator, owned);
        self.allocator.free(self.output_schema);
        self.allocator.free(self.column_map);
        self.allocator.free(self.views);
        if (self.cached_stats.len > 0) self.allocator.free(@constCast(self.cached_stats));
        if (self.owned_probe_map) |m| self.allocator.free(m);
        const allocator = self.allocator;
        allocator.destroy(self);
    }

    pub fn outputSchema(self: *Project) []const Column {
        // Probe-fused pass-through: report the upstream's CURRENT schema
        // live (joined batches, or partial-aggregate batches once a
        // two-phase GROUP BY fuses below) — same rule as a fused Filter.
        if (self.probe_fused) return self.upstream.outputSchema();
        return self.output_schema;
    }

    pub fn addPrune(self: *Project, pred: Predicate) !void {
        return self.upstream.addPrune(pred);
    }

    /// Project keeps row count unchanged. Sort state survives as long
    /// as every key in the upstream's sort_state.keys is still in the
    /// output schema; otherwise the sort claim is truncated to the
    /// leading prefix that survived (a sort by `(a,b,c)` with `b`
    /// projected away yields a stream sorted only by `a`).
    pub fn stats(self: *Project) exec.PipelineStats {
        const up = self.upstream.stats();
        var kept_prefix_len: usize = 0;
        outer: for (up.sort_state.keys) |key| {
            for (self.output_schema) |c| {
                if (@import("../types.zig").columnNameEql(c.name, key)) {
                    kept_prefix_len += 1;
                    continue :outer;
                }
            }
            break; // first missing key truncates the sort claim
        }
        return .{
            .upper_rows = up.upper_rows,
            .sort_state = .{
                .keys = up.sort_state.keys[0..kept_prefix_len],
                .descs = if (up.sort_state.descs.len == 0) &.{} else up.sort_state.descs[0..kept_prefix_len],
                .global = up.sort_state.global,
            },
            .column_stats = self.cached_stats,
        };
    }

    pub fn accountant(self: *Project) ?*exec.memory.MemoryAccountant {
        return self.upstream.accountant();
    }

    pub fn explain(self: *Project, out: *std.ArrayList(u8), allocator: std.mem.Allocator, depth: usize) !void {
        try exec.explainLine(out, allocator, depth, "Project");
        try self.upstream.explain(out, allocator, depth + 1);
    }

    /// Forward a join-probe offer downward. The Join resolved its probe
    /// key/gather indices against OUR output order, so a narrowing or
    /// reordering projection hands the accepting scan its column_map via
    /// `sink.probe_map` — the scan remaps each batch's values before the
    /// sink sees them, replacing the remap next() would have done. A sink
    /// that already carries a map (a projection higher in the chain, e.g.
    /// across a UNION arm boundary) COMPOSES: its indices address our
    /// output, ours address the upstream, so `column_map[m[i]]` addresses
    /// the upstream directly.
    pub fn tryFuseProbe(self: *Project, sink: exec.ProbeSink) !bool {
        var fwd = sink;
        const identity = blk: {
            if (self.column_map.len != self.upstream.outputSchema().len) break :blk false;
            for (self.column_map, 0..) |src, dst| if (src != dst) break :blk false;
            break :blk true;
        };
        if (!identity) {
            if (sink.probe_map) |m| {
                const composed = try self.allocator.alloc(usize, m.len);
                errdefer self.allocator.free(composed);
                for (m, composed) |mi, *c| c.* = self.column_map[mi];
                const ok = try self.upstream.tryFuseProbe(blk2: {
                    fwd.probe_map = composed;
                    break :blk2 fwd;
                });
                if (!ok) {
                    self.allocator.free(composed);
                    return false;
                }
                self.owned_probe_map = composed;
                self.probe_fused = true;
                return true;
            }
            fwd.probe_map = self.column_map;
        }
        const ok = try self.upstream.tryFuseProbe(fwd);
        if (ok) self.probe_fused = true;
        return ok;
    }

    pub fn probeFusionReachable(self: *const Project) bool {
        return self.upstream.probeFusionReachable();
    }

    pub fn rechainProbeSink(self: *Project, sink: exec.ProbeSink) !bool {
        if (!self.probe_fused) return false;
        return self.upstream.rechainProbeSink(sink);
    }

    /// Same probe-fused passthrough rule as AliasRename: post-fusion the
    /// batches are join-schema'd, so the offer belongs to the scan below.
    pub fn tryFuseAggregate(self: *Project, group_cols: []const []const u8, aggs: []const exec.AggSpec) !bool {
        if (!self.probe_fused) return false;
        return self.upstream.tryFuseAggregate(group_cols, aggs);
    }

    /// A projection only keeps/renames columns — a filter commutes with it
    /// as long as no column the predicate uses was RENAMED here (its source
    /// exists below, but under the old name). Forward so the scan can
    /// evaluate (and prune on) the predicate before wider columns decode.
    pub fn tryFuseFilter(self: *Project, expr: exec.predicate.PredicateExpr) !bool {
        if (self.probe_fused) return false;
        const up_schema = self.upstream.outputSchema();
        for (self.output_schema, self.column_map) |out, src| {
            if (types.columnNameEql(out.name, up_schema[src].name)) continue;
            if (exec.predicate.touchesColumn(expr, out.name)) return false;
        }
        return self.upstream.tryFuseFilter(expr);
    }

    pub fn next(self: *Project) !?Batch {
        const batch = (try self.upstream.next()) orelse return null;
        if (self.probe_fused) return batch;
        for (self.column_map, 0..) |src_idx, dst_idx| {
            self.views[dst_idx] = batch.values[src_idx];
        }
        return Batch{
            .schema = self.output_schema,
            .values = self.views,
            .row_count = batch.row_count,
        };
    }
};

fn cloneOutputNames(allocator: Allocator, out_schema: []Column) ![][]u8 {
    const names = try allocator.alloc([]u8, out_schema.len);
    errdefer allocator.free(names);
    var inited: usize = 0;
    errdefer {
        for (names[0..inited]) |name| allocator.free(name);
    }
    for (out_schema, 0..) |*col, i| {
        const duped = try allocator.dupe(u8, col.name);
        names[i] = duped;
        inited += 1;
        col.name = duped;
    }
    return names;
}

fn freeOutputNames(allocator: Allocator, names: [][]u8) void {
    for (names) |name| allocator.free(name);
    allocator.free(names);
}

pub const Limit = struct {
    allocator: Allocator,
    upstream: Query,
    remaining: usize,
    /// Rows still to skip before yielding (SQL OFFSET). Decremented as
    /// upstream batches are consumed; the batch that straddles the
    /// boundary has its leading rows dropped via `dropFront`.
    skip: usize,
    /// We materialize truncated batches into local buffers when the upstream
    /// batch overshoots. We slice the view's slices.
    views: []ColumnView,
    /// Output views for the offset-boundary batch after leading rows are
    /// dropped. Separate from `views` so a boundary batch can be both
    /// front-dropped and tail-truncated in one `next()`.
    drop_views: []ColumnView,
    /// Per-column scratch for the shifted null bitmap of the boundary
    /// batch. Each starts empty; allocated lazily only for nullable
    /// columns when the offset boundary is mid-batch.
    null_scratch: [][]u8,
    /// Upstream per-column stats with each ndv re-capped at this Limit's
    /// row bound. Empty when the upstream carries no array. Cached at
    /// create; borrowed by `stats()`.
    cached_stats: []const exec.ColStat = &.{},

    pub fn create(allocator: Allocator, upstream: Query, n: usize) !Query {
        return createOffset(allocator, upstream, n, 0);
    }

    pub fn createOffset(allocator: Allocator, upstream: Query, n: usize, offset: usize) !Query {
        const schema = upstream.outputSchema();
        const views = try allocator.alloc(ColumnView, schema.len);
        errdefer allocator.free(views);
        const drop_views = try allocator.alloc(ColumnView, schema.len);
        errdefer allocator.free(drop_views);
        const null_scratch = try allocator.alloc([]u8, schema.len);
        errdefer allocator.free(null_scratch);
        for (null_scratch) |*s| s.* = &.{};

        // Recompute the per-column ndv cap from the post-limit row bound.
        const up = upstream.stats();
        const upper = @min(@as(u64, n), up.upper_rows);
        const cached_stats: []const exec.ColStat = if (up.column_stats.len == 0) &.{} else blk: {
            const cs = try allocator.alloc(exec.ColStat, up.column_stats.len);
            @memcpy(cs, up.column_stats);
            exec.capColStats(cs, upper);
            break :blk cs;
        };
        errdefer if (cached_stats.len > 0) allocator.free(@constCast(cached_stats));

        const self = try allocator.create(Limit);
        errdefer allocator.destroy(self);
        self.* = .{
            .allocator = allocator,
            .upstream = upstream,
            .remaining = n,
            .skip = offset,
            .views = views,
            .drop_views = drop_views,
            .null_scratch = null_scratch,
            .cached_stats = cached_stats,
        };
        return makeQuery(allocator, self);
    }

    pub fn deinit(self: *Limit) void {
        var up = self.upstream;
        up.deinit();
        if (self.cached_stats.len > 0) self.allocator.free(@constCast(self.cached_stats));
        for (self.null_scratch) |s| if (s.len > 0) self.allocator.free(s);
        self.allocator.free(self.null_scratch);
        self.allocator.free(self.drop_views);
        self.allocator.free(self.views);
        const allocator = self.allocator;
        allocator.destroy(self);
    }

    pub fn outputSchema(self: *Limit) []const Column {
        return self.upstream.outputSchema();
    }

    pub fn addPrune(self: *Limit, pred: Predicate) !void {
        return self.upstream.addPrune(pred);
    }

    /// Limit clamps row count to `min(n, upstream.upper)`. Sort state
    /// preserved (Limit just truncates, doesn't reorder). Per-column ndv
    /// re-capped at the post-limit bound (cached at create); min/max
    /// unchanged.
    pub fn stats(self: *Limit) exec.PipelineStats {
        const up = self.upstream.stats();
        return .{
            .upper_rows = @min(@as(u64, self.remaining), up.upper_rows),
            .sort_state = up.sort_state,
            .column_stats = if (self.cached_stats.len > 0) self.cached_stats else up.column_stats,
        };
    }

    pub fn accountant(self: *Limit) ?*exec.memory.MemoryAccountant {
        return self.upstream.accountant();
    }

    pub fn explain(self: *Limit, out: *std.ArrayList(u8), allocator: std.mem.Allocator, depth: usize) !void {
        try exec.explainLine(out, allocator, depth, "Limit");
        try self.upstream.explain(out, allocator, depth + 1);
    }

    pub fn next(self: *Limit) !?Batch {
        if (self.remaining == 0) return null;

        while (true) {
            const batch = (try self.upstream.next()) orelse return null;

            // Skip phase: drop whole batches that fall entirely within the
            // offset, then drop the leading rows of the straddling batch.
            if (self.skip >= batch.row_count) {
                self.skip -= batch.row_count;
                continue;
            }
            var src_views = batch.values;
            var avail = batch.row_count;
            if (self.skip > 0) {
                const k = self.skip;
                avail = batch.row_count - k;
                for (batch.values, 0..) |v, i| {
                    self.drop_views[i] = try self.dropFront(v, k, avail, i);
                }
                src_views = self.drop_views;
                self.skip = 0;
            }

            // Limit phase.
            if (avail <= self.remaining) {
                self.remaining -= avail;
                return Batch{ .schema = batch.schema, .values = src_views, .row_count = avail };
            }
            const take = self.remaining;
            for (src_views, 0..) |v, i| {
                self.views[i] = truncateView(v, take);
            }
            self.remaining = 0;
            return Batch{ .schema = batch.schema, .values = self.views, .row_count = take };
        }
    }

    /// Drop the first `k` rows of `view`, keeping `n` rows. Fixed-width
    /// columns and var-length columns (whose offsets are absolute byte
    /// positions) reslice directly. A null bitmap needs its bits shifted
    /// left by `k`, which is built into per-column scratch.
    fn dropFront(self: *Limit, view: ColumnView, k: usize, n: usize, col_idx: usize) !ColumnView {
        const new_data: storage.column.ValueView = switch (view.data) {
            .int => |s| .{ .int = s[k .. k + n] },
            .bigint => |s| .{ .bigint = s[k .. k + n] },
            .boolean => |s| .{ .boolean = s[k .. k + n] },
            .varchar => |sv| .{ .varchar = .{ .offsets = sv.offsets[k .. k + n + 1], .bytes = sv.bytes } },
            .string => |sv| .{ .string = .{ .offsets = sv.offsets[k .. k + n + 1], .bytes = sv.bytes } },
            .float => |s| .{ .float = s[k .. k + n] },
            .double => |s| .{ .double = s[k .. k + n] },
            .date => |s| .{ .date = s[k .. k + n] },
            .datetime => |s| .{ .datetime = s[k .. k + n] },
            .tinyint => |s| .{ .tinyint = s[k .. k + n] },
            .smallint => |s| .{ .smallint = s[k .. k + n] },
            .largeint => |s| .{ .largeint = s[k .. k + n] },
            .char => |sv| .{ .char = .{ .offsets = sv.offsets[k .. k + n + 1], .bytes = sv.bytes } },
            .json => |sv| .{ .json = .{ .offsets = sv.offsets[k .. k + n + 1], .bytes = sv.bytes } },
            .decimal64 => |s| .{ .decimal64 = s[k .. k + n] },
            .decimal128 => |s| .{ .decimal128 = s[k .. k + n] },
            .uuid => |s| .{ .uuid = s[k .. k + n] },
        };
        var new_nulls: ?[]const u8 = null;
        if (view.nulls) |src| {
            const buf = try self.ensureNullScratch(col_idx, n);
            @memset(buf, 0);
            var j: usize = 0;
            while (j < n) : (j += 1) {
                if (storage.column.isValidBit(src, k + j)) {
                    storage.column.setValidBit(buf, j, true);
                }
            }
            new_nulls = buf;
        }
        return .{ .data = new_data, .nulls = new_nulls };
    }

    fn ensureNullScratch(self: *Limit, col_idx: usize, rows: usize) ![]u8 {
        const need = storage.column.bitmapBytes(rows);
        if (self.null_scratch[col_idx].len < need) {
            if (self.null_scratch[col_idx].len > 0) self.allocator.free(self.null_scratch[col_idx]);
            self.null_scratch[col_idx] = try self.allocator.alloc(u8, need);
        }
        return self.null_scratch[col_idx][0..need];
    }

    fn truncateView(view: ColumnView, n: usize) ColumnView {
        const new_data: storage.column.ValueView = switch (view.data) {
            .int => |s| .{ .int = s[0..n] },
            .bigint => |s| .{ .bigint = s[0..n] },
            .boolean => |s| .{ .boolean = s[0..n] },
            .varchar => |sv| .{ .varchar = .{
                .offsets = sv.offsets[0 .. n + 1],
                .bytes = sv.bytes[0..sv.offsets[n]],
            } },
            .string => |sv| .{ .string = .{
                .offsets = sv.offsets[0 .. n + 1],
                .bytes = sv.bytes[0..sv.offsets[n]],
            } },
            .float => |s| .{ .float = s[0..n] },
            .double => |s| .{ .double = s[0..n] },
            .date => |s| .{ .date = s[0..n] },
            .datetime => |s| .{ .datetime = s[0..n] },
            .tinyint => |s| .{ .tinyint = s[0..n] },
            .smallint => |s| .{ .smallint = s[0..n] },
            .largeint => |s| .{ .largeint = s[0..n] },
            .char => |sv| .{ .char = .{
                .offsets = sv.offsets[0 .. n + 1],
                .bytes = sv.bytes[0..sv.offsets[n]],
            } },
            .json => |sv| .{ .json = .{
                .offsets = sv.offsets[0 .. n + 1],
                .bytes = sv.bytes[0..sv.offsets[n]],
            } },
            .decimal64 => |s| .{ .decimal64 = s[0..n] },
            .decimal128 => |s| .{ .decimal128 = s[0..n] },
            .uuid => |s| .{ .uuid = s[0..n] },
        };
        return .{ .data = new_data, .nulls = if (view.nulls) |b| b[0..storage.column.bitmapBytes(n)] else null };
    }
};
