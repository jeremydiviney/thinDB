//! Top-N operator — bounded-memory `ORDER BY ... LIMIT k [OFFSET m]`.
//!
//! A full Sort holds every input row before it can emit; for an
//! `ORDER BY ... LIMIT k` that's wasteful and can blow the query memory
//! budget on a large input. Top-N instead keeps only the `keep = limit +
//! offset` rows it might emit.
//!
//! Warm-up: accumulate upstream batches until the buffer first grows past
//! `2 * keep`, then sort and truncate back to the best `keep` rows. The
//! last (worst) kept row becomes the *threshold* — the cut line below which
//! a row can never be emitted.
//!
//! Steady state: with a threshold in hand, each subsequent row is tested
//! against the worst-kept row with one lexicographic comparison; rows
//! strictly worse than it are skipped (no copy, no re-sort). Only genuine
//! candidates are appended, and the buffer is re-sorted + truncated back to
//! `keep` only on batches that actually contributed a candidate. So the
//! per-row cost is ~one compare, not a full re-sort per batch, and only
//! candidates consume the query budget. Dropped rows' bytes are released
//! back to the accountant, so memory stays O(keep) regardless of input size.
//!
//! The planner fuses `Limit(OrderBy(X))` into a single `TopN(X)` — see
//! the `.limit` compile path in net/local.zig.

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
const Error = exec.Error;
const makeQuery = exec.makeQuery;

const predicate = @import("predicate.zig");
const Predicate = predicate.Predicate;
const SortSpec = @import("sort.zig").SortSpec;

pub const TopN = struct {
    allocator: Allocator,
    upstream: Query,
    schema: []const Column,

    sort_col_indices: []usize,
    sort_desc: []bool,
    /// Sort-key column names for the output sort claim (direction in
    /// `sort_desc`). Allocated once at create-time.
    sort_state_keys: []const []const u8,

    /// Rows we might emit = limit + offset. The buffer is pruned back to
    /// this on warm-up (past `2 * keep`) and thereafter whenever a candidate
    /// batch arrives.
    keep: usize,
    limit: usize,
    offset: usize,
    row_bytes: usize,

    accumulated: []ColumnStore,
    accumulated_rows: u64 = 0,
    /// Row bytes currently reserved against the query accountant. Tracked
    /// so prune can release exactly the dropped rows' share.
    reserved_bytes: usize = 0,
    /// Index into `accumulated` of the worst-kept row (the `keep`-th best),
    /// valid only while `threshold_active`. A row strictly worse than this
    /// can never be emitted, so it's skipped without a copy. Set by `prune`
    /// (which leaves `accumulated` sorted, so the worst is the last row).
    worst_idx: u32 = 0,
    /// True once `accumulated` holds a full `keep` rows and has been sorted,
    /// so `worst_idx` is a meaningful cut line. Cleared whenever a candidate
    /// is appended (the buffer is dirty until the next prune re-establishes
    /// the threshold).
    threshold_active: bool = false,

    drained: bool = false,
    evicted: bool = false,
    perm: []u32 = &.{},
    emit_cursor: usize = 0,
    emit_end: usize = 0,

    output_columns: []ColumnStore,
    views: []ColumnView,
    /// Upstream per-column stats with each ndv re-capped at the bounded
    /// `limit + offset` row count. Empty when the upstream carries no array.
    /// Cached at create; borrowed by `stats()`.
    cached_stats: []const exec.ColStat = &.{},

    const batch_size: usize = 1024;

    pub fn create(
        allocator: Allocator,
        upstream: Query,
        sort_specs: []const SortSpec,
        limit: usize,
        offset: usize,
    ) !Query {
        if (sort_specs.len == 0) return Error.SortNoKeys;
        const schema = upstream.outputSchema();

        const sort_col_indices = try allocator.alloc(usize, sort_specs.len);
        errdefer allocator.free(sort_col_indices);
        const sort_desc = try allocator.alloc(bool, sort_specs.len);
        errdefer allocator.free(sort_desc);
        for (sort_specs, 0..) |spec, i| {
            sort_col_indices[i] = types.findColumn(schema, spec.col) orelse return Error.ColumnNotFound;
            sort_desc[i] = spec.desc;
        }
        const sort_state_keys = try allocator.alloc([]const u8, sort_specs.len);
        errdefer allocator.free(sort_state_keys);
        for (sort_col_indices, 0..) |idx, i| sort_state_keys[i] = schema[idx].name;

        const accumulated = try allocator.alloc(ColumnStore, schema.len);
        errdefer allocator.free(accumulated);
        var inited: usize = 0;
        errdefer for (accumulated[0..inited]) |*c| c.deinit(allocator);
        for (schema, 0..) |col, i| {
            accumulated[i] = try ColumnStore.init(allocator, col.type, col.nullable);
            inited += 1;
        }

        const output_columns = try allocator.alloc(ColumnStore, schema.len);
        errdefer allocator.free(output_columns);
        var oinited: usize = 0;
        errdefer for (output_columns[0..oinited]) |*c| c.deinit(allocator);
        for (schema, 0..) |col, i| {
            output_columns[i] = try ColumnStore.init(allocator, col.type, col.nullable);
            oinited += 1;
        }

        const views = try allocator.alloc(ColumnView, schema.len);
        errdefer allocator.free(views);

        const self = try allocator.create(TopN);
        errdefer allocator.destroy(self);

        // keep = limit + offset, saturating (a pathologically huge LIMIT
        // just degrades to a full sort — no pruning ever fires).
        const keep = std.math.add(usize, limit, offset) catch std.math.maxInt(usize);

        // Re-cap per-column ndv at the bounded output row count.
        const up = upstream.stats();
        const upper = @min(@as(u64, limit), up.upper_rows);
        const cached_stats: []const exec.ColStat = if (up.column_stats.len == 0) &.{} else blk: {
            const cs = try allocator.alloc(exec.ColStat, up.column_stats.len);
            @memcpy(cs, up.column_stats);
            exec.capColStats(cs, upper);
            break :blk cs;
        };
        errdefer if (cached_stats.len > 0) allocator.free(@constCast(cached_stats));

        self.* = .{
            .allocator = allocator,
            .upstream = upstream,
            .schema = schema,
            .sort_col_indices = sort_col_indices,
            .sort_desc = sort_desc,
            .sort_state_keys = sort_state_keys,
            .keep = keep,
            .limit = limit,
            .offset = offset,
            .row_bytes = exec.memory.estimateRowBytes(schema),
            .accumulated = accumulated,
            .output_columns = output_columns,
            .views = views,
            .cached_stats = cached_stats,
        };
        return makeQuery(allocator, self);
    }

    pub fn deinit(self: *TopN) void {
        var up = self.upstream;
        up.deinit();
        if (self.cached_stats.len > 0) self.allocator.free(@constCast(self.cached_stats));
        if (!self.evicted) {
            for (self.accumulated) |*c| c.deinit(self.allocator);
            self.allocator.free(self.accumulated);
            if (self.perm.len > 0) self.allocator.free(self.perm);
        }
        for (self.output_columns) |*c| c.deinit(self.allocator);
        self.allocator.free(self.output_columns);
        self.allocator.free(self.views);
        self.allocator.free(self.sort_col_indices);
        self.allocator.free(self.sort_desc);
        self.allocator.free(self.sort_state_keys);
        const allocator = self.allocator;
        allocator.destroy(self);
    }

    pub fn outputSchema(self: *TopN) []const Column {
        return self.schema;
    }

    pub fn addPrune(self: *TopN, pred: Predicate) !void {
        return self.upstream.addPrune(pred);
    }

    /// Top-N output is globally sorted on its keys (in `sort_desc`
    /// directions) — it sorts the kept window before emitting.
    pub fn stats(self: *TopN) exec.PipelineStats {
        const up = self.upstream.stats();
        return .{
            .upper_rows = @min(@as(u64, self.limit), up.upper_rows),
            .sort_state = .{
                .keys = self.sort_state_keys,
                .descs = self.sort_desc,
                .global = true,
            },
            // Keeping a subset of rows can only shrink distinct counts, so
            // the input's per-column stats stay valid; ndv re-capped at the
            // bounded output row count (cached at create).
            .column_stats = if (self.cached_stats.len > 0) self.cached_stats else up.column_stats,
        };
    }

    pub fn accountant(self: *TopN) ?*exec.memory.MemoryAccountant {
        return self.upstream.accountant();
    }

    pub fn explain(self: *TopN, out: *std.ArrayList(u8), allocator: std.mem.Allocator, depth: usize) !void {
        try exec.explainLine(out, allocator, depth, "TopN (bounded sort)");
        try self.upstream.explain(out, allocator, depth + 1);
    }

    fn comparator(self: *TopN, accumulated: []const ColumnStore) Comparator {
        return .{ .accumulated = accumulated, .indices = self.sort_col_indices, .desc = self.sort_desc };
    }

    const Comparator = struct {
        accumulated: []const ColumnStore,
        indices: []const usize,
        desc: []const bool,

        pub fn lessThan(ctx: @This(), a: u32, b: u32) bool {
            for (ctx.indices, 0..) |ci, i| {
                const ord = engine.transform.compareInColumnNullsFirst(ctx.accumulated[ci], a, b);
                if (ord == .lt) return !ctx.desc[i];
                if (ord == .gt) return ctx.desc[i];
            }
            return false;
        }
    };

    /// Compare raw values at row `a` of `va` against row `b` of `vb`,
    /// where both views address the *same* logical column type. Mirrors
    /// `engine.memtable.compareInColumn`'s per-type ordering (numeric for
    /// scalars, byte-lexicographic for strings) but works across two
    /// independent views/stores rather than two indices into one. NULLs are
    /// compared by their placeholder value, exactly as the in-store
    /// comparator does — keeping the bounded path byte-identical to the
    /// full-sort path, which also ignores the null bitmap.
    fn compareAcrossViews(va: ColumnView, a: usize, vb: ColumnView, b: usize) std.math.Order {
        return switch (va.data) {
            .int => |s| std.math.order(s[a], vb.data.int[b]),
            .bigint => |s| std.math.order(s[a], vb.data.bigint[b]),
            .boolean => |s| std.math.order(s[a], vb.data.boolean[b]),
            .varchar => |s| std.mem.order(u8, s.rowBytes(a), vb.data.varchar.rowBytes(b)),
            .string => |s| std.mem.order(u8, s.rowBytes(a), vb.data.string.rowBytes(b)),
            .char => |s| std.mem.order(u8, s.rowBytes(a), vb.data.char.rowBytes(b)),
            .tinyint => |s| std.math.order(s[a], vb.data.tinyint[b]),
            .smallint => |s| std.math.order(s[a], vb.data.smallint[b]),
            .largeint => |s| std.math.order(s[a], vb.data.largeint[b]),
            .float => |s| types.floatOrder(s[a], vb.data.float[b]),
            .double => |s| types.floatOrder(s[a], vb.data.double[b]),
            .date => |s| std.math.order(s[a], vb.data.date[b]),
            .datetime => |s| std.math.order(s[a], vb.data.datetime[b]),
            .decimal64 => |s| std.math.order(s[a], vb.data.decimal64[b]),
            .decimal128 => |s| std.math.order(s[a], vb.data.decimal128[b]),
            .uuid => |s| std.math.order(s[a], vb.data.uuid[b]),
        };
    }

    /// True when batch row `row` (addressed through `batch_views`) is a
    /// candidate for the kept set — i.e. it is STRICTLY better than the current
    /// worst-kept row, which lives at `worst_idx` in `accumulated`. Only reached
    /// once the buffer is full (`threshold_active`), so a row that merely TIES
    /// the worst-kept can't improve the answer — and accepting it would append
    /// an equivalent row and force a re-sort on every such tie, turning a
    /// heavily-tied key (e.g. `ORDER BY x LIMIT k` where many rows share x) into
    /// O(rows) buffer churn. Reject ties: the kept set already holds `keep` rows
    /// no worse, and tie order beyond the boundary is unspecified anyway.
    fn isCandidate(self: *TopN, batch_views: []const ColumnView, row: usize, worst_idx: u32) bool {
        for (self.sort_col_indices, 0..) |ci, i| {
            const ord = compareAcrossViews(
                self.accumulated[ci].view(),
                worst_idx,
                batch_views[ci],
                row,
            );
            // ord = threshold-vs-row on this key. Translate to sort order:
            // ascending — threshold < row means row sorts after ⇒ reject.
            if (ord == .lt) return self.sort_desc[i];
            if (ord == .gt) return !self.sort_desc[i];
        }
        return false; // exact tie on all keys ⇒ not strictly better ⇒ reject.
    }

    /// Free the bounded buffer + permutation and release the remaining
    /// reserved bytes once all kept rows have been emitted. Idempotent.
    fn evict(self: *TopN) void {
        if (self.evicted) return;
        for (self.accumulated) |*c| c.deinit(self.allocator);
        self.allocator.free(self.accumulated);
        self.accumulated = &.{};
        if (self.perm.len > 0) self.allocator.free(self.perm);
        self.perm = &.{};
        if (self.upstream.accountant()) |a| a.release(.topn, self.reserved_bytes);
        self.reserved_bytes = 0;
        self.evicted = true;
    }

    pub fn next(self: *TopN) !?Batch {
        if (!self.drained) try self.drainAndSelect();

        if (self.emit_cursor >= self.emit_end) {
            self.evict();
            return null;
        }
        const remaining = self.emit_end - self.emit_cursor;
        const n: usize = @min(batch_size, remaining);

        for (self.output_columns) |*c| c.clear();
        for (self.output_columns, 0..) |*out, ci| {
            try engine.memtable.appendByIndices(
                self.allocator,
                self.accumulated[ci].view(),
                self.perm[self.emit_cursor .. self.emit_cursor + n],
                out,
            );
        }
        for (self.output_columns, 0..) |c, i| self.views[i] = c.view();
        self.emit_cursor += n;
        return Batch{ .schema = self.schema, .values = self.views, .row_count = n };
    }

    fn drainAndSelect(self: *TopN) !void {
        const acc = self.upstream.accountant();
        const prune_threshold = std.math.mul(usize, self.keep, 2) catch std.math.maxInt(usize);

        while (try self.upstream.next()) |batch| {
            // A pathologically huge LIMIT (keep == maxInt) never prunes —
            // accumulate everything and fall back to a single final sort.
            if (self.keep == 0 or !self.threshold_active) {
                const b = batch.row_count * self.row_bytes;
                if (acc) |a| try a.reserve(.topn, b);
                self.reserved_bytes += b;
                for (batch.values, 0..) |view, ci| {
                    try engine.memtable.appendAllColumn(self.allocator, view, &self.accumulated[ci]);
                }
                self.accumulated_rows += batch.row_count;
                if (self.keep > 0 and self.accumulated_rows > prune_threshold) {
                    try self.prune(acc);
                }
                continue;
            }

            // Threshold active: only rows that aren't strictly worse than the
            // current worst-kept can ever be emitted. Skip the rest — no copy,
            // no per-batch re-sort — paying just one comparison per input row.
            var added: usize = 0;
            for (0..batch.row_count) |row| {
                if (!self.isCandidate(batch.values, row, self.worst_idx)) continue;
                if (acc) |a| try a.reserve(.topn, self.row_bytes);
                self.reserved_bytes += self.row_bytes;
                for (batch.values, 0..) |view, ci| {
                    try engine.memtable.appendOneRow(self.allocator, view, row, &self.accumulated[ci]);
                }
                added += 1;
            }
            self.accumulated_rows += added;
            // Only re-sort when this batch actually contributed candidates;
            // re-prune back to `keep` so the buffer stays O(keep) and a fresh
            // `worst_idx` cut line is established for the next batch.
            if (added > 0) try self.prune(acc);
        }

        const n: usize = @intCast(self.accumulated_rows);
        if (n == 0) {
            self.drained = true;
            return;
        }
        self.perm = try self.allocator.alloc(u32, n);
        for (self.perm, 0..) |*p, i| p.* = @intCast(i);
        std.sort.pdq(u32, self.perm, self.comparator(self.accumulated), Comparator.lessThan);

        // Emit window: rows [offset, offset+limit), clamped to n.
        self.emit_cursor = @min(self.offset, n);
        const end = std.math.add(usize, self.offset, self.limit) catch n;
        self.emit_end = @min(end, n);
        self.drained = true;
    }

    /// Sort the buffer, keep the best `keep` rows, drop the rest — and
    /// hand the dropped rows' reserved bytes back to the accountant so
    /// steady-state memory stays bounded.
    fn prune(self: *TopN, acc: ?*exec.memory.MemoryAccountant) !void {
        const n: usize = @intCast(self.accumulated_rows);
        var tmp_perm = try self.allocator.alloc(u32, n);
        defer self.allocator.free(tmp_perm);
        for (tmp_perm, 0..) |*p, i| p.* = @intCast(i);
        std.sort.pdq(u32, tmp_perm, self.comparator(self.accumulated), Comparator.lessThan);

        const keep_n: usize = @min(self.keep, n);

        var fresh = try self.allocator.alloc(ColumnStore, self.schema.len);
        var finited: usize = 0;
        errdefer {
            for (fresh[0..finited]) |*c| c.deinit(self.allocator);
            self.allocator.free(fresh);
        }
        for (self.schema, 0..) |col, i| {
            fresh[i] = try ColumnStore.init(self.allocator, col.type, col.nullable);
            finited += 1;
            try engine.memtable.appendByIndices(self.allocator, self.accumulated[i].view(), tmp_perm[0..keep_n], &fresh[i]);
        }

        const dropped = n - keep_n;
        const release_bytes = dropped * self.row_bytes;
        if (acc) |a| a.release(.topn, release_bytes);
        self.reserved_bytes -= release_bytes;

        for (self.accumulated) |*c| c.deinit(self.allocator);
        self.allocator.free(self.accumulated);
        self.accumulated = fresh;
        self.accumulated_rows = keep_n;

        // Whenever the buffer is full (`keep` rows) and freshly sorted, the
        // last row is the worst we'd keep — the cut line for the pre-filter.
        // If we couldn't fill `keep` (fewer rows seen so far), there's no
        // cut line yet, so the next batches still accumulate unconditionally.
        if (keep_n == self.keep) {
            self.worst_idx = @intCast(keep_n - 1);
            self.threshold_active = true;
        } else {
            self.threshold_active = false;
        }
    }
};
