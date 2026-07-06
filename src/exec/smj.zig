//! Sort-merge join. v1: full sort on both sides (no pre-sorted fast
//! path yet — that's added when the planner can detect order-key
//! alignment via PipelineStats.sort_state).
//!
//! Algorithm:
//!   1. Materialize and sort both sides on the join key.
//!   2. Walk both sorted streams in lockstep. For each matching key
//!      run, emit the Cartesian product of the matching rows.
//!
//! Same join contract as the hash variant:
//!   - INNER equi-join only (v1)
//!   - NULL join keys never match (standard SQL)
//!   - Output schema = left columns + (right columns minus right keys)
//!   - Multi-column keys via lexicographic compare on the key tuple
//!
//! When SMJ wins over hash (per the design discussion):
//!   - Both sides large + memory-constrained
//!   - Heavy key skew (SMJ has bounded degradation; hash has bucket
//!     pathology)
//!   - Output needs to be sorted on the join key (SMJ is sorted for
//!     free; hash would need a follow-on sort)

const std = @import("std");
const Allocator = std.mem.Allocator;

const types = @import("../types.zig");
const Column = types.Column;
const TypeTag = types.TypeTag;

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

const transform = @import("../engine/transform.zig");
const join_mod = @import("join.zig");
const Spec = join_mod.Spec;

const cell_io = @import("cell_io.zig");
const appendNullTo = cell_io.appendNullTo;
const appendOneFromBuild = cell_io.appendOneFromBuild;

const output_batch_rows: usize = 1024;

pub const SortMergeJoin = struct {
    allocator: Allocator,
    arena: std.heap.ArenaAllocator,

    left: Query,
    right: Query,

    left_key_indices: []usize,
    right_key_indices: []usize,

    output_schema: []Column,
    left_col_count: usize,
    /// Per right-side column: true if we emit it (false for join keys).
    right_kept_mask: []const bool,
    /// Per-output-column stats (left ⧺ kept right). Cached at create. Empty
    /// when neither side carries stats info.
    cached_stats: []const exec.ColStat = &.{},

    // Materialized + sorted state for both sides. Populated lazily on
    // first .next() call. We sort the row indices (perm), then index
    // by perm[i] when emitting (avoids physically reordering data).
    left_materialized: []ColumnStore,
    right_materialized: []ColumnStore,
    left_rows: u32 = 0,
    right_rows: u32 = 0,

    // When true, materializeAndSort skips draining that side — caller
    // pre-filled left_materialized / right_materialized + row counts.
    // Used by Join's skew-route path to hand its already-built side
    // directly to SMJ without re-materializing.
    pre_left_materialized: bool = false,
    pre_right_materialized: bool = false,
    left_perm: []u32 = &.{},
    right_perm: []u32 = &.{},
    /// Pre-built compound key bytes per row (for fast comparison
    /// during merge). Indexed by the SORTED position (i.e.,
    /// left_keys_bytes[i] corresponds to left_materialized[left_perm[i]]).
    left_keys_bytes: [][]const u8 = &.{},
    right_keys_bytes: [][]const u8 = &.{},
    /// Indices into rows where NULL keys live. NULL keys never match
    /// — we exclude them from the sort + merge entirely.
    /// (After sort, the perm array only contains non-null row indices.)

    // Merge cursor.
    left_cursor: usize = 0,
    right_cursor: usize = 0,

    // Fast-path: skip the `sortByKeys` step when the upstream is
    // already globally sorted on the join keys. buildPermAndKeys
    // emits perm + keys_bytes in source order; if the source is
    // sorted, the resulting arrays are too (per-row order-preserving
    // encoding — see `appendColumnValueBytes`).
    skip_left_sort: bool,
    skip_right_sort: bool,

    // Output staging.
    output_columns: []ColumnStore,
    views: []ColumnView,
    pending_clear: bool = false,

    /// Join semantics — drives unmatched-row emission during merge
    /// and post-merge drain.
    join_type: join_mod.JoinType,

    /// Range predicates resolved to column indices. AND-combined;
    /// every range must hold for the pair to be emitted. Empty when
    /// the user didn't supply any.
    ranges: []const join_mod.Join.ResolvedRange,

    /// Reusable scratch buffers for per-key-run "any actual match"
    /// tracking under outer + range. Sized to the current run's
    /// length each iteration; capacity persists.
    li_match_scratch: std.ArrayList(bool) = .empty,
    ri_match_scratch: std.ArrayList(bool) = .empty,

    phase: Phase = .materializing,

    const Phase = enum { materializing, merging, done };

    pub fn create(
        allocator: Allocator,
        left: Query,
        right: Query,
        spec: Spec,
    ) !Query {
        const self = try createSelf(allocator, left, right, spec);
        return wrapAsQuery(allocator, self, spec);
    }

    fn wrapAsQuery(allocator: Allocator, self: *SortMergeJoin, spec: Spec) !Query {
        const q = makeQuery(allocator, self);
        if (spec.extra_predicate) |pred| {
            return @import("filter.zig").Filter.create(allocator, q, pred);
        }
        return q;
    }

    fn createSelf(
        allocator: Allocator,
        left: Query,
        right: Query,
        spec: Spec,
    ) !*SortMergeJoin {
        if (spec.on.len == 0) return Error.JoinEmptyOnClause;

        var arena = std.heap.ArenaAllocator.init(allocator);
        errdefer arena.deinit();
        const aa = arena.allocator();

        const left_schema = left.outputSchema();
        const right_schema = right.outputSchema();

        const left_keys = try aa.alloc(usize, spec.on.len);
        const right_keys = try aa.alloc(usize, spec.on.len);
        for (spec.on, 0..) |pair, i| {
            left_keys[i] = columnIndex(left_schema, pair.left) orelse return Error.ColumnNotFound;
            right_keys[i] = columnIndex(right_schema, pair.right) orelse return Error.ColumnNotFound;
            const lt: TypeTag = left_schema[left_keys[i]].type;
            const rt: TypeTag = right_schema[right_keys[i]].type;
            if (lt != rt and !(isStringTag(lt) and isStringTag(rt))) {
                return Error.JoinKeyTypeMismatch;
            }
        }

        const resolved_ranges = try aa.alloc(join_mod.Join.ResolvedRange, spec.ranges.len);
        for (spec.ranges, 0..) |rp, i| {
            const lidx = columnIndex(left_schema, rp.left) orelse return Error.ColumnNotFound;
            const ridx = columnIndex(right_schema, rp.right) orelse return Error.ColumnNotFound;
            const lt: TypeTag = left_schema[lidx].type;
            const rt: TypeTag = right_schema[ridx].type;
            if (lt != rt and !(isStringTag(lt) and isStringTag(rt))) {
                return Error.JoinKeyTypeMismatch;
            }
            switch (rp.op) {
                .lt, .lte, .gt, .gte => {},
                else => return Error.UnsupportedOperatorForType,
            }
            resolved_ranges[i] = .{ .left_col = lidx, .right_col = ridx, .op = rp.op };
        }

        const right_kept_mask = try aa.alloc(bool, right_schema.len);
        for (right_kept_mask) |*m| m.* = true;
        for (right_keys) |idx| right_kept_mask[idx] = false;

        var right_kept_count: usize = 0;
        for (right_kept_mask) |m| {
            if (m) right_kept_count += 1;
        }

        // Outer joins: the "other" side's columns become nullable in
        // the output (unmatched preserved rows have NULL on the other
        // side). See join.zig for the same logic.
        const left_nullable_in_output = switch (spec.join_type) {
            .inner, .left => false,
            .right, .full => true,
        };
        const right_nullable_in_output = switch (spec.join_type) {
            .inner, .right => false,
            .left, .full => true,
        };

        const output_schema = try allocator.alloc(Column, left_schema.len + right_kept_count);
        errdefer allocator.free(output_schema);
        for (left_schema, 0..) |c, i| {
            output_schema[i] = c;
            if (left_nullable_in_output) output_schema[i].nullable = true;
        }
        var out_idx: usize = left_schema.len;
        for (right_schema, 0..) |c, i| {
            if (!right_kept_mask[i]) continue;
            for (output_schema[0..out_idx]) |prior| {
                if (types.columnNameEql(prior.name, c.name)) return Error.JoinColumnNameCollision;
            }
            output_schema[out_idx] = c;
            if (right_nullable_in_output) output_schema[out_idx].nullable = true;
            out_idx += 1;
        }

        const right_kept_mask_owned = try allocator.alloc(bool, right_schema.len);
        @memcpy(right_kept_mask_owned, right_kept_mask);
        errdefer allocator.free(right_kept_mask_owned);

        // Per-side materialization buffers.
        const left_mat = try allocator.alloc(ColumnStore, left_schema.len);
        errdefer allocator.free(left_mat);
        var li: usize = 0;
        errdefer for (left_mat[0..li]) |*c| c.deinit(allocator);
        for (left_schema, 0..) |col, i| {
            left_mat[i] = try ColumnStore.init(allocator, col.type, col.nullable);
            li += 1;
        }

        const right_mat = try allocator.alloc(ColumnStore, right_schema.len);
        errdefer allocator.free(right_mat);
        var ri: usize = 0;
        errdefer for (right_mat[0..ri]) |*c| c.deinit(allocator);
        for (right_schema, 0..) |col, i| {
            right_mat[i] = try ColumnStore.init(allocator, col.type, col.nullable);
            ri += 1;
        }

        const output_columns = try allocator.alloc(ColumnStore, output_schema.len);
        errdefer allocator.free(output_columns);
        var oi: usize = 0;
        errdefer for (output_columns[0..oi]) |*c| c.deinit(allocator);
        for (output_schema, 0..) |col, i| {
            output_columns[i] = try ColumnStore.init(allocator, col.type, col.nullable);
            oi += 1;
        }

        const views = try allocator.alloc(ColumnView, output_schema.len);
        errdefer allocator.free(views);

        // Fast-path detection: stats are cheap and captured upstream
        // at scan-create time. If both sides report global sort on a
        // prefix that covers the join keys, sortByKeys becomes a no-op.
        const left_stats = left.stats();
        const right_stats = right.stats();
        const skip_left = left_stats.sort_state.global and
            join_mod.joinKeysCovered(left_stats.sort_state, spec.on, .left);
        const skip_right = right_stats.sort_state.global and
            join_mod.joinKeysCovered(right_stats.sort_state, spec.on, .right);

        const cached_stats = try exec.concatJoinStats(allocator, left, right, left_schema.len, right_kept_mask_owned, output_schema.len);
        errdefer if (cached_stats.len > 0) allocator.free(cached_stats);

        const self = try allocator.create(SortMergeJoin);
        errdefer allocator.destroy(self);
        self.* = .{
            .allocator = allocator,
            .arena = arena,
            .left = left,
            .right = right,
            .left_key_indices = left_keys,
            .right_key_indices = right_keys,
            .output_schema = output_schema,
            .left_col_count = left_schema.len,
            .right_kept_mask = right_kept_mask_owned,
            .cached_stats = cached_stats,
            .left_materialized = left_mat,
            .right_materialized = right_mat,
            .skip_left_sort = skip_left,
            .skip_right_sort = skip_right,
            .output_columns = output_columns,
            .views = views,
            .join_type = spec.join_type,
            .ranges = resolved_ranges,
        };
        return self;
    }

    /// Construct an SMJ where one side is already materialized
    /// (by the caller, typically hash join's skew-route path). The
    /// pre-materialized columns are TRANSFERRED to SMJ — it owns them
    /// and frees on deinit. The unpopulated side will be drained from
    /// its Query normally.
    ///
    /// Used internally; users should not call this directly.
    pub fn createForSkewRoute(
        allocator: Allocator,
        left_q: Query,
        right_q: Query,
        spec: Spec,
        pre_columns: []ColumnStore,
        pre_rows: u32,
        pre_is_left: bool,
    ) !Query {
        const self = try createSelf(allocator, left_q, right_q, spec);

        if (pre_is_left) {
            for (self.left_materialized) |*c| c.deinit(allocator);
            allocator.free(self.left_materialized);
            self.left_materialized = pre_columns;
            self.left_rows = pre_rows;
            self.pre_left_materialized = true;
        } else {
            for (self.right_materialized) |*c| c.deinit(allocator);
            allocator.free(self.right_materialized);
            self.right_materialized = pre_columns;
            self.right_rows = pre_rows;
            self.pre_right_materialized = true;
        }
        return wrapAsQuery(allocator, self, spec);
    }

    pub fn deinit(self: *SortMergeJoin) void {
        var l = self.left;
        l.deinit();
        var r = self.right;
        r.deinit();
        self.li_match_scratch.deinit(self.allocator);
        self.ri_match_scratch.deinit(self.allocator);
        for (self.left_materialized) |*c| c.deinit(self.allocator);
        self.allocator.free(self.left_materialized);
        for (self.right_materialized) |*c| c.deinit(self.allocator);
        self.allocator.free(self.right_materialized);
        if (self.left_perm.len > 0) self.allocator.free(self.left_perm);
        if (self.right_perm.len > 0) self.allocator.free(self.right_perm);
        if (self.left_keys_bytes.len > 0) {
            for (self.left_keys_bytes) |k| self.allocator.free(k);
            self.allocator.free(self.left_keys_bytes);
        }
        if (self.right_keys_bytes.len > 0) {
            for (self.right_keys_bytes) |k| self.allocator.free(k);
            self.allocator.free(self.right_keys_bytes);
        }
        for (self.output_columns) |*c| c.deinit(self.allocator);
        self.allocator.free(self.output_columns);
        self.allocator.free(self.views);
        self.allocator.free(self.output_schema);
        self.allocator.free(self.right_kept_mask);
        if (self.cached_stats.len > 0) self.allocator.free(@constCast(self.cached_stats));
        self.arena.deinit();
        const allocator = self.allocator;
        allocator.destroy(self);
    }

    pub fn outputSchema(self: *SortMergeJoin) []const Column {
        return self.output_schema;
    }

    pub fn addPrune(self: *SortMergeJoin, pred: Predicate) !void {
        self.left.addPrune(pred) catch |e| switch (e) {
            error.ColumnNotFound => {},
            else => return e,
        };
        self.right.addPrune(pred) catch |e| switch (e) {
            error.ColumnNotFound => {},
            else => return e,
        };
    }

    pub fn accountant(self: *SortMergeJoin) ?*exec.memory.MemoryAccountant {
        return self.left.accountant();
    }

    pub fn explain(self: *SortMergeJoin, out: *std.ArrayList(u8), allocator: std.mem.Allocator, depth: usize) !void {
        try exec.explainIndent(out, allocator, depth);
        try out.appendSlice(allocator, "SortMergeJoin (left: ");
        try out.appendSlice(allocator, if (self.skip_left_sort) "pre-sorted" else "sorts");
        try out.appendSlice(allocator, ", right: ");
        try out.appendSlice(allocator, if (self.skip_right_sort) "pre-sorted" else "sorts");
        try out.appendSlice(allocator, ")\n");
        try self.left.explain(out, allocator, depth + 1);
        try self.right.explain(out, allocator, depth + 1);
    }

    pub fn stats(self: *SortMergeJoin) exec.PipelineStats {
        const l = self.left.stats();
        const r = self.right.stats();
        const product = std.math.mul(u64, l.upper_rows, r.upper_rows) catch std.math.maxInt(u64);
        // SMJ output IS sorted on the join keys — a real downstream
        // advantage we should publish (e.g., a downstream merge or
        // groupBy on join keys can skip its own sort).
        const key_names = self.allocator.alloc([]const u8, self.left_key_indices.len) catch {
            return .{ .upper_rows = product, .column_stats = self.cached_stats };
        };
        defer self.allocator.free(key_names);
        // Use the LEFT-side column names — those are the ones that
        // remain in the output schema (right-side keys are dropped
        // per USING-clause semantics).
        const left_schema = blk: {
            // Construct a temporary slice of just our left columns
            // (output_schema[0..left_col_count]).
            break :blk self.output_schema[0..self.left_col_count];
        };
        for (self.left_key_indices, 0..) |idx, i| key_names[i] = left_schema[idx].name;
        // The sort_state lifetime is "until next call": the stats() caller
        // reads it immediately and never retains the slice.
        return .{ .upper_rows = product, .column_stats = self.cached_stats };
    }

    pub fn next(self: *SortMergeJoin) !?Batch {
        while (true) {
            switch (self.phase) {
                .materializing => {
                    try self.materializeAndSort();
                    self.phase = .merging;
                },
                .merging => {
                    if (try self.mergeStep()) |batch| return batch;
                    self.phase = .done;
                    if (try self.flushOutput()) |batch| return batch;
                    return null;
                },
                .done => return null,
            }
        }
    }

    // -----------------------------------------------------------------
    // Materialize + sort both sides.
    // -----------------------------------------------------------------

    fn materializeAndSort(self: *SortMergeJoin) !void {
        const acc = self.left.accountant();
        const left_row_bytes = exec.memory.estimateRowBytes(self.left.outputSchema());
        const right_row_bytes = exec.memory.estimateRowBytes(self.right.outputSchema());

        // Drain left into left_materialized (skip if pre-filled).
        if (!self.pre_left_materialized) {
            while (try self.left.next()) |batch| {
                if (acc) |a| try a.reserve(.sort_merge_join, batch.row_count * left_row_bytes);
                for (batch.values, 0..) |v, i| {
                    try transform.appendAllColumn(self.allocator, v, &self.left_materialized[i]);
                }
                self.left_rows += @intCast(batch.row_count);
            }
        }
        // Drain right (skip if pre-filled).
        if (!self.pre_right_materialized) {
            while (try self.right.next()) |batch| {
                if (acc) |a| try a.reserve(.sort_merge_join, batch.row_count * right_row_bytes);
                for (batch.values, 0..) |v, i| {
                    try transform.appendAllColumn(self.allocator, v, &self.right_materialized[i]);
                }
                self.right_rows += @intCast(batch.row_count);
            }
        }

        // Build compound key bytes per row + filter out NULL-key rows.
        // perm[i] is the source row index in the materialized columns;
        // keys_bytes[i] is the corresponding compound key.
        self.left_perm = try buildPermAndKeys(
            self.allocator,
            &self.left_keys_bytes,
            self.left_materialized,
            self.left_key_indices,
            self.left_rows,
        );
        self.right_perm = try buildPermAndKeys(
            self.allocator,
            &self.right_keys_bytes,
            self.right_materialized,
            self.right_key_indices,
            self.right_rows,
        );

        // Sort perm by corresponding key bytes (in place via a context).
        // Skipped per-side when stats prove the source is already
        // sorted on the join keys — buildPermAndKeys emits in source
        // order, so a pre-sorted source produces a pre-sorted perm.
        if (!self.skip_left_sort) sortByKeys(self.left_perm, self.left_keys_bytes);
        if (!self.skip_right_sort) sortByKeys(self.right_perm, self.right_keys_bytes);
    }

    // -----------------------------------------------------------------
    // Merge.
    // -----------------------------------------------------------------

    fn mergeStep(self: *SortMergeJoin) !?Batch {
        if (self.pending_clear) {
            for (self.output_columns) |*c| c.clear();
            self.pending_clear = false;
        }

        const preserve_left = switch (self.join_type) {
            .left, .full => true,
            else => false,
        };
        const preserve_right = switch (self.join_type) {
            .right, .full => true,
            else => false,
        };

        while (self.left_cursor < self.left_perm.len and self.right_cursor < self.right_perm.len) {
            const lkey = self.left_keys_bytes[self.left_cursor];
            const rkey = self.right_keys_bytes[self.right_cursor];
            switch (std.mem.order(u8, lkey, rkey)) {
                .lt => {
                    if (preserve_left) {
                        try self.emitLeftOnlyRow(self.left_perm[self.left_cursor]);
                        if (self.output_columns[0].data.rowCount() >= output_batch_rows) {
                            self.left_cursor += 1;
                            return try self.flushOutput();
                        }
                    }
                    self.left_cursor += 1;
                },
                .gt => {
                    if (preserve_right) {
                        try self.emitRightOnlyRow(self.right_perm[self.right_cursor]);
                        if (self.output_columns[0].data.rowCount() >= output_batch_rows) {
                            self.right_cursor += 1;
                            return try self.flushOutput();
                        }
                    }
                    self.right_cursor += 1;
                },
                .eq => {
                    // Find runs of equal keys on both sides.
                    var l_end = self.left_cursor + 1;
                    while (l_end < self.left_perm.len and std.mem.eql(u8, self.left_keys_bytes[l_end], lkey)) : (l_end += 1) {}
                    var r_end = self.right_cursor + 1;
                    while (r_end < self.right_perm.len and std.mem.eql(u8, self.right_keys_bytes[r_end], rkey)) : (r_end += 1) {}

                    // Outer + range needs per-row "any actual match"
                    // tracking — a left row may have all candidates
                    // rejected by the range filter, in which case
                    // LEFT/FULL OUTER still null-extends it. Same for
                    // right under RIGHT/FULL. Scratch buffers grow on
                    // demand; capacity persists across runs.
                    const l_size = l_end - self.left_cursor;
                    const r_size = r_end - self.right_cursor;
                    var li_match: []bool = &.{};
                    var ri_match: []bool = &.{};
                    if (preserve_left) {
                        try self.li_match_scratch.resize(self.allocator, l_size);
                        @memset(self.li_match_scratch.items, false);
                        li_match = self.li_match_scratch.items;
                    }
                    if (preserve_right) {
                        try self.ri_match_scratch.resize(self.allocator, r_size);
                        @memset(self.ri_match_scratch.items, false);
                        ri_match = self.ri_match_scratch.items;
                    }

                    // Cartesian. No mid-run buffer check — batches may
                    // exceed output_batch_rows temporarily; we flush at
                    // the run boundary.
                    var li = self.left_cursor;
                    while (li < l_end) : (li += 1) {
                        var ri = self.right_cursor;
                        while (ri < r_end) : (ri += 1) {
                            const lr = self.left_perm[li];
                            const rr = self.right_perm[ri];
                            if (!self.passesAllRanges(lr, rr)) continue;
                            try self.emitOutputRow(lr, rr);
                            if (preserve_left) li_match[li - self.left_cursor] = true;
                            if (preserve_right) ri_match[ri - self.right_cursor] = true;
                        }
                    }

                    // Emit null-extended rows for preserved-side rows
                    // that had no actual match.
                    if (preserve_left) {
                        for (li_match, 0..) |m, i| {
                            if (!m) try self.emitLeftOnlyRow(self.left_perm[self.left_cursor + i]);
                        }
                    }
                    if (preserve_right) {
                        for (ri_match, 0..) |m, i| {
                            if (!m) try self.emitRightOnlyRow(self.right_perm[self.right_cursor + i]);
                        }
                    }

                    self.left_cursor = l_end;
                    self.right_cursor = r_end;

                    if (self.output_columns[0].data.rowCount() >= output_batch_rows) {
                        return try self.flushOutput();
                    }
                },
            }
        }

        // Post-loop drain: when one side is exhausted, drain remaining
        // rows on the other side as unmatched (if preserved).
        if (preserve_left) {
            while (self.left_cursor < self.left_perm.len) {
                try self.emitLeftOnlyRow(self.left_perm[self.left_cursor]);
                self.left_cursor += 1;
                if (self.output_columns[0].data.rowCount() >= output_batch_rows) {
                    return try self.flushOutput();
                }
            }
        }
        if (preserve_right) {
            while (self.right_cursor < self.right_perm.len) {
                try self.emitRightOnlyRow(self.right_perm[self.right_cursor]);
                self.right_cursor += 1;
                if (self.output_columns[0].data.rowCount() >= output_batch_rows) {
                    return try self.flushOutput();
                }
            }
        }

        return null;
    }

    /// Evaluate every range predicate against materialized rows.
    /// AND semantics: any failing range rejects the pair.
    fn passesAllRanges(self: SortMergeJoin, left_row: u32, right_row: u32) bool {
        for (self.ranges) |rg| {
            const lv = self.left_materialized[rg.left_col].view();
            const rv = self.right_materialized[rg.right_col].view();
            if (!join_mod.compareCellsOp(lv, left_row, rv, right_row, rg.op)) return false;
        }
        return true;
    }

    fn emitLeftOnlyRow(self: *SortMergeJoin, left_row: u32) !void {
        try cell_io.emitLeftOnlyRow(
            self.allocator,
            self.output_columns,
            self.left_materialized,
            left_row,
            self.right_kept_mask,
        );
    }

    fn emitRightOnlyRow(self: *SortMergeJoin, right_row: u32) !void {
        try cell_io.emitRightOnlyRow(
            self.allocator,
            self.output_columns,
            self.right_materialized,
            right_row,
            self.right_kept_mask,
            self.left_col_count,
        );
    }

    fn emitOutputRow(self: *SortMergeJoin, left_row: u32, right_row: u32) !void {
        try cell_io.emitMatchedRow(
            self.allocator,
            self.output_columns,
            self.left_materialized,
            left_row,
            self.right_materialized,
            right_row,
            self.right_kept_mask,
        );
    }

    fn flushOutput(self: *SortMergeJoin) !?Batch {
        const rows = self.output_columns[0].data.rowCount();
        if (rows == 0) return null;
        for (self.output_columns, 0..) |c, i| self.views[i] = c.view();
        self.pending_clear = true;
        return Batch{
            .schema = self.output_schema,
            .values = self.views,
            .row_count = rows,
        };
    }
};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn columnIndex(schema: []const Column, name: []const u8) ?usize {
    return types.findColumn(schema, name);
}

fn isStringTag(t: TypeTag) bool {
    return switch (t) {
        .varchar, .string, .char, .json => true,
        else => false,
    };
}

/// Build a per-row compound key + the perm array of non-null-key rows.
/// keys_bytes_out: caller-owned slice, sized to perm length; each
///                  entry is heap-allocated bytes (caller frees).
fn buildPermAndKeys(
    allocator: Allocator,
    keys_out: *[][]const u8,
    columns: []ColumnStore,
    key_indices: []const usize,
    n: u32,
) ![]u32 {
    var perm: std.ArrayList(u32) = .empty;
    defer perm.deinit(allocator);
    var keys: std.ArrayList([]const u8) = .empty;
    errdefer {
        for (keys.items) |k| allocator.free(k);
        keys.deinit(allocator);
    }

    var scratch: std.ArrayList(u8) = .empty;
    defer scratch.deinit(allocator);

    var i: u32 = 0;
    while (i < n) : (i += 1) {
        // Check NULL keys.
        var any_null = false;
        for (key_indices) |idx| {
            if (!columns[idx].view().isValid(i)) {
                any_null = true;
                break;
            }
        }
        if (any_null) continue;

        scratch.clearRetainingCapacity();
        for (key_indices) |idx| {
            try appendColumnValueBytes(allocator, &scratch, columns[idx].view(), i);
        }
        const owned = try allocator.dupe(u8, scratch.items);
        try keys.append(allocator, owned);
        try perm.append(allocator, i);
    }

    keys_out.* = try keys.toOwnedSlice(allocator);
    return try perm.toOwnedSlice(allocator);
}

/// Append a column value as an ORDER-PRESERVING byte sequence — i.e.,
/// `std.mem.order(u8, encode(a), encode(b))` matches the natural value
/// comparison `a <=> b`. Equal values produce equal byte sequences;
/// unequal values never collide. Used to build SMJ compound keys so
/// the sort step produces output in natural-value order.
///
/// Per-type encoding:
///   - Signed ints (incl. decimal64/decimal128, date, datetime):
///     big-endian with the top bit flipped (so signed compare maps to
///     unsigned/lex compare).
///   - Unsigned (boolean as u8, uuid as u128): big-endian.
///   - Floats: IEEE 754 total ordering trick — XOR top bit for
///     positives, XOR all bits for negatives.
///   - Strings: byte-stuffed with 0x00 → (0x00, 0xFF) and a (0x00, 0x00)
///     terminator. Unambiguous across compound-key components.
fn appendColumnValueBytes(
    allocator: Allocator,
    out: *std.ArrayList(u8),
    view: ColumnView,
    row: u32,
) !void {
    switch (view.data) {
        .int => |s| try appendSignedKey(allocator, out, i32, s[row]),
        .bigint => |s| try appendSignedKey(allocator, out, i64, s[row]),
        .boolean => |s| try out.append(allocator, s[row]),
        .float => |s| try appendFloatKey(allocator, out, f32, s[row]),
        .double => |s| try appendFloatKey(allocator, out, f64, s[row]),
        .date => |s| try appendSignedKey(allocator, out, i32, s[row]),
        .datetime => |s| try appendSignedKey(allocator, out, i64, s[row]),
        .tinyint => |s| try appendSignedKey(allocator, out, i8, s[row]),
        .smallint => |s| try appendSignedKey(allocator, out, i16, s[row]),
        .largeint => |s| try appendSignedKey(allocator, out, i128, s[row]),
        .decimal64 => |s| try appendSignedKey(allocator, out, i64, s[row]),
        .decimal128 => |s| try appendSignedKey(allocator, out, i128, s[row]),
        .uuid => |s| try appendUnsignedKey(allocator, out, u128, s[row]),
        .varchar, .string, .char, .json => |sv| try appendStringKey(allocator, out, sv.rowBytes(row)),
    }
}

fn appendSignedKey(allocator: Allocator, out: *std.ArrayList(u8), comptime T: type, v: T) !void {
    const bits = @bitSizeOf(T);
    const U = std.meta.Int(.unsigned, bits);
    const u: U = @bitCast(v);
    const top_bit: U = @as(U, 1) << (bits - 1);
    var b: [@sizeOf(T)]u8 = undefined;
    std.mem.writeInt(U, &b, u ^ top_bit, .big);
    try out.appendSlice(allocator, &b);
}

fn appendUnsignedKey(allocator: Allocator, out: *std.ArrayList(u8), comptime T: type, v: T) !void {
    var b: [@sizeOf(T)]u8 = undefined;
    std.mem.writeInt(T, &b, v, .big);
    try out.appendSlice(allocator, &b);
}

fn appendFloatKey(allocator: Allocator, out: *std.ArrayList(u8), comptime T: type, v: T) !void {
    const bits = @bitSizeOf(T);
    const U = std.meta.Int(.unsigned, bits);
    var u: U = @bitCast(v);
    const top_bit: U = @as(U, 1) << (bits - 1);
    // IEEE total-ordering: positives XOR sign bit; negatives flip all bits.
    if (u & top_bit != 0) {
        u = ~u;
    } else {
        u ^= top_bit;
    }
    var b: [@sizeOf(T)]u8 = undefined;
    std.mem.writeInt(U, &b, u, .big);
    try out.appendSlice(allocator, &b);
}

fn appendStringKey(allocator: Allocator, out: *std.ArrayList(u8), bytes: []const u8) !void {
    for (bytes) |b| {
        try out.append(allocator, b);
        if (b == 0x00) try out.append(allocator, 0xFF);
    }
    try out.append(allocator, 0x00);
    try out.append(allocator, 0x00);
}

/// Sort `perm` so the keys it references are in ascending lex order.
/// Uses indirect sort: `perm[i]` is the row index in the columns;
/// `keys[i]` is the precomputed key for that row.
fn sortByKeys(perm: []u32, keys: [][]const u8) void {
    const SortCtx = struct {
        perm: []u32,
        keys: [][]const u8,

        pub fn lessThan(self: @This(), a: usize, b: usize) bool {
            return std.mem.order(u8, self.keys[a], self.keys[b]) == .lt;
        }
        pub fn swap(self: @This(), a: usize, b: usize) void {
            std.mem.swap(u32, &self.perm[a], &self.perm[b]);
            std.mem.swap([]const u8, &self.keys[a], &self.keys[b]);
        }
    };
    std.sort.pdqContext(0, perm.len, SortCtx{ .perm = perm, .keys = keys });
}
