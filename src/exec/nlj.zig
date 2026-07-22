//! Nested-loop join. Fully general: handles any combination of
//! equi keys + range predicates by materializing both sides and
//! double-looping. O(N*M) — use only when there's no equi prefix
//! to drive a hash/SMJ, or when at least one side is tiny.
//!
//! Same external contract as Hash / SMJ:
//!   - INNER joins only in v1 (outer + range deferred)
//!   - Output schema = left + (right minus right join-key columns)
//!   - Multi-column equi keys via Spec.on
//!   - Range predicates (Spec.ranges) AND-combined with equi keys
//!
//! When the equi `on` clause is empty AND there are range predicates,
//! .auto picks this algorithm — there's no equi prefix to feed a
//! hash table or merge step, but we can still evaluate ranges over
//! the Cartesian product.

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

const output_batch_rows: usize = 1024;

pub const NestedLoopJoin = struct {
    allocator: Allocator,
    arena: std.heap.ArenaAllocator,

    left: Query,
    right: Query,

    /// Per-side equi-key column indices, in order of Spec.on. Empty
    /// when there's no equi part (pure range / pure NLJ).
    left_key_indices: []usize,
    right_key_indices: []usize,

    /// Range predicates resolved to column indices. AND-combined.
    ranges: []const join_mod.Join.ResolvedRange,

    /// Optional opaque per-pair predicate. Evaluated after equi +
    /// range checks; pairs returning false get dropped.
    opaque_predicate: ?join_mod.OpaquePredicate,

    // Scratch ColumnView buffers reused per-call to feed the
    // opaque predicate callback (so we don't allocate per pair).
    left_view_buf: []ColumnView,
    right_view_buf: []ColumnView,

    output_schema: []Column,
    left_col_count: usize,
    /// Per right-side column: true if we emit it (false for join keys).
    right_kept_mask: []const bool,
    /// Per-output-column stats (left ⧺ kept right). Cached at create. Empty
    /// when neither side carries stats info.
    cached_stats: []const exec.ColStat = &.{},

    // Materialized state for both sides. Populated lazily on first .next().
    left_materialized: []ColumnStore,
    right_materialized: []ColumnStore,
    left_rows: u32 = 0,
    right_rows: u32 = 0,

    // Loop cursors. Outer: left row. Inner: right row.
    left_cursor: u32 = 0,
    right_cursor: u32 = 0,

    // Outer join state.
    join_type: join_mod.JoinType,
    /// Tracks whether the current LEFT (outer) row has had any
    /// actual match. Reset when left_cursor advances. When the
    /// inner loop completes with this still false AND the left
    /// side is preserved (LEFT/FULL), we emit one null-extended
    /// left row.
    cur_left_any_match: bool = false,
    /// FULL/RIGHT OUTER: bitmap of matched RIGHT (inner) rows.
    /// After the main loop completes, unmarked rows get emitted
    /// null-extended on the left side.
    matched_right: ?std.DynamicBitSetUnmanaged = null,
    /// Drain cursor for the post-loop unmatched-right phase.
    drain_cursor: u32 = 0,

    // Output staging.
    output_columns: []ColumnStore,
    views: []ColumnView,
    pending_clear: bool = false,

    phase: Phase = .materializing,

    const Phase = enum {
        materializing,
        looping,
        /// RIGHT / FULL OUTER: walk matched_right after the main
        /// loop, emitting null-extended rows for unmatched right
        /// rows.
        draining_right,
        done,
    };

    pub fn create(
        allocator: Allocator,
        left: Query,
        right: Query,
        spec: Spec,
    ) !Query {
        var arena = std.heap.ArenaAllocator.init(allocator);
        errdefer arena.deinit();
        const aa = arena.allocator();

        const left_schema = left.outputSchema();
        const right_schema = right.outputSchema();

        // Resolve equi keys (may be empty).
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

        // Resolve ranges.
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

        // Outer joins force the "other" side's columns to nullable.
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

        // Scratch view buffers for the opaque-predicate callback path.
        // Sized to each side's schema; reused per (lrow, rrow) pair.
        const lvb = try allocator.alloc(ColumnView, left_schema.len);
        errdefer allocator.free(lvb);
        const rvb = try allocator.alloc(ColumnView, right_schema.len);
        errdefer allocator.free(rvb);

        const cached_stats = try exec.concatJoinStats(allocator, left, right, left_schema.len, right_kept_mask_owned, output_schema.len);
        errdefer if (cached_stats.len > 0) allocator.free(cached_stats);

        const self = try allocator.create(NestedLoopJoin);
        errdefer allocator.destroy(self);
        self.* = .{
            .allocator = allocator,
            .arena = arena,
            .left = left,
            .right = right,
            .left_key_indices = left_keys,
            .right_key_indices = right_keys,
            .ranges = resolved_ranges,
            .opaque_predicate = spec.opaque_predicate,
            .left_view_buf = lvb,
            .right_view_buf = rvb,
            .output_schema = output_schema,
            .left_col_count = left_schema.len,
            .right_kept_mask = right_kept_mask_owned,
            .cached_stats = cached_stats,
            .left_materialized = left_mat,
            .right_materialized = right_mat,
            .output_columns = output_columns,
            .views = views,
            .join_type = spec.join_type,
        };
        const q = makeQuery(allocator, self);
        if (spec.extra_predicate) |pred| {
            return @import("filter.zig").Filter.create(allocator, q, pred);
        }
        return q;
    }

    pub fn deinit(self: *NestedLoopJoin) void {
        var l = self.left;
        l.deinit();
        var r = self.right;
        r.deinit();
        for (self.left_materialized) |*c| c.deinit(self.allocator);
        self.allocator.free(self.left_materialized);
        for (self.right_materialized) |*c| c.deinit(self.allocator);
        self.allocator.free(self.right_materialized);
        for (self.output_columns) |*c| c.deinit(self.allocator);
        self.allocator.free(self.output_columns);
        self.allocator.free(self.views);
        self.allocator.free(self.output_schema);
        self.allocator.free(self.right_kept_mask);
        if (self.cached_stats.len > 0) self.allocator.free(@constCast(self.cached_stats));
        self.allocator.free(self.left_view_buf);
        self.allocator.free(self.right_view_buf);
        if (self.matched_right) |*mb| mb.deinit(self.allocator);
        self.arena.deinit();
        const allocator = self.allocator;
        allocator.destroy(self);
    }

    pub fn outputSchema(self: *NestedLoopJoin) []const Column {
        return self.output_schema;
    }

    pub fn addPrune(self: *NestedLoopJoin, pred: Predicate) !void {
        self.left.addPrune(pred) catch |e| switch (e) {
            error.ColumnNotFound => {},
            else => return e,
        };
        self.right.addPrune(pred) catch |e| switch (e) {
            error.ColumnNotFound => {},
            else => return e,
        };
    }

    pub fn accountant(self: *NestedLoopJoin) ?*exec.memory.MemoryAccountant {
        return self.left.accountant();
    }

    pub fn explain(self: *NestedLoopJoin, out: *std.ArrayList(u8), allocator: std.mem.Allocator, depth: usize) !void {
        try exec.explainLine(out, allocator, depth, "NestedLoopJoin");
        try self.left.explain(out, allocator, depth + 1);
        try self.right.explain(out, allocator, depth + 1);
    }

    pub fn stats(self: *NestedLoopJoin) exec.PipelineStats {
        const l = self.left.stats();
        const r = self.right.stats();
        const product = std.math.mul(u64, l.upper_rows, r.upper_rows) catch std.math.maxInt(u64);
        return .{ .upper_rows = product, .column_stats = self.cached_stats };
    }

    pub fn next(self: *NestedLoopJoin) !?Batch {
        while (true) {
            switch (self.phase) {
                .materializing => {
                    try self.materialize();
                    // RIGHT/FULL OUTER: allocate matched-right bitmap
                    // so the draining phase can find unmatched rows.
                    if (self.join_type == .right or self.join_type == .full) {
                        self.matched_right = try std.DynamicBitSetUnmanaged.initEmpty(
                            self.allocator,
                            self.right_rows,
                        );
                    }
                    self.phase = .looping;
                },
                .looping => {
                    if (try self.loopStep()) |batch| return batch;
                    if (self.matched_right != null) {
                        self.phase = .draining_right;
                        continue;
                    }
                    self.phase = .done;
                    if (try self.flushOutput()) |batch| return batch;
                    return null;
                },
                .draining_right => {
                    if (try self.drainRightStep()) |batch| return batch;
                    self.phase = .done;
                    if (try self.flushOutput()) |batch| return batch;
                    return null;
                },
                .done => return null,
            }
        }
    }

    fn materialize(self: *NestedLoopJoin) !void {
        const acc = self.left.accountant();
        const left_row_bytes = exec.memory.estimateRowBytes(self.left.outputSchema());
        const right_row_bytes = exec.memory.estimateRowBytes(self.right.outputSchema());

        while (try self.left.next()) |batch| {
            if (acc) |a| try a.reserve(.nested_loop, batch.row_count * left_row_bytes);
            for (batch.values, 0..) |v, i| {
                try transform.appendAllColumn(self.allocator, v, &self.left_materialized[i]);
            }
            self.left_rows += @intCast(batch.row_count);
        }
        while (try self.right.next()) |batch| {
            if (acc) |a| try a.reserve(.nested_loop, batch.row_count * right_row_bytes);
            for (batch.values, 0..) |v, i| {
                try transform.appendAllColumn(self.allocator, v, &self.right_materialized[i]);
            }
            self.right_rows += @intCast(batch.row_count);
        }
    }

    fn loopStep(self: *NestedLoopJoin) !?Batch {
        if (self.pending_clear) {
            for (self.output_columns) |*c| c.clear();
            self.pending_clear = false;
        }

        const preserve_left = switch (self.join_type) {
            .left, .full => true,
            else => false,
        };

        while (self.left_cursor < self.left_rows) {
            // NULL outer key: under inner semantics we skip silently;
            // under LEFT/FULL we still preserve the row by emitting
            // null-extended (NULL never matches anyone).
            if (self.left_key_indices.len > 0 and self.outerHasNullKey()) {
                if (preserve_left) {
                    try self.emitLeftOnlyRow(self.left_cursor);
                }
                self.left_cursor += 1;
                self.right_cursor = 0;
                self.cur_left_any_match = false;
                if (self.output_columns[0].data.rowCount() >= output_batch_rows) {
                    return try self.flushOutput();
                }
                continue;
            }

            while (self.right_cursor < self.right_rows) : (self.right_cursor += 1) {
                if (self.right_key_indices.len > 0 and self.innerHasNullKey()) continue;
                if (!self.passesEquiKeys()) continue;
                if (!self.passesAllRanges()) continue;
                if (!self.passesOpaque()) continue;

                try self.emitRow();
                self.cur_left_any_match = true;
                if (self.matched_right) |*mb| mb.set(self.right_cursor);
                if (self.output_columns[0].data.rowCount() >= output_batch_rows) {
                    self.right_cursor += 1;
                    return try self.flushOutput();
                }
            }
            // Inner loop done for this outer row. Emit null-extended
            // if outer is preserved and no match occurred.
            if (preserve_left and !self.cur_left_any_match) {
                try self.emitLeftOnlyRow(self.left_cursor);
            }
            self.left_cursor += 1;
            self.right_cursor = 0;
            self.cur_left_any_match = false;
            if (self.output_columns[0].data.rowCount() >= output_batch_rows) {
                return try self.flushOutput();
            }
        }

        return null;
    }

    /// RIGHT/FULL OUTER drain: walk matched_right and emit
    /// null-extended rows for unmatched right rows.
    fn drainRightStep(self: *NestedLoopJoin) !?Batch {
        const mb = if (self.matched_right) |*m| m else return null;
        if (self.pending_clear) {
            for (self.output_columns) |*c| c.clear();
            self.pending_clear = false;
        }
        while (self.drain_cursor < self.right_rows) : (self.drain_cursor += 1) {
            if (mb.isSet(self.drain_cursor)) continue;
            try self.emitRightOnlyRow(self.drain_cursor);
            if (self.output_columns[0].data.rowCount() >= output_batch_rows) {
                self.drain_cursor += 1;
                return try self.flushOutput();
            }
        }
        return null;
    }

    fn emitLeftOnlyRow(self: *NestedLoopJoin, left_row: u32) !void {
        try cell_io.emitLeftOnlyRow(
            self.allocator,
            self.output_columns,
            self.left_materialized,
            left_row,
            self.right_kept_mask,
        );
    }

    fn emitRightOnlyRow(self: *NestedLoopJoin, right_row: u32) !void {
        try cell_io.emitRightOnlyRow(
            self.allocator,
            self.output_columns,
            self.right_materialized,
            right_row,
            self.right_kept_mask,
            self.left_col_count,
        );
    }

    fn outerHasNullKey(self: NestedLoopJoin) bool {
        for (self.left_key_indices) |idx| {
            if (!self.left_materialized[idx].view().isValid(self.left_cursor)) return true;
        }
        return false;
    }

    fn innerHasNullKey(self: NestedLoopJoin) bool {
        for (self.right_key_indices) |idx| {
            if (!self.right_materialized[idx].view().isValid(self.right_cursor)) return true;
        }
        return false;
    }

    fn passesEquiKeys(self: NestedLoopJoin) bool {
        for (self.left_key_indices, self.right_key_indices) |li, ri| {
            const lv = self.left_materialized[li].view();
            const rv = self.right_materialized[ri].view();
            if (!join_mod.compareCellsOp(lv, self.left_cursor, rv, self.right_cursor, .eq)) return false;
        }
        return true;
    }

    fn passesOpaque(self: *NestedLoopJoin) bool {
        const op = self.opaque_predicate orelse return true;
        // Fill the per-side ColumnView scratch buffers from the
        // materialized columns. (We do this on every pair; the
        // buffers are reused.)
        for (self.left_materialized, 0..) |*col, i| self.left_view_buf[i] = col.view();
        for (self.right_materialized, 0..) |*col, i| self.right_view_buf[i] = col.view();
        return op.eval(op.ctx, self.left_view_buf, self.left_cursor, self.right_view_buf, self.right_cursor);
    }

    fn passesAllRanges(self: NestedLoopJoin) bool {
        for (self.ranges) |rg| {
            const lv = self.left_materialized[rg.left_col].view();
            const rv = self.right_materialized[rg.right_col].view();
            if (!join_mod.compareCellsOp(lv, self.left_cursor, rv, self.right_cursor, rg.op)) return false;
        }
        return true;
    }

    fn emitRow(self: *NestedLoopJoin) !void {
        try cell_io.emitMatchedRow(
            self.allocator,
            self.output_columns,
            self.left_materialized,
            self.left_cursor,
            self.right_materialized,
            self.right_cursor,
            self.right_kept_mask,
        );
    }

    fn flushOutput(self: *NestedLoopJoin) !?Batch {
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

fn columnIndex(schema: []const Column, name: []const u8) ?usize {
    return types.findColumn(schema, name);
}

fn isStringTag(t: TypeTag) bool {
    return switch (t) {
        .varchar, .string, .char, .json => true,
        else => false,
    };
}
