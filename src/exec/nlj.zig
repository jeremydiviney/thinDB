//! Nested-loop join. Fully general: handles any combination of
//! equi keys + range predicates by materializing both sides and
//! double-looping. O(N*M) — use only when there's no equi prefix
//! to drive a hash/SMJ, or when at least one side is tiny.
//!
//! Same external contract as Hash / SMJ:
//!   - INNER, LEFT, RIGHT and FULL joins
//!   - Output schema = left + (right minus right join-key columns)
//!   - Multi-column equi keys via Spec.on
//!   - Range predicates (Spec.ranges) AND-combined with equi keys
//!
//! When the equi `on` clause is empty AND there are range predicates,
//! .auto picks this algorithm — there's no equi prefix to feed a
//! hash table or merge step, but we can still evaluate ranges over
//! the Cartesian product.
//!
//! An ON residual (Spec.residual) always runs here, evaluated over
//! batches of candidate pairs (`ResidualState`).

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
const PredicateExpr = predicate.PredicateExpr;
const Compute = @import("compute.zig").Compute;

const transform = @import("../engine/transform.zig");
const join_mod = @import("join.zig");
const Spec = join_mod.Spec;

const cell_io = @import("cell_io.zig");

const output_batch_rows: usize = 1024;

/// Candidate pairs per residual evaluation.
const residual_chunk_pairs: usize = 2048;

/// Pairs between cancellation checks while the loop emits nothing.
const cancel_check_pairs: usize = 1 << 16;

/// Schema-only upstream for the residual's Compute, which only ever
/// evaluates caller-supplied pair batches.
const PairSchema = struct {
    schema: []const Column,
    pub fn next(_: *PairSchema) !?Batch {
        return null;
    }
    pub fn deinit(_: *PairSchema) void {}
    pub fn outputSchema(self: *PairSchema) []const Column {
        return self.schema;
    }
    pub fn addPrune(_: *PairSchema, _: Predicate) !void {}
    pub fn stats(_: *PairSchema) exec.PipelineStats {
        return .{ .upper_rows = std.math.maxInt(u64) };
    }
    pub fn accountant(_: *PairSchema) ?*exec.memory.MemoryAccountant {
        return null;
    }
    pub fn explain(_: *PairSchema, _: *std.ArrayList(u8), _: std.mem.Allocator, _: usize) !void {}
};

/// Evaluates an ON residual over batches of candidate pairs. A batch lays
/// its pairs out like the output (left columns, kept right columns), so the
/// residual reads the names it would read above the join. Pairs run
/// left-row-major; with the left side preserved, each left row's candidates
/// end in a pair with no right row: the row's null-extension, emitted only
/// when none of its candidates passed.
const ResidualState = struct {
    /// Computes the residual's expression operands; null when it reads only
    /// columns.
    compute: ?Query,
    stub: ?*PairSchema,
    predicate: PredicateExpr,
    eval_schema: []const Column,
    /// Right rows by equi key; null without keys, where every right row is
    /// a candidate.
    index: ?std.StringHashMapUnmanaged(std.ArrayListUnmanaged(u32)) = null,
    key_scratch: std.ArrayList(u8) = .empty,
    left_views: []ColumnView,
    right_views: []ColumnView,
    pair_left: []u32,
    pair_right: []u32,
    pairs: usize = 0,
    pair_columns: []ColumnStore,
    pair_views: []ColumnView,
    mask: []bool,
    emit: []bool,
    /// Next candidate of the current left row.
    cand: u32 = 0,
    /// Whether a candidate of the current left row has passed.
    any_pass: bool = false,

    fn create(
        allocator: Allocator,
        aa: Allocator,
        residual: join_mod.Residual,
        output_schema: []const Column,
        left_width: usize,
        right_width: usize,
    ) !*ResidualState {
        const self = try aa.create(ResidualState);
        const pair_columns = try aa.alloc(ColumnStore, output_schema.len);
        var inited: usize = 0;
        errdefer for (pair_columns[0..inited]) |*c| c.deinit(allocator);
        for (output_schema, pair_columns) |col, *c| {
            c.* = try ColumnStore.init(allocator, col.type, col.nullable);
            inited += 1;
        }
        var stub: ?*PairSchema = null;
        errdefer if (stub) |s| allocator.destroy(s);
        var compute: ?Query = null;
        var eval_schema = output_schema;
        if (residual.derived.len > 0) {
            stub = try allocator.create(PairSchema);
            stub.?.* = .{ .schema = output_schema };
            compute = try Compute.createWithRegistry(allocator, makeQuery(allocator, stub.?), residual.derived, residual.udf_registry);
            eval_schema = compute.?.outputSchema();
        }
        errdefer if (compute) |*q| q.deinit();
        var validated = residual.predicate;
        try predicate.validateExpr(&validated, eval_schema);
        self.* = .{
            .compute = compute,
            .stub = stub,
            .predicate = validated,
            .eval_schema = eval_schema,
            .left_views = try aa.alloc(ColumnView, left_width),
            .right_views = try aa.alloc(ColumnView, right_width),
            .pair_left = try aa.alloc(u32, residual_chunk_pairs + 1),
            .pair_right = try aa.alloc(u32, residual_chunk_pairs + 1),
            .pair_columns = pair_columns,
            .pair_views = try aa.alloc(ColumnView, output_schema.len),
            .mask = try aa.alloc(bool, residual_chunk_pairs + 1),
            .emit = try aa.alloc(bool, residual_chunk_pairs + 1),
        };
        return self;
    }

    fn deinit(self: *ResidualState, allocator: Allocator) void {
        if (self.compute) |*q| q.deinit();
        if (self.stub) |s| allocator.destroy(s);
        for (self.pair_columns) |*c| c.deinit(allocator);
        self.key_scratch.deinit(allocator);
    }
};

/// A left row's candidate right rows: a bucket of the equi index, or every
/// right row.
const Candidates = struct {
    rows: ?[]const u32,
    count: u32,

    fn at(self: Candidates, i: u32) u32 {
        return if (self.rows) |r| r[i] else i;
    }
};

pub const NestedLoopJoin = struct {
    allocator: Allocator,
    arena: std.heap.ArenaAllocator,

    left: Query,
    right: Query,

    /// Per-side equi-key column indices, in order of Spec.on. Empty
    /// when there's no equi part (pure range / pure NLJ).
    left_key_indices: []usize,
    right_key_indices: []usize,
    /// Per equi key: whether it matches NULL to NULL. Empty when none does.
    null_safe_keys: []const bool,

    /// Range predicates resolved to column indices. AND-combined.
    ranges: []const join_mod.Join.ResolvedRange,

    /// Optional opaque per-pair predicate. Evaluated after equi +
    /// range checks; pairs returning false get dropped.
    opaque_predicate: ?join_mod.OpaquePredicate,

    residual: ?*ResidualState = null,

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
    pairs_unchecked: usize = 0,

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
    /// Rows staged in output_columns, counted apart from them: a COUNT(*)
    /// above prunes the join to zero output columns.
    output_rows: usize = 0,
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
        const left_emit = try join_mod.leftEmitCount(left_schema, spec);

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

        const output_schema = try allocator.alloc(Column, left_emit + right_kept_count);
        errdefer allocator.free(output_schema);
        for (left_schema[0..left_emit], 0..) |c, i| {
            output_schema[i] = c;
            if (left_nullable_in_output) output_schema[i].nullable = true;
        }
        var out_idx: usize = left_emit;
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

        const cached_stats = try exec.concatJoinStats(allocator, left, right, left_emit, right_kept_mask_owned, output_schema.len);
        errdefer if (cached_stats.len > 0) allocator.free(cached_stats);

        const residual = if (spec.residual) |res|
            try ResidualState.create(allocator, aa, res, output_schema, left_schema.len, right_schema.len)
        else
            null;
        errdefer if (residual) |rs| rs.deinit(allocator);

        const self = try allocator.create(NestedLoopJoin);
        errdefer allocator.destroy(self);
        self.* = .{
            .allocator = allocator,
            .arena = arena,
            .left = left,
            .right = right,
            .left_key_indices = left_keys,
            .right_key_indices = right_keys,
            .null_safe_keys = try join_mod.nullSafeKeyFlags(aa, spec.on),
            .ranges = resolved_ranges,
            .opaque_predicate = spec.opaque_predicate,
            .residual = residual,
            .left_view_buf = lvb,
            .right_view_buf = rvb,
            .output_schema = output_schema,
            .left_col_count = left_emit,
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
        if (self.residual) |rs| rs.deinit(self.allocator);
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

    pub fn addPruneSet(self: *NestedLoopJoin, set: predicate.InSet) !void {
        return self.offerPrune(.{ .set = set });
    }

    pub fn addPrune(self: *NestedLoopJoin, pred: Predicate) !void {
        return self.offerPrune(.{ .range = pred });
    }

    fn offerPrune(self: *NestedLoopJoin, offer: exec.PruneOffer) !void {
        offer.offerTo(&self.left, offer.column()) catch |e| switch (e) {
            error.ColumnNotFound => {},
            else => return e,
        };
        offer.offerTo(&self.right, offer.column()) catch |e| switch (e) {
            error.ColumnNotFound => {},
            else => return e,
        };
    }

    pub fn accountant(self: *NestedLoopJoin) ?*exec.memory.MemoryAccountant {
        return self.left.accountant();
    }

    pub fn explain(self: *NestedLoopJoin, out: *std.ArrayList(u8), allocator: std.mem.Allocator, depth: usize) !void {
        try exec.explainLine(out, allocator, depth, if (self.residual != null) "NestedLoopJoin (ON residual)" else "NestedLoopJoin");
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
                    if (self.residual) |rs| try self.prepareResidual(rs);
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
                    const step = if (self.residual) |rs| try self.residualStep(rs) else try self.loopStep();
                    if (step) |batch| return batch;
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
            self.output_rows = 0;
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
            try self.countPairs(1);
            if (self.left_key_indices.len > 0 and self.outerHasNullKey()) {
                if (preserve_left) {
                    try self.emitLeftOnlyRow(self.left_cursor);
                }
                self.left_cursor += 1;
                self.right_cursor = 0;
                self.cur_left_any_match = false;
                if (self.output_rows >= output_batch_rows) {
                    return try self.flushOutput();
                }
                continue;
            }

            while (self.right_cursor < self.right_rows) : (self.right_cursor += 1) {
                try self.countPairs(1);
                if (self.right_key_indices.len > 0 and self.innerHasNullKey()) continue;
                if (!self.passesEquiKeys()) continue;
                if (!self.passesAllRanges()) continue;
                if (!self.passesOpaque()) continue;

                try self.emitRow();
                self.cur_left_any_match = true;
                if (self.matched_right) |*mb| mb.set(self.right_cursor);
                if (self.output_rows >= output_batch_rows) {
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
            if (self.output_rows >= output_batch_rows) {
                return try self.flushOutput();
            }
        }

        return null;
    }

    fn prepareResidual(self: *NestedLoopJoin, rs: *ResidualState) !void {
        for (self.left_materialized, rs.left_views) |*c, *v| v.* = c.view();
        for (self.right_materialized, rs.right_views) |*c, *v| v.* = c.view();
        if (self.right_key_indices.len == 0) return;
        const aa = self.arena.allocator();
        var index: std.StringHashMapUnmanaged(std.ArrayListUnmanaged(u32)) = .empty;
        const right_batch = Batch{ .schema = self.right.outputSchema(), .values = rs.right_views, .row_count = self.right_rows };
        var r: u32 = 0;
        while (r < self.right_rows) : (r += 1) {
            if (join_mod.anyKeyNull(right_batch, self.right_key_indices, self.null_safe_keys, r)) continue;
            rs.key_scratch.clearRetainingCapacity();
            try join_mod.buildCompoundKey(self.allocator, &rs.key_scratch, right_batch, self.right_key_indices, self.null_safe_keys, r);
            const entry = try index.getOrPut(aa, rs.key_scratch.items);
            if (!entry.found_existing) {
                entry.key_ptr.* = try aa.dupe(u8, rs.key_scratch.items);
                entry.value_ptr.* = .empty;
            }
            try entry.value_ptr.append(aa, r);
        }
        rs.index = index;
    }

    fn residualCandidates(self: *NestedLoopJoin, rs: *ResidualState) !Candidates {
        const index = rs.index orelse return .{ .rows = null, .count = self.right_rows };
        const none: Candidates = .{ .rows = &.{}, .count = 0 };
        const left_batch = Batch{ .schema = self.left.outputSchema(), .values = rs.left_views, .row_count = self.left_rows };
        if (join_mod.anyKeyNull(left_batch, self.left_key_indices, self.null_safe_keys, self.left_cursor)) return none;
        rs.key_scratch.clearRetainingCapacity();
        try join_mod.buildCompoundKey(self.allocator, &rs.key_scratch, left_batch, self.left_key_indices, self.null_safe_keys, self.left_cursor);
        const bucket = index.get(rs.key_scratch.items) orelse return none;
        return .{ .rows = bucket.items, .count = @intCast(bucket.items.len) };
    }

    fn residualStep(self: *NestedLoopJoin, rs: *ResidualState) !?Batch {
        if (self.pending_clear) {
            for (self.output_columns) |*c| c.clear();
            self.output_rows = 0;
            self.pending_clear = false;
        }
        const preserve_left = self.join_type == .left or self.join_type == .full;
        while (self.left_cursor < self.left_rows) {
            rs.pairs = 0;
            while (self.left_cursor < self.left_rows and rs.pairs < residual_chunk_pairs) {
                const cands = try self.residualCandidates(rs);
                while (rs.cand < cands.count and rs.pairs < residual_chunk_pairs) : (rs.cand += 1) {
                    rs.pair_left[rs.pairs] = self.left_cursor;
                    rs.pair_right[rs.pairs] = cands.at(rs.cand);
                    rs.pairs += 1;
                }
                if (rs.cand < cands.count) break;
                if (preserve_left) {
                    rs.pair_left[rs.pairs] = self.left_cursor;
                    rs.pair_right[rs.pairs] = join_mod.FAST_EMPTY;
                    rs.pairs += 1;
                }
                self.left_cursor += 1;
                rs.cand = 0;
            }
            try self.evaluateResidualPairs(rs);
            if (self.output_rows > 0) return try self.flushOutput();
            try self.countPairs(@max(rs.pairs, 1));
        }
        return null;
    }

    /// A stretch of pairs that emits nothing never returns to `Query.next`,
    /// which is where KILL and disconnects are noticed, so it checks here.
    fn countPairs(self: *NestedLoopJoin, n: usize) !void {
        self.pairs_unchecked += n;
        if (self.pairs_unchecked < cancel_check_pairs) return;
        self.pairs_unchecked = 0;
        if (self.left.accountant()) |a| try a.checkCancelled();
    }

    fn evaluateResidualPairs(self: *NestedLoopJoin, rs: *ResidualState) !void {
        const n = rs.pairs;
        if (n == 0) return;
        const left_rows = rs.pair_left[0..n];
        const right_rows = rs.pair_right[0..n];
        for (rs.pair_columns) |*c| c.clear();
        for (rs.pair_columns[0..self.left_col_count], rs.left_views[0..self.left_col_count]) |*col, view| {
            try transform.appendByIndices(self.allocator, view, left_rows, col);
        }
        var out_idx = self.left_col_count;
        for (rs.right_views, self.right_kept_mask) |view, kept| {
            if (!kept) continue;
            try join_mod.Join.gatherBuildColumn(self.allocator, view, right_rows, &rs.pair_columns[out_idx]);
            out_idx += 1;
        }
        for (rs.pair_columns, rs.pair_views) |*c, *v| v.* = c.view();
        const pairs = Batch{ .schema = self.output_schema, .values = rs.pair_views, .row_count = n };
        const evaluated = if (rs.compute) |q| try exec.queryAs(Compute, q).?.evalBatch(pairs) else pairs;
        const mask = rs.mask[0..n];
        try predicate.evaluateExprGuided(self.allocator, rs.predicate, rs.eval_schema, evaluated, mask, null);

        const emit = rs.emit[0..n];
        var emitted: usize = 0;
        for (right_rows, mask, emit) |r, pass, *out| {
            if (r == join_mod.FAST_EMPTY) {
                out.* = !rs.any_pass;
                rs.any_pass = false;
            } else {
                out.* = pass;
                if (pass) {
                    rs.any_pass = true;
                    if (self.matched_right) |*mb| mb.set(r);
                }
            }
            if (out.*) emitted += 1;
        }
        if (emitted == 0) return;
        for (self.output_columns, rs.pair_views) |*out, view| {
            try transform.appendMaskedColumn(self.allocator, view, emit, out);
        }
        self.output_rows += emitted;
    }

    /// RIGHT/FULL OUTER drain: walk matched_right and emit
    /// null-extended rows for unmatched right rows.
    fn drainRightStep(self: *NestedLoopJoin) !?Batch {
        const mb = if (self.matched_right) |*m| m else return null;
        if (self.pending_clear) {
            for (self.output_columns) |*c| c.clear();
            self.output_rows = 0;
            self.pending_clear = false;
        }
        while (self.drain_cursor < self.right_rows) : (self.drain_cursor += 1) {
            if (mb.isSet(self.drain_cursor)) continue;
            try self.emitRightOnlyRow(self.drain_cursor);
            if (self.output_rows >= output_batch_rows) {
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
            self.left_materialized[0..self.left_col_count],
            left_row,
            self.right_kept_mask,
        );
        self.output_rows += 1;
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
        self.output_rows += 1;
    }

    fn outerHasNullKey(self: NestedLoopJoin) bool {
        for (self.left_key_indices, 0..) |idx, k| {
            if (!self.left_materialized[idx].view().isValid(self.left_cursor) and !self.keyNullSafe(k)) return true;
        }
        return false;
    }

    fn innerHasNullKey(self: NestedLoopJoin) bool {
        for (self.right_key_indices, 0..) |idx, k| {
            if (!self.right_materialized[idx].view().isValid(self.right_cursor) and !self.keyNullSafe(k)) return true;
        }
        return false;
    }

    fn keyNullSafe(self: NestedLoopJoin, k: usize) bool {
        return join_mod.keyIsNullSafe(self.null_safe_keys, k);
    }

    fn passesEquiKeys(self: NestedLoopJoin) bool {
        for (self.left_key_indices, self.right_key_indices, 0..) |li, ri, k| {
            const lv = self.left_materialized[li].view();
            const rv = self.right_materialized[ri].view();
            if (self.keyNullSafe(k)) {
                const left_valid = lv.isValid(self.left_cursor);
                if (left_valid != rv.isValid(self.right_cursor)) return false;
                if (!left_valid) continue;
            }
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
            self.left_materialized[0..self.left_col_count],
            self.left_cursor,
            self.right_materialized,
            self.right_cursor,
            self.right_kept_mask,
        );
        self.output_rows += 1;
    }

    fn flushOutput(self: *NestedLoopJoin) !?Batch {
        const rows = self.output_rows;
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
