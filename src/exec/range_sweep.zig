//! Range-sweep join. Specialized algorithm for pure-range INNER joins
//! with a single inequality predicate `a.x OP b.y` where OP is one of
//! `<`, `<=`, `>`, `>=`.
//!
//! Algorithm:
//!   1. Materialize both sides.
//!   2. Sort each side by its range column.
//!   3. Two-pointer sweep: as B advances in sorted order, the matching
//!      slice of A advances monotonically. Emit pairs.
//!
//! Complexity: O(N log N + M log M + matches). Compares favorably to
//! NLJ's O(N*M) when the predicate is selective. For dense matches
//! (output ≈ N*M) the per-pair predicate cost is saved but the emit
//! itself is the bottleneck.
//!
//! Restricted to:
//!   - INNER joins (v1; outer would need per-row "any actual match")
//!   - Empty `on` (no equi prefix; that path uses hash/SMJ)
//!   - Exactly one range predicate
//!   - Op in {lt, lte, gt, gte}
//!
//! Anything outside these constraints falls back to NLJ via .auto.

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
const PredicateOp = predicate.PredicateOp;

const transform = @import("../engine/transform.zig");
const join_mod = @import("join.zig");
const Spec = join_mod.Spec;


const output_batch_rows: usize = 8192;

pub const RangeSweepJoin = struct {
    allocator: Allocator,
    arena: std.heap.ArenaAllocator,

    left: Query,
    right: Query,

    /// Resolved range predicate (validated single-range constraint
    /// in create()).
    range: join_mod.Join.ResolvedRange,

    output_schema: []Column,
    left_col_count: usize,
    /// Per-output-column stats (left ⧺ all right; range joins drop no key).
    /// Cached at create. Empty when neither side has info.
    cached_stats: []const exec.ColStat = &.{},

    // Materialized state.
    left_materialized: []ColumnStore,
    right_materialized: []ColumnStore,
    left_rows: u32 = 0,
    right_rows: u32 = 0,

    // Perms sorted by the range column on each side.
    left_perm: []u32 = &.{},
    right_perm: []u32 = &.{},

    // Sweep cursors. Semantics depend on the operator:
    //   lt / lte:  walk B ascending; a_cur = first A whose x >= b.y
    //              (or > for lte). Matches are A[0..a_cur).
    //   gt / gte:  walk B descending in conceptual order; we
    //              implement by walking B ascending but tracking
    //              a_cur = first A whose x > b.y (or >= for gte).
    //              Matches are A[a_cur..N).
    b_cursor: u32 = 0,
    a_cursor: u32 = 0,
    // Per-b state: which A indices to emit. We refill these as
    // b_cursor advances. cur_emit_pos = next index to emit; loop
    // continues while cur_emit_pos < cur_emit_hi.
    cur_emit_hi: u32 = 0,
    cur_emit_pos: u32 = 0,


    // Output staging. Matched (A,B) row pairs accumulate in `a_rows`/
    // `b_rows` and flush through a bulk per-column gather — never a
    // per-cell emit.
    output_columns: []ColumnStore,
    views: []ColumnView,
    a_rows: []u32,
    b_rows: []u32,
    pair_len: usize = 0,
    pending_clear: bool = false,

    phase: Phase = .materializing,

    // `--profile-ops` phase splits (raw perf-counter ticks).
    prof_mat_ticks: i64 = 0,
    prof_sort_ticks: i64 = 0,
    prof_emitted: u64 = 0,

    const Phase = enum { materializing, sweeping, done };

    pub fn create(
        allocator: Allocator,
        left: Query,
        right: Query,
        spec: Spec,
    ) !Query {
        // Caller (Join.create routing) has already vetted that we
        // match the shape, but double-check defensively.
        if (spec.on.len != 0) return Error.JoinEmptyOnClause;
        if (spec.ranges.len != 1) return Error.JoinUnsupportedType;
        if (spec.join_type != .inner) return Error.JoinUnsupportedType;
        switch (spec.ranges[0].op) {
            .lt, .lte, .gt, .gte => {},
            else => return Error.UnsupportedOperatorForType,
        }

        var arena = std.heap.ArenaAllocator.init(allocator);
        errdefer arena.deinit();

        const left_schema = left.outputSchema();
        const right_schema = right.outputSchema();

        const rp = spec.ranges[0];
        const lidx = columnIndex(left_schema, rp.left) orelse return Error.ColumnNotFound;
        const ridx = columnIndex(right_schema, rp.right) orelse return Error.ColumnNotFound;
        const lt: TypeTag = left_schema[lidx].type;
        const rt: TypeTag = right_schema[ridx].type;
        if (lt != rt and !(isStringTag(lt) and isStringTag(rt))) {
            return Error.JoinKeyTypeMismatch;
        }

        // Output schema: all left + all right (pure range has no
        // USING-style key drop).
        const output_schema = try allocator.alloc(Column, left_schema.len + right_schema.len);
        errdefer allocator.free(output_schema);
        for (left_schema, 0..) |c, i| output_schema[i] = c;
        var out_idx: usize = left_schema.len;
        for (right_schema) |c| {
            for (output_schema[0..out_idx]) |prior| {
                if (types.columnNameEql(prior.name, c.name)) return Error.JoinColumnNameCollision;
            }
            output_schema[out_idx] = c;
            out_idx += 1;
        }

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

        const a_rows = try allocator.alloc(u32, output_batch_rows);
        errdefer allocator.free(a_rows);
        const b_rows = try allocator.alloc(u32, output_batch_rows);
        errdefer allocator.free(b_rows);

        const cached_stats = try exec.concatJoinStats(allocator, left, right, left_schema.len, null, output_schema.len);
        errdefer if (cached_stats.len > 0) allocator.free(cached_stats);

        const self = try allocator.create(RangeSweepJoin);
        errdefer allocator.destroy(self);
        self.* = .{
            .allocator = allocator,
            .arena = arena,
            .left = left,
            .right = right,
            .range = .{ .left_col = lidx, .right_col = ridx, .op = rp.op },
            .output_schema = output_schema,
            .left_col_count = left_schema.len,
            .cached_stats = cached_stats,
            .left_materialized = left_mat,
            .right_materialized = right_mat,
            .output_columns = output_columns,
            .views = views,
            .a_rows = a_rows,
            .b_rows = b_rows,
        };
        const q = makeQuery(allocator, self);
        if (spec.extra_predicate) |pred| {
            return @import("filter.zig").Filter.create(allocator, q, pred);
        }
        return q;
    }

    pub fn deinit(self: *RangeSweepJoin) void {
        var l = self.left;
        l.deinit();
        var r = self.right;
        r.deinit();
        for (self.left_materialized) |*c| c.deinit(self.allocator);
        self.allocator.free(self.left_materialized);
        for (self.right_materialized) |*c| c.deinit(self.allocator);
        self.allocator.free(self.right_materialized);
        if (self.left_perm.len > 0) self.allocator.free(self.left_perm);
        if (self.right_perm.len > 0) self.allocator.free(self.right_perm);
        for (self.output_columns) |*c| c.deinit(self.allocator);
        self.allocator.free(self.output_columns);
        self.allocator.free(self.views);
        self.allocator.free(self.a_rows);
        self.allocator.free(self.b_rows);
        self.allocator.free(self.output_schema);
        if (self.cached_stats.len > 0) self.allocator.free(@constCast(self.cached_stats));
        self.arena.deinit();
        const allocator = self.allocator;
        allocator.destroy(self);
    }

    pub fn outputSchema(self: *RangeSweepJoin) []const Column {
        return self.output_schema;
    }

    pub fn addPrune(self: *RangeSweepJoin, pred: Predicate) !void {
        self.left.addPrune(pred) catch |e| switch (e) {
            error.ColumnNotFound => {},
            else => return e,
        };
        self.right.addPrune(pred) catch |e| switch (e) {
            error.ColumnNotFound => {},
            else => return e,
        };
    }

    pub fn accountant(self: *RangeSweepJoin) ?*exec.memory.MemoryAccountant {
        return self.left.accountant();
    }

    pub fn explain(self: *RangeSweepJoin, out: *std.ArrayList(u8), allocator: std.mem.Allocator, depth: usize) !void {
        try exec.explainLine(out, allocator, depth, "RangeSweepJoin");
        try self.left.explain(out, allocator, depth + 1);
        try self.right.explain(out, allocator, depth + 1);
    }

    pub fn stats(self: *RangeSweepJoin) exec.PipelineStats {
        const l = self.left.stats();
        const r = self.right.stats();
        const product = std.math.mul(u64, l.upper_rows, r.upper_rows) catch std.math.maxInt(u64);
        return .{ .upper_rows = product, .column_stats = self.cached_stats };
    }

    pub fn next(self: *RangeSweepJoin) !?Batch {
        while (true) {
            switch (self.phase) {
                .materializing => {
                    try self.materializeAndSort();
                    self.phase = .sweeping;
                },
                .sweeping => {
                    if (try self.sweepStep()) |batch| return batch;
                    self.phase = .done;
                    if (exec.prof.enabled) {
                        std.debug.print("[rsweep] left={d} right={d} emitted={d}  mat={d:.1}ms sort={d:.1}ms\n", .{
                            self.left_rows,
                            self.right_rows,
                            self.prof_emitted,
                            exec.prof.ticksToMs(self.prof_mat_ticks),
                            exec.prof.ticksToMs(self.prof_sort_ticks),
                        });
                    }
                    if (try self.flushOutput()) |batch| return batch;
                    return null;
                },
                .done => return null,
            }
        }
    }

    fn materializeAndSort(self: *RangeSweepJoin) !void {
        const t0 = if (exec.prof.enabled) exec.prof.nowTicks() else 0;
        const acc = self.left.accountant();
        const left_row_bytes = exec.memory.estimateRowBytes(self.left.outputSchema());
        const right_row_bytes = exec.memory.estimateRowBytes(self.right.outputSchema());

        while (try self.left.next()) |batch| {
            if (acc) |a| try a.reserve(.range_sweep, batch.row_count * left_row_bytes);
            for (batch.values, 0..) |v, i| {
                try transform.appendAllColumn(self.allocator, v, &self.left_materialized[i]);
            }
            self.left_rows += @intCast(batch.row_count);
        }
        while (try self.right.next()) |batch| {
            if (acc) |a| try a.reserve(.range_sweep, batch.row_count * right_row_bytes);
            for (batch.values, 0..) |v, i| {
                try transform.appendAllColumn(self.allocator, v, &self.right_materialized[i]);
            }
            self.right_rows += @intCast(batch.row_count);
        }

        // Build perms (skip null-key rows on either side — NULL never
        // satisfies any inequality).
        var lp: std.ArrayList(u32) = .empty;
        defer lp.deinit(self.allocator);
        const lv = self.left_materialized[self.range.left_col].view();
        var li: u32 = 0;
        while (li < self.left_rows) : (li += 1) {
            if (lv.isValid(li)) try lp.append(self.allocator, li);
        }
        var rp: std.ArrayList(u32) = .empty;
        defer rp.deinit(self.allocator);
        const rv = self.right_materialized[self.range.right_col].view();
        var ri: u32 = 0;
        while (ri < self.right_rows) : (ri += 1) {
            if (rv.isValid(ri)) try rp.append(self.allocator, ri);
        }

        // Sort each perm by the range column (ASC).
        const t1 = if (exec.prof.enabled) exec.prof.nowTicks() else 0;
        if (exec.prof.enabled) self.prof_mat_ticks = t1 - t0;
        sortByColumn(lp.items, lv);
        sortByColumn(rp.items, rv);
        if (exec.prof.enabled) self.prof_sort_ticks = exec.prof.nowTicks() - t1;

        self.left_perm = try lp.toOwnedSlice(self.allocator);
        self.right_perm = try rp.toOwnedSlice(self.allocator);

        // For .lt/.lte: a_cursor tracks first A whose x >= b.y (or >).
        //   Matches are A[0..a_cursor).
        // For .gt/.gte: a_cursor tracks first A whose x > b.y (or >=).
        //   Matches are A[a_cursor..N).
        self.a_cursor = 0;
    }

    fn sweepStep(self: *RangeSweepJoin) !?Batch {
        if (self.pending_clear) {
            for (self.output_columns) |*c| c.clear();
            self.pending_clear = false;
        }

        while (true) {
            // Drain any in-progress emit slice first: stage as many (A,b)
            // pairs as the batch has room for, then bulk-gather.
            if (self.cur_emit_pos < self.cur_emit_hi) {
                const b_row = self.right_perm[self.b_cursor];
                const room = output_batch_rows - self.pair_len;
                const avail: usize = self.cur_emit_hi - self.cur_emit_pos;
                const take = @min(room, avail);
                @memcpy(
                    self.a_rows[self.pair_len..][0..take],
                    self.left_perm[self.cur_emit_pos..][0..take],
                );
                @memset(self.b_rows[self.pair_len..][0..take], b_row);
                self.pair_len += take;
                self.cur_emit_pos += @intCast(take);
                if (self.cur_emit_pos >= self.cur_emit_hi) {
                    // Slice exhausted — advance to the next b.
                    self.b_cursor += 1;
                    self.cur_emit_hi = 0;
                    self.cur_emit_pos = 0;
                }
                if (self.pair_len == output_batch_rows) {
                    try self.gatherPairs();
                    return try self.flushOutput();
                }
                continue;
            }

            // Advance to the next b and compute its matching A slice.
            if (self.b_cursor >= self.right_perm.len) break;
            const b_row = self.right_perm[self.b_cursor];
            const slice = self.matchingSliceFor(b_row);
            if (slice.lo >= slice.hi) {
                // Empty slice — skip this b without entering the
                // emit loop (otherwise we'd loop forever).
                self.b_cursor += 1;
                continue;
            }
            self.cur_emit_hi = slice.hi;
            self.cur_emit_pos = slice.lo;
        }
        // Tail: gather any staged pairs; the caller's final flushOutput
        // picks up the rows.
        if (self.pair_len > 0) try self.gatherPairs();
        return null;
    }

    /// Bulk-gather the staged (A,B) row pairs into the output columns —
    /// one `appendByIndices` per column instead of a per-cell emit.
    fn gatherPairs(self: *RangeSweepJoin) !void {
        const a = self.a_rows[0..self.pair_len];
        const b = self.b_rows[0..self.pair_len];
        for (self.left_materialized, 0..) |*store, i| {
            try transform.appendByIndices(self.allocator, store.view(), a, &self.output_columns[i]);
        }
        for (self.right_materialized, 0..) |*store, i| {
            try transform.appendByIndices(self.allocator, store.view(), b, &self.output_columns[self.left_col_count + i]);
        }
        self.prof_emitted += self.pair_len;
        self.pair_len = 0;
    }

    const Slice = struct { lo: u32, hi: u32 };

    /// Compute the slice of `left_perm` whose A's match this b.
    /// For lt/lte: a_cursor monotonically grows (more A's satisfy as
    /// b.y grows). For gt/gte: a_cursor monotonically grows too (more
    /// A's get EXCLUDED as b.y grows).
    fn matchingSliceFor(self: *RangeSweepJoin, b_row: u32) Slice {
        const lv = self.left_materialized[self.range.left_col].view();
        const rv = self.right_materialized[self.range.right_col].view();
        switch (self.range.op) {
            .lt => {
                // Advance a_cursor while A[a_cursor].x < b.y.
                while (self.a_cursor < self.left_perm.len and
                    cmpCells(lv, self.left_perm[self.a_cursor], rv, b_row) == .lt)
                {
                    self.a_cursor += 1;
                }
                return .{ .lo = 0, .hi = self.a_cursor };
            },
            .lte => {
                while (self.a_cursor < self.left_perm.len and
                    cmpCells(lv, self.left_perm[self.a_cursor], rv, b_row) != .gt)
                {
                    self.a_cursor += 1;
                }
                return .{ .lo = 0, .hi = self.a_cursor };
            },
            .gt => {
                // Advance a_cursor while A[a_cursor].x <= b.y. Match
                // slice = A[a_cursor..N).
                while (self.a_cursor < self.left_perm.len and
                    cmpCells(lv, self.left_perm[self.a_cursor], rv, b_row) != .gt)
                {
                    self.a_cursor += 1;
                }
                return .{ .lo = self.a_cursor, .hi = @intCast(self.left_perm.len) };
            },
            .gte => {
                while (self.a_cursor < self.left_perm.len and
                    cmpCells(lv, self.left_perm[self.a_cursor], rv, b_row) == .lt)
                {
                    self.a_cursor += 1;
                }
                return .{ .lo = self.a_cursor, .hi = @intCast(self.left_perm.len) };
            },
            else => unreachable,
        }
    }

    fn flushOutput(self: *RangeSweepJoin) !?Batch {
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
        .varchar, .string, .char => true,
        else => false,
    };
}

/// Compare two cells in materialized columns (same-typed by construction).
fn cmpCells(left: ColumnView, lrow: u32, right: ColumnView, rrow: u32) std.math.Order {
    switch (left.data) {
        .int => return std.math.order(left.data.int[lrow], right.data.int[rrow]),
        .bigint => return std.math.order(left.data.bigint[lrow], right.data.bigint[rrow]),
        .boolean => return std.math.order(left.data.boolean[lrow], right.data.boolean[rrow]),
        .float => return std.math.order(left.data.float[lrow], right.data.float[rrow]),
        .double => return std.math.order(left.data.double[lrow], right.data.double[rrow]),
        .date => return std.math.order(left.data.date[lrow], right.data.date[rrow]),
        .datetime => return std.math.order(left.data.datetime[lrow], right.data.datetime[rrow]),
        .tinyint => return std.math.order(left.data.tinyint[lrow], right.data.tinyint[rrow]),
        .smallint => return std.math.order(left.data.smallint[lrow], right.data.smallint[rrow]),
        .largeint => return std.math.order(left.data.largeint[lrow], right.data.largeint[rrow]),
        .decimal64 => return std.math.order(left.data.decimal64[lrow], right.data.decimal64[rrow]),
        .decimal128 => return std.math.order(left.data.decimal128[lrow], right.data.decimal128[rrow]),
        .uuid => return std.math.order(left.data.uuid[lrow], right.data.uuid[rrow]),
        .varchar => return std.mem.order(u8, left.data.varchar.rowBytes(lrow), right.data.varchar.rowBytes(rrow)),
        .string => return std.mem.order(u8, left.data.string.rowBytes(lrow), right.data.string.rowBytes(rrow)),
        .char => return std.mem.order(u8, left.data.char.rowBytes(lrow), right.data.char.rowBytes(rrow)),
    }
}

/// Sort perm[] ASC by the values in `col`. Indirect — perm is reordered
/// but `col` data is not.
fn sortByColumn(perm: []u32, col: ColumnView) void {
    const Ctx = struct {
        perm: []u32,
        col: ColumnView,
        pub fn lessThan(self: @This(), a: usize, b: usize) bool {
            return cmpInColumn(self.col, self.perm[a], self.perm[b]) == .lt;
        }
        pub fn swap(self: @This(), a: usize, b: usize) void {
            std.mem.swap(u32, &self.perm[a], &self.perm[b]);
        }
    };
    std.sort.pdqContext(0, perm.len, Ctx{ .perm = perm, .col = col });
}

fn cmpInColumn(col: ColumnView, a: u32, b: u32) std.math.Order {
    return switch (col.data) {
        .int => std.math.order(col.data.int[a], col.data.int[b]),
        .bigint => std.math.order(col.data.bigint[a], col.data.bigint[b]),
        .boolean => std.math.order(col.data.boolean[a], col.data.boolean[b]),
        .float => std.math.order(col.data.float[a], col.data.float[b]),
        .double => std.math.order(col.data.double[a], col.data.double[b]),
        .date => std.math.order(col.data.date[a], col.data.date[b]),
        .datetime => std.math.order(col.data.datetime[a], col.data.datetime[b]),
        .tinyint => std.math.order(col.data.tinyint[a], col.data.tinyint[b]),
        .smallint => std.math.order(col.data.smallint[a], col.data.smallint[b]),
        .largeint => std.math.order(col.data.largeint[a], col.data.largeint[b]),
        .decimal64 => std.math.order(col.data.decimal64[a], col.data.decimal64[b]),
        .decimal128 => std.math.order(col.data.decimal128[a], col.data.decimal128[b]),
        .uuid => std.math.order(col.data.uuid[a], col.data.uuid[b]),
        .varchar => std.mem.order(u8, col.data.varchar.rowBytes(a), col.data.varchar.rowBytes(b)),
        .string => std.mem.order(u8, col.data.string.rowBytes(a), col.data.string.rowBytes(b)),
        .char => std.mem.order(u8, col.data.char.rowBytes(a), col.data.char.rowBytes(b)),
    };
}

