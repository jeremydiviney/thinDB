//! Join operator. v1 scope:
//!   - INNER equi-join only.
//!   - Hash algorithm only (SMJ + INLJ + NLJ come in follow-ups).
//!   - Single- or multi-column join key (all keys equality).
//!   - Build side chosen automatically by `upper_rows` comparison;
//!     the smaller-by-upper-bound side is materialized fully and
//!     becomes the hash table; the other side streams as probe.
//!   - NULL join-key values never match anything (standard SQL).
//!
//! Future (other commits add):
//!   - SMJ + decision tree (hash vs SMJ via observed skew stats)
//!   - INLJ for small × large-sorted
//!   - NLJ for tiny × tiny + non-equi predicates
//!   - LEFT / RIGHT / FULL OUTER, SEMI, ANTI join types
//!   - Memory accountant integration (refuse pre-flight when build
//!     too big; abort mid-build if it grows past budget)
//!   - HLL / peek-scan for refined cardinality before materialization

const std = @import("std");
const Allocator = std.mem.Allocator;

const types = @import("../types.zig");
const Column = types.Column;
const TypeTag = types.TypeTag;

const storage = @import("../storage/storage.zig");
const ColumnView = storage.ColumnView;
const StringView = storage.StringView;

const engine = @import("../engine/engine.zig");
const ColumnStore = engine.ColumnStore;
const StringStore = engine.StringStore;

const exec = @import("exec.zig");
const Query = exec.Query;
const Batch = exec.Batch;
const Error = exec.Error;
const makeQuery = exec.makeQuery;

const predicate = @import("predicate.zig");
const Predicate = predicate.Predicate;

const transform = @import("../engine/transform.zig");

/// One column-pair equality in the ON clause.
pub const KeyPair = struct {
    left: []const u8,
    right: []const u8,
};

pub const JoinType = enum {
    inner,
    // Future: left, right, full, semi, anti, cross.
};

pub const Algorithm = enum {
    /// Planner picks per-query using cheap stats (sort_state per
    /// side, eventually Misra-Gries-observed skew). Default. Always
    /// safe — the planner only ever picks an algorithm it can prove
    /// won't catastrophically fail given the available signal.
    auto,
    /// Build a hash table on the smaller side; probe with the other.
    /// Best for equi-joins with at least one side fitting comfortably
    /// in memory, no heavy skew. Pick explicitly when you know the
    /// shape and want to skip the planner.
    hash,
    /// Sort both sides on the join key, walk in lockstep. Predictable
    /// memory + degrades smoothly under skew. Best when both sides
    /// are large, skew is heavy, or output needs to be sorted.
    sort_merge,
    // Future: inlj, nlj
};

pub const Spec = struct {
    join_type: JoinType = .inner,
    on: []const KeyPair,
    /// Algorithm choice. Default `.auto` lets the planner decide
    /// from cheap stats. Override with `.hash` or `.sort_merge`
    /// when you want to lock the choice (benchmarking, known shape).
    algorithm: Algorithm = .auto,
};

/// Number of rows emitted per output batch. Bounded so emission stays
/// streaming even when one probe row matches many build rows.
const output_batch_rows: usize = 1024;

pub const Join = struct {
    allocator: Allocator,
    arena: std.heap.ArenaAllocator,

    left: Query,
    right: Query,
    join_type: JoinType,

    /// Per-side join key column indices, in the order they appear in
    /// the `on` spec. Used by the compound-key builder.
    left_key_indices: []usize,
    right_key_indices: []usize,

    /// True iff left is the smaller side and is therefore the build
    /// side. False iff right is build. Build is materialized fully;
    /// the other side streams as probe.
    build_is_left: bool,

    /// Output schema = left.schema ++ (right.schema − right join keys).
    /// Right-side join keys are dropped from output (USING semantic):
    /// they'd be equal to the left's copies by construction.
    output_schema: []Column,
    /// Number of columns from the left side; right columns start at
    /// this index in `output_schema` and `output_columns`.
    left_col_count: usize,
    /// Per right-side column: true if it should be emitted (i.e., it
    /// isn't a join key). Sized to right_schema.len.
    right_kept_mask: []const bool,

    /// Materialized build side. One ColumnStore per build-side column.
    build_columns: []ColumnStore,
    build_rows: u32 = 0,

    /// Hash table: compound key bytes → list of row indices into
    /// `build_columns`. Multiple matches per key carried as a list.
    /// All allocations land in `arena`; map and lists too.
    hash_table: std.StringHashMapUnmanaged(std.ArrayListUnmanaged(u32)),

    /// Reusable scratch buffer for building per-row compound keys.
    /// Cleared+reused per row to avoid one alloc per probe.
    key_scratch: std.ArrayList(u8),

    /// Output staging: ColumnStores we append matched rows into,
    /// emitted as a single Batch when full or when probe is exhausted.
    output_columns: []ColumnStore,
    views: []ColumnView,

    /// State machine.
    phase: Phase = .building,
    /// While probing: current probe batch + position within it. We
    /// can't fully consume a probe row in one .next() call if it
    /// matches many build rows; we resume from where we left off.
    cur_probe_batch: ?Batch = null,
    cur_probe_row: u32 = 0,
    /// While processing one probe row's matches: the bucket list +
    /// position within it.
    cur_match_list: []const u32 = &.{},
    cur_match_pos: usize = 0,
    /// True if we just flushed a batch and the next emit should
    /// clear output_columns before appending. We can't clear at the
    /// end of flushOutput because the returned Batch's views still
    /// borrow into output_columns' buffers.
    pending_clear: bool = false,

    const Phase = enum { building, probing, done };

    pub fn create(
        allocator: Allocator,
        left: Query,
        right: Query,
        spec: Spec,
    ) !Query {
        if (spec.join_type != .inner) return Error.JoinUnsupportedType;
        if (spec.on.len == 0) return Error.JoinEmptyOnClause;

        // Resolve algorithm. .auto consults cheap stats — see chooseAlgorithm.
        const chosen = if (spec.algorithm == .auto)
            chooseAlgorithm(left, right, spec.on)
        else
            spec.algorithm;

        if (chosen == .sort_merge) {
            // Re-spec with explicit algorithm so SMJ doesn't recurse
            // back through .auto if it ever calls back.
            const sm_spec: Spec = .{
                .join_type = spec.join_type,
                .on = spec.on,
                .algorithm = .sort_merge,
            };
            return @import("smj.zig").SortMergeJoin.create(allocator, left, right, sm_spec);
        }

        var arena = std.heap.ArenaAllocator.init(allocator);
        errdefer arena.deinit();
        const aa = arena.allocator();

        const left_schema = left.outputSchema();
        const right_schema = right.outputSchema();

        // Resolve key column indices on each side.
        const left_keys = try aa.alloc(usize, spec.on.len);
        const right_keys = try aa.alloc(usize, spec.on.len);
        for (spec.on, 0..) |pair, i| {
            left_keys[i] = columnIndex(left_schema, pair.left) orelse return Error.ColumnNotFound;
            right_keys[i] = columnIndex(right_schema, pair.right) orelse return Error.ColumnNotFound;
            // Types must match by tag (varchar/string/char are
            // compatible string-family; otherwise must match exactly).
            const lt: TypeTag = left_schema[left_keys[i]].type;
            const rt: TypeTag = right_schema[right_keys[i]].type;
            if (lt != rt and !(isStringTag(lt) and isStringTag(rt))) {
                return Error.JoinKeyTypeMismatch;
            }
        }

        // Build right-side column index → keep? map. The right side's
        // join-key columns are DROPPED from the output (USING-clause
        // semantic: emit just the left's copy since they're equal by
        // construction). This lets users join two tables that share a
        // join column name (the common case) without manual aliasing.
        const right_kept_mask = try aa.alloc(bool, right_schema.len);
        for (right_kept_mask) |*m| m.* = true;
        for (right_keys) |idx| right_kept_mask[idx] = false;

        var right_kept_count: usize = 0;
        for (right_kept_mask) |m| {
            if (m) right_kept_count += 1;
        }

        // Compose output schema: left columns + right columns minus
        // join keys. Refuse if any NON-KEY column name collides — the
        // user must explicitly rename via .compute() / .exclude() in
        // that case.
        const output_schema = try allocator.alloc(Column, left_schema.len + right_kept_count);
        errdefer allocator.free(output_schema);
        for (left_schema, 0..) |c, i| output_schema[i] = c;
        var out_idx: usize = left_schema.len;
        for (right_schema, 0..) |c, i| {
            if (!right_kept_mask[i]) continue;
            // Check against left columns + already-placed right columns.
            for (output_schema[0..out_idx]) |prior| {
                if (std.mem.eql(u8, prior.name, c.name)) return Error.JoinColumnNameCollision;
            }
            output_schema[out_idx] = c;
            out_idx += 1;
        }

        // Persist the right-keep mask for emission.
        const right_kept_mask_owned = try allocator.alloc(bool, right_schema.len);
        @memcpy(right_kept_mask_owned, right_kept_mask);
        errdefer allocator.free(right_kept_mask_owned);

        // Pick build side by upper-bound row count.
        const left_stats = left.stats();
        const right_stats = right.stats();
        const build_is_left = left_stats.upper_rows <= right_stats.upper_rows;

        const build_schema = if (build_is_left) left_schema else right_schema;

        // Allocate build-side column stores.
        const build_columns = try allocator.alloc(ColumnStore, build_schema.len);
        errdefer allocator.free(build_columns);
        var binited: usize = 0;
        errdefer for (build_columns[0..binited]) |*c| c.deinit(allocator);
        for (build_schema, 0..) |col, i| {
            build_columns[i] = try ColumnStore.init(allocator, col.type, col.nullable);
            binited += 1;
        }

        // Output staging.
        const output_columns = try allocator.alloc(ColumnStore, output_schema.len);
        errdefer allocator.free(output_columns);
        var oinited: usize = 0;
        errdefer for (output_columns[0..oinited]) |*c| c.deinit(allocator);
        for (output_schema, 0..) |col, i| {
            output_columns[i] = try ColumnStore.init(allocator, col.type, col.nullable);
            oinited += 1;
        }

        const views = try allocator.alloc(ColumnView, output_schema.len);
        errdefer allocator.free(views);

        const self = try allocator.create(Join);
        errdefer allocator.destroy(self);
        self.* = .{
            .allocator = allocator,
            .arena = arena,
            .left = left,
            .right = right,
            .join_type = spec.join_type,
            .left_key_indices = left_keys,
            .right_key_indices = right_keys,
            .build_is_left = build_is_left,
            .output_schema = output_schema,
            .left_col_count = left_schema.len,
            .right_kept_mask = right_kept_mask_owned,
            .build_columns = build_columns,
            .hash_table = .empty,
            .key_scratch = .empty,
            .output_columns = output_columns,
            .views = views,
        };
        return makeQuery(allocator, self);
    }

    pub fn deinit(self: *Join) void {
        var l = self.left;
        l.deinit();
        var r = self.right;
        r.deinit();
        for (self.build_columns) |*c| c.deinit(self.allocator);
        self.allocator.free(self.build_columns);
        for (self.output_columns) |*c| c.deinit(self.allocator);
        self.allocator.free(self.output_columns);
        self.allocator.free(self.views);
        self.allocator.free(self.output_schema);
        self.allocator.free(self.right_kept_mask);
        self.key_scratch.deinit(self.allocator);
        self.arena.deinit();
        const allocator = self.allocator;
        allocator.destroy(self);
    }

    pub fn outputSchema(self: *Join) []const Column {
        return self.output_schema;
    }

    pub fn addPrune(self: *Join, pred: Predicate) !void {
        // Push pruning to both sides; each will only accept predicates
        // referencing its own columns (via the column-not-found check
        // in its addPrune). The other side silently ignores via the
        // existing error path.
        self.left.addPrune(pred) catch |e| switch (e) {
            error.ColumnNotFound => {},
            else => return e,
        };
        self.right.addPrune(pred) catch |e| switch (e) {
            error.ColumnNotFound => {},
            else => return e,
        };
    }

    /// Pre-execution stats on join output.
    /// Upper bound: left × right (cross-product worst case for inner).
    /// Sort state: empty (hash join output is unordered).
    pub fn stats(self: *Join) exec.PipelineStats {
        const l = self.left.stats();
        const r = self.right.stats();
        const product = std.math.mul(u64, l.upper_rows, r.upper_rows) catch std.math.maxInt(u64);
        return .{ .upper_rows = product };
    }

    pub fn next(self: *Join) !?Batch {
        while (true) {
            switch (self.phase) {
                .building => {
                    try self.buildPhase();
                    self.phase = .probing;
                },
                .probing => {
                    if (try self.probeStep()) |batch| return batch;
                    // probeStep returned null → exhausted
                    self.phase = .done;
                    // Flush any remaining staged output before going to done.
                    if (try self.flushOutput()) |batch| return batch;
                    return null;
                },
                .done => return null,
            }
        }
    }

    // -----------------------------------------------------------------
    // Build phase: drain the build side, materialize into
    // build_columns, populate the hash table.
    // -----------------------------------------------------------------

    fn buildPhase(self: *Join) !void {
        var up = if (self.build_is_left) &self.left else &self.right;
        const key_indices = if (self.build_is_left) self.left_key_indices else self.right_key_indices;

        while (try up.next()) |batch| {
            const n = batch.row_count;
            // Append batch into build_columns.
            for (batch.values, 0..) |v, i| {
                try transform.appendAllColumn(self.allocator, v, &self.build_columns[i]);
            }
            // Insert into hash table per row.
            var i: u32 = 0;
            while (i < n) : (i += 1) {
                // Skip rows where any join key is null — SQL semantic:
                // NULL never matches anything.
                if (anyKeyNull(batch, key_indices, i)) {
                    self.build_rows += 1;
                    continue;
                }
                self.key_scratch.clearRetainingCapacity();
                try buildCompoundKey(self.allocator, &self.key_scratch, batch, key_indices, i);

                const aa = self.arena.allocator();
                const gop = try self.hash_table.getOrPut(aa, self.key_scratch.items);
                if (!gop.found_existing) {
                    // Dup the key into the arena — scratch is reused.
                    gop.key_ptr.* = try aa.dupe(u8, self.key_scratch.items);
                    gop.value_ptr.* = .empty;
                }
                try gop.value_ptr.append(aa, self.build_rows + i);
            }
            self.build_rows += @intCast(n);
        }
    }

    // -----------------------------------------------------------------
    // Probe phase: stream the probe side, look each row up in the
    // hash table, emit matched output rows.
    // -----------------------------------------------------------------

    fn probeStep(self: *Join) !?Batch {
        var probe = if (self.build_is_left) &self.right else &self.left;
        const probe_key_indices = if (self.build_is_left) self.right_key_indices else self.left_key_indices;

        while (true) {
            // If we still have an in-progress match list, continue emitting from it.
            if (self.cur_match_pos < self.cur_match_list.len) {
                const consumed_full = try self.emitMatchesUntilFull(probe_key_indices);
                if (consumed_full) return try self.flushOutput();
                // Otherwise we exhausted the current match list; fall
                // through to advance to the next probe row.
            }

            // Advance within the current probe batch, or fetch next.
            if (self.cur_probe_batch) |batch| {
                if (self.cur_probe_row >= batch.row_count) {
                    self.cur_probe_batch = null;
                    self.cur_probe_row = 0;
                    continue;
                }
                // Look up this probe row.
                if (!anyKeyNull(batch, probe_key_indices, self.cur_probe_row)) {
                    self.key_scratch.clearRetainingCapacity();
                    try buildCompoundKey(self.allocator, &self.key_scratch, batch, probe_key_indices, self.cur_probe_row);
                    if (self.hash_table.get(self.key_scratch.items)) |bucket| {
                        self.cur_match_list = bucket.items;
                        self.cur_match_pos = 0;
                        continue; // emit matches in next loop iteration
                    }
                }
                // No matches for this probe row — advance.
                self.cur_probe_row += 1;
                continue;
            }

            // Need a new probe batch.
            const next_batch = (try probe.next()) orelse {
                return null; // probe exhausted
            };
            self.cur_probe_batch = next_batch;
            self.cur_probe_row = 0;
        }
    }

    /// Emit (build_row, probe_row) output rows for the current
    /// match list until the output buffer fills. Returns `true` if
    /// the buffer is full (caller should flush+return); `false` if
    /// the current match list was exhausted (caller should advance
    /// to the next probe row).
    ///
    /// Output ordering: left columns (all of them), then right
    /// columns minus right-side join keys (USING semantic). Which
    /// physical side (build/probe) provides which depends on
    /// `build_is_left`.
    fn emitMatchesUntilFull(self: *Join, probe_key_indices: []const usize) !bool {
        _ = probe_key_indices;
        const batch = self.cur_probe_batch.?;
        const probe_row = self.cur_probe_row;
        const left_count = self.left_col_count;

        // First emit after a flush: clear leftover-but-already-yielded
        // rows from output_columns. We deferred the clear from flush
        // because the returned Batch's views borrow into these buffers.
        if (self.pending_clear) {
            for (self.output_columns) |*c| c.clear();
            self.pending_clear = false;
        }

        while (self.cur_match_pos < self.cur_match_list.len) {
            const build_row = self.cur_match_list[self.cur_match_pos];
            self.cur_match_pos += 1;

            // Append left-side columns first.
            var out_idx: usize = 0;
            if (self.build_is_left) {
                // Left side = build. All build columns are left.
                var i: usize = 0;
                while (i < left_count) : (i += 1) {
                    try appendOneFromBuild(self.allocator, &self.output_columns[out_idx], &self.build_columns[i], build_row);
                    out_idx += 1;
                }
            } else {
                // Left side = probe. All probe columns are left.
                var i: usize = 0;
                while (i < left_count) : (i += 1) {
                    try appendOneFromBatch(self.allocator, &self.output_columns[out_idx], batch.values[i], probe_row);
                    out_idx += 1;
                }
            }

            // Append right-side columns, skipping the ones in the join key mask.
            if (self.build_is_left) {
                // Right side = probe. Skip right-key columns.
                for (batch.values, 0..) |v, i| {
                    if (!self.right_kept_mask[i]) continue;
                    try appendOneFromBatch(self.allocator, &self.output_columns[out_idx], v, probe_row);
                    out_idx += 1;
                }
            } else {
                // Right side = build. Skip right-key columns.
                for (self.build_columns, 0..) |*bc, i| {
                    if (!self.right_kept_mask[i]) continue;
                    try appendOneFromBuild(self.allocator, &self.output_columns[out_idx], bc, build_row);
                    out_idx += 1;
                }
            }

            if (self.output_columns[0].data.rowCount() >= output_batch_rows) {
                return true;
            }
        }

        // Match list exhausted. Advance probe row.
        self.cur_probe_row += 1;
        self.cur_match_list = &.{};
        self.cur_match_pos = 0;
        return false;
    }

    fn flushOutput(self: *Join) !?Batch {
        const rows = self.output_columns[0].data.rowCount();
        if (rows == 0) return null;
        for (self.output_columns, 0..) |c, i| self.views[i] = c.view();
        const batch = Batch{
            .schema = self.output_schema,
            .values = self.views,
            .row_count = rows,
        };
        // Defer clearing output_columns until the START of the next
        // emit. Clearing here would set items.len = 0 on the buffers
        // the returned Batch's views point into. Caller contract
        // (matches Aggregate / Sort): consume the returned Batch
        // synchronously before calling next() again.
        self.pending_clear = true;
        return batch;
    }
};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Decision tree — picks the best algorithm from cheap stats only.
// ---------------------------------------------------------------------------
//
// v1 rules (extensible):
//
//   1. Both sides globally sorted on a prefix that covers the join
//      keys → sort_merge (the merge-only fast path is essentially
//      free; full SMJ here still beats hash by skipping the build).
//   2. Otherwise → hash (the OLAP default — best for the common
//      dim × fact shape).
//
// Future v2 refinements:
//   - Detect "small × large pre-sorted" → INLJ
//   - Materialize the smaller side first, observe max-frequency via
//     Misra-Gries, route to SMJ if skew threshold exceeded
//   - Use HLL-derived NDV estimates for output cardinality prediction
//
// The decision is intentionally conservative — we only route to SMJ
// when we can prove the structural advantage (both sides sorted).
// Hash remains the default for the broad middle of analytics shapes
// where it wins.

fn chooseAlgorithm(left: Query, right: Query, on: []const KeyPair) Algorithm {
    const ls = left.stats();
    const rs = right.stats();

    // Build the list of join-key column names per side.
    if (joinKeysCovered(ls.sort_state, on, .left) and
        joinKeysCovered(rs.sort_state, on, .right) and
        ls.sort_state.global and rs.sort_state.global)
    {
        return .sort_merge;
    }

    return .hash;
}

/// Returns true if `state.keys` is a leading prefix of (or equal to)
/// the join-key columns on the named side of the `on` pairs.
fn joinKeysCovered(state: exec.SortState, on: []const KeyPair, side: enum { left, right }) bool {
    if (state.keys.len < on.len) return false;
    for (on, 0..) |pair, i| {
        const required = switch (side) {
            .left => pair.left,
            .right => pair.right,
        };
        if (!std.mem.eql(u8, state.keys[i], required)) return false;
    }
    return true;
}

fn columnIndex(schema: []const Column, name: []const u8) ?usize {
    for (schema, 0..) |c, i| {
        if (std.mem.eql(u8, c.name, name)) return i;
    }
    return null;
}

fn isStringTag(t: TypeTag) bool {
    return switch (t) {
        .varchar, .string, .char => true,
        else => false,
    };
}

/// True if any key column has a NULL value at row `i`.
fn anyKeyNull(batch: Batch, key_indices: []const usize, i: u32) bool {
    for (key_indices) |idx| {
        if (!batch.values[idx].isValid(i)) return true;
    }
    return false;
}

/// Build a compound key for hashing/comparison. Mirrors the layout
/// used by Aggregate's groupBy key builder.
fn buildCompoundKey(
    allocator: Allocator,
    out: *std.ArrayList(u8),
    batch: Batch,
    key_indices: []const usize,
    row: u32,
) !void {
    for (key_indices) |ci| {
        const view = batch.values[ci];
        switch (view.data) {
            .int => |s| try appendInt(allocator, out, i32, s[row]),
            .bigint => |s| try appendInt(allocator, out, i64, s[row]),
            .boolean => |s| try out.append(allocator, s[row]),
            .float => |s| try appendBits(allocator, out, u32, @bitCast(s[row])),
            .double => |s| try appendBits(allocator, out, u64, @bitCast(s[row])),
            .date => |s| try appendInt(allocator, out, i32, s[row]),
            .datetime => |s| try appendInt(allocator, out, i64, s[row]),
            .tinyint => |s| try out.append(allocator, @bitCast(s[row])),
            .smallint => |s| try appendInt(allocator, out, i16, s[row]),
            .largeint => |s| try appendInt(allocator, out, i128, s[row]),
            .decimal64 => |s| try appendInt(allocator, out, i64, s[row]),
            .decimal128 => |s| try appendInt(allocator, out, i128, s[row]),
            .uuid => |s| try appendInt(allocator, out, u128, s[row]),
            .varchar, .string, .char => |sv| {
                const bytes = sv.rowBytes(row);
                try appendBits(allocator, out, u32, @intCast(bytes.len));
                try out.appendSlice(allocator, bytes);
            },
        }
    }
}

fn appendInt(allocator: Allocator, out: *std.ArrayList(u8), comptime T: type, v: T) !void {
    var b: [@sizeOf(T)]u8 = undefined;
    std.mem.writeInt(T, &b, v, .little);
    try out.appendSlice(allocator, &b);
}

fn appendBits(allocator: Allocator, out: *std.ArrayList(u8), comptime T: type, v: T) !void {
    var b: [@sizeOf(T)]u8 = undefined;
    std.mem.writeInt(T, &b, v, .little);
    try out.appendSlice(allocator, &b);
}

/// Append one row's worth of data from `src` (a build-side
/// ColumnStore) into `dst` (an output ColumnStore).
fn appendOneFromBuild(
    allocator: Allocator,
    dst: *ColumnStore,
    src: *const ColumnStore,
    row: u32,
) !void {
    try appendOneFromView(allocator, dst, src.view(), row);
}

fn appendOneFromBatch(
    allocator: Allocator,
    dst: *ColumnStore,
    src: ColumnView,
    row: u32,
) !void {
    try appendOneFromView(allocator, dst, src, row);
}

fn appendOneFromView(
    allocator: Allocator,
    dst: *ColumnStore,
    src: ColumnView,
    row: u32,
) !void {
    const valid = src.isValid(row);
    switch (src.data) {
        .int => |s| try dst.data.int.append(allocator, s[row]),
        .bigint => |s| try dst.data.bigint.append(allocator, s[row]),
        .boolean => |s| try dst.data.boolean.append(allocator, s[row]),
        .float => |s| try dst.data.float.append(allocator, s[row]),
        .double => |s| try dst.data.double.append(allocator, s[row]),
        .date => |s| try dst.data.date.append(allocator, s[row]),
        .datetime => |s| try dst.data.datetime.append(allocator, s[row]),
        .tinyint => |s| try dst.data.tinyint.append(allocator, s[row]),
        .smallint => |s| try dst.data.smallint.append(allocator, s[row]),
        .largeint => |s| try dst.data.largeint.append(allocator, s[row]),
        .decimal64 => |s| try dst.data.decimal64.append(allocator, s[row]),
        .decimal128 => |s| try dst.data.decimal128.append(allocator, s[row]),
        .uuid => |s| try dst.data.uuid.append(allocator, s[row]),
        .varchar => |sv| try dst.data.varchar.appendValue(allocator, sv.rowBytes(row)),
        .string => |sv| try dst.data.string.appendValue(allocator, sv.rowBytes(row)),
        .char => |sv| try dst.data.char.appendValue(allocator, sv.rowBytes(row)),
    }
    if (dst.nulls != null) {
        try dst.appendValidBit(allocator, dst.data.rowCount() - 1, valid);
    }
}
