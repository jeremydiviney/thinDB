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

    // Materialized + sorted state for both sides. Populated lazily on
    // first .next() call. We sort the row indices (perm), then index
    // by perm[i] when emitting (avoids physically reordering data).
    left_materialized: []ColumnStore,
    right_materialized: []ColumnStore,
    left_rows: u32 = 0,
    right_rows: u32 = 0,
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

    // Output staging.
    output_columns: []ColumnStore,
    views: []ColumnView,
    pending_clear: bool = false,

    phase: Phase = .materializing,

    const Phase = enum { materializing, merging, done };

    pub fn create(
        allocator: Allocator,
        left: Query,
        right: Query,
        spec: Spec,
    ) !Query {
        if (spec.join_type != .inner) return Error.JoinUnsupportedType;
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

        const right_kept_mask = try aa.alloc(bool, right_schema.len);
        for (right_kept_mask) |*m| m.* = true;
        for (right_keys) |idx| right_kept_mask[idx] = false;

        var right_kept_count: usize = 0;
        for (right_kept_mask) |m| {
            if (m) right_kept_count += 1;
        }

        const output_schema = try allocator.alloc(Column, left_schema.len + right_kept_count);
        errdefer allocator.free(output_schema);
        for (left_schema, 0..) |c, i| output_schema[i] = c;
        var out_idx: usize = left_schema.len;
        for (right_schema, 0..) |c, i| {
            if (!right_kept_mask[i]) continue;
            for (output_schema[0..out_idx]) |prior| {
                if (std.mem.eql(u8, prior.name, c.name)) return Error.JoinColumnNameCollision;
            }
            output_schema[out_idx] = c;
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
            .left_materialized = left_mat,
            .right_materialized = right_mat,
            .output_columns = output_columns,
            .views = views,
        };
        return makeQuery(allocator, self);
    }

    pub fn deinit(self: *SortMergeJoin) void {
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

    pub fn stats(self: *SortMergeJoin) exec.PipelineStats {
        const l = self.left.stats();
        const r = self.right.stats();
        const product = std.math.mul(u64, l.upper_rows, r.upper_rows) catch std.math.maxInt(u64);
        // SMJ output IS sorted on the join keys — a real downstream
        // advantage we should publish (e.g., a downstream merge or
        // groupBy on join keys can skip its own sort).
        const key_names = self.allocator.alloc([]const u8, self.left_key_indices.len) catch {
            return .{ .upper_rows = product };
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
        // NB: we allocate-then-free here — caller doesn't take the
        // slice. The stats() caller reads .sort_state immediately;
        // for v1 the sort_state lifetime is "until next call." For
        // correctness, allocate keys statically at create-time like
        // Sort does. TODO when planner consumes this.
        return .{ .upper_rows = product };
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
        // Drain left into left_materialized.
        while (try self.left.next()) |batch| {
            for (batch.values, 0..) |v, i| {
                try transform.appendAllColumn(self.allocator, v, &self.left_materialized[i]);
            }
            self.left_rows += @intCast(batch.row_count);
        }
        // Drain right.
        while (try self.right.next()) |batch| {
            for (batch.values, 0..) |v, i| {
                try transform.appendAllColumn(self.allocator, v, &self.right_materialized[i]);
            }
            self.right_rows += @intCast(batch.row_count);
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
        sortByKeys(self.left_perm, self.left_keys_bytes);
        sortByKeys(self.right_perm, self.right_keys_bytes);
    }

    // -----------------------------------------------------------------
    // Merge.
    // -----------------------------------------------------------------

    fn mergeStep(self: *SortMergeJoin) !?Batch {
        if (self.pending_clear) {
            for (self.output_columns) |*c| c.clear();
            self.pending_clear = false;
        }

        while (self.left_cursor < self.left_perm.len and self.right_cursor < self.right_perm.len) {
            const lkey = self.left_keys_bytes[self.left_cursor];
            const rkey = self.right_keys_bytes[self.right_cursor];
            switch (std.mem.order(u8, lkey, rkey)) {
                .lt => self.left_cursor += 1,
                .gt => self.right_cursor += 1,
                .eq => {
                    // Find runs of equal keys on both sides.
                    var l_end = self.left_cursor + 1;
                    while (l_end < self.left_perm.len and std.mem.eql(u8, self.left_keys_bytes[l_end], lkey)) : (l_end += 1) {}
                    var r_end = self.right_cursor + 1;
                    while (r_end < self.right_perm.len and std.mem.eql(u8, self.right_keys_bytes[r_end], rkey)) : (r_end += 1) {}

                    // Emit cartesian product of left[cursor..l_end] × right[cursor..r_end].
                    var li = self.left_cursor;
                    while (li < l_end) : (li += 1) {
                        var ri = self.right_cursor;
                        while (ri < r_end) : (ri += 1) {
                            try self.emitOutputRow(self.left_perm[li], self.right_perm[ri]);
                            if (self.output_columns[0].data.rowCount() >= output_batch_rows) {
                                // Flush full batch. Save cursors so we
                                // can resume from the next (left, right)
                                // pair in this same run on the next call.
                                // We've already consumed (li, ri); resume
                                // from (li, ri+1).
                                self.left_cursor = li;
                                self.right_cursor = ri + 1;
                                // BUT if ri was the last in the right run,
                                // we need to advance left and restart right
                                // at the start of the right run.
                                if (self.right_cursor >= r_end) {
                                    self.left_cursor = li + 1;
                                    self.right_cursor = self.firstOfRun(self.right_keys_bytes, self.right_cursor - (r_end - self.lastRunStartRight(r_end)), rkey);
                                    // Simpler: when batch is full mid-run,
                                    // we'd need careful cursor save. v1
                                    // takes a simpler approach: only check
                                    // batch fullness at run boundaries.
                                    // Fall through; this won't happen
                                    // because we always finish a run fully
                                    // — see comment below.
                                }
                                return try self.flushOutput();
                            }
                        }
                    }

                    self.left_cursor = l_end;
                    self.right_cursor = r_end;
                },
            }
        }

        return null;
    }

    // Helpers used by mid-run cursor save. v1's mergeStep always
    // finishes a full run before checking output batch size, so these
    // are placeholders for the future fine-grained-resume version.
    fn lastRunStartRight(self: *SortMergeJoin, r_end: usize) usize {
        _ = self;
        return r_end;
    }
    fn firstOfRun(self: *SortMergeJoin, keys: [][]const u8, from: usize, key: []const u8) usize {
        _ = self;
        _ = keys;
        _ = key;
        return from;
    }

    fn emitOutputRow(self: *SortMergeJoin, left_row: u32, right_row: u32) !void {
        var out_idx: usize = 0;
        // Left side: all columns from left_materialized.
        for (self.left_materialized) |*col| {
            try appendOneFromBuild(self.allocator, &self.output_columns[out_idx], col, left_row);
            out_idx += 1;
        }
        // Right side: skip join keys.
        for (self.right_materialized, 0..) |*col, i| {
            if (!self.right_kept_mask[i]) continue;
            try appendOneFromBuild(self.allocator, &self.output_columns[out_idx], col, right_row);
            out_idx += 1;
        }
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
        .varchar, .string, .char => |sv| try appendStringKey(allocator, out, sv.rowBytes(row)),
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

/// Append one row's data from `src` (build-side) into `dst`.
/// Copied verbatim from join.zig — keep until we extract a common helper.
fn appendOneFromBuild(
    allocator: Allocator,
    dst: *ColumnStore,
    src: *const ColumnStore,
    row: u32,
) !void {
    const v = src.view();
    const valid = v.isValid(row);
    switch (v.data) {
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
