//! Metadata-only MIN/MAX. When a query is `SELECT MIN(c)|MAX(c) [, ...] FROM t`
//! with no GROUP BY and no WHERE, and every aggregate is over a bare column
//! whose type carries exact per-row-group stats, the global min/max is just a
//! fold of the manifest's per-segment column min/max — no scan or decode.
//!
//! Nullable columns are supported: the writer computes null-aware min/max
//! (NULL slots are skipped), and an all-null row group/segment stores an
//! inverted `min > max` "no values" sentinel that this fold skips.
//!
//! `create` returns null (caller falls back to scan+aggregate) when any
//! precondition fails, keeping the result correct in every case:
//!   - float/double: no stats stored. string/varchar/char: only a 16-byte
//!     prefix is kept, so the extreme is approximate. uuid: skipped.
//!   - a populated memtable: unflushed rows aren't in any segment's stats.
//!   - any tombstone: a deleted row could have been the extreme, leaving the
//!     stored min/max stale.
//!   - a v1 manifest with no per-column stats, or an empty table (the normal
//!     path emits the SQL-standard NULL).

const std = @import("std");
const Allocator = std.mem.Allocator;

const types = @import("../types.zig");
const Column = types.Column;
const Type = types.Type;

const storage = @import("../storage/storage.zig");
const ColumnView = storage.ColumnView;

const engine = @import("../engine/engine.zig");
const ColumnStore = engine.ColumnStore;

const api = @import("../api/api.zig");
const Table = api.Table;

const exec = @import("exec.zig");
const Query = exec.Query;
const Batch = exec.Batch;
const predicate = @import("predicate.zig");
const cell_io = @import("cell_io.zig");

pub const Spec = struct {
    /// Column index in the table schema.
    col_idx: usize,
    is_min: bool,
    /// Output column name (the aggregate's alias). Borrowed from the IR,
    /// which outlives the operator (same lifetime contract as Aggregate).
    out_name: []const u8,
};

pub const MinMaxStats = struct {
    allocator: Allocator,
    output_schema: []Column,
    output_columns: []ColumnStore,
    views: []ColumnView,
    emitted: bool = false,

    /// Build the operator if the stats shortcut applies for every `spec`,
    /// else return null so the caller compiles the normal scan+aggregate.
    pub fn create(allocator: Allocator, table: *Table, specs: []const Spec) !?Query {
        const cols = table.schema.columns;
        for (specs) |sp| {
            if (!exactStatsType(cols[sp.col_idx].type)) return null;
        }
        // Unflushed rows aren't reflected in segment stats.
        if (table.memtable.row_count != 0) return null;

        const segs = table.manifest.segments.items;
        if (segs.len == 0) return null; // empty table → defer to normal path
        for (segs) |entry| {
            if (entry.column_stats.len < cols.len) return null; // v1 manifest
            // A deleted row may have been the extreme.
            if (try table.tombstoneCount(entry.segment_id) > 0) return null;
        }

        const out_schema = try allocator.alloc(Column, specs.len);
        errdefer allocator.free(out_schema);
        const out_cols = try allocator.alloc(ColumnStore, specs.len);
        errdefer allocator.free(out_cols);
        var inited: usize = 0;
        errdefer for (out_cols[0..inited]) |*c| c.deinit(allocator);

        for (specs, 0..) |sp, i| {
            const col = cols[sp.col_idx];
            var acc: i128 = if (sp.is_min) std.math.maxInt(i128) else std.math.minInt(i128);
            var seen = false;
            for (segs) |entry| {
                const s = entry.column_stats[sp.col_idx];
                if (s.min > s.max) continue; // all-null segment (inverted sentinel)
                seen = true;
                const v = if (sp.is_min) s.min else s.max;
                if (sp.is_min) {
                    if (v < acc) acc = v;
                } else {
                    if (v > acc) acc = v;
                }
            }
            out_schema[i] = .{ .name = sp.out_name, .type = col.type, .nullable = true };
            out_cols[i] = try ColumnStore.init(allocator, col.type, true);
            inited += 1;
            if (seen) {
                try appendStatValue(allocator, &out_cols[i], col.type, acc);
                try out_cols[i].appendValidBit(allocator, 0, true);
            } else {
                // No non-null value anywhere → MIN/MAX is NULL.
                try cell_io.appendNullTo(allocator, &out_cols[i]);
            }
        }

        const views = try allocator.alloc(ColumnView, specs.len);
        errdefer allocator.free(views);

        const self = try allocator.create(MinMaxStats);
        self.* = .{
            .allocator = allocator,
            .output_schema = out_schema,
            .output_columns = out_cols,
            .views = views,
        };
        return exec.makeQuery(allocator, self);
    }

    pub fn next(self: *MinMaxStats) !?Batch {
        if (self.emitted) return null;
        self.emitted = true;
        for (self.output_columns, 0..) |c, i| self.views[i] = c.view();
        return Batch{ .schema = self.output_schema, .values = self.views, .row_count = 1 };
    }

    pub fn deinit(self: *MinMaxStats) void {
        for (self.output_columns) |*c| c.deinit(self.allocator);
        self.allocator.free(self.output_columns);
        self.allocator.free(self.output_schema);
        self.allocator.free(self.views);
        self.allocator.destroy(self);
    }

    pub fn outputSchema(self: *MinMaxStats) []const Column {
        return self.output_schema;
    }

    pub fn addPrune(_: *MinMaxStats, _: predicate.Predicate) !void {}

    pub fn stats(_: *MinMaxStats) exec.PipelineStats {
        return .{ .upper_rows = 1 };
    }

    pub fn accountant(_: *MinMaxStats) ?*exec.memory.MemoryAccountant {
        return null;
    }

    pub fn explain(self: *MinMaxStats, out: *std.ArrayList(u8), allocator: Allocator, depth: usize) !void {
        _ = self;
        try exec.explainIndent(out, allocator, depth);
        try out.appendSlice(allocator, "MinMaxStats (metadata-only MIN/MAX)\n");
    }
};

pub const MetaKind = enum { count_star, count_col, min, max, sum, avg };

pub const MetaSpec = struct {
    kind: MetaKind,
    /// Column index in the table schema. Unused for `count_star`.
    col_idx: usize = 0,
    /// Output column name (the aggregate's alias). Borrowed from the IR.
    out_name: []const u8,
};

/// Rows the stats lane may read itself (the memtable's, plus those of the
/// row groups it decodes) before it hands the statement to the parallel
/// scan: always one row group's worth, else an eighth of the table, past
/// which a serial read stops beating the parallel scan's wall time.
const READ_BUDGET_MIN_ROWS: u64 = 1 << 16;
const READ_BUDGET_TABLE_FRACTION: u64 = 8;

/// Metadata lane for a bare global aggregate: any mix of COUNT(*),
/// COUNT(col), MIN/MAX over exact-stats columns, and SUM/AVG over numeric
/// columns (the SMA sums, see `format.Stats`), with no WHERE / GROUP BY /
/// HAVING / derived. COUNT(*) and COUNT(non-nullable col) are exact for any
/// table state (tombstones subtract, the memtable adds). The stats-dependent
/// specs (MIN/MAX, SUM/AVG, COUNT(nullable col) via null_count) fold:
///   - a tombstone-free segment's stats;
///   - in a segment with tombstones, each tombstone-free row group's stats,
///     and the live rows of the others, decoded: a deleted row is still in
///     its row group's stats;
///   - the memtable's rows, which no stats cover.
/// MIN/MAX decode a row group with tombstones only while its stored bound
/// can still beat the running extreme, best bound first, so a deleted
/// extreme usually costs one row group. SUM/AVG/COUNT(nullable col) decode
/// every row group with tombstones. `create` returns null, for the scan
/// path, when those reads would pass the `READ_BUDGET_*` rows, and for an
/// empty table (the normal path emits the documented default).
///
/// Held under the ddl shared lock so compaction can't retire a captured
/// segment (and its tombstone file) mid-read; it reads the statement's
/// snapshot of the table, as its scans do.
pub const MetaAggStats = struct {
    allocator: Allocator,
    output_schema: []Column,
    output_columns: []ColumnStore,
    views: []ColumnView,
    /// Row-group column blocks `create` decoded, for EXPLAIN.
    blocks_read: usize,
    emitted: bool = false,

    pub fn create(allocator: Allocator, table: *Table, specs: []const MetaSpec, statement_accountant: ?*exec.memory.MemoryAccountant) !?Query {
        const cols = table.schema.columns;
        const roles = try allocator.alloc(Role, specs.len);
        defer allocator.free(roles);
        var needs_stats = false;
        for (specs, roles) |sp, *role| {
            role.* = switch (sp.kind) {
                .min, .max => if (exactStatsType(cols[sp.col_idx].type)) .bounded else return null,
                .sum, .avg => if (sumStatsType(cols[sp.col_idx].type)) .full else return null,
                // A nullable column's non-null rows come from stats null_count.
                .count_col => if (cols[sp.col_idx].nullable) .full else .rows,
                .count_star => .rows,
            };
            if (role.* != .rows) needs_stats = true;
        }
        // A MIN/MAX over a column that every row group with tombstones
        // decodes anyway folds those rows rather than visiting by bound.
        for (specs, roles) |sp, *role| {
            if (role.* != .bounded) continue;
            for (specs, roles) |other, other_role| {
                if (other_role == .full and other.col_idx == sp.col_idx) role.* = .full;
            }
        }

        table.ddl_lock.lockSharedUncancelable(table.io);
        defer table.ddl_lock.unlockShared(table.io);
        const snap = try exec.Scan.captureSnapshotAlloc(table, allocator, statement_accountant);
        defer {
            snap.release(table);
            allocator.free(snap.segments);
        }

        const segs = snap.segments;
        if (needs_stats) {
            if (segs.len == 0 and snap.memtable_row_count == 0) return null;
            for (segs) |entry| {
                if (entry.column_stats.len < cols.len) return null; // v1 manifest
            }
        }

        const folds = try allocator.alloc(Fold, specs.len);
        defer allocator.free(folds);
        for (folds, specs) |*f, sp| f.* = .init(sp.kind);

        var dirty: DirtyRowGroups = .{};
        defer dirty.deinit(allocator, table);

        var total: u64 = snap.memtable_row_count;
        for (segs) |entry| {
            const dead = try table.tombstoneCountAt(entry.segment_id, snap.tomb_epoch);
            total += entry.row_count - dead;
            if (!needs_stats) continue;
            if (dead == 0) {
                foldUnitStats(folds, specs, roles, cols, entry.column_stats, entry.row_count);
                continue;
            }
            try dirty.segments.ensureUnusedCapacity(allocator, 1);
            const handle = try table.acquireSegment(entry.segment_id);
            const tombs = (table.acquireSegmentTombstones(handle, snap.tomb_epoch) catch |err| {
                table.releaseSegment(handle);
                return err;
            }) orelse {
                // Counted above as of the same epoch, so not expected; the
                // scan path stays exact either way.
                table.releaseSegment(handle);
                return null;
            };
            dirty.segments.appendAssumeCapacity(.{ .handle = handle, .tombs = tombs });
            const seg_slot: u32 = @intCast(dirty.segments.items.len - 1);
            var first: u32 = 0;
            var cursor: usize = 0;
            for (handle.seg.info.row_groups, 0..) |rg, rg_idx| {
                const end = first + rg.row_count;
                const lo = cursor;
                while (cursor < tombs.rows.len and tombs.rows[cursor] < end) cursor += 1;
                if (cursor == lo) {
                    foldUnitStats(folds, specs, roles, cols, rg.stats, rg.row_count);
                } else {
                    try dirty.row_groups.append(allocator, .{
                        .segment = seg_slot,
                        .rg_idx = @intCast(rg_idx),
                        .first = first,
                        .row_count = rg.row_count,
                        .dead_lo = @intCast(lo),
                        .dead_hi = @intCast(cursor),
                    });
                }
                first = end;
            }
        }

        var blocks_read: usize = 0;
        if (needs_stats) {
            const budget = @max(READ_BUDGET_MIN_ROWS, total / READ_BUDGET_TABLE_FRACTION);
            var read_rows: u64 = snap.memtable_row_count;
            const any_full = std.mem.indexOfScalar(Role, roles, .full) != null;
            if (any_full) {
                for (dirty.row_groups.items) |rg| read_rows += rg.row_count;
            }
            if (read_rows > budget) return null;

            const mem_rows: usize = @intCast(snap.memtable_row_count);
            if (mem_rows > 0) {
                for (specs, roles, folds) |sp, role, *f| {
                    if (role == .rows) continue;
                    f.addLive(sp.kind, snap.memtable_snap.columns[sp.col_idx].view(), mem_rows, 0, &.{});
                }
            }

            if (any_full) {
                for (dirty.row_groups.items) |rg| {
                    const seg = dirty.segments.items[rg.segment].handle.seg;
                    for (specs, roles, 0..) |sp, role, i| {
                        if (role != .full or fullSpecOn(specs[0..i], roles[0..i], sp.col_idx)) continue;
                        var block: storage.ReadSegment.BorrowedBlock = undefined;
                        const view = try readRowGroupColumn(allocator, table, seg, rg.rg_idx, sp.col_idx, &block);
                        defer block.release(allocator, table.cacheRef());
                        blocks_read += 1;
                        for (specs[i..], roles[i..], folds[i..]) |other, other_role, *f| {
                            if (other_role != .full or other.col_idx != sp.col_idx) continue;
                            f.addLive(other.kind, view, rg.row_count, rg.first, dirty.dead(rg));
                        }
                    }
                }
            }

            var order: std.ArrayListUnmanaged(Bound) = .empty;
            defer order.deinit(allocator);
            for (specs, roles, folds) |sp, role, *f| {
                if (role != .bounded) continue;
                order.clearRetainingCapacity();
                for (dirty.row_groups.items, 0..) |rg, d| {
                    const s = dirty.stats(rg, sp.col_idx);
                    if (s.min > s.max) continue; // all-null row group (inverted sentinel)
                    try order.append(allocator, .{
                        .value = if (sp.kind == .min) s.min else s.max,
                        .row_group = @intCast(d),
                    });
                }
                std.sort.pdq(Bound, order.items, sp.kind == .min, Bound.before);
                for (order.items) |b| {
                    if (!f.couldImprove(sp.kind, b.value)) break;
                    const rg = dirty.row_groups.items[b.row_group];
                    read_rows += rg.row_count;
                    if (read_rows > budget) return null;
                    var block: storage.ReadSegment.BorrowedBlock = undefined;
                    const view = try readRowGroupColumn(allocator, table, dirty.segments.items[rg.segment].handle.seg, rg.rg_idx, sp.col_idx, &block);
                    defer block.release(allocator, table.cacheRef());
                    blocks_read += 1;
                    f.addLive(sp.kind, view, rg.row_count, rg.first, dirty.dead(rg));
                }
            }
        }

        const out_schema = try allocator.alloc(Column, specs.len);
        errdefer allocator.free(out_schema);
        const out_cols = try allocator.alloc(ColumnStore, specs.len);
        errdefer allocator.free(out_cols);
        var inited: usize = 0;
        errdefer for (out_cols[0..inited]) |*c| c.deinit(allocator);

        for (specs, roles, folds, 0..) |sp, role, f, i| {
            switch (sp.kind) {
                .count_star, .count_col => {
                    const cnt: u64 = if (role == .rows) total else f.non_null;
                    out_schema[i] = .{ .name = sp.out_name, .type = .bigint, .nullable = false };
                    out_cols[i] = try ColumnStore.init(allocator, .bigint, false);
                    inited += 1;
                    try out_cols[i].data.bigint.append(allocator, @intCast(cnt));
                },
                .sum, .avg => {
                    const col = cols[sp.col_idx];
                    const is_float = col.type == .float or col.type == .double;
                    // SQL: SUM/AVG over zero non-null inputs finalize to NULL.
                    if (sp.kind == .avg) {
                        out_schema[i] = .{ .name = sp.out_name, .type = .double, .nullable = true };
                        out_cols[i] = try ColumnStore.init(allocator, .double, true);
                        inited += 1;
                        if (f.non_null == 0) {
                            try cell_io.appendNullTo(allocator, &out_cols[i]);
                        } else {
                            const dividend: f64 = if (is_float) f.fsum else @floatFromInt(f.isum);
                            try out_cols[i].data.double.append(allocator, dividend / @as(f64, @floatFromInt(f.non_null)));
                            try out_cols[i].appendValidBit(allocator, 0, true);
                        }
                    } else if (is_float) {
                        out_schema[i] = .{ .name = sp.out_name, .type = .double, .nullable = true };
                        out_cols[i] = try ColumnStore.init(allocator, .double, true);
                        inited += 1;
                        if (f.non_null == 0) {
                            try cell_io.appendNullTo(allocator, &out_cols[i]);
                        } else {
                            try out_cols[i].data.double.append(allocator, f.fsum);
                            try out_cols[i].appendValidBit(allocator, 0, true);
                        }
                    } else {
                        // DESIGN.md §3.4: an integer SUM wraps to BIGINT (aggOutputType).
                        out_schema[i] = .{ .name = sp.out_name, .type = .bigint, .nullable = true };
                        out_cols[i] = try ColumnStore.init(allocator, .bigint, true);
                        inited += 1;
                        if (f.non_null == 0) {
                            try cell_io.appendNullTo(allocator, &out_cols[i]);
                        } else {
                            try out_cols[i].data.bigint.append(allocator, @truncate(f.isum));
                            try out_cols[i].appendValidBit(allocator, 0, true);
                        }
                    }
                },
                .min, .max => {
                    const col = cols[sp.col_idx];
                    out_schema[i] = .{ .name = sp.out_name, .type = col.type, .nullable = true };
                    out_cols[i] = try ColumnStore.init(allocator, col.type, true);
                    inited += 1;
                    if (f.seen) {
                        try appendStatValue(allocator, &out_cols[i], col.type, f.extreme);
                        try out_cols[i].appendValidBit(allocator, 0, true);
                    } else {
                        // No non-null value anywhere → MIN/MAX is NULL.
                        try cell_io.appendNullTo(allocator, &out_cols[i]);
                    }
                },
            }
        }

        const views = try allocator.alloc(ColumnView, specs.len);
        errdefer allocator.free(views);

        const self = try allocator.create(MetaAggStats);
        self.* = .{
            .allocator = allocator,
            .output_schema = out_schema,
            .output_columns = out_cols,
            .views = views,
            .blocks_read = blocks_read,
        };
        return exec.makeQuery(allocator, self);
    }

    pub fn next(self: *MetaAggStats) !?Batch {
        if (self.emitted) return null;
        self.emitted = true;
        for (self.output_columns, 0..) |c, i| self.views[i] = c.view();
        return Batch{ .schema = self.output_schema, .values = self.views, .row_count = 1 };
    }

    pub fn deinit(self: *MetaAggStats) void {
        for (self.output_columns) |*c| c.deinit(self.allocator);
        self.allocator.free(self.output_columns);
        self.allocator.free(self.output_schema);
        self.allocator.free(self.views);
        self.allocator.destroy(self);
    }

    pub fn outputSchema(self: *MetaAggStats) []const Column {
        return self.output_schema;
    }

    pub fn addPrune(_: *MetaAggStats, _: predicate.Predicate) !void {}

    pub fn stats(_: *MetaAggStats) exec.PipelineStats {
        return .{ .upper_rows = 1 };
    }

    pub fn accountant(_: *MetaAggStats) ?*exec.memory.MemoryAccountant {
        return null;
    }

    pub fn explain(self: *MetaAggStats, out: *std.ArrayList(u8), allocator: Allocator, depth: usize) !void {
        try exec.explainIndent(out, allocator, depth);
        try out.print(allocator, "MetaAggStats (metadata COUNT/MIN/MAX/SUM/AVG, {d} row-group blocks decoded)\n", .{self.blocks_read});
    }
};

/// How a `MetaAggStats` spec reads the table.
const Role = enum {
    /// Answered by the live row count alone.
    rows,
    /// Folds every unit's stats or rows; decodes each row group with
    /// tombstones.
    full,
    /// MIN/MAX: decodes a row group with tombstones only while its stored
    /// bound can beat the running extreme.
    bounded,
};

/// True when a spec in `specs` (with `roles`) already decodes `col_idx` as
/// `.full`.
fn fullSpecOn(specs: []const MetaSpec, roles: []const Role, col_idx: usize) bool {
    for (specs, roles) |sp, role| {
        if (role == .full and sp.col_idx == col_idx) return true;
    }
    return false;
}

/// One spec's running state over the units `MetaAggStats` folds.
const Fold = struct {
    non_null: u64 = 0,
    isum: i128 = 0,
    fsum: f64 = 0,
    extreme: i128,
    seen: bool = false,

    fn init(kind: MetaKind) Fold {
        return .{ .extreme = if (kind == .min) std.math.maxInt(i128) else std.math.minInt(i128) };
    }

    /// Fold the stats of a unit (segment or row group) of `rows` rows, none
    /// deleted.
    fn addStats(self: *Fold, kind: MetaKind, is_float: bool, s: storage.format.Stats, rows: u64) void {
        self.non_null += rows - s.null_count;
        switch (kind) {
            .sum, .avg => if (is_float) {
                self.fsum += storage.format.f64FromSumSlot(s.sum);
            } else {
                self.isum += s.sum;
            },
            // An all-null unit stores the inverted `min > max` sentinel.
            .min => if (s.min <= s.max) self.addExtreme(kind, s.min),
            .max => if (s.min <= s.max) self.addExtreme(kind, s.max),
            .count_star, .count_col => {},
        }
    }

    fn addExtreme(self: *Fold, kind: MetaKind, v: i128) void {
        self.seen = true;
        if (kind == .min) {
            if (v < self.extreme) self.extreme = v;
        } else {
            if (v > self.extreme) self.extreme = v;
        }
    }

    /// Whether a row group whose non-null values reach `bound` (its min for
    /// MIN, max for MAX) could still move the extreme.
    fn couldImprove(self: Fold, kind: MetaKind, bound: i128) bool {
        if (!self.seen) return true;
        return if (kind == .min) bound < self.extreme else bound > self.extreme;
    }

    /// Fold rows `[0, rows)` of `view`, skipping NULLs and each row whose
    /// segment offset (`first` + row) is in the sorted `dead`.
    fn addLive(self: *Fold, kind: MetaKind, view: ColumnView, rows: usize, first: u32, dead: []const u32) void {
        switch (kind) {
            .count_star => {},
            .count_col => {
                var d: usize = 0;
                for (0..rows) |row| {
                    if (d < dead.len and dead[d] - first == row) {
                        d += 1;
                        continue;
                    }
                    if (view.isValid(row)) self.non_null += 1;
                }
            },
            inline .sum, .avg, .min, .max => |k| switch (view.data) {
                inline .int, .date, .bigint, .datetime, .decimal64, .boolean, .tinyint, .smallint, .largeint, .decimal128, .float, .double => |values| self.addLiveValues(k, values[0..rows], view, first, dead),
                // `exactStatsType` and `sumStatsType` admit no other type.
                else => unreachable,
            },
        }
    }

    fn addLiveValues(self: *Fold, comptime kind: MetaKind, values: anytype, view: ColumnView, first: u32, dead: []const u32) void {
        const is_float = @typeInfo(@TypeOf(values[0])) == .float;
        var d: usize = 0;
        for (values, 0..) |v, row| {
            if (d < dead.len and dead[d] - first == row) {
                d += 1;
                continue;
            }
            if (!view.isValid(row)) continue;
            self.non_null += 1;
            switch (kind) {
                .sum, .avg => if (is_float) {
                    self.fsum += v;
                } else {
                    self.isum += v;
                },
                // `exactStatsType` admits no float column to MIN/MAX.
                .min, .max => if (is_float) unreachable else self.addExtreme(kind, v),
                .count_star, .count_col => comptime unreachable,
            }
        }
    }
};

/// Fold a unit's stats (one `Stats` per table column) covering `rows` live
/// rows into every stats-dependent spec.
fn foldUnitStats(folds: []Fold, specs: []const MetaSpec, roles: []const Role, cols: []const Column, stats: []const storage.format.Stats, rows: u64) void {
    for (specs, roles, folds) |sp, role, *f| {
        if (role == .rows) continue;
        const t = cols[sp.col_idx].type;
        f.addStats(sp.kind, t == .float or t == .double, stats[sp.col_idx], rows);
    }
}

/// The row groups with tombstones a `MetaAggStats` read decodes, with the
/// segment handles and tombstone lists they read from, pinned until
/// `deinit`.
const DirtyRowGroups = struct {
    segments: std.ArrayListUnmanaged(Segment) = .empty,
    row_groups: std.ArrayListUnmanaged(RowGroup) = .empty,

    const Segment = struct {
        handle: *storage.cache.SegmentHandles.Entry,
        tombs: *storage.cache.SegmentHandles.Tombstones,
    };

    const RowGroup = struct {
        segment: u32,
        rg_idx: u32,
        /// Segment offset of the row group's first row.
        first: u32,
        row_count: u32,
        /// The row group's slice of its segment's sorted tombstone list.
        dead_lo: u32,
        dead_hi: u32,
    };

    fn deinit(self: *DirtyRowGroups, allocator: Allocator, table: *Table) void {
        for (self.segments.items) |s| {
            table.releaseSegmentTombstones(s.tombs);
            table.releaseSegment(s.handle);
        }
        self.segments.deinit(allocator);
        self.row_groups.deinit(allocator);
    }

    fn stats(self: DirtyRowGroups, rg: RowGroup, col_idx: usize) storage.format.Stats {
        return self.segments.items[rg.segment].handle.seg.info.row_groups[rg.rg_idx].stats[col_idx];
    }

    fn dead(self: DirtyRowGroups, rg: RowGroup) []const u32 {
        return self.segments.items[rg.segment].tombs.rows[rg.dead_lo..rg.dead_hi];
    }
};

/// A row group's MIN/MAX bound, for the best-first visit.
const Bound = struct {
    value: i128,
    row_group: u32,

    fn before(ascending: bool, a: Bound, b: Bound) bool {
        return if (ascending) a.value < b.value else a.value > b.value;
    }
};

/// Row group `rg_idx`'s block of column `col_idx` as a view, valid until
/// `block` is released.
fn readRowGroupColumn(allocator: Allocator, table: *Table, seg: storage.ReadSegment, rg_idx: usize, col_idx: usize, block: *storage.ReadSegment.BorrowedBlock) !ColumnView {
    block.* = try seg.borrowColumnBlock(allocator, rg_idx, col_idx, table.cacheRef());
    errdefer block.release(allocator, table.cacheRef());
    const col = table.schema.columns[col_idx];
    const rows = seg.info.row_groups[rg_idx].row_count;
    const flags = storage.format.ColumnBlockFlags{ .has_nulls = col.nullable };
    if (storage.segment_reader.viewRawColumn(col.type, block.bytes, rows, flags, block.encoding)) |view| return view;
    block.expanded = try storage.segment_reader.decodeColumnPayload(allocator, col.type, block.bytes, rows, flags, block.encoding);
    return block.expanded.?.view();
}

/// Types whose per-row-group `Stats` carry an exact min/max (not a prefix,
/// not absent). Mirrors the integer/date/decimal cases of `appendAccToColumn`.
fn exactStatsType(t: Type) bool {
    return switch (t) {
        .int, .bigint, .smallint, .tinyint, .boolean, .date, .datetime, .decimal64, .largeint, .decimal128 => true,
        .uuid, .varchar, .string, .char, .json, .nested, .float, .double => false,
    };
}

/// Types whose `Stats.sum` slot the SUM/AVG metadata lane consumes. Decimals
/// are excluded (scale handling lives in the scan path — keep one
/// implementation of it); largeint has no stored sum (i128 overflow).
fn sumStatsType(t: Type) bool {
    return switch (t) {
        .int, .bigint, .smallint, .tinyint, .boolean, .float, .double => true,
        else => false,
    };
}

/// Decode the i128-encoded stat into the column's typed value and append it.
/// Matches the MIN/MAX paths of `aggregate.appendAccToColumn`.
fn appendStatValue(allocator: Allocator, col: *ColumnStore, t: Type, v128: i128) !void {
    switch (t) {
        .largeint => try col.data.largeint.append(allocator, v128),
        .decimal128 => try col.data.decimal128.append(allocator, v128),
        else => {
            const v: i64 = @intCast(v128);
            switch (t) {
                .int => try col.data.int.append(allocator, @intCast(v)),
                .bigint => try col.data.bigint.append(allocator, v),
                .boolean => try col.data.boolean.append(allocator, @intCast(v)),
                .date => try col.data.date.append(allocator, @intCast(v)),
                .datetime => try col.data.datetime.append(allocator, v),
                .tinyint => try col.data.tinyint.append(allocator, @intCast(v)),
                .smallint => try col.data.smallint.append(allocator, @intCast(v)),
                .decimal64 => try col.data.decimal64.append(allocator, v),
                else => unreachable,
            }
        },
    }
}
