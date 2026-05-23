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
//!     path emits the documented 0/empty default).

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
            const tombs = try storage.tombstone.read(allocator, table.io, table.segments_dir, entry.segment_id);
            if (tombs) |t| {
                allocator.free(t);
                return null; // a deleted row may have been the extreme
            }
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
            // No non-null value anywhere → MIN/MAX is NULL; thindb emits the
            // type's 0 default, matching the normal scan path.
            const folded: i128 = if (seen) acc else 0;
            out_schema[i] = .{ .name = sp.out_name, .type = col.type, .nullable = false };
            out_cols[i] = try ColumnStore.init(allocator, col.type, false);
            inited += 1;
            try appendStatValue(allocator, &out_cols[i], col.type, folded);
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

/// Types whose per-row-group `Stats` carry an exact min/max (not a prefix,
/// not absent). Mirrors the integer/date/decimal cases of `appendAccToColumn`.
fn exactStatsType(t: Type) bool {
    return switch (t) {
        .int, .bigint, .smallint, .tinyint, .boolean, .date, .datetime, .decimal64, .largeint, .decimal128 => true,
        .uuid, .varchar, .string, .char, .float, .double => false,
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
