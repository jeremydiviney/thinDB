//! Scan operator — reads segments (in manifest order), then the memtable.
//! Emits one Batch per row group (with tombstones applied) plus one final
//! Batch for the memtable.

const std = @import("std");
const Allocator = std.mem.Allocator;
const Io = std.Io;

const types = @import("../types.zig");
const Column = types.Column;
const Value = types.Value;

const storage = @import("../storage/storage.zig");
const ColumnView = storage.ColumnView;

const engine = @import("../engine/engine.zig");
const ColumnStore = engine.ColumnStore;

const api = @import("../api/api.zig");
const Table = api.Table;

const exec = @import("exec.zig");
const Query = exec.Query;
const Batch = exec.Batch;
const Error = exec.Error;
const makeQuery = exec.makeQuery;

const predicate = @import("predicate.zig");
const Predicate = predicate.Predicate;
const PredicateOp = predicate.PredicateOp;
const statsOverlapPredicate = predicate.statsOverlapPredicate;

pub const Scan = struct {
    allocator: Allocator,
    io: Io,
    table: *Table,

    segment_count: usize,

    phase: Phase = .segments,
    cur_seg_idx: usize = 0,
    cur_rg_idx: usize = 0,
    cur_segment: ?storage.ReadSegment = null,
    /// Sorted, deduped tombstone offsets for the current segment (or null).
    cur_segment_tomb: ?[]u32 = null,
    /// Prefix sum: `cur_rg_first_row[k]` is the first row offset of row group k
    /// within the current segment.
    cur_rg_first_row: []u32 = &.{},

    decoded: []storage.OwnedColumn,
    decoded_valid: bool = false,
    views: []ColumnView,

    /// Lazily allocated when a row group has rows tombstoned and we need to
    /// materialize a filtered batch. Reused across batches.
    filtered: ?[]ColumnStore = null,

    /// Pushed-down predicates used to skip row groups via min/max stats.
    prunes: std.ArrayList(PruneHint),

    const Phase = enum { segments, memtable, done };

    pub const PruneHint = struct {
        col_idx: usize,
        op: PredicateOp,
        val: Value,
    };

    pub fn create(allocator: Allocator, table: *Table) !Query {
        const n = table.schema.columns.len;

        const decoded = try allocator.alloc(storage.OwnedColumn, n);
        errdefer allocator.free(decoded);
        const views = try allocator.alloc(ColumnView, n);
        errdefer allocator.free(views);

        const self = try allocator.create(Scan);
        errdefer allocator.destroy(self);

        self.* = .{
            .allocator = allocator,
            .io = table.io,
            .table = table,
            .segment_count = table.manifest.segments.items.len,
            .decoded = decoded,
            .views = views,
            .prunes = .empty,
        };

        return makeQuery(allocator, self);
    }

    pub fn deinit(self: *Scan) void {
        self.releaseBatch();
        self.closeCurSegment();
        self.prunes.deinit(self.allocator);
        if (self.filtered) |arr| {
            for (arr) |*c| c.deinit(self.allocator);
            self.allocator.free(arr);
        }
        self.allocator.free(self.decoded);
        self.allocator.free(self.views);
        const allocator = self.allocator;
        allocator.destroy(self);
    }

    fn closeCurSegment(self: *Scan) void {
        if (self.cur_segment) |*seg| {
            seg.deinit();
            self.cur_segment = null;
        }
        if (self.cur_segment_tomb) |t| {
            self.allocator.free(t);
            self.cur_segment_tomb = null;
        }
        if (self.cur_rg_first_row.len > 0) {
            self.allocator.free(self.cur_rg_first_row);
            self.cur_rg_first_row = &.{};
        }
    }

    fn ensureFilteredBuffers(self: *Scan) ![]ColumnStore {
        if (self.filtered) |arr| return arr;
        const arr = try self.allocator.alloc(ColumnStore, self.table.schema.columns.len);
        errdefer self.allocator.free(arr);
        var inited: usize = 0;
        errdefer for (arr[0..inited]) |*c| c.deinit(self.allocator);
        for (self.table.schema.columns, 0..) |col, i| {
            arr[i] = try ColumnStore.init(self.allocator, col.type, col.nullable);
            inited += 1;
        }
        self.filtered = arr;
        return arr;
    }

    pub fn addPrune(self: *Scan, pred: Predicate) !void {
        const col_idx = blk: {
            for (self.table.schema.columns, 0..) |c, i| {
                if (std.mem.eql(u8, c.name, pred.col)) break :blk i;
            }
            return Error.ColumnNotFound;
        };
        // Strings have no stats — skip silently.
        if (self.table.schema.columns[col_idx].type.isString()) return;

        try self.prunes.append(self.allocator, .{
            .col_idx = col_idx,
            .op = pred.op,
            .val = pred.val,
        });
    }

    fn rowGroupCanMatch(self: Scan, rg: storage.RowGroupMeta) bool {
        for (self.prunes.items) |hint| {
            const stats = rg.stats[hint.col_idx];
            if (!statsOverlapPredicate(stats, hint.op, hint.val)) return false;
        }
        return true;
    }

    pub fn outputSchema(self: *Scan) []const Column {
        return self.table.schema.columns;
    }

    pub fn next(self: *Scan) !?Batch {
        self.releaseBatch();

        // Segments phase
        while (self.phase == .segments) {
            if (self.cur_segment == null) {
                if (self.cur_seg_idx >= self.segment_count) {
                    self.phase = .memtable;
                    break;
                }
                const entry = self.table.manifest.segments.items[self.cur_seg_idx];
                var name_buf: [32]u8 = undefined;
                const file_name = try Table.segmentFileName(&name_buf, entry.segment_id);
                self.cur_segment = try storage.readSegment(
                    self.allocator,
                    self.io,
                    self.table.segments_dir,
                    file_name,
                    self.table.schema,
                );

                self.cur_segment_tomb = try storage.tombstone.read(
                    self.allocator,
                    self.io,
                    self.table.segments_dir,
                    entry.segment_id,
                );

                const rgs = self.cur_segment.?.info.row_groups;
                self.cur_rg_first_row = try self.allocator.alloc(u32, rgs.len);
                var running: u32 = 0;
                for (rgs, 0..) |rg, i| {
                    self.cur_rg_first_row[i] = running;
                    running += rg.row_count;
                }
                self.cur_rg_idx = 0;
            }

            const seg = &self.cur_segment.?;
            if (self.cur_rg_idx >= seg.info.row_groups.len) {
                self.closeCurSegment();
                self.cur_seg_idx += 1;
                continue;
            }

            const rg = seg.info.row_groups[self.cur_rg_idx];
            if (!self.rowGroupCanMatch(rg)) {
                self.cur_rg_idx += 1;
                continue;
            }

            for (self.table.schema.columns, 0..) |_, i| {
                self.decoded[i] = try seg.decodeColumn(
                    self.allocator,
                    self.table.schema,
                    self.cur_rg_idx,
                    i,
                );
            }
            self.decoded_valid = true;

            const rg_first = self.cur_rg_first_row[self.cur_rg_idx];
            const rg_count = rg.row_count;
            self.cur_rg_idx += 1;

            // Apply tombstones if any fall within this row group.
            const masked = try self.applyTombsIfAny(rg_first, rg_count);
            if (masked) |out| return out;

            for (self.decoded, 0..) |c, i| self.views[i] = c.view();
            return Batch{
                .schema = self.table.schema.columns,
                .values = self.views,
                .row_count = rg_count,
            };
        }

        // Memtable phase
        if (self.phase == .memtable) {
            self.phase = .done;
            if (self.table.memtable.row_count == 0) return null;

            for (self.table.memtable.columns, 0..) |c, i| {
                self.views[i] = c.view();
            }
            return Batch{
                .schema = self.table.schema.columns,
                .values = self.views,
                .row_count = @intCast(self.table.memtable.row_count),
            };
        }

        return null;
    }

    fn releaseBatch(self: *Scan) void {
        if (self.decoded_valid) {
            for (self.decoded) |*c| c.deinit(self.allocator);
            self.decoded_valid = false;
        }
    }

    /// If any tombstone offsets fall within `[rg_first, rg_first + rg_count)`,
    /// materialize a filtered batch into `filtered` and return it. Otherwise
    /// returns null so the caller emits the unfiltered batch.
    fn applyTombsIfAny(self: *Scan, rg_first: u32, rg_count: u32) !?Batch {
        const tombs = self.cur_segment_tomb orelse return null;
        if (tombs.len == 0) return null;

        const rg_end = rg_first + rg_count;
        const lo = std.sort.lowerBound(u32, tombs, rg_first, cmpU32);
        const hi = std.sort.lowerBound(u32, tombs, rg_end, cmpU32);
        if (lo == hi) return null;

        const tomb_slice = tombs[lo..hi];
        const filtered_cols = try self.ensureFilteredBuffers();
        for (filtered_cols) |*c| c.clear();

        const mask = try self.allocator.alloc(bool, rg_count);
        defer self.allocator.free(mask);
        @memset(mask, true);
        for (tomb_slice) |off| {
            mask[off - rg_first] = false;
        }

        var kept: usize = 0;
        for (mask) |m| if (m) {
            kept += 1;
        };

        for (self.decoded, filtered_cols) |src, *dst| {
            try engine.memtable.appendMaskedColumn(self.allocator, src.view(), mask, dst);
        }
        for (filtered_cols, 0..) |c, i| self.views[i] = c.view();

        return Batch{
            .schema = self.table.schema.columns,
            .values = self.views,
            .row_count = kept,
        };
    }
};

fn cmpU32(target: u32, item: u32) std.math.Order {
    return std.math.order(target, item);
}
