//! Bulk load: a Parquet file into a table as final-size segments, written
//! straight from the file, past the memtable and the WAL.
//!
//! Consecutive row groups make one output segment of about `segment_rows`
//! rows; by default, as many as memory allows and no more than the
//! background compactor leaves unmerged (`segmentRowsFor`), so a load
//! leaves it nothing to redo. One pool of workers does every step, each
//! taking the next task in priority order:
//! - decode a row group's order-key columns, claimed in file order, and sort
//!   its rows by the key (a share of the workers first, so reading never
//!   stalls behind writing);
//! - per segment, oldest first: start a column's string table once the
//!   column's row groups have all committed, or encode a few of a started
//!   table's pieces (pages and blocks); else gather and encode one open
//!   column of one output row group; else decode one open column of one row
//!   group; else open the segment's next column;
//! - decode keys, with any worker left.
//! The worker that decodes a segment's last keys k-way merges its row groups
//! into output order, publishing each output row group as it's picked.
//! Columns open in schema order, a few at once: each decodes from every row
//! group, reading its chunks again through the OS file cache, gathers into
//! the output row groups, and is freed. Per column, row groups commit to the
//! string table in order. The file is laid out column by column: each
//! column's blocks append in row group order as they encode, and its string
//! table once built, so the segment reaches the disk while the load runs
//! instead of in one write and sync at its end. A worker that finishes a
//! step whose predecessors are done carries on with its successors, so no
//! worker waits on another's step.
//!
//! At most `lanes` segments hold decoded keys at once, so memory is about
//! `lanes × segment_rows` rows of keys, plus the open columns, their
//! string tables, and blocks waiting on the ones before them in the file.
//! Every segment publishes in one manifest swap at the end: a failed load
//! leaves the table as it was.
//!
//! Each table column reads the Parquet leaf of the same name, matched
//! case-insensitively, and the file's other leaves are skipped; or, with
//! `.match = .position`, the leaf at its position. The table's order key
//! must not be unique, since unique keys resolve upserts row by row through
//! the memtable.

const std = @import("std");
const Io = std.Io;
const Allocator = std.mem.Allocator;

const storage = @import("../storage/storage.zig");
const engine = @import("../engine/engine.zig");
const hll = @import("../util/hll.zig");
const core_scheduler = @import("../util/core_scheduler.zig");
const types = @import("../types.zig");
const api = @import("api.zig");
const compact = @import("compact.zig");
const Table = api.Table;
const parquet = engine.parquet;
const segment_writer = storage.segment_writer;
const segment_strings = storage.segment_strings;

pub const Options = struct {
    /// Rows per written segment, before rounding to whole row groups; null
    /// sizes segments from the machine's memory (`segmentRowsFor`).
    segment_rows: ?usize = null,
    /// Segments holding decoded keys at once.
    lanes: usize = 2,
    /// Workers at most; 0 means one per hardware thread the process may
    /// use. Concurrent statements share the machine's threads: a load runs
    /// on its share, which it re-checks as statements come and go.
    threads: usize = 0,
    /// How each table column finds its Parquet leaf: the leaf of its name,
    /// matched without case, or the leaf at its position, as SQL's
    /// INSERT ... SELECT * pairs them. By position, the file holds exactly
    /// the table's columns.
    match: enum { name, position } = .name,
};

pub const Stats = struct {
    rows: u64,
    segments: usize,
};

pub fn loadParquet(t: *Table, dir: Io.Dir, sub_path: []const u8, options: Options) !Stats {
    if (t.schema.unique) return error.BulkLoadUniqueTable;
    const allocator = t.allocator;
    const io = t.io;

    var file = try dir.openFile(io, sub_path, .{});
    defer file.close(io);
    var meta = try parquet.readMetadata(allocator, io, file);
    defer meta.deinit();

    const leaves = try allocator.alloc(usize, t.schema.columns.len);
    defer allocator.free(leaves);
    switch (options.match) {
        .name => for (t.schema.columns, leaves) |c, *leaf| {
            leaf.* = meta.columnIndex(c.name) orelse return error.ParquetColumnMissing;
        },
        .position => {
            if (meta.columns.len != leaves.len) return error.ParquetColumnMissing;
            for (leaves, 0..) |*leaf, i| leaf.* = i;
        },
    }

    const workers = if (options.threads == 0) api.hardwareThreadCount() else options.threads;
    var grant = core_scheduler.reserveWorkers(workers);
    defer grant.release();
    const lanes = @max(options.lanes, 1);
    const open_columns = @max(2, workers / 2);
    const segment_rows = options.segment_rows orelse rows: {
        var cost = try sampleRowCost(t, dir, sub_path, &meta, leaves, grant.count);
        defer cost.deinit(allocator);
        break :rows segmentRowsFor(cost, t.order_key_indices, t.compact_memory, t.memory_pool.statementLimit(), lanes, open_columns);
    };
    var plan = try Plan.init(allocator, meta.row_groups, @max(segment_rows, 1));
    defer plan.deinit(allocator);
    if (plan.segment_count == 0) return .{ .rows = 0, .segments = 0 };

    const statement_lease = try t.acquireStatement();
    defer if (statement_lease) |lease| lease.release();
    t.lockCompactPreempting();
    defer t.compact_lock.unlock(io);

    const segments = try allocator.alloc(Segment, plan.segment_count);
    defer allocator.free(segments);
    var inited: usize = 0;
    defer for (segments[0..inited]) |*s| s.deinit(allocator);
    for (segments, 0..) |*s, i| {
        s.* = try Segment.init(allocator, plan.first[i + 1] - plan.first[i], t.schema.columns.len);
        inited += 1;
    }

    var load: Load = .{
        .t = t,
        .dir = dir,
        .sub_path = sub_path,
        .meta = &meta,
        .leaves = leaves,
        .plan = &plan,
        .segments = segments,
        .lanes = lanes,
        .workers = workers,
        .grant = &grant,
        .decode_share = @max(1, workers / 4),
        .open_columns = open_columns,
    };
    load.run();
    if (load.err) |e| {
        for (segments) |s| if (s.info) |info| t.deleteSegmentFiles(info.segment_id) catch {};
        return e;
    }

    var rows: u64 = 0;
    for (segments) |s| rows += s.info.?.row_count;
    // A failed publish keeps the segments: its manifest may have reached the
    // disk. Ones it doesn't name go in the next open's orphan sweep.
    try publish(t, segments);
    return .{ .rows = rows, .segments = segments.len };
}

/// Which row groups each segment takes: consecutive ones, cut so the
/// segments hold about equal rows.
const Plan = struct {
    /// Row groups that hold rows, in file order.
    units: []usize,
    /// Segment `s` takes `units[first[s]..first[s + 1]]`, for `s` below
    /// `segment_count`, which big row groups can leave under the target.
    first: []usize,
    segment_count: usize,
    /// The segment each unit belongs to.
    segment_of: []usize,

    fn init(allocator: Allocator, row_groups: []const parquet.RowGroup, segment_rows: usize) !Plan {
        var total: u64 = 0;
        var unit_count: usize = 0;
        for (row_groups) |rg| if (rg.rows > 0) {
            total += rg.rows;
            unit_count += 1;
        };
        const target: usize = @intCast((total + segment_rows - 1) / segment_rows);

        const units = try allocator.alloc(usize, unit_count);
        errdefer allocator.free(units);
        const segment_of = try allocator.alloc(usize, unit_count);
        errdefer allocator.free(segment_of);
        const first = try allocator.alloc(usize, target + 1);
        first[0] = 0;
        var segment: usize = 0;
        var rows: u64 = 0;
        var i: usize = 0;
        for (row_groups, 0..) |rg, rg_index| {
            if (rg.rows == 0) continue;
            units[i] = rg_index;
            segment_of[i] = segment;
            rows += rg.rows;
            i += 1;
            if (i < unit_count and segment + 1 < target and rows >= total * (segment + 1) / target) {
                segment += 1;
                first[segment] = i;
            }
        }
        const segment_count = if (unit_count == 0) 0 else segment + 1;
        first[segment_count] = unit_count;
        return .{ .units = units, .first = first, .segment_count = segment_count, .segment_of = segment_of };
    }

    fn deinit(self: *Plan, allocator: Allocator) void {
        allocator.free(self.units);
        allocator.free(self.first);
        allocator.free(self.segment_of);
    }
};

/// Row groups sampled, spread over the file, to size segments.
const sample_row_groups = 8;

/// Per column, bytes per row in sampled row groups: what its decoded rows
/// hold, and what a merge holds for its string table, counting each row
/// group's distinct values as though no other held them, as a merge counts
/// its inputs'.
const RowCost = struct {
    decoded: []f64,
    merge: []f64,

    fn deinit(self: *RowCost, allocator: Allocator) void {
        allocator.free(self.decoded);
        allocator.free(self.merge);
    }
};

/// The load's resident peak over what `segmentRowsFor` counts it holds.
/// ClickBench's `hits` peaked at 1.35-1.43x: string tables outlive their
/// columns while their pages encode, and freed buffers stay resident.
const held_peak_ratio: f64 = 1.5;

/// Rows per segment by the tighter of two limits, both from the machine's
/// memory. Half as many as one merge may build within `compact_memory`: the
/// background compactor merges four segments of a size at once, so four
/// loaded ones would need twice its memory, and the estimate may be off by
/// as much either way before a merge takes them or a segment outgrows what
/// a merge builds. And as many as the load may hold within
/// `statement_limit`: each lane's keys and output order, and its open
/// columns, decoded and gathered, with their string tables. A table without
/// string tables meets only the second.
fn segmentRowsFor(cost: RowCost, key_columns: []const usize, compact_memory: u64, statement_limit: u64, lanes: usize, open_columns: usize) u64 {
    var merge: f64 = 0;
    for (cost.merge) |m| merge += m;
    var keys: f64 = @sizeOf(Pick) + @sizeOf(u32);
    for (key_columns) |ci| keys += cost.decoded[ci];
    var open: f64 = 0;
    var widest: f64 = 0;
    for (cost.decoded, cost.merge, 0..) |decoded, m, ci| {
        open += 2 * decoded + m;
        if (ci >= open_columns) open -= 2 * cost.decoded[ci - open_columns] + cost.merge[ci - open_columns];
        widest = @max(widest, open);
    }
    const by_memory = @as(f64, @floatFromInt(statement_limit)) / (held_peak_ratio * (@as(f64, @floatFromInt(lanes)) * keys + widest));
    const by_merge = if (merge == 0) by_memory else @as(f64, @floatFromInt(compact.mergeStringCapacity(compact_memory))) / (2 * merge);
    const most: f64 = @floatFromInt(std.math.maxInt(u63));
    return @intFromFloat(@min(by_memory, by_merge, most));
}

/// Decodes every column of up to `sample_row_groups` row groups, spread over
/// the file, on `threads` threads.
fn sampleRowCost(t: *Table, dir: Io.Dir, sub_path: []const u8, meta: *const parquet.Metadata, leaves: []const usize, threads: usize) !RowCost {
    const allocator = t.allocator;
    var with_rows: usize = 0;
    for (meta.row_groups) |rg| with_rows += @intFromBool(rg.rows > 0);
    const units = try allocator.alloc(usize, @min(with_rows, sample_row_groups));
    defer allocator.free(units);
    var i: usize = 0;
    var seen: usize = 0;
    for (meta.row_groups, 0..) |rg, index| {
        if (rg.rows == 0) continue;
        if (i < units.len and seen == (2 * i + 1) * with_rows / (2 * units.len)) {
            units[i] = index;
            i += 1;
        }
        seen += 1;
    }

    const columns = t.schema.columns.len;
    var sampler: Sampler = .{
        .t = t,
        .dir = dir,
        .sub_path = sub_path,
        .meta = meta,
        .leaves = leaves,
        .units = units,
        .decoded = try allocator.alloc(u64, columns),
        .merge = try allocator.alloc(u64, columns),
    };
    defer allocator.free(sampler.decoded);
    defer allocator.free(sampler.merge);
    @memset(sampler.decoded, 0);
    @memset(sampler.merge, 0);
    sampler.run(@min(threads, units.len * columns));
    if (sampler.err) |e| return e;

    var rows: u64 = 0;
    for (units) |u| rows += meta.row_groups[u].rows;
    const decoded = try allocator.alloc(f64, columns);
    errdefer allocator.free(decoded);
    const merge = try allocator.alloc(f64, columns);
    for (decoded, merge, sampler.decoded, sampler.merge) |*d, *m, sd, sm| {
        d.* = @as(f64, @floatFromInt(sd)) / @as(f64, @floatFromInt(@max(rows, 1)));
        m.* = @as(f64, @floatFromInt(sm)) / @as(f64, @floatFromInt(@max(rows, 1)));
    }
    return .{ .decoded = decoded, .merge = merge };
}

/// Measures sampled row groups' columns, a column of a row group per task.
const Sampler = struct {
    t: *Table,
    dir: Io.Dir,
    sub_path: []const u8,
    meta: *const parquet.Metadata,
    leaves: []const usize,
    units: []const usize,
    next: std.atomic.Value(usize) = .init(0),
    /// Guards the sums and `err`.
    mutex: std.atomic.Mutex = .unlocked,
    /// Per column, the bytes decoded and a merge's bytes, summed.
    decoded: []u64,
    merge: []u64,
    err: ?anyerror = null,

    fn run(self: *Sampler, threads: usize) void {
        const allocator = self.t.allocator;
        if (threads > 1) {
            if (allocator.alloc(std.Thread, threads - 1)) |handles| {
                defer allocator.free(handles);
                var spawned: usize = 0;
                for (handles) |*h| {
                    h.* = std.Thread.spawn(.{}, worker, .{self}) catch break;
                    spawned += 1;
                }
                self.worker();
                for (handles[0..spawned]) |h| h.join();
                return;
            } else |_| {}
        }
        self.worker();
    }

    fn worker(self: *Sampler) void {
        const t = self.t;
        const columns = t.schema.columns.len;
        var w = Worker.init(t.allocator, t.io, self.dir, self.sub_path, columns) catch |e| return self.fail(e);
        defer w.deinit();
        var distinct: std.StringHashMapUnmanaged(void) = .empty;
        defer distinct.deinit(t.allocator);
        while (true) {
            const task = self.next.fetchAdd(1, .monotonic);
            if (task >= self.units.len * columns) return;
            self.measure(&w, &distinct, self.units[task / columns], task % columns) catch |e| return self.fail(e);
        }
    }

    fn measure(self: *Sampler, w: *Worker, distinct: *std.StringHashMapUnmanaged(void), unit: usize, ci: usize) !void {
        const t = self.t;
        const rg = self.meta.row_groups[unit];
        const column = t.schema.columns[ci];
        var store = try decodeChunk(t, self.meta, w, rg, self.leaves[ci], column);
        defer store.deinit(t.allocator);
        var merge: u64 = 0;
        const view = store.view();
        if (segment_strings.eligible(column.type)) switch (view.data) {
            .varchar, .string, .char => |strings| {
                distinct.clearRetainingCapacity();
                var value_bytes: u64 = 0;
                for (0..view.rowCount()) |r| {
                    if (!view.isValid(r)) continue;
                    const value = strings.rowBytes(r);
                    const slot = try distinct.getOrPut(t.allocator, value);
                    if (!slot.found_existing) value_bytes += value.len;
                }
                merge = compact.stringTableMergeBytes(value_bytes, distinct.count(), rg.rows);
            },
            else => {},
        };
        while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
        defer self.mutex.unlock();
        self.decoded[ci] += store.liveBytes();
        self.merge[ci] += merge;
    }

    fn fail(self: *Sampler, e: anyerror) void {
        while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
        defer self.mutex.unlock();
        if (self.err == null) self.err = e;
        self.next.store(std.math.maxInt(usize) / 2, .monotonic);
    }
};

/// One row group: its columns as they decode, the order key's first, and
/// its rows in file order sorted by the key.
const Batch = struct {
    /// Null until decoded, and again once every output row group gathers it.
    columns: []?engine.ColumnStore,
    views: []storage.ColumnView,
    order: []u32,

    fn deinit(self: *Batch, allocator: Allocator) void {
        for (self.columns) |*c| if (c.*) |*store| store.deinit(allocator);
        allocator.free(self.columns);
        allocator.free(self.views);
        allocator.free(self.order);
    }
};

const Segment = struct {
    batches: []?Batch,
    decoded: usize = 0,
    /// Set once every column is gathered and the batches are freed.
    released: bool = false,
    /// Per-column HLL sketches of the rows decoded so far.
    sketches: []u8,
    sketch_mutex: std.atomic.Mutex = .unlocked,
    /// Set once the segment's last keys are decoded.
    write: ?*Write = null,
    info: ?storage.SegmentInfo = null,

    fn init(allocator: Allocator, units: usize, columns: usize) !Segment {
        const batches = try allocator.alloc(?Batch, units);
        errdefer allocator.free(batches);
        @memset(batches, null);
        const sketches = try allocator.alloc(u8, columns * hll.m);
        @memset(sketches, 0);
        return .{ .batches = batches, .sketches = sketches };
    }

    fn deinit(self: *Segment, allocator: Allocator) void {
        self.freeBatches(allocator);
        allocator.free(self.batches);
        allocator.free(self.sketches);
        if (self.write) |w| w.destroy(allocator);
        if (self.info) |*info| info.deinit(allocator);
    }

    fn freeBatches(self: *Segment, allocator: Allocator) void {
        for (self.batches) |*b| if (b.*) |*batch| {
            batch.deinit(allocator);
            b.* = null;
        };
    }

    /// Folds in `sketches`, of the columns from `first_column` on.
    fn addSketches(self: *Segment, first_column: usize, sketches: []const u8) void {
        while (!self.sketch_mutex.tryLock()) std.atomic.spinLoopHint();
        defer self.sketch_mutex.unlock();
        for (self.sketches[first_column * hll.m ..][0..sketches.len], sketches) |*a, b| a.* = @max(a.*, b);
    }

    /// Column `ci`'s distinct values by its sketch, padded past the sketch's
    /// few percent of error; whole once every batch of the column decodes.
    fn distinctEstimate(self: *const Segment, ci: usize) u32 {
        const estimate = hll.Hll.fromBytes(self.sketches[ci * hll.m ..][0..hll.m]).estimate();
        return std.math.cast(u32, estimate + estimate / 16) orelse std.math.maxInt(u32);
    }
};

/// A row of the merged output: batch `batch`'s row `row`.
const Pick = struct {
    batch: u32,
    row: u32,
};

/// A segment being written: its rows' merged order, and how far each step
/// has come. Guarded by `Load.mutex`, except where noted.
const Write = struct {
    segment: usize,
    writer: storage.MergedSegmentWriter,
    finished: bool = false,
    /// Every row in output order, row group `r` at `r * rg_rows`. The merge
    /// fills it outside the lock, publishing each row group in `picked`;
    /// freed with the batches.
    picks: []Pick,
    rows: usize,
    rg_rows: usize,
    rg_count: usize,
    picked: usize = 0,
    /// Columns open in schema order, a few at once: `opened` have started,
    /// `closed` of those are gathered and freed, every one before
    /// `first_open`.
    opened: usize = 0,
    closed: usize = 0,
    first_open: usize = 0,
    /// Per column: the next row group to decode and those decoded, then the
    /// next output row group to gather and those gathered.
    decode_next: []usize,
    decoded: []usize,
    gather_next: []usize,
    gathered: []usize,
    groups: []Group,
    /// The next block to append, column by column: row group `append_rg`
    /// of column `append_column`; and whether a worker is appending.
    append_column: usize = 0,
    append_rg: usize = 0,
    appending: bool = false,
    /// Per column: whether it has a string table, the next row group to
    /// commit to it, and whether a worker is committing.
    has_table: []bool,
    commit_next: []usize,
    committing: []bool,
    /// The columns with a string table.
    table_columns: []usize,
    /// Per column, its string table's step.
    tables: []TableStep,
    /// Per column, whether its blocks' table ids (DESIGN.md §4.7) are still
    /// to plan, which its gathers wait on.
    id_steps: []IdStep,

    const IdStep = enum { pending, planning, done };

    fn create(t: *Table, segment: usize, seg: *Segment) !*Write {
        const allocator = t.allocator;
        const columns = t.schema.columns.len;
        var rows: usize = 0;
        for (seg.batches) |b| rows += b.?.order.len;
        const rg_rows = t.row_group_size;
        const rg_count = (rows + rg_rows - 1) / rg_rows;

        const self = try allocator.create(Write);
        errdefer allocator.destroy(self);
        const picks = try allocator.alloc(Pick, rows);
        errdefer allocator.free(picks);
        const groups = try allocator.alloc(Group, rg_count);
        errdefer allocator.free(groups);
        @memset(groups, .{});
        const has_table = try allocator.alloc(bool, columns);
        errdefer allocator.free(has_table);
        const commit_next = try allocator.alloc(usize, columns);
        errdefer allocator.free(commit_next);
        @memset(commit_next, 0);
        const committing = try allocator.alloc(bool, columns);
        errdefer allocator.free(committing);
        @memset(committing, false);
        const tables = try allocator.alloc(TableStep, columns);
        errdefer allocator.free(tables);
        const decode_next = try allocator.alloc(usize, columns);
        errdefer allocator.free(decode_next);
        const decoded = try allocator.alloc(usize, columns);
        errdefer allocator.free(decoded);
        const gather_next = try allocator.alloc(usize, columns);
        errdefer allocator.free(gather_next);
        @memset(gather_next, 0);
        const gathered = try allocator.alloc(usize, columns);
        errdefer allocator.free(gathered);
        @memset(gathered, 0);
        const id_steps = try allocator.alloc(Write.IdStep, columns);
        errdefer allocator.free(id_steps);
        for (id_steps, 0..) |*step, ci| step.* = if (t.strings.fixedListWidth(ci) == null) .done else .pending;
        @memset(decode_next, 0);
        @memset(decoded, 0);
        for (t.order_key_indices) |ci| {
            decode_next[ci] = seg.batches.len;
            decoded[ci] = seg.batches.len;
        }

        const id = t.allocSegmentId();
        var name_buf: [32]u8 = undefined;
        const file_name = try Table.segmentFileName(&name_buf, id);
        // The sketches are whole only once every column decodes; `appendReady`
        // hands them over before `finish`.
        var writer = try storage.MergedSegmentWriter.begin(allocator, t.io, t.segments_dir, file_name, t.schema, id, t.schema_fingerprint, rg_rows, &.{}, 1);
        errdefer writer.deinit();
        writer.useTableIds(t.tableIdsTarget());
        try writer.beginRowGroups(rows);

        var table_count: usize = 0;
        for (writer.strings.columns, has_table, tables) |c, *has, *step| {
            has.* = c != null;
            step.* = .{ .done = c == null };
            table_count += @intFromBool(c != null);
        }
        const table_columns = try allocator.alloc(usize, table_count);
        var i: usize = 0;
        for (writer.strings.columns, 0..) |c, ci| if (c != null) {
            table_columns[i] = ci;
            i += 1;
        };

        self.* = .{
            .segment = segment,
            .writer = writer,
            .picks = picks,
            .rows = rows,
            .rg_rows = rg_rows,
            .rg_count = rg_count,
            .groups = groups,
            .has_table = has_table,
            .commit_next = commit_next,
            .committing = committing,
            .table_columns = table_columns,
            .tables = tables,
            .decode_next = decode_next,
            .decoded = decoded,
            .gather_next = gather_next,
            .gathered = gathered,
            .id_steps = id_steps,
        };
        return self;
    }

    fn destroy(self: *Write, allocator: Allocator) void {
        if (!self.finished) self.writer.deinit();
        allocator.free(self.picks);
        for (self.groups) |*g| g.deinit(allocator);
        allocator.free(self.groups);
        allocator.free(self.has_table);
        allocator.free(self.commit_next);
        allocator.free(self.committing);
        allocator.free(self.table_columns);
        for (self.tables) |*step| if (step.build) |*b| b.deinit(allocator);
        allocator.free(self.tables);
        allocator.free(self.decode_next);
        allocator.free(self.decoded);
        allocator.free(self.gather_next);
        allocator.free(self.gathered);
        allocator.free(self.id_steps);
        allocator.destroy(self);
    }

    fn rowsOf(self: *const Write, rg: usize) usize {
        return @min(self.rg_rows, self.rows - rg * self.rg_rows);
    }

    fn picksOf(self: *const Write, rg: usize) []const Pick {
        return self.picks[rg * self.rg_rows ..][0..self.rowsOf(rg)];
    }
};

/// A column's string table, from the claim of `beginStringTable` until
/// the table is placed.
const TableStep = struct {
    begun: bool = false,
    build: ?segment_writer.StringTableBuild = null,
    /// The next piece to claim, and the pieces encoded.
    next_piece: usize = 0,
    pieces_done: usize = 0,
    /// Set from the start for a column without a table.
    done: bool = false,
};

/// String-table pieces a task encodes.
const table_piece_batch = 4;

/// One output row group in flight: claimed once its picks are published.
const Group = struct {
    /// Per column, its block until appended.
    blocks: []segment_writer.ColumnBlock = &.{},
    /// Per column, the gathered rows a string-table commit still reads.
    held: []?engine.ColumnStore = &.{},
    /// Per column, encoded and awaiting its commit.
    ready: []bool = &.{},
    /// Per column, encoded and committed: its block may append.
    done: []bool = &.{},

    fn claimed(self: Group) bool {
        return self.blocks.len > 0;
    }

    fn init(self: *Group, allocator: Allocator, columns: usize) !void {
        const blocks = try allocator.alloc(segment_writer.ColumnBlock, columns);
        errdefer allocator.free(blocks);
        @memset(blocks, .{});
        const held = try allocator.alloc(?engine.ColumnStore, columns);
        errdefer allocator.free(held);
        @memset(held, null);
        const ready = try allocator.alloc(bool, columns);
        errdefer allocator.free(ready);
        @memset(ready, false);
        const done = try allocator.alloc(bool, columns);
        @memset(done, false);
        self.* = .{ .blocks = blocks, .held = held, .ready = ready, .done = done };
    }

    fn deinit(self: *Group, allocator: Allocator) void {
        for (self.blocks) |*b| b.deinit(allocator);
        allocator.free(self.blocks);
        for (self.held) |*h| if (h.*) |*store| store.deinit(allocator);
        allocator.free(self.held);
        allocator.free(self.ready);
        allocator.free(self.done);
        self.* = .{};
    }
};

const Task = union(enum) {
    decode: usize,
    decode_column: struct { segment: usize, column: usize, batch: usize },
    column: struct { segment: usize, rg: usize, column: usize },
    table_begin: struct { segment: usize, column: usize },
    table_pieces: struct { segment: usize, column: usize, first: usize, end: usize },
    id_plan: struct { segment: usize, column: usize },
};

/// A worker's file handle and buffers, kept across its tasks.
const Worker = struct {
    allocator: Allocator,
    io: Io,
    /// Its own handle, since Windows serializes the reads of one synchronous
    /// handle.
    file: Io.File,
    scratch: parquet.Scratch = .{},
    bytes: std.ArrayList(u8) = .empty,
    compressor: storage.compression.Compressor,
    /// Per column, the store a gather fills; a string table's commit takes
    /// it over.
    stores: []?engine.ColumnStore,

    fn init(allocator: Allocator, io: Io, dir: Io.Dir, sub_path: []const u8, columns: usize) !Worker {
        const stores = try allocator.alloc(?engine.ColumnStore, columns);
        errdefer allocator.free(stores);
        @memset(stores, null);
        var compressor = try storage.compression.Compressor.init();
        errdefer compressor.deinit();
        const file = try dir.openFile(io, sub_path, .{});
        return .{ .allocator = allocator, .io = io, .file = file, .compressor = compressor, .stores = stores };
    }

    fn deinit(self: *Worker) void {
        self.file.close(self.io);
        self.scratch.deinit(self.allocator);
        self.bytes.deinit(self.allocator);
        self.compressor.deinit();
        for (self.stores) |*s| if (s.*) |*store| store.deinit(self.allocator);
        self.allocator.free(self.stores);
    }

    /// Gathers column `ci` of the rows `picks` names from `batches`.
    fn gather(self: *Worker, batches: []const ?Batch, picks: []const Pick, ci: usize, column: types.Column) !*engine.ColumnStore {
        if (self.stores[ci] == null) self.stores[ci] = try engine.ColumnStore.initCapacity(self.allocator, column.type, column.nullable, picks.len, 0);
        const store = &self.stores[ci].?;
        store.clear();
        try gatherPicks(self.allocator, batches, picks, ci, store);
        return store;
    }
};

/// Decodes row group `rg`'s chunk of Parquet leaf `leaf` as `column`,
/// reading through `w`'s buffers.
fn decodeChunk(t: *Table, meta: *const parquet.Metadata, w: *Worker, rg: parquet.RowGroup, leaf: usize, column: types.Column) !engine.ColumnStore {
    const allocator = t.allocator;
    const chunk = rg.chunks[leaf];
    try w.bytes.resize(allocator, @intCast(chunk.len));
    if (try w.file.readPositionalAll(t.io, w.bytes.items, chunk.start) != w.bytes.items.len) return error.CorruptParquet;
    var out = try engine.ColumnStore.initCapacity(allocator, column.type, column.nullable, @intCast(rg.rows), 0);
    errdefer out.deinit(allocator);
    try parquet.decodeChunk(allocator, &w.scratch, meta.columns[leaf], chunk, w.bytes.items, &out);
    if (out.rowCount() != rg.rows) return error.CorruptParquet;
    if (out.isWide()) return error.UnsupportedParquet;
    return out;
}

/// Appends column `ci` of the rows `picks` names from `batches` to `out`, of
/// the column's type, with one dispatch on the type rather than one per row.
fn gatherPicks(allocator: Allocator, batches: []const ?Batch, picks: []const Pick, ci: usize, out: *engine.ColumnStore) !void {
    const start = out.data.rowCount();
    switch (out.data) {
        inline else => |*dst, tag| {
            const name = @tagName(tag);
            if (comptime @hasDecl(@TypeOf(dst.*), "appendValueAssumeCapacity")) {
                var total: usize = 0;
                for (picks) |p| total += @field(batches[p.batch].?.views[ci].data, name).rowBytes(p.row).len;
                if (dst.isWide() or dst.bytes.items.len + total > std.math.maxInt(u32)) {
                    for (picks) |p| try dst.appendValue(allocator, @field(batches[p.batch].?.views[ci].data, name).rowBytes(p.row));
                } else {
                    try dst.ensureUnusedValueCapacity(allocator, picks.len, total);
                    for (picks) |p| dst.appendValueAssumeCapacity(@field(batches[p.batch].?.views[ci].data, name).rowBytes(p.row));
                }
            } else {
                const values = try dst.addManyAsSlice(allocator, picks.len);
                for (picks, values) |p, *v| v.* = @field(batches[p.batch].?.views[ci].data, name)[p.row];
            }
        },
    }
    // Bits past the row count stay 0, so only valid rows need setting.
    if (out.nulls) |*bits| {
        const need = (start + picks.len + 7) / 8;
        if (bits.items.len < need) try bits.appendNTimes(allocator, 0, need - bits.items.len);
        for (picks, start..) |p, r| {
            if (storage.column.isValidBit(batches[p.batch].?.views[ci].nulls, p.row)) bits.items[r >> 3] |= @as(u8, 1) << @intCast(r & 7);
        }
    }
}

const Load = struct {
    t: *Table,
    dir: Io.Dir,
    sub_path: []const u8,
    meta: *const parquet.Metadata,
    leaves: []const usize,
    plan: *const Plan,
    segments: []Segment,
    lanes: usize,
    workers: usize,
    /// The workers that may take tasks now: those numbered below its count.
    grant: *core_scheduler.Workers,
    /// Decodes in flight that come before writing tasks.
    decode_share: usize,
    /// Columns a segment may have open.
    open_columns: usize,

    mutex: Io.Mutex = .init,
    /// Signals new tasks, and the load's end.
    changed: Io.Condition = .init,
    next_unit: usize = 0,
    decoding: usize = 0,
    /// Workers waiting for a task, and the threads of theirs a table's id
    /// resolve has borrowed.
    idle: usize = 0,
    lent: usize = 0,
    /// The lowest segment still holding decoded batches.
    low: usize = 0,
    finished: usize = 0,
    failed: bool = false,
    err: ?anyerror = null,

    fn run(self: *Load) void {
        const allocator = self.t.allocator;
        if (self.workers > 1) {
            if (allocator.alloc(std.Thread, self.workers - 1)) |handles| {
                defer allocator.free(handles);
                var spawned: usize = 0;
                for (handles, 1..) |*h, index| {
                    h.* = std.Thread.spawn(.{}, worker, .{ self, index }) catch break;
                    spawned += 1;
                }
                worker(self, 0);
                for (handles[0..spawned]) |h| h.join();
                return;
            } else |_| {}
        }
        worker(self, 0);
    }

    fn worker(self: *Load, index: usize) void {
        const io = self.t.io;
        var w = Worker.init(self.t.allocator, io, self.dir, self.sub_path, self.t.schema.columns.len) catch |e| {
            self.mutex.lockUncancelable(io);
            defer self.mutex.unlock(io);
            return self.fail(e);
        };
        defer w.deinit();
        self.mutex.lockUncancelable(io);
        defer self.mutex.unlock(io);
        while (!self.failed and self.finished < self.segments.len) {
            const allowed = self.grant.count;
            self.grant.refit(self.workers);
            if (self.grant.count > allowed) self.changed.broadcast(io);
            if (index >= self.grant.count) {
                self.changed.waitUncancelable(io, &self.mutex);
                continue;
            }
            const task = self.claim() catch |e| return self.fail(e);
            if (task) |tk| {
                self.perform(&w, tk) catch |e| return self.fail(e);
            } else {
                self.idle += 1;
                defer self.idle -= 1;
                self.changed.waitUncancelable(io, &self.mutex);
            }
        }
    }

    /// With the lock held.
    fn fail(self: *Load, e: anyerror) void {
        if (self.err == null) self.err = e;
        self.failed = true;
        self.changed.broadcast(self.t.io);
    }

    /// Runs `f(args)` with the lock released.
    fn unlocked(self: *Load, comptime f: anytype, args: anytype) @TypeOf(@call(.auto, f, args)) {
        const io = self.t.io;
        self.mutex.unlock(io);
        defer self.mutex.lockUncancelable(io);
        return @call(.auto, f, args);
    }

    fn claim(self: *Load) !?Task {
        if (self.decoding < self.decode_share) if (self.claimDecode()) |u| return .{ .decode = u };
        const columns = self.t.schema.columns.len;
        for (self.segments, 0..) |*seg, s| {
            const w = seg.write orelse continue;
            for (w.table_columns) |ci| {
                const step = &w.tables[ci];
                if (!step.begun) {
                    if (w.commit_next[ci] < w.rg_count) continue;
                    step.begun = true;
                    return .{ .table_begin = .{ .segment = s, .column = ci } };
                }
                const build = &(step.build orelse continue);
                if (step.next_piece == build.pieces.len) continue;
                const first = step.next_piece;
                step.next_piece = @min(first + table_piece_batch, build.pieces.len);
                return .{ .table_pieces = .{ .segment = s, .column = ci, .first = first, .end = step.next_piece } };
            }
            const units = seg.batches.len;
            while (true) {
                for (w.first_open..w.opened) |ci| {
                    if (w.decoded[ci] < units) continue;
                    switch (w.id_steps[ci]) {
                        .pending => {
                            // The plan weighs the output's row groups.
                            if (w.picked < w.groups.len) continue;
                            w.id_steps[ci] = .planning;
                            return .{ .id_plan = .{ .segment = s, .column = ci } };
                        },
                        .planning => continue,
                        .done => {},
                    }
                    if (w.gather_next[ci] >= w.picked) continue;
                    const rg = w.gather_next[ci];
                    w.gather_next[ci] += 1;
                    if (!w.groups[rg].claimed()) try w.groups[rg].init(self.t.allocator, columns);
                    return .{ .column = .{ .segment = s, .rg = rg, .column = ci } };
                }
                for (w.first_open..w.opened) |ci| {
                    if (w.decode_next[ci] == units) continue;
                    w.decode_next[ci] += 1;
                    return .{ .decode_column = .{ .segment = s, .column = ci, .batch = w.decode_next[ci] - 1 } };
                }
                if (w.opened == columns or w.opened - w.closed == self.open_columns) break;
                w.opened += 1;
            }
        }
        if (self.claimDecode()) |u| return .{ .decode = u };
        return null;
    }

    fn claimDecode(self: *Load) ?usize {
        const u = self.next_unit;
        if (u >= self.plan.units.len or self.plan.segment_of[u] >= self.low + self.lanes) return null;
        self.next_unit += 1;
        self.decoding += 1;
        return u;
    }

    /// With the lock held, as on return.
    fn perform(self: *Load, w: *Worker, task: Task) !void {
        switch (task) {
            .decode => |u| try self.decode(w, u),
            .decode_column => |c| try self.decodeColumn(w, c.segment, c.column, c.batch),
            .column => |c| try self.column(w, c.segment, c.rg, c.column),
            .table_begin => |c| try self.tableBegin(c.segment, c.column),
            .table_pieces => |c| try self.tablePieces(w, c.segment, c.column, c.first, c.end),
            .id_plan => |c| try self.idPlan(c.segment, c.column),
        }
    }

    /// Plans fixed-width column `ci`'s table ids over segment `s`'s rows,
    /// all decoded, ahead of its gathers.
    fn idPlan(self: *Load, s: usize, ci: usize) !void {
        const seg = &self.segments[s];
        const write = seg.write.?;
        defer {
            write.id_steps[ci] = .done;
            self.changed.broadcast(self.t.io);
        }
        if (try self.unlocked(planIds, .{ self.t, seg, write, ci })) |plan| write.writer.useIdPlan(ci, plan);
    }

    fn planIds(t: *Table, seg: *const Segment, write: *const Write, ci: usize) !?storage.int_ids.Plan {
        const target = t.tableIdsTarget();
        const state = target.strings.state(target.io, ci) orelse return null;
        if (state.closed) return null;
        var rows: u64 = 0;
        for (seg.batches) |b| rows += b.?.order.len;
        const distinct = hll.Hll.fromBytes(seg.sketches[ci * hll.m ..][0..hll.m]).estimate();
        if (!storage.int_ids.worthPlanning(rows, distinct - distinct / 16, state.id_count)) return null;
        var builder: storage.int_ids.Builder = .init(t.allocator);
        for (seg.batches) |b| builder.add(b.?.views[ci]) catch |e| {
            builder.deinit();
            return e;
        };
        for (0..write.groups.len) |rg| {
            var span: storage.int_ids.GroupSpan = .{};
            for (write.picksOf(rg)) |p| {
                const view = seg.batches[p.batch].?.views[ci];
                if (view.isValid(p.row)) span.add(storage.int_ids.valuesOf(view).?[p.row]);
            }
            builder.addGroup(span);
        }
        return builder.finish(target, ci);
    }

    fn decode(self: *Load, w: *Worker, u: usize) !void {
        const s = self.plan.segment_of[u];
        const seg = &self.segments[s];
        const batch = self.unlocked(decodeKeys, .{ self, w, u, s });
        self.decoding -= 1;
        seg.batches[u - self.plan.first[s]] = try batch;
        seg.decoded += 1;
        if (seg.decoded < seg.batches.len) return;

        seg.write = try self.unlocked(Write.create, .{ self.t, s, seg });
        try self.unlocked(mergeBatches, .{ self, seg.write.?, seg.batches });
    }

    /// Decodes row group `u`'s order-key columns and sorts its rows by the
    /// key.
    fn decodeKeys(self: *Load, w: *Worker, u: usize, s: usize) !Batch {
        const t = self.t;
        const allocator = t.allocator;
        const rows = std.math.cast(u32, self.meta.row_groups[self.plan.units[u]].rows) orelse return error.UnsupportedParquet;
        const ncols = t.schema.columns.len;
        const columns = try allocator.alloc(?engine.ColumnStore, ncols);
        @memset(columns, null);
        errdefer {
            for (columns) |*c| if (c.*) |*store| store.deinit(allocator);
            allocator.free(columns);
        }
        const views = try allocator.alloc(storage.ColumnView, ncols);
        errdefer allocator.free(views);
        for (t.order_key_indices) |ci| {
            columns[ci] = try self.readColumn(w, s, u, ci);
            views[ci] = columns[ci].?.view();
        }

        const order = try allocator.alloc(u32, rows);
        errdefer allocator.free(order);
        for (order, 0..) |*o, i| o.* = @intCast(i);
        std.sort.pdq(u32, order, OrderKey{ .columns = columns, .keys = t.order_key_indices }, OrderKey.lessThan);
        return .{ .columns = columns, .views = views, .order = order };
    }

    /// Reads and decodes column `ci` of row group `u`, folding its sketch
    /// into segment `s`'s.
    fn readColumn(self: *Load, w: *Worker, s: usize, u: usize, ci: usize) !engine.ColumnStore {
        const allocator = self.t.allocator;
        var out = try decodeChunk(self.t, self.meta, w, self.meta.row_groups[self.plan.units[u]], self.leaves[ci], self.t.schema.columns[ci]);
        errdefer out.deinit(allocator);
        const sketch = try segment_writer.computeSketches(allocator, &.{out.view()}, out.rowCount());
        defer allocator.free(sketch);
        self.segments[s].addSketches(ci, sketch);
        return out;
    }

    fn decodeColumn(self: *Load, w: *Worker, s: usize, ci: usize, b: usize) !void {
        const seg = &self.segments[s];
        const store = try self.unlocked(readColumn, .{ self, w, s, self.plan.first[s] + b, ci });
        const batch = &seg.batches[b].?;
        batch.columns[ci] = store;
        batch.views[ci] = store.view();
        const write = seg.write.?;
        write.decoded[ci] += 1;
        if (write.decoded[ci] == seg.batches.len) self.changed.broadcast(self.t.io);
    }

    fn column(self: *Load, w: *Worker, s: usize, rg: usize, ci: usize) !void {
        const seg = &self.segments[s];
        const write = seg.write.?;
        const group = &write.groups[rg];
        const store = try self.unlocked(Worker.gather, .{ w, seg.batches, write.picksOf(rg), ci, self.t.schema.columns[ci] });
        write.gathered[ci] += 1;
        if (write.gathered[ci] == write.rg_count) self.close(seg, write, ci);

        try self.unlocked(storage.MergedSegmentWriter.encodeColumn, .{ &write.writer, &w.compressor, ci, store.view(), &group.blocks[ci] });
        if (!write.has_table[ci]) return self.blockDone(write, rg, ci);
        // Even a row group encoded in place takes its turn, so the column's
        // later row groups commit after it.
        if (group.blocks[ci].strings != null) {
            group.held[ci] = store.*;
            w.stores[ci] = null;
        }
        group.ready[ci] = true;
        try self.commitReady(w, write, ci);
    }

    /// Frees column `ci` of the segment's row groups, now every output row
    /// group has gathered it, and with the last column the rest of the
    /// input.
    fn close(self: *Load, seg: *Segment, write: *Write, ci: usize) void {
        const allocator = self.t.allocator;
        self.unlocked(freeColumn, .{ seg.batches, ci, allocator });
        write.closed += 1;
        while (write.first_open < write.opened and write.gathered[write.first_open] == write.rg_count) write.first_open += 1;
        if (write.closed == self.t.schema.columns.len) {
            self.unlocked(release, .{ seg, write, allocator });
            seg.released = true;
            while (self.low < self.segments.len and self.segments[self.low].released) self.low += 1;
        }
        self.changed.broadcast(self.t.io);
    }

    fn freeColumn(batches: []?Batch, ci: usize, allocator: Allocator) void {
        for (batches) |b| {
            const columns = b.?.columns;
            if (columns[ci]) |*store| store.deinit(allocator);
            columns[ci] = null;
        }
    }

    fn release(seg: *Segment, write: *Write, allocator: Allocator) void {
        seg.freeBatches(allocator);
        allocator.free(write.picks);
        write.picks = &.{};
    }

    /// Commits column `ci`'s ready row groups to its string table, in order,
    /// unless another worker is.
    fn commitReady(self: *Load, w: *Worker, write: *Write, ci: usize) !void {
        if (write.committing[ci]) return;
        write.committing[ci] = true;
        defer write.committing[ci] = false;
        while (write.commit_next[ci] < write.rg_count) {
            const r = write.commit_next[ci];
            const g = &write.groups[r];
            if (!g.claimed() or !g.ready[ci]) return;
            if (r == 0) try self.unlocked(storage.MergedSegmentWriter.reserveColumnStrings, .{ &write.writer, ci, self.segments[write.segment].distinctEstimate(ci) });
            try self.unlocked(commitHeld, .{ write, w, ci, r, g });
            write.commit_next[ci] = r + 1;
            try self.blockDone(write, r, ci);
        }
    }

    fn commitHeld(write: *Write, w: *Worker, ci: usize, r: usize, g: *Group) !void {
        if (g.held[ci]) |*held| {
            defer {
                held.deinit(w.allocator);
                g.held[ci] = null;
            }
            try write.writer.commitColumn(&w.compressor, ci, @intCast(r), held.view(), &g.blocks[ci]);
        }
    }

    fn blockDone(self: *Load, write: *Write, r: usize, ci: usize) !void {
        write.groups[r].done[ci] = true;
        if (r == write.append_rg and ci == write.append_column) try self.appendReady(write);
    }

    /// Appends what is done, unless another worker is: the blocks that come
    /// next, column by column, and each string table once encoded and its
    /// column's blocks appended, in whatever order the tables finish; then,
    /// with everything appended, finishes the segment. Each column's data
    /// thus lies together, and the file grows as columns complete, not all
    /// at the segment's end, so its writeback overlaps the encoding. A table
    /// takes a while to encode once its column is done, and the columns
    /// after it don't wait for it.
    fn appendReady(self: *Load, write: *Write) !void {
        if (write.appending or write.finished) return;
        write.appending = true;
        defer write.appending = false;
        const columns = self.t.schema.columns.len;
        var appended = true;
        while (appended) {
            appended = false;
            for (write.table_columns) |ci| {
                const step = &write.tables[ci];
                if (!step.done or ci >= write.append_column) continue;
                var build = step.build orelse continue;
                step.build = null;
                try self.unlocked(storage.MergedSegmentWriter.appendStringTable, .{ &write.writer, &build });
                appended = true;
            }
            // A table may finish while another appends with the lock released,
            // and its worker leaves it to this pass, so only a pass that
            // appended nothing ends the loop.
            if (write.append_column == columns) continue;
            if (write.append_rg == write.rg_count) {
                write.append_column += 1;
                write.append_rg = 0;
                appended = true;
                continue;
            }
            const g = &write.groups[write.append_rg];
            if (!g.claimed() or !g.done[write.append_column]) continue;
            try self.unlocked(appendBlock, .{ write, write.append_rg, write.append_column, self.t.allocator });
            write.append_rg += 1;
            appended = true;
        }
        if (write.append_column < columns) return;
        for (write.table_columns) |ci| if (!write.tables[ci].done or write.tables[ci].build != null) return;
        write.writer.column_sketches = try self.t.allocator.dupe(u8, self.segments[write.segment].sketches);
        const info = try self.unlocked(storage.MergedSegmentWriter.finish, .{ &write.writer, self.t.syncEnabled() });
        write.finished = true;
        self.segments[write.segment].info = info;
        self.finished += 1;
        self.changed.broadcast(self.t.io);
    }

    fn appendBlock(write: *Write, r: usize, ci: usize, allocator: Allocator) !void {
        const block = &write.groups[r].blocks[ci];
        defer {
            block.deinit(allocator);
            block.* = .{};
        }
        try write.writer.appendColumnBlock(r, ci, block);
    }

    fn tableBegin(self: *Load, s: usize, ci: usize) !void {
        const write = self.segments[s].write.?;
        // A table's id resolve is its one step that doesn't split into tasks,
        // so it runs on the cores of the workers left without one, as at the
        // load's end, where it would otherwise run alone.
        const borrowed = self.idle -| self.lent;
        self.lent += borrowed;
        defer self.lent -= borrowed;
        const threads = @max(self.t.compact_threads, 1 + borrowed);
        const build = try self.unlocked(storage.MergedSegmentWriter.beginStringTableOn, .{ &write.writer, ci, threads });
        const step = &write.tables[ci];
        if (build) |b| {
            step.build = b;
            self.changed.broadcast(self.t.io);
            return;
        }
        step.done = true;
        try self.appendReady(write);
    }

    fn tablePieces(self: *Load, w: *Worker, s: usize, ci: usize, first: usize, end: usize) !void {
        const write = self.segments[s].write.?;
        const step = &write.tables[ci];
        try self.unlocked(encodePieces, .{ write, &w.compressor, &step.build.?, first, end });
        step.pieces_done += end - first;
        if (step.pieces_done < step.build.?.pieces.len) return;
        step.done = true;
        try self.appendReady(write);
    }

    fn encodePieces(write: *Write, compressor: *storage.compression.Compressor, build: *segment_writer.StringTableBuild, first: usize, end: usize) !void {
        for (first..end) |i| try write.writer.encodeStringTablePiece(compressor, build, i);
    }

    /// K-way merges a segment's sorted batches into `write.picks`,
    /// publishing each output row group as it fills.
    fn mergeBatches(self: *Load, write: *Write, batches: []const ?Batch) !void {
        const allocator = self.t.allocator;
        const keys = self.t.order_key_indices;
        const cursors = try allocator.alloc(Cursor, batches.len);
        defer allocator.free(cursors);
        const heap = try allocator.alloc(usize, batches.len);
        defer allocator.free(heap);
        var heap_len: usize = 0;
        for (batches, cursors, 0..) |*b, *c, i| {
            c.* = .{ .batch = &b.*.?, .pos = 0 };
            if (c.batch.order.len == 0) continue;
            heap[heap_len] = i;
            heap_len += 1;
            siftUp(keys, cursors, heap[0..heap_len], heap_len - 1);
        }

        var n: usize = 0;
        while (heap_len > 0) {
            const top = heap[0];
            const c = &cursors[top];
            write.picks[n] = .{ .batch = @intCast(top), .row = @intCast(c.row()) };
            n += 1;
            c.pos += 1;
            if (c.pos == c.batch.order.len) {
                heap_len -= 1;
                heap[0] = heap[heap_len];
            }
            if (heap_len > 0) siftDown(keys, cursors, heap[0..heap_len], 0);

            if (n % write.rg_rows == 0 or heap_len == 0) {
                self.mutex.lockUncancelable(self.t.io);
                defer self.mutex.unlock(self.t.io);
                write.picked = (n + write.rg_rows - 1) / write.rg_rows;
                self.changed.broadcast(self.t.io);
            }
        }
    }
};

const OrderKey = struct {
    columns: []const ?engine.ColumnStore,
    keys: []const usize,

    fn lessThan(ctx: OrderKey, a: u32, b: u32) bool {
        for (ctx.keys) |ci| {
            switch (engine.transform.compareInColumn(ctx.columns[ci].?, a, b)) {
                .lt => return true,
                .gt => return false,
                .eq => {},
            }
        }
        return false;
    }
};

/// One merge input: a batch and how far into its sorted order the merge is.
const Cursor = struct {
    batch: *const Batch,
    pos: usize,

    fn row(self: Cursor) usize {
        return self.batch.order[self.pos];
    }
};

/// Whether `a`'s current row sorts before `b`'s; ties go to the earlier
/// batch, so a file loads into the same segments on every run.
fn cursorLess(keys: []const usize, a: Cursor, a_index: usize, b: Cursor, b_index: usize) bool {
    for (keys) |ci| {
        switch (engine.transform.compareViewRows(a.batch.views[ci], a.row(), b.batch.views[ci], b.row())) {
            .lt => return true,
            .gt => return false,
            .eq => {},
        }
    }
    return a_index < b_index;
}

fn siftUp(keys: []const usize, cursors: []const Cursor, heap: []usize, at: usize) void {
    var i = at;
    while (i > 0) {
        const parent = (i - 1) / 2;
        if (!cursorLess(keys, cursors[heap[i]], heap[i], cursors[heap[parent]], heap[parent])) return;
        std.mem.swap(usize, &heap[i], &heap[parent]);
        i = parent;
    }
}

fn siftDown(keys: []const usize, cursors: []const Cursor, heap: []usize, at: usize) void {
    var i = at;
    while (true) {
        var least = i;
        for ([_]usize{ 2 * i + 1, 2 * i + 2 }) |child| {
            if (child < heap.len and cursorLess(keys, cursors[heap[child]], heap[child], cursors[heap[least]], heap[least])) least = child;
        }
        if (least == i) return;
        std.mem.swap(usize, &heap[i], &heap[least]);
        i = least;
    }
}

/// Adds the written segments to the manifest, after every existing one, in
/// one swap.
fn publish(t: *Table, segments: []Segment) !void {
    t.ddl_lock.lockUncancelable(t.io);
    defer t.ddl_lock.unlock(t.io);
    t.mutex.lockUncancelable(t.io);
    defer t.mutex.unlock(t.io);
    try t.ensureUsable();

    var entries: std.ArrayList(storage.ManifestEntry) = .empty;
    defer entries.deinit(t.allocator);
    try entries.ensureTotalCapacity(t.allocator, t.manifest.segments.items.len + segments.len);
    entries.appendSliceAssumeCapacity(t.manifest.segments.items);
    var added: usize = 0;
    errdefer for (entries.items[t.manifest.segments.items.len..][0..added]) |*e| e.deinit(t.allocator);
    for (segments) |s| {
        entries.appendAssumeCapacity(try t.entryFor(s.info.?));
        added += 1;
    }

    const sync = t.syncEnabled();
    var candidate = t.manifest;
    candidate.segments = entries;
    if (sync) try storage.syncDirectory(t.io, t.segments_dir);
    try t.persistManifest(candidate, sync);

    t.manifest.segments.deinit(t.allocator);
    t.manifest = candidate;
    entries = .empty;
}

test "segmentRowsFor sizes segments by the tighter of merge and load memory" {
    const pick: f64 = @sizeOf(Pick) + @sizeOf(u32);
    var decoded = [_]f64{ 8, 16, 40 };
    var merge = [_]f64{ 0, 0, 50 };
    var no_merge = [_]f64{ 0, 0, 0 };
    const strings: RowCost = .{ .decoded = &decoded, .merge = &merge };
    const numbers: RowCost = .{ .decoded = &decoded, .merge = &no_merge };
    const compact_memory: u64 = (4 << 30) + (512 << 20);
    const plenty: u64 = 1 << 50;

    // Half what a merge of 50 string bytes a row builds in compact_memory.
    try std.testing.expectEqual(compact.mergeStringCapacity(compact_memory) / 100, segmentRowsFor(strings, &.{0}, compact_memory, plenty, 2, 2));

    // Two lanes of keys, and the widest two open columns: 2 * 16 + 2 * 40 + 50.
    const tight: f64 = 1 << 30;
    const by_memory: u64 = @intFromFloat(tight / (held_peak_ratio * (2 * (pick + 8) + 162)));
    try std.testing.expectEqual(by_memory, segmentRowsFor(strings, &.{0}, compact_memory, 1 << 30, 2, 2));

    // Without string tables no merge limit applies, however small compact_memory.
    const by_memory_only: u64 = @intFromFloat(tight / (held_peak_ratio * (2 * (pick + 8) + 112)));
    try std.testing.expectEqual(by_memory_only, segmentRowsFor(numbers, &.{0}, 0, 1 << 30, 2, 2));
}
