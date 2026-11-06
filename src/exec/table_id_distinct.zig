//! `COUNT(DISTINCT <string column>)` by table ids (DESIGN.md §4.6),
//! reading no value's bytes.
//!
//! Each segment's string table stores the table id of every distinct value
//! it holds, and one value has one table id in every segment, so the
//! column's distinct values over the segments are the union of those id
//! lists. Each worker marks the ids of its share of the lists in a bitmap
//! of its own, then the workers OR the bitmaps together, word range by word
//! range, counting the bits.

const std = @import("std");
const Allocator = std.mem.Allocator;
const parallel = @import("../util/parallel.zig");
const api = @import("../api/api.zig");
const exec = @import("exec.zig");
const storage = @import("../storage/storage.zig");

/// Ids a marking job takes: small enough to balance the workers, large
/// enough that claiming a job costs nothing beside it.
const JOB_IDS: usize = 1 << 16;

pub const Request = struct {
    table: *api.Table,
    snapshot: exec.Scan.Snapshot,
    column_idx: usize,
    workers: usize,
};

/// The column's distinct non-NULL values over the snapshot. Null when the
/// snapshot holds memtable rows or tombstones, the column takes no table
/// ids, or a segment stores none for some of its values.
pub fn count(allocator: Allocator, request: Request) !?u64 {
    const table = request.table;
    const snapshot = request.snapshot;
    if (snapshot.memtable_row_count != 0 or snapshot.segments.len == 0) return null;
    const state = table.strings.readerState(request.column_idx) orelse return null;
    if (state.closed) return null;

    const worker_allocator = try exec.memory.workerAllocatorOf(allocator);
    var run: Run = .{
        .table = table,
        .column_idx = request.column_idx,
        .id_count = state.id_count,
        .segments = try worker_allocator.alloc(Segment, snapshot.segments.len),
    };
    var opened: usize = 0;
    defer {
        for (run.segments[0..opened]) |*s| s.deinit(table);
        worker_allocator.free(run.segments);
    }
    for (snapshot.segments, run.segments) |entry, *s| {
        s.* = .{ .handle = try table.acquireSegment(entry.segment_id) };
        opened += 1;
        if (!try qualifies(table, s.handle, snapshot, request.column_idx)) return null;
    }

    parallel.forJobs(request.workers, run.segments.len, &run, Run.fetch);
    if (run.failure) |err| return err;
    if (run.missing.load(.monotonic)) return null;

    const workers = @max(1, @min(request.workers, parallel.MAX_THREADS));
    const words = (@as(usize, state.id_count) + 63) / 64;
    if (exec.memory.accountantOf(allocator)) |accountant| {
        if (workers * words * 8 > accountant.pool.statementLimit() / 8) return null;
    }
    const bitmaps = try worker_allocator.alloc(u64, workers * words);
    defer worker_allocator.free(bitmaps);
    @memset(bitmaps, 0);
    run.bitmaps = bitmaps;
    run.words = words;

    var first: usize = 0;
    for (run.segments) |*s| {
        s.first_job = first;
        first += (s.ids.?.ids.len + JOB_IDS - 1) / JOB_IDS;
    }
    parallel.forJobs(workers, first, &run, Run.mark);
    if (run.out_of_range.load(.monotonic)) return null;

    var counts: [parallel.MAX_THREADS]u64 = @splat(0);
    run.counts = counts[0..workers];
    parallel.forRanges(workers, words, &run, Run.merge);
    var total: u64 = 0;
    for (run.counts) |c| total += c;
    return total;
}

/// Whether the segment's string table holds every non-NULL value of the
/// column: a row group whose block stands apart (past the table's cap)
/// holds values it doesn't, unless the row group's values are all NULL.
/// Tombstoned rows may hold a value no live row does, so a segment with
/// any is out.
fn qualifies(table: *api.Table, handle: *storage.cache.SegmentHandles.Entry, snapshot: exec.Scan.Snapshot, column_idx: usize) !bool {
    const info = handle.seg.info;
    const string_table = info.stringTable(column_idx) orelse return false;
    if (string_table.table_ids.length == 0) return false;
    for (string_table.blocks, info.row_groups) |parts, rg| {
        if (!parts.isSplit() and rg.stats[column_idx].null_count != rg.row_count) return false;
    }
    if (try table.acquireSegmentTombstones(handle, snapshot.tomb_epoch)) |tombs| {
        defer table.releaseSegmentTombstones(tombs);
        if (tombs.rows.len > 0) return false;
    }
    return true;
}

const Segment = struct {
    handle: *storage.cache.SegmentHandles.Entry,
    ids: ?storage.segment_reader.ReadSegment.TableIds = null,
    first_job: usize = 0,

    fn deinit(self: *Segment, table: *api.Table) void {
        if (self.ids) |ids| ids.release();
        table.releaseSegment(self.handle);
    }
};

const Run = struct {
    table: *api.Table,
    column_idx: usize,
    id_count: u32,
    segments: []Segment,
    bitmaps: []u64 = &.{},
    words: usize = 0,
    counts: []u64 = &.{},
    lock: std.Io.Mutex = .init,
    failure: ?anyerror = null,
    missing: std.atomic.Value(bool) = .init(false),
    out_of_range: std.atomic.Value(bool) = .init(false),

    fn fetch(self: *Run, _: usize, i: usize) void {
        const s = &self.segments[i];
        s.ids = s.handle.seg.cachedTableIds(self.column_idx, self.table.cacheRef()) catch |err| {
            self.lock.lockUncancelable(self.table.io);
            defer self.lock.unlock(self.table.io);
            if (self.failure == null) self.failure = err;
            return;
        };
        if (s.ids == null) self.missing.store(true, .monotonic);
    }

    fn mark(self: *Run, t: usize, job: usize) void {
        var si: usize = 0;
        while (si + 1 < self.segments.len and self.segments[si + 1].first_job <= job) si += 1;
        const s = self.segments[si];
        const ids = s.ids.?.ids;
        const lo = (job - s.first_job) * JOB_IDS;
        const hi = @min(lo + JOB_IDS, ids.len);
        const bitmap = self.bitmaps[t * self.words ..][0..self.words];
        var wide: u32 = 0;
        for (ids[lo..hi]) |stored| {
            const id = std.mem.littleToNative(u32, stored);
            wide |= @intFromBool(id >= self.id_count);
            const word = @min(id / 64, self.words - 1);
            bitmap[word] |= @as(u64, 1) << @intCast(id % 64);
        }
        if (wide != 0) self.out_of_range.store(true, .monotonic);
    }

    fn merge(self: *Run, t: usize, lo: usize, hi: usize) void {
        var n: u64 = 0;
        for (lo..hi) |w| {
            var word: u64 = 0;
            var b: usize = w;
            while (b < self.bitmaps.len) : (b += self.words) word |= self.bitmaps[b];
            n += @popCount(word);
        }
        self.counts[t] = n;
    }
};
