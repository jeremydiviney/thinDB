//! Exact `GROUP BY <string column> ORDER BY COUNT(*) DESC LIMIT k` by
//! segment ids, reading no value's bytes but the few that may place.
//!
//! A segment string table (DESIGN.md §4.4) numbers its column's values: a
//! value two or more row groups hold is shared, numbered by its rank in the
//! segment's sorted table, and each row-group block stores every value's
//! row count. Summing those counts by id gives each shared value's exact
//! count in its segment from the blocks' index and stats parts alone. A
//! value one row group holds (local) has that row group's count in the
//! segment.
//!
//! The segments then rank values as the units of a TPUT top-K (Cao & Wang,
//! PODC 2004; see count_topk_sketch.HeavyKeys): segment s reports each value
//! it holds at least t_s rows of, its bytes read from the shared pages (or,
//! for a local value, its row group's block), so a value no segment reports
//! holds fewer than Σ t_s rows. The need-th best reported partial τ bounds
//! the candidates: a value's partial plus t_s - 1 for each segment that
//! didn't report it must reach τ. A candidate's exact count adds its count
//! in each such segment: its shared count, found by search in the sorted
//! table, or, when the table doesn't hold it, its count as a local value,
//! found in the row groups' blocks, read only for a candidate that can still
//! place.
//!
//! A WHERE reading the column alone keeps or drops each value's rows whole,
//! so it is tested on the reported values: one it fails holds no filtered
//! row, and the thresholds, set over every value, still bound the rest.
//!
//! With no WHERE, when every segment carries the column's table ids
//! (DESIGN.md §4.6), a value is its table id in every segment: reports name
//! values by id, read from the segments' ids blocks instead of the values'
//! bytes, and one pass over each segment's ids finds every candidate's
//! segment id there, so its exact count needs no search by bytes. Only the
//! values that place have their bytes read.

const std = @import("std");
const Allocator = std.mem.Allocator;
const parallel = @import("../util/parallel.zig");
const api = @import("../api/api.zig");
const types = @import("../types.zig");
const exec = @import("exec.zig");
const storage = @import("../storage/storage.zig");
const count_topk_sketch = @import("count_topk_sketch.zig");

const TopRow = exec.silo_group_core.TopRow;
const page_values = storage.segment_strings.page_values;

/// A segment's t - 1 is 1/this of its need-th best count: every segment's
/// need-th best count is at most τ, so with a few segments their slack
/// stays well under it.
const SLACK_SHARE_DENOM: u64 = 4;

/// Past this many reports, the values' bytes the pass reads and merges
/// outweigh the heavy-key tally it stands in for.
const MAX_REPORTS: usize = 1 << 16;

/// Past this many candidates, the searches outweigh the heavy-key tally.
const MAX_CANDIDATES: usize = 1 << 12;

pub const Request = struct {
    table: *api.Table,
    snapshot: exec.Scan.Snapshot,
    column: []const u8,
    /// A WHERE reading `column` alone (predicate.readsOnlyColumn), which
    /// keeps or drops each of its values' rows whole.
    filter: ?exec.PredicateExpr = null,
    need: usize,
    /// The fewest rows a group needs to place at all.
    floor: u64 = 0,
    workers: usize,
    key_digest_mask: u64 = std.math.maxInt(u64),
};

/// Appends the top `need` values of the request's column by row count to
/// `rows`, each with its digest as key and a row holding it as rowref.
/// False, leaving `rows` as it was, when the snapshot doesn't keep the
/// column in segment string tables with entry stats, holds memtable rows or
/// tombstones, or the bounds can't prove the top.
pub fn run(allocator: Allocator, request: Request, rows: *std.ArrayListUnmanaged(TopRow)) !bool {
    const table = request.table;
    const snapshot = request.snapshot;
    if (request.need == 0 or request.need > count_topk_sketch.MAX_NEED) return false;
    if (snapshot.memtable_row_count != 0 or snapshot.segments.len == 0) return false;
    const column_idx = types.findColumn(table.schema.columns, request.column) orelse return false;
    const col = table.schema.columns[column_idx];
    if (col.nullable or !storage.segment_strings.eligible(col.type)) return false;
    if (request.filter != null and col.type != .varchar and col.type != .string) return false;

    const worker_allocator = try exec.memory.workerAllocatorOf(allocator);
    var state: Run = .{
        .allocator = worker_allocator,
        .table = table,
        .column_idx = column_idx,
        .segments = try worker_allocator.alloc(Segment, snapshot.segments.len),
    };
    var opened: usize = 0;
    defer {
        for (state.segments[0..opened]) |*s| s.deinit(worker_allocator, table);
        worker_allocator.free(state.segments);
    }
    var by_id = request.filter == null and idsOpen(table, column_idx);
    var table_bytes: u64 = 0;
    for (snapshot.segments, state.segments) |entry, *s| {
        s.* = .{ .handle = try table.acquireSegment(entry.segment_id) };
        opened += 1;
        if (!try qualifies(table, s, snapshot, column_idx)) return false;
        by_id = by_id and s.has_ids;
        table_bytes += @as(u64, s.shared_count) * 8 + s.handle.seg.info.row_groups.len * 12;
    }
    // The ids blocks load first, side by side with the row groups' counts.
    state.id_jobs = if (by_id) state.segments.len else 0;
    var jobs = state.id_jobs;
    for (state.segments) |*s| {
        s.first_job = jobs;
        jobs += s.handle.seg.info.row_groups.len;
    }
    if (exec.memory.accountantOf(allocator)) |accountant| {
        if (table_bytes > accountant.pool.statementLimit() / 8) return false;
    }
    for (state.segments) |*s| try s.allocTables(worker_allocator);

    parallel.forJobs(request.workers, jobs, &state, countJob);
    if (try state.outcome()) return false;
    const slack = setThresholds(state.segments, request, snapshotRows(snapshot));

    var reports = Reports.init(worker_allocator);
    defer reports.deinit();
    try collectReports(&state, &reports, request.workers);
    if (try state.outcome()) return false;

    var merge_arena = std.heap.ArenaAllocator.init(worker_allocator);
    defer merge_arena.deinit();
    const candidates = (try pickCandidates(merge_arena.allocator(), &state, &reports, request, slack)) orelse return false;
    if (candidates.len == 0) return true;

    const placing = if (by_id) placing: {
        try resolveById(&state, merge_arena.allocator(), candidates, request.workers);
        if (try state.outcome()) return false;
        const top = placingByCount(candidates, request.need);
        readValues(&state, &reports, top, request.workers);
        if (try state.outcome()) return false;
        break :placing top;
    } else placing: {
        try searchShared(&state, merge_arena.allocator(), candidates, request.workers);
        if (try state.outcome()) return false;
        try resolveLocals(&state, merge_arena.allocator(), candidates, request);
        if (try state.outcome()) return false;
        break :placing candidates;
    };

    std.sort.pdq(Candidate, placing, {}, Candidate.before);
    const keep = @min(request.need, placing.len);
    try rows.ensureUnusedCapacity(allocator, keep);
    for (placing[0..keep]) |c| {
        rows.appendAssumeCapacity(.{
            .key = exec.stringKeyDigest(c.bytes) & request.key_digest_mask,
            .count = c.count,
            .rowref = try state.rowOf(c.loc),
        });
    }
    return true;
}

fn snapshotRows(snapshot: exec.Scan.Snapshot) u64 {
    var n: u64 = 0;
    for (snapshot.segments) |entry| n += entry.row_count;
    return n;
}

/// Whether the column takes table ids: ids of a closed column don't name
/// values across segments.
fn idsOpen(table: *api.Table, column_idx: usize) bool {
    const ids = table.strings.readerState(column_idx) orelse return false;
    return !ids.closed;
}

/// Whether segment `s` keeps the column in a string table with entry stats
/// for every row group, its counts fit u32 and the snapshot holds no
/// tombstones in it; sets its shared count and whether it carries ids.
fn qualifies(table: *api.Table, s: *Segment, snapshot: exec.Scan.Snapshot, column_idx: usize) !bool {
    const info = s.handle.seg.info;
    if (info.row_count > std.math.maxInt(u32)) return false;
    const string_table = info.stringTable(column_idx) orelse return false;
    for (string_table.blocks) |parts| {
        if (!parts.isSplit() or parts.lengths.get(.stats) == 0) return false;
    }
    if (try table.acquireSegmentTombstones(s.handle, snapshot.tomb_epoch)) |tombs| {
        defer table.releaseSegmentTombstones(tombs);
        if (tombs.rows.len > 0) return false;
    }
    s.shared_count = string_table.shared_count;
    s.value_count = string_table.value_count;
    s.has_ids = string_table.table_ids.length != 0;
    return true;
}

const Segment = struct {
    handle: *storage.cache.SegmentHandles.Entry,
    shared_count: u32 = 0,
    value_count: u32 = 0,
    /// Per shared value, by rank: its rows in the segment, and one row
    /// group holding it, plus one (0 until one is seen).
    counts: []u32 = &.{},
    holders: []u32 = &.{},
    /// Per row group, its largest local value's rows.
    local_max: []u32 = &.{},
    /// Per row group, the segment ids its local values span, [lo, hi).
    local_ids: [][2]u32 = &.{},
    has_ids: bool = false,
    /// Each segment id's table id, when read by id.
    ids: ?storage.ReadSegment.TableIds = null,
    /// Per bucket b, the segment's local values of [2^b, 2^(b+1)) rows: a
    /// bound on its local reports before any is read.
    local_hist: [32]u64 = @splat(0),
    /// t_s: the segment reports every value it holds this many rows of.
    threshold: u64 = 1,
    /// The segment's largest local value's rows.
    local_bound: u64 = 0,
    first_job: usize = 0,
    /// The ranks the segment reports, ascending.
    reported: std.ArrayList(u32) = .empty,
    /// The candidates its shared table doesn't hold and that may still
    /// place: their count here is their local count, if any.
    pending: std.ArrayList(u32) = .empty,

    fn allocTables(self: *Segment, allocator: Allocator) !void {
        self.counts = try allocator.alloc(u32, self.shared_count);
        @memset(self.counts, 0);
        self.holders = try allocator.alloc(u32, self.shared_count);
        @memset(self.holders, 0);
        self.local_max = try allocator.alloc(u32, self.handle.seg.info.row_groups.len);
        @memset(self.local_max, 0);
        self.local_ids = try allocator.alloc([2]u32, self.handle.seg.info.row_groups.len);
        @memset(self.local_ids, .{ 0, 0 });
    }

    fn deinit(self: *Segment, allocator: Allocator, table: *api.Table) void {
        if (self.ids) |ids| ids.release();
        allocator.free(self.counts);
        allocator.free(self.holders);
        allocator.free(self.local_max);
        allocator.free(self.local_ids);
        self.reported.deinit(allocator);
        self.pending.deinit(allocator);
        table.releaseSegment(self.handle);
    }
};

const Loc = union(enum) {
    /// A row group of segment `seg` holds its shared value `rank`.
    shared: struct { seg: u32, rank: u32 },
    /// Row `at` holds its segment's value `id`.
    row: struct { at: i64, id: u32 },
};

const Candidate = struct {
    /// Empty when read by id, until the value may place (`readValues`).
    bytes: []const u8,
    /// Its table id, when read by id.
    id: u32,
    count: u64,
    /// Σ t_s - 1 over the segments that reported it.
    reported_slack: u64,
    /// The segments whose count of it `count` holds.
    known: std.DynamicBitSetUnmanaged,
    loc: Loc,
    /// Its count in the segments not yet known, at most.
    open_bound: u64 = 0,

    fn before(_: void, a: Candidate, b: Candidate) bool {
        if (a.count != b.count) return a.count > b.count;
        return std.mem.lessThan(u8, a.bytes, b.bytes);
    }

    fn moreRows(_: void, a: Candidate, b: Candidate) bool {
        return a.count > b.count;
    }
};

const Run = struct {
    allocator: Allocator,
    table: *api.Table,
    column_idx: usize,
    segments: []Segment,
    /// Count jobs that load a segment's table ids before the row groups'
    /// (one per segment when read by id, else none).
    id_jobs: usize = 0,
    failure: std.atomic.Value(bool) = .init(false),
    declined: std.atomic.Value(bool) = .init(false),
    err: anyerror = error.Unexpected,

    fn fail(self: *Run, err: anyerror) void {
        if (self.failure.cmpxchgStrong(false, true, .acq_rel, .monotonic) == null) self.err = err;
    }

    fn stopped(self: *const Run) bool {
        return self.failure.load(.acquire) or self.declined.load(.acquire);
    }

    /// The first job error, or whether a job declined.
    fn outcome(self: *const Run) !bool {
        if (self.failure.load(.acquire)) return self.err;
        return self.declined.load(.acquire);
    }

    fn cacheRef(self: *const Run) storage.cache.TableCache {
        return self.table.cacheRef();
    }

    fn byId(self: *const Run) bool {
        return self.id_jobs != 0;
    }

    /// The segment and row group of count job `j`.
    fn locate(self: *const Run, j: usize) struct { usize, usize } {
        var lo: usize = 0;
        var hi = self.segments.len;
        while (hi - lo > 1) {
            const mid = lo + (hi - lo) / 2;
            if (self.segments[mid].first_job <= j) lo = mid else hi = mid;
        }
        return .{ lo, j - self.segments[lo].first_job };
    }

    /// A packed row location (rowloc.zig) holding the value `loc` names.
    fn rowOf(self: *const Run, loc: Loc) !i64 {
        switch (loc) {
            .row => |r| return r.at,
            .shared => |sh| {
                const s = &self.segments[sh.seg];
                const holder = s.holders[sh.rank];
                if (holder == 0) return storage.format.Error.CorruptColumnBlockHeader;
                const rg = holder - 1;
                var borrowed = (try s.handle.seg.borrowStringEntries(self.allocator, rg, self.column_idx, self.cacheRef())) orelse
                    return storage.format.Error.CorruptColumnBlockHeader;
                defer borrowed.release(self.allocator, self.cacheRef());
                const e = borrowed.entries;
                for (0..e.ndv) |c| {
                    if (e.id(c) == sh.rank) return exec.rowloc.packSegment(sh.seg, rg, e.firstRow(c));
                }
                return storage.format.Error.CorruptColumnBlockHeader;
            },
        }
    }
};

fn countJob(state: *Run, _: usize, j: usize) void {
    if (state.stopped()) return;
    if (j < state.id_jobs) {
        loadIds(state, &state.segments[j]) catch |err| state.fail(err);
        return;
    }
    const si, const rg = state.locate(j);
    countRowGroup(state, &state.segments[si], rg) catch |err| state.fail(err);
}

/// Takes segment `s`'s table ids through the cache, held until the run
/// ends.
fn loadIds(state: *Run, s: *Segment) !void {
    const ids = (try s.handle.seg.cachedTableIds(state.column_idx, state.cacheRef())) orelse {
        state.declined.store(true, .release);
        return;
    };
    s.ids = ids;
    if (ids.ids.len != s.value_count) return storage.format.Error.CorruptColumnBlockHeader;
}

/// Adds row group `rg`'s shared values' rows to `s.counts` and notes its
/// largest local value's and the ids its local values span.
fn countRowGroup(state: *Run, s: *Segment, rg: usize) !void {
    var borrowed = (try s.handle.seg.borrowStringEntries(state.allocator, rg, state.column_idx, state.cacheRef())) orelse {
        state.declined.store(true, .release);
        return;
    };
    defer borrowed.release(state.allocator, state.cacheRef());
    const e = borrowed.entries;
    var local_max: u32 = 0;
    var local_lo: u32 = std.math.maxInt(u32);
    var local_hi: u32 = 0;
    var hist: [32]u32 = @splat(0);
    switch (e.stat_width) {
        inline 2, 4 => |w| {
            const T = @Int(.unsigned, @as(u16, w) * 8);
            for (0..e.ndv) |c| {
                const n: u32 = @as(u32, std.mem.readInt(T, e.entry_counts[c * w ..][0..w], .little)) + 1;
                const id = e.id(c);
                if (id >= s.shared_count) {
                    local_max = @max(local_max, n);
                    local_lo = @min(local_lo, id);
                    local_hi = @max(local_hi, id + 1);
                    hist[std.math.log2_int(u32, n)] += 1;
                    continue;
                }
                _ = @atomicRmw(u32, &s.counts[id], .Add, n, .monotonic);
                if (@atomicLoad(u32, &s.holders[id], .monotonic) == 0) @atomicStore(u32, &s.holders[id], @intCast(rg + 1), .monotonic);
            }
        },
        else => return storage.format.Error.CorruptColumnBlockHeader,
    }
    s.local_max[rg] = local_max;
    if (local_hi > 0) s.local_ids[rg] = .{ local_lo, local_hi };
    for (hist, &s.local_hist) |h, *total| {
        if (h != 0) _ = @atomicRmw(u64, total, .Add, h, .monotonic);
    }
}

/// Sets each segment's t_s and local bound, and returns the slack Σ t_s - 1.
fn setThresholds(segments: []Segment, request: Request, total_rows: u64) u64 {
    var best: [count_topk_sketch.MAX_NEED]u32 = undefined;
    var slack: u64 = 0;
    for (segments) |*s| {
        const kth = @max(
            count_topk_sketch.kthLargest(u32, s.counts, best[0..request.need]),
            count_topk_sketch.kthLargest(u32, s.local_max, best[0..request.need]),
        );
        // A floor above the need-th best count raises the segment's share
        // to the floor's, prorated over its rows.
        const rows = s.handle.seg.info.row_count;
        const floor_share: u64 = if (total_rows > 0) @intCast(@as(u128, request.floor) * rows / total_rows) else 0;
        s.threshold = 1 + @max(kth, floor_share) / SLACK_SHARE_DENOM;
        for (s.local_max) |n| s.local_bound = @max(s.local_bound, n);
        slack += s.threshold - 1;
    }
    return slack;
}

/// A segment's count of a value, named by its bytes, or by its table id
/// when read by id.
const Report = struct { seg: u32, bytes: []const u8 = "", id: u32 = 0, count: u64, loc: Loc };

const ReportJob = union(enum) {
    /// Segment `seg`'s reported ranks `reported[lo..hi]`, all on one page.
    page: struct { seg: u32, lo: usize, hi: usize },
    /// Segment `seg`'s row group `rg`, which holds a local value of t_s rows
    /// or more.
    locals: struct { seg: u32, rg: u32 },
};

const Reports = struct {
    allocator: Allocator,
    jobs: std.ArrayList(ReportJob) = .empty,
    arenas: [parallel.MAX_THREADS]std.heap.ArenaAllocator,
    lists: [parallel.MAX_THREADS]std.ArrayList(Report) = @splat(.empty),

    fn init(allocator: Allocator) Reports {
        var self: Reports = .{ .allocator = allocator, .arenas = undefined };
        for (&self.arenas) |*a| a.* = .init(allocator);
        return self;
    }

    fn deinit(self: *Reports) void {
        self.jobs.deinit(self.allocator);
        for (&self.arenas) |*a| a.deinit();
    }
};

const ReportRun = struct { state: *Run, reports: *Reports };

/// Every segment's reports into `reports`: its shared values of t_s rows or
/// more, read page by page (by id, from its ids), and its local values of
/// t_s rows or more, read from their row groups' blocks (by id, their entry
/// counts).
fn collectReports(state: *Run, reports: *Reports, workers: usize) !void {
    var reported: usize = 0;
    for (state.segments, 0..) |*s, si| {
        for (s.counts, 0..) |n, rank| {
            if (n >= s.threshold) try s.reported.append(state.allocator, @intCast(rank));
        }
        reported += s.reported.items.len;
        if (s.local_bound >= s.threshold) {
            for (s.local_hist[std.math.log2_int(u64, s.threshold)..]) |n| reported += n;
        }
        if (reported > MAX_REPORTS) {
            state.declined.store(true, .release);
            return;
        }
        if (s.ids) |ids| {
            for (s.reported.items) |rank| try reports.lists[0].append(reports.arenas[0].allocator(), .{
                .seg = @intCast(si),
                .id = ids.ids[rank],
                .count = s.counts[rank],
                .loc = .{ .shared = .{ .seg = @intCast(si), .rank = rank } },
            });
        }
        var lo: usize = 0;
        while (s.ids == null and lo < s.reported.items.len) {
            const page = s.reported.items[lo] / page_values;
            var hi = lo + 1;
            while (hi < s.reported.items.len and s.reported.items[hi] / page_values == page) hi += 1;
            try reports.jobs.append(state.allocator, .{ .page = .{ .seg = @intCast(si), .lo = lo, .hi = hi } });
            lo = hi;
        }
        if (s.local_bound < s.threshold) continue;
        for (s.local_max, 0..) |n, rg| {
            if (n >= s.threshold) try reports.jobs.append(state.allocator, .{ .locals = .{ .seg = @intCast(si), .rg = @intCast(rg) } });
        }
    }
    var ctx: ReportRun = .{ .state = state, .reports = reports };
    parallel.forJobs(workers, reports.jobs.items.len, &ctx, reportJob);
}

fn reportJob(ctx: *ReportRun, w: usize, j: usize) void {
    if (ctx.state.stopped()) return;
    const out = &ctx.reports.lists[w];
    const arena = ctx.reports.arenas[w].allocator();
    switch (ctx.reports.jobs.items[j]) {
        .page => |p| reportShared(ctx.state, arena, out, p.seg, p.lo, p.hi) catch |err| ctx.state.fail(err),
        .locals => |l| if (ctx.state.byId())
            reportLocalIds(ctx.state, arena, out, l.seg, l.rg) catch |err| ctx.state.fail(err)
        else
            reportLocals(ctx.state, arena, out, l.seg, l.rg) catch |err| ctx.state.fail(err),
    }
}

fn reportShared(state: *Run, arena: Allocator, out: *std.ArrayList(Report), seg: u32, lo: usize, hi: usize) !void {
    const s = &state.segments[seg];
    var shared = s.handle.seg.sharedStrings(state.allocator, state.column_idx, state.cacheRef()) orelse
        return storage.format.Error.CorruptColumnBlockHeader;
    defer shared.release();
    for (s.reported.items[lo..hi]) |rank| {
        try out.append(arena, .{
            .seg = seg,
            .bytes = try arena.dupe(u8, try shared.value(rank)),
            .count = s.counts[rank],
            .loc = .{ .shared = .{ .seg = seg, .rank = rank } },
        });
    }
}

fn reportLocals(state: *Run, arena: Allocator, out: *std.ArrayList(Report), seg: u32, rg: u32) !void {
    const s = &state.segments[seg];
    var block = try s.handle.seg.borrowStringBlock(state.allocator, rg, state.column_idx, state.cacheRef(), .no_codes);
    defer block.release(state.allocator, state.cacheRef());
    const db = try dictOf(s, rg, block);
    const entries = db.entryCounts();
    for (0..db.ndv) |c| {
        const id = entries.id(c);
        if (id < s.shared_count) continue;
        const n = entries.count(c);
        if (n < s.threshold) continue;
        try out.append(arena, .{
            .seg = seg,
            .bytes = try arena.dupe(u8, db.dictValue(@intCast(c))),
            .count = n,
            .loc = .{ .row = .{ .at = exec.rowloc.packSegment(seg, rg, entries.firstRow(c)), .id = id } },
        });
    }
}

/// `reportLocals` by table id, from the row group's entry counts.
fn reportLocalIds(state: *Run, arena: Allocator, out: *std.ArrayList(Report), seg: u32, rg: u32) !void {
    const s = &state.segments[seg];
    const ids = s.ids.?.ids;
    var borrowed = (try s.handle.seg.borrowStringEntries(state.allocator, rg, state.column_idx, state.cacheRef())) orelse
        return storage.format.Error.CorruptColumnBlockHeader;
    defer borrowed.release(state.allocator, state.cacheRef());
    const e = borrowed.entries;
    for (0..e.ndv) |c| {
        const id = e.id(c);
        if (id < s.shared_count) continue;
        const n = e.count(c);
        if (n < s.threshold) continue;
        if (id >= ids.len) return storage.format.Error.CorruptColumnBlockHeader;
        try out.append(arena, .{
            .seg = seg,
            .id = ids[id],
            .count = n,
            .loc = .{ .row = .{ .at = exec.rowloc.packSegment(seg, rg, e.firstRow(c)), .id = id } },
        });
    }
}

/// A row group's block of the column, read with its values' bytes, as a
/// dictionary with segment ids and entry stats.
fn dictOf(s: *const Segment, rg: usize, block: storage.ReadSegment.BorrowedBlock) !storage.segment_reader.DictBlock {
    if (block.encoding != .dict) return storage.format.Error.CorruptColumnBlockHeader;
    const db = storage.segment_reader.dictBlockOf(block.bytes, s.handle.seg.info.row_groups[rg].row_count);
    if (!db.hasSegmentIds() or !db.hasEntryStats()) return storage.format.Error.CorruptColumnBlockHeader;
    return db;
}

/// The reported values that may place, merged by bytes, or by table id when
/// read by id; null when an unreported value might place too.
fn pickCandidates(arena: Allocator, state: *Run, reports: *const Reports, request: Request, slack: u64) !?[]Candidate {
    return if (state.byId())
        pickMerged(true, arena, state, reports, request, slack)
    else
        pickMerged(false, arena, state, reports, request, slack);
}

fn pickMerged(comptime by_id: bool, arena: Allocator, state: *Run, reports: *const Reports, request: Request, slack: u64) !?[]Candidate {
    const Merged = if (by_id) std.AutoHashMapUnmanaged(u32, Candidate) else std.StringHashMapUnmanaged(Candidate);
    var merged: Merged = .empty;
    const m = state.segments.len;
    for (reports.lists) |list| for (list.items) |r| {
        const gop = try merged.getOrPut(arena, if (by_id) r.id else r.bytes);
        if (!gop.found_existing) gop.value_ptr.* = .{
            .bytes = r.bytes,
            .id = r.id,
            .count = 0,
            .reported_slack = 0,
            .known = try std.DynamicBitSetUnmanaged.initEmpty(arena, m),
            .loc = r.loc,
        };
        const c = gop.value_ptr;
        c.count += r.count;
        c.reported_slack += state.segments[r.seg].threshold - 1;
        c.known.set(r.seg);
    };
    if (!by_id) if (request.filter) |filter| try dropFailing(arena, state, &merged, filter);

    const counts = try arena.alloc(u64, merged.count());
    var i: usize = 0;
    var values = merged.valueIterator();
    while (values.next()) |c| : (i += 1) counts[i] = c.count;
    var best: [count_topk_sketch.MAX_NEED]u64 = undefined;
    const tau = @max(count_topk_sketch.kthLargest(u64, counts, best[0..request.need]), request.floor);
    if (slack > 0 and slack >= tau) return null;

    var picked: std.ArrayList(Candidate) = .empty;
    values = merged.valueIterator();
    while (values.next()) |c| {
        if (c.count + (slack - c.reported_slack) < tau) continue;
        if (picked.items.len == MAX_CANDIDATES) return null;
        try picked.append(arena, c.*);
    }
    return picked.items;
}

/// Removes from `merged` the values `filter` fails: their rows all fail
/// it, so they hold none of the filtered rows.
fn dropFailing(arena: Allocator, state: *const Run, merged: *std.StringHashMapUnmanaged(Candidate), filter: exec.PredicateExpr) !void {
    const n = merged.count();
    if (n == 0) return;
    const offsets = try arena.alloc(u32, n + 1);
    var bytes: std.ArrayList(u8) = .empty;
    offsets[0] = 0;
    var keys = merged.keyIterator();
    var i: usize = 0;
    while (keys.next()) |k| : (i += 1) {
        try bytes.appendSlice(arena, k.*);
        offsets[i + 1] = @intCast(bytes.items.len);
    }
    const values: storage.StringView = .{ .offsets = offsets, .bytes = bytes.items };
    const schema = state.table.schema.columns[state.column_idx..][0..1];
    const view: storage.ColumnView = .{ .data = switch (schema[0].type) {
        .varchar => .{ .varchar = values },
        .string => .{ .string = values },
        else => return error.UnsupportedOperatorForType,
    } };
    const pass = try arena.alloc(bool, n);
    try exec.predicate.evaluateExprGuided(arena, filter, schema, exec.Batch{ .schema = schema, .values = (&view)[0..1], .row_count = n }, pass, null);
    var failing: std.ArrayList([]const u8) = .empty;
    keys = merged.keyIterator();
    i = 0;
    while (keys.next()) |k| : (i += 1) {
        if (!pass[i]) try failing.append(arena, k.*);
    }
    for (failing.items) |k| _ = merged.remove(k);
}

const SearchRun = struct { state: *Run, candidates: []Candidate, found: []std.atomic.Value(u64) };

/// Adds to each candidate its count in each segment whose shared table
/// holds it but didn't report it, and marks the others' local bound open.
fn searchShared(state: *Run, arena: Allocator, candidates: []Candidate, workers: usize) !void {
    const found = try arena.alloc(std.atomic.Value(u64), candidates.len * state.segments.len);
    for (found) |*f| f.* = .init(std.math.maxInt(u64));
    var ctx: SearchRun = .{ .state = state, .candidates = candidates, .found = found };
    parallel.forJobs(workers, state.segments.len, &ctx, searchJob);
    if (state.stopped()) return;
    const m = state.segments.len;
    for (candidates, 0..) |*c, ci| {
        for (state.segments, 0..) |s, si| {
            if (c.known.isSet(si)) continue;
            const rank = found[ci * m + si].load(.monotonic);
            if (rank == std.math.maxInt(u64)) {
                c.open_bound += @min(s.threshold - 1, s.local_bound);
                continue;
            }
            c.count += s.counts[@intCast(rank)];
            c.known.set(si);
        }
    }
}

fn searchJob(ctx: *SearchRun, _: usize, si: usize) void {
    if (ctx.state.stopped()) return;
    searchSegment(ctx, si) catch |err| ctx.state.fail(err);
}

fn searchSegment(ctx: *SearchRun, si: usize) !void {
    const state = ctx.state;
    const m = state.segments.len;
    var shared = state.segments[si].handle.seg.sharedStrings(state.allocator, state.column_idx, state.cacheRef()) orelse
        return storage.format.Error.CorruptColumnBlockHeader;
    defer shared.release();
    for (ctx.candidates, 0..) |c, ci| {
        if (c.known.isSet(si)) continue;
        if (try shared.rankOf(c.bytes)) |rank| ctx.found[ci * m + si].store(rank, .monotonic);
    }
}

const LocalRun = struct { state: *Run, candidates: []Candidate, jobs: []const [2]u32, hits: []std.atomic.Value(u64) };

/// Adds to each candidate that may still place its local count in each
/// segment whose shared table doesn't hold it, read from the row groups
/// whose string bounds admit it.
fn resolveLocals(state: *Run, arena: Allocator, candidates: []Candidate, request: Request) !void {
    var best: [count_topk_sketch.MAX_NEED]u64 = undefined;
    const lows = try arena.alloc(u64, candidates.len);
    for (candidates, lows) |c, *l| l.* = c.count;
    const least_placing = count_topk_sketch.kthLargest(u64, lows, best[0..@min(request.need, candidates.len)]);

    var jobs: std.ArrayList([2]u32) = .empty;
    for (state.segments, 0..) |*s, si| {
        for (candidates, 0..) |c, ci| {
            if (c.known.isSet(si) or c.count + c.open_bound < least_placing) continue;
            try s.pending.append(state.allocator, @intCast(ci));
        }
        if (s.pending.items.len == 0) continue;
        for (0..s.handle.seg.info.row_groups.len) |rg| try jobs.append(arena, .{ @intCast(si), @intCast(rg) });
    }
    if (jobs.items.len == 0) return;
    const hits = try arena.alloc(std.atomic.Value(u64), candidates.len);
    for (hits) |*h| h.* = .init(0);
    var ctx: LocalRun = .{ .state = state, .candidates = candidates, .jobs = jobs.items, .hits = hits };
    parallel.forJobs(request.workers, jobs.items.len, &ctx, localJob);
    if (state.stopped()) return;
    for (candidates, hits) |*c, h| c.count += h.load(.monotonic);
}

fn localJob(ctx: *LocalRun, _: usize, j: usize) void {
    if (ctx.state.stopped()) return;
    const job = ctx.jobs[j];
    resolveRowGroup(ctx, job[0], job[1]) catch |err| ctx.state.fail(err);
}

fn resolveRowGroup(ctx: *LocalRun, seg: u32, rg: u32) !void {
    const state = ctx.state;
    const s = &state.segments[seg];
    const bounds = s.handle.seg.info.row_groups[rg].stats[state.column_idx];
    var admitted = false;
    for (s.pending.items) |ci| {
        const prefix = storage.format.encodeStringPrefix(ctx.candidates[ci].bytes);
        if (prefix >= bounds.min and prefix <= bounds.max) admitted = true;
    }
    if (!admitted) return;
    var block = try s.handle.seg.borrowStringBlock(state.allocator, rg, state.column_idx, state.cacheRef(), .no_codes);
    defer block.release(state.allocator, state.cacheRef());
    const db = try dictOf(s, rg, block);
    for (s.pending.items) |ci| {
        const bytes = ctx.candidates[ci].bytes;
        var lo: u32 = 0;
        var hi: u32 = db.ndv;
        while (lo < hi) {
            const mid = lo + (hi - lo) / 2;
            if (std.mem.lessThan(u8, db.dictValue(mid), bytes)) lo = mid + 1 else hi = mid;
        }
        if (lo == db.ndv or !std.mem.eql(u8, db.dictValue(lo), bytes)) continue;
        if (db.segmentId(lo) < s.shared_count) return storage.format.Error.CorruptColumnBlockHeader;
        _ = ctx.hits[ci].fetchAdd(db.entryCounts().count(lo), .monotonic);
    }
}

/// Ids one job of `resolveById`'s pass over a segment's ids takes.
const ID_CHUNK: usize = 1 << 18;

const IdChunk = struct { seg: u32, lo: usize, hi: usize };

const IdSearch = struct {
    state: *Run,
    chunks: []const IdChunk,
    /// The candidates' table ids plus one, open-addressed (0: empty).
    slots: []const u32,
    /// The candidate at each slot.
    slot_candidates: []const u32,
    /// Per candidate and segment: its segment id there plus one, 0 when the
    /// segment holds no such value.
    found: []u32,
};

/// A candidate local in a segment that didn't report it: segment `seg`'s
/// row group `rg` holds it as its own value `id`, `count` rows of it.
const LocalHit = struct {
    seg: u32,
    rg: u32,
    id: u32,
    candidate: u32,
    count: u32 = 0,

    fn before(_: void, a: LocalHit, b: LocalHit) bool {
        if (a.seg != b.seg) return a.seg < b.seg;
        if (a.rg != b.rg) return a.rg < b.rg;
        return a.id < b.id;
    }
};

const LocalIdRun = struct {
    state: *Run,
    hits: []LocalHit,
    /// Where each row group's hits start in `hits`, then `hits.len`.
    groups: []const usize,
};

/// Makes every candidate's count exact by adding its count in each segment
/// that didn't report it. One pass over such a segment's ids finds each
/// candidate's segment id there, if any: a shared value's count is summed
/// already, and a local value's is its row group's entry count. A segment
/// whose ids lack the candidate holds none of its rows.
fn resolveById(state: *Run, arena: Allocator, candidates: []Candidate, workers: usize) !void {
    const m = state.segments.len;
    var chunks: std.ArrayList(IdChunk) = .empty;
    for (state.segments, 0..) |*s, si| {
        const open = for (candidates) |c| {
            if (!c.known.isSet(si)) break true;
        } else false;
        if (!open) continue;
        var lo: usize = 0;
        while (lo < s.value_count) : (lo += ID_CHUNK) {
            try chunks.append(arena, .{ .seg = @intCast(si), .lo = lo, .hi = @min(lo + ID_CHUNK, s.value_count) });
        }
    }
    if (chunks.items.len == 0) return;

    const n_slots = try std.math.ceilPowerOfTwo(usize, @max(candidates.len * 4, 64));
    const slots = try arena.alloc(u32, n_slots);
    @memset(slots, 0);
    const slot_candidates = try arena.alloc(u32, n_slots);
    for (candidates, 0..) |c, ci| {
        var slot = idSlot(c.id, n_slots);
        while (slots[slot] != 0) slot = (slot + 1) & (n_slots - 1);
        slots[slot] = c.id + 1;
        slot_candidates[slot] = @intCast(ci);
    }
    const found = try arena.alloc(u32, candidates.len * m);
    @memset(found, 0);
    var search: IdSearch = .{ .state = state, .chunks = chunks.items, .slots = slots, .slot_candidates = slot_candidates, .found = found };
    parallel.forJobs(workers, chunks.items.len, &search, searchIdChunk);
    if (state.stopped()) return;

    var hits: std.ArrayList(LocalHit) = .empty;
    for (candidates, 0..) |*c, ci| for (state.segments, 0..) |*s, si| {
        if (c.known.isSet(si)) continue;
        c.known.set(si);
        const at = found[ci * m + si];
        if (at == 0) continue;
        const id = at - 1;
        if (id < s.shared_count) {
            c.count += s.counts[id];
            continue;
        }
        const holder = for (s.local_ids, 0..) |span, rg| {
            if (id >= span[0] and id < span[1]) break rg;
        } else return storage.format.Error.CorruptColumnBlockHeader;
        try hits.append(arena, .{ .seg = @intCast(si), .rg = @intCast(holder), .id = id, .candidate = @intCast(ci) });
    };
    if (hits.items.len == 0) return;

    std.sort.pdq(LocalHit, hits.items, {}, LocalHit.before);
    var groups: std.ArrayList(usize) = .empty;
    for (hits.items, 0..) |h, i| {
        if (i == 0 or h.seg != hits.items[i - 1].seg or h.rg != hits.items[i - 1].rg) try groups.append(arena, i);
    }
    try groups.append(arena, hits.items.len);
    var locals: LocalIdRun = .{ .state = state, .hits = hits.items, .groups = groups.items };
    parallel.forJobs(workers, groups.items.len - 1, &locals, localIdJob);
    if (state.stopped()) return;
    for (hits.items) |h| candidates[h.candidate].count += h.count;
}

fn idSlot(id: u32, n_slots: usize) usize {
    return @intCast(((@as(u64, id) *% 0x9E3779B97F4A7C15) >> 32) & (n_slots - 1));
}

fn searchIdChunk(ctx: *IdSearch, _: usize, j: usize) void {
    if (ctx.state.stopped()) return;
    const chunk = ctx.chunks[j];
    const ids = ctx.state.segments[chunk.seg].ids.?.ids;
    const m = ctx.state.segments.len;
    const mask = ctx.slots.len - 1;
    for (chunk.lo..chunk.hi) |i| {
        const want = ids[i] +% 1;
        var slot = idSlot(ids[i], ctx.slots.len);
        while (ctx.slots[slot] != 0) : (slot = (slot + 1) & mask) {
            if (ctx.slots[slot] != want) continue;
            ctx.found[@as(usize, ctx.slot_candidates[slot]) * m + chunk.seg] = @intCast(i + 1);
            break;
        }
    }
}

fn localIdJob(ctx: *LocalIdRun, _: usize, g: usize) void {
    if (ctx.state.stopped()) return;
    countLocalHits(ctx.state, ctx.hits[ctx.groups[g]..ctx.groups[g + 1]]) catch |err| ctx.state.fail(err);
}

/// Sets the count of each of `hits`, one row group's, ascending by id.
fn countLocalHits(state: *Run, hits: []LocalHit) !void {
    const s = &state.segments[hits[0].seg];
    var borrowed = (try s.handle.seg.borrowStringEntries(state.allocator, hits[0].rg, state.column_idx, state.cacheRef())) orelse
        return storage.format.Error.CorruptColumnBlockHeader;
    defer borrowed.release(state.allocator, state.cacheRef());
    const e = borrowed.entries;
    var counted: usize = 0;
    for (0..e.ndv) |c| {
        const id = e.id(c);
        if (id < hits[0].id or id > hits[hits.len - 1].id) continue;
        var lo: usize = 0;
        var hi = hits.len;
        while (lo < hi) {
            const mid = lo + (hi - lo) / 2;
            if (hits[mid].id < id) lo = mid + 1 else hi = mid;
        }
        if (lo == hits.len or hits[lo].id != id) continue;
        hits[lo].count = e.count(c);
        counted += 1;
    }
    if (counted != hits.len) return storage.format.Error.CorruptColumnBlockHeader;
}

/// The candidates that place, their counts exact, moved to the front: the
/// `need` with the most rows and any tied with the last of them, which the
/// values' bytes order.
fn placingByCount(candidates: []Candidate, need: usize) []Candidate {
    std.sort.pdq(Candidate, candidates, {}, Candidate.moreRows);
    const keep = @min(need, candidates.len);
    if (keep == 0) return candidates[0..0];
    const cut = candidates[keep - 1].count;
    var end = keep;
    while (end < candidates.len and candidates[end].count == cut) end += 1;
    return candidates[0..end];
}

const ValueRun = struct { state: *Run, reports: *Reports, candidates: []Candidate };

/// Reads each of `candidates`' bytes, one job each, into the reports'
/// arenas.
fn readValues(state: *Run, reports: *Reports, candidates: []Candidate, workers: usize) void {
    var ctx: ValueRun = .{ .state = state, .reports = reports, .candidates = candidates };
    parallel.forJobs(workers, candidates.len, &ctx, valueJob);
}

fn valueJob(ctx: *ValueRun, w: usize, j: usize) void {
    if (ctx.state.stopped()) return;
    readValue(ctx.state, ctx.reports.arenas[w].allocator(), &ctx.candidates[j]) catch |err| ctx.state.fail(err);
}

fn readValue(state: *Run, arena: Allocator, c: *Candidate) !void {
    switch (c.loc) {
        .shared => |sh| {
            var shared = state.segments[sh.seg].handle.seg.sharedStrings(state.allocator, state.column_idx, state.cacheRef()) orelse
                return storage.format.Error.CorruptColumnBlockHeader;
            defer shared.release();
            c.bytes = try arena.dupe(u8, try shared.value(sh.rank));
        },
        .row => |r| {
            const at = exec.rowloc.unpack(r.at).segment;
            var values = state.segments[at.seg_idx].handle.seg.entryValues(state.allocator, at.rg_idx, state.column_idx, state.cacheRef()) orelse
                return storage.format.Error.CorruptColumnBlockHeader;
            defer values.release();
            c.bytes = try arena.dupe(u8, try values.value(r.id));
        },
    }
}
