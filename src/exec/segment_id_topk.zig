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
    var jobs: usize = 0;
    var table_bytes: u64 = 0;
    for (snapshot.segments, state.segments) |entry, *s| {
        s.* = .{ .handle = try table.acquireSegment(entry.segment_id) };
        opened += 1;
        if (!try qualifies(table, s, snapshot, column_idx)) return false;
        s.first_job = jobs;
        jobs += s.handle.seg.info.row_groups.len;
        table_bytes += @as(u64, s.shared_count) * 8 + s.handle.seg.info.row_groups.len * 4;
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

    try searchShared(&state, merge_arena.allocator(), candidates, request.workers);
    if (try state.outcome()) return false;
    try resolveLocals(&state, merge_arena.allocator(), candidates, request);
    if (try state.outcome()) return false;

    std.sort.pdq(Candidate, candidates, {}, Candidate.before);
    const keep = @min(request.need, candidates.len);
    try rows.ensureUnusedCapacity(allocator, keep);
    for (candidates[0..keep]) |c| {
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

/// Whether segment `s` keeps the column in a string table with entry stats
/// for every row group, its counts fit u32 and the snapshot holds no
/// tombstones in it; sets its shared count.
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
    return true;
}

const Segment = struct {
    handle: *storage.cache.SegmentHandles.Entry,
    shared_count: u32 = 0,
    /// Per shared value, by rank: its rows in the segment, and one row
    /// group holding it, plus one (0 until one is seen).
    counts: []u32 = &.{},
    holders: []u32 = &.{},
    /// Per row group, its largest local value's rows.
    local_max: []u32 = &.{},
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
    }

    fn deinit(self: *Segment, allocator: Allocator, table: *api.Table) void {
        allocator.free(self.counts);
        allocator.free(self.holders);
        allocator.free(self.local_max);
        self.reported.deinit(allocator);
        self.pending.deinit(allocator);
        table.releaseSegment(self.handle);
    }
};

const Loc = union(enum) {
    /// A row group of segment `seg` holds its shared value `rank`.
    shared: struct { seg: u32, rank: u32 },
    row: i64,
};

const Candidate = struct {
    bytes: []const u8,
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
};

const Run = struct {
    allocator: Allocator,
    table: *api.Table,
    column_idx: usize,
    segments: []Segment,
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
            .row => |r| return r,
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
    const si, const rg = state.locate(j);
    countRowGroup(state, &state.segments[si], rg) catch |err| state.fail(err);
}

/// Adds row group `rg`'s shared values' rows to `s.counts` and notes its
/// largest local value's.
fn countRowGroup(state: *Run, s: *Segment, rg: usize) !void {
    var borrowed = (try s.handle.seg.borrowStringEntries(state.allocator, rg, state.column_idx, state.cacheRef())) orelse {
        state.declined.store(true, .release);
        return;
    };
    defer borrowed.release(state.allocator, state.cacheRef());
    const e = borrowed.entries;
    var local_max: u32 = 0;
    var hist: [32]u32 = @splat(0);
    switch (e.stat_width) {
        inline 2, 4 => |w| {
            const T = @Int(.unsigned, @as(u16, w) * 8);
            for (0..e.ndv) |c| {
                const n: u32 = @as(u32, std.mem.readInt(T, e.entry_counts[c * w ..][0..w], .little)) + 1;
                const id = e.id(c);
                if (id >= s.shared_count) {
                    local_max = @max(local_max, n);
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

const Report = struct { seg: u32, bytes: []const u8, count: u64, loc: Loc };

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
/// more, read page by page, and its local values of t_s rows or more, read
/// from their row groups' blocks.
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
        var lo: usize = 0;
        while (lo < s.reported.items.len) {
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
        .locals => |l| reportLocals(ctx.state, arena, out, l.seg, l.rg) catch |err| ctx.state.fail(err),
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
            .loc = .{ .row = exec.rowloc.packSegment(seg, rg, entries.firstRow(c)) },
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

/// The reported values that may place, merged by bytes; null when an
/// unreported value might place too.
fn pickCandidates(arena: Allocator, state: *Run, reports: *const Reports, request: Request, slack: u64) !?[]Candidate {
    var merged: std.StringHashMapUnmanaged(Candidate) = .empty;
    const m = state.segments.len;
    for (reports.lists) |list| for (list.items) |r| {
        const gop = try merged.getOrPut(arena, r.bytes);
        if (!gop.found_existing) gop.value_ptr.* = .{
            .bytes = r.bytes,
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
    if (request.filter) |filter| try dropFailing(arena, state, &merged, filter);

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
