//! Range-separability harness. Opens the clickbench DB embedded at max_dop=1
//! (so every query is a single-worker serial stack), then runs the SAME GROUP
//! BY-on-leading-order-key query split into 12 balanced CounterID ranges, both
//! sequentially and concurrently on 12 raw OS threads. Each shard prunes to
//! ~1/12 of the table via zonemap on the leading order key (CounterID). Tests
//! whether 12 independent DOP-1 shards scale near-linearly. Uses Io.Threaded.

const std = @import("std");
const thindb = @import("thindb");

extern "c" fn getenv(name: [*:0]const u8) ?[*:0]const u8;

const PROCESS_MEMORY_COUNTERS = extern struct {
    cb: u32 = @sizeOf(PROCESS_MEMORY_COUNTERS),
    PageFaultCount: u32 = 0,
    PeakWorkingSetSize: usize = 0,
    WorkingSetSize: usize = 0,
    QuotaPeakPagedPoolUsage: usize = 0,
    QuotaPagedPoolUsage: usize = 0,
    QuotaPeakNonPagedPoolUsage: usize = 0,
    QuotaNonPagedPoolUsage: usize = 0,
    PagefileUsage: usize = 0,
    PeakPagefileUsage: usize = 0,
};
extern "kernel32" fn K32GetProcessMemoryInfo(process: ?*anyopaque, counters: *PROCESS_MEMORY_COUNTERS, cb: u32) callconv(.winapi) c_int;
extern "kernel32" fn GetCurrentProcess() callconv(.winapi) ?*anyopaque;

const FILETIME = extern struct { low: u32 = 0, high: u32 = 0 };
extern "kernel32" fn GetProcessTimes(process: ?*anyopaque, creation: *FILETIME, exit: *FILETIME, kernel: *FILETIME, user: *FILETIME) callconv(.winapi) c_int;
fn cpuMicros() struct { user: u64, kernel: u64 } {
    var c: FILETIME = .{};
    var e: FILETIME = .{};
    var k: FILETIME = .{};
    var u: FILETIME = .{};
    _ = GetProcessTimes(GetCurrentProcess(), &c, &e, &k, &u);
    // FILETIME is 100ns ticks → microseconds = /10.
    const uu = (@as(u64, u.high) << 32 | u.low) / 10;
    const kk = (@as(u64, k.high) << 32 | k.low) / 10;
    return .{ .user = uu, .kernel = kk };
}
fn pageFaults() u32 {
    var pmc = PROCESS_MEMORY_COUNTERS{};
    _ = K32GetProcessMemoryInfo(GetCurrentProcess(), &pmc, @sizeOf(PROCESS_MEMORY_COUNTERS));
    return pmc.PageFaultCount;
}

// Shard count (set at comptime for the experiment): 12 = one worker each at
// SEP_DOP=1; 6 = two workers each at SEP_DOP=2 (pipeline scan vs group stage).
// Ranges are always derived from the byte-balance sweep, so no static cut table.
const N: usize = 12;

// Int-only variant: no URL/length() → no FSST string decode, no per-block string
// allocs. Isolates whether the concurrent serialization is in the string-decode
// path. WHERE CounterID >= 0 is always true (keeps buildSql's `AND` append valid).
// SEP_SHAPE=group (default): GROUP BY CounterID. SEP_SHAPE=global: no grouping
// (global SUM) to bisect whether the serializer is in the scan or the grouped
// aggregate operator.
const BASE_GROUP = "SELECT CounterID, COUNT(*) AS c FROM hits WHERE CounterID >= 0";
const TAIL_GROUP = "GROUP BY CounterID HAVING COUNT(*) > 100000 ORDER BY c DESC LIMIT 25";
const BASE_GLOBAL = "SELECT SUM(CounterID) AS c FROM hits WHERE CounterID >= 0";
const TAIL_GLOBAL = "";
// SEP_SHAPE=heavy: group by the leading order key but with AVG(length(URL)) —
// forces FSST string decode + per-row length() so the scan/compute work dwarfs
// the per-query fixed overhead. Tests whether heavier work lets independent
// shards scale closer to the cooperative silo.
const BASE_HEAVY = "SELECT CounterID, AVG(length(URL)) AS l, COUNT(*) AS c FROM hits WHERE URL <> ''";
const TAIL_HEAVY = "GROUP BY CounterID HAVING COUNT(*) > 100000 ORDER BY l DESC LIMIT 25";
var BASE: []const u8 = BASE_GROUP;
var TAIL: []const u8 = TAIL_GROUP;

const Shard = struct {
    db: *thindb.Database,
    gpa: std.mem.Allocator,
    lo: ?i64,
    hi: ?i64,
    lo_date: ?[]const u8 = null,
    hi_date: ?[]const u8 = null,
    ms: f64 = 0,
    compile_ms: f64 = 0,
    drain_ms: f64 = 0,
    rows: usize = 0,
    err: ?anyerror = null,
};

fn buildSql(a: std.mem.Allocator, lo: ?i64, hi: ?i64, lo_date: ?[]const u8, hi_date: ?[]const u8) ![]u8 {
    var buf: std.ArrayListUnmanaged(u8) = .empty;
    try buf.appendSlice(a, BASE);
    // Lower bound. With a composite date, keep CounterID>=lo as a top-level
    // (prunable) conjunct and trim the partial boundary CounterID by date.
    if (lo) |l| {
        if (lo_date) |d| {
            try buf.print(a, " AND CounterID >= {d} AND NOT (CounterID = {d} AND EventDate < '{s}')", .{ l, l, d });
        } else {
            try buf.print(a, " AND CounterID >= {d}", .{l});
        }
    }
    if (hi) |h| {
        if (hi_date) |d| {
            try buf.print(a, " AND CounterID <= {d} AND NOT (CounterID = {d} AND EventDate >= '{s}')", .{ h, h, d });
        } else {
            try buf.print(a, " AND CounterID < {d}", .{h});
        }
    }
    try buf.print(a, " {s}", .{TAIL});
    return buf.items;
}

fn runShard(sh: *Shard) void {
    runShardInner(sh) catch |e| {
        sh.err = e;
    };
}

fn runShardInner(sh: *Shard) !void {
    var arena = std.heap.ArenaAllocator.init(sh.gpa);
    defer arena.deinit();
    const a = arena.allocator();
    const sql = try buildSql(a, sh.lo, sh.hi, sh.lo_date, sh.hi_date);
    const op = try thindb.sql.parse(a, sql);
    const t0 = std.Io.Timestamp.now(sh.db.io, .real).toMicroseconds();
    var compiled = try thindb.net.compileWithSession(a, sh.db, .{ .current_db = "clickbench_fsst", .current_schema = "public" }, op);
    defer compiled.deinit();
    const tc = std.Io.Timestamp.now(sh.db.io, .real).toMicroseconds();
    var n: usize = 0;
    while (try compiled.next()) |batch| n += batch.row_count;
    const t1 = std.Io.Timestamp.now(sh.db.io, .real).toMicroseconds();
    sh.compile_ms = @as(f64, @floatFromInt(tc - t0)) / 1000.0;
    sh.drain_ms = @as(f64, @floatFromInt(t1 - tc)) / 1000.0;
    sh.ms = @as(f64, @floatFromInt(t1 - t0)) / 1000.0;
    sh.rows = n;
}

// Sweep per-CounterID work weight = SUM(length(URL)) and compute N-1 cut points
// that split the order key into ranges of ~equal total URL bytes (the heavy
// query's actual work) instead of ~equal rows. In production this weight would
// come from per-row-group footer SMA byte-sums (cheap, no scan); here we run the
// GROUP BY once (one-time cost, reused across all shard runs).
fn sweepBalancedCuts(db: *thindb.Database, gpa: std.mem.Allocator, out_cuts: *[N - 1]i64, weight_sql: []const u8) !void {
    var arena = std.heap.ArenaAllocator.init(gpa);
    defer arena.deinit();
    const a = arena.allocator();
    const sql = try std.fmt.allocPrint(a, "SELECT CounterID, {s} AS w FROM hits GROUP BY CounterID ORDER BY CounterID", .{weight_sql});
    const op = try thindb.sql.parse(a, sql);
    var compiled = try thindb.net.compileWithSession(a, db, .{ .current_db = "clickbench_fsst", .current_schema = "public" }, op);
    defer compiled.deinit();
    var cids: std.ArrayListUnmanaged(i64) = .empty;
    var ws: std.ArrayListUnmanaged(i64) = .empty;
    var total: i128 = 0;
    while (try compiled.next()) |batch| {
        const cv_cid = batch.columnView("CounterID") orelse return error.NoCounterIDCol;
        const cv_w = batch.columnView("w") orelse return error.NoWeightCol;
        for (0..batch.row_count) |r| {
            const cid: i64 = switch (cv_cid.data) {
                .int => |s| s[r],
                .bigint => |s| s[r],
                else => 0,
            };
            const w: i64 = switch (cv_w.data) {
                .bigint => |s| s[r],
                .int => |s| s[r],
                .double => |s| @intFromFloat(s[r]),
                else => 0,
            };
            try cids.append(a, cid);
            try ws.append(a, w);
            total += w;
        }
    }
    const n_cuts = N - 1;
    var acc: i128 = 0;
    var k: usize = 0;
    for (cids.items, ws.items) |cid, w| {
        acc += w;
        // Close a shard each time the running weight crosses the next 1/N target.
        while (k < n_cuts and acc * @as(i128, N) >= total * @as(i128, @intCast(k + 1))) {
            out_cuts[k] = cid;
            k += 1;
        }
    }
    while (k < n_cuts) : (k += 1) out_cuts[k] = if (cids.items.len > 0) cids.items[cids.items.len - 1] else 0;
}

fn timeSqlWarm(db: *thindb.Database, gpa: std.mem.Allocator, sql: []const u8, passes: usize) !f64 {
    var best: f64 = std.math.floatMax(f64);
    var p: usize = 0;
    while (p < passes) : (p += 1) {
        var arena = std.heap.ArenaAllocator.init(gpa);
        defer arena.deinit();
        const a = arena.allocator();
        const op = try thindb.sql.parse(a, sql);
        const t0 = std.Io.Timestamp.now(db.io, .real).toMicroseconds();
        var compiled = try thindb.net.compileWithSession(a, db, .{ .current_db = "clickbench_fsst", .current_schema = "public" }, op);
        defer compiled.deinit();
        while (try compiled.next()) |batch| std.mem.doNotOptimizeAway(batch.row_count);
        const t1 = std.Io.Timestamp.now(db.io, .real).toMicroseconds();
        const ms = @as(f64, @floatFromInt(t1 - t0)) / 1000.0;
        if (ms < best) best = ms;
    }
    return best;
}

fn profileStages(db: *thindb.Database, gpa: std.mem.Allocator, label: []const u8, lo: i64, hi: i64) !void {
    const passes: usize = 3;
    // Cumulative stages — each adds exactly one unit of work over the previous.
    const s1 = try std.fmt.allocPrint(gpa, "SELECT COUNT(*) FROM hits WHERE CounterID >= {d} AND CounterID < {d}", .{ lo, hi });
    defer gpa.free(s1);
    const s2 = try std.fmt.allocPrint(gpa, "SELECT COUNT(*) FROM hits WHERE URL <> '' AND CounterID >= {d} AND CounterID < {d}", .{ lo, hi });
    defer gpa.free(s2);
    const s3 = try std.fmt.allocPrint(gpa, "SELECT SUM(length(URL)) FROM hits WHERE URL <> '' AND CounterID >= {d} AND CounterID < {d}", .{ lo, hi });
    defer gpa.free(s3);
    const s4 = try std.fmt.allocPrint(gpa, "SELECT CounterID, AVG(length(URL)) AS l, COUNT(*) AS c FROM hits WHERE URL <> '' AND CounterID >= {d} AND CounterID < {d} GROUP BY CounterID HAVING COUNT(*) > 100000 ORDER BY l DESC LIMIT 25", .{ lo, hi });
    defer gpa.free(s4);

    const t1 = try timeSqlWarm(db, gpa, s1, passes);
    const t2 = try timeSqlWarm(db, gpa, s2, passes);
    const t3 = try timeSqlWarm(db, gpa, s3, passes);
    const t4 = try timeSqlWarm(db, gpa, s4, passes);

    std.debug.print("\n=== PROFILE {s}: CounterID [{d} .. {d}) (best of {d} warm passes) ===\n", .{ label, lo, hi, passes });
    std.debug.print("  S1 CID-scan + range-filter (no URL)        : {d:7.1} ms\n", .{t1});
    std.debug.print("  S2 + URL FSST-decode + (URL<>'') filter    : {d:7.1} ms   (delta {d:7.1})\n", .{ t2, t2 - t1 });
    std.debug.print("  S3 + length(URL) + global SUM (no group)   : {d:7.1} ms   (delta {d:7.1})\n", .{ t3, t3 - t2 });
    std.debug.print("  S4 + GROUP BY CounterID + HAVING/ORDER/LIM : {d:7.1} ms   (delta {d:7.1})\n", .{ t4, t4 - t3 });
    std.debug.print("  --- attribution ---\n", .{});
    std.debug.print("    scan+rangefilter : {d:7.1} ms\n", .{t1});
    std.debug.print("    URL decode+filter: {d:7.1} ms\n", .{t2 - t1});
    std.debug.print("    length+aggregate : {d:7.1} ms\n", .{t3 - t2});
    std.debug.print("    grouping         : {d:7.1} ms\n", .{t4 - t3});
}

pub fn main(init: std.process.Init) !u8 {
    const gpa = init.gpa;
    var threaded: std.Io.Threaded = .init(gpa, .{});
    defer threaded.deinit();
    const io = threaded.io();

    var data_root = try std.Io.Dir.cwd().createDirPathOpen(io, ".clickbench-db", .{});
    defer data_root.close(io);
    // max_dop from env SEP_DOP (default 1). With SEP_DOP=12 the "full" line is
    // the silo running in THIS process (apples-to-apples vs the shards).
    var dop: usize = 1;
    if (getenv("SEP_DOP")) |p| dop = std.fmt.parseInt(usize, std.mem.span(p), 10) catch 1;
    if (getenv("SEP_SHAPE")) |p| {
        const s = std.mem.span(p);
        if (std.mem.eql(u8, s, "global")) {
            BASE = BASE_GLOBAL;
            TAIL = TAIL_GLOBAL;
        } else if (std.mem.eql(u8, s, "heavy")) {
            BASE = BASE_HEAVY;
            TAIL = TAIL_HEAVY;
        }
    }
    const catalog = try thindb.Catalog.open(gpa, io, data_root, .{ .max_dop = dop, .memory_budget = 0, .query_memory_budget = 0 });
    defer catalog.close();
    const db = catalog.database("clickbench_fsst") orelse {
        std.debug.print("clickbench_fsst not found\n", .{});
        return 1;
    };

    if (getenv("SEP_PROFILE")) |_| {
        // Warm the cache with one full pass, then profile fast vs slow shard.
        _ = try timeSqlWarm(db, gpa, "SELECT CounterID, AVG(length(URL)) AS l, COUNT(*) AS c FROM hits WHERE URL <> '' GROUP BY CounterID HAVING COUNT(*) > 100000 ORDER BY l DESC LIMIT 25", 1);
        try profileStages(db, gpa, "FAST shard 4", 99062, 109363);
        try profileStages(db, gpa, "SLOW shard 10 (whale 233773)", 230962, 245438);
        return 0;
    }

    var cuts: [N - 1]i64 = undefined;
    var date_cuts: [N - 1]?[]const u8 = .{null} ** (N - 1);
    {
        // SEP_BALANCE=rows (default): balance ranges by source-row count
        // (COUNT(*) per CounterID). =bytes: by SUM(length(URL)) decode weight.
        var weight_sql: []const u8 = "COUNT(*)";
        var mode: []const u8 = "row-balanced";
        var use_optimal = false;
        var composite: ?[]const u8 = null;
        if (getenv("SEP_BALANCE")) |p| {
            const s = std.mem.span(p);
            if (std.mem.eql(u8, s, "bytes")) {
                weight_sql = "SUM(length(URL))";
                mode = "byte-balanced";
            } else if (std.mem.eql(u8, s, "optimal")) {
                use_optimal = true;
                mode = "optimal-minmax (precomputed)";
            } else if (std.mem.eql(u8, s, "composite_rows")) {
                composite = "rows";
                mode = "composite(CounterID,EventDate) row-balanced";
            } else if (std.mem.eql(u8, s, "composite_bytes")) {
                composite = "bytes";
                mode = "composite(CounterID,EventDate) byte-balanced";
            }
        }
        const sw0 = std.Io.Timestamp.now(db.io, .real).toMicroseconds();
        if (composite) |cw| {
            // (CounterID, EventDate) min-max partition from bench/clickbench/
            // _balance_cid_date.py — boundaries can fall inside a CounterID, so the
            // whale (233773) splits at a date boundary. Splits groups => wrong
            // values (intended: timing-only demonstration).
            if (std.mem.eql(u8, cw, "rows")) {
                cuts = .{ 3922, 5521, 46626, 99062, 117917, 130295, 173277, 199550, 220992, 233773, 245620 };
                date_cuts = .{ "2013-07-03", "2013-07-02", "2013-07-02", "2013-07-21", "2013-07-15", "2013-07-21", "2013-07-28", "2013-07-20", "2013-07-21", "2013-07-09", "2013-07-09" };
            } else {
                cuts = .{ 3922, 65847, 122612, 135316, 199550, 225510, 233773, 233797, 256004, 262115, 262115 };
                date_cuts = .{ "2013-07-31", "2013-07-28", "2013-07-21", "2013-07-03", "2013-07-21", "2013-07-21", "2013-07-09", "2013-07-05", "2013-07-06", "2999-01-01", "2999-01-01" };
            }
        } else if (use_optimal) {
            // min-max contiguous partition of source rows (URL<>'') into 12 blocks,
            // computed offline by bench/clickbench/_balance_cids.py — peels the
            // 8.5% whale (CounterID 233773) onto its own block.
            const OPT = [_]i64{ 3922, 5521, 46626, 99062, 109363, 128858, 159935, 190994, 204041, 230962, 245438 };
            cuts = OPT;
        } else {
            try sweepBalancedCuts(db, gpa, &cuts, weight_sql);
        }
        const sw1 = std.Io.Timestamp.now(db.io, .real).toMicroseconds();
        std.debug.print("{s} sweep ({d:.0} ms one-time) {d} ranges, cuts:", .{ mode, @as(f64, @floatFromInt(sw1 - sw0)) / 1000.0, N });
        for (cuts) |c| std.debug.print(" {d}", .{c});
        std.debug.print("\n", .{});
    }
    var los: [N]?i64 = undefined;
    var his: [N]?i64 = undefined;
    var lo_dates: [N]?[]const u8 = undefined;
    var hi_dates: [N]?[]const u8 = undefined;
    for (0..N) |i| {
        los[i] = if (i == 0) null else cuts[i - 1];
        his[i] = if (i == N - 1) null else cuts[i];
        lo_dates[i] = if (i == 0) null else date_cuts[i - 1];
        hi_dates[i] = if (i == N - 1) null else date_cuts[i];
    }

    // (probe 0) pure-CPU parallel sanity: 12 raw threads each spin the same
    // fixed arithmetic. If wall ~ single-thread time, raw-thread fan-out works
    // in this binary (rules out a stray affinity pin / single-thread artifact).
    {
        const Spin = struct {
            sink: u64 = 0,
            fn run(s: *@This()) void {
                var acc: u64 = @intFromPtr(s) | 1;
                var i: u64 = 0;
                // Nonlinear recurrence (acc depends on acc|1) so LLVM can't
                // close-form it away; volatile store forces the work to happen.
                while (i < 300_000_000) : (i += 1) acc = acc *% (acc | 3) +% 1442695040888963407;
                @as(*volatile u64, &s.sink).* = acc;
            }
        };
        var one = Spin{};
        const s0 = std.Io.Timestamp.now(db.io, .real).toMicroseconds();
        Spin.run(&one);
        const s1 = std.Io.Timestamp.now(db.io, .real).toMicroseconds();
        var spins: [N]Spin = undefined;
        for (0..N) |i| spins[i] = .{};
        var sth: [N]std.Thread = undefined;
        const p0 = std.Io.Timestamp.now(db.io, .real).toMicroseconds();
        for (0..N) |i| sth[i] = try std.Thread.spawn(.{}, Spin.run, .{&spins[i]});
        for (0..N) |i| sth[i].join();
        const p1 = std.Io.Timestamp.now(db.io, .real).toMicroseconds();
        std.debug.print("probe0 CPU   : single={d:.0} ms  12-concurrent wall={d:.0} ms  (parallel if wall ~ single)\n", .{ @as(f64, @floatFromInt(s1 - s0)) / 1000.0, @as(f64, @floatFromInt(p1 - p0)) / 1000.0 });
    }

    // (a) full query once, warm
    var full = Shard{ .db = db, .gpa = gpa, .lo = null, .hi = null };
    const fh0 = thindb.storage.cache.globalStats().hits;
    runShard(&full);
    const fh1 = thindb.storage.cache.globalStats().hits;
    std.debug.print("full query cache hits = {d}\n", .{fh1 - fh0});
    if (full.err) |e| {
        std.debug.print("full err: {t}\n", .{e});
        return 1;
    }
    std.debug.print("full (cold load)      : {d:.0} ms  rows={d}\n", .{ full.ms, full.rows });
    // Second full run = truly warm (cache populated by the cold pass). This is
    // the clean single-query baseline (silo at SEP_DOP>1) for the shard compare.
    var full2 = Shard{ .db = db, .gpa = gpa, .lo = null, .hi = null };
    runShard(&full2);
    full.ms = full2.ms;
    std.debug.print("full (WARM)           : {d:.0} ms  rows={d}\n", .{ full2.ms, full2.rows });

    // (b) 12 shards sequential
    {
        var sum: f64 = 0;
        var maxms: f64 = 0;
        const pfs0 = pageFaults();
        for (0..N) |i| {
            var sh = Shard{ .db = db, .gpa = gpa, .lo = los[i], .hi = his[i] };
            const h0 = thindb.storage.cache.globalStats();
            runShard(&sh);
            const h1 = thindb.storage.cache.globalStats();
            if (sh.err) |e| {
                std.debug.print("shard {d} err: {t}\n", .{ i, e });
                return 1;
            }
            sum += sh.ms;
            if (sh.ms > maxms) maxms = sh.ms;
            const lo_s = if (sh.lo) |l| l else 0;
            const hi_s = if (sh.hi) |h| h else 999999;
            std.debug.print("  shard {d:>2}: CounterID [{d:>6} .. {d:>6})  {d:>6.0} ms  rows={d}  cache hits={d} misses={d}\n", .{ i, lo_s, hi_s, sh.ms, sh.rows, h1.hits - h0.hits, h1.misses - h0.misses });
        }
        const pfs1 = pageFaults();
        std.debug.print("{d} shards SEQ sum     : {d:.0} ms  (avg {d:.0} ms, slowest shard {d:.0} ms = parallel ceiling)  seq_faults={d}\n", .{ N, sum, sum / @as(f64, N), maxms, pfs1 - pfs0 });
    }

    // (c) 12 shards concurrent on raw threads — run twice back-to-back to test
    // whether workspace pages stay resident (pass 2 faster => pooling is the fix).
    for (0..2) |pass| {
        std.debug.print("-- concurrent pass {d} --\n", .{pass});
        var shards: [N]Shard = undefined;
        for (0..N) |i| shards[i] = .{ .db = db, .gpa = gpa, .lo = los[i], .hi = his[i] };
        var threads: [N]std.Thread = undefined;
        const cs0 = thindb.storage.cache.globalStats();
        const pf0 = pageFaults();
        const cpu0 = cpuMicros();
        const w0 = std.Io.Timestamp.now(db.io, .real).toMicroseconds();
        for (0..N) |i| threads[i] = try std.Thread.spawn(.{}, runShard, .{&shards[i]});
        for (0..N) |i| threads[i].join();
        const w1 = std.Io.Timestamp.now(db.io, .real).toMicroseconds();
        const cpu1 = cpuMicros();
        const cs1 = thindb.storage.cache.globalStats();
        const pf1 = pageFaults();
        std.debug.print("  cache delta: hits={d} misses={d} miss_bytes={d} evictions={d}\n", .{ cs1.hits - cs0.hits, cs1.misses - cs0.misses, cs1.miss_bytes - cs0.miss_bytes, cs1.evictions - cs0.evictions });
        std.debug.print("  page faults: {d}\n", .{pf1 - pf0});
        const wall = @as(f64, @floatFromInt(w1 - w0)) / 1000.0;
        const user_ms = @as(f64, @floatFromInt(cpu1.user - cpu0.user)) / 1000.0;
        const kern_ms = @as(f64, @floatFromInt(cpu1.kernel - cpu0.kernel)) / 1000.0;
        std.debug.print("  CPU time   : user={d:.0} ms  kernel={d:.0} ms  (user/wall = avg busy cores; 12=full parallel, 1=serialized)\n", .{ user_ms, kern_ms });
        var maxms: f64 = 0;
        var totrows: usize = 0;
        for (0..N) |i| {
            if (shards[i].err) |e| {
                std.debug.print("shard {d} err: {t}\n", .{ i, e });
                return 1;
            }
            if (shards[i].ms > maxms) maxms = shards[i].ms;
            totrows += shards[i].rows;
        }
        var max_comp: f64 = 0;
        var max_drain: f64 = 0;
        for (0..N) |i| {
            if (shards[i].compile_ms > max_comp) max_comp = shards[i].compile_ms;
            if (shards[i].drain_ms > max_drain) max_drain = shards[i].drain_ms;
        }
        std.debug.print("  per-shard concurrent: max_compile={d:.0} ms  max_drain={d:.0} ms\n", .{ max_comp, max_drain });
        std.debug.print("12 shards CONCURRENT  : {d:.0} ms wall  (slowest shard {d:.0} ms)  rows={d}\n", .{ wall, maxms, totrows });
        std.debug.print("  => speedup vs full DOP1 = {d:.2}x ; vs SEQ-sum = near-linear if wall ~ slowest shard\n", .{full.ms / wall});
    }
    return 0;
}
