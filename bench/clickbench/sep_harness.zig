//! Range-separability harness. Opens the clickbench DB embedded at max_dop=1
//! (so every query is a single-worker serial stack), then runs the SAME GROUP
//! BY-on-leading-order-key query split into 12 balanced CounterID ranges, both
//! sequentially and concurrently on 12 raw OS threads. Each shard prunes to
//! ~1/12 of the table via zonemap on the leading order key (CounterID). Tests
//! whether 12 independent DOP-1 shards scale near-linearly. Uses Io.Threaded.

const std = @import("std");
const thindb = @import("thindb");

extern "c" fn getenv(name: [*:0]const u8) ?[*:0]const u8;

const CUTS = [_]i64{ 3922, 3968, 46626, 99171, 117946, 132069, 170852, 194561, 206805, 229578, 245449 };
const N: usize = CUTS.len + 1; // 12 shards

// Int-only variant: no URL/length() → no FSST string decode, no per-block string
// allocs. Isolates whether the concurrent serialization is in the string-decode
// path. WHERE CounterID >= 0 is always true (keeps buildSql's `AND` append valid).
const BASE = "SELECT CounterID, COUNT(*) AS c FROM hits WHERE CounterID >= 0";
const TAIL = "GROUP BY CounterID HAVING COUNT(*) > 100000 ORDER BY c DESC LIMIT 25";

const Shard = struct {
    db: *thindb.Database,
    gpa: std.mem.Allocator,
    lo: ?i64,
    hi: ?i64,
    ms: f64 = 0,
    rows: usize = 0,
    err: ?anyerror = null,
};

fn buildSql(a: std.mem.Allocator, lo: ?i64, hi: ?i64) ![]u8 {
    if (lo) |l| {
        if (hi) |h| return std.fmt.allocPrint(a, "{s} AND CounterID >= {d} AND CounterID < {d} {s}", .{ BASE, l, h, TAIL });
        return std.fmt.allocPrint(a, "{s} AND CounterID >= {d} {s}", .{ BASE, l, TAIL });
    }
    if (hi) |h| return std.fmt.allocPrint(a, "{s} AND CounterID < {d} {s}", .{ BASE, h, TAIL });
    return std.fmt.allocPrint(a, "{s} {s}", .{ BASE, TAIL });
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
    const sql = try buildSql(a, sh.lo, sh.hi);
    const op = try thindb.sql.parse(a, sql);
    const t0 = std.Io.Timestamp.now(sh.db.io, .real).toMicroseconds();
    var compiled = try thindb.net.compileWithSession(a, sh.db, .{ .current_db = "clickbench_fsst", .current_schema = "public" }, op);
    defer compiled.deinit();
    var n: usize = 0;
    while (try compiled.next()) |batch| n += batch.row_count;
    const t1 = std.Io.Timestamp.now(sh.db.io, .real).toMicroseconds();
    sh.ms = @as(f64, @floatFromInt(t1 - t0)) / 1000.0;
    sh.rows = n;
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
    const catalog = try thindb.Catalog.open(gpa, io, data_root, .{ .max_dop = dop, .memory_budget = 0, .query_memory_budget = 0 });
    defer catalog.close();
    const db = catalog.database("clickbench_fsst") orelse {
        std.debug.print("clickbench_fsst not found\n", .{});
        return 1;
    };

    var los: [N]?i64 = undefined;
    var his: [N]?i64 = undefined;
    for (0..N) |i| {
        los[i] = if (i == 0) null else CUTS[i - 1];
        his[i] = if (i == N - 1) null else CUTS[i];
    }

    // (a) full query once, warm
    var full = Shard{ .db = db, .gpa = gpa, .lo = null, .hi = null };
    runShard(&full);
    if (full.err) |e| {
        std.debug.print("full err: {t}\n", .{e});
        return 1;
    }
    std.debug.print("full DOP1 (warm)      : {d:.0} ms  rows={d}\n", .{ full.ms, full.rows });

    // (b) 12 shards sequential
    {
        var sum: f64 = 0;
        var maxms: f64 = 0;
        for (0..N) |i| {
            var sh = Shard{ .db = db, .gpa = gpa, .lo = los[i], .hi = his[i] };
            runShard(&sh);
            if (sh.err) |e| {
                std.debug.print("shard {d} err: {t}\n", .{ i, e });
                return 1;
            }
            sum += sh.ms;
            if (sh.ms > maxms) maxms = sh.ms;
        }
        std.debug.print("12 shards SEQ sum     : {d:.0} ms  (slowest shard {d:.0} ms = parallel ceiling)\n", .{ sum, maxms });
    }

    // (c) 12 shards concurrent on raw threads
    {
        var shards: [N]Shard = undefined;
        for (0..N) |i| shards[i] = .{ .db = db, .gpa = gpa, .lo = los[i], .hi = his[i] };
        var threads: [N]std.Thread = undefined;
        const w0 = std.Io.Timestamp.now(db.io, .real).toMicroseconds();
        for (0..N) |i| threads[i] = try std.Thread.spawn(.{}, runShard, .{&shards[i]});
        for (0..N) |i| threads[i].join();
        const w1 = std.Io.Timestamp.now(db.io, .real).toMicroseconds();
        const wall = @as(f64, @floatFromInt(w1 - w0)) / 1000.0;
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
        std.debug.print("12 shards CONCURRENT  : {d:.0} ms wall  (slowest shard {d:.0} ms)  rows={d}\n", .{ wall, maxms, totrows });
        std.debug.print("  => speedup vs full DOP1 = {d:.2}x ; vs SEQ-sum = near-linear if wall ~ slowest shard\n", .{full.ms / wall});
    }
    return 0;
}
