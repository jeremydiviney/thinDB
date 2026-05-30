//! Isolated regex-throughput microbench over the REAL Referer column.
//!
//! Q28's `REGEXP_REPLACE(Referer, '^https?://(?:www\.)?([^/]+)/.*$', '\1')`
//! is the workload. The query-engine profile showed ~7s of parallel
//! scan+filter+regex+materialize and only ~875 MB/s aggregate across 8 cores —
//! we wanted to know whether that ceiling is the Pike VM itself or pipeline
//! confounders (scan, decompress, cache, materialize, GROUP BY).
//!
//! This bench strips all confounders: it opens `.clickbench-db`, pulls every
//! non-empty Referer into one flat in-memory buffer (UNTIMED setup — decode +
//! decompress happens here, off the clock), then runs the exact same Pike-VM
//! `replaceAllScratch` loop the operator runs, single-threaded and at 2/4/8
//! threads, timing ONLY the regex. The result is raw regex throughput on the
//! real URL distribution + its thread scaling — no query engine in the way.
//!
//! Timing uses the Windows perf counter directly (this project routes clocks
//! through Io; std.time exposes only constants — see util/prof.zig).
//!
//! Run:  zig build regexreal -Doptimize=ReleaseFast
//!   (stop thindb-server first if it holds .clickbench-db exclusively)

const std = @import("std");
const win = std.os.windows;
const thindb = @import("thindb");
const regex = thindb.regex;

const PATTERN = "^https?://(?:www\\.)?([^/]+)/.*$";
const TEMPLATE = "\\1";
const COLUMN = "Referer";

fn nowTicks() i64 {
    var c: win.LARGE_INTEGER = 0;
    _ = win.ntdll.RtlQueryPerformanceCounter(&c);
    return c;
}
fn perfFreq() i64 {
    var f: win.LARGE_INTEGER = 0;
    _ = win.ntdll.RtlQueryPerformanceFrequency(&f);
    return if (f == 0) 1 else f;
}
fn ticksToMs(ticks: i64, freq: i64) f64 {
    return @as(f64, @floatFromInt(ticks)) * 1000.0 / @as(f64, @floatFromInt(freq));
}

// ---- CPU affinity (Windows) -------------------------------------------------
// Pin each worker to a specific logical CPU so we can guarantee one-thread-per-
// physical-core placement (vs. letting the scheduler risk parking two threads
// on the two SMT siblings of one core while another core idles).
extern "kernel32" fn GetCurrentThread() callconv(.winapi) win.HANDLE;
extern "kernel32" fn SetThreadAffinityMask(hThread: win.HANDLE, mask: usize) callconv(.winapi) usize;
extern "kernel32" fn GetLogicalProcessorInformation(buf: ?[*]LogicalProcInfo, len: *u32) callconv(.winapi) win.BOOL;

const RelationProcessorCore: u32 = 0;
const LogicalProcInfo = extern struct {
    processor_mask: usize,
    relationship: u32,
    _pad: u32 = 0,
    _union: [16]u8 = [_]u8{0} ** 16,
};

/// Returns a logical-CPU ordering that fills distinct physical cores first
/// (one primary logical CPU per physical core), then their SMT siblings. So
/// threads 0..n_physical-1 each land on their own physical core.
fn cpuOrder(allocator: std.mem.Allocator) ![]usize {
    var buf: [256]LogicalProcInfo = undefined;
    var len: u32 = @intCast(@sizeOf(LogicalProcInfo) * buf.len);
    if (!GetLogicalProcessorInformation(&buf, &len).toBool()) return error.QueryFailed;
    const n = len / @sizeOf(LogicalProcInfo);

    var primaries: std.ArrayListUnmanaged(usize) = .empty;
    var siblings: std.ArrayListUnmanaged(usize) = .empty;
    for (buf[0..n]) |info| {
        if (info.relationship != RelationProcessorCore) continue;
        var m = info.processor_mask;
        var first = true;
        while (m != 0) {
            const bit: usize = @ctz(m);
            m &= m - 1;
            if (first) {
                try primaries.append(allocator, bit);
                first = false;
            } else {
                try siblings.append(allocator, bit);
            }
        }
    }
    try primaries.appendSlice(allocator, siblings.items);
    siblings.deinit(allocator);
    return primaries.toOwnedSlice(allocator);
}

const Job = struct {
    data: []const u8,
    offsets: []const usize,
    lo: usize,
    hi: usize,
    checksum: *usize,
    cpu: usize,
};

fn worker(job: Job) void {
    _ = SetThreadAffinityMask(GetCurrentThread(), @as(usize, 1) << @intCast(job.cpu));
    var re = regex.Regex.compile(std.heap.page_allocator, PATTERN) catch return;
    defer re.deinit();
    var scratch = regex.Scratch.init(std.heap.page_allocator);
    defer scratch.deinit();
    var cs: usize = 0;
    var i = job.lo;
    while (i < job.hi) : (i += 1) {
        const url = job.data[job.offsets[i]..job.offsets[i + 1]];
        const out = re.replaceAllScratch(url, TEMPLATE, &scratch) catch break;
        cs +%= out.len;
    }
    job.checksum.* = cs;
}

fn findRefererTable(allocator: std.mem.Allocator, catalog: anytype) !*thindb.api.Table {
    const db_names = try catalog.listDatabases(allocator);
    defer {
        for (db_names) |n| allocator.free(n);
        allocator.free(db_names);
    }
    for (db_names) |dn| {
        const db = catalog.database(dn).?;
        const sc_names = try db.listSchemas(allocator);
        defer {
            for (sc_names) |n| allocator.free(n);
            allocator.free(sc_names);
        }
        for (sc_names) |sn| {
            const sc = db.schema(sn).?;
            const tbl_names = try sc.listTables(allocator);
            defer {
                for (tbl_names) |n| allocator.free(n);
                allocator.free(tbl_names);
            }
            for (tbl_names) |tn| {
                const t = sc.openTable(tn, .{}) catch continue;
                for (t.schema.columns) |c| {
                    if (std.mem.eql(u8, c.name, COLUMN)) {
                        std.debug.print("[regex-real] table {s}.{s}.{s} has {s}\n", .{ dn, sn, tn, COLUMN });
                        return t;
                    }
                }
            }
        }
    }
    return error.NotFound;
}

pub fn main(init: std.process.Init) !u8 {
    const allocator = init.gpa;
    var threaded: std.Io.Threaded = .init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();
    const freq = perfFreq();

    const cwd = std.Io.Dir.cwd();
    var data_root = try cwd.createDirPathOpen(io, ".clickbench-db", .{});
    defer data_root.close(io);

    const catalog = try thindb.Catalog.open(allocator, io, data_root, .{});
    defer catalog.close();

    const table = try findRefererTable(allocator, catalog);

    var total_rows: u64 = 0;
    for (table.manifest.segments.items) |s| total_rows += s.row_count;
    std.debug.print("[regex-real] {s}: {d} total rows; loading non-empty into RAM (untimed)...\n", .{ COLUMN, total_rows });

    // Flat buffer: all non-empty Referer bytes back-to-back + an offsets array.
    var data: std.ArrayListUnmanaged(u8) = .empty;
    defer data.deinit(allocator);
    var offsets: std.ArrayListUnmanaged(usize) = .empty;
    defer offsets.deinit(allocator);
    try data.ensureTotalCapacity(allocator, @intCast(total_rows * 48));
    try offsets.ensureTotalCapacity(allocator, @intCast(total_rows + 1));
    try offsets.append(allocator, 0);

    const load_t0 = nowTicks();
    var q = try thindb.exec.Scan.createWithProjection(allocator, table, null, &.{COLUMN});
    defer q.deinit();
    var scanned: u64 = 0;
    while (try q.next()) |batch| {
        const sv = switch (batch.values[0].data) {
            .varchar, .string, .char => |s| s,
            else => return error.UnexpectedType,
        };
        const n = batch.row_count;
        var r: usize = 0;
        while (r < n) : (r += 1) {
            const bytes = sv.rowBytes(r);
            if (bytes.len == 0) continue;
            try data.appendSlice(allocator, bytes);
            try offsets.append(allocator, data.items.len);
        }
        scanned += n;
    }
    const load_ms = ticksToMs(nowTicks() - load_t0, freq);

    const n_urls = offsets.items.len - 1;
    const total_bytes = data.items.len;
    std.debug.print(
        "[regex-real] loaded {d} non-empty urls ({d} scanned), {d:.2} GB, avg {d:.1} B/url, load {d:.0} ms\n",
        .{ n_urls, scanned, @as(f64, @floatFromInt(total_bytes)) / 1e9, @as(f64, @floatFromInt(total_bytes)) / @as(f64, @floatFromInt(n_urls)), load_ms },
    );

    const data_slice = data.items;
    const off_slice = offsets.items;

    const cpus = try cpuOrder(allocator);
    defer allocator.free(cpus);
    std.debug.print("[regex-real] pinning: {d} physical cores; thread t -> distinct core for first {d} threads\n", .{ cpus.len / 2, cpus.len / 2 });

    inline for (.{ 1, 2, 4, 8, 12, 16, 20, 24, 28, 32 }) |dop| {
        var checksums: [dop]usize = [_]usize{0} ** dop;
        var threads: [dop]std.Thread = undefined;
        const t0 = nowTicks();
        var t: usize = 0;
        while (t < dop) : (t += 1) {
            const lo = t * n_urls / dop;
            const hi = (t + 1) * n_urls / dop;
            threads[t] = try std.Thread.spawn(.{}, worker, .{Job{
                .data = data_slice,
                .offsets = off_slice,
                .lo = lo,
                .hi = hi,
                .checksum = &checksums[t],
                .cpu = cpus[t % cpus.len],
            }});
        }
        t = 0;
        while (t < dop) : (t += 1) threads[t].join();
        const ms = ticksToMs(nowTicks() - t0, freq);

        var cs_total: usize = 0;
        for (checksums) |c| cs_total +%= c;
        const secs = ms / 1000.0;
        const mbps = @as(f64, @floatFromInt(total_bytes)) / 1e6 / secs;
        const murls = @as(f64, @floatFromInt(n_urls)) / 1e6 / secs;
        const us_per_url = ms * 1000.0 / @as(f64, @floatFromInt(n_urls));
        std.debug.print(
            "[regex-real] DOP={d}: {d:.0} ms | {d:.0} MB/s | {d:.1} M urls/s | {d:.3} us/url | cs={d}\n",
            .{ dop, ms, mbps, murls, us_per_url, cs_total },
        );
    }
    return 0;
}
