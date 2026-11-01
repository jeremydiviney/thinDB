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
//! decompress happens here, off the clock), checks `findWith` against the
//! Pike VM on every row, then times the Pike VM alone, `findWith`, and the
//! replace loop the operator's kernel runs, at 1 and 12 threads,
//! timing ONLY the regex. The result is raw regex throughput on the real URL
//! distribution + its thread scaling — no query engine in the way.
//!
//! Timing uses the Windows perf counter directly (this project routes clocks
//! through Io; std.time exposes only constants — see util/prof.zig).
//!
//! Run:  zig build regexreal -Doptimize=ReleaseFast -- [cache-file] [quick]
//!   (`quick` skips the engine cross-check and the Pike VM timings)
//!   (`hand` instead times the regex against the same pattern written out
//!   by hand, the floor for what an engine could cost on this shape)
//!   (`variants` instead times equivalent spellings of the pattern, which
//!   must cost about the same, and checks their results agree)
//!   (the first run reads .clickbench-db, so stop thindb-server first if it
//!   holds it; later runs with the same cache file never open it)

const std = @import("std");
const builtin = @import("builtin");
const win = std.os.windows;
const thindb = @import("thindb");
const regex = thindb.regex;

const PATTERN = "^https?://(?:www\\.)?([^/]+)/.*$";
const TEMPLATE = "\\1";
const COLUMN = "Referer";

fn nowTicks() i64 {
    if (builtin.os.tag == .linux) {
        var ts: std.c.timespec = undefined;
        _ = std.c.clock_gettime(.MONOTONIC, &ts);
        return @as(i64, ts.sec) * std.time.ns_per_s + ts.nsec;
    }
    var c: win.LARGE_INTEGER = 0;
    _ = win.ntdll.RtlQueryPerformanceCounter(&c);
    return c;
}
fn perfFreq() i64 {
    if (builtin.os.tag == .linux) return std.time.ns_per_s;
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
    _union: [16]u8 = @splat(0),
};

/// Returns a logical-CPU ordering that fills distinct physical cores first
/// (one primary logical CPU per physical core), then their SMT siblings. So
/// threads 0..n_physical-1 each land on their own physical core.
fn cpuOrder(allocator: std.mem.Allocator) ![]usize {
    // Linux numbers each physical core's first logical CPU before the
    // siblings, so CPUs in order already fill the cores first.
    if (builtin.os.tag == .linux) {
        const order = try allocator.alloc(usize, try std.Thread.getCpuCount());
        for (order, 0..) |*c, k| c.* = k;
        return order;
    }
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

/// Spellings of Q28's pattern. Those marked `same` give the base's results
/// on every row; the others differ only on rare rows (an empty host, a URL
/// with a line break), so their speed is compared but not their results.
const Variant = struct { pattern: []const u8, template: []const u8, same: bool };
const VARIANTS = [_]Variant{
    .{ .pattern = PATTERN, .template = TEMPLATE, .same = true },
    .{ .pattern = "^https?://(?:www[.])?([^/]+)/.*$", .template = "\\1", .same = true },
    .{ .pattern = "^https?://(www\\.)?([^/]+)/.*$", .template = "\\2", .same = true },
    .{ .pattern = "^http(s)?://(?:www\\.)?([^/]+)/.*$", .template = "\\2", .same = true },
    .{ .pattern = "^https?:\\/\\/(?:www\\.)?([^\\/]+)\\/.*$", .template = "\\1", .same = true },
    .{ .pattern = "^(?:https|http)://(?:www\\.)?([^/]+)/.*$", .template = "\\1", .same = true },
    .{ .pattern = "^(?:http|https)://(?:www\\.)?([^/]+)/.*$", .template = "\\1", .same = true },
    .{ .pattern = "^https?://(?:www\\.)?([^/]*)/.*$", .template = "\\1", .same = false },
    .{ .pattern = "^https?://(?:www\\.)?([^/]+)/.*", .template = "\\1", .same = false },
    .{ .pattern = "^https?://(?:www\\.)?([^/]+?)/.*$", .template = "\\1", .same = true },
    .{ .pattern = "^https{0,1}://(?:w{3}\\.)?([^/]{1,})/.*$", .template = "\\1", .same = true },
};

/// Rows per tail check, as in the kernel (`TEXT_CHUNK_ROWS`).
const CHUNK_ROWS = 64;

const Job = struct {
    data: []const u8,
    offsets: []const usize,
    lo: usize,
    hi: usize,
    checksum: *usize,
    cpu: usize,
    mode: Mode,
    pattern: []const u8 = PATTERN,
    template: []const u8 = TEMPLATE,
};

/// `replace` is the operator's kernel; `find` and `find_vm` time one
/// search per row through `findWith` and the Pike VM alone; `hand` is
/// `handHost` in `replace`'s place, and `hand_bare` is `handHost` with no
/// output to copy and hash.
const Mode = enum { replace, find, find_vm, hand, hand_bare };

/// PATTERN with TEMPLATE written out by hand, as fast as we could make
/// it: the floor for what the regex costs on this shape. `tail_clear` is
/// the chunk check's verdict, which the regex gets too.
fn handHost(url: []const u8, tail_clear: bool) []const u8 {
    if (url.len < 9) return url;
    const head = std.mem.readInt(u64, url[0..8], .little);
    const p: usize = if (head & 0x00ff_ffff_ffff_ffff == HTTP) 7 else if (head == HTTPS) 8 else return url;
    const www = url.len - p >= 4 and std.mem.readInt(u32, url[p..][0..4], .little) == WWW;
    const from = p + 4 * @as(usize, @intFromBool(www));
    const slash = slashAt(url, from);
    if (slash == url.len) return url;
    // An empty host after `www.` makes the regex give `www.` back to it.
    const start = if (slash > from) from else if (www) p else return url;
    if (!tail_clear and std.mem.indexOfScalarPos(u8, url, slash + 1, '\n') != null) return url;
    return url[start..slash];
}

const HTTP = std.mem.readInt(u64, "http://\x00", .little);
const HTTPS = std.mem.readInt(u64, "https://", .little);
const WWW = std.mem.readInt(u32, "www.", .little);

fn slashAt(url: []const u8, from: usize) usize {
    const V = @Vector(32, u8);
    if (url.len < 32) return std.mem.indexOfScalarPos(u8, url, from, '/') orelse url.len;
    var i = from;
    while (i < url.len) {
        const at = @min(i, url.len - 32);
        var hits: u32 = @bitCast(@as(V, url[at..][0..32].*) == @as(V, @splat('/')));
        hits &= ~@as(u32, 0) << @intCast(i - at);
        if (hits != 0) return at + @ctz(hits);
        i = at + 32;
    }
    return url.len;
}

fn worker(job: Job) void {
    if (builtin.os.tag == .linux) {
        var set: std.os.linux.cpu_set_t = @splat(0);
        set[job.cpu / @bitSizeOf(usize)] |= @as(usize, 1) << @intCast(job.cpu % @bitSizeOf(usize));
        std.os.linux.sched_setaffinity(0, &set) catch {};
    } else {
        _ = SetThreadAffinityMask(GetCurrentThread(), @as(usize, 1) << @intCast(job.cpu));
    }
    var re = regex.Regex.compile(std.heap.page_allocator, job.pattern) catch return;
    defer re.deinit();
    var scratch = regex.Scratch.init(std.heap.page_allocator);
    defer scratch.deinit();
    var slots: [4]?usize = undefined;
    var cs: usize = 0;
    var tail_clear = false;
    var i = job.lo;
    while (i < job.hi) : (i += 1) {
        const url = job.data[job.offsets[i]..job.offsets[i + 1]];
        if ((i - job.lo) % CHUNK_ROWS == 0) {
            const end = @min(job.hi, i + CHUNK_ROWS);
            tail_clear = re.tailClearIn(job.data[job.offsets[i]..job.offsets[end]]);
        }
        switch (job.mode) {
            .hand => {
                const out = &scratch.out_buf;
                out.clearRetainingCapacity();
                out.appendSlice(scratch.backing, handHost(url, tail_clear)) catch break;
                cs = std.hash.Wyhash.hash(cs, out.items);
            },
            .hand_bare => cs +%= handHost(url, tail_clear).len,
            .replace => {
                const out = &scratch.out_buf;
                out.clearRetainingCapacity();
                re.replaceInto(scratch.backing, out, url, job.template, &scratch, 0, 0, tail_clear) catch break;
                cs = std.hash.Wyhash.hash(cs, out.items);
            },
            .find, .find_vm => {
                const found = (if (job.mode == .find)
                    re.findWith(&scratch, url, 0, &slots)
                else
                    re.findWithVm(&scratch, url, 0, &slots)) catch break;
                if (found) cs +%= slots[3].? - slots[2].?;
            },
        }
    }
    job.checksum.* = cs;
}

/// Rows where `findWith` and the Pike VM disagree on match or captures.
fn countEngineMismatches(data: []const u8, offsets: []const usize) !usize {
    var re = try regex.Regex.compile(std.heap.page_allocator, PATTERN);
    defer re.deinit();
    var s_new = regex.Scratch.init(std.heap.page_allocator);
    defer s_new.deinit();
    var s_vm = regex.Scratch.init(std.heap.page_allocator);
    defer s_vm.deinit();
    var a: [4]?usize = undefined;
    var b: [4]?usize = undefined;
    var mismatches: usize = 0;
    for (0..offsets.len - 1) |i| {
        const url = data[offsets[i]..offsets[i + 1]];
        @memset(&a, null);
        @memset(&b, null);
        const fa = try re.findWith(&s_new, url, 0, &a);
        const fb = try re.findWithVm(&s_vm, url, 0, &b);
        if (fa != fb or !std.mem.eql(?usize, &a, &b)) {
            if (mismatches == 0) std.debug.print("[regex-real] first mismatch at row {d}\n", .{i});
            mismatches += 1;
        }
    }
    return mismatches;
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

/// Flat column: every non-empty value back-to-back, plus n+1 offsets.
const Column = struct {
    data: std.ArrayListUnmanaged(u8) = .empty,
    offsets: std.ArrayListUnmanaged(usize) = .empty,

    fn deinit(self: *Column, allocator: std.mem.Allocator) void {
        self.data.deinit(allocator);
        self.offsets.deinit(allocator);
    }
};

fn loadFromDb(allocator: std.mem.Allocator, io: std.Io, col: *Column) !void {
    var data_root = try std.Io.Dir.cwd().createDirPathOpen(io, ".clickbench-db", .{});
    defer data_root.close(io);
    const catalog = try thindb.Catalog.open(allocator, io, data_root, .{});
    defer catalog.close();
    const table = try findRefererTable(allocator, catalog);

    var total_rows: u64 = 0;
    for (table.manifest.segments.items) |s| total_rows += s.row_count;
    std.debug.print("[regex-real] {s}: {d} total rows; loading non-empty into RAM (untimed)...\n", .{ COLUMN, total_rows });
    try col.data.ensureTotalCapacity(allocator, @intCast(total_rows * 48));
    try col.offsets.ensureTotalCapacity(allocator, @intCast(total_rows + 1));
    try col.offsets.append(allocator, 0);

    var q = try thindb.exec.Scan.createWithProjection(allocator, table, null, &.{COLUMN});
    defer q.deinit();
    while (try q.next()) |batch| {
        const sv = switch (batch.values[0].data) {
            .varchar, .string, .char => |s| s,
            else => return error.UnexpectedType,
        };
        var r: usize = 0;
        while (r < batch.row_count) : (r += 1) {
            const bytes = sv.rowBytes(r);
            if (bytes.len == 0) continue;
            try col.data.appendSlice(allocator, bytes);
            try col.offsets.append(allocator, col.data.items.len);
        }
    }
}

// Cache layout: n_urls u64, byte count u64, offsets (n_urls+1) × u64, bytes.
fn writeCache(io: std.Io, path: []const u8, col: *const Column) !void {
    var file = try std.Io.Dir.cwd().createFile(io, path, .{});
    defer file.close(io);
    const head = [2]u64{ col.offsets.items.len - 1, col.data.items.len };
    try file.writePositionalAll(io, std.mem.sliceAsBytes(&head), 0);
    const off_bytes = std.mem.sliceAsBytes(col.offsets.items);
    try file.writePositionalAll(io, off_bytes, 16);
    try file.writePositionalAll(io, col.data.items, 16 + off_bytes.len);
}

fn readCache(allocator: std.mem.Allocator, io: std.Io, path: []const u8, col: *Column) !bool {
    var file = std.Io.Dir.cwd().openFile(io, path, .{}) catch |err| switch (err) {
        error.FileNotFound => return false,
        else => return err,
    };
    defer file.close(io);
    var head: [2]u64 = undefined;
    if (try file.readPositionalAll(io, std.mem.sliceAsBytes(&head), 0) != 16) return error.UnexpectedEof;
    try col.offsets.resize(allocator, @intCast(head[0] + 1));
    try col.data.resize(allocator, @intCast(head[1]));
    const off_bytes = std.mem.sliceAsBytes(col.offsets.items);
    if (try file.readPositionalAll(io, off_bytes, 16) != off_bytes.len) return error.UnexpectedEof;
    if (try file.readPositionalAll(io, col.data.items, 16 + off_bytes.len) != col.data.items.len) return error.UnexpectedEof;
    return true;
}

/// Usage: regex_real [cache-file]. With a cache file that exists, the
/// column loads from it and `.clickbench-db` is never opened, so a server
/// may keep running on it; otherwise the column is read from the database
/// and, given a path, written there for the next run.
pub fn main(init: std.process.Init) !u8 {
    const allocator = init.gpa;
    var threaded: std.Io.Threaded = .init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();
    const freq = perfFreq();
    const args = try init.minimal.args.toSlice(init.arena.allocator());
    const cache_path: ?[]const u8 = if (args.len > 1) args[1] else null;
    const quick = args.len > 2 and std.mem.eql(u8, args[2], "quick");
    const variants = args.len > 2 and std.mem.eql(u8, args[2], "variants");
    const hand = args.len > 2 and std.mem.eql(u8, args[2], "hand");

    var col: Column = .{};
    defer col.deinit(allocator);
    const load_t0 = nowTicks();
    const cached = if (cache_path) |p| try readCache(allocator, io, p, &col) else false;
    if (!cached) {
        try loadFromDb(allocator, io, &col);
        if (cache_path) |p| try writeCache(io, p, &col);
    }
    const load_ms = ticksToMs(nowTicks() - load_t0, freq);

    const data_slice = col.data.items;
    const off_slice = col.offsets.items;
    const n_urls = off_slice.len - 1;
    const total_bytes = data_slice.len;
    std.debug.print(
        "[regex-real] {d} non-empty urls, {d:.2} GB, avg {d:.1} B/url, load {d:.0} ms ({s})\n",
        .{ n_urls, @as(f64, @floatFromInt(total_bytes)) / 1e9, @as(f64, @floatFromInt(total_bytes)) / @as(f64, @floatFromInt(n_urls)), load_ms, if (cached) "cache" else "db" },
    );

    const cpus = try cpuOrder(allocator);
    defer allocator.free(cpus);

    if (hand) {
        var want: usize = 0;
        inline for (.{ Mode.replace, Mode.hand, Mode.hand_bare }) |mode| {
            var best: f64 = std.math.inf(f64);
            var cs: usize = 0;
            for (0..3) |_| {
                const t0 = nowTicks();
                worker(.{ .data = data_slice, .offsets = off_slice, .lo = 0, .hi = n_urls, .checksum = &cs, .cpu = cpus[0], .mode = mode });
                best = @min(best, ticksToMs(nowTicks() - t0, freq));
            }
            if (mode == .replace) want = cs;
            const agrees = if (mode == .hand_bare) "" else if (cs == want) "same" else "DIFFERS";
            std.debug.print("[regex-real] {d:>6.1} ns/url  {s:<10} {s}\n", .{ best * 1e6 / @as(f64, @floatFromInt(n_urls)), @tagName(mode), agrees });
        }
        return 0;
    }

    if (variants) {
        var base: usize = 0;
        for (VARIANTS, 0..) |v, k| {
            var best: f64 = std.math.inf(f64);
            var cs: usize = 0;
            for (0..3) |_| {
                const t0 = nowTicks();
                worker(.{ .data = data_slice, .offsets = off_slice, .lo = 0, .hi = n_urls, .checksum = &cs, .cpu = cpus[0], .mode = .replace, .pattern = v.pattern, .template = v.template });
                best = @min(best, ticksToMs(nowTicks() - t0, freq));
            }
            if (k == 0) base = cs;
            const agrees = if (cs == base) "same" else if (v.same) "DIFFERS" else "differs (expected)";
            std.debug.print("[regex-real] {d:>6.1} ns/url  {s:<52} {s}\n", .{ best * 1e6 / @as(f64, @floatFromInt(n_urls)), v.pattern, agrees });
        }
        return 0;
    }

    if (!quick) {
        const mismatches = try countEngineMismatches(data_slice, off_slice);
        std.debug.print("[regex-real] findWith vs Pike VM: {d} mismatching rows\n", .{mismatches});
    }

    inline for (.{ Mode.find_vm, Mode.find, Mode.replace }) |mode| {
        inline for (.{ 1, 12 }) |dop| if (!(quick and mode == .find_vm)) {
            var checksums: [dop]usize = @splat(0);
            var threads: [dop]std.Thread = undefined;
            const t0 = nowTicks();
            var t: usize = 0;
            while (t < dop) : (t += 1) {
                threads[t] = try std.Thread.spawn(.{}, worker, .{Job{
                    .data = data_slice,
                    .offsets = off_slice,
                    .lo = t * n_urls / dop,
                    .hi = (t + 1) * n_urls / dop,
                    .checksum = &checksums[t],
                    .cpu = cpus[t % cpus.len],
                    .mode = mode,
                }});
            }
            t = 0;
            while (t < dop) : (t += 1) threads[t].join();
            const ms = ticksToMs(nowTicks() - t0, freq);

            var cs_total: usize = 0;
            for (checksums) |c| cs_total +%= c;
            const ns_per_url = ms * 1e6 / @as(f64, @floatFromInt(n_urls));
            std.debug.print(
                "[regex-real] {s:>8} DOP={d:>2}: {d:>7.0} ms | {d:>6.1} ns/url (x DOP = {d:.1}) | cs={d}\n",
                .{ @tagName(mode), dop, ms, ns_per_url, ns_per_url * @as(f64, @floatFromInt(dop)), cs_total },
            );
        };
    }
    return 0;
}
