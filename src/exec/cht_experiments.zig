//! Experimental harness for the parallel shared-aggregation-table campaign.
//! NOT part of the product build — every test is gated behind THINDB_BENCH and
//! this file is not imported anywhere. Run a single strategy with:
//!   THINDB_BENCH=1 THINDB_STRAT=combine THINDB_BENCH_T=12 THINDB_DIST=skew \
//!     zig test -OReleaseFast -lc -Dtest-filter="sweep" src/exec/cht_experiments.zig
//! Or run the full sweep by leaving THINDB_STRAT unset.

const std = @import("std");
const builtin = @import("builtin");

extern "c" fn getenv(name: [*:0]const u8) ?[*:0]const u8;

fn envU(name: [*:0]const u8, default: usize) usize {
    if (getenv(name)) |s| return std.fmt.parseInt(usize, std.mem.span(s), 10) catch default;
    return default;
}
fn envStr(name: [*:0]const u8) ?[]const u8 {
    if (getenv(name)) |s| return std.mem.span(s);
    return null;
}

// ---------------------------------------------------------------- timing
const win = std.os.windows;
fn qpcNow() i64 {
    switch (builtin.os.tag) {
        .windows => {
            var c: win.LARGE_INTEGER = undefined;
            _ = win.ntdll.RtlQueryPerformanceCounter(&c);
            return c;
        },
        .linux => {
            var ts: std.os.linux.timespec = undefined;
            _ = std.os.linux.clock_gettime(std.os.linux.CLOCK.MONOTONIC, &ts);
            return @as(i64, ts.sec) * 1_000_000_000 + ts.nsec;
        },
        else => return 0,
    }
}
fn qpcFreq() i64 {
    switch (builtin.os.tag) {
        .windows => {
            var f: win.LARGE_INTEGER = undefined;
            _ = win.ntdll.RtlQueryPerformanceFrequency(&f);
            return f;
        },
        .linux => return 1_000_000_000,
        else => return 1,
    }
}
fn msSince(t0: i64) f64 {
    return @as(f64, @floatFromInt(qpcNow() - t0)) / @as(f64, @floatFromInt(qpcFreq())) * 1000.0;
}

// ---------------------------------------------------------------- hashing
inline fn hashKey(key: u64) u64 {
    var z = key +% 0x9e3779b97f4a7c15;
    z = (z ^ (z >> 30)) *% 0xbf58476d1ce4e5b9;
    z = (z ^ (z >> 27)) *% 0x94d049bb133111eb;
    return z ^ (z >> 31);
}

// ---------------------------------------------------------------- key gen
// Distributions, all producing keys in [1 .. DISTINCT]. The scan would hand us
// batches of decoded keys; we model that by generating a batch at a time.
const DISTINCT: u64 = 17_000_000;
const HOT: u64 = 4096; // hot-set size for the skewed distribution

inline fn nextKeyUniform(x: *u64) u64 {
    x.* = (x.* *% 6364136223846793005) +% 1442695040888963407;
    return (x.* >> 16) % DISTINCT + 1;
}
inline fn nextKeySkew(x: *u64) u64 {
    x.* = (x.* *% 6364136223846793005) +% 1442695040888963407;
    const r = x.*;
    // ~80% of traffic to a small hot set, 20% spread across the full space.
    if ((r & 0xF) < 13) return (r >> 16) % HOT + 1;
    return (r >> 16) % DISTINCT + 1;
}

const Dist = enum { uniform, skew };

// ---------------------------------------------------------------- shared table
// One pre-sized open-addressing table all threads write to. `add(key, n)` folds
// n occurrences of key in atomically: CAS-claim the slot, then atomic-add n.
const Shared = struct {
    keys: []u64,
    counts: []u64,
    mask: u64,
    n_groups: std.atomic.Value(u64),
    allocator: std.mem.Allocator,

    fn init(allocator: std.mem.Allocator, expected: usize) !Shared {
        var cap: usize = 1024;
        const want = (expected *| 10) / 7 + 1;
        while (cap < want) cap <<= 1;
        const keys = try allocator.alloc(u64, cap);
        @memset(keys, 0);
        const counts = try allocator.alloc(u64, cap);
        @memset(counts, 0);
        return .{ .keys = keys, .counts = counts, .mask = cap - 1, .n_groups = .init(0), .allocator = allocator };
    }
    fn deinit(self: *Shared) void {
        self.allocator.free(self.keys);
        self.allocator.free(self.counts);
    }
    inline fn add(self: *Shared, key: u64, n: u64) void {
        var i = hashKey(key) & self.mask;
        while (true) {
            const k = @atomicLoad(u64, &self.keys[i], .acquire);
            if (k == key) {
                _ = @atomicRmw(u64, &self.counts[i], .Add, n, .monotonic);
                return;
            }
            if (k == 0) {
                if (@cmpxchgStrong(u64, &self.keys[i], 0, key, .acq_rel, .acquire) == null) {
                    _ = self.n_groups.fetchAdd(1, .monotonic);
                    _ = @atomicRmw(u64, &self.counts[i], .Add, n, .monotonic);
                    return;
                }
                continue;
            }
            i = (i + 1) & self.mask;
        }
    }
    fn totalCount(self: *const Shared) u64 {
        var s: u64 = 0;
        for (self.counts) |c| s += c;
        return s;
    }
};

// ---------------------------------------------------------------- workers
// Each worker generates `per` keys (keys are >= 1, so no zero sentinel issue) and
// folds them into the shared table by whatever strategy. The realistic input is a
// stream of batches, so every strategy pulls BATCH keys at a time.
const BATCH = 2048;

inline fn genBatch(buf: []u64, x: *u64, dist: Dist, n: usize) void {
    switch (dist) {
        .uniform => for (0..n) |i| {
            buf[i] = nextKeyUniform(x);
        },
        .skew => for (0..n) |i| {
            buf[i] = nextKeySkew(x);
        },
    }
}

// Strategy A: straight atomic bump, one key at a time.
fn workerAtomic(sh: *Shared, per: usize, seed: u64, dist: Dist, _: []u8) void {
    var buf: [BATCH]u64 = undefined;
    var x = seed;
    var done: usize = 0;
    while (done < per) {
        const n = @min(BATCH, per - done);
        genBatch(&buf, &x, dist, n);
        for (buf[0..n]) |key| sh.add(key, 1);
        done += n;
    }
}

// Strategy B: software-prefetch pipeline. Compute every slot in the batch and
// prefetch the line, THEN do the atomic adds — converts dependent serial misses
// into parallel prefetches (more memory-level parallelism per core).
fn workerPrefetch(sh: *Shared, per: usize, seed: u64, dist: Dist, _: []u8) void {
    var buf: [BATCH]u64 = undefined;
    var slot: [BATCH]u64 = undefined;
    var x = seed;
    var done: usize = 0;
    while (done < per) {
        const n = @min(BATCH, per - done);
        genBatch(&buf, &x, dist, n);
        for (0..n) |i| {
            slot[i] = hashKey(buf[i]) & sh.mask;
            @prefetch(&sh.keys[slot[i]], .{ .rw = .write, .locality = 1, .cache = .data });
        }
        for (buf[0..n]) |key| sh.add(key, 1);
        done += n;
    }
}

// Strategy C: per-core software-combining cache. A private direct-mapped table
// folds repeated keys locally; only evictions/misses touch the shared table
// (with a count >= 1). On skewed data the hot set lives entirely in this L2-sized
// cache, so the shared table sees almost no traffic. scratch holds keys||counts.
fn workerCombine(sh: *Shared, per: usize, seed: u64, dist: Dist, scratch: []u8) void {
    const cache_slots = scratch.len / 16; // 8 bytes key + 8 bytes count per slot
    const cmask = cache_slots - 1;
    const ckeys = std.mem.bytesAsSlice(u64, scratch[0 .. cache_slots * 8]);
    const ccounts = std.mem.bytesAsSlice(u64, scratch[cache_slots * 8 .. cache_slots * 16]);
    @memset(ckeys, 0);
    @memset(ccounts, 0);

    var buf: [BATCH]u64 = undefined;
    var x = seed;
    var done: usize = 0;
    while (done < per) {
        const n = @min(BATCH, per - done);
        genBatch(&buf, &x, dist, n);
        for (buf[0..n]) |key| {
            const idx = hashKey(key) & cmask;
            const cur = ckeys[idx];
            if (cur == key) {
                ccounts[idx] += 1;
            } else if (cur == 0) {
                ckeys[idx] = key;
                ccounts[idx] = 1;
            } else {
                sh.add(cur, ccounts[idx]); // evict
                ckeys[idx] = key;
                ccounts[idx] = 1;
            }
        }
        done += n;
    }
    // final flush
    for (ckeys, ccounts) |k, c| {
        if (k != 0) sh.add(k, c);
    }
}

// Strategy D: set-associative combining cache with evict-the-coldest. Each set
// has W ways; a key combines into its set, and when the set is full we flush the
// LOWEST-count way (so hot, high-count keys never get evicted by cold churn).
fn workerCombineAssoc(comptime W: usize) *const fn (*Shared, usize, u64, Dist, []u8) void {
    return struct {
        fn run(sh: *Shared, per: usize, seed: u64, dist: Dist, scratch: []u8) void {
            const total_slots = scratch.len / 16;
            const n_sets = total_slots / W;
            const set_mask = n_sets - 1;
            const ckeys = std.mem.bytesAsSlice(u64, scratch[0 .. total_slots * 8]);
            const ccounts = std.mem.bytesAsSlice(u64, scratch[total_slots * 8 .. total_slots * 16]);
            @memset(ckeys, 0);
            @memset(ccounts, 0);

            var buf: [BATCH]u64 = undefined;
            var x = seed;
            var done: usize = 0;
            while (done < per) {
                const n = @min(BATCH, per - done);
                genBatch(&buf, &x, dist, n);
                for (buf[0..n]) |key| {
                    const base = (hashKey(key) & set_mask) * W;
                    var empty: usize = std.math.maxInt(usize);
                    var minpos: usize = base;
                    var mincnt: u64 = std.math.maxInt(u64);
                    var hit = false;
                    inline for (0..W) |w| {
                        const p = base + w;
                        const ck = ckeys[p];
                        if (ck == key) {
                            ccounts[p] += 1;
                            hit = true;
                        } else if (ck == 0) {
                            if (empty == std.math.maxInt(usize)) empty = p;
                        } else if (ccounts[p] < mincnt) {
                            mincnt = ccounts[p];
                            minpos = p;
                        }
                    }
                    if (hit) continue;
                    if (empty != std.math.maxInt(usize)) {
                        ckeys[empty] = key;
                        ccounts[empty] = 1;
                    } else {
                        sh.add(ckeys[minpos], ccounts[minpos]);
                        ckeys[minpos] = key;
                        ccounts[minpos] = 1;
                    }
                }
                done += n;
            }
            for (ckeys, ccounts) |k, c| {
                if (k != 0) sh.add(k, c);
            }
        }
    }.run;
}

// Strategy E: cheap pinning combine. One slot check. If the resident key is
// already hot (count >= PIN), a colliding key bypasses straight to the shared
// table — no eviction, so hot keys stay pinned and the per-op cost is tiny.
const PIN: u64 = 4;
fn workerCombinePin(sh: *Shared, per: usize, seed: u64, dist: Dist, scratch: []u8) void {
    const cache_slots = scratch.len / 16;
    const cmask = cache_slots - 1;
    const ckeys = std.mem.bytesAsSlice(u64, scratch[0 .. cache_slots * 8]);
    const ccounts = std.mem.bytesAsSlice(u64, scratch[cache_slots * 8 .. cache_slots * 16]);
    @memset(ckeys, 0);
    @memset(ccounts, 0);
    var buf: [BATCH]u64 = undefined;
    var x = seed;
    var done: usize = 0;
    while (done < per) {
        const n = @min(BATCH, per - done);
        genBatch(&buf, &x, dist, n);
        for (buf[0..n]) |key| {
            const idx = hashKey(key) & cmask;
            const cur = ckeys[idx];
            if (cur == key) {
                ccounts[idx] += 1;
            } else if (cur == 0) {
                ckeys[idx] = key;
                ccounts[idx] = 1;
            } else if (ccounts[idx] >= PIN) {
                sh.add(key, 1); // incumbent is hot — don't evict; bypass to shared
            } else {
                sh.add(cur, ccounts[idx]); // incumbent cold — evict + install
                ckeys[idx] = key;
                ccounts[idx] = 1;
            }
        }
        done += n;
    }
    for (ckeys, ccounts) |k, c| {
        if (k != 0) sh.add(k, c);
    }
}

// Strategy F: bounded per-thread linear-probe combining table. Hits early-out
// (cheap, the common case for hot keys). A miss probes up to MAXPROBE; an empty
// is filled, otherwise the coldest key in the window is evicted to the shared
// table. Hot keys (high count) survive; only cold keys reach the shared table,
// so contention nearly vanishes. Duplicate keys across the local + shared tables
// are harmless: the shared table's atomic adds re-combine them.
fn workerCombineLP(sh: *Shared, per: usize, seed: u64, dist: Dist, scratch: []u8) void {
    const S = scratch.len / 16;
    const mask = S - 1;
    const MAXPROBE = 8;
    const ckeys = std.mem.bytesAsSlice(u64, scratch[0 .. S * 8]);
    const ccounts = std.mem.bytesAsSlice(u64, scratch[S * 8 .. S * 16]);
    @memset(ckeys, 0);
    @memset(ccounts, 0);
    var buf: [BATCH]u64 = undefined;
    var x = seed;
    var done: usize = 0;
    while (done < per) {
        const n = @min(BATCH, per - done);
        genBatch(&buf, &x, dist, n);
        for (buf[0..n]) |key| {
            var i = hashKey(key) & mask;
            var p: usize = 0;
            var min_i = i;
            var min_c: u64 = std.math.maxInt(u64);
            var settled = false;
            while (p < MAXPROBE) : (p += 1) {
                const k = ckeys[i];
                if (k == key) {
                    ccounts[i] += 1;
                    settled = true;
                    break;
                }
                if (k == 0) {
                    ckeys[i] = key;
                    ccounts[i] = 1;
                    settled = true;
                    break;
                }
                if (ccounts[i] < min_c) {
                    min_c = ccounts[i];
                    min_i = i;
                }
                i = (i + 1) & mask;
            }
            if (settled) continue;
            sh.add(ckeys[min_i], ccounts[min_i]); // evict coldest in window
            ckeys[min_i] = key;
            ccounts[min_i] = 1;
        }
        done += n;
    }
    for (ckeys, ccounts) |k, c| {
        if (k != 0) sh.add(k, c);
    }
}

// Reference: zero-contention private tables (each thread its own full table, no
// sharing, no merge). Not correct as a result, but it is the ceiling for "what if
// there were no cross-thread contention at all". scratch holds the private table.
fn workerPrivate(sh: *Shared, per: usize, seed: u64, dist: Dist, scratch: []u8) void {
    _ = sh;
    const slots = scratch.len / 16;
    const mask = slots - 1;
    const keys = std.mem.bytesAsSlice(u64, scratch[0 .. slots * 8]);
    const counts = std.mem.bytesAsSlice(u64, scratch[slots * 8 .. slots * 16]);
    // No memset: fresh page_allocator memory is zeroed; this is a timing ceiling,
    // so we deliberately exclude a 0.5GB clear from the measured region.
    var buf: [BATCH]u64 = undefined;
    var x = seed;
    var done: usize = 0;
    while (done < per) {
        const n = @min(BATCH, per - done);
        genBatch(&buf, &x, dist, n);
        for (buf[0..n]) |key| {
            var i = hashKey(key) & mask;
            while (true) {
                const k = keys[i];
                if (k == key) {
                    counts[i] += 1;
                    break;
                }
                if (k == 0) {
                    keys[i] = key;
                    counts[i] = 1;
                    break;
                }
                i = (i + 1) & mask;
            }
        }
        done += n;
    }
}

const Strategy = struct {
    name: []const u8,
    run: *const fn (*Shared, usize, u64, Dist, []u8) void,
    scratch_bytes: usize, // per-thread scratch
};

const strategies = [_]Strategy{
    .{ .name = "atomic", .run = workerAtomic, .scratch_bytes = 0 },
    .{ .name = "prefetch", .run = workerPrefetch, .scratch_bytes = 0 },
    .{ .name = "combine", .run = workerCombine, .scratch_bytes = 0 }, // scratch set from env
    .{ .name = "assoc4", .run = workerCombineAssoc(4), .scratch_bytes = 0 },
    .{ .name = "assoc8", .run = workerCombineAssoc(8), .scratch_bytes = 0 },
    .{ .name = "assoc16", .run = workerCombineAssoc(16), .scratch_bytes = 0 },
    .{ .name = "combinepin", .run = workerCombinePin, .scratch_bytes = 0 },
    .{ .name = "lp", .run = workerCombineLP, .scratch_bytes = 0 },
    .{ .name = "private", .run = workerPrivate, .scratch_bytes = 0 },
};

// ---------------------------------------------------------------- driver
const TOTAL: usize = 100_000_000;

const Ctx = struct {
    sh: *Shared,
    per: usize,
    seed: u64,
    dist: Dist,
    scratch: []u8,
    run: *const fn (*Shared, usize, u64, Dist, []u8) void,
    fn go(self: @This()) void {
        self.run(self.sh, self.per, self.seed, self.dist, self.scratch);
    }
};

const REPEAT: usize = 3;

fn runOne(allocator: std.mem.Allocator, strat: Strategy, T: usize, dist: Dist, cache_bytes: usize) !void {
    const uses_cache = std.mem.startsWith(u8, strat.name, "combine") or std.mem.startsWith(u8, strat.name, "assoc") or std.mem.eql(u8, strat.name, "lp");
    var sb = if (uses_cache) cache_bytes else strat.scratch_bytes;
    if (std.mem.eql(u8, strat.name, "private")) {
        var cap: usize = 1024;
        const want = (DISTINCT *| 10) / 7 + 1;
        while (cap < want) cap <<= 1;
        sb = cap * 16; // full private {key,count} table per thread
    }
    const scratch = try allocator.alloc(u8, @max(1, sb) * T);
    defer allocator.free(scratch);

    const per = TOTAL / T;
    var best_ms: f64 = std.math.floatMax(f64);
    var groups: u64 = 0;
    var ok = true;
    var rep: usize = 0;
    while (rep < REPEAT) : (rep += 1) {
        var sh = try Shared.init(allocator, DISTINCT);
        defer sh.deinit();
        var threads: [64]std.Thread = undefined;
        const t0 = qpcNow();
        for (threads[0..T], 0..) |*th, i| {
            const sc = scratch[i * sb .. i * sb + sb];
            th.* = try std.Thread.spawn(.{}, Ctx.go, .{Ctx{
                .sh = &sh,
                .per = per,
                .seed = @as(u64, i) *% 0x9e3779b97f4a7c15 +% 1,
                .dist = dist,
                .scratch = sc,
                .run = strat.run,
            }});
        }
        for (threads[0..T]) |th| th.join();
        const ms = msSince(t0);
        if (ms < best_ms) best_ms = ms;
        if (sh.totalCount() != per * T) ok = false;
        groups = sh.n_groups.load(.monotonic);
    }
    const mbps = @as(f64, @floatFromInt(TOTAL)) / (best_ms / 1000.0) / 1e6;
    std.debug.print("  {s:<9} T={d:<2} {s:<7} {d:8.1} ms  {d:6.0} M/s  groups={d:<9} ok={}\n", .{ strat.name, T, @tagName(dist), best_ms, mbps, groups, ok });
}

// ============================================================================
// Radix-lease: partition each thread's rows into chunky buckets by key-hash,
// then threads lease whole buckets from one atomic counter and aggregate each
// bucket exclusively (no write contention on group state, no merge — a key lives
// in exactly one bucket). Work-stealing on the lease counter balances skew.
// ============================================================================
const Buckets = struct {
    // per-(thread, bucket) growable buffer of keys
    bufs: []std.ArrayListUnmanaged(u64),
    n_buckets: usize,
    n_threads: usize,
    allocator: std.mem.Allocator,

    fn init(allocator: std.mem.Allocator, n_threads: usize, n_buckets: usize, reserve_each: usize) !Buckets {
        const bufs = try allocator.alloc(std.ArrayListUnmanaged(u64), n_threads * n_buckets);
        for (bufs) |*b| {
            b.* = .empty;
            try b.ensureTotalCapacity(allocator, reserve_each);
        }
        return .{ .bufs = bufs, .n_buckets = n_buckets, .n_threads = n_threads, .allocator = allocator };
    }
    fn deinit(self: *Buckets) void {
        for (self.bufs) |*b| b.deinit(self.allocator);
        self.allocator.free(self.bufs);
    }
    fn reset(self: *Buckets) void {
        for (self.bufs) |*b| b.clearRetainingCapacity();
    }
    inline fn buf(self: *Buckets, t: usize, b: usize) *std.ArrayListUnmanaged(u64) {
        return &self.bufs[t * self.n_buckets + b];
    }
};

// Bucket of a key: take the TOP hash bits so they're independent of the low bits
// the per-bucket table uses for its slot.
inline fn bucketOf(key: u64, bmask: u64, bshift: u6) usize {
    return @intCast((hashKey(key) >> bshift) & bmask);
}

fn radixPartitionWorker(bk: *Buckets, t: usize, per: usize, seed: u64, dist: Dist, bmask: u64, bshift: u6) void {
    var localbuf: [BATCH]u64 = undefined;
    var x = seed;
    var done: usize = 0;
    while (done < per) {
        const n = @min(BATCH, per - done);
        genBatch(&localbuf, &x, dist, n);
        for (localbuf[0..n]) |key| {
            const b = bucketOf(key, bmask, bshift);
            bk.buf(t, b).appendAssumeCapacity(key);
        }
        done += n;
    }
}

// One cache-line-friendly slot: key, running count, and the epoch stamp that
// lazily clears the reused table between buckets — all read together per probe.
const Slot = struct { key: u64, count: u64, epoch: u64 };

const LeaseAgg = struct {
    bk: *Buckets,
    ctr: *std.atomic.Value(usize),
    slots: []Slot, // reusable per-thread bucket table (epoch-cleared)
    tmask: u64,
    epoch: u64,
    distinct: u64,
    total: u64,

    fn run(self: *LeaseAgg) void {
        const B = self.bk.n_buckets;
        const T = self.bk.n_threads;
        const slots = self.slots;
        while (true) {
            const b = self.ctr.fetchAdd(1, .monotonic);
            if (b >= B) break;
            self.epoch += 1;
            const ep = self.epoch;
            var t: usize = 0;
            while (t < T) : (t += 1) {
                const keys = self.bk.buf(t, b).items;
                self.total += keys.len;
                for (keys) |key| {
                    var i = hashKey(key) & self.tmask;
                    while (true) {
                        const s = &slots[i];
                        if (s.epoch != ep) {
                            s.* = .{ .key = key, .count = 1, .epoch = ep };
                            self.distinct += 1;
                            break;
                        }
                        if (s.key == key) {
                            s.count += 1;
                            break;
                        }
                        i = (i + 1) & self.tmask;
                    }
                }
            }
        }
    }
};

fn runRadixLease(allocator: std.mem.Allocator, T: usize, dist: Dist, n_buckets: usize) !void {
    const per = TOTAL / T;
    // bucket mask/shift (n_buckets must be pow2)
    const bmask = n_buckets - 1;
    const bshift: u6 = 40; // top bits for bucketing
    // per-bucket table size: distinct/bucket with slack, pow2
    const slack = envU("THINDB_SLACK", 4);
    const groups_per_bucket = (DISTINCT / n_buckets) * slack;
    var tcap: usize = 256;
    while (tcap < groups_per_bucket) tcap <<= 1;

    var best_ms: f64 = std.math.floatMax(f64);
    var best_p1: f64 = 0;
    var best_p2: f64 = 0;
    var dgroups: u64 = 0;
    var ok = true;

    // pre-reserve buffers generously to avoid concurrent grows under skew
    const reserve_each = (per / n_buckets) * 3 + 64;
    var bk = try Buckets.init(allocator, T, n_buckets, reserve_each);
    defer bk.deinit();

    // per-thread aggregate tables (reused across repeats), one Slot array each
    const allslots = try allocator.alloc(Slot, tcap * T);
    defer allocator.free(allslots);
    @memset(std.mem.sliceAsBytes(allslots), 0); // epoch 0 = empty

    var rep: usize = 0;
    while (rep < REPEAT) : (rep += 1) {
        bk.reset();
        const t0 = qpcNow();

        // Phase 1: partition (parallel)
        const PCtx = struct {
            bk: *Buckets,
            t: usize,
            per: usize,
            seed: u64,
            dist: Dist,
            bmask: u64,
            bshift: u6,
            fn go(s: @This()) void {
                radixPartitionWorker(s.bk, s.t, s.per, s.seed, s.dist, s.bmask, s.bshift);
            }
        };
        var pthreads: [64]std.Thread = undefined;
        for (0..T) |i| {
            pthreads[i] = try std.Thread.spawn(.{}, PCtx.go, .{PCtx{
                .bk = &bk,
                .t = i,
                .per = per,
                .seed = @as(u64, i) *% 0x9e3779b97f4a7c15 +% 1,
                .dist = dist,
                .bmask = bmask,
                .bshift = bshift,
            }});
        }
        for (pthreads[0..T]) |th| th.join();
        const t_mid = qpcNow();

        // Phase 2: lease + aggregate (parallel)
        var ctr = std.atomic.Value(usize).init(0);
        var aggs = try allocator.alloc(LeaseAgg, T);
        defer allocator.free(aggs);
        for (0..T) |i| {
            aggs[i] = .{
                .bk = &bk,
                .ctr = &ctr,
                .slots = allslots[i * tcap .. i * tcap + tcap],
                .tmask = tcap - 1,
                // Monotonic across repeats AND threads so stale epoch stamps from a
                // prior run never alias a current one. Each (rep,thread) reserves a
                // 4096-wide range; a thread touches <= n_buckets epochs per run.
                .epoch = @as(u64, rep * T + i) *% 4096,
                .distinct = 0,
                .total = 0,
            };
        }
        var athreads: [64]std.Thread = undefined;
        const ACtx = struct {
            a: *LeaseAgg,
            fn go(s: @This()) void {
                s.a.run();
            }
        };
        for (0..T) |i| {
            athreads[i] = try std.Thread.spawn(.{}, ACtx.go, .{ACtx{ .a = &aggs[i] }});
        }
        for (athreads[0..T]) |th| th.join();

        const t_end = qpcNow();
        const freq = @as(f64, @floatFromInt(qpcFreq()));
        const ms = @as(f64, @floatFromInt(t_end - t0)) / freq * 1000.0;
        if (ms < best_ms) {
            best_ms = ms;
            best_p1 = @as(f64, @floatFromInt(t_mid - t0)) / freq * 1000.0;
            best_p2 = @as(f64, @floatFromInt(t_end - t_mid)) / freq * 1000.0;
        }

        var d: u64 = 0;
        var tot: u64 = 0;
        for (aggs) |a| {
            d += a.distinct;
            tot += a.total;
        }
        dgroups = d;
        if (tot != per * T) ok = false;
    }
    const mbps = @as(f64, @floatFromInt(TOTAL)) / (best_ms / 1000.0) / 1e6;
    std.debug.print("  radix(B={d:<4}) T={d:<2} {s:<7} {d:8.1} ms (part={d:6.1} agg={d:6.1})  {d:6.0} M/s  groups={d:<9} ok={}\n", .{ n_buckets, T, @tagName(dist), best_ms, best_p1, best_p2, mbps, dgroups, ok });
}

test "radix" {
    if (getenv("THINDB_BENCH") == null) return error.SkipZigTest;
    if (getenv("THINDB_RADIX") == null) return error.SkipZigTest;
    const allocator = std.heap.page_allocator;
    const T = envU("THINDB_BENCH_T", 12);
    const nb = envU("THINDB_BUCKETS", 64);
    const dist: Dist = if (envStr("THINDB_DIST")) |d| (if (std.mem.eql(u8, d, "skew")) .skew else .uniform) else .skew;

    std.debug.print("\n=== radix-lease (TOTAL={d}, distinct={d}, hot={d}) ===\n", .{ TOTAL, DISTINCT, HOT });
    // reference points in the same invocation (thermal-fair)
    try runOne(allocator, strategies[0], 1, dist, 0); // atomic T=1
    try runOne(allocator, strategies[0], T, dist, 0); // atomic T=12
    try runOne(allocator, strategies[strategies.len - 1], T, dist, 0); // private T=12
    try runRadixLease(allocator, 1, dist, nb);
    try runRadixLease(allocator, T, dist, nb);
}

// Deterministic correctness proof for the radix-lease invariants:
//   1. bucketOf is a stable partition — a key never lands in two buckets.
//   2. Concatenating independent per-bucket aggregates reproduces the exact
//      per-group counts and the exact distinct count.
// This is the property the whole no-merge design rests on. Runs under plain
// `zig test` (not gated), uses the testing allocator for leak detection.
test "radix-lease partition + per-bucket aggregate is exact" {
    const allocator = std.testing.allocator;
    const K: u64 = 1000; // distinct keys 1..=K
    const T: usize = 5; // simulated source threads
    const ROUNDS: u64 = 7; // each thread emits each key ROUNDS times
    const B: usize = 16;
    const bmask: u64 = B - 1;
    const bshift: u6 = 40;

    // Partition: scatter every (thread, round, key) occurrence into its bucket.
    var bufs = try allocator.alloc(std.ArrayListUnmanaged(u64), B);
    defer {
        for (bufs) |*b| b.deinit(allocator);
        allocator.free(bufs);
    }
    for (bufs) |*b| b.* = .empty;
    var t: usize = 0;
    while (t < T) : (t += 1) {
        var r: u64 = 0;
        while (r < ROUNDS) : (r += 1) {
            var k: u64 = 1;
            while (k <= K) : (k += 1) {
                const b = bucketOf(k, bmask, bshift);
                try bufs[b].append(allocator, k);
            }
        }
    }

    // Aggregate each bucket independently, then concatenate. Track which bucket
    // each key was seen in to prove the partition is disjoint.
    var seen_in_bucket = std.AutoHashMap(u64, usize).init(allocator);
    defer seen_in_bucket.deinit();
    var final = std.AutoHashMap(u64, u64).init(allocator);
    defer final.deinit();

    var total: u64 = 0;
    var b: usize = 0;
    while (b < B) : (b += 1) {
        var local = std.AutoHashMap(u64, u64).init(allocator);
        defer local.deinit();
        for (bufs[b].items) |key| {
            const gop = try local.getOrPut(key);
            if (gop.found_existing) gop.value_ptr.* += 1 else gop.value_ptr.* = 1;
            total += 1;
        }
        var it = local.iterator();
        while (it.next()) |e| {
            // partition invariant: a key must not have appeared in another bucket
            const sop = try seen_in_bucket.getOrPut(e.key_ptr.*);
            if (sop.found_existing) {
                try std.testing.expectEqual(b, sop.value_ptr.*);
            } else sop.value_ptr.* = b;
            // concatenate (no merge needed — disjoint keys across buckets)
            try std.testing.expect(!final.contains(e.key_ptr.*));
            try final.put(e.key_ptr.*, e.value_ptr.*);
        }
    }

    try std.testing.expectEqual(@as(u64, T * ROUNDS * K), total);
    try std.testing.expectEqual(@as(usize, K), final.count());
    var k: u64 = 1;
    while (k <= K) : (k += 1) {
        try std.testing.expectEqual(@as(u64, T * ROUNDS), final.get(k).?);
    }
}

test "radixscale" {
    if (getenv("THINDB_BENCH") == null) return error.SkipZigTest;
    if (getenv("THINDB_SCALE") == null) return error.SkipZigTest;
    const allocator = std.heap.page_allocator;
    const nb = envU("THINDB_BUCKETS", 2048);
    const dist: Dist = if (envStr("THINDB_DIST")) |d| (if (std.mem.eql(u8, d, "skew")) .skew else .uniform) else .uniform;
    std.debug.print("\n=== radix-lease scaling curve (B={d}, {s}) — full time incl. partition ===\n", .{ nb, @tagName(dist) });
    try runOne(allocator, strategies[0], 1, dist, 0); // naive serial hash baseline
    const Ts = [_]usize{ 1, 2, 3, 4, 6, 8, 10, 12 };
    for (Ts) |t| try runRadixLease(allocator, t, dist, nb);
}

test "sweep" {
    if (getenv("THINDB_BENCH") == null) return error.SkipZigTest;
    const allocator = std.heap.page_allocator;

    const cache_bytes = envU("THINDB_CACHE_BYTES", 512 * 1024); // per-thread combine cache
    const only = envStr("THINDB_STRAT");
    const Tonly = envU("THINDB_BENCH_T", 0);

    const dists = [_]Dist{ .uniform, .skew };
    const dist_only = envStr("THINDB_DIST");

    std.debug.print("\n=== shared-table sweep (TOTAL={d}, distinct={d}, hot={d}, combine-cache={d}KB/thread) ===\n", .{ TOTAL, DISTINCT, HOT, cache_bytes / 1024 });
    for (strategies) |strat| {
        if (only) |o| if (!std.mem.eql(u8, o, strat.name)) continue;
        for (dists) |dist| {
            if (dist_only) |d| if (!std.mem.eql(u8, d, @tagName(dist))) continue;
            if (Tonly != 0) {
                try runOne(allocator, strat, Tonly, dist, cache_bytes);
            } else {
                try runOne(allocator, strat, 1, dist, cache_bytes);
                try runOne(allocator, strat, 12, dist, cache_bytes);
            }
        }
    }
}
