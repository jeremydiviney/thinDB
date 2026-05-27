//! Isolated microbench for the high-cardinality GROUP BY kernels where Q08/Q32
//! lose to DuckDB. NO pipeline confounders: no scan, no decompression, no
//! memtable, no wire. Synthetic input columns in memory, fed through the FULL
//! aggregate kernel — key build + probe + the heavy per-group state work the
//! real operator does (the `counts[gid]` bump for COUNT(DISTINCT); the gstate
//! accumulator read-modify-write for COUNT/SUM/AVG) — timed.
//!
//! Two faithful families, each calibrated against the profiler:
//!   Q08  RegionID, COUNT(DISTINCT UserID)  — combined-distinct set + counts[gid]
//!   Q32  WatchID, ClientIP, COUNT/SUM/AVG  — group table + 96-B/group gstate
//! The production baseline (E0) mirrors aggregate.zig; idea variants live here.
//! Every experiment returns a checksum of the per-group results — all variants
//! in a family must agree, or the kernel is wrong, not faster.
//!
//! A third block, "Q32 REAL OPERATOR", drives the actual `exec.Aggregate`
//! operator over a synthetic in-memory source (no scan/decompress/wire) so the
//! bench reproduces the profiler's ~335 ms and we can A/B real operator-level
//! changes (compact state, fewer reserves, leaner emit) against it — the lean
//! E0 kernel above plateaus at ~120 ms, so the missing 200 ms lives in the
//! operator machinery, which only the real operator exercises.
//!
//! Timing uses the Windows perf counter directly (this project routes clocks
//! through Io; std.time exposes only constants — see util/prof.zig).
//!
//! Run:  zig build gbmicro -Doptimize=ReleaseFast

const std = @import("std");
const win = std.os.windows;

const thindb = @import("thindb");
const texec = thindb.exec;
const gt = texec.group_table;
const ra = texec.radix_aggregate;
const IntTable96 = gt.IntKeyTable(96);
const Column = thindb.types.Column;
const ColumnView = thindb.storage.ColumnView;

const PF: usize = 12;
const REPEAT: usize = 5;
const N: usize = 5_000_000;

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
fn scatter(i: u64) u64 {
    var z = (i +% 1) *% 0x9E3779B97F4A7C15;
    z = (z ^ (z >> 30)) *% 0xBF58476D1CE4E5B9;
    z = (z ^ (z >> 27)) *% 0x94D049BB133111EB;
    return z ^ (z >> 31);
}

const Result = struct { ticks: i64, cksum: u64 };

// ===========================================================================
// Q08 — RegionID, COUNT(DISTINCT UserID)
//   key = (region_gid << 64) | userid  → combined-distinct membership set;
//   on a first sighting bump counts[region_gid]. Result = the per-region
//   distinct counts. Each UserID maps to one region, so distinct pairs ≈ users.
// ===========================================================================
const Q08 = struct {
    region: []u32, // group gid (RegionID), low-card
    userid: []u64, // high-card distinct value
    n_regions: usize,

    fn gen(a: std.mem.Allocator, n_regions: usize, n_users: usize) !Q08 {
        const region = try a.alloc(u32, N);
        const userid = try a.alloc(u64, N);
        for (region, userid, 0..) |*rg, *u, i| {
            const uid = scatter(@intCast(i % n_users));
            u.* = uid;
            rg.* = @intCast(uid % n_regions); // region is a function of the user
        }
        var rng = std.Random.DefaultPrng.init(0xC0FFEE);
        // shuffle rows together (same permutation) so probes hit in random order
        const r = rng.random();
        var i: usize = N;
        while (i > 1) {
            i -= 1;
            const j = r.uintLessThan(usize, i + 1);
            std.mem.swap(u32, &region[i], &region[j]);
            std.mem.swap(u64, &userid[i], &userid[j]);
        }
        return .{ .region = region, .userid = userid, .n_regions = n_regions };
    }
    fn free(self: Q08, a: std.mem.Allocator) void {
        a.free(self.region);
        a.free(self.userid);
    }
    fn cksum(counts: []const u64) u64 {
        var s: u64 = 0;
        for (counts, 0..) |c, i| s ^= c *% (@as(u64, @intCast(i)) +% 0x100000001b3);
        return s;
    }
};

/// E0 — current combined-distinct: IntTable96 set keyed on pack(gid,userid),
/// counts[gid] bumped on first sighting (mirrors accumulateCombinedDistinct).
fn q08_intTable96(a: std.mem.Allocator, d: Q08, hashes: []u64) !Result {
    var t = try IntTable96.init(a, N);
    defer t.deinit(a);
    const counts = try a.alloc(u64, d.n_regions);
    defer a.free(counts);
    @memset(counts, 0);
    const t0 = nowTicks();
    for (0..N) |i| {
        const key = (@as(u128, d.region[i]) << 64) | d.userid[i];
        hashes[i] = IntTable96.hashKey(key);
    }
    var gid: u32 = 0;
    for (0..N) |i| {
        if (i + PF < N) @prefetch(t.slotAddr(t.bucketOf(hashes[i + PF])), .{ .rw = .write, .locality = 1 });
        const key = (@as(u128, d.region[i]) << 64) | d.userid[i];
        const p = t.getOrPut(hashes[i], key);
        if (!p.found) {
            t.commit(p.slot, key, gid);
            gid += 1;
            counts[d.region[i]] += 1;
        }
    }
    return .{ .ticks = nowTicks() - t0, .cksum = Q08.cksum(counts) };
}

const q08_experiments = [_]struct {
    name: []const u8,
    run: *const fn (std.mem.Allocator, Q08, []u64) anyerror!Result,
}{
    .{ .name = "E0 IntTable96 combined-distinct", .run = q08_intTable96 },
};

// ===========================================================================
// Q32 — WatchID, ClientIP, COUNT(*), SUM(IsRefresh), AVG(ResolutionWidth)
//   group key → gid (IntTable96), then per-group gstate (96 B = 3 × 32-B
//   accumulators, contiguous, like aggregate.zig's gid*n_aggs+ai) read-modify-
//   written every row. That ~480-MB random RMW is the real bottleneck.
// ===========================================================================
const Q32 = struct {
    watchid: []i64, // group key col 0 (bigint), near-unique
    clientip: []i32, // group key col 1 (int)
    isref: []i16, // SUM input (smallint, 0/1)
    reswidth: []i16, // AVG input (smallint, 0..2559)

    fn gen(a: std.mem.Allocator, n_groups: usize) !Q32 {
        const watchid = try a.alloc(i64, N);
        const clientip = try a.alloc(i32, N);
        const isref = try a.alloc(i16, N);
        const reswidth = try a.alloc(i16, N);
        for (watchid, clientip, isref, reswidth, 0..) |*wid, *cip, *r, *w, i| {
            const s = scatter(@intCast(i % n_groups));
            wid.* = @bitCast(s);
            cip.* = @bitCast(@as(u32, @truncate(scatter(s)))); // distinct per group → ~n_groups pairs
            r.* = @intCast(s & 1);
            w.* = @intCast(s % 2560);
        }
        var rng = std.Random.DefaultPrng.init(0xBEEF);
        const rnd = rng.random();
        var i: usize = N;
        while (i > 1) {
            i -= 1;
            const j = rnd.uintLessThan(usize, i + 1);
            std.mem.swap(i64, &watchid[i], &watchid[j]);
            std.mem.swap(i32, &clientip[i], &clientip[j]);
            std.mem.swap(i16, &isref[i], &isref[j]);
            std.mem.swap(i16, &reswidth[i], &reswidth[j]);
        }
        return .{ .watchid = watchid, .clientip = clientip, .isref = isref, .reswidth = reswidth };
    }
    fn free(self: Q32, a: std.mem.Allocator) void {
        a.free(self.watchid);
        a.free(self.clientip);
        a.free(self.isref);
        a.free(self.reswidth);
    }
    fn keyOf(self: Q32, i: usize) u128 {
        const wid: u64 = @bitCast(self.watchid[i]);
        const cip: u32 = @bitCast(self.clientip[i]);
        return (@as(u128, cip) << 64) | wid;
    }
};

const BATCH: usize = 8192; // per-batch chunk, like the real scan→agg handoff

/// BUILD-ONLY — the irreducible floor: iterate all 5M rows, build the key, hash,
/// probe the group table, and commit a new group on a miss. NO accumulator state,
/// NO scatter, NO emit. This is the minimum any hash GROUP BY must pay just to
/// discover the groups. Batched key-build + look-ahead-prefetched probe (the
/// optimized form). Checksum = group count (≈5M).
fn q32_build_only(a: std.mem.Allocator, d: Q32, _: []u64) !Result {
    var t = try IntTable96.init(a, N);
    defer t.deinit(a);
    const kb = try a.alloc(u128, BATCH);
    defer a.free(kb);
    const hb = try a.alloc(u64, BATCH);
    defer a.free(hb);

    const t0 = nowTicks();
    var gid: u32 = 0;
    var off: usize = 0;
    while (off < N) : (off += BATCH) {
        const m = @min(BATCH, N - off);
        for (0..m) |j| {
            const key = d.keyOf(off + j);
            kb[j] = key;
            hb[j] = IntTable96.hashKey(key);
        }
        for (0..m) |j| {
            if (j + PF < m) @prefetch(t.slotAddr(t.bucketOf(hb[j + PF])), .{ .rw = .write, .locality = 1 });
            const p = t.getOrPut(hb[j], kb[j]);
            if (!p.found) {
                t.commit(p.slot, kb[j], gid);
                gid += 1;
            }
        }
    }
    return .{ .ticks = nowTicks() - t0, .cksum = gid };
}

fn lessU128(_: void, x: u128, y: u128) bool {
    return x < y;
}
/// BUILD on pre-materialized keys (key-build NOT timed) — times only the
/// single-table hash work (hash + probe + commit). `sorted` sorts the keys by
/// value first (also NOT timed). Isolates whether feeding the hash table sorted
/// keys helps: for near-unique keys the hash scatters sorted keys to random
/// buckets, so adjacency in the input doesn't become adjacency in the table.
fn q32BuildPrebuilt(a: std.mem.Allocator, d: Q32, comptime sorted: bool) !Result {
    const keys = try a.alloc(u128, N);
    defer a.free(keys);
    for (0..N) |i| keys[i] = d.keyOf(i);
    if (sorted) std.sort.pdq(u128, keys, {}, lessU128);
    var t = try IntTable96.init(a, N);
    defer t.deinit(a);
    const hb = try a.alloc(u64, BATCH);
    defer a.free(hb);

    const t0 = nowTicks();
    var gid: u32 = 0;
    var off: usize = 0;
    while (off < N) : (off += BATCH) {
        const m = @min(BATCH, N - off);
        for (0..m) |j| hb[j] = IntTable96.hashKey(keys[off + j]);
        for (0..m) |j| {
            if (j + PF < m) @prefetch(t.slotAddr(t.bucketOf(hb[j + PF])), .{ .rw = .write, .locality = 1 });
            const p = t.getOrPut(hb[j], keys[off + j]);
            if (!p.found) {
                t.commit(p.slot, keys[off + j], gid);
                gid += 1;
            }
        }
    }
    return .{ .ticks = nowTicks() - t0, .cksum = gid };
}
fn q32_build_keys_unsorted(a: std.mem.Allocator, d: Q32, _: []u64) !Result {
    return q32BuildPrebuilt(a, d, false);
}
fn q32_build_keys_sorted(a: std.mem.Allocator, d: Q32, _: []u64) !Result {
    return q32BuildPrebuilt(a, d, true);
}

/// BUILD on a controllable cardinality — `n_groups` distinct keys over N rows
/// (≈ N/n_groups rows per group). Keys gen'd + shuffled + optionally sorted, all
/// NOT timed; times only the single-table hash build. Tests whether sorting helps
/// when keys actually REPEAT: sorted duplicates are adjacent, so after the first
/// probe warms a group's bucket the rest hit it hot. Table sized to n_groups.
fn q32BuildCard(a: std.mem.Allocator, comptime n_groups: usize, comptime sorted: bool) !Result {
    const keys = try a.alloc(u128, N);
    defer a.free(keys);
    for (0..N) |i| {
        const wid = scatter(@intCast(i % n_groups));
        const cip: u64 = @truncate(scatter(wid));
        keys[i] = (@as(u128, @as(u32, @truncate(cip))) << 64) | wid;
    }
    var rng = std.Random.DefaultPrng.init(0xABCDEF);
    const r = rng.random();
    var s: usize = N;
    while (s > 1) {
        s -= 1;
        std.mem.swap(u128, &keys[s], &keys[r.uintLessThan(usize, s + 1)]);
    }
    if (sorted) std.sort.pdq(u128, keys, {}, lessU128);
    var t = try IntTable96.init(a, n_groups + n_groups / 4);
    defer t.deinit(a);
    const hb = try a.alloc(u64, BATCH);
    defer a.free(hb);

    const t0 = nowTicks();
    var gid: u32 = 0;
    var off: usize = 0;
    while (off < N) : (off += BATCH) {
        const m = @min(BATCH, N - off);
        for (0..m) |j| hb[j] = IntTable96.hashKey(keys[off + j]);
        for (0..m) |j| {
            if (j + PF < m) @prefetch(t.slotAddr(t.bucketOf(hb[j + PF])), .{ .rw = .write, .locality = 1 });
            const p = t.getOrPut(hb[j], keys[off + j]);
            if (!p.found) {
                t.commit(p.slot, keys[off + j], gid);
                gid += 1;
            }
        }
    }
    return .{ .ticks = nowTicks() - t0, .cksum = gid };
}
fn q32_card1m_unsorted(a: std.mem.Allocator, _: Q32, _: []u64) !Result {
    return q32BuildCard(a, 1_000_000, false);
}
fn q32_card1m_sorted(a: std.mem.Allocator, _: Q32, _: []u64) !Result {
    return q32BuildCard(a, 1_000_000, true);
}
fn q32_card5m_unsorted(a: std.mem.Allocator, _: Q32, _: []u64) !Result {
    return q32BuildCard(a, 5_000_000, false);
}
fn q32_card5m_sorted(a: std.mem.Allocator, _: Q32, _: []u64) !Result {
    return q32BuildCard(a, 5_000_000, true);
}

/// BUILD+4ACC — build-only plus four per-group u64 accumulators (32 B/group =
/// one cache line), all updated inline the moment the group is resolved (fused,
/// single touch of the group's state line per row — not separate scatter passes).
/// Models count/sum/avg/avg-count with the real isref/reswidth values. The delta
/// vs BUILD-ONLY is the cost of maintaining 4 accumulators on a compact state.
fn q32_build_acc4(a: std.mem.Allocator, d: Q32, _: []u64) !Result {
    var t = try IntTable96.init(a, N);
    defer t.deinit(a);
    const gstate = try a.alloc(u64, N * 4); // 4 u64 / group = 32 B, one line
    defer a.free(gstate);
    const kb = try a.alloc(u128, BATCH);
    defer a.free(kb);
    const hb = try a.alloc(u64, BATCH);
    defer a.free(hb);

    const t0 = nowTicks();
    var gid: u32 = 0;
    var off: usize = 0;
    while (off < N) : (off += BATCH) {
        const m = @min(BATCH, N - off);
        for (0..m) |j| {
            const key = d.keyOf(off + j);
            kb[j] = key;
            hb[j] = IntTable96.hashKey(key);
        }
        for (0..m) |j| {
            if (j + PF < m) @prefetch(t.slotAddr(t.bucketOf(hb[j + PF])), .{ .rw = .write, .locality = 1 });
            const p = t.getOrPut(hb[j], kb[j]);
            const g = if (p.found) p.gid else blk: {
                t.commit(p.slot, kb[j], gid);
                const ng = gid;
                gid += 1;
                gstate[@as(usize, ng) * 4 + 0] = 0;
                gstate[@as(usize, ng) * 4 + 1] = 0;
                gstate[@as(usize, ng) * 4 + 2] = 0;
                gstate[@as(usize, ng) * 4 + 3] = 0;
                break :blk ng;
            };
            const b = @as(usize, g) * 4;
            gstate[b + 0] += 1; // count
            gstate[b + 1] += @as(u64, @intCast(d.isref[off + j])); // sum
            gstate[b + 2] += @as(u64, @intCast(d.reswidth[off + j])); // avg sum
            gstate[b + 3] += 1; // avg count
        }
    }
    return .{ .ticks = nowTicks() - t0, .cksum = gid };
}

/// BUILD+4ACC(real) — like BUILD+4ACC but the accumulators are the real types
/// the aggregator uses: COUNT u64, SUM i128 (overflow-safe), AVG f64 sum + u64
/// count. State is 48 B/group (i128 forces 16-align) vs the 32 B all-u64 version
/// — shows the cost of the realistic accumulator widths + float/i128 arithmetic.
const RAcc = struct { sum: i128 = 0, count: u64 = 0, avg_sum: f64 = 0, avg_cnt: u64 = 0 };
fn q32_build_acc4_real(a: std.mem.Allocator, d: Q32, _: []u64) !Result {
    var t = try IntTable96.init(a, N);
    defer t.deinit(a);
    const gstate = try a.alloc(RAcc, N);
    defer a.free(gstate);
    const kb = try a.alloc(u128, BATCH);
    defer a.free(kb);
    const hb = try a.alloc(u64, BATCH);
    defer a.free(hb);

    const t0 = nowTicks();
    var gid: u32 = 0;
    var off: usize = 0;
    while (off < N) : (off += BATCH) {
        const m = @min(BATCH, N - off);
        for (0..m) |j| {
            const key = d.keyOf(off + j);
            kb[j] = key;
            hb[j] = IntTable96.hashKey(key);
        }
        for (0..m) |j| {
            if (j + PF < m) @prefetch(t.slotAddr(t.bucketOf(hb[j + PF])), .{ .rw = .write, .locality = 1 });
            const p = t.getOrPut(hb[j], kb[j]);
            const g = if (p.found) p.gid else blk: {
                t.commit(p.slot, kb[j], gid);
                const ng = gid;
                gid += 1;
                gstate[ng] = .{};
                break :blk ng;
            };
            const s = &gstate[g];
            s.count += 1;
            s.sum += @as(i128, d.isref[off + j]);
            s.avg_sum += @as(f64, @floatFromInt(d.reswidth[off + j]));
            s.avg_cnt += 1;
        }
    }
    return .{ .ticks = nowTicks() - t0, .cksum = gid };
}

/// E0 — the FULL current high-card GROUP BY, modeled in isolation: batched
/// accumulate (build key + hash into cache-resident per-batch buffers, then
/// look-ahead-prefetch probe), per-new-group key storage (`gkeys`) + 96-B state
/// init, the 3-aggregate gstate scatter, AND the emit (a top-10 walk over every
/// group + finalize of the survivors). Mirrors the aggregate.zig groupByTopK
/// path; everything is timed, like the real Aggregate operator.
fn q32_full(a: std.mem.Allocator, d: Q32, _: []u64) !Result {
    var t = try IntTable96.init(a, N);
    defer t.deinit(a);
    const gstate = try a.alloc(u64, N * 12); // 96 B/group; init per-group below
    defer a.free(gstate);
    const gkeys = try a.alloc(u128, N); // per-group key store, for emit decode
    defer a.free(gkeys);
    const kb = try a.alloc(u128, BATCH);
    defer a.free(kb);
    const hb = try a.alloc(u64, BATCH);
    defer a.free(hb);

    const gidbuf = try a.alloc(u32, BATCH); // resolved gids for the batch
    defer a.free(gidbuf);

    const t0 = nowTicks();
    var gid: u32 = 0;
    var off: usize = 0;
    while (off < N) : (off += BATCH) {
        const m = @min(BATCH, N - off);
        // phase a: build key + hash into cache-resident batch buffers
        for (0..m) |j| {
            const key = d.keyOf(off + j);
            kb[j] = key;
            hb[j] = IntTable96.hashKey(key);
        }
        // phase b: resolve gids (prefetch-pipelined); new group → store key + init state
        for (0..m) |j| {
            if (j + PF < m) @prefetch(t.slotAddr(t.bucketOf(hb[j + PF])), .{ .rw = .write, .locality = 1 });
            const p = t.getOrPut(hb[j], kb[j]);
            if (p.found) {
                gidbuf[j] = p.gid;
            } else {
                t.commit(p.slot, kb[j], gid);
                gkeys[gid] = kb[j];
                @memset(gstate[@as(usize, gid) * 12 ..][0..12], 0); // initialState
                gidbuf[j] = gid;
                gid += 1;
            }
        }
        // 3 separate vectorized scatter passes (mirrors scatterCount/Sum/Avg) —
        // each re-reads gids + its column and re-touches the gstate slots.
        for (0..m) |j| gstate[@as(usize, gidbuf[j]) * 12 + 0] += 1; // COUNT(*)
        for (0..m) |j| gstate[@as(usize, gidbuf[j]) * 12 + 4] += @as(u64, @intCast(d.isref[off + j])); // SUM
        for (0..m) |j| {
            const b = @as(usize, gidbuf[j]) * 12;
            gstate[b + 8] += @as(u64, @intCast(d.reswidth[off + j])); // AVG sum
            gstate[b + 9] += 1; // AVG count
        }
    }

    // Emit: top-10 by COUNT over every group (the O(groups) walk), then finalize
    // the survivors (decode key → WatchID/ClientIP, finalize AVG).
    var top_cnt = [_]u64{0} ** 10;
    var top_gid = [_]usize{0} ** 10;
    for (0..gid) |g| {
        const cnt = gstate[g * 12];
        var mi: usize = 0;
        for (1..10) |k| {
            if (top_cnt[k] < top_cnt[mi]) mi = k;
        }
        if (cnt > top_cnt[mi]) {
            top_cnt[mi] = cnt;
            top_gid[mi] = g;
        }
    }
    var ck: u64 = 0;
    for (0..10) |slot| {
        const g = top_gid[slot];
        const key = gkeys[g];
        const wid: u64 = @truncate(key);
        const cip: u32 = @truncate(key >> 64);
        const avg: u64 = if (gstate[g * 12 + 9] > 0) gstate[g * 12 + 8] / gstate[g * 12 + 9] else 0;
        ck ^= wid ^ cip ^ gstate[g * 12] ^ gstate[g * 12 + 4] ^ avg;
    }
    return .{ .ticks = nowTicks() - t0, .cksum = ck };
}

/// ER — radix-partitioned GROUP BY. The single-table E0 is latency-bound: the
/// 8M-slot table + 480-MB gstate dwarf the 32-MB L3, so every probe/scatter is
/// a DRAM miss. Here we first partition rows by the TOP `PBITS` of the key hash
/// into `P = 2^PBITS` buckets (one cache-line-resident write head each), then
/// aggregate each partition independently — its table + state now fits cache, so
/// the probe/scatter hit L2/L3 instead of DRAM. Each group lives in exactly one
/// partition (same key → same hash → same bucket), so there is no cross-partition
/// merge; the global top-10 is just the running max over every partition's groups.
/// Passes: (a) hash+count, (b) prefix-sum→offsets, (c) scatter rows into per-
/// partition arrays, (d) per-partition probe+scatter+emit. One reusable table +
/// state buffer, cleared between partitions.
fn q32RadixImpl(a: std.mem.Allocator, d: Q32, comptime PBITS: u8) !Result {
    const P: usize = @as(usize, 1) << @as(u6, @intCast(PBITS));
    const SHIFT: u6 = @intCast(64 - @as(usize, PBITS)); // partition = top PBITS of the 64-bit hash
    const partcap = (N / P) * 7 / 5 + 64; // generous per-partition group bound

    const srch = try a.alloc(u64, N);
    defer a.free(srch);
    const pkey = try a.alloc(u128, N);
    defer a.free(pkey);
    const phash = try a.alloc(u64, N);
    defer a.free(phash);
    const pref = try a.alloc(i16, N);
    defer a.free(pref);
    const pres = try a.alloc(i16, N);
    defer a.free(pres);
    const off = try a.alloc(usize, P + 1);
    defer a.free(off);
    const cur = try a.alloc(usize, P);
    defer a.free(cur);
    var t = try IntTable96.init(a, partcap);
    defer t.deinit(a);
    const gstate = try a.alloc(u64, (partcap + 1) * 12);
    defer a.free(gstate);
    const gkeys = try a.alloc(u128, partcap + 1);
    defer a.free(gkeys);

    const t0 = nowTicks();
    // (a) hash + count per partition.
    @memset(off, 0);
    for (0..N) |i| {
        const h = IntTable96.hashKey(d.keyOf(i));
        srch[i] = h;
        off[(h >> SHIFT) + 1] += 1;
    }
    // (b) prefix-sum the counts (shifted by one) into start offsets.
    for (1..P + 1) |p| off[p] += off[p - 1];
    @memcpy(cur, off[0..P]);
    // (c) scatter each row into its partition's slice.
    for (0..N) |i| {
        const p = srch[i] >> SHIFT;
        const j = cur[p];
        cur[p] += 1;
        pkey[j] = d.keyOf(i);
        phash[j] = srch[i];
        pref[j] = d.isref[i];
        pres[j] = d.reswidth[i];
    }
    // (d) aggregate each (now cache-resident) partition independently.
    var top_cnt = [_]u64{0} ** 10;
    var top_key = [_]u128{0} ** 10;
    var top_sum = [_]u64{0} ** 10;
    var top_avg = [_]u64{0} ** 10;
    var total_groups: u64 = 0;
    var total_count: u64 = 0;
    for (0..P) |p| {
        const lo = off[p];
        const hi = off[p + 1];
        for (t.slots) |*s| s.gid = gt.EMPTY;
        var gid: u32 = 0;
        var i = lo;
        while (i < hi) : (i += 1) {
            if (i + PF < hi) @prefetch(t.slotAddr(t.bucketOf(phash[i + PF])), .{ .rw = .write, .locality = 1 });
            const probe = t.getOrPut(phash[i], pkey[i]);
            const g = if (probe.found) probe.gid else blk: {
                t.commit(probe.slot, pkey[i], gid);
                gkeys[gid] = pkey[i];
                @memset(gstate[@as(usize, gid) * 12 ..][0..12], 0);
                const ng = gid;
                gid += 1;
                break :blk ng;
            };
            const b = @as(usize, g) * 12;
            gstate[b + 0] += 1;
            gstate[b + 4] += @as(u64, @intCast(pref[i]));
            gstate[b + 8] += @as(u64, @intCast(pres[i]));
            gstate[b + 9] += 1;
        }
        total_groups += gid;
        for (0..gid) |g| {
            const b = g * 12;
            const cnt = gstate[b];
            total_count += cnt;
            var mi: usize = 0;
            for (1..10) |k| {
                if (top_cnt[k] < top_cnt[mi]) mi = k;
            }
            if (cnt > top_cnt[mi]) {
                top_cnt[mi] = cnt;
                top_key[mi] = gkeys[g];
                top_sum[mi] = gstate[b + 4];
                top_avg[mi] = if (gstate[b + 9] > 0) gstate[b + 8] / gstate[b + 9] else 0;
            }
        }
    }
    const ticks = nowTicks() - t0;
    // Checksum the algorithm invariants only — total distinct groups and the
    // total COUNT(*) (must equal N) — so every P variant agrees regardless of
    // top-10 tie-break order (all groups have count 1 in this data, so the
    // chosen 10 are arbitrary). top_* are kept correct by construction.
    _ = .{ top_key, top_sum, top_avg };
    const ck: u64 = total_groups *% 0x100000001b3 ^ total_count;
    return .{ .ticks = ticks, .cksum = ck };
}

/// ER1 — one-pass radix. Drops E R's separate count + prefix-sum passes: each
/// partition gets a fixed over-allocated stride, so a single pass hashes each
/// key once and scatters it straight into its bucket (no `srch` array, no key
/// recompute). Trades ~1.5× partition memory for one fewer pass over 5M rows.
fn q32Radix1PassImpl(a: std.mem.Allocator, d: Q32, comptime PBITS: u8) !Result {
    const P: usize = @as(usize, 1) << @as(u6, @intCast(PBITS));
    const SHIFT: u6 = @intCast(64 - @as(usize, PBITS));
    const stride = (N / P) * 3 / 2 + 64; // generous fixed per-partition capacity
    const partcap = (N / P) * 7 / 5 + 64;

    const pkey = try a.alloc(u128, P * stride);
    defer a.free(pkey);
    const phash = try a.alloc(u64, P * stride);
    defer a.free(phash);
    const pref = try a.alloc(i16, P * stride);
    defer a.free(pref);
    const pres = try a.alloc(i16, P * stride);
    defer a.free(pres);
    const used = try a.alloc(usize, P);
    defer a.free(used);
    var t = try IntTable96.init(a, partcap);
    defer t.deinit(a);
    const gstate = try a.alloc(u64, (partcap + 1) * 12);
    defer a.free(gstate);
    const gkeys = try a.alloc(u128, partcap + 1);
    defer a.free(gkeys);

    const t0 = nowTicks();
    @memset(used, 0);
    for (0..N) |i| {
        const key = d.keyOf(i);
        const h = IntTable96.hashKey(key);
        const p = h >> SHIFT;
        const j = p * stride + used[p];
        used[p] += 1;
        pkey[j] = key;
        phash[j] = h;
        pref[j] = d.isref[i];
        pres[j] = d.reswidth[i];
    }
    var total_groups: u64 = 0;
    var total_count: u64 = 0;
    for (0..P) |p| {
        const lo = p * stride;
        const hi = lo + used[p];
        for (t.slots) |*s| s.gid = gt.EMPTY;
        var gid: u32 = 0;
        var i = lo;
        while (i < hi) : (i += 1) {
            if (i + PF < hi) @prefetch(t.slotAddr(t.bucketOf(phash[i + PF])), .{ .rw = .write, .locality = 1 });
            const probe = t.getOrPut(phash[i], pkey[i]);
            const g = if (probe.found) probe.gid else blk: {
                t.commit(probe.slot, pkey[i], gid);
                gkeys[gid] = pkey[i];
                @memset(gstate[@as(usize, gid) * 12 ..][0..12], 0);
                const ng = gid;
                gid += 1;
                break :blk ng;
            };
            const b = @as(usize, g) * 12;
            gstate[b + 0] += 1;
            gstate[b + 4] += @as(u64, @intCast(pref[i]));
            gstate[b + 8] += @as(u64, @intCast(pres[i]));
            gstate[b + 9] += 1;
        }
        total_groups += gid;
        for (0..gid) |g| total_count += gstate[g * 12];
    }
    const ck: u64 = total_groups *% 0x100000001b3 ^ total_count;
    return .{ .ticks = nowTicks() - t0, .cksum = ck };
}
fn q32_radix1p64(a: std.mem.Allocator, d: Q32, _: []u64) !Result {
    return q32Radix1PassImpl(a, d, 6);
}
fn q32_radix1p128(a: std.mem.Allocator, d: Q32, _: []u64) !Result {
    return q32Radix1PassImpl(a, d, 7);
}
fn q32_radix1p256(a: std.mem.Allocator, d: Q32, _: []u64) !Result {
    return q32Radix1PassImpl(a, d, 8);
}

/// Distribution probe: run the 2-pass radix COUNT pass (P=128) and dump the
/// per-bucket row-count distribution — mean / min / max / stddev plus the raw
/// 128 counts. This is the histogram the two-pass path gets for free and would
/// use to decide which buckets need adaptive 2nd-level recursion. Prints once.
var g_dist_printed = false;
fn q32_radix_dist(a: std.mem.Allocator, d: Q32, _: []u64) !Result {
    const PBITS = 7;
    const P: usize = @as(usize, 1) << PBITS;
    const SHIFT: u6 = 64 - PBITS;
    const counts = try a.alloc(usize, P);
    defer a.free(counts);
    @memset(counts, 0);
    const t0 = nowTicks();
    for (0..N) |i| counts[IntTable96.hashKey(d.keyOf(i)) >> SHIFT] += 1;
    const ticks = nowTicks() - t0;
    if (!g_dist_printed) {
        g_dist_printed = true;
        var mn: usize = std.math.maxInt(usize);
        var mx: usize = 0;
        var total: u64 = 0;
        for (counts) |c| {
            if (c < mn) mn = c;
            if (c > mx) mx = c;
            total += c;
        }
        const mean: f64 = @as(f64, @floatFromInt(total)) / @as(f64, @floatFromInt(P));
        var sq: f64 = 0;
        for (counts) |c| {
            const dv = @as(f64, @floatFromInt(c)) - mean;
            sq += dv * dv;
        }
        const stddev = std.math.sqrt(sq / @as(f64, @floatFromInt(P)));
        std.debug.print("\n[2-pass radix bucket distribution]  P={d}  N={d}\n", .{ P, N });
        std.debug.print("  mean={d:.1}  min={d}  max={d}  stddev={d:.1}  spread max/mean={d:.4}  min/mean={d:.4}\n", .{
            mean, mn, mx, stddev,
            @as(f64, @floatFromInt(mx)) / mean,
            @as(f64, @floatFromInt(mn)) / mean,
        });
        std.debug.print("  per-bucket row counts (16/line):\n", .{});
        for (counts, 0..) |c, idx| {
            std.debug.print(" {d:>6}", .{c});
            if ((idx + 1) % 16 == 0) std.debug.print("\n", .{});
        }
    }
    return .{ .ticks = ticks, .cksum = @as(u64, N) };
}

/// ER-acc4 — 2-pass radix with the compact 32 B fused accumulator (same as
/// BUILD+4ACC, but the build+accumulate runs per cache-resident partition). No
/// emit; checksum = invariants.
fn q32RadixAcc4(a: std.mem.Allocator, d: Q32, comptime PBITS: u8) !Result {
    const P: usize = @as(usize, 1) << @as(u6, @intCast(PBITS));
    const SHIFT: u6 = @intCast(64 - @as(usize, PBITS));
    const partcap = (N / P) * 7 / 5 + 64;
    const srch = try a.alloc(u64, N);
    defer a.free(srch);
    const pkey = try a.alloc(u128, N);
    defer a.free(pkey);
    const phash = try a.alloc(u64, N);
    defer a.free(phash);
    const pref = try a.alloc(i16, N);
    defer a.free(pref);
    const pres = try a.alloc(i16, N);
    defer a.free(pres);
    const off = try a.alloc(usize, P + 1);
    defer a.free(off);
    const cur = try a.alloc(usize, P);
    defer a.free(cur);
    var t = try IntTable96.init(a, partcap);
    defer t.deinit(a);
    const gstate = try a.alloc(u64, (partcap + 1) * 4);
    defer a.free(gstate);

    const t0 = nowTicks();
    @memset(off, 0);
    for (0..N) |i| {
        const h = IntTable96.hashKey(d.keyOf(i));
        srch[i] = h;
        off[(h >> SHIFT) + 1] += 1;
    }
    for (1..P + 1) |p| off[p] += off[p - 1];
    @memcpy(cur, off[0..P]);
    for (0..N) |i| {
        const p = srch[i] >> SHIFT;
        const j = cur[p];
        cur[p] += 1;
        pkey[j] = d.keyOf(i);
        phash[j] = srch[i];
        pref[j] = d.isref[i];
        pres[j] = d.reswidth[i];
    }
    var total_groups: u64 = 0;
    var total_count: u64 = 0;
    for (0..P) |p| {
        const lo = off[p];
        const hi = off[p + 1];
        for (t.slots) |*s| s.gid = gt.EMPTY;
        var gid: u32 = 0;
        var i = lo;
        while (i < hi) : (i += 1) {
            if (i + PF < hi) @prefetch(t.slotAddr(t.bucketOf(phash[i + PF])), .{ .rw = .write, .locality = 1 });
            const probe = t.getOrPut(phash[i], pkey[i]);
            const g = if (probe.found) probe.gid else blk: {
                t.commit(probe.slot, pkey[i], gid);
                const ng = gid;
                gid += 1;
                gstate[@as(usize, ng) * 4 + 0] = 0;
                gstate[@as(usize, ng) * 4 + 1] = 0;
                gstate[@as(usize, ng) * 4 + 2] = 0;
                gstate[@as(usize, ng) * 4 + 3] = 0;
                break :blk ng;
            };
            const b = @as(usize, g) * 4;
            gstate[b + 0] += 1;
            gstate[b + 1] += @as(u64, @intCast(pref[i]));
            gstate[b + 2] += @as(u64, @intCast(pres[i]));
            gstate[b + 3] += 1;
        }
        total_groups += gid;
        for (0..gid) |g| total_count += gstate[g * 4];
    }
    return .{ .ticks = nowTicks() - t0, .cksum = total_groups *% 0x100000001b3 ^ total_count };
}

/// ER1-acc4 — one-pass radix with the compact 32 B fused accumulator.
fn q32Radix1PassAcc4(a: std.mem.Allocator, d: Q32, comptime PBITS: u8) !Result {
    const P: usize = @as(usize, 1) << @as(u6, @intCast(PBITS));
    const SHIFT: u6 = @intCast(64 - @as(usize, PBITS));
    const stride = (N / P) * 3 / 2 + 64;
    const partcap = (N / P) * 7 / 5 + 64;
    const pkey = try a.alloc(u128, P * stride);
    defer a.free(pkey);
    const phash = try a.alloc(u64, P * stride);
    defer a.free(phash);
    const pref = try a.alloc(i16, P * stride);
    defer a.free(pref);
    const pres = try a.alloc(i16, P * stride);
    defer a.free(pres);
    const used = try a.alloc(usize, P);
    defer a.free(used);
    var t = try IntTable96.init(a, partcap);
    defer t.deinit(a);
    const gstate = try a.alloc(u64, (partcap + 1) * 4);
    defer a.free(gstate);

    const t0 = nowTicks();
    @memset(used, 0);
    for (0..N) |i| {
        const key = d.keyOf(i);
        const h = IntTable96.hashKey(key);
        const p = h >> SHIFT;
        const j = p * stride + used[p];
        used[p] += 1;
        pkey[j] = key;
        phash[j] = h;
        pref[j] = d.isref[i];
        pres[j] = d.reswidth[i];
    }
    var total_groups: u64 = 0;
    var total_count: u64 = 0;
    for (0..P) |p| {
        const lo = p * stride;
        const hi = lo + used[p];
        for (t.slots) |*s| s.gid = gt.EMPTY;
        var gid: u32 = 0;
        var i = lo;
        while (i < hi) : (i += 1) {
            if (i + PF < hi) @prefetch(t.slotAddr(t.bucketOf(phash[i + PF])), .{ .rw = .write, .locality = 1 });
            const probe = t.getOrPut(phash[i], pkey[i]);
            const g = if (probe.found) probe.gid else blk: {
                t.commit(probe.slot, pkey[i], gid);
                const ng = gid;
                gid += 1;
                gstate[@as(usize, ng) * 4 + 0] = 0;
                gstate[@as(usize, ng) * 4 + 1] = 0;
                gstate[@as(usize, ng) * 4 + 2] = 0;
                gstate[@as(usize, ng) * 4 + 3] = 0;
                break :blk ng;
            };
            const b = @as(usize, g) * 4;
            gstate[b + 0] += 1;
            gstate[b + 1] += @as(u64, @intCast(pref[i]));
            gstate[b + 2] += @as(u64, @intCast(pres[i]));
            gstate[b + 3] += 1;
        }
        total_groups += gid;
        for (0..gid) |g| total_count += gstate[g * 4];
    }
    return .{ .ticks = nowTicks() - t0, .cksum = total_groups *% 0x100000001b3 ^ total_count };
}
/// ER2L-acc4 — TWO-LEVEL one-pass radix with the compact 32 B fused accumulator.
/// Level 1 partitions by the top `PB1` hash bits (low write fan-out), then each
/// L1 partition (cache-resident) is sub-partitioned by the next `PB2` bits, so
/// each final sub-partition (P1·P2 of them) is small enough to fit L2 — without
/// the high write-head fan-out that thrashed single-level high-P. Aggregate each
/// L2 sub-partition with the compact fused accumulator.
fn q32Radix2LevelImpl(a: std.mem.Allocator, d: Q32, comptime PB1: u8, comptime PB2: u8) !Result {
    const P1: usize = @as(usize, 1) << @as(u6, @intCast(PB1));
    const P2: usize = @as(usize, 1) << @as(u6, @intCast(PB2));
    const SHIFT1: u6 = @intCast(64 - @as(usize, PB1));
    const SHIFT2: u6 = @intCast(64 - @as(usize, PB1) - @as(usize, PB2));
    const M2: u64 = P2 - 1;
    const l1stride = (N / P1) * 3 / 2 + 64;
    const l2stride = (N / P1 / P2) * 3 / 2 + 64;
    const partcap = (N / P1 / P2) * 7 / 5 + 64;

    const l1key = try a.alloc(u128, P1 * l1stride);
    defer a.free(l1key);
    const l1hash = try a.alloc(u64, P1 * l1stride);
    defer a.free(l1hash);
    const l1ref = try a.alloc(i16, P1 * l1stride);
    defer a.free(l1ref);
    const l1res = try a.alloc(i16, P1 * l1stride);
    defer a.free(l1res);
    const l1used = try a.alloc(usize, P1);
    defer a.free(l1used);
    const l2key = try a.alloc(u128, P2 * l2stride);
    defer a.free(l2key);
    const l2hash = try a.alloc(u64, P2 * l2stride);
    defer a.free(l2hash);
    const l2ref = try a.alloc(i16, P2 * l2stride);
    defer a.free(l2ref);
    const l2res = try a.alloc(i16, P2 * l2stride);
    defer a.free(l2res);
    const l2used = try a.alloc(usize, P2);
    defer a.free(l2used);
    var t = try IntTable96.init(a, partcap);
    defer t.deinit(a);
    const gstate = try a.alloc(u64, (partcap + 1) * 4);
    defer a.free(gstate);

    const t0 = nowTicks();
    @memset(l1used, 0);
    for (0..N) |i| {
        const key = d.keyOf(i);
        const h = IntTable96.hashKey(key);
        const p1 = h >> SHIFT1;
        const j = p1 * l1stride + l1used[p1];
        l1used[p1] += 1;
        l1key[j] = key;
        l1hash[j] = h;
        l1ref[j] = d.isref[i];
        l1res[j] = d.reswidth[i];
    }
    var total_groups: u64 = 0;
    var total_count: u64 = 0;
    for (0..P1) |p1| {
        // sub-partition L1 bucket p1 (cache-resident) by the next PB2 bits.
        @memset(l2used, 0);
        const lo1 = p1 * l1stride;
        const hi1 = lo1 + l1used[p1];
        var ri = lo1;
        while (ri < hi1) : (ri += 1) {
            const h = l1hash[ri];
            const p2 = (h >> SHIFT2) & M2;
            const j = p2 * l2stride + l2used[p2];
            l2used[p2] += 1;
            l2key[j] = l1key[ri];
            l2hash[j] = h;
            l2ref[j] = l1ref[ri];
            l2res[j] = l1res[ri];
        }
        // aggregate each L2 sub-partition (fits L2).
        for (0..P2) |p2| {
            const lo2 = p2 * l2stride;
            const hi2 = lo2 + l2used[p2];
            for (t.slots) |*s| s.gid = gt.EMPTY;
            var gid: u32 = 0;
            var i = lo2;
            while (i < hi2) : (i += 1) {
                if (i + PF < hi2) @prefetch(t.slotAddr(t.bucketOf(l2hash[i + PF])), .{ .rw = .write, .locality = 1 });
                const probe = t.getOrPut(l2hash[i], l2key[i]);
                const g = if (probe.found) probe.gid else blk: {
                    t.commit(probe.slot, l2key[i], gid);
                    const ng = gid;
                    gid += 1;
                    gstate[@as(usize, ng) * 4 + 0] = 0;
                    gstate[@as(usize, ng) * 4 + 1] = 0;
                    gstate[@as(usize, ng) * 4 + 2] = 0;
                    gstate[@as(usize, ng) * 4 + 3] = 0;
                    break :blk ng;
                };
                const b = @as(usize, g) * 4;
                gstate[b + 0] += 1;
                gstate[b + 1] += @as(u64, @intCast(l2ref[i]));
                gstate[b + 2] += @as(u64, @intCast(l2res[i]));
                gstate[b + 3] += 1;
            }
            total_groups += gid;
            for (0..gid) |g| total_count += gstate[g * 4];
        }
    }
    return .{ .ticks = nowTicks() - t0, .cksum = total_groups *% 0x100000001b3 ^ total_count };
}
fn q32_radix2l_256(a: std.mem.Allocator, d: Q32, _: []u64) !Result {
    return q32Radix2LevelImpl(a, d, 4, 4); // 16 × 16 = 256
}
fn q32_radix2l_512(a: std.mem.Allocator, d: Q32, _: []u64) !Result {
    return q32Radix2LevelImpl(a, d, 4, 5); // 16 × 32 = 512
}
fn q32_radix2l_1024(a: std.mem.Allocator, d: Q32, _: []u64) !Result {
    return q32Radix2LevelImpl(a, d, 5, 5); // 32 × 32 = 1024
}
fn q32_radix2l_128x2(a: std.mem.Allocator, d: Q32, _: []u64) !Result {
    return q32Radix2LevelImpl(a, d, 7, 1); // 128 × 2 = 256
}
fn q32_radix2l_128x4(a: std.mem.Allocator, d: Q32, _: []u64) !Result {
    return q32Radix2LevelImpl(a, d, 7, 2); // 128 × 4 = 512
}
fn q32_radix2l_128x8(a: std.mem.Allocator, d: Q32, _: []u64) !Result {
    return q32Radix2LevelImpl(a, d, 7, 3); // 128 × 8 = 1024
}

fn q32_radix_acc4_64(a: std.mem.Allocator, d: Q32, _: []u64) !Result {
    return q32RadixAcc4(a, d, 6);
}
fn q32_radix_acc4_128(a: std.mem.Allocator, d: Q32, _: []u64) !Result {
    return q32RadixAcc4(a, d, 7);
}
fn q32_radix1p_acc4_64(a: std.mem.Allocator, d: Q32, _: []u64) !Result {
    return q32Radix1PassAcc4(a, d, 6);
}
fn q32_radix1p_acc4_128(a: std.mem.Allocator, d: Q32, _: []u64) !Result {
    return q32Radix1PassAcc4(a, d, 7);
}

/// ER-compact — 2-pass radix P=128 driving the REAL radix_aggregate compact
/// core (planCompact / scatter / finalize) for COUNT(*)/SUM/AVG. Validates the
/// Phase-1 core inside the radix structure end-to-end and benches it against the
/// hand-tuned acc4 kernels. Invariant checksum must match the other radix rows.
fn q32_radix_compact(a: std.mem.Allocator, d: Q32, _: []u64) !Result {
    const schema = [_]Column{
        .{ .name = "WatchID", .type = .bigint },
        .{ .name = "ClientIP", .type = .int },
        .{ .name = "IsRefresh", .type = .smallint },
        .{ .name = "ResolutionWidth", .type = .smallint },
    };
    const aggs = [_]texec.AggSpec{
        .{ .func = .count, .col = null, .as = "c" },
        .{ .func = .sum, .col = "IsRefresh", .as = "s" },
        .{ .func = .avg, .col = "ResolutionWidth", .as = "avg" },
    };
    const agg_col_idx = [_]?usize{ null, 2, 3 };
    const layout = (try ra.planCompact(a, &aggs, &agg_col_idx, &schema)).?;
    defer layout.deinit(a);

    const PBITS: u8 = 7;
    const P: usize = @as(usize, 1) << @as(u6, @intCast(PBITS));
    const SHIFT: u6 = @intCast(64 - @as(usize, PBITS));
    const partcap = (N / P) * 7 / 5 + 64;
    const maxpart = (N / P) * 2 + 64;

    const srch = try a.alloc(u64, N);
    defer a.free(srch);
    const pkey = try a.alloc(u128, N);
    defer a.free(pkey);
    const phash = try a.alloc(u64, N);
    defer a.free(phash);
    const pref = try a.alloc(i16, N);
    defer a.free(pref);
    const pres = try a.alloc(i16, N);
    defer a.free(pres);
    const off = try a.alloc(usize, P + 1);
    defer a.free(off);
    const cur = try a.alloc(usize, P);
    defer a.free(cur);
    var t = try IntTable96.init(a, partcap);
    defer t.deinit(a);
    const gstate = try a.alloc(u64, (partcap + 1) * layout.words);
    defer a.free(gstate);
    const gidbuf = try a.alloc(u32, maxpart);
    defer a.free(gidbuf);

    const t0 = nowTicks();
    @memset(off, 0);
    for (0..N) |i| {
        const h = IntTable96.hashKey(d.keyOf(i));
        srch[i] = h;
        off[(h >> SHIFT) + 1] += 1;
    }
    for (1..P + 1) |p| off[p] += off[p - 1];
    @memcpy(cur, off[0..P]);
    for (0..N) |i| {
        const p = srch[i] >> SHIFT;
        const j = cur[p];
        cur[p] += 1;
        pkey[j] = d.keyOf(i);
        phash[j] = srch[i];
        pref[j] = d.isref[i];
        pres[j] = d.reswidth[i];
    }
    var total_groups: u64 = 0;
    var total_count: u64 = 0;
    for (0..P) |p| {
        const lo = off[p];
        const hi = off[p + 1];
        const m = hi - lo;
        for (t.slots) |*s| s.gid = gt.EMPTY;
        var gid: u32 = 0;
        for (0..m) |j| {
            const idx = lo + j;
            if (j + PF < m) @prefetch(t.slotAddr(t.bucketOf(phash[lo + j + PF])), .{ .rw = .write, .locality = 1 });
            const probe = t.getOrPut(phash[idx], pkey[idx]);
            if (probe.found) {
                gidbuf[j] = probe.gid;
            } else {
                t.commit(probe.slot, pkey[idx], gid);
                @memset(gstate[@as(usize, gid) * layout.words ..][0..layout.words], 0);
                gidbuf[j] = gid;
                gid += 1;
            }
        }
        var vals = [_]ColumnView{
            .{ .data = .{ .bigint = &[_]i64{} } },
            .{ .data = .{ .int = &[_]i32{} } },
            .{ .data = .{ .smallint = pref[lo..hi] } },
            .{ .data = .{ .smallint = pres[lo..hi] } },
        };
        const pbatch = texec.Batch{ .schema = schema[0..], .values = vals[0..], .row_count = m };
        ra.scatter(layout, gstate, gidbuf[0..m], pbatch);
        total_groups += gid;
        for (0..gid) |g| total_count += ra.finalize(layout, gstate, g, 0).int;
    }
    return .{ .ticks = nowTicks() - t0, .cksum = total_groups *% 0x100000001b3 ^ total_count };
}

fn q32_radix32(a: std.mem.Allocator, d: Q32, _: []u64) !Result {
    return q32RadixImpl(a, d, 5);
}
fn q32_radix64(a: std.mem.Allocator, d: Q32, _: []u64) !Result {
    return q32RadixImpl(a, d, 6);
}
fn q32_radix128(a: std.mem.Allocator, d: Q32, _: []u64) !Result {
    return q32RadixImpl(a, d, 7);
}
fn q32_radix256(a: std.mem.Allocator, d: Q32, _: []u64) !Result {
    return q32RadixImpl(a, d, 8);
}
fn q32_radix1024(a: std.mem.Allocator, d: Q32, _: []u64) !Result {
    return q32RadixImpl(a, d, 10);
}
fn q32_radix4096(a: std.mem.Allocator, d: Q32, _: []u64) !Result {
    return q32RadixImpl(a, d, 12);
}

// ===========================================================================
// FAITHFUL MACHINERY — mirror the real operator's per-group costs so the
// algorithm comparison is at operator cost, not lean cost: SUM accumulates in
// i128 (packed across gstate[b+4],[b+5]), AVG sum in f64 (gstate[b+8]), and the
// emit finalizes AVG as an f64 division + builds each survivor's output tuple.
// All faithful variants checksum the invariants (total groups, total COUNT=N)
// so they must agree with the radix family.
// ===========================================================================
var g_faith_sink: u64 = 0; // defeats dead-code elision of the finalize work

inline fn faithSum(gstate: []u64, b: usize, v: i64) void {
    const cur: i128 = @bitCast([2]u64{ gstate[b + 4], gstate[b + 5] });
    const nv: [2]u64 = @bitCast(cur + v);
    gstate[b + 4] = nv[0];
    gstate[b + 5] = nv[1];
}
inline fn faithAvg(gstate: []u64, b: usize, v: i64) void {
    var s: f64 = @bitCast(gstate[b + 8]);
    s += @floatFromInt(v);
    gstate[b + 8] = @bitCast(s);
    gstate[b + 9] += 1;
}
/// Faithful emit: walk every group, keep the top-10 by COUNT, and for each new
/// survivor finalize AVG (f64 divide) + fold the output tuple into a sink so the
/// work isn't elided. Returns the invariant checksum.
fn faithEmit(gstate: []const u64, gkeys: []const u128, n_groups: usize) u64 {
    var groups: u64 = 0;
    var count: u64 = 0;
    var top_cnt = [_]u64{0} ** 10;
    for (0..n_groups) |g| {
        const b = g * 12;
        const c = gstate[b];
        groups += 1;
        count += c;
        var mi: usize = 0;
        for (1..10) |k| {
            if (top_cnt[k] < top_cnt[mi]) mi = k;
        }
        if (c > top_cnt[mi]) {
            top_cnt[mi] = c;
            const sum: i128 = @bitCast([2]u64{ gstate[b + 4], gstate[b + 5] });
            const asum: f64 = @bitCast(gstate[b + 8]);
            const avg: f64 = if (gstate[b + 9] > 0) asum / @as(f64, @floatFromInt(gstate[b + 9])) else 0;
            g_faith_sink +%= @as(u64, @truncate(gkeys[g])) ^ @as(u64, @bitCast(@as(i64, @truncate(sum)))) ^ @as(u64, @intFromFloat(avg * 1000.0));
        }
    }
    return groups *% 0x100000001b3 ^ count;
}

/// ST(f) — single-table, faithful machinery (i128 SUM / f64 AVG / real emit).
/// Should land near the real operator; the delta vs E0 lean is the arithmetic +
/// emit machinery (the rest of the real-op gap is the generic operator framework).
fn q32_full_faith(a: std.mem.Allocator, d: Q32, _: []u64) !Result {
    var t = try IntTable96.init(a, N);
    defer t.deinit(a);
    const gstate = try a.alloc(u64, N * 12);
    defer a.free(gstate);
    const gkeys = try a.alloc(u128, N);
    defer a.free(gkeys);
    const kb = try a.alloc(u128, BATCH);
    defer a.free(kb);
    const hb = try a.alloc(u64, BATCH);
    defer a.free(hb);
    const gidbuf = try a.alloc(u32, BATCH);
    defer a.free(gidbuf);

    const t0 = nowTicks();
    var gid: u32 = 0;
    var off: usize = 0;
    while (off < N) : (off += BATCH) {
        const m = @min(BATCH, N - off);
        for (0..m) |j| {
            const key = d.keyOf(off + j);
            kb[j] = key;
            hb[j] = IntTable96.hashKey(key);
        }
        for (0..m) |j| {
            if (j + PF < m) @prefetch(t.slotAddr(t.bucketOf(hb[j + PF])), .{ .rw = .write, .locality = 1 });
            const p = t.getOrPut(hb[j], kb[j]);
            if (p.found) {
                gidbuf[j] = p.gid;
            } else {
                t.commit(p.slot, kb[j], gid);
                gkeys[gid] = kb[j];
                @memset(gstate[@as(usize, gid) * 12 ..][0..12], 0);
                gidbuf[j] = gid;
                gid += 1;
            }
        }
        for (0..m) |j| gstate[@as(usize, gidbuf[j]) * 12] += 1;
        for (0..m) |j| faithSum(gstate, @as(usize, gidbuf[j]) * 12, d.isref[off + j]);
        for (0..m) |j| faithAvg(gstate, @as(usize, gidbuf[j]) * 12, d.reswidth[off + j]);
    }
    const ck = faithEmit(gstate, gkeys, gid);
    return .{ .ticks = nowTicks() - t0, .cksum = ck };
}

/// ER1(f) — one-pass radix P=128, faithful machinery. The decision-critical
/// number: what a radix aggregate would cost paying the real per-group arithmetic
/// + emit, vs ST(f) (single-table at the same cost).
fn q32_radix1p_faith(a: std.mem.Allocator, d: Q32, _: []u64) !Result {
    const PBITS: u8 = 7;
    const P: usize = @as(usize, 1) << @as(u6, @intCast(PBITS));
    const SHIFT: u6 = @intCast(64 - @as(usize, PBITS));
    const stride = (N / P) * 3 / 2 + 64;
    const partcap = (N / P) * 7 / 5 + 64;

    const pkey = try a.alloc(u128, P * stride);
    defer a.free(pkey);
    const phash = try a.alloc(u64, P * stride);
    defer a.free(phash);
    const pref = try a.alloc(i16, P * stride);
    defer a.free(pref);
    const pres = try a.alloc(i16, P * stride);
    defer a.free(pres);
    const used = try a.alloc(usize, P);
    defer a.free(used);
    var t = try IntTable96.init(a, partcap);
    defer t.deinit(a);
    const gstate = try a.alloc(u64, (partcap + 1) * 12);
    defer a.free(gstate);
    const gkeys = try a.alloc(u128, partcap + 1);
    defer a.free(gkeys);

    const t0 = nowTicks();
    @memset(used, 0);
    for (0..N) |i| {
        const key = d.keyOf(i);
        const h = IntTable96.hashKey(key);
        const p = h >> SHIFT;
        const j = p * stride + used[p];
        used[p] += 1;
        pkey[j] = key;
        phash[j] = h;
        pref[j] = d.isref[i];
        pres[j] = d.reswidth[i];
    }
    var total_groups: u64 = 0;
    var total_count: u64 = 0;
    for (0..P) |p| {
        const lo = p * stride;
        const hi = lo + used[p];
        for (t.slots) |*s| s.gid = gt.EMPTY;
        var gid: u32 = 0;
        var i = lo;
        while (i < hi) : (i += 1) {
            if (i + PF < hi) @prefetch(t.slotAddr(t.bucketOf(phash[i + PF])), .{ .rw = .write, .locality = 1 });
            const probe = t.getOrPut(phash[i], pkey[i]);
            const g = if (probe.found) probe.gid else blk: {
                t.commit(probe.slot, pkey[i], gid);
                gkeys[gid] = pkey[i];
                @memset(gstate[@as(usize, gid) * 12 ..][0..12], 0);
                const ng = gid;
                gid += 1;
                break :blk ng;
            };
            const b = @as(usize, g) * 12;
            gstate[b] += 1;
            faithSum(gstate, b, pref[i]);
            faithAvg(gstate, b, pres[i]);
        }
        // per-partition faithful emit folds into the global top-10 by re-walking
        // here; accumulate invariants and let faithEmit's sink absorb the finalize.
        total_groups += gid;
        _ = faithEmit(gstate, gkeys, gid);
        for (0..gid) |g| total_count += gstate[g * 12];
    }
    const ck: u64 = total_groups *% 0x100000001b3 ^ total_count;
    return .{ .ticks = nowTicks() - t0, .cksum = ck };
}

const KP = struct { key: u128, ref: i16, res: i16 };
fn lessKP(_: void, x: KP, y: KP) bool {
    return x.key < y.key;
}
/// SORT — sort-based aggregation: materialize (key, agg inputs), sort by key so
/// equal keys are adjacent, then one streaming pass over the runs. Faithful
/// arithmetic (i128 SUM, f64 AVG). The sort itself is the cost.
fn q32_sort(a: std.mem.Allocator, d: Q32, _: []u64) !Result {
    const kp = try a.alloc(KP, N);
    defer a.free(kp);
    const t0 = nowTicks();
    for (0..N) |i| kp[i] = .{ .key = d.keyOf(i), .ref = d.isref[i], .res = d.reswidth[i] };
    std.sort.pdq(KP, kp, {}, lessKP);
    var total_groups: u64 = 0;
    var total_count: u64 = 0;
    var top_cnt = [_]u64{0} ** 10;
    var i: usize = 0;
    while (i < N) {
        const k = kp[i].key;
        var cnt: u64 = 0;
        var sum: i128 = 0;
        var asum: f64 = 0;
        var acnt: u64 = 0;
        while (i < N and kp[i].key == k) : (i += 1) {
            cnt += 1;
            sum += kp[i].ref;
            asum += @floatFromInt(kp[i].res);
            acnt += 1;
        }
        total_groups += 1;
        total_count += cnt;
        var mi: usize = 0;
        for (1..10) |x| {
            if (top_cnt[x] < top_cnt[mi]) mi = x;
        }
        if (cnt > top_cnt[mi]) {
            top_cnt[mi] = cnt;
            const avg: f64 = if (acnt > 0) asum / @as(f64, @floatFromInt(acnt)) else 0;
            g_faith_sink +%= @as(u64, @truncate(k)) ^ @as(u64, @bitCast(@as(i64, @truncate(sum)))) ^ @as(u64, @intFromFloat(avg * 1000.0));
        }
    }
    const ck: u64 = total_groups *% 0x100000001b3 ^ total_count;
    return .{ .ticks = nowTicks() - t0, .cksum = ck };
}

const q32_experiments = [_]struct {
    name: []const u8,
    run: *const fn (std.mem.Allocator, Q32, []u64) anyerror!Result,
}{
    .{ .name = "BUILD-ONLY table (probe+commit)", .run = q32_build_only },
    .{ .name = "BUILD keys UNSORTED (hash only)", .run = q32_build_keys_unsorted },
    .{ .name = "BUILD keys SORTED (hash only)", .run = q32_build_keys_sorted },
    .{ .name = "CARD 1M groups (5 rows/grp) UNSORTED", .run = q32_card1m_unsorted },
    .{ .name = "CARD 1M groups (5 rows/grp) SORTED", .run = q32_card1m_sorted },
    .{ .name = "CARD 5M groups (1 row/grp) UNSORTED", .run = q32_card5m_unsorted },
    .{ .name = "CARD 5M groups (1 row/grp) SORTED", .run = q32_card5m_sorted },
    .{ .name = "BUILD+4ACC compact 32B, fused", .run = q32_build_acc4 },
    .{ .name = "BUILD+4ACC real 48B (i128/f64)", .run = q32_build_acc4_real },
    .{ .name = "E0 full (probe+init+scatter+emit)", .run = q32_full },
    .{ .name = "REAL exec.Aggregate operator", .run = q32_real },
    .{ .name = "REAL op, COUNT(*) only (2 keys)", .run = q32_real_count_only },
    .{ .name = "REAL Aggregate groupBy (full emit)", .run = q32_real_fullemit },
    .{ .name = "REAL RadixAggregate op (full emit)", .run = q32_real_radix },
    .{ .name = "REAL RadixAggregate op (top-10)", .run = q32_real_radix_topk },
    .{ .name = "ER radix-partition P=32", .run = q32_radix32 },
    .{ .name = "ER radix-partition P=64", .run = q32_radix64 },
    .{ .name = "ER radix-partition P=128", .run = q32_radix128 },
    .{ .name = "ER radix-partition P=256", .run = q32_radix256 },
    .{ .name = "ER radix-partition P=1024", .run = q32_radix1024 },
    .{ .name = "ER radix-partition P=4096", .run = q32_radix4096 },
    .{ .name = "ER1 one-pass radix P=64", .run = q32_radix1p64 },
    .{ .name = "ER1 one-pass radix P=128", .run = q32_radix1p128 },
    .{ .name = "ER1 one-pass radix P=256", .run = q32_radix1p256 },
    .{ .name = "RADIX bucket-distribution probe P=128", .run = q32_radix_dist },
    .{ .name = "ER-acc4 2-pass radix P=64 +4acc", .run = q32_radix_acc4_64 },
    .{ .name = "ER-acc4 2-pass radix P=128 +4acc", .run = q32_radix_acc4_128 },
    .{ .name = "ER1-acc4 1-pass radix P=64 +4acc", .run = q32_radix1p_acc4_64 },
    .{ .name = "ER1-acc4 1-pass radix P=128 +4acc", .run = q32_radix1p_acc4_128 },
    .{ .name = "ERC 2-pass radix P=128 REAL compact core", .run = q32_radix_compact },
    .{ .name = "ER2L 2-level radix 16x16=256 +4acc", .run = q32_radix2l_256 },
    .{ .name = "ER2L 2-level radix 16x32=512 +4acc", .run = q32_radix2l_512 },
    .{ .name = "ER2L 2-level radix 32x32=1024 +4acc", .run = q32_radix2l_1024 },
    .{ .name = "ER2L 2-level radix 128x2=256 +4acc", .run = q32_radix2l_128x2 },
    .{ .name = "ER2L 2-level radix 128x4=512 +4acc", .run = q32_radix2l_128x4 },
    .{ .name = "ER2L 2-level radix 128x8=1024 +4acc", .run = q32_radix2l_128x8 },
    .{ .name = "SORT sort-based (pdq by key)", .run = q32_sort },
    .{ .name = "ST(f) single-table FAITHFUL", .run = q32_full_faith },
    .{ .name = "ER1(f) radix P=128 FAITHFUL", .run = q32_radix1p_faith },
};

// ===========================================================================
// Q32 REAL OPERATOR — drive the actual exec.Aggregate over a synthetic source.
//   The source emits the 4 Q32 columns in BATCH-row chunks (exactly what the
//   cache-aware scan hands the aggregate), reports an upstream `stats()` with
//   the real high-card NDV so the operator presizes its group table the same
//   way, then groupByTopK(...) builds the real Aggregate and we drain + time it.
//   This is the operator, not a model — so it pays every cost the profiler saw
//   (AccState 32-B union RMW, gkeys_int/gstate growth, per-grow accountant
//   reserve when a budget is set, TopK heap emit + finalize).
// ===========================================================================

/// Synthetic upstream: borrows the pre-generated Q32 columns and re-emits them
/// as borrowed `ColumnView`s in BATCH-row slices. Implements the minimal
/// operator surface `makeQuery` requires (next/deinit/outputSchema/addPrune/
/// stats/accountant/explain); the dict-coding hooks are absent (declined via
/// `@hasDecl`), matching a plain int-keyed scan. `deinit` frees only itself —
/// the column arrays are owned by the caller's `Q32` and reused across repeats.
const Q32Source = struct {
    d: Q32,
    pos: usize = 0,
    schema: [4]Column,
    views: [4]ColumnView = undefined,
    col_stats: [4]texec.ColStat,
    allocator: std.mem.Allocator,

    fn create(a: std.mem.Allocator, d: Q32) !*Q32Source {
        const self = try a.create(Q32Source);
        self.* = .{
            .d = d,
            .schema = .{
                .{ .name = "WatchID", .type = .bigint },
                .{ .name = "ClientIP", .type = .int },
                .{ .name = "IsRefresh", .type = .smallint },
                .{ .name = "ResolutionWidth", .type = .smallint },
            },
            // Group keys (WatchID, ClientIP) carry their real near-unique NDV so
            // the aggregate presizes its table to the provable ceiling (capped at
            // upper_rows) — the genuine high-card path, not grow-from-small.
            .col_stats = .{
                .{ .ndv = .{ .exact = @intCast(N) } },
                .{ .ndv = .{ .exact = @intCast(N) } },
                .{ .ndv = .{ .exact = 2 } },
                .{ .ndv = .{ .exact = 2560 } },
            },
            .allocator = a,
        };
        return self;
    }
    pub fn next(self: *Q32Source) !?texec.Batch {
        if (self.pos >= N) return null;
        const lo = self.pos;
        const m = @min(BATCH, N - lo);
        const hi = lo + m;
        self.views[0] = .{ .data = .{ .bigint = self.d.watchid[lo..hi] } };
        self.views[1] = .{ .data = .{ .int = self.d.clientip[lo..hi] } };
        self.views[2] = .{ .data = .{ .smallint = self.d.isref[lo..hi] } };
        self.views[3] = .{ .data = .{ .smallint = self.d.reswidth[lo..hi] } };
        self.pos = hi;
        return .{ .schema = self.schema[0..], .values = self.views[0..], .row_count = m };
    }
    pub fn deinit(self: *Q32Source) void {
        self.allocator.destroy(self);
    }
    pub fn outputSchema(self: *Q32Source) []const Column {
        return self.schema[0..];
    }
    pub fn addPrune(_: *Q32Source, _: texec.Predicate) !void {}
    pub fn stats(self: *Q32Source) texec.PipelineStats {
        return .{ .upper_rows = N, .column_stats = self.col_stats[0..] };
    }
    pub fn accountant(_: *Q32Source) ?*texec.memory.MemoryAccountant {
        return null;
    }
    pub fn explain(_: *Q32Source, _: *std.ArrayList(u8), _: std.mem.Allocator, _: usize) !void {}
};

/// Build the real Aggregate over `Q32Source` and drain it, timing only the
/// build+drain (data is pre-generated). `group_cols`/`aggs` pick the plan shape;
/// `ORDER BY <first agg> DESC LIMIT 10` (top-k emit) is always applied. Checksums
/// the first group column + count of the survivors.
fn runRealAgg(a: std.mem.Allocator, d: Q32, group_cols: []const []const u8, aggs: []const texec.AggSpec) !Result {
    const src = try Q32Source.create(a, d);
    const q = texec.makeQuery(a, src);

    const tk_keys = [_]thindb.ir.SortSpec{.{ .col = aggs[0].as, .desc = true }};
    const tk = thindb.ir.Op.TopK{ .k = 10, .keys = tk_keys[0..] };

    const t0 = nowTicks();
    // groupByTopK moves `q` into the Aggregate; on success `agg` owns the whole
    // tree (its deinit frees the source), so only clean up `q` if it fails.
    var agg = q.groupByTopK(group_cols, aggs, tk, null) catch |e| {
        var qq = q;
        qq.deinit();
        return e;
    };
    defer agg.deinit();

    const cnt_idx = group_cols.len; // COUNT(*) is the first agg, just after the keys
    var ck: u64 = 0;
    while (try agg.next()) |b| {
        const k0 = b.values[0].data.bigint;
        const cnt = b.values[cnt_idx].data.bigint;
        for (0..b.row_count) |r| ck ^= @as(u64, @bitCast(k0[r])) ^ @as(u64, @bitCast(cnt[r]));
    }
    return .{ .ticks = nowTicks() - t0, .cksum = ck };
}

/// Faithful Q32: hash GROUP BY (WatchID, ClientIP), COUNT(*)/SUM/AVG, top-10.
fn q32_real(a: std.mem.Allocator, d: Q32, _: []u64) !Result {
    return runRealAgg(a, d, &.{ "WatchID", "ClientIP" }, &.{
        .{ .func = .count, .col = null, .as = "c" },
        .{ .func = .sum, .col = "IsRefresh", .as = "s" },
        .{ .func = .avg, .col = "ResolutionWidth", .as = "avg" },
    });
}

/// Same two keys + top-10 but ONLY COUNT(*) (one agg). Two keys keep it on the
/// generic int-96 path (not the single-key count-in-slot fast path), so the
/// delta vs `q32_real` isolates the cost of the two extra per-aggregate scatter
/// passes (SUM, AVG) re-sweeping the ~480-MB gstate — the lever for a fused
/// single-pass multi-aggregate scatter (#278).
fn q32_real_count_only(a: std.mem.Allocator, d: Q32, _: []u64) !Result {
    return runRealAgg(a, d, &.{ "WatchID", "ClientIP" }, &.{
        .{ .func = .count, .col = null, .as = "c" },
    });
}

const q32_full_aggs = [_]texec.AggSpec{
    .{ .func = .count, .col = null, .as = "c" },
    .{ .func = .sum, .col = "IsRefresh", .as = "s" },
    .{ .func = .avg, .col = "ResolutionWidth", .as = "avg" },
};
fn ckDrain(q: *texec.Query) !u64 {
    var ck: u64 = 0;
    while (try q.next()) |b| {
        const wid = b.values[0].data.bigint;
        const cip = b.values[1].data.int;
        const cnt = b.values[2].data.bigint;
        for (0..b.row_count) |r| ck ^= @as(u64, @bitCast(wid[r])) ^ @as(u64, @as(u32, @bitCast(cip[r]))) ^ @as(u64, @bitCast(cnt[r]));
    }
    return ck;
}

/// The REAL RadixAggregate operator (Phase 2b-i: single-table compact) over the
/// synthetic Q32 source, full emit. Times create+drain+emit.
fn q32_real_radix(a: std.mem.Allocator, d: Q32, _: []u64) !Result {
    const q0 = texec.makeQuery(a, try Q32Source.create(a, d));
    const t0 = nowTicks();
    var q = ra.RadixAggregate.create(a, q0, &.{ "WatchID", "ClientIP" }, q32_full_aggs[0..], null) catch |e| {
        var qq = q0;
        qq.deinit();
        return e;
    };
    defer q.deinit();
    const ck = try ckDrain(&q);
    return .{ .ticks = nowTicks() - t0, .cksum = ck };
}

/// The REAL RadixAggregate operator with the Q32 top-10 hint (ORDER BY c DESC
/// LIMIT 10) — comparable to `q32_real` (generic top-10 at ~249 ms). Selection
/// happens at emit, so only 10 rows leave the operator.
fn q32_real_radix_topk(a: std.mem.Allocator, d: Q32, _: []u64) !Result {
    const q0 = texec.makeQuery(a, try Q32Source.create(a, d));
    const t0 = nowTicks();
    var q = ra.RadixAggregate.create(a, q0, &.{ "WatchID", "ClientIP" }, q32_full_aggs[0..], .{ .k = 10, .col = "c", .desc = true }) catch |e| {
        var qq = q0;
        qq.deinit();
        return e;
    };
    defer q.deinit();
    const ck = try ckDrain(&q);
    return .{ .ticks = nowTicks() - t0, .cksum = ck };
}

/// The existing generic Aggregate via groupBy, FULL emit (no top-k) — the fair
/// apples-to-apples baseline for q32_real_radix. Same checksum confirms identical
/// results on the full 5M-group dataset.
fn q32_real_fullemit(a: std.mem.Allocator, d: Q32, _: []u64) !Result {
    const q0 = texec.makeQuery(a, try Q32Source.create(a, d));
    const t0 = nowTicks();
    var q = q0.groupBy(&.{ "WatchID", "ClientIP" }, q32_full_aggs[0..]) catch |e| {
        var qq = q0;
        qq.deinit();
        return e;
    };
    defer q.deinit();
    const ck = try ckDrain(&q);
    return .{ .ticks = nowTicks() - t0, .cksum = ck };
}

pub fn main() !void {
    var gpa: std.heap.DebugAllocator(.{}) = .init;
    defer _ = gpa.deinit();
    const a = gpa.allocator();
    const hz: f64 = @floatFromInt(perfFreq());
    const hashes = try a.alloc(u64, N);
    defer a.free(hashes);

    std.debug.print("\n=== Q08  RegionID, COUNT(DISTINCT UserID)   (5M rows, 3238 regions, 1.06M users) ===\n", .{});
    std.debug.print("{s:<34} {s:>9} {s:>9} {s:>7}  {s}\n", .{ "experiment", "ns/row", "total ms", "rel", "cksum" });
    std.debug.print("{s}\n", .{"-" ** 78});
    {
        const d = try Q08.gen(a, 3238, 1_062_330);
        defer d.free(a);
        var base: f64 = 0;
        for (q08_experiments) |e| {
            var best: i64 = std.math.maxInt(i64);
            var ck: u64 = 0;
            for (0..REPEAT) |_| {
                const r = try e.run(a, d, hashes);
                if (r.ticks < best) best = r.ticks;
                ck = r.cksum;
            }
            const ms: f64 = @as(f64, @floatFromInt(best)) * 1000.0 / hz;
            const per: f64 = ms * 1e6 / @as(f64, @floatFromInt(N));
            if (base == 0) base = per;
            std.debug.print("{s:<34} {d:>8.2} {d:>8.1} {d:>6.2}x  {x}\n", .{ e.name, per, ms, per / base, ck });
        }
    }

    std.debug.print("\n=== Q32  WatchID, ClientIP, COUNT/SUM/AVG   (5M rows, ~5M groups, 480 MB gstate) ===\n", .{});
    std.debug.print("{s:<34} {s:>9} {s:>9} {s:>7}  {s}\n", .{ "experiment", "ns/row", "total ms", "rel", "cksum" });
    std.debug.print("{s}\n", .{"-" ** 78});
    {
        const d = try Q32.gen(a, N);
        defer d.free(a);
        var base: f64 = 0;
        for (q32_experiments) |e| {
            var best: i64 = std.math.maxInt(i64);
            var ck: u64 = 0;
            for (0..REPEAT) |_| {
                const r = try e.run(a, d, hashes);
                if (r.ticks < best) best = r.ticks;
                ck = r.cksum;
            }
            const ms: f64 = @as(f64, @floatFromInt(best)) * 1000.0 / hz;
            const per: f64 = ms * 1e6 / @as(f64, @floatFromInt(N));
            if (base == 0) base = per;
            std.debug.print("{s:<34} {d:>8.2} {d:>8.1} {d:>6.2}x  {x}\n", .{ e.name, per, ms, per / base, ck });
        }
    }
}
