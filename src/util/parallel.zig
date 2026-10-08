//! Fan one pass out over a fixed number of threads. `forRanges` cuts
//! `[0, n)` into near-equal ranges, `forJobs` hands out jobs under a
//! dynamic claim; both spawn one thread per worker, run inline where a
//! spawn fails or a single worker suffices, and join before returning. For
//! passes whose cost is memory bandwidth and whose units are independent:
//! a lift, a histogram, a scatter through private offsets, a gather. The
//! body gets its worker index so per-worker outputs can live in arrays
//! sized `MAX_THREADS`.
const std = @import("std");

pub const MAX_THREADS: usize = 64;

pub fn forRanges(threads: usize, n: usize, ctx: anytype, comptime body: fn (@TypeOf(ctx), usize, usize, usize) void) void {
    const nt = @min(@max(threads, 1), MAX_THREADS);
    if (nt == 1) return body(ctx, 0, 0, n);
    var handles: [MAX_THREADS]?std.Thread = .{null} ** MAX_THREADS;
    for (0..nt) |t| {
        const lo = n * t / nt;
        const hi = n * (t + 1) / nt;
        handles[t] = std.Thread.spawn(.{}, body, .{ ctx, t, lo, hi }) catch null;
        if (handles[t] == null) body(ctx, t, lo, hi);
    }
    for (handles[0..nt]) |h| if (h) |th| th.join();
}

pub fn forJobs(threads: usize, n_jobs: usize, ctx: anytype, comptime body: fn (@TypeOf(ctx), usize, usize) void) void {
    const nt = @min(@min(@max(threads, 1), MAX_THREADS), @max(n_jobs, 1));
    if (nt == 1) {
        for (0..n_jobs) |j| body(ctx, 0, j);
        return;
    }
    var next = std.atomic.Value(usize).init(0);
    const Claim = struct {
        fn run(c: @TypeOf(ctx), t: usize, counter: *std.atomic.Value(usize), total: usize) void {
            while (true) {
                const j = counter.fetchAdd(1, .monotonic);
                if (j >= total) return;
                body(c, t, j);
            }
        }
    };
    var handles: [MAX_THREADS]?std.Thread = .{null} ** MAX_THREADS;
    for (0..nt) |t| {
        handles[t] = std.Thread.spawn(.{}, Claim.run, .{ ctx, t, &next, n_jobs }) catch null;
        if (handles[t] == null) Claim.run(ctx, t, &next, n_jobs);
    }
    for (handles[0..nt]) |h| if (h) |th| th.join();
}

/// A counter idle helper threads wait on for their next unit of work. The
/// publisher bumps it per hand-out (and once more to stop them); a helper
/// spins, then yields, then parks on it, and a bump wakes a parked helper
/// at once rather than at its next poll: a stop no longer waits out a sleep
/// (a whole timer tick on Windows) before the helper can be joined.
pub const WorkEpoch = struct {
    value: std.atomic.Value(u32) = .init(0),
    sleepers: std.atomic.Value(u32) = .init(0),

    const SPINS: usize = 2048;
    const YIELDS: usize = 2048;
    /// Bounds a park only as a safeguard; a bump ends it.
    const PARK_TIMEOUT_NS: u64 = 10 * std.time.ns_per_ms;

    pub fn load(self: *const WorkEpoch) u32 {
        return self.value.load(.acquire);
    }

    pub fn publish(self: *WorkEpoch) void {
        _ = self.value.fetchAdd(1, .seq_cst);
        if (self.sleepers.load(.seq_cst) == 0) return;
        futexIo().futexWake(u32, &self.value.raw, std.math.maxInt(u32));
    }

    /// The first value past `seen`.
    pub fn waitPast(self: *WorkEpoch, seen: u32) u32 {
        var spins: usize = 0;
        while (true) {
            const now = self.value.load(.acquire);
            if (now != seen) return now;
            spins += 1;
            if (spins < SPINS) {
                std.atomic.spinLoopHint();
            } else if (spins < SPINS + YIELDS) {
                std.Thread.yield() catch std.atomic.spinLoopHint();
            } else {
                self.park(seen);
            }
        }
    }

    fn park(self: *WorkEpoch, seen: u32) void {
        // Announced before the futex reads the value, and `publish` bumps
        // before it reads `sleepers`: either it sees this sleeper and wakes
        // it, or the futex sees the bump and returns at once.
        _ = self.sleepers.fetchAdd(1, .seq_cst);
        defer _ = self.sleepers.fetchSub(1, .seq_cst);
        futexIo().futexWaitTimeout(u32, &self.value.raw, seen, .{ .duration = .{
            .raw = .fromNanoseconds(PARK_TIMEOUT_NS),
            .clock = .awake,
        } }) catch {};
    }

    /// Helpers are raw threads with no Io of their own; futex waits and
    /// wakes hold no per-instance state, so the process-wide instance does.
    fn futexIo() std.Io {
        return std.Io.Threaded.global_single_threaded.io();
    }
};

test "WorkEpoch wakes a parked helper on publish" {
    const Helper = struct {
        fn run(epoch: *WorkEpoch, got: *std.atomic.Value(u32)) void {
            got.store(epoch.waitPast(0), .release);
        }
    };
    var epoch: WorkEpoch = .{};
    var got = std.atomic.Value(u32).init(0);
    const thread = try std.Thread.spawn(.{}, Helper.run, .{ &epoch, &got });
    var spins: usize = 0;
    while (epoch.sleepers.load(.acquire) == 0 and spins < 100_000_000) : (spins += 1) std.atomic.spinLoopHint();
    epoch.publish();
    thread.join();
    try std.testing.expectEqual(@as(u32, 1), got.load(.acquire));
}

test "forRanges tiles the range once per worker and forJobs claims every job once" {
    const Sum = struct {
        hits: []u32,
        fn range(c: *const @This(), _: usize, lo: usize, hi: usize) void {
            for (c.hits[lo..hi]) |*h| h.* += 1;
        }
        fn job(c: *const @This(), _: usize, j: usize) void {
            c.hits[j] += 1;
        }
    };
    const allocator = std.testing.allocator;
    const hits = try allocator.alloc(u32, 1001);
    defer allocator.free(hits);
    inline for (.{ 1, 3, 8 }) |threads| {
        @memset(hits, 0);
        const sum = Sum{ .hits = hits };
        forRanges(threads, hits.len, &sum, Sum.range);
        forJobs(threads, hits.len, &sum, Sum.job);
        for (hits) |h| try std.testing.expectEqual(@as(u32, 2), h);
    }
}
