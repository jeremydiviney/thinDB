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
