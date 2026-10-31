//! Helper threads for a statement's fan-outs. `spawn` runs a call on a
//! pooled thread and returns a `Helper` to join (or detach), the same shape
//! as `std.Thread`. `forRanges` cuts `[0, n)` into near-equal ranges,
//! `forJobs` hands out jobs under a dynamic claim; both run one helper per
//! worker, run inline where a spawn fails or a single worker suffices, and
//! join before returning. For passes whose cost is memory bandwidth and
//! whose units are independent: a lift, a histogram, a scatter through
//! private offsets, a gather. The body gets its worker index so per-worker
//! outputs can live in arrays sized `MAX_THREADS`.
//!
//! The pool is process-wide, like the core scheduler: threads are a machine
//! resource, and it holds no per-Database state. A fresh OS thread costs a
//! stack mapping, its page faults, a TLS block to clear and an unmap with a
//! TLB shootdown on exit; a statement fanning out to 16 workers paid that 16
//! times, about 1 ms, before doing any work. A pooled thread parks on a futex
//! between calls and keeps its stack, so a spawn is one wake.
const std = @import("std");

pub const MAX_THREADS: usize = 64;

/// Bytes of a call's argument tuple a helper slot holds inline; a bigger
/// tuple is a compile error at its spawn. A slot takes a page either way.
const ARGS_BYTES: usize = 1024;

const SlotState = enum(u32) { idle, posted, done, detached };

const Slot = struct {
    state: std.atomic.Value(SlotState) = .init(.idle),
    call: *const fn (*Slot) void = undefined,
    args: [ARGS_BYTES]u8 align(16) = undefined,
    next_idle: ?*Slot = null,
};

const Pool = struct {
    lock: std.atomic.Mutex = .unlocked,
    idle: ?*Slot = null,

    fn take(self: *Pool) std.Thread.SpawnError!*Slot {
        while (!self.lock.tryLock()) std.atomic.spinLoopHint();
        if (self.idle) |slot| {
            self.idle = slot.next_idle;
            self.lock.unlock();
            return slot;
        }
        self.lock.unlock();
        const slot = std.heap.page_allocator.create(Slot) catch return error.SystemResources;
        slot.* = .{};
        const thread = std.Thread.spawn(.{}, serve, .{slot}) catch |err| {
            std.heap.page_allocator.destroy(slot);
            return err;
        };
        thread.detach();
        return slot;
    }

    fn giveBack(self: *Pool, slot: *Slot) void {
        slot.state.store(.idle, .monotonic);
        while (!self.lock.tryLock()) std.atomic.spinLoopHint();
        slot.next_idle = self.idle;
        self.idle = slot;
        self.lock.unlock();
    }
};

var pool: Pool = .{};

fn serve(slot: *Slot) void {
    while (true) {
        // A caller usually detaches before this thread wakes, so a detached
        // call is still one waiting to run.
        var state = slot.state.load(.acquire);
        while (state != .posted and state != .detached) {
            futexIo().futexWaitUncancelable(SlotState, &slot.state.raw, state);
            state = slot.state.load(.acquire);
        }
        slot.call(slot);
        if (slot.state.cmpxchgStrong(.posted, .done, .acq_rel, .acquire) == null) {
            futexIo().futexWake(SlotState, &slot.state.raw, 1);
        } else {
            pool.giveBack(slot);
        }
    }
}

/// A call running on a pooled helper thread. Exactly one of `join` or
/// `detach` hands the thread back to the pool.
pub const Helper = struct {
    slot: *Slot,

    pub fn join(self: Helper) void {
        var state = self.slot.state.load(.acquire);
        while (state != .done) {
            futexIo().futexWaitUncancelable(SlotState, &self.slot.state.raw, state);
            state = self.slot.state.load(.acquire);
        }
        pool.giveBack(self.slot);
    }

    pub fn detach(self: Helper) void {
        if (self.slot.state.cmpxchgStrong(.posted, .detached, .acq_rel, .acquire) == null) return;
        pool.giveBack(self.slot);
    }
};

/// Run `function(args...)` on a pooled helper thread, starting a new one
/// only when none is parked. An error the call returns is logged, as
/// `std.Thread.spawn` does.
pub fn spawn(comptime function: anytype, args: anytype) std.Thread.SpawnError!Helper {
    const Args = @TypeOf(args);
    if (@sizeOf(Args) > ARGS_BYTES or @alignOf(Args) > 16) @compileError("helper call arguments exceed the slot's inline buffer");
    const Call = struct {
        fn run(slot: *Slot) void {
            const call_args: *const Args = @ptrCast(@alignCast(&slot.args));
            const result = @call(.auto, function, call_args.*);
            if (@typeInfo(@TypeOf(result)) == .error_union) {
                result catch |err| std.log.err("helper thread call failed: {s}", .{@errorName(err)});
            }
        }
    };
    const slot = try pool.take();
    const slot_args: *Args = @ptrCast(@alignCast(&slot.args));
    slot_args.* = args;
    slot.call = Call.run;
    slot.state.store(.posted, .release);
    futexIo().futexWake(SlotState, &slot.state.raw, 1);
    return .{ .slot = slot };
}

/// Helpers are raw threads with no Io of their own; futex waits and wakes
/// hold no per-instance state, so the process-wide instance does.
fn futexIo() std.Io {
    return std.Io.Threaded.global_single_threaded.io();
}

pub fn forRanges(threads: usize, n: usize, ctx: anytype, comptime body: fn (@TypeOf(ctx), usize, usize, usize) void) void {
    const nt = @min(@max(threads, 1), MAX_THREADS);
    if (nt == 1) return body(ctx, 0, 0, n);
    var handles: [MAX_THREADS]?Helper = .{null} ** MAX_THREADS;
    for (0..nt) |t| {
        const lo = n * t / nt;
        const hi = n * (t + 1) / nt;
        handles[t] = spawn(body, .{ ctx, t, lo, hi }) catch null;
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
    var handles: [MAX_THREADS]?Helper = .{null} ** MAX_THREADS;
    for (0..nt) |t| {
        handles[t] = spawn(Claim.run, .{ ctx, t, &next, n_jobs }) catch null;
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
};

test "WorkEpoch wakes a parked helper on publish" {
    const Waiter = struct {
        fn run(epoch: *WorkEpoch, got: *std.atomic.Value(u32)) void {
            got.store(epoch.waitPast(0), .release);
        }
    };
    var epoch: WorkEpoch = .{};
    var got = std.atomic.Value(u32).init(0);
    const thread = try std.Thread.spawn(.{}, Waiter.run, .{ &epoch, &got });
    var spins: usize = 0;
    while (epoch.sleepers.load(.acquire) == 0 and spins < 100_000_000) : (spins += 1) std.atomic.spinLoopHint();
    epoch.publish();
    thread.join();
    try std.testing.expectEqual(@as(u32, 1), got.load(.acquire));
}

test "spawn reuses parked helpers, and every joined or detached call runs once" {
    const Count = struct {
        fn bump(n: *std.atomic.Value(u32)) void {
            _ = n.fetchAdd(1, .monotonic);
        }
        fn idleSlots() usize {
            while (!pool.lock.tryLock()) std.atomic.spinLoopHint();
            defer pool.lock.unlock();
            var count: usize = 0;
            var slot = pool.idle;
            while (slot) |s| : (slot = s.next_idle) count += 1;
            return count;
        }
    };
    var n = std.atomic.Value(u32).init(0);
    var parked_after_first: usize = 0;
    for (0..50) |round| {
        var helpers: [4]Helper = undefined;
        for (&helpers) |*h| h.* = try spawn(Count.bump, .{&n});
        for (helpers) |h| h.join();
        if (round == 0) parked_after_first = Count.idleSlots();
    }
    try std.testing.expectEqual(@as(u32, 200), n.load(.monotonic));
    try std.testing.expectEqual(parked_after_first, Count.idleSlots());

    for (0..64) |_| (try spawn(Count.bump, .{&n})).detach();
    var spins: usize = 0;
    while (n.load(.acquire) < 264 and spins < 1_000_000_000) : (spins += 1) std.atomic.spinLoopHint();
    try std.testing.expectEqual(@as(u32, 264), n.load(.acquire));
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
