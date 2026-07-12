//! FIFO-fair ticket mutex for long-hold, high-contention locks — the
//! per-table write mutex. `std.Io.Mutex` is a barging lock: `unlock` wakes
//! one parked waiter, but any already-running thread can steal the lock via
//! the unlocked→locked cmpxchg before the woken thread gets CPU. On a
//! saturated box, a writer re-acquiring in a tight loop (multi-statement
//! sink batches, inline auto-flush) therefore starves a parked waiter for
//! unbounded time — the 2026-07-11 incident held one sink connection at
//! zero packets for 559 s while the server kept serving others (#164).
//!
//! Tickets grant strict arrival order: a waiter's delay is bounded by the
//! critical sections queued ahead of it. `unlock` must wake ALL waiters
//! (each parks on the shared `serving` word but proceeds only on its own
//! ticket); waiter counts on a table mutex are small — a handful of sink
//! connections plus the compactor — so the herd is cheap.

const std = @import("std");
const Io = std.Io;

/// Wait time after which a single rate-limited stall warning is printed.
/// The 559 s starvation was invisible precisely because a stalled waiter
/// emits nothing; this turns the next one into a log fingerprint.
const stall_warn_secs = 10;

pub const FairMutex = struct {
    /// Next ticket to hand out. `serving == next_ticket` ⇔ lock free.
    next_ticket: std.atomic.Value(u32) = .init(0),
    /// Ticket currently holding (or free to take) the lock.
    serving: std.atomic.Value(u32) = .init(0),

    pub const init: FairMutex = .{};

    /// Acquire only when the lock is free AND nobody is queued — a
    /// tryLock never jumps the line. Fail-fast callers (XA staging,
    /// stats probes) keep their semantics.
    pub fn tryLock(self: *FairMutex) bool {
        const s = self.serving.load(.monotonic);
        return self.next_ticket.cmpxchgStrong(s, s +% 1, .acquire, .monotonic) == null;
    }

    pub fn lockUncancelable(self: *FairMutex, io: Io) void {
        const ticket = self.next_ticket.fetchAdd(1, .monotonic);
        if (self.serving.load(.acquire) == ticket) return;

        // Slow path: park until served. Timed waits so a stalled waiter
        // periodically wakes to (a) recheck and (b) emit the diagnostic;
        // timeout expiry is indistinguishable from a spurious wake and
        // the loop treats both the same.
        const t0 = Io.Clock.awake.now(io);
        var warned = false;
        var canceled = false;
        while (true) {
            const s = self.serving.load(.acquire);
            if (s == ticket) break;
            if (canceled) {
                io.futexWaitUncancelable(u32, &self.serving.raw, s);
            } else {
                io.futexWaitTimeout(u32, &self.serving.raw, s, .{ .duration = .{
                    .raw = .fromSeconds(stall_warn_secs),
                    .clock = .awake,
                } }) catch {
                    // Task canceled: this lock is uncancelable, so keep
                    // waiting — but on the plain futex (a canceled task's
                    // cancelable waits return immediately = busy spin).
                    canceled = true;
                };
            }
            if (!warned) {
                const waited = t0.durationTo(Io.Clock.awake.now(io));
                if (waited.toSeconds() >= stall_warn_secs and self.serving.load(.acquire) != ticket) {
                    warned = true;
                    std.debug.print(
                        "thindb: fair-mutex stall: waited {d}s for table write lock ({d} holders queued ahead)\n",
                        .{ waited.toSeconds(), ticket -% self.serving.load(.monotonic) },
                    );
                }
            }
        }
        if (warned) {
            const waited = t0.durationTo(Io.Clock.awake.now(io));
            std.debug.print("thindb: fair-mutex stall resolved: acquired after {d}s\n", .{waited.toSeconds()});
        }
    }

    pub fn unlock(self: *FairMutex, io: Io) void {
        const new_serving = self.serving.fetchAdd(1, .release) +% 1;
        // Wake only if someone is queued. A ticket taken between the load
        // and the wake is fine: that waiter rechecks `serving` before its
        // futexWait, and a stale `expected` makes the wait return at once.
        if (self.next_ticket.load(.monotonic) != new_serving) {
            io.futexWake(u32, &self.serving.raw, std.math.maxInt(u32));
        }
    }
};

test "fair mutex: uncontended lock/unlock and tryLock" {
    var threaded: std.Io.Threaded = .init(std.testing.allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    var m: FairMutex = .init;
    m.lockUncancelable(io);
    try std.testing.expect(!m.tryLock());
    m.unlock(io);
    try std.testing.expect(m.tryLock());
    m.unlock(io);
}

test "fair mutex: mutual exclusion under contention" {
    var threaded: std.Io.Threaded = .init(std.testing.allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    var m: FairMutex = .init;
    var counter: u64 = 0;

    const Worker = struct {
        fn run(mu: *FairMutex, w_io: Io, c: *u64) void {
            for (0..10_000) |_| {
                mu.lockUncancelable(w_io);
                defer mu.unlock(w_io);
                c.* += 1;
            }
        }
    };

    var threads: [4]std.Thread = undefined;
    for (&threads) |*t| t.* = try std.Thread.spawn(.{}, Worker.run, .{ &m, io, &counter });
    for (threads) |t| t.join();

    try std.testing.expectEqual(@as(u64, 40_000), counter);
}

test "fair mutex: tryLock never jumps a queued waiter" {
    var threaded: std.Io.Threaded = .init(std.testing.allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    var m: FairMutex = .init;
    m.lockUncancelable(io);

    // Simulate a queued waiter by taking the next ticket directly.
    _ = m.next_ticket.fetchAdd(1, .monotonic);
    try std.testing.expect(!m.tryLock());

    // Serve both the holder's and the simulated waiter's tickets.
    m.unlock(io);
    m.unlock(io);
    try std.testing.expect(m.tryLock());
    m.unlock(io);
}
