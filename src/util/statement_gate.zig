const std = @import("std");
const Io = std.Io;
const Allocator = std.mem.Allocator;

/// Nested API calls reuse their statement's lock. Taking a second read lock
/// while a writer waits would deadlock an otherwise valid in-flight query.
pub const StatementGate = struct {
    allocator: Allocator,
    io: Io,
    lock: Io.RwLock = .init,
    owners_mutex: Io.Mutex = .init,
    owners: std.AutoHashMapUnmanaged(std.Thread.Id, Owner) = .empty,
    recovery_required: std.atomic.Value(bool) = .init(false),
    closing: bool = false,
    closing_owner: std.Thread.Id = undefined,
    allocator_owners: usize = 0,
    /// Bumped by each release once closing starts, so the closer's timed
    /// wait wakes as soon as a lease goes.
    close_releases: std.atomic.Value(u32) = .init(0),

    const Owner = struct { exclusive: bool, references: usize };

    const CLOSE_FIRST_LOG_SECS = 5;
    const CLOSE_LOG_INTERVAL_SECS = 10;

    pub const Lease = struct {
        gate: *StatementGate,
        owner: std.Thread.Id,

        pub fn release(self: Lease) void {
            const gate = self.gate;
            gate.owners_mutex.lockUncancelable(gate.io);
            const entry = gate.owners.getPtr(self.owner).?;
            std.debug.assert(entry.references > 0);
            entry.references -= 1;
            const done = entry.references == 0;
            const exclusive = entry.exclusive;
            if (done) _ = gate.owners.remove(self.owner);
            const wake_closer = done and gate.noteCloseReleaseLocked();
            gate.owners_mutex.unlock(gate.io);
            if (done) {
                if (exclusive) gate.lock.unlock(gate.io) else gate.lock.unlockShared(gate.io);
            }
            if (wake_closer) gate.wakeCloser();
        }
    };

    pub const LifetimeLease = struct {
        gate: *StatementGate,

        pub fn release(self: LifetimeLease) void {
            self.gate.owners_mutex.lockUncancelable(self.gate.io);
            std.debug.assert(self.gate.allocator_owners > 0);
            self.gate.allocator_owners -= 1;
            const wake_closer = self.gate.noteCloseReleaseLocked();
            self.gate.owners_mutex.unlock(self.gate.io);
            if (wake_closer) self.gate.wakeCloser();
        }
    };

    pub fn retainAllocator(self: *StatementGate) !LifetimeLease {
        self.owners_mutex.lockUncancelable(self.io);
        defer self.owners_mutex.unlock(self.io);
        const owner = std.Thread.getCurrentId();
        if (self.closing and self.closing_owner != owner and !self.owners.contains(owner)) return error.DatabaseClosed;
        self.allocator_owners += 1;
        return .{ .gate = self };
    }

    pub fn init(allocator: Allocator, io: Io) StatementGate {
        return .{ .allocator = allocator, .io = io };
    }

    pub fn deinit(self: *StatementGate) void {
        std.debug.assert(self.owners.count() == 0);
        std.debug.assert(self.allocator_owners == 0);
        self.owners.deinit(self.allocator);
    }

    pub fn beginClose(self: *StatementGate) void {
        self.owners_mutex.lockUncancelable(self.io);
        self.closing = true;
        self.closing_owner = std.Thread.getCurrentId();
        self.owners_mutex.unlock(self.io);
        // Every lock holder past this point is a leased statement or an
        // acquire about to fail with DatabaseClosed, so waiting for the
        // leases first lets the wait log who holds them.
        self.awaitClosePending(.statements);
        self.lock.lockUncancelable(self.io);
        self.lock.unlock(self.io);
        self.awaitClosePending(.lifetimes);
    }

    const ClosePending = enum { statements, lifetimes };

    /// Waits, with no deadline, until nothing of `pending` is held. Once the
    /// wait passes a few seconds it logs what is still held, and keeps
    /// logging periodically, so a close stuck on a leaked lease says so.
    fn awaitClosePending(self: *StatementGate, pending: ClosePending) void {
        const io = self.io;
        const protection = io.swapCancelProtection(.blocked);
        defer _ = io.swapCancelProtection(protection);
        const start = Io.Clock.Timestamp.now(io, .awake);
        var log_at_secs: i64 = CLOSE_FIRST_LOG_SECS;
        self.owners_mutex.lockUncancelable(io);
        defer self.owners_mutex.unlock(io);
        while (self.closePendingCountLocked(pending) != 0) {
            const log_at = start.addDuration(.{ .raw = .fromSeconds(log_at_secs), .clock = .awake });
            if (Io.Clock.Timestamp.now(io, .awake).compare(.gte, log_at)) {
                self.logClosePendingLocked(pending, log_at_secs);
                log_at_secs += CLOSE_LOG_INTERVAL_SECS;
                continue;
            }
            // Read under the mutex: a release after this bumps the word, so
            // the futex wait returns at once instead of missing it.
            const seen = self.close_releases.load(.acquire);
            self.owners_mutex.unlock(io);
            io.futexWaitTimeout(u32, &self.close_releases.raw, seen, .{ .deadline = log_at }) catch |err| switch (err) {
                error.Canceled => unreachable,
            };
            self.owners_mutex.lockUncancelable(io);
        }
    }

    fn closePendingCountLocked(self: *const StatementGate, pending: ClosePending) usize {
        return switch (pending) {
            .statements => self.owners.count(),
            .lifetimes => self.allocator_owners,
        };
    }

    fn logClosePendingLocked(self: *const StatementGate, pending: ClosePending, waited_secs: i64) void {
        switch (pending) {
            .statements => {
                std.log.warn("database close has waited {d}s for {d} statement lease(s)", .{ waited_secs, self.owners.count() });
                var it = self.owners.iterator();
                while (it.next()) |entry| {
                    const mode = if (entry.value_ptr.exclusive) "exclusive" else "shared";
                    std.log.warn("  thread {d}: {s} lease, {d} reference(s)", .{ entry.key_ptr.*, mode, entry.value_ptr.references });
                }
            },
            .lifetimes => std.log.warn("database close has waited {d}s for {d} query memory or background sweep lease(s)", .{ waited_secs, self.allocator_owners }),
        }
    }

    fn noteCloseReleaseLocked(self: *StatementGate) bool {
        if (!self.closing) return false;
        _ = self.close_releases.fetchAdd(1, .release);
        return true;
    }

    fn wakeCloser(self: *StatementGate) void {
        self.io.futexWake(u32, &self.close_releases.raw, std.math.maxInt(u32));
    }

    pub fn acquire(self: *StatementGate, exclusive: bool) !Lease {
        if (self.recovery_required.load(.acquire)) return error.RecoveryRequired;
        const owner = std.Thread.getCurrentId();
        self.owners_mutex.lockUncancelable(self.io);
        if (self.owners.getPtr(owner)) |entry| {
            if (exclusive and !entry.exclusive) {
                self.owners_mutex.unlock(self.io);
                return error.TableBusy;
            }
            entry.references += 1;
            self.owners_mutex.unlock(self.io);
            return .{ .gate = self, .owner = owner };
        }
        if (self.closing and self.closing_owner != owner) {
            self.owners_mutex.unlock(self.io);
            return error.DatabaseClosed;
        }
        self.owners_mutex.unlock(self.io);
        if (exclusive) self.lock.lockUncancelable(self.io) else self.lock.lockSharedUncancelable(self.io);
        errdefer if (exclusive) self.lock.unlock(self.io) else self.lock.unlockShared(self.io);
        if (self.recovery_required.load(.acquire)) return error.RecoveryRequired;
        self.owners_mutex.lockUncancelable(self.io);
        defer self.owners_mutex.unlock(self.io);
        if (self.closing and self.closing_owner != owner) return error.DatabaseClosed;
        try self.owners.put(self.allocator, owner, .{ .exclusive = exclusive, .references = 1 });
        return .{ .gate = self, .owner = owner };
    }
};

test "statement gate retains nested readers and reuses the writer lease" {
    var gate = StatementGate.init(std.testing.allocator, std.testing.io);
    defer gate.deinit();
    const first = try gate.acquire(false);
    const second = try gate.acquire(false);
    first.release();
    try std.testing.expectError(error.TableBusy, gate.acquire(true));
    second.release();
    const writer = try gate.acquire(true);
    const nested = try gate.acquire(false);
    nested.release();
    writer.release();
    try std.testing.expectEqual(@as(usize, 0), gate.owners.count());
}

test "statement gate waits for allocator cleanup without blocking later statements" {
    var gate = StatementGate.init(std.testing.allocator, std.testing.io);
    defer gate.deinit();
    const lifetime = try gate.retainAllocator();
    const writer = try gate.acquire(true);
    writer.release();
    const Closer = struct {
        gate: *StatementGate,
        done: std.atomic.Value(bool) = .init(false),
        fn run(self: *@This()) void {
            self.gate.beginClose();
            self.done.store(true, .release);
        }
    };
    var closer = Closer{ .gate = &gate };
    const thread = std.Thread.spawn(.{}, Closer.run, .{&closer}) catch |err| {
        lifetime.release();
        return err;
    };
    defer thread.join();
    defer lifetime.release();
    while (true) {
        gate.owners_mutex.lockUncancelable(gate.io);
        const closing = gate.closing;
        gate.owners_mutex.unlock(gate.io);
        if (closing) break;
        std.atomic.spinLoopHint();
    }
    try std.testing.expect(!closer.done.load(.acquire));
    try std.testing.expectError(error.DatabaseClosed, gate.acquire(false));
}

test "statement gate close waits for a held statement lease" {
    var gate = StatementGate.init(std.testing.allocator, std.testing.io);
    defer gate.deinit();
    const reader = try gate.acquire(false);
    const Closer = struct {
        gate: *StatementGate,
        done: std.atomic.Value(bool) = .init(false),
        fn run(self: *@This()) void {
            self.gate.beginClose();
            self.done.store(true, .release);
        }
    };
    var closer = Closer{ .gate = &gate };
    const thread = std.Thread.spawn(.{}, Closer.run, .{&closer}) catch |err| {
        reader.release();
        return err;
    };
    {
        defer thread.join();
        defer reader.release();
        while (true) {
            gate.owners_mutex.lockUncancelable(gate.io);
            const closing = gate.closing;
            gate.owners_mutex.unlock(gate.io);
            if (closing) break;
            std.atomic.spinLoopHint();
        }
        const nested = try gate.acquire(false);
        nested.release();
        try std.testing.expect(!closer.done.load(.acquire));
    }
    try std.testing.expect(closer.done.load(.acquire));
}
