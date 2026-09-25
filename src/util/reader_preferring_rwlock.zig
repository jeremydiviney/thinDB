//! Reader-preferring RW lock for `Table.ddl_lock`. A shared acquire waits only
//! while a writer HOLDS the lock, never behind one that is merely queued.
//!
//! `std.Io.RwLock` prefers writers: once a writer queues, every new shared
//! acquire parks behind it. One statement takes a table's ddl_lock shared once
//! per Scan it opens, and a CTE plan opens several Scans on the same table
//! while the earlier ones still hold theirs. A background compaction commit
//! queuing between two of those acquires then deadlocks: the commit waits for
//! the statement's held Scans, and the statement's next Scan waits for the
//! commit (2026-09-25: four concurrent INSERT ... SELECTs over one CTE plan
//! wedged at zero CPU).
//!
//! The cost is that a writer can wait for as long as readers keep overlapping.
//! DDL takes the catalog statement gate exclusively before this lock, so no new
//! statement starts and the readers drain. A compaction commit waits for a gap,
//! but only until a deadline (`lockBefore`): on a table whose readers never
//! drain it gives up rather than wait forever.

const std = @import("std");
const Io = std.Io;

pub const ReaderPreferringRwLock = struct {
    mutex: Io.Mutex = .init,
    changed: Io.Condition = .init,
    readers: usize = 0,
    writing: bool = false,
    /// `lockBefore` callers parked on `released`. The condition has no timed
    /// wait, so they sleep on this futex word instead.
    timed_writers: usize = 0,
    /// Bumped each time the lock becomes free while a timed writer waits.
    released: std.atomic.Value(u32) = .init(0),

    pub const init: ReaderPreferringRwLock = .{};

    pub fn lockSharedUncancelable(self: *ReaderPreferringRwLock, io: Io) void {
        self.mutex.lockUncancelable(io);
        defer self.mutex.unlock(io);
        while (self.writing) self.changed.waitUncancelable(io, &self.mutex);
        self.readers += 1;
    }

    pub fn unlockShared(self: *ReaderPreferringRwLock, io: Io) void {
        self.mutex.lockUncancelable(io);
        defer self.mutex.unlock(io);
        self.readers -= 1;
        if (self.readers == 0) {
            self.changed.broadcast(io);
            self.wakeTimedWriters(io);
        }
    }

    pub fn lockUncancelable(self: *ReaderPreferringRwLock, io: Io) void {
        self.mutex.lockUncancelable(io);
        defer self.mutex.unlock(io);
        while (self.writing or self.readers > 0) self.changed.waitUncancelable(io, &self.mutex);
        self.writing = true;
    }

    /// `lockUncancelable` that gives up once `deadline` passes. Returns
    /// false, without the lock, when it timed out.
    pub fn lockBefore(self: *ReaderPreferringRwLock, io: Io, deadline: Io.Clock.Timestamp) bool {
        self.mutex.lockUncancelable(io);
        defer self.mutex.unlock(io);
        self.timed_writers += 1;
        defer self.timed_writers -= 1;
        while (self.writing or self.readers > 0) {
            if (Io.Clock.Timestamp.now(io, deadline.clock).compare(.gte, deadline)) return false;
            // Read under the mutex: a release after this bumps the word, so the
            // futex wait below returns at once instead of missing it.
            const seen = self.released.load(.acquire);
            self.mutex.unlock(io);
            const waited = io.futexWaitTimeout(u32, &self.released.raw, seen, .{ .deadline = deadline });
            self.mutex.lockUncancelable(io);
            waited catch return false;
        }
        self.writing = true;
        return true;
    }

    pub fn unlock(self: *ReaderPreferringRwLock, io: Io) void {
        self.mutex.lockUncancelable(io);
        defer self.mutex.unlock(io);
        self.writing = false;
        self.changed.broadcast(io);
        self.wakeTimedWriters(io);
    }

    fn wakeTimedWriters(self: *ReaderPreferringRwLock, io: Io) void {
        if (self.timed_writers == 0) return;
        _ = self.released.fetchAdd(1, .release);
        io.futexWake(u32, &self.released.raw, std.math.maxInt(u32));
    }
};

const Contender = struct {
    lock: *ReaderPreferringRwLock,
    io: Io,
    exclusive: bool,
    started: std.atomic.Value(bool) = .init(false),
    acquired: std.atomic.Value(bool) = .init(false),

    fn run(self: *Contender) void {
        self.started.store(true, .release);
        if (self.exclusive) self.lock.lockUncancelable(self.io) else self.lock.lockSharedUncancelable(self.io);
        self.acquired.store(true, .release);
        if (self.exclusive) self.lock.unlock(self.io) else self.lock.unlockShared(self.io);
    }

    /// Spawn the contender and give it time to park on the lock.
    fn start(self: *Contender) !std.Thread {
        const thread = try std.Thread.spawn(.{}, run, .{self});
        while (!self.started.load(.acquire)) std.Thread.yield() catch {};
        try Io.sleep(self.io, .fromMilliseconds(20), .awake);
        return thread;
    }
};

test "a shared holder takes the lock again while a writer waits" {
    const io = std.testing.io;
    var lock: ReaderPreferringRwLock = .init;
    lock.lockSharedUncancelable(io);

    var writer: Contender = .{ .lock = &lock, .io = io, .exclusive = true };
    const thread = try writer.start();
    lock.lockSharedUncancelable(io);
    try std.testing.expect(!writer.acquired.load(.acquire));
    lock.unlockShared(io);
    try std.testing.expect(!writer.acquired.load(.acquire));
    lock.unlockShared(io);

    thread.join();
    try std.testing.expect(writer.acquired.load(.acquire));
}

fn deadlineIn(io: Io, ms: i64) Io.Clock.Timestamp {
    return .fromNow(io, .{ .raw = .fromMilliseconds(ms), .clock = .awake });
}

test "a timed writer gives up while readers keep the lock" {
    const io = std.testing.io;
    var lock: ReaderPreferringRwLock = .init;
    lock.lockSharedUncancelable(io);
    try std.testing.expect(!lock.lockBefore(io, deadlineIn(io, 30)));
    lock.lockSharedUncancelable(io);
    lock.unlockShared(io);
    lock.unlockShared(io);
    try std.testing.expect(lock.lockBefore(io, deadlineIn(io, 30)));
    lock.unlock(io);
}

test "a timed writer takes the lock when the last reader leaves" {
    const io = std.testing.io;
    var lock: ReaderPreferringRwLock = .init;
    lock.lockSharedUncancelable(io);
    const Writer = struct {
        lock: *ReaderPreferringRwLock,
        io: Io,
        waiting: std.atomic.Value(bool) = .init(false),
        acquired: std.atomic.Value(bool) = .init(false),
        fn run(self: *@This()) void {
            self.waiting.store(true, .release);
            if (self.lock.lockBefore(self.io, deadlineIn(self.io, 10_000))) {
                self.acquired.store(true, .release);
                self.lock.unlock(self.io);
            }
        }
    };
    var writer: Writer = .{ .lock = &lock, .io = io };
    const thread = try std.Thread.spawn(.{}, Writer.run, .{&writer});
    while (!writer.waiting.load(.acquire)) std.Thread.yield() catch {};
    try Io.sleep(io, .fromMilliseconds(20), .awake);
    try std.testing.expect(!writer.acquired.load(.acquire));
    lock.unlockShared(io);
    thread.join();
    try std.testing.expect(writer.acquired.load(.acquire));
}

test "a reader waits while a writer holds the lock" {
    const io = std.testing.io;
    var lock: ReaderPreferringRwLock = .init;
    lock.lockUncancelable(io);

    var reader: Contender = .{ .lock = &lock, .io = io, .exclusive = false };
    const thread = try reader.start();
    try std.testing.expect(!reader.acquired.load(.acquire));
    lock.unlock(io);

    thread.join();
    try std.testing.expect(reader.acquired.load(.acquire));
}
