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
//! statement starts and the readers drain. A compaction commit waits for a gap.

const std = @import("std");
const Io = std.Io;

pub const ReaderPreferringRwLock = struct {
    mutex: Io.Mutex = .init,
    changed: Io.Condition = .init,
    readers: usize = 0,
    writing: bool = false,

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
        if (self.readers == 0) self.changed.broadcast(io);
    }

    pub fn lockUncancelable(self: *ReaderPreferringRwLock, io: Io) void {
        self.mutex.lockUncancelable(io);
        defer self.mutex.unlock(io);
        while (self.writing or self.readers > 0) self.changed.waitUncancelable(io, &self.mutex);
        self.writing = true;
    }

    pub fn unlock(self: *ReaderPreferringRwLock, io: Io) void {
        self.mutex.lockUncancelable(io);
        defer self.mutex.unlock(io);
        self.writing = false;
        self.changed.broadcast(io);
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
