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
    allocator_idle: Io.Condition = .init,

    const Owner = struct { exclusive: bool, references: usize };

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
            gate.owners_mutex.unlock(gate.io);
            if (done) {
                if (exclusive) gate.lock.unlock(gate.io) else gate.lock.unlockShared(gate.io);
            }
        }
    };

    pub const LifetimeLease = struct {
        gate: *StatementGate,

        pub fn release(self: LifetimeLease) void {
            self.gate.owners_mutex.lockUncancelable(self.gate.io);
            std.debug.assert(self.gate.allocator_owners > 0);
            self.gate.allocator_owners -= 1;
            if (self.gate.allocator_owners == 0) self.gate.allocator_idle.broadcast(self.gate.io);
            self.gate.owners_mutex.unlock(self.gate.io);
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
        self.lock.lockUncancelable(self.io);
        self.lock.unlock(self.io);
        self.owners_mutex.lockUncancelable(self.io);
        defer self.owners_mutex.unlock(self.io);
        while (self.allocator_owners != 0) self.allocator_idle.waitUncancelable(self.io, &self.owners_mutex);
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
