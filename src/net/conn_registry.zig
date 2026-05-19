//! Process-wide connection registry — used for cross-connection
//! cancellation: MySQL `KILL <id>` and PG `CancelRequest` /
//! `pg_cancel_backend(pid)` both need to reach into another
//! connection's state, set its cancel flag, and let the executor
//! abort at the next batch boundary.
//!
//! One Registry is shared across all wire frontends (mysql, pg,
//! native). Each accepted connection registers a ConnectionState on
//! accept and unregisters on disconnect.
//!
//! The cancel flag is best-effort: it's polled at batch boundaries
//! inside `CompiledQuery.next()`. A query mid-batch (e.g. a hash
//! aggregate consuming 10M rows into a single output batch) won't
//! see the cancel until it finishes the batch. Closing-the-socket
//! style hard cancellation would need operator-level cooperation and
//! isn't in v1.

const std = @import("std");
const Allocator = std.mem.Allocator;

pub const ConnectionState = struct {
    /// Process-unique connection id. Stable for the connection's
    /// lifetime. Equal to the value sent as PG BackendKeyData.pid /
    /// the MySQL HandshakeV10 connection_id.
    backend_id: u32,
    /// PG CancelRequest carries (pid, secret). We verify secret to
    /// stop a malicious peer who knows the pid from cancelling
    /// someone else's query. MySQL KILL has no secret — pass 0 to
    /// `requestCancel` to skip the check.
    secret_key: u32,
    /// Polled by CompiledQuery.next() at batch boundaries. Setting
    /// it to true causes the in-flight query to abort with
    /// error.QueryCancelled. Reset to false when a new query starts.
    cancel_flag: std.atomic.Value(bool) = .{ .raw = false },

    pub fn init(backend_id: u32, secret_key: u32) ConnectionState {
        return .{ .backend_id = backend_id, .secret_key = secret_key };
    }

    /// Derive a stable secret key from a backend id alone. Used by
    /// transports that don't have a separate crypto context (the
    /// MySQL HandshakeV10 connection_id is the only token they have;
    /// PG mints the BackendKeyData secret from the same id). The
    /// derivation is intentionally not cryptographically strong — the
    /// PG `CancelRequest` protocol just needs "an attacker who didn't
    /// see the OK handshake can't predict the secret from the pid."
    pub fn deriveSecret(backend_id: u32) u32 {
        return backend_id ^ 0xA1B2C3D4;
    }

    pub fn requestCancel(self: *ConnectionState) void {
        self.cancel_flag.store(true, .release);
    }

    pub fn clearCancel(self: *ConnectionState) void {
        self.cancel_flag.store(false, .release);
    }

    pub fn isCancelled(self: *const ConnectionState) bool {
        return self.cancel_flag.load(.acquire);
    }
};

/// CAS-based spinlock — Zig 0.16's stdlib `std.Thread.Mutex` is
/// gone and the Io.Mutex requires an Io instance, which the
/// registry doesn't have a reason to depend on. The registry is
/// touched only on connection accept/close + cancellation, so
/// spinning is fine.
const SpinLock = struct {
    state: std.atomic.Value(bool) = .{ .raw = false },

    fn lock(self: *SpinLock) void {
        while (self.state.cmpxchgWeak(false, true, .acquire, .monotonic) != null) {
            std.atomic.spinLoopHint();
        }
    }

    fn unlock(self: *SpinLock) void {
        self.state.store(false, .release);
    }
};

pub const Registry = struct {
    allocator: Allocator,
    mutex: SpinLock = .{},
    entries: std.AutoHashMapUnmanaged(u32, *ConnectionState) = .empty,
    next_id: std.atomic.Value(u32) = .{ .raw = 0 },

    pub fn init(allocator: Allocator) Registry {
        return .{ .allocator = allocator };
    }

    pub fn deinit(self: *Registry) void {
        self.mutex.lock();
        defer self.mutex.unlock();
        self.entries.deinit(self.allocator);
    }

    /// Reserve a fresh backend_id. Stable across the connection's
    /// lifetime; suitable as the PG BackendKeyData.pid /
    /// MySQL HandshakeV10 connection_id.
    pub fn nextBackendId(self: *Registry) u32 {
        return self.next_id.fetchAdd(1, .monotonic) +% 1;
    }

    pub fn register(self: *Registry, state: *ConnectionState) !void {
        self.mutex.lock();
        defer self.mutex.unlock();
        try self.entries.put(self.allocator, state.backend_id, state);
    }

    pub fn unregister(self: *Registry, backend_id: u32) void {
        self.mutex.lock();
        defer self.mutex.unlock();
        _ = self.entries.remove(backend_id);
    }

    /// Request cancellation of a peer connection's in-flight query.
    /// `secret_or_zero == 0` skips the secret check (used by MySQL
    /// KILL which has no secret). Returns true iff the cancel was
    /// applied; false on unknown id or secret mismatch.
    pub fn requestCancel(self: *Registry, backend_id: u32, secret_or_zero: u32) bool {
        self.mutex.lock();
        defer self.mutex.unlock();
        const state = self.entries.get(backend_id) orelse return false;
        if (secret_or_zero != 0 and state.secret_key != secret_or_zero) return false;
        state.requestCancel();
        return true;
    }

    pub fn count(self: *Registry) usize {
        self.mutex.lock();
        defer self.mutex.unlock();
        return self.entries.count();
    }
};

test "register / requestCancel / unregister" {
    var reg = Registry.init(std.testing.allocator);
    defer reg.deinit();

    var s1 = ConnectionState.init(1, 0xABCD);
    var s2 = ConnectionState.init(2, 0x1234);
    try reg.register(&s1);
    try reg.register(&s2);
    try std.testing.expectEqual(@as(usize, 2), reg.count());

    // MySQL-style: no secret.
    try std.testing.expect(reg.requestCancel(1, 0));
    try std.testing.expect(s1.isCancelled());
    try std.testing.expect(!s2.isCancelled());

    // PG-style: correct secret accepted.
    try std.testing.expect(reg.requestCancel(2, 0x1234));
    try std.testing.expect(s2.isCancelled());

    // Unknown id rejected.
    try std.testing.expect(!reg.requestCancel(99, 0));

    s1.clearCancel();
    try std.testing.expect(!s1.isCancelled());

    reg.unregister(1);
    try std.testing.expectEqual(@as(usize, 1), reg.count());
    try std.testing.expect(!reg.requestCancel(1, 0));
}

test "nextBackendId is monotonically increasing" {
    var reg = Registry.init(std.testing.allocator);
    defer reg.deinit();

    const a = reg.nextBackendId();
    const b = reg.nextBackendId();
    const c = reg.nextBackendId();
    try std.testing.expect(a < b);
    try std.testing.expect(b < c);
}
