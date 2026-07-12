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
    /// Socket handle for the net_read_timeout reaper (#164). Set once,
    /// before `Registry.register` publishes this state (the register
    /// lock is the publication barrier). Null for transports that
    /// don't arm the reaper.
    reap_socket: ?std.Io.net.Socket.Handle = null,
    /// Transfer-wait mark: `(began_ms << 2) | class`, 0 = no transfer
    /// in flight. `began_ms` is the awake clock in milliseconds when
    /// the currently-posted socket operation began. Classes:
    ///
    ///   0 (nonzero ms) — packet-HEADER read: idle-between-commands,
    ///     unbounded, but probed for the wedged-read signature (bytes
    ///     queued while the read pends).
    ///   1 — packet-BODY read: the client committed to a length, so
    ///     the wait is bounded hard by net_read_timeout (MySQL
    ///     semantics).
    ///   2 — response WRITE: bounded by net_write_timeout (2× the
    ///     read timeout, mirroring MySQL's 30/60 defaults).
    ///
    /// Why (#164): the 2026-07-11 incident held one sink connection at
    /// zero packets for 559 s while the server kept serving others — a
    /// server must never depend on the CLIENT's timeout for its own
    /// liveness. These marks make every socket transfer observable and
    /// every stall bounded, whatever the underlying cause.
    transfer_wait: std.atomic.Value(u64) = .{ .raw = 0 },

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

    pub fn beginRead(self: *ConnectionState, now_ms: u64, mid_packet: bool) void {
        self.transfer_wait.store((@max(now_ms, 1) << 2) | @intFromBool(mid_packet), .release);
    }

    pub fn beginWrite(self: *ConnectionState, now_ms: u64) void {
        self.transfer_wait.store((@max(now_ms, 1) << 2) | 2, .release);
    }

    pub fn endTransfer(self: *ConnectionState) void {
        self.transfer_wait.store(0, .release);
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

    /// Grace before probing a header-wait for the wedged-read signature.
    /// Long enough that a legitimately in-flight command (client mid-send)
    /// never probes positive; short enough that a wedged sink connection
    /// recovers in seconds, not minutes.
    const wedge_probe_grace_ms: u64 = 10_000;

    /// Stalled-read enforcement (#164). Two independent conditions:
    ///
    ///   1. net_read_timeout: a mid-packet read (client committed to a
    ///      length, payload incomplete) older than `timeout_ms`. MySQL
    ///      semantics (its default is 30 s).
    ///   2. Wedged read: a header read pending past the grace period
    ///      while the socket has bytes QUEUED — an idle connection has
    ///      an empty receive buffer, so queued-but-undelivered bytes
    ///      mean the pended read lost its completion (Windows AFD race,
    ///      the 559 s incident signature). Idle connections are never
    ///      touched: no bytes, no reap, no matter how long they idle.
    ///
    /// Shutdown — not close — so the handle stays valid for the owning
    /// thread (no reuse race); the pending read completes with EOF or
    /// reset and the connection thread exits through its normal error
    /// path. Runs under the registry lock, which excludes a concurrent
    /// unregister: an entry seen here cannot have had its socket closed
    /// yet (close happens after unregister on the connection thread).
    ///
    /// `io` supplies netShutdown — on Windows the Io.net sockets are
    /// AFD handles that ws2_32 calls reject, so the shutdown must go
    /// through the same Io vtable that opened them.
    pub fn reapStalledReads(self: *Registry, io: std.Io, now_ms: u64, timeout_ms: u64) usize {
        const afd_probe = @import("afd_probe.zig");
        self.mutex.lock();
        defer self.mutex.unlock();
        var reaped: usize = 0;
        var it = self.entries.valueIterator();
        while (it.next()) |entry| {
            const state = entry.*;
            const raw = state.transfer_wait.load(.acquire);
            if (raw == 0) continue;
            const began = raw >> 2;
            const class = raw & 3;
            if (now_ms < began) continue;
            const waited = now_ms - began;
            const handle = state.reap_socket orelse continue;

            switch (class) {
                1 => { // packet-body read: net_read_timeout
                    if (timeout_ms == 0 or waited < timeout_ms) continue;
                    std.debug.print(
                        "thindb: net_read_timeout: connection {d} stuck mid-packet for {d}ms — shutting down its socket\n",
                        .{ state.backend_id, waited },
                    );
                },
                2 => { // response write: net_write_timeout = 2× read timeout
                    if (timeout_ms == 0 or waited < timeout_ms * 2) continue;
                    std.debug.print(
                        "thindb: net_write_timeout: connection {d} response write stuck for {d}ms — shutting down its socket\n",
                        .{ state.backend_id, waited },
                    );
                },
                else => { // header read: idle unless bytes are queued
                    if (waited < wedge_probe_grace_ms) continue;
                    const avail = afd_probe.bytesAvailable(handle) orelse continue;
                    if (avail == 0) continue; // genuinely idle
                    std.debug.print(
                        "thindb: wedged read: connection {d} has {d} bytes queued but its read has pended {d}ms — shutting down its socket\n",
                        .{ state.backend_id, avail, waited },
                    );
                },
            }
            state.endTransfer(); // one-shot per stall
            io.vtable.netShutdown(io.userdata, handle, .both) catch {};
            reaped += 1;
        }
        return reaped;
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

test "transfer-wait marks encode class; reap skips unarmed sockets" {
    var threaded: std.Io.Threaded = .init(std.testing.allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    var reg = Registry.init(std.testing.allocator);
    defer reg.deinit();
    var s = ConnectionState.init(7, 0);
    try reg.register(&s);

    s.beginRead(500, false);
    try std.testing.expectEqual(@as(u64, 500 << 2), s.transfer_wait.load(.monotonic));
    s.beginRead(500, true);
    try std.testing.expectEqual(@as(u64, (500 << 2) | 1), s.transfer_wait.load(.monotonic));
    s.beginWrite(500);
    try std.testing.expectEqual(@as(u64, (500 << 2) | 2), s.transfer_wait.load(.monotonic));

    // No socket armed: even a grossly stale mid-packet mark is skipped.
    s.beginRead(1_000, true);
    try std.testing.expectEqual(@as(usize, 0), reg.reapStalledReads(io, 10_000_000, 15_000));

    s.endTransfer();
    try std.testing.expectEqual(@as(u64, 0), s.transfer_wait.load(.monotonic));
    reg.unregister(7);
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
