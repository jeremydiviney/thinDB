//! Process-wide connection registry — used for cross-connection
//! cancellation: MySQL `KILL QUERY <id>` and PG `CancelRequest` /
//! `pg_cancel_backend(pid)` both need to reach into another
//! connection's state, set its cancel flag, and let the executor
//! abort at the next batch boundary. The reaper sets the same flag
//! when a client disconnects mid-query (`cancelAbandonedQueries`).
//! MySQL `KILL [CONNECTION] <id>` and `pg_terminate_backend(pid)`
//! also close the target connection (`requestClose`).
//! `processList` snapshots what every connection is doing, for
//! SHOW PROCESSLIST and the process-list relations, so a runaway
//! query's id can be found to KILL it.
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

/// Awake-clock milliseconds: the time base of transfer marks and
/// activity timestamps.
pub fn nowMs(io: std.Io) u64 {
    const ns = std.Io.Clock.awake.now(io).nanoseconds;
    return @intCast(@divTrunc(@max(ns, 0), std.time.ns_per_ms));
}

/// The longest prefix of `text` that fits `max_len` bytes without
/// splitting a UTF-8 character.
pub fn utf8Prefix(text: []const u8, max_len: usize) []const u8 {
    if (text.len <= max_len) return text;
    var n = max_len;
    while (n > 0 and text[n] & 0xC0 == 0x80) n -= 1;
    return text[0..n];
}

/// Text copied into a fixed buffer, so another thread can snapshot it
/// without sharing the writer's allocations. Text that doesn't fit is
/// cut at a UTF-8 character boundary.
pub fn BoundedText(comptime capacity: usize) type {
    return struct {
        bytes: [capacity]u8 = undefined,
        len: usize = 0,

        pub fn set(self: *@This(), text: []const u8) void {
            const kept = utf8Prefix(text, capacity);
            @memcpy(self.bytes[0..kept.len], kept);
            self.len = kept.len;
        }

        pub fn slice(self: *const @This()) []const u8 {
            return self.bytes[0..self.len];
        }
    };
}

/// What a connection is doing, in MySQL's PROCESSLIST terms. Commands
/// that finish in microseconds (ping, statement close, ...) aren't
/// tracked; the connection reads as sleeping through them.
pub const Command = enum {
    connect,
    sleep,
    query,
    prepare,
    execute,

    pub fn label(self: Command) []const u8 {
        return switch (self) {
            .connect => "Connect",
            .sleep => "Sleep",
            .query => "Query",
            .prepare => "Prepare",
            .execute => "Execute",
        };
    }

    pub fn running(self: Command) bool {
        return switch (self) {
            .query, .prepare, .execute => true,
            .connect, .sleep => false,
        };
    }

    /// The PROCESSLIST State column.
    pub fn state(self: Command) []const u8 {
        return switch (self) {
            .connect => "login",
            .sleep => "",
            .query, .prepare, .execute => "executing",
        };
    }
};

pub const Activity = struct {
    /// Longest statement text kept for SHOW FULL PROCESSLIST.
    pub const max_info_len = 1024;

    /// Empty until the client authenticates.
    user: BoundedText(64) = .{},
    /// The client's address, `ip:port`.
    host: BoundedText(64) = .{},
    /// The PostgreSQL client's `application_name`; empty on MySQL.
    application: BoundedText(64) = .{},
    /// Awake-clock milliseconds when the connection was accepted.
    connected_ms: u64 = 0,
    db: BoundedText(128) = .{},
    command: Command = .connect,
    /// Awake-clock milliseconds when `command` began.
    since_ms: u64 = 0,
    /// The running statement's text; empty unless a statement runs.
    info: BoundedText(max_info_len) = .{},
};

pub const Process = struct {
    backend_id: u32,
    activity: Activity,
};

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
    /// Set while the connection runs a statement whose only product is
    /// its result set (`local.producesOnlyResult`). If the client closes
    /// the connection meanwhile, nobody is left to receive the result, so
    /// `Registry.cancelAbandonedQueries` cancels the statement.
    cancel_on_disconnect: std.atomic.Value(bool) = .{ .raw = false },
    /// Set by a KILL CONNECTION or pg_terminate_backend aimed at this
    /// connection. The connection's own thread closes the connection at
    /// its next command boundary, releasing the session exactly as a
    /// client disconnect does.
    close_requested: std.atomic.Value(bool) = .{ .raw = false },
    /// Socket handle for the transfer reaper (#164). Set once,
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
    ///   2 — response WRITE (`GuardedStreamWriter`): bounded by
    ///     net_write_timeout. A client that stops reading its result
    ///     would otherwise hold the statement's gate lease and core
    ///     slot for as long as it likes (#87).
    ///
    /// Why (#164): the 2026-07-11 incident held one sink connection at
    /// zero packets for 559 s while the server kept serving others — a
    /// server must never depend on the CLIENT's timeout for its own
    /// liveness. These marks make every socket transfer observable and
    /// every stall bounded, whatever the underlying cause.
    transfer_wait: std.atomic.Value(u64) = .{ .raw = 0 },
    /// Written by the connection thread, snapshotted by any thread that
    /// lists processes.
    activity: Activity = .{},
    activity_lock: SpinLock = .{},

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

    /// Clear a stale cancel before a new statement. A pending close keeps
    /// the flag set: with both flags seq_cst, a `requestClose` racing this
    /// either stores its cancel after the clear or has its close seen here.
    pub fn clearCancel(self: *ConnectionState) void {
        self.cancel_flag.store(false, .seq_cst);
        if (self.close_requested.load(.seq_cst)) self.cancel_flag.store(true, .seq_cst);
    }

    /// Interrupt the running statement and have this connection's thread
    /// close the connection at its next command boundary.
    pub fn requestClose(self: *ConnectionState) void {
        self.close_requested.store(true, .seq_cst);
        self.cancel_flag.store(true, .seq_cst);
    }

    pub fn closeRequested(self: *const ConnectionState) bool {
        return self.close_requested.load(.acquire);
    }

    pub fn isCancelled(self: *const ConnectionState) bool {
        return self.cancel_flag.load(.acquire);
    }

    pub fn setCancelOnDisconnect(self: *ConnectionState, on: bool) void {
        self.cancel_on_disconnect.store(on, .release);
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

    pub fn setPeer(self: *ConnectionState, host: []const u8, now_ms: u64) void {
        self.activity_lock.lock();
        defer self.activity_lock.unlock();
        self.activity.host.set(host);
        self.activity.since_ms = now_ms;
        self.activity.connected_ms = now_ms;
    }

    pub fn setApplication(self: *ConnectionState, name: []const u8) void {
        self.activity_lock.lock();
        defer self.activity_lock.unlock();
        self.activity.application.set(name);
    }

    pub fn setUser(self: *ConnectionState, user: []const u8) void {
        self.activity_lock.lock();
        defer self.activity_lock.unlock();
        self.activity.user.set(user);
    }

    pub fn beginCommand(self: *ConnectionState, command: Command, info: []const u8, db: []const u8, now_ms: u64) void {
        self.activity_lock.lock();
        defer self.activity_lock.unlock();
        self.activity.command = command;
        self.activity.info.set(info);
        self.activity.db.set(db);
        self.activity.since_ms = now_ms;
    }

    pub fn endCommand(self: *ConnectionState, db: []const u8, now_ms: u64) void {
        self.beginCommand(.sleep, "", db, now_ms);
    }

    pub fn snapshotActivity(self: *ConnectionState) Activity {
        self.activity_lock.lock();
        defer self.activity_lock.unlock();
        return self.activity;
    }
};

/// CAS-based spinlock — Zig 0.16's stdlib `std.Thread.Mutex` is
/// gone and the Io.Mutex requires an Io instance, which the
/// registry doesn't have a reason to depend on. The registry is
/// touched only on connection accept/close + cancellation, so
/// spinning is fine.
/// A connection's socket writer. Every send, including the implicit
/// ones when a large result overflows the buffer, carries a write mark
/// on the connection's state, so the reaper can end a send the client
/// has stopped draining. Must not move once `interface` is in use.
pub const GuardedStreamWriter = struct {
    stream_writer: std.Io.net.Stream.Writer,
    stream_vtable: *const std.Io.Writer.VTable,
    state: *ConnectionState,

    const vtable: std.Io.Writer.VTable = .{ .drain = drain, .sendFile = sendFile };

    pub fn init(stream: std.Io.net.Stream, io: std.Io, buffer: []u8, state: *ConnectionState) GuardedStreamWriter {
        var stream_writer = stream.writer(io, buffer);
        const stream_vtable = stream_writer.interface.vtable;
        stream_writer.interface.vtable = &vtable;
        return .{ .stream_writer = stream_writer, .stream_vtable = stream_vtable, .state = state };
    }

    pub fn interface(self: *GuardedStreamWriter) *std.Io.Writer {
        return &self.stream_writer.interface;
    }

    fn fromInterface(w: *std.Io.Writer) *GuardedStreamWriter {
        const stream_writer: *std.Io.net.Stream.Writer = @alignCast(@fieldParentPtr("interface", w));
        return @alignCast(@fieldParentPtr("stream_writer", stream_writer));
    }

    fn drain(w: *std.Io.Writer, data: []const []const u8, splat: usize) std.Io.Writer.Error!usize {
        const self = fromInterface(w);
        self.state.beginWrite(nowMs(self.stream_writer.io));
        defer self.state.endTransfer();
        return self.stream_vtable.drain(w, data, splat);
    }

    fn sendFile(w: *std.Io.Writer, file_reader: *std.Io.File.Reader, limit: std.Io.Limit) std.Io.Writer.FileError!usize {
        const self = fromInterface(w);
        self.state.beginWrite(nowMs(self.stream_writer.io));
        defer self.state.endTransfer();
        return self.stream_vtable.sendFile(w, file_reader, limit);
    }
};

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

    /// Kill a peer connection: cancel its statement, ask its thread to close
    /// the connection, and shut its socket down so a read or write the
    /// thread is blocked in returns now. The thread then leaves through its
    /// normal error path, which releases everything the connection holds.
    /// Windows is the exception: a shutdown there does not complete a read
    /// already pending, so an idle target leaves once its client closes its
    /// end or the stack times the half-closed connection out.
    /// Shutdown, not close, under the registry lock, for the reasons
    /// `reapStalledTransfers` gives. A connection killing itself calls
    /// `ConnectionState.requestClose` instead, so its reply still goes
    /// out. Returns false on an unknown id.
    pub fn requestClose(self: *Registry, io: std.Io, backend_id: u32) bool {
        self.mutex.lock();
        defer self.mutex.unlock();
        const state = self.entries.get(backend_id) orelse return false;
        state.requestClose();
        if (state.reap_socket) |handle| io.vtable.netShutdown(io.userdata, handle, .both) catch {};
        return true;
    }

    pub fn count(self: *Registry) usize {
        self.mutex.lock();
        defer self.mutex.unlock();
        return self.entries.count();
    }

    /// What every registered connection is doing, ordered by id. The
    /// caller owns the slice.
    pub fn processList(self: *Registry, allocator: Allocator) ![]Process {
        const list = blk: {
            self.mutex.lock();
            defer self.mutex.unlock();
            const list = try allocator.alloc(Process, self.entries.count());
            var it = self.entries.valueIterator();
            for (list) |*process| {
                const state = it.next().?.*;
                process.* = .{ .backend_id = state.backend_id, .activity = state.snapshotActivity() };
            }
            break :blk list;
        };
        std.mem.sortUnstable(Process, list, {}, processIdLessThan);
        return list;
    }

    fn processIdLessThan(_: void, a: Process, b: Process) bool {
        return a.backend_id < b.backend_id;
    }

    /// Grace before probing a header-wait for the wedged-read signature.
    /// Long enough that a legitimately in-flight command (client mid-send)
    /// never probes positive; short enough that a wedged sink connection
    /// recovers in seconds, not minutes.
    const wedge_probe_grace_ms: u64 = 10_000;

    /// How long a stalled transfer may wait before the reaper ends its
    /// connection, in milliseconds. 0 disables that class.
    pub const TransferLimits = struct {
        /// net_read_timeout: a read inside a packet (class 1). Also
        /// gates the wedged-read probe (class 0).
        read_ms: u64,
        /// net_write_timeout: a send the client is not draining (class 2).
        write_ms: u64,
    };

    /// Stalled-transfer enforcement (#164, #87). Three conditions:
    ///
    ///   1. net_read_timeout: a mid-packet read (client committed to a
    ///      length, payload incomplete) older than `read_ms`. MySQL
    ///      semantics (its default is 30 s).
    ///   2. Wedged read: a header read pending past the grace period
    ///      while the socket has bytes QUEUED — an idle connection has
    ///      an empty receive buffer, so queued-but-undelivered bytes
    ///      mean the pended read lost its completion (Windows AFD race,
    ///      the 559 s incident signature). Idle connections are never
    ///      touched: no bytes, no reap, no matter how long they idle.
    ///   3. net_write_timeout: a send older than `write_ms`. The client
    ///      has stopped reading, and the statement behind the send holds
    ///      its gate lease and core slot until the send ends.
    ///
    /// The connection is aborted, not closed (`socket_probe.abortTransfers`),
    /// so the handle stays valid for the owning thread (no reuse race);
    /// the pending transfer fails and the connection thread exits through
    /// its normal error path, releasing what the statement held. Runs
    /// under the registry lock, which excludes a concurrent unregister:
    /// an entry seen here cannot have had its socket closed yet (close
    /// happens after unregister on the connection thread).
    pub fn reapStalledTransfers(self: *Registry, io: std.Io, now_ms: u64, limits: TransferLimits) usize {
        const socket_probe = @import("socket_probe.zig");
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
                1 => { // packet-body read
                    if (limits.read_ms == 0 or waited < limits.read_ms) continue;
                    std.debug.print(
                        "thindb: net_read_timeout: connection {d} stuck mid-packet for {d}ms — aborting it\n",
                        .{ state.backend_id, waited },
                    );
                },
                2 => { // response write
                    if (limits.write_ms == 0 or waited < limits.write_ms) continue;
                    std.debug.print(
                        "thindb: net_write_timeout: connection {d} response write stuck for {d}ms — aborting it\n",
                        .{ state.backend_id, waited },
                    );
                },
                else => { // header read: idle unless bytes are queued
                    if (limits.read_ms == 0 or waited < wedge_probe_grace_ms) continue;
                    const avail = socket_probe.bytesAvailable(handle) orelse continue;
                    if (avail == 0) continue; // genuinely idle
                    std.debug.print(
                        "thindb: wedged read: connection {d} has {d} bytes queued but its read has pended {d}ms — aborting it\n",
                        .{ state.backend_id, avail, waited },
                    );
                },
            }
            state.endTransfer(); // one-shot per stall
            socket_probe.abortTransfers(self.allocator, io, handle);
            reaped += 1;
        }
        return reaped;
    }

    /// Cancel every result-only statement whose client has closed its
    /// connection. The connection thread would notice the close only when
    /// it next touched the socket, after the statement finished, so a
    /// long or runaway query otherwise keeps its cores and memory with
    /// nobody waiting for it. Statements that write are left to finish,
    /// as MySQL finishes them. Runs under the registry lock for the same
    /// reason as `reapStalledTransfers`: a registered socket is still open.
    pub fn cancelAbandonedQueries(self: *Registry) usize {
        const socket_probe = @import("socket_probe.zig");
        self.mutex.lock();
        defer self.mutex.unlock();
        var cancelled: usize = 0;
        var it = self.entries.valueIterator();
        while (it.next()) |entry| {
            const state = entry.*;
            if (!state.cancel_on_disconnect.load(.acquire) or state.isCancelled()) continue;
            const handle = state.reap_socket orelse continue;
            if (socket_probe.peerClosed(handle) != true) continue;
            std.debug.print("thindb: connection {d} closed by its client mid-query — cancelling the query\n", .{state.backend_id});
            state.requestCancel();
            cancelled += 1;
        }
        return cancelled;
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
    try std.testing.expectEqual(@as(usize, 0), reg.reapStalledTransfers(io, 10_000_000, .{ .read_ms = 15_000, .write_ms = 30_000 }));

    s.endTransfer();
    try std.testing.expectEqual(@as(u64, 0), s.transfer_wait.load(.monotonic));
    reg.unregister(7);
}

const StalledSender = struct {
    io: std.Io,
    stream: std.Io.net.Stream,
    state: *ConnectionState,
    result: ?std.Io.Writer.Error = null,
    bytes_sent: usize = 0,
    done: std.atomic.Value(bool) = .init(false),

    /// Far more than loopback buffers hold, so the send blocks for good
    /// once the peer stops reading.
    const give_up_bytes: usize = 1 << 30;

    fn run(self: *StalledSender) void {
        defer self.done.store(true, .release);
        var buffer: [16 * 1024]u8 = undefined;
        var writer: GuardedStreamWriter = .init(self.stream, self.io, &buffer, self.state);
        const chunk: [64 * 1024]u8 = @splat('x');
        while (self.bytes_sent < give_up_bytes) : (self.bytes_sent += chunk.len) {
            writer.interface().writeAll(&chunk) catch |err| {
                self.result = err;
                return;
            };
        }
    }
};

test "the write deadline aborts a send the client stopped draining" {
    const io = std.testing.io;
    const address = try std.Io.net.IpAddress.parse("127.0.0.1", 0);
    var listener = try address.listen(io, .{ .mode = .stream, .protocol = .tcp });
    defer listener.deinit(io);
    const client = try listener.socket.address.connect(io, .{ .mode = .stream, .protocol = .tcp });
    var client_open = true;
    defer if (client_open) client.close(io);
    const accepted = try listener.accept(io);
    defer accepted.close(io);

    var reg = Registry.init(std.testing.allocator);
    defer reg.deinit();
    var s = ConnectionState.init(5, 0);
    s.reap_socket = accepted.socket.handle;
    try reg.register(&s);
    defer reg.unregister(5);

    var sender: StalledSender = .{ .io = io, .stream = accepted, .state = &s };
    const thread = try std.Thread.spawn(.{}, StalledSender.run, .{&sender});
    var reaped: usize = 0;
    for (0..1000) |_| {
        reaped = reg.reapStalledTransfers(io, nowMs(io), .{ .read_ms = 0, .write_ms = 200 });
        if (reaped > 0) break;
        try std.Io.sleep(io, .fromMilliseconds(10), .awake);
    }
    var send_ended = false;
    for (0..10_000) |_| {
        send_ended = sender.done.load(.acquire);
        if (send_ended) break;
        try std.Io.sleep(io, .fromMilliseconds(1), .awake);
    }
    if (!send_ended) {
        // Unblock the sender so a failing run ends instead of hanging.
        client.close(io);
        client_open = false;
    }
    thread.join();

    try std.testing.expectEqual(@as(usize, 1), reaped);
    try std.testing.expect(send_ended);
    try std.testing.expectEqual(@as(?std.Io.Writer.Error, error.WriteFailed), sender.result);
    try std.testing.expectEqual(@as(u64, 0), s.transfer_wait.load(.acquire));
}

test "cancelAbandonedQueries cancels only an armed statement whose client is gone" {
    const socket_probe = @import("socket_probe.zig");
    const io = std.testing.io;
    const address = try std.Io.net.IpAddress.parse("127.0.0.1", 0);
    var listener = try address.listen(io, .{ .mode = .stream, .protocol = .tcp });
    defer listener.deinit(io);
    const client = try listener.socket.address.connect(io, .{ .mode = .stream, .protocol = .tcp });
    var client_open = true;
    defer if (client_open) client.close(io);
    const accepted = try listener.accept(io);
    defer accepted.close(io);

    var reg = Registry.init(std.testing.allocator);
    defer reg.deinit();
    var s = ConnectionState.init(9, 0);
    s.reap_socket = accepted.socket.handle;
    try reg.register(&s);
    defer reg.unregister(9);

    s.setCancelOnDisconnect(true);
    try std.testing.expectEqual(@as(usize, 0), reg.cancelAbandonedQueries());
    try std.testing.expect(!s.isCancelled());

    client.close(io);
    client_open = false;
    for (0..100) |_| {
        if (socket_probe.peerClosed(accepted.socket.handle) == true) break;
        try std.Io.sleep(io, .fromMilliseconds(10), .awake);
    }

    s.setCancelOnDisconnect(false);
    try std.testing.expectEqual(@as(usize, 0), reg.cancelAbandonedQueries());
    try std.testing.expect(!s.isCancelled());

    s.setCancelOnDisconnect(true);
    try std.testing.expectEqual(@as(usize, 1), reg.cancelAbandonedQueries());
    try std.testing.expect(s.isCancelled());
    try std.testing.expectEqual(@as(usize, 0), reg.cancelAbandonedQueries());
}

test "requestClose cancels, marks the close and shuts the peer's socket down" {
    const io = std.testing.io;
    const address = try std.Io.net.IpAddress.parse("127.0.0.1", 0);
    var listener = try address.listen(io, .{ .mode = .stream, .protocol = .tcp });
    defer listener.deinit(io);
    const client = try listener.socket.address.connect(io, .{ .mode = .stream, .protocol = .tcp });
    defer client.close(io);
    const accepted = try listener.accept(io);
    defer accepted.close(io);

    var reg = Registry.init(std.testing.allocator);
    defer reg.deinit();
    var s = ConnectionState.init(4, 0);
    s.reap_socket = accepted.socket.handle;
    try reg.register(&s);
    defer reg.unregister(4);

    try std.testing.expect(!reg.requestClose(io, 99));
    try std.testing.expect(reg.requestClose(io, 4));
    try std.testing.expect(s.closeRequested());
    try std.testing.expect(s.isCancelled());
    s.clearCancel();
    try std.testing.expect(s.isCancelled());

    var buf: [8]u8 = undefined;
    var reader = client.reader(io, &buf);
    try std.testing.expectError(error.EndOfStream, reader.interface.takeByte());
}

test "processList snapshots each connection's activity in id order" {
    var reg = Registry.init(std.testing.allocator);
    defer reg.deinit();
    var s5 = ConnectionState.init(5, 0);
    var s2 = ConnectionState.init(2, 0);
    s5.setPeer("10.0.0.5:4000", 1_000);
    s2.setPeer("10.0.0.2:4000", 1_000);
    try reg.register(&s5);
    try reg.register(&s2);
    defer reg.unregister(5);
    defer reg.unregister(2);

    s2.setUser("alice");
    s2.beginCommand(.query, "SELECT 1", "main__public", 2_000);
    s5.setUser("bob");
    s5.endCommand("main__sales", 3_000);

    const list = try reg.processList(std.testing.allocator);
    defer std.testing.allocator.free(list);
    try std.testing.expectEqual(@as(usize, 2), list.len);

    try std.testing.expectEqual(@as(u32, 2), list[0].backend_id);
    const running = list[0].activity;
    try std.testing.expectEqualStrings("alice", running.user.slice());
    try std.testing.expectEqualStrings("10.0.0.2:4000", running.host.slice());
    try std.testing.expectEqualStrings("main__public", running.db.slice());
    try std.testing.expectEqual(Command.query, running.command);
    try std.testing.expectEqual(@as(u64, 2_000), running.since_ms);
    try std.testing.expectEqualStrings("SELECT 1", running.info.slice());

    try std.testing.expectEqual(@as(u32, 5), list[1].backend_id);
    const idle = list[1].activity;
    try std.testing.expectEqualStrings("bob", idle.user.slice());
    try std.testing.expectEqualStrings("main__sales", idle.db.slice());
    try std.testing.expectEqual(Command.sleep, idle.command);
    try std.testing.expectEqual(@as(u64, 3_000), idle.since_ms);
    try std.testing.expectEqualStrings("", idle.info.slice());
}

test "BoundedText cuts long text at a character boundary" {
    var text: BoundedText(4) = .{};
    text.set("abc");
    try std.testing.expectEqualStrings("abc", text.slice());
    text.set("abcdef");
    try std.testing.expectEqualStrings("abcd", text.slice());
    text.set("abéé");
    try std.testing.expectEqualStrings("abé", text.slice());
    text.set("a€b");
    try std.testing.expectEqualStrings("a€", text.slice());
    text.set("ab€");
    try std.testing.expectEqualStrings("ab", text.slice());
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
