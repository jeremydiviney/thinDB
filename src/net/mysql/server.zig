//! MySQL wire-compatible server. Accepts connections, runs the
//! HandshakeV10 + HandshakeResponse41 exchange, then dispatches
//! COM_QUERY / COM_INIT_DB / COM_PING / COM_QUIT.
//!
//! Auth is trust-mode: credentials are read and discarded. The session
//! tracks (current_db, current_schema) and routes queries through the
//! existing SQL parser → `compileWithSession` pipeline.

const std = @import("std");
const Allocator = std.mem.Allocator;
const Io = std.Io;

/// Default TCP port for a MySQL-wire listener. Matches the upstream
/// MySQL default so existing client tooling Just Works.
pub const default_port: u16 = 3306;

const thindb_api = @import("../../api/api.zig");
const Catalog = thindb_api.Catalog;
const Session = thindb_api.Session;
const SessionVars = thindb_api.SessionVars;
const TempNamespace = thindb_api.TempNamespace;
const ApiError = thindb_api.Error;
const Schema = thindb_api.Schema;
const Table = thindb_api.Table;

const local = @import("../local.zig");
const ir = @import("../../ir/ir.zig");
const PredicateExpr = @import("../../exec/predicate.zig").PredicateExpr;
const exec_predicate = @import("../../exec/predicate.zig");
const xa_mod = @import("../xa.zig");
const sql = @import("../../sql/sql.zig");
const types = @import("../../types.zig");
const core_scheduler = @import("../../util/core_scheduler.zig");

const packet = @import("packet.zig");
const handshake = @import("handshake.zig");
const result = @import("result.zig");
const canned = @import("canned.zig");
const errors = @import("errors.zig");
const auth = @import("auth.zig");
const prepared = @import("prepared.zig");
const sql_text_mod = @import("../sql_text.zig");
const conn_registry = @import("../conn_registry.zig");
const oprof = @import("../../util/prof.zig");
const counting_allocator = @import("../../util/counting_allocator.zig");
const rg_cache = @import("../../storage/cache.zig");
const scan_mod = @import("../../exec/scan.zig");

// Previous process-wide cache counters, per connection thread, so the handler
// can print this query's hit/miss/evict delta under `--profile-ops`.
threadlocal var prev_cache_stats: rg_cache.GlobalStats = .{ .hits = 0, .misses = 0, .evictions = 0, .miss_bytes = 0, .cache_bytes = 0 };
const ConnectionState = conn_registry.ConnectionState;
const ConnectionRegistry = conn_registry.Registry;
const ConnectionLimiter = @import("../conn_limit.zig").ConnectionLimiter;
const sock_opts = @import("../sock_opts.zig");

pub const Error = error{
    ServerClosed,
};

pub const Server = struct {
    allocator: Allocator,
    io: Io,
    catalog: *Catalog,
    listener: Io.net.Server,
    connection_counter: std.atomic.Value(u32) = .{ .raw = 0 },
    /// Shared across every wire when started from the binary; per-Server
    /// when started from a test. Slot is taken on accept, released when
    /// the per-connection handler returns.
    limiter: *ConnectionLimiter,
    /// True when this Server created its own limiter and must free it
    /// on close. False when the limiter was passed in by the caller.
    owns_limiter: bool,
    /// Per-connection read timeout. Pulled from `Config.idle_timeout_secs`
    /// and applied via SO_RCVTIMEO on accepted sockets. 0 disables.
    idle_timeout_secs: u32 = 0,
    /// Optional `mysql_native_password` cleartext. When null (default),
    /// the server runs in trust mode and accepts any auth response.
    /// When set, the client must respond with the correct 20-byte
    /// SHA1 challenge or the handshake is rejected (ER_ACCESS_DENIED).
    /// Caller-owned slice; must outlive the Server. Set after
    /// serveMysql() returns and before run().
    auth_password: ?[]const u8 = null,
    /// Optional shared connection registry for cross-connection
    /// cancellation (KILL <id>). When null, KILL becomes a no-op
    /// canned response. The binary supplies a registry shared with
    /// the PG wire so a KILL from one wire can target connections
    /// on either. Caller-owned; must outlive the Server.
    registry: ?*ConnectionRegistry = null,
    /// Opt-in per-connection profiling for the MySQL wire path. Set by
    /// the server binary from THINDB_MYSQL_PROFILE.
    profile: bool = false,

    pub fn close(self: *Server) void {
        self.listener.socket.close(self.io);
        if (self.owns_limiter) self.allocator.destroy(self.limiter);
        self.allocator.destroy(self);
    }

    /// Accept ONE connection and serve it synchronously on the calling
    /// thread. Used by tests that want a deterministic accept count.
    pub fn acceptOne(self: *Server) !void {
        const stream = try self.listener.accept(self.io);
        defer stream.close(self.io);

        _ = sock_opts.enableKeepalive(stream.socket.handle);
        if (self.idle_timeout_secs > 0) {
            _ = sock_opts.setReadTimeoutSeconds(stream.socket.handle, self.idle_timeout_secs);
        }

        if (!self.limiter.acquire()) {
            sendTooManyConnections(self.allocator, stream, self.io) catch {};
            return;
        }
        defer self.limiter.release();

        const cid = self.connection_counter.fetchAdd(1, .monotonic) + 1;
        handleConnection(self.allocator, self.io, self.catalog, stream, cid, self.auth_password, self.registry, self.profile) catch |err| {
            std.debug.print("mysql: connection error: {s}\n", .{@errorName(err)});
        };
    }

    /// Long-running accept loop. Each connection runs on its own thread
    /// so multiple clients (and pool drivers) can be in-flight at once.
    /// The shared limiter enforces the global concurrency cap.
    pub fn run(self: *Server, should_stop: *std.atomic.Value(bool)) !void {
        while (!should_stop.load(.acquire)) {
            const stream = self.listener.accept(self.io) catch |err| {
                std.debug.print("mysql: accept error: {s}\n", .{@errorName(err)});
                continue;
            };
            _ = sock_opts.enableKeepalive(stream.socket.handle);
            if (self.idle_timeout_secs > 0) {
                _ = sock_opts.setReadTimeoutSeconds(stream.socket.handle, self.idle_timeout_secs);
            }
            if (!self.limiter.acquire()) {
                sendTooManyConnections(self.allocator, stream, self.io) catch {};
                stream.close(self.io);
                continue;
            }
            const cid = self.connection_counter.fetchAdd(1, .monotonic) + 1;
            const job = self.allocator.create(ConnJob) catch {
                self.limiter.release();
                stream.close(self.io);
                continue;
            };
            job.* = .{
                .allocator = self.allocator,
                .io = self.io,
                .catalog = self.catalog,
                .stream = stream,
                .connection_id = cid,
                .limiter = self.limiter,
                .auth_password = self.auth_password,
                .registry = self.registry,
                .profile = self.profile,
            };
            // Query compilation recurses per IR op; a deep CTE chain compiled
            // as ONE block (e.g. a SEPARABLE closure privatizing a ~130-CTE
            // statement) overflows the 16 MiB default stack — silent segfault
            // on Windows. Reserve-only cost: pages commit lazily.
            const thread = std.Thread.spawn(.{ .stack_size = 1024 << 20 }, ConnJob.run, .{job}) catch {
                self.limiter.release();
                stream.close(self.io);
                self.allocator.destroy(job);
                continue;
            };
            thread.detach();
        }
    }
};

const ConnJob = struct {
    allocator: Allocator,
    io: Io,
    catalog: *Catalog,
    stream: Io.net.Stream,
    connection_id: u32,
    limiter: *ConnectionLimiter,
    auth_password: ?[]const u8,
    registry: ?*ConnectionRegistry,
    profile: bool,

    fn run(self: *ConnJob) void {
        defer self.allocator.destroy(self);
        defer self.limiter.release();
        defer self.stream.close(self.io);
        handleConnection(self.allocator, self.io, self.catalog, self.stream, self.connection_id, self.auth_password, self.registry, self.profile) catch |err| {
            std.debug.print("mysql: connection error: {s}\n", .{@errorName(err)});
        };
    }
};

/// Bind a MySQL listener on `address` for queries against `catalog`.
/// The Server does NOT own the catalog — caller manages its lifetime.
/// When `limiter` is null the Server allocates a private limiter sized
/// from `catalog.config.max_connections`; pass an external limiter
/// (caller-owned) to share a single budget across multiple wires.
pub fn serveMysql(
    allocator: Allocator,
    io: Io,
    catalog: *Catalog,
    address: Io.net.IpAddress,
    limiter: ?*ConnectionLimiter,
) !*Server {
    const listener = try Io.net.IpAddress.listen(&address, io, .{
        .mode = .stream,
        .protocol = .tcp,
        .reuse_address = true,
    });
    const effective_limiter = if (limiter) |lim| lim else blk: {
        const lp = try allocator.create(ConnectionLimiter);
        lp.* = ConnectionLimiter.init(catalog.config.max_connections);
        break :blk lp;
    };
    errdefer if (limiter == null) allocator.destroy(effective_limiter);

    const self = try allocator.create(Server);
    self.* = .{
        .allocator = allocator,
        .io = io,
        .catalog = catalog,
        .listener = listener,
        .limiter = effective_limiter,
        .owns_limiter = limiter == null,
        .idle_timeout_secs = catalog.config.idle_timeout_secs,
    };
    return self;
}

/// Emit ER_CON_COUNT_ERROR (1040 / 08004) and close. Used when the
/// server-wide limiter rejected the slot at accept time. Best-effort:
/// any I/O failure here is silently swallowed by the caller.
fn sendTooManyConnections(
    allocator: Allocator,
    stream: Io.net.Stream,
    io: Io,
) !void {
    var write_buf: [256]u8 = undefined;
    var writer = stream.writer(io, &write_buf);
    const w = &writer.interface;
    try handshake.sendErrPacket(allocator, w, 0, 1040, "08004".*, "Too many connections");
    try w.flush();
}

const SessionState = struct {
    current_db: []u8,
    current_schema: []u8,
    allocator: Allocator,
    /// Catalog root used to lazily open the per-session temp namespace
    /// when the first `CREATE TEMP TABLE` arrives. Borrowed.
    catalog: *Catalog,
    /// Backend / connection id; used as the per-session subdir name
    /// inside `_temp/`.
    backend_id: u32,
    /// Session-local temp table namespace. Lazily allocated on the
    /// first CREATE TEMP TABLE. Closed (and its on-disk dir removed)
    /// in `deinit` and on COM_RESET_CONNECTION / COM_CHANGE_USER.
    temp_namespace: ?*TempNamespace = null,
    /// Snapshot of the client's HandshakeResponse41 capability bits.
    /// Controls result-set terminator format (DEPRECATE_EOF) and any
    /// future per-connection feature toggles. Zero before handshake.
    client_caps: u32 = 0,
    /// Tracks whether a BEGIN/START TRANSACTION has been issued without
    /// a matching COMMIT/ROLLBACK. Reflected in OK/EOF status_flags via
    /// SERVER_STATUS_IN_TRANS so drivers know not to issue savepoints
    /// against a fresh connection. thinDB doesn't enforce real
    /// transactions yet; the bit is bookkeeping for client tooling.
    in_transaction: bool = false,
    /// Salt sent in the HandshakeV10 greeting. Retained on the
    /// SessionState so COM_CHANGE_USER can re-verify the client's
    /// hash against the same challenge.
    auth_salt: [auth.SALT_LEN]u8 = std.mem.zeroes([auth.SALT_LEN]u8),
    /// Pointer into the shared registry entry for this connection.
    /// Used by handleQuery to wire the cancel_flag into the
    /// in-flight CompiledQuery so a peer KILL aborts at the next
    /// batch boundary. Null when no registry is configured.
    conn_state: ?*ConnectionState = null,
    /// Shared registry pointer so this connection's KILL can reach
    /// peer connections. Null when no registry is configured (KILL
    /// becomes a no-op success).
    registry: ?*ConnectionRegistry = null,
    /// Per-connection prepared-statement registry. Statements are owned
    /// here and freed on COM_STMT_CLOSE or at connection teardown.
    prepared_statements: std.AutoHashMapUnmanaged(u32, *prepared.PreparedStmt) = .empty,
    /// Next stmt id to hand out. MySQL drivers don't care about the
    /// numbering scheme; just needs to be stable per-connection.
    next_stmt_id: u32 = 1,
    /// MySQL user-defined variables (`SET @x = ...`). Persisted across
    /// statements on this connection so a `SET` is visible to a later
    /// query. Lazily allocated by the first SET (inside compileWithSession);
    /// the connection owns the lifetime (freed in `deinit`, reset on
    /// COM_RESET_CONNECTION / COM_CHANGE_USER). `captureVars` copies the
    /// pointer back after each statement compiles.
    vars: ?*SessionVars = null,
    /// The XA branch xid started on this connection (between XA START and XA
    /// END). While set, DML is staged into that branch instead of executing.
    xa_active: ?[]const u8 = null,

    fn init(allocator: Allocator, catalog: *Catalog, backend_id: u32) !SessionState {
        return .{
            .allocator = allocator,
            .catalog = catalog,
            .backend_id = backend_id,
            .current_db = try allocator.dupe(u8, "main"),
            .current_schema = try allocator.dupe(u8, "public"),
        };
    }

    fn deinit(self: *SessionState) void {
        self.dropTempNamespace();
        self.resetVars();
        if (self.xa_active) |x| self.allocator.free(x);
        self.xa_active = null;
        self.allocator.free(self.current_db);
        self.allocator.free(self.current_schema);
        var it = self.prepared_statements.iterator();
        while (it.next()) |entry| entry.value_ptr.*.deinit();
        self.prepared_statements.deinit(self.allocator);
    }

    /// Persist the user-variable map a just-compiled statement produced (the
    /// pointer is stable — `CompiledQuery.deinit` deliberately doesn't free it),
    /// so the next statement's `asSession` threads it back in.
    fn captureVars(self: *SessionState, s: Session) void {
        self.vars = s.vars;
    }

    /// Drop the connection's user variables (end of connection, or a reset).
    fn resetVars(self: *SessionState) void {
        local.CompiledQuery.freeSessionVars(self.allocator, self.vars);
        self.vars = null;
    }

    /// Open the per-session temp namespace if it hasn't been opened yet.
    /// Returns the existing one on repeat calls.
    fn ensureTempNamespace(self: *SessionState) !*TempNamespace {
        if (self.temp_namespace) |ns| return ns;
        const ns = try TempNamespace.open(
            self.allocator,
            self.catalog.io,
            self.catalog.root_dir,
            self.backend_id,
            self.catalog.config,
        );
        self.temp_namespace = ns;
        return ns;
    }

    /// Tear down the session's temp namespace if open. Used on
    /// disconnect, COM_RESET_CONNECTION, COM_CHANGE_USER.
    fn dropTempNamespace(self: *SessionState) void {
        if (self.temp_namespace) |ns| {
            ns.close();
            self.temp_namespace = null;
        }
    }

    fn replace(self: *SessionState, db: []const u8, schema: []const u8) !void {
        const new_db = try self.allocator.dupe(u8, db);
        errdefer self.allocator.free(new_db);
        const new_schema = try self.allocator.dupe(u8, schema);
        self.allocator.free(self.current_db);
        self.allocator.free(self.current_schema);
        self.current_db = new_db;
        self.current_schema = new_schema;
    }

    fn asSession(self: SessionState) Session {
        return .{
            .current_db = self.current_db,
            .current_schema = self.current_schema,
            .dialect = .mysql,
            .temp_namespace = self.temp_namespace,
            .vars = self.vars,
        };
    }

    /// OR-able status bits derived from session state. Callers combine
    /// with any per-response extra flags (e.g. SERVER_MORE_RESULTS_EXISTS)
    /// before passing to OK/EOF emitters.
    fn transactionStatus(self: SessionState) u16 {
        return if (self.in_transaction) handshake.SERVER_STATUS_IN_TRANS else 0;
    }
};

const profile_report_every_commands: u64 = 50;
const max_prepare_param_defs_to_emit: u16 = 256;
var profile_print_mutex: ProfileSpinLock = .{};

const ProfileSpinLock = struct {
    state: std.atomic.Value(bool) = .{ .raw = false },

    fn lock(self: *ProfileSpinLock) void {
        while (self.state.cmpxchgWeak(false, true, .acquire, .monotonic) != null) {
            std.atomic.spinLoopHint();
        }
    }

    fn unlock(self: *ProfileSpinLock) void {
        self.state.store(false, .release);
    }
};

const ProfilePhase = enum(u8) {
    packet_read,
    command_total,
    response_flush,
    query_parse,
    query_compile,
    query_execute,
    query_write,
    query_execute_write,
    stmt_prepare_count,
    stmt_prepare_infer,
    stmt_prepare_write,
    stmt_execute_decode,
    stmt_execute_substitute,
    stmt_execute_parse,
    stmt_execute_compile,
    stmt_execute_engine,
    stmt_execute_write,
    stmt_long_data,
    stmt_close,
    stmt_reset,
};

const profile_phases = [_]ProfilePhase{
    .packet_read,
    .command_total,
    .response_flush,
    .query_parse,
    .query_compile,
    .query_execute,
    .query_write,
    .query_execute_write,
    .stmt_prepare_count,
    .stmt_prepare_infer,
    .stmt_prepare_write,
    .stmt_execute_decode,
    .stmt_execute_substitute,
    .stmt_execute_parse,
    .stmt_execute_compile,
    .stmt_execute_engine,
    .stmt_execute_write,
    .stmt_long_data,
    .stmt_close,
    .stmt_reset,
};

const profile_phase_count = profile_phases.len;

const SqlKind = enum(u8) {
    select,
    insert,
    ddl,
    other,
};

const sql_kind_count = @typeInfo(SqlKind).@"enum".fields.len;

const ProfileStats = struct {
    count: u64 = 0,
    total_ns: u64 = 0,
    max_ns: u64 = 0,

    fn add(self: *ProfileStats, ns: u64) void {
        self.count += 1;
        self.total_ns += ns;
        if (ns > self.max_ns) self.max_ns = ns;
    }
};

const MysqlProfiler = struct {
    enabled: bool,
    io: Io,
    connection_id: u32,
    total_commands: u64 = 0,
    commands_since_report: u64 = 0,
    rows_affected: u64 = 0,
    rows_returned: u64 = 0,
    phases: [profile_phase_count]ProfileStats,
    sql_kinds: [sql_kind_count]u64,

    fn init(io: Io, connection_id: u32, enabled: bool) MysqlProfiler {
        return .{
            .enabled = enabled,
            .io = io,
            .connection_id = connection_id,
            .phases = std.mem.zeroes([profile_phase_count]ProfileStats),
            .sql_kinds = std.mem.zeroes([sql_kind_count]u64),
        };
    }

    fn start(self: *const MysqlProfiler) ?Io.Timestamp {
        if (!self.enabled) return null;
        return Io.Clock.awake.now(self.io);
    }

    fn recordSince(self: *MysqlProfiler, phase: ProfilePhase, start_ts: ?Io.Timestamp) void {
        if (!self.enabled) return;
        const t0 = start_ts orelse return;
        const elapsed = t0.durationTo(Io.Clock.awake.now(self.io));
        self.phases[@intFromEnum(phase)].add(@intCast(elapsed.toNanoseconds()));
    }

    fn recordSqlKind(self: *MysqlProfiler, kind: SqlKind) void {
        if (!self.enabled) return;
        self.sql_kinds[@intFromEnum(kind)] += 1;
    }

    fn addRowsAffected(self: *MysqlProfiler, rows: u64) void {
        if (!self.enabled) return;
        self.rows_affected += rows;
    }

    fn addRowsReturned(self: *MysqlProfiler, rows: u64) void {
        if (!self.enabled) return;
        self.rows_returned += rows;
    }

    fn finishCommand(self: *MysqlProfiler) void {
        if (!self.enabled) return;
        self.total_commands += 1;
        self.commands_since_report += 1;
        if (self.commands_since_report >= profile_report_every_commands) {
            self.report(false);
            self.commands_since_report = 0;
        }
    }

    fn finish(self: *MysqlProfiler) void {
        if (!self.enabled) return;
        if (self.total_commands == 0 and !self.hasPhaseData()) return;
        self.report(true);
    }

    fn hasPhaseData(self: *const MysqlProfiler) bool {
        for (self.phases) |stats| {
            if (stats.count > 0) return true;
        }
        return false;
    }

    fn report(self: *const MysqlProfiler, final: bool) void {
        profile_print_mutex.lock();
        defer profile_print_mutex.unlock();

        const label = if (final) "final" else "periodic";
        std.debug.print(
            "mysql profile cid={d} {s}: commands={d} rows_affected={d} rows_returned={d} sql(select={d}, insert={d}, ddl={d}, other={d})\n",
            .{
                self.connection_id,
                label,
                self.total_commands,
                self.rows_affected,
                self.rows_returned,
                self.sql_kinds[@intFromEnum(SqlKind.select)],
                self.sql_kinds[@intFromEnum(SqlKind.insert)],
                self.sql_kinds[@intFromEnum(SqlKind.ddl)],
                self.sql_kinds[@intFromEnum(SqlKind.other)],
            },
        );
        for (profile_phases) |phase| {
            const stats = self.phases[@intFromEnum(phase)];
            if (stats.count == 0) continue;
            const total_ms = @as(f64, @floatFromInt(stats.total_ns)) / 1e6;
            const mean_ms = total_ms / @as(f64, @floatFromInt(stats.count));
            const max_ms = @as(f64, @floatFromInt(stats.max_ns)) / 1e6;
            std.debug.print(
                "  {s:<24} count={d:<8} total={d:>10.3}ms mean={d:>9.3}ms max={d:>9.3}ms\n",
                .{ profilePhaseName(phase), stats.count, total_ms, mean_ms, max_ms },
            );
        }
    }
};

fn profilePhaseName(phase: ProfilePhase) []const u8 {
    return switch (phase) {
        .packet_read => "packet_read",
        .command_total => "command_total",
        .response_flush => "response_flush",
        .query_parse => "query_parse",
        .query_compile => "query_compile",
        .query_execute => "query_execute",
        .query_write => "query_write",
        .query_execute_write => "query_execute_write",
        .stmt_prepare_count => "stmt_prepare_count",
        .stmt_prepare_infer => "stmt_prepare_infer",
        .stmt_prepare_write => "stmt_prepare_write",
        .stmt_execute_decode => "stmt_execute_decode",
        .stmt_execute_substitute => "stmt_execute_substitute",
        .stmt_execute_parse => "stmt_execute_parse",
        .stmt_execute_compile => "stmt_execute_compile",
        .stmt_execute_engine => "stmt_execute_engine",
        .stmt_execute_write => "stmt_execute_write",
        .stmt_long_data => "stmt_long_data",
        .stmt_close => "stmt_close",
        .stmt_reset => "stmt_reset",
    };
}

/// Millisecond reading of the awake clock — the same timebase the
/// net_read_timeout reaper compares against (cmd/server.zig).
fn nowMs(io: Io) u64 {
    const ns = Io.Clock.awake.now(io).nanoseconds;
    return @intCast(@divTrunc(@max(ns, 0), std.time.ns_per_ms));
}

// ---------------------------------------------------------------------------
// Guarded socket writes (#164): every send on a connection's stream writer is
// bracketed with a transfer-wait mark so the reaper can bound a wedged
// response write (net_write_timeout — a stuck send otherwise hangs until the
// client gives up, exactly the failure class a server must bound itself).
// Interposing at the drain vtable catches every send, including implicit
// drains when a large result set overflows the write buffer. Threadlocal is
// safe here because each connection owns a dedicated thread for its whole
// lifetime.
// ---------------------------------------------------------------------------

const WriteGuard = struct { state: *ConnectionState, io: Io };
threadlocal var tl_write_guard: ?WriteGuard = null;

/// The stream writer's original vtable — one static value for every
/// `Io.net.Stream.Writer`, captured at first interpose. Atomic only to
/// keep the cross-thread publication defined; all stores write the same
/// pointer.
var orig_stream_writer_vtable = std.atomic.Value(?*const Io.Writer.VTable).init(null);

const guarded_writer_vtable: Io.Writer.VTable = .{
    .drain = guardedDrain,
    .sendFile = guardedSendFile,
};

fn interposeWriteGuard(w: *Io.Writer, state: *ConnectionState, io: Io) void {
    orig_stream_writer_vtable.store(w.vtable, .release);
    w.vtable = &guarded_writer_vtable;
    tl_write_guard = .{ .state = state, .io = io };
}

fn guardedDrain(w: *Io.Writer, data: []const []const u8, splat: usize) Io.Writer.Error!usize {
    const orig = orig_stream_writer_vtable.load(.acquire).?;
    if (tl_write_guard) |g| {
        g.state.beginWrite(nowMs(g.io));
        defer g.state.endTransfer();
        return orig.drain(w, data, splat);
    }
    return orig.drain(w, data, splat);
}

fn guardedSendFile(w: *Io.Writer, file_reader: *std.Io.File.Reader, limit: std.Io.Limit) Io.Writer.FileError!usize {
    const orig = orig_stream_writer_vtable.load(.acquire).?;
    if (tl_write_guard) |g| {
        g.state.beginWrite(nowMs(g.io));
        defer g.state.endTransfer();
        return orig.sendFile(w, file_reader, limit);
    }
    return orig.sendFile(w, file_reader, limit);
}

fn classifySqlKind(op: ir.Op) SqlKind {
    return switch (std.meta.activeTag(op)) {
        .scan,
        .limit,
        .select,
        .exclude,
        .filter,
        .order_by,
        .group_by,
        .compute,
        .join,
        .materialize,
        .alias,
        .show,
        .window,
        .set_union,
        => .select,
        .insert, .insert_select => .insert,
        .ddl, .create_table_as => .ddl,
        else => .other,
    };
}

fn handleConnection(
    allocator: Allocator,
    io: Io,
    catalog: *Catalog,
    stream: Io.net.Stream,
    connection_id: u32,
    auth_password: ?[]const u8,
    registry: ?*ConnectionRegistry,
    profile_enabled: bool,
) !void {
    var read_buf: [16 * 1024]u8 = undefined;
    var write_buf: [16 * 1024]u8 = undefined;
    var reader = stream.reader(io, &read_buf);
    var writer = stream.writer(io, &write_buf);
    const w = &writer.interface;
    const r = &reader.interface;

    var session = try SessionState.init(allocator, catalog, connection_id);
    defer session.deinit();
    var profiler = MysqlProfiler.init(io, connection_id, profile_enabled);
    defer profiler.finish();

    // Register in the shared registry so peer connections can KILL
    // this one's in-flight query. The connection state must outlive
    // any in-flight query; tying it to this stack frame is fine
    // because handleConnection only returns after the connection
    // closes.
    var conn_state = ConnectionState.init(connection_id, ConnectionState.deriveSecret(connection_id));
    // Arm the reaper BEFORE register publishes the state (#164): a
    // guarded read or write that stalls past its timeout gets the
    // socket shut down, which completes the wedged operation and lets
    // this thread exit through the normal error path.
    conn_state.reap_socket = stream.socket.handle;
    interposeWriteGuard(w, &conn_state, io);
    defer tl_write_guard = null;
    if (registry) |reg| {
        try reg.register(&conn_state);
    }
    defer if (registry) |reg| reg.unregister(connection_id);
    session.conn_state = &conn_state;
    session.registry = registry;

    var salt: [auth.SALT_LEN]u8 = undefined;
    auth.randomSalt(&salt);
    session.auth_salt = salt;

    try handshake.sendInitialHandshake(allocator, w, connection_id, salt);
    try w.flush();

    const hs_packet = blk: {
        conn_state.beginRead(nowMs(io), false);
        const hdr = try packet.readHeader(r);
        conn_state.beginRead(nowMs(io), true);
        defer conn_state.endTransfer();
        break :blk .{ .seq_id = hdr.seq_id, .payload = try packet.readBody(allocator, r, hdr.len) };
    };
    defer allocator.free(hs_packet.payload);
    const client = handshake.parseHandshakeResponse(hs_packet.payload) catch {
        try handshake.sendErrPacket(allocator, w, 2, 1064, "42000".*, "Malformed handshake");
        try w.flush();
        return;
    };
    session.client_caps = client.capabilities;

    // Plugin dispatch: client.auth_plugin tells us which math to apply.
    // Both `mysql_native_password` (20-byte hash) and
    // `caching_sha2_password` (32-byte hash) are accepted unconditionally;
    // the server doesn't care which plugin the client picked — only that
    // the response math verifies. When auth_password is null (trust
    // mode), all responses pass regardless of plugin choice.
    const using_caching_sha2 = blk: {
        const plugin = client.auth_plugin orelse break :blk false;
        break :blk std.mem.eql(u8, plugin, "caching_sha2_password");
    };

    if (auth_password) |pw| {
        const ok = if (using_caching_sha2)
            auth.verifyCachingSha2(auth.deriveCachingSha2Credentials(pw), salt, client.auth_response)
        else
            auth.verify(pw, salt, client.auth_response);
        if (!ok) {
            try handshake.sendErrPacket(allocator, w, 2, 1045, "28000".*, "Access denied");
            try w.flush();
            return;
        }
    }

    if (client.initial_database) |db_name| {
        if (db_name.len > 0) {
            applyInitDb(catalog, &session, db_name) catch |err| {
                const mapped = errors.mapInternal(err, null);
                try handshake.sendErrPacket(allocator, w, 2, mapped.code, mapped.sqlstate, mapped.message);
                try w.flush();
                return;
            };
        }
    }

    if (using_caching_sha2) {
        // caching_sha2_password protocol: server must send an
        // AuthMoreData packet with body byte 0x03 (fast_auth_success)
        // before the OK packet. Seq advances: response=1, AuthMoreData=2,
        // OK=3.
        try handshake.sendAuthMoreData(allocator, w, 2, &[_]u8{0x03});
        try handshake.sendOkPacket(allocator, w, 3, 0, 0);
    } else {
        try handshake.sendHandshakeOk(allocator, w);
    }
    try w.flush();

    while (true) {
        const read_start = profiler.start();
        // Both read waits are marked for the reaper (#164). Header wait
        // = idle between commands: unbounded UNLESS bytes are queued on
        // the socket while the read pends (the wedged-read signature).
        // Payload wait = mid-packet: bounded hard by net_read_timeout.
        // Without the reaper a wedged transfer hangs until the CLIENT
        // gives up; the sink treats a dead connection fine, but a
        // silently wedged one delays failover by minutes.
        const pkt = blk: {
            conn_state.beginRead(nowMs(io), false);
            const hdr = packet.readHeader(r) catch |err| switch (err) {
                error.EndOfStream => return,
                else => return err,
            };
            conn_state.beginRead(nowMs(io), true);
            defer conn_state.endTransfer();
            const payload = packet.readBody(allocator, r, hdr.len) catch |err| switch (err) {
                error.EndOfStream => return,
                else => return err,
            };
            break :blk .{ .seq_id = hdr.seq_id, .payload = payload };
        };
        profiler.recordSince(.packet_read, read_start);
        defer allocator.free(pkt.payload);
        if (pkt.payload.len == 0) continue;

        const cmd = pkt.payload[0];
        const body = pkt.payload[1..];
        const command_start = profiler.start();
        switch (cmd) {
            0x01 => {
                profiler.recordSince(.command_total, command_start);
                profiler.finishCommand();
                return;
            }, // COM_QUIT
            0x02 => try handleInitDb(allocator, w, catalog, &session, body),
            0x03 => try handleQuery(allocator, w, catalog, &session, body, &profiler),
            // COM_STMT_PREPARE (0x16) — parse SQL, count `?` placeholders,
            // register a per-connection statement id, return prepare-ok.
            0x16 => try handleStmtPrepare(allocator, w, catalog, &session, body, &profiler),
            // COM_STMT_EXECUTE (0x17) — bind parameters + run.
            0x17 => try handleStmtExecute(allocator, w, catalog, &session, body, &profiler),
            // COM_STMT_SEND_LONG_DATA (0x18) — accumulate long-data
            // bytes into a per-stmt per-param buffer. No response.
            0x18 => {
                const phase_start = profiler.start();
                try handleStmtSendLongData(&session, body);
                profiler.recordSince(.stmt_long_data, phase_start);
            },
            // COM_STMT_CLOSE (0x19) — free the stmt entry. No response.
            0x19 => {
                const phase_start = profiler.start();
                try handleStmtClose(&session, body);
                profiler.recordSince(.stmt_close, phase_start);
            },
            // COM_STMT_RESET (0x1A) — clear long-data buffers; reply OK.
            0x1A => {
                const phase_start = profiler.start();
                try handleStmtReset(allocator, w, &session, body);
                profiler.recordSince(.stmt_reset, phase_start);
            },
            0x0E => try handshake.sendOkPacket(allocator, w, 1, 0, 0), // COM_PING
            // COM_RESET_CONNECTION — wipes per-connection state without
            // closing the socket. Connection poolers (e.g. ProxySQL,
            // mysql2's pool with `connectionLimit`) send this when
            // returning a borrowed connection to scrub session state.
            // Resets: txn flag, current schema to default, session temp
            // tables (drops + deletes the per-session _temp dir).
            0x1F => {
                session.dropTempNamespace();
                session.resetVars();
                session.in_transaction = false;
                try session.replace("main", "public");
                try handshake.sendOkPacketStatus(allocator, w, 1, 0, 0, session.transactionStatus());
            },
            // COM_CHANGE_USER — re-authenticate over the existing
            // connection. ProxySQL and similar use this to recycle a
            // pooled connection as a different user. We share the
            // verifier path with the initial handshake (same salt is
            // retained on SessionState).
            0x11 => try handleChangeUser(allocator, w, catalog, &session, body, auth_password),
            // COM_SET_OPTION — toggle CLIENT_MULTI_STATEMENTS mid-connection.
            // Connector/J sends this (rather than negotiating the capability
            // at handshake) right before a semicolon-joined batch, e.g. the
            // Flink JDBC sink's DELETE batches, which can't be rewritten as a
            // single multi-VALUES INSERT. Body: u16 LE, 0 = ON, 1 = OFF.
            0x1B => {
                if (body.len >= 2 and std.mem.readInt(u16, body[0..2], .little) == 0) {
                    session.client_caps |= handshake.CLIENT_MULTI_STATEMENTS;
                } else {
                    session.client_caps &= ~handshake.CLIENT_MULTI_STATEMENTS;
                }
                if ((session.client_caps & handshake.CLIENT_DEPRECATE_EOF) != 0) {
                    try handshake.sendEofOkPacketStatus(allocator, w, 1, session.transactionStatus());
                } else {
                    try handshake.sendLegacyEofPacketStatus(allocator, w, 1, session.transactionStatus());
                }
            },
            else => try handshake.sendErrPacket(allocator, w, 1, 1047, "HY000".*, "Unknown command"),
        }
        const flush_start = profiler.start();
        try w.flush();
        profiler.recordSince(.response_flush, flush_start);
        profiler.recordSince(.command_total, command_start);
        profiler.finishCommand();
    }
}

/// Apply the flattening rule for COM_INIT_DB / USE: `db__schema` splits
/// into both parts; otherwise treat the name as either a schema within
/// the current db or a top-level database.
fn applyInitDb(catalog: *Catalog, session: *SessionState, name: []const u8) !void {
    if (std.mem.indexOf(u8, name, "__")) |sep| {
        const db_name = name[0..sep];
        const sc_name = name[sep + 2 ..];
        const db = catalog.database(db_name) orelse return ApiError.DatabaseNotFound;
        _ = db.schema(sc_name) orelse return ApiError.SchemaNotFound;
        try session.replace(db_name, sc_name);
        return;
    }

    const cur_db = catalog.database(session.current_db) orelse return ApiError.DatabaseNotFound;
    if (cur_db.schema(name) != null) {
        try session.replace(session.current_db, name);
        return;
    }
    if (catalog.database(name) != null) {
        try session.replace(name, "public");
        return;
    }
    return ApiError.DatabaseNotFound;
}

fn handleInitDb(
    allocator: Allocator,
    w: *std.Io.Writer,
    catalog: *Catalog,
    session: *SessionState,
    payload: []const u8,
) !void {
    if (payload.len == 0) {
        try handshake.sendOkPacket(allocator, w, 1, 0, 0);
        return;
    }
    applyInitDb(catalog, session, payload) catch |err| {
        const mapped = errors.mapInternal(err, null);
        try handshake.sendErrPacket(allocator, w, 1, mapped.code, mapped.sqlstate, mapped.message);
        return;
    };
    try handshake.sendOkPacket(allocator, w, 1, 0, 0);
}

/// COM_CHANGE_USER payload layout (CLIENT_SECURE_CONNECTION +
/// CLIENT_PLUGIN_AUTH negotiated):
///   string<NUL>  user
///   int<1>       auth_response_length
///   string<var>  auth_response
///   string<NUL>  schema  (database name; may be empty)
///   int<2>       character_set
///   string<NUL>  auth_plugin_name
///   [optional CLIENT_CONNECT_ATTRS payload — skipped]
fn handleChangeUser(
    allocator: Allocator,
    w: *std.Io.Writer,
    catalog: *Catalog,
    session: *SessionState,
    payload: []const u8,
    auth_password: ?[]const u8,
) !void {
    // Parse user (NUL-terminated).
    var i: usize = 0;
    while (i < payload.len and payload[i] != 0) i += 1;
    if (i >= payload.len) {
        try handshake.sendErrPacket(allocator, w, 1, 1064, "42000".*, "malformed COM_CHANGE_USER");
        return;
    }
    i += 1; // skip NUL after user

    if (i >= payload.len) {
        try handshake.sendErrPacket(allocator, w, 1, 1064, "42000".*, "malformed COM_CHANGE_USER");
        return;
    }
    const auth_len: usize = payload[i];
    i += 1;
    if (i + auth_len > payload.len) {
        try handshake.sendErrPacket(allocator, w, 1, 1064, "42000".*, "malformed COM_CHANGE_USER");
        return;
    }
    const client_auth = payload[i .. i + auth_len];
    i += auth_len;

    // Parse schema (NUL-terminated; empty allowed).
    var schema_end = i;
    while (schema_end < payload.len and payload[schema_end] != 0) schema_end += 1;
    if (schema_end >= payload.len) {
        try handshake.sendErrPacket(allocator, w, 1, 1064, "42000".*, "malformed COM_CHANGE_USER");
        return;
    }
    const schema_name = payload[i..schema_end];

    if (auth_password) |pw| {
        if (!auth.verify(pw, session.auth_salt, client_auth)) {
            try handshake.sendErrPacket(allocator, w, 1, 1045, "28000".*, "Access denied");
            return;
        }
    }

    // Reset session state. COM_CHANGE_USER is more aggressive than
    // COM_RESET_CONNECTION: it explicitly re-authenticates and
    // optionally switches DB. Reset = drop temp namespace, clear txn,
    // reapply default schema, honor whatever schema the client passed.
    session.dropTempNamespace();
    session.resetVars();
    session.in_transaction = false;
    try session.replace("main", "public");
    if (schema_name.len > 0) {
        applyInitDb(catalog, session, schema_name) catch |err| {
            const mapped = errors.mapInternal(err, null);
            try handshake.sendErrPacket(allocator, w, 1, mapped.code, mapped.sqlstate, mapped.message);
            return;
        };
    }

    try handshake.sendOkPacketStatus(allocator, w, 1, 0, 0, session.transactionStatus());
}

extern "c" fn _putenv_s(name: [*:0]const u8, value: [*:0]const u8) c_int;
extern "c" fn setenv(name: [*:0]const u8, value: [*:0]const u8, overwrite: c_int) c_int;

fn setProcessEnv(allocator: Allocator, name: []const u8, value: []const u8) !void {
    const name_z = try allocator.dupeZ(u8, name);
    defer allocator.free(name_z);
    const value_z = try allocator.dupeZ(u8, value);
    defer allocator.free(value_z);
    if (@import("builtin").os.tag == .windows) {
        _ = _putenv_s(name_z.ptr, value_z.ptr);
    } else {
        _ = setenv(name_z.ptr, value_z.ptr, 1);
    }
}

/// Bench-only hot-switch: `SET THINDB_<NAME> = <value>` mutates the server
/// PROCESS env in place, so the next query's `paramsFromEnv` (`getenv`, read
/// per query) sees the new tunable without a restart — the config sweep cycles
/// values on one warm server. Scoped to the `THINDB_` prefix so it never
/// shadows a real session `SET`.
fn trySetThindbEnvVar(allocator: Allocator, payload: []const u8) !bool {
    const trimmed = std.mem.trim(u8, payload, " \t\r\n;");
    if (trimmed.len < 4 or !std.ascii.eqlIgnoreCase(trimmed[0..4], "SET ")) return false;
    const rest = std.mem.trimStart(u8, trimmed[4..], " \t");
    if (rest.len < 7 or !std.ascii.eqlIgnoreCase(rest[0..7], "THINDB_")) return false;
    const eq = std.mem.indexOfScalar(u8, rest, '=') orelse return false;
    const name = std.mem.trim(u8, rest[0..eq], " \t");
    const value = std.mem.trim(u8, rest[eq + 1 ..], " \t'\"");
    if (name.len == 0 or value.len == 0) return false;
    try setProcessEnv(allocator, name, value);
    return true;
}

fn handleQuery(
    allocator: Allocator,
    w: *std.Io.Writer,
    catalog: *Catalog,
    session: *SessionState,
    payload: []const u8,
    profiler: *MysqlProfiler,
) !void {
    var seq_id: u8 = 1;
    const caps = session.client_caps;

    if (try trySetThindbEnvVar(allocator, payload)) {
        try handshake.sendOkPacketStatus(allocator, w, seq_id, 0, 0, session.transactionStatus());
        return;
    }

    // Transaction verbs are handled before the canned matcher so the
    // OK packet carries the freshly-flipped SERVER_STATUS_IN_TRANS bit.
    if (matchTxnVerb(payload)) |verb| {
        switch (verb) {
            .begin => session.in_transaction = true,
            .commit, .rollback => session.in_transaction = false,
        }
        try handshake.sendOkPacketStatus(
            allocator,
            w,
            seq_id,
            0,
            0,
            session.transactionStatus(),
        );
        return;
    }

    if (try canned.match(allocator, payload, session.current_schema)) |outcome| {
        switch (outcome) {
            .ok_packet => try handshake.sendOkPacketStatus(
                allocator,
                w,
                seq_id,
                0,
                0,
                session.transactionStatus(),
            ),
            .reset_connection => {
                session.dropTempNamespace();
                session.resetVars();
                session.in_transaction = false;
                try session.replace("main", "public");
                try handshake.sendOkPacketStatus(
                    allocator,
                    w,
                    seq_id,
                    0,
                    0,
                    session.transactionStatus(),
                );
            },
            .single_value => |sv| try result.sendSingleValueResult(allocator, w, sv.col, sv.val, &seq_id, caps),
            .single_null => |col| try result.sendSingleValueResult(allocator, w, col, null, &seq_id, caps),
            .variable_row => |vr| try result.sendVariableRow(allocator, w, vr.name, vr.value, &seq_id, caps),
            .empty_variables => try result.sendEmptyVariables(allocator, w, &seq_id, caps),
            .empty_result => |kind| try sendMetadataResult(allocator, w, catalog, session, payload, kind, &seq_id, caps),
            .kill => |target_id| {
                // No registry → KILL is a no-op success. With a
                // registry, look up the target and set its cancel
                // flag. Unknown id → ER_NO_SUCH_THREAD (1094).
                if (session.registry) |reg| {
                    if (!reg.requestCancel(target_id, 0)) {
                        try handshake.sendErrPacket(allocator, w, seq_id, 1094, "HY000".*, "Unknown thread id");
                        return;
                    }
                }
                try handshake.sendOkPacketStatus(
                    allocator,
                    w,
                    seq_id,
                    0,
                    0,
                    session.transactionStatus(),
                );
            },
        }
        return;
    }

    if (isShowDatabases(payload)) {
        try sendFlattenedDatabases(allocator, w, catalog, &seq_id, caps);
        return;
    }

    if (try sendSyntheticWorkbenchSelect(allocator, w, catalog, session, payload, &seq_id, caps)) return;

    try runEngineQuery(allocator, w, catalog, session, payload, &seq_id, profiler);
}

fn sendSyntheticWorkbenchSelect(
    allocator: Allocator,
    w: *std.Io.Writer,
    catalog: *Catalog,
    session: *SessionState,
    payload: []const u8,
    seq_id: *u8,
    client_caps: u32,
) !bool {
    const lc = try sql_text_mod.normalizeForCannedMatch(allocator, payload);
    defer allocator.free(lc);

    if (!std.mem.startsWith(u8, lc, "select ")) return false;

    const from_idx = topLevelKeyword(lc, "from");
    if (from_idx) |idx| {
        const tail = lc[idx..];
        if (std.mem.indexOf(u8, tail, "information_schema") == null and
            std.mem.indexOf(u8, tail, "`information_schema`") == null)
            return false;

        if (try sendInformationSchemaSelect(
            allocator,
            w,
            catalog,
            lc["select ".len..idx],
            tail,
            seq_id,
            client_caps,
        )) return true;

        var cols = std.ArrayList(types.Column).empty;
        defer cols.deinit(allocator);
        try appendSelectColumns(allocator, &cols, lc["select ".len..idx]);
        if (cols.items.len == 0) try cols.append(allocator, .{ .name = "Name", .type = .string });
        try sendEmptyColumns(allocator, w, cols.items, seq_id, client_caps);
        return true;
    }

    // Parallel case-preserved view of the same normalized statement:
    // matching runs on `lc`, column labels slice from `orig` at the same
    // offsets so they echo the client's typed case.
    const orig_full = sql_text_mod.normalizeForCannedMatchKeepCase(payload);

    var select_list: []const u8 = lc["select ".len..];
    var orig_list: []const u8 = orig_full["select ".len..];
    if (topLevelKeyword(select_list, "limit")) |limit_idx| {
        select_list = std.mem.trim(u8, select_list[0..limit_idx], " \t\r\n");
        orig_list = std.mem.trim(u8, orig_list[0..limit_idx], " \t\r\n");
    }
    if (std.mem.startsWith(u8, select_list, "sql_no_cache ")) {
        select_list = std.mem.trim(u8, select_list["sql_no_cache ".len..], " \t\r\n");
        orig_list = std.mem.trim(u8, orig_list["sql_no_cache ".len..], " \t\r\n");
    }

    var cols = std.ArrayList(types.Column).empty;
    defer cols.deinit(allocator);
    var cells = std.ArrayList(?[]const u8).empty;
    defer cells.deinit(allocator);

    var start: usize = 0;
    while (start < select_list.len) {
        const end = nextTopLevelComma(select_list, start) orelse select_list.len;
        const raw_expr = std.mem.trim(u8, select_list[start..end], " \t\r\n");
        const orig_raw = std.mem.trim(u8, orig_list[start..end], " \t\r\n");
        if (raw_expr.len > 0) {
            const expr = stripAlias(raw_expr).expr;
            // Anything this layer doesn't positively recognize — literals,
            // arithmetic, real scalar functions, temporal nullaries — bails
            // the WHOLE statement to the engine, which answers FROM-less
            // SELECTs with proper values and types. Answering "" here (the
            // old fallback) silently lost values.
            const value = syntheticSelectValue(expr, session.current_schema) orelse return false;
            const col_name = stripIdentifierQuotes(stripAlias(orig_raw).alias orelse orig_raw);
            try cols.append(allocator, .{ .name = col_name, .type = .string, .nullable = value == .null_value });
            try cells.append(allocator, switch (value) {
                .text => |t| t,
                .null_value => null,
            });
        }
        start = end + 1;
    }

    if (cols.items.len == 0) return false;
    try result.sendResultHeader(allocator, w, cols.items, "", "", seq_id);
    try result.sendColumnDefBoundary(allocator, w, seq_id, client_caps);
    try result.sendTextRow(allocator, w, cells.items, seq_id);
    try result.sendResultTerminator(allocator, w, seq_id, client_caps);
    return true;
}

fn appendSelectColumns(
    allocator: Allocator,
    cols: *std.ArrayList(types.Column),
    select_list: []const u8,
) !void {
    var start: usize = 0;
    while (start < select_list.len) {
        const end = nextTopLevelComma(select_list, start) orelse select_list.len;
        const raw_expr = std.mem.trim(u8, select_list[start..end], " \t\r\n");
        if (raw_expr.len > 0 and !std.mem.eql(u8, raw_expr, "*")) {
            const alias = stripAlias(raw_expr).alias orelse raw_expr;
            try cols.append(allocator, .{ .name = stripIdentifierQuotes(alias), .type = .string, .nullable = true });
        }
        start = end + 1;
    }
}

fn StripAliasResult(comptime T: type) type {
    return struct {
        expr: T,
        alias: ?T,
    };
}

fn stripAlias(raw_expr: []const u8) StripAliasResult([]const u8) {
    const expr = std.mem.trim(u8, raw_expr, " \t\r\n");
    if (std.mem.lastIndexOf(u8, expr, " as ")) |idx| {
        return .{
            .expr = std.mem.trim(u8, expr[0..idx], " \t\r\n"),
            .alias = std.mem.trim(u8, expr[idx + 4 ..], " \t\r\n"),
        };
    }
    return .{ .expr = expr, .alias = null };
}

fn stripIdentifierQuotes(s_in: []const u8) []const u8 {
    const s = std.mem.trim(u8, s_in, " \t\r\n");
    if (s.len >= 2 and ((s[0] == '`' and s[s.len - 1] == '`') or
        (s[0] == '"' and s[s.len - 1] == '"') or
        (s[0] == '\'' and s[s.len - 1] == '\'')))
        return s[1 .. s.len - 1];
    return s;
}

/// Best-effort identification of the missing column behind a
/// ColumnNotFound compile error, so ER_BAD_FIELD can NAME it the way MySQL
/// does. Walks a linear SELECT chain down to its scan, collecting referenced
/// column names and Compute-provided names; the first reference missing from
/// the table's schema (and not Compute-provided) is the culprit. Conservative
/// by construction: joins, unions, subqueries, qualified/star/synthetic names
/// and overflowing shapes return null and the generic message stands.
fn findUnknownColumn(catalog: *Catalog, session: *SessionState, root: *const ir.Op) ?[]const u8 {
    var needed_buf: [64][]const u8 = undefined;
    var needed_n: usize = 0;
    var provided_buf: [32][]const u8 = undefined;
    var provided_n: usize = 0;

    var op = root;
    const scan_table = walk: while (true) {
        switch (op.*) {
            .scan => |s| break :walk s,
            .select, .exclude => |pr| {
                for (pr.columns) |c| {
                    if (!plausibleColumnName(c)) continue;
                    if (needed_n == needed_buf.len) return null;
                    needed_buf[needed_n] = c;
                    needed_n += 1;
                }
                op = pr.upstream;
            },
            .filter => |f| {
                if (!collectPredicateColumns(f.predicate, &needed_buf, &needed_n)) return null;
                op = f.upstream;
            },
            .group_by => |g| {
                for (g.group_cols) |c| {
                    if (!plausibleColumnName(c)) continue;
                    if (needed_n == needed_buf.len) return null;
                    needed_buf[needed_n] = c;
                    needed_n += 1;
                }
                for (g.aggs) |a| {
                    for ([_]?[]const u8{ a.col, a.arg2_col }) |maybe| {
                        const c = maybe orelse continue;
                        if (std.mem.eql(u8, c, "*") or !plausibleColumnName(c)) continue;
                        if (needed_n == needed_buf.len) return null;
                        needed_buf[needed_n] = c;
                        needed_n += 1;
                    }
                }
                op = g.upstream;
            },
            .order_by => |o| {
                for (o.specs) |sp| {
                    if (!plausibleColumnName(sp.col)) continue;
                    if (needed_n == needed_buf.len) return null;
                    needed_buf[needed_n] = sp.col;
                    needed_n += 1;
                }
                op = o.upstream;
            },
            .compute => |cm| {
                for (cm.derived) |d| {
                    if (provided_n == provided_buf.len) return null;
                    provided_buf[provided_n] = d.name;
                    provided_n += 1;
                }
                op = cm.upstream;
            },
            .limit => |l| op = l.upstream,
            .alias => |a| op = a.upstream,
            else => return null,
        }
    };

    const db = catalog.database(scan_table.table.database orelse session.current_db) orelse return null;
    const sc = db.schema(scan_table.table.schema orelse session.current_schema) orelse return null;
    const t = schemaTable(sc, scan_table.table.name) orelse return null;

    outer: for (needed_buf[0..needed_n]) |name| {
        if (types.findColumn(t.schema.columns, name) != null) continue;
        for (provided_buf[0..provided_n]) |pn| {
            if (types.columnNameEql(pn, name)) continue :outer;
        }
        return name;
    }
    return null;
}

fn plausibleColumnName(name: []const u8) bool {
    if (name.len == 0 or std.mem.eql(u8, name, "*")) return false;
    // Qualified (alias.col), star-expanded (t.*), and synthetic (__agg_arg_n)
    // names carry resolution rules this walker doesn't model.
    if (std.mem.indexOfScalar(u8, name, '.') != null) return false;
    if (std.mem.startsWith(u8, name, "__")) return false;
    return true;
}

fn collectPredicateColumns(expr: PredicateExpr, buf: *[64][]const u8, n: *usize) bool {
    switch (expr) {
        .leaf => |p| {
            if (plausibleColumnName(p.col)) {
                if (n.* == buf.len) return false;
                buf[n.*] = p.col;
                n.* += 1;
            }
        },
        .in_set => |s| {
            if (plausibleColumnName(s.col)) {
                if (n.* == buf.len) return false;
                buf[n.*] = s.col;
                n.* += 1;
            }
        },
        .@"and", .@"or" => |kids| for (kids) |k| {
            if (!collectPredicateColumns(k, buf, n)) return false;
        },
        .not => |child| return collectPredicateColumns(child.*, buf, n),
        else => {},
    }
    return true;
}

const SyntheticValue = union(enum) { text: []const u8, null_value };

/// A recognized connection-setup probe expression, or null when this layer
/// has no answer — the caller then bails the statement to the engine (which
/// evaluates FROM-less SELECTs with real values and types). This layer only
/// exists for the multi-column `@@var` init probes drivers send, which the
/// engine has no system-variable support for.
fn syntheticSelectValue(expr_in: []const u8, current_schema: []const u8) ?SyntheticValue {
    const expr = std.mem.trim(u8, expr_in, " \t\r\n");
    if (std.mem.eql(u8, expr, "null")) return .null_value;

    if (std.mem.startsWith(u8, expr, "@@")) {
        return .{ .text = syntheticVariableValue(expr[2..], current_schema) };
    }

    if (std.mem.eql(u8, expr, "version()")) return .{ .text = handshake.server_version };
    if (std.mem.eql(u8, expr, "database()") or std.mem.eql(u8, expr, "schema()")) return .{ .text = current_schema };
    if (std.mem.eql(u8, expr, "user()") or
        std.mem.eql(u8, expr, "current_user()") or
        std.mem.eql(u8, expr, "session_user()") or
        std.mem.eql(u8, expr, "system_user()"))
        return .{ .text = "thindb@localhost" };
    if (std.mem.eql(u8, expr, "connection_id()")) return .{ .text = "1" };
    if (std.mem.eql(u8, expr, "connection_id")) return .{ .text = "1" };

    return null;
}

fn syntheticVariableValue(var_in: []const u8, current_schema: []const u8) []const u8 {
    var v = std.mem.trim(u8, var_in, " \t\r\n");
    if (std.mem.startsWith(u8, v, "global.")) v = v["global.".len..];
    if (std.mem.startsWith(u8, v, "session.")) v = v["session.".len..];
    if (std.mem.startsWith(u8, v, "local.")) v = v["local.".len..];

    if (std.mem.eql(u8, v, "version")) return handshake.server_version;
    if (std.mem.eql(u8, v, "version_comment")) return "thinDB";
    if (std.mem.eql(u8, v, "version_compile_os")) return osLabel();
    if (std.mem.eql(u8, v, "version_compile_machine")) return "x86_64";
    if (std.mem.eql(u8, v, "protocol_version")) return "10";
    if (std.mem.eql(u8, v, "license")) return "thinDB";
    if (std.mem.eql(u8, v, "hostname")) return "localhost";
    if (std.mem.eql(u8, v, "port")) return "3307";
    if (std.mem.eql(u8, v, "server_id")) return "1";

    if (std.mem.eql(u8, v, "database") or std.mem.eql(u8, v, "schema")) return current_schema;
    if (std.mem.eql(u8, v, "max_allowed_packet")) return "16777216";
    if (std.mem.eql(u8, v, "net_buffer_length")) return "16384";
    if (std.mem.eql(u8, v, "wait_timeout")) return "28800";
    if (std.mem.eql(u8, v, "interactive_timeout")) return "28800";
    if (std.mem.eql(u8, v, "max_connections")) return "256";
    if (std.mem.eql(u8, v, "group_concat_max_len")) return "1024";
    if (std.mem.eql(u8, v, "sql_select_limit")) return "18446744073709551615";

    if (std.mem.eql(u8, v, "tx_isolation") or std.mem.eql(u8, v, "transaction_isolation"))
        return "REPEATABLE-READ";
    if (std.mem.eql(u8, v, "sql_mode")) return "STRICT_TRANS_TABLES";
    if (std.mem.eql(u8, v, "autocommit")) return "1";
    if (std.mem.eql(u8, v, "lower_case_table_names")) return "1";
    if (std.mem.eql(u8, v, "auto_increment_increment")) return "1";
    if (std.mem.eql(u8, v, "auto_increment_offset")) return "1";
    // Writable server. MySQL Connector/J parses these as integers on every
    // batch (isReadOnly()); returning "" throws NumberFormatException and
    // breaks the JDBC sink. thinDB is never read-only.
    if (std.mem.eql(u8, v, "read_only") or
        std.mem.eql(u8, v, "super_read_only") or
        std.mem.eql(u8, v, "transaction_read_only") or
        std.mem.eql(u8, v, "tx_read_only")) return "0";
    if (std.mem.eql(u8, v, "default_storage_engine") or std.mem.eql(u8, v, "storage_engine")) return "thinDB";

    if (std.mem.eql(u8, v, "character_set_client") or
        std.mem.eql(u8, v, "character_set_connection") or
        std.mem.eql(u8, v, "character_set_results") or
        std.mem.eql(u8, v, "character_set_server") or
        std.mem.eql(u8, v, "character_set_database"))
        return "utf8mb4";
    if (std.mem.eql(u8, v, "collation_connection") or
        std.mem.eql(u8, v, "collation_server") or
        std.mem.eql(u8, v, "collation_database"))
        return "utf8mb4_general_ci";
    if (std.mem.eql(u8, v, "time_zone")) return "SYSTEM";
    if (std.mem.eql(u8, v, "system_time_zone")) return "UTC";

    if (std.mem.startsWith(u8, v, "have_")) return "NO";
    return "";
}

fn osLabel() []const u8 {
    return switch (@import("builtin").os.tag) {
        .windows => "Windows",
        .macos => "macOS",
        else => "Linux",
    };
}

fn topLevelKeyword(text: []const u8, keyword: []const u8) ?usize {
    var depth: usize = 0;
    var i: usize = 0;
    while (i < text.len) : (i += 1) {
        const c = text[i];
        if (c == '\'' or c == '"' or c == '`') {
            i = skipQuoted(text, i, c);
            continue;
        }
        if (c == '(') {
            depth += 1;
            continue;
        }
        if (c == ')' and depth > 0) {
            depth -= 1;
            continue;
        }
        if (depth == 0 and tokenAt(text, i, keyword)) return i;
    }
    return null;
}

fn tokenAt(text: []const u8, i: usize, token: []const u8) bool {
    if (i + token.len > text.len) return false;
    if (!std.mem.eql(u8, text[i .. i + token.len], token)) return false;
    if (i > 0 and isIdentByte(text[i - 1])) return false;
    if (i + token.len < text.len and isIdentByte(text[i + token.len])) return false;
    return true;
}

fn isIdentByte(c: u8) bool {
    return std.ascii.isAlphanumeric(c) or c == '_' or c == '$' or c == '@';
}

fn nextTopLevelComma(text: []const u8, start: usize) ?usize {
    var depth: usize = 0;
    var i = start;
    while (i < text.len) : (i += 1) {
        const c = text[i];
        if (c == '\'' or c == '"' or c == '`') {
            i = skipQuoted(text, i, c);
            continue;
        }
        if (c == '(') {
            depth += 1;
            continue;
        }
        if (c == ')' and depth > 0) {
            depth -= 1;
            continue;
        }
        if (c == ',' and depth == 0) return i;
    }
    return null;
}

fn skipQuoted(text: []const u8, start: usize, quote: u8) usize {
    var i = start + 1;
    while (i < text.len) : (i += 1) {
        if (text[i] == quote) {
            if (quote == '\'' and i + 1 < text.len and text[i + 1] == '\'') {
                i += 1;
                continue;
            }
            return i;
        }
    }
    return text.len;
}

fn sendMetadataResult(
    allocator: Allocator,
    w: *std.Io.Writer,
    catalog: *Catalog,
    session: *SessionState,
    payload: []const u8,
    kind: canned.EmptyResultKind,
    seq_id: *u8,
    client_caps: u32,
) !void {
    return switch (kind) {
        .tables => sendTablesResult(allocator, w, catalog, session, payload, seq_id, client_caps),
        .full_tables => sendFullTablesResult(allocator, w, catalog, session, payload, seq_id, client_caps),
        .table_status => sendTableStatusResult(allocator, w, catalog, session, payload, seq_id, client_caps),
        .create_table => sendCreateTableResult(allocator, w, catalog, session, payload, seq_id, client_caps),
        .columns => sendColumnsResult(allocator, w, catalog, session, payload, seq_id, client_caps),
        .indexes => sendIndexesResult(allocator, w, catalog, session, payload, seq_id, client_caps),
        else => sendEmptyMetadataResult(allocator, w, kind, seq_id, client_caps),
    };
}

const ResolvedSchema = struct {
    db_name: []const u8,
    schema_name: []const u8,
    schema: *Schema,
};

const ShowTableTarget = struct {
    schema_name: ?[]const u8,
    table_name: []const u8,
};

const IdentSegment = struct {
    text: []const u8,
    next: usize,
};

const IdentPath = struct {
    first: []const u8,
    last: []const u8,
    count: usize,
    next: usize,
};

/// Extract the string literal of a top-level `LIKE '...'` clause, or null.
/// Doubled '' escapes stay raw — table/column names can't contain quotes,
/// so a pattern with them simply matches nothing.
fn parseShowLikePattern(sql_text: []const u8) ?[]const u8 {
    const s = trimSqlForMetadata(sql_text);
    const idx = topLevelKeywordIgnoreCase(s, "like") orelse return null;
    var i = idx + "like".len;
    while (i < s.len and (s[i] == ' ' or s[i] == '\t')) i += 1;
    if (i >= s.len or s[i] != '\'') return null;
    const end = skipQuoted(s, i, '\'');
    if (end >= s.len) return null;
    return s[i + 1 .. end];
}

/// Case-insensitive SQL LIKE over an identifier — SHOW ... LIKE filters
/// compare case-insensitively in MySQL regardless of platform.
fn likeMatchesIdent(allocator: Allocator, name: []const u8, pattern: []const u8) bool {
    const lname = std.ascii.allocLowerString(allocator, name) catch return true;
    defer allocator.free(lname);
    const lpat = std.ascii.allocLowerString(allocator, pattern) catch return true;
    defer allocator.free(lpat);
    return exec_predicate.likeMatch(lname, lpat);
}

fn sendTablesResult(
    allocator: Allocator,
    w: *std.Io.Writer,
    catalog: *Catalog,
    session: *SessionState,
    payload: []const u8,
    seq_id: *u8,
    client_caps: u32,
) !void {
    const schema_name = parseShowSchemaName(payload);
    const like = parseShowLikePattern(payload);
    const resolved = resolveMetadataSchema(catalog, session, schema_name) orelse {
        return sendEmptyMetadataResult(allocator, w, .tables, seq_id, client_caps);
    };

    const client_schema = try std.fmt.allocPrint(allocator, "{s}__{s}", .{ resolved.db_name, resolved.schema_name });
    defer allocator.free(client_schema);
    const tables_col = try std.fmt.allocPrint(allocator, "Tables_in_{s}", .{client_schema});
    defer allocator.free(tables_col);

    const cols = [_]types.Column{.{ .name = tables_col, .type = .string }};
    try result.sendResultHeader(allocator, w, cols[0..], "", "", seq_id);
    try result.sendColumnDefBoundary(allocator, w, seq_id, client_caps);

    const table_names = try resolved.schema.listTables(allocator);
    defer freeNameList(allocator, table_names);
    for (table_names) |name| {
        if (like) |pat| if (!likeMatchesIdent(allocator, name, pat)) continue;
        const cells = [_]?[]const u8{name};
        try result.sendTextRow(allocator, w, cells[0..], seq_id);
    }
    // Session temp tables appear only in the unqualified form — the temp
    // namespace is session-local, not schema-local (mirrors compileShow).
    if (schema_name == null) if (session.temp_namespace) |ns| {
        const temps = try ns.listTables(allocator);
        defer freeNameList(allocator, temps);
        outer: for (temps) |name| {
            for (table_names) |p| if (std.mem.eql(u8, p, name)) continue :outer;
            if (like) |pat| if (!likeMatchesIdent(allocator, name, pat)) continue;
            const cells = [_]?[]const u8{name};
            try result.sendTextRow(allocator, w, cells[0..], seq_id);
        }
    };

    try result.sendResultTerminator(allocator, w, seq_id, client_caps);
}

fn sendCreateTableResult(
    allocator: Allocator,
    w: *std.Io.Writer,
    catalog: *Catalog,
    session: *SessionState,
    payload: []const u8,
    seq_id: *u8,
    client_caps: u32,
) !void {
    const cols = [_]types.Column{
        .{ .name = "Table", .type = .string },
        .{ .name = "Create Table", .type = .string },
    };
    try result.sendResultHeader(allocator, w, cols[0..], "", "", seq_id);
    try result.sendColumnDefBoundary(allocator, w, seq_id, client_caps);

    blk: {
        const s = trimSqlForMetadata(payload);
        const idx = topLevelKeywordIgnoreCase(s, "table") orelse break :blk;
        const path = readIdentifierPath(s, idx + "table".len + 1) orelse break :blk;
        const target = ShowTableTarget{
            .schema_name = if (path.count >= 2) path.first else null,
            .table_name = path.last,
        };
        const resolved = resolveMetadataTable(catalog, session, target) orelse break :blk;
        const ddl = try allocCreateTableText(allocator, resolved.table);
        defer allocator.free(ddl);
        const cells = [_]?[]const u8{ resolved.table.name, ddl };
        try result.sendTextRow(allocator, w, cells[0..], seq_id);
    }

    try result.sendResultTerminator(allocator, w, seq_id, client_caps);
}

/// Reconstruct re-runnable DDL from the live schema. thinDB dialect: unique
/// tables carry PRIMARY KEY inside the parens; non-unique tables carry the
/// ORDER BY clustering clause after them (that's what CREATE TABLE accepts).
fn allocCreateTableText(allocator: Allocator, t: *Table) ![]u8 {
    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(allocator);
    try out.print(allocator, "CREATE TABLE `{s}` (", .{t.name});
    for (t.schema.columns, 0..) |col, i| {
        const type_text = try allocMysqlColumnType(allocator, col.type);
        defer allocator.free(type_text);
        try out.print(allocator, "{s}\n  `{s}` {s}", .{ if (i == 0) "" else ",", col.name, type_text });
        if (!col.nullable) try out.appendSlice(allocator, " NOT NULL");
        if (col.default_value) |dv| {
            const dtext = (try allocColumnDefaultText(allocator, col)).?;
            defer allocator.free(dtext);
            switch (dv) {
                .text => try out.print(allocator, " DEFAULT '{s}'", .{dtext}),
                else => try out.print(allocator, " DEFAULT {s}", .{dtext}),
            }
        }
        if (col.auto_increment) try out.appendSlice(allocator, " AUTO_INCREMENT");
    }
    if (t.schema.unique) {
        try out.appendSlice(allocator, ",\n  PRIMARY KEY (");
        for (t.schema.order_key, 0..) |k, i| {
            try out.print(allocator, "{s}`{s}`", .{ if (i == 0) "" else ", ", k });
        }
        try out.appendSlice(allocator, ")\n)");
    } else {
        try out.appendSlice(allocator, "\n) ORDER BY (");
        for (t.schema.order_key, 0..) |k, i| {
            try out.print(allocator, "{s}`{s}`", .{ if (i == 0) "" else ", ", k });
        }
        try out.appendSlice(allocator, ")");
    }
    return out.toOwnedSlice(allocator);
}

fn sendFullTablesResult(
    allocator: Allocator,
    w: *std.Io.Writer,
    catalog: *Catalog,
    session: *SessionState,
    payload: []const u8,
    seq_id: *u8,
    client_caps: u32,
) !void {
    const schema_name = parseShowSchemaName(payload);
    const resolved = resolveMetadataSchema(catalog, session, schema_name) orelse {
        return sendEmptyMetadataResult(allocator, w, .full_tables, seq_id, client_caps);
    };

    const client_schema = try std.fmt.allocPrint(allocator, "{s}__{s}", .{ resolved.db_name, resolved.schema_name });
    defer allocator.free(client_schema);
    const tables_col = try std.fmt.allocPrint(allocator, "Tables_in_{s}", .{client_schema});
    defer allocator.free(tables_col);

    const cols = [_]types.Column{
        .{ .name = tables_col, .type = .string },
        .{ .name = "Table_type", .type = .string },
    };
    try result.sendResultHeader(allocator, w, cols[0..], "", "", seq_id);
    try result.sendColumnDefBoundary(allocator, w, seq_id, client_caps);

    const like = parseShowLikePattern(payload);
    const table_names = try resolved.schema.listTables(allocator);
    defer freeNameList(allocator, table_names);
    for (table_names) |name| {
        if (like) |pat| if (!likeMatchesIdent(allocator, name, pat)) continue;
        const cells = [_]?[]const u8{ name, "BASE TABLE" };
        try result.sendTextRow(allocator, w, cells[0..], seq_id);
    }

    try result.sendResultTerminator(allocator, w, seq_id, client_caps);
}

fn sendTableStatusResult(
    allocator: Allocator,
    w: *std.Io.Writer,
    catalog: *Catalog,
    session: *SessionState,
    payload: []const u8,
    seq_id: *u8,
    client_caps: u32,
) !void {
    const cols = [_]types.Column{
        .{ .name = "Name", .type = .string },
        .{ .name = "Engine", .type = .string },
        .{ .name = "Version", .type = .int },
        .{ .name = "Row_format", .type = .string },
        .{ .name = "Rows", .type = .bigint },
        .{ .name = "Avg_row_length", .type = .bigint },
        .{ .name = "Data_length", .type = .bigint },
        .{ .name = "Max_data_length", .type = .bigint },
        .{ .name = "Index_length", .type = .bigint },
        .{ .name = "Data_free", .type = .bigint },
        .{ .name = "Auto_increment", .type = .bigint, .nullable = true },
        .{ .name = "Create_time", .type = .string, .nullable = true },
        .{ .name = "Update_time", .type = .string, .nullable = true },
        .{ .name = "Check_time", .type = .string, .nullable = true },
        .{ .name = "Collation", .type = .string, .nullable = true },
        .{ .name = "Checksum", .type = .bigint, .nullable = true },
        .{ .name = "Create_options", .type = .string },
        .{ .name = "Comment", .type = .string },
    };
    try result.sendResultHeader(allocator, w, cols[0..], "", "", seq_id);
    try result.sendColumnDefBoundary(allocator, w, seq_id, client_caps);

    const schema_name = parseShowSchemaName(payload);
    if (resolveMetadataSchema(catalog, session, schema_name)) |resolved| {
        const table_names = try resolved.schema.listTables(allocator);
        defer freeNameList(allocator, table_names);
        for (table_names) |name| {
            const table = schemaTable(resolved.schema, name) orelse continue;
            const row_count = tableRowCount(table);
            var row_count_buf: [32]u8 = undefined;
            const row_count_text = try std.fmt.bufPrint(&row_count_buf, "{d}", .{row_count});
            const cells = [_]?[]const u8{
                name,
                "thinDB",
                "10",
                "Columnar",
                row_count_text,
                "0",
                "0",
                "0",
                "0",
                "0",
                null,
                null,
                null,
                null,
                "utf8mb4_general_ci",
                null,
                "",
                "",
            };
            try result.sendTextRow(allocator, w, cells[0..], seq_id);
        }
    }

    try result.sendResultTerminator(allocator, w, seq_id, client_caps);
}

fn sendColumnsResult(
    allocator: Allocator,
    w: *std.Io.Writer,
    catalog: *Catalog,
    session: *SessionState,
    payload: []const u8,
    seq_id: *u8,
    client_caps: u32,
) !void {
    const lc = try sql_text_mod.normalizeForCannedMatch(allocator, payload);
    defer allocator.free(lc);
    const full = std.mem.startsWith(u8, lc, "show full columns") or
        std.mem.startsWith(u8, lc, "show full fields");

    const simple_cols = [_]types.Column{
        .{ .name = "Field", .type = .string },
        .{ .name = "Type", .type = .string },
        .{ .name = "Null", .type = .string },
        .{ .name = "Key", .type = .string },
        .{ .name = "Default", .type = .string, .nullable = true },
        .{ .name = "Extra", .type = .string },
    };
    const full_cols = [_]types.Column{
        .{ .name = "Field", .type = .string },
        .{ .name = "Type", .type = .string },
        .{ .name = "Collation", .type = .string, .nullable = true },
        .{ .name = "Null", .type = .string },
        .{ .name = "Key", .type = .string },
        .{ .name = "Default", .type = .string, .nullable = true },
        .{ .name = "Extra", .type = .string },
        .{ .name = "Privileges", .type = .string },
        .{ .name = "Comment", .type = .string },
    };
    const cols = if (full) full_cols[0..] else simple_cols[0..];
    try result.sendResultHeader(allocator, w, cols, "", "", seq_id);
    try result.sendColumnDefBoundary(allocator, w, seq_id, client_caps);

    const like = parseShowLikePattern(payload);
    if (parseShowTableTarget(payload)) |target| {
        if (resolveMetadataTable(catalog, session, target)) |resolved| {
            const table = resolved.table;
            for (table.schema.columns) |col| {
                if (like) |pat| if (!likeMatchesIdent(allocator, col.name, pat)) continue;
                const type_text = try allocMysqlColumnType(allocator, col.type);
                defer allocator.free(type_text);
                const default_text = try allocColumnDefaultText(allocator, col);
                defer if (default_text) |d| allocator.free(d);
                const key = columnKey(table, col.name);
                const nullable = if (col.nullable) "YES" else "NO";
                const extra: []const u8 = if (col.auto_increment) "auto_increment" else "";
                if (full) {
                    const cells = [_]?[]const u8{
                        col.name,
                        type_text,
                        columnCollation(col),
                        nullable,
                        key,
                        default_text,
                        extra,
                        "select,insert,update,references",
                        "",
                    };
                    try result.sendTextRow(allocator, w, cells[0..], seq_id);
                } else {
                    const cells = [_]?[]const u8{
                        col.name,
                        type_text,
                        nullable,
                        key,
                        default_text,
                        extra,
                    };
                    try result.sendTextRow(allocator, w, cells[0..], seq_id);
                }
            }
        }
    }

    try result.sendResultTerminator(allocator, w, seq_id, client_caps);
}

fn sendIndexesResult(
    allocator: Allocator,
    w: *std.Io.Writer,
    catalog: *Catalog,
    session: *SessionState,
    payload: []const u8,
    seq_id: *u8,
    client_caps: u32,
) !void {
    const cols = [_]types.Column{
        .{ .name = "Table", .type = .string },
        .{ .name = "Non_unique", .type = .int },
        .{ .name = "Key_name", .type = .string },
        .{ .name = "Seq_in_index", .type = .int },
        .{ .name = "Column_name", .type = .string },
        .{ .name = "Collation", .type = .string, .nullable = true },
        .{ .name = "Cardinality", .type = .bigint, .nullable = true },
        .{ .name = "Sub_part", .type = .bigint, .nullable = true },
        .{ .name = "Packed", .type = .string, .nullable = true },
        .{ .name = "Null", .type = .string },
        .{ .name = "Index_type", .type = .string },
        .{ .name = "Comment", .type = .string },
        .{ .name = "Index_comment", .type = .string },
        .{ .name = "Visible", .type = .string },
        .{ .name = "Expression", .type = .string, .nullable = true },
    };
    try result.sendResultHeader(allocator, w, cols[0..], "", "", seq_id);
    try result.sendColumnDefBoundary(allocator, w, seq_id, client_caps);

    if (parseShowTableTarget(payload)) |target| {
        if (resolveMetadataTable(catalog, session, target)) |resolved| {
            const table = resolved.table;
            const non_unique = if (table.schema.unique) "0" else "1";
            const key_name = if (table.schema.unique) "PRIMARY" else "order_key";
            const row_count = tableRowCount(table);
            var cardinality_buf: [32]u8 = undefined;
            const cardinality_text = try std.fmt.bufPrint(&cardinality_buf, "{d}", .{row_count});
            for (table.schema.order_key, 0..) |col_name, i| {
                var seq_buf: [24]u8 = undefined;
                const seq_text = try std.fmt.bufPrint(&seq_buf, "{d}", .{i + 1});
                const col = table.schema.column(col_name);
                const null_text = if (col != null and col.?.nullable) "YES" else "";
                const cells = [_]?[]const u8{
                    table.name,
                    non_unique,
                    key_name,
                    seq_text,
                    col_name,
                    "A",
                    cardinality_text,
                    null,
                    null,
                    null_text,
                    "BTREE",
                    "",
                    "",
                    "YES",
                    null,
                };
                try result.sendTextRow(allocator, w, cells[0..], seq_id);
            }
        }
    }

    try result.sendResultTerminator(allocator, w, seq_id, client_caps);
}

const MetadataTable = struct {
    resolved: ResolvedSchema,
    table: *Table,
};

fn resolveMetadataTable(
    catalog: *Catalog,
    session: *SessionState,
    target: ShowTableTarget,
) ?MetadataTable {
    const resolved = resolveMetadataSchema(catalog, session, target.schema_name) orelse return null;
    const table = schemaTable(resolved.schema, target.table_name) orelse return null;
    return .{ .resolved = resolved, .table = table };
}

fn resolveMetadataSchema(
    catalog: *Catalog,
    session: *SessionState,
    maybe_name: ?[]const u8,
) ?ResolvedSchema {
    if (maybe_name) |raw_name| {
        const name = stripIdentifierQuotes(std.mem.trim(u8, raw_name, " \t\r\n"));
        if (name.len == 0) return null;
        if (std.mem.indexOf(u8, name, "__")) |sep| {
            const db_name = name[0..sep];
            const schema_name = name[sep + 2 ..];
            const db = catalog.database(db_name) orelse return null;
            const sc = db.schema(schema_name) orelse return null;
            return .{ .db_name = db.name, .schema_name = sc.name, .schema = sc };
        }

        if (catalog.database(session.current_db)) |cur_db| {
            if (cur_db.schema(name)) |sc| {
                return .{ .db_name = cur_db.name, .schema_name = sc.name, .schema = sc };
            }
        }
        if (catalog.database(name)) |db| {
            if (db.schema("public")) |sc| {
                return .{ .db_name = db.name, .schema_name = sc.name, .schema = sc };
            }
        }
        return null;
    }

    const db = catalog.database(session.current_db) orelse return null;
    const sc = db.schema(session.current_schema) orelse return null;
    return .{ .db_name = db.name, .schema_name = sc.name, .schema = sc };
}

fn schemaTable(sc: *Schema, name: []const u8) ?*Table {
    {
        sc.tables_mutex.lockUncancelable(sc.io);
        defer sc.tables_mutex.unlock(sc.io);
        if (sc.tables.get(name)) |t| return t;
    }
    return sc.openTable(name, .{}) catch null;
}

fn tableRowCount(t: *Table) u64 {
    t.mutex.lockUncancelable(t.io);
    defer t.mutex.unlock(t.io);
    var rows = t.memtable.row_count;
    for (t.manifest.segments.items) |entry| rows += entry.row_count;
    return rows;
}

fn parseShowSchemaName(sql_text: []const u8) ?[]const u8 {
    const s = trimSqlForMetadata(sql_text);
    // Keyword-specific offsets: `idx + 4` on an `IN` match would start the
    // identifier scan two bytes late and truncate the name.
    if (topLevelKeywordIgnoreCase(s, "from")) |idx| {
        const path = readIdentifierPath(s, idx + "from".len) orelse return null;
        return path.last;
    }
    if (topLevelKeywordIgnoreCase(s, "in")) |idx| {
        const path = readIdentifierPath(s, idx + "in".len) orelse return null;
        return path.last;
    }
    return null;
}

fn parseShowTableTarget(sql_text: []const u8) ?ShowTableTarget {
    const s = trimSqlForMetadata(sql_text);
    if (startsWithIgnoreCase(s, "desc ")) {
        const path = readIdentifierPath(s, "desc ".len) orelse return null;
        return .{ .schema_name = if (path.count >= 2) path.first else null, .table_name = path.last };
    }
    if (startsWithIgnoreCase(s, "describe ")) {
        const path = readIdentifierPath(s, "describe ".len) orelse return null;
        return .{ .schema_name = if (path.count >= 2) path.first else null, .table_name = path.last };
    }

    const idx = topLevelKeywordIgnoreCase(s, "from") orelse topLevelKeywordIgnoreCase(s, "in") orelse return null;
    const path = readIdentifierPath(s, idx + 4) orelse return null;
    var schema_name: ?[]const u8 = if (path.count >= 2) path.first else null;
    if (schema_name == null) {
        const rest = s[path.next..];
        if (topLevelKeywordIgnoreCase(rest, "from")) |schema_idx| {
            if (readIdentifierPath(rest, schema_idx + 4)) |schema_path| schema_name = schema_path.last;
        } else if (topLevelKeywordIgnoreCase(rest, "in")) |schema_idx| {
            if (readIdentifierPath(rest, schema_idx + 2)) |schema_path| schema_name = schema_path.last;
        }
    }
    return .{ .schema_name = schema_name, .table_name = path.last };
}

fn trimSqlForMetadata(sql_text: []const u8) []const u8 {
    var s = stripLeadingCommentsForMetadata(std.mem.trim(u8, sql_text, " \t\r\n"));
    while (s.len > 0 and s[s.len - 1] == ';') s = std.mem.trim(u8, s[0 .. s.len - 1], " \t\r\n");
    return s;
}

fn stripLeadingCommentsForMetadata(sql_text: []const u8) []const u8 {
    var s = sql_text;
    while (true) {
        s = std.mem.trim(u8, s, " \t\r\n");
        if (std.mem.startsWith(u8, s, "/*")) {
            const end = std.mem.indexOf(u8, s[2..], "*/") orelse return s;
            s = s[end + 4 ..];
            continue;
        }
        if (std.mem.startsWith(u8, s, "--")) {
            const end = std.mem.indexOfScalar(u8, s, '\n') orelse return "";
            s = s[end + 1 ..];
            continue;
        }
        return s;
    }
}

fn readIdentifierPath(text: []const u8, start: usize) ?IdentPath {
    var i = skipMetadataWhitespace(text, start);
    var first: []const u8 = "";
    var last: []const u8 = "";
    var count: usize = 0;
    while (true) {
        const seg = readIdentifierSegment(text, i) orelse break;
        const clean = stripIdentifierQuotes(seg.text);
        if (count == 0) first = clean;
        last = clean;
        count += 1;
        i = skipMetadataWhitespace(text, seg.next);
        if (i < text.len and text[i] == '.') {
            i += 1;
            i = skipMetadataWhitespace(text, i);
            continue;
        }
        break;
    }
    if (count == 0) return null;
    return .{ .first = first, .last = last, .count = count, .next = i };
}

fn readIdentifierSegment(text: []const u8, start: usize) ?IdentSegment {
    var i = skipMetadataWhitespace(text, start);
    if (i >= text.len) return null;
    const begin = i;
    const c = text[i];
    if (c == '`' or c == '"' or c == '\'') {
        i += 1;
        while (i < text.len) : (i += 1) {
            if (text[i] == c) {
                if (i + 1 < text.len and text[i + 1] == c) {
                    i += 1;
                    continue;
                }
                return .{ .text = text[begin .. i + 1], .next = i + 1 };
            }
        }
        return .{ .text = text[begin..], .next = text.len };
    }

    while (i < text.len) : (i += 1) {
        switch (text[i]) {
            ' ', '\t', '\r', '\n', '.', ',', ';', '(', ')' => break,
            else => {},
        }
    }
    if (i == begin) return null;
    return .{ .text = text[begin..i], .next = i };
}

fn skipMetadataWhitespace(text: []const u8, start: usize) usize {
    var i = start;
    while (i < text.len and std.ascii.isWhitespace(text[i])) : (i += 1) {}
    return i;
}

fn startsWithIgnoreCase(text: []const u8, prefix: []const u8) bool {
    return text.len >= prefix.len and std.ascii.eqlIgnoreCase(text[0..prefix.len], prefix);
}

fn topLevelKeywordIgnoreCase(text: []const u8, keyword: []const u8) ?usize {
    var depth: usize = 0;
    var i: usize = 0;
    while (i < text.len) : (i += 1) {
        const c = text[i];
        if (c == '\'' or c == '"' or c == '`') {
            i = skipQuoted(text, i, c);
            continue;
        }
        if (c == '(') {
            depth += 1;
            continue;
        }
        if (c == ')' and depth > 0) {
            depth -= 1;
            continue;
        }
        if (depth == 0 and tokenAtIgnoreCase(text, i, keyword)) return i;
    }
    return null;
}

fn tokenAtIgnoreCase(text: []const u8, i: usize, token: []const u8) bool {
    if (i + token.len > text.len) return false;
    if (!std.ascii.eqlIgnoreCase(text[i .. i + token.len], token)) return false;
    if (i > 0 and isIdentByte(text[i - 1])) return false;
    if (i + token.len < text.len and isIdentByte(text[i + token.len])) return false;
    return true;
}

fn allocMysqlColumnType(allocator: Allocator, t: types.Type) ![]u8 {
    return switch (t) {
        .tinyint => allocator.dupe(u8, "tinyint"),
        .smallint => allocator.dupe(u8, "smallint"),
        .int => allocator.dupe(u8, "int"),
        .bigint => allocator.dupe(u8, "bigint"),
        .largeint => allocator.dupe(u8, "decimal(38,0)"),
        .boolean => allocator.dupe(u8, "tinyint(1)"),
        .float => allocator.dupe(u8, "float"),
        .double => allocator.dupe(u8, "double"),
        .date => allocator.dupe(u8, "date"),
        .datetime => allocator.dupe(u8, "datetime(6)"),
        .decimal64 => |spec| std.fmt.allocPrint(allocator, "decimal({d},{d})", .{ spec.p, spec.s }),
        .decimal128 => |spec| std.fmt.allocPrint(allocator, "decimal({d},{d})", .{ spec.p, spec.s }),
        .uuid => allocator.dupe(u8, "char(36)"),
        .varchar => |n| std.fmt.allocPrint(allocator, "varchar({d})", .{n}),
        .char => |n| std.fmt.allocPrint(allocator, "char({d})", .{n}),
        .string => allocator.dupe(u8, "text"),
        .json => allocator.dupe(u8, "json"),
    };
}

/// Render a column's literal DEFAULT as MySQL would display it in
/// SHOW COLUMNS / DESCRIBE, or null when there is no default. Caller owns
/// the returned slice.
fn allocColumnDefaultText(allocator: Allocator, col: types.Column) !?[]const u8 {
    const v = col.default_value orelse return null;
    return switch (v) {
        .boolean => |b| try allocator.dupe(u8, if (b) "1" else "0"),
        .text => |s| try allocator.dupe(u8, s),
        .int => |x| try std.fmt.allocPrint(allocator, "{d}", .{x}),
        .bigint => |x| try std.fmt.allocPrint(allocator, "{d}", .{x}),
        .tinyint => |x| try std.fmt.allocPrint(allocator, "{d}", .{x}),
        .smallint => |x| try std.fmt.allocPrint(allocator, "{d}", .{x}),
        .largeint => |x| try std.fmt.allocPrint(allocator, "{d}", .{x}),
        .decimal64 => |x| try std.fmt.allocPrint(allocator, "{d}", .{x}),
        .decimal128 => |x| try std.fmt.allocPrint(allocator, "{d}", .{x}),
        .float => |x| try std.fmt.allocPrint(allocator, "{d}", .{x}),
        .double => |x| try std.fmt.allocPrint(allocator, "{d}", .{x}),
        .date => |x| try std.fmt.allocPrint(allocator, "{d}", .{x}),
        .datetime => |x| try std.fmt.allocPrint(allocator, "{d}", .{x}),
        .uuid => |x| try std.fmt.allocPrint(allocator, "{d}", .{x}),
    };
}

fn allocInfoDataType(allocator: Allocator, t: types.Type) ![]u8 {
    return switch (t) {
        .tinyint => allocator.dupe(u8, "tinyint"),
        .smallint => allocator.dupe(u8, "smallint"),
        .int => allocator.dupe(u8, "int"),
        .bigint => allocator.dupe(u8, "bigint"),
        .largeint, .decimal64, .decimal128 => allocator.dupe(u8, "decimal"),
        .boolean => allocator.dupe(u8, "tinyint"),
        .float => allocator.dupe(u8, "float"),
        .double => allocator.dupe(u8, "double"),
        .date => allocator.dupe(u8, "date"),
        .datetime => allocator.dupe(u8, "datetime"),
        .uuid => allocator.dupe(u8, "char"),
        .varchar => allocator.dupe(u8, "varchar"),
        .char => allocator.dupe(u8, "char"),
        .json => allocator.dupe(u8, "json"),
        .string => allocator.dupe(u8, "text"),
    };
}

fn columnCollation(col: types.Column) ?[]const u8 {
    return if (col.type.isString()) "utf8mb4_general_ci" else null;
}

fn columnKey(t: *Table, col_name: []const u8) []const u8 {
    if (!isOrderKeyColumn(t, col_name)) return "";
    return if (t.schema.unique) "PRI" else "MUL";
}

fn isOrderKeyColumn(t: *Table, col_name: []const u8) bool {
    for (t.schema.order_key) |key| {
        if (std.mem.eql(u8, key, col_name)) return true;
    }
    return false;
}

const InfoSchemaKind = enum { schemata, tables, columns, statistics, key_column_usage, table_constraints };

/// LIMIT/OFFSET for the streamed information_schema emitters. `admit`
/// consumes one row slot; once it reports done the emitters stop scanning.
const InfoLimit = struct {
    skip: usize = 0,
    remaining: usize = std.math.maxInt(usize),

    fn admit(self: *InfoLimit) bool {
        if (self.skip > 0) {
            self.skip -= 1;
            return false;
        }
        if (self.remaining == 0) return false;
        self.remaining -= 1;
        return true;
    }
    fn done(self: *const InfoLimit) bool {
        return self.remaining == 0;
    }
};

/// Parse `LIMIT n`, `LIMIT m, n`, and `LIMIT n OFFSET m` from the statement
/// tail. Anything unparseable leaves the unbounded default.
fn parseInfoLimit(tail: []const u8) InfoLimit {
    const idx = topLevelKeywordIgnoreCase(tail, "limit") orelse return .{};
    var i = skipMetadataWhitespace(tail, idx + "limit".len);
    const first = parseInfoUint(tail, &i) orelse return .{};
    i = skipMetadataWhitespace(tail, i);
    if (i < tail.len and tail[i] == ',') {
        i = skipMetadataWhitespace(tail, i + 1);
        const second = parseInfoUint(tail, &i) orelse return .{};
        return .{ .skip = first, .remaining = second };
    }
    if (tokenAtIgnoreCase(tail, i, "offset")) {
        i = skipMetadataWhitespace(tail, i + "offset".len);
        const off = parseInfoUint(tail, &i) orelse return .{ .remaining = first };
        return .{ .skip = off, .remaining = first };
    }
    return .{ .remaining = first };
}

fn parseInfoUint(text: []const u8, i: *usize) ?usize {
    const start = i.*;
    var end = start;
    while (end < text.len and text[end] >= '0' and text[end] <= '9') end += 1;
    if (end == start) return null;
    i.* = end;
    return std.fmt.parseInt(usize, text[start..end], 10) catch null;
}

const InfoProjection = struct {
    column: types.Column,
    key: []const u8,
};

const InfoFilters = struct {
    schema_name: ?[]const u8 = null,
    table_name: ?[]const u8 = null,
};

const InfoRow = struct {
    db_name: []const u8,
    schema_name: []const u8,
    table_name: ?[]const u8 = null,
    table: ?*Table = null,
    column: ?types.Column = null,
    ordinal: usize = 0,
    key_seq: usize = 0,
};

fn sendInformationSchemaSelect(
    allocator: Allocator,
    w: *std.Io.Writer,
    catalog: *Catalog,
    select_list: []const u8,
    tail: []const u8,
    seq_id: *u8,
    client_caps: u32,
) !bool {
    const kind = informationSchemaKind(tail) orelse return false;
    const filters = parseInfoFilters(tail);
    var lim = parseInfoLimit(tail);

    var projections = std.ArrayList(InfoProjection).empty;
    defer projections.deinit(allocator);
    try appendInfoProjections(allocator, &projections, select_list, kind);

    var cols = std.ArrayList(types.Column).empty;
    defer cols.deinit(allocator);
    for (projections.items) |p| try cols.append(allocator, p.column);

    try result.sendResultHeader(allocator, w, cols.items, "", "", seq_id);
    try result.sendColumnDefBoundary(allocator, w, seq_id, client_caps);

    switch (kind) {
        .schemata => try emitInfoSchemataRows(allocator, w, catalog, projections.items, filters, &lim, seq_id),
        .tables => try emitInfoTablesRows(allocator, w, catalog, projections.items, filters, &lim, seq_id),
        .columns => try emitInfoColumnsRows(allocator, w, catalog, projections.items, filters, &lim, seq_id),
        .statistics => try emitInfoStatisticsRows(allocator, w, catalog, projections.items, filters, &lim, seq_id),
        .key_column_usage => try emitInfoKeyColumnUsageRows(allocator, w, catalog, projections.items, filters, &lim, seq_id),
        .table_constraints => try emitInfoTableConstraintsRows(allocator, w, catalog, projections.items, filters, &lim, seq_id),
    }

    try result.sendResultTerminator(allocator, w, seq_id, client_caps);
    return true;
}

fn informationSchemaKind(tail: []const u8) ?InfoSchemaKind {
    // Most-specific names first: substring matching would misroute
    // `key_column_usage`/`table_constraints` to columns/tables.
    if (std.mem.indexOf(u8, tail, "key_column_usage") != null) return .key_column_usage;
    if (std.mem.indexOf(u8, tail, "table_constraints") != null) return .table_constraints;
    if (std.mem.indexOf(u8, tail, "schemata") != null) return .schemata;
    if (std.mem.indexOf(u8, tail, "statistics") != null) return .statistics;
    if (std.mem.indexOf(u8, tail, "columns") != null) return .columns;
    if (std.mem.indexOf(u8, tail, "tables") != null) return .tables;
    return null;
}

fn parseInfoFilters(tail: []const u8) InfoFilters {
    return .{
        .schema_name = parseInfoStringFilter(tail, "table_schema") orelse parseInfoStringFilter(tail, "schema_name"),
        .table_name = parseInfoStringFilter(tail, "table_name"),
    };
}

fn parseInfoStringFilter(tail: []const u8, column_name: []const u8) ?[]const u8 {
    var search_start: usize = 0;
    while (search_start < tail.len) {
        const rel = std.mem.indexOf(u8, tail[search_start..], column_name) orelse return null;
        const idx = search_start + rel;
        if ((idx > 0 and isIdentByte(tail[idx - 1])) or
            (idx + column_name.len < tail.len and isIdentByte(tail[idx + column_name.len])))
        {
            search_start = idx + column_name.len;
            continue;
        }
        const eq_rel = std.mem.indexOfScalar(u8, tail[idx + column_name.len ..], '=') orelse return null;
        var value_start = skipMetadataWhitespace(tail, idx + column_name.len + eq_rel + 1);
        if (value_start >= tail.len) return null;
        const quote = tail[value_start];
        if (quote == '\'' or quote == '"' or quote == '`') {
            value_start += 1;
            var value_end = value_start;
            while (value_end < tail.len) : (value_end += 1) {
                if (tail[value_end] == quote) return tail[value_start..value_end];
            }
            return tail[value_start..];
        }
        const seg = readIdentifierSegment(tail, value_start) orelse return null;
        return stripIdentifierQuotes(seg.text);
    }
    return null;
}

fn rowMatchesInfoFilters(row: InfoRow, filters: InfoFilters) bool {
    if (filters.schema_name) |schema_filter| {
        if (!schemaFilterMatches(schema_filter, row.db_name, row.schema_name)) return false;
    }
    if (filters.table_name) |table_filter| {
        const table_name = row.table_name orelse return false;
        if (!std.ascii.eqlIgnoreCase(table_name, table_filter)) return false;
    }
    return true;
}

fn schemaFilterMatches(filter_in: []const u8, db_name: []const u8, schema_name: []const u8) bool {
    const filter = stripIdentifierQuotes(std.mem.trim(u8, filter_in, " \t\r\n"));
    if (std.mem.indexOf(u8, filter, "__")) |sep| {
        return std.ascii.eqlIgnoreCase(filter[0..sep], db_name) and
            std.ascii.eqlIgnoreCase(filter[sep + 2 ..], schema_name);
    }
    return std.ascii.eqlIgnoreCase(filter, schema_name) or std.ascii.eqlIgnoreCase(filter, db_name);
}

fn appendInfoProjections(
    allocator: Allocator,
    projections: *std.ArrayList(InfoProjection),
    select_list: []const u8,
    kind: InfoSchemaKind,
) !void {
    var saw_star = false;
    var start: usize = 0;
    while (start < select_list.len) {
        const end = nextTopLevelComma(select_list, start) orelse select_list.len;
        const raw_expr = std.mem.trim(u8, select_list[start..end], " \t\r\n");
        if (raw_expr.len > 0) {
            if (std.mem.eql(u8, raw_expr, "*") or std.mem.endsWith(u8, raw_expr, ".*")) {
                saw_star = true;
            } else {
                const stripped = stripAlias(raw_expr);
                const key = infoColumnKey(stripped.expr);
                const name = stripIdentifierQuotes(stripped.alias orelse key);
                try projections.append(allocator, .{
                    .column = .{ .name = name, .type = .string, .nullable = true },
                    .key = key,
                });
            }
        }
        start = end + 1;
    }
    if (projections.items.len == 0 or saw_star) {
        projections.clearRetainingCapacity();
        try appendDefaultInfoProjections(allocator, projections, kind);
    }
}

fn appendInfoProjectionLiteral(
    allocator: Allocator,
    projections: *std.ArrayList(InfoProjection),
    name: []const u8,
    key: []const u8,
) !void {
    try projections.append(allocator, .{
        .column = .{ .name = name, .type = .string, .nullable = true },
        .key = key,
    });
}

fn appendDefaultInfoProjections(
    allocator: Allocator,
    projections: *std.ArrayList(InfoProjection),
    kind: InfoSchemaKind,
) !void {
    switch (kind) {
        .schemata => {
            try appendInfoProjectionLiteral(allocator, projections, "CATALOG_NAME", "catalog_name");
            try appendInfoProjectionLiteral(allocator, projections, "SCHEMA_NAME", "schema_name");
            try appendInfoProjectionLiteral(allocator, projections, "DEFAULT_CHARACTER_SET_NAME", "default_character_set_name");
            try appendInfoProjectionLiteral(allocator, projections, "DEFAULT_COLLATION_NAME", "default_collation_name");
            try appendInfoProjectionLiteral(allocator, projections, "SQL_PATH", "sql_path");
        },
        .tables => {
            try appendInfoProjectionLiteral(allocator, projections, "TABLE_CATALOG", "table_catalog");
            try appendInfoProjectionLiteral(allocator, projections, "TABLE_SCHEMA", "table_schema");
            try appendInfoProjectionLiteral(allocator, projections, "TABLE_NAME", "table_name");
            try appendInfoProjectionLiteral(allocator, projections, "TABLE_TYPE", "table_type");
            try appendInfoProjectionLiteral(allocator, projections, "ENGINE", "engine");
            try appendInfoProjectionLiteral(allocator, projections, "VERSION", "version");
            try appendInfoProjectionLiteral(allocator, projections, "ROW_FORMAT", "row_format");
            try appendInfoProjectionLiteral(allocator, projections, "TABLE_ROWS", "table_rows");
            try appendInfoProjectionLiteral(allocator, projections, "TABLE_COLLATION", "table_collation");
            try appendInfoProjectionLiteral(allocator, projections, "TABLE_COMMENT", "table_comment");
        },
        .columns => {
            try appendInfoProjectionLiteral(allocator, projections, "TABLE_CATALOG", "table_catalog");
            try appendInfoProjectionLiteral(allocator, projections, "TABLE_SCHEMA", "table_schema");
            try appendInfoProjectionLiteral(allocator, projections, "TABLE_NAME", "table_name");
            try appendInfoProjectionLiteral(allocator, projections, "COLUMN_NAME", "column_name");
            try appendInfoProjectionLiteral(allocator, projections, "ORDINAL_POSITION", "ordinal_position");
            try appendInfoProjectionLiteral(allocator, projections, "COLUMN_DEFAULT", "column_default");
            try appendInfoProjectionLiteral(allocator, projections, "IS_NULLABLE", "is_nullable");
            try appendInfoProjectionLiteral(allocator, projections, "DATA_TYPE", "data_type");
            try appendInfoProjectionLiteral(allocator, projections, "COLUMN_TYPE", "column_type");
            try appendInfoProjectionLiteral(allocator, projections, "COLUMN_KEY", "column_key");
            try appendInfoProjectionLiteral(allocator, projections, "EXTRA", "extra");
            try appendInfoProjectionLiteral(allocator, projections, "COLUMN_COMMENT", "column_comment");
        },
        .statistics => {
            try appendInfoProjectionLiteral(allocator, projections, "TABLE_CATALOG", "table_catalog");
            try appendInfoProjectionLiteral(allocator, projections, "TABLE_SCHEMA", "table_schema");
            try appendInfoProjectionLiteral(allocator, projections, "TABLE_NAME", "table_name");
            try appendInfoProjectionLiteral(allocator, projections, "NON_UNIQUE", "non_unique");
            try appendInfoProjectionLiteral(allocator, projections, "INDEX_SCHEMA", "index_schema");
            try appendInfoProjectionLiteral(allocator, projections, "INDEX_NAME", "index_name");
            try appendInfoProjectionLiteral(allocator, projections, "SEQ_IN_INDEX", "seq_in_index");
            try appendInfoProjectionLiteral(allocator, projections, "COLUMN_NAME", "column_name");
            try appendInfoProjectionLiteral(allocator, projections, "COLLATION", "collation");
            try appendInfoProjectionLiteral(allocator, projections, "CARDINALITY", "cardinality");
            try appendInfoProjectionLiteral(allocator, projections, "NULLABLE", "nullable");
            try appendInfoProjectionLiteral(allocator, projections, "INDEX_TYPE", "index_type");
            try appendInfoProjectionLiteral(allocator, projections, "IS_VISIBLE", "is_visible");
        },
        .key_column_usage => {
            try appendInfoProjectionLiteral(allocator, projections, "CONSTRAINT_CATALOG", "constraint_catalog");
            try appendInfoProjectionLiteral(allocator, projections, "CONSTRAINT_SCHEMA", "constraint_schema");
            try appendInfoProjectionLiteral(allocator, projections, "CONSTRAINT_NAME", "constraint_name");
            try appendInfoProjectionLiteral(allocator, projections, "TABLE_CATALOG", "table_catalog");
            try appendInfoProjectionLiteral(allocator, projections, "TABLE_SCHEMA", "table_schema");
            try appendInfoProjectionLiteral(allocator, projections, "TABLE_NAME", "table_name");
            try appendInfoProjectionLiteral(allocator, projections, "COLUMN_NAME", "column_name");
            try appendInfoProjectionLiteral(allocator, projections, "ORDINAL_POSITION", "ordinal_position");
            try appendInfoProjectionLiteral(allocator, projections, "POSITION_IN_UNIQUE_CONSTRAINT", "position_in_unique_constraint");
            try appendInfoProjectionLiteral(allocator, projections, "REFERENCED_TABLE_SCHEMA", "referenced_table_schema");
            try appendInfoProjectionLiteral(allocator, projections, "REFERENCED_TABLE_NAME", "referenced_table_name");
            try appendInfoProjectionLiteral(allocator, projections, "REFERENCED_COLUMN_NAME", "referenced_column_name");
        },
        .table_constraints => {
            try appendInfoProjectionLiteral(allocator, projections, "CONSTRAINT_CATALOG", "constraint_catalog");
            try appendInfoProjectionLiteral(allocator, projections, "CONSTRAINT_SCHEMA", "constraint_schema");
            try appendInfoProjectionLiteral(allocator, projections, "CONSTRAINT_NAME", "constraint_name");
            try appendInfoProjectionLiteral(allocator, projections, "TABLE_SCHEMA", "table_schema");
            try appendInfoProjectionLiteral(allocator, projections, "TABLE_NAME", "table_name");
            try appendInfoProjectionLiteral(allocator, projections, "CONSTRAINT_TYPE", "constraint_type");
            try appendInfoProjectionLiteral(allocator, projections, "ENFORCED", "enforced");
        },
    }
}

fn infoColumnKey(expr_in: []const u8) []const u8 {
    var expr = std.mem.trim(u8, expr_in, " \t\r\n");
    if (std.mem.lastIndexOfScalar(u8, expr, '.')) |dot| {
        expr = expr[dot + 1 ..];
    }
    return stripIdentifierQuotes(std.mem.trim(u8, expr, " \t\r\n"));
}

fn emitInfoSchemataRows(
    allocator: Allocator,
    w: *std.Io.Writer,
    catalog: *Catalog,
    projections: []const InfoProjection,
    filters: InfoFilters,
    lim: *InfoLimit,
    seq_id: *u8,
) !void {
    const db_names = try catalog.listDatabases(allocator);
    defer freeNameList(allocator, db_names);
    for (db_names) |db_name| {
        if (lim.done()) return;
        const db = catalog.database(db_name) orelse continue;
        const schema_names = try db.listSchemas(allocator);
        defer freeNameList(allocator, schema_names);
        for (schema_names) |schema_name| {
            const row: InfoRow = .{ .db_name = db_name, .schema_name = schema_name };
            if (!rowMatchesInfoFilters(row, filters)) continue;
            if (!lim.admit()) {
                if (lim.done()) return;
                continue;
            }
            try sendInfoRow(allocator, w, projections, row, seq_id);
        }
    }
}

fn emitInfoTablesRows(
    allocator: Allocator,
    w: *std.Io.Writer,
    catalog: *Catalog,
    projections: []const InfoProjection,
    filters: InfoFilters,
    lim: *InfoLimit,
    seq_id: *u8,
) !void {
    const db_names = try catalog.listDatabases(allocator);
    defer freeNameList(allocator, db_names);
    for (db_names) |db_name| {
        if (lim.done()) return;
        const db = catalog.database(db_name) orelse continue;
        const schema_names = try db.listSchemas(allocator);
        defer freeNameList(allocator, schema_names);
        for (schema_names) |schema_name| {
            if (lim.done()) return;
            const sc = db.schema(schema_name) orelse continue;
            const table_names = try sc.listTables(allocator);
            defer freeNameList(allocator, table_names);
            for (table_names) |table_name| {
                const table = schemaTable(sc, table_name) orelse continue;
                const row: InfoRow = .{
                    .db_name = db_name,
                    .schema_name = schema_name,
                    .table_name = table_name,
                    .table = table,
                };
                if (!rowMatchesInfoFilters(row, filters)) continue;
                if (!lim.admit()) {
                    if (lim.done()) return;
                    continue;
                }
                try sendInfoRow(allocator, w, projections, row, seq_id);
            }
        }
    }
}

fn emitInfoColumnsRows(
    allocator: Allocator,
    w: *std.Io.Writer,
    catalog: *Catalog,
    projections: []const InfoProjection,
    filters: InfoFilters,
    lim: *InfoLimit,
    seq_id: *u8,
) !void {
    const db_names = try catalog.listDatabases(allocator);
    defer freeNameList(allocator, db_names);
    for (db_names) |db_name| {
        if (lim.done()) return;
        const db = catalog.database(db_name) orelse continue;
        const schema_names = try db.listSchemas(allocator);
        defer freeNameList(allocator, schema_names);
        for (schema_names) |schema_name| {
            if (lim.done()) return;
            const sc = db.schema(schema_name) orelse continue;
            const table_names = try sc.listTables(allocator);
            defer freeNameList(allocator, table_names);
            for (table_names) |table_name| {
                const table = schemaTable(sc, table_name) orelse continue;
                for (table.schema.columns, 0..) |col, i| {
                    const row: InfoRow = .{
                        .db_name = db_name,
                        .schema_name = schema_name,
                        .table_name = table_name,
                        .table = table,
                        .column = col,
                        .ordinal = i + 1,
                    };
                    if (!rowMatchesInfoFilters(row, filters)) continue;
                    if (!lim.admit()) {
                        if (lim.done()) return;
                        continue;
                    }
                    try sendInfoRow(allocator, w, projections, row, seq_id);
                }
            }
        }
    }
}

fn emitInfoStatisticsRows(
    allocator: Allocator,
    w: *std.Io.Writer,
    catalog: *Catalog,
    projections: []const InfoProjection,
    filters: InfoFilters,
    lim: *InfoLimit,
    seq_id: *u8,
) !void {
    return emitInfoOrderKeyRows(allocator, w, catalog, projections, filters, lim, seq_id, false);
}

/// key_column_usage emits the same one-row-per-order-key-column shape as
/// statistics, but only for unique tables — MySQL lists PRIMARY KEY
/// constraint columns there, and a non-unique clustering key is not a
/// constraint.
fn emitInfoKeyColumnUsageRows(
    allocator: Allocator,
    w: *std.Io.Writer,
    catalog: *Catalog,
    projections: []const InfoProjection,
    filters: InfoFilters,
    lim: *InfoLimit,
    seq_id: *u8,
) !void {
    return emitInfoOrderKeyRows(allocator, w, catalog, projections, filters, lim, seq_id, true);
}

fn emitInfoOrderKeyRows(
    allocator: Allocator,
    w: *std.Io.Writer,
    catalog: *Catalog,
    projections: []const InfoProjection,
    filters: InfoFilters,
    lim: *InfoLimit,
    seq_id: *u8,
    unique_only: bool,
) !void {
    const db_names = try catalog.listDatabases(allocator);
    defer freeNameList(allocator, db_names);
    for (db_names) |db_name| {
        if (lim.done()) return;
        const db = catalog.database(db_name) orelse continue;
        const schema_names = try db.listSchemas(allocator);
        defer freeNameList(allocator, schema_names);
        for (schema_names) |schema_name| {
            if (lim.done()) return;
            const sc = db.schema(schema_name) orelse continue;
            const table_names = try sc.listTables(allocator);
            defer freeNameList(allocator, table_names);
            for (table_names) |table_name| {
                const table = schemaTable(sc, table_name) orelse continue;
                if (unique_only and !table.schema.unique) continue;
                for (table.schema.order_key, 0..) |col_name, i| {
                    const col = table.schema.column(col_name) orelse continue;
                    const row: InfoRow = .{
                        .db_name = db_name,
                        .schema_name = schema_name,
                        .table_name = table_name,
                        .table = table,
                        .column = col,
                        .ordinal = i + 1,
                        .key_seq = i + 1,
                    };
                    if (!rowMatchesInfoFilters(row, filters)) continue;
                    if (!lim.admit()) {
                        if (lim.done()) return;
                        continue;
                    }
                    try sendInfoRow(allocator, w, projections, row, seq_id);
                }
            }
        }
    }
}

/// One PRIMARY KEY constraint row per unique table.
fn emitInfoTableConstraintsRows(
    allocator: Allocator,
    w: *std.Io.Writer,
    catalog: *Catalog,
    projections: []const InfoProjection,
    filters: InfoFilters,
    lim: *InfoLimit,
    seq_id: *u8,
) !void {
    const db_names = try catalog.listDatabases(allocator);
    defer freeNameList(allocator, db_names);
    for (db_names) |db_name| {
        if (lim.done()) return;
        const db = catalog.database(db_name) orelse continue;
        const schema_names = try db.listSchemas(allocator);
        defer freeNameList(allocator, schema_names);
        for (schema_names) |schema_name| {
            if (lim.done()) return;
            const sc = db.schema(schema_name) orelse continue;
            const table_names = try sc.listTables(allocator);
            defer freeNameList(allocator, table_names);
            for (table_names) |table_name| {
                const table = schemaTable(sc, table_name) orelse continue;
                if (!table.schema.unique) continue;
                const row: InfoRow = .{
                    .db_name = db_name,
                    .schema_name = schema_name,
                    .table_name = table_name,
                    .table = table,
                };
                if (!rowMatchesInfoFilters(row, filters)) continue;
                if (!lim.admit()) {
                    if (lim.done()) return;
                    continue;
                }
                try sendInfoRow(allocator, w, projections, row, seq_id);
            }
        }
    }
}

fn sendInfoRow(
    allocator: Allocator,
    w: *std.Io.Writer,
    projections: []const InfoProjection,
    row: InfoRow,
    seq_id: *u8,
) !void {
    const cells = try allocator.alloc(?[]const u8, projections.len);
    defer allocator.free(cells);
    var owned = std.ArrayList([]u8).empty;
    defer {
        for (owned.items) |s| allocator.free(s);
        owned.deinit(allocator);
    }

    for (projections, 0..) |projection, i| {
        cells[i] = try infoCell(allocator, &owned, projection.key, row);
    }
    try result.sendTextRow(allocator, w, cells, seq_id);
}

fn infoCell(
    allocator: Allocator,
    owned: *std.ArrayList([]u8),
    key: []const u8,
    row: InfoRow,
) !?[]const u8 {
    if (keyContains(key, "catalog")) return try cellDup(allocator, owned, "def");
    // FK-only columns come before the generic table/column matches —
    // `referenced_table_name` would otherwise hit the `table_name` arm.
    // thinDB has no foreign keys, so these are always NULL.
    if (keyContains(key, "referenced_") or keyContains(key, "position_in_unique_constraint")) return null;
    if (keyContains(key, "constraint_type")) return try cellDup(allocator, owned, "PRIMARY KEY");
    if (keyContains(key, "constraint_name")) return try cellDup(allocator, owned, "PRIMARY");
    if (keyContains(key, "enforced")) return try cellDup(allocator, owned, "YES");
    if (keyContains(key, "table_schema") or keyContains(key, "schema_name") or
        keyContains(key, "index_schema") or keyContains(key, "constraint_schema"))
        return try cellSchemaName(allocator, owned, row.db_name, row.schema_name);
    if (keyContains(key, "default_character_set_name") or keyContains(key, "character_set_name"))
        return try cellDup(allocator, owned, "utf8mb4");
    if (keyContains(key, "default_collation_name") or keyContains(key, "collation_name") or keyContains(key, "table_collation"))
        return try cellDup(allocator, owned, "utf8mb4_general_ci");
    if (keyContains(key, "sql_path")) return null;

    if (keyContains(key, "table_name")) return try cellDup(allocator, owned, row.table_name orelse "");
    if (keyContains(key, "table_type")) return try cellDup(allocator, owned, "BASE TABLE");
    if (keyContains(key, "engine")) return try cellDup(allocator, owned, "thinDB");
    if (keyContains(key, "version")) return try cellDup(allocator, owned, "10");
    if (keyContains(key, "row_format")) return try cellDup(allocator, owned, "Columnar");
    if (keyContains(key, "table_rows") or keyContains(key, "cardinality")) {
        const t = row.table orelse return try cellDup(allocator, owned, "0");
        return try cellFmt(allocator, owned, "{d}", .{tableRowCount(t)});
    }
    if (keyContains(key, "avg_row_length") or
        keyContains(key, "data_length") or
        keyContains(key, "max_data_length") or
        keyContains(key, "index_length") or
        keyContains(key, "data_free") or
        keyContains(key, "checksum"))
        return try cellDup(allocator, owned, "0");
    if (keyContains(key, "auto_increment") or keyContains(key, "create_time") or keyContains(key, "update_time") or keyContains(key, "check_time"))
        return null;
    if (keyContains(key, "create_options") or keyContains(key, "table_comment"))
        return try cellDup(allocator, owned, "");

    if (keyContains(key, "column_name")) return try cellDup(allocator, owned, if (row.column) |c| c.name else "");
    if (keyContains(key, "ordinal_position") or keyContains(key, "seq_in_index"))
        return try cellFmt(allocator, owned, "{d}", .{row.ordinal});
    if (keyContains(key, "column_default")) return null;
    if (keyContains(key, "is_nullable")) return try cellDup(allocator, owned, if (row.column != null and row.column.?.nullable) "YES" else "NO");
    if (keyContains(key, "data_type")) {
        const col = row.column orelse return try cellDup(allocator, owned, "");
        const text = try allocInfoDataType(allocator, col.type);
        try owned.append(allocator, text);
        return text;
    }
    if (keyContains(key, "column_type")) {
        const col = row.column orelse return try cellDup(allocator, owned, "");
        const text = try allocMysqlColumnType(allocator, col.type);
        try owned.append(allocator, text);
        return text;
    }
    if (keyContains(key, "character_maximum_length")) return try columnCharLengthCell(allocator, owned, row.column, false);
    if (keyContains(key, "character_octet_length")) return try columnCharLengthCell(allocator, owned, row.column, true);
    if (keyContains(key, "numeric_precision")) return try numericPrecisionCell(allocator, owned, row.column);
    if (keyContains(key, "numeric_scale")) return try numericScaleCell(allocator, owned, row.column);
    if (keyContains(key, "datetime_precision")) {
        if (row.column) |col| {
            if (col.type == .datetime) return try cellDup(allocator, owned, "6");
        }
        return null;
    }
    if (keyContains(key, "column_key")) {
        const t = row.table orelse return try cellDup(allocator, owned, "");
        const col = row.column orelse return try cellDup(allocator, owned, "");
        return try cellDup(allocator, owned, columnKey(t, col.name));
    }
    if (keyContains(key, "extra") or keyContains(key, "column_comment") or keyContains(key, "generation_expression"))
        return try cellDup(allocator, owned, "");
    if (keyContains(key, "privileges")) return try cellDup(allocator, owned, "select,insert,update,references");
    if (keyContains(key, "srs_id")) return null;

    if (keyContains(key, "non_unique")) {
        const t = row.table orelse return try cellDup(allocator, owned, "1");
        return try cellDup(allocator, owned, if (t.schema.unique) "0" else "1");
    }
    if (keyContains(key, "index_name")) {
        const t = row.table orelse return try cellDup(allocator, owned, "order_key");
        return try cellDup(allocator, owned, if (t.schema.unique) "PRIMARY" else "order_key");
    }
    if (keyContains(key, "collation")) return try cellDup(allocator, owned, "A");
    if (keyContains(key, "sub_part") or keyContains(key, "packed") or keyContains(key, "expression"))
        return null;
    if (keyContains(key, "nullable")) return try cellDup(allocator, owned, if (row.column != null and row.column.?.nullable) "YES" else "");
    if (keyContains(key, "index_type")) return try cellDup(allocator, owned, "BTREE");
    if (keyContains(key, "comment") or keyContains(key, "index_comment")) return try cellDup(allocator, owned, "");
    if (keyContains(key, "is_visible")) return try cellDup(allocator, owned, "YES");

    return try cellDup(allocator, owned, "");
}

fn keyContains(key: []const u8, needle: []const u8) bool {
    return std.mem.indexOf(u8, key, needle) != null;
}

fn cellDup(allocator: Allocator, owned: *std.ArrayList([]u8), text: []const u8) ![]const u8 {
    const copy = try allocator.dupe(u8, text);
    try owned.append(allocator, copy);
    return copy;
}

fn cellFmt(
    allocator: Allocator,
    owned: *std.ArrayList([]u8),
    comptime fmt: []const u8,
    args: anytype,
) ![]const u8 {
    const text = try std.fmt.allocPrint(allocator, fmt, args);
    try owned.append(allocator, text);
    return text;
}

fn cellSchemaName(
    allocator: Allocator,
    owned: *std.ArrayList([]u8),
    db_name: []const u8,
    schema_name: []const u8,
) ![]const u8 {
    return cellFmt(allocator, owned, "{s}__{s}", .{ db_name, schema_name });
}

fn columnCharLengthCell(
    allocator: Allocator,
    owned: *std.ArrayList([]u8),
    maybe_col: ?types.Column,
    octets: bool,
) !?[]const u8 {
    const col = maybe_col orelse return null;
    const chars: ?u64 = switch (col.type) {
        .varchar => |n| n,
        .char => |n| n,
        .string => 65535,
        .uuid => 36,
        else => null,
    };
    const n = chars orelse return null;
    return try cellFmt(allocator, owned, "{d}", .{if (octets) n * 4 else n});
}

fn numericPrecisionCell(
    allocator: Allocator,
    owned: *std.ArrayList([]u8),
    maybe_col: ?types.Column,
) !?[]const u8 {
    const col = maybe_col orelse return null;
    return switch (col.type) {
        .tinyint => try cellDup(allocator, owned, "3"),
        .smallint => try cellDup(allocator, owned, "5"),
        .int => try cellDup(allocator, owned, "10"),
        .bigint => try cellDup(allocator, owned, "19"),
        .largeint => try cellDup(allocator, owned, "38"),
        .float => try cellDup(allocator, owned, "12"),
        .double => try cellDup(allocator, owned, "22"),
        .decimal64 => |spec| try cellFmt(allocator, owned, "{d}", .{spec.p}),
        .decimal128 => |spec| try cellFmt(allocator, owned, "{d}", .{spec.p}),
        else => null,
    };
}

fn numericScaleCell(
    allocator: Allocator,
    owned: *std.ArrayList([]u8),
    maybe_col: ?types.Column,
) !?[]const u8 {
    const col = maybe_col orelse return null;
    return switch (col.type) {
        .tinyint, .smallint, .int, .bigint, .largeint => try cellDup(allocator, owned, "0"),
        .decimal64 => |spec| try cellFmt(allocator, owned, "{d}", .{spec.s}),
        .decimal128 => |spec| try cellFmt(allocator, owned, "{d}", .{spec.s}),
        else => null,
    };
}

fn freeNameList(allocator: Allocator, names: [][]u8) void {
    for (names) |n| allocator.free(n);
    allocator.free(names);
}

fn sendEmptyMetadataResult(
    allocator: Allocator,
    w: *std.Io.Writer,
    kind: canned.EmptyResultKind,
    seq_id: *u8,
    client_caps: u32,
) !void {
    switch (kind) {
        .warnings => {
            const cols = [_]types.Column{
                .{ .name = "Level", .type = .string },
                .{ .name = "Code", .type = .int },
                .{ .name = "Message", .type = .string },
            };
            return sendEmptyColumns(allocator, w, cols[0..], seq_id, client_caps);
        },
        .engines => {
            const cols = [_]types.Column{
                .{ .name = "Engine", .type = .string },
                .{ .name = "Support", .type = .string },
                .{ .name = "Comment", .type = .string },
                .{ .name = "Transactions", .type = .string },
                .{ .name = "XA", .type = .string },
                .{ .name = "Savepoints", .type = .string },
            };
            return sendEmptyColumns(allocator, w, cols[0..], seq_id, client_caps);
        },
        .plugins => {
            const cols = [_]types.Column{
                .{ .name = "Name", .type = .string },
                .{ .name = "Status", .type = .string },
                .{ .name = "Type", .type = .string },
                .{ .name = "Library", .type = .string },
                .{ .name = "License", .type = .string },
            };
            return sendEmptyColumns(allocator, w, cols[0..], seq_id, client_caps);
        },
        .collations => {
            const cols = [_]types.Column{
                .{ .name = "Collation", .type = .string },
                .{ .name = "Charset", .type = .string },
                .{ .name = "Id", .type = .int },
                .{ .name = "Default", .type = .string },
                .{ .name = "Compiled", .type = .string },
                .{ .name = "Sortlen", .type = .int },
            };
            return sendEmptyColumns(allocator, w, cols[0..], seq_id, client_caps);
        },
        .character_sets => {
            const cols = [_]types.Column{
                .{ .name = "Charset", .type = .string },
                .{ .name = "Description", .type = .string },
                .{ .name = "Default collation", .type = .string },
                .{ .name = "Maxlen", .type = .int },
            };
            return sendEmptyColumns(allocator, w, cols[0..], seq_id, client_caps);
        },
        .tables => {
            const cols = [_]types.Column{
                .{ .name = "Tables_in_public", .type = .string },
            };
            return sendEmptyColumns(allocator, w, cols[0..], seq_id, client_caps);
        },
        .create_table => {
            const cols = [_]types.Column{
                .{ .name = "Table", .type = .string },
                .{ .name = "Create Table", .type = .string },
            };
            return sendEmptyColumns(allocator, w, cols[0..], seq_id, client_caps);
        },
        .full_tables => {
            const cols = [_]types.Column{
                .{ .name = "Tables_in_public", .type = .string },
                .{ .name = "Table_type", .type = .string },
            };
            return sendEmptyColumns(allocator, w, cols[0..], seq_id, client_caps);
        },
        .table_status => {
            const cols = [_]types.Column{
                .{ .name = "Name", .type = .string },
                .{ .name = "Engine", .type = .string },
                .{ .name = "Version", .type = .int },
                .{ .name = "Row_format", .type = .string },
                .{ .name = "Rows", .type = .bigint },
                .{ .name = "Avg_row_length", .type = .bigint },
                .{ .name = "Data_length", .type = .bigint },
                .{ .name = "Max_data_length", .type = .bigint },
                .{ .name = "Index_length", .type = .bigint },
                .{ .name = "Data_free", .type = .bigint },
                .{ .name = "Auto_increment", .type = .bigint, .nullable = true },
                .{ .name = "Create_time", .type = .string, .nullable = true },
                .{ .name = "Update_time", .type = .string, .nullable = true },
                .{ .name = "Check_time", .type = .string, .nullable = true },
                .{ .name = "Collation", .type = .string, .nullable = true },
                .{ .name = "Checksum", .type = .bigint, .nullable = true },
                .{ .name = "Create_options", .type = .string },
                .{ .name = "Comment", .type = .string },
            };
            return sendEmptyColumns(allocator, w, cols[0..], seq_id, client_caps);
        },
        .columns => {
            const cols = [_]types.Column{
                .{ .name = "Field", .type = .string },
                .{ .name = "Type", .type = .string },
                .{ .name = "Null", .type = .string },
                .{ .name = "Key", .type = .string },
                .{ .name = "Default", .type = .string, .nullable = true },
                .{ .name = "Extra", .type = .string },
            };
            return sendEmptyColumns(allocator, w, cols[0..], seq_id, client_caps);
        },
        .indexes => {
            const cols = [_]types.Column{
                .{ .name = "Table", .type = .string },
                .{ .name = "Non_unique", .type = .int },
                .{ .name = "Key_name", .type = .string },
                .{ .name = "Seq_in_index", .type = .int },
                .{ .name = "Column_name", .type = .string },
                .{ .name = "Collation", .type = .string, .nullable = true },
                .{ .name = "Cardinality", .type = .bigint, .nullable = true },
                .{ .name = "Sub_part", .type = .bigint, .nullable = true },
                .{ .name = "Packed", .type = .string, .nullable = true },
                .{ .name = "Null", .type = .string },
                .{ .name = "Index_type", .type = .string },
                .{ .name = "Comment", .type = .string },
                .{ .name = "Index_comment", .type = .string },
                .{ .name = "Visible", .type = .string },
                .{ .name = "Expression", .type = .string, .nullable = true },
            };
            return sendEmptyColumns(allocator, w, cols[0..], seq_id, client_caps);
        },
        .grants => {
            const cols = [_]types.Column{.{ .name = "Grants for thindb@localhost", .type = .string }};
            return sendEmptyColumns(allocator, w, cols[0..], seq_id, client_caps);
        },
        .processlist => {
            const cols = [_]types.Column{
                .{ .name = "Id", .type = .bigint },
                .{ .name = "User", .type = .string },
                .{ .name = "Host", .type = .string },
                .{ .name = "db", .type = .string, .nullable = true },
                .{ .name = "Command", .type = .string },
                .{ .name = "Time", .type = .int },
                .{ .name = "State", .type = .string, .nullable = true },
                .{ .name = "Info", .type = .string, .nullable = true },
            };
            return sendEmptyColumns(allocator, w, cols[0..], seq_id, client_caps);
        },
        .generic_status => {
            const cols = [_]types.Column{.{ .name = "Name", .type = .string }};
            return sendEmptyColumns(allocator, w, cols[0..], seq_id, client_caps);
        },
    }
}

fn sendEmptyColumns(
    allocator: Allocator,
    w: *std.Io.Writer,
    cols: []const types.Column,
    seq_id: *u8,
    client_caps: u32,
) !void {
    try result.sendResultHeader(allocator, w, cols, "", "", seq_id);
    try result.sendColumnDefBoundary(allocator, w, seq_id, client_caps);
    try result.sendResultTerminator(allocator, w, seq_id, client_caps);
}

const TxnVerb = enum { begin, commit, rollback };

fn matchTxnVerb(sql_text: []const u8) ?TxnVerb {
    var s = std.mem.trim(u8, sql_text, " \t\r\n");
    while (s.len > 0 and s[s.len - 1] == ';') s = std.mem.trim(u8, s[0 .. s.len - 1], " \t\r\n");
    if (std.ascii.eqlIgnoreCase(s, "begin")) return .begin;
    if (std.ascii.eqlIgnoreCase(s, "begin work")) return .begin;
    if (std.ascii.eqlIgnoreCase(s, "start transaction")) return .begin;
    if (std.ascii.eqlIgnoreCase(s, "commit")) return .commit;
    if (std.ascii.eqlIgnoreCase(s, "commit work")) return .commit;
    if (std.ascii.eqlIgnoreCase(s, "rollback")) return .rollback;
    if (std.ascii.eqlIgnoreCase(s, "rollback work")) return .rollback;
    return null;
}

fn isShowDatabases(sql_text: []const u8) bool {
    var s = std.mem.trim(u8, sql_text, " \t\r\n");
    while (s.len > 0 and s[s.len - 1] == ';') s = std.mem.trim(u8, s[0 .. s.len - 1], " \t\r\n");
    return std.ascii.eqlIgnoreCase(s, "show databases") or std.ascii.eqlIgnoreCase(s, "show schemas");
}

fn sendFlattenedDatabases(
    allocator: Allocator,
    w: *std.Io.Writer,
    catalog: *Catalog,
    seq_id: *u8,
    client_caps: u32,
) !void {
    const db_names = try catalog.listDatabases(allocator);
    defer {
        for (db_names) |n| allocator.free(n);
        allocator.free(db_names);
    }

    var flat: std.ArrayList([]u8) = .empty;
    defer {
        for (flat.items) |s| allocator.free(s);
        flat.deinit(allocator);
    }

    for (db_names) |db_name| {
        const db = catalog.database(db_name) orelse continue;
        const schema_names = try db.listSchemas(allocator);
        defer {
            for (schema_names) |n| allocator.free(n);
            allocator.free(schema_names);
        }
        for (schema_names) |sc_name| {
            const joined = try std.fmt.allocPrint(allocator, "{s}__{s}", .{ db_name, sc_name });
            try flat.append(allocator, joined);
        }
    }

    var as_const: std.ArrayList([]const u8) = .empty;
    defer as_const.deinit(allocator);
    try as_const.ensureTotalCapacity(allocator, flat.items.len);
    for (flat.items) |s| as_const.appendAssumeCapacity(s);

    try result.sendSingleColumnRows(allocator, w, "Database", as_const.items, seq_id, client_caps);
}

fn runEngineQuery(
    allocator: Allocator,
    w: *std.Io.Writer,
    catalog: *Catalog,
    session: *SessionState,
    payload: []const u8,
    seq_id: *u8,
    profiler: *MysqlProfiler,
) !void {
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();

    // XA transaction control (Flink exactly-once): `XA START|END|PREPARE|COMMIT|
    // ROLLBACK|RECOVER ...`. Not thinDB SQL — intercept before the parser.
    {
        const t = std.mem.trim(u8, payload, " \t\r\n;");
        if (t.len > 2 and std.ascii.eqlIgnoreCase(t[0..2], "XA") and (t[2] == ' ' or t[2] == '\t')) {
            try handleXaCommand(allocator, w, catalog, session, t, seq_id.*);
            return;
        }
    }

    const parse_start = profiler.start();
    const op = sql.parseWithContext(arena.allocator(), payload, .mysql, &catalog.udfs, .{ .registry = &catalog.sql_fns, .db = session.current_db, .views = &catalog.views }) catch |err| {
        profiler.recordSince(.query_parse, parse_start);
        const mapped = errors.mapInternal(err, "Parse error");
        try handshake.sendErrPacket(allocator, w, seq_id.*, mapped.code, mapped.sqlstate, @errorName(err));
        return;
    };
    profiler.recordSince(.query_parse, parse_start);

    if (op.* == .batch) {
        if ((session.client_caps & handshake.CLIENT_MULTI_STATEMENTS) == 0) {
            try handshake.sendErrPacket(
                allocator,
                w,
                seq_id.*,
                1064,
                "42000".*,
                "Multi-statement requires CLIENT_MULTI_STATEMENTS capability",
            );
            return;
        }
        const stmts = op.batch.statements;
        var i: usize = 0;
        while (i < stmts.len) {
            // Coalesce a run of same-table DELETEs into one batched keyed
            // delete (#146) — the JDBC sink's rewriteBatchedStatements
            // DELETE chain is exactly this shape. One segment sweep + one
            // tombstone write per segment instead of one per statement;
            // each statement still gets its own OK with real affected_rows.
            const run = deleteRunLen(stmts[i..]);
            if (run >= 2) {
                if (try runKeyedDeleteBatch(allocator, w, catalog, session, stmts[i .. i + run], seq_id, i + run == stmts.len, profiler)) |ok| {
                    if (!ok) return;
                    i += run;
                    continue;
                }
            }
            const is_last = i + 1 == stmts.len;
            const base: u16 = session.transactionStatus();
            const extra: u16 = if (is_last) base else base | handshake.SERVER_MORE_RESULTS_EXISTS;
            const ok = try runSingleStatement(allocator, w, catalog, session, stmts[i], seq_id, extra, profiler);
            // An ERR terminates a multi-statement response; the client stops
            // reading there, so any further packets would desync the
            // connection permanently (Connector/J then NPEs on every
            // subsequent command).
            if (!ok) return;
            i += 1;
        }
        return;
    }

    _ = try runSingleStatement(allocator, w, catalog, session, op, seq_id, session.transactionStatus(), profiler);
}

fn optStrEql(a: ?[]const u8, b: ?[]const u8) bool {
    if (a == null and b == null) return true;
    if (a == null or b == null) return false;
    return std.mem.eql(u8, a.?, b.?);
}

fn tableRefEql(a: ir.TableRef, b: ir.TableRef) bool {
    return optStrEql(a.database, b.database) and
        optStrEql(a.schema, b.schema) and
        std.mem.eql(u8, a.name, b.name);
}

/// Length of the leading run of DELETE statements against one table.
fn deleteRunLen(stmts: []const *ir.Op) usize {
    if (stmts.len == 0 or stmts[0].* != .delete_op) return 0;
    const first = stmts[0].delete_op.table;
    var n: usize = 1;
    while (n < stmts.len and stmts[n].* == .delete_op and tableRefEql(stmts[n].delete_op.table, first)) n += 1;
    return n;
}

/// Execute a run of same-table DELETE statements as one batched keyed
/// delete, then emit one OK per statement carrying its affected_rows.
/// Returns null when the batch isn't eligible OR anything errors — the
/// caller then falls back to per-statement execution, which is always
/// safe because keyed deletes are idempotent (re-tombstoning a row and
/// re-filtering the memtable are no-ops) and reports precise per-statement
/// errors. Returns false only if an ERR packet was already sent.
fn runKeyedDeleteBatch(
    allocator: Allocator,
    w: *std.Io.Writer,
    catalog: *Catalog,
    session: *SessionState,
    stmts: []const *ir.Op,
    seq_id: *u8,
    last_is_final: bool,
    profiler: *MysqlProfiler,
) !?bool {
    const main_db = catalog.database(session.current_db) orelse return null;
    const cat = local.catalogFor(main_db) orelse return null;
    const t = local.resolveTable(cat, session.asSession(), stmts[0].delete_op.table) catch return null;

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const aa = arena.allocator();
    const preds = try aa.alloc(?PredicateExpr, stmts.len);
    for (stmts, preds) |s, *p| p.* = s.delete_op.predicate;
    const counts = try aa.alloc(usize, stmts.len);

    var qlease = core_scheduler.global().acquire();
    defer qlease.release();

    const exec_start = profiler.start();
    const maybe_total = t.deleteKeyedBatch(preds, counts) catch null;
    const total = maybe_total orelse {
        profiler.recordSince(.query_execute, exec_start);
        return null;
    };
    profiler.recordSince(.query_execute, exec_start);
    profiler.addRowsAffected(total);
    for (stmts) |s| profiler.recordSqlKind(classifySqlKind(s.*));

    for (counts, 0..) |c, j| {
        const is_last = last_is_final and j + 1 == stmts.len;
        const base: u16 = session.transactionStatus();
        const extra: u16 = if (is_last) base else base | handshake.SERVER_MORE_RESULTS_EXISTS;
        try handshake.sendOkPacketStatus(allocator, w, seq_id.*, c, 0, extra);
        seq_id.* +%= 1;
    }
    return true;
}

fn isXaStageable(op: ir.Op) bool {
    return switch (op) {
        .insert, .insert_select, .delete_op, .update_op => true,
        else => false,
    };
}

// Map internal XA errors to MySQL's XA error codes so Connector/J surfaces the
// right XAException.errorCode and Flink's committer reacts correctly (e.g. it
// treats XAER_NOTA on recovery as already-resolved).
fn xaErr(allocator: Allocator, w: *std.Io.Writer, seq_id: u8, e: anyerror) !void {
    switch (e) {
        error.XaBranchExists => // XAER_DUPID
        try handshake.sendErrPacket(allocator, w, seq_id, 1440, "XAE08".*, "XAER_DUPID: XID already exists"),
        error.XaBranchUnknown => // XAER_NOTA
        try handshake.sendErrPacket(allocator, w, seq_id, 1397, "XAE04".*, "XAER_NOTA: unknown XID"),
        error.XaProtocol => // XAER_RMFAIL: wrong state for this command
        try handshake.sendErrPacket(allocator, w, seq_id, 1399, "XAE07".*, "XAER_RMFAIL: command invalid in the current XA state"),
        else => // XAER_INVAL
        try handshake.sendErrPacket(allocator, w, seq_id, 1398, "XAE05".*, @errorName(e)),
    }
}

/// XA transaction control for the Flink exactly-once JDBC sink. `text` is the
/// trimmed statement (leading `XA `, no trailing `;`). Writes staged by the DML
/// hook in `runSingleStatement` are replayed here at COMMIT. Idempotent: a
/// COMMIT/ROLLBACK for an unknown xid is a no-op success (Flink retries).
fn handleXaCommand(
    allocator: Allocator,
    w: *std.Io.Writer,
    catalog: *Catalog,
    session: *SessionState,
    text: []const u8,
    seq_id: u8,
) !void {
    const rest = std.mem.trim(u8, text[2..], " \t\r\n");
    const vend = blk: {
        for (rest, 0..) |c, i| if (c == ' ' or c == '\t') break :blk i;
        break :blk rest.len;
    };
    const verb = rest[0..vend];
    var xid = std.mem.trim(u8, rest[vend..], " \t\r\n");
    const eq = std.ascii.eqlIgnoreCase;

    if (eq(verb, "recover")) {
        // MySQL XA RECOVER result: formatID, gtrid_length, bqual_length, data
        // (= gtrid ++ bqual). Connector/J's recover() reconstructs each Xid from
        // these, so a crashed job can re-commit its prepared branches.
        var arena = std.heap.ArenaAllocator.init(allocator);
        defer arena.deinit();
        const aa = arena.allocator();
        const xids = catalog.xa.preparedXids(aa) catch &[_][]const u8{};
        var sid = seq_id;
        const cols = [_]types.Column{
            .{ .name = "formatID", .type = .int },
            .{ .name = "gtrid_length", .type = .int },
            .{ .name = "bqual_length", .type = .int },
            .{ .name = "data", .type = .string },
        };
        try result.sendResultHeader(allocator, w, &cols, "", "", &sid);
        try result.sendColumnDefBoundary(allocator, w, &sid, session.client_caps);
        for (xids) |x| {
            const p = xa_mod.parseXid(aa, x) catch continue;
            const cells = [_]?[]const u8{
                try std.fmt.allocPrint(aa, "{d}", .{p.format_id}),
                try std.fmt.allocPrint(aa, "{d}", .{p.gtrid.len}),
                try std.fmt.allocPrint(aa, "{d}", .{p.bqual.len}),
                try std.mem.concat(aa, u8, &.{ p.gtrid, p.bqual }),
            };
            try result.sendTextRow(allocator, w, &cells, &sid);
        }
        try result.sendResultTerminator(allocator, w, &sid, session.client_caps);
        return;
    }
    if (eq(verb, "start") or eq(verb, "begin")) {
        catalog.xa.begin(xid, session.current_db) catch |e| return xaErr(allocator, w, seq_id, e);
        if (session.xa_active) |old| allocator.free(old);
        session.xa_active = try allocator.dupe(u8, xid);
        return handshake.sendOkPacket(allocator, w, seq_id, 0, 0);
    }
    if (eq(verb, "end")) {
        catalog.xa.end(xid) catch |e| return xaErr(allocator, w, seq_id, e);
        if (session.xa_active) |a| {
            allocator.free(a);
            session.xa_active = null;
        }
        return handshake.sendOkPacket(allocator, w, seq_id, 0, 0);
    }
    if (eq(verb, "prepare")) {
        catalog.xa.prepare(xid) catch |e| return xaErr(allocator, w, seq_id, e);
        return handshake.sendOkPacket(allocator, w, seq_id, 0, 0);
    }
    if (eq(verb, "rollback")) {
        catalog.xa.rollback(xid);
        if (session.xa_active) |a| if (std.mem.eql(u8, a, xid)) {
            allocator.free(a);
            session.xa_active = null;
        };
        return handshake.sendOkPacket(allocator, w, seq_id, 0, 0);
    }
    if (eq(verb, "commit")) {
        if (xid.len >= 9 and eq(xid[xid.len - 9 ..], "one phase"))
            xid = std.mem.trim(u8, xid[0 .. xid.len - 9], " \t\r\n,");
        if (catalog.xa.takeForCommit(xid)) |branch| {
            defer catalog.xa.finishCommit(branch);
            const dbname = if (branch.db.len > 0) branch.db else session.current_db;
            if (catalog.database(dbname)) |main_db| {
                var carena = std.heap.ArenaAllocator.init(allocator);
                defer carena.deinit();
                const ca = carena.allocator();
                for (branch.stmts.items) |enc| {
                    const dop = ir.decode(ca, enc) catch continue;
                    var compiled = local.compileWithSession(ca, main_db, session.asSession(), &dop) catch continue;
                    defer compiled.deinit();
                    while (compiled.next() catch null) |_| {}
                }
            }
        }
        if (session.xa_active) |a| if (std.mem.eql(u8, a, xid)) {
            allocator.free(a);
            session.xa_active = null;
        };
        return handshake.sendOkPacket(allocator, w, seq_id, 0, 0);
    }
    try handshake.sendErrPacket(allocator, w, seq_id, 1064, "42000".*, "Unknown XA command");
}

/// Compile + emit ONE statement's packets. `extra_status` is OR'd into
/// the terminator's status_flags — multi-statement responses pass
/// SERVER_MORE_RESULTS_EXISTS for every non-final statement so the
/// client keeps reading the chain. Returns false when an ERR packet was
/// sent — the caller must then abort any remaining chained statements.
fn runSingleStatement(
    allocator: Allocator,
    w: *std.Io.Writer,
    catalog: *Catalog,
    session: *SessionState,
    op: *const ir.Op,
    seq_id: *u8,
    extra_status: u16,
    profiler: *MysqlProfiler,
) !bool {
    // Serial-stage core lease: pin this connection thread to one core for the
    // whole statement (compile + execute + drain). Parallel-scan workers lease
    // ADDITIONAL cores per stage and release between them; this thread reuses
    // its lease as the inline worker (the scheduler's no-hold-and-wait guard).
    // For a single-threaded query this is the only lease — it still gets a
    // reserved core. Blocking here when the machine is saturated is the
    // intended admission control. Best-effort: a no-op when pinning is off/unsupported.
    var qlease = core_scheduler.global().acquire();
    defer qlease.release();

    profiler.recordSqlKind(classifySqlKind(op.*));
    const main_db = catalog.database(session.current_db) orelse {
        try handshake.sendErrPacket(allocator, w, seq_id.*, 1049, "42000".*, "Unknown database");
        return false;
    };

    if (needsTempNamespace(op.*)) {
        _ = session.ensureTempNamespace() catch |err| {
            const mapped = errors.mapInternal(err, null);
            try handshake.sendErrPacket(allocator, w, seq_id.*, mapped.code, mapped.sqlstate, mapped.message);
            return false;
        };
    }

    // Under --profile-ops, route the handler-thread's query allocations through
    // a counting wrapper (operator construction + teardown; parallel-scan
    // workers allocate from the table allocator, not this one) and reset the
    // sub-phase timers so construction timings survive the execute-time reset.
    var mem_stats = counting_allocator.Stats{};
    var counter = counting_allocator.CountingAllocator.init(allocator, &mem_stats);
    const qalloc = if (oprof.enabled) counter.allocator() else allocator;
    if (oprof.enabled) oprof.resetPhases();

    // Inside an XA branch, stage writes instead of executing them; XA COMMIT
    // replays the staged statements. Only DML is staged — Flink sends only
    // writes between XA START and XA END.
    if (session.xa_active) |xid| {
        if (isXaStageable(op.*)) {
            var buf: std.ArrayList(u8) = .empty;
            defer buf.deinit(allocator);
            if (ir.encode(allocator, &buf, op.*)) |_| {
                catalog.xa.stage(xid, buf.items) catch |err| {
                    const mapped = errors.mapInternal(err, "XA stage failed");
                    try handshake.sendErrPacket(allocator, w, seq_id.*, mapped.code, mapped.sqlstate, mapped.message);
                    return false;
                };
                // Must carry extra_status + advance seq: staged DML inside a
                // multi-statement chain (Connector/J DELETE batches) otherwise
                // ends the client's read after the first OK and the remaining
                // responses desync the connection.
                try handshake.sendOkPacketStatus(allocator, w, seq_id.*, 0, 0, extra_status);
                seq_id.* +%= 1;
                return true;
            } else |_| {}
        }
    }

    const compile_start = profiler.start();
    var compiled = local.compileWithSession(qalloc, main_db, session.asSession(), op) catch |err| {
        profiler.recordSince(.query_compile, compile_start);
        const mapped = errors.mapInternal(err, null);
        var msg: []const u8 = mapped.message;
        var msg_owned: ?[]u8 = null;
        defer if (msg_owned) |m| allocator.free(m);
        if (err == error.ColumnNotFound) {
            if (findUnknownColumn(catalog, session, op)) |col| {
                msg_owned = std.fmt.allocPrint(allocator, "Unknown column '{s}' in 'field list'", .{col}) catch null;
                if (msg_owned) |m| msg = m;
            }
        }
        try handshake.sendErrPacket(allocator, w, seq_id.*, mapped.code, mapped.sqlstate, msg);
        return false;
    };
    profiler.recordSince(.query_compile, compile_start);
    // Registered BEFORE the deinit defer so it runs AFTER teardown (LIFO) — the
    // sub-phase timers then include `pscan.deinit.*`, and the memory totals
    // reflect the full construct→drain→teardown lifecycle.
    defer if (oprof.enabled) {
        oprof.dumpPhases("handler");
        counting_allocator.dump("handler-alloc", mem_stats);
    };
    defer compiled.deinit();

    // Clear any stale cancel flag from a previous statement on this
    // connection, then wire the connection's cancel flag into the
    // compiled query. CompiledQuery.next() polls the flag at each
    // batch boundary; a peer KILL sets it via the shared registry.
    if (session.conn_state) |state| {
        state.clearCancel();
        compiled.cancel_flag = &state.cancel_flag;
    }

    if (isSideEffectOp(op.*)) {
        const exec_start = profiler.start();
        _ = compiled.next() catch |err| {
            profiler.recordSince(.query_execute, exec_start);
            const mapped = errors.mapInternal(err, null);
            try handshake.sendErrPacket(allocator, w, seq_id.*, mapped.code, mapped.sqlstate, mapped.message);
            return false;
        };
        profiler.recordSince(.query_execute, exec_start);
        const new_session = compiled.sessionValue();
        try session.replace(new_session.current_db, new_session.current_schema);
        session.captureVars(new_session);
        const affected_rows = compiled.affectedRows();
        profiler.addRowsAffected(affected_rows);
        const write_start = profiler.start();
        try handshake.sendOkPacketStatus(
            allocator,
            w,
            seq_id.*,
            affected_rows,
            0,
            extra_status,
        );
        profiler.recordSince(.query_write, write_start);
        seq_id.* +%= 1;
        return true;
    }

    oprof.reset();
    const write_start = profiler.start();
    try result.sendQueryResultStatus(
        allocator,
        w,
        &compiled,
        session.current_db,
        "",
        seq_id,
        session.client_caps,
        extra_status,
    );
    profiler.recordSince(.query_execute_write, write_start);
    oprof.dumpSelf("query");
    oprof.dump("query");
    if (oprof.enabled) {
        const cs = rg_cache.globalStats();
        const p = prev_cache_stats;
        const dh = cs.hits - p.hits;
        const dm = cs.misses - p.misses;
        const de = cs.evictions - p.evictions;
        const dmb = cs.miss_bytes - p.miss_bytes;
        const total = dh + dm;
        const hit_pct: f64 = if (total == 0) 0 else @as(f64, @floatFromInt(dh)) * 100.0 / @as(f64, @floatFromInt(total));
        std.debug.print("[rgcache] query: hits={d} misses={d} hit%={d:.1} evictions={d} miss_decompressed_MB={d:.1} cache_live_GB={d:.2}\n", .{ dh, dm, hit_pct, de, @as(f64, @floatFromInt(dmb)) / (1024.0 * 1024.0), @as(f64, @floatFromInt(cs.cache_bytes)) / (1024.0 * 1024.0 * 1024.0) });
        prev_cache_stats = cs;
        const fd_ticks = scan_mod.g_fsst_digest_ticks.swap(0, .monotonic);
        if (fd_ticks > 0) {
            const fd_rows = scan_mod.g_fsst_digest_rows.swap(0, .monotonic);
            const fd_bytes = scan_mod.g_fsst_digest_bytes.swap(0, .monotonic);
            const ms = oprof.ticksToMs(@intCast(fd_ticks));
            std.debug.print("[fsst-digest] cpu_ms={d:.1} rows={d} decoded_MB={d:.1} ({d:.2} GB/s/core-equiv)\n", .{
                ms,
                fd_rows,
                @as(f64, @floatFromInt(fd_bytes)) / (1024.0 * 1024.0),
                if (ms > 0) @as(f64, @floatFromInt(fd_bytes)) / (ms * 1_000_000.0) else 0,
            });
        }
    }

    const new_session = compiled.sessionValue();
    try session.replace(new_session.current_db, new_session.current_schema);
    session.captureVars(new_session);
    return true;
}

// DML must answer with a protocol OK carrying affected_rows (MySQL never
// returns a resultset for DELETE/UPDATE) — Connector/J's batch reader NPEs
// on a resultset-shaped response in multi-statement mode.
fn isSideEffectOp(op: ir.Op) bool {
    return switch (op) {
        .ddl, .insert, .insert_select, .set_var, .delete_op, .update_op => true,
        else => false,
    };
}

/// True when an op (or one of its sub-statements in a batch) is a
/// `CREATE TEMP TABLE` — the wire layer pre-allocates the session's
/// temp namespace before compiling so the compile path can rely on
/// it being non-null.
fn needsTempNamespace(op: ir.Op) bool {
    return switch (op) {
        .ddl => |d| switch (d) {
            .create_table => |ct| ct.is_temp,
            else => false,
        },
        else => false,
    };
}

// ---------------------------------------------------------------------------
// COM_STMT_* handlers
// ---------------------------------------------------------------------------

/// Prepare a statement: tokenize to count `?`, register a stmt id, and
/// best-effort compile a `?`-substituted version against the active
/// session to derive output-column metadata. Compile failures (e.g.
/// `WHERE name = ?` against a VARCHAR column when the dummy literal is
/// `0`) are swallowed — we still register the stmt and emit
/// num_columns=0; the real schema rides the EXECUTE response.
fn handleStmtPrepare(
    allocator: Allocator,
    w: *std.Io.Writer,
    catalog: *Catalog,
    session: *SessionState,
    payload: []const u8,
    profiler: *MysqlProfiler,
) !void {
    var seq_id: u8 = 1;
    const caps = session.client_caps;

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();

    const count_start = profiler.start();
    const num_params = prepared.countPlaceholders(arena.allocator(), payload) catch 0;
    profiler.recordSince(.stmt_prepare_count, count_start);

    const stmt_id = session.next_stmt_id;
    session.next_stmt_id +%= 1;
    const stmt = try prepared.createPreparedStmt(allocator, stmt_id, payload, num_params);

    // Best-effort schema inference. We substitute `?` → `0` so the
    // existing parser+compiler can produce an output schema; failures
    // here fall back to num_columns=0. Side-effect ops (DDL, INSERT)
    // are skipped before compile so the prepare-time pass has no
    // observable effect on the catalog or data.
    if (num_params <= max_prepare_param_defs_to_emit) {
        const infer_start = profiler.start();
        blk: {
            const dummy_sql = prepared.renderDummySubstitution(arena.allocator(), payload, num_params) catch break :blk;
            const dummy_op = sql.parseWithContext(arena.allocator(), dummy_sql, .mysql, &catalog.udfs, .{ .registry = &catalog.sql_fns, .db = session.current_db, .views = &catalog.views }) catch break :blk;
            if (dummy_op.* == .batch) break :blk;
            if (isSideEffectOp(dummy_op.*)) break :blk;
            const main_db = catalog.database(session.current_db) orelse break :blk;
            var compiled = local.compileWithSession(arena.allocator(), main_db, session.asSession(), dummy_op) catch break :blk;
            defer compiled.deinit();
            const schema = compiled.outputSchema();

            const names = allocator.alloc([]u8, schema.len) catch break :blk;
            var names_inited: usize = 0;
            errdefer {
                for (names[0..names_inited]) |n| allocator.free(n);
                allocator.free(names);
            }
            for (schema) |col| {
                names[names_inited] = allocator.dupe(u8, col.name) catch break :blk;
                names_inited += 1;
            }
            const cols = allocator.alloc(types.Column, schema.len) catch break :blk;
            for (schema, 0..) |col, i| {
                cols[i] = .{ .name = names[i], .type = col.type, .nullable = col.nullable };
            }
            stmt.column_schema = cols;
            stmt.column_names = names;
            stmt.num_columns = @intCast(schema.len);
        }
        profiler.recordSince(.stmt_prepare_infer, infer_start);
    }

    try session.prepared_statements.put(allocator, stmt_id, stmt);

    const write_start = profiler.start();
    try prepared.sendPrepareOkHeader(allocator, w, &seq_id, stmt_id, stmt.num_columns, num_params);

    // Param column-defs. Real MySQL emits one per `?`, but mysql2 and
    // other clients accept an EOF terminator before all definitions are
    // sent. Large multi-row INSERT prepares can otherwise spend seconds
    // writing metadata that clients do not use when binding parameters.
    const emitted_all_param_defs = num_params <= max_prepare_param_defs_to_emit;
    if (emitted_all_param_defs) {
        var pi: u16 = 0;
        while (pi < num_params) : (pi += 1) {
            try prepared.sendParamColumnDef(allocator, w, &seq_id);
        }
    }
    if (num_params > 0) {
        if ((caps & handshake.CLIENT_DEPRECATE_EOF) == 0) {
            try handshake.sendLegacyEofPacket(allocator, w, seq_id);
            seq_id +%= 1;
        } else if (!emitted_all_param_defs) {
            try handshake.sendEofOkPacket(allocator, w, seq_id);
            seq_id +%= 1;
        }
    }

    // Column column-defs.
    if (stmt.num_columns > 0) {
        var coldef: std.ArrayList(u8) = .empty;
        defer coldef.deinit(allocator);
        const schema = stmt.column_schema.?;
        for (schema) |col| {
            coldef.clearRetainingCapacity();
            try result.appendColumnDef(allocator, &coldef, session.current_db, "", col);
            try packet.writePacket(w, seq_id, coldef.items);
            seq_id +%= 1;
        }
        if ((caps & handshake.CLIENT_DEPRECATE_EOF) == 0) {
            try handshake.sendLegacyEofPacket(allocator, w, seq_id);
            seq_id +%= 1;
        }
    }
    profiler.recordSince(.stmt_prepare_write, write_start);
}

/// Execute a previously-prepared statement: bind parameters from the
/// binary wire format, substitute them as SQL literals into the saved
/// SQL, compile, and stream results back as binary rows.
fn handleStmtExecute(
    allocator: Allocator,
    w: *std.Io.Writer,
    catalog: *Catalog,
    session: *SessionState,
    payload: []const u8,
    profiler: *MysqlProfiler,
) !void {
    var seq_id: u8 = 1;
    const caps = session.client_caps;

    if (payload.len < 9) {
        try handshake.sendErrPacket(allocator, w, seq_id, 1064, "42000".*, "malformed COM_STMT_EXECUTE");
        return;
    }
    const stmt_id = std.mem.readInt(u32, payload[0..4], .little);

    const stmt = session.prepared_statements.get(stmt_id) orelse {
        try handshake.sendErrPacket(allocator, w, seq_id, 1243, "HY000".*, "Unknown prepared statement handler");
        return;
    };

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const arena_alloc = arena.allocator();

    const after_header: usize = 9;
    const decode_start = profiler.start();
    const params = prepared.decodeExecuteParams(arena_alloc, stmt, payload, after_header) catch |err| {
        profiler.recordSince(.stmt_execute_decode, decode_start);
        const msg = switch (err) {
            error.NoBoundParamTypes => "missing parameter types on first execute",
            error.LongDataAndValueBoth => "long-data and value both bound for same param",
            error.UnsupportedParamType => "unsupported parameter type",
            else => "malformed COM_STMT_EXECUTE",
        };
        try handshake.sendErrPacket(allocator, w, seq_id, 1064, "42000".*, msg);
        return;
    };
    profiler.recordSince(.stmt_execute_decode, decode_start);

    const substitute_start = profiler.start();
    const substituted = prepared.substituteSql(arena_alloc, stmt.sql, params) catch |err| {
        profiler.recordSince(.stmt_execute_substitute, substitute_start);
        const msg = switch (err) {
            error.MissingParameter => "missing parameter for placeholder",
            else => "parameter substitution failure",
        };
        try handshake.sendErrPacket(allocator, w, seq_id, 1064, "42000".*, msg);
        return;
    };
    profiler.recordSince(.stmt_execute_substitute, substitute_start);

    // Clear long-data buffers after consuming them — MySQL semantics:
    // long-data accumulates across SEND_LONG_DATA calls but is consumed
    // by the next EXECUTE. Subsequent EXECUTEs start fresh.
    for (stmt.long_data) |*ld| {
        if (ld.*) |*buf| {
            buf.deinit(stmt.allocator);
            ld.* = null;
        }
    }

    const parse_start = profiler.start();
    const op = sql.parseWithContext(arena_alloc, substituted, .mysql, &catalog.udfs, .{ .registry = &catalog.sql_fns, .db = session.current_db, .views = &catalog.views }) catch |err| {
        profiler.recordSince(.stmt_execute_parse, parse_start);
        const mapped = errors.mapInternal(err, null);
        try handshake.sendErrPacket(allocator, w, seq_id, mapped.code, mapped.sqlstate, mapped.message);
        return;
    };
    profiler.recordSince(.stmt_execute_parse, parse_start);
    profiler.recordSqlKind(classifySqlKind(op.*));

    if (op.* == .batch) {
        try handshake.sendErrPacket(allocator, w, seq_id, 1064, "42000".*, "Multi-statement not supported in prepared mode");
        return;
    }

    const main_db = catalog.database(session.current_db) orelse {
        try handshake.sendErrPacket(allocator, w, seq_id, 1049, "42000".*, "Unknown database");
        return;
    };

    if (needsTempNamespace(op.*)) {
        _ = session.ensureTempNamespace() catch |err| {
            const mapped = errors.mapInternal(err, null);
            try handshake.sendErrPacket(allocator, w, seq_id, mapped.code, mapped.sqlstate, mapped.message);
            return;
        };
    }

    // Prepared DML inside an XA branch (Flink's exactly-once sink executes its
    // batch as prepared statements): stage instead of executing.
    if (session.xa_active) |xid| {
        if (isXaStageable(op.*)) {
            var buf: std.ArrayList(u8) = .empty;
            defer buf.deinit(allocator);
            if (ir.encode(allocator, &buf, op.*)) |_| {
                catalog.xa.stage(xid, buf.items) catch |err| {
                    const mapped = errors.mapInternal(err, "XA stage failed");
                    try handshake.sendErrPacket(allocator, w, seq_id, mapped.code, mapped.sqlstate, mapped.message);
                    return;
                };
                try handshake.sendOkPacket(allocator, w, seq_id, 0, 0);
                return;
            } else |_| {}
        }
    }

    const compile_start = profiler.start();
    var compiled = local.compileWithSession(allocator, main_db, session.asSession(), op) catch |err| {
        profiler.recordSince(.stmt_execute_compile, compile_start);
        const mapped = errors.mapInternal(err, null);
        try handshake.sendErrPacket(allocator, w, seq_id, mapped.code, mapped.sqlstate, mapped.message);
        return;
    };
    profiler.recordSince(.stmt_execute_compile, compile_start);
    defer compiled.deinit();

    if (session.conn_state) |state| {
        state.clearCancel();
        compiled.cancel_flag = &state.cancel_flag;
    }

    if (isSideEffectOp(op.*)) {
        const exec_start = profiler.start();
        _ = compiled.next() catch |err| {
            profiler.recordSince(.stmt_execute_engine, exec_start);
            const mapped = errors.mapInternal(err, null);
            try handshake.sendErrPacket(allocator, w, seq_id, mapped.code, mapped.sqlstate, mapped.message);
            return;
        };
        profiler.recordSince(.stmt_execute_engine, exec_start);
        const new_session = compiled.sessionValue();
        try session.replace(new_session.current_db, new_session.current_schema);
        session.captureVars(new_session);
        const affected_rows = compiled.affectedRows();
        profiler.addRowsAffected(affected_rows);
        const write_start = profiler.start();
        try handshake.sendOkPacketStatus(
            allocator,
            w,
            seq_id,
            affected_rows,
            0,
            session.transactionStatus(),
        );
        profiler.recordSince(.stmt_execute_write, write_start);
        return;
    }

    // Binary result set.
    const schema = compiled.outputSchema();

    const header_write_start = profiler.start();
    var col_count_buf: std.ArrayList(u8) = .empty;
    defer col_count_buf.deinit(allocator);
    try packet.appendLenEncInt(allocator, &col_count_buf, @intCast(schema.len));
    try packet.writePacket(w, seq_id, col_count_buf.items);
    seq_id +%= 1;

    var coldef: std.ArrayList(u8) = .empty;
    defer coldef.deinit(allocator);
    for (schema) |col| {
        coldef.clearRetainingCapacity();
        try result.appendColumnDef(allocator, &coldef, session.current_db, "", col);
        try packet.writePacket(w, seq_id, coldef.items);
        seq_id +%= 1;
    }

    if ((caps & handshake.CLIENT_DEPRECATE_EOF) == 0) {
        try handshake.sendLegacyEofPacket(allocator, w, seq_id);
        seq_id +%= 1;
    }
    profiler.recordSince(.stmt_execute_write, header_write_start);

    var row_payload: std.ArrayList(u8) = .empty;
    defer row_payload.deinit(allocator);

    oprof.reset();
    var returned_rows: u64 = 0;
    while (true) {
        // A runtime error after column defs terminates the result set
        // with an ERR packet rather than dropping the connection.
        const next_start = profiler.start();
        const maybe_batch = compiled.next() catch |err| {
            profiler.recordSince(.stmt_execute_engine, next_start);
            const mapped = errors.mapInternal(err, null);
            try handshake.sendErrPacket(allocator, w, seq_id, mapped.code, mapped.sqlstate, mapped.message);
            return;
        };
        profiler.recordSince(.stmt_execute_engine, next_start);
        const batch = maybe_batch orelse break;
        returned_rows += @intCast(batch.row_count);
        const row_write_start = profiler.start();
        var r: usize = 0;
        while (r < batch.row_count) : (r += 1) {
            row_payload.clearRetainingCapacity();
            try prepared.appendBinaryRow(allocator, &row_payload, batch.schema, batch.values, r);
            try packet.writePacket(w, seq_id, row_payload.items);
            seq_id +%= 1;
        }
        profiler.recordSince(.stmt_execute_write, row_write_start);
    }
    profiler.addRowsReturned(returned_rows);
    oprof.dump("stmt_execute");

    const terminator_write_start = profiler.start();
    try result.sendResultTerminatorStatus(allocator, w, &seq_id, caps, session.transactionStatus());
    profiler.recordSince(.stmt_execute_write, terminator_write_start);

    const new_session = compiled.sessionValue();
    try session.replace(new_session.current_db, new_session.current_schema);
    session.captureVars(new_session);
}

/// COM_STMT_CLOSE — destroy the prepared statement. No response.
fn handleStmtClose(session: *SessionState, payload: []const u8) !void {
    if (payload.len < 4) return;
    const stmt_id = std.mem.readInt(u32, payload[0..4], .little);
    if (session.prepared_statements.fetchRemove(stmt_id)) |kv| {
        kv.value.deinit();
    }
}

/// COM_STMT_RESET — drop any accumulated long-data buffers. Reply OK
/// even if the stmt id is unknown (matches mysqld behavior in practice).
fn handleStmtReset(
    allocator: Allocator,
    w: *std.Io.Writer,
    session: *SessionState,
    payload: []const u8,
) !void {
    const seq_id: u8 = 1;
    if (payload.len < 4) {
        try handshake.sendErrPacket(allocator, w, seq_id, 1064, "42000".*, "malformed COM_STMT_RESET");
        return;
    }
    const stmt_id = std.mem.readInt(u32, payload[0..4], .little);
    if (session.prepared_statements.get(stmt_id)) |stmt| {
        for (stmt.long_data) |*ld| {
            if (ld.*) |*buf| {
                buf.deinit(stmt.allocator);
                ld.* = null;
            }
        }
    }
    try handshake.sendOkPacketStatus(allocator, w, seq_id, 0, 0, session.transactionStatus());
}

/// COM_STMT_SEND_LONG_DATA — append bytes to a per-stmt per-param
/// accumulator. No response. Unknown stmt id / bad layout silently
/// drops the data (matches mysqld behavior).
fn handleStmtSendLongData(session: *SessionState, payload: []const u8) !void {
    if (payload.len < 6) return;
    const stmt_id = std.mem.readInt(u32, payload[0..4], .little);
    const param_index = std.mem.readInt(u16, payload[4..6], .little);
    const data = payload[6..];

    const stmt = session.prepared_statements.get(stmt_id) orelse return;
    if (param_index >= stmt.long_data.len) return;
    if (stmt.long_data[param_index] == null) {
        stmt.long_data[param_index] = .empty;
    }
    var buf = &stmt.long_data[param_index].?;
    try buf.appendSlice(stmt.allocator, data);
}

test "applyInitDb resolves flat db__schema name" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var c = try Catalog.open(allocator, io, tmp.dir, .{});
    defer c.close();
    _ = try c.createDatabase("alpha");
    var session = try SessionState.init(allocator, c, 1);
    defer session.deinit();
    try applyInitDb(c, &session, "alpha__public");
    try std.testing.expectEqualStrings("alpha", session.current_db);
    try std.testing.expectEqualStrings("public", session.current_schema);
}

test "applyInitDb honors schema-within-current-db lookup" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var c = try Catalog.open(allocator, io, tmp.dir, .{});
    defer c.close();
    const main_db = try c.createDatabase("main");
    _ = try main_db.createSchema("reports");
    var session = try SessionState.init(allocator, c, 2);
    defer session.deinit();
    try applyInitDb(c, &session, "reports");
    try std.testing.expectEqualStrings("main", session.current_db);
    try std.testing.expectEqualStrings("reports", session.current_schema);
}
