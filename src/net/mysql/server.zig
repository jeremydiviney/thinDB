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
const ApiError = thindb_api.Error;

const local = @import("../local.zig");
const ir = @import("../../ir/ir.zig");
const sql = @import("../../sql/sql.zig");
const types = @import("../../types.zig");

const packet = @import("packet.zig");
const handshake = @import("handshake.zig");
const result = @import("result.zig");
const canned = @import("canned.zig");
const errors = @import("errors.zig");
const auth = @import("auth.zig");
const prepared = @import("prepared.zig");
const conn_registry = @import("../conn_registry.zig");
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
        handleConnection(self.allocator, self.io, self.catalog, stream, cid, self.auth_password, self.registry) catch |err| {
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
            };
            const thread = std.Thread.spawn(.{}, ConnJob.run, .{job}) catch {
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

    fn run(self: *ConnJob) void {
        defer self.allocator.destroy(self);
        defer self.limiter.release();
        defer self.stream.close(self.io);
        handleConnection(self.allocator, self.io, self.catalog, self.stream, self.connection_id, self.auth_password, self.registry) catch |err| {
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
    auth_salt: [auth.SALT_LEN]u8 = .{0} ** auth.SALT_LEN,
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

    fn init(allocator: Allocator) !SessionState {
        return .{
            .allocator = allocator,
            .current_db = try allocator.dupe(u8, "main"),
            .current_schema = try allocator.dupe(u8, "public"),
        };
    }

    fn deinit(self: *SessionState) void {
        self.allocator.free(self.current_db);
        self.allocator.free(self.current_schema);
        var it = self.prepared_statements.iterator();
        while (it.next()) |entry| entry.value_ptr.*.deinit();
        self.prepared_statements.deinit(self.allocator);
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
        return .{ .current_db = self.current_db, .current_schema = self.current_schema };
    }

    /// OR-able status bits derived from session state. Callers combine
    /// with any per-response extra flags (e.g. SERVER_MORE_RESULTS_EXISTS)
    /// before passing to OK/EOF emitters.
    fn transactionStatus(self: SessionState) u16 {
        return if (self.in_transaction) handshake.SERVER_STATUS_IN_TRANS else 0;
    }
};

fn handleConnection(
    allocator: Allocator,
    io: Io,
    catalog: *Catalog,
    stream: Io.net.Stream,
    connection_id: u32,
    auth_password: ?[]const u8,
    registry: ?*ConnectionRegistry,
) !void {
    var read_buf: [16 * 1024]u8 = undefined;
    var write_buf: [16 * 1024]u8 = undefined;
    var reader = stream.reader(io, &read_buf);
    var writer = stream.writer(io, &write_buf);
    const w = &writer.interface;
    const r = &reader.interface;

    var session = try SessionState.init(allocator);
    defer session.deinit();

    // Register in the shared registry so peer connections can KILL
    // this one's in-flight query. The connection state must outlive
    // any in-flight query; tying it to this stack frame is fine
    // because handleConnection only returns after the connection
    // closes.
    var conn_state = ConnectionState.init(connection_id, ConnectionState.deriveSecret(connection_id));
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

    const hs_packet = try packet.readPacket(allocator, r);
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
        const pkt = packet.readPacket(allocator, r) catch |err| switch (err) {
            error.EndOfStream => return,
            else => return err,
        };
        defer allocator.free(pkt.payload);
        if (pkt.payload.len == 0) continue;

        const cmd = pkt.payload[0];
        const body = pkt.payload[1..];
        switch (cmd) {
            0x01 => return, // COM_QUIT
            0x02 => try handleInitDb(allocator, w, catalog, &session, body),
            0x03 => try handleQuery(allocator, w, catalog, &session, body),
            // COM_STMT_PREPARE (0x16) — parse SQL, count `?` placeholders,
            // register a per-connection statement id, return prepare-ok.
            0x16 => try handleStmtPrepare(allocator, w, catalog, &session, body),
            // COM_STMT_EXECUTE (0x17) — bind parameters + run.
            0x17 => try handleStmtExecute(allocator, w, catalog, &session, body),
            // COM_STMT_SEND_LONG_DATA (0x18) — accumulate long-data
            // bytes into a per-stmt per-param buffer. No response.
            0x18 => try handleStmtSendLongData(&session, body),
            // COM_STMT_CLOSE (0x19) — free the stmt entry. No response.
            0x19 => try handleStmtClose(&session, body),
            // COM_STMT_RESET (0x1A) — clear long-data buffers; reply OK.
            0x1A => try handleStmtReset(allocator, w, &session, body),
            0x0E => try handshake.sendOkPacket(allocator, w, 1, 0, 0), // COM_PING
            // COM_RESET_CONNECTION — wipes per-connection state without
            // closing the socket. Connection poolers (e.g. ProxySQL,
            // mysql2's pool with `connectionLimit`) send this when
            // returning a borrowed connection to scrub session state.
            // We have no temp tables / prepared statements yet, so the
            // only state to reset is the txn flag + reverting the
            // current schema to the default.
            0x1F => {
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
            else => try handshake.sendErrPacket(allocator, w, 1, 1047, "HY000".*, "Unknown command"),
        }
        try w.flush();
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
    // optionally switches DB. We don't have temp tables / prepared
    // statements yet, so reset = clear txn + reapply default schema +
    // honor whatever schema the client passed.
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

fn handleQuery(
    allocator: Allocator,
    w: *std.Io.Writer,
    catalog: *Catalog,
    session: *SessionState,
    payload: []const u8,
) !void {
    var seq_id: u8 = 1;
    const caps = session.client_caps;

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
            .single_value => |sv| try result.sendSingleValueResult(allocator, w, sv.col, sv.val, &seq_id, caps),
            .single_null => |col| try result.sendSingleValueResult(allocator, w, col, null, &seq_id, caps),
            .variable_row => |vr| try result.sendVariableRow(allocator, w, vr.name, vr.value, &seq_id, caps),
            .empty_variables => try result.sendEmptyVariables(allocator, w, &seq_id, caps),
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

    try runEngineQuery(allocator, w, catalog, session, payload, &seq_id);
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
) !void {
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();

    const op = sql.parse(arena.allocator(), payload) catch |err| {
        const mapped = errors.mapInternal(err, "Parse error");
        try handshake.sendErrPacket(allocator, w, seq_id.*, mapped.code, mapped.sqlstate, @errorName(err));
        return;
    };

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
        for (stmts, 0..) |stmt, i| {
            const is_last = i + 1 == stmts.len;
            const base: u16 = session.transactionStatus();
            const extra: u16 = if (is_last) base else base | handshake.SERVER_MORE_RESULTS_EXISTS;
            try runSingleStatement(allocator, w, catalog, session, stmt, seq_id, extra);
        }
        return;
    }

    try runSingleStatement(allocator, w, catalog, session, op, seq_id, session.transactionStatus());
}

/// Compile + emit ONE statement's packets. `extra_status` is OR'd into
/// the terminator's status_flags — multi-statement responses pass
/// SERVER_MORE_RESULTS_EXISTS for every non-final statement so the
/// client keeps reading the chain.
fn runSingleStatement(
    allocator: Allocator,
    w: *std.Io.Writer,
    catalog: *Catalog,
    session: *SessionState,
    op: *const ir.Op,
    seq_id: *u8,
    extra_status: u16,
) !void {
    const main_db = catalog.database(session.current_db) orelse {
        try handshake.sendErrPacket(allocator, w, seq_id.*, 1049, "42000".*, "Unknown database");
        return;
    };

    var compiled = local.compileWithSession(allocator, main_db, session.asSession(), op) catch |err| {
        const mapped = errors.mapInternal(err, null);
        try handshake.sendErrPacket(allocator, w, seq_id.*, mapped.code, mapped.sqlstate, mapped.message);
        return;
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
        _ = compiled.next() catch |err| {
            const mapped = errors.mapInternal(err, null);
            try handshake.sendErrPacket(allocator, w, seq_id.*, mapped.code, mapped.sqlstate, mapped.message);
            return;
        };
        const new_session = compiled.sessionValue();
        try session.replace(new_session.current_db, new_session.current_schema);
        try handshake.sendOkPacketStatus(
            allocator,
            w,
            seq_id.*,
            compiled.affectedRows(),
            0,
            extra_status,
        );
        seq_id.* +%= 1;
        return;
    }

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

    const new_session = compiled.sessionValue();
    try session.replace(new_session.current_db, new_session.current_schema);
}

fn isSideEffectOp(op: ir.Op) bool {
    return switch (op) {
        .ddl, .insert => true,
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
) !void {
    var seq_id: u8 = 1;
    const caps = session.client_caps;

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();

    const num_params = prepared.countPlaceholders(arena.allocator(), payload) catch 0;

    const stmt_id = session.next_stmt_id;
    session.next_stmt_id +%= 1;
    const stmt = try prepared.createPreparedStmt(allocator, stmt_id, payload, num_params);

    // Best-effort schema inference. We substitute `?` → `0` so the
    // existing parser+compiler can produce an output schema; failures
    // here fall back to num_columns=0. Side-effect ops (DDL, INSERT)
    // are skipped before compile so the prepare-time pass has no
    // observable effect on the catalog or data.
    blk: {
        const dummy_sql = prepared.renderDummySubstitution(arena.allocator(), payload, num_params) catch break :blk;
        const dummy_op = sql.parse(arena.allocator(), dummy_sql) catch break :blk;
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

    try session.prepared_statements.put(allocator, stmt_id, stmt);

    try prepared.sendPrepareOkHeader(allocator, w, &seq_id, stmt_id, stmt.num_columns, num_params);

    // Param column-defs. Real MySQL emits one per `?` with empty
    // table/name and type=VAR_STRING. Mirror that.
    var pi: u16 = 0;
    while (pi < num_params) : (pi += 1) {
        try prepared.sendParamColumnDef(allocator, w, &seq_id);
    }
    if (num_params > 0 and (caps & handshake.CLIENT_DEPRECATE_EOF) == 0) {
        try handshake.sendLegacyEofPacket(allocator, w, seq_id);
        seq_id +%= 1;
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
    const params = prepared.decodeExecuteParams(arena_alloc, stmt, payload, after_header) catch |err| {
        const msg = switch (err) {
            error.NoBoundParamTypes => "missing parameter types on first execute",
            error.LongDataAndValueBoth => "long-data and value both bound for same param",
            error.UnsupportedParamType => "unsupported parameter type",
            else => "malformed COM_STMT_EXECUTE",
        };
        try handshake.sendErrPacket(allocator, w, seq_id, 1064, "42000".*, msg);
        return;
    };

    const substituted = prepared.substituteSql(arena_alloc, stmt.sql, params) catch |err| {
        const msg = switch (err) {
            error.MissingParameter => "missing parameter for placeholder",
            else => "parameter substitution failure",
        };
        try handshake.sendErrPacket(allocator, w, seq_id, 1064, "42000".*, msg);
        return;
    };

    // Clear long-data buffers after consuming them — MySQL semantics:
    // long-data accumulates across SEND_LONG_DATA calls but is consumed
    // by the next EXECUTE. Subsequent EXECUTEs start fresh.
    for (stmt.long_data) |*ld| {
        if (ld.*) |*buf| {
            buf.deinit(stmt.allocator);
            ld.* = null;
        }
    }

    const op = sql.parse(arena_alloc, substituted) catch |err| {
        const mapped = errors.mapInternal(err, null);
        try handshake.sendErrPacket(allocator, w, seq_id, mapped.code, mapped.sqlstate, mapped.message);
        return;
    };

    if (op.* == .batch) {
        try handshake.sendErrPacket(allocator, w, seq_id, 1064, "42000".*, "Multi-statement not supported in prepared mode");
        return;
    }

    const main_db = catalog.database(session.current_db) orelse {
        try handshake.sendErrPacket(allocator, w, seq_id, 1049, "42000".*, "Unknown database");
        return;
    };

    var compiled = local.compileWithSession(allocator, main_db, session.asSession(), op) catch |err| {
        const mapped = errors.mapInternal(err, null);
        try handshake.sendErrPacket(allocator, w, seq_id, mapped.code, mapped.sqlstate, mapped.message);
        return;
    };
    defer compiled.deinit();

    if (session.conn_state) |state| {
        state.clearCancel();
        compiled.cancel_flag = &state.cancel_flag;
    }

    if (isSideEffectOp(op.*)) {
        _ = compiled.next() catch |err| {
            const mapped = errors.mapInternal(err, null);
            try handshake.sendErrPacket(allocator, w, seq_id, mapped.code, mapped.sqlstate, mapped.message);
            return;
        };
        const new_session = compiled.sessionValue();
        try session.replace(new_session.current_db, new_session.current_schema);
        try handshake.sendOkPacketStatus(
            allocator,
            w,
            seq_id,
            compiled.affectedRows(),
            0,
            session.transactionStatus(),
        );
        return;
    }

    // Binary result set.
    const schema = compiled.outputSchema();

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

    var row_payload: std.ArrayList(u8) = .empty;
    defer row_payload.deinit(allocator);

    while (try compiled.next()) |batch| {
        var r: usize = 0;
        while (r < batch.row_count) : (r += 1) {
            row_payload.clearRetainingCapacity();
            try prepared.appendBinaryRow(allocator, &row_payload, batch.schema, batch.values, r);
            try packet.writePacket(w, seq_id, row_payload.items);
            seq_id +%= 1;
        }
    }

    try result.sendResultTerminatorStatus(allocator, w, &seq_id, caps, session.transactionStatus());

    const new_session = compiled.sessionValue();
    try session.replace(new_session.current_db, new_session.current_schema);
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
    var session = try SessionState.init(allocator);
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
    var session = try SessionState.init(allocator);
    defer session.deinit();
    try applyInitDb(c, &session, "reports");
    try std.testing.expectEqualStrings("main", session.current_db);
    try std.testing.expectEqualStrings("reports", session.current_schema);
}
