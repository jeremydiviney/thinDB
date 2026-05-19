//! PostgreSQL wire-compatible server. Accepts connections, runs the
//! StartupMessage exchange in trust-auth mode, then dispatches Simple
//! Query (`Q`) frames against the existing SQL parser →
//! `compileWithSession` pipeline.
//!
//! Extended-query, COPY, and cancellation aren't implemented. The session
//! tracks (current_db, current_schema) and surfaces the full 3-level
//! hierarchy (no MySQL-style flattening).

const std = @import("std");
const Allocator = std.mem.Allocator;
const Io = std.Io;

/// Default TCP port for a Postgres-wire listener. Matches the upstream
/// PostgreSQL default so existing client tooling Just Works.
pub const default_port: u16 = 5432;

const thindb_api = @import("../../api/api.zig");
const Catalog = thindb_api.Catalog;
const Session = thindb_api.Session;
const ApiError = thindb_api.Error;

const local = @import("../local.zig");
const ir = @import("../../ir/ir.zig");
const sql = @import("../../sql/sql.zig");

const packet = @import("packet.zig");
const startup = @import("startup.zig");
const result = @import("result.zig");
const canned = @import("canned.zig");
const errors = @import("errors.zig");
const auth = @import("auth.zig");
const ConnectionLimiter = @import("../conn_limit.zig").ConnectionLimiter;
const conn_registry = @import("../conn_registry.zig");
const ConnectionState = conn_registry.ConnectionState;
const ConnectionRegistry = conn_registry.Registry;
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
    limiter: *ConnectionLimiter,
    owns_limiter: bool,
    idle_timeout_secs: u32 = 0,
    /// Optional SCRAM-SHA-256 credentials. When null (default), the
    /// server runs in trust mode and accepts any SASL response (or no
    /// SASL exchange at all). Derived once via setAuthPassword from
    /// the cleartext password; the cleartext is not retained on the
    /// Server. Set after servePg() returns and before run().
    auth_credentials: ?auth.Credentials = null,
    /// Optional shared connection registry for cross-connection
    /// cancellation (CancelRequest / pg_cancel_backend / pg_terminate_backend).
    /// When null, those become no-ops. The binary supplies a registry
    /// shared with the MySQL wire so cancels can target connections on
    /// either. Caller-owned; must outlive the Server.
    registry: ?*ConnectionRegistry = null,

    /// Configure the SCRAM-SHA-256 password. Derives credentials
    /// (salt + StoredKey + ServerKey) once and stores them on the
    /// Server. Subsequent connections will require correct
    /// SASLResponse before AuthenticationOk.
    pub fn setAuthPassword(self: *Server, password: ?[]const u8) void {
        if (password) |pw| {
            self.auth_credentials = auth.deriveCredentials(pw);
        } else {
            self.auth_credentials = null;
        }
    }

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
        handleConnection(self.allocator, self.io, self.catalog, stream, cid, self.auth_credentials, self.registry) catch |err| {
            std.debug.print("pg: connection error: {s}\n", .{@errorName(err)});
        };
    }

    /// Long-running accept loop. Each connection runs on its own thread
    /// so multiple clients (and pool drivers) can be in-flight at once.
    /// The shared limiter enforces the global concurrency cap.
    pub fn run(self: *Server, should_stop: *std.atomic.Value(bool)) !void {
        while (!should_stop.load(.acquire)) {
            const stream = self.listener.accept(self.io) catch |err| {
                std.debug.print("pg: accept error: {s}\n", .{@errorName(err)});
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
                .auth_credentials = self.auth_credentials,
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
    auth_credentials: ?auth.Credentials,
    registry: ?*ConnectionRegistry,

    fn run(self: *ConnJob) void {
        defer self.allocator.destroy(self);
        defer self.limiter.release();
        defer self.stream.close(self.io);
        handleConnection(self.allocator, self.io, self.catalog, self.stream, self.connection_id, self.auth_credentials, self.registry) catch |err| {
            std.debug.print("pg: connection error: {s}\n", .{@errorName(err)});
        };
    }
};

/// Bind a PostgreSQL listener on `address` for queries against `catalog`.
/// The Server does NOT own the catalog — caller manages its lifetime.
/// When `limiter` is null the Server allocates a private limiter sized
/// from `catalog.config.max_connections`; pass an external limiter
/// (caller-owned) to share a single budget across multiple wires.
pub fn servePg(
    allocator: Allocator,
    io: Io,
    catalog: *Catalog,
    address: Io.net.IpAddress,
    limiter: ?*ConnectionLimiter,
) !*Server {
    var listen_addr = address;
    const listener = try Io.net.IpAddress.listen(&listen_addr, io, .{
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

/// Emit ErrorResponse 53300 / too_many_connections and close. Used when
/// the server-wide limiter rejected the slot at accept time. Best-effort.
fn sendTooManyConnections(
    allocator: Allocator,
    stream: Io.net.Stream,
    io: Io,
) !void {
    var write_buf: [256]u8 = undefined;
    var writer = stream.writer(io, &write_buf);
    const w = &writer.interface;
    try errors.sendErrorResponse(allocator, w, "53300".*, "too many connections");
    try w.flush();
}

const SessionState = struct {
    current_db: []u8,
    current_schema: []u8,
    application_name: []u8,
    allocator: Allocator,
    /// Reflected in the trailing ReadyForQuery byte. Drivers (notably
    /// node-pg and asyncpg) inspect this to decide whether the next
    /// statement opens an implicit transaction. We flip on BEGIN /
    /// START TRANSACTION and clear on COMMIT / ROLLBACK; thinDB doesn't
    /// enforce real transactions yet — bookkeeping only.
    in_transaction: bool = false,
    /// Pointer into the shared registry entry for this connection.
    /// Used to wire the cancel_flag into the in-flight CompiledQuery
    /// so a peer CancelRequest / pg_cancel_backend aborts at the
    /// next batch boundary.
    conn_state: ?*ConnectionState = null,
    /// Shared registry pointer so this connection's
    /// pg_cancel_backend / pg_terminate_backend can reach peers.
    registry: ?*ConnectionRegistry = null,

    fn init(allocator: Allocator) !SessionState {
        return .{
            .allocator = allocator,
            .current_db = try allocator.dupe(u8, "main"),
            .current_schema = try allocator.dupe(u8, "public"),
            .application_name = try allocator.dupe(u8, ""),
        };
    }

    /// 'I' = idle (no transaction). 'T' = in a transaction. 'E' = in
    /// a failed transaction (we don't enter this state; placeholder
    /// for the day we add real transactions).
    fn txStatusByte(self: SessionState) u8 {
        return if (self.in_transaction) 'T' else 'I';
    }

    fn deinit(self: *SessionState) void {
        self.allocator.free(self.current_db);
        self.allocator.free(self.current_schema);
        self.allocator.free(self.application_name);
    }

    fn replaceDbSchema(self: *SessionState, db: []const u8, schema: []const u8) !void {
        const new_db = try self.allocator.dupe(u8, db);
        errdefer self.allocator.free(new_db);
        const new_schema = try self.allocator.dupe(u8, schema);
        self.allocator.free(self.current_db);
        self.allocator.free(self.current_schema);
        self.current_db = new_db;
        self.current_schema = new_schema;
    }

    fn replaceAppName(self: *SessionState, name: []const u8) !void {
        const new_name = try self.allocator.dupe(u8, name);
        self.allocator.free(self.application_name);
        self.application_name = new_name;
    }

    fn asSession(self: SessionState) Session {
        return .{ .current_db = self.current_db, .current_schema = self.current_schema };
    }
};

fn handleConnection(
    allocator: Allocator,
    io: Io,
    catalog: *Catalog,
    stream: Io.net.Stream,
    connection_id: u32,
    auth_creds: ?auth.Credentials,
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

    var conn_state = ConnectionState.init(connection_id, connection_id ^ 0xA1B2C3D4);
    if (registry) |reg| {
        try reg.register(&conn_state);
    }
    defer if (registry) |reg| reg.unregister(connection_id);
    session.conn_state = &conn_state;
    session.registry = registry;

    if (!try completeStartup(allocator, w, r, catalog, &session, connection_id, auth_creds, registry)) return;

    while (true) {
        const frame = packet.readFrame(allocator, r) catch |err| switch (err) {
            error.EndOfStream => return,
            else => return err,
        };
        defer allocator.free(frame.payload);

        switch (frame.type_byte) {
            'X' => return,
            'Q' => try handleQuery(allocator, w, catalog, &session, frame.payload),
            'S' => {
                try startup.sendReadyForQuery(allocator, w, session.txStatusByte());
                try w.flush();
            },
            else => {
                try errors.sendErrorResponse(allocator, w, "0A000".*, "unsupported message type");
                try startup.sendReadyForQuery(allocator, w, session.txStatusByte());
                try w.flush();
            },
        }
    }
}

/// Handles SSLRequest negotiation if needed, then the real StartupMessage
/// and the post-auth parameter / key / ready frames. Returns true on
/// success, false if the client aborted (cancellation request, etc.).
fn completeStartup(
    allocator: Allocator,
    w: *std.Io.Writer,
    r: *std.Io.Reader,
    catalog: *Catalog,
    session: *SessionState,
    connection_id: u32,
    auth_creds: ?auth.Credentials,
    registry: ?*ConnectionRegistry,
) !bool {
    const first = try packet.readStartupFrame(allocator, r);
    defer allocator.free(first.payload);

    var classified = try startup.parseFirstFrame(first.payload);
    var second_buf: ?[]u8 = null;
    defer if (second_buf) |b| allocator.free(b);
    if (classified == .ssl_request or classified == .gss_request) {
        try startup.sendSslDenial(w);
        try w.flush();

        const second = try packet.readStartupFrame(allocator, r);
        second_buf = second.payload;
        classified = try startup.parseFirstFrame(second.payload);
    }

    const params = switch (classified) {
        .startup => |p| p,
        .cancel_request => |cr| {
            // CancelRequest is a one-shot side connection: client
            // opens fresh socket, sends pid+secret, server applies
            // the cancel to the matching connection's in-flight
            // query, then both sides close. No response packet.
            if (registry) |reg| {
                _ = reg.requestCancel(cr.process_id, cr.secret_key);
            }
            return false;
        },
        .ssl_request, .gss_request => return false,
    };

    if (params.database) |db_name| {
        applyDatabase(catalog, session, db_name) catch |err| {
            const mapped = errors.mapInternal(err);
            try errors.sendErrorResponse(allocator, w, mapped.sqlstate, mapped.message);
            try w.flush();
            return false;
        };
    }
    if (params.application_name) |an| try session.replaceAppName(an);

    if (auth_creds) |creds| {
        if (!try runScramSha256(allocator, w, r, creds)) return false;
    }

    try startup.sendAuthenticationOk(allocator, w);
    try startup.sendStandardParameterStatus(
        allocator,
        w,
        session.application_name,
        if (params.user.len > 0) params.user else "thindb",
    );
    try startup.sendBackendKeyData(allocator, w, connection_id, connection_id ^ 0xA1B2C3D4);
    try startup.sendReadyForQuery(allocator, w, 'I');
    try w.flush();
    return true;
}

/// Execute the SCRAM-SHA-256 SASL exchange. Returns true on auth
/// success. On any failure (malformed messages, proof mismatch,
/// client picked unsupported mechanism) the client gets an
/// ErrorResponse with SQLSTATE 28P01 and the function returns false.
fn runScramSha256(
    allocator: Allocator,
    w: *std.Io.Writer,
    r: *std.Io.Reader,
    creds: auth.Credentials,
) !bool {
    // 1. Server -> AuthenticationSASL (mechanism list).
    const mechs = [_][]const u8{"SCRAM-SHA-256"};
    try startup.sendAuthenticationSASL(allocator, w, &mechs);
    try w.flush();

    // 2. Client -> SASLInitialResponse ('p' frame): mechanism name +
    // int32 length + initial-response bytes.
    const f1 = try packet.readFrame(allocator, r);
    defer allocator.free(f1.payload);
    if (f1.type_byte != 'p') return scramReject(allocator, w, "expected SASLInitialResponse");

    var cursor: usize = 0;
    const selected_mech = try packet.readCString(f1.payload, &cursor);
    if (!std.mem.eql(u8, selected_mech, "SCRAM-SHA-256"))
        return scramReject(allocator, w, "unsupported SASL mechanism");
    const initial_len = try packet.readU32(f1.payload, &cursor);
    if (cursor + initial_len > f1.payload.len)
        return scramReject(allocator, w, "truncated SASLInitialResponse");
    const client_first = f1.payload[cursor .. cursor + initial_len];

    const cf = auth.parseClientFirst(allocator, client_first) catch {
        return scramReject(allocator, w, "malformed client-first-message");
    };

    // 3. Server -> AuthenticationSASLContinue (server-first-message).
    var server_nonce_raw: [18]u8 = undefined;
    auth.randomServerNonce(&server_nonce_raw);
    var server_nonce_b64: [24]u8 = undefined;
    _ = std.base64.standard.Encoder.encode(&server_nonce_b64, &server_nonce_raw);

    var salt_b64_buf: [32]u8 = undefined;
    const salt_b64 = std.base64.standard.Encoder.encode(&salt_b64_buf, &creds.salt);

    var server_first: std.ArrayList(u8) = .empty;
    defer server_first.deinit(allocator);
    try server_first.appendSlice(allocator, "r=");
    try server_first.appendSlice(allocator, cf.client_nonce);
    try server_first.appendSlice(allocator, &server_nonce_b64);
    try server_first.appendSlice(allocator, ",s=");
    try server_first.appendSlice(allocator, salt_b64);
    try server_first.appendSlice(allocator, ",i=");
    var iter_buf: [16]u8 = undefined;
    const iter_str = try std.fmt.bufPrint(&iter_buf, "{d}", .{creds.iter_count});
    try server_first.appendSlice(allocator, iter_str);

    try startup.sendAuthenticationSASLContinue(allocator, w, server_first.items);
    try w.flush();

    // 4. Client -> SASLResponse ('p' frame): client-final-message.
    const f2 = try packet.readFrame(allocator, r);
    defer allocator.free(f2.payload);
    if (f2.type_byte != 'p') return scramReject(allocator, w, "expected SASLResponse");
    const client_final = f2.payload;

    const cfinal = auth.parseClientFinal(client_final) catch {
        return scramReject(allocator, w, "malformed client-final-message");
    };

    // Combined-nonce check: client must echo exactly client_nonce ++
    // server_nonce.
    var expected_nonce: std.ArrayList(u8) = .empty;
    defer expected_nonce.deinit(allocator);
    try expected_nonce.appendSlice(allocator, cf.client_nonce);
    try expected_nonce.appendSlice(allocator, &server_nonce_b64);
    if (!std.mem.eql(u8, expected_nonce.items, cfinal.combined_nonce))
        return scramReject(allocator, w, "nonce mismatch");

    // AuthMessage = client-first-bare + "," + server-first-message +
    //               "," + client-final-without-proof
    var auth_msg: std.ArrayList(u8) = .empty;
    defer auth_msg.deinit(allocator);
    try auth_msg.appendSlice(allocator, cf.bare);
    try auth_msg.append(allocator, ',');
    try auth_msg.appendSlice(allocator, server_first.items);
    try auth_msg.append(allocator, ',');
    try auth_msg.appendSlice(allocator, cfinal.without_proof);

    // Decode the base64 ClientProof.
    var proof: [auth.HASH_LEN]u8 = undefined;
    const decoder = std.base64.standard.Decoder;
    const decoded_len = decoder.calcSizeForSlice(cfinal.client_proof_b64) catch {
        return scramReject(allocator, w, "invalid ClientProof base64");
    };
    if (decoded_len != auth.HASH_LEN) return scramReject(allocator, w, "wrong ClientProof length");
    decoder.decode(&proof, cfinal.client_proof_b64) catch {
        return scramReject(allocator, w, "invalid ClientProof base64");
    };

    if (!auth.verifyClientProof(creds, auth_msg.items, proof))
        return scramReject(allocator, w, "authentication failed");

    // 5. Server -> AuthenticationSASLFinal (server-final-message).
    const sig = auth.serverSignature(creds, auth_msg.items);
    var sig_b64_buf: [64]u8 = undefined;
    const sig_b64 = std.base64.standard.Encoder.encode(&sig_b64_buf, &sig);
    var server_final: std.ArrayList(u8) = .empty;
    defer server_final.deinit(allocator);
    try server_final.appendSlice(allocator, "v=");
    try server_final.appendSlice(allocator, sig_b64);
    try startup.sendAuthenticationSASLFinal(allocator, w, server_final.items);
    try w.flush();

    return true;
}

fn scramReject(allocator: Allocator, w: *std.Io.Writer, message: []const u8) !bool {
    try errors.sendErrorResponse(allocator, w, "28P01".*, message);
    try w.flush();
    return false;
}

fn applyDatabase(catalog: *Catalog, session: *SessionState, name: []const u8) !void {
    if (catalog.database(name) != null) {
        try session.replaceDbSchema(name, "public");
        return;
    }
    return ApiError.DatabaseNotFound;
}

fn handleQuery(
    allocator: Allocator,
    w: *std.Io.Writer,
    catalog: *Catalog,
    session: *SessionState,
    payload: []const u8,
) !void {
    if (payload.len == 0) {
        try errors.sendErrorResponse(allocator, w, "42000".*, "empty query");
        try startup.sendReadyForQuery(allocator, w, session.txStatusByte());
        try w.flush();
        return;
    }

    var query_end = payload.len;
    while (query_end > 0 and payload[query_end - 1] == 0) query_end -= 1;
    const sql_text = payload[0..query_end];

    if (try canned.match(allocator, sql_text, session.current_db, session.current_schema)) |probe| {
        try dispatchProbe(allocator, w, catalog, session, probe);
        try startup.sendReadyForQuery(allocator, w, session.txStatusByte());
        try w.flush();
        return;
    }

    runEngineQuery(allocator, w, catalog, session, sql_text) catch |err| {
        const mapped = errors.mapInternal(err);
        try errors.sendErrorResponse(allocator, w, mapped.sqlstate, mapped.message);
    };

    try startup.sendReadyForQuery(allocator, w, session.txStatusByte());
    try w.flush();
}

fn dispatchProbe(
    allocator: Allocator,
    w: *std.Io.Writer,
    catalog: *Catalog,
    session: *SessionState,
    probe: canned.Probe,
) !void {
    switch (probe) {
        .accept => |tag| {
            // BEGIN / COMMIT / ROLLBACK flip the session's transaction
            // bookkeeping so the trailing ReadyForQuery byte reflects
            // the new state. SAVEPOINT / RELEASE / etc. are silently
            // accepted with no state change.
            if (std.ascii.eqlIgnoreCase(tag, "BEGIN")) {
                session.in_transaction = true;
            } else if (std.ascii.eqlIgnoreCase(tag, "COMMIT") or
                std.ascii.eqlIgnoreCase(tag, "ROLLBACK"))
            {
                session.in_transaction = false;
            }
            try result.sendCommandComplete(allocator, w, tag);
        },
        .single_value => |sv| {
            const cols = [_]@import("../../types.zig").Column{.{ .name = sv.col, .type = .string, .nullable = sv.val == null }};
            try result.sendRowDescription(allocator, w, cols[0..]);
            const cells = [_]?[]const u8{sv.val};
            try result.sendDataRow(allocator, w, cells[0..]);
            try sendSelectComplete(allocator, w, 1);
        },
        .catalog_listing => |cl| try dispatchCatalogListing(allocator, w, catalog, session, cl.col, cl.kind),
        .static_rows => |sr| {
            var col_types: std.ArrayList(@import("../../types.zig").Column) = .empty;
            defer col_types.deinit(allocator);
            for (sr.col_names) |cn| try col_types.append(allocator, .{ .name = cn, .type = .string, .nullable = true });
            try result.sendRowDescription(allocator, w, col_types.items);
            for (sr.rows) |row| try result.sendDataRow(allocator, w, row);
            try sendSelectComplete(allocator, w, sr.rows.len);
        },
        .cancel_backend => |pid| {
            const success = if (session.registry) |reg| reg.requestCancel(pid, 0) else false;
            const cols = [_]@import("../../types.zig").Column{.{ .name = "pg_cancel_backend", .type = .boolean, .nullable = false }};
            try result.sendRowDescription(allocator, w, cols[0..]);
            const cell: ?[]const u8 = if (success) "t" else "f";
            const cells = [_]?[]const u8{cell};
            try result.sendDataRow(allocator, w, cells[0..]);
            try sendSelectComplete(allocator, w, 1);
        },
    }
}

fn dispatchCatalogListing(
    allocator: Allocator,
    w: *std.Io.Writer,
    catalog: *Catalog,
    session: *SessionState,
    col_name: []const u8,
    kind: canned.ListingKind,
) !void {
    var names: std.ArrayList([]u8) = .empty;
    defer {
        for (names.items) |s| allocator.free(s);
        names.deinit(allocator);
    }

    switch (kind) {
        .databases => {
            const db_names = try catalog.listDatabases(allocator);
            defer allocator.free(db_names);
            for (db_names) |n| try names.append(allocator, n);
        },
        .schemas => {
            const db = catalog.database(session.current_db) orelse return ApiError.DatabaseNotFound;
            const schema_names = try db.listSchemas(allocator);
            defer allocator.free(schema_names);
            for (schema_names) |n| try names.append(allocator, n);
        },
        .tables => {
            const db = catalog.database(session.current_db) orelse return ApiError.DatabaseNotFound;
            const sc = db.schema(session.current_schema) orelse return ApiError.SchemaNotFound;
            const table_names = try sc.listTables(allocator);
            defer allocator.free(table_names);
            for (table_names) |n| try names.append(allocator, n);
        },
    }

    const cols = [_]@import("../../types.zig").Column{.{ .name = col_name, .type = .string }};
    try result.sendRowDescription(allocator, w, cols[0..]);
    for (names.items) |n| {
        const cells = [_]?[]const u8{n};
        try result.sendDataRow(allocator, w, cells[0..]);
    }
    try sendSelectComplete(allocator, w, names.items.len);
}

fn runEngineQuery(
    allocator: Allocator,
    w: *std.Io.Writer,
    catalog: *Catalog,
    session: *SessionState,
    sql_text: []const u8,
) !void {
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();

    const op = try sql.parse(arena.allocator(), sql_text);

    if (op.* == .batch) {
        // PG simple-Query protocol natively supports `;`-separated
        // statements: emit each statement's response packets in turn,
        // then a single ReadyForQuery (sent by handleQuery, not here).
        // Per spec, if any statement errors the remaining ones are
        // skipped; we propagate the error to handleQuery which emits
        // ErrorResponse + ReadyForQuery.
        for (op.batch.statements) |stmt| {
            try runSingleStatement(allocator, w, catalog, session, stmt);
        }
        return;
    }

    try runSingleStatement(allocator, w, catalog, session, op);
}

/// Run + emit the response packets for ONE statement (RowDescription/
/// DataRow*/CommandComplete for SELECT, or CommandComplete-only for
/// side-effect statements). Caller (or the multi-statement loop) emits
/// the single trailing ReadyForQuery.
fn runSingleStatement(
    allocator: Allocator,
    w: *std.Io.Writer,
    catalog: *Catalog,
    session: *SessionState,
    op: *const ir.Op,
) !void {
    const main_db = catalog.database(session.current_db) orelse return ApiError.DatabaseNotFound;

    var compiled = try local.compileWithSession(allocator, main_db, session.asSession(), op);
    defer compiled.deinit();

    // Wire the connection's cancel flag into the compiled query so a
    // peer CancelRequest / pg_cancel_backend aborts at the next batch
    // boundary. Clear any stale cancel from a previous statement.
    if (session.conn_state) |state| {
        state.clearCancel();
        compiled.cancel_flag = &state.cancel_flag;
    }

    if (isSideEffectOp(op.*)) {
        _ = try compiled.next();
        const new_session = compiled.sessionValue();
        try session.replaceDbSchema(new_session.current_db, new_session.current_schema);
        switch (op.*) {
            .insert => {
                var tag_buf: [48]u8 = undefined;
                const tag = try std.fmt.bufPrint(&tag_buf, "INSERT 0 {d}", .{compiled.affectedRows()});
                try result.sendCommandComplete(allocator, w, tag);
            },
            else => try result.sendCommandComplete(allocator, w, commandTagFor(op.*)),
        }
        return;
    }

    const rows = try result.sendQueryResult(allocator, w, &compiled);
    const new_session = compiled.sessionValue();
    try session.replaceDbSchema(new_session.current_db, new_session.current_schema);

    var tag_buf: [40]u8 = undefined;
    const tag = try std.fmt.bufPrint(&tag_buf, "SELECT {d}", .{rows});
    try result.sendCommandComplete(allocator, w, tag);
}

fn isSideEffectOp(op: ir.Op) bool {
    return switch (op) {
        .ddl, .insert => true,
        else => false,
    };
}

fn commandTagFor(op: ir.Op) []const u8 {
    return switch (op) {
        .ddl => |d| switch (d) {
            .create_database => "CREATE DATABASE",
            .drop_database => "DROP DATABASE",
            .create_schema => "CREATE SCHEMA",
            .drop_schema => "DROP SCHEMA",
            .create_table => "CREATE TABLE",
            .drop_table => "DROP TABLE",
            .use_schema, .use_database_schema => "SET",
        },
        else => "OK",
    };
}

fn sendSelectComplete(allocator: Allocator, w: *std.Io.Writer, rows: usize) !void {
    var buf: [40]u8 = undefined;
    const tag = try std.fmt.bufPrint(&buf, "SELECT {d}", .{rows});
    try result.sendCommandComplete(allocator, w, tag);
}

test "applyDatabase resolves an existing database" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var c = try Catalog.open(allocator, io, tmp.dir, .{});
    defer c.close();
    _ = try c.createDatabase("alpha");
    var session = try SessionState.init(allocator);
    defer session.deinit();
    try applyDatabase(c, &session, "alpha");
    try std.testing.expectEqualStrings("alpha", session.current_db);
    try std.testing.expectEqualStrings("public", session.current_schema);
}

test "applyDatabase fails on unknown database" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var c = try Catalog.open(allocator, io, tmp.dir, .{});
    defer c.close();
    var session = try SessionState.init(allocator);
    defer session.deinit();
    try std.testing.expectError(error.DatabaseNotFound, applyDatabase(c, &session, "ghost"));
}
