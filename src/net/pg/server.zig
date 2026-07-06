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
const TempNamespace = thindb_api.TempNamespace;
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
const copy = @import("copy.zig");
const extended = @import("extended.zig");
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
    /// Catalog root used to lazily open the per-session temp namespace
    /// when the first `CREATE TEMP TABLE` arrives. Borrowed.
    catalog: *Catalog,
    /// Backend / connection id; used as the per-session subdir name
    /// inside `_temp/`.
    backend_id: u32,
    /// Session-local temp table namespace. Lazily allocated on the
    /// first CREATE TEMP TABLE. Closed (and its on-disk dir removed)
    /// in `deinit` and on `DISCARD ALL` / `DISCARD TEMP`.
    temp_namespace: ?*TempNamespace = null,
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
    /// Per-connection Extended Query state — prepared statements,
    /// portals, and the error-skip latch that survives until the next
    /// Sync.
    ext: extended.ExtendedState = .{},

    fn init(allocator: Allocator, catalog: *Catalog, backend_id: u32) !SessionState {
        return .{
            .allocator = allocator,
            .catalog = catalog,
            .backend_id = backend_id,
            .current_db = try allocator.dupe(u8, "main"),
            .current_schema = try allocator.dupe(u8, "public"),
            .application_name = try allocator.dupe(u8, ""),
        };
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
    /// disconnect, DISCARD ALL, DISCARD TEMP.
    fn dropTempNamespace(self: *SessionState) void {
        if (self.temp_namespace) |ns| {
            ns.close();
            self.temp_namespace = null;
        }
    }

    /// 'I' = idle (no transaction). 'T' = in a transaction. 'E' = in
    /// a failed transaction (we don't enter this state; placeholder
    /// for the day we add real transactions).
    fn txStatusByte(self: SessionState) u8 {
        return if (self.in_transaction) 'T' else 'I';
    }

    fn deinit(self: *SessionState) void {
        self.dropTempNamespace();
        self.ext.deinit(self.allocator);
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
        return .{
            .current_db = self.current_db,
            .current_schema = self.current_schema,
            .dialect = .postgres,
            .temp_namespace = self.temp_namespace,
        };
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

    var session = try SessionState.init(allocator, catalog, connection_id);
    defer session.deinit();

    var conn_state = ConnectionState.init(connection_id, ConnectionState.deriveSecret(connection_id));
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

        // Extended-Query error-skip latch: once a frame in an Extended
        // sequence (P/B/D/E/C/H) fails, the server discards every
        // subsequent frame until Sync. Terminate must still close the
        // connection cleanly.
        if (session.ext.in_error_until_sync) {
            switch (frame.type_byte) {
                'X' => return,
                'S' => {
                    session.ext.in_error_until_sync = false;
                    try startup.sendReadyForQuery(allocator, w, session.txStatusByte());
                    try w.flush();
                },
                else => continue,
            }
            continue;
        }

        switch (frame.type_byte) {
            'X' => return,
            'Q' => try handleQuery(allocator, w, r, catalog, &session, frame.payload),
            'P' => try handleExtendedFrame(allocator, w, catalog, &session, .parse, frame.payload),
            'B' => try handleExtendedFrame(allocator, w, catalog, &session, .bind, frame.payload),
            'D' => try handleExtendedFrame(allocator, w, catalog, &session, .describe, frame.payload),
            'E' => try handleExtendedFrame(allocator, w, catalog, &session, .execute, frame.payload),
            'C' => try handleExtendedFrame(allocator, w, catalog, &session, .close_stmt, frame.payload),
            'H' => try w.flush(),
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

const ExtendedKind = enum { parse, bind, describe, execute, close_stmt };

/// Generic Extended-Query frame entrypoint. Catches any handler error,
/// emits an ErrorResponse, and engages the error-skip latch so the
/// remaining frames in the current Extended sequence are silently
/// dropped until Sync. Does NOT emit ReadyForQuery — Sync owns that.
fn handleExtendedFrame(
    allocator: Allocator,
    w: *std.Io.Writer,
    catalog: *Catalog,
    session: *SessionState,
    kind: ExtendedKind,
    payload: []const u8,
) !void {
    const handler_result = switch (kind) {
        .parse => extended_handleParse(allocator, w, catalog, session, payload),
        .bind => extended_handleBind(allocator, w, session, payload),
        .describe => extended_handleDescribe(allocator, w, session, payload),
        .execute => extended_handleExecute(allocator, w, catalog, session, payload),
        .close_stmt => extended_handleClose(allocator, w, session, payload),
    };
    handler_result catch |err| {
        const mapped = errors.mapInternal(err);
        try errors.sendErrorResponse(allocator, w, mapped.sqlstate, mapped.message);
        session.ext.in_error_until_sync = true;
    };
}

fn extended_handleParse(
    allocator: Allocator,
    w: *std.Io.Writer,
    catalog: *Catalog,
    session: *SessionState,
    payload: []const u8,
) !void {
    var temp_arena = std.heap.ArenaAllocator.init(allocator);
    defer temp_arena.deinit();
    const parsed = try extended.parseParseFrame(temp_arena.allocator(), payload);

    // Per PG spec: client may understate num_params. We size from the
    // max `$N` actually referenced, then take the larger of the
    // type-hint list length and that count.
    const max_idx = try extended.maxDollarIndex(temp_arena.allocator(), parsed.sql);
    const num_params: u32 = @max(@as(u32, @intCast(parsed.type_oids.len)), max_idx);

    // Replace any pre-existing statement under this name (per spec).
    session.ext.dropStatement(allocator, parsed.statement_name);

    const stmt = try allocator.create(extended.PreparedStmt);
    errdefer allocator.destroy(stmt);

    const name_copy = try allocator.dupe(u8, parsed.statement_name);
    errdefer allocator.free(name_copy);
    const sql_copy = try allocator.dupe(u8, parsed.sql);
    errdefer allocator.free(sql_copy);
    const oids_copy = try allocator.alloc(u32, num_params);
    errdefer allocator.free(oids_copy);
    var i: usize = 0;
    while (i < num_params) : (i += 1) {
        oids_copy[i] = if (i < parsed.type_oids.len) parsed.type_oids[i] else 0;
    }

    stmt.* = .{
        .name = name_copy,
        .sql = sql_copy,
        .num_params = num_params,
        .param_oids = oids_copy,
        .arena = std.heap.ArenaAllocator.init(allocator),
    };

    // Best-effort schema inference; failure is silently swallowed and
    // Describe-statement reports NoData.
    if (extended.dryCompileSchema(allocator, catalog, session.asSession(), parsed.sql, num_params) catch null) |inferred| {
        stmt.column_schema = inferred.columns;
        stmt.column_names = inferred.names;
    }

    // Statements are keyed by their stored name (owned by stmt). We
    // also need a copy of the key bytes for the map itself — keep the
    // key slot pointing at the same bytes as stmt.name to avoid double
    // ownership.
    const map_key = try allocator.dupe(u8, parsed.statement_name);
    errdefer allocator.free(map_key);
    try session.ext.statements.put(allocator, map_key, stmt);

    try extended.sendParseComplete(w);
}

fn extended_handleBind(
    allocator: Allocator,
    w: *std.Io.Writer,
    session: *SessionState,
    payload: []const u8,
) !void {
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const aa = arena.allocator();
    const bind = try extended.parseBindFrame(aa, payload);

    const stmt = session.ext.statements.get(bind.statement_name) orelse {
        return extended.Error.UnknownStatement;
    };

    if (bind.param_values.len != stmt.num_params) {
        return extended.Error.BindParamCountMismatch;
    }

    const literals = try extended.renderBindParams(aa, bind, stmt.param_oids);
    const bound_sql = try extended.substituteDollarSql(allocator, stmt.sql, literals);
    errdefer allocator.free(bound_sql);

    const result_formats = try allocator.alloc(u16, bind.result_formats.len);
    errdefer allocator.free(result_formats);
    for (bind.result_formats, 0..) |f, idx| result_formats[idx] = f;

    // Replace any portal currently sharing this name (per spec).
    session.ext.dropPortal(allocator, bind.portal_name);

    const portal_name = try allocator.dupe(u8, bind.portal_name);
    errdefer allocator.free(portal_name);

    const portal = try allocator.create(extended.Portal);
    errdefer allocator.destroy(portal);
    portal.* = .{
        .name = portal_name,
        .stmt = stmt,
        .bound_sql = bound_sql,
        .result_formats = result_formats,
    };

    const map_key = try allocator.dupe(u8, bind.portal_name);
    errdefer allocator.free(map_key);
    try session.ext.portals.put(allocator, map_key, portal);

    try extended.sendBindComplete(w);
}

fn extended_handleDescribe(
    allocator: Allocator,
    w: *std.Io.Writer,
    session: *SessionState,
    payload: []const u8,
) !void {
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const desc = try extended.parseDescribeFrame(payload);

    switch (desc.kind) {
        'S' => {
            const stmt = session.ext.statements.get(desc.name) orelse {
                return extended.Error.UnknownStatement;
            };
            const oids = try extended.paramOidsForDescribe(arena.allocator(), stmt.param_oids, stmt.num_params);
            try extended.sendParameterDescription(allocator, w, oids);
            if (stmt.column_schema) |schema| {
                try result.sendRowDescription(allocator, w, schema);
            } else {
                try extended.sendNoData(w);
            }
        },
        'P' => {
            const portal = session.ext.portals.get(desc.name) orelse {
                return extended.Error.UnknownPortal;
            };
            if (portal.stmt.column_schema) |schema| {
                try result.sendRowDescription(allocator, w, schema);
            } else {
                try extended.sendNoData(w);
            }
        },
        else => return extended.Error.InvalidDescribeTarget,
    }
}

fn extended_handleExecute(
    allocator: Allocator,
    w: *std.Io.Writer,
    catalog: *Catalog,
    session: *SessionState,
    payload: []const u8,
) !void {
    const exec_payload = try extended.parseExecuteFrame(payload);
    const portal = session.ext.portals.get(exec_payload.portal_name) orelse {
        return extended.Error.UnknownPortal;
    };

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const aa = arena.allocator();

    const op = try sql.parseWithContext(aa, portal.bound_sql, .postgres, &catalog.udfs, .{ .registry = &catalog.sql_fns, .db = session.current_db });

    if (op.* == .batch) {
        for (op.batch.statements) |stmt| {
            if (stmt.* == .copy) return copy.Error.CopyMustBeSoleStatement;
            try runExtendedStatement(allocator, w, catalog, session, stmt);
        }
        return;
    }

    try runExtendedStatement(allocator, w, catalog, session, op);
}

fn runExtendedStatement(
    allocator: Allocator,
    w: *std.Io.Writer,
    catalog: *Catalog,
    session: *SessionState,
    op: *const ir.Op,
) !void {
    if (op.* == .copy) return copy.Error.CopyMustBeSoleStatement;

    const main_db = catalog.database(session.current_db) orelse return ApiError.DatabaseNotFound;

    if (needsTempNamespace(op.*)) {
        _ = try session.ensureTempNamespace();
    }

    var compiled = try local.compileWithSession(allocator, main_db, session.asSession(), op);
    defer compiled.deinit();

    if (session.conn_state) |state| {
        state.clearCancel();
        compiled.cancel_flag = &state.cancel_flag;
    }

    if (isSideEffectOp(op.*)) {
        _ = try compiled.next();
        const new_session = compiled.sessionValue();
        try session.replaceDbSchema(new_session.current_db, new_session.current_schema);
        switch (op.*) {
            .insert, .insert_select => {
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

fn extended_handleClose(
    allocator: Allocator,
    w: *std.Io.Writer,
    session: *SessionState,
    payload: []const u8,
) !void {
    const cls = try extended.parseCloseFrame(payload);
    switch (cls.kind) {
        'S' => session.ext.dropStatement(allocator, cls.name),
        'P' => session.ext.dropPortal(allocator, cls.name),
        else => return extended.Error.InvalidCloseTarget,
    }
    try extended.sendCloseComplete(w);
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
    try startup.sendBackendKeyData(allocator, w, connection_id, ConnectionState.deriveSecret(connection_id));
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
    r: *std.Io.Reader,
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
        try dispatchProbe(allocator, w, session, probe);
        try startup.sendReadyForQuery(allocator, w, session.txStatusByte());
        try w.flush();
        return;
    }

    runEngineQuery(allocator, w, r, catalog, session, sql_text) catch |err| {
        const mapped = errors.mapInternal(err);
        try errors.sendErrorResponse(allocator, w, mapped.sqlstate, mapped.message);
    };

    try startup.sendReadyForQuery(allocator, w, session.txStatusByte());
    try w.flush();
}

fn dispatchProbe(
    allocator: Allocator,
    w: *std.Io.Writer,
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
        .discard_temp => |tag| {
            session.dropTempNamespace();
            try result.sendCommandComplete(allocator, w, tag);
        },
        .set_search_path => |schema| {
            try session.replaceDbSchema(session.current_db, schema);
            allocator.free(schema);
            try result.sendCommandComplete(allocator, w, "SET");
        },
        .single_value => |sv| {
            const cols = [_]@import("../../types.zig").Column{.{ .name = sv.col, .type = .string, .nullable = sv.val == null }};
            try result.sendRowDescription(allocator, w, cols[0..]);
            const cells = [_]?[]const u8{sv.val};
            try result.sendDataRow(allocator, w, cells[0..]);
            try sendSelectComplete(allocator, w, 1);
        },
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

fn runEngineQuery(
    allocator: Allocator,
    w: *std.Io.Writer,
    r: *std.Io.Reader,
    catalog: *Catalog,
    session: *SessionState,
    sql_text: []const u8,
) !void {
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();

    const op = try sql.parseWithContext(arena.allocator(), sql_text, .postgres, &catalog.udfs, .{ .registry = &catalog.sql_fns, .db = session.current_db });

    if (op.* == .batch) {
        // PG simple-Query protocol natively supports `;`-separated
        // statements: emit each statement's response packets in turn,
        // then a single ReadyForQuery (sent by handleQuery, not here).
        // Per spec, if any statement errors the remaining ones are
        // skipped; we propagate the error to handleQuery which emits
        // ErrorResponse + ReadyForQuery.
        for (op.batch.statements) |stmt| {
            // COPY can't co-mingle with other statements — its wire
            // protocol takes over the connection until CopyDone.
            if (stmt.* == .copy) return copy.Error.CopyMustBeSoleStatement;
            try runSingleStatement(allocator, w, r, catalog, session, stmt);
        }
        return;
    }

    try runSingleStatement(allocator, w, r, catalog, session, op);
}

/// Run + emit the response packets for ONE statement (RowDescription/
/// DataRow*/CommandComplete for SELECT, or CommandComplete-only for
/// side-effect statements). Caller (or the multi-statement loop) emits
/// the single trailing ReadyForQuery.
fn runSingleStatement(
    allocator: Allocator,
    w: *std.Io.Writer,
    r: *std.Io.Reader,
    catalog: *Catalog,
    session: *SessionState,
    op: *const ir.Op,
) !void {
    // COPY is wire-driven and can't ride the generic compile path —
    // hand it off before we open a CompileCtx.
    if (op.* == .copy) {
        return copy.handleCopy(allocator, w, r, catalog, session.asSession(), op.copy);
    }

    const main_db = catalog.database(session.current_db) orelse return ApiError.DatabaseNotFound;

    if (needsTempNamespace(op.*)) {
        _ = try session.ensureTempNamespace();
    }

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
            .insert, .insert_select => {
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
        .ddl, .insert, .insert_select => true,
        else => false,
    };
}

/// True when an op is a `CREATE TEMP TABLE`. The wire layer
/// pre-allocates the session's temp namespace before compile so
/// the compile path can rely on it being non-null.
fn needsTempNamespace(op: ir.Op) bool {
    return switch (op) {
        .ddl => |d| switch (d) {
            .create_table => |ct| ct.is_temp,
            else => false,
        },
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
            .rename_table => "RENAME TABLE",
            .alter_table_add_column => "ALTER TABLE",
            .truncate_table => "TRUNCATE TABLE",
            .use_schema, .use_database_schema => "SET",
            .create_sql_function => "CREATE FUNCTION",
        .create_zig_function => "CREATE FUNCTION",
            .drop_sql_function => "DROP FUNCTION",
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
    var session = try SessionState.init(allocator, c, 1);
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
    var session = try SessionState.init(allocator, c, 2);
    defer session.deinit();
    try std.testing.expectError(error.DatabaseNotFound, applyDatabase(c, &session, "ghost"));
}
