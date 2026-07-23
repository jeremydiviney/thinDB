//! TCP server. `thindb.serveTcp(...)` returns a Server that accepts
//! connections on a TCP port and handles them via the same compileWithSession
//! path the in-process Connection uses. One query per connection (stateless
//! RPC); the connection closes after the resp_end frame is written.
//!
//! Lifecycle:
//!   - `serveTcp(allocator, io, data_dir, address, cfg)` opens a Database
//!     and starts listening. Returns a *Server.
//!   - `server.boundAddress()` reports the actual bound port (useful when
//!     the caller passed port=0 to get an ephemeral port).
//!   - `server.acceptOne()` handles one connection synchronously. Tests
//!     use this in a worker thread.
//!   - `server.run(should_stop)` is the long-running accept loop. Sets
//!     a non-cancellable accept; use should_stop atomic to ask it to
//!     wind down between connections.
//!   - `server.close()` deinits the Database and the listening socket.

const std = @import("std");
const builtin = @import("builtin");
const Allocator = std.mem.Allocator;
const Io = std.Io;

const thindb_api = @import("../api/api.zig");
const Database = thindb_api.Database;
const Catalog = thindb_api.Catalog;
const Config = thindb_api.Config;

const database_mod = @import("../api/database.zig");
const back_compat_database_name = database_mod.back_compat_database_name;

const ir = @import("../ir/ir.zig");
const wire = @import("wire.zig");
const local = @import("local.zig");
const ConnectionLimiter = @import("conn_limit.zig").ConnectionLimiter;
const sock_opts = @import("sock_opts.zig");

pub const Error = error{
    ServerClosed,
} || local.Error;

pub const Server = struct {
    allocator: Allocator,
    io: Io,
    db: *Database,
    listener: std.Io.net.Server,
    /// True when this Server owns `db` (created via `serveTcp`); false
    /// when the caller passed in a Catalog-rooted Database via
    /// `serveTcpCatalog` and is responsible for that Database's lifetime.
    owns_db: bool,
    limiter: *ConnectionLimiter,
    owns_limiter: bool,
    idle_timeout_secs: u32 = 0,

    /// Whether to zstd-compress outgoing batch frames whose payload
    /// exceeds `wire.compression_threshold_bytes`. Default false — see
    /// the matching field on Connection for the rationale (localhost
    /// pays CPU without bandwidth gain). Flip after `serveTcp` returns
    /// for bandwidth-constrained deployments. Decompression on incoming
    /// requests is always supported regardless of this flag.
    compress_writes: bool = false,

    /// Optional shared-secret token required of every client. When
    /// null, the server accepts unauthenticated connections (the
    /// default — convenient for local dev). When set, the very first
    /// frame on each connection MUST be a `req_auth` whose payload
    /// matches this token bytes-for-bytes, or the server replies
    /// `resp_error(auth_failed)` and closes. Caller-owned slice;
    /// must outlive the Server.
    auth_token: ?[]const u8 = null,

    /// Wake any thread blocked in `accept` and close the listening socket.
    /// POSIX close() does not interrupt an in-flight accept, so shut the
    /// socket down first. Frees nothing — call `destroy` after joining
    /// the accept thread.
    pub fn close(self: *Server) void {
        if (builtin.os.tag != .windows) {
            // Windows closesocket aborts a pending accept on its own; the
            // shutdown there fails with a noisy NTSTATUS on a listener.
            const listen_stream: std.Io.net.Stream = .{ .socket = self.listener.socket };
            listen_stream.shutdown(self.io, .both) catch {};
        }
        self.listener.socket.close(self.io);
    }

    pub fn destroy(self: *Server) void {
        if (self.owns_db) self.db.close();
        if (self.owns_limiter) self.allocator.destroy(self.limiter);
        self.allocator.destroy(self);
    }

    /// Accept one connection and handle it inline. Returns when the
    /// connection is fully drained (resp_end written and stream closed).
    /// Errors from the handler don't take the server down — they're
    /// reported as resp_error frames to the client.
    pub fn acceptOne(self: *Server) !void {
        const stream = try self.listener.accept(self.io);
        defer stream.close(self.io);

        _ = sock_opts.enableKeepalive(stream.socket.handle);
        if (self.idle_timeout_secs > 0) {
            _ = sock_opts.setReadTimeoutSeconds(stream.socket.handle, self.idle_timeout_secs);
        }

        if (!self.limiter.acquire()) {
            sendTooManyConnectionsNative(self.allocator, stream, self.io) catch {};
            return;
        }
        defer self.limiter.release();

        try handleConnection(self.allocator, self.io, self.db, stream, self.compress_writes, self.auth_token);
    }

    /// Long-running accept loop. Each connection runs on its own thread
    /// so multiple clients can be in-flight concurrently. Breaks when
    /// `should_stop` flips to true AFTER an accept returns (so currently
    /// in-flight connections drain).
    pub fn run(self: *Server, should_stop: *std.atomic.Value(bool)) !void {
        while (!should_stop.load(.acquire)) {
            const stream = self.listener.accept(self.io) catch |err| {
                if (should_stop.load(.acquire)) return;
                std.debug.print("tcp_server: accept error: {s}\n", .{@errorName(err)});
                continue;
            };
            _ = sock_opts.enableKeepalive(stream.socket.handle);
            if (self.idle_timeout_secs > 0) {
                _ = sock_opts.setReadTimeoutSeconds(stream.socket.handle, self.idle_timeout_secs);
            }
            if (!self.limiter.acquire()) {
                sendTooManyConnectionsNative(self.allocator, stream, self.io) catch {};
                stream.close(self.io);
                continue;
            }
            const job = self.allocator.create(ConnJob) catch {
                self.limiter.release();
                stream.close(self.io);
                continue;
            };
            job.* = .{
                .allocator = self.allocator,
                .io = self.io,
                .db = self.db,
                .stream = stream,
                .compress_writes = self.compress_writes,
                .auth_token = self.auth_token,
                .limiter = self.limiter,
            };
            // Query compilation recurses per IR op; a deep CTE chain compiled
            // as ONE block (a ~130-CTE statement's inlined closure)
            // overflows the default 16 MiB stack — silent segfault
            // on Windows. Reserve big; pages commit lazily, so idle cost ~0.
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
    db: *Database,
    stream: std.Io.net.Stream,
    compress_writes: bool,
    auth_token: ?[]const u8,
    limiter: *ConnectionLimiter,

    fn run(self: *ConnJob) void {
        defer self.allocator.destroy(self);
        defer self.limiter.release();
        defer self.stream.close(self.io);
        handleConnection(self.allocator, self.io, self.db, self.stream, self.compress_writes, self.auth_token) catch |err| {
            std.debug.print("tcp_server: connection error: {s}\n", .{@errorName(err)});
        };
    }
};

/// Open a Database at `data_dir`, start listening on `address`. Caller
/// owns the returned *Server and must call `server.close()`. The Server
/// owns the internal Database — `close()` tears it down.
pub fn serveTcp(
    allocator: Allocator,
    io: Io,
    data_dir: Io.Dir,
    address: std.Io.net.IpAddress,
    config: Config,
) !*Server {
    const db = try Database.open(allocator, io, data_dir, config);
    errdefer db.close();

    var listen_addr = address;
    const listener = try std.Io.net.IpAddress.listen(&listen_addr, io, .{
        .mode = .stream,
        .protocol = .tcp,
        .reuse_address = true,
    });

    const limiter = try allocator.create(ConnectionLimiter);
    errdefer allocator.destroy(limiter);
    limiter.* = ConnectionLimiter.init(config.max_connections);

    const self = try allocator.create(Server);
    self.* = .{
        .allocator = allocator,
        .io = io,
        .db = db,
        .listener = listener,
        .owns_db = true,
        .limiter = limiter,
        .owns_limiter = true,
        .idle_timeout_secs = config.idle_timeout_secs,
    };
    return self;
}

/// Bind a native-wire listener on `address` that serves queries against
/// the "main" database inside an existing Catalog. Unlike `serveTcp`,
/// the caller owns the Catalog (and therefore the Database) and must
/// keep it alive for the Server's lifetime. Use this when several wire
/// protocols need to share one on-disk dataset.
/// When `limiter` is null the Server allocates a private one sized from
/// `catalog.config.max_connections`; pass an external limiter
/// (caller-owned) to share a single budget across multiple wires.
pub fn serveTcpCatalog(
    allocator: Allocator,
    io: Io,
    catalog: *Catalog,
    address: std.Io.net.IpAddress,
    limiter: ?*ConnectionLimiter,
) !*Server {
    const db = catalog.database(back_compat_database_name) orelse
        try catalog.createDatabase(back_compat_database_name);

    var listen_addr = address;
    const listener = try std.Io.net.IpAddress.listen(&listen_addr, io, .{
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
        .db = db,
        .listener = listener,
        .owns_db = false,
        .limiter = effective_limiter,
        .owns_limiter = limiter == null,
        .idle_timeout_secs = catalog.config.idle_timeout_secs,
    };
    return self;
}

/// Emit resp_error(too_many_connections) and close. Best-effort.
fn sendTooManyConnectionsNative(
    allocator: Allocator,
    stream: std.Io.net.Stream,
    io: Io,
) !void {
    var write_buf: [256]u8 = undefined;
    var writer = stream.writer(io, &write_buf);
    const w = &writer.interface;
    var payload: std.ArrayList(u8) = .empty;
    defer payload.deinit(allocator);
    try wire.encodeError(allocator, &payload, .unknown_error, "too many connections");
    try wire.writeFrameToIo(w, .resp_error, payload.items);
    try w.flush();
}

// ---------------------------------------------------------------------------
// Per-connection handler
// ---------------------------------------------------------------------------

fn handleConnection(
    allocator: Allocator,
    io: Io,
    db: *Database,
    stream: std.Io.net.Stream,
    compress_writes: bool,
    auth_token: ?[]const u8,
) !void {
    var read_buf: [8 * 1024]u8 = undefined;
    var write_buf: [8 * 1024]u8 = undefined;
    var reader = stream.reader(io, &read_buf);
    var writer = stream.writer(io, &write_buf);

    // Read one frame. If it's req_auth, validate and read the next
    // frame as the real request. Otherwise treat it as the request
    // (and reject if the server requires auth).
    var frame = try wire.readFramePayload(allocator, &reader.interface);
    defer allocator.free(frame.payload);

    if (frame.msg_type == .req_auth) {
        // Token format: [len u32][bytes]. Use readLenString to parse.
        var cursor: usize = 0;
        const presented = wire.readLenString(frame.payload, &cursor) catch {
            try sendError(allocator, &writer.interface, error.BadRequest);
            try writer.interface.flush();
            return;
        };
        if (auth_token) |required| {
            if (!std.mem.eql(u8, presented, required)) {
                try sendError(allocator, &writer.interface, error.AuthFailed);
                try writer.interface.flush();
                return;
            }
        }
        // Auth OK (or server has no token configured and just ignores
        // the presented one). Read the next frame as the real request.
        allocator.free(frame.payload);
        frame = try wire.readFramePayload(allocator, &reader.interface);
    } else if (auth_token != null) {
        // Server requires auth but client didn't send req_auth first.
        try sendError(allocator, &writer.interface, error.AuthFailed);
        try writer.interface.flush();
        return;
    }

    const payload = frame.payload;

    // Dispatch on request type. Read-path streams batches via
    // handleQuery; admin/write requests reply with a single resp_ok
    // (optionally carrying a small payload) or a resp_error.
    switch (@intFromEnum(frame.msg_type)) {
        @intFromEnum(wire.MsgType.req_query) => {
            handleQuery(allocator, db, payload, &writer.interface, compress_writes) catch |err| {
                try sendError(allocator, &writer.interface, err);
            };
        },
        @intFromEnum(wire.MsgType.req_create_table) => {
            handleCreateTable(allocator, db, payload, &writer.interface) catch |err| {
                try sendError(allocator, &writer.interface, err);
            };
        },
        @intFromEnum(wire.MsgType.req_drop_table) => {
            handleDropTable(db, payload, &writer.interface) catch |err| {
                try sendError(allocator, &writer.interface, err);
            };
        },
        @intFromEnum(wire.MsgType.req_rename_table) => {
            handleRenameTable(db, payload, &writer.interface) catch |err| {
                try sendError(allocator, &writer.interface, err);
            };
        },
        @intFromEnum(wire.MsgType.req_alter_table) => {
            handleAlterTable(allocator, db, payload, &writer.interface) catch |err| {
                try sendError(allocator, &writer.interface, err);
            };
        },
        @intFromEnum(wire.MsgType.req_flush) => {
            handleFlush(db, payload, &writer.interface) catch |err| {
                try sendError(allocator, &writer.interface, err);
            };
        },
        @intFromEnum(wire.MsgType.req_compact) => {
            handleCompact(db, payload, &writer.interface) catch |err| {
                try sendError(allocator, &writer.interface, err);
            };
        },
        @intFromEnum(wire.MsgType.req_delete) => {
            handleDelete(allocator, db, payload, &writer.interface) catch |err| {
                try sendError(allocator, &writer.interface, err);
            };
        },
        @intFromEnum(wire.MsgType.req_insert) => {
            handleInsert(allocator, db, payload, &writer.interface) catch |err| {
                try sendError(allocator, &writer.interface, err);
            };
        },
        else => {
            try sendError(allocator, &writer.interface, error.UnsupportedOp);
        },
    }
    try writer.interface.flush();
}

fn handleQuery(
    allocator: Allocator,
    db: *Database,
    ir_bytes: []const u8,
    writer: *std.Io.Writer,
    compress_writes: bool,
) !void {
    // Decode the operator tree.
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    var op = try ir.decode(arena.allocator(), ir_bytes);

    // Compile the server-side query (V2-first, same as the SQL frontends).
    var server_query = try local.compileWithSession(allocator, db, .{}, &op);
    defer server_query.deinit();

    // Stream batches — resp_batch payloads are typically large
    // (multi-KB up to MB per row group), so route through the
    // opportunistic-compression writer.
    var enc: std.ArrayList(u8) = .empty;
    defer enc.deinit(allocator);
    while (try server_query.next()) |batch| {
        enc.clearRetainingCapacity();
        try wire.encodeBatch(allocator, &enc, batch);
        if (compress_writes) {
            try wire.writeFrameToIoMaybeCompressed(allocator, writer, .resp_batch, enc.items);
        } else {
            try wire.writeFrameToIo(writer, .resp_batch, enc.items);
        }
    }

    try wire.writeFrameToIo(writer, .resp_end, &.{});
}

/// Send a resp_error frame carrying both a typed WireErrorCode and a
/// human-readable message (the original error name). Client maps the
/// code back to a local typed Error.
fn sendError(allocator: Allocator, writer: *std.Io.Writer, err: anyerror) !void {
    const code = wire.codeFromError(err);
    const msg = @errorName(err);
    var payload: std.ArrayList(u8) = .empty;
    defer payload.deinit(allocator);
    try wire.encodeError(allocator, &payload, code, msg);
    try wire.writeFrameToIo(writer, .resp_error, payload.items);
}

fn sendOk(writer: *std.Io.Writer) !void {
    try wire.writeFrameToIo(writer, .resp_ok, &.{});
}

fn handleCreateTable(
    allocator: Allocator,
    db: *Database,
    payload: []const u8,
    writer: *std.Io.Writer,
) !void {
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const aa = arena.allocator();

    var cursor: usize = 0;
    const name = try wire.readLenString(payload, &cursor);
    const schema = try wire.decodeSchema(aa, payload, &cursor);

    if (cursor + 1 > payload.len) return wire.Error.WireCorrupt;
    const has_rgs = payload[cursor] != 0;
    cursor += 1;
    var rgs: ?usize = null;
    if (has_rgs) {
        if (cursor + 8 > payload.len) return wire.Error.WireCorrupt;
        rgs = @intCast(std.mem.readInt(u64, payload[cursor..][0..8], .little));
        cursor += 8;
    }

    const opts: @import("../api/api.zig").TableOptions = .{
        .order_key = schema.order_key,
        .unique = schema.unique,
        .row_group_size = rgs,
    };

    _ = try db.table(name, schema, opts);
    try sendOk(writer);
}

fn handleDropTable(db: *Database, payload: []const u8, writer: *std.Io.Writer) !void {
    var cursor: usize = 0;
    const name = try wire.readLenString(payload, &cursor);
    try db.dropTable(name);
    try sendOk(writer);
}

fn handleAlterTable(
    allocator: Allocator,
    db: *Database,
    payload: []const u8,
    writer: *std.Io.Writer,
) !void {
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const aa = arena.allocator();

    var cursor: usize = 0;
    const name = try wire.readLenString(payload, &cursor);

    if (cursor + 4 > payload.len) return wire.Error.WireCorrupt;
    const n_ops = std.mem.readInt(u32, payload[cursor..][0..4], .little);
    cursor += 4;

    const ops = try aa.alloc(thindb_api.AlterOp, n_ops);
    for (ops) |*op| op.* = try wire.decodeAlterOp(aa, payload, &cursor);

    try db.alterTable(name, ops);
    try sendOk(writer);
}

fn handleRenameTable(db: *Database, payload: []const u8, writer: *std.Io.Writer) !void {
    var cursor: usize = 0;
    const old_name = try wire.readLenString(payload, &cursor);
    const new_name = try wire.readLenString(payload, &cursor);
    try db.renameTable(old_name, new_name);
    try sendOk(writer);
}

fn handleFlush(db: *Database, payload: []const u8, writer: *std.Io.Writer) !void {
    var cursor: usize = 0;
    const name = try wire.readLenString(payload, &cursor);
    const t = db.findTable(name) orelse return local.Error.TableNotFound;
    try t.flush();
    try sendOk(writer);
}

fn handleCompact(db: *Database, payload: []const u8, writer: *std.Io.Writer) !void {
    var cursor: usize = 0;
    const name = try wire.readLenString(payload, &cursor);
    const t = db.findTable(name) orelse return local.Error.TableNotFound;
    try t.compact();
    try sendOk(writer);
}

fn handleInsert(
    allocator: Allocator,
    db: *Database,
    payload: []const u8,
    writer: *std.Io.Writer,
) !void {
    var cursor: usize = 0;
    const name = try wire.readLenString(payload, &cursor);
    const t = db.findTable(name) orelse return local.Error.TableNotFound;

    var decoded = try wire.decodeBatch(allocator, payload[cursor..]);
    defer decoded.deinit();

    try t.insertBatch(decoded.schema, decoded.views, decoded.row_count);

    var resp_payload: [8]u8 = undefined;
    std.mem.writeInt(u64, &resp_payload, @intCast(decoded.row_count), .little);
    try wire.writeFrameToIo(writer, .resp_ok, &resp_payload);
}

fn handleDelete(
    allocator: Allocator,
    db: *Database,
    payload: []const u8,
    writer: *std.Io.Writer,
) !void {
    var cursor: usize = 0;
    const name = try wire.readLenString(payload, &cursor);

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const aa = arena.allocator();

    const expr = try ir.decodePredicate(aa, payload, &cursor);
    // v1: Table.delete takes a scalar Predicate. Reject tree shapes
    // until that API gains tree support.
    const leaf = switch (expr) {
        .leaf => |p| p,
        else => return local.Error.UnsupportedOp,
    };

    const t = db.findTable(name) orelse return local.Error.TableNotFound;
    const deleted = try t.delete(leaf);

    var resp_payload: [8]u8 = undefined;
    std.mem.writeInt(u64, &resp_payload, @intCast(deleted), .little);
    try wire.writeFrameToIo(writer, .resp_ok, &resp_payload);
}
