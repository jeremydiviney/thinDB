//! TCP server. `thindb.serveTcp(...)` returns a Server that accepts
//! connections on a TCP port and handles them via the same buildServerQuery
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
const Allocator = std.mem.Allocator;
const Io = std.Io;

const thindb_api = @import("../api/api.zig");
const Database = thindb_api.Database;
const Config = thindb_api.Config;

const exec = @import("../exec/exec.zig");
const Query = exec.Query;

const ir = @import("../ir/ir.zig");
const wire = @import("wire.zig");
const local = @import("local.zig");

pub const Error = error{
    ServerClosed,
} || local.Error;

pub const Server = struct {
    allocator: Allocator,
    io: Io,
    db: *Database,
    listener: std.Io.net.Server,

    pub fn close(self: *Server) void {
        self.listener.socket.close(self.io);
        self.db.close();
        self.allocator.destroy(self);
    }

    /// Accept one connection and handle it inline. Returns when the
    /// connection is fully drained (resp_end written and stream closed).
    /// Errors from the handler don't take the server down — they're
    /// reported as resp_error frames to the client.
    pub fn acceptOne(self: *Server) !void {
        const stream = try self.listener.accept(self.io);
        defer stream.close(self.io);
        try handleConnection(self.allocator, self.io, self.db, stream);
    }

    /// Long-running accept loop. Breaks when `should_stop` flips to true
    /// AFTER an accept returns (so currently in-flight connections drain).
    pub fn run(self: *Server, should_stop: *std.atomic.Value(bool)) !void {
        while (!should_stop.load(.acquire)) {
            self.acceptOne() catch |err| {
                // Don't bring the server down on a per-connection error.
                std.debug.print("tcp_server: connection error: {s}\n", .{@errorName(err)});
            };
        }
    }
};

/// Open a Database at `data_dir`, start listening on `address`. Caller
/// owns the returned *Server and must call `server.close()`.
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

    const self = try allocator.create(Server);
    self.* = .{
        .allocator = allocator,
        .io = io,
        .db = db,
        .listener = listener,
    };
    return self;
}

// ---------------------------------------------------------------------------
// Per-connection handler
// ---------------------------------------------------------------------------

fn handleConnection(
    allocator: Allocator,
    io: Io,
    db: *Database,
    stream: std.Io.net.Stream,
) !void {
    var read_buf: [8 * 1024]u8 = undefined;
    var write_buf: [8 * 1024]u8 = undefined;
    var reader = stream.reader(io, &read_buf);
    var writer = stream.writer(io, &write_buf);

    // Read one frame — the request.
    var hdr: [wire.frame_header_size]u8 = undefined;
    try reader.interface.readSliceAll(&hdr);
    const msg_type_byte = hdr[0];
    const payload_len = std.mem.readInt(u32, hdr[4..8], .little);

    const payload = try allocator.alloc(u8, payload_len);
    defer allocator.free(payload);
    try reader.interface.readSliceAll(payload);

    // Dispatch on request type. Read-path streams batches via
    // handleQuery; admin/write requests reply with a single resp_ok
    // (optionally carrying a small payload) or a resp_error.
    switch (msg_type_byte) {
        @intFromEnum(wire.MsgType.req_query) => {
            handleQuery(allocator, db, payload, &writer.interface) catch |err| {
                try sendError(&writer.interface, @errorName(err));
            };
        },
        @intFromEnum(wire.MsgType.req_create_table) => {
            handleCreateTable(allocator, db, payload, &writer.interface) catch |err| {
                try sendError(&writer.interface, @errorName(err));
            };
        },
        @intFromEnum(wire.MsgType.req_drop_table) => {
            handleDropTable(db, payload, &writer.interface) catch |err| {
                try sendError(&writer.interface, @errorName(err));
            };
        },
        @intFromEnum(wire.MsgType.req_rename_table) => {
            handleRenameTable(db, payload, &writer.interface) catch |err| {
                try sendError(&writer.interface, @errorName(err));
            };
        },
        @intFromEnum(wire.MsgType.req_alter_table) => {
            handleAlterTable(allocator, db, payload, &writer.interface) catch |err| {
                try sendError(&writer.interface, @errorName(err));
            };
        },
        @intFromEnum(wire.MsgType.req_flush) => {
            handleFlush(db, payload, &writer.interface) catch |err| {
                try sendError(&writer.interface, @errorName(err));
            };
        },
        @intFromEnum(wire.MsgType.req_compact) => {
            handleCompact(db, payload, &writer.interface) catch |err| {
                try sendError(&writer.interface, @errorName(err));
            };
        },
        @intFromEnum(wire.MsgType.req_delete) => {
            handleDelete(allocator, db, payload, &writer.interface) catch |err| {
                try sendError(&writer.interface, @errorName(err));
            };
        },
        else => {
            try sendError(&writer.interface, "unknown request type");
        },
    }
    try writer.interface.flush();
}

fn handleQuery(
    allocator: Allocator,
    db: *Database,
    ir_bytes: []const u8,
    writer: *std.Io.Writer,
) !void {
    // Decode the operator tree.
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const op = try ir.decode(arena.allocator(), ir_bytes);

    // Build the server-side query.
    var server_query = try local.buildServerQuery(allocator, db, op);
    defer server_query.deinit();

    // Stream batches.
    var enc: std.ArrayList(u8) = .empty;
    defer enc.deinit(allocator);
    while (try server_query.next()) |batch| {
        enc.clearRetainingCapacity();
        try wire.encodeBatch(allocator, &enc, batch);
        try wire.writeFrameToIo(writer, .resp_batch, enc.items);
    }

    try wire.writeFrameToIo(writer, .resp_end, &.{});
}

fn sendError(writer: *std.Io.Writer, msg: []const u8) !void {
    try wire.writeFrameToIo(writer, .resp_error, msg);
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
    const t = db.tables.get(name) orelse return local.Error.TableNotFound;
    try t.flush();
    try sendOk(writer);
}

fn handleCompact(db: *Database, payload: []const u8, writer: *std.Io.Writer) !void {
    var cursor: usize = 0;
    const name = try wire.readLenString(payload, &cursor);
    const t = db.tables.get(name) orelse return local.Error.TableNotFound;
    try t.compact();
    try sendOk(writer);
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

    const t = db.tables.get(name) orelse return local.Error.TableNotFound;
    const deleted = try t.delete(leaf);

    var resp_payload: [8]u8 = undefined;
    std.mem.writeInt(u64, &resp_payload, @intCast(deleted), .little);
    try wire.writeFrameToIo(writer, .resp_ok, &resp_payload);
}
