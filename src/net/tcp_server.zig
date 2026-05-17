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

    // Dispatch on request type. Today: just QUERY. Future: DELETE,
    // INSERT, CREATE_TABLE, etc.
    switch (msg_type_byte) {
        @intFromEnum(wire.MsgType.req_query) => {
            handleQuery(allocator, db, payload, &writer.interface) catch |err| {
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
