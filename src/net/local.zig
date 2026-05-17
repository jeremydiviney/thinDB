//! In-process transport — the foundation of thinDB's client/server split.
//!
//! `thindb.local(...)` returns a `Connection`. From a user's perspective
//! a Connection is the only handle they touch: `conn.scan("orders").limit(5)`
//! builds a query, `q.next()` streams Batches back. Whether the bytes
//! travel through an in-process channel or a TCP socket is a transport
//! detail.
//!
//! For the walking skeleton (Scan + Limit only) we:
//!   1. Client builds an IR tree.
//!   2. ClientQuery.next() (first call) encodes the IR to bytes,
//!      hands them to the server dispatcher (in the same process,
//!      same allocator).
//!   3. Dispatcher decodes IR, builds the existing exec.Query operator
//!      tree, returns it.
//!   4. Each subsequent ClientQuery.next() pulls a Batch from the
//!      server-side Query.
//!
//! In-process specifically passes Batch values directly (no batch
//! wire-encoding yet). When we add TCP, batch encoding lands; the
//! client API surface doesn't change.

const std = @import("std");
const Allocator = std.mem.Allocator;
const Io = std.Io;

const thindb_api = @import("../api/api.zig");
const Database = thindb_api.Database;
const Config = thindb_api.Config;

const exec = @import("../exec/exec.zig");
const Query = exec.Query;
const Batch = exec.Batch;

const ir = @import("../ir/ir.zig");

pub const Error = error{
    TableNotFound,
    UnsupportedOp,
} || ir.Error;

/// Connection — owns an in-process Database. Closing the Connection
/// closes the Database.
pub const Connection = struct {
    allocator: Allocator,
    io: Io,
    db: *Database,

    pub fn close(self: *Connection) void {
        self.db.close();
        self.allocator.destroy(self);
    }

    /// Start a query against `table_name`. Returns a ClientQuery that
    /// the caller extends (`.limit`, future `.filter`, etc.) and drains
    /// via `.next()`. Caller `.deinit()`s the returned query.
    pub fn scan(self: *Connection, table_name: []const u8) ClientQuery {
        return .{
            .allocator = self.allocator,
            .conn = self,
            .root = .{ .scan = .{ .table_name = table_name } },
            // Trees of nested ops allocate their non-root nodes on this
            // arena so the user doesn't have to think about ownership.
            .arena = std.heap.ArenaAllocator.init(self.allocator),
            .server_query = null,
        };
    }
};

/// Client-side query builder + executor handle. Holds an IR tree that
/// grows as the user chains operators, plus (after the first call to
/// `next()`) a handle to the running server-side Query.
pub const ClientQuery = struct {
    allocator: Allocator,
    conn: *Connection,
    root: ir.Op,
    arena: std.heap.ArenaAllocator,
    server_query: ?Query,

    pub fn deinit(self: *ClientQuery) void {
        if (self.server_query) |*q| q.deinit();
        self.arena.deinit();
    }

    /// Wrap the current root in a Limit. Returns self (mutated) by value
    /// so the typical chain `conn.scan("t").limit(5).next()` reads
    /// naturally. The previous root is moved into a fresh arena-owned
    /// node, kept alive by self.arena.
    pub fn limit(self: ClientQuery, n: u64) !ClientQuery {
        var copy = self;
        const upstream = try copy.arena.allocator().create(ir.Op);
        upstream.* = copy.root;
        copy.root = .{ .limit = .{ .n = n, .upstream = upstream } };
        return copy;
    }

    /// Pull the next batch. On first call, serializes IR + dispatches to
    /// the server-side executor; on subsequent calls just pulls from the
    /// server Query.
    pub fn next(self: *ClientQuery) !?Batch {
        if (self.server_query == null) {
            try self.dispatch();
        }
        return try self.server_query.?.next();
    }

    fn dispatch(self: *ClientQuery) !void {
        // Encode IR — exercises the wire path even though we're staying
        // in-process. When we add TCP, the bytes will flow over a socket
        // instead of straight back into a decoder.
        var encoded: std.ArrayList(u8) = .empty;
        defer encoded.deinit(self.allocator);
        try ir.encode(self.allocator, &encoded, self.root);

        // Decode + build the server-side Query.
        var decoded = try ir.decode(self.allocator, encoded.items);
        defer decoded.deinitDecoded(self.allocator);

        self.server_query = try buildServerQuery(self.allocator, self.conn.db, decoded);
    }
};

/// Server-side IR dispatcher. Recursively walks the decoded IR tree and
/// builds the corresponding exec.Query operator chain using existing
/// in-process operators.
fn buildServerQuery(allocator: Allocator, db: *Database, op: ir.Op) !Query {
    return switch (op) {
        .scan => |s| blk: {
            const t = db.tables.get(s.table_name) orelse return Error.TableNotFound;
            break :blk try exec.scan(allocator, t);
        },
        .limit => |l| blk: {
            const upstream = try buildServerQuery(allocator, db, l.upstream.*);
            // exec.Query.limit takes usize; cast safely.
            break :blk try upstream.limit(@intCast(l.n));
        },
    };
}

/// Open an in-process Connection. Equivalent to opening a Database, but
/// surfaces only the client-facing API. Going forward, ALL queries
/// should flow through a Connection (in-process or eventual TCP).
pub fn local(
    allocator: Allocator,
    io: Io,
    data_dir: Io.Dir,
    config: Config,
) !*Connection {
    const db = try Database.open(allocator, io, data_dir, config);
    errdefer db.close();
    const conn = try allocator.create(Connection);
    conn.* = .{ .allocator = allocator, .io = io, .db = db };
    return conn;
}

/// Expose the underlying Database for setup operations (creating tables,
/// inserting seed data) that don't yet have a client-facing API. The
/// long-term plan is to move table creation + writes through the
/// Connection too, but for the walking skeleton the existing
/// `db.table(...)` / `t.insert(...)` API is the way to seed test data.
pub fn underlyingDb(conn: *Connection) *Database {
    return conn.db;
}
