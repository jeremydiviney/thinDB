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
    pub fn scan(self: *Connection, table_name: []const u8) !ClientQuery {
        // Heap-allocate the arena so builder methods can clone the
        // ClientQuery value (the chain `conn.scan(t).limit(5).select(...)`
        // returns successive values; each shares the same underlying
        // arena via this pointer).
        const arena = try self.allocator.create(std.heap.ArenaAllocator);
        arena.* = std.heap.ArenaAllocator.init(self.allocator);
        return .{
            .allocator = self.allocator,
            .conn = self,
            .root = .{ .scan = .{ .table_name = table_name } },
            .arena = arena,
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
    /// Heap-allocated so builder methods can return new ClientQuery values
    /// sharing the same underlying arena. Freed by `deinit`.
    arena: *std.heap.ArenaAllocator,
    server_query: ?Query,

    pub fn deinit(self: *ClientQuery) void {
        if (self.server_query) |*q| q.deinit();
        self.arena.deinit();
        self.allocator.destroy(self.arena);
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

    /// Keep only `fields`, in the listed order. Errors if any field isn't
    /// present in upstream's schema (validated server-side at dispatch).
    pub fn select(self: ClientQuery, fields: []const []const u8) !ClientQuery {
        var copy = self;
        const aa = copy.arena.allocator();
        // Snapshot the previous root BEFORE constructing the new one.
        // Zig's result-location semantics may initialize `copy.root`'s tag
        // in place for the LHS struct literal, before evaluating the RHS;
        // if we read `copy.root` inside the RHS we'd see the partially-
        // mutated value (new tag, stale payload).
        const prev_root = copy.root;
        copy.root = .{ .select = try cloneProject(aa, prev_root, fields) };
        return copy;
    }

    /// Drop `fields` from the working schema. Strict pipeline semantics:
    /// downstream operators cannot reference the dropped fields. Errors
    /// if any field isn't present in upstream's schema (validated server-
    /// side at dispatch).
    pub fn exclude(self: ClientQuery, fields: []const []const u8) !ClientQuery {
        var copy = self;
        const aa = copy.arena.allocator();
        const prev_root = copy.root;
        copy.root = .{ .exclude = try cloneProject(aa, prev_root, fields) };
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

/// Helper used by `.select()` and `.exclude()`: move the current root
/// into the arena as the new upstream, dupe `fields` into arena-owned
/// storage, return the Project carrier. Caller chooses whether to wrap
/// it in `.select` or `.exclude`.
///
/// We dupe the bytes (not just slice headers) because callers commonly
/// pass `&.{ "a", "b" }` — anonymous-tuple coercions whose backing
/// temporary may not outlive the call expression. Owning the strings
/// ourselves makes the lifetime obvious: arena until `deinit`.
fn cloneProject(
    aa: Allocator,
    current_root: ir.Op,
    fields: []const []const u8,
) !ir.Op.Project {
    const upstream = try aa.create(ir.Op);
    upstream.* = current_root;
    const cols = try aa.alloc([]const u8, fields.len);
    for (fields, 0..) |f, i| cols[i] = try aa.dupe(u8, f);
    return .{ .columns = cols, .upstream = upstream };
}

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
        .select => |s| blk: {
            var upstream = try buildServerQuery(allocator, db, s.upstream.*);
            // If project() fails (e.g. column doesn't exist in upstream's
            // schema), tear the upstream down to avoid a leak.
            errdefer upstream.deinit();
            // .project already validates column refs against upstream's
            // outputSchema — any missing column surfaces as an error.
            break :blk try upstream.project(s.columns);
        },
        .exclude => |e| blk: {
            var upstream = try buildServerQuery(allocator, db, e.upstream.*);
            errdefer upstream.deinit();
            // Translate exclude into a project of the complement: take
            // upstream's current schema, drop the excluded columns, keep
            // the rest in their original order. Strict pipeline semantic
            // is enforced by the existing operators — once we project to
            // the complement, downstream cannot reference the dropped
            // columns.
            const upstream_cols = upstream.outputSchema();
            const remaining = try complementColumns(allocator, upstream_cols, e.columns);
            defer allocator.free(remaining);
            break :blk try upstream.project(remaining);
        },
    };
}

/// Allocator-owned slice of column-name slices. Caller frees via
/// `allocator.free(out)` once — the inner string slices are borrowed
/// from `upstream_cols` and don't need separate freeing.
fn complementColumns(
    allocator: Allocator,
    upstream_cols: []const @import("../types.zig").Column,
    excluded: []const []const u8,
) ![][]const u8 {
    var out: std.ArrayList([]const u8) = .empty;
    errdefer out.deinit(allocator);
    for (upstream_cols) |c| {
        var dropped = false;
        for (excluded) |ex| {
            if (std.mem.eql(u8, c.name, ex)) {
                dropped = true;
                break;
            }
        }
        if (!dropped) try out.append(allocator, c.name);
    }
    return try out.toOwnedSlice(allocator);
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
