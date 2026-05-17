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
const PredicateExpr = exec.PredicateExpr;
const SortSpec = exec.SortSpec;
const AggSpec = exec.AggSpec;

const ir = @import("../ir/ir.zig");
const wire = @import("wire.zig");

pub const Error = error{
    TableNotFound,
    UnsupportedOp,
    /// The server returned a resp_error frame. (Detailed error info is
    /// in the frame payload; surfacing it as a typed error is future work.)
    RemoteError,
    /// The server sent a response frame whose type the client doesn't
    /// recognize (protocol mismatch, future server feature).
    UnexpectedResponse,
} || ir.Error || wire.Error;

/// Connection — abstracts over transports. Today: in-process (owns a
/// Database that runs server-side queries in the same address space).
/// Future: TCP (owns the remote endpoint; opens a fresh stream per
/// query).
pub const Connection = struct {
    allocator: Allocator,
    io: Io,
    transport: Transport,

    pub const Transport = union(enum) {
        /// In-process — server runs in this process. The Connection
        /// owns the Database.
        in_process: *Database,
        /// TCP — server runs in another process at this address.
        /// Each query opens a fresh stream (stateless RPC model).
        tcp: TcpEndpoint,
    };

    pub const TcpEndpoint = struct {
        address: std.Io.net.IpAddress,
    };

    pub fn close(self: *Connection) void {
        switch (self.transport) {
            .in_process => |db| db.close(),
            .tcp => {}, // no persistent connection-level state to release
        }
        self.allocator.destroy(self);
    }

    /// SQL-flavored alias for `scan`. Same semantics; pick whichever
    /// reads better at the call site.
    pub fn from(self: *Connection, table_name: []const u8) !ClientQuery {
        return self.scan(table_name);
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
            .state = null,
        };
    }
};

/// Client-side query builder + executor handle. Holds an IR tree that
/// grows as the user chains operators, plus (after the first call to
/// `next()`) transport-specific execution state.
pub const ClientQuery = struct {
    allocator: Allocator,
    conn: *Connection,
    root: ir.Op,
    /// Heap-allocated so builder methods can return new ClientQuery values
    /// sharing the same underlying arena. Freed by `deinit`.
    arena: *std.heap.ArenaAllocator,
    /// Execution state. Null until `next()` triggers `dispatch()`. Variant
    /// matches the Connection's transport.
    state: ?ExecutingState = null,

    pub const ExecutingState = union(enum) {
        in_process: InProcessState,
        /// Heap-allocated so the Reader/Writer interface pointers inside
        /// it stay valid across union-variant assignments.
        tcp: *TcpState,
    };

    pub const InProcessState = struct {
        /// Server-side operator tree we drive via `q.next()`.
        server_query: Query,
        /// Encoded IR bytes kept alive: the decoded operator tree borrows
        /// string slices (column names, text values) from these bytes.
        /// ArrayList rather than raw slice so we own a stable address —
        /// `toOwnedSlice` is allowed to relocate the backing buffer.
        encoded_buffer: std.ArrayList(u8),
    };

    pub const TcpState = struct {
        stream: std.Io.net.Stream,
        /// Stream Reader/Writer with their own backing buffers. The
        /// interface's vtable closures use `@fieldParentPtr` so the
        /// Reader/Writer struct itself must live at a stable address —
        /// that's why TcpState is heap-allocated.
        read_buf: []u8,
        write_buf: []u8,
        reader: std.Io.net.Stream.Reader,
        writer: std.Io.net.Stream.Writer,
        /// Scratch buffer used to assemble each incoming frame payload.
        frame_payload: std.ArrayList(u8),
        /// Decoded current batch — kept alive so the Batch we returned
        /// to the user via `next()` stays valid until the next call.
        current_batch: ?wire.DecodedBatch = null,
        /// Once `resp_end` arrives, future next() calls return null.
        done: bool = false,
    };

    pub fn deinit(self: *ClientQuery) void {
        if (self.state) |*s| switch (s.*) {
            .in_process => |*ip| {
                ip.server_query.deinit();
                ip.encoded_buffer.deinit(self.allocator);
            },
            .tcp => |tcp| {
                if (tcp.current_batch) |*b| b.deinit();
                tcp.frame_payload.deinit(self.allocator);
                tcp.stream.close(self.conn.io);
                self.allocator.free(tcp.read_buf);
                self.allocator.free(tcp.write_buf);
                self.allocator.destroy(tcp);
            },
        };
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
    ///
    /// `select(&.{})` is `SELECT *` — empty list means "keep upstream's
    /// schema unchanged." We short-circuit by returning self as-is; no
    /// IR node is added, no work is done.
    pub fn select(self: ClientQuery, fields: []const []const u8) !ClientQuery {
        if (fields.len == 0) return self;
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

    /// Predicate filter. SQL-flavored canonical name; `.filter()` is the
    /// alias for code that prefers the previous library spelling.
    pub fn where(self: ClientQuery, expr: PredicateExpr) !ClientQuery {
        var copy = self;
        const aa = copy.arena.allocator();
        const prev_root = copy.root;
        const owned = try clonePredicate(aa, expr);
        const upstream = try aa.create(ir.Op);
        upstream.* = prev_root;
        copy.root = .{ .filter = .{ .predicate = owned, .upstream = upstream } };
        return copy;
    }

    /// Alias for `where`. Keeps existing-style call sites compiling.
    pub fn filter(self: ClientQuery, expr: PredicateExpr) !ClientQuery {
        return self.where(expr);
    }

    /// Sort upstream rows. Multi-column with per-key ASC/DESC. Blocking
    /// at the server — sort materializes the full input before emitting.
    pub fn orderBy(self: ClientQuery, specs: []const SortSpec) !ClientQuery {
        var copy = self;
        const aa = copy.arena.allocator();
        const prev_root = copy.root;
        // Dupe column names + the spec array into the arena so the
        // client-side IR tree owns its strings (same convention as
        // select/exclude/where).
        const dup = try aa.alloc(SortSpec, specs.len);
        for (specs, 0..) |s, i| dup[i] = .{
            .col = try aa.dupe(u8, s.col),
            .desc = s.desc,
        };
        const upstream = try aa.create(ir.Op);
        upstream.* = prev_root;
        copy.root = .{ .order_by = .{ .specs = dup, .upstream = upstream } };
        return copy;
    }

    /// Grouped aggregation. Each AggSpec names its output column via `.as`.
    /// `group_cols` lists the upstream columns to group by; one row per
    /// distinct group. Empty `group_cols` = global aggregate (see
    /// `.aggregate(...)`).
    pub fn groupBy(
        self: ClientQuery,
        group_cols: []const []const u8,
        aggs: []const AggSpec,
    ) !ClientQuery {
        var copy = self;
        const aa = copy.arena.allocator();
        const prev_root = copy.root;

        const grp_dup = try aa.alloc([]const u8, group_cols.len);
        for (group_cols, 0..) |c, i| grp_dup[i] = try aa.dupe(u8, c);

        const aggs_dup = try aa.alloc(AggSpec, aggs.len);
        for (aggs, 0..) |a, i| aggs_dup[i] = .{
            .func = a.func,
            .col = if (a.col) |c| try aa.dupe(u8, c) else null,
            .as = try aa.dupe(u8, a.as),
        };

        const upstream = try aa.create(ir.Op);
        upstream.* = prev_root;
        copy.root = .{ .group_by = .{
            .group_cols = grp_dup,
            .aggs = aggs_dup,
            .upstream = upstream,
        } };
        return copy;
    }

    /// Global aggregate (no group keys). Sugar for `.groupBy(&.{}, aggs)`.
    pub fn aggregate(self: ClientQuery, aggs: []const AggSpec) !ClientQuery {
        return self.groupBy(&.{}, aggs);
    }

    /// Compose a sub-pipeline. `f` takes the current ClientQuery and
    /// returns a new one. Pure function composition — `pipe(a).pipe(b)`
    /// runs a then b; `pipe(closure-that-itself-uses-pipe)` nests.
    ///
    /// Mirrors the existing `exec.Query.pipe`. Same plan-time semantic:
    /// the IR is extended by whatever `f` adds, just like writing the
    /// chain inline.
    pub fn pipe(self: ClientQuery, f: anytype) !ClientQuery {
        return f(self);
    }

    /// Pull the next batch. On first call, dispatches to the appropriate
    /// transport (in-process: builds server-side query directly; TCP:
    /// opens stream and sends a req_query frame). On subsequent calls,
    /// just pulls from whichever state we set up.
    pub fn next(self: *ClientQuery) !?Batch {
        if (self.state == null) try self.dispatch();
        return switch (self.state.?) {
            .in_process => |*ip| try ip.server_query.next(),
            .tcp => |tcp| try self.tcpNext(tcp),
        };
    }

    fn dispatch(self: *ClientQuery) !void {
        // Encode IR — same encoding regardless of transport.
        var encoded: std.ArrayList(u8) = .empty;
        errdefer encoded.deinit(self.allocator);
        try ir.encode(self.allocator, &encoded, self.root);

        switch (self.conn.transport) {
            .in_process => |db| {
                // In-process: decode the IR back, build the server-side
                // query, hold both. The decoded tree's children allocs
                // land in the arena (ClientQuery-lifetime); string slices
                // borrow from encoded.items, which we keep alive in
                // the InProcessState.
                const decoded = try ir.decode(self.arena.allocator(), encoded.items);
                const sq = try buildServerQuery(self.allocator, db, decoded);
                self.state = .{ .in_process = .{
                    .server_query = sq,
                    .encoded_buffer = encoded,
                } };
            },
            .tcp => |endpoint| {
                // TCP: open a stream, send the request, set up read state.
                // Each query gets its own stream (stateless RPC model).
                const tcp = try self.allocator.create(TcpState);
                errdefer self.allocator.destroy(tcp);

                const stream = try std.Io.net.IpAddress.connect(
                    &endpoint.address,
                    self.conn.io,
                    .{ .mode = .stream, .protocol = .tcp },
                );
                errdefer stream.close(self.conn.io);

                const read_buf = try self.allocator.alloc(u8, 8 * 1024);
                errdefer self.allocator.free(read_buf);
                const write_buf = try self.allocator.alloc(u8, 8 * 1024);
                errdefer self.allocator.free(write_buf);

                tcp.* = .{
                    .stream = stream,
                    .read_buf = read_buf,
                    .write_buf = write_buf,
                    .reader = stream.reader(self.conn.io, read_buf),
                    .writer = stream.writer(self.conn.io, write_buf),
                    .frame_payload = .empty,
                };

                // Write the request frame.
                try wire.writeFrameToIo(&tcp.writer.interface, .req_query, encoded.items);
                try tcp.writer.interface.flush();

                // We can drop the encoded IR now — the server has its own copy.
                encoded.deinit(self.allocator);

                self.state = .{ .tcp = tcp };
            },
        }
    }

    /// Read the next response frame and yield a Batch (or null on
    /// resp_end). Errors out on resp_error frames.
    fn tcpNext(self: *ClientQuery, tcp: *TcpState) !?Batch {
        if (tcp.done) return null;

        // Release the previous batch's storage now that the caller is
        // asking for the next one (mirrors how other operators reuse
        // buffers across next() calls).
        if (tcp.current_batch) |*b| {
            b.deinit();
            tcp.current_batch = null;
        }

        // Read a frame header.
        var hdr: [wire.frame_header_size]u8 = undefined;
        try tcp.reader.interface.readSliceAll(&hdr);
        const tag_byte = hdr[0];
        const payload_len = std.mem.readInt(u32, hdr[4..8], .little);

        // Read payload into our scratch buffer.
        tcp.frame_payload.clearRetainingCapacity();
        try tcp.frame_payload.resize(self.allocator, payload_len);
        try tcp.reader.interface.readSliceAll(tcp.frame_payload.items);

        // Dispatch on type.
        return switch (tag_byte) {
            @intFromEnum(wire.MsgType.resp_batch) => blk: {
                tcp.current_batch = try wire.decodeBatch(self.allocator, tcp.frame_payload.items);
                break :blk tcp.current_batch.?.batch();
            },
            @intFromEnum(wire.MsgType.resp_end) => blk: {
                tcp.done = true;
                break :blk null;
            },
            @intFromEnum(wire.MsgType.resp_error) => return Error.RemoteError,
            else => return Error.UnexpectedResponse,
        };
    }
};

/// Deep-clone a PredicateExpr into the given arena allocator. All strings
/// (column names + `.text` value bytes) and children arrays are dup'd so
/// the cloned predicate has no borrowed pointers — the arena owns
/// everything, freed at ClientQuery.deinit.
fn clonePredicate(aa: Allocator, expr: PredicateExpr) Allocator.Error!PredicateExpr {
    return switch (expr) {
        .leaf => |p| PredicateExpr{ .leaf = .{
            .col = try aa.dupe(u8, p.col),
            .op = p.op,
            .val = try cloneValue(aa, p.val),
        } },
        .is_null => |col| PredicateExpr{ .is_null = try aa.dupe(u8, col) },
        .is_not_null => |col| PredicateExpr{ .is_not_null = try aa.dupe(u8, col) },
        .@"and" => |children| PredicateExpr{ .@"and" = try cloneChildren(aa, children) },
        .@"or" => |children| PredicateExpr{ .@"or" = try cloneChildren(aa, children) },
        .not => |child| blk: {
            const dup = try aa.create(PredicateExpr);
            dup.* = try clonePredicate(aa, child.*);
            break :blk PredicateExpr{ .not = dup };
        },
    };
}

fn cloneChildren(aa: Allocator, children: []const PredicateExpr) Allocator.Error![]PredicateExpr {
    const out = try aa.alloc(PredicateExpr, children.len);
    for (children, 0..) |c, i| out[i] = try clonePredicate(aa, c);
    return out;
}

fn cloneValue(aa: Allocator, v: @import("../types.zig").Value) Allocator.Error!@import("../types.zig").Value {
    return switch (v) {
        .text => |s| .{ .text = try aa.dupe(u8, s) },
        else => v, // fixed-width values are value-typed; no allocation
    };
}

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
pub fn buildServerQuery(allocator: Allocator, db: *Database, op: ir.Op) !Query {
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
        .filter => |f| blk: {
            var upstream = try buildServerQuery(allocator, db, f.upstream.*);
            errdefer upstream.deinit();
            // exec.Query.filter validates the predicate against upstream's
            // schema and pushes leaves through top-level ANDs down to Scan
            // for row-group pruning.
            break :blk try upstream.filter(f.predicate);
        },
        .order_by => |o| blk: {
            var upstream = try buildServerQuery(allocator, db, o.upstream.*);
            errdefer upstream.deinit();
            break :blk try upstream.orderBy(o.specs);
        },
        .group_by => |g| blk: {
            var upstream = try buildServerQuery(allocator, db, g.upstream.*);
            errdefer upstream.deinit();
            // exec.Query.groupBy with empty group_cols is the same operator
            // used by exec.Query.aggregate (global aggregate).
            break :blk try upstream.groupBy(g.group_cols, g.aggs);
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
    conn.* = .{
        .allocator = allocator,
        .io = io,
        .transport = .{ .in_process = db },
    };
    return conn;
}

/// Open a TCP Connection. Each query opens a fresh stream to `address`
/// (stateless RPC model — server handles the request, streams batches
/// back, closes the stream).
pub fn connect(
    allocator: Allocator,
    io: Io,
    address: std.Io.net.IpAddress,
) !*Connection {
    const conn = try allocator.create(Connection);
    conn.* = .{
        .allocator = allocator,
        .io = io,
        .transport = .{ .tcp = .{ .address = address } },
    };
    return conn;
}

/// Expose the underlying Database for setup operations (creating tables,
/// inserting seed data) that don't yet have a client-facing API. The
/// long-term plan is to move table creation + writes through the
/// Connection too, but for the walking skeleton the existing
/// `db.table(...)` / `t.insert(...)` API is the way to seed test data.
///
/// Asserts the connection is in-process — TCP connections have no
/// local Database to return.
pub fn underlyingDb(conn: *Connection) *Database {
    return switch (conn.transport) {
        .in_process => |db| db,
        .tcp => @panic("underlyingDb called on a TCP connection"),
    };
}
