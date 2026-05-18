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
const TableOptions = thindb_api.TableOptions;
const AlterOp = thindb_api.AlterOp;

const types = @import("../types.zig");
const Schema = types.Schema;

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
    TableAlreadyExists,
    ColumnNotFound,
    ColumnAlreadyExists,
    SchemaMismatch,
    TypeMismatch,
    UnsupportedOp,
    /// Server requires authentication and the client either didn't
    /// present a token or presented an invalid one.
    AuthFailed,
    /// Server-side error the client couldn't map to a known code.
    /// The wire payload still carries the server's error name; pull it
    /// via a future "last error message" accessor (not exposed yet).
    RemoteError,
    /// Wire-format violation reported by the server (malformed request,
    /// unknown message type, etc.).
    BadRequest,
    /// The server sent a response frame whose type the client doesn't
    /// recognize (protocol mismatch, future server feature).
    UnexpectedResponse,
} || ir.Error || wire.Error;

/// Map a typed wire error code into the local Error set. Unknown
/// codes (or `unknown_error`) fall through to `RemoteError`.
pub fn errorFromCode(code: wire.WireErrorCode) Error {
    return switch (code) {
        .table_not_found => Error.TableNotFound,
        .table_already_exists => Error.TableAlreadyExists,
        .column_not_found => Error.ColumnNotFound,
        .column_already_exists => Error.ColumnAlreadyExists,
        .schema_mismatch => Error.SchemaMismatch,
        .type_mismatch => Error.TypeMismatch,
        .unsupported_op => Error.UnsupportedOp,
        .bad_request => Error.BadRequest,
        .auth_failed => Error.AuthFailed,
        .unknown_error => Error.RemoteError,
    };
}

/// Connection — abstracts over transports. Today: in-process (owns a
/// Database that runs server-side queries in the same address space).
/// Future: TCP (owns the remote endpoint; opens a fresh stream per
/// query).
pub const Connection = struct {
    allocator: Allocator,
    io: Io,
    transport: Transport,

    /// Whether to zstd-compress outgoing TCP frames whose payload
    /// exceeds `wire.compression_threshold_bytes`. Default false:
    /// compression costs CPU and is a loss on localhost / fast LANs
    /// where bandwidth isn't the bottleneck. Flip to true for
    /// bandwidth-constrained links (WAN, mobile, etc.). Decompression
    /// is always supported on the read side regardless of this flag.
    compress_writes: bool = false,

    /// Optional shared-secret token sent on every outgoing TCP
    /// connection as a `req_auth` frame *before* the real request.
    /// Caller-owned slice; must outlive the Connection. Null = no
    /// auth (server with a token configured will reject).
    ///
    /// Note: the token rides as plaintext over the wire. This is
    /// "weak" auth — useful for "only the right deployment can
    /// connect", not a substitute for TLS on hostile networks.
    auth_token: ?[]const u8 = null,

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

    // -----------------------------------------------------------------
    // Admin RPCs — wrap the existing Database/Table operations behind
    // the Connection API so client code never reaches past the boundary.
    //
    // Wire framing for these is one-shot: client sends one request,
    // server replies with `resp_ok` (optionally carrying a u64 count) or
    // `resp_error`. Each call opens a fresh TCP stream (stateless model).
    // -----------------------------------------------------------------

    /// Insert rows into a table. Rows have the same shape as
    /// `Table.insert`'s argument: a slice/array/tuple of struct literals
    /// whose fields map to schema columns.
    ///
    /// In-process: forwards to `Table.insert` directly.
    /// TCP: comptime-encodes rows into a columnar wire batch, sends one
    /// frame, server bulk-appends via `Memtable.insertColumnarBatch`.
    pub fn insert(self: *Connection, table_name: []const u8, rows: anytype) !void {
        switch (self.transport) {
            .in_process => |db| {
                const t = db.tables.get(table_name) orelse return Error.TableNotFound;
                try t.insert(rows);
            },
            .tcp => {
                var payload: std.ArrayList(u8) = .empty;
                defer payload.deinit(self.allocator);
                try wire.appendLenString(self.allocator, &payload, table_name);
                try encodeRowsAsBatch(self.allocator, &payload, rows);

                // Insert payloads are typically large — route through
                // the compressing path. sendAdminTcp does both compress
                // (for the request) and decompress (for the response)
                // when respective sizes warrant.
                const resp = try self.sendAdminTcp(.req_insert, payload.items);
                defer self.allocator.free(resp);
                if (resp.len != 8) return Error.UnexpectedResponse;
            },
        }
    }

    /// Create or open a table. If the table exists on disk its schema
    /// must match. `opts.row_group_size` falls back to the server's
    /// configured default. Same semantics as `Database.table`.
    pub fn createTable(
        self: *Connection,
        name: []const u8,
        schema: Schema,
        opts: TableOptions,
    ) !void {
        switch (self.transport) {
            .in_process => |db| {
                _ = try db.table(name, schema, opts);
            },
            .tcp => {
                var payload: std.ArrayList(u8) = .empty;
                defer payload.deinit(self.allocator);
                try wire.appendLenString(self.allocator, &payload, name);
                try wire.encodeSchema(self.allocator, &payload, schema);
                // row_group_size: 1-byte presence flag + u64 value if present.
                if (opts.row_group_size) |rgs| {
                    try payload.append(self.allocator, 1);
                    var buf: [8]u8 = undefined;
                    std.mem.writeInt(u64, &buf, @intCast(rgs), .little);
                    try payload.appendSlice(self.allocator, &buf);
                } else {
                    try payload.append(self.allocator, 0);
                }
                _ = try self.sendAdminTcp(.req_create_table, payload.items);
            },
        }
    }

    /// Drop a table. Removes the in-memory entry, waits for in-flight
    /// scans on this table to finish, then deletes the on-disk directory.
    pub fn dropTable(self: *Connection, name: []const u8) !void {
        switch (self.transport) {
            .in_process => |db| try db.dropTable(name),
            .tcp => {
                var payload: std.ArrayList(u8) = .empty;
                defer payload.deinit(self.allocator);
                try wire.appendLenString(self.allocator, &payload, name);
                _ = try self.sendAdminTcp(.req_drop_table, payload.items);
            },
        }
    }

    /// Apply schema operations to a table. Same semantics as
    /// `Database.alterTable` — caller must not hold active queries on
    /// this table when calling.
    pub fn alterTable(self: *Connection, name: []const u8, ops: []const AlterOp) !void {
        switch (self.transport) {
            .in_process => |db| try db.alterTable(name, ops),
            .tcp => {
                var payload: std.ArrayList(u8) = .empty;
                defer payload.deinit(self.allocator);
                try wire.appendLenString(self.allocator, &payload, name);
                try wire.appendU32(self.allocator, &payload, @intCast(ops.len));
                for (ops) |op| try wire.encodeAlterOp(self.allocator, &payload, op);
                _ = try self.sendAdminTcp(.req_alter_table, payload.items);
            },
        }
    }

    /// Rename a table. Blocks until in-flight scans on the source table
    /// drain (DDL semantics — same as the in-process API).
    pub fn renameTable(self: *Connection, old_name: []const u8, new_name: []const u8) !void {
        switch (self.transport) {
            .in_process => |db| try db.renameTable(old_name, new_name),
            .tcp => {
                var payload: std.ArrayList(u8) = .empty;
                defer payload.deinit(self.allocator);
                try wire.appendLenString(self.allocator, &payload, old_name);
                try wire.appendLenString(self.allocator, &payload, new_name);
                _ = try self.sendAdminTcp(.req_rename_table, payload.items);
            },
        }
    }

    /// Force a flush of the table's memtable to disk. Useful in tests
    /// and at shutdown.
    pub fn flush(self: *Connection, table_name: []const u8) !void {
        switch (self.transport) {
            .in_process => |db| {
                const t = db.tables.get(table_name) orelse return Error.TableNotFound;
                try t.flush();
            },
            .tcp => {
                var payload: std.ArrayList(u8) = .empty;
                defer payload.deinit(self.allocator);
                try wire.appendLenString(self.allocator, &payload, table_name);
                _ = try self.sendAdminTcp(.req_flush, payload.items);
            },
        }
    }

    /// Compact all segments of a table into one. Background compaction
    /// usually handles this; this is the manual escape hatch.
    pub fn compact(self: *Connection, table_name: []const u8) !void {
        switch (self.transport) {
            .in_process => |db| {
                const t = db.tables.get(table_name) orelse return Error.TableNotFound;
                try t.compact();
            },
            .tcp => {
                var payload: std.ArrayList(u8) = .empty;
                defer payload.deinit(self.allocator);
                try wire.appendLenString(self.allocator, &payload, table_name);
                _ = try self.sendAdminTcp(.req_compact, payload.items);
            },
        }
    }

    /// Delete rows matching `pred`. Returns the number of rows deleted.
    /// `pred` is encoded as a full predicate tree on the wire; v1
    /// only honors a single leaf (matches `Table.delete`'s capability).
    /// Tree-shaped predicates surface as `UnsupportedOp` from the server.
    pub fn delete(self: *Connection, table_name: []const u8, pred: PredicateExpr) !usize {
        switch (self.transport) {
            .in_process => |db| {
                const t = db.tables.get(table_name) orelse return Error.TableNotFound;
                // v1 in-process matches v1 wire: only leaf predicates flow
                // through to Table.delete (which takes a scalar Predicate).
                switch (pred) {
                    .leaf => |p| return try t.delete(p),
                    else => return Error.UnsupportedOp,
                }
            },
            .tcp => {
                var payload: std.ArrayList(u8) = .empty;
                defer payload.deinit(self.allocator);
                try wire.appendLenString(self.allocator, &payload, table_name);
                try ir.encodePredicate(self.allocator, &payload, pred);

                const resp = try self.sendAdminTcp(.req_delete, payload.items);
                defer self.allocator.free(resp);
                if (resp.len != 8) return Error.UnexpectedResponse;
                return @intCast(std.mem.readInt(u64, resp[0..8], .little));
            },
        }
    }

    /// If an auth_token is configured, write a `req_auth` frame to
    /// `w` (NOT yet flushed — callers send the real request right
    /// after so the two frames pipeline in one round-trip). No-op
    /// when auth_token is null.
    fn maybeSendAuth(self: *Connection, w: *std.Io.Writer) !void {
        const token = self.auth_token orelse return;
        var payload: std.ArrayList(u8) = .empty;
        defer payload.deinit(self.allocator);
        try wire.appendLenString(self.allocator, &payload, token);
        try wire.writeFrameToIo(w, .req_auth, payload.items);
    }

    /// One-shot admin TCP round-trip. Opens a fresh stream, sends one
    /// request frame, reads the first response frame, returns its
    /// payload (caller-owned, freed by caller). Errors out on
    /// `resp_error`. Used by every admin method's TCP branch.
    fn sendAdminTcp(self: *Connection, msg_type: wire.MsgType, payload: []const u8) ![]u8 {
        const endpoint = self.transport.tcp;
        const stream = try std.Io.net.IpAddress.connect(
            &endpoint.address,
            self.io,
            .{ .mode = .stream, .protocol = .tcp },
        );
        defer stream.close(self.io);

        var read_buf: [4 * 1024]u8 = undefined;
        var write_buf: [4 * 1024]u8 = undefined;
        var reader = stream.reader(self.io, &read_buf);
        var writer = stream.writer(self.io, &write_buf);

        try self.maybeSendAuth(&writer.interface);
        if (self.compress_writes) {
            try wire.writeFrameToIoMaybeCompressed(self.allocator, &writer.interface, msg_type, payload);
        } else {
            try wire.writeFrameToIo(&writer.interface, msg_type, payload);
        }
        try writer.interface.flush();

        const frame = try wire.readFramePayload(self.allocator, &reader.interface);
        // readFramePayload returns owned, uncompressed bytes regardless
        // of whether the server compressed. Cleanup contract:
        //   - resp_ok: hand the buffer to the caller.
        //   - any other arm: free here.
        return switch (@intFromEnum(frame.msg_type)) {
            @intFromEnum(wire.MsgType.resp_ok) => frame.payload,
            @intFromEnum(wire.MsgType.resp_error) => {
                defer self.allocator.free(frame.payload);
                const decoded = wire.decodeError(frame.payload) catch return Error.RemoteError;
                return errorFromCode(decoded.code);
            },
            else => {
                self.allocator.free(frame.payload);
                return Error.UnexpectedResponse;
            },
        };
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

                // Pipeline auth (if configured) before the real
                // request — one round-trip total.
                try self.conn.maybeSendAuth(&tcp.writer.interface);
                if (self.conn.compress_writes) {
                    try wire.writeFrameToIoMaybeCompressed(self.allocator, &tcp.writer.interface, .req_query, encoded.items);
                } else {
                    try wire.writeFrameToIo(&tcp.writer.interface, .req_query, encoded.items);
                }
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

        // readFramePayload handles decompression transparently.
        // Allocates the (uncompressed) payload buffer — we copy into
        // our scratch ArrayList so the buffer lifetimes match the
        // existing pattern (current_batch borrows from scratch).
        const frame = try wire.readFramePayload(self.allocator, &tcp.reader.interface);
        defer self.allocator.free(frame.payload);

        tcp.frame_payload.clearRetainingCapacity();
        try tcp.frame_payload.appendSlice(self.allocator, frame.payload);

        return switch (@intFromEnum(frame.msg_type)) {
            @intFromEnum(wire.MsgType.resp_batch) => blk: {
                tcp.current_batch = try wire.decodeBatch(self.allocator, tcp.frame_payload.items);
                break :blk tcp.current_batch.?.batch();
            },
            @intFromEnum(wire.MsgType.resp_end) => blk: {
                tcp.done = true;
                break :blk null;
            },
            @intFromEnum(wire.MsgType.resp_error) => {
                const decoded = wire.decodeError(tcp.frame_payload.items) catch return Error.RemoteError;
                return errorFromCode(decoded.code);
            },
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
        .compute => |c| blk: {
            var upstream = try buildServerQuery(allocator, db, c.upstream.*);
            errdefer upstream.deinit();
            // Pass the IR Derived slice straight through — Compute.create
            // takes the same shape (it's a re-export).
            break :blk try upstream.compute(c.derived);
        },
        .join => |j| blk: {
            var left = try buildServerQuery(allocator, db, j.left.*);
            errdefer left.deinit();
            const right = try buildServerQuery(allocator, db, j.right.*);
            // No errdefer on right: exec.Query.join consumes both on
            // success AND failure (it always takes ownership for cleanup).
            const spec: ir.JoinSpec = .{
                .join_type = j.join_type,
                .algorithm = j.algorithm,
                .on = j.on,
                .ranges = j.ranges,
                .extra_predicate = j.extra_predicate,
                .skew_ratio_threshold = j.skew_ratio_threshold,
                .skew_absolute_threshold = j.skew_absolute_threshold,
                .skew_sample_interval = j.skew_sample_interval,
            };
            break :blk try left.join(right, spec);
        },
        .materialize => {
            // Plans containing Materialize nodes must go through
            // `compile()` instead — it threads a CompileCtx that
            // shares buffers across multi-references.
            return Error.UnsupportedOp;
        },
    };
}

// ---------------------------------------------------------------------------
// Compile path with materialization support.
//
// Threads a CompileCtx through the recursion so a `*ir.Op.materialize`
// referenced from multiple parents resolves to ONE drained buffer with
// multiple Reader cursors (instead of N independent drains). The ctx
// owns the buffers; CompiledQuery bundles the resulting Query with the
// ctx so deinit tears down both in order.
//
// Plans that contain no `.materialize` nodes route exactly like
// buildServerQuery (the recursive cases are duplicated for now;
// could be unified later via an internal context-taking helper).
// ---------------------------------------------------------------------------

pub const CompileCtx = struct {
    allocator: Allocator,
    db: *Database,
    materialized: std.AutoHashMapUnmanaged(*const ir.Op, *@import("../exec/materialize.zig").MaterializedBuffer) = .empty,

    pub fn deinit(self: *CompileCtx) void {
        var it = self.materialized.iterator();
        while (it.next()) |entry| entry.value_ptr.*.deinit();
        self.materialized.deinit(self.allocator);
    }
};

pub const CompiledQuery = struct {
    query: Query,
    ctx: CompileCtx,

    pub fn deinit(self: *CompiledQuery) void {
        self.query.deinit();
        self.ctx.deinit();
    }

    pub fn next(self: *CompiledQuery) !?exec.Batch {
        return self.query.next();
    }

    pub fn outputSchema(self: *CompiledQuery) []const @import("../types.zig").Column {
        return self.query.outputSchema();
    }
};

pub fn compile(allocator: Allocator, db: *Database, root: *const ir.Op) !CompiledQuery {
    var ctx = CompileCtx{ .allocator = allocator, .db = db };
    errdefer ctx.deinit();
    const q = try compileOp(&ctx, root);
    return .{ .query = q, .ctx = ctx };
}

fn compileOp(ctx: *CompileCtx, op: *const ir.Op) !Query {
    return switch (op.*) {
        .scan => |s| blk: {
            const t = ctx.db.tables.get(s.table_name) orelse return Error.TableNotFound;
            break :blk try exec.scan(ctx.allocator, t);
        },
        .limit => |l| blk: {
            const upstream = try compileOp(ctx, l.upstream);
            break :blk try upstream.limit(@intCast(l.n));
        },
        .select => |s| blk: {
            var upstream = try compileOp(ctx, s.upstream);
            errdefer upstream.deinit();
            break :blk try upstream.project(s.columns);
        },
        .exclude => |e| blk: {
            var upstream = try compileOp(ctx, e.upstream);
            errdefer upstream.deinit();
            const upstream_cols = upstream.outputSchema();
            const remaining = try complementColumns(ctx.allocator, upstream_cols, e.columns);
            defer ctx.allocator.free(remaining);
            break :blk try upstream.project(remaining);
        },
        .filter => |f| blk: {
            var upstream = try compileOp(ctx, f.upstream);
            errdefer upstream.deinit();
            break :blk try upstream.filter(f.predicate);
        },
        .order_by => |o| blk: {
            var upstream = try compileOp(ctx, o.upstream);
            errdefer upstream.deinit();
            break :blk try upstream.orderBy(o.specs);
        },
        .group_by => |g| blk: {
            var upstream = try compileOp(ctx, g.upstream);
            errdefer upstream.deinit();
            break :blk try upstream.groupBy(g.group_cols, g.aggs);
        },
        .compute => |c| blk: {
            var upstream = try compileOp(ctx, c.upstream);
            errdefer upstream.deinit();
            break :blk try upstream.compute(c.derived);
        },
        .join => |j| blk: {
            var left = try compileOp(ctx, j.left);
            errdefer left.deinit();
            const right = try compileOp(ctx, j.right);
            const spec: ir.JoinSpec = .{
                .join_type = j.join_type,
                .algorithm = j.algorithm,
                .on = j.on,
                .ranges = j.ranges,
                .extra_predicate = j.extra_predicate,
                .skew_ratio_threshold = j.skew_ratio_threshold,
                .skew_absolute_threshold = j.skew_absolute_threshold,
                .skew_sample_interval = j.skew_sample_interval,
            };
            break :blk try left.join(right, spec);
        },
        .materialize => |m| blk: {
            const mat = @import("../exec/materialize.zig");
            // Cache hit → another Reader over the existing buffer
            // (no redrain).
            if (ctx.materialized.get(op)) |existing| {
                break :blk try mat.Reader.create(ctx.allocator, existing);
            }
            // First reference: build the upstream, wrap in a buffer,
            // cache, hand back a Reader. The buffer drains lazily
            // when the first reader's next() is called.
            const upstream = try compileOp(ctx, m.upstream);
            const buf = try mat.MaterializedBuffer.init(ctx.allocator, upstream);
            errdefer buf.deinit();
            try ctx.materialized.put(ctx.allocator, op, buf);
            break :blk try mat.Reader.create(ctx.allocator, buf);
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

// ---------------------------------------------------------------------------
// Row → wire batch encoder (TCP insert path)
//
// Walks the user's row struct at comptime, emits the column-major wire
// format that `wire.decodeBatch` consumes on the server. Width-info
// (varchar N, decimal precision) isn't conveyed from Zig types — the
// client always picks the most general tag for a given Zig type, and
// the server's column-family match in `Memtable.insertColumnarBatch`
// accepts string-like tags interchangeably so `[]const u8` lands in
// VARCHAR / STRING / CHAR columns alike.
// ---------------------------------------------------------------------------

fn encodeRowsAsBatch(allocator: Allocator, out: *std.ArrayList(u8), rows: anytype) !void {
    const Rows = @TypeOf(rows);
    const info = @typeInfo(Rows);
    switch (info) {
        .pointer => |p| switch (p.size) {
            .slice => return encodeRowsAsBatchFromIterable(allocator, out, rows),
            .one => {
                const child_info = @typeInfo(p.child);
                if (comptime child_info == .@"struct" and child_info.@"struct".is_tuple) {
                    return encodeRowsAsBatchFromTuple(allocator, out, rows);
                } else if (comptime child_info == .array) {
                    return encodeRowsAsBatchFromIterable(allocator, out, rows);
                } else {
                    @compileError("encodeRowsAsBatch: expected slice/array/tuple, got " ++ @typeName(Rows));
                }
            },
            else => @compileError("encodeRowsAsBatch: unsupported pointer shape " ++ @typeName(Rows)),
        },
        .array => return encodeRowsAsBatchFromIterable(allocator, out, &rows),
        else => @compileError("encodeRowsAsBatch: unsupported rows type " ++ @typeName(Rows)),
    }
}

fn encodeRowsAsBatchFromIterable(
    allocator: Allocator,
    out: *std.ArrayList(u8),
    rows: anytype,
) !void {
    const Row = @TypeOf(rows[0]);
    const fields = @typeInfo(Row).@"struct".fields;
    const row_count = rows.len;

    try wire.appendU32(allocator, out, @intCast(row_count));
    try wire.appendU32(allocator, out, @intCast(fields.len));

    inline for (fields) |field| {
        const Inner = comptime innerType(field.type);
        const is_nullable = comptime isOptionalType(field.type);

        try wire.appendLenString(allocator, out, field.name);
        try out.append(allocator, @intFromEnum(zigTypeToWireTag(Inner)));
        try out.append(allocator, @intFromBool(is_nullable));
        try wire.appendU32(allocator, out, 0); // type_extra unused

        if (is_nullable) {
            try writeOptionalColumnFromRows(Inner, allocator, out, rows, field.name);
        } else {
            try writeColumnFromRows(field.type, allocator, out, rows, field.name);
            try wire.appendU32(allocator, out, 0); // empty null bitmap
        }
    }
}

/// Tuples don't support runtime indexing AND each anonymous struct
/// literal has its own type, so we can't trivially collect to a typed
/// array. Instead, derive the row schema from element 0 and pull each
/// column's values via comptime `@field` extraction from each tuple
/// element independently. Works for any tuple whose elements all
/// declare the same set of named fields with coercible types.
fn encodeRowsAsBatchFromTuple(
    allocator: Allocator,
    out: *std.ArrayList(u8),
    tuple_ptr: anytype,
) !void {
    const TupleT = @TypeOf(tuple_ptr.*);
    const tuple_info = @typeInfo(TupleT).@"struct";
    const tuple_fields = tuple_info.fields;
    if (tuple_fields.len == 0) {
        @compileError("encodeRowsAsBatch: empty tuple has no row type to infer");
    }
    const Row0 = tuple_fields[0].type;
    const row_fields = @typeInfo(Row0).@"struct".fields;

    try wire.appendU32(allocator, out, @intCast(tuple_fields.len));
    try wire.appendU32(allocator, out, @intCast(row_fields.len));

    inline for (row_fields) |rf| {
        const Inner = comptime innerType(rf.type);
        const is_nullable = comptime isOptionalType(rf.type);

        try wire.appendLenString(allocator, out, rf.name);
        try out.append(allocator, @intFromEnum(zigTypeToWireTag(Inner)));
        try out.append(allocator, @intFromBool(is_nullable));
        try wire.appendU32(allocator, out, 0);

        // Per-field collection across tuple elements. Each tuple slot may
        // have its own anonymous-struct type, but `@field(row, rf.name)`
        // works regardless — the value coerces to the schema column type
        // we declared above. For nullable fields we collect Inner +
        // build a validity bitmap in parallel.
        if (is_nullable) {
            var values: std.ArrayList(Inner) = .empty;
            defer values.deinit(allocator);
            try values.ensureTotalCapacity(allocator, tuple_fields.len);

            const bitmap_len = (tuple_fields.len + 7) / 8;
            const bitmap = try allocator.alloc(u8, bitmap_len);
            defer allocator.free(bitmap);
            @memset(bitmap, 0);

            inline for (tuple_fields, 0..) |tf, i| {
                const row = @field(tuple_ptr.*, tf.name);
                const optv = @field(row, rf.name);
                if (optv) |v| {
                    values.appendAssumeCapacity(v);
                    bitmap[i >> 3] |= @as(u8, 1) << @intCast(i & 7);
                } else {
                    values.appendAssumeCapacity(comptime zeroValue(Inner));
                }
            }
            try writeColumnFromTypedSlice(Inner, allocator, out, values.items);
            try wire.appendU32(allocator, out, @intCast(bitmap_len));
            try out.appendSlice(allocator, bitmap);
        } else {
            var typed: std.ArrayList(rf.type) = .empty;
            defer typed.deinit(allocator);
            try typed.ensureTotalCapacity(allocator, tuple_fields.len);
            inline for (tuple_fields) |tf| {
                const row = @field(tuple_ptr.*, tf.name);
                typed.appendAssumeCapacity(@field(row, rf.name));
            }
            try writeColumnFromTypedSlice(rf.type, allocator, out, typed.items);
            try wire.appendU32(allocator, out, 0); // empty null bitmap
        }
    }
}

fn isOptionalType(comptime T: type) bool {
    return @typeInfo(T) == .optional;
}

fn innerType(comptime T: type) type {
    return switch (@typeInfo(T)) {
        .optional => |o| o.child,
        else => T,
    };
}

/// Type-appropriate placeholder written into the data slot for a NULL
/// row. Server ignores it (bitmap clears the valid bit) — only here so
/// the data section keeps a fixed row stride for fixed-width types and
/// a zero-length offset slot for strings.
fn zeroValue(comptime T: type) T {
    return switch (T) {
        i8, i16, i32, i64, i128, u128 => 0,
        bool => false,
        f32 => @as(f32, 0.0),
        f64 => @as(f64, 0.0),
        []const u8, []u8 => &.{},
        else => @compileError("zeroValue: unsupported " ++ @typeName(T)),
    };
}

fn writeOptionalColumnFromRows(
    comptime Inner: type,
    allocator: Allocator,
    out: *std.ArrayList(u8),
    rows: anytype,
    comptime field_name: []const u8,
) !void {
    var values: std.ArrayList(Inner) = .empty;
    defer values.deinit(allocator);
    try values.ensureTotalCapacity(allocator, rows.len);

    const bitmap_len = (rows.len + 7) / 8;
    const bitmap = try allocator.alloc(u8, bitmap_len);
    defer allocator.free(bitmap);
    @memset(bitmap, 0);

    for (rows, 0..) |row, i| {
        const optv = @field(row, field_name);
        if (optv) |v| {
            values.appendAssumeCapacity(v);
            bitmap[i >> 3] |= @as(u8, 1) << @intCast(i & 7);
        } else {
            values.appendAssumeCapacity(comptime zeroValue(Inner));
        }
    }

    try writeColumnFromTypedSlice(Inner, allocator, out, values.items);
    try wire.appendU32(allocator, out, @intCast(bitmap_len));
    try out.appendSlice(allocator, bitmap);
}

fn writeColumnFromTypedSlice(
    comptime T: type,
    allocator: Allocator,
    out: *std.ArrayList(u8),
    values: []const T,
) !void {
    switch (T) {
        i8, i16, i32, i64, i128, u128 => {
            const size = @sizeOf(T);
            try wire.appendU32(allocator, out, @intCast(values.len * size));
            for (values) |v| {
                var buf: [size]u8 = undefined;
                std.mem.writeInt(T, &buf, v, .little);
                try out.appendSlice(allocator, &buf);
            }
        },
        bool => {
            try wire.appendU32(allocator, out, @intCast(values.len));
            for (values) |v| try out.append(allocator, @intFromBool(v));
        },
        f32 => {
            try wire.appendU32(allocator, out, @intCast(values.len * 4));
            for (values) |v| {
                var buf: [4]u8 = undefined;
                std.mem.writeInt(u32, &buf, @bitCast(v), .little);
                try out.appendSlice(allocator, &buf);
            }
        },
        f64 => {
            try wire.appendU32(allocator, out, @intCast(values.len * 8));
            for (values) |v| {
                var buf: [8]u8 = undefined;
                std.mem.writeInt(u64, &buf, @bitCast(v), .little);
                try out.appendSlice(allocator, &buf);
            }
        },
        []const u8, []u8 => {
            const row_count: u32 = @intCast(values.len);
            try wire.appendU32(allocator, out, (row_count + 1) * 4);
            var byte_total: u32 = 0;
            var off_buf: [4]u8 = undefined;
            std.mem.writeInt(u32, &off_buf, 0, .little);
            try out.appendSlice(allocator, &off_buf);
            for (values) |s| {
                byte_total += @intCast(s.len);
                std.mem.writeInt(u32, &off_buf, byte_total, .little);
                try out.appendSlice(allocator, &off_buf);
            }
            try wire.appendU32(allocator, out, byte_total);
            for (values) |s| try out.appendSlice(allocator, s);
        },
        else => @compileError("writeColumnFromTypedSlice: unsupported type " ++ @typeName(T)),
    }
}

fn zigTypeToWireTag(comptime T: type) @import("../types.zig").TypeTag {
    return switch (T) {
        i8 => .tinyint,
        i16 => .smallint,
        i32 => .int,
        i64 => .bigint,
        i128 => .largeint,
        u128 => .uuid,
        bool => .boolean,
        f32 => .float,
        f64 => .double,
        []const u8, []u8 => .string,
        else => @compileError("encodeRowsAsBatch: unsupported field type " ++ @typeName(T)),
    };
}

fn writeColumnFromRows(
    comptime T: type,
    allocator: Allocator,
    out: *std.ArrayList(u8),
    rows: anytype,
    comptime field_name: []const u8,
) !void {
    switch (T) {
        i8, i16, i32, i64, i128, u128 => {
            const size = @sizeOf(T);
            try wire.appendU32(allocator, out, @intCast(rows.len * size));
            for (rows) |row| {
                var buf: [size]u8 = undefined;
                std.mem.writeInt(T, &buf, @field(row, field_name), .little);
                try out.appendSlice(allocator, &buf);
            }
        },
        bool => {
            try wire.appendU32(allocator, out, @intCast(rows.len));
            for (rows) |row| try out.append(allocator, @intFromBool(@field(row, field_name)));
        },
        f32 => {
            try wire.appendU32(allocator, out, @intCast(rows.len * 4));
            for (rows) |row| {
                const v: f32 = @field(row, field_name);
                var buf: [4]u8 = undefined;
                std.mem.writeInt(u32, &buf, @bitCast(v), .little);
                try out.appendSlice(allocator, &buf);
            }
        },
        f64 => {
            try wire.appendU32(allocator, out, @intCast(rows.len * 8));
            for (rows) |row| {
                const v: f64 = @field(row, field_name);
                var buf: [8]u8 = undefined;
                std.mem.writeInt(u64, &buf, @bitCast(v), .little);
                try out.appendSlice(allocator, &buf);
            }
        },
        []const u8, []u8 => {
            // offsets: row_count+1 entries × 4 bytes
            const row_count: u32 = @intCast(rows.len);
            try wire.appendU32(allocator, out, (row_count + 1) * 4);
            var byte_total: u32 = 0;
            var off_buf: [4]u8 = undefined;
            std.mem.writeInt(u32, &off_buf, 0, .little);
            try out.appendSlice(allocator, &off_buf);
            for (rows) |row| {
                const s = @field(row, field_name);
                byte_total += @intCast(s.len);
                std.mem.writeInt(u32, &off_buf, byte_total, .little);
                try out.appendSlice(allocator, &off_buf);
            }
            try wire.appendU32(allocator, out, byte_total);
            for (rows) |row| try out.appendSlice(allocator, @field(row, field_name));
        },
        else => @compileError("writeColumnFromRows: unsupported field type " ++ @typeName(T)),
    }
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
