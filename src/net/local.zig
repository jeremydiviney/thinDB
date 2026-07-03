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
const Catalog = thindb_api.Catalog;
const DbSchema = thindb_api.Schema;
const Config = thindb_api.Config;
const Session = thindb_api.Session;
const TableOptions = thindb_api.TableOptions;
const AlterOp = thindb_api.AlterOp;
const ApiError = thindb_api.Error;
const ApiTable = thindb_api.Table;

const types = @import("../types.zig");
const TableSchema = types.TableSchema;
const Value = types.Value;

const exec = @import("../exec/exec.zig");
const group_route = @import("../exec/group_route.zig");
const engine_v2 = @import("../exec/engine_v2.zig");
const cte_stages = @import("cte_stages.zig");
const Query = exec.Query;
const Batch = exec.Batch;
const PredicateExpr = exec.PredicateExpr;
const SortSpec = exec.SortSpec;
const AggSpec = exec.AggSpec;

const storage = @import("../storage/storage.zig");

const ir = @import("../ir/ir.zig");
const wire = @import("wire.zig");
const wire_format = @import("wire_format.zig");
const subquery_resolve = @import("subquery_resolve.zig");
const predicate_pushdown = @import("predicate_pushdown.zig");
const const_fold = @import("const_fold.zig");
const pgcat = @import("pg_catalog.zig");

pub const Error = error{
    TableNotFound,
    TableAlreadyExists,
    ColumnNotFound,
    ColumnAlreadyExists,
    SchemaMismatch,
    TypeMismatch,
    UnsupportedOp,
    DatabaseNotFound,
    DatabaseAlreadyExists,
    SchemaNotFound,
    SchemaAlreadyExists,
    FunctionAlreadyExists,
    FunctionInvalidDefinition,
    /// In-flight query was cancelled by a peer (MySQL KILL or PG
    /// CancelRequest / pg_cancel_backend). Caller should map this to
    /// the protocol-specific abort code and continue serving the
    /// connection (KILL doesn't close the socket).
    QueryCancelled,
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
                const t = db.findTable(table_name) orelse return Error.TableNotFound;
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
        schema: TableSchema,
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
                const t = db.findTable(table_name) orelse return Error.TableNotFound;
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
                const t = db.findTable(table_name) orelse return Error.TableNotFound;
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
                const t = db.findTable(table_name) orelse return Error.TableNotFound;
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
            .root = .{ .scan = .{ .table = .{ .name = table_name } } },
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
        /// Server-side compiled query (operator tree + CompileCtx) we
        /// drive via `next()`.
        server_query: CompiledQuery,
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
        for (aggs, 0..) |a, i| {
            const udf_arg_cols = try aa.alloc([]const u8, a.udf_arg_cols.len);
            for (a.udf_arg_cols, udf_arg_cols) |c, *dst| dst.* = try aa.dupe(u8, c);
            aggs_dup[i] = .{
                .func = a.func,
                .udf_name = if (a.udf_name) |n| try aa.dupe(u8, n) else null,
                .udf_arg_cols = udf_arg_cols,
                .col = if (a.col) |c| try aa.dupe(u8, c) else null,
                .as = try aa.dupe(u8, a.as),
                .params = a.params,
                .out_type_override = a.out_type_override,
            };
        }

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
                // In-process: decode the IR back, compile the server-side
                // query, hold both. The decoded tree's children allocs
                // land in the arena (ClientQuery-lifetime); string slices
                // borrow from encoded.items, which we keep alive in
                // the InProcessState.
                var decoded = try ir.decode(self.arena.allocator(), encoded.items);
                const cq = try compileWithSession(self.allocator, db, .{}, &decoded);
                self.state = .{ .in_process = .{
                    .server_query = cq,
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
        .day_leaf => |p| PredicateExpr{ .day_leaf = .{
            .col = try aa.dupe(u8, p.col),
            .op = p.op,
            .val = try cloneValue(aa, p.val),
        } },
        .leaf_col_col => |lc| PredicateExpr{ .leaf_col_col = .{
            .left = try aa.dupe(u8, lc.left),
            .op = lc.op,
            .right = try aa.dupe(u8, lc.right),
        } },
        .is_null => |col| PredicateExpr{ .is_null = try aa.dupe(u8, col) },
        .is_not_null => |col| PredicateExpr{ .is_not_null = try aa.dupe(u8, col) },
        .like => |lp| PredicateExpr{ .like = .{
            .col = try aa.dupe(u8, lp.col),
            .pattern = try aa.dupe(u8, lp.pattern),
        } },
        .scalar_subquery => |sq| PredicateExpr{ .scalar_subquery = .{
            .col = try aa.dupe(u8, sq.col),
            .op = sq.op,
            .source = sq.source,
        } },
        .exists_subquery => |src| PredicateExpr{ .exists_subquery = src },
        .always => |b| PredicateExpr{ .always = b },
        .unknown => .unknown,
        .in_subquery => |s| PredicateExpr{ .in_subquery = .{
            .col = try aa.dupe(u8, s.col),
            .source = s.source,
            .negate = s.negate,
        } },
        .in_set => |s| blk: {
            const vals = try aa.alloc(types.Value, s.values.len);
            for (s.values, vals) |v, *out| out.* = try cloneValue(aa, v);
            break :blk PredicateExpr{ .in_set = .{
                .col = try aa.dupe(u8, s.col),
                .values = vals,
                .negate = s.negate,
            } };
        },
        .correlated_set => |s| blk: {
            const outer_cols = try aa.alloc([]const u8, s.outer_cols.len);
            for (s.outer_cols, outer_cols) |src, *dst| dst.* = try aa.dupe(u8, src);
            const rows = try aa.alloc([]const types.Value, s.rows.len);
            for (s.rows, rows) |src, *dst| {
                const tuple = try aa.alloc(types.Value, src.len);
                for (src, tuple) |v, *t| t.* = try cloneValue(aa, v);
                dst.* = tuple;
            }
            break :blk PredicateExpr{ .correlated_set = .{
                .outer_cols = outer_cols,
                .rows = rows,
                .negate = s.negate,
            } };
        },
        .correlated_scalar => |s| blk: {
            const PredMod = @import("../exec/predicate.zig");
            const outer_keys = try aa.alloc([]const u8, s.outer_keys.len);
            for (s.outer_keys, outer_keys) |src, *dst| dst.* = try aa.dupe(u8, src);
            const rows = try aa.alloc(PredMod.CorrelatedScalarRow, s.rows.len);
            for (s.rows, rows) |src, *dst| {
                const key = try aa.alloc(types.Value, src.key.len);
                for (src.key, key) |v, *k| k.* = try cloneValue(aa, v);
                dst.* = .{ .key = key, .value = try cloneValue(aa, src.value) };
            }
            break :blk PredicateExpr{ .correlated_scalar = .{
                .outer_compared = try aa.dupe(u8, s.outer_compared),
                .op = s.op,
                .outer_keys = outer_keys,
                .rows = rows,
            } };
        },
        .correlated_range => |s| blk: {
            const PredMod = @import("../exec/predicate.zig");
            const outer_keys = try aa.alloc([]const u8, s.outer_keys.len);
            for (s.outer_keys, outer_keys) |src, *dst| dst.* = try aa.dupe(u8, src);
            const groups = try aa.alloc(PredMod.CorrelatedRangeGroup, s.groups.len);
            for (s.groups, groups) |src, *dst| {
                const key = try aa.alloc(types.Value, src.key.len);
                for (src.key, key) |v, *k| k.* = try cloneValue(aa, v);
                const values = try aa.alloc(types.Value, src.values.len);
                for (src.values, values) |v, *o| o.* = try cloneValue(aa, v);
                dst.* = .{ .key = key, .values = values };
            }
            const upper_col_dup: ?[]const u8 = if (s.outer_range_col_upper) |c| try aa.dupe(u8, c) else null;
            break :blk PredicateExpr{ .correlated_range = .{
                .outer_keys = outer_keys,
                .outer_range_col = try aa.dupe(u8, s.outer_range_col),
                .op = s.op,
                .outer_range_col_upper = upper_col_dup,
                .op_upper = s.op_upper,
                .groups = groups,
                .negate = s.negate,
            } };
        },
        .@"and" => |children| PredicateExpr{ .@"and" = try cloneChildren(aa, children) },
        .@"or" => |children| PredicateExpr{ .@"or" = try cloneChildren(aa, children) },
        .not => |child| blk: {
            const dup = try aa.create(PredicateExpr);
            dup.* = try clonePredicate(aa, child.*);
            break :blk PredicateExpr{ .not = dup };
        },
        .leaf_var => |v| PredicateExpr{ .leaf_var = .{
            .col = try aa.dupe(u8, v.col),
            .op = v.op,
            .var_name = try aa.dupe(u8, v.var_name),
        } },
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

/// Pick the *Catalog this Database hangs off of — non-owning `catalog`
/// pointer first (set by `Catalog.createDatabase`), then the implicit
/// `owned_catalog` (set by the back-compat `Database.open` shim).
pub fn catalogFor(db: *Database) ?*Catalog {
    if (db.catalog) |c| return c;
    if (db.owned_catalog) |c| return c;
    return null;
}

/// Resolve a `TableRef` against the active Session. Null segments fall
/// back to `session.current_db` / `session.current_schema`. Each
/// resolution step returns a precise typed error so callers can map
/// failures back to user-facing messages.
///
/// Unqualified refs (no database / schema) consult the session's temp
/// namespace first, so a temp table named `foo` shadows any persistent
/// table named `foo` for the creating session only.
pub fn resolveTable(catalog: *Catalog, session: Session, ref: ir.TableRef) !*ApiTable {
    if (ref.database == null and ref.schema == null) {
        if (session.temp_namespace) |ns| {
            if (ns.findTable(ref.name)) |t| return t;
        }
    }
    var db_name: []const u8 = ref.database orelse session.current_db;
    var schema_name: []const u8 = ref.schema orelse session.current_schema;
    // MySQL-style `db__schema.table` arrives here as ref.schema = "db__schema",
    // ref.database = null. Flatten it back to (db, schema) so the resolver
    // doesn't go hunting for a literal schema named "db__schema".
    if (ref.database == null and ref.schema != null) {
        if (splitDoubleUnderscore(ref.schema.?)) |parts| {
            db_name = parts.db;
            schema_name = parts.schema;
        }
    }
    const db = catalog.database(db_name) orelse return Error.DatabaseNotFound;
    const sc = db.schema(schema_name) orelse return Error.SchemaNotFound;
    {
        sc.tables_mutex.lockUncancelable(sc.io);
        defer sc.tables_mutex.unlock(sc.io);
        if (sc.tables.get(ref.name)) |t| return t;
    }
    return sc.openTable(ref.name, .{});
}

const PersistentTableTarget = struct {
    db_name: []const u8,
    schema_name: []const u8,
    schema: *DbSchema,
    table_name: []const u8,
};

fn resolvePersistentTableTarget(catalog: *Catalog, session: Session, ref: ir.TableRef) !PersistentTableTarget {
    var db_name: []const u8 = ref.database orelse session.current_db;
    var schema_name: []const u8 = ref.schema orelse session.current_schema;
    if (ref.database == null and ref.schema != null) {
        if (splitDoubleUnderscore(ref.schema.?)) |parts| {
            db_name = parts.db;
            schema_name = parts.schema;
        }
    }
    const db = catalog.database(db_name) orelse return Error.DatabaseNotFound;
    const sc = db.schema(schema_name) orelse return Error.SchemaNotFound;
    return .{
        .db_name = db_name,
        .schema_name = schema_name,
        .schema = sc,
        .table_name = ref.name,
    };
}

fn sameNamespace(a: PersistentTableTarget, b: PersistentTableTarget) bool {
    return std.mem.eql(u8, a.db_name, b.db_name) and std.mem.eql(u8, a.schema_name, b.schema_name);
}

const NameParts = struct { db: []const u8, schema: []const u8 };

/// Split `db__schema` into `(db, schema)` for MySQL-style flattened
/// names. Returns null if `__` is absent. Mirrors the rule in
/// `mysql/server.zig::applyInitDb` so wire-side and SQL-side resolve
/// the same way.
fn splitDoubleUnderscore(name: []const u8) ?NameParts {
    const sep = std.mem.indexOf(u8, name, "__") orelse return null;
    return .{ .db = name[0..sep], .schema = name[sep + 2 ..] };
}

// ---------------------------------------------------------------------------
// Compile path.
//
// Threads a CompileCtx through the recursion so a `*ir.Op.materialize`
// referenced from multiple parents resolves to ONE drained buffer with
// multiple Reader cursors (instead of N independent drains). The ctx
// owns the buffers; CompiledQuery bundles the resulting Query with the
// ctx so deinit tears down both in order.
// ---------------------------------------------------------------------------

pub const CompileCtx = struct {
    allocator: Allocator,
    db: *Database,
    udf_registry: ?*const @import("../udf.zig").UdfRegistry = null,
    /// Mutable so DDL `USE` statements can update `current_db` /
    /// `current_schema` for subsequent statements that share the
    /// same Session. Borrowed; caller owns the value.
    session: *Session,
    materialized: std.AutoHashMapUnmanaged(*const ir.Op, *@import("../exec/materialize.zig").MaterializedBuffer) = .empty,
    /// Number of shared V2 stages the staged compiler produced (the V2
    /// analogue of `materialized.count()`). Tests assert the sum of both.
    stage_count: u32 = 0,
    /// Strings duplicated into `allocator` to back Session updates from
    /// `USE` statements. Freed at `deinit`.
    session_strings: std.ArrayListUnmanaged([]u8) = .empty,
    /// Per-query arena for Values produced by the subquery pre-compile
    /// pass (scalar values, IN-set materializations, text dupes). The
    /// IR tree borrows pointers into this arena; freed at `deinit`.
    /// Lazily initialized so plans without subqueries pay nothing.
    subquery_arena: ?std.heap.ArenaAllocator = null,
    /// Per-query arena for plan-node data the compiled operator tree borrows
    /// but doesn't own — notably the affine-aggregate reduction's derived
    /// names / expression trees, which the silo / global aggregate operators
    /// reference by pointer for their whole lifetime. Freed in `deinit` AFTER
    /// `query.deinit()` (CompiledQuery tears the tree down first), so those
    /// borrows stay valid until the operators are gone. Lazily initialized.
    node_arena: ?std.heap.ArenaAllocator = null,
    /// Rows affected by a top-level side-effect statement (INSERT today).
    /// Wire layers (MySQL OK_Packet, PG CommandComplete) read this after
    /// running the CompiledQuery. Zero for ops that don't mutate data.
    affected_rows: u64 = 0,
    /// Query-scoped memory accountant. Lazily created (via
    /// `queryAccountant`) the first time a Scan is compiled, then
    /// injected into every Scan / materialized buffer / subquery drain so
    /// the budget is shared across the whole query DAG. Owned here and
    /// freed LAST in `deinit` — after the operator tree and the
    /// materialized buffers, both of which may release against it.
    /// This is the single seam where a future global memory pool would
    /// hand out (and reclaim) the per-query budget.
    accountant: ?*exec.memory.MemoryAccountant = null,
    /// Query-scoped global string dictionary (Phase 4.2 Option A). Lazily
    /// created the first time a scan needs to emit a dict string column as
    /// codes; the scan interns each segment's local dict into it (building a
    /// local→global LUT) so GROUP BY / DISTINCT / ORDER BY operate on a single
    /// code space across segments, and the codes decode back to strings at emit.
    /// Owned here, freed in `deinit` after the operator tree (which borrows it).
    global_dict: ?*exec.GlobalDict = null,
    /// Additional per-column global dicts for the multi-key coded GROUP BY path
    /// (one per coded key column). Owned; freed in `deinit`.
    extra_dicts: std.ArrayListUnmanaged(*exec.GlobalDict) = .empty,
    /// Wall-clock microseconds since the Unix epoch, captured once when
    /// the query is compiled. The subquery pre-compile pass substitutes
    /// it for `now()` / `current_timestamp()` / `current_date()` so those
    /// are stable across the whole statement (PG/MySQL semantics).
    now_micros: i64 = 0,

    /// Projection pushdown: base-column names the (single) scan must
    /// produce, or null to read every column. Computed once after subquery
    /// resolution. The name strings are borrowed from the IR; only the
    /// outer slice is owned. See `analyzeProjection`.
    prune_names: ?[][]const u8 = null,

    pub fn deinit(self: *CompileCtx) void {
        var it = self.materialized.iterator();
        while (it.next()) |entry| entry.value_ptr.*.deinit();
        self.materialized.deinit(self.allocator);
        for (self.session_strings.items) |s| self.allocator.free(s);
        self.session_strings.deinit(self.allocator);
        if (self.subquery_arena) |*ar| ar.deinit();
        if (self.node_arena) |*ar| ar.deinit();
        if (self.accountant) |a| {
            // Release any reservation still outstanding (non-evicted materialize
            // buffers freed just above, error-path remnants) back to the shared
            // pool before the accountant is gone — otherwise the cross-query
            // pool leaks and later queries fail MemoryBudgetExceeded.
            a.drainToPool();
            self.allocator.destroy(a);
        }
        if (self.global_dict) |g| {
            g.deinit(self.allocator);
            self.allocator.destroy(g);
        }
        for (self.extra_dicts.items) |g| {
            g.deinit(self.allocator);
            self.allocator.destroy(g);
        }
        self.extra_dicts.deinit(self.allocator);
        if (self.prune_names) |p| self.allocator.free(p);
    }

    /// The query-scoped global string dictionary, created on first use. Borrowed
    /// by scans (to intern + translate dict codes) and by the aggregate / emit
    /// (to decode codes back to strings); owned + freed by the CompileCtx.
    pub fn queryGlobalDict(self: *CompileCtx) !*exec.GlobalDict {
        if (self.global_dict) |g| return g;
        const g = try self.allocator.create(exec.GlobalDict);
        g.* = .{};
        self.global_dict = g;
        return g;
    }

    /// A fresh query-scoped global dict — one per coded column on the multi-key
    /// coded GROUP BY path (each column has its own code space). Tracked + freed
    /// by the CompileCtx, like `queryGlobalDict`'s.
    pub fn newGlobalDict(self: *CompileCtx) !*exec.GlobalDict {
        const g = try self.allocator.create(exec.GlobalDict);
        errdefer self.allocator.destroy(g);
        g.* = .{};
        try self.extra_dicts.append(self.allocator, g);
        return g;
    }

    /// The query-scoped accountant, created on first use from the
    /// Database's resolved per-query budget and the Catalog's shared
    /// cross-query pool. Returns null only when both are disabled
    /// (budget 0, no pool).
    pub fn queryAccountant(self: *CompileCtx) !?*exec.memory.MemoryAccountant {
        if (self.accountant) |a| return a;
        const budget = thindb_api.autoQueryBudgetBytes(self.db.config.query_memory_budget);
        const pool = self.db.config.memory_pool;
        if (budget == 0 and pool == null) return null;
        const acc = try self.allocator.create(exec.memory.MemoryAccountant);
        acc.* = exec.memory.MemoryAccountant.initWithPool(budget, pool);
        self.accountant = acc;
        return acc;
    }

    /// Get (or lazily create) the subquery arena. Resolution-time
    /// allocations from the pre-compile pass live here.
    pub fn subqueryArena(self: *CompileCtx) Allocator {
        if (self.subquery_arena == null) {
            self.subquery_arena = std.heap.ArenaAllocator.init(self.allocator);
        }
        return self.subquery_arena.?.allocator();
    }

    /// Get (or lazily create) the plan-node arena. Used at compile time for
    /// data the operator tree borrows but doesn't own (see `node_arena`).
    pub fn nodeArena(self: *CompileCtx) Allocator {
        if (self.node_arena == null) {
            self.node_arena = std.heap.ArenaAllocator.init(self.allocator);
        }
        return self.node_arena.?.allocator();
    }
};

/// Result of `compile()`. Holds the executable Query plus the
/// CompileCtx (which owns shared materialization buffers). The
/// CompileCtx's `session` field is a stable pointer into a heap-
/// allocated cell that survives until `deinit()` — DDL ops mutate
/// it in place; callers read the post-run value via `sessionValue()`.
pub const CompiledQuery = struct {
    query: Query,
    ctx: CompileCtx,
    /// Heap-allocated so the address inside `ctx.session` stays stable
    /// across the lifetime of this CompiledQuery (CompiledQuery itself
    /// is returned by value).
    session_cell: *Session,
    /// Optional cancel flag — when non-null, polled before each
    /// `query.next()` call. Wire frontends set this to the
    /// per-connection ConnectionState.cancel_flag so KILL /
    /// CancelRequest from a peer connection can abort an in-flight
    /// query at the next batch boundary. Cost is one atomic load
    /// per batch; trivial compared to the work a batch represents.
    cancel_flag: ?*std.atomic.Value(bool) = null,

    pub fn deinit(self: *CompiledQuery) void {
        self.query.deinit();
        // SessionVars (if any) is intentionally NOT freed here —
        // it must survive across statements in a multi-statement
        // batch so SET @x persists into a later SELECT. Callers
        // (test helpers, wire connections) own the lifetime and
        // free it via `freeSessionVars` at end-of-connection.
        self.ctx.deinit();
        const allocator = self.ctx.allocator;
        allocator.destroy(self.session_cell);
    }

    /// Free a SessionVars previously created by a SET statement.
    /// Callers that iterate multi-statement batches retrieve the
    /// vars pointer from `sessionValue().vars` after the last
    /// statement and pass it here.
    pub fn freeSessionVars(allocator: std.mem.Allocator, vars: ?*thindb_api.SessionVars) void {
        if (vars) |v| {
            v.deinit();
            allocator.destroy(v);
        }
    }

    pub fn next(self: *CompiledQuery) !?exec.Batch {
        if (self.cancel_flag) |f| {
            if (f.load(.acquire)) return Error.QueryCancelled;
        }
        return self.query.next();
    }

    pub fn outputSchema(self: *CompiledQuery) []const @import("../types.zig").Column {
        return self.query.outputSchema();
    }

    /// Current session bound to this query (reflects any USE that
    /// executed during a DDL pipeline).
    pub fn sessionValue(self: *const CompiledQuery) Session {
        return self.session_cell.*;
    }

    /// Rows touched by the most recent side-effect statement (INSERT
    /// today; DELETE eventually). Zero for SELECT and metadata-only DDL.
    pub fn affectedRows(self: *const CompiledQuery) u64 {
        return self.ctx.affected_rows;
    }
};

/// Back-compat shim: route a plan through `compile` with a default
/// Session (current_db="main", current_schema="public").
pub fn compile(allocator: Allocator, db: *Database, root: *const ir.Op) !CompiledQuery {
    return compileWithSession(allocator, db, .{}, root);
}

/// Compile with an explicit initial Session. The Session is copied
/// into a heap cell on the returned CompiledQuery; in-flight DDL
/// statements mutate that cell. Multi-statement connections pass the
/// post-run value back in via `cq.sessionValue()` before re-compiling.
pub fn compileWithSession(
    allocator: Allocator,
    db: *Database,
    session: Session,
    root: *const ir.Op,
) !CompiledQuery {
    const session_cell = try allocator.create(Session);
    session_cell.* = session;
    errdefer allocator.destroy(session_cell);

    var ctx = CompileCtx{
        .allocator = allocator,
        .db = db,
        .udf_registry = if (catalogFor(db)) |catalog| &catalog.udfs else null,
        .session = session_cell,
        .now_micros = std.Io.Timestamp.now(db.io, .real).toMicroseconds(),
    };
    errdefer ctx.deinit();
    // Pre-compile pass: walk the IR and run each uncorrelated scalar
    // subquery once, replacing the `.scalar_subquery` marker with a
    // concrete `.leaf` / `.lit`. After this pass operators never see
    // subquery nodes — they're a parse-time-only construct.
    try subquery_resolve.resolveSubqueriesInOp(&ctx, @constCast(root));
    // Predicate pushdown: relocate single-side WHERE conjuncts below their join
    // so the source is narrowed before the join runs. Pure plan rewrite shared
    // by every handler. Runs on resolved predicates (concrete leaves).
    try predicate_pushdown.pushJoinFilters(ctx.nodeArena(), catalogFor(db), session_cell.*, @constCast(root));
    // Dead-branch elimination: prune UNION arms that provably yield zero rows
    // (constant-false filters — the parser already folds literal comparisons
    // to `.always`) and unlink WHERE-TRUE plumbing. Runs before projection
    // analysis and staging so ref counts see the pruned tree.
    const_fold.foldDeadBranches(@constCast(root));
    // Projection pushdown: after subqueries are resolved to constants,
    // figure out which base columns the (single) scan must produce.
    ctx.prune_names = analyzeProjection(allocator, root);
    const v2_input = engine_v2.CompileInput{
        .allocator = allocator,
        .db = db,
        .session = session_cell.*,
        .prune_names = ctx.prune_names,
        .udf_registry = ctx.udf_registry,
        .node_arena = ctx.nodeArena(),
        .accountant = try ctx.queryAccountant(),
    };
    // SELECT-shaped roots are the engine's: CTE / FROM-subquery boundaries,
    // pg_catalog virtual tables, and non-table leaves compile as (generic)
    // staged blocks; everything else is one V2 block. Statements (DDL/DML/
    // SET/SHOW/EXPLAIN/...) compile on the statement router.
    if (engine_v2.isSelectQuery(root)) {
        if (cte_stages.needsStaging(root) or referencesPgCatalog(root, session_cell.*)) {
            const q = try cte_stages.compileStaged(v2_input, root, &ctx.stage_count);
            return .{ .query = q, .ctx = ctx, .session_cell = session_cell };
        }
        const q = try engine_v2.compileSelectBlock(v2_input, root);
        return .{ .query = q, .ctx = ctx, .session_cell = session_cell };
    }
    const q = try compileOp(&ctx, root);
    return .{ .query = q, .ctx = ctx, .session_cell = session_cell };
}

/// Compile a nested plan — subquery inners, CTAS / INSERT-SELECT sources,
/// EXPLAIN inners — from an already-open CompileCtx. Mirrors
/// `compileWithSession`'s dispatch exactly: staged / pg_catalog / non-table-
/// leaf blocks → cte_stages, single SELECT blocks → engine_v2, statements →
/// the statement router.
pub fn compileSubplan(ctx: *CompileCtx, op: *const ir.Op) anyerror!Query {
    if (engine_v2.isSelectQuery(op)) {
        const v2_input = engine_v2.CompileInput{
            .allocator = ctx.allocator,
            .db = ctx.db,
            .session = ctx.session.*,
            .prune_names = ctx.prune_names,
            .udf_registry = ctx.udf_registry,
            .node_arena = ctx.nodeArena(),
            .accountant = try ctx.queryAccountant(),
        };
        if (cte_stages.needsStaging(op) or referencesPgCatalog(op, ctx.session.*)) {
            return try cte_stages.compileStaged(v2_input, op, &ctx.stage_count);
        }
        return try engine_v2.compileSelectBlock(v2_input, op);
    }
    return try compileOp(ctx, op);
}

/// True when any scan in the plan resolves to a pg_catalog virtual table
/// (PG/neutral dialects only — MySQL has no such schema, mirroring the
/// compileOp gate). Such plans compile as generic staged blocks over the
/// PgCatalogSource batches (cte_stages).
fn referencesPgCatalog(op: *const ir.Op, session: Session) bool {
    if (session.dialect == .mysql) return false;
    return scanMatchesPgCatalog(op);
}

fn scanMatchesPgCatalog(op: *const ir.Op) bool {
    return switch (op.*) {
        .scan => |s| pgcat.match(s.table) != null,
        .select => |p| scanMatchesPgCatalog(p.upstream),
        .exclude => |p| scanMatchesPgCatalog(p.upstream),
        .filter => |f| scanMatchesPgCatalog(f.upstream),
        .order_by => |o| scanMatchesPgCatalog(o.upstream),
        .group_by => |g| scanMatchesPgCatalog(g.upstream),
        .compute => |c| scanMatchesPgCatalog(c.upstream),
        .alias => |a| scanMatchesPgCatalog(a.upstream),
        .limit => |l| scanMatchesPgCatalog(l.upstream),
        .window => |w| scanMatchesPgCatalog(w.upstream),
        .materialize => |m| scanMatchesPgCatalog(m.upstream),
        .join => |j| scanMatchesPgCatalog(j.left) or scanMatchesPgCatalog(j.right),
        .set_union => |u| scanMatchesPgCatalog(u.left) or scanMatchesPgCatalog(u.right),
        else => false,
    };
}

// ---------------------------------------------------------------------------
// Projection pushdown (column pruning) — compile-time analysis.
//
// Walk the resolved IR collecting every column NAME referenced anywhere
// (SELECT, WHERE, GROUP BY / aggregates, ORDER BY, computed exprs,
// correlated-subquery outer refs). The result feeds the scan's projection
// so it decodes only those columns. Conservative by construction: any plan
// shape we don't exhaustively account for (joins, set ops, windows,
// materialized CTEs, anti-projection, > 1 scan) sets `bail` and pruning is
// disabled (the scan reads every column). Keeping extra columns is always
// correct; missing one would be a bug — so when unsure, keep all.
// ---------------------------------------------------------------------------

const ProjScan = struct {
    names: std.ArrayListUnmanaged([]const u8) = .empty,
    bail: bool = false,
    /// True once a `select` or `group_by` shapes the output. Without one,
    /// the raw scan rows ARE the result (`SELECT *`), so every column is
    /// needed and pruning must be disabled — distinct from `COUNT(*)`,
    /// where a group_by is present but references no column.
    has_shaper: bool = false,

    fn add(self: *ProjScan, allocator: Allocator, name: []const u8) void {
        self.names.append(allocator, name) catch {
            self.bail = true;
        };
    }
};

fn projWalkPredicate(c: *ProjScan, allocator: Allocator, p: exec.predicate.PredicateExpr) void {
    if (c.bail) return;
    switch (p) {
        .leaf => |lf| c.add(allocator, lf.col),
        .day_leaf => |lf| c.add(allocator, lf.col),
        .leaf_col_col => |lc| {
            c.add(allocator, lc.left);
            c.add(allocator, lc.right);
        },
        .is_null, .is_not_null => |col| c.add(allocator, col),
        .like => |lp| c.add(allocator, lp.col),
        .in_set => |s| c.add(allocator, s.col),
        .@"and", .@"or" => |children| for (children) |ch| projWalkPredicate(c, allocator, ch),
        .not => |child| projWalkPredicate(c, allocator, child.*),
        .always, .unknown => {},
        .correlated_set => |s| for (s.outer_cols) |nm| c.add(allocator, nm),
        .correlated_scalar => |s| {
            c.add(allocator, s.outer_compared);
            for (s.outer_keys) |nm| c.add(allocator, nm);
        },
        .correlated_range => |s| {
            c.add(allocator, s.outer_range_col);
            if (s.outer_range_col_upper) |upper| c.add(allocator, upper);
            for (s.outer_keys) |nm| c.add(allocator, nm);
        },
        // Unresolved subquery markers must not survive the pre-compile
        // pass; if one does, disable pruning rather than miss a column.
        .scalar_subquery, .exists_subquery, .in_subquery, .leaf_var => c.bail = true,
    }
}

fn projWalkExpr(c: *ProjScan, allocator: Allocator, e: ir.Expr) void {
    if (c.bail) return;
    switch (e) {
        .col_ref => |nm| c.add(allocator, nm),
        .lit, .null_lit => {},
        .call => |call| for (call.args) |a| projWalkExpr(c, allocator, a),
        .case => |cs| {
            for (cs.branches) |br| {
                projWalkPredicate(c, allocator, br.cond);
                projWalkExpr(c, allocator, br.then);
            }
            if (cs.else_branch) |eb| projWalkExpr(c, allocator, eb.*);
        },
        .scalar_subquery, .exists_subquery, .var_ref => c.bail = true,
    }
}

fn projWalkOp(c: *ProjScan, allocator: Allocator, op: *const ir.Op) void {
    if (c.bail) return;
    switch (op.*) {
        .scan, .file_scan => {},
        .alias => |a| projWalkOp(c, allocator, a.upstream),
        .single_row => {},
        .limit => |l| projWalkOp(c, allocator, l.upstream),
        .select => |p| {
            c.has_shaper = true;
            for (p.columns) |nm| {
                if (std.mem.eql(u8, nm, "*") or aliasStarSource(nm) != null) {
                    c.bail = true;
                    return;
                }
                c.add(allocator, nm);
            }
            projWalkOp(c, allocator, p.upstream);
        },
        .filter => |f| {
            projWalkPredicate(c, allocator, f.predicate);
            projWalkOp(c, allocator, f.upstream);
        },
        .order_by => |o| {
            for (o.specs) |sp| c.add(allocator, sp.col);
            projWalkOp(c, allocator, o.upstream);
        },
        .group_by => |g| {
            c.has_shaper = true;
            for (g.group_cols) |nm| c.add(allocator, nm);
            for (g.aggs) |a| {
                if (a.col) |nm| c.add(allocator, nm);
                if (a.arg2_col) |nm| c.add(allocator, nm);
                for (a.udf_arg_cols) |nm| c.add(allocator, nm);
            }
            projWalkOp(c, allocator, g.upstream);
        },
        .compute => |cmp| {
            for (cmp.derived) |d| projWalkExpr(c, allocator, d.expr);
            projWalkOp(c, allocator, cmp.upstream);
        },
        .join => |j| {
            // Per-scan filtering attributes each name to its own table via
            // findColumn's prefix handling, so collecting every join-key /
            // range / post-filter column into the flat set is enough — each
            // input scan keeps the subset that resolves to it.
            for (j.on) |kp| {
                c.add(allocator, kp.left);
                c.add(allocator, kp.right);
            }
            for (j.ranges) |rp| {
                c.add(allocator, rp.left);
                c.add(allocator, rp.right);
            }
            if (j.extra_predicate) |p| projWalkPredicate(c, allocator, p);
            projWalkOp(c, allocator, j.left);
            projWalkOp(c, allocator, j.right);
        },
        .set_union => |u| {
            projWalkOp(c, allocator, u.left);
            projWalkOp(c, allocator, u.right);
        },
        .window => |w| {
            for (w.specs) |sp| {
                for (sp.partition_by) |nm| c.add(allocator, nm);
                for (sp.order_by) |so| c.add(allocator, so.col);
            }
            for (w.calls) |call| for (call.args) |a| projWalkExpr(c, allocator, a);
            projWalkOp(c, allocator, w.upstream);
        },
        .materialize => |m| projWalkOp(c, allocator, m.upstream),
        .create_table_as => |ct| projWalkOp(c, allocator, ct.source),
        .insert_select => |i| projWalkOp(c, allocator, i.source),
        .explain => |e| projWalkOp(c, allocator, e.inner),
        // Anything else — `exclude` (anti-projection / SELECT * EXCEPT, whose
        // output is "all but these"), DDL/DML, raw inserts, etc. — disables
        // pruning. Keeping every column is always correct.
        else => c.bail = true,
    }
}

/// Names a single-scan query references, or null to keep all columns.
/// The strings are borrowed from the IR; caller owns the outer slice.
/// Column names a window operator's INPUT must carry: everything the ops
/// ABOVE `win` reference (window output passes every input column through)
/// plus the window's own partition/order/argument columns. Used by the
/// staged compiler to prune the below-window block — without it a fused
/// filter's column (often a wide string) rides through the blocking
/// accumulate/sort/emit for nothing. Over-collection is safe (pruning
/// keeps a superset); returns null when `win` wasn't found on a linear
/// path or a shape above it wasn't fully accounted for — caller skips
/// pruning. Caller owns the slice.
pub fn windowInputNames(allocator: Allocator, root: *const ir.Op, win: *const ir.Op) ?[][]const u8 {
    var c = ProjScan{};
    const found = walkAboveWindow(&c, allocator, root, win);
    if (!found or c.bail) {
        c.names.deinit(allocator);
        return null;
    }
    return c.names.toOwnedSlice(allocator) catch null;
}

fn walkAboveWindow(c: *ProjScan, allocator: Allocator, op: *const ir.Op, win: *const ir.Op) bool {
    if (c.bail) return false;
    if (op == win) {
        const w = op.window;
        for (w.specs) |sp| {
            for (sp.partition_by) |nm| c.add(allocator, nm);
            for (sp.order_by) |so| c.add(allocator, so.col);
        }
        for (w.calls) |call| {
            for (call.args) |a| projWalkExpr(c, allocator, a);
        }
        return true;
    }
    switch (op.*) {
        .select => |p| {
            for (p.columns) |nm| {
                if (std.mem.eql(u8, nm, "*") or aliasStarSource(nm) != null) {
                    c.bail = true;
                    return false;
                }
                c.add(allocator, nm);
            }
            return walkAboveWindow(c, allocator, p.upstream, win);
        },
        .filter => |f| {
            projWalkPredicate(c, allocator, f.predicate);
            return walkAboveWindow(c, allocator, f.upstream, win);
        },
        .order_by => |o| {
            for (o.specs) |sp| c.add(allocator, sp.col);
            return walkAboveWindow(c, allocator, o.upstream, win);
        },
        .group_by => |g| {
            for (g.group_cols) |nm| c.add(allocator, nm);
            for (g.aggs) |a| {
                if (a.col) |nm| c.add(allocator, nm);
                if (a.arg2_col) |nm| c.add(allocator, nm);
                for (a.udf_arg_cols) |nm| c.add(allocator, nm);
            }
            return walkAboveWindow(c, allocator, g.upstream, win);
        },
        .compute => |cmp| {
            for (cmp.derived) |d| projWalkExpr(c, allocator, d.expr);
            return walkAboveWindow(c, allocator, cmp.upstream, win);
        },
        .alias => |a| return walkAboveWindow(c, allocator, a.upstream, win),
        .limit => |l| return walkAboveWindow(c, allocator, l.upstream, win),
        // `exclude` ("all but these") can't be expressed as a keep-set;
        // anything else above a window is a shape this walker doesn't
        // understand. Keep every column.
        else => {
            c.bail = true;
            return false;
        },
    }
}

fn analyzeProjection(allocator: Allocator, root: *const ir.Op) ?[][]const u8 {
    var c = ProjScan{};
    projWalkOp(&c, allocator, root);
    // No `scan_count` cap: with multiple scans (joins, CTEs, set ops) each
    // scan independently keeps the collected names that resolve to its own
    // table, so the flat set is a correct per-scan superset. Bail only when
    // a shape wasn't fully accounted for, or nothing shapes the output.
    if (c.bail or !c.has_shaper) {
        c.names.deinit(allocator);
        return null;
    }
    return c.names.toOwnedSlice(allocator) catch null;
}

/// Bits a group column contributes to a coded int key: a string keys on its
/// 32-bit global code; an int-family column on its type width; anything else
/// (float/double) can't pack (null). Mirrors aggregate.zig `intKeyBits` (+ the
/// coded 32-bit field) so the gate's fit pre-check matches `planIntKey` exactly.
fn codedKeyBits(t: types.Type) ?u16 {
    return switch (t) {
        .varchar, .string, .char => 32,
        .boolean, .tinyint => 8,
        .smallint => 16,
        .int, .date => 32,
        .bigint, .datetime, .decimal64 => 64,
        .largeint, .decimal128, .uuid => 128,
        else => null,
    };
}

/// True if any aggregate reads `name` as its argument — then the group key
/// can't be dict-coded (the aggregate needs that column's materialized strings).
fn aggUsesColumn(aggs: []const AggSpec, name: []const u8) bool {
    for (aggs) |a| {
        if (a.col) |c| {
            if (@import("../types.zig").columnNameEql(c, name)) return true;
        }
        for (a.udf_arg_cols) |c| {
            if (@import("../types.zig").columnNameEql(c, name)) return true;
        }
    }
    return false;
}

fn hasUdfAgg(aggs: []const AggSpec) bool {
    for (aggs) |a| {
        if (a.func == .udf) return true;
    }
    return false;
}

fn stripAlias(alias: []const u8, name: []const u8) ?[]const u8 {
    const dot = std.mem.indexOfScalar(u8, name, '.') orelse return null;
    if (!types.columnNameEql(name[0..dot], alias)) return null;
    return name[dot + 1 ..];
}

/// Layer a projection Compute onto `upstream`, splitting it so row-local
/// derived columns (regex / scalar fns) run inside the parallel scan workers
/// and the remainder (CASE, anything referencing a sibling derived, etc.) runs
/// in a serial Compute above the parallel merge. When nothing is fusable — or
/// the upstream isn't a parallel scan (DOP=1) — it degrades to a single serial
/// Compute over all derived. The serial Compute is UDF-registry-aware so
/// user-defined scalar fns still resolve.
fn fuseSplitCompute(ctx: *CompileCtx, upstream: Query, derived: []const ir.Derived) !Query {
    var up = upstream;
    const scan_cols = up.outputSchema();
    var fusable: std.ArrayListUnmanaged(ir.Derived) = .empty;
    defer fusable.deinit(ctx.allocator);
    var serial: std.ArrayListUnmanaged(ir.Derived) = .empty;
    defer serial.deinit(ctx.allocator);
    for (derived) |d| {
        if (exec.derivedFusable(d, scan_cols)) {
            try fusable.append(ctx.allocator, d);
        } else {
            try serial.append(ctx.allocator, d);
        }
    }
    // Nothing fusable, or fusion declined (no parallel scan beneath) → all serial.
    if (fusable.items.len == 0) return up.computeWithRegistry(derived, ctx.udf_registry);
    if (!try up.tryFuseCompute(fusable.items)) return up.computeWithRegistry(derived, ctx.udf_registry);
    // Fused subset now runs in the workers; layer the remainder serially (it can
    // reference the fused columns, which are now in `up`'s output schema).
    if (serial.items.len == 0) return up;
    return up.computeWithRegistry(serial.items, ctx.udf_registry);
}

/// Tell `upstream` the exact set of columns this GROUP BY reads — its group keys
/// plus every aggregate argument. A parallel scan uses it to drop columns from
/// its survivor deep-copy that were decoded only to feed a fused filter/compute
/// and are dead above (e.g. raw `URL` behind `length(URL)` + `URL<>''`). Best
/// effort: non-parallel scans and unfused filters ignore it (see
/// `Query.setEmitProjection`).
fn pushAggEmitProjection(ctx: *CompileCtx, upstream: Query, group_cols: []const []const u8, aggs: []const AggSpec) !void {
    var keep: std.ArrayListUnmanaged([]const u8) = .empty;
    defer keep.deinit(ctx.allocator);
    for (group_cols) |c| try keep.append(ctx.allocator, c);
    for (aggs) |a| {
        if (a.col) |c| try keep.append(ctx.allocator, c);
        for (a.udf_arg_cols) |c| try keep.append(ctx.allocator, c);
    }
    try upstream.setEmitProjection(keep.items);
}

fn aliasStarSource(name: []const u8) ?[]const u8 {
    if (name.len <= 2) return null;
    if (name[name.len - 2] != '.' or name[name.len - 1] != '*') return null;
    return name[0 .. name.len - 2];
}

fn appendExpandedProjectItem(
    allocator: Allocator,
    sources: *std.ArrayListUnmanaged([]const u8),
    outputs: *std.ArrayListUnmanaged([]const u8),
    stripped: *std.ArrayListUnmanaged(bool),
    schema: []const types.Column,
    item: []const u8,
    output: ?[]const u8,
    replace_on_collision: bool,
) !void {
    if (std.mem.eql(u8, item, "*")) {
        for (schema) |c| {
            try sources.append(allocator, c.name);
            try outputs.append(allocator, c.name);
            try stripped.append(allocator, false);
        }
        return;
    }
    if (aliasStarSource(item)) |alias| {
        var matched = false;
        for (schema) |c| {
            if (stripAlias(alias, c.name)) |bare| {
                matched = true;
                try sources.append(allocator, c.name);
                try outputs.append(allocator, bare);
                try stripped.append(allocator, true);
            }
        }
        if (!matched) return Error.ColumnNotFound;
        return;
    }

    _ = types.findColumn(schema, item) orelse return Error.ColumnNotFound;
    // Standard SQL result naming: a bare or qualified column reference
    // outputs its UNQUALIFIED name (`SELECT sub.id` and `SELECT id` over an
    // aliased source both yield a column named `id`), regardless of how the
    // upstream schema spells it (AliasRename qualifies every column).
    const bare = if (std.mem.lastIndexOfScalar(u8, item, '.')) |dot| item[dot + 1 ..] else item;
    const output_name = output orelse bare;
    const is_stripped = output == null and bare.ptr != item.ptr;
    // Replace-on-collision: a derived/aliased item whose final name already
    // exists in the partial output overwrites that slot (`SELECT *, f(x) AS x`).
    if (replace_on_collision) {
        for (outputs.items, 0..) |existing, out_idx| {
            if (types.columnNameEql(existing, output_name)) {
                sources.items[out_idx] = item;
                outputs.items[out_idx] = output_name;
                stripped.items[out_idx] = is_stripped;
                return;
            }
        }
    }
    try sources.append(allocator, item);
    try outputs.append(allocator, output_name);
    try stripped.append(allocator, is_stripped);
}

fn projectReplaceFlag(p: ir.Op.Project, index: usize) bool {
    if (p.replace_on_collision) |flags| {
        if (index < flags.len) return flags[index];
    }
    return false;
}

fn projectTargetName(p: ir.Op.Project, index: usize) []const u8 {
    if (p.outputs) |outs| {
        if (outs[index]) |out| return out;
    }
    return p.columns[index];
}

fn projectHasReplaceTarget(p: ir.Op.Project, name: []const u8) bool {
    for (p.columns, 0..) |_, i| {
        if (!projectReplaceFlag(p, i)) continue;
        if (types.columnNameEql(projectTargetName(p, i), name)) return true;
    }
    return false;
}

/// A trailing derived column the `*` would normally skip stays in the star
/// expansion when a later projection item replaces it by name — otherwise the
/// replacement would have no slot to overwrite.
fn effectiveStarSkip(schema: []const types.Column, p: ir.Op.Project) u32 {
    var skip: u32 = 0;
    while (skip < p.star_skip_trailing and skip < schema.len) : (skip += 1) {
        const idx = schema.len - 1 - skip;
        if (!projectHasReplaceTarget(p, schema[idx].name)) break;
    }
    return skip;
}

pub fn compileSelectProject(allocator: Allocator, upstream: Query, p: ir.Op.Project) !Query {
    const schema = upstream.outputSchema();
    if (p.star_skip_trailing > schema.len) return Error.BadRequest;
    const star_skip = effectiveStarSkip(schema, p);
    const star_schema = schema[0 .. schema.len - star_skip];

    var sources: std.ArrayListUnmanaged([]const u8) = .empty;
    defer sources.deinit(allocator);
    var outputs: std.ArrayListUnmanaged([]const u8) = .empty;
    defer outputs.deinit(allocator);
    var stripped: std.ArrayListUnmanaged(bool) = .empty;
    defer stripped.deinit(allocator);

    for (p.columns, 0..) |item, i| {
        const explicit_output: ?[]const u8 = if (p.outputs) |outs| outs[i] else null;
        const expansion_schema = if (std.mem.eql(u8, item, "*") or aliasStarSource(item) != null) star_schema else schema;
        try appendExpandedProjectItem(allocator, &sources, &outputs, &stripped, expansion_schema, item, explicit_output, projectReplaceFlag(p, i));
    }

    // Unqualifying two references from different join sides can collide
    // (`SELECT e.name, m.name`). Keep the qualified name for every stripped
    // item whose bare name another output also claims — deterministic and
    // resolvable, where SQL's duplicate result names wouldn't be. Collisions
    // are detected against the pre-rename names so every duplicate reverts.
    var any_collision = false;
    detect: for (outputs.items, 0..) |out, i| {
        if (!stripped.items[i]) continue;
        for (outputs.items, 0..) |other, j| {
            if (j != i and types.columnNameEql(out, other)) {
                any_collision = true;
                break :detect;
            }
        }
    }
    if (any_collision) {
        const original = try allocator.dupe([]const u8, outputs.items);
        defer allocator.free(original);
        for (original, 0..) |out, i| {
            if (!stripped.items[i]) continue;
            for (original, 0..) |other, j| {
                if (j != i and types.columnNameEql(out, other)) {
                    outputs.items[i] = sources.items[i];
                    break;
                }
            }
        }
    }

    const source_slice = try sources.toOwnedSlice(allocator);
    defer allocator.free(source_slice);
    const output_slice = try outputs.toOwnedSlice(allocator);
    defer allocator.free(output_slice);
    return upstream.projectNamed(source_slice, output_slice);
}

pub fn compileOp(ctx: *CompileCtx, op: *const ir.Op) !Query {
    return switch (op.*) {
        // SELECT-shaped pipeline ops compile through the V2-first dispatch
        // (compileWithSession / compileSubplan → engine_v2 / cte_stages) and
        // never reach this statement router. Reaching one here is a bug in
        // a caller compiling a pipeline op directly.
        .scan,
        .file_scan,
        .alias,
        .limit,
        .select,
        .exclude,
        .filter,
        .order_by,
        .group_by,
        .compute,
        .join,
        .materialize,
        .window,
        .set_union,
        .single_row,
        => Error.UnsupportedOp,
        .ddl => |d| try compileDdl(ctx, d),
        .show => |s| try compileShow(ctx, s),
        .insert => |i| try compileInsert(ctx, i),
        // Multi-statement batches are not a single pipeline — wire
        // layers (mysql/server.zig, pg/server.zig) iterate sub-statements
        // and compile each one independently. Reaching this branch means
        // a caller compiled a batch op directly; that's a bug.
        .batch => Error.UnsupportedOp,
        // COPY is wire-driven; only the PG dispatcher handles it.
        .copy => Error.UnsupportedOp,
        .create_table_as => |c| try compileCreateTableAs(ctx, c),
        .insert_select => |i| try compileInsertSelect(ctx, i),
        .set_var => |sv| try compileSetVar(ctx, sv),
        .delete_op => |d| try compileDelete(ctx, d),
        .update_op => |u| try compileUpdate(ctx, u),
        .explain => |e| blk: {
            // Compile the inner statement, render its physical plan, and
            // return the plan as a one-column result. The inner query is
            // never executed. Column name follows the connecting wire's
            // convention; JSON renders the whole tree into a single row.
            var inner = try compileSubplan(ctx, e.inner);
            const plan = inner.explainPlan(ctx.allocator) catch |err| {
                inner.deinit();
                return err;
            };
            inner.deinit();
            defer ctx.allocator.free(plan);
            const col_name: []const u8 = switch (ctx.session.dialect) {
                .mysql => "EXPLAIN",
                .neutral, .postgres => "QUERY PLAN",
            };
            if (e.format == .json) {
                const json = try planTextToJson(ctx.allocator, plan);
                defer ctx.allocator.free(json);
                var rows = [_][]u8{@constCast(json)};
                break :blk try NamesOp.create(ctx.allocator, col_name, rows[0..]);
            }
            var lines: std.ArrayList([]u8) = .empty;
            defer lines.deinit(ctx.allocator);
            var it = std.mem.splitScalar(u8, plan, '\n');
            while (it.next()) |line| {
                if (line.len > 0) try lines.append(ctx.allocator, @constCast(line));
            }
            break :blk try NamesOp.create(ctx.allocator, col_name, lines.items);
        },
    };
}

/// `DELETE FROM t [WHERE ...]` — resolve the table, call
/// `Table.deleteByExpr` (which streams per segment), and report
/// the affected row count back through CompileCtx.
fn compileDelete(ctx: *CompileCtx, d: ir.DeleteOp) !Query {
    const catalog = catalogFor(ctx.db) orelse return Error.DatabaseNotFound;
    const t = try resolveTable(catalog, ctx.session.*, d.table);
    const deleted = try t.deleteByExpr(d.predicate);
    ctx.affected_rows = @intCast(deleted);
    return try EmptyOp.createWithCount(ctx.allocator, deleted);
}

/// `UPDATE t SET col = expr [, ...] [WHERE ...]` — streaming
/// implementation. Each segment is processed as a self-contained
/// "delete-old + insert-new" batch under the table mutex, so memory
/// peak is bounded by row-group size + memtable budget regardless
/// of how many rows the UPDATE touches.
///
/// Subqueries / @var refs in the predicate and assignment exprs are
/// already resolved by the pre-compile pass before this runs.
fn compileUpdate(ctx: *CompileCtx, u: ir.UpdateOp) anyerror!Query {
    const catalog = catalogFor(ctx.db) orelse return Error.DatabaseNotFound;
    const t = try resolveTable(catalog, ctx.session.*, u.table);

    const update_mod = @import("../api/update.zig");
    const assigns_buf = try ctx.allocator.alloc(update_mod.Assignment, u.assignments.len);
    defer ctx.allocator.free(assigns_buf);
    for (u.assignments, assigns_buf) |src, *dst| {
        dst.* = .{ .col = src.col, .value = src.value };
    }

    const affected = try t.updateStreaming(u.predicate, assigns_buf);
    ctx.affected_rows = @intCast(affected);
    return try EmptyOp.createWithCount(ctx.allocator, affected);
}

/// `SET @name = expr` — evaluate the RHS Expr (which may contain a
/// scalar subquery resolved by the pre-compile pass into a `.lit`)
/// and write it into `Session.vars` for use by subsequent
/// statements. Errors if the Expr didn't constant-fold to a single
/// literal.
fn compileSetVar(ctx: *CompileCtx, sv: ir.SetVar) !Query {
    const value: ?Value = switch (sv.value) {
        .lit => |v| v,
        .null_lit => null,
        else => return Error.UnsupportedOp,
    };

    // Lazily create the session's var map. Owned by the CompileCtx's
    // allocator so it outlives the statement.
    if (ctx.session.vars == null) {
        const vars = try ctx.allocator.create(thindb_api.SessionVars);
        vars.* = thindb_api.SessionVars.init(ctx.allocator);
        ctx.session.vars = vars;
    }
    try ctx.session.vars.?.set(sv.name, value);

    return try EmptyOp.createWithCount(ctx.allocator, 0);
}

fn compileDdl(ctx: *CompileCtx, d: ir.DdlOp) !Query {
    const catalog = catalogFor(ctx.db) orelse return Error.DatabaseNotFound;
    switch (d) {
        .create_database => |name| _ = catalog.createDatabase(name) catch |e| return thindb_api.remapError(Error, e),
        .drop_database => |name| catalog.dropDatabase(name) catch |e| return thindb_api.remapError(Error, e),
        .create_schema => |name| {
            const db = catalog.database(ctx.session.current_db) orelse return Error.DatabaseNotFound;
            _ = db.createSchema(name) catch |e| return thindb_api.remapError(Error, e);
        },
        .drop_schema => |name| {
            const db = catalog.database(ctx.session.current_db) orelse return Error.DatabaseNotFound;
            db.dropSchema(name) catch |e| return thindb_api.remapError(Error, e);
        },
        .use_schema => |name| {
            // Accept MySQL-style `USE db__schema` by splitting on `__`.
            // Without this the SQL `USE` path diverges from COM_INIT_DB
            // and tools like MySQL Workbench (which always emit the
            // flattened form) fail with "Schema not found".
            if (splitDoubleUnderscore(name)) |parts| {
                const db = catalog.database(parts.db) orelse return Error.DatabaseNotFound;
                _ = db.schema(parts.schema) orelse return Error.SchemaNotFound;
                const db_owned = try ctx.allocator.dupe(u8, parts.db);
                try ctx.session_strings.append(ctx.allocator, db_owned);
                const sc_owned = try ctx.allocator.dupe(u8, parts.schema);
                try ctx.session_strings.append(ctx.allocator, sc_owned);
                ctx.session.current_db = db_owned;
                ctx.session.current_schema = sc_owned;
            } else {
                const db = catalog.database(ctx.session.current_db) orelse return Error.DatabaseNotFound;
                _ = db.schema(name) orelse return Error.SchemaNotFound;
                const owned = try ctx.allocator.dupe(u8, name);
                try ctx.session_strings.append(ctx.allocator, owned);
                ctx.session.current_schema = owned;
            }
        },
        .use_database_schema => |p| {
            const db = catalog.database(p.database) orelse return Error.DatabaseNotFound;
            _ = db.schema(p.schema) orelse return Error.SchemaNotFound;
            const db_owned = try ctx.allocator.dupe(u8, p.database);
            try ctx.session_strings.append(ctx.allocator, db_owned);
            const sc_owned = try ctx.allocator.dupe(u8, p.schema);
            try ctx.session_strings.append(ctx.allocator, sc_owned);
            ctx.session.current_db = db_owned;
            ctx.session.current_schema = sc_owned;
        },
        .create_table => |ct| {
            const cols = try ctx.allocator.alloc(types.Column, ct.columns.len);
            defer ctx.allocator.free(cols);
            var saw_auto_increment = false;
            for (ct.columns, 0..) |c, ci| {
                if (c.auto_increment) {
                    // MySQL allows exactly one AI column per table. The
                    // column must be integer-typed; DEFAULT alongside is
                    // ambiguous (which one wins?) and forbidden. These
                    // checks fire before the DEFAULT type-tag validation
                    // below so AI+DEFAULT surfaces as UnsupportedOp rather
                    // than getting filtered as a tag mismatch.
                    if (saw_auto_increment) return Error.UnsupportedOp;
                    if (c.default_value != null) return Error.UnsupportedOp;
                    if (!c.column_type.isInteger()) return Error.TypeMismatch;
                    saw_auto_increment = true;
                }
                // Validate DEFAULT value-tag matches the column type so a
                // mismatch errors at CREATE TABLE rather than first INSERT.
                if (c.default_value) |dv| {
                    if (types.ValueTag.fromType(c.column_type) != std.meta.activeTag(dv)) {
                        return Error.TypeMismatch;
                    }
                }
                cols[ci] = .{
                    .name = c.name,
                    .type = c.column_type,
                    .nullable = c.nullable,
                    .default_value = c.default_value,
                    .auto_increment = c.auto_increment,
                };
            }
            const schema_def: TableSchema = .{
                .columns = cols,
                .order_key = ct.order_key,
                .unique = true,
                .compression = ct.compression orelse types.default_table_compression,
            };
            const opts: TableOptions = .{
                .order_key = ct.order_key,
                .unique = true,
                .row_group_size = null,
            };

            if (ct.is_temp) {
                const ns = ctx.session.temp_namespace orelse return Error.UnsupportedOp;
                if (ns.contains(ct.table.name)) {
                    if (ct.if_not_exists) return try EmptyOp.create(ctx.allocator);
                    return Error.TableAlreadyExists;
                }
                _ = ns.createTable(ct.table.name, schema_def, opts) catch |e| return thindb_api.remapError(Error, e);
            } else {
                const db_name = ct.table.database orelse ctx.session.current_db;
                const db = catalog.database(db_name) orelse return Error.DatabaseNotFound;
                const sc_name = ct.table.schema orelse ctx.session.current_schema;
                const sc = db.schema(sc_name) orelse return Error.SchemaNotFound;

                sc.tables_mutex.lockUncancelable(sc.io);
                const exists = sc.tables.get(ct.table.name) != null;
                sc.tables_mutex.unlock(sc.io);
                if (exists) {
                    if (ct.if_not_exists) return try EmptyOp.create(ctx.allocator);
                    return Error.TableAlreadyExists;
                }

                _ = sc.table(ct.table.name, schema_def, opts) catch |e| return thindb_api.remapError(Error, e);
            }
        },
        .drop_table => |dt| {
            // Unqualified refs hit the temp namespace first. Per spec:
            // DROP TABLE foo drops the temp if it shadows; the persistent
            // table (if any) stays put.
            if (dt.table.database == null and dt.table.schema == null) {
                if (ctx.session.temp_namespace) |ns| {
                    if (ns.contains(dt.table.name)) {
                        ns.dropTable(dt.table.name) catch |e| return thindb_api.remapError(Error, e);
                        return try EmptyOp.create(ctx.allocator);
                    }
                }
            }
            const db_name = dt.table.database orelse ctx.session.current_db;
            const db = catalog.database(db_name) orelse return Error.DatabaseNotFound;
            const sc_name = dt.table.schema orelse ctx.session.current_schema;
            const sc = db.schema(sc_name) orelse return Error.SchemaNotFound;
            sc.dropTable(dt.table.name) catch |e| switch (e) {
                ApiError.TableNotFound => if (!dt.if_exists) return Error.TableNotFound,
                else => return thindb_api.remapError(Error, e),
            };
        },
        .rename_table => |rt| {
            if (rt.from.database == null and rt.from.schema == null) {
                if (ctx.session.temp_namespace) |ns| {
                    if (ns.contains(rt.from.name)) return Error.UnsupportedOp;
                }
            }
            const from = try resolvePersistentTableTarget(catalog, ctx.session.*, rt.from);
            const to = try resolvePersistentTableTarget(catalog, ctx.session.*, rt.to);
            if (!sameNamespace(from, to)) return Error.UnsupportedOp;
            from.schema.renameTable(from.table_name, to.table_name) catch |e| return thindb_api.remapError(Error, e);
        },
        .alter_table_add_column => |at| {
            if (at.table.database == null and at.table.schema == null) {
                if (ctx.session.temp_namespace) |ns| {
                    if (ns.contains(at.table.name)) return Error.UnsupportedOp;
                }
            }
            const target = try resolvePersistentTableTarget(catalog, ctx.session.*, at.table);
            const c = at.column;
            if (c.auto_increment) return Error.UnsupportedOp;
            if (c.default_value) |dv| {
                if (types.ValueTag.fromType(c.column_type) != std.meta.activeTag(dv)) return Error.TypeMismatch;
            } else if (!c.nullable) {
                return Error.UnsupportedOp;
            }
            var ops = [_]AlterOp{.{ .add = .{
                .name = c.name,
                .type = c.column_type,
                .nullable = c.nullable,
                .default = c.default_value,
            } }};
            target.schema.alterTable(target.table_name, &ops) catch |e| return thindb_api.remapError(Error, e);
        },
        .truncate_table => |ref| {
            const t = try resolveTable(catalog, ctx.session.*, ref);
            try t.truncate();
            ctx.affected_rows = 0;
        },
    }
    return try EmptyOp.create(ctx.allocator);
}

/// CREATE TABLE name AS SELECT ... — infer target schema from the
/// source query's output schema, create the table, then drain the
/// source and bulk-insert.
fn compileCreateTableAs(ctx: *CompileCtx, op: ir.CreateTableAs) anyerror!Query {
    const catalog = catalogFor(ctx.db) orelse return Error.DatabaseNotFound;

    var source = try compileSubplan(ctx, op.source);
    defer source.deinit();

    const src_schema = source.outputSchema();
    if (src_schema.len == 0) return Error.BadRequest;

    // Materialize the inferred table schema. v1: first column is the
    // order key by default (matches what tests + bulk loaders expect);
    // unique = false since CTAS doesn't declare a PK.
    const cols = try ctx.allocator.alloc(types.Column, src_schema.len);
    defer ctx.allocator.free(cols);
    for (src_schema, cols) |s, *c| {
        c.* = .{ .name = s.name, .type = s.type, .nullable = s.nullable };
    }
    const order_key = try ctx.allocator.alloc([]const u8, 1);
    defer ctx.allocator.free(order_key);
    order_key[0] = cols[0].name;
    const target_schema: TableSchema = .{
        .columns = cols,
        .order_key = order_key,
        .unique = false,
    };
    const opts: TableOptions = .{
        .order_key = order_key,
        .unique = false,
        .row_group_size = null,
    };

    var t: *ApiTable = undefined;
    if (op.is_temp) {
        const ns = ctx.session.temp_namespace orelse return Error.UnsupportedOp;
        if (ns.contains(op.table.name)) {
            if (op.if_not_exists) return try EmptyOp.createWithCount(ctx.allocator, 0);
            return Error.TableAlreadyExists;
        }
        t = ns.createTable(op.table.name, target_schema, opts) catch |e| return thindb_api.remapError(Error, e);
    } else {
        const db_name = op.table.database orelse ctx.session.current_db;
        const db = catalog.database(db_name) orelse return Error.DatabaseNotFound;
        const sc_name = op.table.schema orelse ctx.session.current_schema;
        const sc = db.schema(sc_name) orelse return Error.SchemaNotFound;

        sc.tables_mutex.lockUncancelable(sc.io);
        const exists = sc.tables.get(op.table.name) != null;
        sc.tables_mutex.unlock(sc.io);
        if (exists) {
            if (op.if_not_exists) return try EmptyOp.createWithCount(ctx.allocator, 0);
            return Error.TableAlreadyExists;
        }
        t = sc.table(op.table.name, target_schema, opts) catch |e| return thindb_api.remapError(Error, e);
    }

    var total_rows: usize = 0;
    while (try source.next()) |b| {
        try t.insertBatch(b.schema, b.values, b.row_count);
        total_rows += b.row_count;
    }
    ctx.affected_rows = @intCast(total_rows);
    return try EmptyOp.createWithCount(ctx.allocator, @intCast(total_rows));
}

/// INSERT INTO target [(cols)] SELECT ... — drain the source query
/// and bulk-insert each batch into the target table. The source's
/// output columns are renamed (per the column list, or positional
/// against the target schema) so the memtable's name-based column
/// matching finds them.
fn compileInsertSelect(ctx: *CompileCtx, op: ir.InsertSelect) anyerror!Query {
    const catalog = catalogFor(ctx.db) orelse return Error.DatabaseNotFound;
    const t = try resolveTable(catalog, ctx.session.*, op.table);
    const tbl_schema = t.schema;

    var source = try compileSubplan(ctx, op.source);
    defer source.deinit();

    const src_schema = source.outputSchema();
    if (op.columns) |cols| {
        if (cols.len != src_schema.len) return Error.BadRequest;
        for (cols) |cname| {
            _ = tbl_schema.columnIndex(cname) orelse return Error.ColumnNotFound;
        }
    } else {
        if (src_schema.len != tbl_schema.columns.len) return Error.BadRequest;
    }

    // Synthesize a batch_schema where each source column carries the
    // target column name expected by `Memtable.insertColumnarBatch`.
    const renamed = try ctx.allocator.alloc(types.Column, src_schema.len);
    defer ctx.allocator.free(renamed);
    for (src_schema, renamed, 0..) |s, *r, i| {
        const target_name = if (op.columns) |cols| cols[i] else tbl_schema.columns[i].name;
        r.* = .{ .name = target_name, .type = s.type, .nullable = s.nullable };
    }

    var total_rows: usize = 0;
    while (try source.next()) |b| {
        try t.insertBatch(renamed, b.values, b.row_count);
        total_rows += b.row_count;
    }
    ctx.affected_rows = @intCast(total_rows);
    return try EmptyOp.createWithCount(ctx.allocator, @intCast(total_rows));
}

fn compileInsert(ctx: *CompileCtx, op: ir.InsertOp) !Query {
    const catalog = catalogFor(ctx.db) orelse return Error.DatabaseNotFound;
    const t = try resolveTable(catalog, ctx.session.*, op.table);
    const tbl_schema = t.schema;

    const source_widths = if (op.columns) |cols| cols.len else tbl_schema.columns.len;
    if (op.columns) |cols| {
        for (cols) |cname| {
            _ = tbl_schema.columnIndex(cname) orelse return Error.ColumnNotFound;
        }
    } else {
        if (source_widths != tbl_schema.columns.len) return Error.BadRequest;
    }

    // schema_to_source[i] = which source column feeds table column i, or null
    // (meaning: not in the user-supplied list; must be NULL-fillable).
    const schema_to_source = try ctx.allocator.alloc(?usize, tbl_schema.columns.len);
    defer ctx.allocator.free(schema_to_source);
    for (schema_to_source) |*s| s.* = null;
    if (op.columns) |cols| {
        for (cols, 0..) |cname, src_idx| {
            const si = tbl_schema.columnIndex(cname).?;
            schema_to_source[si] = src_idx;
        }
    } else {
        for (schema_to_source, 0..) |*s, i| s.* = i;
    }

    for (schema_to_source, 0..) |maybe_src, si| {
        const col = tbl_schema.columns[si];
        // Omitted column is OK if it's nullable OR has a DEFAULT — the
        // INSERT row build below substitutes the default for missing
        // values. AUTO_INCREMENT cols also auto-fill, so they don't
        // require an explicit source. NOT NULL columns without any of
        // these still error.
        if (maybe_src == null and !col.nullable and col.default_value == null and !col.auto_increment) {
            return Error.ColumnNotFound;
        }
    }
    for (op.rows) |row| {
        if (row.len != source_widths) return Error.BadRequest;
        for (schema_to_source, 0..) |maybe_src, si| {
            if (maybe_src) |src| {
                const col = tbl_schema.columns[si];
                // Explicit NULL on an AI column is fine — the counter
                // fills it. Otherwise NULL on NOT NULL is a type error.
                if (row[src] == null and !col.nullable and !col.auto_increment) {
                    return Error.TypeMismatch;
                }
            }
        }
    }

    const row_count = op.rows.len;
    if (row_count == 0) {
        return try EmptyOp.createWithCount(ctx.allocator, 0);
    }

    // AUTO_INCREMENT resolution must run under the Table mutex so the
    // counter we reserve and the rows we hand to insertBatch stay in
    // lock-step. We reserve a contiguous block up front, then fill each
    // omitted/NULL AI cell with the next id while also bumping the
    // counter past any explicit value the caller supplied.
    const ai_idx_opt = @import("../api/table.zig").autoIncrementColumnIndex(tbl_schema);

    var builder = try InsertColumnBuilder.init(ctx.allocator, tbl_schema, row_count);
    defer builder.deinit();

    if (ai_idx_opt) |ai_idx| {
        t.mutex.lockUncancelable(t.io);
        defer t.mutex.unlock(t.io);

        const ai_col = tbl_schema.columns[ai_idx];
        const ai_src = schema_to_source[ai_idx];

        var next_counter = t.reserveAutoIncrement(@intCast(row_count));
        for (op.rows) |row| {
            for (tbl_schema.columns, 0..) |col, si| {
                const maybe_src = schema_to_source[si];
                const cell: ?Value = if (si == ai_idx) blk: {
                    const user_cell: ?Value = if (ai_src) |s| row[s] else null;
                    if (user_cell) |uv| {
                        // Explicit value; observe to push counter
                        // past it. integer-only validated at create.
                        t.observeAutoIncrement(integerValueAsI128(uv) catch return Error.TypeMismatch);
                        break :blk uv;
                    }
                    // Omitted or explicit NULL → take the next id.
                    const id = next_counter;
                    next_counter += 1;
                    break :blk integerLiteralForType(ai_col.type, id) catch return Error.TypeMismatch;
                } else if (maybe_src) |src|
                    row[src]
                else if (col.default_value) |dv|
                    dv
                else
                    null;
                try builder.appendCell(si, col, cell);
            }
        }

        try t.insertBatchLocked(builder.schemaSlice(), builder.views(), row_count);
    } else {
        for (op.rows) |row| {
            for (tbl_schema.columns, 0..) |col, si| {
                const maybe_src = schema_to_source[si];
                // Resolution order for the cell:
                //   1. User-supplied value (incl. an explicit NULL on a
                //      nullable column).
                //   2. Column DEFAULT, when no source AND a default exists.
                //   3. NULL (the column must be nullable to reach this
                //      branch — validation above ensures it).
                const cell: ?Value = if (maybe_src) |src|
                    row[src]
                else if (col.default_value) |dv|
                    dv
                else
                    null;
                try builder.appendCell(si, col, cell);
            }
        }

        try t.insertBatch(builder.schemaSlice(), builder.views(), row_count);
    }

    ctx.affected_rows = @intCast(row_count);
    return try EmptyOp.createWithCount(ctx.allocator, @intCast(row_count));
}

/// Extract an integer Value as i128 for AUTO_INCREMENT counter
/// observation. Rejects non-integer tags so a stray DEFAULT for a
/// text/decimal column never bumps the counter.
fn integerValueAsI128(v: Value) !i128 {
    return switch (v) {
        .tinyint => |x| @as(i128, x),
        .smallint => |x| @as(i128, x),
        .int => |x| @as(i128, x),
        .bigint => |x| @as(i128, x),
        .largeint => |x| x,
        else => Error.TypeMismatch,
    };
}

/// Build a typed integer literal for the AI column type, given a
/// freshly reserved u64 counter value. Saturates at the column's
/// representable max — beyond that, future inserts will keep getting
/// the same value, which surfaces as a unique-key violation downstream.
fn integerLiteralForType(t: types.Type, id: u64) !Value {
    return switch (t) {
        .tinyint => Value{ .tinyint = @intCast(@min(id, @as(u64, @intCast(std.math.maxInt(i8))))) },
        .smallint => Value{ .smallint = @intCast(@min(id, @as(u64, @intCast(std.math.maxInt(i16))))) },
        .int => Value{ .int = @intCast(@min(id, @as(u64, @intCast(std.math.maxInt(i32))))) },
        .bigint => Value{ .bigint = @intCast(@min(id, @as(u64, @intCast(std.math.maxInt(i64))))) },
        .largeint => Value{ .largeint = @as(i128, id) },
        else => Error.TypeMismatch,
    };
}

/// Per-column buffer used to bulk-collect literal Value rows for an
/// INSERT, then hand them to `Table.insertBatch` as ColumnView slices.
/// Each fixed-width column owns an aligned-byte slab so `bytesAsSlice(T)`
/// returns a properly-aligned typed slice without an extra copy.
pub const InsertColumnBuilder = struct {
    allocator: Allocator,
    schema_copy: []types.Column,
    /// 16-byte aligned slabs sized for the full row count up front.
    /// Indexed by column index, length = row_count * size_of_type.
    fixed_slabs: [][]align(16) u8,
    /// Per-column write cursor into `fixed_slabs[i]` (bytes written so far).
    fixed_cursor: []usize,
    string_offsets: []std.ArrayListUnmanaged(u32),
    string_bytes: []std.ArrayListUnmanaged(u8),
    nulls: []std.ArrayListUnmanaged(u8),
    view_slice: []storage.ColumnView,
    row_count: usize,

    pub fn init(allocator: Allocator, table_schema: TableSchema, row_count: usize) !InsertColumnBuilder {
        const n_cols = table_schema.columns.len;
        const schema_copy = try allocator.alloc(types.Column, n_cols);
        errdefer allocator.free(schema_copy);
        for (table_schema.columns, 0..) |c, i| schema_copy[i] = c;

        const fixed_slabs = try allocator.alloc([]align(16) u8, n_cols);
        errdefer allocator.free(fixed_slabs);
        var slabs_inited: usize = 0;
        errdefer for (fixed_slabs[0..slabs_inited]) |s| if (s.len > 0) allocator.free(s);
        for (table_schema.columns, 0..) |c, i| {
            const per_row: usize = switch (c.type) {
                .int, .date, .float => 4,
                .bigint, .double, .datetime, .decimal64 => 8,
                .smallint => 2,
                .tinyint, .boolean => 1,
                .largeint, .decimal128, .uuid => 16,
                .varchar, .string, .char => 0,
            };
            fixed_slabs[i] = if (per_row == 0)
                &[_]u8{}
            else
                try allocator.alignedAlloc(u8, .@"16", per_row * row_count);
            slabs_inited = i + 1;
        }

        const fixed_cursor = try allocator.alloc(usize, n_cols);
        errdefer allocator.free(fixed_cursor);
        for (fixed_cursor) |*c| c.* = 0;

        const string_offsets = try allocator.alloc(std.ArrayListUnmanaged(u32), n_cols);
        errdefer allocator.free(string_offsets);
        for (string_offsets) |*b| b.* = .empty;

        const string_bytes = try allocator.alloc(std.ArrayListUnmanaged(u8), n_cols);
        errdefer allocator.free(string_bytes);
        for (string_bytes) |*b| b.* = .empty;

        const nulls = try allocator.alloc(std.ArrayListUnmanaged(u8), n_cols);
        errdefer allocator.free(nulls);
        for (nulls) |*b| b.* = .empty;

        const view_slice = try allocator.alloc(storage.ColumnView, n_cols);
        errdefer allocator.free(view_slice);

        const bitmap_len = (row_count + 7) / 8;
        for (table_schema.columns, 0..) |c, i| {
            if (c.nullable) try nulls[i].appendNTimes(allocator, 0, bitmap_len);
            if (c.type.isString()) try string_offsets[i].append(allocator, 0);
        }
        return .{
            .allocator = allocator,
            .schema_copy = schema_copy,
            .fixed_slabs = fixed_slabs,
            .fixed_cursor = fixed_cursor,
            .string_offsets = string_offsets,
            .string_bytes = string_bytes,
            .nulls = nulls,
            .view_slice = view_slice,
            .row_count = row_count,
        };
    }

    pub fn deinit(self: *InsertColumnBuilder) void {
        for (self.fixed_slabs) |s| if (s.len > 0) self.allocator.free(s);
        for (self.string_offsets) |*b| b.deinit(self.allocator);
        for (self.string_bytes) |*b| b.deinit(self.allocator);
        for (self.nulls) |*b| b.deinit(self.allocator);
        self.allocator.free(self.fixed_slabs);
        self.allocator.free(self.fixed_cursor);
        self.allocator.free(self.string_offsets);
        self.allocator.free(self.string_bytes);
        self.allocator.free(self.nulls);
        self.allocator.free(self.schema_copy);
        self.allocator.free(self.view_slice);
    }

    pub fn schemaSlice(self: *InsertColumnBuilder) []const types.Column {
        return self.schema_copy;
    }

    pub fn appendCell(self: *InsertColumnBuilder, col_idx: usize, col: types.Column, maybe_val: ?Value) !void {
        const row_in_col = self.currentRow(col_idx, col);
        if (maybe_val == null) {
            try self.appendPlaceholder(col_idx, col);
            return;
        }
        const v = maybe_val.?;
        if (col.nullable) {
            self.nulls[col_idx].items[row_in_col >> 3] |= @as(u8, 1) << @intCast(row_in_col & 7);
        }
        try self.appendCoerced(col_idx, col, v);
    }

    fn currentRow(self: *InsertColumnBuilder, col_idx: usize, col: types.Column) usize {
        return switch (col.type) {
            .int, .date, .float => self.fixed_cursor[col_idx] / 4,
            .bigint, .double, .datetime, .decimal64 => self.fixed_cursor[col_idx] / 8,
            .smallint => self.fixed_cursor[col_idx] / 2,
            .tinyint, .boolean => self.fixed_cursor[col_idx],
            .largeint, .decimal128, .uuid => self.fixed_cursor[col_idx] / 16,
            .varchar, .string, .char => self.string_offsets[col_idx].items.len - 1,
        };
    }

    fn appendPlaceholder(self: *InsertColumnBuilder, col_idx: usize, col: types.Column) !void {
        switch (col.type) {
            .int, .date, .float => self.writeFixedZero(col_idx, 4),
            .bigint, .double, .datetime, .decimal64 => self.writeFixedZero(col_idx, 8),
            .smallint => self.writeFixedZero(col_idx, 2),
            .tinyint, .boolean => self.writeFixedZero(col_idx, 1),
            .largeint, .decimal128, .uuid => self.writeFixedZero(col_idx, 16),
            .varchar, .string, .char => {
                const cur = self.string_offsets[col_idx].items[self.string_offsets[col_idx].items.len - 1];
                try self.string_offsets[col_idx].append(self.allocator, cur);
            },
        }
    }

    fn writeFixedZero(self: *InsertColumnBuilder, col_idx: usize, width: usize) void {
        const cursor = self.fixed_cursor[col_idx];
        @memset(self.fixed_slabs[col_idx][cursor .. cursor + width], 0);
        self.fixed_cursor[col_idx] = cursor + width;
    }

    fn writeFixedBytes(self: *InsertColumnBuilder, col_idx: usize, bytes: []const u8) void {
        const cursor = self.fixed_cursor[col_idx];
        @memcpy(self.fixed_slabs[col_idx][cursor .. cursor + bytes.len], bytes);
        self.fixed_cursor[col_idx] = cursor + bytes.len;
    }

    fn appendCoerced(self: *InsertColumnBuilder, col_idx: usize, col: types.Column, v: Value) !void {
        switch (col.type) {
            .int => self.writeFixedInt(col_idx, i32, try coerceToI32(v)),
            .bigint => self.writeFixedInt(col_idx, i64, try coerceToI64(v)),
            .smallint => self.writeFixedInt(col_idx, i16, try coerceToI16(v)),
            .tinyint => self.writeFixedBytes(col_idx, &[_]u8{@as(u8, @bitCast(try coerceToI8(v)))}),
            .largeint => self.writeFixedInt(col_idx, i128, try coerceToI128(v)),
            .boolean => self.writeFixedBytes(col_idx, &[_]u8{@intFromBool(try coerceToBool(v))}),
            .float => self.writeFixedFloat(col_idx, f32, try coerceToF32(v)),
            .double => self.writeFixedFloat(col_idx, f64, try coerceToF64(v)),
            .date => self.writeFixedInt(col_idx, i32, try coerceToDate(v)),
            .datetime => self.writeFixedInt(col_idx, i64, try coerceToDateTime(v)),
            .decimal64 => |spec| self.writeFixedInt(col_idx, i64, try coerceToDecimal64(v, spec)),
            .decimal128 => |spec| self.writeFixedInt(col_idx, i128, try coerceToDecimal128(v, spec)),
            .uuid => self.writeFixedInt(col_idx, u128, try coerceToUuid(v)),
            .varchar, .string, .char => {
                const s = try coerceToText(v);
                const sb = &self.string_bytes[col_idx];
                try sb.appendSlice(self.allocator, s);
                try self.string_offsets[col_idx].append(self.allocator, @intCast(sb.items.len));
            },
        }
    }

    fn writeFixedInt(self: *InsertColumnBuilder, col_idx: usize, comptime T: type, v: T) void {
        var buf: [@sizeOf(T)]u8 = undefined;
        std.mem.writeInt(T, &buf, v, .little);
        self.writeFixedBytes(col_idx, &buf);
    }

    fn writeFixedFloat(self: *InsertColumnBuilder, col_idx: usize, comptime T: type, v: T) void {
        const Bits = if (T == f32) u32 else u64;
        var buf: [@sizeOf(T)]u8 = undefined;
        std.mem.writeInt(Bits, &buf, @bitCast(v), .little);
        self.writeFixedBytes(col_idx, &buf);
    }

    pub fn views(self: *InsertColumnBuilder) []const storage.ColumnView {
        for (self.schema_copy, 0..) |col, i| {
            const data: @import("../storage/column.zig").ValueView = switch (col.type) {
                .int => .{ .int = std.mem.bytesAsSlice(i32, self.fixed_slabs[i])[0..self.row_count] },
                .bigint => .{ .bigint = std.mem.bytesAsSlice(i64, self.fixed_slabs[i])[0..self.row_count] },
                .smallint => .{ .smallint = std.mem.bytesAsSlice(i16, self.fixed_slabs[i])[0..self.row_count] },
                .tinyint => .{ .tinyint = std.mem.bytesAsSlice(i8, self.fixed_slabs[i])[0..self.row_count] },
                .largeint => .{ .largeint = std.mem.bytesAsSlice(i128, self.fixed_slabs[i])[0..self.row_count] },
                .boolean => .{ .boolean = self.fixed_slabs[i][0..self.row_count] },
                .float => .{ .float = std.mem.bytesAsSlice(f32, self.fixed_slabs[i])[0..self.row_count] },
                .double => .{ .double = std.mem.bytesAsSlice(f64, self.fixed_slabs[i])[0..self.row_count] },
                .date => .{ .date = std.mem.bytesAsSlice(i32, self.fixed_slabs[i])[0..self.row_count] },
                .datetime => .{ .datetime = std.mem.bytesAsSlice(i64, self.fixed_slabs[i])[0..self.row_count] },
                .decimal64 => .{ .decimal64 = std.mem.bytesAsSlice(i64, self.fixed_slabs[i])[0..self.row_count] },
                .decimal128 => .{ .decimal128 = std.mem.bytesAsSlice(i128, self.fixed_slabs[i])[0..self.row_count] },
                .uuid => .{ .uuid = std.mem.bytesAsSlice(u128, self.fixed_slabs[i])[0..self.row_count] },
                .varchar => .{ .varchar = .{
                    .offsets = self.string_offsets[i].items,
                    .bytes = self.string_bytes[i].items,
                } },
                .string => .{ .string = .{
                    .offsets = self.string_offsets[i].items,
                    .bytes = self.string_bytes[i].items,
                } },
                .char => .{ .char = .{
                    .offsets = self.string_offsets[i].items,
                    .bytes = self.string_bytes[i].items,
                } },
            };
            self.view_slice[i] = .{ .data = data, .nulls = if (col.nullable) self.nulls[i].items else null };
        }
        return self.view_slice;
    }
};

fn coerceToI64(v: Value) !i64 {
    return switch (v) {
        .int => |x| @as(i64, x),
        .bigint => |x| x,
        .smallint => |x| @as(i64, x),
        .tinyint => |x| @as(i64, x),
        else => Error.TypeMismatch,
    };
}

fn coerceToI32(v: Value) !i32 {
    return switch (v) {
        .int => |x| x,
        .bigint => |x| if (x >= std.math.minInt(i32) and x <= std.math.maxInt(i32)) @intCast(x) else Error.TypeMismatch,
        .smallint => |x| @as(i32, x),
        .tinyint => |x| @as(i32, x),
        else => Error.TypeMismatch,
    };
}

fn coerceToI16(v: Value) !i16 {
    return switch (v) {
        .int => |x| if (x >= std.math.minInt(i16) and x <= std.math.maxInt(i16)) @intCast(x) else Error.TypeMismatch,
        .bigint => |x| if (x >= std.math.minInt(i16) and x <= std.math.maxInt(i16)) @intCast(x) else Error.TypeMismatch,
        .smallint => |x| x,
        .tinyint => |x| @as(i16, x),
        else => Error.TypeMismatch,
    };
}

fn coerceToI8(v: Value) !i8 {
    return switch (v) {
        .int => |x| if (x >= std.math.minInt(i8) and x <= std.math.maxInt(i8)) @intCast(x) else Error.TypeMismatch,
        .bigint => |x| if (x >= std.math.minInt(i8) and x <= std.math.maxInt(i8)) @intCast(x) else Error.TypeMismatch,
        .smallint => |x| if (x >= std.math.minInt(i8) and x <= std.math.maxInt(i8)) @intCast(x) else Error.TypeMismatch,
        .tinyint => |x| x,
        else => Error.TypeMismatch,
    };
}

fn coerceToI128(v: Value) !i128 {
    return switch (v) {
        .int => |x| @as(i128, x),
        .bigint => |x| @as(i128, x),
        .smallint => |x| @as(i128, x),
        .tinyint => |x| @as(i128, x),
        .largeint => |x| x,
        else => Error.TypeMismatch,
    };
}

fn coerceToBool(v: Value) !bool {
    return switch (v) {
        .boolean => |x| x,
        else => Error.TypeMismatch,
    };
}

fn coerceToF32(v: Value) !f32 {
    return switch (v) {
        .float => |x| x,
        .double => |x| @floatCast(x),
        .int => |x| @floatFromInt(x),
        .bigint => |x| @floatFromInt(x),
        else => Error.TypeMismatch,
    };
}

fn coerceToF64(v: Value) !f64 {
    return switch (v) {
        .float => |x| @floatCast(x),
        .double => |x| x,
        .int => |x| @floatFromInt(x),
        .bigint => |x| @floatFromInt(x),
        else => Error.TypeMismatch,
    };
}

fn coerceToText(v: Value) ![]const u8 {
    return switch (v) {
        .text => |s| s,
        else => Error.TypeMismatch,
    };
}

fn coerceToDate(v: Value) !i32 {
    return switch (v) {
        .date => |d| d,
        .text => |s| parseDateLiteral(s) catch Error.TypeMismatch,
        else => Error.TypeMismatch,
    };
}

fn coerceToDateTime(v: Value) !i64 {
    return switch (v) {
        .datetime => |d| d,
        .text => |s| parseDateTimeLiteral(s) catch Error.TypeMismatch,
        .date => |d| @as(i64, d) * (86_400 * 1_000_000),
        else => Error.TypeMismatch,
    };
}

fn coerceToUuid(v: Value) !u128 {
    return switch (v) {
        .uuid => |u| u,
        .text => |s| parseUuidLiteral(s) catch Error.TypeMismatch,
        else => Error.TypeMismatch,
    };
}

fn coerceToDecimal64(v: Value, spec: @import("../types.zig").DecimalSpec) !i64 {
    return switch (v) {
        .decimal64 => |d| d,
        .int => |x| try scaleIntToDecimal(i64, x, spec.s),
        .bigint => |x| try scaleIntToDecimal(i64, x, spec.s),
        .float => |x| try scaleFloatToDecimal(i64, x, spec),
        .double => |x| try scaleFloatToDecimal(i64, x, spec),
        .text => |s| try parseDecimalLiteral(i64, s, spec),
        else => Error.TypeMismatch,
    };
}

fn coerceToDecimal128(v: Value, spec: @import("../types.zig").DecimalSpec) !i128 {
    return switch (v) {
        .decimal64 => |d| @as(i128, d),
        .decimal128 => |d| d,
        .int => |x| try scaleIntToDecimal(i128, x, spec.s),
        .bigint => |x| try scaleIntToDecimal(i128, x, spec.s),
        .float => |x| try scaleFloatToDecimal(i128, x, spec),
        .double => |x| try scaleFloatToDecimal(i128, x, spec),
        .text => |s| try parseDecimalLiteral(i128, s, spec),
        else => Error.TypeMismatch,
    };
}

/// Floating literal → decimal mantissa. The lexer parses `1.50` to f64 before
/// the column type is known, so a decimal-pointed literal inserted into a
/// DECIMAL column arrives here; round to the column scale. Precision is bounded
/// by f64 (~15 sig digits) — exact for the scales seen in practice.
fn scaleFloatToDecimal(comptime T: type, x: f64, spec: @import("../types.zig").DecimalSpec) !T {
    const factor = std.math.pow(f64, 10.0, @floatFromInt(spec.s));
    const scaled = @round(x * factor);
    const limit = std.math.pow(f64, 10.0, @floatFromInt(spec.p));
    if (scaled >= limit or scaled <= -limit) return Error.TypeMismatch;
    return @intFromFloat(scaled);
}

fn scaleIntToDecimal(comptime T: type, x: anytype, scale: u8) !T {
    var out: T = @intCast(x);
    var i: u8 = 0;
    while (i < scale) : (i += 1) {
        out = std.math.mul(T, out, 10) catch return Error.TypeMismatch;
    }
    return out;
}

pub fn parseDecimalLiteral(comptime T: type, s: []const u8, spec: @import("../types.zig").DecimalSpec) !T {
    // Accept optional sign, digits, optional '.' followed by digits. Right-
    // pad or truncate the fractional part to the column's scale.
    var idx: usize = 0;
    var negate = false;
    if (idx < s.len and (s[idx] == '-' or s[idx] == '+')) {
        negate = s[idx] == '-';
        idx += 1;
    }
    var int_part: T = 0;
    while (idx < s.len and s[idx] >= '0' and s[idx] <= '9') : (idx += 1) {
        int_part = std.math.mul(T, int_part, 10) catch return Error.TypeMismatch;
        int_part = std.math.add(T, int_part, @as(T, s[idx] - '0')) catch return Error.TypeMismatch;
    }
    var frac_digits: u8 = 0;
    var frac_part: T = 0;
    if (idx < s.len and s[idx] == '.') {
        idx += 1;
        while (idx < s.len and s[idx] >= '0' and s[idx] <= '9' and frac_digits < spec.s) : (idx += 1) {
            frac_part = std.math.mul(T, frac_part, 10) catch return Error.TypeMismatch;
            frac_part = std.math.add(T, frac_part, @as(T, s[idx] - '0')) catch return Error.TypeMismatch;
            frac_digits += 1;
        }
        // Skip trailing digits beyond the target scale (truncate).
        while (idx < s.len and s[idx] >= '0' and s[idx] <= '9') : (idx += 1) {}
    }
    if (idx != s.len) return Error.TypeMismatch;
    // Right-pad the fractional part to the target scale.
    while (frac_digits < spec.s) : (frac_digits += 1) {
        frac_part = std.math.mul(T, frac_part, 10) catch return Error.TypeMismatch;
    }
    var scaled = std.math.mul(T, int_part, std.math.powi(T, 10, spec.s) catch return Error.TypeMismatch) catch return Error.TypeMismatch;
    scaled = std.math.add(T, scaled, frac_part) catch return Error.TypeMismatch;
    if (negate) scaled = -scaled;
    return scaled;
}

/// `YYYY-MM-DD` → days since the Unix epoch. Uses civil-from-days math
/// (Howard Hinnant's algorithm) for correctness across leap years.
pub fn parseDateLiteral(s: []const u8) !i32 {
    if (s.len < 10) return Error.TypeMismatch;
    if (s[4] != '-' or s[7] != '-') return Error.TypeMismatch;
    const year = try parseIntField(i32, s[0..4]);
    const month = try parseIntField(u32, s[5..7]);
    const day = try parseIntField(u32, s[8..10]);
    if (month < 1 or month > 12 or day < 1 or day > 31) return Error.TypeMismatch;
    return civilToDays(year, month, day);
}

pub fn parseDateTimeLiteral(s: []const u8) !i64 {
    if (s.len < 19) return Error.TypeMismatch;
    if (s[4] != '-' or s[7] != '-') return Error.TypeMismatch;
    const sep = s[10];
    if (sep != ' ' and sep != 'T') return Error.TypeMismatch;
    if (s[13] != ':' or s[16] != ':') return Error.TypeMismatch;
    const year = try parseIntField(i32, s[0..4]);
    const month = try parseIntField(u32, s[5..7]);
    const day = try parseIntField(u32, s[8..10]);
    const hour = try parseIntField(u32, s[11..13]);
    const minute = try parseIntField(u32, s[14..16]);
    const second = try parseIntField(u32, s[17..19]);
    if (hour > 23 or minute > 59 or second > 59) return Error.TypeMismatch;
    var micros: u64 = 0;
    if (s.len > 19) {
        if (s[19] != '.') return Error.TypeMismatch;
        var idx: usize = 20;
        var digits: usize = 0;
        while (idx < s.len and digits < 6 and s[idx] >= '0' and s[idx] <= '9') : (idx += 1) {
            micros = micros * 10 + (s[idx] - '0');
            digits += 1;
        }
        // Right-pad to microseconds.
        while (digits < 6) : (digits += 1) micros *= 10;
        if (idx != s.len) return Error.TypeMismatch;
    }
    const days = try civilToDays(year, month, day);
    const day_secs: i64 = @as(i64, hour) * 3600 + @as(i64, minute) * 60 + @as(i64, second);
    return @as(i64, days) * 86_400 * 1_000_000 + day_secs * 1_000_000 + @as(i64, @intCast(micros));
}

pub fn parseUuidLiteral(s: []const u8) !u128 {
    if (s.len != 36) return Error.TypeMismatch;
    if (s[8] != '-' or s[13] != '-' or s[18] != '-' or s[23] != '-') return Error.TypeMismatch;
    var out: u128 = 0;
    var idx: usize = 0;
    for (s) |ch| {
        if (ch == '-') continue;
        const nib: u128 = switch (ch) {
            '0'...'9' => ch - '0',
            'a'...'f' => ch - 'a' + 10,
            'A'...'F' => ch - 'A' + 10,
            else => return Error.TypeMismatch,
        };
        out = (out << 4) | nib;
        idx += 1;
    }
    if (idx != 32) return Error.TypeMismatch;
    return out;
}

fn parseIntField(comptime T: type, s: []const u8) !T {
    return std.fmt.parseInt(T, s, 10) catch return Error.TypeMismatch;
}

fn civilToDays(year: i32, month: u32, day: u32) !i32 {
    if (month == 0 or day == 0) return Error.TypeMismatch;
    return wire_format.daysFromCivil(year, month, day);
}

fn compileShow(ctx: *CompileCtx, s: ir.ShowOp) !Query {
    const catalog = catalogFor(ctx.db) orelse return Error.DatabaseNotFound;
    return switch (s) {
        .databases => blk: {
            const names = try catalog.listDatabases(ctx.allocator);
            defer freeOwnedNames(ctx.allocator, names);
            break :blk try NamesOp.create(ctx.allocator, "name", names);
        },
        .schemas => |db_arg| blk: {
            const db_name = db_arg orelse ctx.session.current_db;
            const db = catalog.database(db_name) orelse return Error.DatabaseNotFound;
            const names = try db.listSchemas(ctx.allocator);
            defer freeOwnedNames(ctx.allocator, names);
            break :blk try NamesOp.create(ctx.allocator, "name", names);
        },
        .tables => |ref| blk: {
            const db_name = ref.database orelse ctx.session.current_db;
            const db = catalog.database(db_name) orelse return Error.DatabaseNotFound;
            const sc_name = ref.schema orelse ctx.session.current_schema;
            const sc = db.schema(sc_name) orelse return Error.SchemaNotFound;
            const names = try unionSchemaAndTempTables(ctx.allocator, sc, ctx.session.temp_namespace, ref);
            defer freeOwnedNames(ctx.allocator, names);
            break :blk try NamesOp.create(ctx.allocator, "name", names);
        },
    };
}

fn freeOwnedNames(allocator: Allocator, names: [][]u8) void {
    for (names) |n| allocator.free(n);
    allocator.free(names);
}

/// Build the union of a schema's persistent tables with the active
/// session's temp tables. Temp tables only appear when the SHOW TABLES
/// target schema matches the session's current schema (the temp
/// namespace is conceptually session-local, not schema-local — listing
/// them in some other schema would surprise users).
fn unionSchemaAndTempTables(
    allocator: Allocator,
    sc: *DbSchema,
    temp_ns: ?*thindb_api.TempNamespace,
    ref: ir.TableRef,
) ![][]u8 {
    const include_temps = ref.database == null and ref.schema == null and temp_ns != null;
    if (!include_temps) return sc.listTables(allocator);

    var out: std.ArrayList([]u8) = .empty;
    errdefer {
        for (out.items) |s| allocator.free(s);
        out.deinit(allocator);
    }

    const persistent = try sc.listTables(allocator);
    defer allocator.free(persistent);
    for (persistent) |n| try out.append(allocator, n);

    const temps = try temp_ns.?.listTables(allocator);
    defer freeOwnedNames(allocator, temps);
    for (temps) |n| {
        var clash = false;
        for (persistent) |p| {
            if (std.mem.eql(u8, p, n)) {
                clash = true;
                break;
            }
        }
        if (clash) continue;
        try out.append(allocator, try allocator.dupe(u8, n));
    }
    return try out.toOwnedSlice(allocator);
}

// ---------------------------------------------------------------------------
// Synthetic operators for DDL / SHOW.
//
// DDL ops return `EmptyOp` — single output column carrying a status name
// only when needed (today: nothing — `.next()` returns null on first
// call). SHOW ops return `NamesOp`, which materializes a list of names
// into one string column and emits a single Batch.
// ---------------------------------------------------------------------------

/// FROM-less SELECT source: emits exactly one row with zero columns.
/// A Compute/Project on top turns the projected expressions into the
/// result (`SELECT 1+1`, `SELECT now()`).
pub const SingleRowSource = struct {
    allocator: Allocator,
    emitted: bool = false,

    pub fn create(allocator: Allocator) !Query {
        const self = try allocator.create(SingleRowSource);
        self.* = .{ .allocator = allocator };
        return exec.makeQuery(allocator, self);
    }

    pub fn next(self: *SingleRowSource) !?exec.Batch {
        if (self.emitted) return null;
        self.emitted = true;
        return exec.Batch{ .schema = &.{}, .values = &.{}, .row_count = 1 };
    }

    pub fn deinit(self: *SingleRowSource) void {
        self.allocator.destroy(self);
    }

    pub fn outputSchema(_: *SingleRowSource) []const types.Column {
        return &.{};
    }

    pub fn addPrune(_: *SingleRowSource, _: exec.Predicate) !void {}

    pub fn stats(_: *SingleRowSource) exec.PipelineStats {
        return .{ .upper_rows = 1 };
    }

    pub fn accountant(_: *SingleRowSource) ?*exec.memory.MemoryAccountant {
        return null;
    }

    pub fn explain(_: *SingleRowSource, out: *std.ArrayList(u8), allocator: Allocator, depth: usize) !void {
        try exec.explainLine(out, allocator, depth, "SingleRow");
    }
};

const EmptyOp = struct {
    allocator: Allocator,
    schema: [1]types.Column,
    /// Affected-row count carried by side-effect operators (today: only
    /// INSERT). Wire layers (MySQL OK_Packet.affected_rows, PG
    /// CommandComplete "INSERT 0 N") read this off the CompiledQuery
    /// before tearing it down.
    affected_rows: u64 = 0,

    fn create(allocator: Allocator) !Query {
        return createWithCount(allocator, 0);
    }

    fn createWithCount(allocator: Allocator, affected: u64) !Query {
        const self = try allocator.create(EmptyOp);
        errdefer allocator.destroy(self);
        self.* = .{
            .allocator = allocator,
            .schema = .{.{ .name = "result", .type = .string }},
            .affected_rows = affected,
        };
        return exec.makeQuery(allocator, self);
    }

    pub fn next(_: *EmptyOp) !?exec.Batch {
        return null;
    }

    pub fn deinit(self: *EmptyOp) void {
        const allocator = self.allocator;
        allocator.destroy(self);
    }

    pub fn outputSchema(self: *EmptyOp) []const types.Column {
        return self.schema[0..];
    }

    pub fn addPrune(_: *EmptyOp, _: exec.Predicate) !void {}

    pub fn stats(_: *EmptyOp) exec.PipelineStats {
        return .{ .upper_rows = 0 };
    }

    pub fn accountant(_: *EmptyOp) ?*exec.memory.MemoryAccountant {
        return null;
    }

    pub fn explain(_: *EmptyOp, out: *std.ArrayList(u8), allocator: Allocator, depth: usize) !void {
        try exec.explainLine(out, allocator, depth, "Empty");
    }
};

/// Indent level of a plan-text line: each tree level is exactly two
/// leading spaces (see `exec.explainIndent`).
fn planLineDepth(line: []const u8) usize {
    var n: usize = 0;
    while (n + 2 <= line.len and line[n] == ' ' and line[n + 1] == ' ') n += 2;
    return n / 2;
}

fn jsonEscapeInto(out: *std.ArrayList(u8), allocator: Allocator, s: []const u8) !void {
    const hex = "0123456789abcdef";
    for (s) |c| switch (c) {
        '"' => try out.appendSlice(allocator, "\\\""),
        '\\' => try out.appendSlice(allocator, "\\\\"),
        '\n' => try out.appendSlice(allocator, "\\n"),
        '\t' => try out.appendSlice(allocator, "\\t"),
        else => if (c < 0x20) {
            try out.appendSlice(allocator, "\\u00");
            try out.append(allocator, hex[(c >> 4) & 0xf]);
            try out.append(allocator, hex[c & 0xf]);
        } else try out.append(allocator, c),
    };
}

/// Emit the node at `lines[idx.*]` (known to sit at `depth`) and recurse
/// into its children (each at `depth + 1`), advancing `idx` past the
/// whole subtree. Children always sit exactly one level deeper because
/// the text renderer increments depth by one per nesting level.
fn emitPlanNode(
    out: *std.ArrayList(u8),
    allocator: Allocator,
    lines: []const []const u8,
    idx: *usize,
    depth: usize,
) !void {
    const label = lines[idx.*][depth * 2 ..];
    idx.* += 1;
    try out.appendSlice(allocator, "{\"node\":\"");
    try jsonEscapeInto(out, allocator, label);
    try out.appendSlice(allocator, "\",\"children\":[");
    var first = true;
    while (idx.* < lines.len and planLineDepth(lines[idx.*]) == depth + 1) {
        if (!first) try out.append(allocator, ',');
        first = false;
        try emitPlanNode(out, allocator, lines, idx, depth + 1);
    }
    try out.appendSlice(allocator, "]}");
}

/// Render the indented plan text into a JSON tree of
/// `{"node": <label>, "children": [...]}`. Caller owns the result.
fn planTextToJson(allocator: Allocator, plan: []const u8) ![]u8 {
    var lines: std.ArrayListUnmanaged([]const u8) = .empty;
    defer lines.deinit(allocator);
    var it = std.mem.splitScalar(u8, plan, '\n');
    while (it.next()) |l| {
        if (std.mem.trim(u8, l, " ").len > 0) try lines.append(allocator, l);
    }

    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(allocator);
    if (lines.items.len == 0) {
        try out.appendSlice(allocator, "{}");
        return out.toOwnedSlice(allocator);
    }
    var idx: usize = 0;
    try emitPlanNode(&out, allocator, lines.items, &idx, 0);
    return out.toOwnedSlice(allocator);
}

const NamesOp = struct {
    allocator: Allocator,
    schema: [1]types.Column,
    offsets: []u32,
    bytes: []u8,
    views: [1]storage.ColumnView,
    row_count: usize,
    emitted: bool,

    fn create(allocator: Allocator, col_name: []const u8, names: [][]u8) !Query {
        const owned_name = try allocator.dupe(u8, col_name);
        errdefer allocator.free(owned_name);

        const offsets = try allocator.alloc(u32, names.len + 1);
        errdefer allocator.free(offsets);

        var total: u32 = 0;
        offsets[0] = 0;
        for (names, 0..) |n, i| {
            total += @intCast(n.len);
            offsets[i + 1] = total;
        }

        const bytes = try allocator.alloc(u8, total);
        errdefer allocator.free(bytes);
        var pos: usize = 0;
        for (names) |n| {
            @memcpy(bytes[pos .. pos + n.len], n);
            pos += n.len;
        }

        const self = try allocator.create(NamesOp);
        errdefer allocator.destroy(self);
        self.* = .{
            .allocator = allocator,
            .schema = .{.{ .name = owned_name, .type = .string }},
            .offsets = offsets,
            .bytes = bytes,
            .views = undefined,
            .row_count = names.len,
            .emitted = false,
        };
        self.views[0] = .{ .data = .{ .string = .{ .offsets = self.offsets, .bytes = self.bytes } } };
        return exec.makeQuery(allocator, self);
    }

    pub fn next(self: *NamesOp) !?exec.Batch {
        if (self.emitted) return null;
        self.emitted = true;
        return exec.Batch{
            .schema = self.schema[0..],
            .values = self.views[0..],
            .row_count = self.row_count,
        };
    }

    pub fn deinit(self: *NamesOp) void {
        const allocator = self.allocator;
        allocator.free(self.schema[0].name);
        allocator.free(self.offsets);
        allocator.free(self.bytes);
        allocator.destroy(self);
    }

    pub fn outputSchema(self: *NamesOp) []const types.Column {
        return self.schema[0..];
    }

    pub fn addPrune(_: *NamesOp, _: exec.Predicate) !void {}

    pub fn stats(self: *NamesOp) exec.PipelineStats {
        return .{ .upper_rows = self.row_count };
    }

    pub fn accountant(_: *NamesOp) ?*exec.memory.MemoryAccountant {
        return null;
    }

    pub fn explain(_: *NamesOp, out: *std.ArrayList(u8), allocator: Allocator, depth: usize) !void {
        try exec.explainLine(out, allocator, depth, "Names");
    }
};

/// Allocator-owned slice of column-name slices. Caller frees via
/// `allocator.free(out)` once — the inner string slices are borrowed
/// from `upstream_cols` and don't need separate freeing.
pub fn complementColumns(
    allocator: Allocator,
    upstream_cols: []const @import("../types.zig").Column,
    excluded: []const []const u8,
) ![][]const u8 {
    var out: std.ArrayList([]const u8) = .empty;
    errdefer out.deinit(allocator);
    for (upstream_cols) |c| {
        var dropped = false;
        for (excluded) |ex| {
            if (types.columnNameEql(c.name, ex)) {
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
