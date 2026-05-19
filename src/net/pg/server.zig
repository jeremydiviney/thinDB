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

pub const Error = error{
    ServerClosed,
};

pub const Server = struct {
    allocator: Allocator,
    io: Io,
    catalog: *Catalog,
    listener: Io.net.Server,
    connection_counter: std.atomic.Value(u32) = .{ .raw = 0 },

    pub fn close(self: *Server) void {
        self.listener.socket.close(self.io);
        self.allocator.destroy(self);
    }

    pub fn acceptOne(self: *Server) !void {
        const stream = try self.listener.accept(self.io);
        defer stream.close(self.io);
        const cid = self.connection_counter.fetchAdd(1, .monotonic) + 1;
        handleConnection(self.allocator, self.io, self.catalog, stream, cid) catch |err| {
            std.debug.print("pg: connection error: {s}\n", .{@errorName(err)});
        };
    }

    pub fn run(self: *Server, should_stop: *std.atomic.Value(bool)) !void {
        while (!should_stop.load(.acquire)) {
            self.acceptOne() catch |err| {
                std.debug.print("pg: accept error: {s}\n", .{@errorName(err)});
            };
        }
    }
};

/// Bind a PostgreSQL listener on `address` for queries against `catalog`.
/// The Server does NOT own the catalog — caller manages its lifetime.
pub fn servePg(
    allocator: Allocator,
    io: Io,
    catalog: *Catalog,
    address: Io.net.IpAddress,
) !*Server {
    var listen_addr = address;
    const listener = try Io.net.IpAddress.listen(&listen_addr, io, .{
        .mode = .stream,
        .protocol = .tcp,
        .reuse_address = true,
    });
    const self = try allocator.create(Server);
    self.* = .{
        .allocator = allocator,
        .io = io,
        .catalog = catalog,
        .listener = listener,
    };
    return self;
}

const SessionState = struct {
    current_db: []u8,
    current_schema: []u8,
    application_name: []u8,
    allocator: Allocator,

    fn init(allocator: Allocator) !SessionState {
        return .{
            .allocator = allocator,
            .current_db = try allocator.dupe(u8, "main"),
            .current_schema = try allocator.dupe(u8, "public"),
            .application_name = try allocator.dupe(u8, ""),
        };
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
) !void {
    var read_buf: [16 * 1024]u8 = undefined;
    var write_buf: [16 * 1024]u8 = undefined;
    var reader = stream.reader(io, &read_buf);
    var writer = stream.writer(io, &write_buf);
    const w = &writer.interface;
    const r = &reader.interface;

    var session = try SessionState.init(allocator);
    defer session.deinit();

    if (!try completeStartup(allocator, w, r, catalog, &session, connection_id)) return;

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
                try startup.sendReadyForQuery(allocator, w, 'I');
                try w.flush();
            },
            else => {
                try errors.sendErrorResponse(allocator, w, "0A000".*, "unsupported message type");
                try startup.sendReadyForQuery(allocator, w, 'I');
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
        .cancel_request => return false,
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
        try startup.sendReadyForQuery(allocator, w, 'I');
        try w.flush();
        return;
    }

    var query_end = payload.len;
    while (query_end > 0 and payload[query_end - 1] == 0) query_end -= 1;
    const sql_text = payload[0..query_end];

    if (try canned.match(allocator, sql_text, session.current_db, session.current_schema)) |probe| {
        try dispatchProbe(allocator, w, catalog, session, probe);
        try startup.sendReadyForQuery(allocator, w, 'I');
        try w.flush();
        return;
    }

    runEngineQuery(allocator, w, catalog, session, sql_text) catch |err| {
        const mapped = errors.mapInternal(err);
        try errors.sendErrorResponse(allocator, w, mapped.sqlstate, mapped.message);
    };

    try startup.sendReadyForQuery(allocator, w, 'I');
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
        .accept => |tag| try result.sendCommandComplete(allocator, w, tag),
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

    const main_db = catalog.database(session.current_db) orelse return ApiError.DatabaseNotFound;

    var compiled = try local.compileWithSession(allocator, main_db, session.asSession(), op);
    defer compiled.deinit();

    if (isSideEffectOp(op.*)) {
        _ = try compiled.next();
        const new_session = compiled.sessionValue();
        try session.replaceDbSchema(new_session.current_db, new_session.current_schema);
        try result.sendCommandComplete(allocator, w, commandTagFor(op.*));
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
        .ddl => true,
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
