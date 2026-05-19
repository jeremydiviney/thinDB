//! MySQL wire-compatible server. Accepts connections, runs the
//! HandshakeV10 + HandshakeResponse41 exchange, then dispatches
//! COM_QUERY / COM_INIT_DB / COM_PING / COM_QUIT.
//!
//! Auth is trust-mode: credentials are read and discarded. The session
//! tracks (current_db, current_schema) and routes queries through the
//! existing SQL parser → `compileWithSession` pipeline.

const std = @import("std");
const Allocator = std.mem.Allocator;
const Io = std.Io;

/// Default TCP port for a MySQL-wire listener. Matches the upstream
/// MySQL default so existing client tooling Just Works.
pub const default_port: u16 = 3306;

const thindb_api = @import("../../api/api.zig");
const Catalog = thindb_api.Catalog;
const Session = thindb_api.Session;
const ApiError = thindb_api.Error;

const local = @import("../local.zig");
const ir = @import("../../ir/ir.zig");
const sql = @import("../../sql/sql.zig");

const packet = @import("packet.zig");
const handshake = @import("handshake.zig");
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
            std.debug.print("mysql: connection error: {s}\n", .{@errorName(err)});
        };
    }

    pub fn run(self: *Server, should_stop: *std.atomic.Value(bool)) !void {
        while (!should_stop.load(.acquire)) {
            self.acceptOne() catch |err| {
                std.debug.print("mysql: accept error: {s}\n", .{@errorName(err)});
            };
        }
    }
};

/// Bind a MySQL listener on `address` for queries against `catalog`.
/// The Server does NOT own the catalog — caller manages its lifetime.
pub fn serveMysql(
    allocator: Allocator,
    io: Io,
    catalog: *Catalog,
    address: Io.net.IpAddress,
) !*Server {
    const listener = try Io.net.IpAddress.listen(&address, io, .{
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
    allocator: Allocator,

    fn init(allocator: Allocator) !SessionState {
        return .{
            .allocator = allocator,
            .current_db = try allocator.dupe(u8, "main"),
            .current_schema = try allocator.dupe(u8, "public"),
        };
    }

    fn deinit(self: *SessionState) void {
        self.allocator.free(self.current_db);
        self.allocator.free(self.current_schema);
    }

    fn replace(self: *SessionState, db: []const u8, schema: []const u8) !void {
        const new_db = try self.allocator.dupe(u8, db);
        errdefer self.allocator.free(new_db);
        const new_schema = try self.allocator.dupe(u8, schema);
        self.allocator.free(self.current_db);
        self.allocator.free(self.current_schema);
        self.current_db = new_db;
        self.current_schema = new_schema;
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

    try handshake.sendInitialHandshake(allocator, w, connection_id);
    try w.flush();

    const hs_packet = try packet.readPacket(allocator, r);
    defer allocator.free(hs_packet.payload);
    const client = handshake.parseHandshakeResponse(hs_packet.payload) catch {
        try handshake.sendErrPacket(allocator, w, 2, 1064, "42000".*, "Malformed handshake");
        try w.flush();
        return;
    };

    if (client.initial_database) |db_name| {
        applyInitDb(catalog, &session, db_name) catch |err| {
            const mapped = errors.mapInternal(err, @errorName(err));
            try handshake.sendErrPacket(allocator, w, 2, mapped.code, mapped.sqlstate, mapped.message);
            try w.flush();
            return;
        };
    }

    try handshake.sendHandshakeOk(allocator, w);
    try w.flush();

    while (true) {
        const pkt = packet.readPacket(allocator, r) catch |err| switch (err) {
            error.EndOfStream => return,
            else => return err,
        };
        defer allocator.free(pkt.payload);
        if (pkt.payload.len == 0) continue;

        const cmd = pkt.payload[0];
        const body = pkt.payload[1..];
        switch (cmd) {
            0x01 => return,
            0x02 => try handleInitDb(allocator, w, catalog, &session, body),
            0x03 => try handleQuery(allocator, w, catalog, &session, body),
            0x0E => try handshake.sendOkPacket(allocator, w, 1, 0, 0),
            else => try handshake.sendErrPacket(allocator, w, 1, 1047, "HY000".*, "Unknown command"),
        }
        try w.flush();
    }
}

/// Apply the flattening rule for COM_INIT_DB / USE: `db__schema` splits
/// into both parts; otherwise treat the name as either a schema within
/// the current db or a top-level database.
fn applyInitDb(catalog: *Catalog, session: *SessionState, name: []const u8) !void {
    if (std.mem.indexOf(u8, name, "__")) |sep| {
        const db_name = name[0..sep];
        const sc_name = name[sep + 2 ..];
        const db = catalog.database(db_name) orelse return ApiError.DatabaseNotFound;
        _ = db.schema(sc_name) orelse return ApiError.SchemaNotFound;
        try session.replace(db_name, sc_name);
        return;
    }

    const cur_db = catalog.database(session.current_db) orelse return ApiError.DatabaseNotFound;
    if (cur_db.schema(name) != null) {
        try session.replace(session.current_db, name);
        return;
    }
    if (catalog.database(name) != null) {
        try session.replace(name, "public");
        return;
    }
    return ApiError.DatabaseNotFound;
}

fn handleInitDb(
    allocator: Allocator,
    w: *std.Io.Writer,
    catalog: *Catalog,
    session: *SessionState,
    payload: []const u8,
) !void {
    applyInitDb(catalog, session, payload) catch |err| {
        const mapped = errors.mapInternal(err, @errorName(err));
        try handshake.sendErrPacket(allocator, w, 1, mapped.code, mapped.sqlstate, mapped.message);
        return;
    };
    try handshake.sendOkPacket(allocator, w, 1, 0, 0);
}

fn handleQuery(
    allocator: Allocator,
    w: *std.Io.Writer,
    catalog: *Catalog,
    session: *SessionState,
    payload: []const u8,
) !void {
    var seq_id: u8 = 1;

    if (try canned.match(allocator, payload, session.current_schema)) |outcome| {
        switch (outcome) {
            .ok_packet => try handshake.sendOkPacket(allocator, w, seq_id, 0, 0),
            .single_value => |sv| try result.sendSingleValueResult(allocator, w, sv.col, sv.val, &seq_id),
            .single_null => |col| try result.sendSingleValueResult(allocator, w, col, null, &seq_id),
            .variable_row => |vr| try result.sendVariableRow(allocator, w, vr.name, vr.value, &seq_id),
            .empty_variables => try result.sendEmptyVariables(allocator, w, &seq_id),
        }
        return;
    }

    if (isShowDatabases(payload)) {
        try sendFlattenedDatabases(allocator, w, catalog, &seq_id);
        return;
    }

    try runEngineQuery(allocator, w, catalog, session, payload, &seq_id);
}

fn isShowDatabases(sql_text: []const u8) bool {
    var s = std.mem.trim(u8, sql_text, " \t\r\n");
    while (s.len > 0 and s[s.len - 1] == ';') s = std.mem.trim(u8, s[0 .. s.len - 1], " \t\r\n");
    return std.ascii.eqlIgnoreCase(s, "show databases") or std.ascii.eqlIgnoreCase(s, "show schemas");
}

fn sendFlattenedDatabases(
    allocator: Allocator,
    w: *std.Io.Writer,
    catalog: *Catalog,
    seq_id: *u8,
) !void {
    const db_names = try catalog.listDatabases(allocator);
    defer {
        for (db_names) |n| allocator.free(n);
        allocator.free(db_names);
    }

    var flat: std.ArrayList([]u8) = .empty;
    defer {
        for (flat.items) |s| allocator.free(s);
        flat.deinit(allocator);
    }

    for (db_names) |db_name| {
        const db = catalog.database(db_name) orelse continue;
        const schema_names = try db.listSchemas(allocator);
        defer {
            for (schema_names) |n| allocator.free(n);
            allocator.free(schema_names);
        }
        for (schema_names) |sc_name| {
            const joined = try std.fmt.allocPrint(allocator, "{s}__{s}", .{ db_name, sc_name });
            try flat.append(allocator, joined);
        }
    }

    var as_const: std.ArrayList([]const u8) = .empty;
    defer as_const.deinit(allocator);
    try as_const.ensureTotalCapacity(allocator, flat.items.len);
    for (flat.items) |s| as_const.appendAssumeCapacity(s);

    try result.sendSingleColumnRows(allocator, w, "Database", as_const.items, seq_id);
}

fn runEngineQuery(
    allocator: Allocator,
    w: *std.Io.Writer,
    catalog: *Catalog,
    session: *SessionState,
    payload: []const u8,
    seq_id: *u8,
) !void {
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();

    const op = sql.parse(arena.allocator(), payload) catch |err| {
        const mapped = errors.mapInternal(err, "Parse error");
        try handshake.sendErrPacket(allocator, w, seq_id.*, mapped.code, mapped.sqlstate, @errorName(err));
        return;
    };

    const main_db = catalog.database(session.current_db) orelse {
        try handshake.sendErrPacket(allocator, w, seq_id.*, 1049, "42000".*, "Unknown database");
        return;
    };

    var compiled = local.compileWithSession(allocator, main_db, session.asSession(), op) catch |err| {
        const mapped = errors.mapInternal(err, @errorName(err));
        try handshake.sendErrPacket(allocator, w, seq_id.*, mapped.code, mapped.sqlstate, mapped.message);
        return;
    };
    defer compiled.deinit();

    if (isDdl(op.*)) {
        _ = compiled.next() catch |err| {
            const mapped = errors.mapInternal(err, @errorName(err));
            try handshake.sendErrPacket(allocator, w, seq_id.*, mapped.code, mapped.sqlstate, mapped.message);
            return;
        };
        const new_session = compiled.sessionValue();
        try session.replace(new_session.current_db, new_session.current_schema);
        try handshake.sendOkPacket(allocator, w, seq_id.*, 0, 0);
        return;
    }

    try result.sendQueryResult(allocator, w, &compiled, session.current_db, "", seq_id);

    const new_session = compiled.sessionValue();
    try session.replace(new_session.current_db, new_session.current_schema);
}

fn isDdl(op: ir.Op) bool {
    return switch (op) {
        .ddl => true,
        else => false,
    };
}

test "applyInitDb resolves flat db__schema name" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var c = try Catalog.open(allocator, io, tmp.dir, .{});
    defer c.close();
    _ = try c.createDatabase("alpha");
    var session = try SessionState.init(allocator);
    defer session.deinit();
    try applyInitDb(c, &session, "alpha__public");
    try std.testing.expectEqualStrings("alpha", session.current_db);
    try std.testing.expectEqualStrings("public", session.current_schema);
}

test "applyInitDb honors schema-within-current-db lookup" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var c = try Catalog.open(allocator, io, tmp.dir, .{});
    defer c.close();
    const main_db = try c.createDatabase("main");
    _ = try main_db.createSchema("reports");
    var session = try SessionState.init(allocator);
    defer session.deinit();
    try applyInitDb(c, &session, "reports");
    try std.testing.expectEqualStrings("main", session.current_db);
    try std.testing.expectEqualStrings("reports", session.current_schema);
}
