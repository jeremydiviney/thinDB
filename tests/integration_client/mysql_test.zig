//! MySQL wire protocol integration tests.
//!
//! Two flavors:
//!   - Standalone: a small in-Zig client speaks just enough wire format
//!     to verify the server end-to-end (no external binary required).
//!   - mysql CLI: spawns the real `mysql` binary, asserts on its stdout.
//!     Skipped automatically when `mysql` isn't on PATH so CI without
//!     MySQL installed still passes.

const std = @import("std");
const thindb = @import("thindb");

const mysql_packet = thindb.mysql.packet;
const mysql_handshake = thindb.mysql.handshake;

const test_port_base: u16 = 28543;

const schema_orders = thindb.TableSchema{
    .columns = &.{
        .{ .name = "id", .type = .bigint },
        .{ .name = "qty", .type = .int },
        .{ .name = "tag", .type = .string },
    },
    .order_key = &.{"id"},
    .unique = true,
};
const ok_orders = [_][]const u8{"id"};
const opts_orders = thindb.TableOptions{
    .order_key = &ok_orders,
    .unique = true,
    .row_group_size = 4,
};

/// Extract the 20-byte mysql_native_password salt from a HandshakeV10
/// greeting payload. Layout: see src/net/mysql/handshake.zig —
/// auth-plugin-data is split as 8 bytes after the connection_id and
/// 12 bytes after the 10-byte reserved block.
fn parseGreetingSalt(payload: []const u8) ![20]u8 {
    // protocol_version(1) + server_version(NUL-terminated) + conn_id(4)
    var cursor: usize = 1;
    while (cursor < payload.len and payload[cursor] != 0) cursor += 1;
    if (cursor >= payload.len) return error.MalformedGreeting;
    cursor += 1; // skip NUL
    cursor += 4; // connection_id

    if (cursor + 8 > payload.len) return error.MalformedGreeting;
    var salt: [20]u8 = undefined;
    @memcpy(salt[0..8], payload[cursor .. cursor + 8]);
    cursor += 8;

    // filler(1) + cap_lower(2) + charset(1) + status(2) + cap_upper(2)
    //   + auth_plugin_data_len(1) + reserved(10) = 19 bytes
    cursor += 19;

    if (cursor + 12 > payload.len) return error.MalformedGreeting;
    @memcpy(salt[8..20], payload[cursor .. cursor + 12]);
    return salt;
}

/// Minimal in-Zig MySQL client. Speaks just enough to complete a
/// handshake and exchange one COM_QUERY round-trip.
const TestClient = struct {
    allocator: std.mem.Allocator,
    io: std.Io,
    stream: std.Io.net.Stream,
    read_buf: []u8,
    write_buf: []u8,
    reader: std.Io.net.Stream.Reader,
    writer: std.Io.net.Stream.Writer,
    /// Salt captured from the most recent greeting. Used by
    /// COM_CHANGE_USER tests to recompute the SHA1 hash.
    last_salt: [20]u8 = .{0} ** 20,

    fn connect(allocator: std.mem.Allocator, io: std.Io, addr: std.Io.net.IpAddress) !TestClient {
        const stream = try std.Io.net.IpAddress.connect(&addr, io, .{ .mode = .stream, .protocol = .tcp });
        const read_buf = try allocator.alloc(u8, 16 * 1024);
        errdefer allocator.free(read_buf);
        const write_buf = try allocator.alloc(u8, 16 * 1024);
        errdefer allocator.free(write_buf);
        return .{
            .allocator = allocator,
            .io = io,
            .stream = stream,
            .read_buf = read_buf,
            .write_buf = write_buf,
            .reader = stream.reader(io, read_buf),
            .writer = stream.writer(io, write_buf),
        };
    }

    fn close(self: *TestClient) void {
        self.stream.close(self.io);
        self.allocator.free(self.read_buf);
        self.allocator.free(self.write_buf);
    }

    fn doHandshake(self: *TestClient, initial_db: ?[]const u8) !void {
        try self.doHandshakeWithCaps(initial_db, true);
    }

    fn doHandshakeWithCaps(
        self: *TestClient,
        initial_db: ?[]const u8,
        deprecate_eof: bool,
    ) !void {
        try self.doHandshakeFull(initial_db, deprecate_eof, null);
    }

    /// Full HandshakeResponse41 with an optional password. When
    /// `password` is null we send an empty auth response (matches
    /// the legacy trust-mode tests). When set, we parse the 20-byte
    /// salt out of the greeting and reply with the proper
    /// mysql_native_password 20-byte hash.
    fn doHandshakeFull(
        self: *TestClient,
        initial_db: ?[]const u8,
        deprecate_eof: bool,
        password: ?[]const u8,
    ) !void {
        const greet = try mysql_packet.readPacket(self.allocator, &self.reader.interface);
        defer self.allocator.free(greet.payload);

        // Always capture the salt so COM_CHANGE_USER tests have it.
        self.last_salt = try parseGreetingSalt(greet.payload);

        var auth_bytes: [20]u8 = undefined;
        var send_hash = false;
        if (password) |pw| {
            auth_bytes = thindb.mysql.auth.nativeHash(pw, self.last_salt);
            send_hash = true;
        }

        var payload: std.ArrayList(u8) = .empty;
        defer payload.deinit(self.allocator);

        const caps = mysql_handshake.CLIENT_PROTOCOL_41 |
            mysql_handshake.CLIENT_SECURE_CONNECTION |
            mysql_handshake.CLIENT_PLUGIN_AUTH |
            (if (initial_db != null) mysql_handshake.CLIENT_CONNECT_WITH_DB else 0) |
            (if (deprecate_eof) mysql_handshake.CLIENT_DEPRECATE_EOF else 0);

        var buf4: [4]u8 = undefined;
        std.mem.writeInt(u32, &buf4, caps, .little);
        try payload.appendSlice(self.allocator, &buf4);

        std.mem.writeInt(u32, &buf4, 0x01000000, .little);
        try payload.appendSlice(self.allocator, &buf4);

        try payload.append(self.allocator, 0xff);
        try payload.appendSlice(self.allocator, &([_]u8{0} ** 23));

        try payload.appendSlice(self.allocator, "test\x00");

        if (send_hash) {
            try payload.append(self.allocator, 20);
            try payload.appendSlice(self.allocator, &auth_bytes);
        } else {
            try payload.append(self.allocator, 0);
        }

        if (initial_db) |db| {
            try payload.appendSlice(self.allocator, db);
            try payload.append(self.allocator, 0);
        }

        try payload.appendSlice(self.allocator, "mysql_native_password\x00");

        try mysql_packet.writePacket(&self.writer.interface, 1, payload.items);
        try self.writer.interface.flush();

        const auth_resp = try mysql_packet.readPacket(self.allocator, &self.reader.interface);
        defer self.allocator.free(auth_resp.payload);
        if (auth_resp.payload.len == 0 or auth_resp.payload[0] != 0x00) {
            return error.AuthRejected;
        }
    }

    fn sendQuery(self: *TestClient, sql_text: []const u8) !void {
        try self.sendCommand(0x03, sql_text);
    }

    fn sendCommand(self: *TestClient, command: u8, body: []const u8) !void {
        var payload: std.ArrayList(u8) = .empty;
        defer payload.deinit(self.allocator);
        try payload.append(self.allocator, command);
        try payload.appendSlice(self.allocator, body);
        try mysql_packet.writePacket(&self.writer.interface, 0, payload.items);
        try self.writer.interface.flush();
    }

    fn sendQuit(self: *TestClient) !void {
        const buf = [_]u8{0x01};
        try mysql_packet.writePacket(&self.writer.interface, 0, &buf);
        try self.writer.interface.flush();
    }

    fn sendStmtPrepare(self: *TestClient, sql_text: []const u8) !void {
        var payload: std.ArrayList(u8) = .empty;
        defer payload.deinit(self.allocator);
        try payload.append(self.allocator, 0x16);
        try payload.appendSlice(self.allocator, sql_text);
        try mysql_packet.writePacket(&self.writer.interface, 0, payload.items);
        try self.writer.interface.flush();
    }

    const PrepareReply = struct {
        stmt_id: u32,
        num_columns: u16,
        num_params: u16,
    };

    /// Drain a successful COM_STMT_PREPARE response (header + param +
    /// column ColumnDef41 packets + any EOFs). Returns the parsed
    /// header. If the server replied ERR_Packet, returns
    /// error.PrepareRejected.
    fn readPrepareReply(self: *TestClient, deprecate_eof: bool) !PrepareReply {
        const hdr_pkt = try mysql_packet.readPacket(self.allocator, &self.reader.interface);
        defer self.allocator.free(hdr_pkt.payload);
        if (hdr_pkt.payload.len == 0) return error.MalformedPrepareReply;
        if (hdr_pkt.payload[0] == 0xFF) return error.PrepareRejected;
        if (hdr_pkt.payload[0] != 0x00) return error.MalformedPrepareReply;
        if (hdr_pkt.payload.len < 12) return error.MalformedPrepareReply;
        const stmt_id = std.mem.readInt(u32, hdr_pkt.payload[1..5], .little);
        const num_columns = std.mem.readInt(u16, hdr_pkt.payload[5..7], .little);
        const num_params = std.mem.readInt(u16, hdr_pkt.payload[7..9], .little);

        // Drain `num_params` param-column-def packets (+ EOF if not deprecated)
        var i: u16 = 0;
        while (i < num_params) : (i += 1) {
            const p = try mysql_packet.readPacket(self.allocator, &self.reader.interface);
            self.allocator.free(p.payload);
        }
        if (num_params > 0 and !deprecate_eof) {
            const eof = try mysql_packet.readPacket(self.allocator, &self.reader.interface);
            self.allocator.free(eof.payload);
        }
        // Drain `num_columns` column-def packets (+ EOF if not deprecated)
        var j: u16 = 0;
        while (j < num_columns) : (j += 1) {
            const p = try mysql_packet.readPacket(self.allocator, &self.reader.interface);
            self.allocator.free(p.payload);
        }
        if (num_columns > 0 and !deprecate_eof) {
            const eof = try mysql_packet.readPacket(self.allocator, &self.reader.interface);
            self.allocator.free(eof.payload);
        }
        return .{ .stmt_id = stmt_id, .num_columns = num_columns, .num_params = num_params };
    }

    /// One bound parameter value for sendStmtExecute. `type_byte` is a
    /// MYSQL_TYPE_*; `value_bytes` is already encoded in the on-wire
    /// binary format expected by the server (lenenc string, fixed int
    /// LE, etc). Use null to bind SQL NULL.
    const Param = struct {
        type_byte: u8,
        unsigned: bool = false,
        value_bytes: ?[]const u8,
    };

    fn sendStmtExecute(self: *TestClient, stmt_id: u32, params: []const Param) !void {
        var payload: std.ArrayList(u8) = .empty;
        defer payload.deinit(self.allocator);
        try payload.append(self.allocator, 0x17);

        var hdr: [4]u8 = undefined;
        std.mem.writeInt(u32, &hdr, stmt_id, .little);
        try payload.appendSlice(self.allocator, &hdr);
        try payload.append(self.allocator, 0); // flags = CURSOR_TYPE_NO_CURSOR
        std.mem.writeInt(u32, &hdr, 1, .little);
        try payload.appendSlice(self.allocator, &hdr); // iteration_count

        if (params.len > 0) {
            const nullmap_bytes = (params.len + 7) / 8;
            const nullmap_start = payload.items.len;
            var i: usize = 0;
            while (i < nullmap_bytes) : (i += 1) try payload.append(self.allocator, 0);
            for (params, 0..) |p, idx| {
                if (p.value_bytes == null) {
                    payload.items[nullmap_start + idx / 8] |= @as(u8, 1) << @as(u3, @intCast(idx % 8));
                }
            }
            try payload.append(self.allocator, 1); // new_params_bound_flag
            for (params) |p| {
                try payload.append(self.allocator, p.type_byte);
                try payload.append(self.allocator, if (p.unsigned) 0x80 else 0);
            }
            for (params) |p| {
                if (p.value_bytes) |vb| try payload.appendSlice(self.allocator, vb);
            }
        }
        try mysql_packet.writePacket(&self.writer.interface, 0, payload.items);
        try self.writer.interface.flush();
    }

    /// Send COM_STMT_EXECUTE with `new_params_bound_flag = 0` (reuse the
    /// previously-bound types). Useful for "reuse types" tests.
    fn sendStmtExecuteReuse(self: *TestClient, stmt_id: u32, num_params: u16, value_bytes: []const u8) !void {
        var payload: std.ArrayList(u8) = .empty;
        defer payload.deinit(self.allocator);
        try payload.append(self.allocator, 0x17);

        var hdr: [4]u8 = undefined;
        std.mem.writeInt(u32, &hdr, stmt_id, .little);
        try payload.appendSlice(self.allocator, &hdr);
        try payload.append(self.allocator, 0);
        std.mem.writeInt(u32, &hdr, 1, .little);
        try payload.appendSlice(self.allocator, &hdr);

        if (num_params > 0) {
            const nullmap_bytes = (@as(usize, num_params) + 7) / 8;
            var i: usize = 0;
            while (i < nullmap_bytes) : (i += 1) try payload.append(self.allocator, 0);
            try payload.append(self.allocator, 0); // new_params_bound_flag = 0
            try payload.appendSlice(self.allocator, value_bytes);
        }
        try mysql_packet.writePacket(&self.writer.interface, 0, payload.items);
        try self.writer.interface.flush();
    }

    fn sendStmtClose(self: *TestClient, stmt_id: u32) !void {
        var payload: [5]u8 = undefined;
        payload[0] = 0x19;
        std.mem.writeInt(u32, payload[1..5], stmt_id, .little);
        try mysql_packet.writePacket(&self.writer.interface, 0, &payload);
        try self.writer.interface.flush();
    }

    fn sendStmtReset(self: *TestClient, stmt_id: u32) !void {
        var payload: [5]u8 = undefined;
        payload[0] = 0x1A;
        std.mem.writeInt(u32, payload[1..5], stmt_id, .little);
        try mysql_packet.writePacket(&self.writer.interface, 0, &payload);
        try self.writer.interface.flush();
    }

    fn sendStmtSendLongData(self: *TestClient, stmt_id: u32, param_idx: u16, data: []const u8) !void {
        var payload: std.ArrayList(u8) = .empty;
        defer payload.deinit(self.allocator);
        try payload.append(self.allocator, 0x18);
        var hdr4: [4]u8 = undefined;
        std.mem.writeInt(u32, &hdr4, stmt_id, .little);
        try payload.appendSlice(self.allocator, &hdr4);
        var hdr2: [2]u8 = undefined;
        std.mem.writeInt(u16, &hdr2, param_idx, .little);
        try payload.appendSlice(self.allocator, &hdr2);
        try payload.appendSlice(self.allocator, data);
        try mysql_packet.writePacket(&self.writer.interface, 0, payload.items);
        try self.writer.interface.flush();
    }

    /// Drain a binary-protocol result set. Returns the raw binary
    /// row-payload slices (excluding the 0x00 header byte) — caller
    /// decodes per known schema. NULL columns are recorded via the
    /// per-row null-bitmap byte buffer (callers extract them per the
    /// MySQL binary protocol's "bit i+2" rule).
    const BinaryRow = struct {
        nullmap: []u8,
        cells: []const u8,
    };

    fn readBinaryResultSet(self: *TestClient, arena: std.mem.Allocator, deprecate_eof: bool) ![]const BinaryRow {
        return (try self.readBinaryResult(arena, deprecate_eof)).rows;
    }

    /// A binary-protocol result set: each column definition's MYSQL_TYPE_*
    /// byte, and the rows as `readBinaryResultSet` returns them.
    const BinaryResult = struct {
        column_types: []const u8,
        rows: []const BinaryRow,
    };

    fn readBinaryResult(self: *TestClient, arena: std.mem.Allocator, deprecate_eof: bool) !BinaryResult {
        const col_count_pkt = try mysql_packet.readPacket(self.allocator, &self.reader.interface);
        defer self.allocator.free(col_count_pkt.payload);
        if (col_count_pkt.payload.len == 0) return error.MalformedResultSet;
        if (col_count_pkt.payload[0] == 0xFF) return error.QueryRejected;
        if (col_count_pkt.payload[0] == 0x00) return error.UnexpectedOk;

        var cursor: usize = 0;
        const col_count = try mysql_packet.readLenEncInt(col_count_pkt.payload, &cursor);

        const column_types = try arena.alloc(u8, @intCast(col_count));
        for (column_types) |*column_type| {
            const p = try mysql_packet.readPacket(self.allocator, &self.reader.interface);
            defer self.allocator.free(p.payload);
            // catalog, schema, table, org_table, name, org_name, then the
            // fixed fields' length, charset (2) and column length (4).
            var c: usize = 0;
            for (0..6) |_| _ = try mysql_packet.readLenEncString(p.payload, &c);
            c += 1 + 2 + 4;
            if (c >= p.payload.len) return error.MalformedResultSet;
            column_type.* = p.payload[c];
        }
        if (!deprecate_eof) {
            const eof = try mysql_packet.readPacket(self.allocator, &self.reader.interface);
            self.allocator.free(eof.payload);
        }

        var rows: std.ArrayList(BinaryRow) = .empty;
        while (true) {
            const row_pkt = try mysql_packet.readPacket(self.allocator, &self.reader.interface);
            defer self.allocator.free(row_pkt.payload);
            if (row_pkt.payload.len == 0) return error.MalformedResultSet;
            if (row_pkt.payload[0] == 0xFE and row_pkt.payload.len < 0xFFFFFF) break;
            if (row_pkt.payload[0] == 0xFF) return error.QueryRejected;
            if (row_pkt.payload[0] != 0x00) return error.MalformedResultSet;

            const nullmap_bytes = (@as(usize, @intCast(col_count)) + 7 + 2) / 8;
            if (row_pkt.payload.len < 1 + nullmap_bytes) return error.MalformedResultSet;
            const nullmap_owned = try arena.dupe(u8, row_pkt.payload[1 .. 1 + nullmap_bytes]);
            const cells_owned = try arena.dupe(u8, row_pkt.payload[1 + nullmap_bytes ..]);
            try rows.append(arena, .{ .nullmap = nullmap_owned, .cells = cells_owned });
        }
        return .{ .column_types = column_types, .rows = try rows.toOwnedSlice(arena) };
    }

    /// Run the caching_sha2_password client side of the handshake.
    /// Sends a 32-byte response computed from the greeting's salt and
    /// expects an AuthMoreData(fast_auth_success) packet followed by
    /// OK. Returns error.AuthRejected if the server replies with ERR.
    fn doHandshakeCachingSha2(self: *TestClient, password: []const u8) !void {
        const greet = try mysql_packet.readPacket(self.allocator, &self.reader.interface);
        defer self.allocator.free(greet.payload);
        self.last_salt = try parseGreetingSalt(greet.payload);

        const hash = thindb.mysql.auth.cachingSha2ClientHash(password, self.last_salt);

        var payload: std.ArrayList(u8) = .empty;
        defer payload.deinit(self.allocator);

        const caps = mysql_handshake.CLIENT_PROTOCOL_41 |
            mysql_handshake.CLIENT_SECURE_CONNECTION |
            mysql_handshake.CLIENT_PLUGIN_AUTH |
            mysql_handshake.CLIENT_DEPRECATE_EOF;

        var buf4: [4]u8 = undefined;
        std.mem.writeInt(u32, &buf4, caps, .little);
        try payload.appendSlice(self.allocator, &buf4);
        std.mem.writeInt(u32, &buf4, 0x01000000, .little);
        try payload.appendSlice(self.allocator, &buf4);
        try payload.append(self.allocator, 0xff);
        try payload.appendSlice(self.allocator, &([_]u8{0} ** 23));
        try payload.appendSlice(self.allocator, "test\x00");
        try payload.append(self.allocator, 32);
        try payload.appendSlice(self.allocator, &hash);
        try payload.appendSlice(self.allocator, "caching_sha2_password\x00");

        try mysql_packet.writePacket(&self.writer.interface, 1, payload.items);
        try self.writer.interface.flush();

        // Server: AuthMoreData(0x01, 0x03) — fast_auth_success.
        const more = try mysql_packet.readPacket(self.allocator, &self.reader.interface);
        defer self.allocator.free(more.payload);
        if (more.payload.len == 0 or more.payload[0] == 0xFF) return error.AuthRejected;
        if (more.payload[0] != 0x01 or more.payload.len < 2 or more.payload[1] != 0x03)
            return error.UnexpectedAuthMoreData;

        // Server: OK packet.
        const ok = try mysql_packet.readPacket(self.allocator, &self.reader.interface);
        defer self.allocator.free(ok.payload);
        if (ok.payload.len == 0 or ok.payload[0] != 0x00) return error.AuthRejected;
    }

    fn sendResetConnection(self: *TestClient) !void {
        const buf = [_]u8{0x1F};
        try mysql_packet.writePacket(&self.writer.interface, 0, &buf);
        try self.writer.interface.flush();
    }

    /// Send COM_CHANGE_USER (0x11). `auth_response` must be the
    /// 20-byte SHA1 challenge response using whatever salt was sent
    /// in the original HandshakeV10 greeting (we store the salt on
    /// the client after `doHandshakeFull` if the caller wants to
    /// re-use it via `last_salt`).
    fn sendChangeUser(
        self: *TestClient,
        user: []const u8,
        auth_response: []const u8,
        schema: []const u8,
    ) !void {
        var payload: std.ArrayList(u8) = .empty;
        defer payload.deinit(self.allocator);
        try payload.append(self.allocator, 0x11);
        try payload.appendSlice(self.allocator, user);
        try payload.append(self.allocator, 0);
        try payload.append(self.allocator, @intCast(auth_response.len));
        try payload.appendSlice(self.allocator, auth_response);
        try payload.appendSlice(self.allocator, schema);
        try payload.append(self.allocator, 0);
        // character_set (2 bytes, utf8mb4)
        try payload.append(self.allocator, 0xff);
        try payload.append(self.allocator, 0x00);
        try payload.appendSlice(self.allocator, "mysql_native_password\x00");
        try mysql_packet.writePacket(&self.writer.interface, 0, payload.items);
        try self.writer.interface.flush();
    }

    /// Drain a result set after a COM_QUERY. Returns a flattened slice
    /// of rows, each row a slice of optional column-text slices owned
    /// by `dest_arena`.
    fn readResultSet(self: *TestClient, dest_arena: std.mem.Allocator) ![]const []const ?[]const u8 {
        const col_count_pkt = try mysql_packet.readPacket(self.allocator, &self.reader.interface);
        defer self.allocator.free(col_count_pkt.payload);
        return self.readResultRows(dest_arena, col_count_pkt.payload);
    }

    /// The rows of a result set whose column-count packet was already read.
    fn readResultRows(self: *TestClient, dest_arena: std.mem.Allocator, col_count_payload: []const u8) ![]const []const ?[]const u8 {
        var cursor: usize = 0;
        const col_count = try mysql_packet.readLenEncInt(col_count_payload, &cursor);

        var i: u64 = 0;
        while (i < col_count) : (i += 1) {
            const cd = try mysql_packet.readPacket(self.allocator, &self.reader.interface);
            self.allocator.free(cd.payload);
        }

        var rows: std.ArrayList([]const ?[]const u8) = .empty;
        while (true) {
            const row_pkt = try mysql_packet.readPacket(self.allocator, &self.reader.interface);
            defer self.allocator.free(row_pkt.payload);

            if (row_pkt.payload.len > 0 and (row_pkt.payload[0] == 0xFE or row_pkt.payload[0] == 0xFF)) break;

            var cells: std.ArrayList(?[]const u8) = .empty;
            var rc: usize = 0;
            var j: u64 = 0;
            while (j < col_count) : (j += 1) {
                if (rc >= row_pkt.payload.len) return error.TruncatedRow;
                if (row_pkt.payload[rc] == 0xFB) {
                    rc += 1;
                    try cells.append(dest_arena, null);
                } else {
                    const s = try mysql_packet.readLenEncString(row_pkt.payload, &rc);
                    const copy = try dest_arena.dupe(u8, s);
                    try cells.append(dest_arena, copy);
                }
            }
            const cells_slice = try cells.toOwnedSlice(dest_arena);
            try rows.append(dest_arena, cells_slice);
        }
        return try rows.toOwnedSlice(dest_arena);
    }
};

const ServerCtx = struct {
    server: *thindb.MysqlServer,
    n: usize,
    err: ?anyerror = null,
    fn run(self: *@This()) void {
        var i: usize = 0;
        while (i < self.n) : (i += 1) {
            self.server.acceptOne() catch |e| {
                self.err = e;
                return;
            };
        }
    }
};

fn openCatalog(allocator: std.mem.Allocator, io: std.Io, dir: std.Io.Dir) !*thindb.Catalog {
    const c = try thindb.Catalog.open(allocator, io, dir, .{});
    _ = try c.createDatabase("main");
    return c;
}

test "mysql wire: xa commit reports failure without partial effects and retries once" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const catalog = try openCatalog(a, io, tmp.dir);
    defer catalog.close();
    const addr = std.Io.net.IpAddress{ .ip4 = .{ .bytes = .{ 127, 0, 0, 1 }, .port = test_port_base + 2500 } };
    const server = try thindb.serveMysql(a, io, catalog, addr, null);
    defer server.destroy();
    defer server.close();
    var sctx: ServerCtx = .{ .server = server, .n = 1 };
    const thread = try std.Thread.spawn(.{}, ServerCtx.run, .{&sctx});
    defer thread.join();
    var client = try TestClient.connect(a, io, addr);
    defer client.close();
    try client.doHandshake("main");
    for ([_][]const u8{
        "CREATE TABLE t (id BIGINT NOT NULL) ORDER BY (id)",
        "INSERT INTO t VALUES (1)",
        "XA START 'failed'",
        "INSERT INTO t VALUES (2)",
        "INSERT INTO t(missing) VALUES (3)",
        "XA END 'failed'",
        "XA PREPARE 'failed'",
    }) |sql_text| {
        try client.sendQuery(sql_text);
        const packet = try mysql_packet.readPacket(a, &client.reader.interface);
        defer a.free(packet.payload);
        try std.testing.expectEqual(@as(u8, 0), packet.payload[0]);
    }
    try client.sendQuery("XA COMMIT 'failed'");
    {
        const packet = try mysql_packet.readPacket(a, &client.reader.interface);
        defer a.free(packet.payload);
        try std.testing.expectEqual(@as(u8, 0xff), packet.payload[0]);
        try std.testing.expectEqual(@as(u16, 1401), std.mem.readInt(u16, packet.payload[1..3], .little));
    }
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    try client.sendQuery("SELECT id FROM t ORDER BY id");
    const before = try client.readResultSet(arena.allocator());
    try std.testing.expectEqual(@as(usize, 1), before.len);
    try std.testing.expectEqualStrings("1", before[0][0].?);
    try client.sendQuery("XA RECOVER");
    try std.testing.expectEqual(@as(usize, 1), (try client.readResultSet(arena.allocator())).len);
    for ([_][]const u8{
        "XA ROLLBACK 'failed'",
        "XA START 'success'",
        "INSERT INTO t VALUES (3)",
        "XA END 'success'",
        "XA PREPARE 'success'",
        "XA COMMIT 'success'",
        "XA COMMIT 'success'",
    }) |sql_text| {
        try client.sendQuery(sql_text);
        const packet = try mysql_packet.readPacket(a, &client.reader.interface);
        defer a.free(packet.payload);
        try std.testing.expectEqual(@as(u8, 0), packet.payload[0]);
    }
    try client.sendQuery("SELECT id FROM t ORDER BY id");
    const after = try client.readResultSet(arena.allocator());
    try std.testing.expectEqual(@as(usize, 2), after.len);
    try std.testing.expectEqualStrings("3", after[1][0].?);
}

test "mysql wire: standalone client handshake + SELECT 1" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var catalog = try openCatalog(allocator, io, tmp.dir);
    defer catalog.close();

    const port: u16 = test_port_base + 0;
    const addr = std.Io.net.IpAddress{ .ip4 = .{ .bytes = .{ 127, 0, 0, 1 }, .port = port } };
    var server = try thindb.serveMysql(allocator, io, catalog, addr, null);
    defer server.destroy();
    defer server.close();

    var sctx: ServerCtx = .{ .server = server, .n = 1 };
    const t = try std.Thread.spawn(.{}, ServerCtx.run, .{&sctx});
    defer t.join();

    var client = try TestClient.connect(allocator, io, addr);
    defer client.close();

    try client.doHandshake(null);

    try client.sendQuery("SELECT @@version");

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const rows = try client.readResultSet(arena.allocator());
    try std.testing.expectEqual(@as(usize, 1), rows.len);
    try std.testing.expectEqual(@as(usize, 1), rows[0].len);
    try std.testing.expect(rows[0][0] != null);
    try std.testing.expectEqualStrings("8.0.32-thinDB", rows[0][0].?);

    try client.sendQuit();
    if (sctx.err) |e| return e;
}

test "mysql wire: SHOW DATABASES returns flattened db__schema" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var catalog = try openCatalog(allocator, io, tmp.dir);
    defer catalog.close();

    const port: u16 = test_port_base + 1;
    const addr = std.Io.net.IpAddress{ .ip4 = .{ .bytes = .{ 127, 0, 0, 1 }, .port = port } };
    var server = try thindb.serveMysql(allocator, io, catalog, addr, null);
    defer server.destroy();
    defer server.close();

    var sctx: ServerCtx = .{ .server = server, .n = 1 };
    const t = try std.Thread.spawn(.{}, ServerCtx.run, .{&sctx});
    defer t.join();

    var client = try TestClient.connect(allocator, io, addr);
    defer client.close();
    try client.doHandshake(null);

    try client.sendQuery("SHOW DATABASES");

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const rows = try client.readResultSet(arena.allocator());

    var seen_main_public = false;
    for (rows) |row| {
        if (row.len > 0 and row[0] != null and std.mem.eql(u8, row[0].?, "main__public")) {
            seen_main_public = true;
        }
    }
    try std.testing.expect(seen_main_public);

    try client.sendQuit();
    if (sctx.err) |e| return e;
}

test "mysql wire: COM_QUERY against seeded table returns rows" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var catalog = try openCatalog(allocator, io, tmp.dir);
    defer catalog.close();

    const db = catalog.database("main").?;
    const sc = db.schema("public").?;
    const t = try sc.table("orders", schema_orders, opts_orders);
    try t.insert(&.{
        .{ .id = @as(i64, 1), .qty = @as(i32, 10), .tag = "a" },
        .{ .id = @as(i64, 2), .qty = @as(i32, 20), .tag = "b" },
        .{ .id = @as(i64, 3), .qty = @as(i32, 30), .tag = "c" },
    });
    try t.flush();

    const port: u16 = test_port_base + 2;
    const addr = std.Io.net.IpAddress{ .ip4 = .{ .bytes = .{ 127, 0, 0, 1 }, .port = port } };
    var server = try thindb.serveMysql(allocator, io, catalog, addr, null);
    defer server.destroy();
    defer server.close();

    var sctx: ServerCtx = .{ .server = server, .n = 1 };
    const th = try std.Thread.spawn(.{}, ServerCtx.run, .{&sctx});
    defer th.join();

    var client = try TestClient.connect(allocator, io, addr);
    defer client.close();
    try client.doHandshake(null);

    try client.sendQuery("SELECT * FROM orders");

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const rows = try client.readResultSet(arena.allocator());
    try std.testing.expectEqual(@as(usize, 3), rows.len);
    try std.testing.expectEqualStrings("1", rows[0][0].?);
    try std.testing.expectEqualStrings("10", rows[0][1].?);
    try std.testing.expectEqualStrings("a", rows[0][2].?);
    try std.testing.expectEqualStrings("3", rows[2][0].?);
    try std.testing.expectEqualStrings("c", rows[2][2].?);

    try client.sendQuery("SELECT o.*, NULL AS note, qty + 1 AS next_qty FROM orders AS o ORDER BY id ASC");
    const rows2 = try client.readResultSet(arena.allocator());
    try std.testing.expectEqual(@as(usize, 3), rows2.len);
    try std.testing.expectEqual(@as(usize, 5), rows2[0].len);
    try std.testing.expectEqualStrings("1", rows2[0][0].?);
    try std.testing.expectEqualStrings("10", rows2[0][1].?);
    try std.testing.expect(rows2[0][3] == null);
    try std.testing.expectEqualStrings("11", rows2[0][4].?);

    try client.sendQuit();
    if (sctx.err) |e| return e;
}

test "mysql wire: Workbench metadata probes reflect catalog tables" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var catalog = try openCatalog(allocator, io, tmp.dir);
    defer catalog.close();

    const db = catalog.database("main").?;
    const sc = db.schema("public").?;
    const t = try sc.table("orders", schema_orders, opts_orders);
    try t.insert(&.{
        .{ .id = @as(i64, 1), .qty = @as(i32, 10), .tag = "a" },
        .{ .id = @as(i64, 2), .qty = @as(i32, 20), .tag = "b" },
    });

    const port: u16 = test_port_base + 210;
    const addr = std.Io.net.IpAddress{ .ip4 = .{ .bytes = .{ 127, 0, 0, 1 }, .port = port } };
    var server = try thindb.serveMysql(allocator, io, catalog, addr, null);
    defer server.destroy();
    defer server.close();

    var sctx: ServerCtx = .{ .server = server, .n = 1 };
    const th = try std.Thread.spawn(.{}, ServerCtx.run, .{&sctx});
    defer th.join();

    var client = try TestClient.connect(allocator, io, addr);
    defer client.close();
    try client.doHandshake("main__public");

    try client.sendQuery("SHOW FULL TABLES FROM `main__public`");
    {
        var arena = std.heap.ArenaAllocator.init(allocator);
        defer arena.deinit();
        const rows = try client.readResultSet(arena.allocator());
        try std.testing.expectEqual(@as(usize, 1), rows.len);
        try std.testing.expectEqualStrings("orders", rows[0][0].?);
        try std.testing.expectEqualStrings("BASE TABLE", rows[0][1].?);
    }

    try client.sendQuery("SHOW FULL COLUMNS FROM `orders`");
    {
        var arena = std.heap.ArenaAllocator.init(allocator);
        defer arena.deinit();
        const rows = try client.readResultSet(arena.allocator());
        try std.testing.expectEqual(@as(usize, 3), rows.len);
        try std.testing.expectEqualStrings("id", rows[0][0].?);
        try std.testing.expectEqualStrings("bigint", rows[0][1].?);
        try std.testing.expectEqualStrings("PRI", rows[0][4].?);
        try std.testing.expectEqualStrings("tag", rows[2][0].?);
        try std.testing.expectEqualStrings("text", rows[2][1].?);
    }

    try client.sendQuery("SELECT TABLE_NAME, TABLE_TYPE FROM information_schema.TABLES WHERE TABLE_SCHEMA = 'main__public'");
    {
        var arena = std.heap.ArenaAllocator.init(allocator);
        defer arena.deinit();
        const rows = try client.readResultSet(arena.allocator());
        try std.testing.expectEqual(@as(usize, 1), rows.len);
        try std.testing.expectEqualStrings("orders", rows[0][0].?);
        try std.testing.expectEqualStrings("BASE TABLE", rows[0][1].?);
    }

    try client.sendQuery("SELECT COLUMN_NAME, DATA_TYPE, COLUMN_KEY FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = 'main__public' AND TABLE_NAME = 'orders'");
    {
        var arena = std.heap.ArenaAllocator.init(allocator);
        defer arena.deinit();
        const rows = try client.readResultSet(arena.allocator());
        try std.testing.expectEqual(@as(usize, 3), rows.len);
        try std.testing.expectEqualStrings("id", rows[0][0].?);
        try std.testing.expectEqualStrings("bigint", rows[0][1].?);
        try std.testing.expectEqualStrings("PRI", rows[0][2].?);
    }

    try client.sendQuit();
    if (sctx.err) |e| return e;
}

test "mysql wire: SHOW COLUMNS reports DEFAULT and auto_increment Extra" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var catalog = try openCatalog(allocator, io, tmp.dir);
    defer catalog.close();

    const schema_items = thindb.TableSchema{
        .columns = &.{
            .{ .name = "id", .type = .bigint, .auto_increment = true },
            .{ .name = "qty", .type = .int, .default_value = .{ .int = 7 } },
        },
        .order_key = &.{"id"},
        .unique = true,
    };
    const ok_items = [_][]const u8{"id"};
    const opts_items = thindb.TableOptions{ .order_key = &ok_items, .unique = true };

    const db = catalog.database("main").?;
    const sc = db.schema("public").?;
    _ = try sc.table("items", schema_items, opts_items);

    const port: u16 = test_port_base + 211;
    const addr = std.Io.net.IpAddress{ .ip4 = .{ .bytes = .{ 127, 0, 0, 1 }, .port = port } };
    var server = try thindb.serveMysql(allocator, io, catalog, addr, null);
    defer server.destroy();
    defer server.close();

    var sctx: ServerCtx = .{ .server = server, .n = 1 };
    const th = try std.Thread.spawn(.{}, ServerCtx.run, .{&sctx});
    defer th.join();

    var client = try TestClient.connect(allocator, io, addr);
    defer client.close();
    try client.doHandshake("main__public");

    try client.sendQuery("SHOW COLUMNS FROM `items`");
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const rows = try client.readResultSet(arena.allocator());
    // simple SHOW COLUMNS layout: Field, Type, Null, Key, Default, Extra
    try std.testing.expectEqual(@as(usize, 2), rows.len);
    try std.testing.expectEqualStrings("id", rows[0][0].?);
    try std.testing.expectEqualStrings("auto_increment", rows[0][5].?);
    try std.testing.expectEqualStrings("qty", rows[1][0].?);
    try std.testing.expectEqualStrings("7", rows[1][4].?);

    try client.sendQuit();
    if (sctx.err) |e| return e;
}

test "mysql wire: SELECT NOW() returns real wall-clock, not 1970" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var catalog = try openCatalog(allocator, io, tmp.dir);
    defer catalog.close();

    const port: u16 = test_port_base + 212;
    const addr = std.Io.net.IpAddress{ .ip4 = .{ .bytes = .{ 127, 0, 0, 1 }, .port = port } };
    var server = try thindb.serveMysql(allocator, io, catalog, addr, null);
    defer server.destroy();
    defer server.close();

    var sctx: ServerCtx = .{ .server = server, .n = 1 };
    const th = try std.Thread.spawn(.{}, ServerCtx.run, .{&sctx});
    defer th.join();

    var client = try TestClient.connect(allocator, io, addr);
    defer client.close();
    try client.doHandshake("main__public");

    try client.sendQuery("SELECT NOW()");
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const rows = try client.readResultSet(arena.allocator());
    try std.testing.expectEqual(@as(usize, 1), rows.len);
    const v = rows[0][0].?;
    try std.testing.expect(v.len > 0);
    try std.testing.expect(!std.mem.eql(u8, v, "1970-01-01 00:00:00"));

    try client.sendQuit();
    if (sctx.err) |e| return e;
}

test "mysql wire: SET / SHOW VARIABLES probes return canned OK / rows" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var catalog = try openCatalog(allocator, io, tmp.dir);
    defer catalog.close();

    const port: u16 = test_port_base + 3;
    const addr = std.Io.net.IpAddress{ .ip4 = .{ .bytes = .{ 127, 0, 0, 1 }, .port = port } };
    var server = try thindb.serveMysql(allocator, io, catalog, addr, null);
    defer server.destroy();
    defer server.close();

    var sctx: ServerCtx = .{ .server = server, .n = 1 };
    const t = try std.Thread.spawn(.{}, ServerCtx.run, .{&sctx});
    defer t.join();

    var client = try TestClient.connect(allocator, io, addr);
    defer client.close();
    try client.doHandshake(null);

    try client.sendQuery("SET NAMES utf8mb4");
    {
        const pkt = try mysql_packet.readPacket(allocator, &client.reader.interface);
        defer allocator.free(pkt.payload);
        try std.testing.expect(pkt.payload.len > 0);
        try std.testing.expectEqual(@as(u8, 0x00), pkt.payload[0]);
    }

    try client.sendQuery("SHOW VARIABLES LIKE 'sql_mode'");
    {
        var arena = std.heap.ArenaAllocator.init(allocator);
        defer arena.deinit();
        const rows = try client.readResultSet(arena.allocator());
        try std.testing.expectEqual(@as(usize, 1), rows.len);
        try std.testing.expectEqualStrings("sql_mode", rows[0][0].?);
        try std.testing.expectEqualStrings("STRICT_TRANS_TABLES", rows[0][1].?);
    }

    try client.sendQuit();
    if (sctx.err) |e| return e;
}

test "mysql wire: CREATE DATABASE then SHOW DATABASES includes new db" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var catalog = try openCatalog(allocator, io, tmp.dir);
    defer catalog.close();

    const port: u16 = test_port_base + 5;
    const addr = std.Io.net.IpAddress{ .ip4 = .{ .bytes = .{ 127, 0, 0, 1 }, .port = port } };
    var server = try thindb.serveMysql(allocator, io, catalog, addr, null);
    defer server.destroy();
    defer server.close();

    var sctx: ServerCtx = .{ .server = server, .n = 1 };
    const t = try std.Thread.spawn(.{}, ServerCtx.run, .{&sctx});
    defer t.join();

    var client = try TestClient.connect(allocator, io, addr);
    defer client.close();
    try client.doHandshake(null);

    try client.sendQuery("CREATE DATABASE reports");
    {
        const pkt = try mysql_packet.readPacket(allocator, &client.reader.interface);
        defer allocator.free(pkt.payload);
        try std.testing.expect(pkt.payload.len > 0);
        try std.testing.expectEqual(@as(u8, 0x00), pkt.payload[0]);
    }

    try client.sendQuery("SHOW DATABASES");
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const rows = try client.readResultSet(arena.allocator());

    var saw_reports = false;
    for (rows) |row| {
        if (row.len > 0 and row[0] != null and std.mem.eql(u8, row[0].?, "reports__public")) {
            saw_reports = true;
        }
    }
    try std.testing.expect(saw_reports);

    try client.sendQuit();
    if (sctx.err) |e| return e;
}

test "mysql wire: COM_INIT_DB on bogus name returns ER_BAD_DB" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var catalog = try openCatalog(allocator, io, tmp.dir);
    defer catalog.close();

    const port: u16 = test_port_base + 4;
    const addr = std.Io.net.IpAddress{ .ip4 = .{ .bytes = .{ 127, 0, 0, 1 }, .port = port } };
    var server = try thindb.serveMysql(allocator, io, catalog, addr, null);
    defer server.destroy();
    defer server.close();

    var sctx: ServerCtx = .{ .server = server, .n = 1 };
    const t = try std.Thread.spawn(.{}, ServerCtx.run, .{&sctx});
    defer t.join();

    var client = try TestClient.connect(allocator, io, addr);
    defer client.close();
    try client.doHandshake(null);

    var payload: std.ArrayList(u8) = .empty;
    defer payload.deinit(allocator);
    try payload.append(allocator, 0x02);
    try payload.appendSlice(allocator, "does_not_exist");
    try mysql_packet.writePacket(&client.writer.interface, 0, payload.items);
    try client.writer.interface.flush();

    const pkt = try mysql_packet.readPacket(allocator, &client.reader.interface);
    defer allocator.free(pkt.payload);
    try std.testing.expect(pkt.payload.len > 3);
    try std.testing.expectEqual(@as(u8, 0xFF), pkt.payload[0]);
    const code = std.mem.readInt(u16, pkt.payload[1..3], .little);
    try std.testing.expectEqual(@as(u16, 1049), code);

    try client.sendQuit();
    if (sctx.err) |e| return e;
}

test "mysql wire: legacy client (no DEPRECATE_EOF) gets two EOF packets" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var catalog = try openCatalog(allocator, io, tmp.dir);
    defer catalog.close();

    const port: u16 = test_port_base + 6;
    const addr = std.Io.net.IpAddress{ .ip4 = .{ .bytes = .{ 127, 0, 0, 1 }, .port = port } };
    var server = try thindb.serveMysql(allocator, io, catalog, addr, null);
    defer server.destroy();
    defer server.close();

    var sctx: ServerCtx = .{ .server = server, .n = 1 };
    const t = try std.Thread.spawn(.{}, ServerCtx.run, .{&sctx});
    defer t.join();

    var client = try TestClient.connect(allocator, io, addr);
    defer client.close();
    try client.doHandshakeWithCaps(null, false);

    try client.sendQuery("SELECT @@version");

    const col_count_pkt = try mysql_packet.readPacket(allocator, &client.reader.interface);
    defer allocator.free(col_count_pkt.payload);
    try std.testing.expectEqual(@as(usize, 1), col_count_pkt.payload.len);

    const col_def_pkt = try mysql_packet.readPacket(allocator, &client.reader.interface);
    defer allocator.free(col_def_pkt.payload);

    const sep_eof = try mysql_packet.readPacket(allocator, &client.reader.interface);
    defer allocator.free(sep_eof.payload);
    try std.testing.expectEqual(@as(usize, 5), sep_eof.payload.len);
    try std.testing.expectEqual(@as(u8, 0xFE), sep_eof.payload[0]);

    const row_pkt = try mysql_packet.readPacket(allocator, &client.reader.interface);
    defer allocator.free(row_pkt.payload);
    try std.testing.expect(row_pkt.payload[0] != 0xFE);

    const tail_eof = try mysql_packet.readPacket(allocator, &client.reader.interface);
    defer allocator.free(tail_eof.payload);
    try std.testing.expectEqual(@as(usize, 5), tail_eof.payload.len);
    try std.testing.expectEqual(@as(u8, 0xFE), tail_eof.payload[0]);

    try client.sendQuit();
    if (sctx.err) |e| return e;
}

test "mysql wire: empty initial_db leaves session at main/public" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var catalog = try openCatalog(allocator, io, tmp.dir);
    defer catalog.close();

    const db = catalog.database("main").?;
    const sc = db.schema("public").?;
    const tbl = try sc.table("orders", schema_orders, opts_orders);
    try tbl.insert(&.{
        .{ .id = @as(i64, 7), .qty = @as(i32, 70), .tag = "ok" },
    });
    try tbl.flush();

    const port: u16 = test_port_base + 7;
    const addr = std.Io.net.IpAddress{ .ip4 = .{ .bytes = .{ 127, 0, 0, 1 }, .port = port } };
    var server = try thindb.serveMysql(allocator, io, catalog, addr, null);
    defer server.destroy();
    defer server.close();

    var sctx: ServerCtx = .{ .server = server, .n = 1 };
    const t = try std.Thread.spawn(.{}, ServerCtx.run, .{&sctx});
    defer t.join();

    var client = try TestClient.connect(allocator, io, addr);
    defer client.close();
    try client.doHandshake("");

    try client.sendQuery("SELECT * FROM orders");

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const rows = try client.readResultSet(arena.allocator());
    try std.testing.expectEqual(@as(usize, 1), rows.len);
    try std.testing.expectEqualStrings("ok", rows[0][2].?);

    try client.sendQuit();
    if (sctx.err) |e| return e;
}

test "mysql wire: COM_RESET_CONNECTION (0x1F) clears session state" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var catalog = try openCatalog(allocator, io, tmp.dir);
    defer catalog.close();
    const main_db = catalog.database("main").?;
    _ = try main_db.createSchema("scratch");

    const port: u16 = test_port_base + 20;
    const addr = std.Io.net.IpAddress{ .ip4 = .{ .bytes = .{ 127, 0, 0, 1 }, .port = port } };
    var server = try thindb.serveMysql(allocator, io, catalog, addr, null);
    defer server.destroy();
    defer server.close();

    var sctx: ServerCtx = .{ .server = server, .n = 1 };
    const t = try std.Thread.spawn(.{}, ServerCtx.run, .{&sctx});
    defer t.join();

    var client = try TestClient.connect(allocator, io, addr);
    defer client.close();
    try client.doHandshake(null);

    // Open a transaction and switch to a non-default schema.
    try client.sendQuery("BEGIN");
    {
        const pkt = try mysql_packet.readPacket(allocator, &client.reader.interface);
        defer allocator.free(pkt.payload);
        try std.testing.expectEqual(@as(u8, 0x00), pkt.payload[0]);
    }
    try client.sendQuery("USE scratch");
    {
        const pkt = try mysql_packet.readPacket(allocator, &client.reader.interface);
        defer allocator.free(pkt.payload);
        try std.testing.expectEqual(@as(u8, 0x00), pkt.payload[0]);
    }

    // Binary RESET_CONNECTION wipes both.
    try client.sendResetConnection();
    {
        const pkt = try mysql_packet.readPacket(allocator, &client.reader.interface);
        defer allocator.free(pkt.payload);
        try std.testing.expectEqual(@as(u8, 0x00), pkt.payload[0]);
    }

    // SELECT DATABASE() should now return "public" again.
    try client.sendQuery("SELECT DATABASE()");
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const rows = try client.readResultSet(arena.allocator());
    try std.testing.expectEqual(@as(usize, 1), rows.len);
    try std.testing.expectEqualStrings("public", rows[0][0].?);

    try client.sendQuit();
    if (sctx.err) |e| return e;
}

test "mysql wire: user variables persist across statements and clear on RESET" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var catalog = try openCatalog(allocator, io, tmp.dir);
    defer catalog.close();

    const db = catalog.database("main").?;
    const sc = db.schema("public").?;
    const t = try sc.table("orders", schema_orders, opts_orders);
    try t.insert(&.{
        .{ .id = @as(i64, 1), .qty = @as(i32, 10), .tag = "a" },
        .{ .id = @as(i64, 2), .qty = @as(i32, 20), .tag = "b" },
        .{ .id = @as(i64, 3), .qty = @as(i32, 30), .tag = "c" },
    });
    try t.flush();

    const port: u16 = test_port_base + 21;
    const addr = std.Io.net.IpAddress{ .ip4 = .{ .bytes = .{ 127, 0, 0, 1 }, .port = port } };
    var server = try thindb.serveMysql(allocator, io, catalog, addr, null);
    defer server.destroy();
    defer server.close();

    var sctx: ServerCtx = .{ .server = server, .n = 1 };
    const th = try std.Thread.spawn(.{}, ServerCtx.run, .{&sctx});
    defer th.join();

    var client = try TestClient.connect(allocator, io, addr);
    defer client.close();
    try client.doHandshake("main__public");

    // SET a user variable on this connection.
    try client.sendQuery("SET @c = 15");
    {
        const pkt = try mysql_packet.readPacket(allocator, &client.reader.interface);
        defer allocator.free(pkt.payload);
        try std.testing.expectEqual(@as(u8, 0x00), pkt.payload[0]);
    }

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();

    // A LATER statement on the same connection sees @c = 15:
    // qty > 15 selects rows 2 and 3.
    try client.sendQuery("SELECT id FROM orders WHERE qty > @c ORDER BY id ASC");
    {
        const rows = try client.readResultSet(arena.allocator());
        try std.testing.expectEqual(@as(usize, 2), rows.len);
        try std.testing.expectEqualStrings("2", rows[0][0].?);
        try std.testing.expectEqualStrings("3", rows[1][0].?);
    }

    // RESET CONNECTION is what a connection pool sends on release. It must
    // wipe user variables so the next borrower never sees stale state.
    try client.sendResetConnection();
    {
        const pkt = try mysql_packet.readPacket(allocator, &client.reader.interface);
        defer allocator.free(pkt.payload);
        try std.testing.expectEqual(@as(u8, 0x00), pkt.payload[0]);
    }

    // @c is now unset → resolves to NULL → `qty > NULL` is UNKNOWN under 3VL,
    // which excludes every row. (Was previously an UnsupportedOp error.)
    try client.sendQuery("SELECT id FROM orders WHERE qty > @c ORDER BY id ASC");
    {
        const rows = try client.readResultSet(arena.allocator());
        try std.testing.expectEqual(@as(usize, 0), rows.len);
    }

    try client.sendQuit();
    if (sctx.err) |e| return e;
}

test "mysql wire: RESET CONNECTION returns OK" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var catalog = try openCatalog(allocator, io, tmp.dir);
    defer catalog.close();

    const port: u16 = test_port_base + 8;
    const addr = std.Io.net.IpAddress{ .ip4 = .{ .bytes = .{ 127, 0, 0, 1 }, .port = port } };
    var server = try thindb.serveMysql(allocator, io, catalog, addr, null);
    defer server.destroy();
    defer server.close();

    var sctx: ServerCtx = .{ .server = server, .n = 1 };
    const t = try std.Thread.spawn(.{}, ServerCtx.run, .{&sctx});
    defer t.join();

    var client = try TestClient.connect(allocator, io, addr);
    defer client.close();
    try client.doHandshake(null);

    try client.sendQuery("RESET CONNECTION");
    const pkt = try mysql_packet.readPacket(allocator, &client.reader.interface);
    defer allocator.free(pkt.payload);
    try std.testing.expect(pkt.payload.len > 0);
    try std.testing.expectEqual(@as(u8, 0x00), pkt.payload[0]);

    try client.sendQuit();
    if (sctx.err) |e| return e;
}

test "mysql wire: auth — trust mode accepts any password" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var catalog = try openCatalog(allocator, io, tmp.dir);
    defer catalog.close();

    const port: u16 = test_port_base + 30;
    const addr = std.Io.net.IpAddress{ .ip4 = .{ .bytes = .{ 127, 0, 0, 1 }, .port = port } };
    var server = try thindb.serveMysql(allocator, io, catalog, addr, null);
    defer server.destroy();
    defer server.close();
    // auth_password stays null → trust mode.

    var sctx: ServerCtx = .{ .server = server, .n = 1 };
    const t = try std.Thread.spawn(.{}, ServerCtx.run, .{&sctx});
    defer t.join();

    var client = try TestClient.connect(allocator, io, addr);
    defer client.close();
    // Client claims password "anything"; server should ignore.
    try client.doHandshakeFull(null, true, "anything");
    try client.sendQuit();
    if (sctx.err) |e| return e;
}

test "mysql wire: auth — correct password accepted" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var catalog = try openCatalog(allocator, io, tmp.dir);
    defer catalog.close();

    const port: u16 = test_port_base + 31;
    const addr = std.Io.net.IpAddress{ .ip4 = .{ .bytes = .{ 127, 0, 0, 1 }, .port = port } };
    var server = try thindb.serveMysql(allocator, io, catalog, addr, null);
    defer server.destroy();
    defer server.close();
    server.auth_password = "hunter2";

    var sctx: ServerCtx = .{ .server = server, .n = 1 };
    const t = try std.Thread.spawn(.{}, ServerCtx.run, .{&sctx});
    defer t.join();

    var client = try TestClient.connect(allocator, io, addr);
    defer client.close();
    try client.doHandshakeFull(null, true, "hunter2");
    try client.sendQuit();
    if (sctx.err) |e| return e;
}

test "mysql wire: auth — wrong password rejected with 1045 / 28000" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var catalog = try openCatalog(allocator, io, tmp.dir);
    defer catalog.close();

    const port: u16 = test_port_base + 32;
    const addr = std.Io.net.IpAddress{ .ip4 = .{ .bytes = .{ 127, 0, 0, 1 }, .port = port } };
    var server = try thindb.serveMysql(allocator, io, catalog, addr, null);
    defer server.destroy();
    defer server.close();
    server.auth_password = "hunter2";

    var sctx: ServerCtx = .{ .server = server, .n = 1 };
    const t = try std.Thread.spawn(.{}, ServerCtx.run, .{&sctx});
    defer t.join();

    var client = try TestClient.connect(allocator, io, addr);
    defer client.close();
    const rc = client.doHandshakeFull(null, true, "wrong");
    try std.testing.expectError(error.AuthRejected, rc);

    if (sctx.err) |e| return e;
}

test "mysql wire: auth — empty client response rejected when password set" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var catalog = try openCatalog(allocator, io, tmp.dir);
    defer catalog.close();

    const port: u16 = test_port_base + 33;
    const addr = std.Io.net.IpAddress{ .ip4 = .{ .bytes = .{ 127, 0, 0, 1 }, .port = port } };
    var server = try thindb.serveMysql(allocator, io, catalog, addr, null);
    defer server.destroy();
    defer server.close();
    server.auth_password = "hunter2";

    var sctx: ServerCtx = .{ .server = server, .n = 1 };
    const t = try std.Thread.spawn(.{}, ServerCtx.run, .{&sctx});
    defer t.join();

    var client = try TestClient.connect(allocator, io, addr);
    defer client.close();
    // No password passed → empty auth_response sent. Should be rejected.
    const rc = client.doHandshakeFull(null, true, null);
    try std.testing.expectError(error.AuthRejected, rc);

    if (sctx.err) |e| return e;
}

test "mysql wire: caching_sha2_password — correct password accepted" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var catalog = try openCatalog(allocator, io, tmp.dir);
    defer catalog.close();

    const port: u16 = test_port_base + 50;
    const addr = std.Io.net.IpAddress{ .ip4 = .{ .bytes = .{ 127, 0, 0, 1 }, .port = port } };
    var server = try thindb.serveMysql(allocator, io, catalog, addr, null);
    defer server.destroy();
    defer server.close();
    server.auth_password = "hunter2";

    var sctx: ServerCtx = .{ .server = server, .n = 1 };
    const t = try std.Thread.spawn(.{}, ServerCtx.run, .{&sctx});
    defer t.join();

    var client = try TestClient.connect(allocator, io, addr);
    defer client.close();
    try client.doHandshakeCachingSha2("hunter2");

    try client.sendQuit();
    if (sctx.err) |e| return e;
}

test "mysql wire: caching_sha2_password — wrong password rejected" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var catalog = try openCatalog(allocator, io, tmp.dir);
    defer catalog.close();

    const port: u16 = test_port_base + 51;
    const addr = std.Io.net.IpAddress{ .ip4 = .{ .bytes = .{ 127, 0, 0, 1 }, .port = port } };
    var server = try thindb.serveMysql(allocator, io, catalog, addr, null);
    defer server.destroy();
    defer server.close();
    server.auth_password = "hunter2";

    var sctx: ServerCtx = .{ .server = server, .n = 1 };
    const t = try std.Thread.spawn(.{}, ServerCtx.run, .{&sctx});
    defer t.join();

    var client = try TestClient.connect(allocator, io, addr);
    defer client.close();
    try std.testing.expectError(error.AuthRejected, client.doHandshakeCachingSha2("wrong"));

    if (sctx.err) |e| return e;
}

test "mysql wire: caching_sha2_password — trust mode accepts any hash" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var catalog = try openCatalog(allocator, io, tmp.dir);
    defer catalog.close();

    const port: u16 = test_port_base + 52;
    const addr = std.Io.net.IpAddress{ .ip4 = .{ .bytes = .{ 127, 0, 0, 1 }, .port = port } };
    var server = try thindb.serveMysql(allocator, io, catalog, addr, null);
    defer server.destroy();
    defer server.close();
    // auth_password stays null — trust mode.

    var sctx: ServerCtx = .{ .server = server, .n = 1 };
    const t = try std.Thread.spawn(.{}, ServerCtx.run, .{&sctx});
    defer t.join();

    var client = try TestClient.connect(allocator, io, addr);
    defer client.close();
    // Trust mode: server doesn't verify the hash for either plugin.
    try client.doHandshakeCachingSha2("whatever");

    try client.sendQuit();
    if (sctx.err) |e| return e;
}

test "mysql wire: COM_CHANGE_USER (0x11) — trust mode resets session state" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var catalog = try openCatalog(allocator, io, tmp.dir);
    defer catalog.close();
    const main_db = catalog.database("main").?;
    _ = try main_db.createSchema("warehouse");

    const port: u16 = test_port_base + 40;
    const addr = std.Io.net.IpAddress{ .ip4 = .{ .bytes = .{ 127, 0, 0, 1 }, .port = port } };
    var server = try thindb.serveMysql(allocator, io, catalog, addr, null);
    defer server.destroy();
    defer server.close();
    // Trust mode — no auth_password set.

    var sctx: ServerCtx = .{ .server = server, .n = 1 };
    const t = try std.Thread.spawn(.{}, ServerCtx.run, .{&sctx});
    defer t.join();

    var client = try TestClient.connect(allocator, io, addr);
    defer client.close();
    try client.doHandshake(null);

    // Open a transaction in the original session.
    try client.sendQuery("BEGIN");
    {
        const pkt = try mysql_packet.readPacket(allocator, &client.reader.interface);
        defer allocator.free(pkt.payload);
        try std.testing.expectEqual(@as(u8, 0x00), pkt.payload[0]);
    }

    // COM_CHANGE_USER as a new user, switching to a different schema.
    try client.sendChangeUser("newuser", "", "warehouse");
    {
        const pkt = try mysql_packet.readPacket(allocator, &client.reader.interface);
        defer allocator.free(pkt.payload);
        try std.testing.expectEqual(@as(u8, 0x00), pkt.payload[0]);
    }

    // SELECT DATABASE() should now report the new schema.
    try client.sendQuery("SELECT DATABASE()");
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const rows = try client.readResultSet(arena.allocator());
    try std.testing.expectEqual(@as(usize, 1), rows.len);
    try std.testing.expectEqualStrings("warehouse", rows[0][0].?);

    try client.sendQuit();
    if (sctx.err) |e| return e;
}

test "mysql wire: COM_CHANGE_USER — correct password accepted" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var catalog = try openCatalog(allocator, io, tmp.dir);
    defer catalog.close();

    const port: u16 = test_port_base + 41;
    const addr = std.Io.net.IpAddress{ .ip4 = .{ .bytes = .{ 127, 0, 0, 1 }, .port = port } };
    var server = try thindb.serveMysql(allocator, io, catalog, addr, null);
    defer server.destroy();
    defer server.close();
    server.auth_password = "hunter2";

    var sctx: ServerCtx = .{ .server = server, .n = 1 };
    const t = try std.Thread.spawn(.{}, ServerCtx.run, .{&sctx});
    defer t.join();

    var client = try TestClient.connect(allocator, io, addr);
    defer client.close();
    try client.doHandshakeFull(null, true, "hunter2");

    // Recompute hash against the same salt for COM_CHANGE_USER.
    const new_hash = thindb.mysql.auth.nativeHash("hunter2", client.last_salt);
    try client.sendChangeUser("otheruser", &new_hash, "");
    const pkt = try mysql_packet.readPacket(allocator, &client.reader.interface);
    defer allocator.free(pkt.payload);
    try std.testing.expectEqual(@as(u8, 0x00), pkt.payload[0]);

    try client.sendQuit();
    if (sctx.err) |e| return e;
}

test "mysql wire: COM_CHANGE_USER — wrong password rejected with 1045 / 28000" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var catalog = try openCatalog(allocator, io, tmp.dir);
    defer catalog.close();

    const port: u16 = test_port_base + 42;
    const addr = std.Io.net.IpAddress{ .ip4 = .{ .bytes = .{ 127, 0, 0, 1 }, .port = port } };
    var server = try thindb.serveMysql(allocator, io, catalog, addr, null);
    defer server.destroy();
    defer server.close();
    server.auth_password = "hunter2";

    var sctx: ServerCtx = .{ .server = server, .n = 1 };
    const t = try std.Thread.spawn(.{}, ServerCtx.run, .{&sctx});
    defer t.join();

    var client = try TestClient.connect(allocator, io, addr);
    defer client.close();
    try client.doHandshakeFull(null, true, "hunter2");

    // Hash for the wrong password.
    const bad_hash = thindb.mysql.auth.nativeHash("wrong", client.last_salt);
    try client.sendChangeUser("otheruser", &bad_hash, "");

    const pkt = try mysql_packet.readPacket(allocator, &client.reader.interface);
    defer allocator.free(pkt.payload);
    try std.testing.expect(pkt.payload.len > 3);
    try std.testing.expectEqual(@as(u8, 0xFF), pkt.payload[0]); // ERR
    const code = std.mem.readInt(u16, pkt.payload[1..3], .little);
    try std.testing.expectEqual(@as(u16, 1045), code);

    if (sctx.err) |e| return e;
}

test "mysql wire: KILL <unknown_id> → ER_NO_SUCH_THREAD (1094)" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var catalog = try openCatalog(allocator, io, tmp.dir);
    defer catalog.close();

    var registry = thindb.ConnectionRegistry.init(allocator);
    defer registry.deinit();

    const port: u16 = test_port_base + 60;
    const addr = std.Io.net.IpAddress{ .ip4 = .{ .bytes = .{ 127, 0, 0, 1 }, .port = port } };
    var server = try thindb.serveMysql(allocator, io, catalog, addr, null);
    defer server.destroy();
    defer server.close();
    server.registry = &registry;

    var sctx: ServerCtx = .{ .server = server, .n = 1 };
    const t = try std.Thread.spawn(.{}, ServerCtx.run, .{&sctx});
    defer t.join();

    var client = try TestClient.connect(allocator, io, addr);
    defer client.close();
    try client.doHandshake(null);

    try client.sendQuery("KILL 99999");
    const pkt = try mysql_packet.readPacket(allocator, &client.reader.interface);
    defer allocator.free(pkt.payload);
    try std.testing.expect(pkt.payload.len > 3);
    try std.testing.expectEqual(@as(u8, 0xFF), pkt.payload[0]);
    const code = std.mem.readInt(u16, pkt.payload[1..3], .little);
    try std.testing.expectEqual(@as(u16, 1094), code);

    try client.sendQuit();
    if (sctx.err) |e| return e;
}

fn expectErrPacket(client: *TestClient, code: u16) !void {
    const pkt = try mysql_packet.readPacket(client.allocator, &client.reader.interface);
    defer client.allocator.free(pkt.payload);
    try std.testing.expect(pkt.payload.len > 3);
    try std.testing.expectEqual(@as(u8, 0xFF), pkt.payload[0]);
    try std.testing.expectEqual(code, std.mem.readInt(u16, pkt.payload[1..3], .little));
}

/// The server closed the connection: the next read ends the stream (or, on
/// Windows, may see the reset instead).
fn expectConnectionClosed(client: *TestClient) !void {
    const pkt = mysql_packet.readPacket(client.allocator, &client.reader.interface) catch return;
    client.allocator.free(pkt.payload);
    return error.ConnectionStillOpen;
}

test "mysql wire: KILL of its own connection interrupts itself; only KILL QUERY keeps the connection" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var catalog = try openCatalog(allocator, io, tmp.dir);
    defer catalog.close();

    var registry = thindb.ConnectionRegistry.init(allocator);
    defer registry.deinit();

    const port: u16 = test_port_base + 61;
    const addr = std.Io.net.IpAddress{ .ip4 = .{ .bytes = .{ 127, 0, 0, 1 }, .port = port } };
    var server = try thindb.serveMysql(allocator, io, catalog, addr, null);
    defer server.destroy();
    defer server.close();
    server.registry = &registry;

    var sctx: ServerCtx = .{ .server = server, .n = 1 };
    const t = try std.Thread.spawn(.{}, ServerCtx.run, .{&sctx});
    var server_joined = false;
    defer if (!server_joined) t.join();

    var client = try TestClient.connect(allocator, io, addr);
    defer client.close();
    try client.doHandshake(null);

    // The first connection's id is 1.
    try client.sendQuery("KILL QUERY 1");
    try expectErrPacket(&client, 1317);
    try client.sendQuery("SELECT 1");
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const rows = try client.readResultSet(arena.allocator());
    try std.testing.expectEqualStrings("1", rows[0][0].?);

    try client.sendQuery("KILL 1");
    try expectErrPacket(&client, 1317);
    try expectConnectionClosed(&client);

    t.join();
    server_joined = true;
    try std.testing.expectEqual(@as(usize, 0), registry.count());
    if (sctx.err) |e| return e;
}

const slow_probe = @import("slow_probe.zig");
const SlowProbe = slow_probe.SlowProbe;
const slow_probe_rows = slow_probe.total_rows;
const seedSlowProbe = slow_probe.seed;

test "mysql wire: a read-only query is cancelled once its client disconnects" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var catalog = try openCatalog(allocator, io, tmp.dir);
    defer catalog.close();
    var probe: SlowProbe = .{ .io = io };
    try seedSlowProbe(catalog, &probe);

    var registry = thindb.ConnectionRegistry.init(allocator);
    defer registry.deinit();

    const port: u16 = test_port_base + 62;
    const addr = std.Io.net.IpAddress{ .ip4 = .{ .bytes = .{ 127, 0, 0, 1 }, .port = port } };
    var server = try thindb.serveMysql(allocator, io, catalog, addr, null);
    defer server.destroy();
    defer server.close();
    server.registry = &registry;

    var sctx: ServerCtx = .{ .server = server, .n = 1 };
    const t = try std.Thread.spawn(.{}, ServerCtx.run, .{&sctx});
    var server_joined = false;
    defer if (!server_joined) t.join();

    var client = try TestClient.connect(allocator, io, addr);
    var client_open = true;
    defer if (client_open) client.close();
    try client.doHandshake("main");

    try client.sendQuery("SELECT max(slow_probe(id)) AS m FROM t");
    try probe.awaitFirstBatch();
    try std.testing.expectEqual(@as(usize, 0), registry.cancelAbandonedQueries());

    client.close();
    client_open = false;
    var cancelled: usize = 0;
    for (0..400) |_| {
        cancelled = registry.cancelAbandonedQueries();
        if (cancelled != 0) break;
        try std.Io.sleep(io, .fromMilliseconds(5), .awake);
    }
    try std.testing.expectEqual(@as(usize, 1), cancelled);

    t.join();
    server_joined = true;
    try std.testing.expect(probe.rows.load(.monotonic) < slow_probe_rows);
    if (sctx.err) |e| return e;
}

test "mysql wire: a write keeps running after its client disconnects" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var catalog = try openCatalog(allocator, io, tmp.dir);
    defer catalog.close();
    var probe: SlowProbe = .{ .io = io };
    try seedSlowProbe(catalog, &probe);

    var registry = thindb.ConnectionRegistry.init(allocator);
    defer registry.deinit();

    const port: u16 = test_port_base + 63;
    const addr = std.Io.net.IpAddress{ .ip4 = .{ .bytes = .{ 127, 0, 0, 1 }, .port = port } };
    var server = try thindb.serveMysql(allocator, io, catalog, addr, null);
    defer server.destroy();
    defer server.close();
    server.registry = &registry;

    var sctx: ServerCtx = .{ .server = server, .n = 2 };
    const t = try std.Thread.spawn(.{}, ServerCtx.run, .{&sctx});
    defer t.join();

    {
        var writer_client = try TestClient.connect(allocator, io, addr);
        defer writer_client.close();
        try writer_client.doHandshake("main");
        try writer_client.sendQuery("INSERT INTO dst SELECT slow_probe(id) FROM t");
        try probe.awaitFirstBatch();
    }

    var cancelled: usize = 0;
    for (0..2000) |_| {
        if (probe.rows.load(.monotonic) >= slow_probe_rows) break;
        cancelled += registry.cancelAbandonedQueries();
        try std.Io.sleep(io, .fromMilliseconds(5), .awake);
    }
    try std.testing.expectEqual(@as(usize, 0), cancelled);

    // The server serves one connection at a time, so this handshake waits
    // for the INSERT to finish.
    var client = try TestClient.connect(allocator, io, addr);
    defer client.close();
    try client.doHandshake("main");
    try client.sendQuery("SELECT count(*) FROM dst");
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const rows = try client.readResultSet(arena.allocator());
    try std.testing.expectEqualStrings("4096", rows[0][0].?);
    try client.sendQuit();
    if (sctx.err) |e| return e;
}

/// A result far larger than loopback socket buffers, so a client that never
/// reads it leaves the server's send blocked.
const unread_result_query = "SELECT id, repeat('x', 8192) AS pad FROM t";

/// Reaps with a short write deadline until the stalled send is ended.
fn reapStalledWrite(registry: *thindb.ConnectionRegistry, io: std.Io) !usize {
    for (0..3000) |_| {
        const reaped = registry.reapStalledTransfers(io, thindb.conn_registry.nowMs(io), .{ .read_ms = 0, .write_ms = 200 });
        if (reaped != 0) return reaped;
        try std.Io.sleep(io, .fromMilliseconds(10), .awake);
    }
    return 0;
}

fn connectionsDrainWithin(registry: *thindb.ConnectionRegistry, io: std.Io, ms: usize) !bool {
    for (0..ms) |_| {
        if (registry.count() == 0) return true;
        try std.Io.sleep(io, .fromMilliseconds(1), .awake);
    }
    return false;
}

test "mysql wire: a client that stops reading its result hits the write deadline" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var catalog = try openCatalog(allocator, io, tmp.dir);
    defer catalog.close();
    var probe: SlowProbe = .{ .io = io };
    try seedSlowProbe(catalog, &probe);

    var registry = thindb.ConnectionRegistry.init(allocator);
    defer registry.deinit();

    const port: u16 = test_port_base + 68;
    const addr = std.Io.net.IpAddress{ .ip4 = .{ .bytes = .{ 127, 0, 0, 1 }, .port = port } };
    var server = try thindb.serveMysql(allocator, io, catalog, addr, null);
    defer server.destroy();
    defer server.close();
    server.registry = &registry;

    var sctx: ServerCtx = .{ .server = server, .n = 1 };
    const t = try std.Thread.spawn(.{}, ServerCtx.run, .{&sctx});
    var server_joined = false;
    defer if (!server_joined) t.join();

    var client = try TestClient.connect(allocator, io, addr);
    // Runs before the join: a failing run's blocked send ends with the reset.
    defer client.close();
    try client.doHandshake("main");
    try client.sendQuery(unread_result_query);

    try std.testing.expectEqual(@as(usize, 1), try reapStalledWrite(&registry, io));
    // The statement fails, and the connection closes and unregisters.
    try std.testing.expect(try connectionsDrainWithin(&registry, io, 10_000));
    t.join();
    server_joined = true;
    if (sctx.err) |e| return e;

    // Nothing of the statement is left holding the catalog gate.
    const lease = try catalog.acquireStatement(true);
    lease.release();
}

test "mysql wire: SHOW PROCESSLIST finds a running query and KILL QUERY interrupts it" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var catalog = try openCatalog(allocator, io, tmp.dir);
    defer catalog.close();
    var probe: SlowProbe = .{ .io = io };
    try seedSlowProbe(catalog, &probe);

    var registry = thindb.ConnectionRegistry.init(allocator);
    defer registry.deinit();

    const port: u16 = test_port_base + 64;
    const addr = std.Io.net.IpAddress{ .ip4 = .{ .bytes = .{ 127, 0, 0, 1 }, .port = port } };
    var server = try thindb.serveMysql(allocator, io, catalog, addr, null);
    defer server.destroy();
    defer server.close();
    server.registry = &registry;

    // One acceptOne per connection, each on its own thread, so both
    // connections are served at once. Connecting the runner first gives
    // it id 1.
    var runner_ctx: ServerCtx = .{ .server = server, .n = 1 };
    const runner_thread = try std.Thread.spawn(.{}, ServerCtx.run, .{&runner_ctx});
    defer runner_thread.join();
    var runner = try TestClient.connect(allocator, io, addr);
    defer runner.close();
    try runner.doHandshake("main");

    const slow_sql = "SELECT max(slow_probe(id)) AS largest_id_the_probe_saw FROM t WHERE id >= 0 AND id < 1000000000 AND id <> -1";
    try std.testing.expect(slow_sql.len > 100);
    try runner.sendQuery(slow_sql);
    try probe.awaitFirstBatch();

    var admin_ctx: ServerCtx = .{ .server = server, .n = 1 };
    const admin_thread = try std.Thread.spawn(.{}, ServerCtx.run, .{&admin_ctx});
    defer admin_thread.join();
    var admin = try TestClient.connect(allocator, io, addr);
    defer admin.close();
    try admin.doHandshake("main");

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();

    try admin.sendQuery("SHOW FULL PROCESSLIST");
    const full = try admin.readResultSet(arena.allocator());
    try std.testing.expectEqual(@as(usize, 2), full.len);
    try std.testing.expectEqual(@as(usize, 8), full[0].len);
    try std.testing.expectEqualStrings("1", full[0][0].?);
    try std.testing.expectEqualStrings("test", full[0][1].?);
    try std.testing.expect(std.mem.startsWith(u8, full[0][2].?, "127.0.0.1:"));
    try std.testing.expectEqualStrings("main__public", full[0][3].?);
    try std.testing.expectEqualStrings("Query", full[0][4].?);
    _ = try std.fmt.parseInt(u64, full[0][5].?, 10);
    try std.testing.expectEqualStrings("executing", full[0][6].?);
    try std.testing.expectEqualStrings(slow_sql, full[0][7].?);
    try std.testing.expectEqualStrings("2", full[1][0].?);
    try std.testing.expectEqualStrings("SHOW FULL PROCESSLIST", full[1][7].?);

    try admin.sendQuery("SHOW PROCESSLIST");
    const short = try admin.readResultSet(arena.allocator());
    try std.testing.expectEqualStrings(slow_sql[0..100], short[0][7].?);

    try admin.sendQuery("KILL QUERY 1");
    {
        const ok = try mysql_packet.readPacket(allocator, &admin.reader.interface);
        defer allocator.free(ok.payload);
        try std.testing.expectEqual(@as(u8, 0x00), ok.payload[0]);
    }

    // The runner's result ends in ER_QUERY_INTERRUPTED, possibly after
    // its column definitions.
    const interrupted_code = for (0..16) |_| {
        const pkt = try mysql_packet.readPacket(allocator, &runner.reader.interface);
        defer allocator.free(pkt.payload);
        if (pkt.payload[0] == 0xFF) break std.mem.readInt(u16, pkt.payload[1..3], .little);
    } else return error.NoErrorPacket;
    try std.testing.expectEqual(@as(u16, 1317), interrupted_code);
    try std.testing.expect(probe.rows.load(.monotonic) < slow_probe_rows);

    // The runner goes back to sleep once its reply is flushed.
    const idle = for (0..200) |_| {
        _ = arena.reset(.retain_capacity);
        try admin.sendQuery("SHOW PROCESSLIST");
        const rows = try admin.readResultSet(arena.allocator());
        if (std.mem.eql(u8, rows[0][4].?, "Sleep")) break rows[0];
        try std.Io.sleep(io, .fromMilliseconds(5), .awake);
    } else return error.RunnerNeverSlept;
    try std.testing.expectEqualStrings("", idle[6].?);
    try std.testing.expect(idle[7] == null);

    try runner.sendQuery("SELECT 1");
    const after = try runner.readResultSet(arena.allocator());
    try std.testing.expectEqualStrings("1", after[0][0].?);

    try runner.sendQuit();
    try admin.sendQuit();
    if (runner_ctx.err) |e| return e;
    if (admin_ctx.err) |e| return e;
}

test "mysql wire: KILL closes a running connection found through information_schema.PROCESSLIST, and an idle one" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var catalog = try openCatalog(allocator, io, tmp.dir);
    defer catalog.close();
    var probe: SlowProbe = .{ .io = io };
    try seedSlowProbe(catalog, &probe);

    var registry = thindb.ConnectionRegistry.init(allocator);
    defer registry.deinit();

    const port: u16 = test_port_base + 66;
    const addr = std.Io.net.IpAddress{ .ip4 = .{ .bytes = .{ 127, 0, 0, 1 }, .port = port } };
    var server = try thindb.serveMysql(allocator, io, catalog, addr, null);
    defer server.destroy();
    defer server.close();
    server.registry = &registry;

    // Ids follow connect order: runner 1, idler 2, admin 3.
    var runner_ctx: ServerCtx = .{ .server = server, .n = 1 };
    const runner_thread = try std.Thread.spawn(.{}, ServerCtx.run, .{&runner_ctx});
    var runner_joined = false;
    defer if (!runner_joined) runner_thread.join();
    var runner = try TestClient.connect(allocator, io, addr);
    defer runner.close();
    try runner.doHandshake("main");
    const slow_sql = "SELECT max(slow_probe(id)) AS m FROM t";
    try runner.sendQuery(slow_sql);
    try probe.awaitFirstBatch();

    var idler_ctx: ServerCtx = .{ .server = server, .n = 1 };
    const idler_thread = try std.Thread.spawn(.{}, ServerCtx.run, .{&idler_ctx});
    var idler_joined = false;
    defer if (!idler_joined) idler_thread.join();
    var idler = try TestClient.connect(allocator, io, addr);
    var idler_open = true;
    defer if (idler_open) idler.close();
    try idler.doHandshake("main");
    for ([_][]const u8{ "CREATE TEMP TABLE scratch (id BIGINT PRIMARY KEY)", "XA START 'killed'" }) |setup| {
        try idler.sendQuery(setup);
        const ok = try mysql_packet.readPacket(allocator, &idler.reader.interface);
        defer allocator.free(ok.payload);
        try std.testing.expectEqual(@as(u8, 0x00), ok.payload[0]);
    }

    var admin_ctx: ServerCtx = .{ .server = server, .n = 1 };
    const admin_thread = try std.Thread.spawn(.{}, ServerCtx.run, .{&admin_ctx});
    defer admin_thread.join();
    var admin = try TestClient.connect(allocator, io, addr);
    defer admin.close();
    try admin.doHandshake("main");

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();

    try admin.sendQuery("SELECT ID, USER, DB, STATE, INFO FROM information_schema.PROCESSLIST WHERE COMMAND = 'Query' AND ID <> CONNECTION_ID() AND TIME >= 0 ORDER BY ID DESC");
    const running = try admin.readResultSet(arena.allocator());
    try std.testing.expectEqual(@as(usize, 1), running.len);
    try std.testing.expectEqualStrings("1", running[0][0].?);
    try std.testing.expectEqualStrings("test", running[0][1].?);
    try std.testing.expectEqualStrings("main__public", running[0][2].?);
    try std.testing.expectEqualStrings("executing", running[0][3].?);
    try std.testing.expectEqualStrings(slow_sql, running[0][4].?);

    try admin.sendQuery("SELECT ID, EXECUTION_ENGINE FROM performance_schema.processlist ORDER BY ID");
    const all = try admin.readResultSet(arena.allocator());
    try std.testing.expectEqual(@as(usize, 3), all.len);
    for (all, [_][]const u8{ "1", "2", "3" }) |row, id| {
        try std.testing.expectEqualStrings(id, row[0].?);
        try std.testing.expectEqualStrings("PRIMARY", row[1].?);
    }

    try admin.sendQuery("KILL 1");
    {
        const ok = try mysql_packet.readPacket(allocator, &admin.reader.interface);
        defer allocator.free(ok.payload);
        try std.testing.expectEqual(@as(u8, 0x00), ok.payload[0]);
    }
    // The runner's reply, if any gets out before the socket goes, ends in
    // ER_QUERY_INTERRUPTED; then the connection is gone.
    for (0..16) |_| {
        const pkt = mysql_packet.readPacket(allocator, &runner.reader.interface) catch break;
        allocator.free(pkt.payload);
    } else return error.RunnerStillOpen;
    runner_thread.join();
    runner_joined = true;
    try std.testing.expect(probe.rows.load(.monotonic) < slow_probe_rows);

    try admin.sendQuery("KILL CONNECTION 2");
    {
        const ok = try mysql_packet.readPacket(allocator, &admin.reader.interface);
        defer allocator.free(ok.payload);
        try std.testing.expectEqual(@as(u8, 0x00), ok.payload[0]);
    }
    try expectConnectionClosed(&idler);
    // A client closes its end once it sees the server's close. Windows
    // completes the server's already-pending read only then, not on the
    // shutdown itself.
    idler.close();
    idler_open = false;
    idler_thread.join();
    idler_joined = true;
    try std.testing.expectError(error.FileNotFound, tmp.dir.access(io, "_temp/2", .{}));
    // The killed connection's ACTIVE branch was rolled back, so its xid is
    // free again.
    for ([_][]const u8{ "XA START 'killed'", "XA END 'killed'", "XA ROLLBACK 'killed'" }) |xa| {
        try admin.sendQuery(xa);
        const ok = try mysql_packet.readPacket(allocator, &admin.reader.interface);
        defer allocator.free(ok.payload);
        try std.testing.expectEqual(@as(u8, 0x00), ok.payload[0]);
    }

    try std.testing.expectEqual(@as(usize, 1), registry.count());
    try admin.sendQuery("KILL 2");
    try expectErrPacket(&admin, 1094);

    try admin.sendQuit();
    if (runner_ctx.err) |e| return e;
    if (idler_ctx.err) |e| return e;
}

test "mysql wire: limiter at zero capacity emits ER_CON_COUNT_ERROR on accept" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var catalog = try thindb.Catalog.open(allocator, io, tmp.dir, .{});
    defer catalog.close();
    _ = try catalog.createDatabase("main");

    var limiter = thindb.ConnectionLimiter.init(0);

    const port: u16 = test_port_base + 9;
    const addr = std.Io.net.IpAddress{ .ip4 = .{ .bytes = .{ 127, 0, 0, 1 }, .port = port } };
    var server = try thindb.serveMysql(allocator, io, catalog, addr, &limiter);
    defer server.destroy();
    defer server.close();

    var sctx: ServerCtx = .{ .server = server, .n = 1 };
    const t = try std.Thread.spawn(.{}, ServerCtx.run, .{&sctx});
    defer t.join();

    var c = try TestClient.connect(allocator, io, addr);
    defer c.close();

    const pkt = try mysql_packet.readPacket(allocator, &c.reader.interface);
    defer allocator.free(pkt.payload);
    try std.testing.expect(pkt.payload.len > 3);
    try std.testing.expectEqual(@as(u8, 0xFF), pkt.payload[0]);
    const code = std.mem.readInt(u16, pkt.payload[1..3], .little);
    try std.testing.expectEqual(@as(u16, 1040), code);

    if (sctx.err) |e| return e;
}

// ---------------------------------------------------------------------------
// mysql CLI subprocess tests — skipped when the binary isn't installed.
// ---------------------------------------------------------------------------

fn mysqlCliAvailable(allocator: std.mem.Allocator, io: std.Io) bool {
    const result = std.process.run(allocator, io, .{
        .argv = &.{ "mysql", "--version" },
    }) catch return false;
    defer allocator.free(result.stdout);
    defer allocator.free(result.stderr);
    return switch (result.term) {
        .exited => |code| code == 0,
        else => false,
    };
}

fn runMysqlCli(
    allocator: std.mem.Allocator,
    io: std.Io,
    port: u16,
    db: ?[]const u8,
    sql_text: []const u8,
) !std.process.RunResult {
    var port_buf: [16]u8 = undefined;
    const port_str = try std.fmt.bufPrint(&port_buf, "{d}", .{port});

    var args: std.ArrayList([]const u8) = .empty;
    defer args.deinit(allocator);
    try args.appendSlice(allocator, &.{
        "mysql",
        "--host=127.0.0.1",
        "--protocol=tcp",
        "--user=test",
        "--password=test",
        "--silent",
        "--batch",
        "--ssl-mode=DISABLED",
    });
    const port_arg = try std.fmt.allocPrint(allocator, "--port={s}", .{port_str});
    defer allocator.free(port_arg);
    try args.append(allocator, port_arg);

    var db_arg_buf: ?[]u8 = null;
    defer if (db_arg_buf) |b| allocator.free(b);
    if (db) |d| {
        const arg = try std.fmt.allocPrint(allocator, "--database={s}", .{d});
        db_arg_buf = arg;
        try args.append(allocator, arg);
    }
    const e_arg = try std.fmt.allocPrint(allocator, "--execute={s}", .{sql_text});
    defer allocator.free(e_arg);
    try args.append(allocator, e_arg);

    return std.process.run(allocator, io, .{
        .argv = args.items,
    });
}

test "mysql CLI: SELECT @@version round-trips when mysql is on PATH" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    if (!mysqlCliAvailable(allocator, io)) {
        std.debug.print("mysql CLI not available, skipping\n", .{});
        return;
    }

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var catalog = try openCatalog(allocator, io, tmp.dir);
    defer catalog.close();

    const port: u16 = test_port_base + 100;
    const addr = std.Io.net.IpAddress{ .ip4 = .{ .bytes = .{ 127, 0, 0, 1 }, .port = port } };
    var server = try thindb.serveMysql(allocator, io, catalog, addr, null);
    defer server.destroy();
    defer server.close();

    var sctx: ServerCtx = .{ .server = server, .n = 1 };
    const t = try std.Thread.spawn(.{}, ServerCtx.run, .{&sctx});
    defer t.join();

    const result = try runMysqlCli(allocator, io, port, null, "SELECT @@version");
    defer allocator.free(result.stdout);
    defer allocator.free(result.stderr);

    if (std.mem.indexOf(u8, result.stdout, "thinDB") == null) {
        std.debug.print("mysql CLI stdout: {s}\nstderr: {s}\n", .{ result.stdout, result.stderr });
        return error.MissingVersionString;
    }
    if (sctx.err) |e| return e;
}

test "mysql CLI: SELECT * FROM orders streams seeded rows when mysql is on PATH" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    if (!mysqlCliAvailable(allocator, io)) {
        std.debug.print("mysql CLI not available, skipping\n", .{});
        return;
    }

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var catalog = try openCatalog(allocator, io, tmp.dir);
    defer catalog.close();

    const db = catalog.database("main").?;
    const sc = db.schema("public").?;
    const tbl = try sc.table("orders", schema_orders, opts_orders);
    try tbl.insert(&.{
        .{ .id = @as(i64, 1), .qty = @as(i32, 10), .tag = "alpha" },
        .{ .id = @as(i64, 2), .qty = @as(i32, 20), .tag = "beta" },
    });
    try tbl.flush();

    const port: u16 = test_port_base + 101;
    const addr = std.Io.net.IpAddress{ .ip4 = .{ .bytes = .{ 127, 0, 0, 1 }, .port = port } };
    var server = try thindb.serveMysql(allocator, io, catalog, addr, null);
    defer server.destroy();
    defer server.close();

    var sctx: ServerCtx = .{ .server = server, .n = 1 };
    const t = try std.Thread.spawn(.{}, ServerCtx.run, .{&sctx});
    defer t.join();

    const result = try runMysqlCli(allocator, io, port, "main__public", "SELECT * FROM orders");
    defer allocator.free(result.stdout);
    defer allocator.free(result.stderr);

    if (std.mem.indexOf(u8, result.stdout, "alpha") == null or
        std.mem.indexOf(u8, result.stdout, "beta") == null)
    {
        std.debug.print("mysql CLI stdout: {s}\nstderr: {s}\n", .{ result.stdout, result.stderr });
        return error.MissingRowText;
    }
    if (sctx.err) |e| return e;
}

// ---------------------------------------------------------------------------
// COM_STMT_* — prepared-statement wire protocol
// ---------------------------------------------------------------------------

const MYSQL_TYPE_TINY: u8 = 0x01;
const MYSQL_TYPE_LONG: u8 = 0x03;
const MYSQL_TYPE_LONGLONG: u8 = 0x08;
const MYSQL_TYPE_DOUBLE: u8 = 0x05;
const MYSQL_TYPE_VAR_STRING: u8 = 0xfd;
const MYSQL_TYPE_DATE: u8 = 0x0a;
const MYSQL_TYPE_DATETIME: u8 = 0x0c;
const MYSQL_TYPE_NEWDECIMAL: u8 = 0xf6;

fn encodeLenEncString(allocator: std.mem.Allocator, payload: *std.ArrayList(u8), s: []const u8) !void {
    try mysql_packet.appendLenEncString(allocator, payload, s);
}

test "mysql wire: COM_STMT_PREPARE on parameterized SELECT returns param + column counts" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var catalog = try openCatalog(allocator, io, tmp.dir);
    defer catalog.close();

    const db = catalog.database("main").?;
    const sc = db.schema("public").?;
    const tbl = try sc.table("orders", schema_orders, opts_orders);
    try tbl.insert(&.{
        .{ .id = @as(i64, 1), .qty = @as(i32, 10), .tag = "a" },
        .{ .id = @as(i64, 2), .qty = @as(i32, 20), .tag = "b" },
    });
    try tbl.flush();

    const port: u16 = test_port_base + 200;
    const addr = std.Io.net.IpAddress{ .ip4 = .{ .bytes = .{ 127, 0, 0, 1 }, .port = port } };
    var server = try thindb.serveMysql(allocator, io, catalog, addr, null);
    defer server.destroy();
    defer server.close();

    var sctx: ServerCtx = .{ .server = server, .n = 1 };
    const t = try std.Thread.spawn(.{}, ServerCtx.run, .{&sctx});
    defer t.join();

    var client = try TestClient.connect(allocator, io, addr);
    defer client.close();
    try client.doHandshake(null);

    try client.sendStmtPrepare("SELECT id, qty FROM orders WHERE qty >= ?");
    const reply = try client.readPrepareReply(true);
    try std.testing.expect(reply.stmt_id != 0);
    try std.testing.expectEqual(@as(u16, 1), reply.num_params);
    // Schema inference: SELECT id, qty against a non-string predicate
    // succeeds; expect 2 output columns.
    try std.testing.expectEqual(@as(u16, 2), reply.num_columns);

    try client.sendQuit();
    if (sctx.err) |e| return e;
}

test "mysql wire: COM_STMT_EXECUTE returns rows matching the bound int param" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var catalog = try openCatalog(allocator, io, tmp.dir);
    defer catalog.close();

    const db = catalog.database("main").?;
    const sc = db.schema("public").?;
    const tbl = try sc.table("orders", schema_orders, opts_orders);
    try tbl.insert(&.{
        .{ .id = @as(i64, 1), .qty = @as(i32, 10), .tag = "a" },
        .{ .id = @as(i64, 2), .qty = @as(i32, 50), .tag = "b" },
        .{ .id = @as(i64, 3), .qty = @as(i32, 100), .tag = "c" },
    });
    try tbl.flush();

    const port: u16 = test_port_base + 201;
    const addr = std.Io.net.IpAddress{ .ip4 = .{ .bytes = .{ 127, 0, 0, 1 }, .port = port } };
    var server = try thindb.serveMysql(allocator, io, catalog, addr, null);
    defer server.destroy();
    defer server.close();

    var sctx: ServerCtx = .{ .server = server, .n = 1 };
    const t = try std.Thread.spawn(.{}, ServerCtx.run, .{&sctx});
    defer t.join();

    var client = try TestClient.connect(allocator, io, addr);
    defer client.close();
    try client.doHandshake(null);

    try client.sendStmtPrepare("SELECT id, qty FROM orders WHERE qty >= ?");
    const reply = try client.readPrepareReply(true);

    // Bind qty >= 50 (INT, 4 bytes LE).
    var val_buf: [4]u8 = undefined;
    std.mem.writeInt(i32, &val_buf, 50, .little);
    const params = [_]TestClient.Param{
        .{ .type_byte = MYSQL_TYPE_LONG, .value_bytes = &val_buf },
    };
    try client.sendStmtExecute(reply.stmt_id, &params);

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const rows = try client.readBinaryResultSet(arena.allocator(), true);
    try std.testing.expectEqual(@as(usize, 2), rows.len);

    // Each row: id (BIGINT 8-byte LE) then qty (INT 4-byte LE), no NULL.
    // 2 columns + 2-bit prefix = 4 bits → 1 nullmap byte.
    try std.testing.expectEqual(@as(usize, 1), rows[0].nullmap.len);
    try std.testing.expectEqual(@as(u8, 0), rows[0].nullmap[0] & 0b1111);

    const r0_id = std.mem.readInt(i64, rows[0].cells[0..8], .little);
    const r0_qty = std.mem.readInt(i32, rows[0].cells[8..12], .little);
    try std.testing.expectEqual(@as(i64, 2), r0_id);
    try std.testing.expectEqual(@as(i32, 50), r0_qty);

    const r1_id = std.mem.readInt(i64, rows[1].cells[0..8], .little);
    try std.testing.expectEqual(@as(i64, 3), r1_id);

    try client.sendQuit();
    if (sctx.err) |e| return e;
}

test "mysql wire: COM_STMT_EXECUTE declares a LARGEINT column DECIMAL and sends its digits" {
    // A bit operator's BIGINT UNSIGNED result is a LARGEINT (#323). Its
    // binary cell is its digits, which a LONGLONG column would misread.
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var catalog = try openCatalog(allocator, io, tmp.dir);
    defer catalog.close();

    const db = catalog.database("main").?;
    const sc = db.schema("public").?;
    const tbl = try sc.table("orders", schema_orders, opts_orders);
    try tbl.insert(&.{
        .{ .id = @as(i64, 1), .qty = @as(i32, 10), .tag = "a" },
        .{ .id = @as(i64, 2), .qty = @as(i32, 50), .tag = "b" },
    });
    try tbl.flush();

    const port: u16 = test_port_base + 214;
    const addr = std.Io.net.IpAddress{ .ip4 = .{ .bytes = .{ 127, 0, 0, 1 }, .port = port } };
    var server = try thindb.serveMysql(allocator, io, catalog, addr, null);
    defer server.destroy();
    defer server.close();

    var sctx: ServerCtx = .{ .server = server, .n = 1 };
    const t = try std.Thread.spawn(.{}, ServerCtx.run, .{&sctx});
    defer t.join();

    var client = try TestClient.connect(allocator, io, addr);
    defer client.close();
    try client.doHandshake(null);

    try client.sendStmtPrepare("SELECT id, ~qty FROM orders WHERE id <= ? ORDER BY id");
    const reply = try client.readPrepareReply(true);

    var val_buf: [8]u8 = undefined;
    std.mem.writeInt(i64, &val_buf, 2, .little);
    try client.sendStmtExecute(reply.stmt_id, &.{.{ .type_byte = MYSQL_TYPE_LONGLONG, .value_bytes = &val_buf }});

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const result = try client.readBinaryResult(arena.allocator(), true);
    try std.testing.expectEqualSlices(u8, &.{ MYSQL_TYPE_LONGLONG, MYSQL_TYPE_NEWDECIMAL }, result.column_types);
    try std.testing.expectEqual(@as(usize, 2), result.rows.len);

    // Each row: id (8-byte LE), then the LARGEINT as a length-prefixed string.
    const want = [_][]const u8{ "18446744073709551605", "18446744073709551565" };
    for (result.rows, 1.., want) |row, id, digits| {
        try std.testing.expectEqual(@as(i64, @intCast(id)), std.mem.readInt(i64, row.cells[0..8], .little));
        var c: usize = 8;
        try std.testing.expectEqualStrings(digits, try mysql_packet.readLenEncString(row.cells, &c));
        try std.testing.expectEqual(row.cells.len, c);
    }

    try client.sendQuit();
    if (sctx.err) |e| return e;
}

test "mysql wire: COM_STMT_PREPARE on bogus table returns ERR" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var catalog = try openCatalog(allocator, io, tmp.dir);
    defer catalog.close();

    const port: u16 = test_port_base + 202;
    const addr = std.Io.net.IpAddress{ .ip4 = .{ .bytes = .{ 127, 0, 0, 1 }, .port = port } };
    var server = try thindb.serveMysql(allocator, io, catalog, addr, null);
    defer server.destroy();
    defer server.close();

    var sctx: ServerCtx = .{ .server = server, .n = 1 };
    const t = try std.Thread.spawn(.{}, ServerCtx.run, .{&sctx});
    defer t.join();

    var client = try TestClient.connect(allocator, io, addr);
    defer client.close();
    try client.doHandshake(null);

    // Best-effort schema inference will fail (table missing). PREPARE
    // still SUCCEEDS — we register the stmt with num_columns=0. The
    // actual error surfaces at EXECUTE time. This matches how some
    // drivers expect "describe failure ≠ prepare failure" — both
    // outcomes are technically MySQL-spec-compatible. Verify the
    // statement is registered and EXECUTE produces ERR 1146.
    try client.sendStmtPrepare("SELECT * FROM nonexistent_table WHERE id = ?");
    const reply = try client.readPrepareReply(true);
    try std.testing.expectEqual(@as(u16, 1), reply.num_params);

    var val_buf: [8]u8 = undefined;
    std.mem.writeInt(i64, &val_buf, 1, .little);
    const params = [_]TestClient.Param{
        .{ .type_byte = MYSQL_TYPE_LONGLONG, .value_bytes = &val_buf },
    };
    try client.sendStmtExecute(reply.stmt_id, &params);

    const pkt = try mysql_packet.readPacket(allocator, &client.reader.interface);
    defer allocator.free(pkt.payload);
    try std.testing.expectEqual(@as(u8, 0xFF), pkt.payload[0]);
    const code = std.mem.readInt(u16, pkt.payload[1..3], .little);
    try std.testing.expectEqual(@as(u16, 1146), code);

    try client.sendQuit();
    if (sctx.err) |e| return e;
}

test "mysql wire: COM_STMT_EXECUTE on unknown statement_id returns ER_UNKNOWN_STMT_HANDLER" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var catalog = try openCatalog(allocator, io, tmp.dir);
    defer catalog.close();

    const port: u16 = test_port_base + 203;
    const addr = std.Io.net.IpAddress{ .ip4 = .{ .bytes = .{ 127, 0, 0, 1 }, .port = port } };
    var server = try thindb.serveMysql(allocator, io, catalog, addr, null);
    defer server.destroy();
    defer server.close();

    var sctx: ServerCtx = .{ .server = server, .n = 1 };
    const t = try std.Thread.spawn(.{}, ServerCtx.run, .{&sctx});
    defer t.join();

    var client = try TestClient.connect(allocator, io, addr);
    defer client.close();
    try client.doHandshake(null);

    try client.sendStmtExecute(9999, &.{});

    const pkt = try mysql_packet.readPacket(allocator, &client.reader.interface);
    defer allocator.free(pkt.payload);
    try std.testing.expectEqual(@as(u8, 0xFF), pkt.payload[0]);
    const code = std.mem.readInt(u16, pkt.payload[1..3], .little);
    try std.testing.expectEqual(@as(u16, 1243), code);

    try client.sendQuit();
    if (sctx.err) |e| return e;
}

test "mysql wire: COM_STMT_CLOSE then COM_STMT_EXECUTE on same id → ERR" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var catalog = try openCatalog(allocator, io, tmp.dir);
    defer catalog.close();

    const db = catalog.database("main").?;
    const sc = db.schema("public").?;
    const tbl = try sc.table("orders", schema_orders, opts_orders);
    try tbl.insert(&.{.{ .id = @as(i64, 1), .qty = @as(i32, 10), .tag = "a" }});
    try tbl.flush();

    const port: u16 = test_port_base + 204;
    const addr = std.Io.net.IpAddress{ .ip4 = .{ .bytes = .{ 127, 0, 0, 1 }, .port = port } };
    var server = try thindb.serveMysql(allocator, io, catalog, addr, null);
    defer server.destroy();
    defer server.close();

    var sctx: ServerCtx = .{ .server = server, .n = 1 };
    const t = try std.Thread.spawn(.{}, ServerCtx.run, .{&sctx});
    defer t.join();

    var client = try TestClient.connect(allocator, io, addr);
    defer client.close();
    try client.doHandshake(null);

    try client.sendStmtPrepare("SELECT id FROM orders WHERE id = ?");
    const reply = try client.readPrepareReply(true);

    try client.sendStmtClose(reply.stmt_id);
    // No response from CLOSE.

    var val: [8]u8 = undefined;
    std.mem.writeInt(i64, &val, 1, .little);
    const params = [_]TestClient.Param{
        .{ .type_byte = MYSQL_TYPE_LONGLONG, .value_bytes = &val },
    };
    try client.sendStmtExecute(reply.stmt_id, &params);

    const pkt = try mysql_packet.readPacket(allocator, &client.reader.interface);
    defer allocator.free(pkt.payload);
    try std.testing.expectEqual(@as(u8, 0xFF), pkt.payload[0]);
    const code = std.mem.readInt(u16, pkt.payload[1..3], .little);
    try std.testing.expectEqual(@as(u16, 1243), code);

    try client.sendQuit();
    if (sctx.err) |e| return e;
}

test "mysql wire: COM_STMT_PREPARE + EXECUTE for parameterized INSERT writes rows" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var catalog = try openCatalog(allocator, io, tmp.dir);
    defer catalog.close();

    const db = catalog.database("main").?;
    const sc = db.schema("public").?;
    const tbl = try sc.table("orders", schema_orders, opts_orders);

    const port: u16 = test_port_base + 205;
    const addr = std.Io.net.IpAddress{ .ip4 = .{ .bytes = .{ 127, 0, 0, 1 }, .port = port } };
    var server = try thindb.serveMysql(allocator, io, catalog, addr, null);
    defer server.destroy();
    defer server.close();

    var sctx: ServerCtx = .{ .server = server, .n = 1 };
    const t = try std.Thread.spawn(.{}, ServerCtx.run, .{&sctx});
    defer t.join();

    var client = try TestClient.connect(allocator, io, addr);
    defer client.close();
    try client.doHandshake(null);

    try client.sendStmtPrepare("INSERT INTO orders (id, qty, tag) VALUES (?, ?, ?)");
    const reply = try client.readPrepareReply(true);
    try std.testing.expectEqual(@as(u16, 3), reply.num_params);
    try std.testing.expectEqual(@as(u16, 0), reply.num_columns);

    var id_buf: [8]u8 = undefined;
    std.mem.writeInt(i64, &id_buf, 42, .little);
    var qty_buf: [4]u8 = undefined;
    std.mem.writeInt(i32, &qty_buf, -99, .little);

    // VAR_STRING value = lenenc length + bytes.
    var tag_payload: std.ArrayList(u8) = .empty;
    defer tag_payload.deinit(allocator);
    try mysql_packet.appendLenEncString(allocator, &tag_payload, "via-prepare");

    const params = [_]TestClient.Param{
        .{ .type_byte = MYSQL_TYPE_LONGLONG, .value_bytes = &id_buf },
        .{ .type_byte = MYSQL_TYPE_LONG, .value_bytes = &qty_buf },
        .{ .type_byte = MYSQL_TYPE_VAR_STRING, .value_bytes = tag_payload.items },
    };
    try client.sendStmtExecute(reply.stmt_id, &params);

    const ok = try mysql_packet.readPacket(allocator, &client.reader.interface);
    defer allocator.free(ok.payload);
    try std.testing.expectEqual(@as(u8, 0x00), ok.payload[0]);

    try client.sendQuit();
    if (sctx.err) |e| return e;
    try tbl.flush();

    // Verify the row landed via a direct scan.
    var q = try thindb.scan(allocator, tbl);
    defer q.deinit();
    var saw_match = false;
    while (try q.next()) |batch| {
        const ids = batch.values[0].data.bigint;
        const qtys = batch.values[1].data.int;
        const tags = batch.values[2].data.string;
        var r: usize = 0;
        while (r < batch.row_count) : (r += 1) {
            if (ids[r] == 42 and qtys[r] == -99 and std.mem.eql(u8, tags.rowBytes(r), "via-prepare")) {
                saw_match = true;
            }
        }
    }
    try std.testing.expect(saw_match);
}

test "mysql wire: two COM_STMT_PREPARE in one connection get independent statement_ids" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var catalog = try openCatalog(allocator, io, tmp.dir);
    defer catalog.close();

    const db = catalog.database("main").?;
    const sc = db.schema("public").?;
    const tbl = try sc.table("orders", schema_orders, opts_orders);
    try tbl.insert(&.{.{ .id = @as(i64, 1), .qty = @as(i32, 10), .tag = "a" }});
    try tbl.flush();

    const port: u16 = test_port_base + 206;
    const addr = std.Io.net.IpAddress{ .ip4 = .{ .bytes = .{ 127, 0, 0, 1 }, .port = port } };
    var server = try thindb.serveMysql(allocator, io, catalog, addr, null);
    defer server.destroy();
    defer server.close();

    var sctx: ServerCtx = .{ .server = server, .n = 1 };
    const t = try std.Thread.spawn(.{}, ServerCtx.run, .{&sctx});
    defer t.join();

    var client = try TestClient.connect(allocator, io, addr);
    defer client.close();
    try client.doHandshake(null);

    try client.sendStmtPrepare("SELECT id FROM orders WHERE id = ?");
    const a = try client.readPrepareReply(true);
    try client.sendStmtPrepare("SELECT qty FROM orders WHERE qty = ?");
    const b = try client.readPrepareReply(true);
    try std.testing.expect(a.stmt_id != b.stmt_id);

    try client.sendQuit();
    if (sctx.err) |e| return e;
}

test "mysql wire: COM_STMT_PREPARE on DDL — EXECUTE returns OK with no rows" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var catalog = try openCatalog(allocator, io, tmp.dir);
    defer catalog.close();

    const port: u16 = test_port_base + 207;
    const addr = std.Io.net.IpAddress{ .ip4 = .{ .bytes = .{ 127, 0, 0, 1 }, .port = port } };
    var server = try thindb.serveMysql(allocator, io, catalog, addr, null);
    defer server.destroy();
    defer server.close();

    var sctx: ServerCtx = .{ .server = server, .n = 1 };
    const t = try std.Thread.spawn(.{}, ServerCtx.run, .{&sctx});
    defer t.join();

    var client = try TestClient.connect(allocator, io, addr);
    defer client.close();
    try client.doHandshake(null);

    try client.sendStmtPrepare("CREATE DATABASE reports_stmt");
    const reply = try client.readPrepareReply(true);
    try std.testing.expectEqual(@as(u16, 0), reply.num_params);
    try std.testing.expectEqual(@as(u16, 0), reply.num_columns);

    try client.sendStmtExecute(reply.stmt_id, &.{});
    const pkt = try mysql_packet.readPacket(allocator, &client.reader.interface);
    defer allocator.free(pkt.payload);
    try std.testing.expectEqual(@as(u8, 0x00), pkt.payload[0]);

    try client.sendQuit();
    if (sctx.err) |e| return e;
}

test "mysql wire: COM_STMT_EXECUTE with new_params_bound_flag=0 reuses prior types" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var catalog = try openCatalog(allocator, io, tmp.dir);
    defer catalog.close();

    const db = catalog.database("main").?;
    const sc = db.schema("public").?;
    const tbl = try sc.table("orders", schema_orders, opts_orders);
    try tbl.insert(&.{
        .{ .id = @as(i64, 1), .qty = @as(i32, 10), .tag = "a" },
        .{ .id = @as(i64, 2), .qty = @as(i32, 50), .tag = "b" },
        .{ .id = @as(i64, 3), .qty = @as(i32, 100), .tag = "c" },
    });
    try tbl.flush();

    const port: u16 = test_port_base + 208;
    const addr = std.Io.net.IpAddress{ .ip4 = .{ .bytes = .{ 127, 0, 0, 1 }, .port = port } };
    var server = try thindb.serveMysql(allocator, io, catalog, addr, null);
    defer server.destroy();
    defer server.close();

    var sctx: ServerCtx = .{ .server = server, .n = 1 };
    const t = try std.Thread.spawn(.{}, ServerCtx.run, .{&sctx});
    defer t.join();

    var client = try TestClient.connect(allocator, io, addr);
    defer client.close();
    try client.doHandshake(null);

    try client.sendStmtPrepare("SELECT id FROM orders WHERE qty >= ?");
    const reply = try client.readPrepareReply(true);

    // First execute: bind qty >= 50 with new_params_bound_flag=1.
    var v1: [4]u8 = undefined;
    std.mem.writeInt(i32, &v1, 50, .little);
    {
        const params = [_]TestClient.Param{
            .{ .type_byte = MYSQL_TYPE_LONG, .value_bytes = &v1 },
        };
        try client.sendStmtExecute(reply.stmt_id, &params);
        var a1 = std.heap.ArenaAllocator.init(allocator);
        defer a1.deinit();
        const rows = try client.readBinaryResultSet(a1.allocator(), true);
        try std.testing.expectEqual(@as(usize, 2), rows.len);
    }

    // Second execute: reuse types — send only the value bytes.
    var v2: [4]u8 = undefined;
    std.mem.writeInt(i32, &v2, 100, .little);
    try client.sendStmtExecuteReuse(reply.stmt_id, 1, &v2);
    {
        var a2 = std.heap.ArenaAllocator.init(allocator);
        defer a2.deinit();
        const rows = try client.readBinaryResultSet(a2.allocator(), true);
        try std.testing.expectEqual(@as(usize, 1), rows.len);
    }

    try client.sendQuit();
    if (sctx.err) |e| return e;
}

test "mysql wire: COM_STMT_SEND_LONG_DATA accumulates string consumed by EXECUTE" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var catalog = try openCatalog(allocator, io, tmp.dir);
    defer catalog.close();

    const db = catalog.database("main").?;
    const sc = db.schema("public").?;
    const tbl = try sc.table("orders", schema_orders, opts_orders);

    const port: u16 = test_port_base + 209;
    const addr = std.Io.net.IpAddress{ .ip4 = .{ .bytes = .{ 127, 0, 0, 1 }, .port = port } };
    var server = try thindb.serveMysql(allocator, io, catalog, addr, null);
    defer server.destroy();
    defer server.close();

    var sctx: ServerCtx = .{ .server = server, .n = 1 };
    const t = try std.Thread.spawn(.{}, ServerCtx.run, .{&sctx});
    defer t.join();

    var client = try TestClient.connect(allocator, io, addr);
    defer client.close();
    try client.doHandshake(null);

    try client.sendStmtPrepare("INSERT INTO orders (id, qty, tag) VALUES (?, ?, ?)");
    const reply = try client.readPrepareReply(true);

    // Long-data the tag in two chunks.
    try client.sendStmtSendLongData(reply.stmt_id, 2, "long-");
    try client.sendStmtSendLongData(reply.stmt_id, 2, "tag");

    var id_buf: [8]u8 = undefined;
    std.mem.writeInt(i64, &id_buf, 7, .little);
    var qty_buf: [4]u8 = undefined;
    std.mem.writeInt(i32, &qty_buf, 11, .little);

    // Send the param-types-and-values block. The tag slot is NULL-
    // flagged so the server uses the long-data buffer instead.
    const params = [_]TestClient.Param{
        .{ .type_byte = MYSQL_TYPE_LONGLONG, .value_bytes = &id_buf },
        .{ .type_byte = MYSQL_TYPE_LONG, .value_bytes = &qty_buf },
        .{ .type_byte = MYSQL_TYPE_VAR_STRING, .value_bytes = null },
    };
    try client.sendStmtExecute(reply.stmt_id, &params);

    const ok = try mysql_packet.readPacket(allocator, &client.reader.interface);
    defer allocator.free(ok.payload);
    try std.testing.expectEqual(@as(u8, 0x00), ok.payload[0]);

    try client.sendQuit();
    if (sctx.err) |e| return e;
    try tbl.flush();

    var q = try thindb.scan(allocator, tbl);
    defer q.deinit();
    var saw_match = false;
    while (try q.next()) |batch| {
        const ids = batch.values[0].data.bigint;
        const tags = batch.values[2].data.string;
        var r: usize = 0;
        while (r < batch.row_count) : (r += 1) {
            if (ids[r] == 7 and std.mem.eql(u8, tags.rowBytes(r), "long-tag")) saw_match = true;
        }
    }
    try std.testing.expect(saw_match);
}

test "mysql wire: CREATE TEMP TABLE round-trip + RESET CONNECTION drops it" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var catalog = try openCatalog(allocator, io, tmp.dir);
    defer catalog.close();

    const port: u16 = test_port_base + 210;
    const addr = std.Io.net.IpAddress{ .ip4 = .{ .bytes = .{ 127, 0, 0, 1 }, .port = port } };
    var server = try thindb.serveMysql(allocator, io, catalog, addr, null);
    defer server.destroy();
    defer server.close();

    var sctx: ServerCtx = .{ .server = server, .n = 1 };
    const t = try std.Thread.spawn(.{}, ServerCtx.run, .{&sctx});
    defer t.join();

    var client = try TestClient.connect(allocator, io, addr);
    defer client.close();
    try client.doHandshake(null);

    try client.sendQuery("CREATE TEMP TABLE scratch (id BIGINT PRIMARY KEY, val INT)");
    {
        const ok = try mysql_packet.readPacket(allocator, &client.reader.interface);
        defer allocator.free(ok.payload);
        try std.testing.expectEqual(@as(u8, 0x00), ok.payload[0]);
    }

    try client.sendQuery("INSERT INTO scratch VALUES (1, 10), (2, 20)");
    {
        const ok = try mysql_packet.readPacket(allocator, &client.reader.interface);
        defer allocator.free(ok.payload);
        try std.testing.expectEqual(@as(u8, 0x00), ok.payload[0]);
    }

    try client.sendQuery("SELECT id FROM scratch ORDER BY id ASC");
    {
        var arena = std.heap.ArenaAllocator.init(allocator);
        defer arena.deinit();
        const rows = try client.readResultSet(arena.allocator());
        try std.testing.expectEqual(@as(usize, 2), rows.len);
        try std.testing.expectEqualStrings("1", rows[0][0].?);
        try std.testing.expectEqualStrings("2", rows[1][0].?);
    }

    // RESET CONNECTION drops the temp namespace.
    try client.sendQuery("RESET CONNECTION");
    {
        const ok = try mysql_packet.readPacket(allocator, &client.reader.interface);
        defer allocator.free(ok.payload);
        try std.testing.expectEqual(@as(u8, 0x00), ok.payload[0]);
    }

    // Same name now fails to resolve.
    try client.sendQuery("SELECT id FROM scratch");
    {
        const err_pkt = try mysql_packet.readPacket(allocator, &client.reader.interface);
        defer allocator.free(err_pkt.payload);
        try std.testing.expectEqual(@as(u8, 0xFF), err_pkt.payload[0]);
    }

    try client.sendQuit();
    if (sctx.err) |e| return e;
}

test "mysql wire: COM_RESET_CONNECTION binary command drops temp namespace" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var catalog = try openCatalog(allocator, io, tmp.dir);
    defer catalog.close();

    const port: u16 = test_port_base + 211;
    const addr = std.Io.net.IpAddress{ .ip4 = .{ .bytes = .{ 127, 0, 0, 1 }, .port = port } };
    var server = try thindb.serveMysql(allocator, io, catalog, addr, null);
    defer server.destroy();
    defer server.close();

    var sctx: ServerCtx = .{ .server = server, .n = 1 };
    const t = try std.Thread.spawn(.{}, ServerCtx.run, .{&sctx});
    defer t.join();

    var client = try TestClient.connect(allocator, io, addr);
    defer client.close();
    try client.doHandshake(null);

    try client.sendQuery("CREATE TEMP TABLE wipe_me (id BIGINT PRIMARY KEY)");
    {
        const ok = try mysql_packet.readPacket(allocator, &client.reader.interface);
        defer allocator.free(ok.payload);
        try std.testing.expectEqual(@as(u8, 0x00), ok.payload[0]);
    }

    // Send COM_RESET_CONNECTION (0x1F) directly.
    try mysql_packet.writePacket(&client.writer.interface, 0, &[_]u8{0x1F});
    try client.writer.interface.flush();
    {
        const ok = try mysql_packet.readPacket(allocator, &client.reader.interface);
        defer allocator.free(ok.payload);
        try std.testing.expectEqual(@as(u8, 0x00), ok.payload[0]);
    }

    try client.sendQuery("SELECT id FROM wipe_me");
    {
        const err_pkt = try mysql_packet.readPacket(allocator, &client.reader.interface);
        defer allocator.free(err_pkt.payload);
        try std.testing.expectEqual(@as(u8, 0xFF), err_pkt.payload[0]);
    }

    try client.sendQuit();
    if (sctx.err) |e| return e;
}

test "mysql wire: two connections, A's temp invisible to B" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var catalog = try openCatalog(allocator, io, tmp.dir);
    defer catalog.close();

    const port: u16 = test_port_base + 212;
    const addr = std.Io.net.IpAddress{ .ip4 = .{ .bytes = .{ 127, 0, 0, 1 }, .port = port } };
    var server = try thindb.serveMysql(allocator, io, catalog, addr, null);
    defer server.destroy();
    defer server.close();

    // Two sessions need two concurrent server threads — `acceptOne` runs
    // a session synchronously, so a single-threaded `n=2` would deadlock
    // (A's session can't drain while we're driving B from the main thread).
    var sctx_a: ServerCtx = .{ .server = server, .n = 1 };
    const ta = try std.Thread.spawn(.{}, ServerCtx.run, .{&sctx_a});
    defer ta.join();

    var client_a = try TestClient.connect(allocator, io, addr);
    defer client_a.close();
    try client_a.doHandshake(null);

    try client_a.sendQuery("CREATE TEMP TABLE only_a (id BIGINT PRIMARY KEY)");
    {
        const ok = try mysql_packet.readPacket(allocator, &client_a.reader.interface);
        defer allocator.free(ok.payload);
        try std.testing.expectEqual(@as(u8, 0x00), ok.payload[0]);
    }

    var sctx_b: ServerCtx = .{ .server = server, .n = 1 };
    const tb = try std.Thread.spawn(.{}, ServerCtx.run, .{&sctx_b});
    defer tb.join();

    var client_b = try TestClient.connect(allocator, io, addr);
    defer client_b.close();
    try client_b.doHandshake(null);

    try client_b.sendQuery("SELECT id FROM only_a");
    {
        const err_pkt = try mysql_packet.readPacket(allocator, &client_b.reader.interface);
        defer allocator.free(err_pkt.payload);
        try std.testing.expectEqual(@as(u8, 0xFF), err_pkt.payload[0]);
    }

    try client_a.sendQuit();
    try client_b.sendQuit();
    if (sctx_a.err) |e| return e;
    if (sctx_b.err) |e| return e;
}

test "mysql wire: an unqualified ON column resolves against the session's tables" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const catalog = try openCatalog(allocator, io, tmp.dir);
    defer catalog.close();
    const addr = std.Io.net.IpAddress{ .ip4 = .{ .bytes = .{ 127, 0, 0, 1 }, .port = test_port_base + 65 } };
    const server = try thindb.serveMysql(allocator, io, catalog, addr, null);
    defer server.destroy();
    defer server.close();
    var sctx: ServerCtx = .{ .server = server, .n = 1 };
    const thread = try std.Thread.spawn(.{}, ServerCtx.run, .{&sctx});
    defer thread.join();
    var client = try TestClient.connect(allocator, io, addr);
    defer client.close();
    try client.doHandshake("main");
    for ([_][]const u8{
        "CREATE TABLE t (id BIGINT NOT NULL, qty INT NOT NULL) ORDER BY (id)",
        "CREATE TABLE o (oid BIGINT NOT NULL, tid BIGINT NOT NULL) ORDER BY (oid)",
        "INSERT INTO t VALUES (1, 10), (2, 20)",
        "INSERT INTO o VALUES (10, 1), (11, 2), (12, 2)",
    }) |sql_text| {
        try client.sendQuery(sql_text);
        const packet = try mysql_packet.readPacket(allocator, &client.reader.interface);
        defer allocator.free(packet.payload);
        try std.testing.expectEqual(@as(u8, 0), packet.payload[0]);
    }
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    try client.sendQuery("SELECT oid FROM t JOIN o ON id = tid WHERE qty = 20 ORDER BY oid");
    const rows = try client.readResultSet(arena.allocator());
    try std.testing.expectEqual(@as(usize, 2), rows.len);
    try std.testing.expectEqualStrings("11", rows[0][0].?);
    try std.testing.expectEqualStrings("12", rows[1][0].?);

    try client.sendQuery("SELECT a.id FROM t a JOIN t b ON id = b.qty");
    {
        const packet = try mysql_packet.readPacket(allocator, &client.reader.interface);
        defer allocator.free(packet.payload);
        try std.testing.expectEqual(@as(u8, 0xff), packet.payload[0]);
        try std.testing.expectEqual(@as(u16, 1052), std.mem.readInt(u16, packet.payload[1..3], .little));
    }
    try client.sendQuit();
    if (sctx.err) |e| return e;
}

const doomed_schema = thindb.TableSchema{
    .columns = &.{.{ .name = "doomed_col", .type = .bigint }},
    .order_key = &.{"doomed_col"},
    .unique = false,
};
const doomed_opts = thindb.TableOptions{ .order_key = &.{"doomed_col"} };

/// Whether a reply shows nothing of the dropped `doomed` database: an ERR,
/// or a result set none of whose cells names it.
fn replyForgetsDoomed(client: *TestClient, arena: std.mem.Allocator) !bool {
    const first = try mysql_packet.readPacket(arena, &client.reader.interface);
    if (first.payload[0] == 0xFF) return true;
    if (first.payload[0] == 0x00) return false;
    const rows = try client.readResultRows(arena, first.payload);
    for (rows) |row| for (row) |cell| {
        if (std.mem.indexOf(u8, cell orelse "", "doomed") != null) return false;
    };
    return true;
}

// Metadata answers and COM_INIT_DB read the catalog outside any statement
// (#90). Each must wait out a DROP DATABASE's lease rather than read the
// database the drop frees.
test "mysql wire: catalog reads outside a statement wait out a DROP DATABASE" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const catalog = try openCatalog(allocator, io, tmp.dir);
    defer catalog.close();
    const addr = std.Io.net.IpAddress{ .ip4 = .{ .bytes = .{ 127, 0, 0, 1 }, .port = test_port_base + 67 } };
    const server = try thindb.serveMysql(allocator, io, catalog, addr, null);
    defer server.destroy();
    defer server.close();
    var sctx: ServerCtx = .{ .server = server, .n = 1 };
    const thread = try std.Thread.spawn(.{}, ServerCtx.run, .{&sctx});
    defer thread.join();
    var client = try TestClient.connect(allocator, io, addr);
    defer client.close();
    try client.doHandshake("main");

    const com_query: u8 = 0x03;
    const com_init_db: u8 = 0x02;
    const probes = .{
        .{ com_query, "SHOW TABLES FROM `doomed__public`" },
        .{ com_query, "SHOW FULL COLUMNS FROM `doomed_t` FROM `doomed__public`" },
        .{ com_query, "SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA = 'doomed__public'" },
        .{ com_query, "SHOW DATABASES" },
        .{ com_init_db, "doomed__public" },
    };
    inline for (probes) |probe| {
        const db = try catalog.createDatabase("doomed");
        _ = try db.table("doomed_t", doomed_schema, doomed_opts);
        {
            var drop_lease: ?thindb.Catalog.StatementLease = try catalog.acquireStatement(true);
            defer if (drop_lease) |lease| lease.release();
            try client.sendCommand(probe[0], probe[1]);
            try std.Io.sleep(io, .fromMilliseconds(50), .awake);
            try catalog.dropDatabase("doomed");
            drop_lease.?.release();
            drop_lease = null;
        }
        var arena = std.heap.ArenaAllocator.init(allocator);
        defer arena.deinit();
        if (!try replyForgetsDoomed(&client, arena.allocator())) {
            std.debug.print("answered from the dropped database: {s}\n", .{probe[1]});
            return error.TestUnexpectedResult;
        }
    }
    try client.sendQuit();
    if (sctx.err) |e| return e;
}

test "mysql wire: a bound parameter no DATE or DATETIME reads matches nothing" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var catalog = try openCatalog(allocator, io, tmp.dir);
    defer catalog.close();

    const port: u16 = test_port_base + 213;
    const addr = std.Io.net.IpAddress{ .ip4 = .{ .bytes = .{ 127, 0, 0, 1 }, .port = port } };
    var server = try thindb.serveMysql(allocator, io, catalog, addr, null);
    defer server.destroy();
    defer server.close();

    var sctx: ServerCtx = .{ .server = server, .n = 1 };
    const t = try std.Thread.spawn(.{}, ServerCtx.run, .{&sctx});
    defer t.join();

    var client = try TestClient.connect(allocator, io, addr);
    defer client.close();
    try client.doHandshake(null);

    for ([_][]const u8{
        "CREATE TABLE dt (id BIGINT PRIMARY KEY, d DATE, ts DATETIME)",
        "INSERT INTO dt VALUES (1, '2026-09-26', '2026-09-26 10:00:00'), (2, '2026-09-27', '2026-09-27 00:00:00')",
    }) |sql_text| {
        try client.sendQuery(sql_text);
        const packet = try mysql_packet.readPacket(allocator, &client.reader.interface);
        defer allocator.free(packet.payload);
        try std.testing.expectEqual(@as(u8, 0), packet.payload[0]);
    }

    // Spelled in the statement, the constant fails it with MySQL's 1525.
    for ([_][]const u8{ "SELECT id FROM dt WHERE d = 'abc'", "SELECT id, NULLIF(d, 'abc') FROM dt" }) |sql_text| {
        try client.sendQuery(sql_text);
        const packet = try mysql_packet.readPacket(allocator, &client.reader.interface);
        defer allocator.free(packet.payload);
        try std.testing.expectEqual(@as(u8, 0xff), packet.payload[0]);
        try std.testing.expectEqual(@as(u16, 1525), std.mem.readInt(u16, packet.payload[1..3], .little));
    }

    // Bound, it matches nothing: MySQL returns no rows for such a parameter,
    // and NULLIF returns its first argument.
    const statements = [_][]const u8{
        "SELECT id FROM dt WHERE d = ?",
        "SELECT id FROM dt WHERE d <> ?",
        "SELECT id FROM dt WHERE ts >= ?",
        "SELECT id FROM dt WHERE ts < ? AND id > 0",
        "SELECT id FROM dt WHERE d BETWEEN ? AND '2026-12-31'",
        "SELECT id FROM dt WHERE NULLIF(d, ?) IS NULL",
        "SELECT id FROM dt WHERE NULLIF(ts, ?) IS NULL",
    };
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    for (statements) |sql_text| {
        try client.sendStmtPrepare(sql_text);
        const reply = try client.readPrepareReply(true);
        for ([_][]const u8{ "", "abc", "Invalid Date" }) |value| {
            var param: std.ArrayList(u8) = .empty;
            defer param.deinit(allocator);
            try encodeLenEncString(allocator, &param, value);
            try client.sendStmtExecute(reply.stmt_id, &.{.{ .type_byte = MYSQL_TYPE_VAR_STRING, .value_bytes = param.items }});
            const rows = client.readBinaryResultSet(arena.allocator(), true) catch |err| {
                std.debug.print("bound '{s}' failed: {s}\n", .{ value, sql_text });
                return err;
            };
            try std.testing.expectEqual(@as(usize, 0), rows.len);
        }
    }

    try client.sendStmtPrepare("SELECT id FROM dt WHERE d = ?");
    const reply = try client.readPrepareReply(true);
    var param: std.ArrayList(u8) = .empty;
    defer param.deinit(allocator);
    try encodeLenEncString(allocator, &param, "2026-9-27");
    try client.sendStmtExecute(reply.stmt_id, &.{.{ .type_byte = MYSQL_TYPE_VAR_STRING, .value_bytes = param.items }});
    const rows = try client.readBinaryResultSet(arena.allocator(), true);
    try std.testing.expectEqual(@as(usize, 1), rows.len);
    try std.testing.expectEqual(@as(i64, 2), std.mem.readInt(i64, rows[0].cells[0..8], .little));

    try client.sendQuit();
    if (sctx.err) |e| return e;
}

test "mysql wire: a binary DATE or DATETIME parameter no date holds fails its statement with 1292" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var catalog = try openCatalog(allocator, io, tmp.dir);
    defer catalog.close();

    const port: u16 = test_port_base + 215;
    const addr = std.Io.net.IpAddress{ .ip4 = .{ .bytes = .{ 127, 0, 0, 1 }, .port = port } };
    var server = try thindb.serveMysql(allocator, io, catalog, addr, null);
    defer server.destroy();
    defer server.close();

    var sctx: ServerCtx = .{ .server = server, .n = 1 };
    const t = try std.Thread.spawn(.{}, ServerCtx.run, .{&sctx});
    defer t.join();

    var client = try TestClient.connect(allocator, io, addr);
    defer client.close();
    try client.doHandshake(null);
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();

    try expectQueryOk(&client, "CREATE TABLE bd (id BIGINT PRIMARY KEY, d DATE, ts DATETIME(6))");
    try expectQueryOk(&client, "INSERT INTO bd VALUES (1, '2026-09-29', '2026-09-29 10:00:00')");

    try client.sendStmtPrepare("SELECT id FROM bd WHERE d = ?");
    const select = try client.readPrepareReply(true);
    const sept_29 = [_]u8{ 4, 0xea, 0x07, 9, 29 };
    // A day 0 of March once cast -1 to an unsigned day of the year, and
    // mysql2 sends an invalid JavaScript Date as every part zero.
    const invalid = .{
        .{ MYSQL_TYPE_DATE, &[_]u8{0} },
        .{ MYSQL_TYPE_DATE, &[_]u8{ 4, 0xea, 0x07, 3, 0 } },
        .{ MYSQL_TYPE_DATE, &[_]u8{ 4, 0xea, 0x07, 2, 30 } },
        .{ MYSQL_TYPE_DATETIME, &[_]u8{ 11, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 } },
        .{ MYSQL_TYPE_DATETIME, &[_]u8{ 7, 0xea, 0x07, 9, 29, 24, 0, 0 } },
    };
    for (0..2) |_| {
        try client.sendStmtExecute(select.stmt_id, &.{.{ .type_byte = MYSQL_TYPE_DATE, .value_bytes = &sept_29 }});
        const rows = try client.readBinaryResultSet(arena.allocator(), true);
        try std.testing.expectEqual(@as(usize, 1), rows.len);
        try std.testing.expectEqual(@as(i64, 1), std.mem.readInt(i64, rows[0].cells[0..8], .little));
        inline for (invalid) |c| {
            try client.sendStmtExecute(select.stmt_id, &.{.{ .type_byte = c[0], .value_bytes = c[1] }});
            const err = try mysql_packet.readPacket(allocator, &client.reader.interface);
            defer allocator.free(err.payload);
            try std.testing.expectEqual(@as(u8, 0xff), err.payload[0]);
            try std.testing.expectEqual(@as(u16, 1292), std.mem.readInt(u16, err.payload[1..3], .little));
            try std.testing.expectEqualStrings("22007", err.payload[4..9]);
        }
    }

    try client.sendStmtPrepare("INSERT INTO bd VALUES (?, ?, ?)");
    const insert = try client.readPrepareReply(true);
    var id_buf: [8]u8 = undefined;
    std.mem.writeInt(i64, &id_buf, 2, .little);
    try client.sendStmtExecute(insert.stmt_id, &.{
        .{ .type_byte = MYSQL_TYPE_LONGLONG, .value_bytes = &id_buf },
        .{ .type_byte = MYSQL_TYPE_DATE, .value_bytes = &.{ 4, 0, 0, 2, 29 } },
        .{ .type_byte = MYSQL_TYPE_DATETIME, .value_bytes = &.{ 11, 0x0f, 0x27, 12, 31, 23, 59, 59, 0x3f, 0x42, 0x0f, 0x00 } },
    });
    const ok = try mysql_packet.readPacket(allocator, &client.reader.interface);
    defer allocator.free(ok.payload);
    try std.testing.expectEqual(@as(u8, 0x00), ok.payload[0]);
    try std.testing.expectEqualStrings("0000-02-29", (try queryCell(&client, arena.allocator(), "SELECT CAST(d AS CHAR) FROM bd WHERE id = 2")).?);
    try std.testing.expectEqualStrings("9999-12-31 23:59:59.999999", (try queryCell(&client, arena.allocator(), "SELECT CAST(ts AS CHAR) FROM bd WHERE id = 2")).?);

    try client.sendQuit();
    if (sctx.err) |e| return e;
}

fn expectQueryOk(client: *TestClient, sql_text: []const u8) !void {
    try client.sendQuery(sql_text);
    const packet = try mysql_packet.readPacket(client.allocator, &client.reader.interface);
    defer client.allocator.free(packet.payload);
    if (packet.payload[0] == 0x00) return;
    if (packet.payload[0] == 0xFF and packet.payload.len > 9) {
        std.debug.print("{s} failed: {d} {s}\n", .{ sql_text, std.mem.readInt(u16, packet.payload[1..3], .little), packet.payload[9..] });
    }
    return error.TestUnexpectedResult;
}

/// The one cell a one-row query returns.
fn queryCell(client: *TestClient, arena: std.mem.Allocator, sql_text: []const u8) !?[]const u8 {
    try client.sendQuery(sql_text);
    const first = try mysql_packet.readPacket(arena, &client.reader.interface);
    if (first.payload[0] == 0xFF) {
        if (first.payload.len > 9) {
            std.debug.print("{s} failed: {d} {s}\n", .{ sql_text, std.mem.readInt(u16, first.payload[1..3], .little), first.payload[9..] });
        }
        return error.TestUnexpectedResult;
    }
    const rows = try client.readResultRows(arena, first.payload);
    try std.testing.expectEqual(@as(usize, 1), rows.len);
    return rows[0][0];
}

fn expectQueryErr(client: *TestClient, sql_text: []const u8, code: u16, sqlstate: []const u8) !void {
    try client.sendQuery(sql_text);
    const packet = try mysql_packet.readPacket(client.allocator, &client.reader.interface);
    defer client.allocator.free(packet.payload);
    if (packet.payload[0] != 0xFF) {
        std.debug.print("{s} was not refused\n", .{sql_text});
        return error.TestUnexpectedResult;
    }
    try std.testing.expectEqual(code, std.mem.readInt(u16, packet.payload[1..3], .little));
    try std.testing.expectEqualStrings(sqlstate, packet.payload[4..9]);
}

fn seedCurrentDbProbe(client: *TestClient) !void {
    for ([_][]const u8{
        "CREATE DATABASE probe",
        "CREATE TABLE probe__public.t (id BIGINT PRIMARY KEY)",
        "INSERT INTO probe__public.t VALUES (7)",
        "CREATE DATABASE probe_v",
        "USE probe_v__public",
    }) |sql_text| try expectQueryOk(client, sql_text);
}

/// A session whose current database is gone behaves as MySQL's with none
/// selected (#372): only an unqualified table reference fails.
fn expectSessionWithoutDatabase(client: *TestClient, arena: std.mem.Allocator) !void {
    try std.testing.expectEqual(@as(?[]const u8, null), try queryCell(client, arena, "SELECT DATABASE()"));
    try std.testing.expectEqualStrings("1", (try queryCell(client, arena, "SELECT 1")).?);
    try std.testing.expectEqualStrings("7", (try queryCell(client, arena, "SELECT id FROM probe__public.t")).?);
    try expectQueryErr(client, "SELECT id FROM t", 1046, "3D000");
    try client.sendQuery("SHOW DATABASES");
    _ = try client.readResultSet(arena);
    try expectQueryOk(client, "CREATE DATABASE probe_v");
    try expectQueryOk(client, "USE probe__public");
    try std.testing.expectEqualStrings("7", (try queryCell(client, arena, "SELECT id FROM t")).?);
}

test "mysql wire: dropping the current database leaves the session with none selected" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const catalog = try openCatalog(allocator, io, tmp.dir);
    defer catalog.close();
    const addr = std.Io.net.IpAddress{ .ip4 = .{ .bytes = .{ 127, 0, 0, 1 }, .port = test_port_base + 372 } };
    const server = try thindb.serveMysql(allocator, io, catalog, addr, null);
    defer server.destroy();
    defer server.close();
    var sctx: ServerCtx = .{ .server = server, .n = 1 };
    const thread = try std.Thread.spawn(.{}, ServerCtx.run, .{&sctx});
    defer thread.join();
    var client = try TestClient.connect(allocator, io, addr);
    defer client.close();
    try client.doHandshake(null);

    try seedCurrentDbProbe(&client);
    try expectQueryOk(&client, "DROP DATABASE probe_v");
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    try expectSessionWithoutDatabase(&client, arena.allocator());

    try client.sendQuit();
    if (sctx.err) |e| return e;
}

test "mysql wire: a current database another session drops leaves this one with none selected" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const catalog = try openCatalog(allocator, io, tmp.dir);
    defer catalog.close();
    const addr = std.Io.net.IpAddress{ .ip4 = .{ .bytes = .{ 127, 0, 0, 1 }, .port = test_port_base + 373 } };
    const server = try thindb.serveMysql(allocator, io, catalog, addr, null);
    defer server.destroy();
    defer server.close();

    var sctx_a: ServerCtx = .{ .server = server, .n = 1 };
    const thread_a = try std.Thread.spawn(.{}, ServerCtx.run, .{&sctx_a});
    defer thread_a.join();
    var client_a = try TestClient.connect(allocator, io, addr);
    defer client_a.close();
    try client_a.doHandshake(null);
    try seedCurrentDbProbe(&client_a);

    var sctx_b: ServerCtx = .{ .server = server, .n = 1 };
    const thread_b = try std.Thread.spawn(.{}, ServerCtx.run, .{&sctx_b});
    defer thread_b.join();
    var client_b = try TestClient.connect(allocator, io, addr);
    defer client_b.close();
    try client_b.doHandshake(null);
    try expectQueryOk(&client_b, "DROP DATABASE probe_v");

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    try expectSessionWithoutDatabase(&client_a, arena.allocator());

    try client_a.sendQuit();
    try client_b.sendQuit();
    if (sctx_a.err) |e| return e;
    if (sctx_b.err) |e| return e;
}

test "mysql wire: the engine's own root directories are not databases" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const catalog = try openCatalog(allocator, io, tmp.dir);
    defer catalog.close();
    const addr = std.Io.net.IpAddress{ .ip4 = .{ .bytes = .{ 127, 0, 0, 1 }, .port = test_port_base + 374 } };
    const server = try thindb.serveMysql(allocator, io, catalog, addr, null);
    defer server.destroy();
    defer server.close();
    const markers = [_][]const u8{ "_xa/keep", "_temp/keep", "_zigfn_build/main_f/keep" };
    for (markers) |marker| {
        try tmp.dir.createDirPath(io, std.fs.path.dirname(marker).?);
        try tmp.dir.writeFile(io, .{ .sub_path = marker, .data = "engine" });
    }

    var sctx: ServerCtx = .{ .server = server, .n = 1 };
    const thread = try std.Thread.spawn(.{}, ServerCtx.run, .{&sctx});
    defer thread.join();
    var client = try TestClient.connect(allocator, io, addr);
    defer client.close();
    try client.doHandshake(null);

    for ([_][]const u8{
        "DROP DATABASE _xa",
        "DROP DATABASE IF EXISTS _temp",
        "DROP DATABASE `_ZIGFN_BUILD`",
        "CREATE DATABASE _xa",
        "CREATE DATABASE IF NOT EXISTS _temp",
        "USE _zigfn_build",
        "USE _xa__public",
    }) |sql_text| try expectQueryErr(&client, sql_text, 1102, "42000");
    try client.sendCommand(0x02, "_temp__public");
    try expectErrPacket(&client, 1102);

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    try client.sendQuery("SHOW DATABASES");
    const rows = try client.readResultSet(arena.allocator());
    for (rows) |row| {
        if (std.mem.startsWith(u8, row[0].?, "_")) {
            std.debug.print("SHOW DATABASES lists {s}\n", .{row[0].?});
            return error.TestUnexpectedResult;
        }
    }
    try client.sendQuit();
    if (sctx.err) |e| return e;

    for (markers) |marker| try tmp.dir.access(io, marker, .{});
}

/// The ERR packet a statement ends with, whether the server refuses it
/// before any result set or fails it after the column definitions.
fn expectStatementErr(client: *TestClient, sql_text: []const u8, code: u16, sqlstate: []const u8, message: ?[]const u8) !void {
    const a = client.allocator;
    try client.sendQuery(sql_text);
    const first = try mysql_packet.readPacket(a, &client.reader.interface);
    defer a.free(first.payload);
    if (first.payload[0] == 0xFF) return expectErrPayload(sql_text, first.payload, code, sqlstate, message);
    var cursor: usize = 0;
    const col_count = try mysql_packet.readLenEncInt(first.payload, &cursor);
    var i: u64 = 0;
    while (i < col_count) : (i += 1) {
        const column_def = try mysql_packet.readPacket(a, &client.reader.interface);
        a.free(column_def.payload);
    }
    while (true) {
        const pkt = try mysql_packet.readPacket(a, &client.reader.interface);
        defer a.free(pkt.payload);
        if (pkt.payload[0] == 0xFF) return expectErrPayload(sql_text, pkt.payload, code, sqlstate, message);
        if (pkt.payload[0] == 0xFE) {
            std.debug.print("{s} succeeded\n", .{sql_text});
            return error.TestUnexpectedResult;
        }
    }
}

fn expectErrPayload(sql_text: []const u8, payload: []const u8, code: u16, sqlstate: []const u8, message: ?[]const u8) !void {
    const got = std.mem.readInt(u16, payload[1..3], .little);
    if (got != code) std.debug.print("{s}: {d} {s} {s}\n", .{ sql_text, got, payload[4..9], payload[9..] });
    try std.testing.expectEqual(code, got);
    try std.testing.expectEqualStrings(sqlstate, payload[4..9]);
    if (message) |m| try std.testing.expectEqualStrings(m, payload[9..]);
}

test "mysql wire: runtime errors carry MySQL's codes, and only parse errors are 1064 (issue #490)" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const catalog = try thindb.Catalog.open(allocator, io, tmp.dir, .{
        .query_memory_budget = 64 * 1024,
        .row_group_size = 128,
    });
    defer catalog.close();
    const db = try catalog.createDatabase("main");
    const schema = thindb.TableSchema{
        .columns = &.{.{ .name = "id", .type = .bigint }},
        .order_key = &.{"id"},
        .unique = false,
    };
    const ok = [_][]const u8{"id"};
    const t = try db.schema("public").?.table("big", schema, .{ .order_key = &ok, .unique = false, .row_group_size = 128 });
    var id: i64 = 0;
    while (id < 20_000) : (id += 1) try t.insert(&.{.{ .id = id }});
    try t.flush();

    const addr = std.Io.net.IpAddress{ .ip4 = .{ .bytes = .{ 127, 0, 0, 1 }, .port = test_port_base + 490 } };
    const server = try thindb.serveMysql(allocator, io, catalog, addr, null);
    defer server.destroy();
    defer server.close();
    var sctx: ServerCtx = .{ .server = server, .n = 1 };
    const thread = try std.Thread.spawn(.{}, ServerCtx.run, .{&sctx});
    defer thread.join();
    var client = try TestClient.connect(allocator, io, addr);
    defer client.close();
    try client.doHandshake("main__public");

    try expectStatementErr(&client, "WAT IS THIS NOT SQL", 1064, "42000", null);
    try expectStatementErr(&client, "SELECT FOUND_ROWS()", 1235, "42000", "SqlFoundRowsUnsupported");
    try expectStatementErr(&client, "SELECT SLEEP(-1)", 1210, "HY000", "IncorrectArgumentsToSleep");
    try expectStatementErr(&client, "SELECT id FROM big ORDER BY id DESC", 3170, "HY000", "MemoryBudgetExceeded");
    try expectStatementErr(&client, "SELECT JSON_EXTRACT('{\"a\": 1}', 'a')", 1105, "HY000", null);

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    try std.testing.expectEqualStrings("19999", (try queryCell(&client, arena.allocator(), "SELECT max(id) FROM big")).?);

    try client.sendQuit();
    if (sctx.err) |e| return e;
}
