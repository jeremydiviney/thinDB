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
        var payload: std.ArrayList(u8) = .empty;
        defer payload.deinit(self.allocator);
        try payload.append(self.allocator, 0x03);
        try payload.appendSlice(self.allocator, sql_text);
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
        const col_count_pkt = try mysql_packet.readPacket(self.allocator, &self.reader.interface);
        defer self.allocator.free(col_count_pkt.payload);
        if (col_count_pkt.payload.len == 0) return error.MalformedResultSet;
        if (col_count_pkt.payload[0] == 0xFF) return error.QueryRejected;
        if (col_count_pkt.payload[0] == 0x00) return error.UnexpectedOk;

        var cursor: usize = 0;
        const col_count = try mysql_packet.readLenEncInt(col_count_pkt.payload, &cursor);

        var i: u64 = 0;
        while (i < col_count) : (i += 1) {
            const p = try mysql_packet.readPacket(self.allocator, &self.reader.interface);
            self.allocator.free(p.payload);
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
        return try rows.toOwnedSlice(arena);
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

        var cursor: usize = 0;
        const col_count = try mysql_packet.readLenEncInt(col_count_pkt.payload, &cursor);

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

test "mysql wire: KILL <self_id> sets the cancel flag (no registry → no-op success)" {
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
    defer t.join();

    var client = try TestClient.connect(allocator, io, addr);
    defer client.close();
    try client.doHandshake(null);

    // The first connection's id is the value after the first
    // fetchAdd, which is 1 (server.connection_counter starts at 0;
    // fetchAdd returns 0 and we +1). KILLing it should succeed.
    try client.sendQuery("KILL 1");
    const pkt = try mysql_packet.readPacket(allocator, &client.reader.interface);
    defer allocator.free(pkt.payload);
    try std.testing.expectEqual(@as(u8, 0x00), pkt.payload[0]);

    try client.sendQuit();
    if (sctx.err) |e| return e;
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
