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
