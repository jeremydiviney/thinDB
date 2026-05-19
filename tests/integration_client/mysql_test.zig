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
        const greet = try mysql_packet.readPacket(self.allocator, &self.reader.interface);
        defer self.allocator.free(greet.payload);

        var payload: std.ArrayList(u8) = .empty;
        defer payload.deinit(self.allocator);

        const caps = mysql_handshake.CLIENT_PROTOCOL_41 |
            mysql_handshake.CLIENT_SECURE_CONNECTION |
            mysql_handshake.CLIENT_PLUGIN_AUTH |
            (if (initial_db != null) mysql_handshake.CLIENT_CONNECT_WITH_DB else 0) |
            mysql_handshake.CLIENT_DEPRECATE_EOF;

        var buf4: [4]u8 = undefined;
        std.mem.writeInt(u32, &buf4, caps, .little);
        try payload.appendSlice(self.allocator, &buf4);

        std.mem.writeInt(u32, &buf4, 0x01000000, .little);
        try payload.appendSlice(self.allocator, &buf4);

        try payload.append(self.allocator, 0xff);
        try payload.appendSlice(self.allocator, &([_]u8{0} ** 23));

        try payload.appendSlice(self.allocator, "test\x00");

        try payload.append(self.allocator, 0);

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
    var server = try thindb.serveMysql(allocator, io, catalog, addr);
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
    var server = try thindb.serveMysql(allocator, io, catalog, addr);
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
    var server = try thindb.serveMysql(allocator, io, catalog, addr);
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
    var server = try thindb.serveMysql(allocator, io, catalog, addr);
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
    var server = try thindb.serveMysql(allocator, io, catalog, addr);
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
    var server = try thindb.serveMysql(allocator, io, catalog, addr);
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
    var server = try thindb.serveMysql(allocator, io, catalog, addr);
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
    var server = try thindb.serveMysql(allocator, io, catalog, addr);
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
