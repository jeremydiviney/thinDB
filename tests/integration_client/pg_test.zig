//! PostgreSQL wire protocol integration tests.
//!
//! Two flavors:
//!   - Standalone: a small in-Zig client speaks just enough wire format
//!     to verify the server end-to-end (no external binary required).
//!   - psql CLI: spawns the real `psql` binary, asserts on its stdout.
//!     Skipped automatically when `psql` isn't on PATH.

const std = @import("std");
const thindb = @import("thindb");

const pg_packet = thindb.pg.packet;
const pg_startup = thindb.pg.startup;

const test_port_base: u16 = 29543;

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

/// Minimal in-Zig PG client. Speaks just enough to complete a startup
/// and run a few Simple Query round-trips.
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

    fn sendStartup(self: *TestClient, user: []const u8, database: ?[]const u8) !void {
        var payload: std.ArrayList(u8) = .empty;
        defer payload.deinit(self.allocator);

        try pg_packet.appendU32(self.allocator, &payload, pg_startup.protocol_v3);
        try pg_packet.appendCString(self.allocator, &payload, "user");
        try pg_packet.appendCString(self.allocator, &payload, user);
        if (database) |d| {
            try pg_packet.appendCString(self.allocator, &payload, "database");
            try pg_packet.appendCString(self.allocator, &payload, d);
        }
        try payload.append(self.allocator, 0);

        try pg_packet.writeStartupFrame(&self.writer.interface, payload.items);
        try self.writer.interface.flush();
    }

    fn sendSslRequest(self: *TestClient) !void {
        var payload: std.ArrayList(u8) = .empty;
        defer payload.deinit(self.allocator);
        try pg_packet.appendU32(self.allocator, &payload, pg_startup.ssl_request_magic);
        try pg_packet.writeStartupFrame(&self.writer.interface, payload.items);
        try self.writer.interface.flush();
    }

    fn readSslReply(self: *TestClient) !u8 {
        var buf: [1]u8 = undefined;
        try self.reader.interface.readSliceAll(&buf);
        return buf[0];
    }

    /// Drain frames until ReadyForQuery (`Z`) arrives. Returns nothing —
    /// caller has already consumed any rows it cared about.
    fn drainUntilReady(self: *TestClient) !void {
        while (true) {
            const f = try pg_packet.readFrame(self.allocator, &self.reader.interface);
            defer self.allocator.free(f.payload);
            if (f.type_byte == 'Z') return;
            if (f.type_byte == 'E') return error.PgErrorResponse;
        }
    }

    fn completeStartup(self: *TestClient, user: []const u8, database: ?[]const u8) !void {
        try self.sendStartup(user, database);
        try self.drainUntilReady();
    }

    fn sendQuery(self: *TestClient, sql_text: []const u8) !void {
        var payload: std.ArrayList(u8) = .empty;
        defer payload.deinit(self.allocator);
        try payload.appendSlice(self.allocator, sql_text);
        try payload.append(self.allocator, 0);
        try pg_packet.writeFrame(&self.writer.interface, 'Q', payload.items);
        try self.writer.interface.flush();
    }

    fn sendTerminate(self: *TestClient) !void {
        try pg_packet.writeFrame(&self.writer.interface, 'X', "");
        try self.writer.interface.flush();
    }

    const Row = []const ?[]const u8;
    const QueryReply = struct {
        rows: []const Row,
        command_tag: []const u8,
        error_code: ?[]const u8 = null,
        error_message: ?[]const u8 = null,
    };

    /// Drain a Simple Query reply: RowDescription? + DataRow* +
    /// CommandComplete or ErrorResponse, then ReadyForQuery.
    fn readQueryReply(self: *TestClient, arena: std.mem.Allocator) !QueryReply {
        var rows: std.ArrayList(Row) = .empty;
        var command_tag: []const u8 = "";
        var col_count: usize = 0;
        var err_code: ?[]const u8 = null;
        var err_msg: ?[]const u8 = null;

        while (true) {
            const f = try pg_packet.readFrame(self.allocator, &self.reader.interface);
            defer self.allocator.free(f.payload);

            switch (f.type_byte) {
                'T' => {
                    var cursor: usize = 0;
                    col_count = try pg_packet.readU16(f.payload, &cursor);
                    var i: usize = 0;
                    while (i < col_count) : (i += 1) {
                        _ = try pg_packet.readCString(f.payload, &cursor);
                        cursor += 4 + 2 + 4 + 2 + 4 + 2;
                    }
                },
                'D' => {
                    var cursor: usize = 0;
                    const ncells = try pg_packet.readU16(f.payload, &cursor);
                    var cells: std.ArrayList(?[]const u8) = .empty;
                    var i: usize = 0;
                    while (i < ncells) : (i += 1) {
                        const len = try pg_packet.readU32(f.payload, &cursor);
                        if (len == 0xFFFFFFFF) {
                            try cells.append(arena, null);
                        } else {
                            const cell = f.payload[cursor .. cursor + len];
                            cursor += len;
                            try cells.append(arena, try arena.dupe(u8, cell));
                        }
                    }
                    try rows.append(arena, try cells.toOwnedSlice(arena));
                },
                'C' => {
                    var cursor: usize = 0;
                    const tag = try pg_packet.readCString(f.payload, &cursor);
                    command_tag = try arena.dupe(u8, tag);
                },
                'E' => {
                    var cursor: usize = 0;
                    while (cursor < f.payload.len and f.payload[cursor] != 0) {
                        const tag_byte = f.payload[cursor];
                        cursor += 1;
                        const val = try pg_packet.readCString(f.payload, &cursor);
                        switch (tag_byte) {
                            'C' => err_code = try arena.dupe(u8, val),
                            'M' => err_msg = try arena.dupe(u8, val),
                            else => {},
                        }
                    }
                },
                'Z' => break,
                else => {},
            }
        }

        return .{
            .rows = try rows.toOwnedSlice(arena),
            .command_tag = command_tag,
            .error_code = err_code,
            .error_message = err_msg,
        };
    }
};

const ServerCtx = struct {
    server: *thindb.PgServer,
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

test "pg wire: startup + SELECT 1" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var catalog = try openCatalog(allocator, io, tmp.dir);
    defer catalog.close();

    const port: u16 = test_port_base + 0;
    const addr = std.Io.net.IpAddress{ .ip4 = .{ .bytes = .{ 127, 0, 0, 1 }, .port = port } };
    var server = try thindb.servePg(allocator, io, catalog, addr);
    defer server.close();

    var sctx: ServerCtx = .{ .server = server, .n = 1 };
    const t = try std.Thread.spawn(.{}, ServerCtx.run, .{&sctx});
    defer t.join();

    var client = try TestClient.connect(allocator, io, addr);
    defer client.close();

    try client.completeStartup("postgres", null);

    try client.sendQuery("SELECT 1");
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const reply = try client.readQueryReply(arena.allocator());
    try std.testing.expect(reply.error_code == null);
    try std.testing.expectEqual(@as(usize, 1), reply.rows.len);
    try std.testing.expectEqualStrings("1", reply.rows[0][0].?);
    try std.testing.expectEqualStrings("SELECT 1", reply.command_tag);

    try client.sendTerminate();
    if (sctx.err) |e| return e;
}

test "pg wire: SELECT version() returns canned PostgreSQL banner" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var catalog = try openCatalog(allocator, io, tmp.dir);
    defer catalog.close();

    const port: u16 = test_port_base + 1;
    const addr = std.Io.net.IpAddress{ .ip4 = .{ .bytes = .{ 127, 0, 0, 1 }, .port = port } };
    var server = try thindb.servePg(allocator, io, catalog, addr);
    defer server.close();

    var sctx: ServerCtx = .{ .server = server, .n = 1 };
    const t = try std.Thread.spawn(.{}, ServerCtx.run, .{&sctx});
    defer t.join();

    var client = try TestClient.connect(allocator, io, addr);
    defer client.close();
    try client.completeStartup("postgres", null);

    try client.sendQuery("SELECT version()");
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const reply = try client.readQueryReply(arena.allocator());
    try std.testing.expectEqual(@as(usize, 1), reply.rows.len);
    try std.testing.expect(std.mem.indexOf(u8, reply.rows[0][0].?, "PostgreSQL") != null);
    try std.testing.expect(std.mem.indexOf(u8, reply.rows[0][0].?, "thinDB") != null);

    try client.sendTerminate();
    if (sctx.err) |e| return e;
}

test "pg wire: SELECT * FROM orders returns seeded rows" {
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
        .{ .id = @as(i64, 3), .qty = @as(i32, 30), .tag = "c" },
    });
    try tbl.flush();

    const port: u16 = test_port_base + 2;
    const addr = std.Io.net.IpAddress{ .ip4 = .{ .bytes = .{ 127, 0, 0, 1 }, .port = port } };
    var server = try thindb.servePg(allocator, io, catalog, addr);
    defer server.close();

    var sctx: ServerCtx = .{ .server = server, .n = 1 };
    const t = try std.Thread.spawn(.{}, ServerCtx.run, .{&sctx});
    defer t.join();

    var client = try TestClient.connect(allocator, io, addr);
    defer client.close();
    try client.completeStartup("postgres", "main");

    try client.sendQuery("SELECT * FROM orders");
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const reply = try client.readQueryReply(arena.allocator());
    try std.testing.expect(reply.error_code == null);
    try std.testing.expectEqual(@as(usize, 3), reply.rows.len);
    try std.testing.expectEqualStrings("1", reply.rows[0][0].?);
    try std.testing.expectEqualStrings("10", reply.rows[0][1].?);
    try std.testing.expectEqualStrings("a", reply.rows[0][2].?);
    try std.testing.expectEqualStrings("3", reply.rows[2][0].?);
    try std.testing.expectEqualStrings("c", reply.rows[2][2].?);
    try std.testing.expectEqualStrings("SELECT 3", reply.command_tag);

    try client.sendTerminate();
    if (sctx.err) |e| return e;
}

test "pg wire: query against missing table returns 42P01 error" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var catalog = try openCatalog(allocator, io, tmp.dir);
    defer catalog.close();

    const port: u16 = test_port_base + 3;
    const addr = std.Io.net.IpAddress{ .ip4 = .{ .bytes = .{ 127, 0, 0, 1 }, .port = port } };
    var server = try thindb.servePg(allocator, io, catalog, addr);
    defer server.close();

    var sctx: ServerCtx = .{ .server = server, .n = 1 };
    const t = try std.Thread.spawn(.{}, ServerCtx.run, .{&sctx});
    defer t.join();

    var client = try TestClient.connect(allocator, io, addr);
    defer client.close();
    try client.completeStartup("postgres", null);

    try client.sendQuery("SELECT * FROM does_not_exist");
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const reply = try client.readQueryReply(arena.allocator());
    try std.testing.expect(reply.error_code != null);
    try std.testing.expectEqualStrings("42P01", reply.error_code.?);

    try client.sendTerminate();
    if (sctx.err) |e| return e;
}

test "pg wire: CREATE DATABASE round-trips" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var catalog = try openCatalog(allocator, io, tmp.dir);
    defer catalog.close();

    const port: u16 = test_port_base + 4;
    const addr = std.Io.net.IpAddress{ .ip4 = .{ .bytes = .{ 127, 0, 0, 1 }, .port = port } };
    var server = try thindb.servePg(allocator, io, catalog, addr);
    defer server.close();

    var sctx: ServerCtx = .{ .server = server, .n = 1 };
    const t = try std.Thread.spawn(.{}, ServerCtx.run, .{&sctx});
    defer t.join();

    var client = try TestClient.connect(allocator, io, addr);
    defer client.close();
    try client.completeStartup("postgres", null);

    try client.sendQuery("CREATE DATABASE analytics");
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const reply = try client.readQueryReply(arena.allocator());
    try std.testing.expect(reply.error_code == null);
    try std.testing.expectEqualStrings("CREATE DATABASE", reply.command_tag);

    try client.sendQuery("SELECT datname FROM pg_catalog.pg_database ORDER BY 1");
    const reply2 = try client.readQueryReply(arena.allocator());
    try std.testing.expect(reply2.error_code == null);
    var saw_analytics = false;
    for (reply2.rows) |row| {
        if (row.len > 0 and row[0] != null and std.mem.eql(u8, row[0].?, "analytics")) saw_analytics = true;
    }
    try std.testing.expect(saw_analytics);

    try client.sendTerminate();
    if (sctx.err) |e| return e;
}

test "pg wire: SET search_path is silently accepted" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var catalog = try openCatalog(allocator, io, tmp.dir);
    defer catalog.close();

    const port: u16 = test_port_base + 5;
    const addr = std.Io.net.IpAddress{ .ip4 = .{ .bytes = .{ 127, 0, 0, 1 }, .port = port } };
    var server = try thindb.servePg(allocator, io, catalog, addr);
    defer server.close();

    var sctx: ServerCtx = .{ .server = server, .n = 1 };
    const t = try std.Thread.spawn(.{}, ServerCtx.run, .{&sctx});
    defer t.join();

    var client = try TestClient.connect(allocator, io, addr);
    defer client.close();
    try client.completeStartup("postgres", null);

    try client.sendQuery("SET search_path = analytics");
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const reply = try client.readQueryReply(arena.allocator());
    try std.testing.expect(reply.error_code == null);
    try std.testing.expectEqualStrings("SET", reply.command_tag);

    try client.sendTerminate();
    if (sctx.err) |e| return e;
}

test "pg wire: SSLRequest is denied with N + startup continues" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var catalog = try openCatalog(allocator, io, tmp.dir);
    defer catalog.close();

    const port: u16 = test_port_base + 6;
    const addr = std.Io.net.IpAddress{ .ip4 = .{ .bytes = .{ 127, 0, 0, 1 }, .port = port } };
    var server = try thindb.servePg(allocator, io, catalog, addr);
    defer server.close();

    var sctx: ServerCtx = .{ .server = server, .n = 1 };
    const t = try std.Thread.spawn(.{}, ServerCtx.run, .{&sctx});
    defer t.join();

    var client = try TestClient.connect(allocator, io, addr);
    defer client.close();

    try client.sendSslRequest();
    const ssl_reply = try client.readSslReply();
    try std.testing.expectEqual(@as(u8, 'N'), ssl_reply);

    try client.sendStartup("postgres", null);
    try client.drainUntilReady();

    try client.sendQuery("SELECT 1");
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const reply = try client.readQueryReply(arena.allocator());
    try std.testing.expect(reply.error_code == null);
    try std.testing.expectEqual(@as(usize, 1), reply.rows.len);

    try client.sendTerminate();
    if (sctx.err) |e| return e;
}

// ---------------------------------------------------------------------------
// psql CLI subprocess tests — skipped when the binary isn't installed.
// ---------------------------------------------------------------------------

fn psqlCliAvailable(allocator: std.mem.Allocator, io: std.Io) bool {
    const r = std.process.run(allocator, io, .{
        .argv = &.{ "psql", "--version" },
    }) catch return false;
    defer allocator.free(r.stdout);
    defer allocator.free(r.stderr);
    return switch (r.term) {
        .exited => |code| code == 0,
        else => false,
    };
}

fn runPsqlCli(
    allocator: std.mem.Allocator,
    io: std.Io,
    port: u16,
    db: []const u8,
    sql_text: []const u8,
) !std.process.RunResult {
    var port_buf: [16]u8 = undefined;
    const port_str = try std.fmt.bufPrint(&port_buf, "{d}", .{port});

    const args = [_][]const u8{
        "psql",
        "--host=127.0.0.1",
        try std.fmt.allocPrint(allocator, "--port={s}", .{port_str}),
        "--username=postgres",
        try std.fmt.allocPrint(allocator, "--dbname={s}", .{db}),
        "--no-psqlrc",
        "--no-password",
        "--tuples-only",
        "--quiet",
        try std.fmt.allocPrint(allocator, "--command={s}", .{sql_text}),
    };
    defer allocator.free(args[2]);
    defer allocator.free(args[4]);
    defer allocator.free(args[9]);

    return std.process.run(allocator, io, .{
        .argv = &args,
    });
}

test "psql CLI: SELECT 1 when psql is on PATH" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    if (!psqlCliAvailable(allocator, io)) {
        std.debug.print("psql CLI not available, skipping\n", .{});
        return;
    }

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var catalog = try openCatalog(allocator, io, tmp.dir);
    defer catalog.close();

    const port: u16 = test_port_base + 100;
    const addr = std.Io.net.IpAddress{ .ip4 = .{ .bytes = .{ 127, 0, 0, 1 }, .port = port } };
    var server = try thindb.servePg(allocator, io, catalog, addr);
    defer server.close();

    var sctx: ServerCtx = .{ .server = server, .n = 1 };
    const t = try std.Thread.spawn(.{}, ServerCtx.run, .{&sctx});
    defer t.join();

    const r = try runPsqlCli(allocator, io, port, "main", "SELECT 1");
    defer allocator.free(r.stdout);
    defer allocator.free(r.stderr);

    if (std.mem.indexOf(u8, r.stdout, "1") == null) {
        std.debug.print("psql stdout: {s}\npsql stderr: {s}\n", .{ r.stdout, r.stderr });
        return error.MissingExpectedOutput;
    }
    if (sctx.err) |e| return e;
}

test "psql CLI: \\l lists databases when psql is on PATH" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    if (!psqlCliAvailable(allocator, io)) {
        std.debug.print("psql CLI not available, skipping\n", .{});
        return;
    }

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var catalog = try openCatalog(allocator, io, tmp.dir);
    defer catalog.close();

    const port: u16 = test_port_base + 101;
    const addr = std.Io.net.IpAddress{ .ip4 = .{ .bytes = .{ 127, 0, 0, 1 }, .port = port } };
    var server = try thindb.servePg(allocator, io, catalog, addr);
    defer server.close();

    var sctx: ServerCtx = .{ .server = server, .n = 1 };
    const t = try std.Thread.spawn(.{}, ServerCtx.run, .{&sctx});
    defer t.join();

    const r = try runPsqlCli(allocator, io, port, "main", "SELECT datname FROM pg_catalog.pg_database ORDER BY 1");
    defer allocator.free(r.stdout);
    defer allocator.free(r.stderr);

    if (std.mem.indexOf(u8, r.stdout, "main") == null) {
        std.debug.print("psql stdout: {s}\npsql stderr: {s}\n", .{ r.stdout, r.stderr });
        return error.MissingExpectedOutput;
    }
    if (sctx.err) |e| return e;
}

test "psql CLI: SELECT * FROM seeded table when psql is on PATH" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    if (!psqlCliAvailable(allocator, io)) {
        std.debug.print("psql CLI not available, skipping\n", .{});
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

    const port: u16 = test_port_base + 102;
    const addr = std.Io.net.IpAddress{ .ip4 = .{ .bytes = .{ 127, 0, 0, 1 }, .port = port } };
    var server = try thindb.servePg(allocator, io, catalog, addr);
    defer server.close();

    var sctx: ServerCtx = .{ .server = server, .n = 1 };
    const t = try std.Thread.spawn(.{}, ServerCtx.run, .{&sctx});
    defer t.join();

    const r = try runPsqlCli(allocator, io, port, "main", "SELECT * FROM orders");
    defer allocator.free(r.stdout);
    defer allocator.free(r.stderr);

    if (std.mem.indexOf(u8, r.stdout, "alpha") == null or
        std.mem.indexOf(u8, r.stdout, "beta") == null)
    {
        std.debug.print("psql stdout: {s}\npsql stderr: {s}\n", .{ r.stdout, r.stderr });
        return error.MissingRowText;
    }
    if (sctx.err) |e| return e;
}
