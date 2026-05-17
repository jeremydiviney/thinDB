//! TCP transport integration tests. Spawn a server in a background
//! thread, connect from the main thread, run a query end-to-end,
//! tear everything down cleanly.
//!
//! These exercise the real socket layer (bind, accept, connect, read,
//! write). Slower than the in-process tests; we keep them limited to
//! a handful of round-trips that prove the wire path works.

const std = @import("std");
const thindb = @import("thindb");

const schema_v1 = thindb.Schema{
    .columns = &.{
        .{ .name = "id", .type = .bigint },
        .{ .name = "qty", .type = .int },
        .{ .name = "active", .type = .boolean },
        .{ .name = "tag", .type = .string },
    },
    .order_key = &.{"id"},
    .unique = true,
};

const order_key = [_][]const u8{"id"};
const opts_v1 = thindb.TableOptions{
    .order_key = &order_key,
    .unique = true,
    .row_group_size = 4,
};

/// Different per test so concurrent runs don't collide. High port in the
/// "transient" range; unlikely to clash with anything else on dev boxes.
const test_port_base: u16 = 27543;

// Spawn a server on an ephemeral port, seed it with rows, run one
// scan over TCP, verify the row count comes back. The simplest
// possible end-to-end test of the socket path.
test "tcp: scan round-trips a small batch over a real socket" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    // Server side: open the database via serveTcp, seed data via the
    // in-process API (no admin RPCs yet).
    const port: u16 = test_port_base + 0;
    const listen_addr = std.Io.net.IpAddress{ .ip4 = .{
        .bytes = .{ 127, 0, 0, 1 },
        .port = port,
    } };
    var server = try thindb.serveTcp(allocator, io, tmp.dir, listen_addr, .{});
    defer server.close();

    // Seed via the underlying Database (until createTable RPC lands).
    const t = try server.db.table("orders", schema_v1, opts_v1);
    try t.insert(&.{
        .{ .id = @as(i64, 1), .qty = @as(i32, 10), .active = true, .tag = "a" },
        .{ .id = @as(i64, 2), .qty = @as(i32, 20), .active = true, .tag = "b" },
        .{ .id = @as(i64, 3), .qty = @as(i32, 30), .active = true, .tag = "c" },
    });
    try t.flush();

    // Spawn server thread that handles one accept.
    const ServerCtx = struct {
        server: *thindb.TcpServer,
        err: ?anyerror = null,
        fn run(self: *@This()) void {
            self.server.acceptOne() catch |e| {
                self.err = e;
            };
        }
    };
    var sctx: ServerCtx = .{ .server = server };
    const server_thread = try std.Thread.spawn(.{}, ServerCtx.run, .{&sctx});
    defer server_thread.join();

    // Client side: connect, run a scan, read all batches.
    var conn = try thindb.connect(allocator, io, listen_addr);
    defer conn.close();

    var q = try conn.scan("orders");
    defer q.deinit();

    var total: usize = 0;
    while (try q.next()) |batch| total += batch.row_count;
    try std.testing.expectEqual(@as(usize, 3), total);

    // server_thread.join() runs via defer.
    if (sctx.err) |e| return e;
}

// Admin RPC: create a table over TCP, then seed + flush + scan it via
// the existing in-process Database (server-side). Verifies that the
// wire schema is faithful enough that downstream operations work on
// the table just like they would after a local createTable.
test "tcp: createTable round-trips a full schema (columns + order_key + unique)" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const port: u16 = test_port_base + 4;
    const listen_addr = std.Io.net.IpAddress{ .ip4 = .{
        .bytes = .{ 127, 0, 0, 1 },
        .port = port,
    } };
    var server = try thindb.serveTcp(allocator, io, tmp.dir, listen_addr, .{});
    defer server.close();

    const ServerCtx = struct {
        server: *thindb.TcpServer,
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
    // Two connections: createTable, then a verification scan.
    var sctx: ServerCtx = .{ .server = server, .n = 2 };
    const server_thread = try std.Thread.spawn(.{}, ServerCtx.run, .{&sctx});
    defer server_thread.join();

    var conn = try thindb.connect(allocator, io, listen_addr);
    defer conn.close();

    try conn.createTable("products", schema_v1, opts_v1);

    // The server now has the table — seed it via the in-process Database
    // pointer (insert RPC lands next), then verify by scanning over TCP.
    const t = server.db.tables.get("products") orelse unreachable;
    try t.insert(&.{
        .{ .id = @as(i64, 1), .qty = @as(i32, 10), .active = true, .tag = "a" },
        .{ .id = @as(i64, 2), .qty = @as(i32, 20), .active = true, .tag = "b" },
    });
    try t.flush();

    var q = try conn.scan("products");
    defer q.deinit();
    var total: usize = 0;
    while (try q.next()) |batch| total += batch.row_count;
    try std.testing.expectEqual(@as(usize, 2), total);

    if (sctx.err) |e| return e;
}

// Write RPC: insert rows over TCP, then read them back over TCP.
// Round-trips through the columnar wire batch format both directions.
test "tcp: insert rows + read back via scan, all over the socket" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const port: u16 = test_port_base + 6;
    const listen_addr = std.Io.net.IpAddress{ .ip4 = .{
        .bytes = .{ 127, 0, 0, 1 },
        .port = port,
    } };
    var server = try thindb.serveTcp(allocator, io, tmp.dir, listen_addr, .{});
    defer server.close();

    // Pre-create the table in-process (createTable RPC is tested separately).
    _ = try server.db.table("orders", schema_v1, opts_v1);

    const ServerCtx = struct {
        server: *thindb.TcpServer,
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
    // Three connections: insert, flush, scan.
    var sctx: ServerCtx = .{ .server = server, .n = 3 };
    const server_thread = try std.Thread.spawn(.{}, ServerCtx.run, .{&sctx});
    defer server_thread.join();

    var conn = try thindb.connect(allocator, io, listen_addr);
    defer conn.close();

    const Row = struct { id: i64, qty: i32, active: bool, tag: []const u8 };
    try conn.insert("orders", &[_]Row{
        .{ .id = 1, .qty = 10, .active = true,  .tag = "a" },
        .{ .id = 2, .qty = 20, .active = false, .tag = "bb" },
        .{ .id = 3, .qty = 30, .active = true,  .tag = "ccc" },
    });
    try conn.flush("orders");

    var q = try conn.scan("orders");
    defer q.deinit();

    var ids: std.ArrayList(i64) = .empty;
    defer ids.deinit(allocator);
    var qtys: std.ArrayList(i32) = .empty;
    defer qtys.deinit(allocator);
    var tags_concat: std.ArrayList(u8) = .empty;
    defer tags_concat.deinit(allocator);

    while (try q.next()) |batch| {
        try ids.appendSlice(allocator, batch.values[0].data.bigint);
        try qtys.appendSlice(allocator, batch.values[1].data.int);
        for (batch.values[3].data.string.offsets[0..batch.row_count], batch.values[3].data.string.offsets[1 .. batch.row_count + 1]) |start, end| {
            try tags_concat.appendSlice(allocator, batch.values[3].data.string.bytes[start..end]);
        }
    }
    try std.testing.expectEqualSlices(i64, &[_]i64{ 1, 2, 3 }, ids.items);
    try std.testing.expectEqualSlices(i32, &[_]i32{ 10, 20, 30 }, qtys.items);
    try std.testing.expectEqualStrings("abbccc", tags_concat.items);

    if (sctx.err) |e| return e;
}

// Write RPC: nullable fields in an insert. The client encoder unwraps
// `?T` field types, emits `nullable=1` in the column header, and
// builds a validity bitmap alongside the data. Round-trip through a
// scan should preserve the null/non-null pattern.
test "tcp: insert with nullable fields preserves nulls through scan" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const port: u16 = test_port_base + 8;
    const listen_addr = std.Io.net.IpAddress{ .ip4 = .{
        .bytes = .{ 127, 0, 0, 1 },
        .port = port,
    } };
    var server = try thindb.serveTcp(allocator, io, tmp.dir, listen_addr, .{});
    defer server.close();

    const nullable_schema = thindb.Schema{
        .columns = &.{
            .{ .name = "id", .type = .bigint },
            .{ .name = "qty", .type = .int, .nullable = true },
            .{ .name = "note", .type = .string, .nullable = true },
        },
        .order_key = &.{"id"},
        .unique = true,
    };
    const ok = [_][]const u8{"id"};
    _ = try server.db.table("events", nullable_schema, .{
        .order_key = &ok,
        .unique = true,
        .row_group_size = 4,
    });

    const ServerCtx = struct {
        server: *thindb.TcpServer,
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
    var sctx: ServerCtx = .{ .server = server, .n = 3 };
    const server_thread = try std.Thread.spawn(.{}, ServerCtx.run, .{&sctx});
    defer server_thread.join();

    var conn = try thindb.connect(allocator, io, listen_addr);
    defer conn.close();

    const Row = struct { id: i64, qty: ?i32, note: ?[]const u8 };
    try conn.insert("events", &[_]Row{
        .{ .id = 1, .qty = 10,   .note = "hello" },
        .{ .id = 2, .qty = null, .note = "world" },
        .{ .id = 3, .qty = 30,   .note = null },
        .{ .id = 4, .qty = null, .note = null },
    });
    try conn.flush("events");

    var q = try conn.scan("events");
    defer q.deinit();

    var qty_valid: std.ArrayList(bool) = .empty;
    defer qty_valid.deinit(allocator);
    var note_valid: std.ArrayList(bool) = .empty;
    defer note_valid.deinit(allocator);

    while (try q.next()) |batch| {
        const qty_idx = batch.columnIndex("qty").?;
        const note_idx = batch.columnIndex("note").?;
        for (0..batch.row_count) |i| {
            try qty_valid.append(allocator, batch.values[qty_idx].isValid(i));
            try note_valid.append(allocator, batch.values[note_idx].isValid(i));
        }
    }
    try std.testing.expectEqualSlices(bool, &[_]bool{ true, false, true, false }, qty_valid.items);
    try std.testing.expectEqualSlices(bool, &[_]bool{ true, true, false, false }, note_valid.items);

    if (sctx.err) |e| return e;
}

// Write RPC: tuple-shape insert (`&.{ .{...}, .{...} }`). Each tuple
// element is its own anonymous struct type, so the encoder has to walk
// fields-per-element rather than coerce to a common row type.
test "tcp: insert accepts the &.{ ... } tuple shape over the wire" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const port: u16 = test_port_base + 7;
    const listen_addr = std.Io.net.IpAddress{ .ip4 = .{
        .bytes = .{ 127, 0, 0, 1 },
        .port = port,
    } };
    var server = try thindb.serveTcp(allocator, io, tmp.dir, listen_addr, .{});
    defer server.close();

    _ = try server.db.table("orders", schema_v1, opts_v1);

    const ServerCtx = struct {
        server: *thindb.TcpServer,
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
    var sctx: ServerCtx = .{ .server = server, .n = 3 };
    const server_thread = try std.Thread.spawn(.{}, ServerCtx.run, .{&sctx});
    defer server_thread.join();

    var conn = try thindb.connect(allocator, io, listen_addr);
    defer conn.close();

    try conn.insert("orders", &.{
        .{ .id = @as(i64, 100), .qty = @as(i32, 1), .active = true,  .tag = @as([]const u8, "x") },
        .{ .id = @as(i64, 200), .qty = @as(i32, 2), .active = false, .tag = @as([]const u8, "y") },
    });
    try conn.flush("orders");

    var q = try conn.scan("orders");
    defer q.deinit();
    var total: usize = 0;
    while (try q.next()) |batch| total += batch.row_count;
    try std.testing.expectEqual(@as(usize, 2), total);

    if (sctx.err) |e| return e;
}

// Admin RPC: alter a table over TCP. Add a column, then verify a scan
// over TCP returns the new column in its schema with the supplied
// default for the existing rows.
test "tcp: alterTable add column populates default + appears in scan output" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const port: u16 = test_port_base + 5;
    const listen_addr = std.Io.net.IpAddress{ .ip4 = .{
        .bytes = .{ 127, 0, 0, 1 },
        .port = port,
    } };
    var server = try thindb.serveTcp(allocator, io, tmp.dir, listen_addr, .{});
    defer server.close();

    // Seed in-process so we have rows to verify the default-fill against.
    const t = try server.db.table("orders", schema_v1, opts_v1);
    try t.insert(&.{
        .{ .id = @as(i64, 1), .qty = @as(i32, 10), .active = true, .tag = "a" },
        .{ .id = @as(i64, 2), .qty = @as(i32, 20), .active = true, .tag = "b" },
    });
    try t.flush();

    const ServerCtx = struct {
        server: *thindb.TcpServer,
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
    var sctx: ServerCtx = .{ .server = server, .n = 2 };
    const server_thread = try std.Thread.spawn(.{}, ServerCtx.run, .{&sctx});
    defer server_thread.join();

    var conn = try thindb.connect(allocator, io, listen_addr);
    defer conn.close();

    const ops = [_]thindb.AlterOp{
        .{ .add = .{
            .name = "rank",
            .type = .int,
            .nullable = false,
            .default = .{ .int = 99 },
        } },
    };
    try conn.alterTable("orders", &ops);

    var q = try conn.scan("orders");
    defer q.deinit();

    var saw_rank = false;
    var rank_values: std.ArrayList(i32) = .empty;
    defer rank_values.deinit(allocator);
    while (try q.next()) |batch| {
        for (batch.schema, 0..) |col, i| {
            if (std.mem.eql(u8, col.name, "rank")) {
                saw_rank = true;
                try rank_values.appendSlice(allocator, batch.values[i].data.int[0..batch.row_count]);
            }
        }
    }
    try std.testing.expect(saw_rank);
    try std.testing.expectEqualSlices(i32, &[_]i32{ 99, 99 }, rank_values.items);

    if (sctx.err) |e| return e;
}

// Auth: server requires a shared-secret token. Client must present a
// matching token in a `req_auth` frame pipelined before the real
// request, or the server replies `auth_failed` and closes.
test "tcp: auth accepts matching token, rejects missing + wrong token" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const port: u16 = test_port_base + 11;
    const listen_addr = std.Io.net.IpAddress{ .ip4 = .{
        .bytes = .{ 127, 0, 0, 1 },
        .port = port,
    } };
    var server = try thindb.serveTcp(allocator, io, tmp.dir, listen_addr, .{});
    defer server.close();
    server.auth_token = "s3cret-token-do-not-reuse";

    _ = try server.db.table("t", schema_v1, opts_v1);

    const ServerCtx = struct {
        server: *thindb.TcpServer,
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
    // 3 attempts: no token (reject), wrong token (reject), right token (accept).
    var sctx: ServerCtx = .{ .server = server, .n = 3 };
    const server_thread = try std.Thread.spawn(.{}, ServerCtx.run, .{&sctx});
    defer server_thread.join();

    // Attempt 1: no auth_token on the client → server rejects.
    {
        var conn = try thindb.connect(allocator, io, listen_addr);
        defer conn.close();
        var q = try conn.scan("t");
        defer q.deinit();
        try std.testing.expectError(thindb.net.Error.AuthFailed, q.next());
    }

    // Attempt 2: wrong token → AuthFailed.
    {
        var conn = try thindb.connect(allocator, io, listen_addr);
        defer conn.close();
        conn.auth_token = "wrong";
        var q = try conn.scan("t");
        defer q.deinit();
        try std.testing.expectError(thindb.net.Error.AuthFailed, q.next());
    }

    // Attempt 3: correct token → scan works (empty table, no rows).
    {
        var conn = try thindb.connect(allocator, io, listen_addr);
        defer conn.close();
        conn.auth_token = "s3cret-token-do-not-reuse";
        var q = try conn.scan("t");
        defer q.deinit();
        var total: usize = 0;
        while (try q.next()) |b| total += b.row_count;
        try std.testing.expectEqual(@as(usize, 0), total);
    }

    if (sctx.err) |e| return e;
}

// Compression: flip the opt-in flag on both ends and verify a big
// scan still round-trips correctly. The wire frames cross the
// threshold and get zstd-compressed; readFramePayload decompresses
// on the way back in.
test "tcp: compression round-trips a large scan when both sides opt in" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const port: u16 = test_port_base + 10;
    const listen_addr = std.Io.net.IpAddress{ .ip4 = .{
        .bytes = .{ 127, 0, 0, 1 },
        .port = port,
    } };
    var server = try thindb.serveTcp(allocator, io, tmp.dir, listen_addr, .{});
    defer server.close();
    server.compress_writes = true; // opt-in on the server side

    // Seed enough rows that the resulting batch payload exceeds the
    // compression threshold (4 KB). 2_000 rows × ~20 bytes/row =
    // ~40 KB raw — comfortably past the threshold.
    const t = try server.db.table("big", schema_v1, opts_v1);
    var i: i64 = 0;
    while (i < 2_000) : (i += 1) {
        try t.insert(&.{.{ .id = i, .qty = @as(i32, @intCast(i)), .active = true, .tag = "compressed" }});
    }
    try t.flush();

    const ServerCtx = struct {
        server: *thindb.TcpServer,
        n: usize,
        err: ?anyerror = null,
        fn run(self: *@This()) void {
            var k: usize = 0;
            while (k < self.n) : (k += 1) {
                self.server.acceptOne() catch |e| {
                    self.err = e;
                    return;
                };
            }
        }
    };
    var sctx: ServerCtx = .{ .server = server, .n = 1 };
    const server_thread = try std.Thread.spawn(.{}, ServerCtx.run, .{&sctx});
    defer server_thread.join();

    var conn = try thindb.connect(allocator, io, listen_addr);
    defer conn.close();
    conn.compress_writes = true; // opt-in on the client side too

    var q = try conn.scan("big");
    defer q.deinit();

    var total: usize = 0;
    var id_sum: i64 = 0;
    while (try q.next()) |batch| {
        total += batch.row_count;
        for (batch.values[0].data.bigint) |v| id_sum += v;
    }
    try std.testing.expectEqual(@as(usize, 2_000), total);
    // 0+1+...+1999 = 1999*2000/2 = 1_999_000
    try std.testing.expectEqual(@as(i64, 1_999_000), id_sum);

    if (sctx.err) |e| return e;
}

// Typed errors round-trip: trigger several distinct server-side
// failures and verify each one surfaces as the expected typed
// variant of `thindb.net.Error` on the client (not the catch-all
// RemoteError). Locks in the wire error contract.
test "tcp: resp_error carries a typed code, client maps to typed Error" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const port: u16 = test_port_base + 9;
    const listen_addr = std.Io.net.IpAddress{ .ip4 = .{
        .bytes = .{ 127, 0, 0, 1 },
        .port = port,
    } };
    var server = try thindb.serveTcp(allocator, io, tmp.dir, listen_addr, .{});
    defer server.close();

    // Seed one table so we can trigger TableAlreadyExists too.
    _ = try server.db.table("existing", schema_v1, opts_v1);

    const ServerCtx = struct {
        server: *thindb.TcpServer,
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
    // 3 connections: drop-missing, rename-collision, scan-missing.
    var sctx: ServerCtx = .{ .server = server, .n = 3 };
    const server_thread = try std.Thread.spawn(.{}, ServerCtx.run, .{&sctx});
    defer server_thread.join();

    var conn = try thindb.connect(allocator, io, listen_addr);
    defer conn.close();

    try std.testing.expectError(
        thindb.net.Error.TableNotFound,
        conn.dropTable("does-not-exist"),
    );

    try std.testing.expectError(
        thindb.net.Error.TableAlreadyExists,
        conn.renameTable("existing", "existing"),
    );

    var q = try conn.scan("nope");
    defer q.deinit();
    try std.testing.expectError(thindb.net.Error.TableNotFound, q.next());

    if (sctx.err) |e| return e;
}

// Admin RPC: drop a table over TCP. Verify the on-disk directory is
// gone AND that a follow-up scan fails. Server runs two requests in
// a row (drop, then a fresh scan), so we need two acceptOne() calls.
test "tcp: dropTable removes the table and frees its directory" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const port: u16 = test_port_base + 2;
    const listen_addr = std.Io.net.IpAddress{ .ip4 = .{
        .bytes = .{ 127, 0, 0, 1 },
        .port = port,
    } };
    var server = try thindb.serveTcp(allocator, io, tmp.dir, listen_addr, .{});
    defer server.close();

    const t = try server.db.table("victims", schema_v1, opts_v1);
    try t.insert(&.{.{ .id = @as(i64, 1), .qty = @as(i32, 5), .active = true, .tag = "x" }});
    try t.flush();

    // Server loop that handles N connections then stops.
    const ServerCtx = struct {
        server: *thindb.TcpServer,
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
    // 2 connections expected: dropTable + a probe scan that should fail.
    var sctx: ServerCtx = .{ .server = server, .n = 2 };
    const server_thread = try std.Thread.spawn(.{}, ServerCtx.run, .{&sctx});
    defer server_thread.join();

    var conn = try thindb.connect(allocator, io, listen_addr);
    defer conn.close();

    try conn.dropTable("victims");

    // Disk: directory should be gone.
    const probe = tmp.dir.openDir(io, "victims", .{});
    try std.testing.expectError(error.FileNotFound, probe);

    // Re-scan: server replies with resp_error carrying the typed
    // TableNotFound code; client maps it back to the local Error variant.
    var q = try conn.scan("victims");
    defer q.deinit();
    try std.testing.expectError(thindb.net.Error.TableNotFound, q.next());

    if (sctx.err) |e| return e;
}

// Admin RPC: delete rows over TCP using a leaf predicate. Verify the
// returned count + that a follow-up scan only returns the surviving rows.
test "tcp: delete with leaf predicate removes matching rows + returns count" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const port: u16 = test_port_base + 3;
    const listen_addr = std.Io.net.IpAddress{ .ip4 = .{
        .bytes = .{ 127, 0, 0, 1 },
        .port = port,
    } };
    var server = try thindb.serveTcp(allocator, io, tmp.dir, listen_addr, .{});
    defer server.close();

    const t = try server.db.table("orders", schema_v1, opts_v1);
    try t.insert(&.{
        .{ .id = @as(i64, 1), .qty = @as(i32, 10), .active = true,  .tag = "a" },
        .{ .id = @as(i64, 2), .qty = @as(i32, 20), .active = false, .tag = "b" },
        .{ .id = @as(i64, 3), .qty = @as(i32, 30), .active = false, .tag = "c" },
        .{ .id = @as(i64, 4), .qty = @as(i32, 40), .active = true,  .tag = "d" },
    });
    try t.flush();

    // Two connections: delete, then a scan to verify.
    const ServerCtx = struct {
        server: *thindb.TcpServer,
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
    var sctx: ServerCtx = .{ .server = server, .n = 2 };
    const server_thread = try std.Thread.spawn(.{}, ServerCtx.run, .{&sctx});
    defer server_thread.join();

    var conn = try thindb.connect(allocator, io, listen_addr);
    defer conn.close();

    const deleted = try conn.delete(
        "orders",
        thindb.leafExpr("active", .eq, .{ .boolean = false }),
    );
    try std.testing.expectEqual(@as(usize, 2), deleted);

    var q = try conn.scan("orders");
    defer q.deinit();
    var ids: std.ArrayList(i64) = .empty;
    defer ids.deinit(allocator);
    while (try q.next()) |batch| try ids.appendSlice(allocator, batch.values[0].data.bigint);
    try std.testing.expectEqualSlices(i64, &[_]i64{ 1, 4 }, ids.items);

    if (sctx.err) |e| return e;
}

test "tcp: query with where + limit returns the right rows" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const port: u16 = test_port_base + 1;
    const listen_addr = std.Io.net.IpAddress{ .ip4 = .{
        .bytes = .{ 127, 0, 0, 1 },
        .port = port,
    } };
    var server = try thindb.serveTcp(allocator, io, tmp.dir, listen_addr, .{});
    defer server.close();

    const t = try server.db.table("orders", schema_v1, opts_v1);
    try t.insert(&.{
        .{ .id = @as(i64, 1), .qty = @as(i32, 10),  .active = true,  .tag = "a" },
        .{ .id = @as(i64, 2), .qty = @as(i32, 100), .active = true,  .tag = "b" },
        .{ .id = @as(i64, 3), .qty = @as(i32, 200), .active = false, .tag = "c" },
        .{ .id = @as(i64, 4), .qty = @as(i32, 300), .active = true,  .tag = "d" },
    });
    try t.flush();

    const ServerCtx = struct {
        server: *thindb.TcpServer,
        err: ?anyerror = null,
        fn run(self: *@This()) void {
            self.server.acceptOne() catch |e| {
                self.err = e;
            };
        }
    };
    var sctx: ServerCtx = .{ .server = server };
    const server_thread = try std.Thread.spawn(.{}, ServerCtx.run, .{&sctx});
    defer server_thread.join();

    var conn = try thindb.connect(allocator, io, listen_addr);
    defer conn.close();

    var base = try conn.scan("orders");
    var filtered = try base.where(thindb.leafExpr("active", .eq, .{ .boolean = true }));
    var q = try filtered.limit(10);
    defer q.deinit();

    var ids: std.ArrayList(i64) = .empty;
    defer ids.deinit(allocator);
    while (try q.next()) |batch| try ids.appendSlice(allocator, batch.values[0].data.bigint);

    try std.testing.expectEqualSlices(i64, &[_]i64{ 1, 2, 4 }, ids.items);

    if (sctx.err) |e| return e;
}
