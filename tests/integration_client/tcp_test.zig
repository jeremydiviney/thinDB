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

    // Re-scan: server should reject with RemoteError (TableNotFound surfaces
    // through the error string but the client just sees RemoteError).
    var q = try conn.scan("victims");
    defer q.deinit();
    try std.testing.expectError(thindb.net.Error.RemoteError, q.next());

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
