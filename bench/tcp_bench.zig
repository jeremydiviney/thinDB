//! TCP transport benchmarks — measure the wire-path cost vs the
//! in-process equivalents in main.zig. Spawns a server in a background
//! thread, runs N operations from the main thread, tears everything
//! down cleanly.
//!
//! All numbers should be compared to the in-process baseline from
//! main.zig at the same row count.

const std = @import("std");
const thindb = @import("thindb");
const common = @import("common.zig");

const Allocator = common.Allocator;
const Io = common.Io;
const schema = common.schema;
const options = common.options;
const buildRows = common.buildRows;
const elapsedNs = common.elapsedNs;
const freshDir = common.freshDir;
const report = common.report;

// Port base picked outside the integration-test range (those run at
// 27543+). Each bench bumps the port so concurrent runs (or a left-over
// socket from a crashed run) don't collide.
const port_base: u16 = 28100;

const ServerCtx = struct {
    server: *thindb.TcpServer,
    n_connections: usize,
    err: ?anyerror = null,

    fn run(self: *@This()) void {
        var i: usize = 0;
        while (i < self.n_connections) : (i += 1) {
            self.server.acceptOne() catch |e| {
                self.err = e;
                return;
            };
        }
    }
};

pub fn benchTcpScan(allocator: Allocator, io: Io, n_rows: usize) !void {
    var dir = try freshDir(io, ".bench-data/tcp_scan");
    defer dir.close(io);

    const addr = std.Io.net.IpAddress{ .ip4 = .{
        .bytes = .{ 127, 0, 0, 1 },
        .port = port_base + 0,
    } };
    var server = try thindb.serveTcp(allocator, io, dir, addr, .{});
    defer server.close();

    // Seed via the in-process Database — we want to isolate scan-over-TCP
    // cost, not measure insert too.
    const t = try server.db.table("t", schema, options);
    const rows = try buildRows(allocator, n_rows);
    defer allocator.free(rows);
    try t.insert(rows);
    try t.flush();

    // One connection: the scan.
    var sctx: ServerCtx = .{ .server = server, .n_connections = 1 };
    const server_thread = try std.Thread.spawn(.{}, ServerCtx.run, .{&sctx});
    defer server_thread.join();

    var conn = try thindb.connect(allocator, io, addr);
    defer conn.close();

    var checksum: i64 = 0;
    var scanned: usize = 0;

    const t0 = Io.Clock.awake.now(io);
    var q = try conn.scan("t");
    defer q.deinit();
    while (try q.next()) |batch| {
        scanned += batch.row_count;
        const ids = batch.values[0].data.bigint;
        for (ids) |v| checksum +%= v;
    }
    const elapsed = elapsedNs(io, t0);

    std.mem.doNotOptimizeAway(&checksum);
    if (scanned != n_rows) return error.RowCountMismatch;

    try report("tcp scan", n_rows, elapsed, null);

    if (sctx.err) |e| return e;
}

pub fn benchTcpInsert(allocator: Allocator, io: Io, n_rows: usize) !void {
    var dir = try freshDir(io, ".bench-data/tcp_insert");
    defer dir.close(io);

    const addr = std.Io.net.IpAddress{ .ip4 = .{
        .bytes = .{ 127, 0, 0, 1 },
        .port = port_base + 1,
    } };
    var server = try thindb.serveTcp(allocator, io, dir, addr, .{
        // Disable auto-flush so the bench measures wire+memtable
        // append, parallel to benchInsertMemtable in main.zig.
        .auto_flush_rows = std.math.maxInt(u64),
        .auto_flush_bytes = std.math.maxInt(usize),
        .auto_flush_secs = 0,
    });
    defer server.close();

    _ = try server.db.table("t", schema, options);
    const rows = try buildRows(allocator, n_rows);
    defer allocator.free(rows);

    // Two connections: the insert, then a flush.
    var sctx: ServerCtx = .{ .server = server, .n_connections = 2 };
    const server_thread = try std.Thread.spawn(.{}, ServerCtx.run, .{&sctx});
    defer server_thread.join();

    var conn = try thindb.connect(allocator, io, addr);
    defer conn.close();

    const t0 = Io.Clock.awake.now(io);
    try conn.insert("t", rows);
    const elapsed = elapsedNs(io, t0);

    // Manual flush so the bench's output lands on disk (mirrors the
    // pattern in main.zig's bench bodies).
    try conn.flush("t");

    try report("tcp insert (memtable)", n_rows, elapsed, null);

    if (sctx.err) |e| return e;
}

pub fn benchTcpInsertAndFlush(allocator: Allocator, io: Io, n_rows: usize) !void {
    var dir = try freshDir(io, ".bench-data/tcp_insert_and_flush");
    defer dir.close(io);

    const addr = std.Io.net.IpAddress{ .ip4 = .{
        .bytes = .{ 127, 0, 0, 1 },
        .port = port_base + 2,
    } };
    var server = try thindb.serveTcp(allocator, io, dir, addr, .{});
    defer server.close();

    _ = try server.db.table("t", schema, options);
    const rows = try buildRows(allocator, n_rows);
    defer allocator.free(rows);

    var sctx: ServerCtx = .{ .server = server, .n_connections = 2 };
    const server_thread = try std.Thread.spawn(.{}, ServerCtx.run, .{&sctx});
    defer server_thread.join();

    var conn = try thindb.connect(allocator, io, addr);
    defer conn.close();

    const t0 = Io.Clock.awake.now(io);
    try conn.insert("t", rows);
    try conn.flush("t");
    const elapsed = elapsedNs(io, t0);

    try report("tcp insert + flush", n_rows, elapsed, null);

    if (sctx.err) |e| return e;
}
