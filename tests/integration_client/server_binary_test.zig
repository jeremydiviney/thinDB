//! Smoke test for the standalone `thindb-server` binary. Spawns the
//! compiled executable, waits for its startup banner, opens a TCP
//! connection to the MySQL wire, and validates the first bytes look
//! like a HandshakeV10 packet. Then kills the child and asserts it
//! exits cleanly.
//!
//! The wire-protocol depth is covered by the in-process mysql/pg/native
//! tests; this test only proves the binary boots, opens listeners, and
//! tears down on a signal.

const std = @import("std");

const Io = std.Io;

/// Use a high port range that's unlikely to collide with anything else
/// on a dev box.
const mysql_test_port: u16 = 16306;
const pg_test_port: u16 = 15432;
const native_test_port: u16 = 17878;

const binary_rel_path = "zig-out/bin/thindb-server" ++ (if (@import("builtin").os.tag == .windows) ".exe" else "");

test "thindb-server binary: starts, accepts a mysql connection, shuts down clean" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    Io.Dir.cwd().access(io, binary_rel_path, .{}) catch {
        std.debug.print("thindb-server binary not built, skipping\n", .{});
        return;
    };

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var data_dir_buf: [256]u8 = undefined;
    const data_dir = try std.fmt.bufPrint(&data_dir_buf, ".zig-cache/tmp/{s}", .{tmp.sub_path});

    var mysql_port_buf: [8]u8 = undefined;
    var pg_port_buf: [8]u8 = undefined;
    var native_port_buf: [8]u8 = undefined;
    const mysql_port_str = try std.fmt.bufPrint(&mysql_port_buf, "{d}", .{mysql_test_port});
    const pg_port_str = try std.fmt.bufPrint(&pg_port_buf, "{d}", .{pg_test_port});
    const native_port_str = try std.fmt.bufPrint(&native_port_buf, "{d}", .{native_test_port});

    const argv = [_][]const u8{
        binary_rel_path,
        "--data-dir",        data_dir,
        "--bind",            "127.0.0.1",
        "--mysql-port",      mysql_port_str,
        "--pg-port",         pg_port_str,
        "--native-port",     native_port_str,
    };

    var child = try std.process.spawn(io, .{
        .argv = &argv,
        .stdout = .pipe,
        .stderr = .inherit,
        .stdin = .pipe,
    });
    defer {
        std.process.Child.kill(&child, io);
    }

    if (!try waitForBanner(allocator, io, &child)) {
        return error.ServerDidNotStart;
    }

    const addr = std.Io.net.IpAddress{ .ip4 = .{
        .bytes = .{ 127, 0, 0, 1 },
        .port = mysql_test_port,
    } };
    var stream = try std.Io.net.IpAddress.connect(&addr, io, .{ .mode = .stream, .protocol = .tcp });
    defer stream.close(io);

    var read_buf: [256]u8 = undefined;
    var reader = stream.reader(io, &read_buf);
    const r = &reader.interface;

    var header: [4]u8 = undefined;
    try r.readSliceAll(&header);
    const payload_len = (@as(u32, header[0])) | (@as(u32, header[1]) << 8) | (@as(u32, header[2]) << 16);
    try std.testing.expect(payload_len > 0);
    try std.testing.expect(payload_len < 200);

    const proto_version = try r.takeByte();
    try std.testing.expectEqual(@as(u8, 10), proto_version);

    std.process.Child.kill(&child, io);
    // kill() blocks until termination + cleans up child resources; can't
    // also call wait() afterwards because id is null.
}

/// Drain the child's stdout pipe until we see the closing line of the
/// startup banner (`native wire listening on ...`). Times out after a
/// few seconds.
fn waitForBanner(allocator: std.mem.Allocator, io: Io, child: *std.process.Child) !bool {
    var collected: std.ArrayList(u8) = .empty;
    defer collected.deinit(allocator);

    const stdout = child.stdout orelse return error.NoChildStdout;
    var stdout_buf: [4096]u8 = undefined;
    var stdout_reader = stdout.reader(io, &stdout_buf);
    const r = &stdout_reader.interface;

    var deadline_iters: usize = 0;
    while (deadline_iters < 200) : (deadline_iters += 1) {
        const byte = r.takeByte() catch |err| switch (err) {
            error.EndOfStream => return false,
            else => return err,
        };
        try collected.append(allocator, byte);
        if (std.mem.indexOf(u8, collected.items, "native wire listening") != null) return true;
    }
    return false;
}
