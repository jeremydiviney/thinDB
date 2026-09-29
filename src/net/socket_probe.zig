//! Socket probes for the connection reaper (#164): questions the reaper
//! asks about another thread's socket without reading from it or
//! disturbing a read that thread has posted, and the abort it applies
//! to a connection whose transfer stalled.
//!
//! Zig 0.16's Io.net sockets on Windows are AFD NT handles, which ws2_32
//! rejects (WSAENOTSOCK), so the Windows probes go through the same NT
//! ioctl surface the Io backend uses. Elsewhere they are plain fds.

const std = @import("std");
const builtin = @import("builtin");

/// Bytes queued in the socket's receive buffer, or null when the probe
/// is unsupported (non-Windows) or the ioctl fails/pends.
///
/// A read that PENDS on Windows (posted before data arrives) occasionally
/// loses its completion — the connection thread then waits forever while
/// the client's bytes sit undelivered in the socket receive buffer.
/// Queued bytes + a long-pending read = wedged socket (an idle
/// connection has zero bytes queued).
pub fn bytesAvailable(handle: std.Io.net.Socket.Handle) ?u32 {
    if (builtin.os.tag != .windows) return null;
    const windows = std.os.windows;

    var info: windows.AFD.RECEIVE_INFORMATION = undefined;
    var iosb: windows.IO_STATUS_BLOCK = undefined;
    const status = windows.ntdll.NtDeviceIoControlFile(
        handle,
        null, // event
        null, // APC routine — synchronous: an info query never pends
        null, // APC context
        &iosb,
        windows.IOCTL.AFD.QUERY_RECEIVE_INFO,
        null,
        0,
        @ptrCast(&info),
        @sizeOf(windows.AFD.RECEIVE_INFORMATION),
    );
    if (status != .SUCCESS) return null;
    return info.BytesAvailable;
}

/// Whether the peer has closed the connection (FIN or reset). False
/// while it is open. On POSIX a FIN queued behind unread bytes reads as
/// open until those bytes are consumed. Null when the probe fails.
pub fn peerClosed(handle: std.Io.net.Socket.Handle) ?bool {
    return switch (builtin.os.tag) {
        .windows => peerClosedWindows(handle),
        else => peerClosedPosix(handle),
    };
}

fn peerClosedPosix(handle: std.Io.net.Socket.Handle) ?bool {
    const posix = std.posix;
    const rdhup = if (@hasDecl(posix.POLL, "RDHUP")) posix.POLL.RDHUP else 0;
    var fds = [1]posix.pollfd{.{ .fd = handle, .events = posix.POLL.IN | rdhup, .revents = 0 }};
    const ready = posix.poll(&fds, 0) catch return null;
    if (ready == 0) return false;
    if (fds[0].revents & (posix.POLL.HUP | posix.POLL.ERR | rdhup) != 0) return true;
    if (fds[0].revents & posix.POLL.IN == 0) return false;
    // Readable without RDHUP (macOS has none): a FIN peeks as zero bytes.
    var byte: [1]u8 = undefined;
    const rc = posix.system.recvfrom(handle, &byte, byte.len, posix.MSG.PEEK | posix.MSG.DONTWAIT, null, null);
    return switch (posix.errno(rc)) {
        .SUCCESS => rc == 0,
        .AGAIN => false,
        .CONNRESET, .NOTCONN, .PIPE => true,
        else => null,
    };
}

/// AFD_POLL_* event bits (afd.h); not in std.os.windows.
const afd_poll_receive: u32 = 0x0001;
const afd_poll_disconnect: u32 = 0x0008;
const afd_poll_abort: u32 = 0x0010;
const afd_poll_local_close: u32 = 0x0020;

/// AFD_POLL_INFO with one handle (afd.h); not in std.os.windows.
const AfdPollInfo = extern struct {
    timeout: i64,
    handle_count: u32,
    exclusive: u32,
    handle: std.os.windows.HANDLE,
    events: u32,
    status: std.os.windows.NTSTATUS,
};

fn peerClosedWindows(handle: std.Io.net.Socket.Handle) ?bool {
    const windows = std.os.windows;
    var event: windows.HANDLE = undefined;
    if (windows.ntdll.NtCreateEvent(&event, windows.ACCESS_MASK.Specific.Event.ALL_ACCESS, null, .Notification, .FALSE) != .SUCCESS) return null;
    defer _ = windows.ntdll.NtClose(event);

    // A zero timeout reports the events already signalled and completes
    // at once. RECEIVE is asked for so the poll returns as soon as
    // anything is pending; only the disconnect bits decide the answer.
    var info: AfdPollInfo = .{
        .timeout = 0,
        .handle_count = 1,
        .exclusive = 0,
        .handle = handle,
        .events = afd_poll_receive | afd_poll_disconnect | afd_poll_abort | afd_poll_local_close,
        .status = .SUCCESS,
    };
    var iosb: windows.IO_STATUS_BLOCK = undefined;
    var status = windows.ntdll.NtDeviceIoControlFile(
        handle,
        event,
        null, // APC routine: completion signals `event`, on this thread's terms
        null,
        &iosb,
        windows.IOCTL.AFD.POLL,
        @ptrCast(&info),
        @sizeOf(AfdPollInfo),
        @ptrCast(&info),
        @sizeOf(AfdPollInfo),
    );
    if (status == .PENDING) {
        const bound: windows.LARGE_INTEGER = -100 * std.time.ns_per_ms / 100; // relative, 100 ns units
        if (windows.ntdll.NtWaitForSingleObject(event, .FALSE, &bound) != .SUCCESS) {
            var cancel_iosb: windows.IO_STATUS_BLOCK = undefined;
            _ = windows.ntdll.NtCancelIoFileEx(handle, &iosb, &cancel_iosb);
            _ = windows.ntdll.NtWaitForSingleObject(event, .FALSE, null);
            return null;
        }
        status = iosb.u.Status;
    }
    if (status != .SUCCESS) return null;
    if (info.handle_count == 0) return false;
    return info.events & (afd_poll_disconnect | afd_poll_abort | afd_poll_local_close) != 0;
}

/// End the connection from any thread, so a read or send another thread
/// has pending on it returns an error now. POSIX `shutdown` wakes both.
/// A graceful disconnect on Windows wakes neither: it queues behind a
/// pending send and leaves a pending read posted. So there the connection
/// is reset (an abortive AFD disconnect), which fails both at once.
/// Best effort; the handle stays open for its owner to close. Returns within
/// about two seconds, because the reaper calls it under the registry lock.
pub fn abortTransfers(allocator: std.mem.Allocator, io: std.Io, handle: std.Io.net.Socket.Handle) void {
    switch (builtin.os.tag) {
        .windows => abortTransfersWindows(allocator, handle),
        else => io.vtable.netShutdown(io.userdata, handle, .both) catch {},
    }
}

fn abortTransfersWindows(allocator: std.mem.Allocator, handle: std.Io.net.Socket.Handle) void {
    const windows = std.os.windows;
    // The kernel reads `info` and writes `iosb` until the request completes,
    // so they live on the heap: a request that outlasts the bounded waits
    // below leaks them rather than leave the kernel a dead stack frame.
    const Request = struct {
        info: windows.AFD.PARTIAL_DISCONNECT_INFO,
        iosb: windows.IO_STATUS_BLOCK,
    };
    const request = allocator.create(Request) catch return;
    request.* = .{
        .info = .{
            .DisconnectMode = .{ .SEND = true, .RECEIVE = true, .ABORTIVE = true },
            .Timeout = -1,
        },
        .iosb = undefined,
    };
    var event: windows.HANDLE = undefined;
    if (windows.ntdll.NtCreateEvent(&event, windows.ACCESS_MASK.Specific.Event.ALL_ACCESS, null, .Notification, .FALSE) != .SUCCESS) {
        allocator.destroy(request);
        return;
    }
    // Closing the handle is safe while the request is pending: the request
    // holds its own reference to the event.
    defer _ = windows.ntdll.NtClose(event);

    const status = windows.ntdll.NtDeviceIoControlFile(
        handle,
        event,
        null, // APC routine: completion signals `event`, on this thread's terms
        null,
        &request.iosb,
        windows.IOCTL.AFD.PARTIAL_DISCONNECT,
        @ptrCast(&request.info),
        @sizeOf(windows.AFD.PARTIAL_DISCONNECT_INFO),
        null,
        0,
    );
    const bound: windows.LARGE_INTEGER = -1000 * std.time.ns_per_ms / 100; // relative, 100 ns units
    const completed = status != .PENDING or
        windows.ntdll.NtWaitForSingleObject(event, .FALSE, &bound) == .SUCCESS or
        cancelled: {
            // Cancelling by the IOSB's address touches only this request,
            // never the owner's pending read or send.
            var cancel_iosb: windows.IO_STATUS_BLOCK = undefined;
            _ = windows.ntdll.NtCancelIoFileEx(handle, &request.iosb, &cancel_iosb);
            break :cancelled windows.ntdll.NtWaitForSingleObject(event, .FALSE, &bound) == .SUCCESS;
        };
    if (completed) allocator.destroy(request);
}

test "peerClosed tells an open connection from one the peer closed" {
    const io = std.testing.io;
    const address = try std.Io.net.IpAddress.parse("127.0.0.1", 0);
    var listener = try address.listen(io, .{ .mode = .stream, .protocol = .tcp });
    defer listener.deinit(io);
    const client = try listener.socket.address.connect(io, .{ .mode = .stream, .protocol = .tcp });
    var client_open = true;
    defer if (client_open) client.close(io);
    const accepted = try listener.accept(io);
    defer accepted.close(io);

    try std.testing.expectEqual(@as(?bool, false), peerClosed(accepted.socket.handle));

    var write_buf: [8]u8 = undefined;
    var client_writer = client.writer(io, &write_buf);
    try client_writer.interface.writeAll("x");
    try client_writer.interface.flush();
    try expectEventually(accepted.socket.handle, false);

    var read_buf: [8]u8 = undefined;
    var server_reader = accepted.reader(io, &read_buf);
    try std.testing.expectEqual(@as(u8, 'x'), try server_reader.interface.takeByte());
    try std.testing.expectEqual(@as(?bool, false), peerClosed(accepted.socket.handle));

    client.close(io);
    client_open = false;
    try expectEventually(accepted.socket.handle, true);
}

/// Loopback delivery is asynchronous: give the kernel a moment to land a
/// FIN or a byte before judging the probe.
fn expectEventually(handle: std.Io.net.Socket.Handle, expected: bool) !void {
    for (0..100) |_| {
        if (peerClosed(handle) == expected) return;
        try std.Io.sleep(std.testing.io, .fromMilliseconds(10), .awake);
    }
    try std.testing.expectEqual(@as(?bool, expected), peerClosed(handle));
}
