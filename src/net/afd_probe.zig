//! Windows AFD socket probe for the net_read_timeout reaper (#164).
//!
//! Zig 0.16's Io.net sockets on Windows are AFD NT handles; a read that
//! PENDS (posted before data arrives) occasionally loses its completion
//! — the connection thread then waits forever while the client's bytes
//! sit undelivered in the socket receive buffer. `bytesAvailable` asks
//! AFD how many bytes are queued, synchronously and without touching
//! the pending read: queued bytes + a long-pending read = wedged socket
//! (an idle connection has zero bytes queued). ws2_32's ioctlsocket
//! (FIONREAD) rejects these handles (WSAENOTSOCK), so the query goes
//! through the same NT ioctl surface the Io backend uses.

const std = @import("std");
const builtin = @import("builtin");

/// Bytes queued in the socket's receive buffer, or null when the probe
/// is unsupported (non-Windows) or the ioctl fails/pends.
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
