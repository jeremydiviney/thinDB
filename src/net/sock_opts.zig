//! Best-effort cross-platform socket-option helpers.
//!
//! POSIX platforms get real `setsockopt(2)` calls for SO_KEEPALIVE and
//! SO_RCVTIMEO. Windows is intentionally a no-op: in Zig 0.16, the
//! `Io.net` sockets opened on Windows are AFD-backed NT handles, not
//! the WSA SOCKET fd you'd expect — so `ws2_32!setsockopt` returns
//! WSAENOTSOCK on them and the only blessed path through stdlib goes
//! via the private `setSocketOptionAfd` helper, which the public API
//! doesn't expose. Documented in DESIGN.md as a known v1 limitation;
//! Linux/macOS deployments get full keepalive + idle-timeout coverage.
//!
//! Failures are NOT propagated to callers. Keepalive / idle-timeout
//! are hygiene tweaks, not correctness requirements: a kernel quirk
//! is never a reason to refuse a client connection.

const std = @import("std");
const builtin = @import("builtin");

pub const Handle = std.posix.fd_t;

const supported = switch (builtin.os.tag) {
    .linux, .macos, .ios, .tvos, .watchos, .freebsd, .openbsd, .netbsd, .dragonfly => true,
    else => false,
};

extern "c" fn setsockopt(
    sockfd: Handle,
    level: c_int,
    optname: c_int,
    optval: ?*const anyopaque,
    optlen: c_int,
) c_int;

const SOL_SOCKET: c_int = switch (builtin.os.tag) {
    .linux => @as(c_int, std.os.linux.SOL.SOCKET),
    .macos, .ios, .tvos, .watchos => @as(c_int, 0xffff),
    else => @as(c_int, 1),
};

const SO_KEEPALIVE_OPT: c_int = switch (builtin.os.tag) {
    .linux => @as(c_int, std.os.linux.SO.KEEPALIVE),
    .macos, .ios, .tvos, .watchos => @as(c_int, 0x0008),
    else => @as(c_int, 9),
};

const SO_RCVTIMEO_OPT: c_int = switch (builtin.os.tag) {
    .linux => @as(c_int, std.os.linux.SO.RCVTIMEO),
    .macos, .ios, .tvos, .watchos => @as(c_int, 0x1006),
    else => @as(c_int, 20),
};

/// Best-effort SO_KEEPALIVE on the socket. Returns false on any error
/// or on platforms where setsockopt isn't reachable from this layer.
pub fn enableKeepalive(handle: Handle) bool {
    if (!supported) return false;
    const on: c_int = 1;
    const rc = setsockopt(
        handle,
        SOL_SOCKET,
        SO_KEEPALIVE_OPT,
        @ptrCast(&on),
        @sizeOf(c_int),
    );
    return rc == 0;
}

/// Best-effort SO_RCVTIMEO. Pass 0 to clear / disable. Returns false
/// on any error or on platforms where setsockopt isn't reachable from
/// this layer.
pub fn setReadTimeoutSeconds(handle: Handle, seconds: u32) bool {
    if (!supported) return false;
    const TimeVal = extern struct { tv_sec: i64, tv_usec: i64 };
    const tv: TimeVal = .{ .tv_sec = @intCast(seconds), .tv_usec = 0 };
    const rc = setsockopt(
        handle,
        SOL_SOCKET,
        SO_RCVTIMEO_OPT,
        @ptrCast(&tv),
        @sizeOf(TimeVal),
    );
    return rc == 0;
}

test "constants resolve at comptime" {
    try std.testing.expect(SO_KEEPALIVE_OPT != 0);
    try std.testing.expect(SO_RCVTIMEO_OPT != 0);
}
