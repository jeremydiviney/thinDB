// The AFD opening sequence is adapted from Zig 0.16 Io.Threaded.
// The MIT License (Expat)
//
// Copyright (c) Zig contributors
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

const std = @import("std");
const builtin = @import("builtin");
const Io = std.Io;

/// Zig 0.16's Windows listener binds AFD in Passive mode even when reuse is
/// disabled; on POSIX its reuse option also enables SO_REUSEPORT. Neither
/// provides the exclusive listener ownership required by a database server.
pub fn listen(address: *const Io.net.IpAddress, io: Io) !Io.net.Server {
    if (builtin.os.tag == .windows) return listenWindows(address, io);
    const p = std.posix;
    const cloexec = if (@hasDecl(p.SOCK, "CLOEXEC")) p.SOCK.CLOEXEC else 0;
    const fd = p.system.socket(Io.Threaded.posixAddressFamily(address), p.SOCK.STREAM | cloexec, p.IPPROTO.TCP);
    if (fd < 0) return error.SystemResources;
    errdefer _ = p.system.close(fd);
    if (cloexec == 0 and p.system.fcntl(fd, p.F.SETFD, @as(c_int, p.FD_CLOEXEC)) == -1) return error.Unexpected;
    const enabled: c_int = 1;
    try p.setsockopt(fd, p.SOL.SOCKET, p.SO.REUSEADDR, std.mem.asBytes(&enabled));
    var sockaddr: Io.Threaded.PosixAddress = undefined;
    var len = Io.Threaded.addressToPosix(address, &sockaddr);
    switch (p.errno(p.system.bind(fd, &sockaddr.any, len))) {
        .SUCCESS => {},
        .ADDRINUSE => return error.AddressInUse,
        .ADDRNOTAVAIL => return error.AddressUnavailable,
        .ACCES => return error.AccessDenied,
        else => return error.Unexpected,
    }
    switch (p.errno(p.system.listen(fd, (Io.net.IpAddress.ListenOptions{}).kernel_backlog))) {
        .SUCCESS => {},
        .ADDRINUSE => return error.AddressInUse,
        else => return error.Unexpected,
    }
    if (p.errno(p.system.getsockname(fd, &sockaddr.any, &len)) != .SUCCESS) return error.Unexpected;
    return .{ .socket = .{ .handle = fd, .address = Io.Threaded.addressFromPosix(&sockaddr) }, .options = {} };
}

fn listenWindows(address: *const Io.net.IpAddress, io: Io) !Io.net.Server {
    const w = std.os.windows;
    const ws = w.ws2_32;
    // Zig's enum omits AFD_SHARE_EXCLUSIVE (3); Passive (1) requests reuse.
    // https://doxygen.reactos.org/de/dd0/sdk_2include_2reactos_2drivers_2afd_2shared_8h_source.html
    const exclusive_bind: w.AFD.BIND_INFO.MODE = @enumFromInt(3);
    var handle: w.HANDLE = undefined;
    var status_block: w.IO_STATUS_BLOCK = undefined;
    // Match the AFD handles consumed by Io.Threaded.accept/read/write, without
    // a process-global Winsock lifetime or a different connection backend.
    const status = w.ntdll.NtCreateFile(
        &handle,
        .{ .STANDARD = .{ .RIGHTS = .{ .WRITE_DAC = true }, .SYNCHRONIZE = true }, .GENERIC = .{ .WRITE = true, .READ = true } },
        &.{ .ObjectName = @constCast(&w.UNICODE_STRING.init(w.AFD.DEVICE_NAME ++ .{ '\\', 'E', 'n', 'd', 'p', 'o', 'i', 'n', 't' })) },
        &status_block,
        null,
        .{},
        .{ .READ = true, .WRITE = true },
        .OPEN_IF,
        .{ .IO = .ASYNCHRONOUS },
        &w.AFD.OPEN_PACKET.FULL_EA_INFORMATION{ .Value = .{
            .EndpointType = .{ .CONNECTIONLESS = false, .MESSAGEMODE = false, .RAW = false },
            .GroupID = 0,
            .AddressFamily = Io.Threaded.posixAddressFamily(address),
            .SocketType = @bitCast(@as(u32, ws.SOCK.STREAM)),
            .Protocol = @bitCast(@as(u32, ws.IPPROTO.TCP)),
            .TransportDeviceNameLength = 0,
            .TransportDeviceName = undefined,
        } },
        @sizeOf(w.AFD.OPEN_PACKET.FULL_EA_INFORMATION),
    );
    try checkStatus(status);
    errdefer w.CloseHandle(handle);
    const file: Io.File = .{ .handle = handle, .flags = .{ .nonblocking = true } };
    var enabled: i32 = 1;
    const value = std.mem.asBytes(&enabled);
    const option = w.AFD.SOCKOPT_INFO{
        .mode = .set,
        .level = ws.SOL.SOCKET,
        // Winsock defines SO_EXCLUSIVEADDRUSE as the complement of SO_REUSEADDR.
        .optname = ~@as(u32, ws.SO.REUSEADDR),
        .optval = value.ptr,
        .optlen = value.len,
    };
    try control(io, .{ .file = file, .code = w.IOCTL.AFD.SOCKOPT, .in = std.mem.asBytes(&option) });
    const Bind = extern struct { info: w.AFD.BIND_INFO, address: Io.Threaded.PosixAddress };
    var binding: Bind = .{ .info = .{ .Mode = exclusive_bind }, .address = undefined };
    const len = Io.Threaded.addressToPosix(address, &binding.address);
    try control(io, .{
        .file = file,
        .code = w.IOCTL.AFD.BIND,
        .in = std.mem.asBytes(&binding)[0 .. @offsetOf(Bind, "address") + len],
        .out = std.mem.asBytes(&binding.address)[0..len],
    });
    const info = w.AFD.LISTEN_INFO{
        .UseSAN = .FALSE,
        .MaximumConnectionQueue = (Io.net.IpAddress.ListenOptions{}).kernel_backlog,
        .UseDelayedAcceptance = .FALSE,
    };
    try control(io, .{ .file = file, .code = w.IOCTL.AFD.START_LISTEN, .in = std.mem.asBytes(&info) });
    return .{
        .socket = .{ .handle = handle, .address = Io.Threaded.addressFromPosix(&binding.address) },
        .options = .{ .mode = .stream, .protocol = .tcp },
    };
}

fn control(io: Io, op: Io.Operation.DeviceIoControl) !void {
    const result = try io.operate(.{ .device_io_control = op });
    try checkStatus(result.device_io_control.u.Status);
}

fn checkStatus(status: std.os.windows.NTSTATUS) !void {
    return switch (status) {
        .SUCCESS => {},
        .CANCELLED => error.Canceled,
        .SHARING_VIOLATION, .ADDRESS_ALREADY_EXISTS => error.AddressInUse,
        .ACCESS_DENIED => error.AccessDenied,
        .INSUFFICIENT_RESOURCES => error.SystemResources,
        else => error.Unexpected,
    };
}

test "TCP listener rejects another bind and permits reuse after close" {
    const io = std.testing.io;
    const loopback = try Io.net.IpAddress.parse("127.0.0.1", 0);
    var first = try listen(&loopback, io);
    const address = first.socket.address;
    {
        defer first.deinit(io);
        if (listen(&address, io)) |unexpected| {
            var owned = unexpected;
            owned.deinit(io);
            return error.TestUnexpectedResult;
        } else |err| try std.testing.expectEqual(error.AddressInUse, err);
    }
    var next = try listen(&address, io);
    defer next.deinit(io);
}
