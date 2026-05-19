//! MySQL wire protocol server — barrel module. Re-exports the public
//! surface from the `mysql/` subdirectory.

pub const packet = @import("mysql/packet.zig");
pub const handshake = @import("mysql/handshake.zig");
pub const result = @import("mysql/result.zig");
pub const canned = @import("mysql/canned.zig");
pub const errors = @import("mysql/errors.zig");
pub const server = @import("mysql/server.zig");

pub const Server = server.Server;
pub const Error = server.Error;
pub const serveMysql = server.serveMysql;
pub const default_port = server.default_port;

test {
    _ = packet;
    _ = handshake;
    _ = result;
    _ = canned;
    _ = errors;
    _ = server;
}
