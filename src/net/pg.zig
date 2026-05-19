//! PostgreSQL wire protocol server — barrel module. Re-exports the public
//! surface from the `pg/` subdirectory.

pub const packet = @import("pg/packet.zig");
pub const startup = @import("pg/startup.zig");
pub const result = @import("pg/result.zig");
pub const canned = @import("pg/canned.zig");
pub const errors = @import("pg/errors.zig");
pub const auth = @import("pg/auth.zig");
pub const copy = @import("pg/copy.zig");
pub const server = @import("pg/server.zig");

pub const Server = server.Server;
pub const Error = server.Error;
pub const servePg = server.servePg;
pub const default_port = server.default_port;

test {
    _ = packet;
    _ = startup;
    _ = result;
    _ = canned;
    _ = errors;
    _ = auth;
    _ = copy;
    _ = server;
}
