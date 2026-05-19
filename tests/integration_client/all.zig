//! Integration tests for the client/server surface (thindb.local + Connection
//! + ClientQuery). Each `_ = @import(...)` pulls in its scenarios.

const std = @import("std");
const thindb = @import("thindb");

test {
    _ = @import("basic_test.zig");
    _ = @import("tcp_test.zig");
    _ = @import("mysql_test.zig");
    _ = @import("pg_test.zig");
    _ = @import("server_binary_test.zig");
}

test "integration_client entry exists" {
    try std.testing.expect(thindb.version.len > 0);
}
