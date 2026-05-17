//! Integration test aggregator. Each `_ = @import(...);` pulls in its tests.

const std = @import("std");
const thindb = @import("thindb");

test {
    _ = @import("roundtrip.zig");
    _ = @import("snapshot_test.zig");
    _ = @import("wal_test.zig");
    _ = @import("durability_test.zig");
    _ = @import("ddl_test.zig");
    _ = @import("uuid_test.zig");
}

test "integration entry exists" {
    try std.testing.expect(thindb.version.len > 0);
}
