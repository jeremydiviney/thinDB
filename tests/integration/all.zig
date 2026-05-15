//! Integration test aggregator. Each `_ = @import(...);` pulls in its tests.

const std = @import("std");
const thindb = @import("thindb");

test {
    _ = @import("roundtrip.zig");
}

test "integration entry exists" {
    try std.testing.expect(thindb.version.len > 0);
}
