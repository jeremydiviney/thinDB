const std = @import("std");
const thindb = @import("thindb");

pub fn main() !void {
    std.debug.print("thinDB demo runner — version {s}\n", .{thindb.version});
}
