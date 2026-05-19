//! Generic map-key snapshot helper. Used by Catalog / Database / Schema to
//! produce a stable list of names for iteration after dropping the lock —
//! prevents a concurrent drop from invalidating pointers mid-iteration.

const std = @import("std");
const Allocator = std.mem.Allocator;
const Io = std.Io;

pub fn snapshotMapKeys(
    allocator: Allocator,
    io: Io,
    mutex: *Io.Mutex,
    map: anytype,
) ![][]u8 {
    mutex.lockUncancelable(io);
    defer mutex.unlock(io);
    const out = try allocator.alloc([]u8, map.count());
    errdefer allocator.free(out);
    var i: usize = 0;
    errdefer for (out[0..i]) |s| allocator.free(s);
    var it = map.keyIterator();
    while (it.next()) |k| : (i += 1) {
        out[i] = try allocator.dupe(u8, k.*);
    }
    return out;
}

pub fn freeNames(allocator: Allocator, names: [][]u8) void {
    for (names) |s| allocator.free(s);
    allocator.free(names);
}
