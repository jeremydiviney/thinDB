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
    comptime {
        if (@typeInfo(@TypeOf(map)) != .pointer) @compileError("snapshotMapKeys requires a map pointer so its header is read under the lock");
    }
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

test "map snapshot observes header changes while waiting for its lock" {
    const Map = std.StringHashMap(u8);
    const Worker = struct {
        map: *Map,
        mutex: *Io.Mutex,
        result: ?[][]u8 = null,
        failure: ?anyerror = null,

        fn run(self: *@This()) void {
            self.result = snapshotMapKeys(std.testing.allocator, std.testing.io, self.mutex, self.map) catch |err| {
                self.failure = err;
                return;
            };
        }
    };
    inline for (.{ 1, 20 }) |added| {
        var map = Map.init(std.testing.allocator);
        defer map.deinit();
        try map.ensureTotalCapacity(8);
        try map.put("initial", 0);
        var mutex: Io.Mutex = .init;
        mutex.lockUncancelable(std.testing.io);
        var worker = Worker{ .map = &map, .mutex = &mutex };
        const thread = std.Thread.spawn(.{}, Worker.run, .{&worker}) catch |err| {
            mutex.unlock(std.testing.io);
            return err;
        };
        var names: [added][8]u8 = undefined;
        {
            defer thread.join();
            defer mutex.unlock(std.testing.io);
            while (mutex.state.load(.acquire) != .contended) std.atomic.spinLoopHint();
            for (&names, 0..) |*name, i| {
                const key = try std.fmt.bufPrint(name, "k{d}", .{i});
                try map.put(key, 0);
            }
        }
        if (worker.failure) |err| return err;
        const snapshot = worker.result.?;
        defer freeNames(std.testing.allocator, snapshot);
        try std.testing.expectEqual(@as(usize, 1 + added), snapshot.len);
        for (snapshot) |name| try std.testing.expect(map.contains(name));
    }
}
