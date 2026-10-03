//! Runs one test binary for `zig build test` and holds its log back: nothing
//! when every test passes, the whole log on stderr when one fails or the
//! binary dies. The build runner prints any stderr a step leaves under a
//! "failed command" header, so a passing binary must leave none; and it runs
//! the binaries in parallel only when their output is captured (build.zig).

const std = @import("std");

pub fn main(init: std.process.Init) !u8 {
    const io = init.io;
    const args = try init.minimal.args.toSlice(init.arena.allocator());
    if (args.len < 2) {
        std.debug.print("usage: run_test_quiet <test binary> [args...]\n", .{});
        return 2;
    }
    var child = try std.process.spawn(io, .{
        .argv = args[1..],
        .stdin = .ignore,
        .stdout = .inherit,
        .stderr = .pipe,
    });
    errdefer std.process.Child.kill(&child, io);
    var reader = child.stderr.?.readerStreaming(io, &.{});
    const log = try reader.interface.allocRemaining(init.gpa, .unlimited);
    defer init.gpa.free(log);
    const term = try std.process.Child.wait(&child, io);
    const code: u8 = switch (term) {
        .exited => |exit_code| exit_code,
        else => 1,
    };
    if (code != 0) {
        std.debug.print("{s}", .{log});
        if (term != .exited) std.debug.print("test binary ended by {any}\n", .{term});
    }
    return code;
}
