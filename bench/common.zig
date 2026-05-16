//! Shared fixtures and reporting helpers used across the bench files.
//! Each bench file (`main.zig`, `compact_bench.zig`, `durability_bench.zig`)
//! imports this module rather than re-declaring the same schema/row pool.

const std = @import("std");
const thindb = @import("thindb");
pub const Allocator = std.mem.Allocator;
pub const Io = std.Io;

pub const default_rows: usize = 1_000_000;

pub const Row = struct {
    id: i64,
    qty: i32,
    active: bool,
    tag: []const u8,
};

pub const schema = thindb.Schema{
    .columns = &.{
        .{ .name = "id", .type = .bigint },
        .{ .name = "qty", .type = .int },
        .{ .name = "active", .type = .boolean },
        .{ .name = "tag", .type = .{ .varchar = 16 } },
    },
    .order_key = &.{"id"},
    .unique = false,
};

const order_key = [_][]const u8{"id"};
pub const options = thindb.TableOptions{
    .order_key = &order_key,
    .unique = false,
    .row_group_size = 65_536,
};

pub const tag_pool = [_][]const u8{ "alpha", "beta", "gamma", "delta", "epsilon", "zeta", "eta", "theta" };

pub fn buildRows(allocator: Allocator, n: usize) ![]Row {
    const rows = try allocator.alloc(Row, n);
    for (rows, 0..) |*r, i| {
        r.* = .{
            .id = @intCast(i),
            .qty = @intCast(i % 100),
            .active = (i & 1) == 0,
            .tag = tag_pool[i % tag_pool.len],
        };
    }
    return rows;
}

pub fn elapsedNs(io: Io, t0: Io.Timestamp) u64 {
    const t1 = Io.Clock.awake.now(io);
    const dur = t0.durationTo(t1);
    return @intCast(dur.toNanoseconds());
}

pub fn freshDir(io: Io, sub_path: []const u8) !Io.Dir {
    const cwd = Io.Dir.cwd();
    try cwd.deleteTree(io, sub_path);
    return cwd.createDirPathOpen(io, sub_path, .{});
}

pub fn report(name: []const u8, rows: u64, elapsed_ns: u64, bytes: ?u64) !void {
    const seconds = @as(f64, @floatFromInt(elapsed_ns)) / 1e9;
    const ms = seconds * 1000.0;
    const rps_m = (@as(f64, @floatFromInt(rows)) / seconds) / 1e6;
    const ns_per_row = @as(f64, @floatFromInt(elapsed_ns)) / @as(f64, @floatFromInt(rows));

    if (bytes) |b| {
        const mb_per_sec = (@as(f64, @floatFromInt(b)) / seconds) / (1024.0 * 1024.0);
        const kb = @as(f64, @floatFromInt(b)) / 1024.0;
        std.debug.print(
            "  {s:<32} {d:>10} rows  {d:>8.2} ms  {d:>7.2} M rows/s  {d:>6.1} ns/row  {d:>8.1} KB  {d:>6.1} MB/s\n",
            .{ name, rows, ms, rps_m, ns_per_row, kb, mb_per_sec },
        );
    } else {
        std.debug.print(
            "  {s:<32} {d:>10} rows  {d:>8.2} ms  {d:>7.2} M rows/s  {d:>6.1} ns/row\n",
            .{ name, rows, ms, rps_m, ns_per_row },
        );
    }
}
