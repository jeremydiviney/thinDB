const std = @import("std");
const Allocator = std.mem.Allocator;

const types = @import("../types.zig");
const table_strings = @import("table_strings.zig");
const TableStrings = table_strings.TableStrings;
const State = table_strings.State;

const io = std.testing.io;

const schema = types.TableSchema{
    .columns = &.{
        .{ .name = "id", .type = .bigint },
        .{ .name = "url", .type = .string },
    },
    .order_key = &.{"id"},
    .unique = false,
};
const url_column = 1;

const Values = struct {
    items: []const []const u8,

    pub fn len(self: Values) usize {
        return self.items.len;
    }

    pub fn get(self: Values, i: usize) []const u8 {
        return self.items[i];
    }
};

fn resolve(ts: *TableStrings, dir: std.Io.Dir, items: []const []const u8, ids: []u32) !bool {
    return ts.resolve(io, dir, 4, url_column, Values{ .items = items }, ids);
}

/// Distinct values `"value-<i>-..."`, padded so `count` of them pass a page.
fn makeValues(allocator: Allocator, first: usize, count: usize) ![][]u8 {
    const values = try allocator.alloc([]u8, count);
    for (values, first..) |*v, i| v.* = try std.fmt.allocPrint(allocator, "value-{d}-https://example.com/a/fairly/long/path?q={d}", .{ i, i * 7 });
    return values;
}

fn freeValues(allocator: Allocator, values: [][]u8) void {
    for (values) |v| allocator.free(v);
    allocator.free(values);
}

fn reopen(allocator: Allocator, ts: *TableStrings, states: []const State) !void {
    ts.deinit();
    ts.* = try TableStrings.init(allocator, schema, states);
}

test "table strings give a value one id across writes and reopens" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var ts = try TableStrings.init(allocator, schema, &.{});
    defer ts.deinit();
    try std.testing.expect(ts.state(io, 0) == null);

    var ids: [3]u32 = undefined;
    try std.testing.expect(try resolve(&ts, tmp.dir, &.{ "a", "b", "c" }, &ids));
    try std.testing.expectEqualSlices(u32, &.{ 0, 1, 2 }, &ids);
    try std.testing.expect(try resolve(&ts, tmp.dir, &.{ "c", "d", "a" }, &ids));
    try std.testing.expectEqualSlices(u32, &.{ 2, 3, 0 }, &ids);

    const states = try ts.persist(allocator, io, tmp.dir, false);
    defer allocator.free(states);
    try std.testing.expectEqualSlices(State, &.{.{ .column_idx = url_column, .id_count = 4, .closed = false }}, states);

    try reopen(allocator, &ts, states);
    var two: [2]u32 = undefined;
    try std.testing.expect(try resolve(&ts, tmp.dir, &.{ "e", "d" }, &two));
    try std.testing.expectEqualSlices(u32, &.{ 4, 3 }, &two);
}

test "table strings find values in sealed pages and their tail after a reopen" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var ts = try TableStrings.init(allocator, schema, &.{});
    defer ts.deinit();

    // About 2.5 pages of values, past the index's first growth.
    const n = 40_000;
    const values = try makeValues(allocator, 0, n);
    defer freeValues(allocator, values);
    const ids = try allocator.alloc(u32, n);
    defer allocator.free(ids);
    try std.testing.expect(try resolve(&ts, tmp.dir, values, ids));
    for (ids, 0..) |id, i| try std.testing.expectEqual(@as(u32, @intCast(i)), id);

    const states = try ts.persist(allocator, io, tmp.dir, true);
    defer allocator.free(states);
    try reopen(allocator, &ts, states);

    // Every other old value, from every page and the tail, then new ones.
    const fresh = try makeValues(allocator, n, 100);
    defer freeValues(allocator, fresh);
    var mixed: std.ArrayList([]const u8) = .empty;
    defer mixed.deinit(allocator);
    var i: usize = n;
    while (i > 0) {
        i -= 2;
        try mixed.append(allocator, values[i]);
    }
    for (fresh) |v| try mixed.append(allocator, v);
    const got = try allocator.alloc(u32, mixed.items.len);
    defer allocator.free(got);
    try std.testing.expect(try resolve(&ts, tmp.dir, mixed.items, got));
    for (got[0 .. n / 2], 0..) |id, k| try std.testing.expectEqual(@as(u32, @intCast(n - 2 - 2 * k)), id);
    for (got[n / 2 ..], 0..) |id, k| try std.testing.expectEqual(@as(u32, @intCast(n + k)), id);
}

test "table strings close a column whose new values pass the cap" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var ts = try TableStrings.init(allocator, schema, &.{});
    defer ts.deinit();
    ts.cap = 4;

    var ids: [3]u32 = undefined;
    try std.testing.expect(try resolve(&ts, tmp.dir, &.{ "a", "b", "c" }, &ids));
    try std.testing.expect(try resolve(&ts, tmp.dir, &.{ "b", "c", "d" }, &ids));
    try std.testing.expect(!try resolve(&ts, tmp.dir, &.{ "d", "e", "f" }, &ids));
    try std.testing.expect(ts.state(io, url_column).?.closed);
    try std.testing.expect(!try resolve(&ts, tmp.dir, &.{"a"}, ids[0..1]));

    const states = try ts.persist(allocator, io, tmp.dir, false);
    defer allocator.free(states);
    try std.testing.expectEqualSlices(State, &.{.{ .column_idx = url_column, .id_count = 0, .closed = true }}, states);
    try reopen(allocator, &ts, states);
    try std.testing.expect(!try resolve(&ts, tmp.dir, &.{"a"}, ids[0..1]));
}

test "table strings cut a torn page and close a column missing published ids" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var ts = try TableStrings.init(allocator, schema, &.{});
    defer ts.deinit();

    const n = 40_000;
    const values = try makeValues(allocator, 0, n);
    defer freeValues(allocator, values);
    const ids = try allocator.alloc(u32, n);
    defer allocator.free(ids);
    try std.testing.expect(try resolve(&ts, tmp.dir, values, ids));
    const states = try ts.persist(allocator, io, tmp.dir, false);
    defer allocator.free(states);

    var strings_dir = try tmp.dir.openDir(io, table_strings.dir_name, .{});
    defer strings_dir.close(io);
    const list_name = std.fmt.comptimePrint("{d}.list", .{url_column});
    const list = try strings_dir.openFile(io, list_name, .{ .mode = .read_write });
    const size = (try list.stat(io)).size;
    try list.writePositionalAll(io, "a torn page", size);
    list.close(io);

    // The torn bytes go; every published id is still there.
    try reopen(allocator, &ts, states);
    var one: [1]u32 = undefined;
    try std.testing.expect(try resolve(&ts, tmp.dir, values[5..6], &one));
    try std.testing.expectEqual(@as(u32, 5), one[0]);
    const list_after = try strings_dir.openFile(io, list_name, .{});
    try std.testing.expectEqual(size, (try list_after.stat(io)).size);
    list_after.close(io);

    // A list cut short of what the manifest published closes the column.
    const cut = try strings_dir.openFile(io, list_name, .{ .mode = .read_write });
    try cut.setLength(io, size / 2);
    cut.close(io);
    try reopen(allocator, &ts, states);
    try std.testing.expect(!try resolve(&ts, tmp.dir, values[5..6], &one));
    try std.testing.expect(ts.state(io, url_column).?.closed);
}
