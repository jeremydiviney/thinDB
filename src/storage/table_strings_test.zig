const std = @import("std");
const Allocator = std.mem.Allocator;

const types = @import("../types.zig");
const table_strings = @import("table_strings.zig");
const TableStrings = table_strings.TableStrings;
const State = table_strings.State;

const io = std.testing.io;

const schema = types.TableSchema{
    .columns = &.{
        .{ .name = "id", .type = .int },
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
    return ts.resolve(io, dir, 4, url_column, Values{ .items = items }, ids, table_strings.admit_all);
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

const fixed_schema = types.TableSchema{
    .columns = &.{
        .{ .name = "id", .type = .int },
        .{ .name = "user", .type = .bigint },
    },
    .order_key = &.{"id"},
    .unique = false,
};
const user_column = 1;

const FixedValues = struct {
    items: []const i64,

    pub fn len(self: FixedValues) usize {
        return self.items.len;
    }

    pub fn get(self: FixedValues, i: usize) []const u8 {
        return std.mem.sliceAsBytes(self.items[i..][0..1]);
    }
};

const AdmitNone = struct {
    pub fn admits(_: AdmitNone, _: usize, _: u32) bool {
        return false;
    }
};

fn userValue(i: usize) i64 {
    return @as(i64, @intCast(i)) * 7_919 - 1_000_000_000_007;
}

test "table strings keep fixed-width values in pages a reader pins" {
    const memory = @import("../memory.zig");
    const storage_cache = @import("cache.zig");
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var ts = try TableStrings.init(allocator, fixed_schema, &.{});
    defer ts.deinit();
    try std.testing.expectEqual(@as(?u8, 8), ts.fixedListWidth(user_column));

    // About five pages of values.
    const n = 40_000;
    const values = try allocator.alloc(i64, n);
    defer allocator.free(values);
    for (values, 0..) |*v, i| v.* = userValue(i);
    const ids = try allocator.alloc(u32, n);
    defer allocator.free(ids);
    try std.testing.expect(try ts.resolve(io, tmp.dir, 4, user_column, FixedValues{ .items = values }, ids, table_strings.admit_all));
    for (ids, 0..) |id, i| try std.testing.expectEqual(@as(u32, @intCast(i)), id);

    // A rejected batch takes no ids and leaves the column open.
    var extra = [_]i64{ userValue(n), userValue(0) };
    var two: [2]u32 = undefined;
    try std.testing.expect(!try ts.resolve(io, tmp.dir, 4, user_column, FixedValues{ .items = &extra }, &two, AdmitNone{}));
    try std.testing.expect(!ts.state(io, user_column).?.closed);

    const states = try ts.persist(allocator, io, tmp.dir, true);
    defer allocator.free(states);
    try std.testing.expectEqualSlices(State, &.{.{ .column_idx = user_column, .id_count = n, .closed = false }}, states);
    ts.deinit();
    ts = try TableStrings.init(allocator, fixed_schema, states);

    var pool = memory.MemoryPool.init(1 << 24);
    var cache = storage_cache.Cache.init(allocator, &pool, 0);
    pool.shareCache(&cache, pool.budget);
    defer cache.deinit();
    ts.attach(io, &tmp.dir, &cache);

    const probe = [_]u32{ 39_999, 0, 17, 8_191, 8_192, 20_001, 17 };
    var got: [probe.len]i64 = undefined;
    try ts.fixedValues(io, user_column, &probe, std.mem.sliceAsBytes(&got));
    for (probe, got) |id, v| try std.testing.expectEqual(userValue(id), v);

    // The reopened writer finds the old values and continues the ids.
    try std.testing.expect(try ts.resolve(io, tmp.dir, 4, user_column, FixedValues{ .items = &extra }, &two, table_strings.admit_all));
    try std.testing.expectEqualSlices(u32, &.{ n, 0 }, &two);
}

test "table strings find a fixed-width value's id through the pages whose span holds it" {
    const memory = @import("../memory.zig");
    const storage_cache = @import("cache.zig");
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var ts = try TableStrings.init(allocator, fixed_schema, &.{});
    defer ts.deinit();

    // Two writes of sorted values over one range, each a few pages.
    const n = 20_000;
    const values = try allocator.alloc(i64, 2 * n);
    defer allocator.free(values);
    for (values[0..n], 0..) |*v, i| v.* = userValue(2 * i);
    for (values[n..], 0..) |*v, i| v.* = userValue(2 * i + 1);
    const ids = try allocator.alloc(u32, 2 * n);
    defer allocator.free(ids);
    try std.testing.expect(try ts.resolve(io, tmp.dir, 4, user_column, FixedValues{ .items = values[0..n] }, ids[0..n], table_strings.admit_all));
    var states = try ts.persist(allocator, io, tmp.dir, true);
    allocator.free(states);
    try std.testing.expect(try ts.resolve(io, tmp.dir, 4, user_column, FixedValues{ .items = values[n..] }, ids[n..], table_strings.admit_all));
    states = try ts.persist(allocator, io, tmp.dir, true);
    defer allocator.free(states);

    var pool = memory.MemoryPool.init(1 << 24);
    var cache = storage_cache.Cache.init(allocator, &pool, 0);
    pool.shareCache(&cache, pool.budget);
    defer cache.deinit();
    const probe = [_]i64{ userValue(0), userValue(2 * n - 1), userValue(12_345), userValue(30_000), userValue(2 * n), userValue(0) - 1, std.math.minInt(i64) };
    const want = [_]?u32{ 0, 2 * n - 1, n + 6_172, 15_000, null, null, null };
    // Spans as written, then as a reopened writer rebuilt them from pages
    // into a `.dir` cut short.
    for (0..2) |round| {
        ts.deinit();
        ts = try TableStrings.init(allocator, fixed_schema, states);
        ts.attach(io, &tmp.dir, &cache);
        var got: [probe.len]?u32 = undefined;
        try ts.fixedIdsOf(io, user_column, &probe, &got);
        try std.testing.expectEqualSlices(?u32, &want, &got);
        cache.purgeTable(ts.reader.?.uid);
        if (round == 0) {
            var strings_dir = try tmp.dir.openDir(io, table_strings.dir_name, .{});
            defer strings_dir.close(io);
            const dir_file = try strings_dir.openFile(io, std.fmt.comptimePrint("{d}.dir", .{user_column}), .{ .mode = .read_write });
            defer dir_file.close(io);
            try dir_file.setLength(io, 0);
        }
        var one: [1]u32 = undefined;
        try std.testing.expect(try ts.resolve(io, tmp.dir, 4, user_column, FixedValues{ .items = values[5..6] }, &one, table_strings.admit_all));
        try std.testing.expectEqual(@as(u32, 5), one[0]);
    }
}
