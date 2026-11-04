//! Exact group keys under real digest collisions. No two keys sharing a full
//! 128-bit digest are known (a search takes ~2^64 digests), so
//! THINDB_V2_KEY_DIGEST_BITS narrows the digest each row stages to 32 bits,
//! where a birthday search finds a pair in ~2^16 digests of the very function
//! and seed the engine takes (tests run under digest seed 0). Without exact
//! keys the pair merges into one group, which proves the narrowed digest
//! reached the group table; with them, each key keeps a group of its own.

const std = @import("std");
const builtin = @import("builtin");
const thindb = @import("thindb");
const helpers = @import("sql_helpers.zig");
const runSql = helpers.runSql;
const exec = helpers.exec;

const DIGEST_BITS = 32;
const Digest = @Int(.unsigned, DIGEST_BITS);
const FILLER_KEYS = 200;
/// A filler key the dictionary sorts between others, so a filter passing
/// every key but it leaves a gap in a row group's used codes.
const GAP_KEY = "f5";

extern "c" fn _putenv_s(name: [*:0]const u8, value: [*:0]const u8) c_int;
extern "c" fn setenv(name: [*:0]const u8, value: [*:0]const u8, overwrite: c_int) c_int;

fn setEnv(name: [*:0]const u8, value: [*:0]const u8) void {
    if (builtin.os.tag == .windows) {
        _ = _putenv_s(name, value);
    } else {
        _ = setenv(name, value, 1);
    }
}

fn setMode(exact: bool) void {
    setEnv("THINDB_V2_KEY_DIGEST_BITS", std.fmt.comptimePrint("{d}", .{DIGEST_BITS}));
    setEnv("THINDB_EXACT_GROUP_KEYS", if (exact) "1" else "0");
}

fn clearMode() void {
    setEnv("THINDB_V2_KEY_DIGEST_BITS", "");
    setEnv("THINDB_EXACT_GROUP_KEYS", "");
}

/// The first `i < j` whose `digestOf` agree in their low DIGEST_BITS.
fn collidingPair(allocator: std.mem.Allocator, comptime digestOf: fn (u32) u128) ![2]u32 {
    var seen: std.AutoHashMapUnmanaged(Digest, u32) = .empty;
    defer seen.deinit(allocator);
    var j: u32 = 0;
    while (true) : (j += 1) {
        const entry = try seen.getOrPut(allocator, @truncate(digestOf(j)));
        if (entry.found_existing) return .{ entry.value_ptr.*, j };
        entry.value_ptr.* = j;
    }
}

fn keyName(buf: []u8, i: u32) []const u8 {
    return std.fmt.bufPrint(buf, "k{d}", .{i}) catch unreachable;
}

fn stringDigest(i: u32) u128 {
    var buf: [16]u8 = undefined;
    return thindb.exec.stringKeyDigest(keyName(&buf, i));
}

/// A `(g BIGINT, s VARCHAR)` key's identity, for `g = i` and `s = "c"`.
fn compositeDigest(i: u32) u128 {
    return thindb.exec.keyPartsDigest(2, .{ @as(u64, i), thindb.exec.stringKeyDigest("c") });
}

const Group = struct { g: i64, s: []const u8, count: i64 };

/// The table's groups: the string pair's keys with 3 and 2 rows, the
/// composite pair's with 4 and 1, and one row per filler key.
fn groupsOf(allocator: std.mem.Allocator, string_pair: [2]u32, composite_pair: [2]u32) ![]Group {
    var groups: std.ArrayList(Group) = .empty;
    errdefer {
        for (groups.items) |grp| allocator.free(grp.s);
        groups.deinit(allocator);
    }
    var buf: [16]u8 = undefined;
    try groups.append(allocator, .{ .g = 0, .s = try allocator.dupe(u8, keyName(&buf, string_pair[0])), .count = 3 });
    try groups.append(allocator, .{ .g = 0, .s = try allocator.dupe(u8, keyName(&buf, string_pair[1])), .count = 2 });
    try groups.append(allocator, .{ .g = composite_pair[0], .s = try allocator.dupe(u8, "c"), .count = 4 });
    try groups.append(allocator, .{ .g = composite_pair[1], .s = try allocator.dupe(u8, "c"), .count = 1 });
    for (0..FILLER_KEYS) |f| {
        try groups.append(allocator, .{ .g = 0, .s = try std.fmt.allocPrint(allocator, "f{d}", .{f}), .count = 1 });
    }
    return groups.toOwnedSlice(allocator);
}

fn freeGroups(allocator: std.mem.Allocator, groups: []Group) void {
    for (groups) |grp| allocator.free(grp.s);
    allocator.free(groups);
}

/// Inserts one copy of every group's rows, ids from `first_id` on; returns
/// the next free id.
fn insertCopy(allocator: std.mem.Allocator, db: *thindb.Database, groups: []const Group, first_id: usize) !usize {
    var sql: std.ArrayList(u8) = .empty;
    defer sql.deinit(allocator);
    try sql.appendSlice(allocator, "INSERT INTO t (id, g, s) VALUES ");
    var id = first_id;
    // Interleaves the groups' rows, so each pair's rows meet in every chunk.
    var round: i64 = 0;
    while (round < 4) : (round += 1) {
        for (groups) |grp| {
            if (round >= grp.count) continue;
            if (id > first_id) try sql.appendSlice(allocator, ", ");
            try sql.print(allocator, "({d}, {d}, '{s}')", .{ id, grp.g, grp.s });
            id += 1;
        }
    }
    try exec(allocator, db, sql.items);
    return id;
}

fn resultKey(allocator: std.mem.Allocator, g: i64, s: []const u8) ![]u8 {
    return std.fmt.allocPrint(allocator, "{d}/{s}", .{ g, s });
}

/// `sql`'s `(…, COUNT(*))` rows, keyed by `resultKey` (`g` = 0 when the
/// query doesn't group by it).
fn runGroups(allocator: std.mem.Allocator, db: *thindb.Database, sql: []const u8) !std.StringHashMapUnmanaged(i64) {
    var q = try runSql(allocator, db, sql);
    defer q.deinit();
    var out: std.StringHashMapUnmanaged(i64) = .empty;
    errdefer freeResult(allocator, &out);
    while (try q.next()) |batch| {
        const with_g = batch.values.len == 3;
        for (0..batch.row_count) |r| {
            const g: i64 = if (with_g) batch.values[0].data.bigint[r] else 0;
            const s = switch (batch.values[@intFromBool(with_g)].data) {
                .string, .varchar => |sv| sv.rowBytes(r),
                else => return error.TestUnexpectedType,
            };
            const key = try resultKey(allocator, g, s);
            const entry = out.getOrPut(allocator, key) catch |err| {
                allocator.free(key);
                return err;
            };
            if (entry.found_existing) {
                allocator.free(key);
                return error.TestUnexpectedResult;
            }
            entry.value_ptr.* = batch.values[batch.values.len - 1].data.bigint[r];
        }
    }
    return out;
}

fn freeResult(allocator: std.mem.Allocator, result: *std.StringHashMapUnmanaged(i64)) void {
    var it = result.keyIterator();
    while (it.next()) |k| allocator.free(k.*);
    result.deinit(allocator);
}

/// Checks one GROUP BY of a table holding `copies` copies of `groups`' rows:
/// `collide` are the indexes in `groups` of the pair whose keys share a
/// narrowed digest, and `with_g` whether the query groups by `g` as well as
/// `s` (a lone `s` groups the composite pair's "c" rows as one key).
fn expectGroups(allocator: std.mem.Allocator, db: *thindb.Database, sql: []const u8, groups: []const Group, copies: i64, collide: [2]usize, with_g: bool) !void {
    // Exact keys: every group apart, each with its own count.
    {
        setMode(true);
        defer clearMode();
        var result = try runGroups(allocator, db, sql);
        defer freeResult(allocator, &result);
        var expected: std.StringHashMapUnmanaged(i64) = .empty;
        defer freeResult(allocator, &expected);
        for (groups) |grp| {
            const key = try resultKey(allocator, if (with_g) grp.g else 0, grp.s);
            const entry = expected.getOrPut(allocator, key) catch |err| {
                allocator.free(key);
                return err;
            };
            if (entry.found_existing) {
                allocator.free(key);
                entry.value_ptr.* += copies * grp.count;
            } else entry.value_ptr.* = copies * grp.count;
        }
        try std.testing.expectEqual(expected.count(), result.count());
        var it = expected.iterator();
        while (it.next()) |want| try std.testing.expectEqual(want.value_ptr.*, result.get(want.key_ptr.*) orelse return error.TestExpectedEqual);
    }
    // Digest keys: the colliding pair shares one group, under either key,
    // with both keys' rows.
    {
        setMode(false);
        defer clearMode();
        var result = try runGroups(allocator, db, sql);
        defer freeResult(allocator, &result);
        var counts: [2]?i64 = undefined;
        for (&counts, collide) |*count, at| {
            const key = try resultKey(allocator, if (with_g) groups[at].g else 0, groups[at].s);
            defer allocator.free(key);
            count.* = result.get(key);
        }
        try std.testing.expect((counts[0] == null) != (counts[1] == null));
        try std.testing.expectEqual(copies * (groups[collide[0]].count + groups[collide[1]].count), (counts[0] orelse counts[1]).?);
    }
}

/// A HAVING each key of the string pair meets but their pooled rows don't:
/// exact keys return both, each with its own count, and digest keys neither.
fn expectHavingPair(allocator: std.mem.Allocator, db: *thindb.Database, groups: []const Group, copies: i64) !void {
    const sql = try std.fmt.allocPrint(allocator, "SELECT s, COUNT(*) FROM t GROUP BY s HAVING COUNT(*) BETWEEN {d} AND {d}", .{ 2 * copies, 3 * copies });
    defer allocator.free(sql);
    inline for (.{ true, false }) |exact| {
        setMode(exact);
        defer clearMode();
        var result = try runGroups(allocator, db, sql);
        defer freeResult(allocator, &result);
        try std.testing.expectEqual(@as(usize, if (exact) 2 else 0), result.count());
        if (exact) for (groups[0..2]) |grp| {
            const key = try resultKey(allocator, 0, grp.s);
            defer allocator.free(key);
            try std.testing.expectEqual(copies * grp.count, result.get(key) orelse return error.TestExpectedEqual);
        };
    }
}

test "exact group keys keep keys whose digests collide in groups of their own" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const groups = try groupsOf(allocator, try collidingPair(allocator, stringDigest), try collidingPair(allocator, compositeDigest));
    defer freeGroups(allocator, groups);
    var gapped: std.ArrayList(Group) = .empty;
    defer gapped.deinit(allocator);
    for (groups) |grp| {
        if (!std.mem.eql(u8, grp.s, GAP_KEY)) try gapped.append(allocator, grp);
    }

    const db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    try exec(allocator, db, "CREATE TABLE t (id BIGINT PRIMARY KEY, g BIGINT NOT NULL, s VARCHAR(16) NOT NULL)");

    // Memtable rows alone, then a segment's beside a memtable's: rows still
    // in the memtable keep the lone key off the lowcard path, which groups a
    // segment's strings by their bytes and never by a digest.
    var next_id = try insertCopy(allocator, db, groups, 0);
    inline for (.{ 1, 2 }) |copies| {
        if (copies == 2) {
            try (try db.openTable("t", .{})).flush();
            next_id = try insertCopy(allocator, db, groups, next_id);
        }
        try expectGroups(allocator, db, "SELECT s, COUNT(*) FROM t GROUP BY s", groups, copies, .{ 0, 1 }, false);
        try expectGroups(allocator, db, "SELECT g, s, COUNT(*) FROM t GROUP BY g, s", groups, copies, .{ 2, 3 }, true);
        try expectGroups(allocator, db, "SELECT s, COUNT(*) FROM t WHERE s <> '" ++ GAP_KEY ++ "' GROUP BY s", gapped.items, copies, .{ 0, 1 }, false);
        try expectHavingPair(allocator, db, groups, copies);
    }
}

test "exact group keys keep colliding values apart in the count top-K by segment ids" {
    // Segments alone, past the lowcard engine's groups in fillers held twice
    // each, so the count top-K runs on the segment string ids. Its two best
    // values share a narrowed digest and every row group of every segment.
    const allocator = std.testing.allocator;
    const sql = "SELECT s, COUNT(*) AS c FROM ids GROUP BY s ORDER BY c DESC LIMIT 2";
    const rg_rows = 1024;
    const rg_count = 64;
    const segment_count = 3;
    const pair = try collidingPair(allocator, stringDigest);
    var names: [2][16]u8 = undefined;
    const keys = [2][]const u8{ keyName(&names[0], pair[0]), keyName(&names[1], pair[1]) };
    const per_rg = [2]usize{ 4, 3 };

    var arena_state = std.heap.ArenaAllocator.init(allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();
    const Row = struct { id: i64, s: []const u8 };

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const db = try thindb.Database.open(allocator, std.testing.io, tmp.dir, .{ .auto_flush_secs = 0 });
    defer db.close();
    const t = try db.table("ids", .{
        .columns = &.{ .{ .name = "id", .type = .bigint }, .{ .name = "s", .type = .string } },
        .order_key = &.{"id"},
        .unique = false,
    }, .{ .order_key = &.{"id"}, .row_group_size = rg_rows });
    var id: i64 = 0;
    for (0..segment_count) |seg| {
        var rows: std.ArrayList(Row) = .empty;
        for (0..rg_count) |rg| {
            for (keys, per_rg) |key, n| {
                for (0..n) |_| {
                    try rows.append(arena, .{ .id = id, .s = key });
                    id += 1;
                }
            }
            var f: usize = 0;
            while (rows.items.len < (rg + 1) * rg_rows) : (f += 1) {
                const filler = try std.fmt.allocPrint(arena, "f{d}-{d}-{d}", .{ seg, rg, f });
                for (0..@min(2, (rg + 1) * rg_rows - rows.items.len)) |_| {
                    try rows.append(arena, .{ .id = id, .s = filler });
                    id += 1;
                }
            }
        }
        try t.insert(rows.items);
        try t.flush();
    }

    setMode(true);
    defer clearMode();
    var plan = try runSql(allocator, db, "EXPLAIN " ++ sql);
    defer plan.deinit();
    var grouped = false;
    while (try plan.next()) |batch| {
        for (0..batch.row_count) |r| {
            if (std.mem.indexOf(u8, batch.values[0].data.string.rowBytes(r), "V2 group-topN") != null) grouped = true;
        }
    }
    try std.testing.expect(grouped);

    var result = try runGroups(allocator, db, sql);
    defer freeResult(allocator, &result);
    try std.testing.expectEqual(@as(usize, 2), result.count());
    for (keys, per_rg) |key, n| {
        const result_key = try resultKey(allocator, 0, key);
        defer allocator.free(result_key);
        try std.testing.expectEqual(@as(i64, @intCast(segment_count * rg_count * n)), result.get(result_key) orelse return error.TestExpectedEqual);
    }
}
