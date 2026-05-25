//! Compacting Filter path (incremental materialization). A non-fused Filter
//! over a top-level AND of ≥2 conjuncts evaluates conjuncts one at a time and,
//! when the cost gate fires, materializes survivors densely so the remaining
//! conjuncts scan only the survivors — possibly more than once (ping-pong
//! buffers). Results must be identical to a brute-force reference.
//!
//! The query goes through the builder as scan → project → filter: Project has
//! no `tryFuseFilter`, so the Filter is NOT fused into the Scan and actually
//! runs `nextCompacting`. Row counts are above `COMPACT_MIN_ROWS` with a
//! selective leading conjunct so compaction triggers.

const std = @import("std");
const thindb = @import("thindb");
const helpers = @import("sql_helpers.zig");
const exec = helpers.exec;

fn insertChunked(allocator: std.mem.Allocator, db: anytype, n: i64) !void {
    var id: i64 = 1;
    while (id <= n) {
        var buf: std.ArrayList(u8) = .empty;
        defer buf.deinit(allocator);
        try buf.appendSlice(allocator, "INSERT INTO t (id, a, b, c, d) VALUES ");
        const chunk_end = @min(id + 2000, n + 1);
        var first = true;
        while (id < chunk_end) : (id += 1) {
            if (!first) try buf.appendSlice(allocator, ", ");
            first = false;
            var line: [96]u8 = undefined;
            const s = try std.fmt.bufPrint(&line, "({d}, {d}, {d}, {d}, {d})", .{
                id, @mod(id, 100), id, @mod(id, 7), @mod(id, 13),
            });
            try buf.appendSlice(allocator, s);
        }
        try exec(allocator, db, buf.items);
    }
}

fn setup(allocator: std.mem.Allocator, io: anytype, dir: anytype, n: i64) !*thindb.Database {
    const db = try thindb.Database.open(allocator, io, dir, .{});
    errdefer db.close();
    try exec(allocator, db,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, a INT NOT NULL, b BIGINT NOT NULL, c INT NOT NULL, d INT NOT NULL)",
    );
    try insertChunked(allocator, db, n);
    const t = try db.openTable("t", .{});
    try t.flush();
    return db;
}

fn collectIds(allocator: std.mem.Allocator, q: *thindb.Query) ![]i64 {
    var ids: std.ArrayListUnmanaged(i64) = .empty;
    errdefer ids.deinit(allocator);
    while (try q.next()) |b| {
        const view = b.values[0];
        for (0..b.row_count) |i| {
            if (view.isValid(i)) try ids.append(allocator, view.data.bigint[i]);
        }
    }
    const owned = try ids.toOwnedSlice(allocator);
    std.mem.sort(i64, owned, {}, std.sort.asc(i64));
    return owned;
}

fn expected(allocator: std.mem.Allocator, n: i64, pred: *const fn (i64) bool) ![]i64 {
    var ids: std.ArrayListUnmanaged(i64) = .empty;
    errdefer ids.deinit(allocator);
    var id: i64 = 1;
    while (id <= n) : (id += 1) {
        if (pred(id)) try ids.append(allocator, id);
    }
    return ids.toOwnedSlice(allocator);
}

fn pred3(id: i64) bool {
    return @mod(id, 100) < 5 and id > 1000 and @mod(id, 7) != 0;
}

fn pred4(id: i64) bool {
    return @mod(id, 100) < 30 and id > 5000 and @mod(id, 7) != 0 and @mod(id, 13) != 0;
}

test "compacting filter: 3-conjunct AND matches brute force (single compaction)" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, io, tmp.dir, 2000);
    defer db.close();

    const t = try db.openTable("t", .{});
    var base = try thindb.scan(allocator, t);
    var proj = try base.project(&.{ "id", "a", "b", "c", "d" });
    const conjuncts = [_]thindb.exec.PredicateExpr{
        thindb.leafExpr("a", .lt, .{ .int = 5 }),
        thindb.leafExpr("b", .gt, .{ .bigint = 1000 }),
        thindb.leafExpr("c", .neq, .{ .int = 0 }),
    };
    var q = try proj.filter(.{ .@"and" = &conjuncts });
    defer q.deinit();

    const got = try collectIds(allocator, &q);
    defer allocator.free(got);
    const want = try expected(allocator, 2000, pred3);
    defer allocator.free(want);
    try std.testing.expectEqualSlices(i64, want, got);
}

test "compacting filter: 4-conjunct AND matches brute force (multiple compactions)" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, io, tmp.dir, 20000);
    defer db.close();

    const t = try db.openTable("t", .{});
    var base = try thindb.scan(allocator, t);
    var proj = try base.project(&.{ "id", "a", "b", "c", "d" });
    const conjuncts = [_]thindb.exec.PredicateExpr{
        thindb.leafExpr("a", .lt, .{ .int = 30 }),
        thindb.leafExpr("b", .gt, .{ .bigint = 5000 }),
        thindb.leafExpr("c", .neq, .{ .int = 0 }),
        thindb.leafExpr("d", .neq, .{ .int = 0 }),
    };
    var q = try proj.filter(.{ .@"and" = &conjuncts });
    defer q.deinit();

    const got = try collectIds(allocator, &q);
    defer allocator.free(got);
    const want = try expected(allocator, 20000, pred4);
    defer allocator.free(want);
    try std.testing.expectEqualSlices(i64, want, got);
}
