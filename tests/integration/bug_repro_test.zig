//! Repro for the "second SELECT returns empty" bug surfaced by Bun
//! complex-query tests (task #169). The wire path doesn't flush
//! between INSERT and SELECT, and a second SELECT against the same
//! memtable comes back empty.

const std = @import("std");
const thindb = @import("thindb");

test "BUG: second SELECT against unflushed memtable returns empty" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    // CREATE + INSERT via SQL — same path the wire uses.
    {
        var arena = std.heap.ArenaAllocator.init(allocator);
        defer arena.deinit();
        const root = try thindb.sql.parse(
            arena.allocator(),
            "CREATE TABLE t (id BIGINT PRIMARY KEY, v INT NOT NULL)",
        );
        var cq = try thindb.net.compile(allocator, db, root);
        defer cq.deinit();
        _ = try cq.next();
    }
    {
        var arena = std.heap.ArenaAllocator.init(allocator);
        defer arena.deinit();
        const root = try thindb.sql.parse(
            arena.allocator(),
            "INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)",
        );
        var cq = try thindb.net.compile(allocator, db, root);
        defer cq.deinit();
        _ = try cq.next();
    }

    // First SELECT — expect 3 rows.
    var first_count: usize = 0;
    {
        var arena = std.heap.ArenaAllocator.init(allocator);
        defer arena.deinit();
        const root = try thindb.sql.parse(arena.allocator(), "SELECT id FROM t");
        var cq = try thindb.net.compile(allocator, db, root);
        defer cq.deinit();
        while (try cq.next()) |b| first_count += b.row_count;
    }
    try std.testing.expectEqual(@as(usize, 3), first_count);

    // Second SELECT — should also return 3. If this is 0, the bug is
    // reproduced at the engine level (below the wire).
    var second_count: usize = 0;
    {
        var arena = std.heap.ArenaAllocator.init(allocator);
        defer arena.deinit();
        const root = try thindb.sql.parse(arena.allocator(), "SELECT id FROM t");
        var cq = try thindb.net.compile(allocator, db, root);
        defer cq.deinit();
        while (try cq.next()) |b| second_count += b.row_count;
    }
    try std.testing.expectEqual(@as(usize, 3), second_count);
}
