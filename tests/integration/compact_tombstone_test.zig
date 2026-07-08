//! Regression for #136 — a double-free in the compactor's segment-cache
//! retirement, exposed when compaction merges segments that carry tombstones
//! from upserts (the shape a continuous CDC upsert feed produces). The bug is a
//! RACE: it only fires with the background compactor running concurrently with
//! the write path (a single-threaded compact is fine), so this drives a
//! background-compact loop against a continuous upsert stream.

const std = @import("std");
const thindb = @import("thindb");
const helpers = @import("sql_helpers.zig");
const exec = helpers.exec;
const runSql = helpers.runSql;

const Stop = std.atomic.Value(bool);

fn compactLoop(db: *thindb.Database, stop: *Stop) void {
    while (!stop.load(.acquire)) {
        _ = db.backgroundCompactSweep() catch {};
    }
}

test "background compactor races upsert tombstoning without double-free (#136)" {
    var gpa: std.heap.DebugAllocator(.{ .thread_safe = true }) = .{};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var db = try thindb.Database.open(allocator, io, tmp.dir, .{
        .compact_min_segments = 3, // compact aggressively so the loop keeps merging
        .compact_tombstone_threshold = 0.1,
        .auto_flush_secs = 0,
        .auto_flush_rows = 1_000_000,
        .auto_flush_bytes = 64 * 1024 * 1024,
    });
    defer db.close();

    try exec(allocator, db, "CREATE TABLE t (id INT NOT NULL, v INT NOT NULL, PRIMARY KEY (id))");
    const tbl = try db.openTable("t", .{});

    var stop = Stop.init(false);
    const th = try std.Thread.spawn(.{}, compactLoop, .{ db, &stop });
    defer th.join();
    defer stop.store(true, .release);

    // Continuous upserts of the same keys → each flush creates a segment and
    // tombstones the prior copies; the background loop merges them concurrently.
    // The SELECT runs on a query-lifetime arena (like every server query) — it
    // must not poison the table-lifetime tombstone cache with arena memory.
    var round: usize = 0;
    while (round < 300) : (round += 1) {
        try exec(allocator, db, "INSERT INTO t (id, v) VALUES (1,0),(2,0),(3,0),(4,0),(5,0),(6,0),(7,0),(8,0)");
        try tbl.flush();

        var arena = std.heap.ArenaAllocator.init(allocator);
        defer arena.deinit();
        // WHERE forces a real segment scan (COUNT(*)/SUM alone are served by
        // metadata lanes and would never touch the tombstone cache).
        var q = try runSql(arena.allocator(), db, "SELECT COUNT(*) FROM t WHERE v = 0");
        defer q.deinit();
        while (try q.next()) |_| {}
    }
}
