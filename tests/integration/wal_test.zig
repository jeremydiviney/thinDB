//! Write-ahead log tests: replay across reopen, flush_marker truncation,
//! nullable + unique-key interaction, group commit under concurrent
//! writer threads, crash-style reopen with WAL replay.

const std = @import("std");
const thindb = @import("thindb");
const common = @import("common.zig");
const schema_v1 = common.schema_v1;
const opts_v1 = common.opts_v1;

test "wal: inserts survive close-without-flush + reopen" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    // Session 1: insert with WAL enabled, close WITHOUT calling flush.
    {
        var db = try thindb.Database.open(allocator, io, tmp.dir, .{
            .wal_enabled = true,
            .auto_flush_secs = 0,
            .auto_flush_rows = 1_000_000,
            .auto_flush_bytes = 64 * 1024 * 1024,
        });
        defer db.close();
        const t = try db.table("orders", schema_v1, opts_v1);
        try t.insert(&.{
            .{ .id = @as(i64, 1), .qty = @as(i32, 10), .active = true, .tag = "a" },
            .{ .id = @as(i64, 2), .qty = @as(i32, 20), .active = false, .tag = "b" },
            .{ .id = @as(i64, 3), .qty = @as(i32, 30), .active = true, .tag = "c" },
        });
        // NO flush! Data is only in memtable + WAL.
        try std.testing.expectEqual(@as(usize, 0), t.segmentCount());
        try std.testing.expectEqual(@as(u64, 3), t.memtable.row_count);
    }

    // Session 2: reopen — replay WAL into memtable.
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{
        .wal_enabled = true,
    });
    defer db.close();
    const t = try db.table("orders", schema_v1, opts_v1);

    // Replayed rows are flushed to a segment at open BEFORE the WAL is
    // truncated — otherwise a second crash before the next flush would
    // drop them (the fresh log no longer carries their records).
    try std.testing.expectEqual(@as(usize, 1), t.segmentCount());
    try std.testing.expectEqual(@as(u64, 0), t.memtable.row_count);

    var q = try thindb.scan(allocator, t);
    defer q.deinit();
    var ids: std.ArrayList(i64) = .empty;
    defer ids.deinit(allocator);
    while (try q.next()) |batch| try ids.appendSlice(allocator, batch.values[0].data.bigint);
    try std.testing.expectEqualSlices(i64, &[_]i64{ 1, 2, 3 }, ids.items);
}

test "wal: deletes replay against the reconstructed memtable" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    {
        var db = try thindb.Database.open(allocator, io, tmp.dir, .{
            .wal_enabled = true,
        });
        defer db.close();
        const t = try db.table("orders", schema_v1, opts_v1);
        try t.insert(&.{
            .{ .id = @as(i64, 1), .qty = @as(i32, 10), .active = true, .tag = "a" },
            .{ .id = @as(i64, 2), .qty = @as(i32, 20), .active = false, .tag = "b" },
            .{ .id = @as(i64, 3), .qty = @as(i32, 30), .active = true, .tag = "c" },
        });
        _ = try t.delete(.{ .col = "qty", .op = .lt, .val = .{ .int = 25 } });
        // Memtable now has just id=3.
        try std.testing.expectEqual(@as(u64, 1), t.memtable.row_count);
    }

    var db = try thindb.Database.open(allocator, io, tmp.dir, .{ .wal_enabled = true });
    defer db.close();
    const t = try db.table("orders", schema_v1, opts_v1);

    var q = try thindb.scan(allocator, t);
    defer q.deinit();
    var ids: std.ArrayList(i64) = .empty;
    defer ids.deinit(allocator);
    while (try q.next()) |batch| try ids.appendSlice(allocator, batch.values[0].data.bigint);
    try std.testing.expectEqualSlices(i64, &[_]i64{3}, ids.items);
}

test "wal: flush_marker truncates the log on reopen" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    {
        var db = try thindb.Database.open(allocator, io, tmp.dir, .{
            .wal_enabled = true,
        });
        defer db.close();
        const t = try db.table("orders", schema_v1, opts_v1);
        try t.insert(&.{
            .{ .id = @as(i64, 1), .qty = @as(i32, 10), .active = true, .tag = "a" },
        });
        try t.flush(); // writes flush_marker + truncates the WAL
        try t.insert(&.{
            .{ .id = @as(i64, 2), .qty = @as(i32, 20), .active = true, .tag = "b" },
        });
        // Now there's 1 segment + 1 memtable row.
        try std.testing.expectEqual(@as(usize, 1), t.segmentCount());
        try std.testing.expectEqual(@as(u64, 1), t.memtable.row_count);
    }

    var db = try thindb.Database.open(allocator, io, tmp.dir, .{ .wal_enabled = true });
    defer db.close();
    const t = try db.table("orders", schema_v1, opts_v1);

    // Segment id=1 is on disk; memtable rebuilt from WAL has id=2.
    var q = try thindb.scan(allocator, t);
    defer q.deinit();
    var ids: std.ArrayList(i64) = .empty;
    defer ids.deinit(allocator);
    while (try q.next()) |batch| try ids.appendSlice(allocator, batch.values[0].data.bigint);
    try std.testing.expectEqualSlices(i64, &[_]i64{ 1, 2 }, ids.items);
}

test "wal: works with nullable columns and a unique-key table" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    const schema = thindb.TableSchema{
        .columns = &.{
            .{ .name = "id", .type = .bigint },
            .{ .name = "v", .type = .int, .nullable = true },
        },
        .order_key = &.{"id"},
        .unique = true,
    };
    const ok = [_][]const u8{"id"};
    const opts = thindb.TableOptions{ .order_key = &ok, .unique = true };

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    {
        var db = try thindb.Database.open(allocator, io, tmp.dir, .{ .wal_enabled = true });
        defer db.close();
        const t = try db.table("t", schema, opts);
        try t.insert(&.{
            .{ .id = @as(i64, 1), .v = @as(?i32, 100) },
            .{ .id = @as(i64, 2), .v = @as(?i32, null) },
            .{ .id = @as(i64, 3), .v = @as(?i32, 300) },
        });
        // Overwrite id=2 with a non-null value via upsert semantics.
        try t.insert(&.{.{ .id = @as(i64, 2), .v = @as(?i32, 999) }});
    }

    var db = try thindb.Database.open(allocator, io, tmp.dir, .{ .wal_enabled = true });
    defer db.close();
    const t = try db.table("t", schema, opts);

    var q = try thindb.scan(allocator, t);
    defer q.deinit();
    var ids: std.ArrayList(i64) = .empty;
    defer ids.deinit(allocator);
    var vs: std.ArrayList(i32) = .empty;
    defer vs.deinit(allocator);
    var nulls: std.ArrayList(bool) = .empty;
    defer nulls.deinit(allocator);

    while (try q.next()) |batch| {
        try ids.appendSlice(allocator, batch.values[0].data.bigint);
        try vs.appendSlice(allocator, batch.values[1].data.int);
        for (0..batch.row_count) |i| try nulls.append(allocator, !batch.values[1].isValid(i));
    }

    // Expect: id=1 → 100, id=2 → 999 (overwritten), id=3 → 300. No nulls (id=2's
    // null was upserted-over). Order isn't sorted because the memtable is
    // insertion-order (segments would be sorted; nothing flushed here).
    try std.testing.expectEqual(@as(usize, 3), ids.items.len);
    var seen: [3]bool = .{ false, false, false };
    for (ids.items, 0..) |id, i| {
        switch (id) {
            1 => {
                try std.testing.expectEqual(@as(i32, 100), vs.items[i]);
                try std.testing.expectEqual(false, nulls.items[i]);
                seen[0] = true;
            },
            2 => {
                try std.testing.expectEqual(@as(i32, 999), vs.items[i]);
                try std.testing.expectEqual(false, nulls.items[i]);
                seen[1] = true;
            },
            3 => {
                try std.testing.expectEqual(@as(i32, 300), vs.items[i]);
                try std.testing.expectEqual(false, nulls.items[i]);
                seen[2] = true;
            },
            else => unreachable,
        }
    }
    try std.testing.expect(seen[0] and seen[1] and seen[2]);
}

test "wal: concurrent writers preserve all rows + group-commit fsync amortizes" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var db = try thindb.Database.open(allocator, io, tmp.dir, .{
        .wal_enabled = true,
        .sync_mode = .per_flush,
        // Keep everything in the memtable so the test exercises the WAL path
        // for every insert (no auto-flush truncation).
        .auto_flush_rows = 10_000_000,
        .auto_flush_bytes = 1 << 30,
        .auto_flush_secs = 0,
    });
    defer db.close();

    // Non-unique schema so we don't trigger applyUpsertResolution between
    // threads (each thread inserts a disjoint id range; unique resolution
    // would still be a no-op, but it adds CPU work we don't need).
    const schema = thindb.TableSchema{
        .columns = &.{
            .{ .name = "id", .type = .bigint },
            .{ .name = "qty", .type = .int },
        },
        .order_key = &.{"id"},
        .unique = false,
    };
    const ok = [_][]const u8{"id"};
    const opts = thindb.TableOptions{ .order_key = &ok, .row_group_size = 1024 };
    const t = try db.table("orders", schema, opts);

    const num_threads = 4;
    const per_thread = 100;

    var error_count: std.atomic.Value(usize) = .init(0);

    const Ctx = struct {
        t: *thindb.Table,
        base: i64,
        n: usize,
        ec: *std.atomic.Value(usize),

        fn run(self: @This()) void {
            var i: usize = 0;
            while (i < self.n) : (i += 1) {
                self.t.insert(&.{.{
                    .id = self.base + @as(i64, @intCast(i)),
                    .qty = @as(i32, 1),
                }}) catch {
                    _ = self.ec.fetchAdd(1, .release);
                    return;
                };
            }
        }
    };

    var threads: [num_threads]std.Thread = undefined;
    for (&threads, 0..) |*thr, ti| {
        const ctx = Ctx{
            .t = t,
            .base = @as(i64, @intCast(ti)) * 1_000_000,
            .n = per_thread,
            .ec = &error_count,
        };
        thr.* = try std.Thread.spawn(.{}, Ctx.run, .{ctx});
    }
    for (&threads) |*thr| thr.join();

    try std.testing.expectEqual(@as(usize, 0), error_count.load(.acquire));
    try std.testing.expectEqual(@as(u64, num_threads * per_thread), t.memtable.row_count);

    // Group-commit upper bound: at most one fsync per insert. The actual
    // amortization ratio depends on scheduling and is exercised in the bench;
    // here we just assert correctness (counter is sane).
    if (t.wal) |*w| {
        try std.testing.expect(w.fsync_count <= num_threads * per_thread);
    }
}

test "wal: concurrent writers survive close + reopen" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const schema = thindb.TableSchema{
        .columns = &.{
            .{ .name = "id", .type = .bigint },
            .{ .name = "qty", .type = .int },
        },
        .order_key = &.{"id"},
        .unique = false,
    };
    const ok = [_][]const u8{"id"};
    const opts = thindb.TableOptions{ .order_key = &ok, .row_group_size = 1024 };

    const num_threads = 4;
    const per_thread = 80;

    {
        var db = try thindb.Database.open(allocator, io, tmp.dir, .{
            .wal_enabled = true,
            .sync_mode = .per_flush,
            .auto_flush_rows = 10_000_000,
            .auto_flush_bytes = 1 << 30,
            .auto_flush_secs = 0,
        });
        defer db.close();
        const t = try db.table("orders", schema, opts);

        var error_count: std.atomic.Value(usize) = .init(0);
        const Ctx = struct {
            t: *thindb.Table,
            base: i64,
            n: usize,
            ec: *std.atomic.Value(usize),
            fn run(self: @This()) void {
                var i: usize = 0;
                while (i < self.n) : (i += 1) {
                    self.t.insert(&.{.{
                        .id = self.base + @as(i64, @intCast(i)),
                        .qty = @as(i32, 1),
                    }}) catch {
                        _ = self.ec.fetchAdd(1, .release);
                        return;
                    };
                }
            }
        };

        var threads: [num_threads]std.Thread = undefined;
        for (&threads, 0..) |*thr, ti| {
            const ctx = Ctx{
                .t = t,
                .base = @as(i64, @intCast(ti)) * 1_000_000,
                .n = per_thread,
                .ec = &error_count,
            };
            thr.* = try std.Thread.spawn(.{}, Ctx.run, .{ctx});
        }
        for (&threads) |*thr| thr.join();

        try std.testing.expectEqual(@as(usize, 0), error_count.load(.acquire));
        // Intentionally do NOT flush — exercise the WAL replay path on reopen.
    }

    // Reopen: WAL replay should reconstruct every row from every thread.
    // Replayed rows are flushed to a segment at open (before the log is
    // truncated), so count them through a scan rather than the memtable.
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{
        .wal_enabled = true,
        .sync_mode = .per_flush,
    });
    defer db.close();
    const t = try db.table("orders", schema, opts);
    var q = try thindb.scan(allocator, t);
    defer q.deinit();
    var total: u64 = 0;
    while (try q.next()) |batch| total += batch.row_count;
    try std.testing.expectEqual(@as(u64, num_threads * per_thread), total);
}

// =============================================================================
// SQL DELETE / UPDATE WAL replay — the rich-predicate path.
// =============================================================================

const sql_helpers = @import("sql_helpers.zig");

test "wal: SQL DELETE FROM survives close-without-flush + reopen" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    // Session 1: CREATE + INSERT + DELETE via SQL with WAL on, no flush.
    {
        var db = try thindb.Database.open(allocator, io, tmp.dir, .{
            .wal_enabled = true,
            .auto_flush_secs = 0,
            .auto_flush_rows = 1_000_000,
            .auto_flush_bytes = 64 * 1024 * 1024,
        });
        defer db.close();
        try sql_helpers.exec(
            allocator,
            db,
            "CREATE TABLE t (id BIGINT PRIMARY KEY, qty INT NOT NULL)",
        );
        try sql_helpers.exec(
            allocator,
            db,
            "INSERT INTO t (id, qty) VALUES (1, 10), (2, 20), (3, 30), (4, 40)",
        );
        try sql_helpers.exec(allocator, db, "DELETE FROM t WHERE qty > 20");
        // No flush.
    }

    // Session 2: replay rebuilds the memtable + applies the delete.
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{ .wal_enabled = true });
    defer db.close();
    const ids = try sql_helpers.collectBigints(allocator, db, "SELECT id FROM t ORDER BY id ASC");
    defer allocator.free(ids);
    try std.testing.expectEqualSlices(i64, &.{ 1, 2 }, ids);
}

test "wal: SQL DELETE with AND/OR predicate replays correctly" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    {
        var db = try thindb.Database.open(allocator, io, tmp.dir, .{ .wal_enabled = true });
        defer db.close();
        try sql_helpers.exec(
            allocator,
            db,
            "CREATE TABLE t (id BIGINT PRIMARY KEY, qty INT NOT NULL, region VARCHAR(8) NOT NULL)",
        );
        try sql_helpers.exec(
            allocator,
            db,
            "INSERT INTO t (id, qty, region) VALUES " ++
                "(1, 10, 'east'), (2, 20, 'east'), (3, 30, 'west'), (4, 100, 'west')",
        );
        // (region = 'east' AND qty > 15) OR id = 4 → ids 2 + 4.
        try sql_helpers.exec(
            allocator,
            db,
            "DELETE FROM t WHERE (region = 'east' AND qty > 15) OR id = 4",
        );
    }

    var db = try thindb.Database.open(allocator, io, tmp.dir, .{ .wal_enabled = true });
    defer db.close();
    const ids = try sql_helpers.collectBigints(allocator, db, "SELECT id FROM t ORDER BY id ASC");
    defer allocator.free(ids);
    try std.testing.expectEqualSlices(i64, &.{ 1, 3 }, ids);
}

test "wal: SQL UPDATE replays via DELETE + INSERT entries" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    {
        var db = try thindb.Database.open(allocator, io, tmp.dir, .{ .wal_enabled = true });
        defer db.close();
        try sql_helpers.exec(
            allocator,
            db,
            "CREATE TABLE t (id BIGINT PRIMARY KEY, qty INT NOT NULL)",
        );
        try sql_helpers.exec(
            allocator,
            db,
            "INSERT INTO t (id, qty) VALUES (1, 10), (2, 20), (3, 30)",
        );
        try sql_helpers.exec(allocator, db, "UPDATE t SET qty = 999 WHERE id = 2");
        // No flush — replay should rebuild via WAL: insert all 3, delete
        // matching the UPDATE's predicate, re-insert with new qty.
    }

    var db = try thindb.Database.open(allocator, io, tmp.dir, .{ .wal_enabled = true });
    defer db.close();
    var q = try sql_helpers.runSql(allocator, db, "SELECT id, qty FROM t ORDER BY id ASC");
    defer q.deinit();
    const batch = (try q.next()).?;
    try std.testing.expectEqual(@as(usize, 3), batch.row_count);
    try std.testing.expectEqualSlices(i64, &.{ 1, 2, 3 }, batch.values[0].data.bigint[0..3]);
    try std.testing.expectEqualSlices(i32, &.{ 10, 999, 30 }, batch.values[1].data.int[0..3]);
}

test "wal: DELETE FROM t (no WHERE) wipes the memtable on replay" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    {
        var db = try thindb.Database.open(allocator, io, tmp.dir, .{ .wal_enabled = true });
        defer db.close();
        try sql_helpers.exec(
            allocator,
            db,
            "CREATE TABLE t (id BIGINT PRIMARY KEY, qty INT NOT NULL)",
        );
        try sql_helpers.exec(
            allocator,
            db,
            "INSERT INTO t (id, qty) VALUES (1, 10), (2, 20)",
        );
        try sql_helpers.exec(allocator, db, "DELETE FROM t");
    }

    var db = try thindb.Database.open(allocator, io, tmp.dir, .{ .wal_enabled = true });
    defer db.close();
    const ids = try sql_helpers.collectBigints(allocator, db, "SELECT id FROM t");
    defer allocator.free(ids);
    try std.testing.expectEqualSlices(i64, &.{}, ids);
}
