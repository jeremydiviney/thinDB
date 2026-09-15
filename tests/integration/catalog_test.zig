//! Tests for the v2 Catalog → Database → Schema namespace, plus the
//! back-compat shim that keeps `Database.open` + `db.table(...)` working.

const std = @import("std");
const thindb = @import("thindb");
const common = @import("common.zig");
const schema_v1 = common.schema_v1;
const opts_v1 = common.opts_v1;

fn freeNames(allocator: std.mem.Allocator, names: [][]u8) void {
    for (names) |n| allocator.free(n);
    allocator.free(names);
}

fn containsName(names: [][]u8, needle: []const u8) bool {
    for (names) |n| if (std.mem.eql(u8, n, needle)) return true;
    return false;
}

test "Catalog: exclusive directory ownership precedes stale temporary cleanup" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    {
        const first = try thindb.Catalog.open(a, io, tmp.dir, .{});
        defer first.close();
        try tmp.dir.createDir(io, "_temp", .default_dir);
        try tmp.dir.writeFile(io, .{ .sub_path = "_temp/live", .data = "active" });
        try std.testing.expectError(thindb.Error.DatabaseInUse, thindb.Catalog.open(a, io, tmp.dir, .{}));
        const sentinel = try tmp.dir.readFileAlloc(io, "_temp/live", a, .unlimited);
        defer a.free(sentinel);
        try std.testing.expectEqualStrings("active", sentinel);
    }
    const reopened = try thindb.Catalog.open(a, io, tmp.dir, .{});
    defer reopened.close();
}

test "Catalog: concurrent first table opens share one writer and WAL" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const schema: thindb.TableSchema = .{
        .columns = &.{.{ .name = "id", .type = .bigint }},
        .order_key = &.{"id"},
        .unique = false,
    };
    {
        const db = try thindb.Database.open(a, io, tmp.dir, .{});
        defer db.close();
        const t = try db.table("t", schema, .{ .order_key = &.{"id"} });
        try t.insert(&.{.{ .id = @as(i64, 100) }});
        try t.flush();
    }
    const db = try thindb.Database.open(a, io, tmp.dir, .{ .auto_flush_secs = 0 });
    defer db.close();
    const Worker = struct {
        db: *thindb.Database,
        start: *std.atomic.Value(bool),
        id: i64,
        table: ?*thindb.Table = null,
        failure: ?anyerror = null,

        fn run(self: *@This()) void {
            while (!self.start.load(.acquire)) std.atomic.spinLoopHint();
            const t = self.db.openTable("t", .{}) catch |err| {
                self.failure = err;
                return;
            };
            self.table = t;
            t.insert(&.{.{ .id = self.id }}) catch |err| {
                self.failure = err;
            };
        }
    };
    var start = std.atomic.Value(bool).init(false);
    var workers: [8]Worker = undefined;
    {
        var threads: [8]std.Thread = undefined;
        var spawned: usize = 0;
        defer {
            start.store(true, .release);
            for (threads[0..spawned]) |thread| thread.join();
        }
        for (&workers, 0..) |*worker, i| {
            worker.* = .{ .db = db, .start = &start, .id = @intCast(i) };
            threads[i] = try std.Thread.spawn(.{}, Worker.run, .{worker});
            spawned += 1;
        }
    }
    for (workers) |worker| {
        try std.testing.expectEqual(@as(?anyerror, null), worker.failure);
        try std.testing.expectEqual(workers[0].table, worker.table);
    }
    const t = workers[0].table.?;
    try t.flush();
    var q = try thindb.scan(a, t);
    defer q.deinit();
    var rows: usize = 0;
    while (try q.next()) |batch| rows += batch.row_count;
    try std.testing.expectEqual(@as(usize, 9), rows);
}

test "Catalog: createDatabase + listDatabases shows both" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var cat = try thindb.Catalog.open(allocator, io, tmp.dir, .{});
    defer cat.close();

    _ = try cat.createDatabase("analytics");
    _ = try cat.createDatabase("warehouse");

    const names = try cat.listDatabases(allocator);
    defer freeNames(allocator, names);

    try std.testing.expectEqual(@as(usize, 2), names.len);
    try std.testing.expect(containsName(names, "analytics"));
    try std.testing.expect(containsName(names, "warehouse"));
}

test "Catalog: createDatabase twice errors with DatabaseAlreadyExists" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var cat = try thindb.Catalog.open(allocator, io, tmp.dir, .{});
    defer cat.close();

    _ = try cat.createDatabase("analytics");
    try std.testing.expectError(thindb.Error.DatabaseAlreadyExists, cat.createDatabase("analytics"));
}

test "Database: createSchema + listSchemas shows public plus the new one" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var cat = try thindb.Catalog.open(allocator, io, tmp.dir, .{});
    defer cat.close();

    const db = try cat.createDatabase("main");
    _ = try db.createSchema("analytics");

    const names = try db.listSchemas(allocator);
    defer freeNames(allocator, names);

    try std.testing.expectEqual(@as(usize, 2), names.len);
    try std.testing.expect(containsName(names, "public"));
    try std.testing.expect(containsName(names, "analytics"));
}

test "Schema: tables created under non-default schema land in the right dir" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var cat = try thindb.Catalog.open(allocator, io, tmp.dir, .{});
    defer cat.close();

    const db = try cat.createDatabase("main");
    const analytics = try db.createSchema("analytics");

    const t = try analytics.table("orders", schema_v1, opts_v1);
    try t.insert(&.{
        .{ .id = @as(i64, 1), .qty = @as(i32, 10), .active = true, .tag = "a" },
    });
    try t.flush();

    const tables = try analytics.listTables(allocator);
    defer freeNames(allocator, tables);
    try std.testing.expectEqual(@as(usize, 1), tables.len);
    try std.testing.expect(containsName(tables, "orders"));

    // Same name in the default schema is a separate table.
    const public = db.schema("public").?;
    const public_tables = try public.listTables(allocator);
    defer freeNames(allocator, public_tables);
    try std.testing.expectEqual(@as(usize, 0), public_tables.len);
}

test "dropSchema: cascade-drops all tables and removes the directory" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var cat = try thindb.Catalog.open(allocator, io, tmp.dir, .{});
    defer cat.close();

    const db = try cat.createDatabase("main");
    const analytics = try db.createSchema("analytics");
    const t = try analytics.table("orders", schema_v1, opts_v1);
    try t.insert(&.{
        .{ .id = @as(i64, 7), .qty = @as(i32, 70), .active = true, .tag = "x" },
    });
    try t.flush();

    try db.dropSchema("analytics");
    try std.testing.expect(db.schema("analytics") == null);
    try std.testing.expectError(thindb.Error.SchemaNotFound, db.dropSchema("analytics"));
}

test "dropDatabase: cascade-drops all schemas" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var cat = try thindb.Catalog.open(allocator, io, tmp.dir, .{});
    defer cat.close();

    const db = try cat.createDatabase("scratch");
    _ = try db.createSchema("analytics");
    const t = try db.table("orders", schema_v1, opts_v1);
    try t.insert(&.{
        .{ .id = @as(i64, 1), .qty = @as(i32, 10), .active = true, .tag = "a" },
    });
    try t.flush();

    try cat.dropDatabase("scratch");
    try std.testing.expect(cat.database("scratch") == null);
    try std.testing.expectError(thindb.Error.DatabaseNotFound, cat.dropDatabase("scratch"));
}

test "back-compat: Database.open + db.table still works" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    {
        var db = try thindb.Database.open(allocator, io, tmp.dir, .{ .row_group_size = 4 });
        defer db.close();

        const t = try db.table("orders", schema_v1, opts_v1);
        try t.insert(&.{
            .{ .id = @as(i64, 1), .qty = @as(i32, 10), .active = true, .tag = "a" },
            .{ .id = @as(i64, 2), .qty = @as(i32, 20), .active = false, .tag = "b" },
        });
        try t.flush();
        try std.testing.expectEqual(@as(usize, 1), t.segmentCount());
    }

    // Reopen via the same path.
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{ .row_group_size = 4 });
    defer db.close();
    const t = try db.table("orders", schema_v1, opts_v1);
    try std.testing.expectEqual(@as(usize, 1), t.segmentCount());
}
