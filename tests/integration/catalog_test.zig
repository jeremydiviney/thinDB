//! Tests for the v2 Catalog → Database → Schema namespace, plus the
//! back-compat shim that keeps `Database.open` + `db.table(...)` working.

const std = @import("std");
const thindb = @import("thindb");
const common = @import("common.zig");
const schema_v1 = common.schema_v1;
const opts_v1 = common.opts_v1;
const sql_helpers = @import("sql_helpers.zig");

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

/// A table lookup the parser makes, run on its own thread.
const ParseLookup = struct {
    tables: thindb.net.SessionTables,
    arena: std.heap.ArenaAllocator,
    columns: ?[]const []const u8 = null,
    done: std.atomic.Value(bool) = .init(false),

    fn run(self: *ParseLookup) void {
        const view = self.tables.columns();
        self.columns = view.lookup(view.context, self.arena.allocator(), null, null, "orders") catch null;
        self.done.store(true, .release);
    }
};

// The parser looks tables up before its statement takes a lease (#90): a
// lookup that ignored the gate read a table a concurrent DROP DATABASE had
// already freed.
test "Catalog: a parse-time table lookup waits out a DROP DATABASE" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var cat = try thindb.Catalog.open(allocator, io, tmp.dir, .{});
    defer cat.close();
    const db = try cat.createDatabase("doomed");
    _ = try db.table("orders", schema_v1, opts_v1);

    var lookup: ParseLookup = .{
        .tables = .{ .catalog = cat, .session = .{ .current_db = "doomed" } },
        .arena = .init(allocator),
    };
    defer lookup.arena.deinit();

    const looked_up_during_drop = drop: {
        var drop_lease: ?thindb.Catalog.StatementLease = try cat.acquireStatement(true);
        errdefer if (drop_lease) |lease| lease.release();
        const thread = try std.Thread.spawn(.{}, ParseLookup.run, .{&lookup});
        defer {
            if (drop_lease) |lease| lease.release();
            drop_lease = null;
            thread.join();
        }
        try std.Io.sleep(io, .fromMilliseconds(50), .awake);
        const early = lookup.done.load(.acquire);
        try cat.dropDatabase("doomed");
        break :drop early;
    };
    try std.testing.expect(!looked_up_during_drop);
    try std.testing.expect(lookup.columns == null);
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

// The catalog root also holds the engine's own directories (#373). None of
// them is a database, so none can be created, dropped or used by name, and a
// drop never deletes a directory the catalog did not load.
test "Catalog: reserved root directory names are refused and never deleted" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(a, io, tmp.dir, .{});
    defer db.close();
    const cat = db.owned_catalog.?;
    const markers = [_][]const u8{ "_xa/keep", "_temp/keep", "_zigfn_build/main_f/keep", "orphan/keep" };
    for (markers) |marker| {
        try tmp.dir.createDirPath(io, std.fs.path.dirname(marker).?);
        try tmp.dir.writeFile(io, .{ .sub_path = marker, .data = "engine" });
    }

    for ([_][]const u8{ "_xa", "_XA", "_temp", "_zigfn_build", "", "..", "../escape", "a/b" }) |name| {
        try std.testing.expectError(thindb.Error.InvalidDatabaseName, cat.dropDatabase(name));
        try std.testing.expectError(thindb.Error.InvalidDatabaseName, cat.createDatabase(name));
    }
    for ([_][]const u8{
        "DROP DATABASE _xa",
        "DROP DATABASE IF EXISTS _temp",
        "CREATE DATABASE _zigfn_build",
        "CREATE DATABASE IF NOT EXISTS _xa",
        "USE _temp",
    }) |sql_text| try sql_helpers.expectRunError(a, db, sql_text, thindb.Error.InvalidDatabaseName);
    try std.testing.expectError(thindb.Error.DatabaseNotFound, cat.dropDatabase("orphan"));

    for (markers) |marker| try tmp.dir.access(io, marker, .{});
    const names = try cat.listDatabases(a);
    defer freeNames(a, names);
    try std.testing.expectEqual(@as(usize, 1), names.len);
    try std.testing.expectEqualStrings("main", names[0]);
}

// A zig function builds under `_zigfn_build` (#373). Reopening must not adopt
// that directory as a database, which would list it and give it a `public`.
test "Catalog: reopening skips the zig function build directory" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    {
        const cat = try thindb.Catalog.open(a, io, tmp.dir, .{});
        defer cat.close();
        _ = try cat.createDatabase("main");
        try tmp.dir.createDirPath(io, thindb.Catalog.zig_fn_build_dir_name ++ "/main_f");
    }
    const cat = try thindb.Catalog.open(a, io, tmp.dir, .{});
    defer cat.close();
    const names = try cat.listDatabases(a);
    defer freeNames(a, names);
    try std.testing.expectEqual(@as(usize, 1), names.len);
    try std.testing.expectEqualStrings("main", names[0]);
    try std.testing.expectError(error.FileNotFound, tmp.dir.access(io, thindb.Catalog.zig_fn_build_dir_name ++ "/public", .{}));
}
