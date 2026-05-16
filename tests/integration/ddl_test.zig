//! DDL tests: drop / rename / alter, plus the ddl_lock reader-vs-DDL
//! coordination behavior (DDL waits for in-flight scans before proceeding).

const std = @import("std");
const thindb = @import("thindb");
const common = @import("common.zig");
const schema_v1 = common.schema_v1;
const opts_v1 = common.opts_v1;

test "dropTable: removes the directory and forgets the table" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    {
        var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
        defer db.close();
        const t = try db.table("orders", schema_v1, opts_v1);
        try t.insert(&.{
            .{ .id = @as(i64, 1), .qty = @as(i32, 10), .active = true, .tag = "a" },
        });
        try t.flush();

        try db.dropTable("orders");

        // Dropping again is an error.
        try std.testing.expectError(thindb.Error.TableNotFound, db.dropTable("orders"));
    }

    // After reopen, second drop attempt confirms the on-disk directory is gone.
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    try std.testing.expectError(thindb.Error.TableNotFound, db.dropTable("orders"));
}

test "dropTable: works on a table that exists only on disk (not yet opened)" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    // Session 1: create + flush + close.
    {
        var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
        defer db.close();
        const t = try db.table("orders", schema_v1, opts_v1);
        try t.insert(&.{
            .{ .id = @as(i64, 1), .qty = @as(i32, 10), .active = true, .tag = "a" },
        });
        try t.flush();
    }

    // Session 2: drop without opening first.
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    try db.dropTable("orders");
    // Dropping again confirms it's gone.
    try std.testing.expectError(thindb.Error.TableNotFound, db.dropTable("orders"));
}

test "renameTable: changes the on-disk directory and the in-memory key" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    {
        var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
        defer db.close();
        const t = try db.table("orders", schema_v1, opts_v1);
        try t.insert(&.{
            .{ .id = @as(i64, 1), .qty = @as(i32, 10), .active = true, .tag = "alpha" },
            .{ .id = @as(i64, 2), .qty = @as(i32, 20), .active = false, .tag = "beta" },
        });
        try t.flush();

        try db.renameTable("orders", "orders_v2");

        // Existing pointer still works; same data, new name.
        try std.testing.expectEqualStrings("orders_v2", t.name);

        // Old name returns TableNotFound on drop.
        try std.testing.expectError(thindb.Error.TableNotFound, db.dropTable("orders"));
    }

    // After reopen, the new name has the rows; the old name's directory
    // is gone (confirmed by a drop attempt returning TableNotFound).
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    try std.testing.expectError(thindb.Error.TableNotFound, db.dropTable("orders"));

    const t = try db.openTable("orders_v2", .{});

    var q = try thindb.scan(allocator, t);
    defer q.deinit();
    var seen: usize = 0;
    while (try q.next()) |batch| seen += batch.row_count;
    try std.testing.expectEqual(@as(usize, 2), seen);
}

test "renameTable: rejects collision with existing name" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    _ = try db.table("orders", schema_v1, opts_v1);
    _ = try db.table("invoices", schema_v1, opts_v1);

    try std.testing.expectError(thindb.Error.TableAlreadyExists, db.renameTable("orders", "invoices"));
}

// ---------------------------------------------------------------------------
// alterTable
// ---------------------------------------------------------------------------

test "alterTable: rename column preserves data" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    {
        var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
        defer db.close();
        const t = try db.table("orders", schema_v1, opts_v1);
        try t.insert(&.{
            .{ .id = @as(i64, 1), .qty = @as(i32, 10), .active = true, .tag = "a" },
            .{ .id = @as(i64, 2), .qty = @as(i32, 20), .active = false, .tag = "b" },
        });
        try t.flush();

        try db.alterTable("orders", &.{
            .{ .rename = .{ .from = "qty", .to = "quantity" } },
        });

        // The table's schema reflects the new name.
        try std.testing.expect(t.schema.columnIndex("quantity") != null);
        try std.testing.expect(t.schema.columnIndex("qty") == null);
    }

    // Reopen: the new schema is on disk; data preserved.
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    const new_schema = thindb.Schema{
        .columns = &.{
            .{ .name = "id", .type = .bigint },
            .{ .name = "quantity", .type = .int },
            .{ .name = "active", .type = .boolean },
            .{ .name = "tag", .type = .string },
        },
        .order_key = &.{"id"},
        .unique = true,
    };
    const t = try db.table("orders", new_schema, opts_v1);
    var q = try thindb.scan(allocator, t);
    defer q.deinit();
    var rows: usize = 0;
    while (try q.next()) |batch| rows += batch.row_count;
    try std.testing.expectEqual(@as(usize, 2), rows);
}

test "alterTable: drop column removes it; data for other columns intact" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    const t = try db.table("orders", schema_v1, opts_v1);
    try t.insert(&.{
        .{ .id = @as(i64, 1), .qty = @as(i32, 10), .active = true, .tag = "a" },
        .{ .id = @as(i64, 2), .qty = @as(i32, 20), .active = false, .tag = "b" },
        .{ .id = @as(i64, 3), .qty = @as(i32, 30), .active = true, .tag = "c" },
    });
    try t.flush();

    try db.alterTable("orders", &.{
        .{ .drop = "active" },
    });

    try std.testing.expect(t.schema.columnIndex("active") == null);
    try std.testing.expectEqual(@as(usize, 3), t.schema.columns.len);

    // Scan the post-alter table — should still have 3 rows, no "active" column.
    var q = try thindb.scan(allocator, t);
    defer q.deinit();
    var seen: usize = 0;
    while (try q.next()) |batch| {
        seen += batch.row_count;
        try std.testing.expectEqual(@as(usize, 3), batch.schema.len);
    }
    try std.testing.expectEqual(@as(usize, 3), seen);
}

test "alterTable: add column fills existing rows with default" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    const t = try db.table("orders", schema_v1, opts_v1);
    try t.insert(&.{
        .{ .id = @as(i64, 1), .qty = @as(i32, 10), .active = true, .tag = "a" },
        .{ .id = @as(i64, 2), .qty = @as(i32, 20), .active = false, .tag = "b" },
    });
    try t.flush();

    try db.alterTable("orders", &.{
        .{ .add = .{
            .name = "priority",
            .type = .int,
            .default = .{ .int = 7 },
        } },
    });

    try std.testing.expectEqual(@as(usize, 5), t.schema.columns.len);
    try std.testing.expect(t.schema.columnIndex("priority") != null);

    // Scan: the new column should be present with the default value
    // populated for the existing 2 rows.
    var q = try thindb.scan(allocator, t);
    defer q.deinit();
    var saw: usize = 0;
    while (try q.next()) |batch| {
        try std.testing.expectEqual(@as(usize, 5), batch.schema.len);
        const priority_idx = t.schema.columnIndex("priority").?;
        const priority_col = batch.values[priority_idx];
        for (priority_col.data.int) |v| try std.testing.expectEqual(@as(i32, 7), v);
        saw += batch.row_count;
    }
    try std.testing.expectEqual(@as(usize, 2), saw);
}

test "alterTable: rejects dropping a column in the order key" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    _ = try db.table("orders", schema_v1, opts_v1);

    try std.testing.expectError(thindb.Error.UnsupportedAlterOp, db.alterTable("orders", &.{
        .{ .drop = "id" }, // "id" is in opts_v1.order_key
    }));
}

test "ddl_lock: dropTable waits for an in-flight scan to release before proceeding" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    const t = try db.table("orders", schema_v1, opts_v1);
    try t.insert(&.{
        .{ .id = @as(i64, 1), .qty = @as(i32, 10), .active = true, .tag = "a" },
        .{ .id = @as(i64, 2), .qty = @as(i32, 20), .active = false, .tag = "b" },
    });
    try t.flush();

    // Start a scan and hold its shared ddl_lock by NOT yet calling deinit.
    var q = try thindb.scan(allocator, t);

    // Spawn a thread that calls dropTable — should block until we deinit q.
    var drop_completed: std.atomic.Value(bool) = .init(false);
    const Ctx = struct {
        db: *thindb.Database,
        completed: *std.atomic.Value(bool),
        fn run(self: @This()) void {
            self.db.dropTable("orders") catch {};
            self.completed.store(true, .release);
        }
    };
    const thr = try std.Thread.spawn(.{}, Ctx.run, .{Ctx{ .db = db, .completed = &drop_completed }});

    // Yield a couple of times to let the drop thread run far enough to
    // attempt the exclusive lock acquisition and BLOCK.
    var i: usize = 0;
    while (i < 100) : (i += 1) std.Thread.yield() catch {};

    // Drop must NOT have completed — the scan still holds shared ddl_lock.
    try std.testing.expect(!drop_completed.load(.acquire));

    // Releasing the scan lets drop proceed.
    q.deinit();
    thr.join();
    try std.testing.expect(drop_completed.load(.acquire));
}

test "alterTable: rejects duplicate column name on add" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    _ = try db.table("orders", schema_v1, opts_v1);

    try std.testing.expectError(thindb.Error.ColumnAlreadyExists, db.alterTable("orders", &.{
        .{ .add = .{ .name = "qty", .type = .int, .default = .{ .int = 0 } } },
    }));
}
