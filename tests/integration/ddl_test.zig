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

    const new_schema = thindb.TableSchema{
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

const IdQty = struct { id: i64, qty: i32 };

fn expectLiveRows(allocator: std.mem.Allocator, t: *thindb.Table, expected: []const IdQty) !void {
    var q = try thindb.scan(allocator, t);
    defer q.deinit();
    var rows: std.ArrayList(IdQty) = .empty;
    defer rows.deinit(allocator);
    const id_idx = t.schema.columnIndex("id").?;
    const qty_idx = t.schema.columnIndex("qty").?;
    while (try q.next()) |batch| {
        for (batch.values[id_idx].data.bigint, batch.values[qty_idx].data.int) |id, qty| {
            try rows.append(allocator, .{ .id = id, .qty = qty });
        }
    }
    std.sort.pdq(IdQty, rows.items, {}, struct {
        fn lessThan(_: void, a: IdQty, b: IdQty) bool {
            return a.id < b.id;
        }
    }.lessThan);
    try std.testing.expectEqualSlices(IdQty, expected, rows.items);
}

test "alterTable: deletes, replaced rows and key filters survive the rewrite" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const expected = [_]IdQty{
        .{ .id = 1, .qty = 10 },
        .{ .id = 3, .qty = 300 },
        .{ .id = 4, .qty = 40 },
        .{ .id = 6, .qty = 60 },
    };
    {
        var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
        defer db.close();
        const t = try db.table("orders", schema_v1, opts_v1);
        // Two row groups (row_group_size 4), so the deletes land in both.
        try t.insert(&.{
            .{ .id = @as(i64, 1), .qty = @as(i32, 10), .active = true, .tag = "a" },
            .{ .id = @as(i64, 2), .qty = @as(i32, 20), .active = true, .tag = "b" },
            .{ .id = @as(i64, 3), .qty = @as(i32, 30), .active = true, .tag = "c" },
            .{ .id = @as(i64, 4), .qty = @as(i32, 40), .active = true, .tag = "d" },
            .{ .id = @as(i64, 5), .qty = @as(i32, 50), .active = true, .tag = "e" },
            .{ .id = @as(i64, 6), .qty = @as(i32, 60), .active = true, .tag = "f" },
        });
        try t.flush();
        _ = try t.delete(.{ .col = "id", .op = .eq, .val = .{ .bigint = 2 } });
        _ = try t.delete(.{ .col = "id", .op = .eq, .val = .{ .bigint = 5 } });
        // An upsert tombstones the flushed copy of its key.
        try t.insert(&.{.{ .id = @as(i64, 3), .qty = @as(i32, 300), .active = true, .tag = "c" }});
        try t.flush();
        try expectLiveRows(allocator, t, &expected);

        try db.alterTable("orders", &.{
            .{ .add = .{ .name = "priority", .type = .int, .default = .{ .int = 7 } } },
        });
        try expectLiveRows(allocator, t, &expected);
        for (t.manifest.segments.items) |entry| try std.testing.expect(entry.key_bloom.len > 0);
    }

    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    try expectLiveRows(allocator, try db.openTable("orders", .{}), &expected);
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

// ---------------------------------------------------------------------------
// ALTER swap recovery
// ---------------------------------------------------------------------------

const add_note: thindb.AlterOp = .{ .add = .{ .name = "note", .type = .bigint, .nullable = true } };

/// Leave `orders` under the original schema and `altered` holding the same
/// rows after `add_note`: the two trees an ALTER of `orders` swaps.
fn seedAlterTrees(allocator: std.mem.Allocator, io: std.Io, dir: std.Io.Dir) !void {
    var db = try thindb.Database.open(allocator, io, dir, .{});
    defer db.close();
    inline for (.{ "orders", "altered" }) |name| {
        const t = try db.table(name, schema_v1, opts_v1);
        try t.insert(&.{
            .{ .id = @as(i64, 1), .qty = @as(i32, 10), .active = true, .tag = "a" },
            .{ .id = @as(i64, 2), .qty = @as(i32, 20), .active = false, .tag = "b" },
        });
        try t.flush();
    }
    try db.alterTable("altered", &.{add_note});
}

fn expectNoSwapDirectories(io: std.Io, dir: std.Io.Dir) !void {
    var public = try dir.openDir(io, "main/public", .{ .iterate = true });
    defer public.close(io);
    var it = public.iterate();
    while (try it.next(io)) |entry| try std.testing.expect(!std.mem.startsWith(u8, entry.name, "__alter_"));
}

test "alterTable: a schema reopening mid-swap restores the table whole" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    const Move = struct { []const u8, []const u8 };
    // Each case leaves the directories an ALTER of `orders` stopped at one
    // step would, then reopens. `note` says which schema must come back.
    const cases = .{
        // Crashed writing the shadow, before its manifest.
        .{ .moves = &[_]Move{.{ "altered", "__alter_new_orders" }}, .drop_manifest = "__alter_new_orders", .drop_tree = "", .note = false },
        // Shadow complete, original not yet set aside.
        .{ .moves = &[_]Move{.{ "altered", "__alter_new_orders" }}, .drop_manifest = "", .drop_tree = "", .note = false },
        // Original set aside, shadow not yet renamed in: rolls back.
        .{ .moves = &[_]Move{ .{ "orders", "__alter_old_orders" }, .{ "altered", "__alter_new_orders" } }, .drop_manifest = "", .drop_tree = "", .note = false },
        // Committed, original not yet deleted.
        .{ .moves = &[_]Move{ .{ "orders", "__alter_old_orders" }, .{ "altered", "orders" } }, .drop_manifest = "", .drop_tree = "", .note = true },
        // The swap before the aside step, stopped between its delete and rename.
        .{ .moves = &[_]Move{.{ "altered", "__alter_orders" }}, .drop_manifest = "", .drop_tree = "orders", .note = true },
        // That swap's incomplete shadow beside the table.
        .{ .moves = &[_]Move{.{ "altered", "__alter_orders" }}, .drop_manifest = "__alter_orders", .drop_tree = "", .note = false },
    };
    inline for (cases) |c| {
        var tmp = std.testing.tmpDir(.{});
        defer tmp.cleanup();
        try seedAlterTrees(allocator, io, tmp.dir);
        var public = try tmp.dir.openDir(io, "main/public", .{});
        if (c.drop_tree.len > 0) try public.deleteTree(io, c.drop_tree);
        for (c.moves) |m| try std.Io.Dir.rename(public, m[0], public, m[1], io);
        if (c.drop_manifest.len > 0) {
            var shadow = try public.openDir(io, c.drop_manifest, .{});
            defer shadow.close(io);
            try shadow.deleteFile(io, "manifest");
        }
        public.close(io);

        var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
        defer db.close();
        const t = try db.openTable("orders", .{});
        try std.testing.expectEqual(c.note, t.schema.columnIndex("note") != null);
        try expectLiveRows(allocator, t, &.{ .{ .id = 1, .qty = 10 }, .{ .id = 2, .qty = 20 } });
        const names = try db.schema("public").?.listTables(allocator);
        defer {
            for (names) |name| allocator.free(name);
            allocator.free(names);
        }
        try std.testing.expectEqual(@as(usize, 1), names.len);
        try std.testing.expectEqualStrings("orders", names[0]);
        try expectNoSwapDirectories(io, tmp.dir);
    }
}

test "alterTable: swap directory names are reserved" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    _ = try db.table("orders", schema_v1, opts_v1);

    try std.testing.expectError(thindb.Error.ReservedTableName, db.table("__alter_new_orders", schema_v1, opts_v1));
    try std.testing.expectError(thindb.Error.ReservedTableName, db.renameTable("orders", "__alter_old_orders"));
    try std.testing.expectError(thindb.Error.TableNotFound, db.openTable("__alter_new_orders", .{}));
    try std.testing.expectError(thindb.Error.TableNotFound, db.dropTable("__alter_new_orders"));
    try std.testing.expectError(thindb.Error.TableNotFound, db.renameTable("__alter_new_orders", "shadow"));
}
