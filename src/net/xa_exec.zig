const std = @import("std");
const api = @import("../api/api.zig");
const local = @import("local.zig");
const ir = @import("../ir/ir.zig");
const storage = @import("../storage/storage.zig");
const engine = @import("../engine/engine.zig");
const journal_mod = @import("../storage/write_journal.zig");
const Allocator = std.mem.Allocator;

const Target = struct {
    table: *api.Table,
    path: []const u8,
    sync_mode: api.SyncMode,
    wal_enabled: bool,
};

pub fn commit(allocator: Allocator, catalog: *api.Catalog, xid: []const u8, one_phase: bool) !void {
    const lease = try catalog.acquireStatement(true);
    defer lease.release();
    if (one_phase) catalog.xa.prepare(xid) catch |err| switch (err) {
        error.XaBranchUnknown => return,
        else => return err,
    };
    const branch = (try catalog.xa.beginCommit(xid)) orelse return;
    var resolved = false;
    defer if (!resolved and !catalog.statement_gate.recovery_required.load(.acquire)) catalog.xa.cancelCommit(xid);

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const session: api.Session = .{ .current_db = branch.db, .current_schema = branch.schema };
    const db = catalog.database(branch.db) orelse return api.Error.DatabaseNotFound;
    const ops = try a.alloc(ir.Op, branch.stmts.items.len);
    var targets: std.ArrayList(Target) = .empty;
    for (branch.stmts.items, ops) |encoded, *op| {
        op.* = try ir.decode(a, encoded);
        const ref = switch (op.*) {
            .insert => |v| v.table,
            .insert_select => |v| v.table,
            .delete_op => |v| v.table,
            .update_op => |v| v.table,
            else => return error.XaProtocol,
        };
        const target = try local.resolvePersistentTableTarget(catalog, session, ref);
        const table = try target.schema.openTable(target.table_name, .{});
        var found = false;
        for (targets.items) |existing| {
            if (existing.table == table) {
                found = true;
                break;
            }
        }
        if (!found) try targets.append(a, .{
            .table = table,
            .path = try std.fmt.allocPrint(a, "{s}/{s}/{s}", .{ target.db_name, target.schema.name, target.table_name }),
            .sync_mode = table.sync_mode,
            .wal_enabled = table.wal != null,
        });
    }
    const paths = try a.alloc([]const u8, targets.items.len);
    var locked: usize = 0;
    defer for (targets.items[0..locked]) |target| {
        target.table.sync_mode = target.sync_mode;
        target.table.compact_lock.unlock(catalog.io);
    };
    for (targets.items, paths) |target, *path| {
        target.table.compact_lock.lockUncancelable(catalog.io);
        locked += 1;
        target.table.sync_mode = .per_flush;
        try target.table.flush();
        path.* = target.path;
    }

    var journal = journal_mod.WriteJournal.begin(allocator, catalog.io, catalog.root_dir, .{ .xid = xid, .tables = paths }) catch |err| {
        journal_mod.recover(allocator, catalog.io, catalog.root_dir) catch {
            catalog.statement_gate.recovery_required.store(true, .release);
            return api.Error.RecoveryRequired;
        };
        return err;
    };
    defer journal.deinit();
    apply(allocator, db, session, ops, targets.items) catch |err| {
        rollback(allocator, catalog, &journal, targets.items, paths) catch {
            catalog.statement_gate.recovery_required.store(true, .release);
            return api.Error.RecoveryRequired;
        };
        return err;
    };
    journal.markCommitted() catch |err| {
        catalog.statement_gate.recovery_required.store(true, .release);
        return err;
    };
    catalog.xa.finishCommit(xid) catch |err| {
        catalog.statement_gate.recovery_required.store(true, .release);
        return err;
    };
    resolved = true;
    journal.retire() catch |err| {
        catalog.statement_gate.recovery_required.store(true, .release);
        return err;
    };
}

fn apply(allocator: Allocator, db: *api.Database, session: api.Session, ops: []ir.Op, targets: []const Target) !void {
    for (ops) |*op| {
        var compiled = try local.compileInStatement(allocator, db, session, op);
        defer compiled.deinit();
        while (try compiled.next()) |_| {}
    }
    for (targets) |target| try target.table.flush();
}

fn rollback(allocator: Allocator, catalog: *api.Catalog, journal: *journal_mod.WriteJournal, targets: []const Target, paths: []const []const u8) !void {
    _ = allocator;
    for (targets) |target| {
        const t = target.table;
        t.mutex.lockUncancelable(catalog.io);
        if (t.wal) |*w| w.deinit();
        t.wal = null;
        t.mutex.unlock(catalog.io);
    }
    try journal.restore(paths);
    for (targets) |target| {
        const t = target.table;
        t.ddl_lock.lockUncancelable(catalog.io);
        defer t.ddl_lock.unlock(catalog.io);
        t.mutex.lockUncancelable(catalog.io);
        defer t.mutex.unlock(catalog.io);
        var manifest = try storage.readManifest(t.allocator, t.io, t.table_dir, t.schema_fingerprint);
        errdefer manifest.deinit();
        if (manifest.column_count == 0) manifest.column_count = @intCast(t.schema.columns.len);
        const memtable = try engine.Memtable.create(t.allocator, t.schema);
        errdefer memtable.release();
        if (target.wal_enabled) t.wal = try engine.wal.WalWriter.create(t.allocator, t.io, t.table_dir, t.schema_fingerprint);
        t.manifest.deinit();
        t.manifest = manifest;
        t.installMemtableLocked(memtable);
        t.first_write_ts = null;
        t.seg_handles.clear(t.allocator);
        t.cache.purgeTable(t.cache_uid);
        t.cache_uid = storage.cache.newTableUid();
    }
    try journal.retire();
}

test "xa commit rolls back earlier statements on later compile failure and retains recovery" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const db = try api.Database.open(a, io, tmp.dir, .{});
    defer db.close();
    const catalog = db.catalog.?;
    const schema: @import("../types.zig").TableSchema = .{
        .columns = &.{.{ .name = "id", .type = .bigint }},
        .order_key = &.{"id"},
        .unique = false,
    };
    const t = try db.table("t", schema, .{ .order_key = &.{"id"} });
    try t.insert(&.{.{ .id = @as(i64, 1) }});
    try t.flush();
    try catalog.xa.begin("atomic", "main");
    const good: ir.Op = .{ .insert = .{
        .table = .{ .name = "t" },
        .columns = null,
        .rows = &.{&.{.{ .bigint = 2 }}},
    } };
    const bad: ir.Op = .{ .insert = .{
        .table = .{ .name = "t" },
        .columns = &.{"missing"},
        .rows = &.{&.{.{ .bigint = 3 }}},
    } };
    for ([_]ir.Op{ good, bad }) |op| {
        var encoded: std.ArrayList(u8) = .empty;
        defer encoded.deinit(a);
        try ir.encode(a, &encoded, op);
        try catalog.xa.stage("atomic", encoded.items);
    }
    try catalog.xa.end("atomic");
    try catalog.xa.prepare("atomic");
    try std.testing.expectError(error.ColumnNotFound, commit(a, catalog, "atomic", false));
    try std.testing.expectEqual(@import("xa.zig").State.prepared, catalog.xa.map.get("atomic").?.state);
    var q = try @import("../exec/exec.zig").scan(a, t);
    defer q.deinit();
    var rows: usize = 0;
    while (try q.next()) |b| {
        rows += b.row_count;
        for (b.values[0].data.bigint) |id| try std.testing.expectEqual(@as(i64, 1), id);
    }
    try std.testing.expectEqual(@as(usize, 1), rows);
}

test "xa successful nonunique commit is durable and retry does not duplicate" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    {
        const db = try api.Database.open(a, io, tmp.dir, .{});
        defer db.close();
        const schema: @import("../types.zig").TableSchema = .{
            .columns = &.{.{ .name = "id", .type = .bigint }},
            .order_key = &.{"id"},
            .unique = false,
        };
        _ = try db.table("t", schema, .{ .order_key = &.{"id"} });
        const catalog = db.catalog.?;
        try catalog.xa.begin("durable", "main");
        var encoded: std.ArrayList(u8) = .empty;
        defer encoded.deinit(a);
        try ir.encode(a, &encoded, .{ .insert = .{
            .table = .{ .name = "t" },
            .columns = null,
            .rows = &.{&.{.{ .bigint = 2 }}},
        } });
        try catalog.xa.stage("durable", encoded.items);
        try catalog.xa.end("durable");
        try catalog.xa.prepare("durable");
        try commit(a, catalog, "durable", false);
        try commit(a, catalog, "durable", false);
    }
    const db = try api.Database.open(a, io, tmp.dir, .{});
    defer db.close();
    try commit(a, db.catalog.?, "durable", false);
    const t = try db.openTable("t", .{});
    var q = try @import("../exec/exec.zig").scan(a, t);
    defer q.deinit();
    var rows: usize = 0;
    while (try q.next()) |b| rows += b.row_count;
    try std.testing.expectEqual(@as(usize, 1), rows);
}

const test_schema: @import("../types.zig").TableSchema = .{
    .columns = &.{.{ .name = "id", .type = .bigint }},
    .order_key = &.{"id"},
    .unique = false,
};

fn stageTestInsert(catalog: *api.Catalog, xid: []const u8, table: []const u8, value: i64, column: ?[]const u8) !void {
    const a = std.testing.allocator;
    var bytes: std.ArrayList(u8) = .empty;
    defer bytes.deinit(a);
    const columns = [_][]const u8{column orelse "id"};
    const row = [_]?@import("../types.zig").Value{.{ .bigint = value }};
    try ir.encode(a, &bytes, .{ .insert = .{
        .table = .{ .name = table },
        .columns = &columns,
        .rows = &.{&row},
    } });
    try catalog.xa.stage(xid, bytes.items);
}

fn expectTestRows(table: *api.Table, expected: []const i64) !void {
    const a = std.testing.allocator;
    var q = try @import("../exec/exec.zig").scan(a, table);
    defer q.deinit();
    var ids: std.ArrayList(i64) = .empty;
    defer ids.deinit(a);
    while (try q.next()) |b| try ids.appendSlice(a, b.values[0].data.bigint[0..b.row_count]);
    std.mem.sort(i64, ids.items, {}, std.sort.asc(i64));
    try std.testing.expectEqualSlices(i64, expected, ids.items);
}

test "xa multi-table undo restores published writes and one-phase commit keeps originating schema" {
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const db = try api.Database.open(a, std.testing.io, tmp.dir, .{ .auto_flush_rows = 1 });
    defer db.close();
    const schema = try db.createSchema("other");
    const first = try schema.table("a", test_schema, .{ .order_key = &.{"id"} });
    const second = try schema.table("b", test_schema, .{ .order_key = &.{"id"} });
    try first.insert(&.{.{ .id = @as(i64, 10) }});
    try second.insert(&.{.{ .id = @as(i64, 20) }});
    try first.flush();
    try second.flush();
    const catalog = db.catalog.?;
    try catalog.xa.beginInSchema("failed", "main", "other");
    try stageTestInsert(catalog, "failed", "a", 11, null);
    try stageTestInsert(catalog, "failed", "b", 21, null);
    try stageTestInsert(catalog, "failed", "b", 22, "absent");
    try catalog.xa.end("failed");
    try catalog.xa.prepare("failed");
    try std.testing.expectError(error.ColumnNotFound, commit(a, catalog, "failed", false));
    try expectTestRows(first, &.{10});
    try expectTestRows(second, &.{20});
    try std.testing.expectEqual(@import("xa.zig").State.prepared, catalog.xa.map.get("failed").?.state);
    try catalog.xa.rollback("failed");
    try catalog.xa.beginInSchema("ok", "main", "other");
    try stageTestInsert(catalog, "ok", "a", 11, null);
    try stageTestInsert(catalog, "ok", "b", 21, null);
    try catalog.xa.end("ok");
    try commit(a, catalog, "ok", true);
    try commit(a, catalog, "ok", true);
    try expectTestRows(first, &.{ 10, 11 });
    try expectTestRows(second, &.{ 20, 21 });
}

test "xa startup resolves real table files at each durable commit phase" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    for (0..4) |phase| {
        var tmp = std.testing.tmpDir(.{});
        defer tmp.cleanup();
        {
            const db = try api.Database.open(a, io, tmp.dir, .{ .sync_mode = .per_flush });
            defer db.close();
            const first = try db.table("a", test_schema, .{ .order_key = &.{"id"} });
            const second = try db.table("b", test_schema, .{ .order_key = &.{"id"} });
            try first.insert(&.{.{ .id = @as(i64, 10) }});
            try second.insert(&.{.{ .id = @as(i64, 20) }});
            try first.flush();
            try second.flush();
            const catalog = db.catalog.?;
            try catalog.xa.begin("restart", "main");
            try stageTestInsert(catalog, "restart", "a", 11, null);
            try stageTestInsert(catalog, "restart", "b", 21, null);
            try catalog.xa.end("restart");
            try catalog.xa.prepare("restart");
            var journal = try journal_mod.WriteJournal.begin(a, io, catalog.root_dir, .{
                .xid = "restart",
                .tables = &.{ "main/public/a", "main/public/b" },
            });
            defer journal.deinit();
            if (phase >= 1) {
                try first.insert(&.{.{ .id = @as(i64, 11) }});
                try first.flush();
            }
            if (phase >= 2) {
                try second.insert(&.{.{ .id = @as(i64, 21) }});
                try second.flush();
            }
            if (phase == 3) try journal.markCommitted();
            // Leave the journal unresolved, as a process exit at this phase would.
        }
        {
            const db = try api.Database.open(a, io, tmp.dir, .{});
            defer db.close();
            try expectTestRows(try db.openTable("a", .{}), if (phase == 3) &.{ 10, 11 } else &.{10});
            try expectTestRows(try db.openTable("b", .{}), if (phase == 3) &.{ 20, 21 } else &.{20});
            try std.testing.expectEqual(phase != 3, db.catalog.?.xa.map.contains("restart"));
            try commit(a, db.catalog.?, "restart", false);
            try expectTestRows(try db.openTable("a", .{}), &.{ 10, 11 });
            try expectTestRows(try db.openTable("b", .{}), &.{ 20, 21 });
        }
    }
}
