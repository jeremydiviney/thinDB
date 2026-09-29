//! A path that frees or rewrites a table stops a background merge of it
//! instead of waiting it out (#87): the merge deletes what it wrote and leaves
//! its inputs for a later sweep. Each test holds the merge inside one file
//! operation, starts the DDL, lets the DDL announce itself, then releases the
//! merge, so every race runs the same way.

const std = @import("std");
const Io = std.Io;
const api = @import("../api/api.zig");
const exec = @import("../exec/exec.zig");
const types = @import("../types.zig");

/// Holds the first open or create, after `arm`, of a file whose name ends in
/// `.dat`, until `release`.
const HoldSegmentFile = struct {
    threaded: Io.Threaded,
    vtable: Io.VTable = undefined,
    op: enum { open, create },
    armed: std.atomic.Value(bool) = .init(false),
    holding: std.atomic.Value(bool) = .init(false),
    released: std.atomic.Value(bool) = .init(false),

    fn io(self: *HoldSegmentFile) Io {
        const base = self.threaded.io();
        self.vtable = base.vtable.*;
        self.vtable.dirOpenFile = openFile;
        self.vtable.dirCreateFile = createFile;
        return .{ .userdata = base.userdata, .vtable = &self.vtable };
    }

    fn fromUserdata(userdata: ?*anyopaque) *HoldSegmentFile {
        const threaded: *Io.Threaded = @ptrCast(@alignCast(userdata.?));
        return @fieldParentPtr("threaded", threaded);
    }

    fn arm(self: *HoldSegmentFile) void {
        self.armed.store(true, .release);
    }

    fn release(self: *HoldSegmentFile) void {
        self.released.store(true, .release);
    }

    fn holdIf(self: *HoldSegmentFile, path: []const u8) void {
        if (!std.mem.endsWith(u8, path, ".dat")) return;
        if (!self.armed.swap(false, .acq_rel)) return;
        self.holding.store(true, .release);
        while (!self.released.load(.acquire)) {
            Io.sleep(self.threaded.io(), .fromMilliseconds(1), .awake) catch {};
        }
    }

    fn openFile(userdata: ?*anyopaque, dir: Io.Dir, path: []const u8, options: Io.Dir.OpenFileOptions) Io.File.OpenError!Io.File {
        const self = fromUserdata(userdata);
        if (self.op == .open) self.holdIf(path);
        const base = self.threaded.io();
        return base.vtable.dirOpenFile(base.userdata, dir, path, options);
    }

    fn createFile(userdata: ?*anyopaque, dir: Io.Dir, path: []const u8, options: Io.Dir.CreateFileOptions) Io.File.OpenError!Io.File {
        const self = fromUserdata(userdata);
        if (self.op == .create) self.holdIf(path);
        const base = self.threaded.io();
        return base.vtable.dirCreateFile(base.userdata, dir, path, options);
    }
};

const cfg: api.Config = .{
    .row_group_size = 4,
    .compact_min_segments = 2,
    .auto_flush_secs = 0,
    .auto_flush_rows = 1_000_000,
    .auto_flush_bytes = 64 * 1024 * 1024,
};

const orders_schema: types.TableSchema = .{
    .columns = &.{
        .{ .name = "id", .type = .bigint },
        .{ .name = "qty", .type = .int },
    },
    .order_key = &.{"id"},
    .unique = true,
};
const orders_options: api.TableOptions = .{ .order_key = &.{"id"}, .unique = true, .row_group_size = 4 };

fn fillTwoSegments(t: *api.Table) !void {
    try t.insert(&.{ .{ .id = @as(i64, 1), .qty = @as(i32, 10) }, .{ .id = @as(i64, 2), .qty = @as(i32, 20) } });
    try t.flush();
    try t.insert(&.{ .{ .id = @as(i64, 3), .qty = @as(i32, 30) }, .{ .id = @as(i64, 4), .qty = @as(i32, 40) } });
    try t.flush();
    try std.testing.expectEqual(@as(usize, 2), t.segmentCount());
}

fn expectIds(a: std.mem.Allocator, t: *api.Table, expected: []const i64) !void {
    var query = try exec.scan(a, t);
    defer query.deinit();
    var ids: std.ArrayList(i64) = .empty;
    defer ids.deinit(a);
    while (try query.next()) |batch| try ids.appendSlice(a, batch.values[0].data.bigint[0..batch.row_count]);
    std.mem.sort(i64, ids.items, {}, std.sort.asc(i64));
    try std.testing.expectEqualSlices(i64, expected, ids.items);
}

fn countSegmentDataFiles(dir: Io.Dir, io: Io) !usize {
    var iter_dir = try dir.openDir(io, ".", .{ .iterate = true });
    defer iter_dir.close(io);
    var it = iter_dir.iterate();
    var n: usize = 0;
    while (try it.next(io)) |entry| {
        if (std.mem.endsWith(u8, entry.name, ".dat")) n += 1;
    }
    return n;
}

fn waitFor(io: Io, flag: *const std.atomic.Value(bool)) !void {
    for (0..30_000) |_| {
        if (flag.load(.acquire)) return;
        try Io.sleep(io, .fromMilliseconds(1), .awake);
    }
    return error.TimedOut;
}

/// Waits until a DDL has asked `t`'s merge to stop, which it does before it
/// blocks on `compact_lock`.
fn waitForPreempt(io: Io, t: *api.Table) !void {
    for (0..30_000) |_| {
        if (t.merge_preempt.load(.acquire) != 0) return;
        try Io.sleep(io, .fromMilliseconds(1), .awake);
    }
    return error.DdlNeverAskedMergeToStop;
}

/// Runs `op` on its own thread.
fn Background(comptime op: anytype) type {
    return struct {
        args: std.meta.ArgsTuple(@TypeOf(op)),
        thread: ?std.Thread = null,
        result: ReturnType = undefined,

        const ReturnType = @typeInfo(@TypeOf(op)).@"fn".return_type.?;

        fn run(self: *@This()) void {
            self.result = @call(.auto, op, self.args);
        }

        fn start(self: *@This()) !void {
            self.thread = try std.Thread.spawn(.{}, run, .{self});
        }

        fn join(self: *@This()) void {
            if (self.thread) |th| th.join();
            self.thread = null;
        }
    };
}

fn databaseSweep(db: *api.Database) bool {
    return db.backgroundCompactSweep() catch false;
}

fn catalogSweep(catalog: *api.Catalog) bool {
    return catalog.backgroundCompactSweep() catch false;
}

fn alterAddNote(db: *api.Database) !void {
    try db.alterTable("orders", &.{.{ .add = .{ .name = "note", .type = .bigint, .nullable = true } }});
}

fn truncate(t: *api.Table) !void {
    try t.truncate();
}

fn dropTable(db: *api.Database) !void {
    try db.dropTable("orders");
}

fn dropSchema(db: *api.Database) !void {
    try db.dropSchema("aux");
}

fn dropDatabase(catalog: *api.Catalog) !void {
    try catalog.dropDatabase("other");
}

test "ALTER stops a background merge before it writes, and keeps every row" {
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var hold: HoldSegmentFile = .{ .threaded = .init(a, .{}), .op = .open };
    defer hold.threaded.deinit();
    const io = hold.io();
    const db = try api.Database.open(a, io, tmp.dir, cfg);
    defer db.close();
    const t = try db.table("orders", orders_schema, orders_options);
    try fillTwoSegments(t);

    var sweep: Background(databaseSweep) = .{ .args = .{db} };
    var alter: Background(alterAddNote) = .{ .args = .{db} };
    defer sweep.join();
    defer alter.join();
    // Runs first, so a failing test never leaves a thread held.
    defer hold.release();
    hold.arm();
    try sweep.start();
    try waitFor(io, &hold.holding);
    try alter.start();
    try waitForPreempt(io, t);
    hold.release();
    sweep.join();
    alter.join();

    try std.testing.expect(!sweep.result);
    try alter.result;
    const altered = db.findTable("orders").?;
    try std.testing.expectEqual(altered.segmentCount(), try countSegmentDataFiles(altered.segments_dir, io));
    try expectIds(a, altered, &.{ 1, 2, 3, 4 });
}

test "TRUNCATE stops a background merge after it wrote, and deletes the output" {
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var hold: HoldSegmentFile = .{ .threaded = .init(a, .{}), .op = .create };
    defer hold.threaded.deinit();
    const io = hold.io();
    const db = try api.Database.open(a, io, tmp.dir, cfg);
    defer db.close();
    const t = try db.table("orders", orders_schema, orders_options);
    try fillTwoSegments(t);

    var sweep: Background(databaseSweep) = .{ .args = .{db} };
    var trunc: Background(truncate) = .{ .args = .{t} };
    defer sweep.join();
    defer trunc.join();
    // Runs first, so a failing test never leaves a thread held.
    defer hold.release();
    hold.arm();
    try sweep.start();
    try waitFor(io, &hold.holding);
    try trunc.start();
    try waitForPreempt(io, t);
    hold.release();
    sweep.join();
    trunc.join();

    try std.testing.expect(!sweep.result);
    try trunc.result;
    try std.testing.expectEqual(@as(usize, 0), t.segmentCount());
    try std.testing.expectEqual(@as(usize, 0), try countSegmentDataFiles(t.segments_dir, io));
    try expectIds(a, t, &.{});
}

test "DROP TABLE stops a background merge of the table" {
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var hold: HoldSegmentFile = .{ .threaded = .init(a, .{}), .op = .create };
    defer hold.threaded.deinit();
    const io = hold.io();
    const db = try api.Database.open(a, io, tmp.dir, cfg);
    defer db.close();
    const t = try db.table("orders", orders_schema, orders_options);
    try fillTwoSegments(t);

    var sweep: Background(databaseSweep) = .{ .args = .{db} };
    var drop: Background(dropTable) = .{ .args = .{db} };
    defer sweep.join();
    defer drop.join();
    // Runs first, so a failing test never leaves a thread held.
    defer hold.release();
    hold.arm();
    try sweep.start();
    try waitFor(io, &hold.holding);
    try drop.start();
    try waitForPreempt(io, t);
    hold.release();
    sweep.join();
    drop.join();

    try std.testing.expect(!sweep.result);
    try drop.result;
    try std.testing.expect(db.findTable("orders") == null);
    try std.testing.expectError(error.FileNotFound, db.db_dir.access(io, "orders", .{}));
}

test "DROP SCHEMA stops a background merge of its table" {
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var hold: HoldSegmentFile = .{ .threaded = .init(a, .{}), .op = .open };
    defer hold.threaded.deinit();
    const io = hold.io();
    const db = try api.Database.open(a, io, tmp.dir, cfg);
    defer db.close();
    const aux = try db.createSchema("aux");
    const t = try aux.table("orders", orders_schema, orders_options);
    try fillTwoSegments(t);

    var sweep: Background(databaseSweep) = .{ .args = .{db} };
    var drop: Background(dropSchema) = .{ .args = .{db} };
    defer sweep.join();
    defer drop.join();
    // Runs first, so a failing test never leaves a thread held.
    defer hold.release();
    hold.arm();
    try sweep.start();
    try waitFor(io, &hold.holding);
    try drop.start();
    try waitForPreempt(io, t);
    hold.release();
    sweep.join();
    drop.join();

    try std.testing.expect(!sweep.result);
    try drop.result;
    try std.testing.expect(db.schema("aux") == null);
    try std.testing.expectError(error.FileNotFound, db.db_dir.access(io, "aux", .{}));
}

test "DROP DATABASE stops a background merge of its table" {
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var hold: HoldSegmentFile = .{ .threaded = .init(a, .{}), .op = .open };
    defer hold.threaded.deinit();
    const io = hold.io();
    const catalog = try api.Catalog.open(a, io, tmp.dir, cfg);
    defer catalog.close();
    const other = try catalog.createDatabase("other");
    const t = try other.table("orders", orders_schema, orders_options);
    try fillTwoSegments(t);

    var sweep: Background(catalogSweep) = .{ .args = .{catalog} };
    var drop: Background(dropDatabase) = .{ .args = .{catalog} };
    defer sweep.join();
    defer drop.join();
    // Runs first, so a failing test never leaves a thread held.
    defer hold.release();
    hold.arm();
    try sweep.start();
    try waitFor(io, &hold.holding);
    try drop.start();
    try waitForPreempt(io, t);
    hold.release();
    sweep.join();
    drop.join();

    try std.testing.expect(!sweep.result);
    try drop.result;
    try std.testing.expect(catalog.database("other") == null);
    try std.testing.expectError(error.FileNotFound, tmp.dir.access(io, "other", .{}));
}
