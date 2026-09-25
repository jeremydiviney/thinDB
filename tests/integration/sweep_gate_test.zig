//! A background merge runs without a statement lease (#87): DDL and XA COMMIT
//! must not queue behind it, and every path that frees its table must wait for
//! it instead. A reader holding the table's `ddl_lock` parks the merge at its
//! commit, which sequences each race deterministically.

const std = @import("std");
const thindb = @import("thindb");
const common = @import("common.zig");
const helpers = @import("sql_helpers.zig");
const schema_v1 = common.schema_v1;
const opts_v1 = common.opts_v1;

const cfg: thindb.Config = .{
    .row_group_size = 4,
    .compact_min_segments = 2,
    .auto_flush_secs = 0,
    .auto_flush_rows = 1_000_000,
    .auto_flush_bytes = 64 * 1024 * 1024,
};

fn fillTwoSegments(t: *thindb.Table) !void {
    inline for (.{ .{ 1, 2 }, .{ 3, 4 } }) |ids| {
        inline for (ids) |id| {
            try t.insert(&.{.{ .id = @as(i64, id), .qty = @as(i32, id * 10), .active = true, .tag = "x" }});
        }
        try t.flush();
    }
    try std.testing.expectEqual(@as(usize, 2), t.segmentCount());
}

fn deadlineIn(io: std.Io, ms: i64) std.Io.Clock.Timestamp {
    return .fromNow(io, .{ .raw = .fromMilliseconds(ms), .clock = .awake });
}

fn passed(io: std.Io, deadline: std.Io.Clock.Timestamp) bool {
    return std.Io.Clock.Timestamp.now(io, .awake).compare(.gte, deadline);
}

/// A catalog compaction sweep whose merge on `t` is parked at its commit by a
/// reader holding `t.ddl_lock`.
const ParkedMerge = struct {
    t: *thindb.Table,
    io: std.Io,
    catalog: *thindb.Catalog,
    thread: ?std.Thread = null,
    reader_held: bool = false,
    worked: bool = false,

    fn sweep(self: *ParkedMerge) void {
        self.worked = self.catalog.backgroundCompactSweep() catch false;
    }

    fn start(self: *ParkedMerge) !void {
        self.t.ddl_lock.lockSharedUncancelable(self.io);
        self.reader_held = true;
        self.thread = try std.Thread.spawn(.{}, sweep, .{self});
        const deadline = deadlineIn(self.io, 30_000);
        while (true) {
            self.t.ddl_lock.mutex.lockUncancelable(self.io);
            const waiting = self.t.ddl_lock.timed_writers > 0;
            self.t.ddl_lock.mutex.unlock(self.io);
            if (waiting) return;
            if (passed(self.io, deadline)) return error.MergeNeverReachedCommit;
            try std.Io.sleep(self.io, .fromMilliseconds(1), .awake);
        }
    }

    /// Let the merge commit and wait for the sweep to end. `t` may be freed
    /// as soon as the reader lets go, so this is the last touch of it.
    fn finish(self: *ParkedMerge) void {
        if (self.reader_held) self.t.ddl_lock.unlockShared(self.io);
        self.reader_held = false;
        if (self.thread) |th| th.join();
        self.thread = null;
    }
};

/// Runs `op` on its own thread so the test can observe whether it is blocked.
fn Background(comptime op: anytype) type {
    return struct {
        args: std.meta.ArgsTuple(@TypeOf(op)),
        thread: ?std.Thread = null,
        done: std.atomic.Value(bool) = .init(false),
        failed: bool = false,

        fn run(self: *@This()) void {
            @call(.auto, op, self.args) catch {
                self.failed = true;
            };
            self.done.store(true, .release);
        }

        fn start(self: *@This()) !void {
            self.thread = try std.Thread.spawn(.{}, run, .{self});
        }

        fn finishesWithin(self: *@This(), io: std.Io, ms: i64) !bool {
            const deadline = deadlineIn(io, ms);
            while (!self.done.load(.acquire)) {
                if (passed(io, deadline)) return false;
                try std.Io.sleep(io, .fromMilliseconds(1), .awake);
            }
            return true;
        }

        fn join(self: *@This()) void {
            if (self.thread) |th| th.join();
            self.thread = null;
        }
    };
}

fn countRows(allocator: std.mem.Allocator, db: *thindb.Database) !i64 {
    const vals = try helpers.collectBigints(allocator, db, "SELECT COUNT(*) FROM orders WHERE qty > 0");
    defer allocator.free(vals);
    if (vals.len != 1) return error.NoRow;
    return vals[0];
}

fn schemaRoundTrip(db: *thindb.Database) !void {
    _ = try db.createSchema("aux");
    try db.dropSchema("aux");
}

test "DDL runs while a background merge waits to commit" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const db = try thindb.Database.open(allocator, io, tmp.dir, cfg);
    defer db.close();
    const t = try db.table("orders", schema_v1, opts_v1);
    try fillTwoSegments(t);

    // CREATE SCHEMA takes the gate shared, DROP SCHEMA exclusively. Before
    // #87 the sweep held the gate for the whole merge and both queued here.
    var ddl: Background(schemaRoundTrip) = .{ .args = .{db} };
    defer ddl.join();
    var parked: ParkedMerge = .{ .t = t, .io = io, .catalog = db.owned_catalog.? };
    defer parked.finish();
    try parked.start();
    try ddl.start();
    const ran = try ddl.finishesWithin(io, 5_000);
    parked.finish();
    ddl.join();
    try std.testing.expect(ran);
    try std.testing.expect(!ddl.failed);

    try std.testing.expect(parked.worked);
    try std.testing.expectEqual(@as(usize, 1), t.segmentCount());
    try std.testing.expectEqual(@as(i64, 4), try countRows(allocator, db));
}

fn dropSchema(db: *thindb.Database, name: []const u8) !void {
    try db.dropSchema(name);
}

test "DROP SCHEMA waits for a background merge of its table" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const db = try thindb.Database.open(allocator, io, tmp.dir, cfg);
    defer db.close();
    const aux = try db.createSchema("aux");
    const t = try aux.table("orders", schema_v1, opts_v1);
    try fillTwoSegments(t);

    var drop: Background(dropSchema) = .{ .args = .{ db, "aux" } };
    defer drop.join();
    var parked: ParkedMerge = .{ .t = t, .io = io, .catalog = db.owned_catalog.? };
    defer parked.finish();
    try parked.start();
    try drop.start();
    try std.testing.expect(!try drop.finishesWithin(io, 100));

    parked.finish();
    drop.join();
    try std.testing.expect(!drop.failed);
    try std.testing.expect(parked.worked);
    try std.testing.expect(db.schema("aux") == null);
    try std.testing.expectError(error.FileNotFound, db.db_dir.access(io, "aux", .{}));
}

fn dropDatabase(catalog: *thindb.Catalog, name: []const u8) !void {
    try catalog.dropDatabase(name);
}

test "DROP DATABASE waits for a background merge of its table" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const catalog = try thindb.Catalog.open(allocator, io, tmp.dir, cfg);
    defer catalog.close();
    const other = try catalog.createDatabase("other");
    const t = try other.table("orders", schema_v1, opts_v1);
    try fillTwoSegments(t);

    var drop: Background(dropDatabase) = .{ .args = .{ catalog, "other" } };
    defer drop.join();
    var parked: ParkedMerge = .{ .t = t, .io = io, .catalog = catalog };
    defer parked.finish();
    try parked.start();
    try drop.start();
    try std.testing.expect(!try drop.finishesWithin(io, 100));

    parked.finish();
    drop.join();
    try std.testing.expect(!drop.failed);
    try std.testing.expect(parked.worked);
    try std.testing.expect(catalog.database("other") == null);
    try std.testing.expectError(error.FileNotFound, tmp.dir.access(io, "other", .{}));
}

fn closeDatabase(db: *thindb.Database) !void {
    db.close();
}

test "closing the catalog waits for a background merge, which keeps its rows" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    {
        const db = try thindb.Database.open(allocator, io, tmp.dir, cfg);
        var closed = false;
        defer if (!closed) db.close();
        const t = try db.table("orders", schema_v1, opts_v1);
        try fillTwoSegments(t);

        var close: Background(closeDatabase) = .{ .args = .{db} };
        defer close.join();
        var parked: ParkedMerge = .{ .t = t, .io = io, .catalog = db.owned_catalog.? };
        defer parked.finish();
        try parked.start();
        try close.start();
        closed = true;
        try std.testing.expect(!try close.finishesWithin(io, 100));

        parked.finish();
        close.join();
        try std.testing.expect(parked.worked);
    }
    const db = try thindb.Database.open(allocator, io, tmp.dir, cfg);
    defer db.close();
    const t = try db.openTable("orders", .{});
    try std.testing.expectEqual(@as(usize, 1), t.segmentCount());
    try std.testing.expectEqual(@as(i64, 4), try countRows(allocator, db));
}
