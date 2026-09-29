//! A background merge runs without a statement lease (#87): DDL and XA COMMIT
//! must not queue behind it, and a path that frees or rewrites its table stops
//! it rather than waiting it out (see also util/merge_preempt_test.zig). A
//! reader holding the table's `ddl_lock` parks the merge at its commit, which
//! sequences each race deterministically.

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
    swept: std.atomic.Value(bool) = .init(false),

    fn sweep(self: *ParkedMerge) void {
        self.worked = self.catalog.backgroundCompactSweep() catch false;
        self.swept.store(true, .release);
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

    fn sweepEndsWithin(self: *ParkedMerge, ms: i64) !bool {
        const deadline = deadlineIn(self.io, ms);
        while (!self.swept.load(.acquire)) {
            if (passed(self.io, deadline)) return false;
            try std.Io.sleep(self.io, .fromMilliseconds(1), .awake);
        }
        return true;
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

fn renameOrders(db: *thindb.Database) !void {
    try db.renameTable("orders", "renamed");
}

fn countDataFiles(dir: std.Io.Dir, io: std.Io) !usize {
    var iter_dir = try dir.openDir(io, ".", .{ .iterate = true });
    defer iter_dir.close(io);
    var it = iter_dir.iterate();
    var n: usize = 0;
    while (try it.next(io)) |entry| {
        if (std.mem.endsWith(u8, entry.name, ".dat")) n += 1;
    }
    return n;
}

test "a merge waiting to commit gives way to DDL on its table, and a later sweep merges" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const db = try thindb.Database.open(allocator, io, tmp.dir, cfg);
    defer db.close();
    const t = try db.table("orders", schema_v1, opts_v1);
    try fillTwoSegments(t);

    var rename: Background(renameOrders) = .{ .args = .{db} };
    defer rename.join();
    var parked: ParkedMerge = .{ .t = t, .io = io, .catalog = db.owned_catalog.? };
    defer parked.finish();
    try parked.start();
    try rename.start();
    // The merge stops waiting at once and deletes its output, so the sweep
    // ends while the reader still holds the table. The rename itself still
    // waits for that reader, as it waits for any statement on the table.
    try std.testing.expect(try parked.sweepEndsWithin(5_000));
    try std.testing.expect(!parked.worked);
    try std.testing.expect(!try rename.finishesWithin(io, 50));
    try std.testing.expectEqual(@as(usize, 2), try countDataFiles(t.segments_dir, io));

    parked.finish();
    rename.join();
    try std.testing.expect(!rename.failed);
    try std.testing.expectEqual(@as(usize, 2), t.segmentCount());

    try std.testing.expect(try db.backgroundCompactSweep());
    try std.testing.expectEqual(@as(usize, 1), t.segmentCount());
    try std.testing.expectEqual(@as(usize, 1), try countDataFiles(t.segments_dir, io));
    const vals = try helpers.collectBigints(allocator, db, "SELECT COUNT(*) FROM renamed WHERE qty > 0");
    defer allocator.free(vals);
    try std.testing.expectEqualSlices(i64, &.{4}, vals);
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
