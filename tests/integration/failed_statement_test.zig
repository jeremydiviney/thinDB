//! A statement that fails, while compiling or while running, must give back
//! everything it took: every operator it built, with the statement-gate
//! lease its scans hold and the tracked memory its accountant counts. One
//! leaked scan keeps the gate read-locked, so every later DDL waits forever;
//! one leaked tracked byte keeps `Database.close` waiting forever.

const std = @import("std");
const thindb = @import("thindb");
const helpers = @import("sql_helpers.zig");

/// Runs `sql` to its end and reports whether it failed.
fn fails(allocator: std.mem.Allocator, db: *thindb.Database, sql: []const u8) bool {
    var r = helpers.runSql(allocator, db, sql) catch return true;
    defer r.deinit();
    while (r.next() catch return true) |_| {}
    return false;
}

fn expectGateIdle(db: *thindb.Database, sql: []const u8) !void {
    const gate = db.config.statement_gate.?;
    std.testing.expectEqual(@as(usize, 0), gate.owners.count()) catch |err| {
        std.debug.print("statement lease leaked by: {s}\n", .{sql});
        return err;
    };
    std.testing.expectEqual(@as(usize, 0), gate.allocator_owners) catch |err| {
        std.debug.print("tracked memory leaked by: {s}\n", .{sql});
        return err;
    };
}

/// Workers still unwinding a failed statement free their buffers, and with
/// the last of them the tracked memory's lease, just after it returns. A
/// leak never drains, so waiting a bounded while is enough.
fn waitGateIdle(db: *thindb.Database) void {
    const gate = db.config.statement_gate.?;
    var waited_ms: u32 = 0;
    while (waited_ms < 5000) : (waited_ms += 1) {
        gate.owners_mutex.lockUncancelable(gate.io);
        const owners = gate.allocator_owners;
        gate.owners_mutex.unlock(gate.io);
        if (owners == 0) return;
        std.Io.sleep(gate.io, .fromMilliseconds(1), .awake) catch return;
    }
}

test "failed statements leave the statement gate idle" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const db = try thindb.Database.open(allocator, std.testing.io, tmp.dir, .{});
    try helpers.exec(allocator, db, "CREATE TABLE fa (id BIGINT PRIMARY KEY, n INT, s VARCHAR(10), dt DATE)");
    try helpers.exec(allocator, db, "CREATE TABLE fb (id BIGINT PRIMARY KEY, n INT, dt DATE)");
    try helpers.exec(allocator, db, "CREATE TABLE fc (id BIGINT PRIMARY KEY, n INT)");
    try helpers.exec(allocator, db, "INSERT INTO fa VALUES (1, 1, 'a', '2024-01-01'), (2, 2, 'b', '2024-01-02')");
    try helpers.exec(allocator, db, "INSERT INTO fb VALUES (1, 1, '2024-01-01'), (2, 0, '2024-01-02')");
    try helpers.exec(allocator, db, "INSERT INTO fc VALUES (1, 1), (2, 1)");

    const statements = [_][]const u8{
        // Join keys that never compare.
        "SELECT fa.id FROM fa JOIN fb ON fa.n = fb.dt",
        "SELECT fa.id FROM fa JOIN fb ON fa.id = fb.id AND fa.n = fb.dt",
        "SELECT fa.id FROM fa LEFT JOIN fb ON fa.n = fb.dt",
        "SELECT fa.id FROM fa JOIN fb ON fa.n < fb.dt",
        "SELECT fa.id FROM fa JOIN fb ON fa.id = fb.id JOIN fc ON fb.dt = fc.n",
        "SELECT fa.id FROM fa JOIN fb ON fa.id = fb.id WHERE fa.n > 0 AND fb.n >= 0 AND fa.n = fb.dt",
        "WITH x AS (SELECT fa.id FROM fa JOIN fb ON fa.n = fb.dt) SELECT id FROM x",
        "SELECT id FROM fc WHERE id IN (SELECT fa.id FROM fa JOIN fb ON fa.n = fb.dt)",
        // Columns and functions that don't resolve, or don't take their arguments.
        "SELECT sqrt('x', 'y') AS v FROM fa",
        "SELECT fa.id FROM fa JOIN fb ON fa.id = fb.id WHERE fa.nope = 1",
        "SELECT id FROM fa ORDER BY nope",
        "SELECT n, COUNT(*) AS c FROM fa GROUP BY nope",
        "SELECT ROW_NUMBER() OVER (ORDER BY nope) AS r FROM fa",
        "SELECT id FROM fa WHERE n = dt",
        "SELECT id FROM fa WHERE n IN (SELECT dt FROM fb)",
        "SELECT id FROM fa WHERE EXISTS (SELECT 1 FROM fb WHERE fb.dt = fa.n)",
        "SELECT id FROM fa UNION ALL SELECT id, n FROM fb",
        "SELECT fa.id, sqrt(fb.dt) AS v FROM fa JOIN fb ON fa.id = fb.id",
        // A correlated lookup whose key matches two rows.
        "SELECT fb.id, (SELECT fc.id FROM fc WHERE fc.n = fb.n) AS x FROM fb",
        // Fails while running, past the join.
        "SELECT fa.id, CAST(fa.n AS DECIMAL(38,0)) * 90000000000000000000000000000000000000 AS v FROM fa JOIN fb ON fa.id = fb.id",
    };
    for (statements) |sql| {
        if (!fails(allocator, db, sql)) {
            std.debug.print("expected a failure: {s}\n", .{sql});
            return error.TestUnexpectedSuccess;
        }
        try expectGateIdle(db, sql);
    }
    try helpers.exec(allocator, db, "DROP TABLE fc");
    db.close();
}

/// Fails the one allocation numbered `fail_index`. Unlike
/// `std.testing.FailingAllocator` it counts atomically: a statement's
/// workers allocate from their own threads. It never grows in place, so
/// every growth is an allocation that can fail.
const FailingAllocator = struct {
    child: std.mem.Allocator,
    alloc_index: std.atomic.Value(usize) = .init(0),
    fail_index: std.atomic.Value(usize) = .init(std.math.maxInt(usize)),
    has_induced_failure: std.atomic.Value(bool) = .init(false),
    stack_addresses: [32]usize = @splat(0),
    stack_len: usize = 0,

    fn allocator(self: *FailingAllocator) std.mem.Allocator {
        return .{ .ptr = self, .vtable = &.{
            .alloc = alloc,
            .resize = std.mem.Allocator.noResize,
            .remap = std.mem.Allocator.noRemap,
            .free = free,
        } };
    }

    fn alloc(ctx: *anyopaque, len: usize, alignment: std.mem.Alignment, ret_addr: usize) ?[*]u8 {
        const self: *FailingAllocator = @ptrCast(@alignCast(ctx));
        if (self.alloc_index.fetchAdd(1, .monotonic) == self.fail_index.load(.monotonic)) {
            const trace = std.debug.captureCurrentStackTrace(.{ .first_address = ret_addr }, &self.stack_addresses);
            self.stack_len = trace.return_addresses.len;
            self.has_induced_failure.store(true, .release);
            return null;
        }
        return self.child.rawAlloc(len, alignment, ret_addr);
    }

    fn free(ctx: *anyopaque, buf: []u8, alignment: std.mem.Alignment, ret_addr: usize) void {
        const self: *FailingAllocator = @ptrCast(@alignCast(ctx));
        self.child.rawFree(buf, alignment, ret_addr);
    }
};

/// Runs `sql` once per allocation it makes, failing that allocation, and
/// checks each failed run gave back its gate lease and tracked memory.
fn expectEveryAllocationFailureClean(failing: *FailingAllocator, db: *thindb.Database, sql: []const u8) !void {
    const allocator = failing.allocator();
    var k: usize = 0;
    while (true) : (k += 1) {
        failing.fail_index.store(failing.alloc_index.load(.monotonic) + k, .monotonic);
        failing.has_induced_failure.store(false, .monotonic);
        const failed = fails(allocator, db, sql);
        const induced = failing.has_induced_failure.load(.acquire);
        failing.fail_index.store(std.math.maxInt(usize), .monotonic);
        waitGateIdle(db);
        expectGateIdle(db, sql) catch |err| {
            std.debug.print("  after failing allocation {d}, made at:\n", .{k});
            const trace: std.debug.StackTrace = .{ .return_addresses = failing.stack_addresses[0..failing.stack_len], .skipped = .none };
            std.debug.dumpStackTrace(&trace);
            return err;
        };
        if (!induced) {
            if (failed) {
                std.debug.print("fails without an induced failure: {s}\n", .{sql});
                return error.TestUnexpectedResult;
            }
            return;
        }
    }
}

test "a statement that runs out of memory at any allocation gives back everything it took" {
    var failing: FailingAllocator = .{ .child = std.testing.allocator };
    const allocator = failing.allocator();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const db = try thindb.Database.open(allocator, std.testing.io, tmp.dir, .{});
    try helpers.exec(allocator, db, "CREATE TABLE oa (id BIGINT PRIMARY KEY, g INT, s VARCHAR(10), d DOUBLE, dt DATE)");
    try helpers.exec(allocator, db, "CREATE TABLE ob (id BIGINT PRIMARY KEY, g INT, s VARCHAR(10))");
    try helpers.exec(allocator, db, "INSERT INTO oa VALUES (1, 1, 'a', 1.5, '2024-01-01'), (2, 2, 'b', 2.5, '2024-01-02'), (3, 1, 'c', NULL, '2024-02-01')");
    try (try db.openTable("oa", .{})).flush();
    try helpers.exec(allocator, db, "INSERT INTO oa VALUES (4, 2, 'd', 4.5, '2024-03-01')");
    try helpers.exec(allocator, db, "INSERT INTO ob VALUES (1, 1, 'x'), (2, 3, 'y'), (4, 2, 'z')");

    const statements = [_][]const u8{
        "SELECT id, s FROM oa WHERE g = 1",
        "SELECT id * 2 AS x, UPPER(s) AS u, CASE WHEN d > 2 THEN 'hi' ELSE 'lo' END AS c FROM oa",
        "SELECT COUNT(*) AS n, SUM(d) AS sd, MIN(s) AS ms, MAX(dt) AS md FROM oa",
        "SELECT g, COUNT(*) AS n, AVG(d) AS a FROM oa GROUP BY g ORDER BY g",
        "SELECT s, COUNT(DISTINCT g) AS n FROM oa GROUP BY s",
        "SELECT DISTINCT g FROM oa",
        "SELECT id FROM oa ORDER BY d DESC, id LIMIT 2",
        "SELECT oa.id, ob.s FROM oa JOIN ob ON oa.id = ob.id",
        "SELECT oa.id, ob.s FROM oa LEFT JOIN ob ON oa.g = ob.g ORDER BY oa.id",
        "SELECT id FROM oa WHERE id IN (SELECT id FROM ob WHERE g > 1)",
        "SELECT id FROM oa WHERE EXISTS (SELECT 1 FROM ob WHERE ob.g = oa.g)",
        "SELECT id, (SELECT MAX(g) FROM ob) AS m FROM oa",
        "SELECT id, ROW_NUMBER() OVER (PARTITION BY g ORDER BY id) AS r, SUM(d) OVER (PARTITION BY g) AS t FROM oa",
        "SELECT id FROM oa UNION ALL SELECT id FROM ob ORDER BY 1",
        "SELECT g FROM oa UNION SELECT g FROM ob",
        "WITH c AS (SELECT g, COUNT(*) AS n FROM oa GROUP BY g) SELECT a.g, b.n FROM c a JOIN c b ON a.g = b.g",
        "SELECT x.g, x.n FROM (SELECT g, COUNT(*) AS n FROM oa GROUP BY g) x WHERE x.n > 1",
        "SELECT g, SUM(d) AS t FROM oa GROUP BY g WITH ROLLUP",
        "SELECT g, COUNT(*) AS n FROM oa GROUP BY g HAVING COUNT(*) > 1",
    };
    for (statements) |sql| try expectEveryAllocationFailureClean(&failing, db, sql);
    db.close();
}
