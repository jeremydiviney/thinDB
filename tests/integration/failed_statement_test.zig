//! A statement that fails, while compiling or while running, must give back
//! everything it took: every operator it built, with the statement-gate
//! lease its scans hold and the tracked memory its accountant counts. One
//! leaked scan keeps the gate read-locked, so every later DDL waits forever;
//! one leaked tracked byte keeps `Database.close` waiting forever.

const std = @import("std");
const thindb = @import("thindb");
const helpers = @import("sql_helpers.zig");

/// Runs `sql` to its end and reports whether it failed.
fn fails(comptime run: anytype, allocator: std.mem.Allocator, db: *thindb.Database, sql: []const u8) bool {
    var r = run(allocator, db, sql) catch return true;
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
    try helpers.exec(allocator, db, "CREATE TABLE fb (id BIGINT PRIMARY KEY, n INT, dt DATE, u UUID)");
    try helpers.exec(allocator, db, "CREATE TABLE fc (id BIGINT PRIMARY KEY, n INT)");
    try helpers.exec(allocator, db, "INSERT INTO fa VALUES (1, 1, 'a', '2024-01-01'), (2, 2, 'b', '2024-01-02')");
    try helpers.exec(allocator, db, "INSERT INTO fb (id, n, dt) VALUES (1, 1, '2024-01-01'), (2, 0, '2024-01-02')");
    try helpers.exec(allocator, db, "INSERT INTO fc VALUES (1, 1), (2, 1)");

    const statements = [_][]const u8{
        // Join keys that never compare.
        "SELECT fa.id FROM fa JOIN fb ON fa.n = fb.dt",
        "SELECT fa.id FROM fa JOIN fb ON fa.id = fb.id AND fa.n = fb.dt",
        "SELECT fa.id FROM fa LEFT JOIN fb ON fa.n = fb.dt",
        "SELECT fa.id FROM fa JOIN fb ON fa.n < fb.dt",
        "SELECT fa.id FROM fa JOIN fb ON fa.id = fb.id JOIN fc ON fb.dt = fc.n",
        "SELECT fa.id FROM fa JOIN fb ON fa.id = fb.id WHERE fa.n > 0 AND fb.n >= 0 AND fa.n = fb.u",
        "WITH x AS (SELECT fa.id FROM fa JOIN fb ON fa.n = fb.dt) SELECT id FROM x",
        "SELECT id FROM fc WHERE id IN (SELECT fa.id FROM fa JOIN fb ON fa.n = fb.dt)",
        // Columns and functions that don't resolve, or don't take their arguments.
        "SELECT sqrt('x', 'y') AS v FROM fa",
        "SELECT fa.id FROM fa JOIN fb ON fa.id = fb.id WHERE fa.nope = 1",
        "SELECT id FROM fa ORDER BY nope",
        "SELECT n, COUNT(*) AS c FROM fa GROUP BY nope",
        "SELECT ROW_NUMBER() OVER (ORDER BY nope) AS r FROM fa",
        "SELECT id FROM fb WHERE n = u",
        "SELECT id FROM fa WHERE n IN (SELECT dt FROM fb)",
        "SELECT id FROM fa WHERE EXISTS (SELECT 1 FROM fb WHERE fb.dt = fa.n)",
        "SELECT id FROM fa UNION ALL SELECT id, n FROM fb",
        "SELECT fa.id, sqrt(fb.n, fb.dt) AS v FROM fa JOIN fb ON fa.id = fb.id",
        // A correlated lookup whose key matches two rows.
        "SELECT fb.id, (SELECT fc.id FROM fc WHERE fc.n = fb.n) AS x FROM fb",
        // Fails while running, past the join.
        "SELECT fa.id, CAST(fa.n AS DECIMAL(38,0)) * 90000000000000000000000000000000000000 AS v FROM fa JOIN fb ON fa.id = fb.id",
    };
    for (statements) |sql| {
        if (!fails(helpers.runSql, allocator, db, sql)) {
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

const FailingRun = struct { failed: bool, induced: bool };

/// Runs `sql` to its end with its allocation numbered `k` failing.
fn runFailingAt(failing: *FailingAllocator, k: usize, comptime run: anytype, db: *thindb.Database, sql: []const u8) FailingRun {
    failing.fail_index.store(failing.alloc_index.load(.monotonic) + k, .monotonic);
    failing.has_induced_failure.store(false, .monotonic);
    const failed = fails(run, failing.allocator(), db, sql);
    const induced = failing.has_induced_failure.load(.acquire);
    failing.fail_index.store(std.math.maxInt(usize), .monotonic);
    return .{ .failed = failed, .induced = induced };
}

fn printInducedFailure(failing: *FailingAllocator, k: usize) void {
    std.debug.print("  after failing allocation {d}, made at:\n", .{k});
    const trace: std.debug.StackTrace = .{ .return_addresses = failing.stack_addresses[0..failing.stack_len], .skipped = .none };
    std.debug.dumpStackTrace(&trace);
}

/// What a query gave its client: an error, or its rows as a count and a
/// digest of their values that ignores their order.
const Outcome = union(enum) {
    failed,
    rows: struct { count: u64, digest: u64 },
};

/// Runs `sql` to its end. The digest's scratch comes from the testing
/// allocator, so it never takes an allocation number from `allocator`.
fn queryOutcome(comptime run: anytype, allocator: std.mem.Allocator, db: *thindb.Database, sql: []const u8) !Outcome {
    var r = run(allocator, db, sql) catch return .failed;
    defer r.deinit();
    var row: std.ArrayList(u8) = .empty;
    defer row.deinit(std.testing.allocator);
    var count: u64 = 0;
    var digest: u64 = 0;
    while (r.next() catch return .failed) |batch| {
        for (0..batch.row_count) |i| {
            row.clearRetainingCapacity();
            for (batch.values) |column| {
                const valid = column.isValid(i);
                try row.append(std.testing.allocator, @intFromBool(valid));
                if (valid) try column.appendValueBytes(std.testing.allocator, &row, @intCast(i));
            }
            digest +%= std.hash.Wyhash.hash(0, row.items);
            count += 1;
        }
    }
    return .{ .rows = .{ .count = count, .digest = digest } };
}

/// Runs `sql` once per allocation it makes, failing that allocation, and
/// checks each failed run gave back its gate lease and tracked memory, and
/// that the client saw an error or the rows the statement returns without
/// a failure: never an empty or partial result in their place.
fn expectEveryAllocationFailureClean(failing: *FailingAllocator, db: *thindb.Database, sql: []const u8) !void {
    const expected = try queryOutcome(helpers.runSqlCtx, failing.allocator(), db, sql);
    if (expected == .failed) {
        std.debug.print("fails without an induced failure: {s}\n", .{sql});
        return error.TestUnexpectedResult;
    }
    var k: usize = 0;
    while (true) : (k += 1) {
        failing.fail_index.store(failing.alloc_index.load(.monotonic) + k, .monotonic);
        failing.has_induced_failure.store(false, .monotonic);
        const got = try queryOutcome(helpers.runSqlCtx, failing.allocator(), db, sql);
        const induced = failing.has_induced_failure.load(.acquire);
        failing.fail_index.store(std.math.maxInt(usize), .monotonic);
        waitGateIdle(db);
        expectGateIdle(db, sql) catch |err| {
            printInducedFailure(failing, k);
            return err;
        };
        if (got != .failed and !std.meta.eql(got, expected)) {
            std.debug.print("a failed allocation changed the result instead of failing the statement: {s}\n", .{sql});
            if (induced) printInducedFailure(failing, k);
            return error.TestUnexpectedResult;
        }
        if (!induced) {
            if (got == .failed) {
                std.debug.print("fails without an induced failure: {s}\n", .{sql});
                return error.TestUnexpectedResult;
            }
            return;
        }
    }
}

const tdb = thindb.tdb;

fn runningTotal(
    ctx: *const thindb.udf.TvfContext,
    parts: []const thindb.udf.TvfPartition,
    out: *thindb.udf.TvfOutput,
) !void {
    _ = ctx;
    const part = &parts[0];
    var running: i64 = 0;
    for (0..part.row_count) |i| {
        running += part.columns[2].data.bigint[i];
        try out.columns[0].data.bigint.append(out.allocator, part.columns[0].data.bigint[i]);
        try out.columns[1].data.bigint.append(out.allocator, running);
    }
}

const running_total_input = [_]thindb.Column{
    .{ .name = "id", .type = .bigint },
    .{ .name = "g", .type = .int, .nullable = true },
    .{ .name = "amt", .type = .bigint },
};
const running_total_output = [_]thindb.Column{
    .{ .name = "id", .type = .bigint },
    .{ .name = "running", .type = .bigint },
};

/// A two-input table UDF: each row's amount times its group's scale from
/// the second input, 0 when the group has none.
fn scaledByGroup(
    ctx: *const thindb.udf.TvfContext,
    parts: []const thindb.udf.TvfPartition,
    out: *thindb.udf.TvfOutput,
) !void {
    _ = ctx;
    const rows = &parts[0];
    const scales = &parts[1];
    const scale: i64 = if (scales.row_count > 0) scales.columns[1].data.bigint[0] else 0;
    for (0..rows.row_count) |i| {
        try out.columns[0].data.bigint.append(out.allocator, rows.columns[0].data.bigint[i]);
        try out.columns[1].data.bigint.append(out.allocator, rows.columns[2].data.bigint[i] * scale);
    }
}

const scale_input = [_]thindb.Column{
    .{ .name = "g", .type = .int, .nullable = true },
    .{ .name = "k", .type = .bigint },
};
const scaled_output = [_]thindb.Column{
    .{ .name = "id", .type = .bigint },
    .{ .name = "v", .type = .bigint },
};

/// A row-aligned SDK table function whose string column rides through.
const previous_id = struct {
    pub const spec = tdb.TableFnSpec{ .name = "previous_id", .execution = .partitioned, .row_aligned = true };
    pub const Input = struct { id: i64, g: ?i32 };
    pub const Carry = struct { s: ?[]const u8 };
    pub const passthrough = .{ "id", "s" };
    pub const Output = struct { id: i64, s: ?[]const u8, prev: ?i64 };
    pub const Computed = struct { prev: ?i64 };

    pub fn process(ctx: *tdb.Ctx, p: tdb.Partition(Input), out: *tdb.Writer(Computed)) !void {
        _ = ctx;
        const ids = p.col(.id);
        var prev: ?i64 = null;
        for (0..p.len) |i| {
            try out.row(.{ .prev = prev });
            prev = ids[i];
        }
    }
};

/// Two small tables, one flushed with a memtable tail, a flushed table with
/// enough rows that its integer and date columns are stored narrow, a table UDF,
/// an SDK table function, a SQL inline function and a file for each reader.
fn openCorpusDb(allocator: std.mem.Allocator, tmp: std.testing.TmpDir, file_root: []const u8, max_dop: usize) !*thindb.Database {
    const io = std.testing.io;
    try tmp.dir.writeFile(io, .{ .sub_path = "oc.csv", .data = "id,g,s\n1,1,a\n2,2,b\n5,1,\n" });
    try tmp.dir.writeFile(io, .{ .sub_path = "oj.ndjson", .data = "{\"id\":1,\"s\":\"p\"}\n{\"id\":4,\"s\":\"q\",\"m\":{\"k\":1}}\n" });
    const db = try thindb.Database.open(allocator, io, tmp.dir, .{ .max_dop = max_dop, .file_scan_access = .{ .root = file_root } });
    errdefer db.close();
    try helpers.exec(allocator, db, "CREATE TABLE oa (id BIGINT PRIMARY KEY, g INT, s VARCHAR(10), d DOUBLE, dt DATE)");
    try helpers.exec(allocator, db, "CREATE TABLE ob (id BIGINT PRIMARY KEY, g INT, s VARCHAR(10))");
    try helpers.exec(allocator, db, "INSERT INTO oa VALUES (1, 1, 'a', 1.5, '2024-01-01'), (2, 2, 'b', 2.5, '2024-01-02'), (3, 1, 'c', NULL, '2024-02-01')");
    try (try db.openTable("oa", .{})).flush();
    try helpers.exec(allocator, db, "INSERT INTO oa VALUES (4, 2, 'd', 4.5, '2024-03-01')");
    try helpers.exec(allocator, db, "INSERT INTO ob VALUES (1, 1, 'x'), (2, 3, 'y'), (4, 2, 'z')");
    try helpers.exec(allocator, db, "CREATE TABLE oe (id BIGINT PRIMARY KEY, g INT, n BIGINT, dt DATE)");
    try helpers.exec(allocator, db, "INSERT INTO oe VALUES (1, 1, 10, '2024-01-01'), (2, 2, 20, '2024-01-02'), (3, 1, 30, '2024-01-03'), (4, 2, 40, '2024-01-04'), (5, 1, 50, '2024-01-05'), (6, 2, 60, '2024-01-06'), (7, 1, 70, '2024-01-07'), (8, 2, 80, '2024-01-08')");
    try (try db.openTable("oe", .{})).flush();
    try db.registerTableUdf(.{
        .name = "running_total",
        .input_schemas = &.{&running_total_input},
        .output_schema = &running_total_output,
        .execution = .either,
        .process = runningTotal,
    });
    try db.registerTableUdf(.{
        .name = "scaled_by_group",
        .input_schemas = &.{ &running_total_input, &scale_input },
        .output_schema = &scaled_output,
        .execution = .partitioned,
        .process = scaledByGroup,
    });
    try db.registerTableFn(previous_id);
    try helpers.exec(allocator, db, "CREATE FUNCTION oa_in(pg INT) RETURNS TABLE AS (SELECT id, s FROM oa WHERE g = pg)");
    return db;
}

const query_statements = [_][]const u8{
    "SELECT id, s FROM oa WHERE g = 1",
    // A level of the predicate is rebuilt before the level above it.
    "SELECT id, g, dt FROM oa WHERE (g = 1 AND id > 1) OR dt > '2024-01-15'",
    // A filter the scan evaluates over borrowed blocks, expanding each of
    // the four narrow-encoded columns in turn.
    "SELECT id, g, n, dt FROM oe WHERE (g = 1 AND id > 2) OR n > 60",
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
    "WITH src AS (SELECT id, g, d FROM oa), w AS (SELECT id, g, ROW_NUMBER() OVER (PARTITION BY g ORDER BY id) AS r FROM src) SELECT w.id, w.r, s.d FROM w JOIN src s ON s.id = w.id",
    "WITH src AS (SELECT id, g, d FROM oa), w AS (SELECT id, ROW_NUMBER() OVER (PARTITION BY g ORDER BY id) AS r FROM src) SELECT id, r FROM w UNION ALL SELECT id, id FROM src",
    "SELECT id FROM oa UNION ALL SELECT id FROM ob ORDER BY 1",
    "SELECT g FROM oa UNION SELECT g FROM ob",
    "WITH c AS (SELECT g, COUNT(*) AS n FROM oa GROUP BY g) SELECT a.g, b.n FROM c a JOIN c b ON a.g = b.g",
    "SELECT x.g, x.n FROM (SELECT g, COUNT(*) AS n FROM oa GROUP BY g) x WHERE x.n > 1",
    "SELECT g, SUM(d) AS t FROM oa GROUP BY g WITH ROLLUP",
    "SELECT g, COUNT(*) AS n FROM oa GROUP BY g HAVING COUNT(*) > 1",
};

const table_function_statements = [_][]const u8{
    "SELECT id, running FROM TABLE(running_total((SELECT id, g, id * 10 AS amt FROM oa)) PARTITION BY g ORDER BY id)",
    "SELECT id, running FROM TABLE(running_total((SELECT id, g, id * 10 AS amt FROM oa)) ORDER BY id)",
    "SELECT r.id, r.running, ob.s FROM TABLE(running_total((SELECT id, g, id * 10 AS amt FROM oa)) PARTITION BY g ORDER BY id) r JOIN ob ON r.id = ob.id",
    "WITH r AS (SELECT id, running FROM TABLE(running_total((SELECT id, g, id * 10 AS amt FROM oa)) PARTITION BY g ORDER BY id)) SELECT COUNT(*) AS n, MAX(running) AS m FROM r",
    "WITH src AS (SELECT id, g, id * 10 AS amt FROM oa) SELECT id, running FROM TABLE(running_total((SELECT id, g, amt FROM src)) PARTITION BY g ORDER BY id)",
    // A shared stage the table function reads its input from in place.
    "WITH src AS (SELECT id, g, id * 10 AS amt FROM oa), r AS (SELECT id, running FROM TABLE(running_total((SELECT id, g, amt FROM src)) PARTITION BY g ORDER BY id)) SELECT r.id, r.running, s.amt FROM r JOIN src s ON s.id = r.id",
    "WITH src AS (SELECT id, g, id * 10 AS amt FROM oa), r AS (SELECT id, v FROM TABLE(scaled_by_group((SELECT id, g, amt FROM src), (SELECT g, id AS k FROM ob)) PARTITION BY g)) SELECT r.id, r.v, s.amt FROM r JOIN src s ON s.id = r.id",
    // The table function runs the shared stage before anything else reads it.
    "WITH src AS (SELECT id, g, id * 10 AS amt FROM oa), r AS (SELECT id, running FROM TABLE(running_total((SELECT id, g, amt FROM src)) PARTITION BY g ORDER BY id)) SELECT id, running FROM r UNION ALL SELECT id, amt FROM src",
    "WITH src AS (SELECT id, g, id * 10 AS amt FROM oa), r AS (SELECT id, v FROM TABLE(scaled_by_group((SELECT id, g, amt FROM src), (SELECT g, id AS k FROM ob)) PARTITION BY g)) SELECT id, v FROM r UNION ALL SELECT id, amt FROM src",
    "SELECT id, s, prev FROM TABLE(previous_id((SELECT id, g, s FROM oa)) PARTITION BY g ORDER BY id)",
    "SELECT id, s FROM oa_in(1)",
    "SELECT f.id, ob.s FROM oa_in(2) f JOIN ob ON f.id = ob.id",
    "WITH c AS (SELECT id FROM oa_in(1)) SELECT COUNT(*) AS n FROM c",
    "SELECT id, s FROM read_csv('oc.csv', header=true) WHERE g = 1",
    "SELECT c.id, oa.s FROM 'oc.csv' c JOIN oa ON c.id = oa.id",
    "SELECT id, s, m FROM read_json('oj.ndjson') ORDER BY id",
};

test "a statement that runs out of memory at any allocation gives back everything it took" {
    var failing: FailingAllocator = .{ .child = std.testing.allocator };
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const file_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}", .{tmp.sub_path});
    defer std.testing.allocator.free(file_root);
    const db = try openCorpusDb(failing.allocator(), tmp, file_root, 1);
    for (query_statements ++ table_function_statements) |sql| try expectEveryAllocationFailureClean(&failing, db, sql);
    db.close();
}

test "a statement that runs out of memory in parallel gives back everything it took" {
    thindb.exec.table_fn.force_parallel_in_tests = true;
    defer thindb.exec.table_fn.force_parallel_in_tests = false;
    var failing: FailingAllocator = .{ .child = std.testing.allocator };
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const file_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}", .{tmp.sub_path});
    defer std.testing.allocator.free(file_root);
    const db = try openCorpusDb(failing.allocator(), tmp, file_root, 4);
    for (query_statements ++ table_function_statements) |sql| try expectEveryAllocationFailureClean(&failing, db, sql);
    db.close();
}

const DmlCase = struct {
    sql: []const u8,
    /// The table the statement writes.
    target: []const u8,
};

/// Run against `createDmlDb`'s table: rows 1-3 flushed, rows 4-5 in the
/// memtable.
const dml_cases = [_]DmlCase{
    .{ .sql = "INSERT INTO da VALUES (10, 1, 100, 'x'), (11, 2, 110, 'y')", .target = "da" },
    .{ .sql = "INSERT INTO da SELECT id + 100, g, n, s FROM da WHERE g = 1", .target = "da" },
    .{ .sql = "INSERT INTO da VALUES (1, 1, 7, 'a'), (20, 2, 200, 'z') ON DUPLICATE KEY UPDATE n = n + VALUES(n)", .target = "da" },
    .{ .sql = "CREATE TABLE dc AS SELECT id, g, n FROM da WHERE g = 2", .target = "dc" },
    .{ .sql = "UPDATE da SET n = n + 1 WHERE id = 1", .target = "da" },
    .{ .sql = "UPDATE da SET n = n * 2 WHERE n + g > 40", .target = "da" },
    .{ .sql = "UPDATE da SET n = n + g WHERE g * 2 + id > 5", .target = "da" },
    .{ .sql = "DELETE FROM da WHERE id = 2", .target = "da" },
    .{ .sql = "DELETE FROM da WHERE id + g = 4", .target = "da" },
    .{ .sql = "DELETE FROM da WHERE id * 2 > 5 AND g = 1", .target = "da" },
};

fn openDmlDb(allocator: std.mem.Allocator, dir: std.Io.Dir) !*thindb.Database {
    return thindb.Database.open(allocator, std.testing.io, dir, .{ .auto_flush_secs = 0 });
}

fn createDmlDb(allocator: std.mem.Allocator, dir: std.Io.Dir) !*thindb.Database {
    const db = try openDmlDb(allocator, dir);
    errdefer db.close();
    try helpers.exec(allocator, db, "CREATE TABLE da (id BIGINT PRIMARY KEY, g INT, n BIGINT, s VARCHAR(10))");
    try helpers.exec(allocator, db, "INSERT INTO da VALUES (1, 1, 10, 'a'), (2, 2, 20, 'b'), (3, 1, 30, 'c')");
    try (try db.openTable("da", .{})).flush();
    try helpers.exec(allocator, db, "INSERT INTO da VALUES (4, 2, 40, 'd'), (5, 1, 50, 'e')");
    return db;
}

/// Every row of `table` as `id * 1000 + n`, sorted; null when there is no
/// such table.
fn tableRows(allocator: std.mem.Allocator, db: *thindb.Database, table: []const u8) !?[]i64 {
    var buf: [64]u8 = undefined;
    const sql = try std.fmt.bufPrint(&buf, "SELECT id * 1000 + n AS v FROM {s} ORDER BY v", .{table});
    return helpers.collectBigints(allocator, db, sql) catch |err| {
        if (err == error.TableNotFound) return null;
        return err;
    };
}

fn sameRows(a: ?[]const i64, b: ?[]const i64) bool {
    if (a == null or b == null) return a == null and b == null;
    return std.mem.eql(i64, a.?, b.?);
}

const DmlRun = struct {
    run: FailingRun,
    fenced: bool,
    /// The target as readers saw it after the run; null when fenced.
    live: ?[]i64,
};

/// Runs `case.sql` on a fresh `createDmlDb` in `dir`, failing its allocation
/// numbered `k`, and checks the run gave back its gate lease and tracked
/// memory.
fn runDmlFailingAt(failing: *FailingAllocator, dir: std.Io.Dir, case: DmlCase, k: usize) !DmlRun {
    const allocator = failing.allocator();
    const db = try createDmlDb(allocator, dir);
    var close_db = true;
    // Closing waits for a leaked lease forever; the leak check reports it.
    defer if (close_db) db.close();
    const run = runFailingAt(failing, k, helpers.runSql, db, case.sql);
    waitGateIdle(db);
    expectGateIdle(db, case.sql) catch |err| {
        close_db = false;
        printInducedFailure(failing, k);
        return err;
    };
    const fenced = db.config.statement_gate.?.recovery_required.load(.acquire);
    return .{ .run = run, .fenced = fenced, .live = if (fenced) null else try tableRows(allocator, db, case.target) };
}

/// Runs `case.sql` once per allocation it makes, failing that allocation.
/// A failed run gives back its gate lease and tracked memory, and a reopen
/// finds its target as readers saw it (unless the run fenced the database
/// with RecoveryRequired) and either as it was or as the statement leaves
/// it. A run may fail after its write took effect, as a lost reply would.
fn expectEveryDmlAllocationFailureClean(failing: *FailingAllocator, tmp: std.testing.TmpDir, case: DmlCase) !void {
    const allocator = failing.allocator();
    const io = std.testing.io;
    var before: ?[]i64 = null;
    defer if (before) |rows| allocator.free(rows);
    var after: ?[]i64 = null;
    defer if (after) |rows| allocator.free(rows);
    {
        var dir = try tmp.dir.createDirPathOpen(io, "reference", .{});
        defer tmp.dir.deleteTree(io, "reference") catch {};
        defer dir.close(io);
        const db = try createDmlDb(allocator, dir);
        defer db.close();
        before = try tableRows(allocator, db, case.target);
        try helpers.exec(allocator, db, case.sql);
        after = try tableRows(allocator, db, case.target);
    }

    var k: usize = 0;
    while (true) : (k += 1) {
        var dir = try tmp.dir.createDirPathOpen(io, "run", .{});
        defer tmp.dir.deleteTree(io, "run") catch {};
        defer dir.close(io);
        const outcome = try runDmlFailingAt(failing, dir, case, k);
        defer if (outcome.live) |rows| allocator.free(rows);
        const db = try openDmlDb(allocator, dir);
        defer db.close();
        const now = try tableRows(allocator, db, case.target);
        defer if (now) |rows| allocator.free(rows);
        const diverged = !outcome.fenced and !sameRows(outcome.live, now);
        const torn = !sameRows(now, before) and !sameRows(now, after);
        if (diverged or torn) {
            std.debug.print("{s} reopens as {any} (readers saw {any}, before {any}, after {any}), fenced={}, failed={}: {s}\n", .{ case.target, now, outcome.live, before, after, outcome.fenced, outcome.run.failed, case.sql });
            printInducedFailure(failing, k);
            return error.TestUnexpectedResult;
        }
        if (!outcome.run.induced) {
            if (outcome.run.failed) {
                std.debug.print("fails without an induced failure: {s}\n", .{case.sql});
                return error.TestUnexpectedResult;
            }
            return;
        }
    }
}

test "a write that runs out of memory at any allocation leaves its table as it was or as it wrote it" {
    var failing: FailingAllocator = .{ .child = std.testing.allocator };
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    for (dml_cases) |case| try expectEveryDmlAllocationFailureClean(&failing, tmp, case);
}
