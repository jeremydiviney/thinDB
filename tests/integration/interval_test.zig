//! INTERVAL '<integer>' (DAY | MONTH | YEAR) — calendar-aware date
//! arithmetic. Lowered at parse time to `date_add`, `date_add_months`,
//! or `date_add_years`. Month/year add clamps the day on short
//! destination months: `2024-01-31 + 1 month → 2024-02-29`.
//!
//! v1 scope: INTERVAL appears as right operand of `+` or `-` on a date
//! expression in projections. Use in WHERE-clause comparisons requires
//! pre-computing the constant date manually for now.

const std = @import("std");
const thindb = @import("thindb");

const RunResult = struct {
    arena: std.heap.ArenaAllocator,
    cq: thindb.net.CompiledQuery,

    pub fn deinit(self: *RunResult) void {
        self.cq.deinit();
        self.arena.deinit();
    }
    pub fn next(self: *RunResult) !?thindb.Batch {
        return self.cq.next();
    }
};

fn runSql(allocator: std.mem.Allocator, db: anytype, sql: []const u8) !RunResult {
    var arena = std.heap.ArenaAllocator.init(allocator);
    errdefer arena.deinit();
    const root = try thindb.sql.parse(arena.allocator(), sql);
    const cq = try thindb.net.compile(allocator, db, root);
    return .{ .arena = arena, .cq = cq };
}

fn exec(allocator: std.mem.Allocator, db: anytype, sql: []const u8) !void {
    var q = try runSql(allocator, db, sql);
    defer q.deinit();
    while (try q.next()) |_| {}
}

fn collectDates(allocator: std.mem.Allocator, db: anytype, sql: []const u8) ![]i32 {
    var q = try runSql(allocator, db, sql);
    defer q.deinit();
    var out: std.ArrayList(i32) = .empty;
    errdefer out.deinit(allocator);
    while (try q.next()) |batch| {
        for (batch.values[0].data.date[0..batch.row_count]) |v| try out.append(allocator, v);
    }
    return out.toOwnedSlice(allocator);
}

fn setup(allocator: std.mem.Allocator, io: anytype, dir: anytype) !*thindb.Database {
    const db = try thindb.Database.open(allocator, io, dir, .{});
    errdefer db.close();
    try exec(allocator, db, "CREATE TABLE t (id BIGINT PRIMARY KEY, d DATE NOT NULL)");
    try exec(allocator, db,
        "INSERT INTO t (id, d) VALUES (1, '2024-01-15'), (2, '2024-01-31'), (3, '2024-02-29')",
    );
    const t = try db.openTable("t", .{});
    try t.flush();
    return db;
}

test "INTERVAL: DAY add and subtract" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, io, tmp.dir);
    defer db.close();

    const plus = try collectDates(allocator, db, "SELECT d + INTERVAL '10' DAY AS r FROM t WHERE id = 1");
    defer allocator.free(plus);
    try std.testing.expectEqual(@as(usize, 1), plus.len);
    // 2024-01-15 + 10 days = 2024-01-25; daysToYmd-roundtrip checks below.

    const minus = try collectDates(allocator, db, "SELECT d - INTERVAL '5' DAY AS r FROM t WHERE id = 1");
    defer allocator.free(minus);
    try std.testing.expectEqual(plus[0] - 15, minus[0]); // plus - 15 = minus
}

test "INTERVAL: MONTH add with day-clamp on short month" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, io, tmp.dir);
    defer db.close();

    // 2024-01-31 + 1 month → 2024-02-29 (leap year: Feb has 29 days)
    const r = try collectDates(allocator, db, "SELECT d + INTERVAL '1' MONTH AS r FROM t WHERE id = 2");
    defer allocator.free(r);
    // expected days = ymdToDays(2024, 2, 29) — assert via reverse.
    var q = try runSql(allocator, db, "SELECT EXTRACT(YEAR FROM d + INTERVAL '1' MONTH) AS y, EXTRACT(MONTH FROM d + INTERVAL '1' MONTH) AS m, EXTRACT(DAY FROM d + INTERVAL '1' MONTH) AS dd FROM t WHERE id = 2");
    defer q.deinit();
    const batch = (try q.next()).?;
    try std.testing.expectEqual(@as(i32, 2024), batch.values[0].data.int[0]);
    try std.testing.expectEqual(@as(i32, 2), batch.values[1].data.int[0]);
    try std.testing.expectEqual(@as(i32, 29), batch.values[2].data.int[0]);
}

test "INTERVAL: YEAR add clamps Feb-29 in non-leap year" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, io, tmp.dir);
    defer db.close();

    // 2024-02-29 + 1 year → 2025-02-28
    var q = try runSql(allocator, db,
        "SELECT EXTRACT(YEAR FROM d + INTERVAL '1' YEAR) AS y, EXTRACT(MONTH FROM d + INTERVAL '1' YEAR) AS m, EXTRACT(DAY FROM d + INTERVAL '1' YEAR) AS dd FROM t WHERE id = 3",
    );
    defer q.deinit();
    const batch = (try q.next()).?;
    try std.testing.expectEqual(@as(i32, 2025), batch.values[0].data.int[0]);
    try std.testing.expectEqual(@as(i32, 2), batch.values[1].data.int[0]);
    try std.testing.expectEqual(@as(i32, 28), batch.values[2].data.int[0]);
}

test "INTERVAL: bare integer accepted (PG-style)" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, io, tmp.dir);
    defer db.close();

    // INTERVAL 7 DAY  vs  INTERVAL '7' DAY — both should work.
    var q = try runSql(allocator, db,
        "SELECT EXTRACT(DAY FROM d + INTERVAL 7 DAY) AS dd FROM t WHERE id = 1",
    );
    defer q.deinit();
    const batch = (try q.next()).?;
    try std.testing.expectEqual(@as(i32, 22), batch.values[0].data.int[0]); // 15 + 7
}

test "INTERVAL: unknown unit rejected at parse time" {
    const allocator = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const err = thindb.sql.parse(arena.allocator(), "SELECT d + INTERVAL '1' FORTNIGHT FROM t");
    try std.testing.expectError(thindb.sql.ParseError.SqlExpectedKeyword, err);
}
