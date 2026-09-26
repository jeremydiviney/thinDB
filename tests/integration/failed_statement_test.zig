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
        "SELECT sqrt('x') AS v FROM fa",
        "SELECT fa.id FROM fa JOIN fb ON fa.id = fb.id WHERE fa.nope = 1",
        "SELECT id FROM fa ORDER BY nope",
        "SELECT n, COUNT(*) AS c FROM fa GROUP BY nope",
        "SELECT ROW_NUMBER() OVER (ORDER BY nope) AS r FROM fa",
        "SELECT id FROM fa WHERE n = dt",
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
