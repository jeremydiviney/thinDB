//! Subqueries inside CASE WHEN branches. Both the boolean condition
//! (e.g. `CASE WHEN EXISTS(...)`) and the THEN-side expression
//! (`CASE WHEN ... THEN (SELECT ...) END`) go through the pre-compile
//! subquery resolver, so correlated and uncorrelated variants both
//! work without any new operator support.

const std = @import("std");
const thindb = @import("thindb");
const helpers = @import("sql_helpers.zig");
const exec = helpers.exec;
const runSql = helpers.runSql;

fn setupUsersPayments(allocator: std.mem.Allocator, io: anytype, dir: anytype) !*thindb.Database {
    const db = try thindb.Database.open(allocator, io, dir, .{});
    errdefer db.close();
    try exec(
        allocator,
        db,
        "CREATE TABLE users (id BIGINT PRIMARY KEY, name VARCHAR(16) NOT NULL)",
    );
    try exec(
        allocator,
        db,
        "INSERT INTO users (id, name) VALUES (1, 'alice'), (2, 'bob'), (3, 'carol')",
    );
    try exec(
        allocator,
        db,
        "CREATE TABLE payments (id BIGINT PRIMARY KEY, user_id BIGINT NOT NULL, amount INT NOT NULL)",
    );
    // alice has 2 payments, bob has 1, carol has 0.
    try exec(
        allocator,
        db,
        "INSERT INTO payments (id, user_id, amount) VALUES (1, 1, 100), (2, 1, 200), (3, 2, 50)",
    );
    const t1 = try db.openTable("users", .{});
    try t1.flush();
    const t2 = try db.openTable("payments", .{});
    try t2.flush();
    return db;
}

test "CASE WHEN EXISTS(correlated) — flag users with any payment" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setupUsersPayments(allocator, io, tmp.dir);
    defer db.close();

    var q = try runSql(
        allocator,
        db,
        "SELECT u.id, " ++
            "CASE WHEN EXISTS (SELECT p.id FROM payments AS p WHERE p.user_id = u.id) " ++
            "THEN 'yes' ELSE 'no' END AS has_payment " ++
            "FROM users AS u ORDER BY u.id ASC",
    );
    defer q.deinit();
    const batch = (try q.next()).?;
    try std.testing.expectEqual(@as(usize, 3), batch.row_count);
    try std.testing.expectEqualStrings("yes", batch.values[1].data.string.rowBytes(0)); // alice
    try std.testing.expectEqualStrings("yes", batch.values[1].data.string.rowBytes(1)); // bob
    try std.testing.expectEqualStrings("no", batch.values[1].data.string.rowBytes(2)); // carol
}

test "CASE WHEN NOT EXISTS(correlated) — flag users without payment" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setupUsersPayments(allocator, io, tmp.dir);
    defer db.close();

    var q = try runSql(
        allocator,
        db,
        "SELECT u.id, " ++
            "CASE WHEN NOT EXISTS (SELECT p.id FROM payments AS p WHERE p.user_id = u.id) " ++
            "THEN 1 ELSE 0 END AS no_pmt " ++
            "FROM users AS u ORDER BY u.id ASC",
    );
    defer q.deinit();
    const batch = (try q.next()).?;
    try std.testing.expectEqual(@as(usize, 3), batch.row_count);
    try std.testing.expectEqual(@as(i32, 0), batch.values[1].data.int[0]); // alice has pmts
    try std.testing.expectEqual(@as(i32, 0), batch.values[1].data.int[1]); // bob has pmts
    try std.testing.expectEqual(@as(i32, 1), batch.values[1].data.int[2]); // carol doesn't
}

test "SUM(CASE WHEN EXISTS(correlated) ...) — count users with payments" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setupUsersPayments(allocator, io, tmp.dir);
    defer db.close();

    var q = try runSql(
        allocator,
        db,
        "SELECT SUM(CASE WHEN EXISTS (SELECT p.id FROM payments AS p WHERE p.user_id = u.id) " ++
            "THEN 1 ELSE 0 END) AS active_users " ++
            "FROM users AS u",
    );
    defer q.deinit();
    const batch = (try q.next()).?;
    try std.testing.expectEqual(@as(i64, 2), batch.values[0].data.bigint[0]);
}

test "CASE WHEN IN(correlated) — flag big spenders" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setupUsersPayments(allocator, io, tmp.dir);
    defer db.close();

    // u.id IN (SELECT p.user_id FROM payments p WHERE p.amount > 100)
    //   payments > 100: id=2 (user 1, amount 200). So only u.id=1 matches.
    var q = try runSql(
        allocator,
        db,
        "SELECT u.id, " ++
            "CASE WHEN u.id IN (SELECT p.user_id FROM payments AS p WHERE p.amount > 100) " ++
            "THEN 'big' ELSE 'small' END AS tier " ++
            "FROM users AS u ORDER BY u.id ASC",
    );
    defer q.deinit();
    const batch = (try q.next()).?;
    try std.testing.expectEqual(@as(usize, 3), batch.row_count);
    try std.testing.expectEqualStrings("big", batch.values[1].data.string.rowBytes(0));
    try std.testing.expectEqualStrings("small", batch.values[1].data.string.rowBytes(1));
    try std.testing.expectEqualStrings("small", batch.values[1].data.string.rowBytes(2));
}
