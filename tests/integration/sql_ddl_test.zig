//! CREATE TABLE / DROP TABLE / INSERT INTO over the SQL surface.
//! Drives the parser → IR → compile path that backs the MySQL and PG
//! wire listeners. Each test runs `compile` against an in-process
//! Database, then verifies via the engine API that the side effect
//! landed.

const std = @import("std");
const thindb = @import("thindb");
const helpers = @import("sql_helpers.zig");
const runSql = helpers.runSql;
const expectRunError = helpers.expectRunError;

test "sql ddl: CREATE TABLE with inline PRIMARY KEY" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    var q = try runSql(allocator, db,
        "CREATE TABLE users (id BIGINT PRIMARY KEY, name VARCHAR(64) NOT NULL, age INT)",
    );
    defer q.deinit();
    _ = try q.next();

    const t = try db.openTable("users", .{});
    try std.testing.expectEqual(@as(usize, 3), t.schema.columns.len);
    try std.testing.expectEqualStrings("id", t.schema.columns[0].name);
    try std.testing.expectEqual(thindb.types.TypeTag.bigint, @as(thindb.types.TypeTag, t.schema.columns[0].type));
    try std.testing.expectEqual(true, t.schema.unique);
    try std.testing.expectEqualStrings("id", t.schema.order_key[0]);
}

test "sql ddl: CREATE TABLE with table-level compound PRIMARY KEY" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    var q = try runSql(allocator, db,
        "CREATE TABLE events (org_id INT NOT NULL, ts BIGINT NOT NULL, payload TEXT, PRIMARY KEY (org_id, ts))",
    );
    defer q.deinit();
    _ = try q.next();

    const t = try db.openTable("events", .{});
    try std.testing.expectEqual(@as(usize, 2), t.schema.order_key.len);
    try std.testing.expectEqualStrings("org_id", t.schema.order_key[0]);
    try std.testing.expectEqualStrings("ts", t.schema.order_key[1]);
}

test "sql ddl: CREATE TABLE IF NOT EXISTS is a no-op the second time" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    var q1 = try runSql(allocator, db, "CREATE TABLE t (id BIGINT PRIMARY KEY)");
    defer q1.deinit();
    _ = try q1.next();
    var q2 = try runSql(allocator, db, "CREATE TABLE IF NOT EXISTS t (id BIGINT PRIMARY KEY)");
    defer q2.deinit();
    _ = try q2.next();
}

test "sql ddl: CREATE TABLE that already exists errors" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    var q1 = try runSql(allocator, db, "CREATE TABLE t (id BIGINT PRIMARY KEY)");
    defer q1.deinit();
    _ = try q1.next();
    try expectRunError(allocator, db, "CREATE TABLE t (id BIGINT PRIMARY KEY)", thindb.net.Error.TableAlreadyExists);
}

test "sql ddl: CREATE TABLE without any PRIMARY KEY errors" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    try std.testing.expectError(
        thindb.sql.ParseError.SqlInvalidProjection,
        thindb.sql.parse(arena.allocator(), "CREATE TABLE t (id BIGINT, name TEXT)"),
    );
}

test "sql ddl: DROP TABLE removes the table from the catalog" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    var q1 = try runSql(allocator, db, "CREATE TABLE t (id BIGINT PRIMARY KEY)");
    defer q1.deinit();
    _ = try q1.next();
    try std.testing.expect(db.findTable("t") != null);
    var q2 = try runSql(allocator, db, "DROP TABLE t");
    defer q2.deinit();
    _ = try q2.next();
    try std.testing.expect(db.findTable("t") == null);
}

test "sql ddl: DROP TABLE IF EXISTS succeeds on missing table" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    var q = try runSql(allocator, db, "DROP TABLE IF EXISTS ghost");
    defer q.deinit();
    _ = try q.next();
}

test "sql ddl: DROP TABLE on missing table errors without IF EXISTS" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    try expectRunError(allocator, db, "DROP TABLE ghost", thindb.net.Error.TableNotFound);
}

test "sql insert: positional values into all columns" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    var q1 = try runSql(allocator, db, "CREATE TABLE t (id BIGINT PRIMARY KEY, qty INT NOT NULL)");
    defer q1.deinit();
    _ = try q1.next();
    var q2 = try runSql(allocator, db, "INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)");
    defer q2.deinit();
    _ = try q2.next();
    try std.testing.expectEqual(@as(u64, 3), q2.affectedRows());

    const t = try db.openTable("t", .{});
    try t.flush();

    var q3 = try runSql(allocator, db, "SELECT id FROM t ORDER BY id ASC");
    defer q3.deinit();
    var ids: std.ArrayList(i64) = .empty;
    defer ids.deinit(allocator);
    while (try q3.next()) |b| {
        for (b.values[0].data.bigint[0..b.row_count]) |v| try ids.append(allocator, v);
    }
    try std.testing.expectEqualSlices(i64, &[_]i64{ 1, 2, 3 }, ids.items);
}

test "sql insert: named column list reorders source values" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    var q1 = try runSql(allocator, db,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, qty INT NOT NULL, tag TEXT NOT NULL)",
    );
    defer q1.deinit();
    _ = try q1.next();
    var q2 = try runSql(allocator, db, "INSERT INTO t (tag, id, qty) VALUES ('alpha', 7, 99)");
    defer q2.deinit();
    _ = try q2.next();
    try std.testing.expectEqual(@as(u64, 1), q2.affectedRows());

    const t = try db.openTable("t", .{});
    try t.flush();

    var q3 = try runSql(allocator, db, "SELECT id, qty, tag FROM t");
    defer q3.deinit();
    const b = (try q3.next()).?;
    try std.testing.expectEqual(@as(usize, 1), b.row_count);
    try std.testing.expectEqual(@as(i64, 7), b.values[0].data.bigint[0]);
    try std.testing.expectEqual(@as(i32, 99), b.values[1].data.int[0]);
    try std.testing.expectEqualStrings("alpha", b.values[2].data.string.rowBytes(0));
}

test "sql insert: NULL into a nullable column" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    var q1 = try runSql(allocator, db,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, nickname TEXT)",
    );
    defer q1.deinit();
    _ = try q1.next();
    var q2 = try runSql(allocator, db, "INSERT INTO t VALUES (1, NULL), (2, 'bob')");
    defer q2.deinit();
    _ = try q2.next();
    try std.testing.expectEqual(@as(u64, 2), q2.affectedRows());

    const t = try db.openTable("t", .{});
    try t.flush();

    var q3 = try runSql(allocator, db, "SELECT id, nickname FROM t ORDER BY id ASC");
    defer q3.deinit();
    const b = (try q3.next()).?;
    try std.testing.expectEqual(@as(usize, 2), b.row_count);
    // Row 0 (id=1) should be NULL.
    const nicks_nulls = b.values[1].nulls.?;
    try std.testing.expectEqual(false, (nicks_nulls[0] & 1) != 0);
    try std.testing.expectEqual(true, (nicks_nulls[0] & 2) != 0);
    try std.testing.expectEqualStrings("bob", b.values[1].data.string.rowBytes(1));
}

test "sql insert: NULL into a NOT NULL column errors" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    var q1 = try runSql(allocator, db,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, qty INT NOT NULL)",
    );
    defer q1.deinit();
    _ = try q1.next();
    try expectRunError(allocator, db, "INSERT INTO t VALUES (1, NULL)", thindb.net.Error.TypeMismatch);
}

test "sql insert: mismatched value count errors" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    var q1 = try runSql(allocator, db, "CREATE TABLE t (id BIGINT PRIMARY KEY, qty INT NOT NULL)");
    defer q1.deinit();
    _ = try q1.next();
    try expectRunError(allocator, db, "INSERT INTO t VALUES (1)", thindb.net.Error.BadRequest);
}

test "sql insert: int literal up-coerces to bigint" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    var q1 = try runSql(allocator, db, "CREATE TABLE t (id BIGINT PRIMARY KEY)");
    defer q1.deinit();
    _ = try q1.next();
    var q2 = try runSql(allocator, db, "INSERT INTO t VALUES (42)");
    defer q2.deinit();
    _ = try q2.next();

    const t = try db.openTable("t", .{});
    try t.flush();

    var q3 = try runSql(allocator, db, "SELECT id FROM t");
    defer q3.deinit();
    const b = (try q3.next()).?;
    try std.testing.expectEqual(@as(i64, 42), b.values[0].data.bigint[0]);
}

test "sql insert: string literal coerces to uuid" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    var q1 = try runSql(allocator, db, "CREATE TABLE t (id UUID PRIMARY KEY)");
    defer q1.deinit();
    _ = try q1.next();
    var q2 = try runSql(allocator, db, "INSERT INTO t VALUES ('123e4567-e89b-12d3-a456-426614174000')");
    defer q2.deinit();
    _ = try q2.next();

    const t = try db.openTable("t", .{});
    try t.flush();

    var q3 = try runSql(allocator, db, "SELECT id FROM t");
    defer q3.deinit();
    const b = (try q3.next()).?;
    try std.testing.expectEqual(@as(usize, 1), b.row_count);
}

test "sql roundtrip: DATE column via string literal" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    var q1 = try runSql(allocator, db, "CREATE TABLE t (id BIGINT PRIMARY KEY, d DATE NOT NULL)");
    defer q1.deinit();
    _ = try q1.next();
    var q2 = try runSql(allocator, db, "INSERT INTO t VALUES (1, '2024-01-15')");
    defer q2.deinit();
    _ = try q2.next();

    const t = try db.openTable("t", .{});
    try t.flush();

    var q3 = try runSql(allocator, db, "SELECT d FROM t");
    defer q3.deinit();
    const b = (try q3.next()).?;
    // 2024-01-15 = day 19737 since 1970-01-01.
    try std.testing.expectEqual(@as(i32, 19737), b.values[0].data.date[0]);
}

test "sql roundtrip: DATETIME column with microseconds" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    var q1 = try runSql(allocator, db, "CREATE TABLE t (id BIGINT PRIMARY KEY, ts DATETIME NOT NULL)");
    defer q1.deinit();
    _ = try q1.next();
    var q2 = try runSql(allocator, db, "INSERT INTO t VALUES (1, '2024-01-15 12:30:45.123456')");
    defer q2.deinit();
    _ = try q2.next();

    const t = try db.openTable("t", .{});
    try t.flush();

    var q3 = try runSql(allocator, db, "SELECT ts FROM t");
    defer q3.deinit();
    const b = (try q3.next()).?;
    // 19737 days * 86_400 * 1_000_000 + 12*3600 + 30*60 + 45 sec * 1e6 + 123456 us
    const expected: i64 = @as(i64, 19737) * 86_400 * 1_000_000 + @as(i64, 12 * 3600 + 30 * 60 + 45) * 1_000_000 + 123_456;
    try std.testing.expectEqual(expected, b.values[0].data.datetime[0]);
}

test "sql roundtrip: DECIMAL column from string literal" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    var q1 = try runSql(allocator, db,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, amt DECIMAL(10,2) NOT NULL)",
    );
    defer q1.deinit();
    _ = try q1.next();
    var q2 = try runSql(allocator, db, "INSERT INTO t VALUES (1, '123.45')");
    defer q2.deinit();
    _ = try q2.next();

    const t = try db.openTable("t", .{});
    try t.flush();

    var q3 = try runSql(allocator, db, "SELECT amt FROM t");
    defer q3.deinit();
    const b = (try q3.next()).?;
    try std.testing.expectEqual(@as(i64, 12345), b.values[0].data.decimal64[0]);
}

test "sql roundtrip: FLOAT and DOUBLE columns" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    var q1 = try runSql(allocator, db,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, f FLOAT NOT NULL, d DOUBLE NOT NULL)",
    );
    defer q1.deinit();
    _ = try q1.next();
    var q2 = try runSql(allocator, db, "INSERT INTO t VALUES (1, 1.5, 2.25)");
    defer q2.deinit();
    _ = try q2.next();

    const t = try db.openTable("t", .{});
    try t.flush();

    var q3 = try runSql(allocator, db, "SELECT f, d FROM t");
    defer q3.deinit();
    const b = (try q3.next()).?;
    try std.testing.expectApproxEqAbs(@as(f32, 1.5), b.values[0].data.float[0], 1e-6);
    try std.testing.expectApproxEqAbs(@as(f64, 2.25), b.values[1].data.double[0], 1e-12);
}

test "sql roundtrip: BOOLEAN column with TRUE/FALSE literals" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    var q1 = try runSql(allocator, db,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, active BOOLEAN NOT NULL)",
    );
    defer q1.deinit();
    _ = try q1.next();
    var q2 = try runSql(allocator, db, "INSERT INTO t VALUES (1, TRUE), (2, FALSE)");
    defer q2.deinit();
    _ = try q2.next();

    const t = try db.openTable("t", .{});
    try t.flush();

    var q3 = try runSql(allocator, db, "SELECT id, active FROM t ORDER BY id ASC");
    defer q3.deinit();
    const b = (try q3.next()).?;
    try std.testing.expectEqual(@as(u8, 1), b.values[1].data.boolean[0]);
    try std.testing.expectEqual(@as(u8, 0), b.values[1].data.boolean[1]);
}
