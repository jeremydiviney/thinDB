//! CREATE TABLE / DROP TABLE / INSERT INTO over the SQL surface.
//! Drives the parser → IR → compile path that backs the MySQL and PG
//! wire listeners. Each test runs `compile` against an in-process
//! Database, then verifies via the engine API that the side effect
//! landed.

const std = @import("std");
const thindb = @import("thindb");
const helpers = @import("sql_helpers.zig");
const runSql = helpers.runSql;
const exec = helpers.exec;
const collectBigints = helpers.collectBigints;
const expectRunError = helpers.expectRunError;

test "sql ddl: CREATE TABLE with inline PRIMARY KEY" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    var q = try runSql(
        allocator,
        db,
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

    var q = try runSql(
        allocator,
        db,
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

test "sql ddl: parser recognizes rename, alter add column, and truncate" {
    const allocator = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();

    {
        const root = try thindb.sql.parse(arena.allocator(), "RENAME TABLE a TO b");
        try std.testing.expect(root.* == .ddl);
        try std.testing.expect(root.ddl == .rename_table);
        try std.testing.expectEqualStrings("a", root.ddl.rename_table.from.name);
        try std.testing.expectEqualStrings("b", root.ddl.rename_table.to.name);
    }
    {
        const root = try thindb.sql.parse(arena.allocator(), "ALTER TABLE t ADD COLUMN note TEXT");
        try std.testing.expect(root.* == .ddl);
        try std.testing.expect(root.ddl == .alter_table_add_column);
        try std.testing.expectEqualStrings("note", root.ddl.alter_table_add_column.column.name);
    }
    {
        const root = try thindb.sql.parse(arena.allocator(), "TRUNCATE TABLE t");
        try std.testing.expect(root.* == .ddl);
        try std.testing.expect(root.ddl == .truncate_table);
        try std.testing.expectEqualStrings("t", root.ddl.truncate_table.name);
    }
}

test "sql replace: parser recognizes values and select forms" {
    const allocator = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();

    {
        const root = try thindb.sql.parse(arena.allocator(), "REPLACE INTO t (id, v) VALUES (1, 'a')");
        try std.testing.expect(root.* == .insert);
        try std.testing.expectEqual(thindb.ir.InsertMode.replace, root.insert.mode);
        try std.testing.expectEqualStrings("t", root.insert.table.name);
        try std.testing.expectEqual(@as(usize, 2), root.insert.columns.?.len);
        try std.testing.expectEqual(@as(usize, 1), root.insert.rows.len);
    }
    {
        const root = try thindb.sql.parse(arena.allocator(), "REPLACE t (id, v) VALUES (1, 'a')");
        try std.testing.expect(root.* == .insert);
        try std.testing.expectEqual(thindb.ir.InsertMode.replace, root.insert.mode);
        try std.testing.expectEqualStrings("t", root.insert.table.name);
    }
    {
        const root = try thindb.sql.parse(arena.allocator(), "REPLACE INTO dst SELECT id, v FROM src");
        try std.testing.expect(root.* == .insert_select);
        try std.testing.expectEqual(thindb.ir.InsertMode.replace, root.insert_select.mode);
        try std.testing.expectEqualStrings("dst", root.insert_select.table.name);
        try std.testing.expect(root.insert_select.source.* == .select);
    }
    {
        const root = try thindb.sql.parse(arena.allocator(), "SELECT REPLACE('abc', 'a', 'z')");
        try std.testing.expect(root.* == .select);
    }
}

test "sql ddl: RENAME TABLE moves data to the new name" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    try exec(allocator, db, "CREATE TABLE src (id BIGINT PRIMARY KEY, qty INT NOT NULL)");
    try exec(allocator, db, "INSERT INTO src VALUES (1, 10), (2, 20)");
    try exec(allocator, db, "RENAME TABLE src TO dst");

    try expectRunError(allocator, db, "SELECT id FROM src", thindb.net.Error.TableNotFound);
    const ids = try collectBigints(allocator, db, "SELECT id FROM dst ORDER BY id ASC");
    defer allocator.free(ids);
    try std.testing.expectEqualSlices(i64, &[_]i64{ 1, 2 }, ids);
}

test "sql ddl: ALTER TABLE ADD COLUMN backfills nullable NULL and default values" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    try exec(allocator, db, "CREATE TABLE t (id BIGINT PRIMARY KEY, qty INT NOT NULL)");
    try exec(allocator, db, "INSERT INTO t VALUES (1, 10), (2, 20)");
    try exec(allocator, db, "ALTER TABLE t ADD COLUMN note TEXT");
    try exec(allocator, db, "ALTER TABLE t ADD COLUMN score INT NOT NULL DEFAULT 0");

    {
        var q = try runSql(allocator, db, "SELECT note, score FROM t ORDER BY id ASC");
        defer q.deinit();
        const batch = (try q.next()).?;
        try std.testing.expectEqual(@as(usize, 2), batch.row_count);
        try std.testing.expect(!batch.values[0].isValid(0));
        try std.testing.expect(!batch.values[0].isValid(1));
        try std.testing.expectEqual(@as(i32, 0), batch.values[1].data.int[0]);
        try std.testing.expectEqual(@as(i32, 0), batch.values[1].data.int[1]);
        try std.testing.expect((try q.next()) == null);
    }

    try exec(allocator, db, "INSERT INTO t (id, qty) VALUES (3, 30)");
    {
        var q = try runSql(allocator, db, "SELECT score FROM t WHERE id = 3");
        defer q.deinit();
        const batch = (try q.next()).?;
        try std.testing.expectEqual(@as(usize, 1), batch.row_count);
        try std.testing.expectEqual(@as(i32, 0), batch.values[0].data.int[0]);
    }
}

test "sql ddl: TRUNCATE TABLE clears persisted rows and preserves schema" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    {
        var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
        defer db.close();
        try exec(allocator, db, "CREATE TABLE t (id BIGINT PRIMARY KEY, qty INT NOT NULL)");
        try exec(allocator, db, "INSERT INTO t VALUES (1, 10), (2, 20)");
        const t = try db.openTable("t", .{});
        try t.flush();
        try exec(allocator, db, "TRUNCATE TABLE t");
    }

    {
        var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
        defer db.close();
        const ids = try collectBigints(allocator, db, "SELECT id FROM t ORDER BY id ASC");
        defer allocator.free(ids);
        try std.testing.expectEqual(@as(usize, 0), ids.len);
        try exec(allocator, db, "INSERT INTO t VALUES (3, 30)");
        const after = try collectBigints(allocator, db, "SELECT id FROM t ORDER BY id ASC");
        defer allocator.free(after);
        try std.testing.expectEqualSlices(i64, &[_]i64{3}, after);
    }
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

    var q1 = try runSql(
        allocator,
        db,
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

    var q1 = try runSql(
        allocator,
        db,
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

    var q1 = try runSql(
        allocator,
        db,
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

test "sql insert: negative numeric literals" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    var q1 = try runSql(allocator, db, "CREATE TABLE t (id BIGINT PRIMARY KEY, qty INT NOT NULL, score DOUBLE NOT NULL)");
    defer q1.deinit();
    _ = try q1.next();
    var q2 = try runSql(allocator, db, "INSERT INTO t VALUES (1, -482021160, -3.5)");
    defer q2.deinit();
    _ = try q2.next();

    const t = try db.openTable("t", .{});
    try t.flush();

    var q3 = try runSql(allocator, db, "SELECT qty, score FROM t");
    defer q3.deinit();
    const b = (try q3.next()).?;
    try std.testing.expectEqual(@as(i32, -482021160), b.values[0].data.int[0]);
    try std.testing.expectApproxEqAbs(@as(f64, -3.5), b.values[1].data.double[0], 1e-12);
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

test "sql ddl: DATETIME/TIMESTAMP fractional-seconds precision accepted 0-6, ignored, rejected above 6" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    var q1 = try runSql(allocator, db, "CREATE TABLE t (id BIGINT PRIMARY KEY, a DATETIME(6) NOT NULL, b TIMESTAMP(0), c TIMESTAMPTZ(3))");
    defer q1.deinit();
    _ = try q1.next();
    var q2 = try runSql(allocator, db, "INSERT INTO t VALUES (1, '2024-01-15 12:30:45.123456', '2024-01-15 12:30:45.123456', NULL)");
    defer q2.deinit();
    _ = try q2.next();

    const t = try db.openTable("t", .{});
    try t.flush();

    var q3 = try runSql(allocator, db, "SELECT a, b FROM t");
    defer q3.deinit();
    const batch = (try q3.next()).?;
    const expected: i64 = @as(i64, 19737) * 86_400 * 1_000_000 + @as(i64, 12 * 3600 + 30 * 60 + 45) * 1_000_000 + 123_456;
    try std.testing.expectEqual(expected, batch.values[0].data.datetime[0]);
    // Declared precision never rounds: TIMESTAMP(0) still keeps full micros.
    try std.testing.expectEqual(expected, batch.values[1].data.datetime[0]);

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    try std.testing.expectError(
        thindb.sql.ParseError.SqlExpectedValue,
        thindb.sql.parse(arena.allocator(), "CREATE TABLE bad (id BIGINT PRIMARY KEY, ts DATETIME(7))"),
    );
}

test "sql roundtrip: DECIMAL column from string literal" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    var q1 = try runSql(
        allocator,
        db,
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

    var q1 = try runSql(
        allocator,
        db,
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

    var q1 = try runSql(
        allocator,
        db,
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

test "sql replace: primary key conflict keeps the new row" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    try exec(allocator, db, "CREATE TABLE t (id BIGINT PRIMARY KEY, qty INT NOT NULL)");
    try exec(allocator, db, "INSERT INTO t VALUES (1, 10)");
    const t = try db.openTable("t", .{});
    try t.flush();

    var q = try runSql(allocator, db, "REPLACE INTO t VALUES (1, 99)");
    defer q.deinit();
    _ = try q.next();
    try std.testing.expectEqual(@as(u64, 1), q.affectedRows());

    var got = try runSql(allocator, db, "SELECT id, qty FROM t ORDER BY id ASC");
    defer got.deinit();
    const b = (try got.next()).?;
    try std.testing.expectEqual(@as(usize, 1), b.row_count);
    try std.testing.expectEqual(@as(i64, 1), b.values[0].data.bigint[0]);
    try std.testing.expectEqual(@as(i32, 99), b.values[1].data.int[0]);
    try std.testing.expect((try got.next()) == null);
}

test "sql replace: omitted columns are fresh defaults or NULL" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    try exec(allocator, db, "CREATE TABLE t (id BIGINT PRIMARY KEY, qty INT NOT NULL, note TEXT, score INT NOT NULL DEFAULT 0)");
    try exec(allocator, db, "INSERT INTO t VALUES (1, 10, 'old', 5)");
    const t = try db.openTable("t", .{});
    try t.flush();

    try exec(allocator, db, "REPLACE INTO t (id, qty) VALUES (1, 99)");

    var q = try runSql(allocator, db, "SELECT qty, note, score FROM t ORDER BY id ASC");
    defer q.deinit();
    const b = (try q.next()).?;
    try std.testing.expectEqual(@as(usize, 1), b.row_count);
    try std.testing.expectEqual(@as(i32, 99), b.values[0].data.int[0]);
    try std.testing.expect(!b.values[1].isValid(0));
    try std.testing.expectEqual(@as(i32, 0), b.values[2].data.int[0]);
    try std.testing.expect((try q.next()) == null);
}

test "sql replace: duplicate values keep the last row" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    try exec(allocator, db, "CREATE TABLE t (id BIGINT PRIMARY KEY, qty INT NOT NULL)");
    try exec(allocator, db, "REPLACE INTO t VALUES (1, 10), (1, 20), (2, 30)");

    var q = try runSql(allocator, db, "SELECT id, qty FROM t ORDER BY id ASC");
    defer q.deinit();
    const b = (try q.next()).?;
    try std.testing.expectEqual(@as(usize, 2), b.row_count);
    try std.testing.expectEqual(@as(i64, 1), b.values[0].data.bigint[0]);
    try std.testing.expectEqual(@as(i32, 20), b.values[1].data.int[0]);
    try std.testing.expectEqual(@as(i64, 2), b.values[0].data.bigint[1]);
    try std.testing.expectEqual(@as(i32, 30), b.values[1].data.int[1]);
}

test "sql replace: insert select replaces conflicting target rows" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    try exec(allocator, db, "CREATE TABLE src (id BIGINT PRIMARY KEY, qty INT NOT NULL)");
    try exec(allocator, db, "CREATE TABLE dst (id BIGINT PRIMARY KEY, qty INT NOT NULL)");
    try exec(allocator, db, "INSERT INTO src VALUES (1, 100), (2, 200)");
    try exec(allocator, db, "INSERT INTO dst VALUES (1, 10)");
    const dst = try db.openTable("dst", .{});
    try dst.flush();

    var repl = try runSql(allocator, db, "REPLACE INTO dst SELECT id, qty FROM src");
    defer repl.deinit();
    _ = try repl.next();
    try std.testing.expectEqual(@as(u64, 2), repl.affectedRows());

    var q = try runSql(allocator, db, "SELECT id, qty FROM dst ORDER BY id ASC");
    defer q.deinit();
    const b = (try q.next()).?;
    try std.testing.expectEqual(@as(usize, 2), b.row_count);
    try std.testing.expectEqual(@as(i64, 1), b.values[0].data.bigint[0]);
    try std.testing.expectEqual(@as(i32, 100), b.values[1].data.int[0]);
    try std.testing.expectEqual(@as(i64, 2), b.values[0].data.bigint[1]);
    try std.testing.expectEqual(@as(i32, 200), b.values[1].data.int[1]);
}

test "sql replace: non-unique table appends rows" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    try exec(allocator, db, "CREATE TABLE src (id BIGINT PRIMARY KEY, qty INT NOT NULL)");
    try exec(allocator, db, "INSERT INTO src VALUES (1, 10)");
    try exec(allocator, db, "CREATE TABLE dst AS SELECT id, qty FROM src");
    try exec(allocator, db, "REPLACE dst VALUES (1, 99)");

    var q = try runSql(allocator, db, "SELECT qty FROM dst ORDER BY qty ASC");
    defer q.deinit();
    const b = (try q.next()).?;
    try std.testing.expectEqual(@as(usize, 2), b.row_count);
    try std.testing.expectEqual(@as(i32, 10), b.values[0].data.int[0]);
    try std.testing.expectEqual(@as(i32, 99), b.values[0].data.int[1]);
}

test "sql ddl: CREATE TABLE PROPERTIES sets table compression" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    try exec(allocator, db, "CREATE TABLE z (id BIGINT PRIMARY KEY) PROPERTIES (\"compression\" = \"ZSTD\")");
    try exec(allocator, db, "CREATE TABLE n (id BIGINT PRIMARY KEY) PROPERTIES ('compression' = 'none')");
    try exec(allocator, db, "CREATE TABLE d (id BIGINT PRIMARY KEY)");

    const tz = try db.openTable("z", .{});
    try std.testing.expectEqual(thindb.types.TableCompression.zstd, tz.schema.compression);
    const tn = try db.openTable("n", .{});
    try std.testing.expectEqual(thindb.types.TableCompression.none, tn.schema.compression);
    const td = try db.openTable("d", .{});
    try std.testing.expectEqual(thindb.types.default_table_compression, td.schema.compression);

    var bad = runSql(allocator, db, "CREATE TABLE x (id BIGINT PRIMARY KEY) PROPERTIES ('compression' = 'brotli')");
    if (bad) |*q| {
        q.deinit();
        return error.TestExpectedError;
    } else |_| {}
}

test "sql ddl: StarRocks CREATE TABLE — DISTRIBUTED BY HASH key + BUCKETS + replication_num" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    // The exact shape StarRocks clients emit (wayroll plan-selection temp
    // table). Keyless column list: the hash columns become a NON-unique
    // order key — duplicates must be kept (SR duplicate-key semantics).
    try exec(allocator, db,
        \\CREATE TABLE ep (externalPlanId VARCHAR(255))
        \\DISTRIBUTED BY HASH(externalPlanId) BUCKETS 1
        \\PROPERTIES ("replication_num" = "1")
    );
    try exec(allocator, db, "INSERT INTO ep VALUES ('a'), ('b'), ('a')");

    var q = try runSql(allocator, db, "SELECT COUNT(*) AS c FROM ep");
    defer q.deinit();
    const b = (try q.next()).?;
    try std.testing.expectEqual(@as(i64, 3), b.values[0].data.bigint[0]);

    // A real key clause wins over DISTRIBUTED BY: still a unique PK table.
    try exec(allocator, db,
        \\CREATE TABLE ep2 (id BIGINT, PRIMARY KEY(id))
        \\DISTRIBUTED BY HASH(id) BUCKETS 4
    );
    try exec(allocator, db, "INSERT INTO ep2 VALUES (1), (1)");
    var q2 = try runSql(allocator, db, "SELECT COUNT(*) AS c FROM ep2");
    defer q2.deinit();
    const b2 = (try q2.next()).?;
    try std.testing.expectEqual(@as(i64, 1), b2.values[0].data.bigint[0]);
}

test "sql ddl: global block cache — two tables share it; TRUNCATE never serves stale blocks" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    try exec(allocator, db, "CREATE TABLE ca (id BIGINT PRIMARY KEY, v BIGINT NOT NULL)");
    try exec(allocator, db, "CREATE TABLE cb (id BIGINT PRIMARY KEY, v BIGINT NOT NULL)");
    try exec(allocator, db, "INSERT INTO ca VALUES (1, 10), (2, 20), (3, 30)");
    try exec(allocator, db, "INSERT INTO cb VALUES (1, 7), (2, 14)");
    const ta = try db.openTable("ca", .{});
    const tb = try db.openTable("cb", .{});
    try ta.flush();
    try tb.flush();

    // Real segment scans on both tables must land blocks in the SAME cache
    // instance, each keyed under its own table uid.
    const va = try collectBigints(allocator, db, "SELECT v FROM ca ORDER BY id");
    defer allocator.free(va);
    try std.testing.expectEqualSlices(i64, &[_]i64{ 10, 20, 30 }, va);
    const vb = try collectBigints(allocator, db, "SELECT v FROM cb ORDER BY id");
    defer allocator.free(vb);
    try std.testing.expectEqualSlices(i64, &[_]i64{ 7, 14 }, vb);

    try std.testing.expectEqual(ta.cache, tb.cache);
    var blocks_a: usize = 0;
    var blocks_b: usize = 0;
    var it = ta.cache.map.keyIterator();
    while (it.next()) |k| {
        if (k.table_uid == ta.cache_uid) blocks_a += 1;
        if (k.table_uid == tb.cache_uid) blocks_b += 1;
    }
    try std.testing.expect(blocks_a > 0);
    try std.testing.expect(blocks_b > 0);

    // TRUNCATE restarts the table's segment IDs at the same coordinates the
    // cached blocks used. A re-read must serve the NEW generation's bytes —
    // the uid bump makes the old entries unreachable.
    try exec(allocator, db, "TRUNCATE TABLE ca");
    try exec(allocator, db, "INSERT INTO ca VALUES (1, 111), (2, 222), (3, 333)");
    try ta.flush();
    const va2 = try collectBigints(allocator, db, "SELECT v FROM ca ORDER BY id");
    defer allocator.free(va2);
    try std.testing.expectEqualSlices(i64, &[_]i64{ 111, 222, 333 }, va2);

    // The sibling table's cached blocks were untouched by the truncate.
    const vb2 = try collectBigints(allocator, db, "SELECT v FROM cb ORDER BY id");
    defer allocator.free(vb2);
    try std.testing.expectEqualSlices(i64, &[_]i64{ 7, 14 }, vb2);
}
