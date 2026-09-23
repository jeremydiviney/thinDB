//! CREATE TABLE name AS SELECT ... (CTAS) and INSERT INTO target [(cols)]
//! SELECT ... — schema-inferred table creation and query-driven inserts.

const std = @import("std");
const thindb = @import("thindb");
const helpers = @import("sql_helpers.zig");
const exec = helpers.exec;
const collectBigints = helpers.collectBigints;

test "CTAS: basic — copies source rows into a new table" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    try exec(allocator, db, "CREATE TABLE src (id BIGINT PRIMARY KEY, qty INT NOT NULL)");
    try exec(allocator, db, "INSERT INTO src (id, qty) VALUES (1, 10), (2, 20), (3, 30)");
    const src = try db.openTable("src", .{});
    try src.flush();

    try exec(allocator, db, "CREATE TABLE dst AS SELECT id, qty FROM src WHERE qty >= 20");
    const dst = try db.openTable("dst", .{});
    try dst.flush();

    const ids = try collectBigints(allocator, db, "SELECT id FROM dst ORDER BY id ASC");
    defer allocator.free(ids);
    try std.testing.expectEqualSlices(i64, &.{ 2, 3 }, ids);
}

test "CTAS and INSERT SELECT preserve aliases and NULL projection schemas" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    try exec(allocator, db, "CREATE TABLE src (id BIGINT PRIMARY KEY, qty INT NOT NULL)");
    try exec(allocator, db, "INSERT INTO src (id, qty) VALUES (1, 10), (2, 20)");
    const src = try db.openTable("src", .{});
    try src.flush();

    try exec(allocator, db, "CREATE TABLE dst AS SELECT id, qty AS amount, NULL AS note FROM src");
    const dst = try db.openTable("dst", .{});
    try std.testing.expectEqualStrings("amount", dst.schema.columns[1].name);
    try std.testing.expectEqual(thindb.Type{ .int = {} }, dst.schema.columns[1].type);
    try std.testing.expectEqualStrings("note", dst.schema.columns[2].name);
    try std.testing.expectEqual(thindb.Type{ .string = {} }, dst.schema.columns[2].type);
    try std.testing.expect(dst.schema.columns[2].nullable);

    try exec(allocator, db, "CREATE TABLE sink (id BIGINT PRIMARY KEY, amount INT NOT NULL, note STRING NULL)");
    try exec(allocator, db, "INSERT INTO sink SELECT id, amount, note FROM dst");
    const sink = try db.openTable("sink", .{});
    try sink.flush();

    const ids = try collectBigints(allocator, db, "SELECT id FROM sink ORDER BY id ASC");
    defer allocator.free(ids);
    try std.testing.expectEqualSlices(i64, &.{ 1, 2 }, ids);
}

test "CTAS: computed projection can replace a star-expanded source column" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    try exec(allocator, db, "CREATE TABLE src (id BIGINT PRIMARY KEY, qty INT NOT NULL, tag STRING NOT NULL)");
    try exec(allocator, db, "INSERT INTO src (id, qty, tag) VALUES (1, 10, 'a'), (2, 20, 'b')");
    const src = try db.openTable("src", .{});
    try src.flush();

    try exec(allocator, db, "CREATE TABLE dst AS SELECT *, qty + 1 AS qty FROM src");
    const dst = try db.openTable("dst", .{});
    try std.testing.expectEqual(@as(usize, 3), dst.schema.columns.len);
    try std.testing.expectEqualStrings("id", dst.schema.columns[0].name);
    try std.testing.expectEqualStrings("qty", dst.schema.columns[1].name);
    try std.testing.expectEqualStrings("tag", dst.schema.columns[2].name);
    try dst.flush();

    var q = try helpers.runSql(allocator, db, "SELECT id, qty, tag FROM dst ORDER BY id ASC");
    defer q.deinit();
    const b = (try q.next()).?;
    try std.testing.expectEqual(@as(i64, 1), b.values[0].data.bigint[0]);
    try std.testing.expectEqual(@as(i32, 11), b.values[1].data.int[0]);
    try std.testing.expectEqualStrings("a", b.values[2].data.string.rowBytes(0));
    try std.testing.expectEqual(@as(i64, 2), b.values[0].data.bigint[1]);
    try std.testing.expectEqual(@as(i32, 21), b.values[1].data.int[1]);
    try std.testing.expectEqualStrings("b", b.values[2].data.string.rowBytes(1));
}

test "CTAS: rejects when target exists (unless IF NOT EXISTS)" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    try exec(allocator, db, "CREATE TABLE src (id BIGINT PRIMARY KEY)");
    try exec(allocator, db, "INSERT INTO src (id) VALUES (1)");
    const src = try db.openTable("src", .{});
    try src.flush();
    try exec(allocator, db, "CREATE TABLE dst (id BIGINT PRIMARY KEY)");

    // Existing dst — CTAS should error.
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const root = try thindb.sql.parse(arena.allocator(), "CREATE TABLE dst AS SELECT id FROM src");
    const cq = thindb.net.compile(allocator, db, root);
    if (cq) |ok| {
        var c = ok;
        c.deinit();
        return error.TestUnexpectedSuccess;
    } else |err| {
        try std.testing.expectEqual(thindb.net.Error.TableAlreadyExists, err);
    }

    // IF NOT EXISTS — silent no-op.
    try exec(allocator, db, "CREATE TABLE IF NOT EXISTS dst AS SELECT id FROM src");
}

test "INSERT SELECT: full positional copy" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    try exec(allocator, db, "CREATE TABLE src (id BIGINT PRIMARY KEY, qty INT NOT NULL)");
    try exec(allocator, db, "CREATE TABLE dst (id BIGINT PRIMARY KEY, qty INT NOT NULL)");
    try exec(allocator, db, "INSERT INTO src (id, qty) VALUES (1, 10), (2, 20)");
    const src = try db.openTable("src", .{});
    try src.flush();

    try exec(allocator, db, "INSERT INTO dst SELECT id, qty FROM src");
    const dst = try db.openTable("dst", .{});
    try dst.flush();

    const ids = try collectBigints(allocator, db, "SELECT id FROM dst ORDER BY id ASC");
    defer allocator.free(ids);
    try std.testing.expectEqualSlices(i64, &.{ 1, 2 }, ids);
}

test "INSERT SELECT: named column list reorders source columns" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    try exec(allocator, db, "CREATE TABLE src (a BIGINT PRIMARY KEY, b INT NOT NULL)");
    try exec(allocator, db, "CREATE TABLE dst (id BIGINT PRIMARY KEY, qty INT NOT NULL)");
    try exec(allocator, db, "INSERT INTO src (a, b) VALUES (5, 50)");
    const src = try db.openTable("src", .{});
    try src.flush();

    // dst.(id, qty) ← src.(a, b)
    try exec(allocator, db, "INSERT INTO dst (id, qty) SELECT a, b FROM src");
    const dst = try db.openTable("dst", .{});
    try dst.flush();

    const ids = try collectBigints(allocator, db, "SELECT id FROM dst");
    defer allocator.free(ids);
    try std.testing.expectEqualSlices(i64, &.{5}, ids);
}

test "INSERT SELECT: width mismatch rejected" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    try exec(allocator, db, "CREATE TABLE src (id BIGINT PRIMARY KEY)");
    try exec(allocator, db, "CREATE TABLE dst (id BIGINT PRIMARY KEY, qty INT NOT NULL)");
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const root = try thindb.sql.parse(arena.allocator(), "INSERT INTO dst SELECT id FROM src");
    const cq = thindb.net.compile(allocator, db, root);
    if (cq) |ok| {
        var c = ok;
        c.deinit();
        return error.TestUnexpectedSuccess;
    } else |err| {
        try std.testing.expectEqual(thindb.net.Error.BadRequest, err);
    }
}

test "INSERT SELECT: bare NULL literal lands in non-string nullable columns" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    try exec(
        allocator,
        db,
        "CREATE TABLE sink (id BIGINT PRIMARY KEY, amt DECIMAL(10,2), ts DATETIME, d DATE, " ++
            "f DOUBLE, k INT, s VARCHAR(8), n INT NOT NULL)",
    );
    try exec(allocator, db, "CREATE TABLE src (id BIGINT PRIMARY KEY, n INT NOT NULL)");
    try exec(allocator, db, "INSERT INTO src VALUES (1, 10), (2, 20), (3, 30)");
    const src = try db.openTable("src", .{});
    try src.flush();

    try exec(
        allocator,
        db,
        "INSERT INTO sink (id, n, amt, ts, d, f, k, s) " ++
            "SELECT id, n, NULL AS a, NULL AS b, NULL AS c, NULL AS e, NULL AS g, NULL AS h FROM src",
    );
    try exec(
        allocator,
        db,
        "INSERT INTO sink (id, amt, ts, d, f, k, s, n) " ++
            "SELECT id + 100, NULL AS a, NULL AS b, NULL AS c, NULL AS e, NULL AS g, NULL AS h, n FROM src WHERE id = 1",
    );
    try exec(
        allocator,
        db,
        "INSERT INTO sink (id, n, k, amt, ts, d, f, s) " ++
            "SELECT id + 10, n, CASE WHEN id = 1 THEN NULL ELSE n END, NULL AS a, NULL AS b, NULL AS c, NULL AS e, NULL AS h FROM src",
    );

    const nulled = try collectBigints(
        allocator,
        db,
        "SELECT id FROM sink WHERE amt IS NULL AND ts IS NULL AND d IS NULL AND f IS NULL " ++
            "AND k IS NULL AND s IS NULL ORDER BY id ASC",
    );
    defer allocator.free(nulled);
    try std.testing.expectEqualSlices(i64, &.{ 1, 2, 3, 11, 101 }, nulled);

    const ns = try collectBigints(allocator, db, "SELECT CAST(n AS BIGINT) FROM sink ORDER BY id ASC");
    defer allocator.free(ns);
    try std.testing.expectEqualSlices(i64, &.{ 10, 20, 30, 10, 20, 30, 10 }, ns);

    const ks = try collectBigints(allocator, db, "SELECT CAST(k AS BIGINT) FROM sink WHERE k IS NOT NULL ORDER BY id ASC");
    defer allocator.free(ks);
    try std.testing.expectEqualSlices(i64, &.{ 20, 30 }, ks);

    // NULL into a NOT NULL target is still a type error, not a silent placeholder.
    try helpers.expectRunError(
        allocator,
        db,
        "INSERT INTO sink (id, n, amt, ts, d, f, k, s) " ++
            "SELECT id + 200, NULL AS z, NULL AS a, NULL AS b, NULL AS c, NULL AS e, NULL AS g, NULL AS h FROM src",
        error.TypeMismatch,
    );
    const count = try collectBigints(allocator, db, "SELECT COUNT(*) FROM sink");
    defer allocator.free(count);
    try std.testing.expectEqualSlices(i64, &.{7}, count);
}

test "INSERT SELECT: repeated unaliased items insert positionally" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    try exec(allocator, db, "CREATE TABLE sink (id BIGINT PRIMARY KEY, n INT NOT NULL, a INT, b INT, c INT)");
    try exec(allocator, db, "CREATE TABLE src (id BIGINT PRIMARY KEY, n INT NOT NULL)");
    try exec(allocator, db, "INSERT INTO src VALUES (1, 10), (2, 20)");
    const src = try db.openTable("src", .{});
    try src.flush();

    try exec(allocator, db, "INSERT INTO sink SELECT id, n, NULL, NULL, n FROM src");
    try exec(allocator, db, "INSERT INTO sink SELECT id + 10, n, n, NULL, n FROM src");

    const nulls = try collectBigints(allocator, db, "SELECT id FROM sink WHERE a IS NULL AND b IS NULL ORDER BY id ASC");
    defer allocator.free(nulls);
    try std.testing.expectEqualSlices(i64, &.{ 1, 2 }, nulls);
    const cs = try collectBigints(allocator, db, "SELECT CAST(c AS BIGINT) + CAST(COALESCE(a, 0) AS BIGINT) FROM sink ORDER BY id ASC");
    defer allocator.free(cs);
    try std.testing.expectEqualSlices(i64, &.{ 10, 20, 20, 40 }, cs);
}

test "INSERT SELECT: literals and narrower columns widen into the target types" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    try exec(allocator, db, "CREATE TABLE sink2 (id BIGINT NOT NULL, amt DECIMAL(10,2), n INT NOT NULL, PRIMARY KEY (id))");
    try exec(allocator, db, "INSERT INTO sink2 (id, amt, n) SELECT 4, 1.5, 40");
    try exec(allocator, db, "INSERT INTO sink2 (id, n) SELECT 5, 50");

    // A DECIMAL(12,4) payload must be rescaled, not stored as if it were
    // already at scale 2.
    try exec(allocator, db, "CREATE TABLE src2 (k INT NOT NULL, price DECIMAL(12,4) NOT NULL, small SMALLINT NOT NULL, m INT, PRIMARY KEY (k))");
    try exec(allocator, db, "INSERT INTO src2 VALUES (1, 1.25, 11, 101), (2, 3.1, 22, NULL)");
    try exec(allocator, db, "INSERT INTO sink2 SELECT k, price, small FROM src2");
    // A nullable source lands in a NOT NULL column when its rows hold no NULL.
    try exec(allocator, db, "INSERT INTO sink2 (id, n) SELECT k + 100, m FROM src2 WHERE k = 1");
    const t = try db.openTable("sink2", .{});
    try t.flush();

    const ids = try collectBigints(allocator, db, "SELECT id FROM sink2 ORDER BY id");
    defer allocator.free(ids);
    try std.testing.expectEqualSlices(i64, &.{ 1, 2, 4, 5, 101 }, ids);
    const cents = try collectBigints(allocator, db, "SELECT CAST(amt * 100 AS BIGINT) FROM sink2 WHERE amt IS NOT NULL ORDER BY id");
    defer allocator.free(cents);
    try std.testing.expectEqualSlices(i64, &.{ 125, 310, 150 }, cents);
    const ns = try collectBigints(allocator, db, "SELECT CAST(n AS BIGINT) FROM sink2 ORDER BY id");
    defer allocator.free(ns);
    try std.testing.expectEqualSlices(i64, &.{ 11, 22, 40, 50, 101 }, ns);
    const null_amt = try collectBigints(allocator, db, "SELECT id FROM sink2 WHERE amt IS NULL ORDER BY id");
    defer allocator.free(null_amt);
    try std.testing.expectEqualSlices(i64, &.{ 5, 101 }, null_amt);

    // Narrowing and a NULL into a NOT NULL column are still rejected.
    try helpers.expectRunError(allocator, db, "INSERT INTO sink2 (id, n) SELECT 6, CAST(5000000000 AS BIGINT)", error.TypeMismatch);
    try helpers.expectRunError(allocator, db, "INSERT INTO sink2 (id, n) SELECT 7, 2.5", error.TypeMismatch);
    try helpers.expectRunError(allocator, db, "INSERT INTO sink2 (id, n) SELECT k + 200, m FROM src2", error.TypeMismatch);
}

test "INSERT SELECT: a date widens into a DATETIME column" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    try exec(allocator, db, "CREATE TABLE src (id BIGINT PRIMARY KEY, d DATE NOT NULL)");
    try exec(allocator, db, "INSERT INTO src VALUES (1, '2024-03-15')");
    try exec(allocator, db, "CREATE TABLE sink (id BIGINT PRIMARY KEY, ts DATETIME NOT NULL)");
    try exec(allocator, db, "INSERT INTO sink SELECT id, d FROM src");

    const hits = try collectBigints(allocator, db, "SELECT id FROM sink WHERE ts = '2024-03-15 00:00:00'");
    defer allocator.free(hits);
    try std.testing.expectEqualSlices(i64, &.{1}, hits);
}

test "INSERT SELECT: text parses into DATE and DATETIME columns" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    try exec(allocator, db, "CREATE TABLE src (id BIGINT PRIMARY KEY, s VARCHAR(32), t STRING NOT NULL)");
    try exec(allocator, db, "INSERT INTO src VALUES (1, '2024-03-15', '2024-03-15 10:20:30'), (2, NULL, '2024-03-16')");
    try exec(allocator, db, "CREATE TABLE sink (id BIGINT PRIMARY KEY, d DATE, ts DATETIME NOT NULL)");
    try exec(allocator, db, "INSERT INTO sink SELECT id, s, t FROM src");
    try exec(allocator, db, "INSERT INTO sink (id, d, ts) SELECT 3, '2024-03-17', '2024-03-17 01:02:03'");

    const cases = .{
        .{ .sql = "SELECT id FROM sink WHERE d = DATE '2024-03-15'", .ids = &[_]i64{1} },
        .{ .sql = "SELECT id FROM sink WHERE d IS NULL", .ids = &[_]i64{2} },
        .{ .sql = "SELECT id FROM sink WHERE d = DATE '2024-03-17'", .ids = &[_]i64{3} },
        .{ .sql = "SELECT id FROM sink WHERE ts = DATETIME '2024-03-15 10:20:30'", .ids = &[_]i64{1} },
        .{ .sql = "SELECT id FROM sink WHERE ts = DATETIME '2024-03-16 00:00:00'", .ids = &[_]i64{2} },
        .{ .sql = "SELECT id FROM sink WHERE ts = DATETIME '2024-03-17 01:02:03'", .ids = &[_]i64{3} },
    };
    inline for (cases) |c| {
        const ids = try collectBigints(allocator, db, c.sql);
        defer allocator.free(ids);
        try std.testing.expectEqualSlices(i64, c.ids, ids);
    }

    // Text that isn't a date is rejected as INSERT ... VALUES rejects it,
    // not stored as NULL, even in a nullable column.
    try helpers.expectRunError(allocator, db, "INSERT INTO sink (id, d, ts) SELECT 4, 'soon', '2024-01-01'", error.TypeMismatch);
    try helpers.expectRunError(allocator, db, "INSERT INTO sink (id, d, ts) SELECT 5, '2024-01-01', 'later'", error.TypeMismatch);
    const count = try collectBigints(allocator, db, "SELECT COUNT(*) FROM sink");
    defer allocator.free(count);
    try std.testing.expectEqualSlices(i64, &.{3}, count);
}

test "INSERT SELECT: omitted columns take their DEFAULT, the clock, or NULL" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    try exec(allocator, db, "CREATE TABLE src (id BIGINT PRIMARY KEY)");
    try exec(allocator, db, "INSERT INTO src VALUES (1), (2)");
    try exec(
        allocator,
        db,
        "CREATE TABLE sink (id BIGINT PRIMARY KEY, qty INT NOT NULL DEFAULT 7, price DECIMAL(8,3) DEFAULT 1.25, " ++
            "ts DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP, note VARCHAR(8))",
    );
    try exec(allocator, db, "INSERT INTO sink (id) SELECT id FROM src");

    const qtys = try collectBigints(allocator, db, "SELECT CAST(qty AS BIGINT) FROM sink ORDER BY id");
    defer allocator.free(qtys);
    try std.testing.expectEqualSlices(i64, &.{ 7, 7 }, qtys);
    const mils = try collectBigints(allocator, db, "SELECT CAST(price * 1000 AS BIGINT) FROM sink ORDER BY id");
    defer allocator.free(mils);
    try std.testing.expectEqualSlices(i64, &.{ 1250, 1250 }, mils);
    const filled = try collectBigints(allocator, db, "SELECT id FROM sink WHERE ts > '2020-01-01 00:00:00' AND note IS NULL ORDER BY id");
    defer allocator.free(filled);
    try std.testing.expectEqualSlices(i64, &.{ 1, 2 }, filled);

    try exec(allocator, db, "CREATE TABLE strict_sink (id BIGINT PRIMARY KEY, must INT NOT NULL)");
    try helpers.expectRunError(allocator, db, "INSERT INTO strict_sink (id) SELECT id FROM src", error.ColumnNotFound);
}
