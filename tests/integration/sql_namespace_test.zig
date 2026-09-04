//! SQL namespace surface: backtick-quoted identifiers, 1-/2-/3-part
//! table refs against the Session, and the DDL + SHOW statements that
//! operate on the Catalog → Database → Schema stack.

const std = @import("std");
const thindb = @import("thindb");
const helpers = @import("sql_helpers.zig");
const RunResult = helpers.RunResult;
const runSql = helpers.runSql;

const schema_t = thindb.TableSchema{
    .columns = &.{
        .{ .name = "id", .type = .bigint },
        .{ .name = "qty", .type = .int },
    },
    .order_key = &.{"id"},
    .unique = true,
};
const ok_t = [_][]const u8{"id"};
const opts_t = thindb.TableOptions{ .order_key = &ok_t, .unique = true, .row_group_size = 8 };

fn seed(db: anytype, name: []const u8) !*thindb.Table {
    const t = try db.table(name, schema_t, opts_t);
    try t.insert(&.{
        .{ .id = @as(i64, 1), .qty = @as(i32, 10) },
        .{ .id = @as(i64, 2), .qty = @as(i32, 20) },
        .{ .id = @as(i64, 3), .qty = @as(i32, 30) },
    });
    try t.flush();
    return t;
}

fn runSqlSession(
    allocator: std.mem.Allocator,
    db: anytype,
    session: thindb.api.Session,
    sql: []const u8,
) !RunResult {
    var arena = std.heap.ArenaAllocator.init(allocator);
    errdefer arena.deinit();
    const root = try thindb.sql.parse(arena.allocator(), sql);
    const cq = try thindb.net.compileWithSession(allocator, db, session, root);
    return .{
        .arena = arena,
        .cq = cq,
        .owned_vars = cq.sessionValue().vars,
        .backing_allocator = allocator,
    };
}

fn collectIds(allocator: std.mem.Allocator, q: *RunResult) ![]i64 {
    var ids: std.ArrayList(i64) = .empty;
    errdefer ids.deinit(allocator);
    while (try q.next()) |b| {
        for (b.values[0].data.bigint[0..b.row_count]) |v| try ids.append(allocator, v);
    }
    return try ids.toOwnedSlice(allocator);
}

fn collectStrings(allocator: std.mem.Allocator, q: *RunResult) !std.ArrayList([]const u8) {
    var out: std.ArrayList([]const u8) = .empty;
    errdefer {
        for (out.items) |s| allocator.free(s);
        out.deinit(allocator);
    }
    while (try q.next()) |b| {
        const sv = b.values[0].data.string;
        for (0..b.row_count) |i| {
            const copy = try allocator.dupe(u8, sv.rowBytes(i));
            try out.append(allocator, copy);
        }
    }
    return out;
}

fn freeStrings(allocator: std.mem.Allocator, ss: *std.ArrayList([]const u8)) void {
    for (ss.items) |s| allocator.free(s);
    ss.deinit(allocator);
}

fn containsString(ss: std.ArrayList([]const u8), needle: []const u8) bool {
    for (ss.items) |s| if (std.mem.eql(u8, s, needle)) return true;
    return false;
}

test "sql namespace: backtick-quoted identifier survives lex + resolves" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    _ = try seed(db, "t");

    var q = try runSql(allocator, db, "SELECT id FROM `t` ORDER BY id ASC");
    defer q.deinit();
    const ids = try collectIds(allocator, &q);
    defer allocator.free(ids);
    try std.testing.expectEqualSlices(i64, &[_]i64{ 1, 2, 3 }, ids);
}

test "sql namespace: 3-part ref SELECT * FROM main.public.t" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    _ = try seed(db, "t");

    var q = try runSql(allocator, db, "SELECT id FROM main.public.t ORDER BY id ASC");
    defer q.deinit();
    const ids = try collectIds(allocator, &q);
    defer allocator.free(ids);
    try std.testing.expectEqualSlices(i64, &[_]i64{ 1, 2, 3 }, ids);
}

test "sql namespace: 2-part ref SELECT * FROM public.t" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    _ = try seed(db, "t");

    var q = try runSql(allocator, db, "SELECT id FROM public.t ORDER BY id ASC");
    defer q.deinit();
    const ids = try collectIds(allocator, &q);
    defer allocator.free(ids);
    try std.testing.expectEqualSlices(i64, &[_]i64{ 1, 2, 3 }, ids);
}

test "sql namespace: session-default 1-part ref still works" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    _ = try seed(db, "t");

    var q = try runSql(allocator, db, "SELECT id FROM t ORDER BY id ASC");
    defer q.deinit();
    const ids = try collectIds(allocator, &q);
    defer allocator.free(ids);
    try std.testing.expectEqualSlices(i64, &[_]i64{ 1, 2, 3 }, ids);
}

test "sql namespace: CREATE DATABASE + DROP DATABASE round-trips" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    var q1 = try runSql(allocator, db, "CREATE DATABASE analytics");
    defer q1.deinit();
    try std.testing.expect((try q1.next()) == null);

    var q2 = try runSql(allocator, db, "SHOW DATABASES");
    defer q2.deinit();
    var names = try collectStrings(allocator, &q2);
    defer freeStrings(allocator, &names);
    try std.testing.expect(containsString(names, "main"));
    try std.testing.expect(containsString(names, "analytics"));

    var q3 = try runSql(allocator, db, "DROP DATABASE analytics");
    defer q3.deinit();
    try std.testing.expect((try q3.next()) == null);
}

test "sql namespace: CREATE SCHEMA + DROP SCHEMA in current database" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    var q1 = try runSql(allocator, db, "CREATE SCHEMA analytics");
    defer q1.deinit();
    _ = try q1.next();

    var q2 = try runSql(allocator, db, "SHOW SCHEMAS");
    defer q2.deinit();
    var names = try collectStrings(allocator, &q2);
    defer freeStrings(allocator, &names);
    try std.testing.expect(containsString(names, "public"));
    try std.testing.expect(containsString(names, "analytics"));

    var q3 = try runSql(allocator, db, "DROP SCHEMA analytics");
    defer q3.deinit();
    _ = try q3.next();
}

test "sql namespace: USE schema shifts subsequent unqualified queries" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    _ = try seed(db, "t");
    const analytics = try db.createSchema("analytics");
    const ta = try analytics.table("t", schema_t, opts_t);
    try ta.insert(&.{
        .{ .id = @as(i64, 100), .qty = @as(i32, 1) },
        .{ .id = @as(i64, 200), .qty = @as(i32, 2) },
    });
    try ta.flush();

    var use_q = try runSql(allocator, db, "USE analytics");
    defer use_q.deinit();
    const post_session = use_q.cq.sessionValue();
    try std.testing.expectEqualStrings("analytics", post_session.current_schema);

    var q = try runSqlSession(allocator, db, post_session, "SELECT id FROM t ORDER BY id ASC");
    defer q.deinit();
    const ids = try collectIds(allocator, &q);
    defer allocator.free(ids);
    try std.testing.expectEqualSlices(i64, &[_]i64{ 100, 200 }, ids);
}

test "sql namespace: USE db.schema shifts both" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    const cat = db.owned_catalog.?;
    const warehouse = try cat.createDatabase("warehouse");
    const reports = try warehouse.createSchema("reports");
    const tr = try reports.table("t", schema_t, opts_t);
    try tr.insert(&.{.{ .id = @as(i64, 42), .qty = @as(i32, 7) }});
    try tr.flush();

    var use_q = try runSql(allocator, db, "USE warehouse.reports");
    defer use_q.deinit();
    const post_session = use_q.cq.sessionValue();
    try std.testing.expectEqualStrings("warehouse", post_session.current_db);
    try std.testing.expectEqualStrings("reports", post_session.current_schema);

    var q = try runSqlSession(allocator, db, post_session, "SELECT id FROM t");
    defer q.deinit();
    const ids = try collectIds(allocator, &q);
    defer allocator.free(ids);
    try std.testing.expectEqualSlices(i64, &[_]i64{42}, ids);
}

test "sql namespace: SHOW TABLES lists the current schema's tables" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    _ = try seed(db, "alpha");
    _ = try seed(db, "beta");

    var q = try runSql(allocator, db, "SHOW TABLES");
    defer q.deinit();
    var names = try collectStrings(allocator, &q);
    defer freeStrings(allocator, &names);
    try std.testing.expectEqual(@as(usize, 2), names.items.len);
    try std.testing.expect(containsString(names, "alpha"));
    try std.testing.expect(containsString(names, "beta"));
}

test "sql namespace: reopened database lists and scans persisted tables" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    {
        var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
        defer db.close();
        _ = try seed(db, "persisted");
    }

    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    var show_q = try runSql(allocator, db, "SHOW TABLES");
    defer show_q.deinit();
    var names = try collectStrings(allocator, &show_q);
    defer freeStrings(allocator, &names);
    try std.testing.expect(containsString(names, "persisted"));

    var scan_q = try runSql(allocator, db, "SELECT id FROM persisted ORDER BY id");
    defer scan_q.deinit();
    const ids = try collectIds(allocator, &scan_q);
    defer allocator.free(ids);
    try std.testing.expectEqualSlices(i64, &[_]i64{ 1, 2, 3 }, ids);
}

test "sql namespace: SHOW TABLES FROM db.schema cross-references" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    const cat = db.owned_catalog.?;
    const wh = try cat.createDatabase("wh");
    const sales = try wh.createSchema("sales");
    _ = try sales.table("orders", schema_t, opts_t);

    var q = try runSql(allocator, db, "SHOW TABLES FROM wh.sales");
    defer q.deinit();
    var names = try collectStrings(allocator, &q);
    defer freeStrings(allocator, &names);
    try std.testing.expectEqual(@as(usize, 1), names.items.len);
    try std.testing.expect(containsString(names, "orders"));
}

test "sql namespace: DROP DATABASE that doesn't exist errors" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const root = try thindb.sql.parse(arena.allocator(), "DROP DATABASE ghost");
    try std.testing.expectError(thindb.net.Error.DatabaseNotFound, thindb.net.compile(allocator, db, root));
}

test "sql namespace: CREATE DATABASE twice errors" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    var q1 = try runSql(allocator, db, "CREATE DATABASE dup");
    _ = try q1.next();
    q1.deinit();

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const root = try thindb.sql.parse(arena.allocator(), "CREATE DATABASE dup");
    try std.testing.expectError(thindb.net.Error.DatabaseAlreadyExists, thindb.net.compile(allocator, db, root));
}

test "sql namespace: CREATE DATABASE IF NOT EXISTS is idempotent and DROP DATABASE IF EXISTS tolerates absence" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    var q1 = try runSql(allocator, db, "CREATE DATABASE IF NOT EXISTS idem");
    defer q1.deinit();
    try std.testing.expect((try q1.next()) == null);
    var q2 = try runSql(allocator, db, "CREATE DATABASE IF NOT EXISTS idem");
    defer q2.deinit();
    try std.testing.expect((try q2.next()) == null);

    var q3 = try runSql(allocator, db, "SHOW DATABASES");
    defer q3.deinit();
    var names = try collectStrings(allocator, &q3);
    defer freeStrings(allocator, &names);
    try std.testing.expect(containsString(names, "idem"));

    var q4 = try runSql(allocator, db, "DROP DATABASE IF EXISTS idem");
    defer q4.deinit();
    try std.testing.expect((try q4.next()) == null);
    var q5 = try runSql(allocator, db, "DROP DATABASE IF EXISTS idem");
    defer q5.deinit();
    try std.testing.expect((try q5.next()) == null);

    var q6 = try runSql(allocator, db, "SHOW DATABASES");
    defer q6.deinit();
    var after = try collectStrings(allocator, &q6);
    defer freeStrings(allocator, &after);
    try std.testing.expect(!containsString(after, "idem"));
}

test "sql namespace: CREATE SCHEMA IF NOT EXISTS and DROP SCHEMA IF EXISTS" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    var q1 = try runSql(allocator, db, "CREATE SCHEMA IF NOT EXISTS staging");
    defer q1.deinit();
    try std.testing.expect((try q1.next()) == null);
    var q2 = try runSql(allocator, db, "CREATE SCHEMA IF NOT EXISTS staging");
    defer q2.deinit();
    try std.testing.expect((try q2.next()) == null);

    var q3 = try runSql(allocator, db, "SHOW SCHEMAS");
    defer q3.deinit();
    var names = try collectStrings(allocator, &q3);
    defer freeStrings(allocator, &names);
    try std.testing.expect(containsString(names, "staging"));

    var q4 = try runSql(allocator, db, "DROP SCHEMA IF EXISTS staging");
    defer q4.deinit();
    try std.testing.expect((try q4.next()) == null);
    var q5 = try runSql(allocator, db, "DROP SCHEMA IF EXISTS staging");
    defer q5.deinit();
    try std.testing.expect((try q5.next()) == null);
}

test "sql namespace: existence clauses need the full keyword sequence and do not mask other errors" {
    const allocator = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    try std.testing.expectError(error.SqlExpectedKeyword, thindb.sql.parse(arena.allocator(), "CREATE DATABASE IF EXISTS x"));
    try std.testing.expectError(error.SqlExpectedKeyword, thindb.sql.parse(arena.allocator(), "DROP DATABASE IF NOT EXISTS x"));
    try std.testing.expectError(error.SqlExpectedKeyword, thindb.sql.parse(arena.allocator(), "CREATE SCHEMA IF x"));

    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    const root = try thindb.sql.parse(arena.allocator(), "DROP DATABASE ghost");
    try std.testing.expectError(thindb.net.Error.DatabaseNotFound, thindb.net.compile(allocator, db, root));
}

test "sql namespace: USE nonexistent db errors" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const root = try thindb.sql.parse(arena.allocator(), "USE ghost.public");
    try std.testing.expectError(thindb.net.Error.DatabaseNotFound, thindb.net.compile(allocator, db, root));
}

test "sql namespace: USE nonexistent schema errors" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const root = try thindb.sql.parse(arena.allocator(), "USE ghost");
    try std.testing.expectError(thindb.net.Error.SchemaNotFound, thindb.net.compile(allocator, db, root));
}
