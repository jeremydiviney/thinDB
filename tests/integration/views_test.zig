//! Views + materialized views (manual REFRESH). A plain view expands its
//! defining query at each reference; a materialized view is a real backing
//! table refreshed on demand. Both survive a catalog close/reopen.

const std = @import("std");
const thindb = @import("thindb");
const helpers = @import("sql_helpers.zig");
const exec = helpers.exec;
const runSqlCtx = helpers.runSqlCtx;
const runSql = helpers.runSql;

fn firstInt(allocator: std.mem.Allocator, db: anytype, sql: []const u8, ctx: bool) !i64 {
    var q = if (ctx) try runSqlCtx(allocator, db, sql) else try runSql(allocator, db, sql);
    defer q.deinit();
    const batch = (try q.next()) orelse return error.NoRows;
    return switch (batch.values[0].data) {
        .int => |s| s[0],
        .bigint => |s| s[0],
        else => error.NotInt,
    };
}

fn seed(allocator: std.mem.Allocator, db: anytype) !void {
    try exec(allocator, db, "CREATE TABLE sales (id INT, region STRING, amt INT, PRIMARY KEY (id))");
    try exec(allocator, db, "INSERT INTO sales (id, region, amt) VALUES (1,'east',10),(2,'west',20),(3,'east',30)");
    const t = try db.openTable("sales", .{});
    try t.flush();
    try exec(allocator, db, "CREATE VIEW east AS SELECT id, amt FROM sales WHERE region = 'east'");
    try exec(allocator, db, "CREATE MATERIALIZED VIEW totals AS SELECT region, SUM(amt) AS s FROM sales GROUP BY region");
}

test "view expands; materialized view snapshots + refreshes" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    try seed(allocator, db);

    // Plain view expands and reflects the base table.
    try std.testing.expectEqual(@as(i64, 40), try firstInt(allocator, db, "SELECT SUM(amt) FROM east", true));

    // Materialized view is a snapshot: adding a row is invisible until REFRESH.
    try std.testing.expectEqual(@as(i64, 40), try firstInt(allocator, db, "SELECT s FROM totals WHERE region = 'east'", false));
    try exec(allocator, db, "INSERT INTO sales (id, region, amt) VALUES (4,'east',100)");
    try std.testing.expectEqual(@as(i64, 40), try firstInt(allocator, db, "SELECT s FROM totals WHERE region = 'east'", false));
    // ...but the plain view sees it live.
    try std.testing.expectEqual(@as(i64, 140), try firstInt(allocator, db, "SELECT SUM(amt) FROM east", true));

    try exec(allocator, db, "REFRESH MATERIALIZED VIEW totals");
    try std.testing.expectEqual(@as(i64, 140), try firstInt(allocator, db, "SELECT s FROM totals WHERE region = 'east'", false));
}

test "views + materialized views survive a reopen" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    {
        var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
        defer db.close();
        try seed(allocator, db);
        try exec(allocator, db, "REFRESH MATERIALIZED VIEW totals");
    }

    // Reopen: the view registry reloads from `_views/`, the MV backing table
    // from the normal table path.
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    // Plain view still expands after reload.
    try std.testing.expectEqual(@as(i64, 40), try firstInt(allocator, db, "SELECT SUM(amt) FROM east", true));
    // Materialized view backing table still holds its snapshot.
    try std.testing.expectEqual(@as(i64, 40), try firstInt(allocator, db, "SELECT s FROM totals WHERE region = 'east'", false));
    // REFRESH works after reload (defining query re-parsed from persisted text).
    try exec(allocator, db, "INSERT INTO sales (id, region, amt) VALUES (9,'east',5)");
    try exec(allocator, db, "REFRESH MATERIALIZED VIEW totals");
    try std.testing.expectEqual(@as(i64, 45), try firstInt(allocator, db, "SELECT s FROM totals WHERE region = 'east'", false));
}

fn runIn(allocator: std.mem.Allocator, db: *thindb.Database, database: []const u8, sql: []const u8) !helpers.RunResult {
    var arena = std.heap.ArenaAllocator.init(allocator);
    errdefer arena.deinit();
    const cat = db.catalog.?;
    const session: thindb.Session = .{ .current_db = database };
    const tables: thindb.net.SessionTables = .{ .catalog = cat, .session = session };
    const root = try thindb.sql.parseWithContext(
        arena.allocator(),
        sql,
        .neutral,
        &cat.udfs,
        .{ .registry = &cat.sql_fns, .db = database, .views = &cat.views, .tables = tables.columns() },
    );
    const cq = try thindb.net.compileWithSession(allocator, db, session, root);
    return .{ .arena = arena, .cq = cq, .owned_vars = cq.sessionValue().vars, .backing_allocator = allocator };
}

fn execIn(allocator: std.mem.Allocator, db: *thindb.Database, database: []const u8, sql: []const u8) !void {
    var q = try runIn(allocator, db, database, sql);
    defer q.deinit();
    while (try q.next()) |_| {}
}

fn firstIntIn(allocator: std.mem.Allocator, db: *thindb.Database, database: []const u8, sql: []const u8) !i64 {
    var q = try runIn(allocator, db, database, sql);
    defer q.deinit();
    const batch = (try q.next()) orelse return error.NoRows;
    return switch (batch.values[0].data) {
        .int => |s| s[0],
        .bigint => |s| s[0],
        else => error.NotInt,
    };
}

test "DROP DATABASE takes its views and functions with it (#370)" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    const expectEqual = std.testing.expectEqual;
    const expectError = std.testing.expectError;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    {
        var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
        defer db.close();
        try exec(allocator, db, "CREATE DATABASE probe");
        try exec(allocator, db, "CREATE DATABASE probe_v");
        for ([_][]const u8{ "probe", "probe_v" }) |database| {
            try execIn(allocator, db, database, "CREATE VIEW ghost AS SELECT 1 AS one");
            try execIn(allocator, db, database, "CREATE MATERIALIZED VIEW mv AS SELECT 5 AS five");
            try execIn(allocator, db, database, "CREATE FUNCTION gf(x BIGINT) RETURNS TABLE AS (SELECT x AS one)");
        }
        try expectEqual(@as(i64, 1), try firstIntIn(allocator, db, "probe_v", "SELECT * FROM ghost"));
        try expectEqual(@as(i64, 7), try firstIntIn(allocator, db, "probe_v", "SELECT * FROM gf(7)"));

        try exec(allocator, db, "DROP DATABASE probe_v");
        // A session still using the dropped database defines nothing there.
        try expectError(error.DatabaseNotFound, execIn(allocator, db, "probe_v", "CREATE VIEW late AS SELECT 3 AS three"));
        try expectError(error.DatabaseNotFound, execIn(allocator, db, "probe_v", "CREATE FUNCTION lf(x BIGINT) RETURNS TABLE AS (SELECT x AS three)"));
        try exec(allocator, db, "CREATE DATABASE probe_v");

        try expectError(error.TableNotFound, firstIntIn(allocator, db, "probe_v", "SELECT * FROM ghost"));
        try expectError(error.TableNotFound, firstIntIn(allocator, db, "probe_v", "SELECT * FROM late"));
        try expectError(error.TableNotFound, firstIntIn(allocator, db, "probe_v", "SELECT * FROM mv"));
        try expectError(error.SqlUnsupportedFileFunction, firstIntIn(allocator, db, "probe_v", "SELECT * FROM gf(7)"));
        try expectError(error.FunctionNotFound, execIn(allocator, db, "probe_v", "SHOW CREATE FUNCTION gf"));
        try expectError(error.FunctionNotFound, execIn(allocator, db, "probe_v", "SHOW CREATE FUNCTION lf"));

        try execIn(allocator, db, "probe_v", "CREATE VIEW ghost AS SELECT 2 AS two");
        try execIn(allocator, db, "probe_v", "CREATE MATERIALIZED VIEW mv AS SELECT 6 AS six");
        try execIn(allocator, db, "probe_v", "CREATE FUNCTION gf(x BIGINT) RETURNS TABLE AS (SELECT 8 AS eight, x AS one)");
        try expectEqual(@as(i64, 2), try firstIntIn(allocator, db, "probe_v", "SELECT * FROM ghost"));
        try expectEqual(@as(i64, 6), try firstIntIn(allocator, db, "probe_v", "SELECT * FROM mv"));
        try expectEqual(@as(i64, 8), try firstIntIn(allocator, db, "probe_v", "SELECT * FROM gf(7)"));
        try expectEqual(@as(i64, 1), try firstIntIn(allocator, db, "probe", "SELECT * FROM ghost"));
        try expectEqual(@as(i64, 7), try firstIntIn(allocator, db, "probe", "SELECT * FROM gf(7)"));
    }

    // A restart loads exactly what the running catalog answered.
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    try expectEqual(@as(i64, 2), try firstIntIn(allocator, db, "probe_v", "SELECT * FROM ghost"));
    try expectEqual(@as(i64, 6), try firstIntIn(allocator, db, "probe_v", "SELECT * FROM mv"));
    try expectEqual(@as(i64, 8), try firstIntIn(allocator, db, "probe_v", "SELECT * FROM gf(7)"));
    try expectError(error.TableNotFound, firstIntIn(allocator, db, "probe_v", "SELECT * FROM late"));
    try expectEqual(@as(i64, 1), try firstIntIn(allocator, db, "probe", "SELECT * FROM ghost"));
    try expectEqual(@as(i64, 5), try firstIntIn(allocator, db, "probe", "SELECT * FROM mv"));
}

/// A UDF the defining queries below call once per batch: the call numbered
/// `fail_on_call` fails the query there, part way through its rows.
const Probe = struct {
    fail_on_call: ?usize = null,
    calls: usize = 0,

    fn kernel(ctx: *const thindb.udf.ScalarContext, args: []const thindb.storage.ColumnView, out: *thindb.engine.ColumnStore, count: usize) !void {
        const self: *Probe = @ptrCast(@alignCast(ctx.user_data.?));
        self.calls += 1;
        if (self.fail_on_call == self.calls) return error.ProbeFailed;
        try out.data.bigint.appendSlice(ctx.allocator, args[0].data.bigint[0..count]);
    }

    fn open(allocator: std.mem.Allocator, io: std.Io, dir: std.Io.Dir, probe: *Probe) !*thindb.Database {
        const db = try thindb.Database.open(allocator, io, dir, .{});
        errdefer db.close();
        try db.registerScalarUdf(.{
            .name = "probe",
            .arg_types = &.{.bigint},
            .return_type = .bigint,
            .kernel = kernel,
            .user_data = probe,
        });
        return db;
    }

    /// Fail the next statement's second batch, after its first has landed.
    fn failPartWay(self: *Probe) void {
        self.calls = 0;
        self.fail_on_call = 2;
    }
};

/// Two batches, one per UNION ALL branch, each through `probe`.
const two_batch_body = "SELECT probe(id) AS id FROM src WHERE id <= 2 UNION ALL SELECT probe(id) AS id FROM src WHERE id > 2";

fn seedSource(allocator: std.mem.Allocator, db: *thindb.Database) !void {
    try exec(allocator, db, "CREATE TABLE src (id BIGINT, PRIMARY KEY (id))");
    try exec(allocator, db, "INSERT INTO src (id) VALUES (1),(2),(3),(4)");
}

fn expectIds(allocator: std.mem.Allocator, db: *thindb.Database, sql: []const u8, expected: []const i64) !void {
    const ids = try helpers.collectBigints(allocator, db, sql);
    defer allocator.free(ids);
    try std.testing.expectEqualSlices(i64, expected, ids);
}

/// Directories of an unfinished build or swap: `__ctas_*` and `__alter_*`.
fn countSwapDirs(sc: *thindb.api.Schema) !usize {
    var n: usize = 0;
    var it = sc.schema_dir.iterate();
    while (try it.next(sc.io)) |entry| {
        if (std.mem.startsWith(u8, entry.name, "__")) n += 1;
    }
    return n;
}

fn expectListed(allocator: std.mem.Allocator, sc: *thindb.api.Schema, expected: []const []const u8) !void {
    const names = try sc.listTables(allocator);
    defer {
        for (names) |name| allocator.free(name);
        allocator.free(names);
    }
    std.mem.sort([]u8, names, {}, struct {
        fn lessThan(_: void, a: []u8, b: []u8) bool {
            return std.mem.lessThan(u8, a, b);
        }
    }.lessThan);
    try std.testing.expectEqual(expected.len, names.len);
    for (expected, names) |want, got| try std.testing.expectEqualStrings(want, got);
}

test "materialized view: a CREATE whose query fails leaves no table and no view, and a retry succeeds" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var probe: Probe = .{};
    var db = try Probe.open(allocator, io, tmp.dir, &probe);
    defer db.close();
    try seedSource(allocator, db);
    const sc = db.schema("public").?;

    probe.failPartWay();
    try helpers.expectRunError(allocator, db, "CREATE MATERIALIZED VIEW mv AS " ++ two_batch_body, error.ProbeFailed);
    try std.testing.expectEqual(@as(usize, 2), probe.calls);
    try std.testing.expectError(thindb.Error.TableNotFound, db.openTable("mv", .{}));
    try std.testing.expect(!db.catalog.?.views.contains("main", "mv"));
    try std.testing.expectEqual(@as(usize, 0), try countSwapDirs(sc));
    try expectListed(allocator, sc, &.{"src"});

    probe.fail_on_call = null;
    try exec(allocator, db, "CREATE MATERIALIZED VIEW mv AS " ++ two_batch_body);
    try expectIds(allocator, db, "SELECT id FROM mv ORDER BY id", &.{ 1, 2, 3, 4 });
    try std.testing.expect(db.catalog.?.views.contains("main", "mv"));
}

test "materialized view: a REFRESH whose query fails keeps the old rows exactly" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var probe: Probe = .{};
    var db = try Probe.open(allocator, io, tmp.dir, &probe);
    defer db.close();
    try seedSource(allocator, db);
    const sc = db.schema("public").?;
    try exec(allocator, db, "CREATE MATERIALIZED VIEW mv AS " ++ two_batch_body);
    try exec(allocator, db, "INSERT INTO src (id) VALUES (5),(6)");

    probe.failPartWay();
    try helpers.expectRunError(allocator, db, "REFRESH MATERIALIZED VIEW mv", error.ProbeFailed);
    try std.testing.expectEqual(@as(usize, 2), probe.calls);
    try expectIds(allocator, db, "SELECT id FROM mv ORDER BY id", &.{ 1, 2, 3, 4 });
    try std.testing.expectEqual(@as(usize, 0), try countSwapDirs(sc));
    try expectListed(allocator, sc, &.{ "mv", "src" });

    probe.fail_on_call = null;
    try exec(allocator, db, "REFRESH MATERIALIZED VIEW mv");
    try expectIds(allocator, db, "SELECT id FROM mv ORDER BY id", &.{ 1, 2, 3, 4, 5, 6 });
    try std.testing.expectEqual(@as(usize, 0), try countSwapDirs(sc));
}

test "materialized view: REFRESH keeps the backing table's keys, compression and row group size" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var probe: Probe = .{};
    var db = try Probe.open(allocator, io, tmp.dir, &probe);
    defer db.close();
    try seedSource(allocator, db);
    try exec(allocator, db, "CREATE MATERIALIZED VIEW mv AS SELECT id, id AS tens FROM src");
    // The table a REFRESH replaces, reshaped as a user may: another order
    // key, unique, zstd, and its own row group size.
    const sc = db.schema("public").?;
    const reshaped: thindb.TableSchema = .{
        .columns = &.{ .{ .name = "id", .type = .bigint, .nullable = true }, .{ .name = "tens", .type = .bigint, .nullable = true } },
        .order_key = &.{ "tens", "id" },
        .unique = true,
        .compression = .zstd,
    };
    try db.dropTable("mv");
    {
        var build = try sc.beginTableBuild("mv", reshaped, .{ .order_key = reshaped.order_key, .unique = true, .row_group_size = 4096 });
        defer build.deinit();
        try build.publish();
    }

    try exec(allocator, db, "REFRESH MATERIALIZED VIEW mv");
    const mv = try db.openTable("mv", .{});
    try std.testing.expectEqual(@as(usize, 4096), mv.row_group_size);
    try std.testing.expect(mv.schema.unique);
    try std.testing.expectEqual(thindb.types.TableCompression.zstd, mv.schema.compression);
    try std.testing.expectEqual(@as(usize, 2), mv.schema.order_key.len);
    try std.testing.expectEqualStrings("tens", mv.schema.order_key[0]);
    try expectIds(allocator, db, "SELECT tens FROM mv ORDER BY id", &.{ 1, 2, 3, 4 });
}

test "materialized view: a CREATE OR REPLACE whose query fails keeps what held the name" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var probe: Probe = .{};
    var db = try Probe.open(allocator, io, tmp.dir, &probe);
    defer db.close();
    try seedSource(allocator, db);
    const sc = db.schema("public").?;
    try exec(allocator, db, "CREATE MATERIALIZED VIEW mv AS SELECT id FROM src WHERE id <= 2");
    try exec(allocator, db, "CREATE TABLE plain (id BIGINT, PRIMARY KEY (id))");
    try exec(allocator, db, "INSERT INTO plain (id) VALUES (7)");

    inline for (.{ "mv", "plain" }) |name| {
        probe.failPartWay();
        try helpers.expectRunError(allocator, db, "CREATE OR REPLACE MATERIALIZED VIEW " ++ name ++ " AS " ++ two_batch_body, error.ProbeFailed);
        try std.testing.expectEqual(@as(usize, 0), try countSwapDirs(sc));
    }
    try expectIds(allocator, db, "SELECT id FROM mv ORDER BY id", &.{ 1, 2 });
    try expectIds(allocator, db, "SELECT id FROM plain ORDER BY id", &.{7});
    try std.testing.expect(!db.catalog.?.views.contains("main", "plain"));
    // The old definition stands too: a REFRESH runs it.
    try exec(allocator, db, "INSERT INTO src (id) VALUES (0)");
    try exec(allocator, db, "REFRESH MATERIALIZED VIEW mv");
    try expectIds(allocator, db, "SELECT id FROM mv ORDER BY id", &.{ 0, 1, 2 });

    probe.fail_on_call = null;
    try exec(allocator, db, "CREATE OR REPLACE MATERIALIZED VIEW mv AS " ++ two_batch_body);
    try expectIds(allocator, db, "SELECT id FROM mv ORDER BY id", &.{ 0, 1, 2, 3, 4 });
    // A definition may read the table it replaces.
    try exec(allocator, db, "CREATE OR REPLACE MATERIALIZED VIEW mv AS SELECT id FROM mv WHERE id >= 3");
    try expectIds(allocator, db, "SELECT id FROM mv ORDER BY id", &.{ 3, 4 });
    try std.testing.expectEqual(@as(usize, 0), try countSwapDirs(sc));
    try expectListed(allocator, sc, &.{ "mv", "plain", "src" });
}

test "materialized view: a definition that fails to persist leaves no table and no view" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var probe: Probe = .{};
    var db = try Probe.open(allocator, io, tmp.dir, &probe);
    defer db.close();
    try seedSource(allocator, db);
    const sc = db.schema("public").?;
    // A file where the definitions' directory goes refuses every write.
    try tmp.dir.writeFile(io, .{ .sub_path = "main/_views", .data = "" });

    try std.testing.expect(std.meta.isError(exec(allocator, db, "CREATE MATERIALIZED VIEW mv AS SELECT id FROM src")));
    try std.testing.expectError(thindb.Error.TableNotFound, db.openTable("mv", .{}));
    try std.testing.expect(!db.catalog.?.views.contains("main", "mv"));
    try std.testing.expect(std.meta.isError(exec(allocator, db, "CREATE VIEW v AS SELECT id FROM src")));
    try std.testing.expect(!db.catalog.?.views.contains("main", "v"));
    try std.testing.expectEqual(@as(usize, 0), try countSwapDirs(sc));
    try expectListed(allocator, sc, &.{"src"});
}

test "materialized view: an interrupted REFRESH reopens with the old rows before its commit, the new after" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    const State = enum { build_only, set_aside, committed };
    inline for (.{ State.build_only, State.set_aside, State.committed }) |state| {
        var tmp = std.testing.tmpDir(.{});
        defer tmp.cleanup();
        var probe: Probe = .{};
        {
            var db = try Probe.open(allocator, io, tmp.dir, &probe);
            defer db.close();
            try seedSource(allocator, db);
            try exec(allocator, db, "CREATE MATERIALIZED VIEW mv AS SELECT id FROM src WHERE id <= 2");
            // The contents a REFRESH would build, in a table of their own.
            try exec(allocator, db, "CREATE TABLE fresh AS SELECT id FROM src");
        }
        // Lay the trees out as a crash at `state` leaves them.
        {
            var public = try tmp.dir.openDir(io, "main/public", .{});
            defer public.close(io);
            const rename = std.Io.Dir.rename;
            switch (state) {
                .build_only => try rename(public, "fresh", public, "__ctas_0", io),
                .set_aside => {
                    try rename(public, "mv", public, "__alter_old_mv", io);
                    try rename(public, "fresh", public, "__ctas_0", io);
                },
                .committed => {
                    try rename(public, "mv", public, "__alter_old_mv", io);
                    try rename(public, "fresh", public, "mv", io);
                },
            }
        }

        var db = try Probe.open(allocator, io, tmp.dir, &probe);
        defer db.close();
        const sc = db.schema("public").?;
        const want: []const i64 = if (state == .committed) &.{ 1, 2, 3, 4 } else &.{ 1, 2 };
        try expectIds(allocator, db, "SELECT id FROM mv ORDER BY id", want);
        try std.testing.expectEqual(@as(usize, 0), try countSwapDirs(sc));
        try expectListed(allocator, sc, &.{ "mv", "src" });
        try exec(allocator, db, "REFRESH MATERIALIZED VIEW mv");
        try expectIds(allocator, db, "SELECT id FROM mv ORDER BY id", &.{ 1, 2 });
    }
}

test "materialized view: DROP leaves nothing a reopen brings back, a leftover aside or build included" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var probe: Probe = .{};
    {
        var db = try Probe.open(allocator, io, tmp.dir, &probe);
        defer db.close();
        try seedSource(allocator, db);
        try exec(allocator, db, "CREATE MATERIALIZED VIEW mv AS SELECT id FROM src");
        try exec(allocator, db, "CREATE TABLE spare AS SELECT id FROM src");
        try exec(allocator, db, "CREATE TABLE spare2 AS SELECT id FROM src");
    }
    {
        var db = try Probe.open(allocator, io, tmp.dir, &probe);
        defer db.close();
        // An aside a committed swap failed to delete, and a build a crash
        // left, appearing while the schema is open.
        {
            var public = try tmp.dir.openDir(io, "main/public", .{});
            defer public.close(io);
            try std.Io.Dir.rename(public, "spare", public, "__alter_old_mv", io);
            try std.Io.Dir.rename(public, "spare2", public, "__ctas_9", io);
        }
        try exec(allocator, db, "DROP MATERIALIZED VIEW mv");
        try std.testing.expectError(thindb.Error.TableNotFound, db.openTable("mv", .{}));
    }
    var db = try Probe.open(allocator, io, tmp.dir, &probe);
    defer db.close();
    const sc = db.schema("public").?;
    try std.testing.expectError(thindb.Error.TableNotFound, db.openTable("mv", .{}));
    try std.testing.expect(!db.catalog.?.views.contains("main", "mv"));
    try std.testing.expectEqual(@as(usize, 0), try countSwapDirs(sc));
    try expectListed(allocator, sc, &.{"src"});
}
