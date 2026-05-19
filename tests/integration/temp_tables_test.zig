//! Session-scoped temp tables. Exercises CREATE TEMP TABLE creation,
//! visibility shadowing, DROP TABLE semantics, auto-flush spill, and
//! per-session cleanup through the same path the MySQL/PG wire layers
//! use (compileWithSession against a Session that carries a
//! TempNamespace pointer).

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

    pub fn affectedRows(self: *const RunResult) u64 {
        return self.cq.affectedRows();
    }
};

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
    return .{ .arena = arena, .cq = cq };
}

fn runAndDrain(
    allocator: std.mem.Allocator,
    db: anytype,
    session: thindb.api.Session,
    sql: []const u8,
) !void {
    var q = try runSqlSession(allocator, db, session, sql);
    defer q.deinit();
    while (try q.next()) |_| {}
}

fn expectCompileError(
    allocator: std.mem.Allocator,
    db: anytype,
    session: thindb.api.Session,
    sql: []const u8,
    expected: anyerror,
) !void {
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const root = try thindb.sql.parse(arena.allocator(), sql);
    const cq_result = thindb.net.compileWithSession(allocator, db, session, root);
    if (cq_result) |cq_ok| {
        var cq = cq_ok;
        cq.deinit();
        return error.TestUnexpectedSuccess;
    } else |err| {
        try std.testing.expectEqual(expected, err);
    }
}

fn collectStringColumn(
    allocator: std.mem.Allocator,
    q: *RunResult,
) !std.ArrayList([]const u8) {
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

fn freeStringSlice(allocator: std.mem.Allocator, ss: *std.ArrayList([]const u8)) void {
    for (ss.items) |s| allocator.free(s);
    ss.deinit(allocator);
}

fn containsName(list: std.ArrayList([]const u8), needle: []const u8) bool {
    for (list.items) |s| if (std.mem.eql(u8, s, needle)) return true;
    return false;
}

fn collectI64Column(allocator: std.mem.Allocator, q: *RunResult) ![]i64 {
    var out: std.ArrayList(i64) = .empty;
    errdefer out.deinit(allocator);
    while (try q.next()) |b| {
        for (b.values[0].data.bigint[0..b.row_count]) |v| try out.append(allocator, v);
    }
    return try out.toOwnedSlice(allocator);
}

test "temp tables: CREATE TEMP TABLE + INSERT + SELECT round-trip" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    const catalog = thindb.net.catalogFor(db).?;
    var ns = try thindb.TempNamespace.open(allocator, io, catalog.root_dir, 42, catalog.config);
    defer ns.close();

    const session: thindb.api.Session = .{ .temp_namespace = ns };

    try runAndDrain(allocator, db, session,
        "CREATE TEMP TABLE scratch (id BIGINT PRIMARY KEY, qty INT)",
    );
    try runAndDrain(allocator, db, session,
        "INSERT INTO scratch VALUES (1, 10), (2, 20), (3, 30)",
    );

    var q = try runSqlSession(allocator, db, session, "SELECT id FROM scratch ORDER BY id ASC");
    defer q.deinit();
    const ids = try collectI64Column(allocator, &q);
    defer allocator.free(ids);
    try std.testing.expectEqualSlices(i64, &[_]i64{ 1, 2, 3 }, ids);
}

test "temp tables: other sessions can't see this session's temps" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    const catalog = thindb.net.catalogFor(db).?;
    var ns_a = try thindb.TempNamespace.open(allocator, io, catalog.root_dir, 1, catalog.config);
    defer ns_a.close();
    var ns_b = try thindb.TempNamespace.open(allocator, io, catalog.root_dir, 2, catalog.config);
    defer ns_b.close();

    const session_a: thindb.api.Session = .{ .temp_namespace = ns_a };
    const session_b: thindb.api.Session = .{ .temp_namespace = ns_b };

    try runAndDrain(allocator, db, session_a,
        "CREATE TEMP TABLE only_a (id BIGINT PRIMARY KEY)",
    );

    // session_b cannot resolve `only_a` — neither in catalog nor in its own ns.
    try expectCompileError(allocator, db, session_b, "SELECT * FROM only_a", error.TableNotFound);

    // SHOW TABLES from session_b must not list `only_a`.
    var q = try runSqlSession(allocator, db, session_b, "SHOW TABLES");
    defer q.deinit();
    var names = try collectStringColumn(allocator, &q);
    defer freeStringSlice(allocator, &names);
    try std.testing.expect(!containsName(names, "only_a"));
}

test "temp tables: temp shadows persistent table for creating session only" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    // Persistent `shared` with one row.
    const schema: thindb.TableSchema = .{
        .columns = &.{ .{ .name = "id", .type = .bigint }, .{ .name = "tag", .type = .string } },
        .order_key = &.{"id"},
        .unique = true,
    };
    const ok = [_][]const u8{"id"};
    const persistent = try db.table("shared", schema, .{ .order_key = &ok, .unique = true });
    try persistent.insert(&.{.{ .id = @as(i64, 100), .tag = @as([]const u8, "persistent") }});
    try persistent.flush();

    const catalog = thindb.net.catalogFor(db).?;
    var ns_a = try thindb.TempNamespace.open(allocator, io, catalog.root_dir, 10, catalog.config);
    defer ns_a.close();
    var ns_b = try thindb.TempNamespace.open(allocator, io, catalog.root_dir, 11, catalog.config);
    defer ns_b.close();

    const session_a: thindb.api.Session = .{ .temp_namespace = ns_a };
    const session_b: thindb.api.Session = .{ .temp_namespace = ns_b };

    // session_a shadows with a different schema (same name).
    try runAndDrain(allocator, db, session_a,
        "CREATE TEMP TABLE shared (id BIGINT PRIMARY KEY, qty INT)",
    );
    try runAndDrain(allocator, db, session_a, "INSERT INTO shared VALUES (1, 999)");

    // session_a sees its temp (id=1).
    var qa = try runSqlSession(allocator, db, session_a, "SELECT id FROM shared ORDER BY id ASC");
    defer qa.deinit();
    const ids_a = try collectI64Column(allocator, &qa);
    defer allocator.free(ids_a);
    try std.testing.expectEqualSlices(i64, &[_]i64{1}, ids_a);

    // session_b sees the persistent (id=100).
    var qb = try runSqlSession(allocator, db, session_b, "SELECT id FROM shared ORDER BY id ASC");
    defer qb.deinit();
    const ids_b = try collectI64Column(allocator, &qb);
    defer allocator.free(ids_b);
    try std.testing.expectEqualSlices(i64, &[_]i64{100}, ids_b);
}

test "temp tables: DROP TABLE removes the shadowing temp; persistent stays" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    const schema: thindb.TableSchema = .{
        .columns = &.{ .{ .name = "id", .type = .bigint }, .{ .name = "tag", .type = .string } },
        .order_key = &.{"id"},
        .unique = true,
    };
    const ok = [_][]const u8{"id"};
    const persistent = try db.table("shared", schema, .{ .order_key = &ok, .unique = true });
    try persistent.insert(&.{.{ .id = @as(i64, 100), .tag = @as([]const u8, "persistent") }});
    try persistent.flush();

    const catalog = thindb.net.catalogFor(db).?;
    var ns = try thindb.TempNamespace.open(allocator, io, catalog.root_dir, 20, catalog.config);
    defer ns.close();
    const session: thindb.api.Session = .{ .temp_namespace = ns };

    try runAndDrain(allocator, db, session,
        "CREATE TEMP TABLE shared (id BIGINT PRIMARY KEY, qty INT)",
    );
    try runAndDrain(allocator, db, session, "INSERT INTO shared VALUES (1, 999)");

    try runAndDrain(allocator, db, session, "DROP TABLE shared");

    // After dropping the temp, the persistent table is visible again.
    var q = try runSqlSession(allocator, db, session, "SELECT id FROM shared ORDER BY id ASC");
    defer q.deinit();
    const ids = try collectI64Column(allocator, &q);
    defer allocator.free(ids);
    try std.testing.expectEqualSlices(i64, &[_]i64{100}, ids);
}

test "temp tables: DROP TEMP TABLE keyword form is accepted" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    const catalog = thindb.net.catalogFor(db).?;
    var ns = try thindb.TempNamespace.open(allocator, io, catalog.root_dir, 21, catalog.config);
    defer ns.close();
    const session: thindb.api.Session = .{ .temp_namespace = ns };

    try runAndDrain(allocator, db, session,
        "CREATE TEMP TABLE scratch (id BIGINT PRIMARY KEY)",
    );
    try runAndDrain(allocator, db, session, "DROP TEMP TABLE scratch");

    try std.testing.expect(ns.findTable("scratch") == null);
}

test "temp tables: SHOW TABLES from creating session lists the temp" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    const catalog = thindb.net.catalogFor(db).?;
    var ns = try thindb.TempNamespace.open(allocator, io, catalog.root_dir, 30, catalog.config);
    defer ns.close();
    const session: thindb.api.Session = .{ .temp_namespace = ns };

    try runAndDrain(allocator, db, session,
        "CREATE TEMP TABLE my_temp (id BIGINT PRIMARY KEY)",
    );

    var q = try runSqlSession(allocator, db, session, "SHOW TABLES");
    defer q.deinit();
    var names = try collectStringColumn(allocator, &q);
    defer freeStringSlice(allocator, &names);
    try std.testing.expect(containsName(names, "my_temp"));
}

test "temp tables: auto-flush spill keeps data readable" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    // Force auto-flush after a tiny number of rows so we exercise the
    // spill path without inserting millions.
    const config: thindb.Config = .{
        .auto_flush_rows = 4,
        .auto_flush_bytes = std.math.maxInt(usize),
        .auto_flush_secs = 0,
    };
    var db = try thindb.Database.open(allocator, io, tmp.dir, config);
    defer db.close();

    const catalog = thindb.net.catalogFor(db).?;
    var ns = try thindb.TempNamespace.open(allocator, io, catalog.root_dir, 40, catalog.config);
    defer ns.close();
    const session: thindb.api.Session = .{ .temp_namespace = ns };

    try runAndDrain(allocator, db, session,
        "CREATE TEMP TABLE spillable (id BIGINT PRIMARY KEY, val INT)",
    );
    try runAndDrain(allocator, db, session,
        "INSERT INTO spillable VALUES (1, 10), (2, 20), (3, 30), (4, 40), (5, 50)",
    );

    const t = ns.findTable("spillable").?;
    try std.testing.expect(t.manifest.segments.items.len > 0);

    var q = try runSqlSession(allocator, db, session, "SELECT id FROM spillable ORDER BY id ASC");
    defer q.deinit();
    const ids = try collectI64Column(allocator, &q);
    defer allocator.free(ids);
    try std.testing.expectEqualSlices(i64, &[_]i64{ 1, 2, 3, 4, 5 }, ids);
}

test "temp tables: namespace close removes the per-session dir" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const config: thindb.Config = .{
        .auto_flush_rows = 2,
        .auto_flush_bytes = std.math.maxInt(usize),
        .auto_flush_secs = 0,
    };
    var db = try thindb.Database.open(allocator, io, tmp.dir, config);
    defer db.close();

    const catalog = thindb.net.catalogFor(db).?;

    // Open, write enough to force a spill, then close — the
    // per-session dir should be gone.
    {
        var ns = try thindb.TempNamespace.open(allocator, io, catalog.root_dir, 99, catalog.config);
        const session: thindb.api.Session = .{ .temp_namespace = ns };
        try runAndDrain(allocator, db, session,
            "CREATE TEMP TABLE doomed (id BIGINT PRIMARY KEY)",
        );
        try runAndDrain(allocator, db, session,
            "INSERT INTO doomed VALUES (1), (2), (3)",
        );
        ns.close();
    }

    // Probe `_temp/99` — should be missing.
    const probe = catalog.root_dir.openDir(io, "_temp/99", .{});
    if (probe) |dir_| {
        var d = dir_;
        d.close(io);
        return error.TempDirNotCleanedUp;
    } else |err| switch (err) {
        error.FileNotFound => {},
        else => return err,
    }
}

test "temp tables: duplicate CREATE TEMP TABLE returns TableAlreadyExists" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    const catalog = thindb.net.catalogFor(db).?;
    var ns = try thindb.TempNamespace.open(allocator, io, catalog.root_dir, 50, catalog.config);
    defer ns.close();
    const session: thindb.api.Session = .{ .temp_namespace = ns };

    try runAndDrain(allocator, db, session,
        "CREATE TEMP TABLE dup (id BIGINT PRIMARY KEY)",
    );
    try expectCompileError(
        allocator,
        db,
        session,
        "CREATE TEMP TABLE dup (id BIGINT PRIMARY KEY)",
        error.TableAlreadyExists,
    );
}
