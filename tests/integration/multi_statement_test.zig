//! Multi-statement SQL frames: `;`-separated CREATE / INSERT / SELECT
//! in any order. The parser returns a single `ir.Op` for one-statement
//! input (back-compat) and an `Op.batch` wrapping the list when there
//! are two or more. Compilation of the batch happens statement-by-
//! statement at the wire layer; these tests exercise the parser shape
//! and drive each sub-statement through `local.compile` manually.

const std = @import("std");
const thindb = @import("thindb");

const ir = thindb.ir;

/// Parse `sql` and compile + drain each top-level statement against
/// `db`, returning the SELECT row count from the *final* statement
/// (zero for side-effect statements). All sub-queries are torn down
/// before this function returns.
fn drainBatch(allocator: std.mem.Allocator, db: anytype, sql: []const u8) !usize {
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const root = try thindb.sql.parse(arena.allocator(), sql);

    if (root.* != .batch) {
        return try drainOne(allocator, db, root);
    }
    var last_rows: usize = 0;
    for (root.batch.statements) |stmt| {
        last_rows = try drainOne(allocator, db, stmt);
    }
    return last_rows;
}

fn drainOne(allocator: std.mem.Allocator, db: anytype, op: *const ir.Op) !usize {
    var cq = try thindb.net.compile(allocator, db, op);
    defer cq.deinit();
    var rows: usize = 0;
    while (try cq.next()) |batch| rows += batch.row_count;
    return rows;
}

test "multi-statement: single statement still returns non-batch op" {
    const allocator = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const root = try thindb.sql.parse(arena.allocator(), "CREATE TABLE t (id BIGINT PRIMARY KEY)");
    try std.testing.expect(root.* != .batch);
    try std.testing.expect(root.* == .ddl);
}

test "multi-statement: trailing semicolon on single statement keeps non-batch shape" {
    const allocator = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const root = try thindb.sql.parse(arena.allocator(), "CREATE TABLE t (id BIGINT PRIMARY KEY);");
    try std.testing.expect(root.* != .batch);
    try std.testing.expect(root.* == .ddl);
}

test "multi-statement: two statements parse to a batch" {
    const allocator = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const root = try thindb.sql.parse(
        arena.allocator(),
        "CREATE TABLE t (id BIGINT PRIMARY KEY); INSERT INTO t VALUES (1)",
    );
    try std.testing.expect(root.* == .batch);
    try std.testing.expectEqual(@as(usize, 2), root.batch.statements.len);
    try std.testing.expect(root.batch.statements[0].* == .ddl);
    try std.testing.expect(root.batch.statements[1].* == .insert);
}

test "multi-statement: CREATE + INSERT applies both" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    _ = try drainBatch(allocator, db, "CREATE TABLE t (id BIGINT PRIMARY KEY, qty INT NOT NULL); " ++
        "INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)");
    const t = try db.openTable("t", .{});
    try t.flush();
    const seen = try drainBatch(allocator, db, "SELECT id FROM t");
    try std.testing.expectEqual(@as(usize, 3), seen);
}

test "multi-statement: CREATE + INSERT + SELECT returns inserted rows" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    // SELECT runs against the memtable inserted in the previous statement
    // (same connection, same compileWithSession run).
    const final_rows = try drainBatch(allocator, db, "CREATE TABLE t (id BIGINT PRIMARY KEY, qty INT NOT NULL); " ++
        "INSERT INTO t VALUES (1, 10), (2, 20), (3, 30); " ++
        "SELECT id FROM t");
    try std.testing.expectEqual(@as(usize, 3), final_rows);
}

test "multi-statement: trailing semicolon after final statement is tolerated" {
    const allocator = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const root = try thindb.sql.parse(
        arena.allocator(),
        "CREATE TABLE t (id BIGINT PRIMARY KEY); INSERT INTO t VALUES (1);",
    );
    try std.testing.expect(root.* == .batch);
    try std.testing.expectEqual(@as(usize, 2), root.batch.statements.len);
}

test "multi-statement: empty statements between real ones are skipped" {
    const allocator = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const root = try thindb.sql.parse(
        arena.allocator(),
        ";;CREATE TABLE t (id BIGINT PRIMARY KEY);;;INSERT INTO t VALUES (1);;",
    );
    try std.testing.expect(root.* == .batch);
    try std.testing.expectEqual(@as(usize, 2), root.batch.statements.len);
    try std.testing.expect(root.batch.statements[0].* == .ddl);
    try std.testing.expect(root.batch.statements[1].* == .insert);
}

test "multi-statement: error in second statement leaves first applied" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    // First statement creates `t`; the INSERT references a non-existent
    // table and must error. The CREATE has already been applied by the
    // time we reach the INSERT, so the catalog reflects it.
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const root = try thindb.sql.parse(
        arena.allocator(),
        "CREATE TABLE t (id BIGINT PRIMARY KEY); INSERT INTO ghost VALUES (1)",
    );
    try std.testing.expect(root.* == .batch);

    // Drain statement 1 — should succeed.
    _ = try drainOne(allocator, db, root.batch.statements[0]);
    try std.testing.expect(db.findTable("t") != null);

    // Drain statement 2 — should error (ghost doesn't exist).
    try std.testing.expectError(
        thindb.net.Error.TableNotFound,
        drainOne(allocator, db, root.batch.statements[1]),
    );
}

test "multi-statement: quoted semicolon is not a separator" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    _ = try drainBatch(allocator, db, "CREATE TABLE t (id BIGINT PRIMARY KEY, tag TEXT NOT NULL); " ++
        "INSERT INTO t VALUES (1, 'a;b;c')");
    const t = try db.openTable("t", .{});
    try t.flush();
    const seen = try drainBatch(allocator, db, "SELECT id FROM t");
    try std.testing.expectEqual(@as(usize, 1), seen);
}

test "multi-statement: compile rejects a batch op directly" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const root = try thindb.sql.parse(
        arena.allocator(),
        "CREATE TABLE t (id BIGINT PRIMARY KEY); INSERT INTO t VALUES (1)",
    );
    try std.testing.expectError(
        thindb.net.Error.UnsupportedOp,
        thindb.net.compile(allocator, db, root),
    );
}
