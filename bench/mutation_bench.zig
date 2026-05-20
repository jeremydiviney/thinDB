//! DELETE / UPDATE benches.
//!
//! Each bench seeds a fresh table with N rows on disk (flushed), then
//! times the SQL mutation statement end-to-end (parse + compile +
//! execute). Selectivity varies — narrow predicates exercise the
//! per-segment tombstone-prune fast path; wide predicates exercise
//! the per-row evaluator.

const std = @import("std");
const thindb = @import("thindb");
const common = @import("common.zig");

const Allocator = common.Allocator;
const Io = common.Io;
const schema = common.schema;
const options = common.options;
const buildRows = common.buildRows;
const elapsedNs = common.elapsedNs;
const freshDir = common.freshDir;
const report = common.report;

/// Public entry point — called from `bench/main.zig`.
pub fn run(allocator: Allocator, io: Io, n_rows: usize) !void {
    try benchDeleteAll(allocator, io, n_rows);
    try benchDeleteNarrow(allocator, io, n_rows);
    try benchDeleteWide(allocator, io, n_rows);
    try benchUpdateAllLiteral(allocator, io, n_rows);
    try benchUpdateNarrowLiteral(allocator, io, n_rows);
    try benchUpdateSelfRef(allocator, io, n_rows);
    try benchUpdateMultiCol(allocator, io, n_rows);
}

// =============================================================================
// DELETE benches
// =============================================================================

fn benchDeleteAll(allocator: Allocator, io: Io, n_rows: usize) !void {
    var dir = try freshDir(io, ".bench-data/delete_all");
    defer dir.close(io);
    var db = try thindb.Database.open(allocator, io, dir, .{});
    defer db.close();
    try seedTable(allocator, db, n_rows);

    const elapsed = try runSqlTimed(allocator, io, db, "DELETE FROM t");
    try report("delete: no WHERE", n_rows, elapsed, null);
}

fn benchDeleteNarrow(allocator: Allocator, io: Io, n_rows: usize) !void {
    var dir = try freshDir(io, ".bench-data/delete_narrow");
    defer dir.close(io);
    var db = try thindb.Database.open(allocator, io, dir, .{});
    defer db.close();
    try seedTable(allocator, db, n_rows);

    // qty cycles 0..99, so qty < 5 matches ~5% of rows.
    const elapsed = try runSqlTimed(allocator, io, db, "DELETE FROM t WHERE qty < 5");
    try report("delete: narrow (~5%)", n_rows, elapsed, null);
}

fn benchDeleteWide(allocator: Allocator, io: Io, n_rows: usize) !void {
    var dir = try freshDir(io, ".bench-data/delete_wide");
    defer dir.close(io);
    var db = try thindb.Database.open(allocator, io, dir, .{});
    defer db.close();
    try seedTable(allocator, db, n_rows);

    // qty < 50 matches ~50% of rows.
    const elapsed = try runSqlTimed(allocator, io, db, "DELETE FROM t WHERE qty < 50");
    try report("delete: wide (~50%)", n_rows, elapsed, null);
}

// =============================================================================
// UPDATE benches
// =============================================================================

fn benchUpdateAllLiteral(allocator: Allocator, io: Io, n_rows: usize) !void {
    var dir = try freshDir(io, ".bench-data/update_all_literal");
    defer dir.close(io);
    var db = try thindb.Database.open(allocator, io, dir, .{});
    defer db.close();
    try seedTable(allocator, db, n_rows);

    const elapsed = try runSqlTimed(allocator, io, db, "UPDATE t SET qty = 0");
    try report("update: no WHERE, literal", n_rows, elapsed, null);
}

fn benchUpdateNarrowLiteral(allocator: Allocator, io: Io, n_rows: usize) !void {
    var dir = try freshDir(io, ".bench-data/update_narrow_literal");
    defer dir.close(io);
    var db = try thindb.Database.open(allocator, io, dir, .{});
    defer db.close();
    try seedTable(allocator, db, n_rows);

    const elapsed = try runSqlTimed(allocator, io, db, "UPDATE t SET qty = 0 WHERE qty < 5");
    try report("update: narrow (~5%), literal", n_rows, elapsed, null);
}

fn benchUpdateSelfRef(allocator: Allocator, io: Io, n_rows: usize) !void {
    var dir = try freshDir(io, ".bench-data/update_self_ref");
    defer dir.close(io);
    var db = try thindb.Database.open(allocator, io, dir, .{});
    defer db.close();
    try seedTable(allocator, db, n_rows);

    // Self-ref RHS exercises the Compute step (qty = qty + 1).
    const elapsed = try runSqlTimed(allocator, io, db, "UPDATE t SET qty = qty + 1");
    try report("update: no WHERE, self-ref", n_rows, elapsed, null);
}

fn benchUpdateMultiCol(allocator: Allocator, io: Io, n_rows: usize) !void {
    var dir = try freshDir(io, ".bench-data/update_multi_col");
    defer dir.close(io);
    var db = try thindb.Database.open(allocator, io, dir, .{});
    defer db.close();
    try seedTable(allocator, db, n_rows);

    // Two assignments — exercises the multi-Derived Compute path.
    const elapsed = try runSqlTimed(allocator, io, db, "UPDATE t SET qty = 0, active = TRUE WHERE qty < 50");
    try report("update: wide (~50%), 2 cols", n_rows, elapsed, null);
}

// =============================================================================
// Helpers
// =============================================================================

fn seedTable(allocator: Allocator, db: *thindb.Database, n_rows: usize) !void {
    const t = try db.table("t", schema, options);
    const rows = try buildRows(allocator, n_rows);
    defer allocator.free(rows);
    try t.insert(rows);
    try t.flush();
}

/// Parse + compile + drain `sql` against `db`, returning the wall-
/// clock duration (parse cost folded in — represents the user-
/// observed latency of a one-shot SQL call).
fn runSqlTimed(allocator: Allocator, io: Io, db: *thindb.Database, sql: []const u8) !u64 {
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const root = try thindb.sql.parse(arena.allocator(), sql);

    const t0 = Io.Clock.awake.now(io);
    var cq = try thindb.net.compile(allocator, db, root);
    defer cq.deinit();
    while (try cq.next()) |_| {}
    return elapsedNs(io, t0);
}
