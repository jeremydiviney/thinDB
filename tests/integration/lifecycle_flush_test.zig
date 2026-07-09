//! Lifecycle durability regressions:
//!  - #142: a WAL-less table's memtable residue must be flushed on a clean
//!    close (Database.close), not silently dropped.
//!  - #137: segment files no manifest entry references (crash mid-compaction,
//!    or a pending delete that never drained) are reclaimed at table open.

const std = @import("std");
const thindb = @import("thindb");
const helpers = @import("sql_helpers.zig");
const exec = helpers.exec;
const runSql = helpers.runSql;

fn countRows(allocator: std.mem.Allocator, db: *thindb.Database) !i64 {
    const vals = try helpers.collectBigints(allocator, db, "SELECT COUNT(*) FROM t");
    defer allocator.free(vals);
    if (vals.len != 1) return error.NoRow;
    return vals[0];
}

test "close without explicit flush persists memtable residue (#142)" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    // Thresholds far above the row count and no time trigger: nothing but
    // the shutdown flush can persist these rows (WAL disabled by default).
    const cfg: thindb.Config = .{
        .auto_flush_rows = 1_000_000,
        .auto_flush_bytes = 64 * 1024 * 1024,
        .auto_flush_secs = 0,
    };

    {
        var db = try thindb.Database.open(allocator, io, tmp.dir, cfg);
        defer db.close();
        try exec(allocator, db, "CREATE TABLE t (id INT NOT NULL, v INT NOT NULL, PRIMARY KEY (id))");
        try exec(allocator, db, "INSERT INTO t (id, v) VALUES (1,10),(2,20),(3,30)");
    }

    var db = try thindb.Database.open(allocator, io, tmp.dir, cfg);
    defer db.close();
    try std.testing.expectEqual(@as(i64, 3), try countRows(allocator, db));
}

test "orphaned segment files are reclaimed at table open (#137)" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    {
        var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
        defer db.close();
        try exec(allocator, db, "CREATE TABLE t (id INT NOT NULL, v INT NOT NULL, PRIMARY KEY (id))");
        try exec(allocator, db, "INSERT INTO t (id, v) VALUES (1,10),(2,20)");
        const tbl = try db.openTable("t", .{});
        try tbl.flush();
    }

    // Plant unreferenced files with valid segment-file names.
    var seg_dir = try tmp.dir.openDir(io, "main/public/t/segments", .{});
    defer seg_dir.close(io);
    try seg_dir.writeFile(io, .{ .sub_path = "00000000000000099999.dat", .data = "garbage" });
    try seg_dir.writeFile(io, .{ .sub_path = "00000000000000099999.tomb", .data = "garbage" });

    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    _ = try db.openTable("t", .{}); // open runs the orphan sweep

    try std.testing.expectError(error.FileNotFound, seg_dir.access(io, "00000000000000099999.dat", .{ .read = true }));
    try std.testing.expectError(error.FileNotFound, seg_dir.access(io, "00000000000000099999.tomb", .{ .read = true }));
    // The real segment is untouched and the data still reads back.
    try std.testing.expectEqual(@as(i64, 2), try countRows(allocator, db));
}
