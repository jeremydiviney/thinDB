//! #143 — full-key Bloom pruning on keyed access paths. The fixture makes
//! three flushed segments that FULLY OVERLAP in key range (interleaved keys),
//! the shape zonemaps cannot prune, so these queries exercise the Bloom pass.
//! Assertions are correctness-first: a Bloom false-negative (wrong prune)
//! would silently drop matching rows, which is exactly what these catch.

const std = @import("std");
const thindb = @import("thindb");
const helpers = @import("sql_helpers.zig");
const exec = helpers.exec;

fn collectIds(allocator: std.mem.Allocator, db: *thindb.Database, sql: []const u8) ![]i64 {
    const vals = try helpers.collectBigints(allocator, db, sql);
    std.mem.sort(i64, vals, {}, std.sort.asc(i64));
    return vals;
}

test "full-key bloom pruning: point, IN, absent, conjunct, keyed DELETE/UPDATE" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    try exec(allocator, db, "CREATE TABLE t (id BIGINT NOT NULL, grp INT NOT NULL, PRIMARY KEY (id))");
    const tbl = try db.openTable("t", .{});

    // Segment s holds ids ≡ s (mod 3); every segment spans the whole range.
    var seg: i64 = 0;
    while (seg < 3) : (seg += 1) {
        var sql: std.ArrayList(u8) = .empty;
        defer sql.deinit(allocator);
        try sql.appendSlice(allocator, "INSERT INTO t (id, grp) VALUES ");
        var k: i64 = 0;
        while (k < 200) : (k += 1) {
            const id = k * 3 + seg;
            if (k > 0) try sql.append(allocator, ',');
            try sql.print(allocator, "({d},{d})", .{ id, seg });
        }
        try exec(allocator, db, sql.items);
        try tbl.flush();
    }
    try std.testing.expectEqual(@as(usize, 3), tbl.manifest.segments.items.len);
    for (tbl.manifest.segments.items) |e| try std.testing.expect(e.key_bloom.len > 0);

    // Point lookup (segment 0 only).
    {
        const ids = try collectIds(allocator, db, "SELECT id FROM t WHERE id = 15");
        defer allocator.free(ids);
        try std.testing.expectEqualSlices(i64, &.{15}, ids);
    }
    // IN across two segments.
    {
        const ids = try collectIds(allocator, db, "SELECT id FROM t WHERE id IN (15, 16)");
        defer allocator.free(ids);
        try std.testing.expectEqualSlices(i64, &.{ 15, 16 }, ids);
    }
    // Absent key: every segment bloom-rejected.
    {
        const ids = try collectIds(allocator, db, "SELECT id FROM t WHERE id = 999999");
        defer allocator.free(ids);
        try std.testing.expectEqual(@as(usize, 0), ids.len);
    }
    // Extra non-key conjunct must not break the pass (grp=0 holds for id=21).
    {
        const ids = try collectIds(allocator, db, "SELECT id FROM t WHERE id = 21 AND grp = 0");
        defer allocator.free(ids);
        try std.testing.expectEqualSlices(i64, &.{21}, ids);
    }
    // Contradicting non-key conjunct still returns nothing (row-level eval).
    {
        const ids = try collectIds(allocator, db, "SELECT id FROM t WHERE id = 21 AND grp = 2");
        defer allocator.free(ids);
        try std.testing.expectEqual(@as(usize, 0), ids.len);
    }

    // Keyed UPDATE through the bloom gate: only id=33 changes.
    try exec(allocator, db, "UPDATE t SET grp = 77 WHERE id = 33");
    {
        const ids = try collectIds(allocator, db, "SELECT id FROM t WHERE grp = 77");
        defer allocator.free(ids);
        try std.testing.expectEqualSlices(i64, &.{33}, ids);
    }

    // Keyed DELETE through the bloom gate: id=42 gone, neighbors intact.
    try exec(allocator, db, "DELETE FROM t WHERE id = 42");
    {
        const ids = try collectIds(allocator, db, "SELECT id FROM t WHERE id IN (41, 42, 43)");
        defer allocator.free(ids);
        try std.testing.expectEqualSlices(i64, &.{ 41, 43 }, ids);
    }
    // Total row count reflects exactly one delete.
    {
        const n = try collectIds(allocator, db, "SELECT COUNT(*) FROM t");
        defer allocator.free(n);
        try std.testing.expectEqual(@as(i64, 599), n[0]);
    }
}

test "full-key bloom pruning: compound key equality" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    try exec(allocator, db, "CREATE TABLE c (a INT NOT NULL, b STRING NOT NULL, v BIGINT NOT NULL, PRIMARY KEY (a, b))");
    const tbl = try db.openTable("c", .{});

    // Two overlapping segments: both cover a ∈ {1,2}, differing in b suffix.
    try exec(allocator, db, "INSERT INTO c (a,b,v) VALUES (1,'x-1',10),(2,'x-2',20),(1,'x-3',30)");
    try tbl.flush();
    try exec(allocator, db, "INSERT INTO c (a,b,v) VALUES (1,'y-1',11),(2,'y-2',21),(2,'y-3',31)");
    try tbl.flush();

    const vals = try helpers.collectBigints(allocator, db, "SELECT v FROM c WHERE a = 2 AND b = 'y-2'");
    defer allocator.free(vals);
    try std.testing.expectEqualSlices(i64, &.{21}, vals);

    // Absent compound key (a exists, b doesn't pair with it).
    const none = try helpers.collectBigints(allocator, db, "SELECT v FROM c WHERE a = 1 AND b = 'y-2'");
    defer allocator.free(none);
    try std.testing.expectEqual(@as(usize, 0), none.len);
}
