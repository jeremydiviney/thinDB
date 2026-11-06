//! Table string ids (DESIGN.md §4.6): every segment of a table maps a
//! string to one table id, whichever flush, reopen, compaction, ALTER or
//! parallel bulk load wrote it, and a column whose list passes its cap
//! stops carrying ids.

const std = @import("std");
const thindb = @import("thindb");
const helpers = @import("sql_helpers.zig");
const exec = helpers.exec;
const bulk_load = thindb.api.bulk_load;
const segment_reader = thindb.storage.segment_reader;

const io = std.testing.io;

const config: thindb.Config = .{
    .compact_min_segments = 100,
    .auto_flush_secs = 0,
    .auto_flush_rows = 1_000_000,
    .auto_flush_bytes = 64 * 1024 * 1024,
};

const tag_column = 1;

/// The table id each value has, gathered over a table's segments.
const Ids = struct {
    arena: std.heap.ArenaAllocator,
    by_value: std.StringHashMapUnmanaged(u32) = .empty,
    by_id: std.AutoHashMapUnmanaged(u32, []const u8) = .empty,
    /// Segments that stored no ids for the column.
    without: usize = 0,

    fn init(allocator: std.mem.Allocator) Ids {
        return .{ .arena = .init(allocator) };
    }

    fn deinit(self: *Ids) void {
        self.arena.deinit();
    }

    fn note(self: *Ids, value: []const u8, id: u32) !void {
        const a = self.arena.allocator();
        if (self.by_value.get(value)) |had| return std.testing.expectEqual(had, id);
        if (self.by_id.get(id)) |other| {
            std.debug.print("ids: {s} and {s} share id {d}\n", .{ value, other, id });
            return error.TestSharedId;
        }
        const owned = try a.dupe(u8, value);
        try self.by_value.put(a, owned, id);
        try self.by_id.put(a, id, owned);
    }

    fn expectSame(self: *const Ids, other: *const Ids) !void {
        try std.testing.expectEqual(self.by_value.count(), other.by_value.count());
        var it = self.by_value.iterator();
        while (it.next()) |e| try std.testing.expectEqual(e.value_ptr.*, other.by_value.get(e.key_ptr.*).?);
    }
};

/// The table ids of column `column_idx` (NOT NULL, so a block is its
/// dictionary) across every segment of `t`, each value checked to have one.
fn gatherIds(allocator: std.mem.Allocator, t: *thindb.Table, column_idx: usize) !Ids {
    var ids = Ids.init(allocator);
    errdefer ids.deinit();
    for (t.manifest.segments.items) |entry| {
        var name_buf: [32]u8 = undefined;
        const name = try thindb.Table.segmentFileName(&name_buf, entry.segment_id);
        var seg = try thindb.storage.readSegment(allocator, t.io, t.segments_dir, name, t.schema);
        defer seg.deinit();
        const table_ids = (try seg.tableIds(allocator, column_idx)) orelse {
            ids.without += 1;
            continue;
        };
        defer allocator.free(table_ids);
        for (seg.info.row_groups, 0..) |rg, rg_idx| {
            var block = try seg.borrowColumnBlock(allocator, rg_idx, column_idx, null);
            defer block.release(allocator, null);
            try std.testing.expectEqual(thindb.storage.format.Encoding.dict, block.encoding);
            const dict = segment_reader.dictBlockOf(block.bytes, rg.row_count);
            try std.testing.expect(dict.hasSegmentIds());
            for (0..dict.ndv) |code| try ids.note(dict.dictValue(@intCast(code)), table_ids[dict.segmentId(@intCast(code))]);
        }
    }
    return ids;
}

/// Inserts `rows` rows from id `first_id`, their tags cycling over
/// `tag<lo>` .. `tag<lo + tags - 1>`.
fn insertTags(allocator: std.mem.Allocator, db: *thindb.Database, first_id: usize, rows: usize, lo: usize, tags: usize) !void {
    var sql: std.ArrayList(u8) = .empty;
    defer sql.deinit(allocator);
    try sql.appendSlice(allocator, "INSERT INTO t (id, tag) VALUES ");
    for (0..rows) |i| {
        if (i != 0) try sql.appendSlice(allocator, ", ");
        try sql.print(allocator, "({d}, 'tag{d}')", .{ first_id + i, lo + i % tags });
    }
    try exec(allocator, db, sql.items);
}

/// Every id below `count` taken, each by one value.
fn expectDense(ids: *const Ids, count: u32) !void {
    try std.testing.expectEqual(@as(usize, count), ids.by_value.count());
    for (0..count) |id| try std.testing.expect(ids.by_id.contains(@intCast(id)));
}

test "table string ids: one id per value across flushes, reopens, compaction and ALTER" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    {
        const db = try thindb.Database.open(allocator, io, tmp.dir, config);
        defer db.close();
        try exec(allocator, db, "CREATE TABLE t (id BIGINT NOT NULL, tag VARCHAR NOT NULL, PRIMARY KEY (id))");
        const t = try db.openTable("t", .{});
        try insertTags(allocator, db, 0, 400, 0, 8);
        try t.flush();
        try insertTags(allocator, db, 400, 400, 4, 8);
        try t.flush();
        var ids = try gatherIds(allocator, t, tag_column);
        defer ids.deinit();
        try std.testing.expectEqual(@as(usize, 0), ids.without);
        try expectDense(&ids, 12);
    }

    const db = try thindb.Database.open(allocator, io, tmp.dir, config);
    defer db.close();
    const t = try db.openTable("t", .{});
    try insertTags(allocator, db, 800, 400, 8, 8);
    try t.flush();
    var before = try gatherIds(allocator, t, tag_column);
    defer before.deinit();
    try std.testing.expectEqual(@as(usize, 3), t.manifest.segments.items.len);
    try std.testing.expectEqual(@as(usize, 0), before.without);
    try expectDense(&before, 16);

    try t.compact();
    try std.testing.expectEqual(@as(usize, 1), t.manifest.segments.items.len);
    var compacted = try gatherIds(allocator, t, tag_column);
    defer compacted.deinit();
    try std.testing.expectEqual(@as(usize, 0), compacted.without);
    try before.expectSame(&compacted);

    try exec(allocator, db, "ALTER TABLE t ADD COLUMN n INT");
    const altered = try db.openTable("t", .{});
    var after_alter = try gatherIds(allocator, altered, tag_column);
    defer after_alter.deinit();
    try std.testing.expectEqual(@as(usize, 0), after_alter.without);
    try expectDense(&after_alter, 16);
    try helpers.expectText(allocator, db, "SELECT COUNT(DISTINCT tag) FROM t", "16");
}

test "table string ids: a column past its cap stops carrying ids" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    {
        const db = try thindb.Database.open(allocator, io, tmp.dir, config);
        defer db.close();
        try exec(allocator, db, "CREATE TABLE t (id BIGINT NOT NULL, tag VARCHAR NOT NULL, PRIMARY KEY (id))");
        const t = try db.openTable("t", .{});
        t.strings.cap = 12;
        try insertTags(allocator, db, 0, 400, 0, 8);
        try t.flush();
        try insertTags(allocator, db, 400, 400, 8, 8);
        try t.flush();
        var ids = try gatherIds(allocator, t, tag_column);
        defer ids.deinit();
        try std.testing.expectEqual(@as(usize, 1), ids.without);
        try expectDense(&ids, 8);
    }

    const db = try thindb.Database.open(allocator, io, tmp.dir, config);
    defer db.close();
    const t = try db.openTable("t", .{});
    try insertTags(allocator, db, 800, 400, 0, 4);
    try t.flush();
    var ids = try gatherIds(allocator, t, tag_column);
    defer ids.deinit();
    try std.testing.expectEqual(@as(usize, 2), ids.without);
    try helpers.expectText(allocator, db, "SELECT COUNT(DISTINCT tag) FROM t", "16");
}

test "table string ids: parallel bulk-load lanes share one id per value" {
    const allocator = std.testing.allocator;
    var db_tmp = std.testing.tmpDir(.{});
    defer db_tmp.cleanup();
    var files = std.testing.tmpDir(.{});
    defer files.cleanup();
    const db = try thindb.Database.open(allocator, io, db_tmp.dir, .{});
    defer db.close();
    try exec(allocator, db, "CREATE TABLE m (id BIGINT NOT NULL, grp VARCHAR NOT NULL, v INT NOT NULL) ORDER BY (grp, id)");
    try files.dir.writeFile(io, .{ .sub_path = "multi.parquet", .data = @embedFile("parquet/multi.parquet") });

    const t = try db.openTable("m", .{ .row_group_size = 100 });
    const stats = try bulk_load.loadParquet(t, files.dir, "multi.parquet", .{ .segment_rows = 1024, .threads = 4 });
    try std.testing.expect(stats.segments > 1);
    var ids = try gatherIds(allocator, t, 1);
    defer ids.deinit();
    try std.testing.expectEqual(@as(usize, 0), ids.without);
    const distinct = ids.by_value.count();
    try std.testing.expect(distinct > 1);
    try expectDense(&ids, @intCast(distinct));
}

/// `COUNT(DISTINCT tag)` of `t` by table ids, or null when they can't
/// answer it.
fn distinctByIds(allocator: std.mem.Allocator, t: *thindb.Table) !?u64 {
    const snap = try thindb.exec.Scan.captureSnapshotAlloc(t, allocator, null);
    defer snap.release(t);
    defer allocator.free(snap.segments);
    return thindb.exec.table_id_distinct.count(allocator, .{ .table = t, .snapshot = snap, .column_idx = tag_column, .workers = 4 });
}

test "table string ids: COUNT(DISTINCT) by ids matches the scan, and declines memtable rows and tombstones" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const db = try thindb.Database.open(allocator, io, tmp.dir, config);
    defer db.close();
    try exec(allocator, db, "CREATE TABLE t (id BIGINT NOT NULL, tag VARCHAR, PRIMARY KEY (id))");
    const t = try db.openTable("t", .{ .row_group_size = 100 });

    // Every fifth tag NULL, then a row group of NULLs alone, which stands
    // apart from the string table.
    var sql: std.ArrayList(u8) = .empty;
    defer sql.deinit(allocator);
    try sql.appendSlice(allocator, "INSERT INTO t (id, tag) VALUES ");
    for (0..500) |i| {
        if (i != 0) try sql.appendSlice(allocator, ", ");
        if (i % 5 == 0 or i >= 400) {
            try sql.print(allocator, "({d}, NULL)", .{i});
        } else {
            try sql.print(allocator, "({d}, 'tag{d}')", .{ i, i % 10 });
        }
    }
    try exec(allocator, db, sql.items);
    try t.flush();
    try insertTags(allocator, db, 500, 400, 5, 10);
    try t.flush();
    try std.testing.expectEqual(@as(?u64, 14), try distinctByIds(allocator, t));
    try helpers.expectText(allocator, db, "SELECT COUNT(DISTINCT tag) FROM t", "14");

    try exec(allocator, db, "INSERT INTO t (id, tag) VALUES (1000, 'tag99')");
    try std.testing.expectEqual(@as(?u64, null), try distinctByIds(allocator, t));
    try helpers.expectText(allocator, db, "SELECT COUNT(DISTINCT tag) FROM t", "15");
    try t.flush();
    try std.testing.expectEqual(@as(?u64, 15), try distinctByIds(allocator, t));
    try helpers.expectText(allocator, db, "SELECT COUNT(DISTINCT tag) FROM t", "15");

    try exec(allocator, db, "DELETE FROM t WHERE id = 1000");
    try std.testing.expectEqual(@as(?u64, null), try distinctByIds(allocator, t));
    try helpers.expectText(allocator, db, "SELECT COUNT(DISTINCT tag) FROM t", "14");
}

test "table string ids: a filtered COUNT(DISTINCT) counts by ids, beside memtable rows and tombstones" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const db = try thindb.Database.open(allocator, io, tmp.dir, config);
    defer db.close();
    try exec(allocator, db, "CREATE TABLE t (id BIGINT NOT NULL, tag VARCHAR NOT NULL, PRIMARY KEY (id))");
    const t = try db.openTable("t", .{ .row_group_size = 100 });
    try insertTags(allocator, db, 0, 400, 0, 10);
    try t.flush();
    try insertTags(allocator, db, 400, 400, 6, 10);
    try t.flush();

    const cases = .{
        .{ .sql = "SELECT COUNT(DISTINCT tag) FROM t WHERE id < 5", .expected = "5" },
        .{ .sql = "SELECT COUNT(DISTINCT tag) FROM t WHERE id >= 395 AND id < 405", .expected = "6" },
        .{ .sql = "SELECT COUNT(DISTINCT tag) FROM t WHERE id >= 400", .expected = "10" },
        .{ .sql = "SELECT COUNT(DISTINCT tag) FROM t WHERE tag <> 'tag3'", .expected = "15" },
        .{ .sql = "SELECT COUNT(DISTINCT tag) FROM t WHERE id % 2 = 0", .expected = "8" },
        .{ .sql = "SELECT COUNT(DISTINCT tag) FROM t WHERE id > 5000", .expected = "0" },
    };
    inline for (cases) |c| try helpers.expectText(allocator, db, c.sql, c.expected);

    try exec(allocator, db, "INSERT INTO t (id, tag) VALUES (1000, 'tag99')");
    try helpers.expectText(allocator, db, "SELECT COUNT(DISTINCT tag) FROM t WHERE id >= 400", "11");
    try t.flush();
    try helpers.expectText(allocator, db, "SELECT COUNT(DISTINCT tag) FROM t WHERE id >= 400", "11");
    try exec(allocator, db, "DELETE FROM t WHERE id = 1000");
    try helpers.expectText(allocator, db, "SELECT COUNT(DISTINCT tag) FROM t WHERE id >= 400", "10");
}
