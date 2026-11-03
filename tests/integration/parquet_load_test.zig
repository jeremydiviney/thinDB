//! Bulk Parquet loads (`bulk_load.loadParquet`) against DuckDB-written
//! fixtures. `parquet/types_*.parquet` hold the same 400 rows under each
//! codec DuckDB writes, one column per conversion the reader makes, and
//! `parquet/types_expected.txt` is DuckDB's own text of every value, so each
//! load is checked cell by cell. `parquet/multi.parquet` holds 9000 rows in
//! five row groups, for segment cuts, lanes, failure cleanup and the SQL
//! path, `INSERT ... SELECT * FROM read_parquet(...)`.

const std = @import("std");
const thindb = @import("thindb");
const helpers = @import("sql_helpers.zig");
const exec = helpers.exec;
const runSql = helpers.runSql;
const bulk_load = thindb.api.bulk_load;

const TYPES_DDL =
    \\CREATE TABLE t (id BIGINT NOT NULL, grp VARCHAR NOT NULL, url VARCHAR, small SMALLINT,
    \\  u INT NOT NULL, ut SMALLINT NOT NULL, d DATE NOT NULL, ts DATETIME, ts_ms DATETIME NOT NULL,
    \\  ts_ns DATETIME NOT NULL, flag BOOLEAN NOT NULL, nflag BOOLEAN, f DOUBLE NOT NULL,
    \\  f32 FLOAT NOT NULL, n INT NOT NULL, secs DATETIME NOT NULL, days DATE NOT NULL, cat VARCHAR)
    \\ORDER BY (grp, id)
;

/// The SELECT expression whose text matches each field of a
/// `types_expected.txt` line, in order.
const TYPES_FIELDS = [_][]const u8{
    "id",                     "grp",                     "url",                "small",
    "u",                      "ut",                      "CAST(d AS VARCHAR)", "CAST(ts AS VARCHAR)",
    "CAST(ts_ms AS VARCHAR)", "CAST(ts_ns AS VARCHAR)",  "flag",               "nflag",
    "CAST(f * 4 AS BIGINT)",  "CAST(f32 * 4 AS BIGINT)", "n",                  "CAST(secs AS VARCHAR)",
    "CAST(days AS VARCHAR)",  "cat",
};

const TYPES_EXPECTED = @embedFile("parquet/types_expected.txt");

const MULTI_DDL = "CREATE TABLE m (id BIGINT NOT NULL, grp VARCHAR NOT NULL, v INT NOT NULL) ORDER BY (grp, id)";
const MULTI = @embedFile("parquet/multi.parquet");

/// A database, and a directory of input files beside it.
const Fixture = struct {
    db_tmp: std.testing.TmpDir,
    files: std.testing.TmpDir,
    db: *thindb.Database,

    fn init(allocator: std.mem.Allocator) !Fixture {
        var db_tmp = std.testing.tmpDir(.{});
        errdefer db_tmp.cleanup();
        var files = std.testing.tmpDir(.{});
        errdefer files.cleanup();
        const db = try thindb.Database.open(allocator, std.testing.io, db_tmp.dir, .{});
        return .{ .db_tmp = db_tmp, .files = files, .db = db };
    }

    fn deinit(self: *Fixture) void {
        self.db.close();
        self.files.cleanup();
        self.db_tmp.cleanup();
    }

    fn put(self: *Fixture, name: []const u8, bytes: []const u8) !void {
        try self.files.dir.writeFile(std.testing.io, .{ .sub_path = name, .data = bytes });
    }

    /// Loads `name` into `table`, whose row groups hold 100 rows, so every
    /// segment has several and the last is short.
    fn load(self: *Fixture, table: []const u8, name: []const u8, options: bulk_load.Options) !bulk_load.Stats {
        const t = try self.db.openTable(table, .{ .row_group_size = 100 });
        return bulk_load.loadParquet(t, self.files.dir, name, options);
    }
};

/// Field `field` of every line of `TYPES_EXPECTED`, `\N` as null.
fn expectedColumn(allocator: std.mem.Allocator, field: usize) ![]?[]const u8 {
    var out: std.ArrayList(?[]const u8) = .empty;
    errdefer out.deinit(allocator);
    var lines = std.mem.splitScalar(u8, std.mem.trimEnd(u8, TYPES_EXPECTED, "\r\n"), '\n');
    while (lines.next()) |raw| {
        const line = std.mem.trimEnd(u8, raw, "\r");
        var fields = std.mem.splitScalar(u8, line, '|');
        var i: usize = 0;
        while (i < field) : (i += 1) _ = fields.next();
        const value = fields.next() orelse return error.TestUnexpectedResult;
        try out.append(allocator, if (std.mem.eql(u8, value, "\\N")) null else value);
    }
    return out.toOwnedSlice(allocator);
}

fn expectTypesTable(allocator: std.mem.Allocator, db: *thindb.Database) !void {
    for (TYPES_FIELDS, 0..) |expr, field| {
        const sql = try std.fmt.allocPrint(allocator, "SELECT {s} FROM t ORDER BY id", .{expr});
        defer allocator.free(sql);
        const want = try expectedColumn(allocator, field);
        defer allocator.free(want);
        try helpers.expectTexts(allocator, db, sql, want);
    }
}

test "bulk load reads every codec and conversion as DuckDB wrote them" {
    const allocator = std.testing.allocator;
    const files = .{
        .{ "types_snappy.parquet", @embedFile("parquet/types_snappy.parquet") },
        .{ "types_zstd.parquet", @embedFile("parquet/types_zstd.parquet") },
        .{ "types_gzip.parquet", @embedFile("parquet/types_gzip.parquet") },
        .{ "types_lz4.parquet", @embedFile("parquet/types_lz4.parquet") },
        .{ "types_plain.parquet", @embedFile("parquet/types_plain.parquet") },
    };
    inline for (files) |file| {
        var fx = try Fixture.init(allocator);
        defer fx.deinit();
        try exec(allocator, fx.db, TYPES_DDL);
        try fx.put(file[0], file[1]);
        const stats = try fx.load("t", file[0], .{});
        try std.testing.expectEqual(@as(u64, 400), stats.rows);
        try std.testing.expectEqual(@as(usize, 1), stats.segments);
        expectTypesTable(allocator, fx.db) catch |err| {
            std.debug.print("fixture {s}\n", .{file[0]});
            return err;
        };
    }
}

/// Each segment's rows in `(grp, id)` order, and the segments' row counts.
fn expectSortedSegments(allocator: std.mem.Allocator, t: *thindb.Table, rows: []const u64) !void {
    const segments = t.manifest.segments.items;
    try std.testing.expectEqual(rows.len, segments.len);
    for (segments, rows) |entry, want| {
        try std.testing.expectEqual(want, entry.row_count);
        var name_buf: [32]u8 = undefined;
        const name = try thindb.Table.segmentFileName(&name_buf, entry.segment_id);
        var seg = try thindb.storage.readSegment(allocator, t.io, t.segments_dir, name, t.schema);
        defer seg.deinit();
        var prev_grp: std.ArrayList(u8) = .empty;
        defer prev_grp.deinit(allocator);
        var prev_id: i64 = -1;
        for (0..seg.info.row_groups.len) |rg| {
            var id_col = try seg.decodeColumn(allocator, t.schema, rg, 0);
            defer id_col.deinit(allocator);
            var grp_col = try seg.decodeColumn(allocator, t.schema, rg, 1);
            defer grp_col.deinit(allocator);
            const ids = id_col.view().data.bigint;
            const grps = switch (grp_col.view().data) {
                .string, .varchar => |sv| sv,
                else => return error.TestUnexpectedType,
            };
            for (ids, 0..) |id, r| {
                const grp = grps.rowBytes(r);
                switch (std.mem.order(u8, prev_grp.items, grp)) {
                    .gt => return error.TestUnsortedSegment,
                    .eq => if (prev_id >= id) return error.TestUnsortedSegment,
                    .lt => {},
                }
                prev_grp.clearRetainingCapacity();
                try prev_grp.appendSlice(allocator, grp);
                prev_id = id;
            }
        }
    }
}

test "bulk load cuts whole row groups into sorted segments and appends them" {
    const allocator = std.testing.allocator;
    var fx = try Fixture.init(allocator);
    defer fx.deinit();
    try exec(allocator, fx.db, MULTI_DDL);
    try exec(allocator, fx.db, "INSERT INTO m (id, grp, v) VALUES (-1, 'zz', -1)");
    try fx.put("multi.parquet", MULTI);

    // 9000 rows in row groups of 2048 at 4096 rows a segment: cuts after the
    // second and third row groups, one lane at a time on four workers.
    const stats = try fx.load("m", "multi.parquet", .{ .segment_rows = 4096, .lanes = 1, .threads = 4 });
    try std.testing.expectEqual(@as(u64, 9000), stats.rows);
    try std.testing.expectEqual(@as(usize, 3), stats.segments);
    const t = try fx.db.openTable("m", .{});
    try expectSortedSegments(allocator, t, &.{ 4096, 2048, 2856 });

    try helpers.expectText(allocator, fx.db, "SELECT COUNT(*) FROM m", "9001");
    try helpers.expectText(allocator, fx.db, "SELECT SUM(id) FROM m WHERE id >= 0", "40495500");
    try helpers.expectText(allocator, fx.db, "SELECT SUM(v) FROM m WHERE v >= 0", "40495500");
    try helpers.expectText(allocator, fx.db, "SELECT COUNT(DISTINCT id) FROM m", "9001");
    try helpers.expectText(allocator, fx.db, "SELECT COUNT(*) FROM m WHERE grp = 'g5'", "392");
    try helpers.expectText(allocator, fx.db, "SELECT COUNT(*) FROM m WHERE grp = 'g7'", "391");
    try helpers.expectText(allocator, fx.db, "SELECT v FROM m WHERE id = 7919", "1");

    // A second load appends after the first, two lanes on every CPU.
    const again = try fx.load("m", "multi.parquet", .{ .segment_rows = 2048 });
    try std.testing.expectEqual(@as(usize, 5), again.segments);
    try std.testing.expectEqual(@as(usize, 8), t.manifest.segments.items.len);
    try helpers.expectText(allocator, fx.db, "SELECT COUNT(*) FROM m", "18001");
    try helpers.expectText(allocator, fx.db, "SELECT COUNT(DISTINCT id) FROM m", "9001");

    // A file without rows adds nothing.
    try fx.put("empty.parquet", @embedFile("parquet/empty.parquet"));
    const empty = try fx.load("m", "empty.parquet", .{});
    try std.testing.expectEqual(@as(u64, 0), empty.rows);
    try std.testing.expectEqual(@as(usize, 0), empty.segments);
    try std.testing.expectEqual(@as(usize, 8), t.manifest.segments.items.len);
}

test "INSERT ... SELECT * from a Parquet file bulk-loads, pairing columns by position" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(io, .{ .sub_path = "multi.parquet", .data = MULTI });
    const root = try std.fmt.allocPrint(allocator, ".zig-cache/tmp/{s}", .{tmp.sub_path});
    defer allocator.free(root);
    const db = try thindb.Database.open(allocator, io, tmp.dir, .{ .file_scan_access = .{ .root = root } });
    defer db.close();

    // The file's columns are id, grp, v and w: other names, same positions.
    try exec(allocator, db, "CREATE TABLE p (a BIGINT NOT NULL, b VARCHAR NOT NULL, c INT NOT NULL, d INT NOT NULL) ORDER BY (b, a)");
    var q = try runSql(allocator, db, "INSERT INTO p SELECT * FROM read_parquet('multi.parquet')");
    defer q.deinit();
    while (try q.next()) |_| {}
    try std.testing.expectEqual(@as(u64, 9000), q.affectedRows());
    const t = try db.openTable("p", .{});
    try std.testing.expectEqual(@as(usize, 1), t.manifest.segments.items.len);
    try helpers.expectText(allocator, db, "SELECT SUM(c) FROM p", "40495500");
    try helpers.expectText(allocator, db, "SELECT SUM(d) FROM p", "1159696");
    try helpers.expectText(allocator, db, "SELECT COUNT(*) FROM p WHERE b = 'g5'", "392");

    try exec(allocator, db, "INSERT INTO p SELECT * FROM 'multi.parquet'");
    try helpers.expectText(allocator, db, "SELECT COUNT(*) FROM p", "18000");

    // Anything past a plain SELECT * would need a Parquet scan.
    try helpers.expectRunError(allocator, db, "INSERT INTO p SELECT * FROM read_parquet('multi.parquet') WHERE id > 5", error.FileScanUnsupportedParquet);
    try helpers.expectRunError(allocator, db, "INSERT INTO p SELECT * FROM read_parquet('../multi.parquet')", error.FileScanPathOutsideRoot);
    try exec(allocator, db, MULTI_DDL);
    try helpers.expectRunError(allocator, db, "INSERT INTO m SELECT * FROM read_parquet('multi.parquet')", error.ParquetColumnMissing);
}

fn segmentFileCount(t: *thindb.Table) !usize {
    var dir = try t.segments_dir.openDir(t.io, ".", .{ .iterate = true });
    defer dir.close(t.io);
    var count: usize = 0;
    var it = dir.iterate();
    while (try it.next(t.io)) |entry| {
        if (std.mem.endsWith(u8, entry.name, ".dat")) count += 1;
    }
    return count;
}

test "a failed bulk load leaves the table as it was" {
    const allocator = std.testing.allocator;
    var fx = try Fixture.init(allocator);
    defer fx.deinit();
    try fx.put("multi.parquet", MULTI);
    try fx.put("types.parquet", @embedFile("parquet/types_snappy.parquet"));
    try fx.put("text.parquet", "not a parquet file at all");

    // `w` passes TINYINT until row 6144, in the fourth row group: the first
    // three segments are written by then, and must go.
    try exec(allocator, fx.db, "CREATE TABLE w (id BIGINT NOT NULL, w TINYINT NOT NULL) ORDER BY (id)");
    try std.testing.expectError(error.ParquetValueOutOfRange, fx.load("w", "multi.parquet", .{ .segment_rows = 2048, .lanes = 1, .threads = 1 }));
    const w = try fx.db.openTable("w", .{});
    try std.testing.expectEqual(@as(usize, 0), w.manifest.segments.items.len);
    try std.testing.expectEqual(@as(usize, 0), try segmentFileCount(w));
    try helpers.expectText(allocator, fx.db, "SELECT COUNT(*) FROM w", "0");

    try exec(allocator, fx.db, "CREATE TABLE k (id BIGINT NOT NULL, v INT NOT NULL, PRIMARY KEY (id))");
    try std.testing.expectError(error.BulkLoadUniqueTable, fx.load("k", "multi.parquet", .{}));
    try exec(allocator, fx.db, "CREATE TABLE x (id BIGINT NOT NULL, missing INT) ORDER BY (id)");
    try std.testing.expectError(error.ParquetColumnMissing, fx.load("x", "multi.parquet", .{}));
    try std.testing.expectError(error.NotParquet, fx.load("x", "text.parquet", .{}));
    try exec(allocator, fx.db, "CREATE TABLE dec (id BIGINT NOT NULL, dec DOUBLE) ORDER BY (id)");
    try std.testing.expectError(error.UnsupportedParquet, fx.load("dec", "types.parquet", .{}));
    try exec(allocator, fx.db, "CREATE TABLE nn (id BIGINT NOT NULL, url VARCHAR NOT NULL) ORDER BY (id)");
    try std.testing.expectError(error.ParquetValueOutOfRange, fx.load("nn", "types.parquet", .{}));
}
