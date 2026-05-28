const std = @import("std");
const thindb = @import("thindb");
const helpers = @import("sql_helpers.zig");

fn fileRootPath(allocator: std.mem.Allocator, tmp: std.testing.TmpDir) ![]u8 {
    return std.fmt.allocPrint(allocator, ".zig-cache/tmp/{s}", .{tmp.sub_path});
}

test "SQL file scan: read_csv supports projection, filter, and count" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(io, .{
        .sub_path = "people.csv",
        .data =
        \\id,name,score
        \\1,"alice, a",10
        \\2,bob,20
        \\3,carol,
        \\
        ,
    });
    const root = try fileRootPath(allocator, tmp);
    defer allocator.free(root);
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{ .file_scan_access = .{ .root = root } });
    defer db.close();

    var q = try helpers.runSql(allocator, db, "SELECT id, name FROM read_csv('people.csv', header=true) WHERE score >= 20 ORDER BY id ASC");
    defer q.deinit();
    const batch = (try q.next()).?;
    try std.testing.expectEqual(@as(usize, 1), batch.row_count);
    try std.testing.expectEqual(@as(i64, 2), batch.values[0].data.bigint[0]);
    try std.testing.expectEqualStrings("bob", batch.values[1].data.string.rowBytes(0));

    const counts = try helpers.collectBigints(allocator, db, "SELECT COUNT(*) FROM read_csv('people.csv', header=true)");
    defer allocator.free(counts);
    try std.testing.expectEqualSlices(i64, &.{3}, counts);

    var quoted = try helpers.runSql(allocator, db, "SELECT name FROM read_csv('people.csv', header=true) WHERE id = 1");
    defer quoted.deinit();
    const quoted_batch = (try quoted.next()).?;
    try std.testing.expectEqualStrings("alice, a", quoted_batch.values[0].data.string.rowBytes(0));
}

test "SQL file scan: read_json handles missing keys and nested values" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(io, .{
        .sub_path = "events.ndjson",
        .data =
        \\{"id":1,"name":"alpha","meta":{"source":"a"}}
        \\{"id":2,"name":"beta"}
        \\
        ,
    });
    const root = try fileRootPath(allocator, tmp);
    defer allocator.free(root);
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{ .file_scan_access = .{ .root = root } });
    defer db.close();

    var q = try helpers.runSql(allocator, db, "SELECT id, name, meta FROM read_json('events.ndjson') AS e ORDER BY id ASC");
    defer q.deinit();
    const batch = (try q.next()).?;
    try std.testing.expectEqual(@as(usize, 2), batch.row_count);
    try std.testing.expectEqual(@as(i64, 1), batch.values[0].data.bigint[0]);
    try std.testing.expectEqualStrings("alpha", batch.values[1].data.string.rowBytes(0));
    try std.testing.expectEqualStrings("{\"source\":\"a\"}", batch.values[2].data.string.rowBytes(0));
    try std.testing.expect(!batch.values[2].isValid(1));
}

test "SQL file scan: CTAS and INSERT SELECT persist external rows" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(io, .{
        .sub_path = "scores.csv",
        .data =
        \\id,score
        \\10,100
        \\20,200
        \\
        ,
    });
    try tmp.dir.writeFile(io, .{
        .sub_path = "more.json",
        .data =
        \\[{"id":30,"score":300},{"id":40,"score":400}]
        ,
    });
    const root = try fileRootPath(allocator, tmp);
    defer allocator.free(root);
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{ .file_scan_access = .{ .root = root } });
    defer db.close();

    try helpers.exec(allocator, db, "CREATE TABLE scores AS SELECT id, score FROM 'scores.csv'");
    try helpers.exec(allocator, db, "INSERT INTO scores SELECT id, score FROM read_json('more.json')");
    const t = try db.openTable("scores", .{});
    try t.flush();

    const ids = try helpers.collectBigints(allocator, db, "SELECT id FROM scores ORDER BY id ASC");
    defer allocator.free(ids);
    try std.testing.expectEqualSlices(i64, &.{ 10, 20, 30, 40 }, ids);
}

test "SQL file scan: file-root rejects traversal" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try fileRootPath(allocator, tmp);
    defer allocator.free(root);
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{ .file_scan_access = .{ .root = root } });
    defer db.close();

    try helpers.expectRunError(allocator, db, "SELECT * FROM '../outside.csv'", error.FileScanPathOutsideRoot);
}
