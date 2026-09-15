const std = @import("std");
const api = @import("../api/api.zig");
const local = @import("../net/local.zig");
const sql = @import("../sql/sql.zig");
const udf = @import("../udf.zig");
const storage = @import("../storage/storage.zig");
const engine = @import("../engine/engine.zig");

const Probe = struct {
    flag: std.atomic.Value(bool) = .init(false),
    rows: std.atomic.Value(usize) = .init(0),
    cancel_on_call: bool = true,
    varying: bool = false,

    fn kernel(ctx: *const udf.ScalarContext, args: []const storage.ColumnView, out: *engine.ColumnStore, count: usize) !void {
        const self: *Probe = @ptrCast(@alignCast(ctx.user_data.?));
        const base = self.rows.fetchAdd(count, .monotonic);
        if (self.varying) {
            for (0..count) |i| try out.data.bigint.append(ctx.allocator, @intCast(base + i));
        } else {
            try out.data.bigint.appendSlice(ctx.allocator, args[0].data.bigint[0..count]);
        }
        if (self.cancel_on_call) self.flag.store(true, .release);
    }
};

test "cancellation propagates through eager CTEs and blocking SQL paths with budgets disabled" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const db = try api.Database.open(a, io, tmp.dir, .{
        .query_memory_budget = 0,
        .memory_budget = 0,
        .max_dop = 2,
    });
    defer db.close();
    var probe = Probe{};
    try db.registerScalarUdf(.{
        .name = "cancel_probe",
        .arg_types = &.{.bigint},
        .return_type = .bigint,
        .volatility = .immutable,
        .kernel = Probe.kernel,
        .user_data = &probe,
    });
    const t = try db.table("t", .{
        .columns = &.{.{ .name = "id", .type = .bigint }},
        .order_key = &.{"id"},
        .unique = false,
    }, .{ .order_key = &.{"id"}, .row_group_size = 128 });
    for (0..8192) |i| try t.insert(&.{.{ .id = @as(i64, @intCast(i)) }});
    try t.flush();
    const cases = [_][]const u8{
        "SELECT id FROM t WHERE id > (SELECT max(cancel_probe(id)) FROM t)",
        "WITH m AS MATERIALIZED (SELECT cancel_probe(id) AS k FROM t) SELECT k FROM m",
        "SELECT cancel_probe(id) AS k FROM t ORDER BY k",
        "SELECT cancel_probe(id) AS k, count(*) FROM t GROUP BY k ORDER BY k LIMIT 10",
        "SELECT a.id FROM t a JOIN (SELECT cancel_probe(id) AS k FROM t) b ON a.id = b.k",
        "SELECT row_number() OVER (ORDER BY k) FROM (SELECT cancel_probe(id) AS k FROM t) w",
    };
    for (cases, 0..) |query, case_index| {
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        const root = try sql.parseWithContext(arena.allocator(), query, .neutral, &db.catalog.?.udfs, .{ .registry = &db.catalog.?.sql_fns, .db = "main" });
        probe.flag.store(true, .release);
        try std.testing.expectError(error.QueryCancelled, local.compileWithOptions(a, db, .{}, root, .{ .cancel_flag = &probe.flag }));
        probe.flag.store(false, .release);
        probe.rows.store(0, .monotonic);
        var q = local.compileWithOptions(a, db, .{}, root, .{ .cancel_flag = &probe.flag }) catch |err| {
            try std.testing.expectEqual(error.QueryCancelled, err);
            try std.testing.expect(probe.rows.load(.monotonic) > 0);
            try std.testing.expect(probe.rows.load(.monotonic) < 8192);
            continue;
        };
        defer q.deinit();
        // Scalar-subquery resolution executes before the root is compiled.
        try std.testing.expect(case_index != 0);
        try std.testing.expectError(error.QueryCancelled, q.next());
        try std.testing.expect(probe.rows.load(.monotonic) > 0);
        try std.testing.expect(probe.rows.load(.monotonic) < 8192);
    }
    var varying = Probe{ .cancel_on_call = false, .varying = true };
    try db.registerScalarUdf(.{
        .name = "varying_probe",
        .arg_types = &.{.bigint},
        .return_type = .bigint,
        .volatility = .@"volatile",
        .kernel = Probe.kernel,
        .user_data = &varying,
    });
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const root = try sql.parseWithContext(arena.allocator(), "SELECT varying_probe(id) AS k, count(*) AS c FROM t GROUP BY k ORDER BY k LIMIT 5", .neutral, &db.catalog.?.udfs, .{ .registry = &db.catalog.?.sql_fns, .db = "main" });
    var q = try local.compile(a, db, root);
    defer q.deinit();
    var result_rows: usize = 0;
    while (try q.next()) |batch| {
        for (0..batch.row_count) |i| {
            try std.testing.expectEqual(@as(i64, @intCast(result_rows)), batch.values[0].data.bigint[i]);
            try std.testing.expectEqual(@as(i64, 1), batch.values[1].data.bigint[i]);
            result_rows += 1;
        }
    }
    try std.testing.expectEqual(@as(usize, 5), result_rows);
    try std.testing.expectEqual(@as(usize, 8192), varying.rows.load(.monotonic));
}
