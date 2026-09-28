//! A slow UDF and the table it runs over, shared by the wire tests that
//! act on a statement while it is still running (disconnect-cancel, KILL,
//! process lists).

const std = @import("std");
const thindb = @import("thindb");

/// A scalar UDF that sleeps on every batch, so a statement calling it is
/// still running when the test acts on it.
pub const SlowProbe = struct {
    io: std.Io,
    rows: std.atomic.Value(usize) = .init(0),

    fn kernel(ctx: *const thindb.udf.ScalarContext, args: []const thindb.storage.ColumnView, out: *thindb.engine.ColumnStore, count: usize) !void {
        const self: *SlowProbe = @ptrCast(@alignCast(ctx.user_data.?));
        _ = self.rows.fetchAdd(count, .monotonic);
        try out.data.bigint.appendSlice(ctx.allocator, args[0].data.bigint[0..count]);
        try std.Io.sleep(self.io, .fromMilliseconds(20), .awake);
    }

    pub fn awaitFirstBatch(self: *SlowProbe) !void {
        for (0..1000) |_| {
            if (self.rows.load(.monotonic) > 0) return;
            try std.Io.sleep(self.io, .fromMilliseconds(5), .awake);
        }
        return error.ProbeNeverCalled;
    }
};

pub const total_rows: usize = 4096;
const schema_ids = thindb.TableSchema{
    .columns = &.{.{ .name = "id", .type = .bigint }},
    .order_key = &.{"id"},
    .unique = false,
};
const ok_ids = [_][]const u8{"id"};
const opts_ids = thindb.TableOptions{
    .order_key = &ok_ids,
    .row_group_size = 128,
};

/// Registers `slow_probe` and seeds `main.public.t` with enough row groups
/// that a statement over it spans many probe batches; `dst` starts empty.
pub fn seed(catalog: *thindb.Catalog, probe: *SlowProbe) !void {
    try catalog.registerScalarUdf(.{
        .name = "slow_probe",
        .arg_types = &.{.bigint},
        .return_type = .bigint,
        .volatility = .immutable,
        .kernel = SlowProbe.kernel,
        .user_data = probe,
    });
    const sc = catalog.database("main").?.schema("public").?;
    const t = try sc.table("t", schema_ids, opts_ids);
    var rows: [total_rows]struct { id: i64 } = undefined;
    for (&rows, 0..) |*row, i| row.* = .{ .id = @intCast(i) };
    try t.insert(&rows);
    try t.flush();
    _ = try sc.table("dst", schema_ids, opts_ids);
}
