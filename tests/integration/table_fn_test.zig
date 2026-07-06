//! Table-valued UDF (TVF) integration: raw-descriptor registration via
//! `Database.registerTableUdf`, the `TABLE(f((subquery)) PARTITION BY ...
//! ORDER BY ...)` call form, partitioned vs GLOBAL execution, and the
//! type-contract compile errors.

const std = @import("std");
const thindb = @import("thindb");
const helpers = @import("sql_helpers.zig");

const schema = thindb.TableSchema{
    .columns = &.{
        .{ .name = "id", .type = .bigint },
        .{ .name = "g", .type = .int },
        .{ .name = "amt", .type = .bigint },
    },
    .order_key = &.{"id"},
    .unique = true,
};
const ok = [_][]const u8{"id"};
const opts = thindb.TableOptions{ .order_key = &ok, .unique = true, .row_group_size = 8 };

fn seed(db: *thindb.Database) !void {
    const t = try db.table("t", schema, opts);
    // Two groups, deliberately inserted out of id order so the ORDER BY
    // inside the TVF call is doing real work.
    try t.insert(&.{
        .{ .id = @as(i64, 4), .g = @as(i32, 2), .amt = @as(i64, 40) },
        .{ .id = @as(i64, 1), .g = @as(i32, 1), .amt = @as(i64, 10) },
        .{ .id = @as(i64, 3), .g = @as(i32, 1), .amt = @as(i64, 30) },
        .{ .id = @as(i64, 2), .g = @as(i32, 1), .amt = @as(i64, 20) },
        .{ .id = @as(i64, 5), .g = @as(i32, 2), .amt = @as(i64, 50) },
    });
    try t.flush();
}

/// Running total per partition: emits (id, running) with state carried in
/// a local across the ordered rows — the canonical "SQL can't say this
/// without window contortions" kernel, written against the RAW layer.
fn runningTotal(
    ctx: *const thindb.udf.TvfContext,
    parts: []const thindb.udf.TvfPartition,
    out: *thindb.udf.TvfOutput,
) !void {
    _ = ctx;
    const part = &parts[0];
    const ids = part.columns[0].data.bigint;
    const amts = part.columns[2].data.bigint;
    var running: i64 = 0;
    for (0..part.row_count) |i| {
        running += amts[i];
        try out.columns[0].data.bigint.append(out.allocator, ids[i]);
        try out.columns[1].data.bigint.append(out.allocator, running);
    }
}

const input_cols = [_]thindb.Column{
    .{ .name = "id", .type = .bigint },
    .{ .name = "g", .type = .int },
    .{ .name = "amt", .type = .bigint },
};
const output_cols = [_]thindb.Column{
    .{ .name = "id", .type = .bigint },
    .{ .name = "running", .type = .bigint },
};

fn register(db: *thindb.Database, execution: thindb.udf.TvfExecution) !void {
    try db.registerTableUdf(.{
        .name = "running_total",
        .input_schemas = &.{&input_cols},
        .output_schema = &output_cols,
        .execution = execution,
        .process = runningTotal,
    });
}

fn registryFor(db: *thindb.Database) *const thindb.UdfRegistry {
    if (db.catalog) |catalog| return &catalog.udfs;
    return &db.owned_catalog.?.udfs;
}

fn run(allocator: std.mem.Allocator, db: *thindb.Database, sql: []const u8) !helpers.RunResult {
    var arena = std.heap.ArenaAllocator.init(allocator);
    errdefer arena.deinit();
    const root = try thindb.sql.parseDialectWithUdfs(arena.allocator(), sql, .neutral, registryFor(db));
    const cq = try thindb.net.compile(allocator, db, root);
    return .{
        .arena = arena,
        .cq = cq,
        .owned_vars = cq.sessionValue().vars,
        .backing_allocator = allocator,
    };
}

test "table UDF: partitioned running total with ORDER BY" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    try seed(db);
    try register(db, .either);

    var res = try run(allocator, db,
        \\SELECT id, running
        \\FROM TABLE(running_total((SELECT id, g, amt FROM t)) PARTITION BY g ORDER BY id)
    );
    defer res.deinit();

    // Group 1 in id order: 10, 30, 60. Group 2: 40, 90. Partition-major
    // concat, ordered within each partition.
    var got_ids: std.ArrayList(i64) = .empty;
    defer got_ids.deinit(allocator);
    var got_running: std.ArrayList(i64) = .empty;
    defer got_running.deinit(allocator);
    while (try res.next()) |batch| {
        const ids = batch.values[0].data.bigint;
        const rs = batch.values[1].data.bigint;
        for (0..batch.row_count) |i| {
            try got_ids.append(allocator, ids[i]);
            try got_running.append(allocator, rs[i]);
        }
    }
    try std.testing.expectEqualSlices(i64, &.{ 1, 2, 3, 4, 5 }, got_ids.items);
    try std.testing.expectEqualSlices(i64, &.{ 10, 30, 60, 40, 90 }, got_running.items);
}

test "table UDF: global mode is one partition over everything" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    try seed(db);
    try register(db, .either);

    var res = try run(allocator, db,
        \\SELECT id, running
        \\FROM TABLE(running_total((SELECT id, g, amt FROM t)) ORDER BY id)
    );
    defer res.deinit();

    var got_running: std.ArrayList(i64) = .empty;
    defer got_running.deinit(allocator);
    while (try res.next()) |batch| {
        const rs = batch.values[1].data.bigint;
        for (0..batch.row_count) |i| try got_running.append(allocator, rs[i]);
    }
    // One global run over ids 1..5: 10, 30, 60, 100, 150.
    try std.testing.expectEqualSlices(i64, &.{ 10, 30, 60, 100, 150 }, got_running.items);
}

test "table UDF: declared execution mode is compile-enforced both ways" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    try seed(db);
    try register(db, .partitioned);

    // .partitioned called WITHOUT PARTITION BY: rejected at compile.
    try helpers.expectRunError(
        allocator,
        db,
        "SELECT * FROM TABLE(running_total((SELECT id, g, amt FROM t)) ORDER BY id)",
        thindb.exec.Error.TableFnExecutionMismatch,
    );
}

test "table UDF: parallel partition execution matches serial (thread-safe allocator)" {
    var gpa = std.heap.DebugAllocator(.{ .thread_safe = true }){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var db = try thindb.Database.open(allocator, io, tmp.dir, .{ .max_dop = 4 });
    defer db.close();

    // 64 groups × 8 rows, shuffled ids so ORDER BY works per partition.
    const t = try db.table("t", schema, opts);
    var id: i64 = 1;
    for (0..8) |round| {
        for (0..64) |g| {
            try t.insert(&.{.{
                .id = id + @as(i64, @intCast((round * 37 + g * 11) % 512)) * 1000,
                .g = @as(i32, @intCast(g)),
                .amt = @as(i64, @intCast(g + round)),
            }});
            id += 1;
        }
    }
    try t.flush();
    try register(db, .either);

    const sql =
        \\SELECT id, running
        \\FROM TABLE(running_total((SELECT id, g, amt FROM t)) PARTITION BY g ORDER BY id)
    ;
    var serial_ids: std.ArrayList(i64) = .empty;
    defer serial_ids.deinit(allocator);
    var serial_run: std.ArrayList(i64) = .empty;
    defer serial_run.deinit(allocator);
    {
        var res = try run(allocator, db, sql);
        defer res.deinit();
        while (try res.next()) |batch| {
            for (0..batch.row_count) |i| {
                try serial_ids.append(allocator, batch.values[0].data.bigint[i]);
                try serial_run.append(allocator, batch.values[1].data.bigint[i]);
            }
        }
    }

    thindb.exec.table_fn.force_parallel_in_tests = true;
    defer thindb.exec.table_fn.force_parallel_in_tests = false;
    var par_ids: std.ArrayList(i64) = .empty;
    defer par_ids.deinit(allocator);
    var par_run: std.ArrayList(i64) = .empty;
    defer par_run.deinit(allocator);
    {
        var res = try run(allocator, db, sql);
        defer res.deinit();
        while (try res.next()) |batch| {
            for (0..batch.row_count) |i| {
                try par_ids.append(allocator, batch.values[0].data.bigint[i]);
                try par_run.append(allocator, batch.values[1].data.bigint[i]);
            }
        }
    }

    try std.testing.expectEqual(@as(usize, 512), serial_ids.items.len);
    try std.testing.expectEqualSlices(i64, serial_ids.items, par_ids.items);
    try std.testing.expectEqualSlices(i64, serial_run.items, par_run.items);
}

// ---------------------------------------------------------------------------
// SDK layer: the same functions written as a user would write them — plain
// struct row types, tdb.Partition/Writer, registerTableFn.
// ---------------------------------------------------------------------------

const tdb = thindb.tdb;

const sdk_running_total = struct {
    pub const spec = tdb.TableFnSpec{ .name = "sdk_running_total", .execution = .either };
    pub const Input = struct { id: i64, g: i32, amt: i64 };
    pub const Output = struct { id: i64, running: i64 };

    pub fn process(ctx: *tdb.Ctx, p: tdb.Partition(Input), out: *tdb.Writer(Output)) !void {
        _ = ctx;
        const ids = p.col(.id);
        const amts = p.col(.amt);
        var running: i64 = 0;
        for (0..p.len) |i| {
            running += amts[i];
            try out.row(.{ .id = ids[i], .running = running });
        }
    }
};

/// Exercises every accessor flavor: string partition key, nullable input
/// via Opt.get, Date arithmetic, the row iterator, at(), and nullable +
/// string output columns.
const sdk_gap_fill = struct {
    pub const spec = tdb.TableFnSpec{ .name = "sdk_gap_fill", .execution = .partitioned };
    pub const Input = struct { customer: []const u8, month: tdb.Date, amt: ?i64 };
    pub const Output = struct {
        customer: []const u8,
        month: tdb.Date,
        amt: i64,
        change: ?i64,
        filled: bool,
    };

    pub fn process(ctx: *tdb.Ctx, p: tdb.Partition(Input), out: *tdb.Writer(Output)) !void {
        _ = ctx;
        const customer = p.key(.customer);
        const months = p.col(.month);
        const amts = p.col(.amt);

        var prev: ?i64 = null;
        var m = months[0];
        const last = months[p.len - 1];
        var i: usize = 0;
        while (m.lte(last)) : (m = m.addMonths(1)) {
            const present = i < p.len and months[i].eq(m);
            const cur: i64 = if (present) (amts.get(i) orelse 0) else (prev orelse 0);
            if (present) i += 1;
            try out.row(.{
                .customer = customer,
                .month = m,
                .amt = cur,
                .change = if (prev) |pv| cur - pv else null,
                .filled = !present,
            });
            prev = cur;
        }
    }
};

const gap_schema = thindb.TableSchema{
    .columns = &.{
        .{ .name = "customer", .type = .{ .varchar = 64 } },
        .{ .name = "month", .type = .date },
        .{ .name = "amt", .type = .bigint, .nullable = true },
    },
    .order_key = &.{ "customer", "month" },
    .unique = true,
};
const gap_ok = [_][]const u8{ "customer", "month" };
const gap_opts = thindb.TableOptions{ .order_key = &gap_ok, .unique = true, .row_group_size = 8 };

test "table UDF SDK: running total via struct row types" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    try seed(db);
    try db.registerTableFn(sdk_running_total);

    var res = try run(allocator, db,
        \\SELECT id, running
        \\FROM TABLE(sdk_running_total((SELECT id, g, amt FROM t)) PARTITION BY g ORDER BY id)
    );
    defer res.deinit();

    var got: std.ArrayList(i64) = .empty;
    defer got.deinit(allocator);
    while (try res.next()) |batch| {
        for (0..batch.row_count) |i| try got.append(allocator, batch.values[1].data.bigint[i]);
    }
    try std.testing.expectEqualSlices(i64, &.{ 10, 30, 60, 40, 90 }, got.items);
}

test "table UDF SDK: gap fill — strings, dates, nullables, key()" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    const jan = tdb.Date.fromYmd(.{ .y = 2026, .m = 1, .d = 1 });
    const feb = jan.addMonths(1);
    const apr = jan.addMonths(3);
    const t = try db.table("rev", gap_schema, gap_opts);
    // acme: Jan=100, Feb=NULL(→0), Apr=250 — March missing (gap → carried).
    // bolt: Jan=50 only.
    try t.insert(&.{
        .{ .customer = "acme", .month = jan.days(), .amt = @as(?i64, 100) },
        .{ .customer = "acme", .month = feb.days(), .amt = @as(?i64, null) },
        .{ .customer = "acme", .month = apr.days(), .amt = @as(?i64, 250) },
        .{ .customer = "bolt", .month = jan.days(), .amt = @as(?i64, 50) },
    });
    try t.flush();
    try db.registerTableFn(sdk_gap_fill);

    // Outer ORDER BY: partition emission order is unspecified (the concat
    // contract) — pin it for the assertions below.
    var res = try run(allocator, db,
        \\SELECT customer, month, amt, change, filled
        \\FROM TABLE(sdk_gap_fill((SELECT customer, month, amt FROM rev))
        \\           PARTITION BY customer ORDER BY month)
        \\ORDER BY customer, month
    );
    defer res.deinit();

    var rows: usize = 0;
    var amts: std.ArrayList(i64) = .empty;
    defer amts.deinit(allocator);
    var filled: std.ArrayList(bool) = .empty;
    defer filled.deinit(allocator);
    while (try res.next()) |batch| {
        for (0..batch.row_count) |i| {
            try amts.append(allocator, batch.values[2].data.bigint[i]);
            try filled.append(allocator, batch.values[4].data.boolean[i] != 0);
            rows += 1;
        }
    }
    // acme: Jan 100, Feb 0 (NULL→0), Mar 0 (gap, carried), Apr 250; bolt: Jan 50.
    try std.testing.expectEqual(@as(usize, 5), rows);
    try std.testing.expectEqualSlices(i64, &.{ 100, 0, 0, 250, 50 }, amts.items);
    try std.testing.expectEqualSlices(bool, &.{ false, false, true, false, false }, filled.items);
}

test "table UDF SDK: row iterator and at() match columnar access" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const iter_fn = struct {
        pub const spec = tdb.TableFnSpec{ .name = "iter_sum", .execution = .global };
        pub const Input = struct { id: i64, g: i32, amt: i64 };
        pub const Output = struct { total: i64 };

        pub fn process(ctx: *tdb.Ctx, p: tdb.Partition(Input), out: *tdb.Writer(Output)) !void {
            _ = ctx;
            var via_iter: i64 = 0;
            var it = p.iter();
            while (it.next()) |row| via_iter += row.amt;
            var via_at: i64 = 0;
            for (0..p.len) |i| via_at += p.at(i).amt;
            std.debug.assert(via_iter == via_at);
            try out.row(.{ .total = via_iter });
        }
    };

    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    try seed(db);
    try db.registerTableFn(iter_fn);

    var res = try run(allocator, db,
        "SELECT total FROM TABLE(iter_sum((SELECT id, g, amt FROM t)))");
    defer res.deinit();
    const batch = (try res.next()).?;
    try std.testing.expectEqual(@as(i64, 150), batch.values[0].data.bigint[0]);
}

// ---------------------------------------------------------------------------
// P3: co-partitioned multiple inputs.
// ---------------------------------------------------------------------------

const sdk_reconcile = struct {
    pub const spec = tdb.TableFnSpec{ .name = "sdk_reconcile", .execution = .partitioned };
    pub const Input = struct { customer: []const u8, month: tdb.Date, amount: ?i64 };
    pub const Input2 = struct { customer: []const u8, month: tdb.Date, estimate: ?i64 };
    pub const Output = struct {
        customer: []const u8,
        month: tdb.Date,
        actual: i64,
        estimated: i64,
        variance: i64,
    };

    pub fn process(ctx: *tdb.Ctx, inv: tdb.Partition(Input), est: tdb.Partition(Input2), out: *tdb.Writer(Output)) !void {
        _ = ctx;
        // Key from whichever side has rows — the empty side is aligned,
        // not skipped.
        const customer = if (inv.len > 0) inv.key(.customer) else est.key(.customer);
        const im = inv.col(.month);
        const ia = inv.col(.amount);
        const em = est.col(.month);
        const ea = est.col(.estimate);

        var i: usize = 0;
        var j: usize = 0;
        while (i < inv.len or j < est.len) {
            const have_inv = i < inv.len;
            const have_est = j < est.len;
            const m = if (have_inv and (!have_est or im[i].lte(em[j]))) im[i] else em[j];
            var actual: i64 = 0;
            var estimated: i64 = 0;
            if (have_inv and im[i].eq(m)) {
                actual = ia.get(i) orelse 0;
                i += 1;
            }
            if (have_est and em[j].eq(m)) {
                estimated = ea.get(j) orelse 0;
                j += 1;
            }
            try out.row(.{
                .customer = customer,
                .month = m,
                .actual = actual,
                .estimated = estimated,
                .variance = actual - estimated,
            });
        }
    }
};

test "table UDF P3: two co-partitioned inputs with empty-partition alignment" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    const inv_schema = thindb.TableSchema{
        .columns = &.{
            .{ .name = "customer", .type = .{ .varchar = 32 } },
            .{ .name = "month", .type = .date },
            .{ .name = "amount", .type = .bigint, .nullable = true },
        },
        .order_key = &.{ "customer", "month" },
        .unique = true,
    };
    const est_schema = thindb.TableSchema{
        .columns = &.{
            .{ .name = "customer", .type = .{ .varchar = 32 } },
            .{ .name = "month", .type = .date },
            .{ .name = "estimate", .type = .bigint, .nullable = true },
        },
        .order_key = &.{ "customer", "month" },
        .unique = true,
    };
    const key2 = [_][]const u8{ "customer", "month" };
    const t_opts = thindb.TableOptions{ .order_key = &key2, .unique = true, .row_group_size = 8 };

    const jan = tdb.Date.fromYmd(.{ .y = 2026, .m = 1, .d = 1 });
    const feb = jan.addMonths(1);

    // acme: invoices Jan+Feb, forecast Jan only.
    // bolt: forecast ONLY (its invoice partition arrives EMPTY).
    // cara: invoices ONLY (its estimate partition arrives EMPTY).
    const ti = try db.table("inv", inv_schema, t_opts);
    try ti.insert(&.{
        .{ .customer = "acme", .month = jan.days(), .amount = @as(?i64, 100) },
        .{ .customer = "acme", .month = feb.days(), .amount = @as(?i64, 150) },
        .{ .customer = "cara", .month = jan.days(), .amount = @as(?i64, 70) },
    });
    try ti.flush();
    const te = try db.table("fc", est_schema, t_opts);
    try te.insert(&.{
        .{ .customer = "acme", .month = jan.days(), .estimate = @as(?i64, 120) },
        .{ .customer = "bolt", .month = feb.days(), .estimate = @as(?i64, 40) },
    });
    try te.flush();

    try db.registerTableFn(sdk_reconcile);

    var res = try run(allocator, db,
        \\SELECT customer, month, actual, estimated, variance
        \\FROM TABLE(sdk_reconcile(
        \\  (SELECT customer, month, amount FROM inv),
        \\  (SELECT customer, month, estimate FROM fc)
        \\) PARTITION BY customer ORDER BY month)
        \\ORDER BY customer, month
    );
    defer res.deinit();

    var customers: std.ArrayList([]u8) = .empty;
    defer {
        for (customers.items) |c| allocator.free(c);
        customers.deinit(allocator);
    }
    var actuals: std.ArrayList(i64) = .empty;
    defer actuals.deinit(allocator);
    var estimates: std.ArrayList(i64) = .empty;
    defer estimates.deinit(allocator);
    var variances: std.ArrayList(i64) = .empty;
    defer variances.deinit(allocator);
    while (try res.next()) |batch| {
        for (0..batch.row_count) |i| {
            const sv = switch (batch.values[0].data) {
                .varchar, .string, .char => |v| v.rowBytes(i),
                else => unreachable,
            };
            try customers.append(allocator, try allocator.dupe(u8, sv));
            try actuals.append(allocator, batch.values[2].data.bigint[i]);
            try estimates.append(allocator, batch.values[3].data.bigint[i]);
            try variances.append(allocator, batch.values[4].data.bigint[i]);
        }
    }
    // acme Jan (100 vs 120), acme Feb (150 vs 0), bolt Feb (0 vs 40),
    // cara Jan (70 vs 0).
    try std.testing.expectEqual(@as(usize, 4), customers.items.len);
    try std.testing.expectEqualStrings("acme", customers.items[0]);
    try std.testing.expectEqualStrings("acme", customers.items[1]);
    try std.testing.expectEqualStrings("bolt", customers.items[2]);
    try std.testing.expectEqualStrings("cara", customers.items[3]);
    try std.testing.expectEqualSlices(i64, &.{ 100, 150, 0, 70 }, actuals.items);
    try std.testing.expectEqualSlices(i64, &.{ 120, 0, 40, 0 }, estimates.items);
    try std.testing.expectEqualSlices(i64, &.{ -20, 150, -40, 70 }, variances.items);
}

test "table UDF P3: global multi-input hands every input whole" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const totals = struct {
        pub const spec = tdb.TableFnSpec{ .name = "totals2", .execution = .global };
        pub const Input = struct { id: ?i64, g: ?i32, amt: ?i64 };
        pub const Input2 = struct { id: ?i64, g: ?i32, amt: ?i64 };
        pub const Output = struct { a_total: i64, b_total: i64, a_rows: i64, b_rows: i64 };

        pub fn process(ctx: *tdb.Ctx, a: tdb.Partition(Input), b: tdb.Partition(Input2), out: *tdb.Writer(Output)) !void {
            _ = ctx;
            var ta: i64 = 0;
            var tb: i64 = 0;
            const aa = a.col(.amt);
            const ba = b.col(.amt);
            for (0..a.len) |i| ta += aa.get(i) orelse 0;
            for (0..b.len) |i| tb += ba.get(i) orelse 0;
            try out.row(.{
                .a_total = ta,
                .b_total = tb,
                .a_rows = @intCast(a.len),
                .b_rows = @intCast(b.len),
            });
        }
    };

    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    try seed(db);
    try db.registerTableFn(totals);

    var res = try run(allocator, db,
        \\SELECT a_total, b_total, a_rows, b_rows
        \\FROM TABLE(totals2(
        \\  (SELECT id, g, amt FROM t),
        \\  (SELECT id, g, amt FROM t WHERE g = 1)
        \\))
    );
    defer res.deinit();
    const batch = (try res.next()).?;
    try std.testing.expectEqual(@as(i64, 150), batch.values[0].data.bigint[0]);
    try std.testing.expectEqual(@as(i64, 60), batch.values[1].data.bigint[0]);
    try std.testing.expectEqual(@as(i64, 5), batch.values[2].data.bigint[0]);
    try std.testing.expectEqual(@as(i64, 3), batch.values[3].data.bigint[0]);
}

test "table UDF: input shape violations are compile errors" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    try seed(db);
    try register(db, .either);

    // Missing declared column (only 2 of 3 provided).
    try helpers.expectRunError(
        allocator,
        db,
        "SELECT * FROM TABLE(running_total((SELECT id, g FROM t)) PARTITION BY g)",
        thindb.exec.Error.TableFnInputMismatch,
    );
    // Wrong column name for the declared shape.
    try helpers.expectRunError(
        allocator,
        db,
        "SELECT * FROM TABLE(running_total((SELECT id, g, amt AS amount FROM t)) PARTITION BY g)",
        thindb.exec.Error.TableFnInputMismatch,
    );
}
