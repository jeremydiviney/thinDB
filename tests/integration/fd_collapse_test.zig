//! Functional-dependency group-key collapse — a pre-execution plan
//! rewrite. A computed grouping key that is a pure, deterministic function
//! of other *retained* group keys (or a constant) adds no grouping
//! distinctions, so the parser drops it from the GroupBy and recomputes it
//! once per output group ABOVE the aggregate.
//!
//! These tests assert two things per case: (1) the rewritten plan shape via
//! `thindb.ir.explain`, and (2) byte-identical results (values + row order)
//! against the obvious hand-grouping. The third case is the control: two
//! independent columns must NOT collapse — its plan stays untouched.

const std = @import("std");
const thindb = @import("thindb");
const helpers = @import("sql_helpers.zig");
const runSql = helpers.runSql;
const exec = helpers.exec;

fn setup(allocator: std.mem.Allocator, io: anytype, dir: anytype) !*thindb.Database {
    const db = try thindb.Database.open(allocator, io, dir, .{});
    errdefer db.close();
    try exec(
        allocator,
        db,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, ip BIGINT NOT NULL, region BIGINT NOT NULL)",
    );
    // Three distinct ip values; region varies independently of ip so the
    // two-independent-column control has real cross products.
    try exec(
        allocator,
        db,
        "INSERT INTO t (id, ip, region) VALUES " ++
            "(1, 100, 1), (2, 100, 1), (3, 100, 2), " ++
            "(4, 200, 1), (5, 200, 2), " ++
            "(6, 300, 1)",
    );
    const t = try db.openTable("t", .{});
    try t.flush();
    return db;
}

/// Parse `sql` and render the rewritten IR plan to text owned by `arena`.
fn planText(arena: std.mem.Allocator, sql: []const u8) ![]const u8 {
    const root = try thindb.sql.parse(arena, sql);
    var out: std.ArrayList(u8) = .empty;
    try thindb.ir.explain(arena, &out, root.*);
    return out.items;
}

test "fd-collapse: Q35-shape drops derived keys, recomputes above the aggregate" {
    const allocator = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();

    // GROUP BY ip, ip-1, ip-2 — the two subtractions are pure functions of
    // `ip`, so they collapse. The GroupBy groups on `ip` alone; a Compute
    // above it recomputes the dropped keys for output order.
    const text = try planText(
        arena.allocator(),
        "SELECT ip, ip - 1, ip - 2, COUNT(*) AS c FROM t GROUP BY ip, ip - 1, ip - 2 ORDER BY c DESC LIMIT 10",
    );
    try std.testing.expectEqualStrings(
        \\Limit n=10
        \\  Select [ip, sub(ip, 1), sub(ip, 2), c]
        \\    OrderBy [c DESC]
        \\      Compute [sub(ip, 1) := sub(ip, 1), sub(ip, 2) := sub(ip, 2)]
        \\        GroupBy keys=[ip] aggs=[count(*) AS c]
        \\          Scan t
        \\
    , text);
}

test "fd-collapse: Q35-shape results identical to grouping on ip alone" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, io, tmp.dir);
    defer db.close();

    // Deterministic tiebreak: ORDER BY c DESC, ip ASC.
    var q = try runSql(
        allocator,
        db,
        "SELECT ip, ip - 1, ip - 2, COUNT(*) AS c FROM t GROUP BY ip, ip - 1, ip - 2 ORDER BY c DESC, ip ASC",
    );
    defer q.deinit();

    // Groups by ip: 100→3, 200→2, 300→1. After ORDER BY c DESC, ip ASC:
    // (100,99,98,3), (200,199,198,2), (300,299,298,1).
    const expect_ip = [_]i64{ 100, 200, 300 };
    const expect_m1 = [_]i64{ 99, 199, 299 };
    const expect_m2 = [_]i64{ 98, 198, 298 };
    const expect_c = [_]i64{ 3, 2, 1 };

    var row: usize = 0;
    while (try q.next()) |b| {
        var i: usize = 0;
        while (i < b.row_count) : (i += 1) {
            try std.testing.expectEqual(expect_ip[row], b.values[0].data.bigint[i]);
            try std.testing.expectEqual(expect_m1[row], b.values[1].data.bigint[i]);
            try std.testing.expectEqual(expect_m2[row], b.values[2].data.bigint[i]);
            try std.testing.expectEqual(expect_c[row], b.values[3].data.bigint[i]);
            row += 1;
        }
    }
    try std.testing.expectEqual(@as(usize, 3), row);
}

test "fd-collapse: constant group key (GROUP BY 1, col) drops the constant" {
    const allocator = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();

    // SELECT item 1 is the literal `1`; GROUP BY 1 references it by ordinal.
    // A constant key adds no distinctions → collapses, recomputed above.
    const text = try planText(
        arena.allocator(),
        "SELECT 1, region, COUNT(*) AS c FROM t GROUP BY 1, region ORDER BY c DESC LIMIT 10",
    );
    try std.testing.expectEqualStrings(
        \\Limit n=10
        \\  Select [1, region, c]
        \\    OrderBy [c DESC]
        \\      Compute [1 := 1]
        \\        GroupBy keys=[region] aggs=[count(*) AS c]
        \\          Scan t
        \\
    , text);
}

test "fd-collapse: constant group key results identical" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try setup(allocator, io, tmp.dir);
    defer db.close();

    var q = try runSql(
        allocator,
        db,
        "SELECT 1, region, COUNT(*) AS c FROM t GROUP BY 1, region ORDER BY c DESC, region ASC",
    );
    defer q.deinit();

    // region groups: 1→4 rows, 2→2 rows. ORDER BY c DESC, region ASC:
    // (1, 1, 4), (1, 2, 2). The literal column is `1` (INT) for every row.
    const expect_const = [_]i32{ 1, 1 };
    const expect_region = [_]i64{ 1, 2 };
    const expect_c = [_]i64{ 4, 2 };

    var row: usize = 0;
    while (try q.next()) |b| {
        var i: usize = 0;
        while (i < b.row_count) : (i += 1) {
            try std.testing.expectEqual(expect_const[row], b.values[0].data.int[i]);
            try std.testing.expectEqual(expect_region[row], b.values[1].data.bigint[i]);
            try std.testing.expectEqual(expect_c[row], b.values[2].data.bigint[i]);
            row += 1;
        }
    }
    try std.testing.expectEqual(@as(usize, 2), row);
}

test "fd-collapse: two independent columns are NOT collapsed (control)" {
    const allocator = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();

    // `ip` and `region` are independent plain columns — neither is a function
    // of the other, so the GroupBy keeps both keys and no extra Compute is
    // inserted. The plan must be byte-identical to the pre-rewrite shape.
    const text = try planText(
        arena.allocator(),
        "SELECT ip, region, COUNT(*) AS c FROM t GROUP BY ip, region ORDER BY c DESC LIMIT 10",
    );
    try std.testing.expectEqualStrings(
        \\Limit n=10
        \\  OrderBy [c DESC]
        \\    GroupBy keys=[ip, region] aggs=[count(*) AS c]
        \\      Scan t
        \\
    , text);
}

test "fd-collapse: non-collapsible derived key (function of a NON-key) stays below" {
    const allocator = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();

    // `region - 1` references `region`, which is NOT a group key here — so it
    // is not a function of the retained keys and must stay a below-aggregate
    // derived key (computed once per input row, grouped on directly).
    const text = try planText(
        arena.allocator(),
        "SELECT ip, region - 1, COUNT(*) AS c FROM t GROUP BY ip, region - 1 ORDER BY c DESC LIMIT 10",
    );
    try std.testing.expectEqualStrings(
        \\Limit n=10
        \\  Select [ip, sub(region, 1), c]
        \\    OrderBy [c DESC]
        \\      GroupBy keys=[ip, sub(region, 1)] aggs=[count(*) AS c]
        \\        Compute [sub(region, 1) := sub(region, 1)]
        \\          Scan t
        \\
    , text);
}

test "fd-collapse: all-constant GROUP BY is left alone (no anchor to collapse onto)" {
    const allocator = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();

    // GROUP BY 1 — the only key is a constant. Collapsing it would turn the
    // aggregate global, which differs on empty input. With no plain-column
    // anchor to retain, the rewrite bails and the key stays below.
    const text = try planText(
        arena.allocator(),
        "SELECT 1, COUNT(*) AS c FROM t GROUP BY 1",
    );
    try std.testing.expectEqualStrings(
        \\Select [1, c]
        \\  GroupBy keys=[1] aggs=[count(*) AS c]
        \\    Compute [1 := 1]
        \\      Scan t
        \\
    , text);
}
