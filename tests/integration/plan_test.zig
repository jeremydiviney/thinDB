//! PlanBuilder integration tests — multi-source / multi-branched
//! pipelines built via thindb.PlanBuilder and compiled to executable
//! Queries. The Query builder API (thindb.scan().filter().join(...))
//! handles linear and single-join shapes; PlanBuilder is the path for
//! everything more complex: joins-of-joins, branches with different
//! filters, computed columns feeding joins, etc.
//!
//! Companion to compute_test.zig + join_test.zig — those exercise the
//! Query builder directly; this file exercises the IR plan tree.

const std = @import("std");
const thindb = @import("thindb");

const PlanBuilder = thindb.PlanBuilder;

// Three small tables, two of them joinable on `k`.

const schema_a = thindb.TableSchema{
    .columns = &.{
        .{ .name = "id", .type = .bigint },
        .{ .name = "k", .type = .int },
        .{ .name = "label", .type = .string },
    },
    .order_key = &.{"id"},
    .unique = true,
};
const schema_b = thindb.TableSchema{
    .columns = &.{
        .{ .name = "bid", .type = .bigint },
        .{ .name = "k", .type = .int },
        .{ .name = "qty", .type = .int },
    },
    .order_key = &.{"bid"},
    .unique = true,
};
const schema_c = thindb.TableSchema{
    .columns = &.{
        .{ .name = "cid", .type = .bigint },
        .{ .name = "label", .type = .string },
        .{ .name = "weight", .type = .double },
    },
    .order_key = &.{"cid"},
    .unique = true,
};

const ok_a = [_][]const u8{"id"};
const ok_b = [_][]const u8{"bid"};
const ok_c = [_][]const u8{"cid"};

fn opts(ok: *const [1][]const u8) thindb.TableOptions {
    return .{ .order_key = ok, .unique = true, .row_group_size = 8 };
}

fn seedTables(db: anytype) !struct { a: *thindb.Table, b: *thindb.Table, c: *thindb.Table } {
    const a = try db.table("a", schema_a, opts(&ok_a));
    const b = try db.table("b", schema_b, opts(&ok_b));
    const c = try db.table("c", schema_c, opts(&ok_c));

    try a.insert(&.{
        .{ .id = @as(i64, 1), .k = @as(i32, 100), .label = "alpha" },
        .{ .id = @as(i64, 2), .k = @as(i32, 200), .label = "beta" },
        .{ .id = @as(i64, 3), .k = @as(i32, 300), .label = "gamma" },
    });
    try b.insert(&.{
        .{ .bid = @as(i64, 10), .k = @as(i32, 100), .qty = @as(i32, 5) },
        .{ .bid = @as(i64, 11), .k = @as(i32, 200), .qty = @as(i32, 7) },
        .{ .bid = @as(i64, 12), .k = @as(i32, 999), .qty = @as(i32, 1) }, // no match
    });
    try c.insert(&.{
        .{ .cid = @as(i64, 50), .label = "alpha", .weight = @as(f64, 1.5) },
        .{ .cid = @as(i64, 51), .label = "gamma", .weight = @as(f64, 2.5) },
        .{ .cid = @as(i64, 52), .label = "zeta", .weight = @as(f64, 9.0) }, // no match
    });
    try a.flush();
    try b.flush();
    try c.flush();
    return .{ .a = a, .b = b, .c = c };
}

test "plan: linear pipeline — scan + filter + select via PlanBuilder" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    _ = try seedTables(db);

    var pb = PlanBuilder.init(allocator);
    defer pb.deinit();

    const root = try pb.select(
        try pb.filter(
            try pb.scan("a"),
            .{ .leaf = .{ .col = "k", .op = .gt, .val = .{ .int = 100 } } },
        ),
        &.{ "id", "label" },
    );

    var q = try pb.compile(allocator, db,root);
    defer q.deinit();

    var ids: std.ArrayList(i64) = .empty;
    defer ids.deinit(allocator);
    while (try q.next()) |batch| {
        for (batch.values[0].data.bigint[0..batch.row_count]) |v| try ids.append(allocator, v);
    }
    try std.testing.expectEqualSlices(i64, &[_]i64{ 2, 3 }, ids.items);
}

test "plan: multi-branched — join two independently scanned tables" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    _ = try seedTables(db);

    var pb = PlanBuilder.init(allocator);
    defer pb.deinit();

    const a_branch = try pb.scan("a");
    const b_branch = try pb.scan("b");
    const joined = try pb.join(a_branch, b_branch, .{
        .on = &.{.{ .left = "k", .right = "k" }},
        .algorithm = .auto,
    });

    var q = try pb.compile(allocator, db,joined);
    defer q.deinit();

    var rows: usize = 0;
    while (try q.next()) |batch| rows += batch.row_count;
    // a has 3 rows; b has 3 (one without a match). Inner join → 2 rows.
    try std.testing.expectEqual(@as(usize, 2), rows);
}

test "plan: filter both branches independently before joining" {
    // Different predicates on each side — exactly the kind of plan a
    // SQL parser would emit for `FROM a JOIN b ON a.k=b.k WHERE a.k>=200 AND b.qty>=5`.
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    _ = try seedTables(db);

    var pb = PlanBuilder.init(allocator);
    defer pb.deinit();

    const a_filtered = try pb.filter(
        try pb.scan("a"),
        .{ .leaf = .{ .col = "k", .op = .gte, .val = .{ .int = 200 } } },
    );
    const b_filtered = try pb.filter(
        try pb.scan("b"),
        .{ .leaf = .{ .col = "qty", .op = .gte, .val = .{ .int = 5 } } },
    );
    const joined = try pb.join(a_filtered, b_filtered, .{
        .on = &.{.{ .left = "k", .right = "k" }},
        .algorithm = .auto,
    });

    var q = try pb.compile(allocator, db,joined);
    defer q.deinit();

    var matched: usize = 0;
    while (try q.next()) |batch| matched += batch.row_count;
    // Only k=200 (a.id=2 ↔ b.bid=11, qty=7) survives both filters.
    try std.testing.expectEqual(@as(usize, 1), matched);
}

test "plan: join-of-join (A ⋈ B) ⋈ C" {
    // Three-table join — chains the join operator: first join A and B
    // on a.k=b.k, then join the resulting (a.label) against C on
    // a.label=c.label. The right side of the outer join is a sibling
    // branch built independently from the left.
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    _ = try seedTables(db);

    var pb = PlanBuilder.init(allocator);
    defer pb.deinit();

    const ab = try pb.join(
        try pb.scan("a"),
        try pb.scan("b"),
        .{ .on = &.{.{ .left = "k", .right = "k" }}, .algorithm = .auto },
    );
    const abc = try pb.join(
        ab,
        try pb.scan("c"),
        .{
            .on = &.{.{ .left = "label", .right = "label" }},
            .algorithm = .auto,
        },
    );

    var q = try pb.compile(allocator, db,abc);
    defer q.deinit();

    var matched: usize = 0;
    while (try q.next()) |batch| matched += batch.row_count;
    // a×b inner join: 2 rows (alpha, beta). Joined to c on label:
    // alpha matches c.alpha; beta has no c row → 1 row.
    try std.testing.expectEqual(@as(usize, 1), matched);
}

test "plan: compute on one branch before joining + aggregate over result" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    _ = try seedTables(db);

    var pb = PlanBuilder.init(allocator);
    defer pb.deinit();
    const aa = pb.arenaAllocator();

    // Left branch: scan a, compute upper(label) as `label_u`.
    const upper_expr = try thindb.exec.scalar_fn.upper(
        aa,
        thindb.exec.expr_mod.col("label"),
    );
    const a_with_upper = try pb.compute(
        try pb.scan("a"),
        &.{.{ .name = "label_u", .expr = upper_expr }},
    );

    // Right branch: scan c.
    const c_branch = try pb.scan("c");

    // Join on the upper-cased label (a side) vs c.label (which we'll
    // upper-case on the c side too to make this test deterministic).
    const c_upper_expr = try thindb.exec.scalar_fn.upper(
        aa,
        thindb.exec.expr_mod.col("label"),
    );
    const c_with_upper = try pb.compute(
        c_branch,
        &.{.{ .name = "c_label_u", .expr = c_upper_expr }},
    );

    // Pre-strip the original `label` from c to avoid a collision in the
    // join output (c.label + a.label would both surface).
    const c_clean = try pb.exclude(c_with_upper, &.{"label"});

    const joined = try pb.join(a_with_upper, c_clean, .{
        .on = &.{.{ .left = "label_u", .right = "c_label_u" }},
        .algorithm = .auto,
    });

    // Aggregate: count joined rows.
    const counted = try pb.groupBy(joined, &.{}, &.{.{ .func = .count, .as = "n" }});

    var q = try pb.compile(allocator, db,counted);
    defer q.deinit();

    const batch = (try q.next()).?;
    try std.testing.expectEqual(@as(usize, 1), batch.row_count);
    // a has labels {alpha, beta, gamma}; c has {alpha, gamma, zeta}.
    // upper-case both → 2 matches.
    try std.testing.expectEqual(@as(i64, 2), batch.values[0].data.bigint[0]);
}

test "plan: explain renders linear pipeline as indented text" {
    const allocator = std.testing.allocator;
    var pb = PlanBuilder.init(allocator);
    defer pb.deinit();

    const root = try pb.select(
        try pb.filter(
            try pb.scan("orders"),
            .{ .leaf = .{ .col = "qty", .op = .gt, .val = .{ .int = 5 } } },
        ),
        &.{ "id", "qty" },
    );
    const text = try pb.explain(root);
    try std.testing.expectEqualStrings(
        \\Select [id, qty]
        \\  Filter (qty > 5)
        \\    Scan orders
        \\
    , text);
}

test "plan: explain renders multi-branched join with both sides" {
    const allocator = std.testing.allocator;
    var pb = PlanBuilder.init(allocator);
    defer pb.deinit();

    const root = try pb.join(
        try pb.filter(
            try pb.scan("a"),
            .{ .leaf = .{ .col = "k", .op = .gte, .val = .{ .int = 200 } } },
        ),
        try pb.scan("b"),
        .{ .on = &.{.{ .left = "k", .right = "k" }}, .algorithm = .auto },
    );
    const text = try pb.explain(root);
    try std.testing.expectEqualStrings(
        \\Join algorithm=auto type=inner on=[k=k]
        \\  Filter (k >= 200)
        \\    Scan a
        \\  Scan b
        \\
    , text);
}

test "plan: explain renders join-of-join with nested indentation" {
    const allocator = std.testing.allocator;
    var pb = PlanBuilder.init(allocator);
    defer pb.deinit();

    const ab = try pb.join(
        try pb.scan("a"),
        try pb.scan("b"),
        .{ .on = &.{.{ .left = "k", .right = "k" }}, .algorithm = .auto },
    );
    const abc = try pb.join(
        ab,
        try pb.scan("c"),
        .{
            .on = &.{.{ .left = "label", .right = "label" }},
            .algorithm = .sort_merge,
        },
    );
    const root = try pb.groupBy(abc, &.{}, &.{.{ .func = .count, .as = "n" }});

    const text = try pb.explain(root);
    try std.testing.expectEqualStrings(
        \\GroupBy keys=[] aggs=[count(*) AS n]
        \\  Join algorithm=sort_merge type=inner on=[label=label]
        \\    Join algorithm=auto type=inner on=[k=k]
        \\      Scan a
        \\      Scan b
        \\    Scan c
        \\
    , text);
}

test "plan: explain renders compute with call expression" {
    const allocator = std.testing.allocator;
    var pb = PlanBuilder.init(allocator);
    defer pb.deinit();
    const aa = pb.arenaAllocator();

    const upper_expr = try thindb.exec.scalar_fn.upper(aa, thindb.exec.expr_mod.col("name"));
    const root = try pb.compute(
        try pb.scan("users"),
        &.{.{ .name = "u", .expr = upper_expr }},
    );
    const text = try pb.explain(root);
    try std.testing.expectEqualStrings(
        \\Compute [u := upper(name)]
        \\  Scan users
        \\
    , text);
}

test "plan: CTE-style subtree shared across two parents (inline semantics)" {
    // SQL equivalent: WITH big_a AS (SELECT * FROM a WHERE k >= 200)
    //                 SELECT * FROM big_a x JOIN big_a y ON x.k = y.k
    //
    // Same filtered-scan subtree appears on BOTH sides of the join.
    // Current PlanBuilder API allows multiple parents to reference the
    // same *ir.Op — compile() walks the IR read-only and instantiates
    // independent exec operators per call site, so the shared node
    // produces independent runtime instances. Confirms "inline-CTE"
    // semantics work without explicit materialization.
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    _ = try seedTables(db);

    var pb = PlanBuilder.init(allocator);
    defer pb.deinit();

    // The shared CTE: rows from `a` with k >= 200. Built once.
    const big_a = try pb.filter(
        try pb.scan("a"),
        .{ .leaf = .{ .col = "k", .op = .gte, .val = .{ .int = 200 } } },
    );

    // Self-join the CTE on k. Need to compute a renamed `k` on the
    // right side to avoid the existing JoinColumnNameCollision (left
    // and right both have a `label` and `id` too — we project the
    // right side first to be a single renamed column).
    const big_a_right = try pb.select(big_a, &.{"k"});
    const joined = try pb.join(big_a, big_a_right, .{
        .on = &.{.{ .left = "k", .right = "k" }},
        .algorithm = .auto,
    });

    var q = try pb.compile(allocator, db, joined);
    defer q.deinit();

    var rows: usize = 0;
    while (try q.next()) |batch| rows += batch.row_count;
    // big_a has 2 rows (k=200, k=300). Self-join on k → 2 matched
    // pairs (each row matches itself on the inner join).
    try std.testing.expectEqual(@as(usize, 2), rows);
}

test "plan: explain shows shared subtree expanded at both reference sites" {
    // The IR has tree-shape — when a node is referenced from two
    // parents, explain renders it twice (inline expansion). Document
    // the behavior so future readers know what to expect.
    const allocator = std.testing.allocator;
    var pb = PlanBuilder.init(allocator);
    defer pb.deinit();

    const shared = try pb.scan("a");
    const right_branch = try pb.select(shared, &.{"k"});
    const root = try pb.join(shared, right_branch, .{
        .on = &.{.{ .left = "k", .right = "k" }},
        .algorithm = .auto,
    });
    const text = try pb.explain(root);
    try std.testing.expectEqualStrings(
        \\Join algorithm=auto type=inner on=[k=k]
        \\  Scan a
        \\  Select [k]
        \\    Scan a
        \\
    , text);
}

test "plan: same PlanBuilder produces two independent Queries" {
    // Compile a plan twice — each call returns its own owning Query.
    // Confirms the plan tree itself is reusable and doesn't get
    // consumed by compile().
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();
    _ = try seedTables(db);

    var pb = PlanBuilder.init(allocator);
    defer pb.deinit();

    const root = try pb.scan("a");

    var q1 = try pb.compile(allocator, db, root);
    defer q1.deinit();
    var n1: usize = 0;
    while (try q1.next()) |b| n1 += b.row_count;

    var q2 = try pb.compile(allocator, db, root);
    defer q2.deinit();
    var n2: usize = 0;
    while (try q2.next()) |b| n2 += b.row_count;

    try std.testing.expectEqual(@as(usize, 3), n1);
    try std.testing.expectEqual(n1, n2);
}
