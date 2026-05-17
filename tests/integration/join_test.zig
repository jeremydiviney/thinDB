//! Join operator integration tests. v1 covers inner equi-join via
//! hash algorithm with automatic build-side selection.
//!
//! Future tests (as features land): SMJ, INLJ, NLJ, outer joins,
//! semi/anti, multi-column keys, type-mismatch errors, etc.

const std = @import("std");
const thindb = @import("thindb");

const users_schema = thindb.Schema{
    .columns = &.{
        .{ .name = "uid", .type = .bigint },
        .{ .name = "name", .type = .string },
    },
    .order_key = &.{"uid"},
    .unique = true,
};
const users_ok = [_][]const u8{"uid"};
const users_opts = thindb.TableOptions{
    .order_key = &users_ok,
    .unique = true,
    .row_group_size = 4,
};

// orders shares no column names with users → no collision on join.
// Join key is `uid` on both sides — right-side `uid` is dropped from
// output per USING-semantic.
const orders_schema = thindb.Schema{
    .columns = &.{
        .{ .name = "oid", .type = .bigint },
        .{ .name = "uid", .type = .bigint },
        .{ .name = "qty", .type = .int },
    },
    .order_key = &.{"oid"},
    .unique = true,
};
const orders_ok = [_][]const u8{"oid"};
const orders_opts = thindb.TableOptions{
    .order_key = &orders_ok,
    .unique = true,
    .row_group_size = 4,
};

test "join: inner equi-join with single key returns matching rows" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    const users = try db.table("users", users_schema, users_opts);
    try users.insert(&.{
        .{ .uid = @as(i64, 1), .name = "alice" },
        .{ .uid = @as(i64, 2), .name = "bob" },
        .{ .uid = @as(i64, 3), .name = "carol" },
    });
    try users.flush();

    const orders = try db.table("orders", orders_schema, orders_opts);
    try orders.insert(&.{
        .{ .oid = @as(i64, 100), .uid = @as(i64, 1), .qty = @as(i32, 10) },
        .{ .oid = @as(i64, 101), .uid = @as(i64, 1), .qty = @as(i32, 20) },
        .{ .oid = @as(i64, 102), .uid = @as(i64, 2), .qty = @as(i32, 30) },
        .{ .oid = @as(i64, 103), .uid = @as(i64, 99), .qty = @as(i32, 40) }, // no matching user
    });
    try orders.flush();

    const left = try thindb.scan(allocator, users);
    const right = try thindb.scan(allocator, orders);
    var q = try left.join(right, .{
        .join_type = .inner,
        .on = &.{.{ .left = "uid", .right = "uid" }},
    });
    defer q.deinit();

    // Output schema: users.uid, users.name, orders.oid, orders.qty
    // (orders.uid dropped per USING-clause semantics)
    const schema = q.outputSchema();
    try std.testing.expectEqual(@as(usize, 4), schema.len);
    try std.testing.expectEqualStrings("uid", schema[0].name);
    try std.testing.expectEqualStrings("name", schema[1].name);
    try std.testing.expectEqualStrings("oid", schema[2].name);
    try std.testing.expectEqualStrings("qty", schema[3].name);

    // Collect output rows.
    var uids: std.ArrayList(i64) = .empty;
    defer uids.deinit(allocator);
    var oids: std.ArrayList(i64) = .empty;
    defer oids.deinit(allocator);
    var qtys: std.ArrayList(i32) = .empty;
    defer qtys.deinit(allocator);

    while (try q.next()) |b| {
        try uids.appendSlice(allocator, b.values[0].data.bigint[0..b.row_count]);
        try oids.appendSlice(allocator, b.values[2].data.bigint[0..b.row_count]);
        try qtys.appendSlice(allocator, b.values[3].data.int[0..b.row_count]);
    }

    // Expected output (any order — sort to verify):
    //   (uid=1, alice, oid=100, qty=10)
    //   (uid=1, alice, oid=101, qty=20)
    //   (uid=2, bob,   oid=102, qty=30)
    // orders.uid=99 has no matching user → dropped.
    try std.testing.expectEqual(@as(usize, 3), uids.items.len);

    // Sort the three parallel arrays by oid (the order is non-
    // deterministic depending on hash iteration). Bubble sort is
    // fine for n=3.
    var i: usize = 0;
    while (i < uids.items.len) : (i += 1) {
        var j: usize = i + 1;
        while (j < uids.items.len) : (j += 1) {
            if (oids.items[j] < oids.items[i]) {
                std.mem.swap(i64, &uids.items[i], &uids.items[j]);
                std.mem.swap(i64, &oids.items[i], &oids.items[j]);
                std.mem.swap(i32, &qtys.items[i], &qtys.items[j]);
            }
        }
    }

    try std.testing.expectEqualSlices(i64, &[_]i64{ 1, 1, 2 }, uids.items);
    try std.testing.expectEqualSlices(i64, &[_]i64{ 100, 101, 102 }, oids.items);
    try std.testing.expectEqualSlices(i32, &[_]i32{ 10, 20, 30 }, qtys.items);
}

test "join: NULL join key never matches" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    // Use nullable join key on the orders side.
    const orders_nullable = thindb.Schema{
        .columns = &.{
            .{ .name = "oid", .type = .bigint },
            .{ .name = "uid", .type = .bigint, .nullable = true },
            .{ .name = "qty", .type = .int },
        },
        .order_key = &.{"oid"},
        .unique = true,
    };
    const ok = [_][]const u8{"oid"};
    const opts = thindb.TableOptions{ .order_key = &ok, .unique = true, .row_group_size = 8 };

    const users = try db.table("users", users_schema, users_opts);
    try users.insert(&.{
        .{ .uid = @as(i64, 1), .name = "alice" },
    });
    try users.flush();

    const orders = try db.table("orders", orders_nullable, opts);
    try orders.insert(&[_]struct { oid: i64, uid: ?i64, qty: i32 }{
        .{ .oid = 100, .uid = 1, .qty = 10 },
        .{ .oid = 101, .uid = null, .qty = 20 }, // null uid → no match
    });
    try orders.flush();

    const left = try thindb.scan(allocator, users);
    const right = try thindb.scan(allocator, orders);
    var q = try left.join(right, .{
        .on = &.{.{ .left = "uid", .right = "uid" }},
    });
    defer q.deinit();

    var rows: usize = 0;
    while (try q.next()) |b| rows += b.row_count;
    // Only oid=100 matches; oid=101 has null uid which doesn't match.
    try std.testing.expectEqual(@as(usize, 1), rows);
}

test "join: empty build side produces empty output" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    const users = try db.table("users", users_schema, users_opts);
    // No users inserted.

    const orders = try db.table("orders", orders_schema, orders_opts);
    try orders.insert(&.{
        .{ .oid = @as(i64, 100), .uid = @as(i64, 1), .qty = @as(i32, 10) },
    });
    try orders.flush();

    const left = try thindb.scan(allocator, users);
    const right = try thindb.scan(allocator, orders);
    var q = try left.join(right, .{
        .on = &.{.{ .left = "uid", .right = "uid" }},
    });
    defer q.deinit();

    var rows: usize = 0;
    while (try q.next()) |b| rows += b.row_count;
    try std.testing.expectEqual(@as(usize, 0), rows);
}

test "join: sort-merge algorithm produces same result as hash" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    const users = try db.table("users", users_schema, users_opts);
    try users.insert(&.{
        .{ .uid = @as(i64, 1), .name = "alice" },
        .{ .uid = @as(i64, 2), .name = "bob" },
        .{ .uid = @as(i64, 3), .name = "carol" },
    });
    try users.flush();

    const orders = try db.table("orders", orders_schema, orders_opts);
    try orders.insert(&.{
        .{ .oid = @as(i64, 100), .uid = @as(i64, 1), .qty = @as(i32, 10) },
        .{ .oid = @as(i64, 101), .uid = @as(i64, 2), .qty = @as(i32, 20) },
        .{ .oid = @as(i64, 102), .uid = @as(i64, 1), .qty = @as(i32, 30) },
        .{ .oid = @as(i64, 103), .uid = @as(i64, 99), .qty = @as(i32, 40) },
    });
    try orders.flush();

    const left = try thindb.scan(allocator, users);
    const right = try thindb.scan(allocator, orders);
    var q = try left.join(right, .{
        .join_type = .inner,
        .on = &.{.{ .left = "uid", .right = "uid" }},
        .algorithm = .sort_merge,
    });
    defer q.deinit();

    // Collect output: should match hash-join result (3 rows).
    var uids: std.ArrayList(i64) = .empty;
    defer uids.deinit(allocator);
    var oids: std.ArrayList(i64) = .empty;
    defer oids.deinit(allocator);
    var qtys: std.ArrayList(i32) = .empty;
    defer qtys.deinit(allocator);

    while (try q.next()) |b| {
        try uids.appendSlice(allocator, b.values[0].data.bigint[0..b.row_count]);
        try oids.appendSlice(allocator, b.values[2].data.bigint[0..b.row_count]);
        try qtys.appendSlice(allocator, b.values[3].data.int[0..b.row_count]);
    }

    try std.testing.expectEqual(@as(usize, 3), uids.items.len);

    // SMJ emits in sorted-by-key order. The two uid=1 rows come first,
    // then uid=2. Within uid=1, internal order depends on orders' scan
    // order which is by oid (100 then 102 — both pre-sorted). So:
    // (uid=1, oid=100), (uid=1, oid=102), (uid=2, oid=101).
    try std.testing.expectEqualSlices(i64, &[_]i64{ 1, 1, 2 }, uids.items);
    try std.testing.expectEqualSlices(i64, &[_]i64{ 100, 102, 101 }, oids.items);
    try std.testing.expectEqualSlices(i32, &[_]i32{ 10, 30, 20 }, qtys.items);
}

test "join: .auto algorithm produces correct results (hash for un-sorted inputs)" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    const users = try db.table("users", users_schema, users_opts);
    try users.insert(&.{
        .{ .uid = @as(i64, 1), .name = "alice" },
        .{ .uid = @as(i64, 2), .name = "bob" },
    });
    try users.flush();

    const orders = try db.table("orders", orders_schema, orders_opts);
    try orders.insert(&.{
        .{ .oid = @as(i64, 100), .uid = @as(i64, 1), .qty = @as(i32, 10) },
        .{ .oid = @as(i64, 101), .uid = @as(i64, 2), .qty = @as(i32, 20) },
    });
    try orders.flush();

    // Default spec uses `.auto` algorithm. Scan publishes
    // sort_state.global = false (pre-compaction; segments may
    // overlap), so the decision tree's "both pre-sorted globally"
    // condition is NOT met. Falls through to hash. Works fine.
    const left = try thindb.scan(allocator, users);
    const right = try thindb.scan(allocator, orders);
    var q = try left.join(right, .{
        .on = &.{.{ .left = "uid", .right = "uid" }},
        // .algorithm defaults to .auto
    });
    defer q.deinit();

    var rows: usize = 0;
    while (try q.next()) |b| rows += b.row_count;
    try std.testing.expectEqual(@as(usize, 2), rows);
}

test "join: .auto picks SMJ when both inputs come from a matching OrderBy" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    const users = try db.table("users", users_schema, users_opts);
    try users.insert(&.{
        .{ .uid = @as(i64, 1), .name = "alice" },
        .{ .uid = @as(i64, 2), .name = "bob" },
        .{ .uid = @as(i64, 3), .name = "carol" },
    });
    try users.flush();

    const orders = try db.table("orders", orders_schema, orders_opts);
    try orders.insert(&.{
        .{ .oid = @as(i64, 100), .uid = @as(i64, 1), .qty = @as(i32, 10) },
        .{ .oid = @as(i64, 101), .uid = @as(i64, 2), .qty = @as(i32, 20) },
        .{ .oid = @as(i64, 102), .uid = @as(i64, 3), .qty = @as(i32, 30) },
    });
    try orders.flush();

    // Wrap each scan in an explicit OrderBy on `uid`. Sort publishes
    // sort_state.global = true. The .auto algorithm should pick SMJ
    // (the merge-only path is essentially free here — though v1 still
    // re-sorts, the result is correct).
    var left_base = try thindb.scan(allocator, users);
    const left_sorted = try left_base.orderBy(&.{.{ .col = "uid", .desc = false }});
    var right_base = try thindb.scan(allocator, orders);
    const right_sorted = try right_base.orderBy(&.{.{ .col = "uid", .desc = false }});

    var q = try left_sorted.join(right_sorted, .{
        .on = &.{.{ .left = "uid", .right = "uid" }},
    });
    defer q.deinit();

    var uids: std.ArrayList(i64) = .empty;
    defer uids.deinit(allocator);
    var oids: std.ArrayList(i64) = .empty;
    defer oids.deinit(allocator);

    while (try q.next()) |b| {
        try uids.appendSlice(allocator, b.values[0].data.bigint[0..b.row_count]);
        try oids.appendSlice(allocator, b.values[2].data.bigint[0..b.row_count]);
    }
    // Output should be sorted on uid (SMJ side-effect): 1, 2, 3.
    try std.testing.expectEqualSlices(i64, &[_]i64{ 1, 2, 3 }, uids.items);
    try std.testing.expectEqualSlices(i64, &[_]i64{ 100, 101, 102 }, oids.items);
}

test "join: sort-merge handles duplicate keys on both sides (Cartesian per key)" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    // Both sides have multiple rows per join key, so the SMJ inner
    // Cartesian product per key has to fire correctly.
    const a_schema = thindb.Schema{
        .columns = &.{
            .{ .name = "rowid", .type = .bigint },
            .{ .name = "k", .type = .int },
            .{ .name = "lval", .type = .string },
        },
        .order_key = &.{"rowid"},
        .unique = true,
    };
    const b_schema = thindb.Schema{
        .columns = &.{
            .{ .name = "b_rowid", .type = .bigint }, // distinct name to avoid join-collision
            .{ .name = "k_other", .type = .int },
            .{ .name = "rval", .type = .string },
        },
        .order_key = &.{"b_rowid"},
        .unique = true,
    };
    const a_ok = [_][]const u8{"rowid"};
    const b_ok = [_][]const u8{"b_rowid"};

    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    const a = try db.table("a", a_schema, .{ .order_key = &a_ok, .unique = true });
    try a.insert(&.{
        .{ .rowid = @as(i64, 1), .k = @as(i32, 1), .lval = @as([]const u8, "L1a") },
        .{ .rowid = @as(i64, 2), .k = @as(i32, 1), .lval = @as([]const u8, "L1b") },
        .{ .rowid = @as(i64, 3), .k = @as(i32, 2), .lval = @as([]const u8, "L2") },
    });
    try a.flush();

    const b = try db.table("b", b_schema, .{ .order_key = &b_ok, .unique = true });
    try b.insert(&.{
        .{ .b_rowid = @as(i64, 1), .k_other = @as(i32, 1), .rval = @as([]const u8, "R1a") },
        .{ .b_rowid = @as(i64, 2), .k_other = @as(i32, 1), .rval = @as([]const u8, "R1b") },
        .{ .b_rowid = @as(i64, 3), .k_other = @as(i32, 1), .rval = @as([]const u8, "R1c") },
    });
    try b.flush();

    // a × b on (a.k = b.k_other):
    //   k=1 (L=2 rows, R=3 rows) → 6 output rows
    //   k=2 (L=1 row, R=0 rows) → 0 output rows
    // Total: 6 rows
    const left = try thindb.scan(allocator, a);
    const right = try thindb.scan(allocator, b);
    var q = try left.join(right, .{
        .on = &.{.{ .left = "k", .right = "k_other" }},
        .algorithm = .sort_merge,
    });
    defer q.deinit();

    var total: usize = 0;
    while (try q.next()) |bat| total += bat.row_count;
    try std.testing.expectEqual(@as(usize, 6), total);
}

test "scan: sort_state.global tracks segment overlap via manifest v2 stats" {
    // Verifies Scan.stats() — the stat surface used by .auto's
    // decision tree — reports global=true when the scan's output is
    // guaranteed sorted by the order key. Manifest v2 stores per-
    // segment leading-key min/max so Scan can prove non-overlap
    // across multiple segments without opening any file.
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    const users = try db.table("users", users_schema, users_opts);

    // Empty table: 0 segments, empty memtable → globally sorted.
    {
        var q = try thindb.scan(allocator, users);
        defer q.deinit();
        const s = q.stats().sort_state;
        try std.testing.expect(s.global);
        try std.testing.expectEqual(@as(usize, 1), s.keys.len);
        try std.testing.expectEqualStrings("uid", s.keys[0]);
    }

    // Single segment, empty memtable → globally sorted.
    try users.insert(&.{.{ .uid = @as(i64, 1), .name = "alice" }});
    try users.flush();
    {
        var q = try thindb.scan(allocator, users);
        defer q.deinit();
        try std.testing.expect(q.stats().sort_state.global);
    }

    // Two segments with disjoint ranges in manifest order
    // (seg1=[1], seg2=[2]) → still globally sorted thanks to v2 stats.
    try users.insert(&.{.{ .uid = @as(i64, 2), .name = "bob" }});
    try users.flush();
    {
        var q = try thindb.scan(allocator, users);
        defer q.deinit();
        try std.testing.expect(q.stats().sort_state.global);
    }

    // Compact merges into one segment — still global=true.
    try users.compact();
    {
        var q = try thindb.scan(allocator, users);
        defer q.deinit();
        try std.testing.expect(q.stats().sort_state.global);
    }

    // Non-empty memtable on top of a sorted segment → global=false:
    // the memtable would emit as an unordered trailing batch.
    try users.insert(&.{.{ .uid = @as(i64, 3), .name = "carol" }});
    {
        var q = try thindb.scan(allocator, users);
        defer q.deinit();
        try std.testing.expect(!q.stats().sort_state.global);
    }
}

test "scan: sort_state.global false when segments overlap" {
    // Two segments whose leading-key ranges overlap → scan output is
    // NOT globally sorted (cross-segment key values interleave).
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    const users = try db.table("users", users_schema, users_opts);

    try users.insert(&.{
        .{ .uid = @as(i64, 1), .name = "alice" },
        .{ .uid = @as(i64, 5), .name = "elaine" },
    });
    try users.flush();
    try users.insert(&.{
        .{ .uid = @as(i64, 3), .name = "carol" }, // 3 falls within [1, 5]
        .{ .uid = @as(i64, 7), .name = "george" },
    });
    try users.flush();

    var q = try thindb.scan(allocator, users);
    defer q.deinit();
    try std.testing.expect(!q.stats().sort_state.global);
}

test "scan: sort_state.global false when segments are in wrong manifest order" {
    // Non-overlapping segment ranges but emitted in reverse manifest
    // order: seg_old has higher leading-key values than seg_new.
    // Scan emits seg_old first → output is not globally sorted.
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    const users = try db.table("users", users_schema, users_opts);

    try users.insert(&.{ .{ .uid = @as(i64, 100), .name = "a" }, .{ .uid = @as(i64, 101), .name = "b" } });
    try users.flush();
    try users.insert(&.{ .{ .uid = @as(i64, 1), .name = "c" }, .{ .uid = @as(i64, 2), .name = "d" } });
    try users.flush();

    var q = try thindb.scan(allocator, users);
    defer q.deinit();
    try std.testing.expect(!q.stats().sort_state.global);
}

test "scan: string-keyed multi-segment table reports global=true when disjoint" {
    // Manifest v2 now stores prefix-encoded leading-key stats for
    // string columns, so non-overlap detection works without compact().
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const slug_schema = thindb.Schema{
        .columns = &.{
            .{ .name = "slug", .type = .string },
            .{ .name = "payload", .type = .int },
        },
        .order_key = &.{"slug"},
        .unique = true,
    };
    const slug_ok = [_][]const u8{"slug"};

    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    const t = try db.table("posts", slug_schema, .{ .order_key = &slug_ok, .unique = true });

    // Two flushes producing disjoint string ranges in manifest order.
    try t.insert(&.{
        .{ .slug = @as([]const u8, "alpha"), .payload = @as(i32, 1) },
        .{ .slug = @as([]const u8, "beta"), .payload = @as(i32, 2) },
    });
    try t.flush();
    try t.insert(&.{
        .{ .slug = @as([]const u8, "charlie"), .payload = @as(i32, 3) },
        .{ .slug = @as([]const u8, "delta"), .payload = @as(i32, 4) },
    });
    try t.flush();

    var q = try thindb.scan(allocator, t);
    defer q.deinit();
    try std.testing.expect(q.stats().sort_state.global);
}

test "scan: string-keyed multi-segment table reports global=false when overlapping" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const slug_schema = thindb.Schema{
        .columns = &.{
            .{ .name = "slug", .type = .string },
            .{ .name = "payload", .type = .int },
        },
        .order_key = &.{"slug"},
        .unique = true,
    };
    const slug_ok = [_][]const u8{"slug"};

    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    const t = try db.table("posts", slug_schema, .{ .order_key = &slug_ok, .unique = true });

    // Overlapping ranges: seg1 covers ["alpha", "delta"], seg2 covers
    // ["bravo", "echo"]. They interleave on prefix, so scan output
    // can't be globally sorted.
    try t.insert(&.{
        .{ .slug = @as([]const u8, "alpha"), .payload = @as(i32, 1) },
        .{ .slug = @as([]const u8, "delta"), .payload = @as(i32, 2) },
    });
    try t.flush();
    try t.insert(&.{
        .{ .slug = @as([]const u8, "bravo"), .payload = @as(i32, 3) },
        .{ .slug = @as([]const u8, "echo"), .payload = @as(i32, 4) },
    });
    try t.flush();

    var q = try thindb.scan(allocator, t);
    defer q.deinit();
    try std.testing.expect(!q.stats().sort_state.global);
}

test "join: .auto picks SMJ for string-keyed multi-segment tables joined on slug" {
    // End-to-end: two string-keyed tables with disjoint multi-flush
    // ranges joined on the order key. .auto's decision tree should
    // pick SMJ (output emitted in join-key sort order).
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const post_schema = thindb.Schema{
        .columns = &.{
            .{ .name = "slug", .type = .string },
            .{ .name = "title", .type = .string },
        },
        .order_key = &.{"slug"},
        .unique = true,
    };
    const author_schema = thindb.Schema{
        .columns = &.{
            .{ .name = "slug", .type = .string },
            .{ .name = "author", .type = .string },
        },
        .order_key = &.{"slug"},
        .unique = true,
    };
    const post_ok = [_][]const u8{"slug"};
    const author_ok = [_][]const u8{"slug"};

    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    const posts = try db.table("posts", post_schema, .{ .order_key = &post_ok, .unique = true });
    try posts.insert(&.{
        .{ .slug = @as([]const u8, "alpha"), .title = @as([]const u8, "A") },
        .{ .slug = @as([]const u8, "beta"), .title = @as([]const u8, "B") },
    });
    try posts.flush();
    try posts.insert(&.{
        .{ .slug = @as([]const u8, "charlie"), .title = @as([]const u8, "C") },
    });
    try posts.flush();

    const authors = try db.table("authors", author_schema, .{ .order_key = &author_ok, .unique = true });
    try authors.insert(&.{
        .{ .slug = @as([]const u8, "alpha"), .author = @as([]const u8, "Alice") },
    });
    try authors.flush();
    try authors.insert(&.{
        .{ .slug = @as([]const u8, "beta"), .author = @as([]const u8, "Bob") },
        .{ .slug = @as([]const u8, "charlie"), .author = @as([]const u8, "Carol") },
    });
    try authors.flush();

    // Both sides should report global=true → .auto picks SMJ → output
    // is sorted by slug.
    {
        var pq = try thindb.scan(allocator, posts);
        defer pq.deinit();
        try std.testing.expect(pq.stats().sort_state.global);
        var aq = try thindb.scan(allocator, authors);
        defer aq.deinit();
        try std.testing.expect(aq.stats().sort_state.global);
    }

    // First with explicit SMJ to know the expected output ordering.
    var smj_slugs: std.ArrayList(u8) = .empty;
    defer smj_slugs.deinit(allocator);
    {
        const left = try thindb.scan(allocator, posts);
        const right = try thindb.scan(allocator, authors);
        var q = try left.join(right, .{
            .on = &.{.{ .left = "slug", .right = "slug" }},
            .algorithm = .sort_merge,
        });
        defer q.deinit();
        while (try q.next()) |b| {
            for (0..b.row_count) |i| {
                try smj_slugs.appendSlice(allocator, b.values[0].data.string.rowBytes(i));
                try smj_slugs.append(allocator, '|');
            }
        }
    }

    // Then with default .auto — output should match SMJ exactly.
    var auto_slugs: std.ArrayList(u8) = .empty;
    defer auto_slugs.deinit(allocator);
    {
        const left = try thindb.scan(allocator, posts);
        const right = try thindb.scan(allocator, authors);
        var q = try left.join(right, .{
            .on = &.{.{ .left = "slug", .right = "slug" }},
        });
        defer q.deinit();
        while (try q.next()) |b| {
            for (0..b.row_count) |i| {
                try auto_slugs.appendSlice(allocator, b.values[0].data.string.rowBytes(i));
                try auto_slugs.append(allocator, '|');
            }
        }
    }

    try std.testing.expectEqualStrings(smj_slugs.items, auto_slugs.items);
}

test "join: .auto picks SMJ for post-compaction tables joined on order key" {
    // End-to-end: two tables, each compacted to a single segment,
    // joined on their order key. .auto's decision tree should pick
    // SMJ — observable via the output ordering (SMJ emits in
    // join-key order; hash join is unordered).
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const emails_schema = thindb.Schema{
        .columns = &.{
            .{ .name = "uid", .type = .bigint },
            .{ .name = "email", .type = .string },
        },
        .order_key = &.{"uid"},
        .unique = true,
    };
    const emails_ok = [_][]const u8{"uid"};

    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    const users = try db.table("users", users_schema, users_opts);
    // Two flushes → two segments.
    try users.insert(&.{
        .{ .uid = @as(i64, 3), .name = "carol" },
        .{ .uid = @as(i64, 1), .name = "alice" },
    });
    try users.flush();
    try users.insert(&.{.{ .uid = @as(i64, 2), .name = "bob" }});
    try users.flush();
    try users.compact();

    const emails = try db.table("emails", emails_schema, .{
        .order_key = &emails_ok,
        .unique = true,
    });
    try emails.insert(&.{
        .{ .uid = @as(i64, 2), .email = @as([]const u8, "b@x") },
        .{ .uid = @as(i64, 1), .email = @as([]const u8, "a@x") },
    });
    try emails.flush();
    try emails.insert(&.{.{ .uid = @as(i64, 3), .email = @as([]const u8, "c@x") }});
    try emails.flush();
    try emails.compact();

    const left = try thindb.scan(allocator, users);
    const right = try thindb.scan(allocator, emails);
    var q = try left.join(right, .{
        .on = &.{.{ .left = "uid", .right = "uid" }},
    });
    defer q.deinit();

    var uids: std.ArrayList(i64) = .empty;
    defer uids.deinit(allocator);
    while (try q.next()) |b| {
        try uids.appendSlice(allocator, b.values[0].data.bigint[0..b.row_count]);
    }
    try std.testing.expectEqualSlices(i64, &[_]i64{ 1, 2, 3 }, uids.items);
}

test "join: type mismatch on join key errors" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const schema_a = thindb.Schema{
        .columns = &.{
            .{ .name = "a_id", .type = .bigint },
            .{ .name = "a_val", .type = .string },
        },
        .order_key = &.{"a_id"},
        .unique = true,
    };
    const schema_b = thindb.Schema{
        .columns = &.{
            .{ .name = "b_id", .type = .int }, // i32, not i64
            .{ .name = "b_val", .type = .string },
        },
        .order_key = &.{"b_id"},
        .unique = true,
    };
    const ok_a = [_][]const u8{"a_id"};
    const ok_b = [_][]const u8{"b_id"};

    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    const a = try db.table("a", schema_a, .{ .order_key = &ok_a, .unique = true });
    const b = try db.table("b", schema_b, .{ .order_key = &ok_b, .unique = true });

    var left = try thindb.scan(allocator, a);
    var right = try thindb.scan(allocator, b);
    try std.testing.expectError(
        thindb.exec.Error.JoinKeyTypeMismatch,
        left.join(right, .{
            .on = &.{.{ .left = "a_id", .right = "b_id" }},
        }),
    );

    // Clean up the queries that didn't get consumed by join.
    left.deinit();
    right.deinit();
}
