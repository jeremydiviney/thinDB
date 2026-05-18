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
    // The new order-preserving key encoding yields naturally-sorted
    // SMJ output. Verify alphabetical slug order.
    try std.testing.expectEqualStrings("alpha|beta|charlie|", smj_slugs.items);
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

test "join: SMJ output preserves natural order across i64 boundary values" {
    // Spans values near 256 and across 0 so the LE-encoded byte order
    // would have differed from numeric order (LE byte 0 = LSB). The new
    // big-endian + sign-XOR encoding produces naturally-sorted output.
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const schema_a = thindb.Schema{
        .columns = &.{
            .{ .name = "k", .type = .bigint },
            .{ .name = "rowid", .type = .bigint },
        },
        .order_key = &.{"rowid"},
        .unique = true,
    };
    const schema_b = thindb.Schema{
        .columns = &.{
            .{ .name = "k", .type = .bigint },
            .{ .name = "b_rowid", .type = .bigint },
        },
        .order_key = &.{"b_rowid"},
        .unique = true,
    };
    const a_ok = [_][]const u8{"rowid"};
    const b_ok = [_][]const u8{"b_rowid"};

    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    const a = try db.table("a", schema_a, .{ .order_key = &a_ok, .unique = true });
    // Keys cross the LE byte-0 boundary: -1 < 0 < 1 < 256.
    try a.insert(&.{
        .{ .k = @as(i64, -1), .rowid = @as(i64, 1) },
        .{ .k = @as(i64, 0), .rowid = @as(i64, 2) },
        .{ .k = @as(i64, 1), .rowid = @as(i64, 3) },
        .{ .k = @as(i64, 256), .rowid = @as(i64, 4) },
    });
    try a.flush();
    const b = try db.table("b", schema_b, .{ .order_key = &b_ok, .unique = true });
    try b.insert(&.{
        .{ .k = @as(i64, 256), .b_rowid = @as(i64, 1) },
        .{ .k = @as(i64, 1), .b_rowid = @as(i64, 2) },
        .{ .k = @as(i64, 0), .b_rowid = @as(i64, 3) },
        .{ .k = @as(i64, -1), .b_rowid = @as(i64, 4) },
    });
    try b.flush();

    const left = try thindb.scan(allocator, a);
    const right = try thindb.scan(allocator, b);
    var q = try left.join(right, .{
        .on = &.{.{ .left = "k", .right = "k" }},
        .algorithm = .sort_merge,
    });
    defer q.deinit();

    var ks: std.ArrayList(i64) = .empty;
    defer ks.deinit(allocator);
    while (try q.next()) |bat| {
        try ks.appendSlice(allocator, bat.values[0].data.bigint[0..bat.row_count]);
    }
    // Naturally sorted by k: -1, 0, 1, 256.
    try std.testing.expectEqualSlices(i64, &[_]i64{ -1, 0, 1, 256 }, ks.items);
}

test "join: hash output is exact across row-group boundaries" {
    // Repro for an over-emit bug: at ~100k rows / row_group=65k, hash
    // join emitted ~97 extra rows. Inner equi-join of two unique-keyed
    // tables [0..N) must emit exactly N rows — no more, no less.
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const left_schema = thindb.Schema{
        .columns = &.{
            .{ .name = "k", .type = .bigint },
            .{ .name = "lval", .type = .int },
        },
        .order_key = &.{"k"},
        .unique = true,
    };
    const right_schema = thindb.Schema{
        .columns = &.{
            .{ .name = "k", .type = .bigint },
            .{ .name = "rval", .type = .int },
        },
        .order_key = &.{"k"},
        .unique = true,
    };
    const ok = [_][]const u8{"k"};
    const opts = thindb.TableOptions{ .order_key = &ok, .unique = true, .row_group_size = 1024 };

    var db = try thindb.Database.open(allocator, io, tmp.dir, .{
        .auto_flush_rows = std.math.maxInt(u64),
        .auto_flush_bytes = std.math.maxInt(usize),
        .auto_flush_secs = 0,
    });
    defer db.close();

    const n: usize = 3000; // 3 row groups at row_group_size = 1024
    const LRow = struct { k: i64, lval: i32 };
    const RRow = struct { k: i64, rval: i32 };

    const l = try db.table("l", left_schema, opts);
    {
        const rows = try allocator.alloc(LRow, n);
        defer allocator.free(rows);
        for (rows, 0..) |*r, i| r.* = .{ .k = @intCast(i), .lval = @intCast(i) };
        try l.insert(rows);
    }
    try l.flush();

    const r = try db.table("r", right_schema, opts);
    {
        const rows = try allocator.alloc(RRow, n);
        defer allocator.free(rows);
        for (rows, 0..) |*r2, i| r2.* = .{ .k = @intCast(i), .rval = @intCast(i) };
        try r.insert(rows);
    }
    try r.flush();

    const left = try thindb.scan(allocator, l);
    const right = try thindb.scan(allocator, r);
    var q = try left.join(right, .{
        .on = &.{.{ .left = "k", .right = "k" }},
        .algorithm = .hash,
    });
    defer q.deinit();

    var total: usize = 0;
    while (try q.next()) |b| total += b.row_count;
    try std.testing.expectEqual(n, total);
}

// Outer joins (hash algorithm). Fixture: users[1,2,3] LEFT JOIN
// orders ON uid=uid, where orders has rows for uid=1 (two), uid=2,
// and uid=99 (orphan). Expected per join type:
//   inner: uid=1×2, uid=2 → 3 rows
//   left:  uid=1×2, uid=2, uid=3 (no orders) → 4 rows
//   right: uid=1×2, uid=2, uid=99 (no user) → 4 rows
//   full:  uid=1×2, uid=2, uid=3, uid=99 → 5 rows
fn outerFixture(allocator: std.mem.Allocator, io: std.Io, dir: std.Io.Dir) !struct {
    db: *thindb.Database,
    users: *thindb.Table,
    orders: *thindb.Table,
} {
    var db = try thindb.Database.open(allocator, io, dir, .{
        .auto_flush_rows = std.math.maxInt(u64),
        .auto_flush_bytes = std.math.maxInt(usize),
        .auto_flush_secs = 0,
    });
    errdefer db.close();

    const users = try db.table("users", users_schema, users_opts);
    try users.insert(&.{
        .{ .uid = @as(i64, 1), .name = "alice" },
        .{ .uid = @as(i64, 2), .name = "bob" },
        .{ .uid = @as(i64, 3), .name = "carol" }, // no orders
    });
    try users.flush();

    const orders = try db.table("orders", orders_schema, orders_opts);
    try orders.insert(&.{
        .{ .oid = @as(i64, 100), .uid = @as(i64, 1), .qty = @as(i32, 10) },
        .{ .oid = @as(i64, 101), .uid = @as(i64, 1), .qty = @as(i32, 20) },
        .{ .oid = @as(i64, 102), .uid = @as(i64, 2), .qty = @as(i32, 30) },
        .{ .oid = @as(i64, 103), .uid = @as(i64, 99), .qty = @as(i32, 40) }, // no user
    });
    try orders.flush();

    return .{ .db = db, .users = users, .orders = orders };
}

test "join: LEFT OUTER preserves unmatched left rows with NULL right" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var f = try outerFixture(allocator, io, tmp.dir);
    defer f.db.close();

    const left = try thindb.scan(allocator, f.users);
    const right = try thindb.scan(allocator, f.orders);
    var q = try left.join(right, .{
        .join_type = .left,
        .on = &.{.{ .left = "uid", .right = "uid" }},
        .algorithm = .hash,
    });
    defer q.deinit();

    var rows: usize = 0;
    var unmatched_uid3: bool = false;
    while (try q.next()) |b| {
        rows += b.row_count;
        // Schema: uid, name, oid, qty. oid is at idx 2.
        for (0..b.row_count) |i| {
            if (b.values[0].data.bigint[i] == 3) {
                unmatched_uid3 = true;
                // oid (right side) must be NULL for this row.
                try std.testing.expect(!b.values[2].isValid(i));
            }
        }
    }
    try std.testing.expectEqual(@as(usize, 4), rows); // 2 + 1 + 1 unmatched
    try std.testing.expect(unmatched_uid3);
}

test "join: RIGHT OUTER preserves unmatched right rows with NULL left" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var f = try outerFixture(allocator, io, tmp.dir);
    defer f.db.close();

    const left = try thindb.scan(allocator, f.users);
    const right = try thindb.scan(allocator, f.orders);
    var q = try left.join(right, .{
        .join_type = .right,
        .on = &.{.{ .left = "uid", .right = "uid" }},
        .algorithm = .hash,
    });
    defer q.deinit();

    var rows: usize = 0;
    var unmatched_oid103: bool = false;
    while (try q.next()) |b| {
        rows += b.row_count;
        // Schema: uid, name, oid, qty. Check the right's orphan oid=103.
        for (0..b.row_count) |i| {
            if (b.values[2].data.bigint[i] == 103) {
                unmatched_oid103 = true;
                // uid (left side, USING-merged column) is NULL here —
                // see JoinType.right comment for the SQL deviation.
                try std.testing.expect(!b.values[0].isValid(i));
                // name (left side) is also NULL.
                try std.testing.expect(!b.values[1].isValid(i));
            }
        }
    }
    try std.testing.expectEqual(@as(usize, 4), rows); // 2 + 1 + 1 unmatched
    try std.testing.expect(unmatched_oid103);
}

test "join: LEFT OUTER via SMJ" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var f = try outerFixture(allocator, io, tmp.dir);
    defer f.db.close();

    const left = try thindb.scan(allocator, f.users);
    const right = try thindb.scan(allocator, f.orders);
    var q = try left.join(right, .{
        .join_type = .left,
        .on = &.{.{ .left = "uid", .right = "uid" }},
        .algorithm = .sort_merge,
    });
    defer q.deinit();

    var rows: usize = 0;
    var unmatched_uid3 = false;
    while (try q.next()) |b| {
        rows += b.row_count;
        for (0..b.row_count) |i| {
            if (b.values[0].data.bigint[i] == 3) {
                unmatched_uid3 = true;
                try std.testing.expect(!b.values[2].isValid(i));
            }
        }
    }
    try std.testing.expectEqual(@as(usize, 4), rows);
    try std.testing.expect(unmatched_uid3);
}

test "join: RIGHT OUTER via SMJ" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var f = try outerFixture(allocator, io, tmp.dir);
    defer f.db.close();

    const left = try thindb.scan(allocator, f.users);
    const right = try thindb.scan(allocator, f.orders);
    var q = try left.join(right, .{
        .join_type = .right,
        .on = &.{.{ .left = "uid", .right = "uid" }},
        .algorithm = .sort_merge,
    });
    defer q.deinit();

    var rows: usize = 0;
    var unmatched_oid103 = false;
    while (try q.next()) |b| {
        rows += b.row_count;
        for (0..b.row_count) |i| {
            if (b.values[2].data.bigint[i] == 103) {
                unmatched_oid103 = true;
                try std.testing.expect(!b.values[0].isValid(i));
                try std.testing.expect(!b.values[1].isValid(i));
            }
        }
    }
    try std.testing.expectEqual(@as(usize, 4), rows);
    try std.testing.expect(unmatched_oid103);
}

test "join: FULL OUTER via SMJ" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var f = try outerFixture(allocator, io, tmp.dir);
    defer f.db.close();

    const left = try thindb.scan(allocator, f.users);
    const right = try thindb.scan(allocator, f.orders);
    var q = try left.join(right, .{
        .join_type = .full,
        .on = &.{.{ .left = "uid", .right = "uid" }},
        .algorithm = .sort_merge,
    });
    defer q.deinit();

    var rows: usize = 0;
    var saw_uid3 = false;
    var saw_oid103 = false;
    while (try q.next()) |b| {
        rows += b.row_count;
        for (0..b.row_count) |i| {
            const uv = b.values[0].isValid(i);
            const ov = b.values[2].isValid(i);
            if (uv and !ov and b.values[0].data.bigint[i] == 3) saw_uid3 = true;
            if (!uv and ov and b.values[2].data.bigint[i] == 103) saw_oid103 = true;
        }
    }
    try std.testing.expectEqual(@as(usize, 5), rows);
    try std.testing.expect(saw_uid3);
    try std.testing.expect(saw_oid103);
}

test "join: FULL OUTER preserves orphans from both sides" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var f = try outerFixture(allocator, io, tmp.dir);
    defer f.db.close();

    const left = try thindb.scan(allocator, f.users);
    const right = try thindb.scan(allocator, f.orders);
    var q = try left.join(right, .{
        .join_type = .full,
        .on = &.{.{ .left = "uid", .right = "uid" }},
        .algorithm = .hash,
    });
    defer q.deinit();

    var rows: usize = 0;
    var saw_uid3_orphan = false;
    var saw_oid103_orphan = false;
    while (try q.next()) |b| {
        rows += b.row_count;
        for (0..b.row_count) |i| {
            const uid_valid = b.values[0].isValid(i);
            const oid_valid = b.values[2].isValid(i);
            if (uid_valid and !oid_valid and b.values[0].data.bigint[i] == 3) {
                saw_uid3_orphan = true;
            }
            if (!uid_valid and oid_valid and b.values[2].data.bigint[i] == 103) {
                saw_oid103_orphan = true;
            }
        }
    }
    try std.testing.expectEqual(@as(usize, 5), rows); // 3 inner + 1 left orphan + 1 right orphan
    try std.testing.expect(saw_uid3_orphan);
    try std.testing.expect(saw_oid103_orphan);
}

test "join: extra_predicate filters output after equi-join" {
    // INNER join on uid, plus an extra predicate qty > 15. Hash join
    // emits 3 rows (uid=1×2 + uid=2); the predicate keeps qty=20 and
    // qty=30 only.
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var f = try outerFixture(allocator, io, tmp.dir);
    defer f.db.close();

    const left = try thindb.scan(allocator, f.users);
    const right = try thindb.scan(allocator, f.orders);
    var q = try left.join(right, .{
        .on = &.{.{ .left = "uid", .right = "uid" }},
        .extra_predicate = thindb.leafExpr("qty", .gt, .{ .int = 15 }),
        .algorithm = .hash,
    });
    defer q.deinit();

    var rows: usize = 0;
    while (try q.next()) |b| rows += b.row_count;
    try std.testing.expectEqual(@as(usize, 2), rows);
}

test "join: extra_predicate works under SMJ + outer join (WHERE semantics)" {
    // LEFT OUTER via SMJ + extra_predicate qty > 25. The equi-join
    // emits 4 rows (uid=1×2, uid=2, uid=3 null-extended). The filter
    // drops uid=1's two rows (qty 10, 20) and keeps uid=2 (qty=30).
    // The null-extended uid=3 row has qty=NULL → predicate fails → dropped.
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var f = try outerFixture(allocator, io, tmp.dir);
    defer f.db.close();

    const left = try thindb.scan(allocator, f.users);
    const right = try thindb.scan(allocator, f.orders);
    var q = try left.join(right, .{
        .join_type = .left,
        .on = &.{.{ .left = "uid", .right = "uid" }},
        .extra_predicate = thindb.leafExpr("qty", .gt, .{ .int = 25 }),
        .algorithm = .sort_merge,
    });
    defer q.deinit();

    var rows: usize = 0;
    var saw_uid2 = false;
    while (try q.next()) |b| {
        rows += b.row_count;
        for (0..b.row_count) |i| {
            if (b.values[0].data.bigint[i] == 2) saw_uid2 = true;
        }
    }
    try std.testing.expectEqual(@as(usize, 1), rows);
    try std.testing.expect(saw_uid2);
}

test "join: range predicate filters cartesian pairs (hash, INNER)" {
    // Two tables share `tenant`. Range adds `lstart <= revent < lend`:
    //   left: (tenant, lstart, lend)
    //   right: (tenant, revent)
    // Match: same tenant AND lstart <= revent AND revent < lend.
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const left_schema = thindb.Schema{
        .columns = &.{
            .{ .name = "tenant", .type = .bigint },
            .{ .name = "lstart", .type = .bigint },
            .{ .name = "lend", .type = .bigint },
        },
        .order_key = &.{ "tenant", "lstart" },
        .unique = false,
    };
    const right_schema = thindb.Schema{
        .columns = &.{
            .{ .name = "tenant", .type = .bigint },
            .{ .name = "revent", .type = .bigint },
        },
        .order_key = &.{ "tenant", "revent" },
        .unique = false,
    };
    const l_ok = [_][]const u8{ "tenant", "lstart" };
    const r_ok = [_][]const u8{ "tenant", "revent" };

    var db = try thindb.Database.open(allocator, io, tmp.dir, .{
        .auto_flush_rows = std.math.maxInt(u64),
        .auto_flush_bytes = std.math.maxInt(usize),
        .auto_flush_secs = 0,
    });
    defer db.close();

    const l = try db.table("l", left_schema, .{ .order_key = &l_ok });
    try l.insert(&.{
        // tenant=1: window [100..200), [200..300)
        .{ .tenant = @as(i64, 1), .lstart = @as(i64, 100), .lend = @as(i64, 200) },
        .{ .tenant = @as(i64, 1), .lstart = @as(i64, 200), .lend = @as(i64, 300) },
        // tenant=2: [50..150)
        .{ .tenant = @as(i64, 2), .lstart = @as(i64, 50), .lend = @as(i64, 150) },
    });
    try l.flush();

    const r = try db.table("r", right_schema, .{ .order_key = &r_ok });
    try r.insert(&.{
        .{ .tenant = @as(i64, 1), .revent = @as(i64, 150) }, // in [100, 200)
        .{ .tenant = @as(i64, 1), .revent = @as(i64, 250) }, // in [200, 300)
        .{ .tenant = @as(i64, 1), .revent = @as(i64, 50) }, // before any window
        .{ .tenant = @as(i64, 2), .revent = @as(i64, 100) }, // in [50, 150)
        .{ .tenant = @as(i64, 2), .revent = @as(i64, 200) }, // after window
        .{ .tenant = @as(i64, 3), .revent = @as(i64, 100) }, // no matching tenant
    });
    try r.flush();

    // First condition: tenant equi. Second: lstart <= revent (range).
    // Third: revent < lend (a second range). We do this as a chain —
    // single Spec.range supports ONE inequality, so apply the other
    // via extra_predicate against the joined output (revent < lend).
    // Wait — extra_predicate references output columns by name, not
    // cross-side. To keep this test focused on Spec.range, use just
    // the first range condition.
    //
    // Expected with `tenant = AND lstart <= revent`:
    //   tenant=1, lstart=100, revent=150  ✓
    //   tenant=1, lstart=100, revent=250  ✓
    //   tenant=1, lstart=200, revent=250  ✓
    //   tenant=2, lstart=50, revent=100   ✓
    //   tenant=2, lstart=50, revent=200   ✓
    //   (revent=50 / revent=100-tenant3 are excluded)
    // = 5 rows
    const left = try thindb.scan(allocator, l);
    const right = try thindb.scan(allocator, r);
    var q = try left.join(right, .{
        .on = &.{.{ .left = "tenant", .right = "tenant" }},
        .ranges = &.{.{ .left = "lstart", .op = .lte, .right = "revent" }},
        .algorithm = .hash,
    });
    defer q.deinit();

    var rows: usize = 0;
    while (try q.next()) |b| rows += b.row_count;
    try std.testing.expectEqual(@as(usize, 5), rows);
}

test "join: range predicate works under SMJ" {
    // Same shape, SMJ path. Validates the inner Cartesian's range
    // filter when SMJ is the engine.
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const schema_a = thindb.Schema{
        .columns = &.{
            .{ .name = "k", .type = .bigint },
            .{ .name = "x", .type = .bigint },
        },
        .order_key = &.{"k"},
        .unique = false,
    };
    const schema_b = thindb.Schema{
        .columns = &.{
            .{ .name = "k", .type = .bigint },
            .{ .name = "y", .type = .bigint },
        },
        .order_key = &.{"k"},
        .unique = false,
    };
    const a_ok = [_][]const u8{"k"};
    const b_ok = [_][]const u8{"k"};

    var db = try thindb.Database.open(allocator, io, tmp.dir, .{
        .auto_flush_rows = std.math.maxInt(u64),
        .auto_flush_bytes = std.math.maxInt(usize),
        .auto_flush_secs = 0,
    });
    defer db.close();

    const a = try db.table("a", schema_a, .{ .order_key = &a_ok });
    try a.insert(&.{
        .{ .k = @as(i64, 1), .x = @as(i64, 10) },
        .{ .k = @as(i64, 1), .x = @as(i64, 20) },
        .{ .k = @as(i64, 2), .x = @as(i64, 100) },
    });
    try a.flush();

    const b = try db.table("b", schema_b, .{ .order_key = &b_ok });
    try b.insert(&.{
        .{ .k = @as(i64, 1), .y = @as(i64, 15) }, // matches x=10 (10<15), not x=20
        .{ .k = @as(i64, 1), .y = @as(i64, 25) }, // matches x=10, x=20
        .{ .k = @as(i64, 2), .y = @as(i64, 50) }, // doesn't match x=100
    });
    try b.flush();

    // Predicate: a.x < b.y. For k=1: 3 matches. For k=2: 0. Total 3.
    const left = try thindb.scan(allocator, a);
    const right = try thindb.scan(allocator, b);
    var q = try left.join(right, .{
        .on = &.{.{ .left = "k", .right = "k" }},
        .ranges = &.{.{ .left = "x", .op = .lt, .right = "y" }},
        .algorithm = .sort_merge,
    });
    defer q.deinit();

    var rows: usize = 0;
    while (try q.next()) |b2| rows += b2.row_count;
    try std.testing.expectEqual(@as(usize, 3), rows);
}

// Same-typed columns fixture used by outer+range tests.
fn outerRangeFixture(allocator: std.mem.Allocator, io: std.Io, dir: std.Io.Dir) !struct {
    db: *thindb.Database,
    l: *thindb.Table,
    r: *thindb.Table,
} {
    const lschema = thindb.Schema{
        .columns = &.{
            .{ .name = "k", .type = .bigint },
            .{ .name = "x", .type = .bigint },
        },
        .order_key = &.{"k"},
        .unique = false,
    };
    const rschema = thindb.Schema{
        .columns = &.{
            .{ .name = "k", .type = .bigint },
            .{ .name = "y", .type = .bigint },
        },
        .order_key = &.{"k"},
        .unique = false,
    };
    const l_ok = [_][]const u8{"k"};
    const r_ok = [_][]const u8{"k"};

    var db = try thindb.Database.open(allocator, io, dir, .{
        .auto_flush_rows = std.math.maxInt(u64),
        .auto_flush_bytes = std.math.maxInt(usize),
        .auto_flush_secs = 0,
    });
    errdefer db.close();

    const l = try db.table("l", lschema, .{ .order_key = &l_ok });
    try l.insert(&.{
        .{ .k = @as(i64, 1), .x = @as(i64, 10) }, // for k=1
        .{ .k = @as(i64, 2), .x = @as(i64, 20) }, // for k=2
        .{ .k = @as(i64, 3), .x = @as(i64, 30) }, // for k=3 (orphan, no r row)
    });
    try l.flush();

    const r = try db.table("r", rschema, .{ .order_key = &r_ok });
    try r.insert(&.{
        .{ .k = @as(i64, 1), .y = @as(i64, 5) }, // k=1: l.x=10 > r.y=5
        .{ .k = @as(i64, 1), .y = @as(i64, 15) }, // k=1: l.x=10 < r.y=15
        .{ .k = @as(i64, 2), .y = @as(i64, 5) }, // k=2: l.x=20 > r.y=5
        .{ .k = @as(i64, 4), .y = @as(i64, 100) }, // orphan, no l row
    });
    try r.flush();

    return .{ .db = db, .l = l, .r = r };
}

test "join: LEFT OUTER + range — preserved rows null-extend when range fails (hash)" {
    // l ⋈ r ON k AND l.x < r.y
    //   k=1, l.x=10: r.y=5 fails, r.y=15 passes → 1 emit
    //   k=2, l.x=20: r.y=5 fails (no other r) → all rejected → null-extend
    //   k=3: no r → null-extend
    // Total: 1 actual + 2 null-extended = 3 rows.
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var f = try outerRangeFixture(allocator, io, tmp.dir);
    defer f.db.close();

    const left = try thindb.scan(allocator, f.l);
    const right = try thindb.scan(allocator, f.r);
    var q = try left.join(right, .{
        .join_type = .left,
        .on = &.{.{ .left = "k", .right = "k" }},
        .ranges = &.{.{ .left = "x", .op = .lt, .right = "y" }},
        .algorithm = .hash,
    });
    defer q.deinit();

    var rows: usize = 0;
    var nulls: usize = 0;
    while (try q.next()) |b| {
        rows += b.row_count;
        // schema: k, x, y. y is at index 2.
        for (0..b.row_count) |i| {
            if (!b.values[2].isValid(i)) nulls += 1;
        }
    }
    try std.testing.expectEqual(@as(usize, 3), rows);
    try std.testing.expectEqual(@as(usize, 2), nulls);
}

test "join: LEFT OUTER + range — all candidates rejected → null-extended (SMJ)" {
    // Same shape, run via SMJ to exercise its outer+range path.
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var f = try outerRangeFixture(allocator, io, tmp.dir);
    defer f.db.close();

    const left = try thindb.scan(allocator, f.l);
    const right = try thindb.scan(allocator, f.r);
    var q = try left.join(right, .{
        .join_type = .left,
        .on = &.{.{ .left = "k", .right = "k" }},
        .ranges = &.{.{ .left = "x", .op = .lt, .right = "y" }},
        .algorithm = .sort_merge,
    });
    defer q.deinit();

    var rows: usize = 0;
    var nulls: usize = 0;
    while (try q.next()) |b| {
        rows += b.row_count;
        for (0..b.row_count) |i| {
            if (!b.values[2].isValid(i)) nulls += 1;
        }
    }
    try std.testing.expectEqual(@as(usize, 3), rows);
    try std.testing.expectEqual(@as(usize, 2), nulls);
}

test "join: FULL OUTER + range — both-side orphans + range-rejected null-extension" {
    // Same fixture: l ⋈ r ON k AND l.x < r.y
    //   k=1: 1 actual match (l.x=10 < r.y=15). Note r.y=5 fails the range
    //     but is still considered "matched" on the build side for FULL
    //     since the equi key matched — but we want to be precise: build
    //     rows that pass range get marked. r row (k=1, y=5) does NOT get
    //     marked → drained later. So:
    //       1 emit (k=1, x=10, y=15)
    //       drain: r (k=1, y=5) unmatched → null-extended on left.
    //   k=2: r.y=5 fails range, all candidates rejected.
    //     LEFT null-extension fires for l (k=2). r row stays unmatched.
    //       1 left-only emit, 1 right-only (k=2, y=5) emit.
    //   k=3: no r → null-extended once.
    //   k=4 (right-only orphan): drained at end → null-extended once.
    // Total: 1 + 1 + 1 + 1 + 1 + 1 = 6 rows.
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var f = try outerRangeFixture(allocator, io, tmp.dir);
    defer f.db.close();

    const left = try thindb.scan(allocator, f.l);
    const right = try thindb.scan(allocator, f.r);
    var q = try left.join(right, .{
        .join_type = .full,
        .on = &.{.{ .left = "k", .right = "k" }},
        .ranges = &.{.{ .left = "x", .op = .lt, .right = "y" }},
        .algorithm = .hash,
    });
    defer q.deinit();

    var rows: usize = 0;
    while (try q.next()) |b| rows += b.row_count;
    try std.testing.expectEqual(@as(usize, 6), rows);
}

test "join: NLJ handles pure range (no equi part)" {
    // Empty `on`, one range. .auto routes to nested_loop.
    // a.x = [10, 20, 100]; b.y = [15, 25, 50].
    // Pairs where a.x < b.y:
    //   x=10: y=15,25,50 → 3
    //   x=20: y=25,50 → 2
    //   x=100: 0
    // Total 5.
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const schema_a = thindb.Schema{
        .columns = &.{ .{ .name = "rowid", .type = .bigint }, .{ .name = "x", .type = .bigint } },
        .order_key = &.{"rowid"},
        .unique = true,
    };
    const schema_b = thindb.Schema{
        .columns = &.{ .{ .name = "b_rowid", .type = .bigint }, .{ .name = "y", .type = .bigint } },
        .order_key = &.{"b_rowid"},
        .unique = true,
    };
    const ok_a = [_][]const u8{"rowid"};
    const ok_b = [_][]const u8{"b_rowid"};

    var db = try thindb.Database.open(allocator, io, tmp.dir, .{
        .auto_flush_rows = std.math.maxInt(u64),
        .auto_flush_bytes = std.math.maxInt(usize),
        .auto_flush_secs = 0,
    });
    defer db.close();

    const a = try db.table("a", schema_a, .{ .order_key = &ok_a, .unique = true });
    try a.insert(&.{
        .{ .rowid = @as(i64, 1), .x = @as(i64, 10) },
        .{ .rowid = @as(i64, 2), .x = @as(i64, 20) },
        .{ .rowid = @as(i64, 3), .x = @as(i64, 100) },
    });
    try a.flush();
    const b = try db.table("b", schema_b, .{ .order_key = &ok_b, .unique = true });
    try b.insert(&.{
        .{ .b_rowid = @as(i64, 1), .y = @as(i64, 15) },
        .{ .b_rowid = @as(i64, 2), .y = @as(i64, 25) },
        .{ .b_rowid = @as(i64, 3), .y = @as(i64, 50) },
    });
    try b.flush();

    const left = try thindb.scan(allocator, a);
    const right = try thindb.scan(allocator, b);
    var q = try left.join(right, .{
        .on = &.{}, // pure range
        .ranges = &.{.{ .left = "x", .op = .lt, .right = "y" }},
    });
    defer q.deinit();

    var rows: usize = 0;
    while (try q.next()) |bat| rows += bat.row_count;
    try std.testing.expectEqual(@as(usize, 5), rows);
}

test "join: NLJ handles multiple range predicates" {
    // a.x BETWEEN-style: x >= y_lo AND x < y_hi for each (y_lo, y_hi).
    // Effectively: which a.x falls inside any b's [lo, hi) interval.
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const schema_a = thindb.Schema{
        .columns = &.{ .{ .name = "rowid", .type = .bigint }, .{ .name = "x", .type = .bigint } },
        .order_key = &.{"rowid"},
        .unique = true,
    };
    const schema_b = thindb.Schema{
        .columns = &.{
            .{ .name = "b_rowid", .type = .bigint },
            .{ .name = "y_lo", .type = .bigint },
            .{ .name = "y_hi", .type = .bigint },
        },
        .order_key = &.{"b_rowid"},
        .unique = true,
    };
    const ok_a = [_][]const u8{"rowid"};
    const ok_b = [_][]const u8{"b_rowid"};

    var db = try thindb.Database.open(allocator, io, tmp.dir, .{
        .auto_flush_rows = std.math.maxInt(u64),
        .auto_flush_bytes = std.math.maxInt(usize),
        .auto_flush_secs = 0,
    });
    defer db.close();

    const a = try db.table("a", schema_a, .{ .order_key = &ok_a, .unique = true });
    try a.insert(&.{
        .{ .rowid = @as(i64, 1), .x = @as(i64, 5) }, // in [0,10)
        .{ .rowid = @as(i64, 2), .x = @as(i64, 15) }, // in [10,20)
        .{ .rowid = @as(i64, 3), .x = @as(i64, 25) }, // outside both
    });
    try a.flush();
    const b = try db.table("b", schema_b, .{ .order_key = &ok_b, .unique = true });
    try b.insert(&.{
        .{ .b_rowid = @as(i64, 1), .y_lo = @as(i64, 0), .y_hi = @as(i64, 10) },
        .{ .b_rowid = @as(i64, 2), .y_lo = @as(i64, 10), .y_hi = @as(i64, 20) },
    });
    try b.flush();

    const left = try thindb.scan(allocator, a);
    const right = try thindb.scan(allocator, b);
    // x >= y_lo AND x < y_hi for each (a, b) pair.
    var q = try left.join(right, .{
        .on = &.{},
        .ranges = &.{
            .{ .left = "x", .op = .gte, .right = "y_lo" },
            .{ .left = "x", .op = .lt, .right = "y_hi" },
        },
    });
    defer q.deinit();

    var rows: usize = 0;
    while (try q.next()) |bat| rows += bat.row_count;
    // x=5  matches b1 only → 1
    // x=15 matches b2 only → 1
    // x=25 matches neither → 0
    try std.testing.expectEqual(@as(usize, 2), rows);
}

test "join: NLJ handles equi + multiple ranges (BETWEEN-style)" {
    // tenant equi + a.x in [b.lo, b.hi).
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const schema_a = thindb.Schema{
        .columns = &.{
            .{ .name = "tenant", .type = .bigint },
            .{ .name = "x", .type = .bigint },
        },
        .order_key = &.{"tenant"},
        .unique = false,
    };
    const schema_b = thindb.Schema{
        .columns = &.{
            .{ .name = "tenant", .type = .bigint },
            .{ .name = "lo", .type = .bigint },
            .{ .name = "hi", .type = .bigint },
        },
        .order_key = &.{"tenant"},
        .unique = false,
    };
    const ok_a = [_][]const u8{"tenant"};
    const ok_b = [_][]const u8{"tenant"};

    var db = try thindb.Database.open(allocator, io, tmp.dir, .{
        .auto_flush_rows = std.math.maxInt(u64),
        .auto_flush_bytes = std.math.maxInt(usize),
        .auto_flush_secs = 0,
    });
    defer db.close();

    const a = try db.table("a", schema_a, .{ .order_key = &ok_a });
    try a.insert(&.{
        .{ .tenant = @as(i64, 1), .x = @as(i64, 50) }, // matches t=1 window
        .{ .tenant = @as(i64, 1), .x = @as(i64, 150) }, // outside t=1 window
        .{ .tenant = @as(i64, 2), .x = @as(i64, 200) }, // matches t=2 window
    });
    try a.flush();
    const b = try db.table("b", schema_b, .{ .order_key = &ok_b });
    try b.insert(&.{
        .{ .tenant = @as(i64, 1), .lo = @as(i64, 0), .hi = @as(i64, 100) },
        .{ .tenant = @as(i64, 2), .lo = @as(i64, 150), .hi = @as(i64, 250) },
    });
    try b.flush();

    const left = try thindb.scan(allocator, a);
    const right = try thindb.scan(allocator, b);
    // Even with equi `on`, multiple ranges work via hash/SMJ Cartesian.
    var q = try left.join(right, .{
        .on = &.{.{ .left = "tenant", .right = "tenant" }},
        .ranges = &.{
            .{ .left = "x", .op = .gte, .right = "lo" },
            .{ .left = "x", .op = .lt, .right = "hi" },
        },
        .algorithm = .hash,
    });
    defer q.deinit();

    var rows: usize = 0;
    while (try q.next()) |bat| rows += bat.row_count;
    try std.testing.expectEqual(@as(usize, 2), rows);
}

test "join: LEFT OUTER via NLJ + pure range" {
    // NLJ-only path: no equi keys, just a range. LEFT OUTER preserves
    // left rows that have no qualifying right match.
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const schema_a = thindb.Schema{
        .columns = &.{ .{ .name = "rowid", .type = .bigint }, .{ .name = "x", .type = .bigint } },
        .order_key = &.{"rowid"},
        .unique = true,
    };
    const schema_b = thindb.Schema{
        .columns = &.{ .{ .name = "b_rowid", .type = .bigint }, .{ .name = "y", .type = .bigint } },
        .order_key = &.{"b_rowid"},
        .unique = true,
    };
    const a_ok = [_][]const u8{"rowid"};
    const b_ok = [_][]const u8{"b_rowid"};

    var db = try thindb.Database.open(allocator, io, tmp.dir, .{
        .auto_flush_rows = std.math.maxInt(u64),
        .auto_flush_bytes = std.math.maxInt(usize),
        .auto_flush_secs = 0,
    });
    defer db.close();

    const a = try db.table("a", schema_a, .{ .order_key = &a_ok, .unique = true });
    try a.insert(&.{
        .{ .rowid = @as(i64, 1), .x = @as(i64, 10) }, // any y > 10 matches
        .{ .rowid = @as(i64, 2), .x = @as(i64, 100) }, // no y > 100
    });
    try a.flush();
    const b = try db.table("b", schema_b, .{ .order_key = &b_ok, .unique = true });
    try b.insert(&.{
        .{ .b_rowid = @as(i64, 1), .y = @as(i64, 50) },
        .{ .b_rowid = @as(i64, 2), .y = @as(i64, 75) },
    });
    try b.flush();

    const left = try thindb.scan(allocator, a);
    const right = try thindb.scan(allocator, b);
    var q = try left.join(right, .{
        .join_type = .left,
        .on = &.{}, // pure range → NLJ
        .ranges = &.{.{ .left = "x", .op = .lt, .right = "y" }},
    });
    defer q.deinit();

    var rows: usize = 0;
    var saw_unmatched_x100 = false;
    while (try q.next()) |bat| {
        rows += bat.row_count;
        for (0..bat.row_count) |i| {
            if (bat.values[1].data.bigint[i] == 100 and !bat.values[3].isValid(i)) {
                saw_unmatched_x100 = true;
            }
        }
    }
    // x=10: matches y=50 and y=75 → 2 rows.
    // x=100: matches none → 1 null-extended row.
    try std.testing.expectEqual(@as(usize, 3), rows);
    try std.testing.expect(saw_unmatched_x100);
}

test "join: equi + multiple ranges + extra_predicate (the kitchen sink)" {
    // tenant equi + (x >= lo AND x < hi) + WHERE x > 5 (extra_predicate).
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const schema_a = thindb.Schema{
        .columns = &.{
            .{ .name = "tenant", .type = .bigint },
            .{ .name = "x", .type = .bigint },
        },
        .order_key = &.{"tenant"},
        .unique = false,
    };
    const schema_b = thindb.Schema{
        .columns = &.{
            .{ .name = "tenant", .type = .bigint },
            .{ .name = "lo", .type = .bigint },
            .{ .name = "hi", .type = .bigint },
        },
        .order_key = &.{"tenant"},
        .unique = false,
    };
    const a_ok = [_][]const u8{"tenant"};
    const b_ok = [_][]const u8{"tenant"};

    var db = try thindb.Database.open(allocator, io, tmp.dir, .{
        .auto_flush_rows = std.math.maxInt(u64),
        .auto_flush_bytes = std.math.maxInt(usize),
        .auto_flush_secs = 0,
    });
    defer db.close();

    const a = try db.table("a", schema_a, .{ .order_key = &a_ok });
    try a.insert(&.{
        .{ .tenant = @as(i64, 1), .x = @as(i64, 3) }, // fits range [0,10) but fails x>5
        .{ .tenant = @as(i64, 1), .x = @as(i64, 8) }, // fits range AND x>5 → keep
        .{ .tenant = @as(i64, 2), .x = @as(i64, 50) }, // fits range AND x>5 → keep
    });
    try a.flush();
    const b = try db.table("b", schema_b, .{ .order_key = &b_ok });
    try b.insert(&.{
        .{ .tenant = @as(i64, 1), .lo = @as(i64, 0), .hi = @as(i64, 10) },
        .{ .tenant = @as(i64, 2), .lo = @as(i64, 40), .hi = @as(i64, 60) },
    });
    try b.flush();

    const left = try thindb.scan(allocator, a);
    const right = try thindb.scan(allocator, b);
    var q = try left.join(right, .{
        .on = &.{.{ .left = "tenant", .right = "tenant" }},
        .ranges = &.{
            .{ .left = "x", .op = .gte, .right = "lo" },
            .{ .left = "x", .op = .lt, .right = "hi" },
        },
        .extra_predicate = thindb.leafExpr("x", .gt, .{ .bigint = 5 }),
        .algorithm = .hash,
    });
    defer q.deinit();

    var rows: usize = 0;
    while (try q.next()) |b2| rows += b2.row_count;
    // a=(1,3) matches range but fails extra → drop. (1,8) and (2,50) keep.
    try std.testing.expectEqual(@as(usize, 2), rows);
}

test "join: FULL OUTER via NLJ + range — both-side orphans" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const schema_a = thindb.Schema{
        .columns = &.{ .{ .name = "rowid", .type = .bigint }, .{ .name = "x", .type = .bigint } },
        .order_key = &.{"rowid"},
        .unique = true,
    };
    const schema_b = thindb.Schema{
        .columns = &.{ .{ .name = "b_rowid", .type = .bigint }, .{ .name = "y", .type = .bigint } },
        .order_key = &.{"b_rowid"},
        .unique = true,
    };
    const a_ok = [_][]const u8{"rowid"};
    const b_ok = [_][]const u8{"b_rowid"};

    var db = try thindb.Database.open(allocator, io, tmp.dir, .{
        .auto_flush_rows = std.math.maxInt(u64),
        .auto_flush_bytes = std.math.maxInt(usize),
        .auto_flush_secs = 0,
    });
    defer db.close();

    const a = try db.table("a", schema_a, .{ .order_key = &a_ok, .unique = true });
    try a.insert(&.{
        .{ .rowid = @as(i64, 1), .x = @as(i64, 10) }, // matches y=50,75
        .{ .rowid = @as(i64, 2), .x = @as(i64, 100) }, // no match
    });
    try a.flush();
    const b = try db.table("b", schema_b, .{ .order_key = &b_ok, .unique = true });
    try b.insert(&.{
        .{ .b_rowid = @as(i64, 1), .y = @as(i64, 50) }, // matched by x=10
        .{ .b_rowid = @as(i64, 2), .y = @as(i64, 75) }, // matched by x=10
        .{ .b_rowid = @as(i64, 3), .y = @as(i64, 5) }, // not matched
    });
    try b.flush();

    const left = try thindb.scan(allocator, a);
    const right = try thindb.scan(allocator, b);
    var q = try left.join(right, .{
        .join_type = .full,
        .on = &.{},
        .ranges = &.{.{ .left = "x", .op = .lt, .right = "y" }},
    });
    defer q.deinit();

    var rows: usize = 0;
    while (try q.next()) |bat| rows += bat.row_count;
    // x=10 matches y=50,75 → 2 rows.
    // x=100 matches nothing → 1 left-only row.
    // y=5 matches nothing → 1 right-only row.
    try std.testing.expectEqual(@as(usize, 4), rows);
}

test "join: range_sweep output matches NLJ for same data" {
    // Stress test: 5000 x 5000 with x=3i, y=4j data shape. Both algorithms
    // must produce the same output count.
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const schema_a = thindb.Schema{
        .columns = &.{ .{ .name = "l_rowid", .type = .bigint }, .{ .name = "x", .type = .bigint } },
        .order_key = &.{"l_rowid"},
        .unique = true,
    };
    const schema_b = thindb.Schema{
        .columns = &.{ .{ .name = "r_rowid", .type = .bigint }, .{ .name = "y", .type = .bigint } },
        .order_key = &.{"r_rowid"},
        .unique = true,
    };
    const ok_a = [_][]const u8{"l_rowid"};
    const ok_b = [_][]const u8{"r_rowid"};

    var db = try thindb.Database.open(allocator, io, tmp.dir, .{
        .auto_flush_rows = std.math.maxInt(u64),
        .auto_flush_bytes = std.math.maxInt(usize),
        .auto_flush_secs = 0,
    });
    defer db.close();

    const N: usize = 5000;
    const a = try db.table("a", schema_a, .{ .order_key = &ok_a, .unique = true });
    {
        const ARow = struct { l_rowid: i64, x: i64 };
        const rows = try allocator.alloc(ARow, N);
        defer allocator.free(rows);
        for (rows, 0..) |*r, i| r.* = .{ .l_rowid = @intCast(i), .x = @intCast(i * 3) };
        try a.insert(rows);
    }
    try a.flush();
    const b = try db.table("b", schema_b, .{ .order_key = &ok_b, .unique = true });
    {
        const BRow = struct { r_rowid: i64, y: i64 };
        const rows = try allocator.alloc(BRow, N);
        defer allocator.free(rows);
        for (rows, 0..) |*r, i| r.* = .{ .r_rowid = @intCast(i), .y = @intCast(i * 4) };
        try b.insert(rows);
    }
    try b.flush();

    // Count via sweep (.auto routes here).
    var sweep_count: usize = 0;
    {
        const left = try thindb.scan(allocator, a);
        const right = try thindb.scan(allocator, b);
        var q = try left.join(right, .{
            .on = &.{},
            .ranges = &.{.{ .left = "x", .op = .lt, .right = "y" }},
        });
        defer q.deinit();
        while (try q.next()) |bat| sweep_count += bat.row_count;
    }

    // Count via explicit NLJ.
    var nlj_count: usize = 0;
    {
        const left = try thindb.scan(allocator, a);
        const right = try thindb.scan(allocator, b);
        var q = try left.join(right, .{
            .on = &.{},
            .ranges = &.{.{ .left = "x", .op = .lt, .right = "y" }},
            .algorithm = .nested_loop,
        });
        defer q.deinit();
        while (try q.next()) |bat| nlj_count += bat.row_count;
    }

    try std.testing.expectEqual(nlj_count, sweep_count);
}

test "memory: sort over tight budget errors with MemoryBudgetExceeded" {
    // Budget = 100 bytes. We try to sort 100 rows (each ~16 bytes
    // accounted, so ~1600 bytes total). Should fail with the typed
    // error rather than an underlying allocator OOM.
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var db = try thindb.Database.open(allocator, io, tmp.dir, .{
        .query_memory_budget = 100,
        .auto_flush_rows = std.math.maxInt(u64),
        .auto_flush_bytes = std.math.maxInt(usize),
        .auto_flush_secs = 0,
    });
    defer db.close();

    const schema = thindb.Schema{
        .columns = &.{ .{ .name = "id", .type = .bigint }, .{ .name = "v", .type = .bigint } },
        .order_key = &.{"id"},
        .unique = true,
    };
    const ok = [_][]const u8{"id"};
    const t = try db.table("t", schema, .{ .order_key = &ok, .unique = true });

    const Row = struct { id: i64, v: i64 };
    const rows = try allocator.alloc(Row, 100);
    defer allocator.free(rows);
    for (rows, 0..) |*r, i| r.* = .{ .id = @intCast(i), .v = @intCast(100 - @as(i64, @intCast(i))) };
    try t.insert(rows);
    try t.flush();

    var base = try thindb.scan(allocator, t);
    var q = try base.orderBy(&.{.{ .col = "v", .desc = false }});
    defer q.deinit();

    // Drain triggers the sort, which should overshoot the budget.
    const result = q.next();
    try std.testing.expectError(thindb.memory.Error.MemoryBudgetExceeded, result);
}

test "memory: budget = 0 disables tracking (default)" {
    // With the default config (budget = 0), even huge sorts succeed.
    // This is the existing behavior — verify the new instrumentation
    // doesn't regress queries that previously worked.
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var db = try thindb.Database.open(allocator, io, tmp.dir, .{
        .auto_flush_rows = std.math.maxInt(u64),
        .auto_flush_bytes = std.math.maxInt(usize),
        .auto_flush_secs = 0,
    });
    defer db.close();

    const schema = thindb.Schema{
        .columns = &.{ .{ .name = "id", .type = .bigint } },
        .order_key = &.{"id"},
        .unique = true,
    };
    const ok = [_][]const u8{"id"};
    const t = try db.table("t", schema, .{ .order_key = &ok, .unique = true });

    const Row = struct { id: i64 };
    const rows = try allocator.alloc(Row, 1000);
    defer allocator.free(rows);
    for (rows, 0..) |*r, i| r.* = .{ .id = @intCast(999 - @as(i64, @intCast(i))) };
    try t.insert(rows);
    try t.flush();

    var base = try thindb.scan(allocator, t);
    var q = try base.orderBy(&.{.{ .col = "id", .desc = false }});
    defer q.deinit();

    var rows_seen: usize = 0;
    while (try q.next()) |b| rows_seen += b.row_count;
    try std.testing.expectEqual(@as(usize, 1000), rows_seen);
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
