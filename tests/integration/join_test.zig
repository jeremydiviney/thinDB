//! Join operator integration tests. v1 covers inner equi-join via
//! hash algorithm with automatic build-side selection.
//!
//! Future tests (as features land): SMJ, INLJ, NLJ, outer joins,
//! semi/anti, multi-column keys, type-mismatch errors, etc.

const std = @import("std");
const thindb = @import("thindb");

const users_schema = thindb.TableSchema{
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
const orders_schema = thindb.TableSchema{
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

test "join: NULL build-side key mid-stream keeps later rows' identity (multi-key path)" {
    // Regression: a null-key BUILD row used to bump build_rows inside the
    // insert loop AND get counted again by the per-batch advance, shifting
    // every later row's bucket id by one (wrong/out-of-bounds rows emitted).
    // Two join keys force the general compound-key path — the single-key
    // FastTable never read the broken map, masking this.
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    const build_schema = thindb.TableSchema{
        .columns = &.{
            .{ .name = "bid", .type = .bigint },
            .{ .name = "k1", .type = .bigint, .nullable = true },
            .{ .name = "k2", .type = .bigint, .nullable = true },
        },
        .order_key = &.{"bid"},
        .unique = true,
    };
    const build_ok = [_][]const u8{"bid"};
    const build_opts = thindb.TableOptions{ .order_key = &build_ok, .unique = true, .row_group_size = 8 };

    const probe_schema = thindb.TableSchema{
        .columns = &.{
            .{ .name = "pid", .type = .bigint },
            .{ .name = "pk1", .type = .bigint },
            .{ .name = "pk2", .type = .bigint },
        },
        .order_key = &.{"pid"},
        .unique = true,
    };
    const probe_ok = [_][]const u8{"pid"};
    const probe_opts = thindb.TableOptions{ .order_key = &probe_ok, .unique = true, .row_group_size = 8 };

    // Build = smaller side. The NULL row sits BETWEEN two keyed rows so
    // the row after it exercises the id arithmetic.
    const bld = try db.table("bld", build_schema, build_opts);
    try bld.insert(&[_]struct { bid: i64, k1: ?i64, k2: ?i64 }{
        .{ .bid = 100, .k1 = 1, .k2 = 1 },
        .{ .bid = 200, .k1 = null, .k2 = 9 },
        .{ .bid = 300, .k1 = 2, .k2 = 2 },
    });
    try bld.flush();

    const prb = try db.table("prb", probe_schema, probe_opts);
    try prb.insert(&.{
        .{ .pid = @as(i64, 10), .pk1 = @as(i64, 1), .pk2 = @as(i64, 1) },
        .{ .pid = @as(i64, 11), .pk1 = @as(i64, 2), .pk2 = @as(i64, 2) },
        .{ .pid = @as(i64, 12), .pk1 = @as(i64, 3), .pk2 = @as(i64, 3) },
        .{ .pid = @as(i64, 13), .pk1 = @as(i64, 9), .pk2 = @as(i64, 9) },
    });
    try prb.flush();

    const left = try thindb.scan(allocator, prb);
    const right = try thindb.scan(allocator, bld);
    var q = try left.join(right, .{
        .on = &.{
            .{ .left = "pk1", .right = "k1" },
            .{ .left = "pk2", .right = "k2" },
        },
    });
    defer q.deinit();

    var pids: std.ArrayList(i64) = .empty;
    defer pids.deinit(allocator);
    var bids: std.ArrayList(i64) = .empty;
    defer bids.deinit(allocator);
    while (try q.next()) |b| {
        const pid_idx = b.columnIndex("pid").?;
        const bid_idx = b.columnIndex("bid").?;
        var r: u32 = 0;
        while (r < b.row_count) : (r += 1) {
            try pids.append(allocator, b.values[pid_idx].data.bigint[r]);
            try bids.append(allocator, b.values[bid_idx].data.bigint[r]);
        }
    }
    try std.testing.expectEqual(@as(usize, 2), pids.items.len);
    if (pids.items[0] > pids.items[1]) {
        std.mem.swap(i64, &pids.items[0], &pids.items[1]);
        std.mem.swap(i64, &bids.items[0], &bids.items[1]);
    }
    try std.testing.expectEqualSlices(i64, &[_]i64{ 10, 11 }, pids.items);
    try std.testing.expectEqualSlices(i64, &[_]i64{ 100, 300 }, bids.items);
}

test "join: NULL join key never matches" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    // Use nullable join key on the orders side.
    const orders_nullable = thindb.TableSchema{
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
    const a_schema = thindb.TableSchema{
        .columns = &.{
            .{ .name = "rowid", .type = .bigint },
            .{ .name = "k", .type = .int },
            .{ .name = "lval", .type = .string },
        },
        .order_key = &.{"rowid"},
        .unique = true,
    };
    const b_schema = thindb.TableSchema{
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

    const slug_schema = thindb.TableSchema{
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

    const slug_schema = thindb.TableSchema{
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

    const post_schema = thindb.TableSchema{
        .columns = &.{
            .{ .name = "slug", .type = .string },
            .{ .name = "title", .type = .string },
        },
        .order_key = &.{"slug"},
        .unique = true,
    };
    const author_schema = thindb.TableSchema{
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

    const emails_schema = thindb.TableSchema{
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

    const schema_a = thindb.TableSchema{
        .columns = &.{
            .{ .name = "k", .type = .bigint },
            .{ .name = "rowid", .type = .bigint },
        },
        .order_key = &.{"rowid"},
        .unique = true,
    };
    const schema_b = thindb.TableSchema{
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

    const left_schema = thindb.TableSchema{
        .columns = &.{
            .{ .name = "k", .type = .bigint },
            .{ .name = "lval", .type = .int },
        },
        .order_key = &.{"k"},
        .unique = true,
    };
    const right_schema = thindb.TableSchema{
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

    const left_schema = thindb.TableSchema{
        .columns = &.{
            .{ .name = "tenant", .type = .bigint },
            .{ .name = "lstart", .type = .bigint },
            .{ .name = "lend", .type = .bigint },
        },
        .order_key = &.{ "tenant", "lstart" },
        .unique = false,
    };
    const right_schema = thindb.TableSchema{
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

    const schema_a = thindb.TableSchema{
        .columns = &.{
            .{ .name = "k", .type = .bigint },
            .{ .name = "x", .type = .bigint },
        },
        .order_key = &.{"k"},
        .unique = false,
    };
    const schema_b = thindb.TableSchema{
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
    const lschema = thindb.TableSchema{
        .columns = &.{
            .{ .name = "k", .type = .bigint },
            .{ .name = "x", .type = .bigint },
        },
        .order_key = &.{"k"},
        .unique = false,
    };
    const rschema = thindb.TableSchema{
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

    const schema_a = thindb.TableSchema{
        .columns = &.{ .{ .name = "rowid", .type = .bigint }, .{ .name = "x", .type = .bigint } },
        .order_key = &.{"rowid"},
        .unique = true,
    };
    const schema_b = thindb.TableSchema{
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

    const schema_a = thindb.TableSchema{
        .columns = &.{ .{ .name = "rowid", .type = .bigint }, .{ .name = "x", .type = .bigint } },
        .order_key = &.{"rowid"},
        .unique = true,
    };
    const schema_b = thindb.TableSchema{
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

    const schema_a = thindb.TableSchema{
        .columns = &.{
            .{ .name = "tenant", .type = .bigint },
            .{ .name = "x", .type = .bigint },
        },
        .order_key = &.{"tenant"},
        .unique = false,
    };
    const schema_b = thindb.TableSchema{
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

    const schema_a = thindb.TableSchema{
        .columns = &.{ .{ .name = "rowid", .type = .bigint }, .{ .name = "x", .type = .bigint } },
        .order_key = &.{"rowid"},
        .unique = true,
    };
    const schema_b = thindb.TableSchema{
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

    const schema_a = thindb.TableSchema{
        .columns = &.{
            .{ .name = "tenant", .type = .bigint },
            .{ .name = "x", .type = .bigint },
        },
        .order_key = &.{"tenant"},
        .unique = false,
    };
    const schema_b = thindb.TableSchema{
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

    const schema_a = thindb.TableSchema{
        .columns = &.{ .{ .name = "rowid", .type = .bigint }, .{ .name = "x", .type = .bigint } },
        .order_key = &.{"rowid"},
        .unique = true,
    };
    const schema_b = thindb.TableSchema{
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

    const schema_a = thindb.TableSchema{
        .columns = &.{ .{ .name = "l_rowid", .type = .bigint }, .{ .name = "x", .type = .bigint } },
        .order_key = &.{"l_rowid"},
        .unique = true,
    };
    const schema_b = thindb.TableSchema{
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

test "join: a left key tail is matched on and never emitted, by every algorithm" {
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

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const expr = thindb.exec.expr_mod;
    const key = try expr.call(arena.allocator(), "mul", &.{ expr.col("uid"), expr.lit(.{ .bigint = 100 }) });

    // The tail key reads uid * 100, so only uid 1 meets an oid (100).
    const algorithms = [_]@FieldType(thindb.exec.JoinSpec, "algorithm"){ .hash, .sort_merge, .nested_loop };
    for (algorithms) |algorithm| {
        const base = try thindb.scan(allocator, users);
        const left = try base.compute(&.{.{ .name = "k", .expr = key }});
        const right = try (try thindb.scan(allocator, orders)).project(&.{ "oid", "qty" });
        var q = try left.join(right, .{
            .on = &.{.{ .left = "k", .right = "oid" }},
            .algorithm = algorithm,
            .left_key_tail = 1,
        });
        defer q.deinit();

        const schema = q.outputSchema();
        try std.testing.expectEqual(@as(usize, 3), schema.len);
        try std.testing.expectEqualStrings("uid", schema[0].name);
        try std.testing.expectEqualStrings("name", schema[1].name);
        try std.testing.expectEqualStrings("qty", schema[2].name);
        var rows: usize = 0;
        while (try q.next()) |b| {
            for (0..b.row_count) |i| {
                try std.testing.expectEqual(@as(i64, 1), b.values[0].data.bigint[i]);
                try std.testing.expectEqual(@as(i32, 10), b.values[2].data.int[i]);
            }
            rows += b.row_count;
        }
        try std.testing.expectEqual(@as(usize, 1), rows);
    }

    // A pure range runs as range_sweep and keeps every right column.
    const base = try thindb.scan(allocator, users);
    const left = try base.compute(&.{.{ .name = "k", .expr = key }});
    const right = try (try thindb.scan(allocator, orders)).project(&.{ "oid", "qty" });
    var q = try left.join(right, .{
        .on = &.{},
        .ranges = &.{.{ .left = "k", .op = .gt, .right = "oid" }},
        .left_key_tail = 1,
    });
    defer q.deinit();
    try std.testing.expectEqual(@as(usize, 4), q.outputSchema().len);
    var uid_sum: i64 = 0;
    var rows: usize = 0;
    while (try q.next()) |b| {
        for (b.values[0].data.bigint[0..b.row_count]) |uid| uid_sum += uid;
        rows += b.row_count;
    }
    try std.testing.expectEqual(@as(usize, 8), rows);
    try std.testing.expectEqual(@as(i64, 20), uid_sum);
}

test "opaque: cross-side predicate via NLJ callback" {
    // 100 left rows, 100 right rows. Opaque predicate: keep pairs
    // where (a.x + a.y) > b.threshold. Can't be expressed as equi
    // or simple range — needs a computed value across sides.
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const schema_a = thindb.TableSchema{
        .columns = &.{
            .{ .name = "rowid", .type = .bigint },
            .{ .name = "x", .type = .bigint },
            .{ .name = "y", .type = .bigint },
        },
        .order_key = &.{"rowid"},
        .unique = true,
    };
    const schema_b = thindb.TableSchema{
        .columns = &.{
            .{ .name = "b_rowid", .type = .bigint },
            .{ .name = "threshold", .type = .bigint },
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
    {
        const Row = struct { rowid: i64, x: i64, y: i64 };
        const rows = try allocator.alloc(Row, 100);
        defer allocator.free(rows);
        // x + y = 2i. So row i has sum 2i.
        for (rows, 0..) |*r, i| r.* = .{ .rowid = @intCast(i), .x = @intCast(i), .y = @intCast(i) };
        try a.insert(rows);
    }
    try a.flush();
    const b = try db.table("b", schema_b, .{ .order_key = &ok_b, .unique = true });
    {
        const Row = struct { b_rowid: i64, threshold: i64 };
        const rows = try allocator.alloc(Row, 3);
        defer allocator.free(rows);
        rows[0] = .{ .b_rowid = 0, .threshold = 50 };
        rows[1] = .{ .b_rowid = 1, .threshold = 100 };
        rows[2] = .{ .b_rowid = 2, .threshold = 150 };
        try b.insert(rows);
    }
    try b.flush();

    // Predicate: a.x + a.y > b.threshold.
    // For each b.threshold T, # of a's with 2i > T: i > T/2 → 100 - ceil(T/2 + 1).
    //   T=50:  i > 25 → i ∈ [26..99] → 74 a's.
    //   T=100: i > 50 → i ∈ [51..99] → 49 a's.
    //   T=150: i > 75 → i ∈ [76..99] → 24 a's.
    // Total: 74 + 49 + 24 = 147.
    const Pred = struct {
        fn eval(
            ctx: ?*anyopaque,
            left: []const thindb.storage.ColumnView,
            lrow: u32,
            right: []const thindb.storage.ColumnView,
            rrow: u32,
        ) bool {
            _ = ctx;
            // Output schema: a.rowid(0), a.x(1), a.y(2), b.b_rowid(0), b.threshold(1).
            const x = left[1].data.bigint[lrow];
            const y = left[2].data.bigint[lrow];
            const t = right[1].data.bigint[rrow];
            return (x + y) > t;
        }
    };

    const left = try thindb.scan(allocator, a);
    const right = try thindb.scan(allocator, b);
    var q = try left.join(right, .{
        .on = &.{}, // no equi
        .opaque_predicate = .{ .eval = Pred.eval },
    });
    defer q.deinit();

    var rows: usize = 0;
    while (try q.next()) |bat| rows += bat.row_count;
    try std.testing.expectEqual(@as(usize, 147), rows);
}

test "skew: heavy build-side skew auto-routes to sort-merge" {
    // Build side has 100 rows all with k=42 → 100% in one key. Right
    // side has exactly one matching row. Test overrides absolute floor
    // down to 5 (sampled top * sample_interval = 10 * 10 = 100 >= 5) so
    // detection fires on a small dataset; production default is 20k.
    // Auto-route hands off to SMJ which emits the same 100 matches.
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const schema_l = thindb.TableSchema{
        .columns = &.{ .{ .name = "rowid", .type = .bigint }, .{ .name = "k", .type = .bigint } },
        .order_key = &.{"rowid"},
        .unique = true,
    };
    const schema_r = thindb.TableSchema{
        .columns = &.{ .{ .name = "b_rowid", .type = .bigint }, .{ .name = "k", .type = .bigint } },
        .order_key = &.{"b_rowid"},
        .unique = true,
    };
    const ok_l = [_][]const u8{"rowid"};
    const ok_r = [_][]const u8{"b_rowid"};

    var db = try thindb.Database.open(allocator, io, tmp.dir, .{
        .auto_flush_rows = std.math.maxInt(u64),
        .auto_flush_bytes = std.math.maxInt(usize),
        .auto_flush_secs = 0,
    });
    defer db.close();

    // Skewed side must be the SMALLER one so that .auto/ hash picks
    // it as the build side (Misra-Gries only watches build).
    const l = try db.table("l", schema_l, .{ .order_key = &ok_l, .unique = true });
    {
        const Row = struct { rowid: i64, k: i64 };
        const rows = try allocator.alloc(Row, 100);
        defer allocator.free(rows);
        for (rows, 0..) |*r, i| r.* = .{ .rowid = @intCast(i), .k = 42 };
        try l.insert(rows);
    }
    try l.flush();
    const r = try db.table("r", schema_r, .{ .order_key = &ok_r, .unique = true });
    {
        const Row = struct { b_rowid: i64, k: i64 };
        const rows = try allocator.alloc(Row, 1000);
        defer allocator.free(rows);
        for (rows, 0..) |*row, i| row.* = .{ .b_rowid = @intCast(i), .k = @intCast(i) };
        try r.insert(rows);
    }
    try r.flush();

    const left = try thindb.scan(allocator, l);
    const right = try thindb.scan(allocator, r);
    var q = try left.join(right, .{
        .on = &.{.{ .left = "k", .right = "k" }},
        .algorithm = .hash,
        .skew_ratio_threshold = 0.5,
        .skew_absolute_threshold = 5,
    });
    defer q.deinit();

    // 100 left rows × 1 matching right row (k=42) = 100 pairs.
    var rows: usize = 0;
    while (try q.next()) |b| rows += b.row_count;
    try std.testing.expectEqual(@as(usize, 100), rows);
}

test "skew: no-skew query with detection enabled runs normally" {
    // 100 distinct keys, no heavy hitter. Ratio gate stays well below
    // 0.5, so detection should NOT fire — query completes via hash.
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const schema_l = thindb.TableSchema{
        .columns = &.{.{ .name = "k", .type = .bigint }},
        .order_key = &.{"k"},
        .unique = true,
    };
    const ok = [_][]const u8{"k"};

    var db = try thindb.Database.open(allocator, io, tmp.dir, .{
        .auto_flush_rows = std.math.maxInt(u64),
        .auto_flush_bytes = std.math.maxInt(usize),
        .auto_flush_secs = 0,
    });
    defer db.close();

    const l = try db.table("l", schema_l, .{ .order_key = &ok, .unique = true });
    const r = try db.table("r", schema_l, .{ .order_key = &ok, .unique = true });
    {
        const Row = struct { k: i64 };
        const rows = try allocator.alloc(Row, 100);
        defer allocator.free(rows);
        for (rows, 0..) |*row, i| row.* = .{ .k = @intCast(i) };
        try l.insert(rows);
        try r.insert(rows);
    }
    try l.flush();
    try r.flush();

    const left = try thindb.scan(allocator, l);
    const right = try thindb.scan(allocator, r);
    var q = try left.join(right, .{
        .on = &.{.{ .left = "k", .right = "k" }},
        .algorithm = .hash,
        .skew_ratio_threshold = 0.5,
        .skew_absolute_threshold = 5,
    });
    defer q.deinit();

    var rows: usize = 0;
    while (try q.next()) |b| rows += b.row_count;
    try std.testing.expectEqual(@as(usize, 100), rows);
}

test "memory: sort over tight budget errors with MemoryBudgetExceeded" {
    // Budget = 4096 bytes. We try to sort 1000 rows (each ~16 bytes
    // accounted, so ~16000 bytes total). Should fail with the typed
    // error rather than an underlying allocator OOM.
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var db = try thindb.Database.open(allocator, io, tmp.dir, .{
        .query_memory_budget = 4096,
        .auto_flush_rows = std.math.maxInt(u64),
        .auto_flush_bytes = std.math.maxInt(usize),
        .auto_flush_secs = 0,
    });
    defer db.close();

    const schema = thindb.TableSchema{
        .columns = &.{ .{ .name = "id", .type = .bigint }, .{ .name = "v", .type = .bigint } },
        .order_key = &.{"id"},
        .unique = true,
    };
    const ok = [_][]const u8{"id"};
    const t = try db.table("t", schema, .{ .order_key = &ok, .unique = true });

    const Row = struct { id: i64, v: i64 };
    const rows = try allocator.alloc(Row, 1000);
    defer allocator.free(rows);
    for (rows, 0..) |*r, i| r.* = .{ .id = @intCast(i), .v = @intCast(100 - @as(i64, @intCast(i))) };
    try t.insert(rows);
    try t.flush();

    var base = try thindb.scan(allocator, t);
    var base_owned = true;
    defer if (base_owned) base.deinit();
    var q = try base.orderBy(&.{.{ .col = "v", .desc = false }});
    base_owned = false;
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

    const schema = thindb.TableSchema{
        .columns = &.{.{ .name = "id", .type = .bigint }},
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

    const schema_a = thindb.TableSchema{
        .columns = &.{
            .{ .name = "a_id", .type = .bigint },
            .{ .name = "a_val", .type = .string },
        },
        .order_key = &.{"a_id"},
        .unique = true,
    };
    const schema_b = thindb.TableSchema{
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

// ---------------------------------------------------------------------------
// Pass-through probe (LEFT/RIGHT + unique or empty build): probe columns are
// borrowed views, build columns gathered or bulk-NULLed. These pin the mode's
// row alignment, NULL handling, and its refusal to engage on duplicate keys.
// ---------------------------------------------------------------------------

test "join: LEFT pass-through — unique build keys emit every probe row in order" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var f = try outerFixture(allocator, io, tmp.dir);
    defer f.db.close();

    // orders LEFT JOIN users: build = users (uid 1,2,3 — unique) → the
    // pass-through probe runs. oid=103 (uid=99) misses → NULL user name.
    const left = try thindb.scan(allocator, f.orders);
    const right = try thindb.scan(allocator, f.users);
    var q = try left.join(right, .{
        .join_type = .left,
        .on = &.{.{ .left = "uid", .right = "uid" }},
        .algorithm = .hash,
    });
    defer q.deinit();

    // Schema: oid, uid, qty, name (right uid dropped).
    var oids: std.ArrayList(i64) = .empty;
    defer oids.deinit(allocator);
    var qtys: std.ArrayList(i32) = .empty;
    defer qtys.deinit(allocator);
    var name_null_oid: i64 = 0;
    var names_seen: usize = 0;
    while (try q.next()) |b| {
        try oids.appendSlice(allocator, b.values[0].data.bigint[0..b.row_count]);
        try qtys.appendSlice(allocator, b.values[2].data.int[0..b.row_count]);
        for (0..b.row_count) |i| {
            if (b.values[3].isValid(i)) {
                names_seen += 1;
            } else {
                name_null_oid = b.values[0].data.bigint[i];
            }
        }
    }
    // Every probe row emits exactly once, in probe order.
    try std.testing.expectEqualSlices(i64, &[_]i64{ 100, 101, 102, 103 }, oids.items);
    try std.testing.expectEqualSlices(i32, &[_]i32{ 10, 20, 30, 40 }, qtys.items);
    try std.testing.expectEqual(@as(usize, 3), names_seen);
    try std.testing.expectEqual(@as(i64, 103), name_null_oid);
}

test "join: LEFT pass-through — multi-key unique build (general probe path)" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const dim_schema = thindb.TableSchema{
        .columns = &.{
            .{ .name = "k1", .type = .bigint },
            .{ .name = "k2", .type = .int },
            .{ .name = "label", .type = .string },
        },
        .order_key = &.{"k1"},
        .unique = false,
    };
    const fact_schema = thindb.TableSchema{
        .columns = &.{
            .{ .name = "fid", .type = .bigint },
            .{ .name = "k1", .type = .bigint },
            .{ .name = "k2", .type = .int },
            .{ .name = "note", .type = .string },
        },
        .order_key = &.{"fid"},
        .unique = true,
    };
    const dim_ok = [_][]const u8{"k1"};
    const fact_ok = [_][]const u8{"fid"};

    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    const dim = try db.table("dim", dim_schema, .{ .order_key = &dim_ok, .row_group_size = 4 });
    try dim.insert(&.{
        .{ .k1 = @as(i64, 1), .k2 = @as(i32, 10), .label = "a" },
        .{ .k1 = @as(i64, 1), .k2 = @as(i32, 20), .label = "b" }, // same k1, unique (k1,k2)
        .{ .k1 = @as(i64, 2), .k2 = @as(i32, 10), .label = "c" },
    });
    try dim.flush();

    const fact = try db.table("fact", fact_schema, .{ .order_key = &fact_ok, .unique = true, .row_group_size = 4 });
    try fact.insert(&.{
        .{ .fid = @as(i64, 1), .k1 = @as(i64, 1), .k2 = @as(i32, 10), .note = "n1" },
        .{ .fid = @as(i64, 2), .k1 = @as(i64, 1), .k2 = @as(i32, 20), .note = "n2" },
        .{ .fid = @as(i64, 3), .k1 = @as(i64, 2), .k2 = @as(i32, 10), .note = "n3" },
        .{ .fid = @as(i64, 4), .k1 = @as(i64, 9), .k2 = @as(i32, 99), .note = "n4" }, // miss
    });
    try fact.flush();

    const left = try thindb.scan(allocator, fact);
    const right = try thindb.scan(allocator, dim);
    var q = try left.join(right, .{
        .join_type = .left,
        .on = &.{ .{ .left = "k1", .right = "k1" }, .{ .left = "k2", .right = "k2" } },
        .algorithm = .hash,
    });
    defer q.deinit();

    // Schema: fid, k1, k2, note, label (right k1/k2 dropped).
    var fids: std.ArrayList(i64) = .empty;
    defer fids.deinit(allocator);
    var labels: std.ArrayList(u8) = .empty;
    defer labels.deinit(allocator);
    while (try q.next()) |b| {
        try fids.appendSlice(allocator, b.values[0].data.bigint[0..b.row_count]);
        for (0..b.row_count) |i| {
            if (b.values[4].isValid(i)) {
                const sv = b.values[4].data.string;
                try labels.appendSlice(allocator, sv.rowBytes(i));
            } else {
                try labels.append(allocator, '-');
            }
        }
    }
    try std.testing.expectEqualSlices(i64, &[_]i64{ 1, 2, 3, 4 }, fids.items);
    try std.testing.expectEqualStrings("abc-", labels.items);
}

test "join: LEFT pass-through — empty build side null-extends every probe row" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var f = try outerFixture(allocator, io, tmp.dir);
    defer f.db.close();

    const empty_schema = thindb.TableSchema{
        .columns = &.{
            .{ .name = "uid", .type = .bigint },
            .{ .name = "tag", .type = .string },
            .{ .name = "score", .type = .int },
        },
        .order_key = &.{"uid"},
        .unique = true,
    };
    const empty_ok = [_][]const u8{"uid"};
    const empty = try f.db.table("empty_dim", empty_schema, .{ .order_key = &empty_ok, .unique = true });

    const left = try thindb.scan(allocator, f.orders);
    const right = try thindb.scan(allocator, empty);
    var q = try left.join(right, .{
        .join_type = .left,
        .on = &.{.{ .left = "uid", .right = "uid" }},
        .algorithm = .hash,
    });
    defer q.deinit();

    // Schema: oid, uid, qty, tag, score. All right cols NULL on all 4 rows.
    var rows: usize = 0;
    var oid_sum: i64 = 0;
    while (try q.next()) |b| {
        rows += b.row_count;
        for (0..b.row_count) |i| {
            oid_sum += b.values[0].data.bigint[i];
            try std.testing.expect(!b.values[3].isValid(i));
            try std.testing.expect(!b.values[4].isValid(i));
        }
    }
    try std.testing.expectEqual(@as(usize, 4), rows);
    try std.testing.expectEqual(@as(i64, 100 + 101 + 102 + 103), oid_sum);
}

test "join: INNER with empty build side short-circuits to no output" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var f = try outerFixture(allocator, io, tmp.dir);
    defer f.db.close();

    const empty_schema = thindb.TableSchema{
        .columns = &.{
            .{ .name = "uid", .type = .bigint },
            .{ .name = "tag", .type = .string },
        },
        .order_key = &.{"uid"},
        .unique = true,
    };
    const empty_ok = [_][]const u8{"uid"};
    const empty = try f.db.table("empty_dim2", empty_schema, .{ .order_key = &empty_ok, .unique = true });

    const left = try thindb.scan(allocator, f.orders);
    const right = try thindb.scan(allocator, empty);
    var q = try left.join(right, .{
        .join_type = .inner,
        .on = &.{.{ .left = "uid", .right = "uid" }},
        .algorithm = .hash,
    });
    defer q.deinit();

    var rows: usize = 0;
    while (try q.next()) |b| rows += b.row_count;
    try std.testing.expectEqual(@as(usize, 0), rows);
}

test "join: LEFT pass-through + range — unique-key candidate rejected null-extends" {
    // Build keys unique (one candidate per key) so the pass-through probe
    // runs; the range then rejects some candidates in place.
    //   k=1, l.x=10 vs r.y=15: passes → real match
    //   k=2, l.x=20 vs r.y=5:  fails  → null-extend
    //   k=3: no candidate      → null-extend
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const l_schema = thindb.TableSchema{
        .columns = &.{
            .{ .name = "k", .type = .bigint },
            .{ .name = "x", .type = .int },
        },
        .order_key = &.{"k"},
        .unique = true,
    };
    const r_schema = thindb.TableSchema{
        .columns = &.{
            .{ .name = "k", .type = .bigint },
            .{ .name = "y", .type = .int },
        },
        .order_key = &.{"k"},
        .unique = true,
    };
    const l_ok = [_][]const u8{"k"};
    const r_ok = [_][]const u8{"k"};

    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    const lt = try db.table("pl", l_schema, .{ .order_key = &l_ok, .unique = true });
    try lt.insert(&.{
        .{ .k = @as(i64, 1), .x = @as(i32, 10) },
        .{ .k = @as(i64, 2), .x = @as(i32, 20) },
        .{ .k = @as(i64, 3), .x = @as(i32, 30) },
    });
    try lt.flush();

    const rt = try db.table("pr", r_schema, .{ .order_key = &r_ok, .unique = true });
    try rt.insert(&.{
        .{ .k = @as(i64, 1), .y = @as(i32, 15) },
        .{ .k = @as(i64, 2), .y = @as(i32, 5) },
    });
    try rt.flush();

    const left = try thindb.scan(allocator, lt);
    const right = try thindb.scan(allocator, rt);
    var q = try left.join(right, .{
        .join_type = .left,
        .on = &.{.{ .left = "k", .right = "k" }},
        .ranges = &.{.{ .left = "x", .op = .lt, .right = "y" }},
        .algorithm = .hash,
    });
    defer q.deinit();

    // Schema: k, x, y.
    var ks: std.ArrayList(i64) = .empty;
    defer ks.deinit(allocator);
    var y_valid: std.ArrayList(u8) = .empty;
    defer y_valid.deinit(allocator);
    while (try q.next()) |b| {
        try ks.appendSlice(allocator, b.values[0].data.bigint[0..b.row_count]);
        for (0..b.row_count) |i| {
            try y_valid.append(allocator, if (b.values[2].isValid(i)) 'v' else '-');
        }
    }
    try std.testing.expectEqualSlices(i64, &[_]i64{ 1, 2, 3 }, ks.items);
    try std.testing.expectEqualStrings("v--", y_valid.items);
}

test "join: pass-through declines on duplicate build keys (fallback intact)" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var f = try outerFixture(allocator, io, tmp.dir);
    defer f.db.close();

    // users LEFT JOIN orders: build = orders whose uid has duplicates
    // (100 and 101 both uid=1) → NOT pass-through. uid=1 must fan out to
    // TWO rows — exactly what pass-through can't produce.
    const left = try thindb.scan(allocator, f.users);
    const right = try thindb.scan(allocator, f.orders);
    var q = try left.join(right, .{
        .join_type = .left,
        .on = &.{.{ .left = "uid", .right = "uid" }},
        .algorithm = .hash,
    });
    defer q.deinit();

    var rows: usize = 0;
    var uid1_rows: usize = 0;
    while (try q.next()) |b| {
        rows += b.row_count;
        for (0..b.row_count) |i| {
            if (b.values[0].data.bigint[i] == 1) uid1_rows += 1;
        }
    }
    try std.testing.expectEqual(@as(usize, 4), rows); // 2 + 1 + 1 null-extended
    try std.testing.expectEqual(@as(usize, 2), uid1_rows);
}

test "join: CROSS JOIN parses without ON and produces the cartesian product" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var db = try thindb.Database.open(allocator, io, tmp.dir, .{});
    defer db.close();

    const helpers = @import("sql_helpers.zig");
    try helpers.exec(allocator, db, "CREATE TABLE base (k BIGINT PRIMARY KEY)");
    try helpers.exec(allocator, db, "INSERT INTO base VALUES (10), (20), (30)");
    try helpers.exec(allocator, db, "CREATE TABLE spine (id BIGINT PRIMARY KEY)");
    try helpers.exec(allocator, db, "INSERT INTO spine VALUES (1), (2), (3), (4), (5)");

    const full = try helpers.collectBigints(allocator, db, "SELECT k + id AS s FROM base CROSS JOIN spine ORDER BY s");
    defer allocator.free(full);
    try std.testing.expectEqual(@as(usize, 15), full.len);

    // One-sided WHERE restricting the spine — the rollforward month-spine shape.
    const filtered = try helpers.collectBigints(allocator, db, "SELECT k + id AS s FROM base CROSS JOIN spine WHERE id <= 2 ORDER BY s");
    defer allocator.free(filtered);
    try std.testing.expectEqualSlices(i64, &.{ 11, 12, 21, 22, 31, 32 }, filtered);
}

test "join: COUNT(*) over a nested-loop join that outputs no columns counts every row" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, std.testing.io, tmp.dir, .{});
    defer db.close();

    const helpers = @import("sql_helpers.zig");
    try helpers.exec(allocator, db, "CREATE TABLE base (k BIGINT PRIMARY KEY)");
    try helpers.exec(allocator, db, "INSERT INTO base VALUES (10), (20), (30)");
    try helpers.exec(allocator, db, "CREATE TABLE spine (id BIGINT PRIMARY KEY)");
    // 300 x 100 rows crosses several output batches.
    var values: std.ArrayList(u8) = .empty;
    defer values.deinit(allocator);
    try values.appendSlice(allocator, "INSERT INTO spine VALUES (1)");
    for (2..301) |i| try values.print(allocator, ", ({d})", .{i});
    try helpers.exec(allocator, db, values.items);
    try helpers.exec(allocator, db, "CREATE TABLE wide (w BIGINT PRIMARY KEY)");
    values.clearRetainingCapacity();
    try values.appendSlice(allocator, "INSERT INTO wide VALUES (1)");
    for (2..101) |i| try values.print(allocator, ", ({d})", .{i});
    try helpers.exec(allocator, db, values.items);

    const cases = .{
        .{ "SELECT COUNT(*) FROM base CROSS JOIN spine", 900 },
        .{ "SELECT COUNT(*) FROM spine CROSS JOIN wide", 30000 },
        .{ "SELECT COUNT(*) FROM base CROSS JOIN spine WHERE id <= 2", 6 },
        .{ "SELECT COUNT(*) FROM base JOIN spine ON base.k > spine.id", 57 },
    };
    inline for (cases) |case| {
        const got = try helpers.collectBigints(allocator, db, case[0]);
        defer allocator.free(got);
        try std.testing.expectEqualSlices(i64, &.{case[1]}, got);
    }
}

/// Every row as its cells joined by `|` (NULL spelled out), rows joined by
/// newlines, after a header of the output column names.
fn crossRowsText(allocator: std.mem.Allocator, db: anytype, sql: []const u8) ![]u8 {
    const helpers = @import("sql_helpers.zig");
    var q = try helpers.runSql(allocator, db, sql);
    defer q.deinit();
    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(allocator);
    for (q.outputSchema()) |col| try out.print(allocator, "{s}:{s} ", .{ col.name, @tagName(col.type) });
    while (try q.next()) |batch| {
        for (0..batch.row_count) |row| {
            try out.append(allocator, '\n');
            for (batch.values, 0..) |col, c| {
                if (c > 0) try out.append(allocator, '|');
                if (!col.isValid(row)) {
                    try out.appendSlice(allocator, "NULL");
                    continue;
                }
                switch (col.data) {
                    .string, .varchar, .char => |sv| try out.appendSlice(allocator, sv.rowBytes(row)),
                    inline .tinyint, .smallint, .int, .bigint, .largeint, .date => |s| try out.print(allocator, "{d}", .{s[row]}),
                    .double => |s| try out.print(allocator, "{d:.3}", .{s[row]}),
                    else => return error.TestUnexpectedType,
                }
            }
        }
    }
    return out.toOwnedSlice(allocator);
}

test "join: GROUP BY over a cross join groups each side first and keeps every group's values" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, std.testing.io, tmp.dir, .{});
    defer db.close();

    const helpers = @import("sql_helpers.zig");
    try helpers.exec(allocator, db, "CREATE TABLE ent (id BIGINT PRIMARY KEY, grp VARCHAR(10), name VARCHAR(10), d DATE, v BIGINT)");
    try helpers.exec(allocator, db,
        \\INSERT INTO ent VALUES (1, 'A', 'x', '2024-01-15', 5), (2, 'a', 'x', '2024-03-01', 7),
        \\  (3, 'B', 'y', '2023-11-30', NULL), (4, NULL, 'z', '2024-02-29', 3), (5, NULL, 'z', '2024-05-31', 3)
    );
    // The spine repeats a value and holds a NULL: the key-only side groups.
    try helpers.exec(allocator, db, "CREATE TABLE spine (n BIGINT)");
    try helpers.exec(allocator, db, "INSERT INTO spine VALUES (1), (2), (2), (3), (NULL), (0)");

    // Each statement's `{s}` takes either nothing or a conjunct reading both
    // sides that holds for every row: it keeps the product whole beneath the
    // GROUP BY, the plan the rewrite replaces. The last statement's SELECT
    // list is the GROUP BY's own output, which keeps its plan and names.
    const cases = .{
        .{ "SELECT LOWER(e.grp) AS k, s.n, MAX(e.v) AS mx, MIN(e.d) AS lo, ANY_VALUE(e.name) AS nm, COUNT(DISTINCT e.v) AS nv, MAX(e.v) + s.n AS bumped " ++
            "FROM ent e CROSS JOIN spine s WHERE s.n > 0 {s} GROUP BY LOWER(e.grp), s.n ORDER BY k, s.n", 9, true },
        .{ "SELECT e.grp AS g, s.n AS step, MAX(e.v) AS mx, MAX_BY(e.name, e.d) AS latest FROM ent e CROSS JOIN spine s WHERE e.id > 0 {s} GROUP BY e.grp, s.n ORDER BY g, step", 20, true },
        .{ "SELECT s.n, e.id, MAX(s.n * 10) AS t FROM ent e CROSS JOIN spine s WHERE e.v > 4 {s} GROUP BY e.id, s.n ORDER BY e.id, s.n", 10, true },
        .{ "SELECT s.n, e.grp, MIN(e.v) AS lo FROM ent e CROSS JOIN spine s WHERE s.n > 100 {s} GROUP BY e.grp, s.n", 0, true },
        .{ "SELECT s.n, e.grp, MIN(e.v) AS lo FROM ent e CROSS JOIN spine s WHERE e.v > 100 {s} GROUP BY e.grp, s.n", 0, true },
        .{ "SELECT e.grp, s.n, MAX(e.v) AS mx FROM ent e CROSS JOIN spine s WHERE s.n > 0 {s} GROUP BY e.grp, s.n ORDER BY e.grp, s.n", 12, false },
    };
    inline for (cases) |c| {
        const sql = comptime std.fmt.comptimePrint(c[0], .{""});
        const rewritten = try crossRowsText(allocator, db, sql);
        defer allocator.free(rewritten);
        const product = try crossRowsText(allocator, db, comptime std.fmt.comptimePrint(c[0], .{"AND (e.id > 0 OR s.n IS NULL)"}));
        defer allocator.free(product);
        try std.testing.expectEqualStrings(product, rewritten);
        try std.testing.expectEqual(@as(usize, c[1]), std.mem.count(u8, rewritten, "\n"));
        try std.testing.expectEqual(c[2], try groupsBeforeProduct(allocator, db, sql));
    }
}

/// Whether the plan of `sql` aggregates below its nested-loop join.
fn groupsBeforeProduct(allocator: std.mem.Allocator, db: anytype, sql: []const u8) !bool {
    const helpers = @import("sql_helpers.zig");
    const explain = try std.mem.concat(allocator, u8, &.{ "EXPLAIN ", sql });
    defer allocator.free(explain);
    const lines = try helpers.collectStrings(allocator, db, explain);
    defer helpers.freeStrings(allocator, lines);
    var in_join = false;
    for (lines) |maybe_line| {
        const line = maybe_line orelse continue;
        if (std.mem.indexOf(u8, line, "NestedLoopJoin") != null) in_join = true;
        if (in_join and (std.mem.indexOf(u8, line, "Aggregate") != null or std.mem.indexOf(u8, line, "GroupBy") != null)) return true;
    }
    return false;
}

test "join: parenthesized ON conditions join like the bare conjuncts" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, std.testing.io, tmp.dir, .{});
    defer db.close();

    const helpers = @import("sql_helpers.zig");
    try helpers.exec(allocator, db, "CREATE TABLE a (id BIGINT PRIMARY KEY, v BIGINT NOT NULL)");
    try helpers.exec(allocator, db, "INSERT INTO a VALUES (1, 10), (2, 20), (3, 30)");
    try helpers.exec(allocator, db, "CREATE TABLE b (bid BIGINT PRIMARY KEY, aid BIGINT NOT NULL, amount BIGINT)");
    try helpers.exec(allocator, db, "INSERT INTO b VALUES (1, 1, 5), (2, 1, 50), (3, 2, 7), (4, 3, 70)");

    const cases = .{
        .{ "SELECT b.bid FROM a JOIN b ON (a.id = b.aid) ORDER BY b.bid", &[_]i64{ 1, 2, 3, 4 } },
        .{ "SELECT b.bid FROM a INNER JOIN b ON (a.id = b.aid AND b.amount > 6) ORDER BY b.bid", &[_]i64{ 2, 3, 4 } },
        .{ "SELECT b.bid FROM a JOIN b ON (a.id = b.aid) AND (b.amount > 6) ORDER BY b.bid", &[_]i64{ 2, 3, 4 } },
        .{ "SELECT b.bid FROM a JOIN b ON ((a.id = b.aid) AND b.amount > 6) ORDER BY b.bid", &[_]i64{ 2, 3, 4 } },
        .{ "SELECT b.bid FROM a JOIN b ON (a.id = b.aid AND (b.amount > 6 AND b.amount < 60)) ORDER BY b.bid", &[_]i64{ 2, 3 } },
        .{ "SELECT b.bid FROM a JOIN b ON (a.id = b.aid AND b.amount BETWEEN 6 AND 60) ORDER BY b.bid", &[_]i64{ 2, 3 } },
        .{ "SELECT b.bid FROM a JOIN b ON (b.amount IS NOT NULL AND a.id = b.aid) ORDER BY b.bid", &[_]i64{ 1, 2, 3, 4 } },
        .{ "SELECT b.bid FROM a JOIN b ON (a.id + 1) = (b.aid + 1) ORDER BY b.bid", &[_]i64{ 1, 2, 3, 4 } },
        .{ "SELECT b.bid FROM a JOIN b ON ((a.id + 1) = b.aid + 1) ORDER BY b.bid", &[_]i64{ 1, 2, 3, 4 } },
        .{ "SELECT COALESCE(b.bid, 0) AS c FROM a LEFT JOIN b ON (a.id = b.aid AND b.amount > 60) ORDER BY a.id", &[_]i64{ 0, 0, 4 } },
        .{ "SELECT b.bid FROM a JOIN b ON (a.id = b.aid) JOIN a AS a2 ON (a2.id = b.aid) ORDER BY b.bid", &[_]i64{ 1, 2, 3, 4 } },
        .{ "SELECT b.bid FROM a JOIN b ON (a.id = b.aid OR b.amount > 6) ORDER BY b.bid", &[_]i64{ 1, 2, 2, 2, 3, 3, 3, 4, 4, 4 } },
        .{ "SELECT COALESCE(b.bid, 0) AS c FROM a LEFT JOIN b ON (a.id = b.aid OR b.amount > 60) ORDER BY a.id, c", &[_]i64{ 1, 2, 4, 3, 4, 4 } },
    };
    inline for (cases) |case| {
        const got = try helpers.collectBigints(allocator, db, case[0]);
        defer allocator.free(got);
        try std.testing.expectEqualSlices(i64, case[1], got);
    }

    try std.testing.expectError(error.SqlExpectedToken, helpers.runSql(allocator, db, "SELECT b.bid FROM a JOIN b ON (a.id = b.aid"));
}

fn planMentions(allocator: std.mem.Allocator, db: anytype, sql: []const u8, needle: []const u8) !bool {
    const helpers = @import("sql_helpers.zig");
    const explain_sql = try std.fmt.allocPrint(allocator, "EXPLAIN {s}", .{sql});
    defer allocator.free(explain_sql);
    var q = try helpers.runSql(allocator, db, explain_sql);
    defer q.deinit();
    var found = false;
    while (try q.next()) |b| {
        const lines = b.values[0].data.string;
        for (0..b.row_count) |i| {
            if (std.mem.indexOf(u8, lines.rowBytes(i), needle) != null) found = true;
        }
    }
    return found;
}

test "join: a comma joins like CROSS JOIN, keyed by the WHERE's equalities" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, std.testing.io, tmp.dir, .{});
    defer db.close();

    const helpers = @import("sql_helpers.zig");
    try helpers.exec(allocator, db, "CREATE TABLE a (id BIGINT PRIMARY KEY, v BIGINT NOT NULL, code VARCHAR(8))");
    try helpers.exec(allocator, db, "INSERT INTO a VALUES (1, 10, '2'), (2, 20, '3'), (3, 30, 'x')");
    try helpers.exec(allocator, db, "CREATE TABLE b (bid BIGINT PRIMARY KEY, aid BIGINT, amount BIGINT NOT NULL)");
    try helpers.exec(allocator, db, "INSERT INTO b VALUES (1, 1, 5), (2, 1, 50), (3, 2, 7), (4, 3, 70), (5, NULL, 9)");
    try helpers.exec(allocator, db, "CREATE TABLE c (cid BIGINT PRIMARY KEY, bid BIGINT NOT NULL, id BIGINT NOT NULL)");
    try helpers.exec(allocator, db, "INSERT INTO c VALUES (1, 1, 100), (2, 3, 200), (3, 3, 300), (4, 9, 400)");

    const cases = .{
        .{ "SELECT COUNT(*) FROM a, b", &[_]i64{15} },
        .{ "SELECT b.bid FROM a, b WHERE a.id = b.aid ORDER BY b.bid", &[_]i64{ 1, 2, 3, 4 } },
        .{ "SELECT b.bid FROM a, b WHERE b.aid = a.id ORDER BY b.bid", &[_]i64{ 1, 2, 3, 4 } },
        // Unqualified columns side by the input that has them.
        .{ "SELECT bid FROM a, b WHERE id = aid AND v > 10 ORDER BY bid", &[_]i64{ 3, 4 } },
        // The right key stays readable.
        .{ "SELECT b.aid FROM a, b WHERE a.id = b.aid ORDER BY b.bid", &[_]i64{ 1, 1, 2, 3 } },
        // An equality beside a cross-input condition that isn't one.
        .{ "SELECT b.bid FROM a, b WHERE a.id = b.aid AND a.v < b.amount ORDER BY b.bid", &[_]i64{ 2, 4 } },
        .{ "SELECT b.bid FROM a, b WHERE a.id = b.aid OR b.amount = 9 ORDER BY b.bid, a.id", &[_]i64{ 1, 2, 3, 4, 5, 5, 5 } },
        // Three inputs: each equality keys the join that first sees both columns.
        .{ "SELECT c.cid FROM a, b, c WHERE a.id = b.aid AND b.bid = c.bid ORDER BY c.cid", &[_]i64{ 1, 2, 3 } },
        .{ "SELECT c.cid FROM a, b, c WHERE b.bid = c.bid AND a.id = b.aid AND a.v = 20 ORDER BY c.cid", &[_]i64{ 2, 3 } },
        .{ "SELECT c.cid FROM a, b, c WHERE a.id = b.aid AND c.bid = a.id ORDER BY c.cid, b.bid", &[_]i64{ 1, 1, 2, 3 } },
        // A comma between join chains, and CROSS JOIN, key the same way.
        .{ "SELECT c.cid FROM a, b JOIN c ON b.bid = c.bid WHERE a.id = b.aid ORDER BY c.cid", &[_]i64{ 1, 2, 3 } },
        .{ "SELECT c.cid FROM a CROSS JOIN b CROSS JOIN c WHERE a.id = b.aid AND b.bid = c.bid ORDER BY c.cid", &[_]i64{ 1, 2, 3 } },
        // A comma binds looser than JOIN: c right-joins b alone, then every
        // row crosses a (4 c rows, 3 with a match in b; 3 a rows).
        .{ "SELECT COUNT(*) FROM a, b RIGHT JOIN c ON b.bid = c.bid", &[_]i64{12} },
        .{ "SELECT COUNT(*) FROM a, b RIGHT JOIN c ON b.bid = c.bid WHERE b.bid IS NULL", &[_]i64{3} },
        // An ON expression's hidden left column doesn't hide the join below it.
        .{ "SELECT b.bid FROM a JOIN b ON a.id + 0 = b.aid WHERE a.v = 20 ORDER BY b.bid", &[_]i64{3} },
        .{ "SELECT c.cid FROM a JOIN b ON a.id + 0 = b.aid, c WHERE b.bid = c.bid ORDER BY c.cid", &[_]i64{ 1, 2, 3 } },
        // A derived table on either side.
        .{ "SELECT a.id FROM a, (SELECT aid, SUM(amount) AS s FROM b GROUP BY aid) t WHERE a.id = t.aid AND t.s > 10 ORDER BY a.id", &[_]i64{ 1, 3 } },
        // Text meets a number as the comparison reads it.
        .{ "SELECT b.bid FROM a, b WHERE a.code = b.aid ORDER BY b.bid", &[_]i64{ 3, 4 } },
    };
    inline for (cases) |case| {
        const got = try helpers.collectBigints(allocator, db, case[0]);
        defer allocator.free(got);
        try std.testing.expectEqualSlices(i64, case[1], got);
    }

    const keyed = .{
        "SELECT b.bid FROM a, b WHERE a.id = b.aid",
        "SELECT c.cid FROM a, b, c WHERE a.id = b.aid AND b.bid = c.bid",
        "SELECT c.cid FROM a, b JOIN c ON b.bid = c.bid WHERE a.id = b.aid",
        "SELECT c.cid FROM a JOIN b ON a.id + 0 = b.aid, c WHERE b.bid = c.bid",
    };
    inline for (keyed) |sql| try std.testing.expect(!try planMentions(allocator, db, sql, "NestedLoopJoin"));
    try std.testing.expect(try planMentions(allocator, db, "SELECT COUNT(*) FROM a, b", "NestedLoopJoin"));

    // Each chain's ON sees only its own names.
    try helpers.expectRunError(allocator, db, "SELECT c.cid FROM a, b JOIN c ON a.id = c.bid", error.SqlOnRefsUnknownTable);
    // `id` is in both a and c: ambiguous, not read from the first input.
    try helpers.expectRunError(allocator, db, "SELECT b.bid FROM a, b, c WHERE id = 1", error.ColumnNotFound);
}

// An ON condition on an outer join's preserved side decides which rows
// match: a row failing it null-extends rather than dropping out.
test "join: an ON condition on the preserved side null-extends the rows it rejects" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, std.testing.io, tmp.dir, .{});
    defer db.close();

    const helpers = @import("sql_helpers.zig");
    try helpers.exec(allocator, db, "CREATE TABLE t (id BIGINT PRIMARY KEY, qty INT)");
    try helpers.exec(allocator, db, "INSERT INTO t VALUES (1, 10), (2, 20), (3, 30), (4, NULL)");
    try helpers.exec(allocator, db, "CREATE TABLE o (oid BIGINT PRIMARY KEY, tid BIGINT, amount INT)");
    try helpers.exec(allocator, db, "INSERT INTO o VALUES (10, 1, 5), (11, 1, 7), (12, 3, 9), (13, 4, 1), (14, NULL, 8)");

    const pair = "SELECT COALESCE(t.id, 0) * 100 + COALESCE(o.oid, 0) AS k FROM ";
    const cases = .{
        .{ pair ++ "t LEFT JOIN o ON t.id = o.tid AND t.qty > 15 ORDER BY k", &[_]i64{ 100, 200, 312, 400 } },
        .{ pair ++ "t RIGHT JOIN o ON t.id = o.tid AND o.amount > 6 ORDER BY k", &[_]i64{ 10, 13, 14, 111, 312 } },
        .{ pair ++ "t FULL JOIN o ON t.id = o.tid AND t.qty > 15 AND o.amount > 6 ORDER BY k", &[_]i64{ 10, 11, 13, 14, 100, 200, 312, 400 } },
        .{ pair ++ "t LEFT JOIN o ON t.id = o.tid AND t.qty IS NOT NULL ORDER BY k", &[_]i64{ 110, 111, 200, 312, 400 } },
        .{ pair ++ "t LEFT JOIN o ON t.id = o.tid AND t.qty IS NULL ORDER BY k", &[_]i64{ 100, 200, 300, 413 } },
        .{ pair ++ "t LEFT JOIN o ON t.id = o.tid AND t.qty > 15 AND t.qty < 25 ORDER BY k", &[_]i64{ 100, 200, 300, 400 } },
        // The null-supplying side's condition still filters that side.
        .{ pair ++ "t LEFT JOIN o ON t.id = o.tid AND t.qty < 25 AND o.amount > 6 ORDER BY k", &[_]i64{ 111, 200, 300, 400 } },
        // An expression key, and a condition against a constant expression.
        .{ pair ++ "t LEFT JOIN o ON t.id + 0 = o.tid AND t.qty > 15 ORDER BY k", &[_]i64{ 100, 200, 312, 400 } },
        .{ pair ++ "t LEFT JOIN o ON t.id = o.tid AND t.qty > abs(-15) ORDER BY k", &[_]i64{ 100, 200, 312, 400 } },
        // No key at all: a filtered cross product.
        .{ pair ++ "t LEFT JOIN o ON t.qty = 20 ORDER BY k", &[_]i64{ 100, 210, 211, 212, 213, 214, 300, 400 } },
        .{ pair ++ "t RIGHT JOIN o ON o.amount > 6 ORDER BY k", &[_]i64{ 10, 13, 111, 112, 114, 211, 212, 214, 311, 312, 314, 411, 412, 414 } },
        // WHERE still filters the joined rows.
        .{ pair ++ "t LEFT JOIN o ON t.id = o.tid WHERE t.qty > 15 ORDER BY k", &[_]i64{ 200, 312 } },
        .{ "SELECT COUNT(*) FROM t LEFT JOIN o ON t.id = o.tid AND t.qty > 15", &[_]i64{4} },
        // A condition on an earlier input of the chain.
        .{ "SELECT COUNT(*) FROM t LEFT JOIN o ON t.id = o.tid LEFT JOIN o AS o2 ON o2.oid = o.oid AND t.qty > 15", &[_]i64{5} },
        .{ "SELECT COUNT(o2.oid) FROM t LEFT JOIN o ON t.id = o.tid LEFT JOIN o AS o2 ON o2.oid = o.oid AND t.qty > 15", &[_]i64{1} },
    };
    for (0..2) |phase| {
        if (phase == 1) for ([_][]const u8{ "t", "o" }) |name| {
            const tbl = try db.openTable(name, .{});
            try tbl.flush();
        };
        inline for (cases) |case| {
            const got = try helpers.collectBigints(allocator, db, case[0]);
            defer allocator.free(got);
            try std.testing.expectEqualSlices(i64, case[1], got);
        }
    }
}

// Any ON condition beyond keys and one-sided filters (a cross-input
// expression, <>, OR, a range on an outer join, a constant) decides per pair
// whether two rows match.
test "join: a general ON condition decides matching per pair" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, std.testing.io, tmp.dir, .{});
    defer db.close();

    const helpers = @import("sql_helpers.zig");
    try helpers.exec(allocator, db, "CREATE TABLE t (id BIGINT PRIMARY KEY, qty INT)");
    try helpers.exec(allocator, db, "INSERT INTO t VALUES (1, 10), (2, 20), (3, 30), (4, NULL)");
    try helpers.exec(allocator, db, "CREATE TABLE o (oid BIGINT PRIMARY KEY, tid BIGINT, amount INT)");
    try helpers.exec(allocator, db, "INSERT INTO o VALUES (10, 1, 5), (11, 1, 7), (12, 3, 9), (13, 4, 1), (14, NULL, 8)");

    const pair = "SELECT COALESCE(t.id, 0) * 100 + COALESCE(o.oid, 0) AS k FROM ";
    const cases = .{
        .{ pair ++ "t LEFT JOIN o ON t.id = o.tid AND t.qty < o.amount * 2 ORDER BY k", &[_]i64{ 111, 200, 300, 400 } },
        .{ pair ++ "t LEFT JOIN o ON t.id < o.tid ORDER BY k", &[_]i64{ 112, 113, 212, 213, 313, 400 } },
        .{ pair ++ "t RIGHT JOIN o ON t.id = o.tid AND o.amount <> t.qty - 3 ORDER BY k", &[_]i64{ 11, 13, 14, 110, 312 } },
        .{ pair ++ "t FULL JOIN o ON t.id = o.tid AND t.qty + o.amount > 20 ORDER BY k", &[_]i64{ 10, 11, 13, 14, 100, 200, 312, 400 } },
        .{ pair ++ "t JOIN o ON t.id = o.tid AND o.amount <> t.qty - 3 ORDER BY k", &[_]i64{ 110, 312 } },
        .{ pair ++ "t JOIN o ON t.id = o.tid OR t.qty = o.amount * 4 ORDER BY k", &[_]i64{ 110, 111, 210, 312, 413 } },
        .{ pair ++ "t LEFT JOIN o ON t.id = o.tid OR t.qty = o.amount * 4 ORDER BY k", &[_]i64{ 110, 111, 210, 312, 413 } },
        .{ pair ++ "t LEFT JOIN o ON t.id = o.tid AND (t.qty > 15 OR o.amount > 6) ORDER BY k", &[_]i64{ 111, 200, 312, 400 } },
        .{ pair ++ "t LEFT JOIN o ON t.id = o.tid AND o.oid IN (SELECT oid FROM o WHERE amount > 6) ORDER BY k", &[_]i64{ 111, 200, 312, 400 } },
        .{ pair ++ "t RIGHT JOIN o ON t.qty > o.amount * 3 ORDER BY k", &[_]i64{ 113, 210, 213, 310, 311, 312, 313, 314 } },
        .{ "SELECT COUNT(*) FROM t JOIN o ON TRUE", &[_]i64{20} },
        .{ "SELECT COUNT(*) FROM t JOIN o ON 1 = 1", &[_]i64{20} },
        .{ "SELECT COUNT(*) FROM t LEFT JOIN o ON 1 = 0", &[_]i64{4} },
        .{ "SELECT COUNT(*) FROM t JOIN o ON id < tid", &[_]i64{5} },
    };
    for (0..2) |phase| {
        if (phase == 1) for ([_][]const u8{ "t", "o" }) |name| {
            const tbl = try db.openTable(name, .{});
            try tbl.flush();
        };
        inline for (cases) |case| {
            const got = try helpers.collectBigints(allocator, db, case[0]);
            defer allocator.free(got);
            try std.testing.expectEqualSlices(i64, case[1], got);
        }
    }

    // The residual's operands stay out of the output.
    inline for (.{
        "SELECT * FROM t JOIN o ON t.id = o.tid AND t.qty + o.amount > 20",
        "SELECT * FROM t LEFT JOIN o ON t.qty + o.amount > 20",
    }) |sql| {
        var q = try helpers.runSql(allocator, db, sql);
        defer q.deinit();
        try std.testing.expectEqual(@as(usize, 5), q.outputSchema().len);
    }
    try std.testing.expect(try planMentions(allocator, db, "SELECT o.oid FROM t LEFT JOIN o ON t.id = o.tid AND t.qty < o.amount", "ON residual"));
    try helpers.expectRunError(allocator, db, "SELECT COUNT(*) FROM t JOIN o ON t.id = x.tid", error.SqlOnRefsUnknownTable);
    try helpers.expectRunError(allocator, db, "SELECT COUNT(*) FROM t JOIN o ON t.id < x.tid", error.SqlOnRefsUnknownTable);
}

/// `dim`: five scattered keys `k` (3, 17, 40, 41, 77) with `c = k % 2`.
/// `fact`: keys 0..99 in key order across many row groups, `20 * (k % 7 + 1)`
/// rows of each, `c = k % 2`.
fn openKeySetDb(allocator: std.mem.Allocator, dir: std.Io.Dir) !*thindb.Database {
    const db = try thindb.Database.open(allocator, std.testing.io, dir, .{ .max_dop = 4, .auto_flush_secs = 0 });
    errdefer db.close();
    const Row = struct { k: i64, c: i64 };
    const keyed = thindb.TableSchema{
        .columns = &.{ .{ .name = "k", .type = .bigint }, .{ .name = "c", .type = .bigint } },
        .order_key = &.{"k"},
        .unique = false,
    };
    const dim = try db.table("dim", keyed, .{ .order_key = &.{"k"}, .unique = false });
    var dim_rows: [5]Row = undefined;
    for (&dim_rows, [_]i64{ 3, 17, 40, 41, 77 }) |*row, k| row.* = .{ .k = k, .c = @mod(k, 2) };
    try dim.insert(&dim_rows);
    try dim.flush();

    const fact = try db.table("fact", keyed, .{ .order_key = &.{"k"}, .unique = false, .row_group_size = 256 });
    var fact_rows: std.ArrayList(Row) = .empty;
    defer fact_rows.deinit(allocator);
    for (0..100) |k| {
        for (0..20 * (k % 7 + 1)) |_| try fact_rows.append(allocator, .{ .k = @intCast(k), .c = @intCast(k % 2) });
    }
    try fact.insert(fact_rows.items);
    try fact.flush();
    return db;
}

// Issue #565. An INNER join offers its build keys' few distinct values to the
// probe side, through a GROUP BY on them, as a set that skips row groups. Only
// probe rows the join can't match may go: a group under a LIMIT, or keyed on
// an expression of the column, keeps every row.
test "join: a small build key set prunes the probe side without changing results" {
    const allocator = std.testing.allocator;
    const helpers = @import("sql_helpers.zig");
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const db = try openKeySetDb(allocator, tmp.dir);
    defer db.close();

    const cases = .{
        .{ "SELECT e.k * 1000 + t.n FROM dim e JOIN (SELECT k, COUNT(*) AS n FROM fact GROUP BY k) t ON t.k = e.k ORDER BY 1", &[_]i64{ 3080, 17080, 40120, 41140, 77020 } },
        .{ "SELECT e.k * 1000 + t.n FROM dim e JOIN (SELECT k, c, COUNT(*) AS n FROM fact GROUP BY k, c) t ON t.k = e.k AND t.c = e.c ORDER BY 1", &[_]i64{ 3080, 17080, 40120, 41140, 77020 } },
        .{ "SELECT e.k * 1000 + t.n FROM dim e JOIN (SELECT k, c, COUNT(*) AS n FROM fact WHERE k = 50 GROUP BY k, c) t ON t.k = e.k AND t.c = e.c ORDER BY 1", &[_]i64{} },
        .{ "SELECT e.k * 1000 + t.n FROM dim e JOIN (SELECT k, c, COUNT(*) AS n FROM fact WHERE k = 40 GROUP BY k, c) t ON t.k = e.k AND t.c = e.c ORDER BY 1", &[_]i64{40120} },
        .{ "SELECT e.k * 1000 + t.n FROM dim e JOIN (SELECT k, COUNT(*) AS n FROM fact GROUP BY k ORDER BY k LIMIT 10) t ON t.k = e.k ORDER BY 1", &[_]i64{3080} },
        .{ "SELECT e.k * 1000 + t.n FROM dim e JOIN (SELECT k, c, COUNT(*) AS n FROM fact GROUP BY k, c ORDER BY k LIMIT 10) t ON t.k = e.k AND t.c = e.c ORDER BY 1", &[_]i64{3080} },
        .{ "SELECT e.k * 1000 + t.n FROM dim e JOIN (SELECT k + 1 AS k1, COUNT(*) AS n FROM fact GROUP BY k + 1) t ON t.k1 = e.k ORDER BY 1", &[_]i64{ 3060, 17060, 40100, 41120, 77140 } },
        .{ "SELECT e.k * 1000 + t.n FROM dim e JOIN (SELECT k + 1 AS k, c, COUNT(*) AS n FROM fact GROUP BY k + 1, c) t ON t.k = e.k ORDER BY 1", &[_]i64{ 3060, 17060, 40100, 41120, 77140 } },
        .{ "SELECT COUNT(*) FROM dim e JOIN fact f ON f.k = e.k", &[_]i64{440} },
    };
    inline for (cases) |case| {
        errdefer std.debug.print("case: {s}\n", .{case[0]});
        const got = try helpers.collectBigints(allocator, db, case[0]);
        defer allocator.free(got);
        try std.testing.expectEqualSlices(i64, case[1], got);
    }
}

// A join learns its probe side's keys before building and filters the build
// side's scan rows to them. Only build rows the join can't match may go: a
// preserved build side, a LIMIT or a window in the build input, and NULL keys
// under `<=>` keep every row. (Tests run the pass whatever the input sizes.)
test "join: probe keys filter the build side without changing results" {
    const allocator = std.testing.allocator;
    const helpers = @import("sql_helpers.zig");
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const db = try openKeySetDb(allocator, tmp.dir);
    defer db.close();
    try helpers.exec(allocator, db, "CREATE TABLE nk (k BIGINT, s VARCHAR(8), v BIGINT NOT NULL, PRIMARY KEY (v))");
    try helpers.exec(allocator, db, "INSERT INTO nk VALUES (1, 'a', 1), (1, 'a', 2), (2, 'b', 3), (NULL, NULL, 4), (NULL, NULL, 5), (3, 'c', 6), (4, 'd', 7)");

    const odd_groups = "(SELECT k, COUNT(*) AS n FROM fact WHERE c = 1 GROUP BY k) t";
    const cases = .{
        .{ "SELECT COUNT(*) * 100000 + SUM(t.n) FROM fact f JOIN " ++ odd_groups ++ " ON t.k = f.k WHERE f.k IN (3, 17, 40)", &[_]i64{16012800} },
        .{ "SELECT COUNT(*) * 100000 + COUNT(t.n) FROM fact f LEFT JOIN " ++ odd_groups ++ " ON t.k = f.k WHERE f.k IN (3, 17, 40)", &[_]i64{28000160} },
        .{ "SELECT COUNT(*) * 100000 + COUNT(f.k) FROM fact f RIGHT JOIN " ++ odd_groups ++ " ON t.k = f.k AND f.k IN (3, 17, 40)", &[_]i64{20800160} },
        .{ "SELECT COUNT(*) FROM (SELECT k FROM fact WHERE k IN (3, 40)) f FULL JOIN " ++ odd_groups ++ " ON t.k = f.k", &[_]i64{249} },
        .{ "SELECT COUNT(*) * 100000 + SUM(t.n) FROM fact f JOIN (SELECT k, c, COUNT(*) AS n FROM fact WHERE c >= 0 GROUP BY k, c) t ON t.k = f.k AND t.c = f.c WHERE f.k IN (3, 40)", &[_]i64{20020800} },
        .{ "SELECT COUNT(*) FROM fact f JOIN (SELECT k, COUNT(*) AS n FROM fact WHERE c = 1 GROUP BY k ORDER BY k LIMIT 3) t ON t.k = f.k WHERE f.k IN (3, 17, 40)", &[_]i64{80} },
        .{ "SELECT SUM(t.rn) FROM fact f JOIN (SELECT k, ROW_NUMBER() OVER (ORDER BY k) AS rn FROM (SELECT DISTINCT k FROM fact WHERE c = 1) d) t ON t.k = f.k WHERE f.k IN (3, 17, 40)", &[_]i64{880} },
        .{ "SELECT COUNT(*) FROM fact f JOIN (SELECT k, COUNT(*) AS n FROM fact GROUP BY k HAVING COUNT(*) > 100) t ON t.k = f.k WHERE f.k IN (3, 40, 41)", &[_]i64{260} },
        .{ "SELECT COUNT(*) FROM nk a JOIN (SELECT k, COUNT(*) AS n FROM nk WHERE v > 0 GROUP BY k) b ON a.k = b.k WHERE a.v < 6", &[_]i64{3} },
        .{ "SELECT COUNT(*) FROM nk a JOIN (SELECT k, COUNT(*) AS n FROM nk WHERE v > 0 GROUP BY k) b ON a.k <=> b.k WHERE a.v < 6", &[_]i64{5} },
        .{ "SELECT COUNT(*) * 100 + COUNT(b.n) FROM nk a LEFT JOIN (SELECT s, COUNT(*) AS n FROM nk WHERE v > 0 GROUP BY s) b ON a.s = b.s WHERE a.v IN (1, 4, 6)", &[_]i64{302} },
        .{ "SELECT COUNT(*) * 100000 + SUM(t.n) FROM fact f JOIN (SELECT g.k AS k, COUNT(*) AS n FROM fact g JOIN dim d ON d.c = g.c GROUP BY g.k) t ON t.k = f.k WHERE f.k IN (3, 17, 40)", &[_]i64{28065600} },
        .{ "SELECT COUNT(*) * 100000 + COUNT(t.n) FROM fact f LEFT JOIN (SELECT g.k AS k, COUNT(*) AS n FROM fact g JOIN dim d ON d.c = g.c WHERE g.k <> 17 GROUP BY g.k) t ON t.k = f.k WHERE f.k IN (3, 17, 40)", &[_]i64{28000200} },
        .{ "SELECT COUNT(*) * 100000 + SUM(t.n) FROM fact f JOIN (SELECT g.k AS k, COUNT(*) AS n FROM fact g JOIN dim d ON d.k = g.k GROUP BY g.k) t ON t.k = f.k WHERE f.k IN (3, 40, 50)", &[_]i64{20020800} },
        .{ "SELECT COUNT(*) FROM fact f JOIN (SELECT g.k AS k, COUNT(*) AS n FROM fact g JOIN dim d ON d.c = g.c GROUP BY g.k ORDER BY n DESC, g.k LIMIT 5) t ON t.k = f.k WHERE f.k IN (3, 17, 40, 97)", &[_]i64{0} },
    };
    inline for (cases) |case| {
        errdefer std.debug.print("case: {s}\n", .{case[0]});
        const got = try helpers.collectBigints(allocator, db, case[0]);
        defer allocator.free(got);
        try std.testing.expectEqualSlices(i64, case[1], got);
    }
}

test "join: a key pinned on one input filters the other input's key" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, std.testing.io, tmp.dir, .{});
    defer db.close();

    const helpers = @import("sql_helpers.zig");
    try helpers.exec(allocator, db, "CREATE TABLE fact (tenant INT NOT NULL, model VARCHAR(8) NOT NULL, k BIGINT NOT NULL, month DATE NOT NULL, amount BIGINT NOT NULL, PRIMARY KEY (tenant, model, k, month))");
    try helpers.exec(allocator, db, "CREATE TABLE dim (tenant INT NOT NULL, model VARCHAR(8) NOT NULL, k BIGINT NOT NULL, since BIGINT NOT NULL, PRIMARY KEY (tenant, model, k))");
    var sql: std.ArrayList(u8) = .empty;
    defer sql.deinit(allocator);
    try sql.appendSlice(allocator, "INSERT INTO fact VALUES ");
    for (1..4) |tenant| for ([_][]const u8{ "x", "y" }) |model| for (1..4) |k| for ([_][]const u8{ "2026-08-01", "2026-09-01" }, 0..) |month, m| {
        if (sql.items[sql.items.len - 1] == ')') try sql.append(allocator, ',');
        try sql.print(allocator, "({d}, '{s}', {d}, '{s}', {d})", .{ tenant, model, k, month, tenant * 100 + k * 10 + m });
    };
    try helpers.exec(allocator, db, sql.items);
    sql.clearRetainingCapacity();
    try sql.appendSlice(allocator, "INSERT INTO dim VALUES ");
    for (1..4) |tenant| for ([_][]const u8{ "x", "y" }) |model| for (1..5) |k| {
        if (sql.items[sql.items.len - 1] == ')') try sql.append(allocator, ',');
        try sql.print(allocator, "({d}, '{s}', {d}, {d})", .{ tenant, model, k, 2000 + k });
    };
    try helpers.exec(allocator, db, sql.items);

    // Each case names the table whose scan should or shouldn't gain the
    // other input's constant. A preserved side, a FULL join and a null-safe
    // key never take one.
    const cases = .{
        .{
            \\WITH scoped AS (SELECT tenant, model, k, month, amount FROM fact
            \\  WHERE tenant = 2 AND model = 'x' AND month BETWEEN '2026-01-01' AND '2026-09-01' AND k <> 9),
            \\latest AS (SELECT s.*, ROW_NUMBER() OVER (PARTITION BY tenant, model, k ORDER BY month DESC) AS rn FROM scoped s),
            \\cur AS (SELECT * FROM latest WHERE rn = 1 AND month = '2026-09-01')
            \\SELECT cur.k, cur.amount, SUM(s.amount) AS total, COUNT(d.k) AS dims FROM cur
            \\LEFT JOIN scoped s ON s.tenant = cur.tenant AND s.model = cur.model AND s.k = cur.k
            \\LEFT JOIN dim d ON d.tenant = cur.tenant AND d.model = cur.model AND d.k = cur.k
            \\GROUP BY cur.k, cur.amount ORDER BY cur.k
            ,
            "\n1|211|421|2\n2|221|441|2\n3|231|461|2",
            "dim",
            true,
        },
        .{ "SELECT COUNT(*), SUM(d.since) FROM fact f JOIN dim d ON d.tenant = f.tenant AND d.k = f.k WHERE f.tenant = 3 AND f.k = 2", "\n8|16016", "dim", true },
        .{ "SELECT COUNT(*), SUM(f.amount) FROM fact f JOIN dim d ON d.tenant = f.tenant AND d.model = f.model AND d.k = f.k WHERE d.model = 'y' AND d.tenant = 1", "\n6|723", "fact", true },
        .{ "SELECT COUNT(*), SUM(g.n) FROM (SELECT tenant, k, COUNT(*) AS n FROM fact WHERE tenant = 2 GROUP BY tenant, k) g JOIN dim d ON d.tenant = g.tenant AND d.k = g.k", "\n6|24", "dim", true },
        .{ "SELECT COUNT(*), COUNT(d.k) FROM dim d RIGHT JOIN (SELECT * FROM fact WHERE tenant = 2 AND model = 'x') f ON d.tenant = f.tenant AND d.model = f.model AND d.k = f.k", "\n6|6", "dim", true },
        .{ "SELECT COUNT(*), COUNT(f.k) FROM dim d LEFT JOIN (SELECT * FROM fact WHERE tenant = 2) f ON f.tenant = d.tenant AND f.k = d.k", "\n42|24", "dim", false },
        .{ "SELECT COUNT(*), COUNT(d.k) FROM (SELECT * FROM dim WHERE tenant = 2) d RIGHT JOIN fact f ON d.tenant = f.tenant AND d.model = f.model AND d.k = f.k", "\n36|12", "fact", false },
        .{ "SELECT COUNT(*), COUNT(f.k), COUNT(d.k) FROM (SELECT * FROM fact WHERE tenant = 2) f FULL JOIN dim d ON d.tenant = f.tenant AND d.model = f.model AND d.k = f.k", "\n30|12|30", "dim", false },
        .{ "SELECT COUNT(*) FROM (SELECT * FROM fact WHERE tenant = 2) f JOIN dim d ON d.tenant <=> f.tenant AND d.model = f.model AND d.k = f.k", "\n12", "dim", false },
        .{ "SELECT COUNT(*), COUNT(f.k) FROM dim d LEFT JOIN fact f ON f.tenant = d.tenant AND f.model = d.model AND f.k = d.k WHERE f.tenant = 2", "\n12|12", "dim", true },
        .{ "SELECT COUNT(*), COUNT(d.k) FROM fact f FULL JOIN dim d ON d.tenant = f.tenant AND d.model = f.model AND d.k = f.k WHERE f.tenant = 2", "\n12|12", "dim", true },
        .{ "SELECT COUNT(*) FROM dim d LEFT JOIN fact f ON f.tenant = d.tenant AND f.model = d.model AND f.k = d.k WHERE f.k IS NULL", "\n6", "dim", false },
        .{ "SELECT COUNT(*), COUNT(f.k) FROM dim d LEFT JOIN fact f ON f.tenant = d.tenant AND f.model = d.model AND f.k = d.k WHERE f.tenant = 2 OR d.k = 4", "\n18|12", "dim", false },
    };
    inline for (cases) |c| {
        errdefer std.debug.print("case: {s}\n", .{c[0]});
        const text = try crossRowsText(allocator, db, c[0]);
        defer allocator.free(text);
        try std.testing.expectEqualStrings(c[1], text[std.mem.indexOfScalar(u8, text, '\n') orelse text.len ..]);
        try std.testing.expectEqual(c[3], try scanFiltered(allocator, db, c[0], c[2]));
    }
}

/// Whether the plan of `sql` filters its scan of `table`.
fn scanFiltered(allocator: std.mem.Allocator, db: anytype, sql: []const u8, table: []const u8) !bool {
    const helpers = @import("sql_helpers.zig");
    const explain_sql = try std.fmt.allocPrint(allocator, "EXPLAIN {s}", .{sql});
    defer allocator.free(explain_sql);
    const lines = try helpers.collectStrings(allocator, db, explain_sql);
    defer helpers.freeStrings(allocator, lines);
    const scan = try std.fmt.allocPrint(allocator, "Scan {s}", .{table});
    defer allocator.free(scan);
    for (lines, 0..) |line, i| {
        if (!std.mem.startsWith(u8, std.mem.trim(u8, line orelse continue, " "), scan)) continue;
        return i > 0 and std.mem.eql(u8, std.mem.trim(u8, lines[i - 1] orelse "", " "), "Filter");
    }
    return error.TestExpectedScan;
}

test "join: a WHERE reaches the inputs of a join under a transferred key filter" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, std.testing.io, tmp.dir, .{});
    defer db.close();

    const helpers = @import("sql_helpers.zig");
    try helpers.exec(allocator, db, "CREATE TABLE fact (t INT NOT NULL, k INT NOT NULL, d INT NOT NULL, v BIGINT NOT NULL)");
    try helpers.exec(allocator, db, "CREATE TABLE starts (t INT NOT NULL, k INT NOT NULL, sd INT NOT NULL)");
    try helpers.exec(allocator, db, "CREATE TABLE plans (t INT NOT NULL, k INT NOT NULL)");
    var sql: std.ArrayList(u8) = .empty;
    defer sql.deinit(allocator);
    try sql.appendSlice(allocator, "INSERT INTO fact VALUES ");
    for (1..4) |t| for (1..7) |k| for (1..11) |d| {
        if (sql.items[sql.items.len - 1] == ')') try sql.append(allocator, ',');
        try sql.print(allocator, "({d}, {d}, {d}, {d})", .{ t, k, d, t * 1000 + k * 10 + d });
    };
    try helpers.exec(allocator, db, sql.items);
    sql.clearRetainingCapacity();
    try sql.appendSlice(allocator, "INSERT INTO starts VALUES ");
    for (1..4) |t| for (1..5) |k| {
        if (sql.items[sql.items.len - 1] == ')') try sql.append(allocator, ',');
        try sql.print(allocator, "({d}, {d}, {d})", .{ t, k, t + k });
    };
    try helpers.exec(allocator, db, sql.items);
    try helpers.exec(allocator, db, "INSERT INTO plans VALUES (2, 1), (2, 2), (2, 3), (2, 5), (2, 5), (1, 4)");

    // `plans` pins `t`, which transfers onto the input `fact ⋈ starts` as a
    // filter that stays above that join: `t` names a column of both of its
    // inputs, as it does of `plans`, so the WHERE stays above the outer join
    // too. Its `fact` conjuncts must still reach `fact` through the filter,
    // leaving above the inner join only the conjuncts that read `starts`.
    const cases = .{
        .{ "LEFT", "f.t = 2 AND f.d BETWEEN 3 AND 7", Cohort.inRange, 0 },
        .{ "LEFT", "f.t = 2 AND f.d BETWEEN 3 AND 7 AND s.sd IS NULL", Cohort.inRangeUnstarted, 1 },
        .{ "LEFT", "f.t = 2 AND (f.d > 8 OR s.sd = 4)", Cohort.lateOrFour, 1 },
        .{ "LEFT", "f.t = 2 AND f.d BETWEEN 3 AND 7 AND f.d >= s.sd", Cohort.inRangeAfterStart, 1 },
        .{ "INNER", "f.t = 2 AND f.d = s.sd", Cohort.onStart, 0 },
    };
    inline for (cases) |c| {
        const query = std.fmt.comptimePrint(
            \\SELECT f.k, COUNT(*), SUM(f.v), SUM(s.sd) FROM fact f
            \\{s} JOIN (SELECT t, k, sd FROM starts WHERE t = 2) s ON s.t = f.t AND s.k = f.k
            \\JOIN (SELECT t, k FROM plans WHERE t = 2 GROUP BY t, k) pm ON pm.t = f.t AND pm.k = f.k
            \\WHERE {s} GROUP BY f.k ORDER BY f.k
        , .{ c[0], c[1] });
        errdefer std.debug.print("case: {s}\n", .{query});
        const expected = try Cohort.expected(allocator, std.mem.eql(u8, c[0], "INNER"), c[2]);
        defer allocator.free(expected);
        const text = try crossRowsText(allocator, db, query);
        defer allocator.free(text);
        try std.testing.expectEqualStrings(expected, text[std.mem.indexOfScalar(u8, text, '\n') orelse text.len ..]);
        try std.testing.expectEqual(@as(usize, c[3]), try filtersOverInnermostJoin(allocator, db, query));
    }
}

/// The rows of the transferred-key-filter test, evaluated directly: `fact`
/// rows of `t = 2` whose `k` has a plan, each with its `starts` row if any.
const Cohort = struct {
    fn inRange(d: i64, _: ?i64) bool {
        return d >= 3 and d <= 7;
    }
    fn inRangeUnstarted(d: i64, sd: ?i64) bool {
        return inRange(d, sd) and sd == null;
    }
    fn lateOrFour(d: i64, sd: ?i64) bool {
        return d > 8 or sd == 4;
    }
    fn inRangeAfterStart(d: i64, sd: ?i64) bool {
        return inRange(d, sd) and if (sd) |s| d >= s else false;
    }
    fn onStart(d: i64, sd: ?i64) bool {
        return sd == d;
    }

    fn expected(allocator: std.mem.Allocator, inner: bool, keep: fn (i64, ?i64) bool) ![]u8 {
        var out: std.ArrayList(u8) = .empty;
        errdefer out.deinit(allocator);
        for ([_]i64{ 1, 2, 3, 5 }) |k| {
            const sd: ?i64 = if (k <= 4) 2 + k else null;
            if (inner and sd == null) continue;
            var count: i64 = 0;
            var sum_v: i64 = 0;
            var sum_sd: ?i64 = null;
            for (1..11) |d_index| {
                const d: i64 = @intCast(d_index);
                if (!keep(d, sd)) continue;
                count += 1;
                sum_v += 2000 + k * 10 + d;
                if (sd) |s| sum_sd = (sum_sd orelse 0) + s;
            }
            if (count == 0) continue;
            try out.print(allocator, "\n{d}|{d}|{d}|", .{ k, count, sum_v });
            if (sum_sd) |s| try out.print(allocator, "{d}", .{s}) else try out.appendSlice(allocator, "NULL");
        }
        return out.toOwnedSlice(allocator);
    }
};

/// How many filters the plan of `sql` stacks right above its innermost join.
fn filtersOverInnermostJoin(allocator: std.mem.Allocator, db: anytype, sql: []const u8) !usize {
    const helpers = @import("sql_helpers.zig");
    const explain_sql = try std.fmt.allocPrint(allocator, "EXPLAIN {s}", .{sql});
    defer allocator.free(explain_sql);
    const lines = try helpers.collectStrings(allocator, db, explain_sql);
    defer helpers.freeStrings(allocator, lines);
    var innermost: ?usize = null;
    var depth: usize = 0;
    for (lines, 0..) |line, i| {
        const text = line orelse continue;
        const trimmed = std.mem.trimStart(u8, text, " ");
        if (!std.mem.startsWith(u8, trimmed, "HashJoin")) continue;
        const indent = text.len - trimmed.len;
        if (innermost == null or indent > depth) {
            innermost = i;
            depth = indent;
        }
    }
    var i = innermost orelse return error.TestExpectedJoin;
    var filters: usize = 0;
    while (i > 0 and std.mem.eql(u8, std.mem.trim(u8, lines[i - 1] orelse "", " "), "Filter")) : (i -= 1) filters += 1;
    return filters;
}

test "join: a key both inputs pin to one literal leaves the join keys" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var db = try thindb.Database.open(allocator, std.testing.io, tmp.dir, .{});
    defer db.close();

    const helpers = @import("sql_helpers.zig");
    try helpers.exec(allocator, db, "CREATE TABLE fact (tenant INT NOT NULL, model VARCHAR(8) NOT NULL, k BIGINT NOT NULL, amount BIGINT NOT NULL)");
    try helpers.exec(allocator, db, "CREATE TABLE dim (tenant INT NOT NULL, model VARCHAR(8) NOT NULL, k BIGINT NOT NULL, since BIGINT NOT NULL)");
    var sql: std.ArrayList(u8) = .empty;
    defer sql.deinit(allocator);
    try sql.appendSlice(allocator, "INSERT INTO fact VALUES ");
    for (1..4) |tenant| for ([_][]const u8{ "x", "y" }) |model| for (1..6) |k| {
        if (sql.items[sql.items.len - 1] == ')') try sql.append(allocator, ',');
        try sql.print(allocator, "({d}, '{s}', {d}, {d})", .{ tenant, model, k, tenant * 100 + k * 10 + model.len });
    };
    try helpers.exec(allocator, db, sql.items);
    sql.clearRetainingCapacity();
    try sql.appendSlice(allocator, "INSERT INTO dim VALUES ");
    for (1..4) |tenant| for ([_][]const u8{ "x", "y" }) |model| for (2..5) |k| {
        if (sql.items[sql.items.len - 1] == ')') try sql.append(allocator, ',');
        try sql.print(allocator, "({d}, '{s}', {d}, {d})", .{ tenant, model, k, tenant * 1000 + k });
    };
    try helpers.exec(allocator, db, sql.items);

    // Each case runs beside a twin whose pins hide behind an expression, so
    // no key there is provably constant: both must return the same columns
    // and rows. The count is the keys left on each join of the case,
    // outermost first.
    const cases = .{
        .{
            "SELECT f.k, f.amount, d.since FROM (SELECT * FROM fact WHERE tenant = 2 AND model = 'x') f JOIN dim d ON d.tenant = f.tenant AND d.model = f.model AND d.k = f.k ORDER BY f.k",
            "SELECT f.k, f.amount, d.since FROM (SELECT * FROM fact WHERE tenant + 0 = 2 AND CONCAT(model, '') = 'x') f JOIN dim d ON d.tenant = f.tenant AND d.model = f.model AND d.k = f.k ORDER BY f.k",
            &[_]usize{1},
        },
        .{
            "SELECT f.k, f.amount, d.since FROM (SELECT * FROM fact WHERE tenant = 3 AND model = 'y') f LEFT JOIN dim d ON d.tenant = f.tenant AND d.model = f.model AND d.k = f.k ORDER BY f.k",
            "SELECT f.k, f.amount, d.since FROM (SELECT * FROM fact WHERE tenant + 0 = 3 AND CONCAT(model, '') = 'y') f LEFT JOIN dim d ON d.tenant = f.tenant AND d.model = f.model AND d.k = f.k ORDER BY f.k",
            &[_]usize{1},
        },
        .{
            "SELECT COUNT(*), SUM(f.amount), SUM(d.since) FROM (SELECT * FROM fact WHERE tenant = 3 AND k = 2) f JOIN dim d ON d.tenant = f.tenant AND d.k = f.k",
            "SELECT COUNT(*), SUM(f.amount), SUM(d.since) FROM (SELECT * FROM fact WHERE tenant + 0 = 3 AND k + 0 = 2) f JOIN dim d ON d.tenant = f.tenant AND d.k = f.k",
            &[_]usize{1},
        },
        .{
            "SELECT COUNT(*) FROM (SELECT * FROM fact WHERE tenant = 2) f JOIN (SELECT * FROM dim WHERE tenant = 3) d ON d.tenant = f.tenant AND d.k = f.k",
            "SELECT COUNT(*) FROM (SELECT * FROM fact WHERE tenant + 0 = 2) f JOIN (SELECT * FROM dim WHERE tenant + 0 = 3) d ON d.tenant = f.tenant AND d.k = f.k",
            &[_]usize{2},
        },
        .{
            "SELECT f.k, f.amount, d.k, d.since FROM (SELECT * FROM fact WHERE tenant = 1 AND model = 'x') f FULL JOIN (SELECT * FROM dim WHERE tenant = 1 AND model = 'x') d ON d.tenant = f.tenant AND d.model = f.model AND d.k = f.k ORDER BY f.k, d.k",
            "SELECT f.k, f.amount, d.k, d.since FROM (SELECT * FROM fact WHERE tenant + 0 = 1 AND CONCAT(model, '') = 'x') f FULL JOIN (SELECT * FROM dim WHERE tenant + 0 = 1 AND CONCAT(model, '') = 'x') d ON d.tenant = f.tenant AND d.model = f.model AND d.k = f.k ORDER BY f.k, d.k",
            &[_]usize{1},
        },
        .{
            "SELECT * FROM dim d JOIN (SELECT tenant, model, k, SUM(amount) AS total FROM fact WHERE tenant = 2 AND model = 'x' GROUP BY tenant, model, k) f ON f.tenant = d.tenant AND f.model = d.model AND f.k = d.k ORDER BY d.k",
            "SELECT * FROM dim d JOIN (SELECT tenant, model, k, SUM(amount) AS total FROM fact WHERE tenant + 0 = 2 AND CONCAT(model, '') = 'x' GROUP BY tenant, model, k) f ON f.tenant = d.tenant AND f.model = d.model AND f.k = d.k ORDER BY d.k",
            &[_]usize{1},
        },
        .{
            "SELECT f.k, d.since FROM (SELECT * FROM fact WHERE tenant = 2 AND model = 'y') f JOIN (SELECT * FROM dim WHERE tenant = 2 AND model = 'y') d ON d.tenant <=> f.tenant AND d.model = f.model AND d.k = f.k ORDER BY f.k",
            "SELECT f.k, d.since FROM (SELECT * FROM fact WHERE tenant + 0 = 2 AND CONCAT(model, '') = 'y') f JOIN (SELECT * FROM dim WHERE tenant + 0 = 2 AND CONCAT(model, '') = 'y') d ON d.tenant <=> f.tenant AND d.model = f.model AND d.k = f.k ORDER BY f.k",
            &[_]usize{2},
        },
        .{
            \\WITH facts AS (SELECT tenant, model, k, amount FROM fact WHERE tenant = 2 AND model = 'x'),
            \\starts AS (SELECT tenant, model, k, since FROM dim WHERE tenant = 2 AND model = 'x'),
            \\firsts AS (SELECT s.tenant, s.model, s.k FROM facts f JOIN starts s ON s.tenant = f.tenant AND s.model = f.model AND s.k = f.k
            \\  WHERE f.amount > 220 GROUP BY s.tenant, s.model, s.k)
            \\SELECT f.k, f.amount, s.since FROM facts f LEFT JOIN starts s ON s.tenant = f.tenant AND s.model = f.model AND s.k = f.k
            \\JOIN firsts p ON p.tenant = f.tenant AND p.model = f.model AND p.k = f.k ORDER BY f.k
            ,
            \\WITH facts AS (SELECT tenant, model, k, amount FROM fact WHERE tenant + 0 = 2 AND CONCAT(model, '') = 'x'),
            \\starts AS (SELECT tenant, model, k, since FROM dim WHERE tenant + 0 = 2 AND CONCAT(model, '') = 'x'),
            \\firsts AS (SELECT s.tenant, s.model, s.k FROM facts f JOIN starts s ON s.tenant = f.tenant AND s.model = f.model AND s.k = f.k
            \\  WHERE f.amount > 220 GROUP BY s.tenant, s.model, s.k)
            \\SELECT f.k, f.amount, s.since FROM facts f LEFT JOIN starts s ON s.tenant = f.tenant AND s.model = f.model AND s.k = f.k
            \\JOIN firsts p ON p.tenant = f.tenant AND p.model = f.model AND p.k = f.k ORDER BY f.k
            ,
            &[_]usize{ 1, 1, 1 },
        },
    };
    inline for (cases) |c| {
        errdefer std.debug.print("case: {s}\n", .{c[0]});
        const got = try crossRowsText(allocator, db, c[0]);
        defer allocator.free(got);
        const want = try crossRowsText(allocator, db, c[1]);
        defer allocator.free(want);
        try std.testing.expectEqualStrings(want, got);
        const keys = try joinKeyCounts(allocator, db, c[0]);
        defer allocator.free(keys);
        try std.testing.expectEqualSlices(usize, c[2], keys);
    }
}

/// The number of key pairs on each keyed join in the plan of `sql`, in plan
/// order.
fn joinKeyCounts(allocator: std.mem.Allocator, db: anytype, sql: []const u8) ![]usize {
    const helpers = @import("sql_helpers.zig");
    const explain_sql = try std.fmt.allocPrint(allocator, "EXPLAIN {s}", .{sql});
    defer allocator.free(explain_sql);
    const lines = try helpers.collectStrings(allocator, db, explain_sql);
    defer helpers.freeStrings(allocator, lines);
    var counts: std.ArrayList(usize) = .empty;
    errdefer counts.deinit(allocator);
    for (lines) |line| {
        const text = line orelse continue;
        const start = std.mem.indexOf(u8, text, "Join on=[") orelse continue;
        const keys = text[start + "Join on=[".len ..];
        const end = std.mem.indexOfScalar(u8, keys, ']') orelse return error.TestExpectedJoinKeys;
        try counts.append(allocator, std.mem.count(u8, keys[0..end], ", ") + 1);
    }
    return counts.toOwnedSlice(allocator);
}
