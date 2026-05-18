//! Join benchmarks. Covers size combinations (small × large, balanced),
//! key types (bigint, string, uuid), single vs compound predicates, and
//! both hash + sort-merge algorithms.
//!
//! Setup (insert + flush of both tables) happens outside the timed
//! region. The measurement is the join itself — query create, drain
//! every batch, count output rows. Output rows = the matched-row total.

const std = @import("std");
const thindb = @import("thindb");
const common = @import("common.zig");

const Allocator = common.Allocator;
const Io = common.Io;
const elapsedNs = common.elapsedNs;
const freshDir = common.freshDir;

// ----------------------------------------------------------------------------
// Schemas
// ----------------------------------------------------------------------------

const bigint_left_schema = thindb.Schema{
    .columns = &.{
        .{ .name = "k", .type = .bigint },
        .{ .name = "lval", .type = .int },
    },
    .order_key = &.{"k"},
    .unique = true,
};
const bigint_right_schema = thindb.Schema{
    .columns = &.{
        .{ .name = "k", .type = .bigint },
        .{ .name = "rval", .type = .int },
    },
    .order_key = &.{"k"},
    .unique = true,
};
const bigint_ok = [_][]const u8{"k"};
const bigint_opts = thindb.TableOptions{
    .order_key = &bigint_ok,
    .unique = true,
    .row_group_size = 65_536,
};

const string_schema_left = thindb.Schema{
    .columns = &.{
        .{ .name = "k", .type = .string },
        .{ .name = "lval", .type = .int },
    },
    .order_key = &.{"k"},
    .unique = true,
};
const string_schema_right = thindb.Schema{
    .columns = &.{
        .{ .name = "k", .type = .string },
        .{ .name = "rval", .type = .int },
    },
    .order_key = &.{"k"},
    .unique = true,
};
const string_ok = [_][]const u8{"k"};
const string_opts = thindb.TableOptions{
    .order_key = &string_ok,
    .unique = true,
    .row_group_size = 65_536,
};

const uuid_schema_left = thindb.Schema{
    .columns = &.{
        .{ .name = "k", .type = .uuid },
        .{ .name = "lval", .type = .int },
    },
    .order_key = &.{"k"},
    .unique = true,
};
const uuid_schema_right = thindb.Schema{
    .columns = &.{
        .{ .name = "k", .type = .uuid },
        .{ .name = "rval", .type = .int },
    },
    .order_key = &.{"k"},
    .unique = true,
};
const uuid_ok = [_][]const u8{"k"};
const uuid_opts = thindb.TableOptions{
    .order_key = &uuid_ok,
    .unique = true,
    .row_group_size = 65_536,
};

// "Unsorted" schemas: rows are emitted in `rowid` order (the order key),
// not in `k` order. SMJ must do a real sort. Same data otherwise — both
// tables still cover k = [0..N) once each, so the inner join produces N
// output rows.
const bigint_unsorted_left_schema = thindb.Schema{
    .columns = &.{
        .{ .name = "rowid", .type = .bigint },
        .{ .name = "k", .type = .bigint },
        .{ .name = "lval", .type = .int },
    },
    .order_key = &.{"rowid"},
    .unique = true,
};
const bigint_unsorted_right_schema = thindb.Schema{
    .columns = &.{
        .{ .name = "b_rowid", .type = .bigint },
        .{ .name = "k", .type = .bigint },
        .{ .name = "rval", .type = .int },
    },
    .order_key = &.{"b_rowid"},
    .unique = true,
};
const bigint_unsorted_ok_left = [_][]const u8{"rowid"};
const bigint_unsorted_ok_right = [_][]const u8{"b_rowid"};
const bigint_unsorted_opts_left = thindb.TableOptions{
    .order_key = &bigint_unsorted_ok_left,
    .unique = true,
    .row_group_size = 65_536,
};
const bigint_unsorted_opts_right = thindb.TableOptions{
    .order_key = &bigint_unsorted_ok_right,
    .unique = true,
    .row_group_size = 65_536,
};

const string_unsorted_left_schema = thindb.Schema{
    .columns = &.{
        .{ .name = "rowid", .type = .bigint },
        .{ .name = "k", .type = .string },
        .{ .name = "lval", .type = .int },
    },
    .order_key = &.{"rowid"},
    .unique = true,
};
const string_unsorted_right_schema = thindb.Schema{
    .columns = &.{
        .{ .name = "b_rowid", .type = .bigint },
        .{ .name = "k", .type = .string },
        .{ .name = "rval", .type = .int },
    },
    .order_key = &.{"b_rowid"},
    .unique = true,
};

const uuid_unsorted_left_schema = thindb.Schema{
    .columns = &.{
        .{ .name = "rowid", .type = .bigint },
        .{ .name = "k", .type = .uuid },
        .{ .name = "lval", .type = .int },
    },
    .order_key = &.{"rowid"},
    .unique = true,
};
const uuid_unsorted_right_schema = thindb.Schema{
    .columns = &.{
        .{ .name = "b_rowid", .type = .bigint },
        .{ .name = "k", .type = .uuid },
        .{ .name = "rval", .type = .int },
    },
    .order_key = &.{"b_rowid"},
    .unique = true,
};

const compound_left_schema = thindb.Schema{
    .columns = &.{
        .{ .name = "g", .type = .bigint },
        .{ .name = "i", .type = .bigint },
        .{ .name = "lval", .type = .int },
    },
    .order_key = &.{ "g", "i" },
    .unique = true,
};
const compound_right_schema = thindb.Schema{
    .columns = &.{
        .{ .name = "g", .type = .bigint },
        .{ .name = "i", .type = .bigint },
        .{ .name = "rval", .type = .int },
    },
    .order_key = &.{ "g", "i" },
    .unique = true,
};
const compound_ok = [_][]const u8{ "g", "i" };
const compound_opts = thindb.TableOptions{
    .order_key = &compound_ok,
    .unique = true,
    .row_group_size = 65_536,
};

// ----------------------------------------------------------------------------
// Reporting
// ----------------------------------------------------------------------------

fn reportJoin(
    label: []const u8,
    algo: thindb.exec.join_op.Algorithm,
    left_rows: usize,
    right_rows: usize,
    output_rows: usize,
    elapsed_ns: u64,
) void {
    const seconds = @as(f64, @floatFromInt(elapsed_ns)) / 1e9;
    const ms = seconds * 1000.0;
    const mout_per_sec = if (seconds > 0)
        (@as(f64, @floatFromInt(output_rows)) / seconds) / 1e6
    else
        0.0;
    const algo_name = switch (algo) {
        .auto => "auto",
        .hash => "hash",
        .sort_merge => "smj ",
        .nested_loop => "nlj ",
        .range_sweep => "sweep",
    };
    std.debug.print(
        "  {s:<28} [{s}] L={d:>7} R={d:>7} out={d:>7}  {d:>8.2} ms  {d:>7.2} M out/s\n",
        .{ label, algo_name, left_rows, right_rows, output_rows, ms, mout_per_sec },
    );
}

// ----------------------------------------------------------------------------
// Bigint key
// ----------------------------------------------------------------------------

fn fillBigintLeft(t: *thindb.Table, n: usize, allocator: Allocator) !void {
    // Insert in chunks so we don't allocate a giant single anytype literal.
    const Row = struct { k: i64, lval: i32 };
    const rows = try allocator.alloc(Row, n);
    defer allocator.free(rows);
    for (rows, 0..) |*r, i| r.* = .{ .k = @intCast(i), .lval = @intCast(i % 1000) };
    try t.insert(rows);
}
fn fillBigintRight(t: *thindb.Table, n: usize, allocator: Allocator) !void {
    const Row = struct { k: i64, rval: i32 };
    const rows = try allocator.alloc(Row, n);
    defer allocator.free(rows);
    for (rows, 0..) |*r, i| r.* = .{ .k = @intCast(i), .rval = @intCast(i % 1000) };
    try t.insert(rows);
}

fn benchBigintJoin(
    allocator: Allocator,
    io: Io,
    label: []const u8,
    sub_path: []const u8,
    left_rows: usize,
    right_rows: usize,
    algo: thindb.exec.join_op.Algorithm,
) !void {
    var dir = try freshDir(io, sub_path);
    defer dir.close(io);
    var db = try thindb.Database.open(allocator, io, dir, .{
        .auto_flush_rows = std.math.maxInt(u64),
        .auto_flush_bytes = std.math.maxInt(usize),
        .auto_flush_secs = 0,
    });
    defer db.close();

    const l = try db.table("l", bigint_left_schema, bigint_opts);
    try fillBigintLeft(l, left_rows, allocator);
    try l.flush();

    const r = try db.table("r", bigint_right_schema, bigint_opts);
    try fillBigintRight(r, right_rows, allocator);
    try r.flush();

    const left = try thindb.scan(allocator, l);
    const right = try thindb.scan(allocator, r);
    const t0 = Io.Clock.awake.now(io);
    var q = try left.join(right, .{
        .on = &.{.{ .left = "k", .right = "k" }},
        .algorithm = algo,
    });
    defer q.deinit();
    var output: usize = 0;
    while (try q.next()) |b| output += b.row_count;
    const elapsed = elapsedNs(io, t0);

    reportJoin(label, algo, left_rows, right_rows, output, elapsed);
}

// ----------------------------------------------------------------------------
// String key — 16-char zero-padded sequential keys (lex order matches numeric)
// ----------------------------------------------------------------------------

fn buildStringKeys(allocator: Allocator, n: usize) ![][16]u8 {
    const keys = try allocator.alloc([16]u8, n);
    for (keys, 0..) |*k, i| {
        _ = std.fmt.bufPrint(k[0..], "k_{d:0>13}", .{i}) catch unreachable;
    }
    return keys;
}

fn fillStringLeft(t: *thindb.Table, keys: [][16]u8, allocator: Allocator) !void {
    const Row = struct { k: []const u8, lval: i32 };
    const rows = try allocator.alloc(Row, keys.len);
    defer allocator.free(rows);
    for (rows, keys, 0..) |*r, *k, i| r.* = .{ .k = k[0..], .lval = @intCast(i % 1000) };
    try t.insert(rows);
}
fn fillStringRight(t: *thindb.Table, keys: [][16]u8, allocator: Allocator) !void {
    const Row = struct { k: []const u8, rval: i32 };
    const rows = try allocator.alloc(Row, keys.len);
    defer allocator.free(rows);
    for (rows, keys, 0..) |*r, *k, i| r.* = .{ .k = k[0..], .rval = @intCast(i % 1000) };
    try t.insert(rows);
}

fn benchStringJoin(
    allocator: Allocator,
    io: Io,
    label: []const u8,
    n: usize,
    algo: thindb.exec.join_op.Algorithm,
) !void {
    var dir = try freshDir(io, ".bench-data/join_string");
    defer dir.close(io);
    var db = try thindb.Database.open(allocator, io, dir, .{
        .auto_flush_rows = std.math.maxInt(u64),
        .auto_flush_bytes = std.math.maxInt(usize),
        .auto_flush_secs = 0,
    });
    defer db.close();

    const keys = try buildStringKeys(allocator, n);
    defer allocator.free(keys);

    const l = try db.table("l", string_schema_left, string_opts);
    try fillStringLeft(l, keys, allocator);
    try l.flush();

    const r = try db.table("r", string_schema_right, string_opts);
    try fillStringRight(r, keys, allocator);
    try r.flush();

    const left = try thindb.scan(allocator, l);
    const right = try thindb.scan(allocator, r);
    const t0 = Io.Clock.awake.now(io);
    var q = try left.join(right, .{
        .on = &.{.{ .left = "k", .right = "k" }},
        .algorithm = algo,
    });
    defer q.deinit();
    var output: usize = 0;
    while (try q.next()) |b| output += b.row_count;
    const elapsed = elapsedNs(io, t0);

    reportJoin(label, algo, n, n, output, elapsed);
}

// ----------------------------------------------------------------------------
// UUID key — sequential u128 with random-looking high bits
// ----------------------------------------------------------------------------

fn fillUuidLeft(t: *thindb.Table, n: usize, allocator: Allocator) !void {
    const Row = struct { k: u128, lval: i32 };
    const rows = try allocator.alloc(Row, n);
    defer allocator.free(rows);
    for (rows, 0..) |*r, i| {
        // High 64 bits: arbitrary pattern; low 64 bits: index. Together
        // unique and sortable across both tables.
        const hi: u128 = @as(u128, 0x1234567890ABCDEF) << 64;
        r.* = .{ .k = hi | @as(u128, i), .lval = @intCast(i % 1000) };
    }
    try t.insert(rows);
}
fn fillUuidRight(t: *thindb.Table, n: usize, allocator: Allocator) !void {
    const Row = struct { k: u128, rval: i32 };
    const rows = try allocator.alloc(Row, n);
    defer allocator.free(rows);
    for (rows, 0..) |*r, i| {
        const hi: u128 = @as(u128, 0x1234567890ABCDEF) << 64;
        r.* = .{ .k = hi | @as(u128, i), .rval = @intCast(i % 1000) };
    }
    try t.insert(rows);
}

fn benchUuidJoin(
    allocator: Allocator,
    io: Io,
    label: []const u8,
    n: usize,
    algo: thindb.exec.join_op.Algorithm,
) !void {
    var dir = try freshDir(io, ".bench-data/join_uuid");
    defer dir.close(io);
    var db = try thindb.Database.open(allocator, io, dir, .{
        .auto_flush_rows = std.math.maxInt(u64),
        .auto_flush_bytes = std.math.maxInt(usize),
        .auto_flush_secs = 0,
    });
    defer db.close();

    const l = try db.table("l", uuid_schema_left, uuid_opts);
    try fillUuidLeft(l, n, allocator);
    try l.flush();

    const r = try db.table("r", uuid_schema_right, uuid_opts);
    try fillUuidRight(r, n, allocator);
    try r.flush();

    const left = try thindb.scan(allocator, l);
    const right = try thindb.scan(allocator, r);
    const t0 = Io.Clock.awake.now(io);
    var q = try left.join(right, .{
        .on = &.{.{ .left = "k", .right = "k" }},
        .algorithm = algo,
    });
    defer q.deinit();
    var output: usize = 0;
    while (try q.next()) |b| output += b.row_count;
    const elapsed = elapsedNs(io, t0);

    reportJoin(label, algo, n, n, output, elapsed);
}

// ----------------------------------------------------------------------------
// Compound (bigint, bigint) key — exercises multi-column key path
// ----------------------------------------------------------------------------

fn fillCompoundLeft(t: *thindb.Table, n: usize, allocator: Allocator) !void {
    // group_size split: ~32 groups, n / 32 items each. Sequential so the
    // single-segment scan is globally sorted on (g, i).
    const groups: usize = 32;
    const Row = struct { g: i64, i: i64, lval: i32 };
    const rows = try allocator.alloc(Row, n);
    defer allocator.free(rows);
    for (rows, 0..) |*r, idx| {
        r.* = .{
            .g = @intCast(idx / (n / groups + 1)),
            .i = @intCast(idx),
            .lval = @intCast(idx % 1000),
        };
    }
    try t.insert(rows);
}
fn fillCompoundRight(t: *thindb.Table, n: usize, allocator: Allocator) !void {
    const groups: usize = 32;
    const Row = struct { g: i64, i: i64, rval: i32 };
    const rows = try allocator.alloc(Row, n);
    defer allocator.free(rows);
    for (rows, 0..) |*r, idx| {
        r.* = .{
            .g = @intCast(idx / (n / groups + 1)),
            .i = @intCast(idx),
            .rval = @intCast(idx % 1000),
        };
    }
    try t.insert(rows);
}

fn benchCompoundJoin(
    allocator: Allocator,
    io: Io,
    label: []const u8,
    n: usize,
    algo: thindb.exec.join_op.Algorithm,
) !void {
    var dir = try freshDir(io, ".bench-data/join_compound");
    defer dir.close(io);
    var db = try thindb.Database.open(allocator, io, dir, .{
        .auto_flush_rows = std.math.maxInt(u64),
        .auto_flush_bytes = std.math.maxInt(usize),
        .auto_flush_secs = 0,
    });
    defer db.close();

    const l = try db.table("l", compound_left_schema, compound_opts);
    try fillCompoundLeft(l, n, allocator);
    try l.flush();

    const r = try db.table("r", compound_right_schema, compound_opts);
    try fillCompoundRight(r, n, allocator);
    try r.flush();

    const left = try thindb.scan(allocator, l);
    const right = try thindb.scan(allocator, r);
    const t0 = Io.Clock.awake.now(io);
    var q = try left.join(right, .{
        .on = &.{
            .{ .left = "g", .right = "g" },
            .{ .left = "i", .right = "i" },
        },
        .algorithm = algo,
    });
    defer q.deinit();
    var output: usize = 0;
    while (try q.next()) |b| output += b.row_count;
    const elapsed = elapsedNs(io, t0);

    reportJoin(label, algo, n, n, output, elapsed);
}

// ----------------------------------------------------------------------------
// Unsorted variants: same data sets, but rows scan in rowid order so SMJ
// has to pay the real sort cost (no pdqsort fast-path on already-sorted
// input).
// ----------------------------------------------------------------------------

/// Full-period LCG permutation: `(21*i + 1) mod n`. Coprime to 21 and 1
/// when n is divisible by 100 (Hull-Dobell). Used to scramble k values
/// across rowid values so scan-order is not k-sorted.
inline fn permute(i: usize, n: usize) usize {
    return (21 * i + 1) % n;
}

fn fillBigintUnsortedLeft(t: *thindb.Table, n: usize, allocator: Allocator) !void {
    const Row = struct { rowid: i64, k: i64, lval: i32 };
    const rows = try allocator.alloc(Row, n);
    defer allocator.free(rows);
    for (rows, 0..) |*r, i| {
        r.* = .{ .rowid = @intCast(i), .k = @intCast(permute(i, n)), .lval = @intCast(i % 1000) };
    }
    try t.insert(rows);
}
fn fillBigintUnsortedRight(t: *thindb.Table, n: usize, allocator: Allocator) !void {
    const Row = struct { b_rowid: i64, k: i64, rval: i32 };
    const rows = try allocator.alloc(Row, n);
    defer allocator.free(rows);
    for (rows, 0..) |*r, i| {
        r.* = .{ .b_rowid = @intCast(i), .k = @intCast(permute(i, n)), .rval = @intCast(i % 1000) };
    }
    try t.insert(rows);
}

fn benchBigintJoinUnsorted(
    allocator: Allocator,
    io: Io,
    label: []const u8,
    n: usize,
    algo: thindb.exec.join_op.Algorithm,
) !void {
    var dir = try freshDir(io, ".bench-data/join_bigint_unsorted");
    defer dir.close(io);
    var db = try thindb.Database.open(allocator, io, dir, .{
        .auto_flush_rows = std.math.maxInt(u64),
        .auto_flush_bytes = std.math.maxInt(usize),
        .auto_flush_secs = 0,
    });
    defer db.close();

    const l = try db.table("l", bigint_unsorted_left_schema, bigint_unsorted_opts_left);
    try fillBigintUnsortedLeft(l, n, allocator);
    try l.flush();

    const r = try db.table("r", bigint_unsorted_right_schema, bigint_unsorted_opts_right);
    try fillBigintUnsortedRight(r, n, allocator);
    try r.flush();

    const left = try thindb.scan(allocator, l);
    const right = try thindb.scan(allocator, r);
    const t0 = Io.Clock.awake.now(io);
    var q = try left.join(right, .{
        .on = &.{.{ .left = "k", .right = "k" }},
        .algorithm = algo,
    });
    defer q.deinit();
    var output: usize = 0;
    while (try q.next()) |b| output += b.row_count;
    const elapsed = elapsedNs(io, t0);

    reportJoin(label, algo, n, n, output, elapsed);
}

fn fillStringUnsortedLeft(t: *thindb.Table, keys: [][16]u8, allocator: Allocator) !void {
    const Row = struct { rowid: i64, k: []const u8, lval: i32 };
    const n = keys.len;
    const rows = try allocator.alloc(Row, n);
    defer allocator.free(rows);
    for (rows, 0..) |*r, i| {
        r.* = .{
            .rowid = @intCast(i),
            .k = keys[permute(i, n)][0..],
            .lval = @intCast(i % 1000),
        };
    }
    try t.insert(rows);
}
fn fillStringUnsortedRight(t: *thindb.Table, keys: [][16]u8, allocator: Allocator) !void {
    const Row = struct { b_rowid: i64, k: []const u8, rval: i32 };
    const n = keys.len;
    const rows = try allocator.alloc(Row, n);
    defer allocator.free(rows);
    for (rows, 0..) |*r, i| {
        r.* = .{
            .b_rowid = @intCast(i),
            .k = keys[permute(i, n)][0..],
            .rval = @intCast(i % 1000),
        };
    }
    try t.insert(rows);
}

fn benchStringJoinUnsorted(
    allocator: Allocator,
    io: Io,
    label: []const u8,
    n: usize,
    algo: thindb.exec.join_op.Algorithm,
) !void {
    var dir = try freshDir(io, ".bench-data/join_string_unsorted");
    defer dir.close(io);
    var db = try thindb.Database.open(allocator, io, dir, .{
        .auto_flush_rows = std.math.maxInt(u64),
        .auto_flush_bytes = std.math.maxInt(usize),
        .auto_flush_secs = 0,
    });
    defer db.close();

    const keys = try buildStringKeys(allocator, n);
    defer allocator.free(keys);

    const l = try db.table("l", string_unsorted_left_schema, bigint_unsorted_opts_left);
    try fillStringUnsortedLeft(l, keys, allocator);
    try l.flush();

    const r = try db.table("r", string_unsorted_right_schema, bigint_unsorted_opts_right);
    try fillStringUnsortedRight(r, keys, allocator);
    try r.flush();

    const left = try thindb.scan(allocator, l);
    const right = try thindb.scan(allocator, r);
    const t0 = Io.Clock.awake.now(io);
    var q = try left.join(right, .{
        .on = &.{.{ .left = "k", .right = "k" }},
        .algorithm = algo,
    });
    defer q.deinit();
    var output: usize = 0;
    while (try q.next()) |b| output += b.row_count;
    const elapsed = elapsedNs(io, t0);

    reportJoin(label, algo, n, n, output, elapsed);
}

fn fillUuidUnsortedLeft(t: *thindb.Table, n: usize, allocator: Allocator) !void {
    const Row = struct { rowid: i64, k: u128, lval: i32 };
    const rows = try allocator.alloc(Row, n);
    defer allocator.free(rows);
    const hi: u128 = @as(u128, 0x1234567890ABCDEF) << 64;
    for (rows, 0..) |*r, i| {
        r.* = .{ .rowid = @intCast(i), .k = hi | @as(u128, permute(i, n)), .lval = @intCast(i % 1000) };
    }
    try t.insert(rows);
}
fn fillUuidUnsortedRight(t: *thindb.Table, n: usize, allocator: Allocator) !void {
    const Row = struct { b_rowid: i64, k: u128, rval: i32 };
    const rows = try allocator.alloc(Row, n);
    defer allocator.free(rows);
    const hi: u128 = @as(u128, 0x1234567890ABCDEF) << 64;
    for (rows, 0..) |*r, i| {
        r.* = .{ .b_rowid = @intCast(i), .k = hi | @as(u128, permute(i, n)), .rval = @intCast(i % 1000) };
    }
    try t.insert(rows);
}

fn benchUuidJoinUnsorted(
    allocator: Allocator,
    io: Io,
    label: []const u8,
    n: usize,
    algo: thindb.exec.join_op.Algorithm,
) !void {
    var dir = try freshDir(io, ".bench-data/join_uuid_unsorted");
    defer dir.close(io);
    var db = try thindb.Database.open(allocator, io, dir, .{
        .auto_flush_rows = std.math.maxInt(u64),
        .auto_flush_bytes = std.math.maxInt(usize),
        .auto_flush_secs = 0,
    });
    defer db.close();

    const l = try db.table("l", uuid_unsorted_left_schema, bigint_unsorted_opts_left);
    try fillUuidUnsortedLeft(l, n, allocator);
    try l.flush();

    const r = try db.table("r", uuid_unsorted_right_schema, bigint_unsorted_opts_right);
    try fillUuidUnsortedRight(r, n, allocator);
    try r.flush();

    const left = try thindb.scan(allocator, l);
    const right = try thindb.scan(allocator, r);
    const t0 = Io.Clock.awake.now(io);
    var q = try left.join(right, .{
        .on = &.{.{ .left = "k", .right = "k" }},
        .algorithm = algo,
    });
    defer q.deinit();
    var output: usize = 0;
    while (try q.next()) |b| output += b.row_count;
    const elapsed = elapsedNs(io, t0);

    reportJoin(label, algo, n, n, output, elapsed);
}

// ----------------------------------------------------------------------------
// Entry point
// ----------------------------------------------------------------------------

pub fn runAll(allocator: Allocator, io: Io) !void {
    std.debug.print("\nJoin (bigint key)\n", .{});
    std.debug.print("--------------------------------------------------------------------------------\n", .{});

    const sizes = .{
        .{ .label = "1k x 1k", .l = @as(usize, 1_000), .r = @as(usize, 1_000) },
        .{ .label = "1k x 1M (dim x fact)", .l = @as(usize, 1_000), .r = @as(usize, 1_000_000) },
        .{ .label = "100k x 100k", .l = @as(usize, 100_000), .r = @as(usize, 100_000) },
        .{ .label = "1M x 1M", .l = @as(usize, 1_000_000), .r = @as(usize, 1_000_000) },
    };

    inline for (sizes) |sz| {
        try benchBigintJoin(allocator, io, sz.label, ".bench-data/join_bigint", sz.l, sz.r, .hash);
        try benchBigintJoin(allocator, io, sz.label, ".bench-data/join_bigint", sz.l, sz.r, .sort_merge);
    }

    std.debug.print("\nJoin (string key, 100k x 100k)\n", .{});
    std.debug.print("--------------------------------------------------------------------------------\n", .{});
    try benchStringJoin(allocator, io, "string 100k", 100_000, .hash);
    try benchStringJoin(allocator, io, "string 100k", 100_000, .sort_merge);

    std.debug.print("\nJoin (uuid key, 100k x 100k)\n", .{});
    std.debug.print("--------------------------------------------------------------------------------\n", .{});
    try benchUuidJoin(allocator, io, "uuid 100k", 100_000, .hash);
    try benchUuidJoin(allocator, io, "uuid 100k", 100_000, .sort_merge);

    std.debug.print("\nJoin (compound bigint+bigint key, 100k x 100k)\n", .{});
    std.debug.print("--------------------------------------------------------------------------------\n", .{});
    try benchCompoundJoin(allocator, io, "compound 100k", 100_000, .hash);
    try benchCompoundJoin(allocator, io, "compound 100k", 100_000, .sort_merge);

    // Unsorted variants: scan emits in rowid order, NOT join-key order,
    // so SMJ pays the real sort cost (pdqsort's pre-sorted fast path
    // doesn't apply). Side-by-side with the sorted SMJ numbers above
    // shows the headroom a future merge-only fast-path would unlock.
    std.debug.print("\nJoin — unsorted input (order_key != join_key)\n", .{});
    std.debug.print("--------------------------------------------------------------------------------\n", .{});
    try benchBigintJoinUnsorted(allocator, io, "bigint 100k unsorted", 100_000, .hash);
    try benchBigintJoinUnsorted(allocator, io, "bigint 100k unsorted", 100_000, .sort_merge);
    try benchBigintJoinUnsorted(allocator, io, "bigint 1M unsorted", 1_000_000, .hash);
    try benchBigintJoinUnsorted(allocator, io, "bigint 1M unsorted", 1_000_000, .sort_merge);
    try benchStringJoinUnsorted(allocator, io, "string 100k unsorted", 100_000, .hash);
    try benchStringJoinUnsorted(allocator, io, "string 100k unsorted", 100_000, .sort_merge);
    try benchUuidJoinUnsorted(allocator, io, "uuid 100k unsorted", 100_000, .hash);
    try benchUuidJoinUnsorted(allocator, io, "uuid 100k unsorted", 100_000, .sort_merge);

    // Range / mixed-predicate joins.
    std.debug.print("\nJoin — range + mixed predicates\n", .{});
    std.debug.print("--------------------------------------------------------------------------------\n", .{});
    try benchEquiPlusRange(allocator, io, 100_000, .hash);
    try benchEquiPlusRange(allocator, io, 100_000, .sort_merge);
    try benchEquiPlusBetween(allocator, io, 100_000, .hash);
    try benchEquiPlusBetween(allocator, io, 100_000, .sort_merge);
    try benchPureRangeNlj(allocator, io, 1_000, 1_000);
    try benchPureRangeNljExplicit(allocator, io, 1_000, 1_000);
    try benchPureRangeNlj(allocator, io, 5_000, 5_000);
    try benchPureRangeNljExplicit(allocator, io, 5_000, 5_000);
    try benchPureRangeNlj(allocator, io, 50_000, 50_000);
    try benchLeftOuterRange(allocator, io, 100_000, .hash);
    try benchLeftOuterRange(allocator, io, 100_000, .sort_merge);

    // Skew detection + opaque-predicate paths.
    std.debug.print("\nJoin — skew + opaque predicate\n", .{});
    std.debug.print("--------------------------------------------------------------------------------\n", .{});
    try benchSkewDetectOverhead(allocator, io, 100_000);
    try benchOpaquePredicateNlj(allocator, io, 1_000);
    try benchOpaquePredicateNlj(allocator, io, 5_000);
}

// ----------------------------------------------------------------------------
// Skew detection / opaque-predicate benchmarks
// ----------------------------------------------------------------------------

/// Measures the per-build-row overhead of running Misra-Gries on a
/// uniform-key dataset (no actual skew). Compares vs the same join
/// with detection disabled.
fn benchSkewDetectOverhead(allocator: Allocator, io: Io, n: usize) !void {
    inline for ([_]struct { label: []const u8, threshold: f32 }{
        .{ .label = "hash equi (no detection)", .threshold = 0.0 },
        .{ .label = "hash equi (skew_threshold=0.9)", .threshold = 0.9 },
    }) |variant| {
        var dir = try freshDir(io, ".bench-data/join_skew_overhead");
        defer dir.close(io);
        var db = try thindb.Database.open(allocator, io, dir, .{
            .auto_flush_rows = std.math.maxInt(u64),
            .auto_flush_bytes = std.math.maxInt(usize),
            .auto_flush_secs = 0,
        });
        defer db.close();

        const l = try db.table("l", bigint_left_schema, bigint_opts);
        try fillBigintLeft(l, n, allocator);
        try l.flush();
        const r = try db.table("r", bigint_right_schema, bigint_opts);
        try fillBigintRight(r, n, allocator);
        try r.flush();

        const left = try thindb.scan(allocator, l);
        const right = try thindb.scan(allocator, r);
        const t0 = Io.Clock.awake.now(io);
        var q = try left.join(right, .{
            .on = &.{.{ .left = "k", .right = "k" }},
            .algorithm = .hash,
            .skew_threshold = variant.threshold,
        });
        defer q.deinit();
        var output: usize = 0;
        while (try q.next()) |b| output += b.row_count;
        const elapsed = elapsedNs(io, t0);
        reportJoin(variant.label, .hash, n, n, output, elapsed);
    }
}

/// Opaque-predicate NLJ: same shape as pure-range NLJ but routed
/// via the callback path. Should be roughly 1.5-2x slower than the
/// hard-coded range NLJ because the predicate call adds per-pair
/// indirection.
fn benchOpaquePredicateNlj(allocator: Allocator, io: Io, n: usize) !void {
    var dir = try freshDir(io, ".bench-data/join_opaque_nlj");
    defer dir.close(io);
    var db = try thindb.Database.open(allocator, io, dir, .{
        .auto_flush_rows = std.math.maxInt(u64),
        .auto_flush_bytes = std.math.maxInt(usize),
        .auto_flush_secs = 0,
    });
    defer db.close();

    const l = try db.table("l", pure_l_schema, .{ .order_key = &pure_l_ok, .unique = true });
    try fillPureLeft(l, n, allocator);
    try l.flush();
    const r = try db.table("r", pure_r_schema, .{ .order_key = &pure_r_ok, .unique = true });
    try fillPureRight(r, n, allocator);
    try r.flush();

    // Same predicate as the test: a.x < b.y, but via callback.
    const Pred = struct {
        fn eval(
            ctx: ?*anyopaque,
            left: []const thindb.storage.ColumnView,
            lrow: u32,
            right: []const thindb.storage.ColumnView,
            rrow: u32,
        ) bool {
            _ = ctx;
            // pure_l_schema: l_rowid(0), x(1)
            // pure_r_schema: r_rowid(0), y(1)
            return left[1].data.bigint[lrow] < right[1].data.bigint[rrow];
        }
    };

    const left = try thindb.scan(allocator, l);
    const right = try thindb.scan(allocator, r);
    const t0 = Io.Clock.awake.now(io);
    var q = try left.join(right, .{
        .on = &.{},
        .opaque_predicate = .{ .eval = Pred.eval },
    });
    defer q.deinit();
    var output: usize = 0;
    while (try q.next()) |b| output += b.row_count;
    const elapsed = elapsedNs(io, t0);
    reportJoin("opaque NLJ (a.x<b.y via callback)", .nested_loop, n, n, output, elapsed);
}

// ----------------------------------------------------------------------------
// Range / mixed-predicate benchmarks
// ----------------------------------------------------------------------------

const range_l_schema = thindb.Schema{
    .columns = &.{
        .{ .name = "k", .type = .bigint },
        .{ .name = "x", .type = .bigint },
    },
    .order_key = &.{"k"},
    .unique = true,
};
const range_r_schema = thindb.Schema{
    .columns = &.{
        .{ .name = "k", .type = .bigint },
        .{ .name = "y", .type = .bigint },
    },
    .order_key = &.{"k"},
    .unique = true,
};
const between_r_schema = thindb.Schema{
    .columns = &.{
        .{ .name = "k", .type = .bigint },
        .{ .name = "lo", .type = .bigint },
        .{ .name = "hi", .type = .bigint },
    },
    .order_key = &.{"k"},
    .unique = true,
};
const range_l_ok = [_][]const u8{"k"};
const range_r_ok = [_][]const u8{"k"};
const range_opts = thindb.TableOptions{
    .order_key = &range_l_ok,
    .unique = true,
    .row_group_size = 65_536,
};
const between_r_opts = thindb.TableOptions{
    .order_key = &range_r_ok,
    .unique = true,
    .row_group_size = 65_536,
};

fn fillRangeLeft(t: *thindb.Table, n: usize, allocator: Allocator) !void {
    const Row = struct { k: i64, x: i64 };
    const rows = try allocator.alloc(Row, n);
    defer allocator.free(rows);
    for (rows, 0..) |*r, i| r.* = .{ .k = @intCast(i), .x = @intCast(i * 3) };
    try t.insert(rows);
}
fn fillRangeRight(t: *thindb.Table, n: usize, allocator: Allocator) !void {
    const Row = struct { k: i64, y: i64 };
    const rows = try allocator.alloc(Row, n);
    defer allocator.free(rows);
    // y = x * 2 — about half the pairs pass `x < y` (since y = 2*k, x = 3*k; 3k < 2k is FALSE for k>0).
    // Adjust: y = x * 4 so most pass.
    for (rows, 0..) |*r, i| r.* = .{ .k = @intCast(i), .y = @intCast(i * 4) };
    try t.insert(rows);
}

fn benchEquiPlusRange(allocator: Allocator, io: Io, n: usize, algo: thindb.exec.join_op.Algorithm) !void {
    var dir = try freshDir(io, ".bench-data/join_equi_range");
    defer dir.close(io);
    var db = try thindb.Database.open(allocator, io, dir, .{
        .auto_flush_rows = std.math.maxInt(u64),
        .auto_flush_bytes = std.math.maxInt(usize),
        .auto_flush_secs = 0,
    });
    defer db.close();

    const l = try db.table("l", range_l_schema, range_opts);
    try fillRangeLeft(l, n, allocator);
    try l.flush();
    const r = try db.table("r", range_r_schema, range_opts);
    try fillRangeRight(r, n, allocator);
    try r.flush();

    const left = try thindb.scan(allocator, l);
    const right = try thindb.scan(allocator, r);
    const t0 = Io.Clock.awake.now(io);
    var q = try left.join(right, .{
        .on = &.{.{ .left = "k", .right = "k" }},
        .ranges = &.{.{ .left = "x", .op = .lt, .right = "y" }},
        .algorithm = algo,
    });
    defer q.deinit();
    var output: usize = 0;
    while (try q.next()) |b| output += b.row_count;
    const elapsed = elapsedNs(io, t0);
    reportJoin("equi + 1 range", algo, n, n, output, elapsed);
}

fn fillBetweenLeft(t: *thindb.Table, n: usize, allocator: Allocator) !void {
    const Row = struct { k: i64, x: i64 };
    const rows = try allocator.alloc(Row, n);
    defer allocator.free(rows);
    for (rows, 0..) |*r, i| r.* = .{ .k = @intCast(i), .x = @intCast(i % 100) };
    try t.insert(rows);
}
fn fillBetweenRight(t: *thindb.Table, n: usize, allocator: Allocator) !void {
    const Row = struct { k: i64, lo: i64, hi: i64 };
    const rows = try allocator.alloc(Row, n);
    defer allocator.free(rows);
    for (rows, 0..) |*r, i| r.* = .{
        .k = @intCast(i),
        .lo = @intCast(i % 50),
        .hi = @intCast(i % 50 + 80),
    };
    try t.insert(rows);
}

fn benchEquiPlusBetween(allocator: Allocator, io: Io, n: usize, algo: thindb.exec.join_op.Algorithm) !void {
    var dir = try freshDir(io, ".bench-data/join_equi_between");
    defer dir.close(io);
    var db = try thindb.Database.open(allocator, io, dir, .{
        .auto_flush_rows = std.math.maxInt(u64),
        .auto_flush_bytes = std.math.maxInt(usize),
        .auto_flush_secs = 0,
    });
    defer db.close();

    const l = try db.table("l", range_l_schema, range_opts);
    try fillBetweenLeft(l, n, allocator);
    try l.flush();
    const r = try db.table("r", between_r_schema, between_r_opts);
    try fillBetweenRight(r, n, allocator);
    try r.flush();

    const left = try thindb.scan(allocator, l);
    const right = try thindb.scan(allocator, r);
    const t0 = Io.Clock.awake.now(io);
    var q = try left.join(right, .{
        .on = &.{.{ .left = "k", .right = "k" }},
        .ranges = &.{
            .{ .left = "x", .op = .gte, .right = "lo" },
            .{ .left = "x", .op = .lt, .right = "hi" },
        },
        .algorithm = algo,
    });
    defer q.deinit();
    var output: usize = 0;
    while (try q.next()) |b| output += b.row_count;
    const elapsed = elapsedNs(io, t0);
    reportJoin("equi + BETWEEN (2 ranges)", algo, n, n, output, elapsed);
}

// Distinct schemas for pure-range NLJ — no shared `k` column to drop
// since there's no equi `on`.
const pure_l_schema = thindb.Schema{
    .columns = &.{
        .{ .name = "l_rowid", .type = .bigint },
        .{ .name = "x", .type = .bigint },
    },
    .order_key = &.{"l_rowid"},
    .unique = true,
};
const pure_r_schema = thindb.Schema{
    .columns = &.{
        .{ .name = "r_rowid", .type = .bigint },
        .{ .name = "y", .type = .bigint },
    },
    .order_key = &.{"r_rowid"},
    .unique = true,
};
const pure_l_ok = [_][]const u8{"l_rowid"};
const pure_r_ok = [_][]const u8{"r_rowid"};

fn fillPureLeft(t: *thindb.Table, n: usize, allocator: Allocator) !void {
    const Row = struct { l_rowid: i64, x: i64 };
    const rows = try allocator.alloc(Row, n);
    defer allocator.free(rows);
    for (rows, 0..) |*r, i| r.* = .{ .l_rowid = @intCast(i), .x = @intCast(i * 3) };
    try t.insert(rows);
}
fn fillPureRight(t: *thindb.Table, n: usize, allocator: Allocator) !void {
    const Row = struct { r_rowid: i64, y: i64 };
    const rows = try allocator.alloc(Row, n);
    defer allocator.free(rows);
    for (rows, 0..) |*r, i| r.* = .{ .r_rowid = @intCast(i), .y = @intCast(i * 4) };
    try t.insert(rows);
}

fn benchPureRangeNlj(allocator: Allocator, io: Io, l_rows: usize, r_rows: usize) !void {
    var dir = try freshDir(io, ".bench-data/join_pure_range");
    defer dir.close(io);
    var db = try thindb.Database.open(allocator, io, dir, .{
        .auto_flush_rows = std.math.maxInt(u64),
        .auto_flush_bytes = std.math.maxInt(usize),
        .auto_flush_secs = 0,
    });
    defer db.close();

    const l = try db.table("l", pure_l_schema, .{ .order_key = &pure_l_ok, .unique = true });
    try fillPureLeft(l, l_rows, allocator);
    try l.flush();
    const r = try db.table("r", pure_r_schema, .{ .order_key = &pure_r_ok, .unique = true });
    try fillPureRight(r, r_rows, allocator);
    try r.flush();

    const left = try thindb.scan(allocator, l);
    const right = try thindb.scan(allocator, r);
    const t0 = Io.Clock.awake.now(io);
    var q = try left.join(right, .{
        .on = &.{}, // pure range → range_sweep via .auto
        .ranges = &.{.{ .left = "x", .op = .lt, .right = "y" }},
    });
    defer q.deinit();
    var output: usize = 0;
    while (try q.next()) |b| output += b.row_count;
    const elapsed = elapsedNs(io, t0);
    reportJoin("pure range (no equi)", .range_sweep, l_rows, r_rows, output, elapsed);
}

/// Same shape, explicit nested_loop algorithm for comparison.
fn benchPureRangeNljExplicit(allocator: Allocator, io: Io, l_rows: usize, r_rows: usize) !void {
    var dir = try freshDir(io, ".bench-data/join_pure_range_nlj");
    defer dir.close(io);
    var db = try thindb.Database.open(allocator, io, dir, .{
        .auto_flush_rows = std.math.maxInt(u64),
        .auto_flush_bytes = std.math.maxInt(usize),
        .auto_flush_secs = 0,
    });
    defer db.close();

    const l = try db.table("l", pure_l_schema, .{ .order_key = &pure_l_ok, .unique = true });
    try fillPureLeft(l, l_rows, allocator);
    try l.flush();
    const r = try db.table("r", pure_r_schema, .{ .order_key = &pure_r_ok, .unique = true });
    try fillPureRight(r, r_rows, allocator);
    try r.flush();

    const left = try thindb.scan(allocator, l);
    const right = try thindb.scan(allocator, r);
    const t0 = Io.Clock.awake.now(io);
    var q = try left.join(right, .{
        .on = &.{},
        .ranges = &.{.{ .left = "x", .op = .lt, .right = "y" }},
        .algorithm = .nested_loop,
    });
    defer q.deinit();
    var output: usize = 0;
    while (try q.next()) |b| output += b.row_count;
    const elapsed = elapsedNs(io, t0);
    reportJoin("pure range (no equi)", .nested_loop, l_rows, r_rows, output, elapsed);
}

fn benchLeftOuterRange(allocator: Allocator, io: Io, n: usize, algo: thindb.exec.join_op.Algorithm) !void {
    var dir = try freshDir(io, ".bench-data/join_left_outer_range");
    defer dir.close(io);
    var db = try thindb.Database.open(allocator, io, dir, .{
        .auto_flush_rows = std.math.maxInt(u64),
        .auto_flush_bytes = std.math.maxInt(usize),
        .auto_flush_secs = 0,
    });
    defer db.close();

    const l = try db.table("l", range_l_schema, range_opts);
    try fillRangeLeft(l, n, allocator);
    try l.flush();
    const r = try db.table("r", range_r_schema, range_opts);
    try fillRangeRight(r, n, allocator);
    try r.flush();

    const left = try thindb.scan(allocator, l);
    const right = try thindb.scan(allocator, r);
    const t0 = Io.Clock.awake.now(io);
    var q = try left.join(right, .{
        .join_type = .left,
        .on = &.{.{ .left = "k", .right = "k" }},
        .ranges = &.{.{ .left = "x", .op = .lt, .right = "y" }},
        .algorithm = algo,
    });
    defer q.deinit();
    var output: usize = 0;
    while (try q.next()) |b| output += b.row_count;
    const elapsed = elapsedNs(io, t0);
    reportJoin("LEFT OUTER + range", algo, n, n, output, elapsed);
}
