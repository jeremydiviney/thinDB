//! Phase 2B: filtering predicates evaluated directly on FOR-encoded blocks.
//! Builds tables whose bounded-range integer columns FOR-encode (forced to disk
//! via flush), then runs every comparison operator with constants at the
//! interesting boundaries (below min, at min, interior, at max, above max) and
//! against a brute-force reference computed in the test. The point: the
//! FOR-domain comparison must give bit-for-bit the same survivors as comparing
//! the native values. Also covers NULLs, multiple segments, a stays-raw
//! column (fallback), and row groups whose stats prove the predicate for
//! every row, which skip the filter.

const std = @import("std");
const thindb = @import("thindb");
const helpers = @import("sql_helpers.zig");
const runSql = helpers.runSql;
const exec = helpers.exec;

const ROWS: i64 = 2500; // > rg_size below so each segment is multi-row-group

// `v`: INT in [V_MIN, V_MAX], 256 distinct → u8 deltas → FOR width 1 over a
// 4-byte native, so the column is FOR-encoded.
const V_MIN: i64 = 100;
const V_MAX: i64 = 355;
fn vFor(i: i64) i64 {
    return V_MIN + @mod(i * 37, V_MAX - V_MIN + 1);
}

// `vbig`: BIGINT in [VB_MIN, VB_MAX] with a ~60k span → u16 deltas → FOR width 2
// over an 8-byte native. Exercises a different code width + column type.
const VB_MIN: i64 = 9_000_000;
const VB_MAX: i64 = 9_060_000;
fn vbigFor(i: i64) i64 {
    return VB_MIN + @mod(i * 101, VB_MAX - VB_MIN + 1);
}

// `nv`: nullable INT in [NV_MIN, NV_MAX] with every 5th row NULL.
const NV_MIN: i64 = -200;
const NV_MAX: i64 = -50;
fn nvValid(i: i64) bool {
    return @mod(i, 5) != 0;
}
fn nvFor(i: i64) i64 {
    return NV_MIN + @mod(i * 13, NV_MAX - NV_MIN + 1);
}

// `wide`: full-spread BIGINT that cannot narrow → stays raw (fallback path).
fn wideFor(i: i64) i64 {
    return @mod(i * 2_400_000_000_007, std.math.maxInt(i64));
}

fn setup(allocator: std.mem.Allocator, db: anytype, rows: i64, id_base: i64) !void {
    var buf: std.ArrayList(u8) = .empty;
    defer buf.deinit(allocator);
    try buf.appendSlice(allocator, "INSERT INTO t (id, v, vbig, nv, wide) VALUES ");
    var line: [160]u8 = undefined;
    var i: i64 = 0;
    while (i < rows) : (i += 1) {
        if (i != 0) try buf.appendSlice(allocator, ", ");
        const id = id_base + i;
        if (nvValid(i)) {
            const s = try std.fmt.bufPrint(&line, "({d}, {d}, {d}, {d}, {d})", .{
                id, vFor(i), vbigFor(i), nvFor(i), wideFor(i),
            });
            try buf.appendSlice(allocator, s);
        } else {
            const s = try std.fmt.bufPrint(&line, "({d}, {d}, {d}, NULL, {d})", .{
                id, vFor(i), vbigFor(i), wideFor(i),
            });
            try buf.appendSlice(allocator, s);
        }
    }
    try exec(allocator, db, buf.items);
}

const Op = enum { eq, neq, lt, lte, gt, gte };

fn opSql(op: Op) []const u8 {
    return switch (op) {
        .eq => "=",
        .neq => "<>",
        .lt => "<",
        .lte => "<=",
        .gt => ">",
        .gte => ">=",
    };
}

fn cmp(a: i64, op: Op, b: i64) bool {
    return switch (op) {
        .eq => a == b,
        .neq => a != b,
        .lt => a < b,
        .lte => a <= b,
        .gt => a > b,
        .gte => a >= b,
    };
}

const Segment = struct { base: i64, rows: i64 };

/// Collect the id set returned by `SELECT id FROM t WHERE <col> <op> <c>`.
fn collectIds(allocator: std.mem.Allocator, db: anytype, sql: []const u8) !std.AutoHashMap(i64, void) {
    var set = std.AutoHashMap(i64, void).init(allocator);
    errdefer set.deinit();
    var q = try runSql(allocator, db, sql);
    defer q.deinit();
    while (try q.next()) |batch| {
        for (batch.values[0].data.bigint[0..batch.row_count]) |id| {
            try set.put(id, {});
        }
    }
    return set;
}

/// Brute-force reference: every id whose generated `gen(i)` satisfies the
/// comparison, across all inserted segments. NULL rows (when `nullable`) never
/// match a comparison.
fn referenceIds(
    allocator: std.mem.Allocator,
    segments: []const Segment,
    gen: *const fn (i64) i64,
    is_null: ?*const fn (i64) bool,
    op: Op,
    c: i64,
) !std.AutoHashMap(i64, void) {
    var set = std.AutoHashMap(i64, void).init(allocator);
    errdefer set.deinit();
    for (segments) |seg| {
        var i: i64 = 0;
        while (i < seg.rows) : (i += 1) {
            const id = seg.base + i;
            if (is_null) |f| if (!f(i)) continue;
            if (cmp(gen(i), op, c)) try set.put(id, {});
        }
    }
    return set;
}

fn expectSameSet(a: *std.AutoHashMap(i64, void), b: *std.AutoHashMap(i64, void)) !void {
    try std.testing.expectEqual(a.count(), b.count());
    var it = a.keyIterator();
    while (it.next()) |k| try std.testing.expect(b.contains(k.*));
}

/// For one column, run every operator at boundary/interior constants and check
/// the engine's survivor id set against the brute-force reference.
fn checkColumn(
    allocator: std.mem.Allocator,
    db: anytype,
    segments: []const Segment,
    col: []const u8,
    gen: *const fn (i64) i64,
    is_null: ?*const fn (i64) bool,
    min: i64,
    max: i64,
) !void {
    const consts = [_]i64{ min - 1, min, @divTrunc(min + max, 2), max, max + 1 };
    inline for (.{ Op.eq, Op.neq, Op.lt, Op.lte, Op.gt, Op.gte }) |op| {
        for (consts) |c| {
            var sql_buf: [256]u8 = undefined;
            const sql = try std.fmt.bufPrint(&sql_buf, "SELECT id FROM t WHERE {s} {s} {d}", .{ col, opSql(op), c });
            var got = try collectIds(allocator, db, sql);
            defer got.deinit();
            var want = try referenceIds(allocator, segments, gen, is_null, op, c);
            defer want.deinit();
            expectSameSet(&want, &got) catch |e| {
                std.debug.print("MISMATCH col={s} op={s} c={d}: want={d} got={d}\n", .{ col, opSql(op), c, want.count(), got.count() });
                return e;
            };
        }
    }
}

fn createTable(allocator: std.mem.Allocator, db: anytype) !void {
    try exec(allocator, db,
        \\CREATE TABLE t (
        \\  id BIGINT PRIMARY KEY, v INT NOT NULL, vbig BIGINT NOT NULL,
        \\  nv INT, wide BIGINT NOT NULL
        \\)
    );
}

test "FOR-aware filter matches native filter across all ops/boundaries (single segment)" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const db = try thindb.Database.open(allocator, io, tmp.dir, .{ .row_group_size = 256 });
    defer db.close();
    try createTable(allocator, db);

    try setup(allocator, db, ROWS, 1);
    const t = try db.openTable("t", .{});
    try t.flush();
    try std.testing.expectEqual(@as(usize, 1), t.segmentCount());

    const segments = [_]Segment{.{ .base = 1, .rows = ROWS }};
    try checkColumn(allocator, db, &segments, "v", vFor, null, V_MIN, V_MAX);
    try checkColumn(allocator, db, &segments, "vbig", vbigFor, null, VB_MIN, VB_MAX);
    try checkColumn(allocator, db, &segments, "nv", nvFor, nvValid, NV_MIN, NV_MAX);
    // wide stays raw → exercises the fallback path; results must still match.
    try checkColumn(allocator, db, &segments, "wide", wideFor, null, 0, std.math.maxInt(i64) - 1);
}

test "FOR-aware filter matches native filter across multiple segments" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const db = try thindb.Database.open(allocator, io, tmp.dir, .{ .row_group_size = 256 });
    defer db.close();
    try createTable(allocator, db);

    try setup(allocator, db, ROWS, 1);
    const t = try db.openTable("t", .{});
    try t.flush();
    try setup(allocator, db, ROWS, ROWS + 1);
    try t.flush();
    try std.testing.expectEqual(@as(usize, 2), t.segmentCount());

    const segments = [_]Segment{
        .{ .base = 1, .rows = ROWS },
        .{ .base = ROWS + 1, .rows = ROWS },
    };
    try checkColumn(allocator, db, &segments, "v", vFor, null, V_MIN, V_MAX);
    try checkColumn(allocator, db, &segments, "vbig", vbigFor, null, VB_MIN, VB_MAX);
    try checkColumn(allocator, db, &segments, "nv", nvFor, nvValid, NV_MIN, NV_MAX);
}

test "FOR-aware AND over FOR + raw columns, projecting FOR + raw + string" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const db = try thindb.Database.open(allocator, io, tmp.dir, .{ .row_group_size = 256 });
    defer db.close();

    // `a`/`b` are bounded-range INTs → FOR-encoded; `r` is a full-spread BIGINT
    // that stays raw; `s` is a low-card VARCHAR. The WHERE below is a 3-leaf AND
    // mixing two FOR columns and the raw column, and the projection mixes a FOR
    // column (`a`), the raw column (`r`), and the string column (`s`) — so the
    // per-column borrow + the AND-of-leaves FOR path are both exercised, and the
    // string column must come back byte-exact through the zero-copy borrow.
    try exec(allocator, db,
        \\CREATE TABLE t (
        \\  id BIGINT PRIMARY KEY, a INT NOT NULL, b INT NOT NULL,
        \\  r BIGINT NOT NULL, s VARCHAR(32)
        \\)
    );

    const ROWS2: i64 = 2000;
    {
        var buf: std.ArrayList(u8) = .empty;
        defer buf.deinit(allocator);
        try buf.appendSlice(allocator, "INSERT INTO t (id, a, b, r, s) VALUES ");
        var line: [128]u8 = undefined;
        var i: i64 = 0;
        while (i < ROWS2) : (i += 1) {
            if (i != 0) try buf.appendSlice(allocator, ", ");
            const a = 100 + @mod(i * 7, 200); // [100,299]
            const b = 50 + @mod(i * 3, 100); //  [50,149]
            const r = @mod(i * 2_400_000_000_007, std.math.maxInt(i64));
            const s = try std.fmt.bufPrint(&line, "({d}, {d}, {d}, {d}, 'cat{d}')", .{ i + 1, a, b, r, @mod(i, 4) });
            try buf.appendSlice(allocator, s);
        }
        try exec(allocator, db, buf.items);
    }
    const t = try db.openTable("t", .{});
    try t.flush();
    try std.testing.expectEqual(@as(usize, 1), t.segmentCount());

    // a > 150 AND b <= 120 AND r >= 0 (r >= 0 is always-true on the raw column,
    // exercising the raw leaf in the AND without changing the result set).
    const sql = "SELECT id, a, r, s FROM t WHERE a > 150 AND b <= 120 AND r >= 0 ORDER BY id";

    var got_ids: std.ArrayList(i64) = .empty;
    defer got_ids.deinit(allocator);
    {
        var q = try runSql(allocator, db, sql);
        defer q.deinit();
        while (try q.next()) |batch| {
            for (0..batch.row_count) |row| {
                const id = batch.values[0].data.bigint[row];
                const a_val = batch.values[1].data.int[row];
                const s_val = batch.values[3].data.varchar.rowBytes(row);
                // The id encodes i = id - 1; recompute the expected derived values.
                const i = id - 1;
                try std.testing.expectEqual(@as(i32, @intCast(100 + @mod(i * 7, 200))), a_val);
                var sbuf: [16]u8 = undefined;
                const want_s = try std.fmt.bufPrint(&sbuf, "cat{d}", .{@mod(i, 4)});
                try std.testing.expectEqualStrings(want_s, s_val);
                try got_ids.append(allocator, id);
            }
        }
    }

    // Brute-force reference: same predicate computed in the test.
    var want_count: usize = 0;
    var i: i64 = 0;
    while (i < ROWS2) : (i += 1) {
        const a = 100 + @mod(i * 7, 200);
        const b = 50 + @mod(i * 3, 100);
        if (a > 150 and b <= 120) want_count += 1;
    }
    try std.testing.expectEqual(want_count, got_ids.items.len);
}

test "FOR-aware filter with a memtable tail (flushed segment + un-flushed rows)" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const db = try thindb.Database.open(allocator, io, tmp.dir, .{ .row_group_size = 256 });
    defer db.close();
    try createTable(allocator, db);

    // First batch flushed to a FOR-encoded segment, second batch left in the
    // memtable (always raw) — the scan filters both and the union must match.
    try setup(allocator, db, ROWS, 1);
    const t = try db.openTable("t", .{});
    try t.flush();
    try setup(allocator, db, 300, ROWS + 1);

    const segments = [_]Segment{
        .{ .base = 1, .rows = ROWS },
        .{ .base = ROWS + 1, .rows = 300 },
    };
    try checkColumn(allocator, db, &segments, "v", vFor, null, V_MIN, V_MAX);
    try checkColumn(allocator, db, &segments, "nv", nvFor, nvValid, NV_MIN, NV_MAX);

    try t.flush();
}

// Six columns of 8+4+4+8+8+32 estimated bytes put the scan's sub-batch at its
// 2048-row floor, so a 16K-row group keeping 80% of its rows emits several
// sub-batches through the dense masked pass, and one keeping 20% emits two
// through the index gather. `r`/`nr` stay raw (full spread), `nr` with NULLs.
const SLICE_RG: i64 = 16384;

fn sliceA(i: i64) i64 {
    return @mod(i * 7, 100);
}
fn sliceB(i: i64) i64 {
    return @mod(i * 3, 50);
}
fn sliceR(i: i64) i64 {
    return @mod(i * 2_400_000_000_007, std.math.maxInt(i64));
}
fn sliceNr(i: i64) ?i64 {
    if (@mod(i, 3) == 0) return null;
    return @mod(i * 1_800_000_000_011, std.math.maxInt(i64));
}

fn checkSlicedFilter(allocator: std.mem.Allocator, t: *thindb.Table, a_below: i32, rows: i64) !void {
    const conjuncts = [_]thindb.exec.PredicateExpr{
        thindb.leafExpr("a", .lt, .{ .int = a_below }),
        thindb.leafExpr("b", .neq, .{ .int = -1 }),
        thindb.leafExpr("r", .gte, .{ .bigint = 0 }),
    };
    var base = try thindb.scan(allocator, t);
    var q = try base.filter(.{ .@"and" = &conjuncts });
    defer q.deinit();

    var seen = std.AutoHashMap(i64, void).init(allocator);
    defer seen.deinit();
    var batches: usize = 0;
    while (try q.next()) |batch| {
        batches += 1;
        for (0..batch.row_count) |row| {
            const id = batch.values[0].data.bigint[row];
            const i = id - 1;
            try std.testing.expectEqual(@as(i32, @intCast(sliceA(i))), batch.values[1].data.int[row]);
            try std.testing.expectEqual(@as(i32, @intCast(sliceB(i))), batch.values[2].data.int[row]);
            try std.testing.expectEqual(sliceR(i), batch.values[3].data.bigint[row]);
            if (sliceNr(i)) |nr| {
                try std.testing.expect(batch.values[4].isValid(row));
                try std.testing.expectEqual(nr, batch.values[4].data.bigint[row]);
            } else {
                try std.testing.expect(!batch.values[4].isValid(row));
            }
            var sbuf: [16]u8 = undefined;
            try std.testing.expectEqualStrings(try std.fmt.bufPrint(&sbuf, "cat{d}", .{@mod(i, 4)}), batch.values[5].data.varchar.rowBytes(row));
            try std.testing.expect(sliceA(i) < a_below);
            try seen.put(id, {});
        }
    }
    var want: usize = 0;
    var i: i64 = 0;
    while (i < rows) : (i += 1) want += @intFromBool(sliceA(i) < a_below);
    try std.testing.expectEqual(want, seen.count());
    try std.testing.expect(batches > @as(usize, @intCast(@divExact(rows, SLICE_RG))));
}

test "guided filter emits a row group's many survivors in sub-batches" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const db = try thindb.Database.open(allocator, io, tmp.dir, .{ .row_group_size = @intCast(SLICE_RG) });
    defer db.close();
    try exec(allocator, db,
        \\CREATE TABLE t (
        \\  id BIGINT PRIMARY KEY, a INT NOT NULL, b INT NOT NULL,
        \\  r BIGINT NOT NULL, nr BIGINT, s VARCHAR(32)
        \\)
    );
    const rows = 2 * SLICE_RG;
    var id: i64 = 1;
    while (id <= rows) {
        var buf: std.ArrayList(u8) = .empty;
        defer buf.deinit(allocator);
        try buf.appendSlice(allocator, "INSERT INTO t (id, a, b, r, nr, s) VALUES ");
        const chunk_end = @min(id + 4096, rows + 1);
        var line: [160]u8 = undefined;
        while (id < chunk_end) : (id += 1) {
            if (buf.items.len > 42) try buf.appendSlice(allocator, ", ");
            const i = id - 1;
            const s = if (sliceNr(i)) |nr|
                try std.fmt.bufPrint(&line, "({d}, {d}, {d}, {d}, {d}, 'cat{d}')", .{ id, sliceA(i), sliceB(i), sliceR(i), nr, @mod(i, 4) })
            else
                try std.fmt.bufPrint(&line, "({d}, {d}, {d}, {d}, NULL, 'cat{d}')", .{ id, sliceA(i), sliceB(i), sliceR(i), @mod(i, 4) });
            try buf.appendSlice(allocator, s);
        }
        try exec(allocator, db, buf.items);
    }
    const t = try db.openTable("t", .{});
    try t.flush();

    try checkSlicedFilter(allocator, t, 80, rows);
    try checkSlicedFilter(allocator, t, 20, rows);
}

// Row groups of `STATS_RG` rows, each holding one `d` value: a range on `d`
// covers whole row groups, which the stats then prove, next to row groups it
// cuts. Even row groups hold blank `s`, odd ones none; the first half holds
// NULL `ns`.
const STATS_RG: i64 = 256;
const STATS_ROWS: i64 = 8 * STATS_RG;
const STATS_DELETED = [_]i64{ 300, 301, 777, 1500 };
const STATS_MEM_BASE: i64 = 3000;
const STATS_MEM_ROWS: i64 = 10;

const StatsRow = struct {
    id: i64,
    g: i64,
    d: i64,
    /// `s` is `'u<s_key>'`, `'m'` when `s_key` is 50, or blank when null.
    s_key: ?i64,
    ns_null: bool,
    v: i64,
};

fn statsRow(i: i64) StatsRow {
    const d = @divTrunc(i, STATS_RG);
    return .{
        .id = i,
        .g = @mod(i, 7),
        .d = d,
        .s_key = if (@mod(d, 2) == 0 and @mod(i, 3) == 0) null else @mod(i, 50),
        .ns_null = d < 4 and @mod(i, 5) == 0,
        .v = i * 3,
    };
}

fn statsMemRow(id: i64) StatsRow {
    return .{ .id = id, .g = @mod(id, 7), .d = 9, .s_key = 50, .ns_null = true, .v = id * 3 };
}

fn statsLiveRows(allocator: std.mem.Allocator) ![]StatsRow {
    var rows: std.ArrayList(StatsRow) = .empty;
    errdefer rows.deinit(allocator);
    var i: i64 = 0;
    while (i < STATS_ROWS) : (i += 1) {
        if (std.mem.indexOfScalar(i64, &STATS_DELETED, i) != null) continue;
        try rows.append(allocator, statsRow(i));
    }
    var id = STATS_MEM_BASE;
    while (id < STATS_MEM_BASE + STATS_MEM_ROWS) : (id += 1) try rows.append(allocator, statsMemRow(id));
    return rows.toOwnedSlice(allocator);
}

fn statsSLen(key: i64) i64 {
    if (key == 50) return 1;
    return if (key < 10) 2 else 3;
}

fn insertStatsRow(allocator: std.mem.Allocator, buf: *std.ArrayList(u8), r: StatsRow) !void {
    var line: [160]u8 = undefined;
    var s_buf: [8]u8 = undefined;
    const s_text = if (r.s_key) |k| (if (k == 50) "m" else try std.fmt.bufPrint(&s_buf, "u{d}", .{k})) else "";
    var ns_buf: [8]u8 = undefined;
    const ns_text = if (r.ns_null) "NULL" else try std.fmt.bufPrint(&ns_buf, "'n{d}'", .{@mod(r.id, 11)});
    try buf.appendSlice(allocator, try std.fmt.bufPrint(&line, "({d}, {d}, {d}, '{s}', {s}, {d})", .{ r.id, r.g, r.d, s_text, ns_text, r.v }));
}

fn expectIdsWhere(allocator: std.mem.Allocator, db: anytype, sql: []const u8, live: []const StatsRow, keep: *const fn (StatsRow) bool) !void {
    const got = try helpers.collectBigints(allocator, db, sql);
    defer allocator.free(got);
    var want: std.ArrayList(i64) = .empty;
    defer want.deinit(allocator);
    for (live) |r| if (keep(r)) try want.append(allocator, r.id);
    errdefer std.debug.print("query: {s}\n", .{sql});
    try std.testing.expectEqualSlices(i64, want.items, got);
}

fn statsMidRange(r: StatsRow) bool {
    return r.d >= 2 and r.d <= 5;
}
fn statsNonBlank(r: StatsRow) bool {
    return r.s_key != null;
}
fn statsNsEarly(r: StatsRow) bool {
    return !r.ns_null and r.d < 6;
}
fn statsNotMem(r: StatsRow) bool {
    return r.d != 9;
}

test "row groups whose stats prove the filter skip it" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const db = try thindb.Database.open(allocator, io, tmp.dir, .{ .row_group_size = @intCast(STATS_RG) });
    defer db.close();
    try exec(allocator, db, "CREATE TABLE z (id BIGINT PRIMARY KEY, g INT NOT NULL, d INT NOT NULL, s VARCHAR(16) NOT NULL, ns VARCHAR(16), v BIGINT NOT NULL)");
    {
        var buf: std.ArrayList(u8) = .empty;
        defer buf.deinit(allocator);
        try buf.appendSlice(allocator, "INSERT INTO z (id, g, d, s, ns, v) VALUES ");
        var i: i64 = 0;
        while (i < STATS_ROWS) : (i += 1) {
            if (i > 0) try buf.appendSlice(allocator, ", ");
            try insertStatsRow(allocator, &buf, statsRow(i));
        }
        try exec(allocator, db, buf.items);
    }
    const t = try db.openTable("z", .{});
    try t.flush();
    try exec(allocator, db, "DELETE FROM z WHERE id IN (300, 301, 777, 1500)");
    {
        var buf: std.ArrayList(u8) = .empty;
        defer buf.deinit(allocator);
        try buf.appendSlice(allocator, "INSERT INTO z (id, g, d, s, ns, v) VALUES ");
        var id = STATS_MEM_BASE;
        while (id < STATS_MEM_BASE + STATS_MEM_ROWS) : (id += 1) {
            if (id > STATS_MEM_BASE) try buf.appendSlice(allocator, ", ");
            try insertStatsRow(allocator, &buf, statsMemRow(id));
        }
        try exec(allocator, db, buf.items);
    }

    const live = try statsLiveRows(allocator);
    defer allocator.free(live);

    try expectIdsWhere(allocator, db, "SELECT id FROM z WHERE d >= 2 AND d <= 5 ORDER BY id", live, statsMidRange);
    try expectIdsWhere(allocator, db, "SELECT id FROM z WHERE s <> '' ORDER BY id", live, statsNonBlank);
    try expectIdsWhere(allocator, db, "SELECT id FROM z WHERE ns IS NOT NULL AND d < 6 ORDER BY id", live, statsNsEarly);
    try expectIdsWhere(allocator, db, "SELECT id FROM z WHERE d <> 9 ORDER BY id", live, statsNotMem);

    // Grouped folds over the proven row groups: per g, COUNT(*) and SUM(v)
    // past d >= 3, then SUM(LENGTH(s)) and COUNT(DISTINCT s) of the non-blank.
    var counts = [_]i64{0} ** 7;
    var sums = [_]i64{0} ** 7;
    var lens = [_]i64{0} ** 7;
    var seen = [_][51]bool{[_]bool{false} ** 51} ** 7;
    for (live) |r| {
        const g: usize = @intCast(r.g);
        if (r.d >= 3) {
            counts[g] += 1;
            sums[g] += r.v;
        }
        if (r.s_key) |k| {
            lens[g] += statsSLen(k);
            seen[g][@intCast(k)] = true;
        }
    }
    {
        var q = try runSql(allocator, db, "SELECT g, COUNT(*), SUM(v) FROM z WHERE d >= 3 GROUP BY g ORDER BY g");
        defer q.deinit();
        const cells = try helpers.collectIntCells(allocator, &q);
        defer allocator.free(cells);
        try std.testing.expectEqual(@as(usize, 21), cells.len);
        for (0..7) |g| {
            try std.testing.expectEqual(@as(?i64, @intCast(g)), cells[g * 3]);
            try std.testing.expectEqual(@as(?i64, counts[g]), cells[g * 3 + 1]);
            try std.testing.expectEqual(@as(?i64, sums[g]), cells[g * 3 + 2]);
        }
    }
    {
        var q = try runSql(allocator, db, "SELECT g, SUM(LENGTH(s)), COUNT(DISTINCT s) FROM z WHERE s <> '' GROUP BY g ORDER BY g");
        defer q.deinit();
        const cells = try helpers.collectIntCells(allocator, &q);
        defer allocator.free(cells);
        try std.testing.expectEqual(@as(usize, 21), cells.len);
        for (0..7) |g| {
            var distinct: i64 = 0;
            for (seen[g]) |b| distinct += @intFromBool(b);
            try std.testing.expectEqual(@as(?i64, @intCast(g)), cells[g * 3]);
            try std.testing.expectEqual(@as(?i64, lens[g]), cells[g * 3 + 1]);
            try std.testing.expectEqual(@as(?i64, distinct), cells[g * 3 + 2]);
        }
    }

    // Late materialization re-fetches the wide columns by row location.
    const top = try helpers.collectBigints(allocator, db, "SELECT id, s, ns FROM z WHERE d >= 4 AND d < 9 ORDER BY v DESC LIMIT 5");
    defer allocator.free(top);
    try std.testing.expectEqualSlices(i64, &.{ 2047, 2046, 2045, 2044, 2043 }, top);
    try helpers.expectText(allocator, db, "SELECT s FROM z WHERE d >= 4 AND d < 9 ORDER BY v DESC LIMIT 1", "u47");
}
