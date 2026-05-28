//! Tests for `src/exec/zonemap_topn.zig`. Brought in via exec.zig's `test`
//! block. The core gate: for many generated tables + queries, the zonemap
//! operator's output rows must be IDENTICAL (same rows, same order) to the
//! reference lateScan (Scan→Filter→TopN→fetch) path.

const std = @import("std");

const exec = @import("exec.zig");
const Query = exec.Query;
const PredicateExpr = exec.PredicateExpr;
const leafExpr = exec.leafExpr;
const SortSpec = exec.SortSpec;

const types = @import("../types.zig");
const Column = types.Column;
const Value = types.Value;

const storage = @import("../storage/storage.zig");
const ColumnView = storage.ColumnView;

const engine = @import("../engine/engine.zig");
const ColumnStore = engine.ColumnStore;

const api = @import("../api/api.zig");

// ---------------------------------------------------------------------------
// Result capture: drain a Query into row-major scalar tuples for comparison.
// ---------------------------------------------------------------------------

/// A single scalar cell, normalized to a comparable representation.
const Cell = union(enum) {
    int: i128,
    str: []u8,
    flt: u64, // f64 bit pattern (NaN-free in these tests), exact-comparable
    nul,
};

const Row = struct {
    cells: []Cell,
};

const Captured = struct {
    rows: std.ArrayListUnmanaged(Row) = .empty,
    arena: std.heap.ArenaAllocator,

    fn init(backing: std.mem.Allocator) Captured {
        return .{ .arena = std.heap.ArenaAllocator.init(backing) };
    }
    fn deinit(self: *Captured) void {
        self.arena.deinit();
    }
};

fn cellOf(arena: std.mem.Allocator, v: ColumnView, row: usize) !Cell {
    if (v.nulls != null and !v.isValid(row)) return .nul;
    return switch (v.data) {
        .int => |s| .{ .int = s[row] },
        .bigint => |s| .{ .int = s[row] },
        .boolean => |s| .{ .int = s[row] },
        .tinyint => |s| .{ .int = s[row] },
        .smallint => |s| .{ .int = s[row] },
        .largeint => |s| .{ .int = s[row] },
        .date => |s| .{ .int = s[row] },
        .datetime => |s| .{ .int = s[row] },
        .decimal64 => |s| .{ .int = s[row] },
        .decimal128 => |s| .{ .int = s[row] },
        .uuid => |s| .{ .int = @bitCast(s[row]) },
        .float => |s| .{ .flt = @as(u64, @bitCast(@as(f64, s[row]))) },
        .double => |s| .{ .flt = @bitCast(s[row]) },
        .varchar => |s| .{ .str = try arena.dupe(u8, s.rowBytes(row)) },
        .string => |s| .{ .str = try arena.dupe(u8, s.rowBytes(row)) },
        .char => |s| .{ .str = try arena.dupe(u8, s.rowBytes(row)) },
    };
}

fn capture(backing: std.mem.Allocator, q: *Query) !Captured {
    var cap = Captured.init(backing);
    errdefer cap.deinit();
    const arena = cap.arena.allocator();
    while (try q.next()) |b| {
        for (0..b.row_count) |r| {
            const cells = try arena.alloc(Cell, b.values.len);
            for (b.values, 0..) |v, c| cells[c] = try cellOf(arena, v, r);
            try cap.rows.append(arena, .{ .cells = cells });
        }
    }
    return cap;
}

fn cellEql(a: Cell, b: Cell) bool {
    if (std.meta.activeTag(a) != std.meta.activeTag(b)) return false;
    return switch (a) {
        .int => |x| x == b.int,
        .flt => |x| x == b.flt,
        .nul => true,
        .str => |x| std.mem.eql(u8, x, b.str),
    };
}

fn expectSameRows(want: Captured, got: Captured) !void {
    try std.testing.expectEqual(want.rows.items.len, got.rows.items.len);
    for (want.rows.items, got.rows.items, 0..) |wr, gr, ri| {
        if (wr.cells.len != gr.cells.len) {
            std.debug.print("row {d}: column count differs\n", .{ri});
            return error.TestExpectedEqual;
        }
        for (wr.cells, gr.cells, 0..) |wc, gc, ci| {
            if (!cellEql(wc, gc)) {
                std.debug.print("row {d} col {d} differs: {any} vs {any}\n", .{ ri, ci, wc, gc });
                return error.TestExpectedEqual;
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Table-building helper: insert runtime-generated rows via insertBatch.
// ---------------------------------------------------------------------------

/// Concrete `Type` for a scenario's `TypeTag` (decimals get a fixed spec).
fn typeOf(tag: types.TypeTag) types.Type {
    return switch (tag) {
        .int => .int,
        .bigint => .bigint,
        .smallint => .smallint,
        .tinyint => .tinyint,
        .date => .date,
        .datetime => .datetime,
        .decimal64 => .{ .decimal64 = .{ .p = 18, .s = 2 } },
        .decimal128 => .{ .decimal128 = .{ .p = 30, .s = 2 } },
        .float => .float,
        .double => .double,
        .string => .string,
        .varchar => .{ .varchar = 64 },
        .char => .{ .char = 8 },
        else => unreachable,
    };
}

/// One inserted batch's data, as parallel value lists. Built into ColumnStores
/// then inserted via `insertBatch`.
fn insertGen(
    allocator: std.mem.Allocator,
    t: *api.Table,
    schema: []const Column,
    cols: []ColumnStore,
    row_count: usize,
) !void {
    const views = try allocator.alloc(ColumnView, cols.len);
    defer allocator.free(views);
    for (cols, 0..) |c, i| views[i] = c.view();
    try t.insertBatch(schema, views, row_count);
}

// Deterministic data generation.
const Gen = struct {
    rng: std.Random.DefaultPrng,
    fn init(seed: u64) Gen {
        return .{ .rng = std.Random.DefaultPrng.init(seed) };
    }
    fn r(self: *Gen) std.Random {
        return self.rng.random();
    }
};

// ---------------------------------------------------------------------------
// The exhaustive cross-check driver.
// ---------------------------------------------------------------------------

const Scenario = struct {
    /// leading key column type tag
    lead_type: types.TypeTag,
    /// secondary key column type tag (string / float / int) — null = none
    second_type: ?types.TypeTag = null,
    lead_desc: bool = false,
    second_desc: bool = false,
    /// 0 = no filter, 1 = selective (k1 > median), 2 = non-selective (k1 >= min)
    filter: u8 = 0,
    n: usize,
    offset: usize = 0,
    /// rows per flushed batch (segments); list length = number of flush groups
    flush_groups: []const usize,
    /// rows left in the memtable (unflushed)
    memtable_rows: usize,
    /// duplicate the leading key heavily (ties)
    ties: bool = false,
    row_group_size: usize = 4,
    seed: u64,
};

/// Build, populate, run both paths, and assert identical output. The output
/// projection is every base column (`SELECT *`-like wide fetch). The probe set
/// is filter ∪ ORDER BY columns. Schema: k1 (leading), k2 (secondary, optional),
/// fcol (filter target = k1 here for simplicity), w0/w1 (wide payload).
fn runScenario(sc: Scenario) !void {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    // Schema: id(bigint, wide payload), k1(lead), k2(optional secondary),
    // w(int wide payload). The ORDER BY targets k1 [, k2]; filter targets k1.
    var col_buf: [4]Column = undefined;
    var n_cols: usize = 0;
    col_buf[n_cols] = .{ .name = "k1", .type = typeOf(sc.lead_type) };
    n_cols += 1;
    if (sc.second_type) |st| {
        col_buf[n_cols] = .{ .name = "k2", .type = typeOf(st), .nullable = false };
        n_cols += 1;
    }
    col_buf[n_cols] = .{ .name = "w", .type = .bigint };
    n_cols += 1;
    col_buf[n_cols] = .{ .name = "tag", .type = .string };
    n_cols += 1;
    const schema_cols = col_buf[0..n_cols];

    const schema = types.TableSchema{
        .columns = schema_cols,
        .order_key = &.{"k1"},
        .unique = false,
    };

    var db = try api.Database.open(allocator, io, tmp.dir, .{
        .row_group_size = sc.row_group_size,
        // Disable auto-flush so the memtable tail stays put.
        .auto_flush_rows = std.math.maxInt(u64),
        .auto_flush_bytes = std.math.maxInt(u64),
    });
    defer db.close();

    const t = try db.table("t", schema, .{ .order_key = &.{"k1"}, .row_group_size = sc.row_group_size });

    var gen = Gen.init(sc.seed);
    var next_w: i64 = 0;

    // Generate one batch of `n` rows into ColumnStores keyed off the scenario.
    const genBatch = struct {
        fn run(g: *Gen, alloc: std.mem.Allocator, scen: Scenario, cols_schema: []const Column, count: usize, w_start: *i64) ![]ColumnStore {
            const cols = try alloc.alloc(ColumnStore, cols_schema.len);
            for (cols_schema, 0..) |c, i| cols[i] = try ColumnStore.init(alloc, c.type, c.nullable);
            const rnd = g.r();
            for (0..count) |_| {
                for (cols_schema, 0..) |c, ci| {
                    if (std.mem.eql(u8, c.name, "k1")) {
                        const base: i64 = if (scen.ties) @intCast(rnd.intRangeAtMost(i64, 0, 5)) else @intCast(rnd.intRangeAtMost(i64, -50, 50));
                        try appendScalar(alloc, &cols[ci], c.type, base);
                    } else if (std.mem.eql(u8, c.name, "k2")) {
                        try appendK2(alloc, &cols[ci], c.type, rnd);
                    } else if (std.mem.eql(u8, c.name, "w")) {
                        try cols[ci].data.bigint.append(alloc, w_start.*);
                        w_start.* += 1;
                    } else { // tag
                        var buf: [16]u8 = undefined;
                        const s = std.fmt.bufPrint(&buf, "t{d}", .{rnd.intRangeAtMost(u32, 0, 999)}) catch "t";
                        try cols[ci].data.string.appendValue(alloc, s);
                    }
                }
            }
            return cols;
        }
        fn appendScalar(alloc: std.mem.Allocator, col: *ColumnStore, ty: types.Type, v: i64) !void {
            switch (ty) {
                .int => try col.data.int.append(alloc, @intCast(v)),
                .smallint => try col.data.smallint.append(alloc, @intCast(v)),
                .tinyint => try col.data.tinyint.append(alloc, @intCast(v)),
                .bigint => try col.data.bigint.append(alloc, v),
                .date => try col.data.date.append(alloc, @intCast(v + 100)),
                .datetime => try col.data.datetime.append(alloc, v * 1000),
                .decimal64 => try col.data.decimal64.append(alloc, v),
                .decimal128 => try col.data.decimal128.append(alloc, v),
                else => unreachable,
            }
        }
        fn appendK2(alloc: std.mem.Allocator, col: *ColumnStore, ty: types.Type, rnd: std.Random) !void {
            switch (ty) {
                .int => try col.data.int.append(alloc, rnd.intRangeAtMost(i32, 0, 9)),
                .float => try col.data.float.append(alloc, @floatFromInt(rnd.intRangeAtMost(i32, 0, 9))),
                .string => {
                    var buf: [8]u8 = undefined;
                    const s = std.fmt.bufPrint(&buf, "s{d}", .{rnd.intRangeAtMost(u32, 0, 9)}) catch "s";
                    try col.data.string.appendValue(alloc, s);
                },
                else => unreachable,
            }
        }
    }.run;

    // Flushed groups (each becomes its own segment).
    for (sc.flush_groups) |grp_rows| {
        if (grp_rows == 0) continue;
        const cols = try genBatch(&gen, allocator, sc, schema_cols, grp_rows, &next_w);
        defer {
            for (cols) |*c| c.deinit(allocator);
            allocator.free(cols);
        }
        try insertGen(allocator, t, schema_cols, cols, grp_rows);
        try t.flush();
    }

    // Memtable tail.
    if (sc.memtable_rows > 0) {
        const cols = try genBatch(&gen, allocator, sc, schema_cols, sc.memtable_rows, &next_w);
        defer {
            for (cols) |*c| c.deinit(allocator);
            allocator.free(cols);
        }
        try insertGen(allocator, t, schema_cols, cols, sc.memtable_rows);
    }

    // Build ORDER BY specs. When ties are stressed, append `w` (a unique
    // monotonic value) as a final key so the full sort tuple is a TOTAL order —
    // otherwise duplicate tuples beyond the limit boundary make the kept set
    // ambiguous and the two paths legitimately diverge on WHICH tied rows win.
    var specs_buf: [3]SortSpec = undefined;
    var n_specs: usize = 0;
    specs_buf[n_specs] = .{ .col = "k1", .desc = sc.lead_desc };
    n_specs += 1;
    if (sc.second_type != null) {
        specs_buf[n_specs] = .{ .col = "k2", .desc = sc.second_desc };
        n_specs += 1;
    }
    if (sc.ties) {
        specs_buf[n_specs] = .{ .col = "w", .desc = false };
        n_specs += 1;
    }
    const order_specs = specs_buf[0..n_specs];

    // Build the probe set (filter ∪ ORDER BY) and predicate.
    var probe_buf: [3][]const u8 = undefined;
    var n_probe: usize = 0;
    probe_buf[n_probe] = "k1";
    n_probe += 1;
    if (sc.second_type != null) {
        probe_buf[n_probe] = "k2";
        n_probe += 1;
    }
    if (sc.ties) {
        probe_buf[n_probe] = "w";
        n_probe += 1;
    }
    const probe_names = probe_buf[0..n_probe];

    const pred: PredicateExpr = switch (sc.filter) {
        0 => .{ .always = true },
        1 => leafExpr("k1", .gt, scalarVal(sc.lead_type, 0)),
        2 => leafExpr("k1", .gte, scalarVal(sc.lead_type, -1000)),
        else => unreachable,
    };

    const output_names = &[_][]const u8{ "k1", "w", "tag" };

    // Reference: explicit lateScan plan (Scan→Filter→TopN→fetch).
    var ref_q = try exec.lateScan(allocator, t, null, probe_names, pred, order_specs, output_names, sc.n, sc.offset);
    defer ref_q.deinit();
    var ref = try capture(allocator, &ref_q);
    defer ref.deinit();

    // Subject: zonemap path. Must be non-null for these (supported) scenarios.
    const z_opt = try exec.zonemapTopN(allocator, t, null, probe_names, pred, order_specs, output_names, sc.n, sc.offset);
    if (z_opt == null) return error.ZonemapShouldHaveApplied;
    var z_q = z_opt.?;
    defer z_q.deinit();
    var got = try capture(allocator, &z_q);
    defer got.deinit();

    try expectSameRows(ref, got);
}

fn scalarVal(ty: types.TypeTag, v: i64) Value {
    return switch (ty) {
        .int => .{ .int = @intCast(v) },
        .smallint => .{ .smallint = @intCast(v) },
        .tinyint => .{ .tinyint = @intCast(v) },
        .bigint => .{ .bigint = v },
        .date => .{ .date = @intCast(v + 100) },
        .datetime => .{ .datetime = v * 1000 },
        .decimal64 => .{ .decimal64 = v },
        .decimal128 => .{ .decimal128 = v },
        else => unreachable,
    };
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

test "zonemap single-key ASC across segments + memtable" {
    inline for (.{ .int, .bigint, .date, .datetime, .decimal64 }) |ty| {
        try runScenario(.{
            .lead_type = ty,
            .n = 5,
            .flush_groups = &.{ 9, 7, 11 },
            .memtable_rows = 4,
            .seed = 0x1111,
        });
    }
}

test "zonemap single-key DESC" {
    inline for (.{ .int, .bigint, .datetime }) |ty| {
        try runScenario(.{
            .lead_type = ty,
            .lead_desc = true,
            .n = 6,
            .flush_groups = &.{ 8, 8, 8 },
            .memtable_rows = 3,
            .seed = 0x2222,
        });
    }
}

test "zonemap multi-key all-ASC, all-DESC, mixed (string secondary)" {
    const dirs = .{
        .{ false, false },
        .{ true, true },
        .{ false, true },
        .{ true, false },
    };
    inline for (dirs) |d| {
        try runScenario(.{
            .lead_type = .int,
            .second_type = .string,
            .lead_desc = d[0],
            .second_desc = d[1],
            .n = 7,
            .flush_groups = &.{ 10, 10 },
            .memtable_rows = 5,
            .seed = 0x3333,
        });
    }
}

test "zonemap multi-key with float secondary" {
    const dirs = .{ .{ false, false }, .{ true, false }, .{ false, true } };
    inline for (dirs) |d| {
        try runScenario(.{
            .lead_type = .bigint,
            .second_type = .float,
            .lead_desc = d[0],
            .second_desc = d[1],
            .n = 5,
            .flush_groups = &.{ 12, 8 },
            .memtable_rows = 4,
            .seed = 0x4444,
        });
    }
}

test "zonemap with selective and non-selective filters" {
    inline for (.{ 1, 2 }) |f| {
        try runScenario(.{
            .lead_type = .int,
            .filter = f,
            .n = 5,
            .flush_groups = &.{ 9, 9, 9 },
            .memtable_rows = 6,
            .seed = 0x5555,
        });
        try runScenario(.{
            .lead_type = .bigint,
            .second_type = .string,
            .lead_desc = true,
            .filter = f,
            .n = 4,
            .flush_groups = &.{ 8, 8 },
            .memtable_rows = 3,
            .seed = 0x5566,
        });
    }
}

test "zonemap LIMIT smaller, equal, larger than match count" {
    // Total rows ~ 14. Try n below, near, and above.
    inline for (.{ 1, 14, 100 }) |n| {
        try runScenario(.{
            .lead_type = .int,
            .n = n,
            .flush_groups = &.{ 6, 8 },
            .memtable_rows = 0,
            .seed = 0x6666,
        });
    }
}

test "zonemap OFFSET variations" {
    inline for (.{ 0, 3, 8, 50 }) |off| {
        try runScenario(.{
            .lead_type = .int,
            .second_type = .string,
            .n = 4,
            .offset = off,
            .flush_groups = &.{ 7, 7, 7 },
            .memtable_rows = 5,
            .seed = 0x7777,
        });
    }
}

test "zonemap ties on leading key + duplicate sort tuples" {
    inline for (.{ false, true }) |desc| {
        try runScenario(.{
            .lead_type = .int,
            .second_type = .int,
            .lead_desc = desc,
            .ties = true,
            .n = 8,
            .flush_groups = &.{ 10, 10, 10 },
            .memtable_rows = 6,
            .seed = 0x8888,
        });
    }
}

test "zonemap fewer than K matches (no early stop)" {
    try runScenario(.{
        .lead_type = .bigint,
        .filter = 1, // selective: roughly half survive
        .n = 1000, // far more than survivors
        .flush_groups = &.{ 8, 8 },
        .memtable_rows = 3,
        .seed = 0x9999,
    });
}

test "zonemap zero matches" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const schema = types.TableSchema{
        .columns = &.{
            .{ .name = "k1", .type = .int },
            .{ .name = "w", .type = .bigint },
            .{ .name = "tag", .type = .string },
        },
        .order_key = &.{"k1"},
        .unique = false,
    };
    var db = try api.Database.open(allocator, io, tmp.dir, .{ .row_group_size = 4 });
    defer db.close();
    const t = try db.table("t", schema, .{ .order_key = &.{"k1"}, .row_group_size = 4 });
    try t.insert(&.{
        .{ .k1 = @as(i32, 1), .w = @as(i64, 1), .tag = "a" },
        .{ .k1 = @as(i32, 2), .w = @as(i64, 2), .tag = "b" },
        .{ .k1 = @as(i32, 3), .w = @as(i64, 3), .tag = "c" },
    });
    try t.flush();

    const probe = &[_][]const u8{"k1"};
    const specs = &[_]SortSpec{.{ .col = "k1", .desc = false }};
    const out = &[_][]const u8{ "k1", "w", "tag" };
    const pred = leafExpr("k1", .gt, .{ .int = 1000 }); // matches nothing

    var ref_q = try exec.lateScan(allocator, t, null, probe, pred, specs, out, 5, 0);
    defer ref_q.deinit();
    var ref = try capture(allocator, &ref_q);
    defer ref.deinit();

    var z_q = (try exec.zonemapTopN(allocator, t, null, probe, pred, specs, out, 5, 0)).?;
    defer z_q.deinit();
    var got = try capture(allocator, &z_q);
    defer got.deinit();

    try std.testing.expectEqual(@as(usize, 0), ref.rows.items.len);
    try expectSameRows(ref, got);
}

test "zonemap all RGs pruned after the first (disjoint ascending ranges)" {
    // Each flushed group has a disjoint, increasing k1 range, so after the
    // first row group fills the heap every later RG's corner is worse.
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const schema = types.TableSchema{
        .columns = &.{
            .{ .name = "k1", .type = .int },
            .{ .name = "w", .type = .bigint },
            .{ .name = "tag", .type = .string },
        },
        .order_key = &.{"k1"},
        .unique = false,
    };
    var db = try api.Database.open(allocator, io, tmp.dir, .{
        .row_group_size = 4,
        .auto_flush_rows = std.math.maxInt(u64),
        .auto_flush_bytes = std.math.maxInt(u64),
    });
    defer db.close();
    const t = try db.table("t", schema, .{ .order_key = &.{"k1"}, .row_group_size = 4 });

    // 3 segments, each strictly increasing and non-overlapping.
    var base: i32 = 0;
    for (0..3) |_| {
        var rows: [8]struct { k1: i32, w: i64, tag: []const u8 } = undefined;
        for (0..8) |i| rows[i] = .{ .k1 = base + @as(i32, @intCast(i)), .w = @intCast(base + @as(i32, @intCast(i))), .tag = "x" };
        try t.insert(&rows);
        try t.flush();
        base += 100;
    }

    const probe = &[_][]const u8{"k1"};
    const specs = &[_]SortSpec{.{ .col = "k1", .desc = false }};
    const out = &[_][]const u8{ "k1", "w", "tag" };

    var ref_q = try exec.lateScan(allocator, t, null, probe, .{ .always = true }, specs, out, 3, 0);
    defer ref_q.deinit();
    var ref = try capture(allocator, &ref_q);
    defer ref.deinit();

    var z_q = (try exec.zonemapTopN(allocator, t, null, probe, .{ .always = true }, specs, out, 3, 0)).?;
    defer z_q.deinit();
    var got = try capture(allocator, &z_q);
    defer got.deinit();

    try expectSameRows(ref, got);
    // Sanity: top-3 ascending are k1 = 0,1,2.
    try std.testing.expectEqual(@as(usize, 3), got.rows.items.len);
    try std.testing.expectEqual(@as(i128, 0), got.rows.items[0].cells[0].int);
    try std.testing.expectEqual(@as(i128, 2), got.rows.items[2].cells[0].int);
}

test "zonemap NO RGs pruned (heavily overlapping ranges)" {
    try runScenario(.{
        .lead_type = .int,
        .ties = true, // k1 in [0,5], so every RG range overlaps
        .n = 10,
        .flush_groups = &.{ 8, 8, 8, 8 },
        .memtable_rows = 7,
        .seed = 0xABCD,
    });
}

test "zonemap memtable-only (no flushed segments)" {
    try runScenario(.{
        .lead_type = .int,
        .second_type = .string,
        .n = 5,
        .flush_groups = &.{},
        .memtable_rows = 12,
        .seed = 0xBEEF,
    });
}

// --- Fall-back cases: zonemapTopN must return null; lateScan still correct. ---

test "zonemap fallback: string leading key returns null" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const schema = types.TableSchema{
        .columns = &.{
            .{ .name = "s", .type = .string },
            .{ .name = "w", .type = .bigint },
        },
        .order_key = &.{"s"},
        .unique = false,
    };
    var db = try api.Database.open(allocator, io, tmp.dir, .{ .row_group_size = 4 });
    defer db.close();
    const t = try db.table("t", schema, .{ .order_key = &.{"s"}, .row_group_size = 4 });
    try t.insert(&.{
        .{ .s = @as([]const u8, "c"), .w = @as(i64, 3) },
        .{ .s = @as([]const u8, "a"), .w = @as(i64, 1) },
        .{ .s = @as([]const u8, "b"), .w = @as(i64, 2) },
    });
    try t.flush();

    const probe = &[_][]const u8{"s"};
    const specs = &[_]SortSpec{.{ .col = "s", .desc = false }};
    const out = &[_][]const u8{ "s", "w" };

    const z = try exec.zonemapTopN(allocator, t, null, probe, .{ .always = true }, specs, out, 2, 0);
    try std.testing.expect(z == null);
}

test "zonemap fallback: float leading key returns null" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const schema = types.TableSchema{
        .columns = &.{
            .{ .name = "f", .type = .float },
            .{ .name = "w", .type = .bigint },
        },
        .order_key = &.{"f"},
        .unique = false,
    };
    var db = try api.Database.open(allocator, io, tmp.dir, .{ .row_group_size = 4 });
    defer db.close();
    const t = try db.table("t", schema, .{ .order_key = &.{"f"}, .row_group_size = 4 });
    try t.insert(&.{
        .{ .f = @as(f32, 3.0), .w = @as(i64, 3) },
        .{ .f = @as(f32, 1.0), .w = @as(i64, 1) },
    });
    try t.flush();

    const probe = &[_][]const u8{"f"};
    const specs = &[_]SortSpec{.{ .col = "f", .desc = false }};
    const out = &[_][]const u8{ "f", "w" };

    const z = try exec.zonemapTopN(allocator, t, null, probe, .{ .always = true }, specs, out, 2, 0);
    try std.testing.expect(z == null);
}

test "zonemap fallback: nullable leading key returns null" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const schema = types.TableSchema{
        .columns = &.{
            .{ .name = "k1", .type = .int, .nullable = true },
            .{ .name = "w", .type = .bigint },
        },
        .order_key = &.{"k1"},
        .unique = false,
    };
    var db = try api.Database.open(allocator, io, tmp.dir, .{ .row_group_size = 4 });
    defer db.close();
    const t = try db.table("t", schema, .{ .order_key = &.{"k1"}, .row_group_size = 4 });
    try t.insert(&.{
        .{ .k1 = @as(i32, 2), .w = @as(i64, 2) },
        .{ .k1 = @as(i32, 1), .w = @as(i64, 1) },
    });
    try t.flush();

    const probe = &[_][]const u8{"k1"};
    const specs = &[_]SortSpec{.{ .col = "k1", .desc = false }};
    const out = &[_][]const u8{ "k1", "w" };

    const z = try exec.zonemapTopN(allocator, t, null, probe, .{ .always = true }, specs, out, 2, 0);
    try std.testing.expect(z == null);
}
