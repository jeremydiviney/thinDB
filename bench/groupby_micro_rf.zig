//! Rollforward-shaped partial aggregate: a wide GROUP BY over a compound
//! (int, string, date) key with MAX_BY(string, int), ANY_VALUE(string), SUM
//! and MAX aggregates over near-unique groups (~3 rows per group) — the
//! per-worker partial aggregate the profiler sees at ~1 µs/row on the
//! rollforward. Drives the REAL exec.Aggregate over an in-memory source, one
//! fresh operator per repeat (the worker instance's create + drain is what
//! the query pays). Experiments subtract aggregate families from the full
//! shape so the deltas isolate each family's per-row cost.
//!
//! Run:  zig build gbmicro -Doptimize=ReleaseFast -- rf

const std = @import("std");
const win = std.os.windows;
const thindb = @import("thindb");
const texec = thindb.exec;
const Column = thindb.types.Column;
const ColumnView = thindb.storage.ColumnView;

const ROWS: usize = 17_000;
const GROUPS: usize = 5_800;
const BATCH: usize = 4096;
const REPEAT: usize = 20;
const N_MAXBY: usize = 9;
const N_ANY: usize = 3;
const N_SUM: usize = 10;
const N_MAX: usize = 3;

fn nowTicks() i64 {
    var c: win.LARGE_INTEGER = 0;
    _ = win.ntdll.RtlQueryPerformanceCounter(&c);
    return c;
}

fn scatter(i: u64) u64 {
    var z = (i +% 1) *% 0x9E3779B97F4A7C15;
    z = (z ^ (z >> 30)) *% 0xBF58476D1CE4E5B9;
    z = (z ^ (z >> 27)) *% 0x94D049BB133111EB;
    return z ^ (z >> 31);
}

const StrCol = struct {
    offsets: []u32,
    bytes: []u8,

    fn build(a: std.mem.Allocator, comptime fmt: []const u8, salt: u64) !StrCol {
        var offsets: std.ArrayList(u32) = .empty;
        errdefer offsets.deinit(a);
        var bytes: std.ArrayList(u8) = .empty;
        errdefer bytes.deinit(a);
        try offsets.append(a, 0);
        var buf: [64]u8 = undefined;
        for (0..ROWS) |i| {
            const g = scatter(i) % GROUPS;
            const s = try std.fmt.bufPrint(&buf, fmt, .{ g, scatter(i +% salt) % 100 });
            try bytes.appendSlice(a, s);
            try offsets.append(a, @intCast(bytes.items.len));
        }
        return .{ .offsets = try offsets.toOwnedSlice(a), .bytes = try bytes.toOwnedSlice(a) };
    }

    fn free(self: StrCol, a: std.mem.Allocator) void {
        a.free(self.offsets);
        a.free(self.bytes);
    }
};

const Data = struct {
    project: []i32,
    cust: StrCol,
    month: []i32,
    order_no: []i32,
    maxby: [N_MAXBY]StrCol,
    any: [N_ANY]StrCol,
    sums: [N_SUM][]i64,
    maxes: [N_MAX][]i32,

    fn gen(a: std.mem.Allocator) !Data {
        var d: Data = undefined;
        d.project = try a.alloc(i32, ROWS);
        d.month = try a.alloc(i32, ROWS);
        d.order_no = try a.alloc(i32, ROWS);
        for (0..ROWS) |i| {
            const g = scatter(i) % GROUPS;
            d.project[i] = 1000073;
            d.month[i] = 20000 + @as(i32, @intCast(g % 3));
            d.order_no[i] = @intCast(scatter(i *% 3) % 1_000_000);
        }
        // The key string varies only with the group; the second argument is
        // consumed as a zero-width field so one format serves every column.
        d.cust = try StrCol.build(a, "CUST-{d:0>7}{d:0>0}", 0);
        for (&d.maxby, 0..) |*c, k| c.* = try StrCol.build(a, "value-{d}-{d}", 11 + k);
        for (&d.any, 0..) |*c, k| c.* = try StrCol.build(a, "attr-{d}-{d}", 31 + k);
        for (&d.sums, 0..) |*c, k| {
            c.* = try a.alloc(i64, ROWS);
            for (c.*, 0..) |*v, i| v.* = @intCast(scatter(i +% 101 *% (k + 1)) % 10_000);
        }
        for (&d.maxes, 0..) |*c, k| {
            c.* = try a.alloc(i32, ROWS);
            for (c.*, 0..) |*v, i| v.* = 20000 + @as(i32, @intCast(scatter(i +% 211 *% (k + 1)) % 1000));
        }
        return d;
    }

    fn free(d: Data, a: std.mem.Allocator) void {
        a.free(d.project);
        a.free(d.month);
        a.free(d.order_no);
        d.cust.free(a);
        for (d.maxby) |c| c.free(a);
        for (d.any) |c| c.free(a);
        for (d.sums) |c| a.free(c);
        for (d.maxes) |c| a.free(c);
    }
};

const N_COLS: usize = 4 + N_MAXBY + N_ANY + N_SUM + N_MAX;

fn colName(comptime prefix: []const u8, comptime k: usize) []const u8 {
    return std.fmt.comptimePrint("{s}{d}", .{ prefix, k });
}

const schema: [N_COLS]Column = blk: {
    @setEvalBranchQuota(20_000);
    var cols: [N_COLS]Column = undefined;
    cols[0] = .{ .name = "projectId", .type = .int };
    cols[1] = .{ .name = "customerNumber", .type = .string };
    cols[2] = .{ .name = "month", .type = .date };
    cols[3] = .{ .name = "groupOrderNumber", .type = .int };
    var c: usize = 4;
    for (0..N_MAXBY) |k| {
        cols[c] = .{ .name = colName("s", k), .type = .string };
        c += 1;
    }
    for (0..N_ANY) |k| {
        cols[c] = .{ .name = colName("a", k), .type = .string };
        c += 1;
    }
    for (0..N_SUM) |k| {
        cols[c] = .{ .name = colName("n", k), .type = .bigint };
        c += 1;
    }
    for (0..N_MAX) |k| {
        cols[c] = .{ .name = colName("d", k), .type = .date };
        c += 1;
    }
    break :blk cols;
};

/// Synthetic upstream: borrows the generated columns and re-emits them as
/// BATCH-row view slices; reports only the row bound (no column NDVs), like a
/// fused probe chunk feeding a worker's partial aggregate.
const Source = struct {
    d: *const Data,
    pos: usize = 0,
    views: [N_COLS]ColumnView = undefined,
    allocator: std.mem.Allocator,

    fn create(a: std.mem.Allocator, d: *const Data) !*Source {
        const self = try a.create(Source);
        self.* = .{ .d = d, .allocator = a };
        return self;
    }

    fn strSlice(c: StrCol, lo: usize, hi: usize) ColumnView {
        return .{ .data = .{ .string = .{ .offsets = c.offsets[lo .. hi + 1], .bytes = c.bytes } } };
    }

    pub fn next(self: *Source) !?texec.Batch {
        if (self.pos >= ROWS) return null;
        const lo = self.pos;
        const hi = @min(ROWS, lo + BATCH);
        const d = self.d;
        self.views[0] = .{ .data = .{ .int = d.project[lo..hi] } };
        self.views[1] = strSlice(d.cust, lo, hi);
        self.views[2] = .{ .data = .{ .date = d.month[lo..hi] } };
        self.views[3] = .{ .data = .{ .int = d.order_no[lo..hi] } };
        var c: usize = 4;
        for (d.maxby) |col| {
            self.views[c] = strSlice(col, lo, hi);
            c += 1;
        }
        for (d.any) |col| {
            self.views[c] = strSlice(col, lo, hi);
            c += 1;
        }
        for (d.sums) |col| {
            self.views[c] = .{ .data = .{ .bigint = col[lo..hi] } };
            c += 1;
        }
        for (d.maxes) |col| {
            self.views[c] = .{ .data = .{ .date = col[lo..hi] } };
            c += 1;
        }
        self.pos = hi;
        return .{ .schema = schema[0..], .values = self.views[0..], .row_count = hi - lo };
    }
    pub fn deinit(self: *Source) void {
        self.allocator.destroy(self);
    }
    pub fn outputSchema(_: *Source) []const Column {
        return schema[0..];
    }
    pub fn addPrune(_: *Source, _: texec.Predicate) !void {}
    pub fn stats(_: *Source) texec.PipelineStats {
        return .{ .upper_rows = ROWS };
    }
    pub fn accountant(_: *Source) ?*texec.memory.MemoryAccountant {
        return null;
    }
    pub fn explain(_: *Source, _: *std.ArrayList(u8), _: std.mem.Allocator, _: usize) !void {}
};

const group_cols = [_][]const u8{ "projectId", "customerNumber", "month" };

const count_agg = [_]texec.AggSpec{.{ .func = .count, .col = null, .as = "cnt" }};

fn maxBySpecs() [N_MAXBY]texec.AggSpec {
    var out: [N_MAXBY]texec.AggSpec = undefined;
    inline for (0..N_MAXBY) |k| out[k] = .{ .func = .max_by, .col = colName("s", k), .arg2_col = "groupOrderNumber", .as = colName("s", k) };
    return out;
}
fn anySpecs() [N_ANY]texec.AggSpec {
    var out: [N_ANY]texec.AggSpec = undefined;
    inline for (0..N_ANY) |k| out[k] = .{ .func = .any_value, .col = colName("a", k), .as = colName("a", k) };
    return out;
}
fn sumSpecs() [N_SUM]texec.AggSpec {
    var out: [N_SUM]texec.AggSpec = undefined;
    inline for (0..N_SUM) |k| out[k] = .{ .func = .sum, .col = colName("n", k), .as = colName("n", k) };
    return out;
}
fn maxSpecs() [N_MAX]texec.AggSpec {
    var out: [N_MAX]texec.AggSpec = undefined;
    inline for (0..N_MAX) |k| out[k] = .{ .func = .max, .col = colName("d", k), .as = colName("d", k) };
    return out;
}

const maxby_specs = maxBySpecs();
const any_specs = anySpecs();
const sum_specs = sumSpecs();
const max_specs = maxSpecs();
const full_specs = count_agg ++ maxby_specs ++ any_specs ++ sum_specs ++ max_specs;

const Result = struct { ticks: i64, cksum: u64 };

/// One operator lifetime: create, drain, checksum the output (row count plus
/// a hash of the first aggregate column's bytes when it is a string).
fn runAgg(a: std.mem.Allocator, d: *const Data, aggs: []const texec.AggSpec) !Result {
    const src = try Source.create(a, d);
    const q = texec.makeQuery(a, src);
    const t0 = nowTicks();
    var agg = q.groupBy(&group_cols, aggs) catch |e| {
        var qq = q;
        qq.deinit();
        return e;
    };
    defer agg.deinit();
    var ck: u64 = 0;
    while (try agg.next()) |b| {
        ck +%= b.row_count;
        const first = b.values[group_cols.len];
        switch (first.data) {
            .string, .varchar => |sv| ck ^= std.hash.Wyhash.hash(0, sv.bytes[sv.offsets[0]..sv.offsets[b.row_count]]),
            .bigint => |s| for (s[0..b.row_count]) |v| {
                ck ^= @as(u64, @bitCast(v));
            },
            else => {},
        }
    }
    return .{ .ticks = nowTicks() - t0, .cksum = ck };
}

const Experiment = struct { name: []const u8, aggs: []const texec.AggSpec };

const experiments = [_]Experiment{
    .{ .name = "full: 9 MAX_BY(str) 3 ANY_VALUE 10 SUM 3 MAX", .aggs = &full_specs },
    .{ .name = "COUNT(*) only (key build+probe)", .aggs = &count_agg },
    .{ .name = "COUNT + 9 MAX_BY(str, int)", .aggs = &(count_agg ++ maxby_specs) },
    .{ .name = "COUNT + 3 ANY_VALUE(str)", .aggs = &(count_agg ++ any_specs) },
    .{ .name = "COUNT + 10 SUM(bigint)", .aggs = &(count_agg ++ sum_specs) },
    .{ .name = "COUNT + 3 MAX(date)", .aggs = &(count_agg ++ max_specs) },
};

pub fn run(a: std.mem.Allocator, hz: f64) !void {
    std.debug.print("\n=== RF  (projectId, customerNumber, month) wide partial aggregate   ({d} rows, {d} groups, {d}-row batches, fresh operator per repeat) ===\n", .{ ROWS, GROUPS, BATCH });
    std.debug.print("{s:<44} {s:>9} {s:>9}  {s}\n", .{ "experiment", "ns/row", "ms/call", "cksum" });
    std.debug.print("{s}\n", .{"-" ** 78});
    const d = try Data.gen(a);
    defer d.free(a);
    for (experiments) |e| {
        var best: i64 = std.math.maxInt(i64);
        var ck: u64 = 0;
        for (0..REPEAT) |_| {
            const r = try runAgg(a, &d, e.aggs);
            if (r.ticks < best) best = r.ticks;
            ck = r.cksum;
        }
        const ms: f64 = @as(f64, @floatFromInt(best)) * 1000.0 / hz;
        const per: f64 = ms * 1e6 / @as(f64, @floatFromInt(ROWS));
        std.debug.print("{s:<44} {d:>8.1} {d:>8.2}  {x}\n", .{ e.name, per, ms, ck });
    }
}
