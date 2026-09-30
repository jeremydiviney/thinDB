//! Narrow MAX_BY / MAX_BY_KEY / ANY_VALUE / FIRST accumulator cells: the
//! compact i64-keyed cells must agree with the wide AccState path (the same
//! aggregate ordered by a DOUBLE key) and with a brute-force walk of the
//! input, across batch boundaries, NULL keys / payloads, tied keys, and
//! all-NULL groups. The source rebuilds every batch into scratch stores that
//! the next batch overwrites, so a retained payload that aliased batch bytes
//! would surface as a corrupted string.
const std = @import("std");
const types = @import("../types.zig");
const Column = types.Column;
const storage = @import("../storage/storage.zig");
const ColumnView = storage.ColumnView;
const engine = @import("../engine/engine.zig");
const ColumnStore = engine.ColumnStore;
const transform = @import("../engine/transform.zig");
const exec = @import("exec.zig");
const Batch = exec.Batch;
const AggSpec = exec.AggSpec;

const ROWS: usize = 60;
const GROUPS: usize = 5;
const BATCH_ROWS: usize = 7;
const N_COLS: usize = 6;

const schema = [_]Column{
    .{ .name = "g", .type = .int, .nullable = false },
    .{ .name = "ord", .type = .bigint, .nullable = true },
    .{ .name = "ordf", .type = .double, .nullable = true },
    .{ .name = "lbl", .type = .string, .nullable = true },
    .{ .name = "v_i", .type = .int, .nullable = true },
    .{ .name = "a_str", .type = .string, .nullable = true },
};

fn groupOf(i: usize) usize {
    return i % GROUPS;
}

/// Order key: ties within a group, NULL on every ninth row, NULL for the
/// whole of group 4 (so its MAX_BY output is NULL).
fn ordOf(i: usize) ?i64 {
    if (i % 9 == 0 or groupOf(i) == 4) return null;
    return @intCast((i * 7) % 11);
}

fn lblPresent(i: usize) bool {
    return i % 13 != 0;
}

fn viOf(i: usize) ?i32 {
    if (i % 6 == 0) return null;
    return @intCast(i * 3);
}

/// NULL on every fourth row and for the whole of group 3 (so its
/// ANY_VALUE / FIRST output is NULL).
fn aStrPresent(i: usize) bool {
    return i % 4 != 0 and groupOf(i) != 3;
}

const Expected = struct {
    /// Winning input row of MAX_BY(lbl, ord) / MAX_BY_KEY(lbl, ord).
    mb_row: ?usize = null,
    /// Winning input row of MAX_BY(v_i, ord).
    mbi_row: ?usize = null,
    /// First row with a non-NULL a_str / v_i.
    av_str_row: ?usize = null,
    av_i_row: ?usize = null,
};

fn bruteForce() [GROUPS]Expected {
    var out = [_]Expected{.{}} ** GROUPS;
    for (0..ROWS) |i| {
        const e = &out[groupOf(i)];
        if (ordOf(i)) |k| {
            if (lblPresent(i) and (e.mb_row == null or k > ordOf(e.mb_row.?).?)) e.mb_row = i;
            if (viOf(i) != null and (e.mbi_row == null or k > ordOf(e.mbi_row.?).?)) e.mbi_row = i;
        }
        if (e.av_str_row == null and aStrPresent(i)) e.av_str_row = i;
        if (e.av_i_row == null and viOf(i) != null) e.av_i_row = i;
    }
    return out;
}

fn appendI32(a: std.mem.Allocator, c: *ColumnStore, row: usize, v: ?i32) !void {
    try c.appendValidBit(a, row, v != null);
    if (v) |x| try c.data.int.append(a, x) else try c.data.appendNullPlaceholder(a);
}

fn appendI64(a: std.mem.Allocator, c: *ColumnStore, row: usize, v: ?i64) !void {
    try c.appendValidBit(a, row, v != null);
    if (v) |x| try c.data.bigint.append(a, x) else try c.data.appendNullPlaceholder(a);
}

fn appendF64(a: std.mem.Allocator, c: *ColumnStore, row: usize, v: ?f64) !void {
    try c.appendValidBit(a, row, v != null);
    if (v) |x| try c.data.double.append(a, x) else try c.data.appendNullPlaceholder(a);
}

fn appendStr(a: std.mem.Allocator, c: *ColumnStore, row: usize, v: ?[]const u8) !void {
    try c.appendValidBit(a, row, v != null);
    if (v) |s| try c.data.string.appendValue(a, s) else try c.data.appendNullPlaceholder(a);
}

fn buildFixture(a: std.mem.Allocator) ![N_COLS]ColumnStore {
    var cols: [N_COLS]ColumnStore = undefined;
    var inited: usize = 0;
    errdefer for (cols[0..inited]) |*c| c.deinit(a);
    for (&cols, schema) |*c, col| {
        c.* = try ColumnStore.init(a, col.type, col.nullable);
        inited += 1;
    }
    var buf: [32]u8 = undefined;
    for (0..ROWS) |i| {
        try appendI32(a, &cols[0], i, @intCast(groupOf(i)));
        try appendI64(a, &cols[1], i, ordOf(i));
        try appendF64(a, &cols[2], i, if (ordOf(i)) |k| @as(f64, @floatFromInt(k)) else null);
        try appendStr(a, &cols[3], i, if (lblPresent(i)) try std.fmt.bufPrint(&buf, "lbl-{d}", .{i}) else null);
        try appendI32(a, &cols[4], i, viOf(i));
        try appendStr(a, &cols[5], i, if (aStrPresent(i)) try std.fmt.bufPrint(&buf, "a-{d}", .{i}) else null);
    }
    return cols;
}

/// Re-emits the fixture in `BATCH_ROWS`-row batches, each rebuilt into the
/// same scratch stores (so an earlier batch's bytes are gone once the next
/// one is produced).
const Source = struct {
    allocator: std.mem.Allocator,
    full: *const [N_COLS]ColumnStore,
    scratch: [N_COLS]ColumnStore,
    views: [N_COLS]ColumnView = undefined,
    pos: usize = 0,

    fn create(a: std.mem.Allocator, full: *const [N_COLS]ColumnStore) !*Source {
        const self = try a.create(Source);
        errdefer a.destroy(self);
        self.* = .{ .allocator = a, .full = full, .scratch = undefined };
        var inited: usize = 0;
        errdefer for (self.scratch[0..inited]) |*c| c.deinit(a);
        for (&self.scratch, schema) |*c, col| {
            c.* = try ColumnStore.init(a, col.type, col.nullable);
            inited += 1;
        }
        return self;
    }

    pub fn next(self: *Source) !?Batch {
        if (self.pos >= ROWS) return null;
        const lo = self.pos;
        const hi = @min(ROWS, lo + BATCH_ROWS);
        for (&self.scratch, self.full, &self.views) |*dst, src, *v| {
            dst.clear();
            try transform.appendColumnRange(self.allocator, src.view(), lo, hi, dst);
            v.* = dst.view();
        }
        self.pos = hi;
        return .{ .schema = schema[0..], .values = self.views[0..], .row_count = hi - lo };
    }

    pub fn deinit(self: *Source) void {
        for (&self.scratch) |*c| c.deinit(self.allocator);
        self.allocator.destroy(self);
    }

    pub fn outputSchema(_: *Source) []const Column {
        return schema[0..];
    }

    pub fn addPrune(_: *Source, _: exec.Predicate) !void {}

    pub fn stats(_: *Source) exec.PipelineStats {
        return .{ .upper_rows = ROWS };
    }

    pub fn accountant(_: *Source) ?*exec.memory.MemoryAccountant {
        return null;
    }

    pub fn explain(_: *Source, _: *std.ArrayList(u8), _: std.mem.Allocator, _: usize) !void {}
};

fn expectStrCell(view: ColumnView, r: usize, want_row: ?usize, comptime prefix: []const u8) !void {
    if (want_row) |i| {
        try std.testing.expect(view.isValid(r));
        var buf: [32]u8 = undefined;
        const want = try std.fmt.bufPrint(&buf, prefix ++ "{d}", .{i});
        try std.testing.expectEqualStrings(want, view.data.string.rowBytes(r));
    } else {
        try std.testing.expect(!view.isValid(r));
    }
}

fn expectI32Cell(view: ColumnView, r: usize, want: ?i32) !void {
    if (want) |w| {
        try std.testing.expect(view.isValid(r));
        try std.testing.expectEqual(w, view.data.int[r]);
    } else {
        try std.testing.expect(!view.isValid(r));
    }
}

fn expectI64Cell(view: ColumnView, r: usize, want: ?i64) !void {
    if (want) |w| {
        try std.testing.expect(view.isValid(r));
        try std.testing.expectEqual(w, view.data.bigint[r]);
    } else {
        try std.testing.expect(!view.isValid(r));
    }
}

fn expectF64Cell(view: ColumnView, r: usize, want: ?i64) !void {
    if (want) |w| {
        try std.testing.expect(view.isValid(r));
        try std.testing.expectEqual(@as(f64, @floatFromInt(w)), view.data.double[r]);
    } else {
        try std.testing.expect(!view.isValid(r));
    }
}

test "narrow MAX_BY / MAX_BY_KEY / ANY_VALUE / FIRST cells match the wide path and brute force" {
    const a = std.testing.allocator;
    var full = try buildFixture(a);
    defer for (&full) |*c| c.deinit(a);

    const aggs = [_]AggSpec{
        .{ .func = .max_by, .col = "lbl", .arg2_col = "ord", .as = "mb_narrow" },
        .{ .func = .max_by, .col = "lbl", .arg2_col = "ordf", .as = "mb_wide" },
        .{ .func = .max_by_key, .col = "lbl", .arg2_col = "ord", .as = "mbk_narrow", .out_type_override = .bigint },
        .{ .func = .max_by_key, .col = "lbl", .arg2_col = "ordf", .as = "mbk_wide", .out_type_override = .double },
        .{ .func = .max_by, .col = "v_i", .arg2_col = "ord", .as = "mbi_narrow" },
        .{ .func = .max_by, .col = "v_i", .arg2_col = "ordf", .as = "mbi_wide" },
        .{ .func = .any_value, .col = "a_str", .as = "av_str" },
        .{ .func = .any_value, .col = "v_i", .as = "av_i" },
        .{ .func = .first, .col = "a_str", .as = "first_str" },
    };
    const group_cols = [_][]const u8{"g"};
    const expected = bruteForce();

    const src = try Source.create(a, &full);
    const q = exec.makeQuery(a, src);
    var agg = q.groupBy(&group_cols, &aggs) catch |e| {
        var qq = q;
        qq.deinit();
        return e;
    };
    defer agg.deinit();

    var groups_seen: usize = 0;
    while (try agg.next()) |b| {
        for (0..b.row_count) |r| {
            const g: usize = @intCast(b.values[0].data.int[r]);
            const ex = expected[g];
            const mb_key: ?i64 = if (ex.mb_row) |i| ordOf(i).? else null;
            try expectStrCell(b.values[1], r, ex.mb_row, "lbl-");
            try expectStrCell(b.values[2], r, ex.mb_row, "lbl-");
            try expectI64Cell(b.values[3], r, mb_key);
            try expectF64Cell(b.values[4], r, mb_key);
            const mbi_val: ?i32 = if (ex.mbi_row) |i| viOf(i).? else null;
            try expectI32Cell(b.values[5], r, mbi_val);
            try expectI32Cell(b.values[6], r, mbi_val);
            try expectStrCell(b.values[7], r, ex.av_str_row, "a-");
            const av_i: ?i32 = if (ex.av_i_row) |i| viOf(i).? else null;
            try expectI32Cell(b.values[8], r, av_i);
            try expectStrCell(b.values[9], r, ex.av_str_row, "a-");
            groups_seen += 1;
        }
    }
    try std.testing.expectEqual(GROUPS, groups_seen);
}

const improving_schema = [_]Column{
    .{ .name = "g", .type = .int, .nullable = false },
    .{ .name = "run", .type = .int, .nullable = false },
    .{ .name = "o", .type = .bigint, .nullable = false },
    .{ .name = "f", .type = .double, .nullable = false },
    .{ .name = "up", .type = .string, .nullable = false },
    .{ .name = "down", .type = .string, .nullable = false },
};

const IMPROVING_GROUPS: usize = 38;
const IMPROVING_RUNS: usize = 4;
const IMPROVING_BATCH: usize = 4096;

/// Row `i` of `rows` rows, with every order key and `up` ascending and
/// `down` descending, so each row improves its group's MAX_BY, MAX, MIN and
/// LAST. The strings' lengths wander between 9 and 57 bytes, so a group's
/// successive copies change size.
fn improvingString(buf: []u8, n: usize, i: usize) ![]const u8 {
    const pad = "x" ** 48;
    return std.fmt.bufPrint(buf, "{d:0>9}{s}", .{ n, pad[0 .. (i * 7) % 49] });
}

const ImprovingSource = struct {
    allocator: std.mem.Allocator,
    rows: usize,
    scratch: [improving_schema.len]ColumnStore,
    views: [improving_schema.len]ColumnView = undefined,
    pos: usize = 0,

    fn init(a: std.mem.Allocator, rows: usize) !ImprovingSource {
        var self = ImprovingSource{ .allocator = a, .rows = rows, .scratch = undefined };
        var inited: usize = 0;
        errdefer for (self.scratch[0..inited]) |*c| c.deinit(a);
        for (&self.scratch, improving_schema) |*c, col| {
            c.* = try ColumnStore.init(a, col.type, col.nullable);
            inited += 1;
        }
        return self;
    }

    pub fn next(self: *ImprovingSource) !?Batch {
        if (self.pos >= self.rows) return null;
        const hi = @min(self.rows, self.pos + IMPROVING_BATCH);
        for (&self.scratch) |*c| c.clear();
        var buf: [64]u8 = undefined;
        for (self.pos..hi) |i| {
            try self.scratch[0].data.int.append(self.allocator, @intCast(i % IMPROVING_GROUPS));
            try self.scratch[1].data.int.append(self.allocator, @intCast(i * IMPROVING_RUNS / self.rows));
            try self.scratch[2].data.bigint.append(self.allocator, @intCast(i));
            try self.scratch[3].data.double.append(self.allocator, @floatFromInt(i));
            try self.scratch[4].data.string.appendValue(self.allocator, try improvingString(&buf, i, i));
            try self.scratch[5].data.string.appendValue(self.allocator, try improvingString(&buf, self.rows - i, i));
        }
        for (&self.scratch, &self.views) |*c, *v| v.* = c.view();
        const n = hi - self.pos;
        self.pos = hi;
        return .{ .schema = improving_schema[0..], .values = self.views[0..], .row_count = n };
    }

    pub fn deinit(self: *ImprovingSource) void {
        for (&self.scratch) |*c| c.deinit(self.allocator);
    }

    pub fn outputSchema(_: *ImprovingSource) []const Column {
        return improving_schema[0..];
    }

    pub fn addPrune(_: *ImprovingSource, _: exec.Predicate) !void {}

    pub fn stats(self: *ImprovingSource) exec.PipelineStats {
        return .{ .upper_rows = self.rows };
    }

    pub fn accountant(_: *ImprovingSource) ?*exec.memory.MemoryAccountant {
        return null;
    }

    pub fn explain(_: *ImprovingSource, _: *std.ArrayList(u8), _: std.mem.Allocator, _: usize) !void {}
};

const improving_aggs = [_]AggSpec{
    .{ .func = .max_by, .col = "up", .arg2_col = "o", .as = "mb_narrow" },
    .{ .func = .max_by, .col = "up", .arg2_col = "f", .as = "mb_wide" },
    .{ .func = .max_by, .col = "o", .arg2_col = "up", .as = "mb_str_key" },
    .{ .func = .max, .col = "up", .as = "mx" },
    .{ .func = .min, .col = "down", .as = "mn" },
    .{ .func = .last, .col = "up", .as = "lst" },
};

const ImprovingShape = enum { grouped, single, sorted };

/// Runs `improving_aggs` over `rows` improving rows in the given shape,
/// checks every group's result is its last row's, and returns the
/// aggregate's accounted peak.
fn improvingPeak(a: std.mem.Allocator, rows: usize, shape: ImprovingShape) !usize {
    const account = try a.create(exec.memory.MemoryAccountant);
    account.* = exec.memory.MemoryAccountant.initWithPool(1 << 40, null);
    account.trackAllocations(a);
    defer account.releaseOwner(a);
    const tracked = try account.executionAllocator();

    var src = try ImprovingSource.init(a, rows);
    const q = exec.makeQuery(tracked, &src);
    const made = switch (shape) {
        .grouped => q.groupBy(&.{"g"}, &improving_aggs),
        .single => q.groupBy(&.{}, &improving_aggs),
        .sorted => q.streamGroupBy(&.{"run"}, &improving_aggs),
    };
    var agg = made catch |e| {
        src.deinit();
        return e;
    };
    defer agg.deinit();

    const n_keys: usize = if (shape == .single) 0 else 1;
    var seen: usize = 0;
    var buf: [64]u8 = undefined;
    while (try agg.next()) |b| {
        for (0..b.row_count) |r| {
            const last = switch (shape) {
                .grouped => blk: {
                    const g: usize = @intCast(b.values[0].data.int[r]);
                    break :blk rows - 1 - (rows - 1 + IMPROVING_GROUPS - g) % IMPROVING_GROUPS;
                },
                .single => rows - 1,
                .sorted => blk: {
                    const run: usize = @intCast(b.values[0].data.int[r]);
                    break :blk ((run + 1) * rows - 1) / IMPROVING_RUNS;
                },
            };
            const up = try improvingString(&buf, last, last);
            for ([_]usize{ 0, 1, 3, 5 }) |ai| try std.testing.expectEqualStrings(up, b.values[n_keys + ai].data.string.rowBytes(r));
            try std.testing.expectEqual(@as(i64, @intCast(last)), b.values[n_keys + 2].data.bigint[r]);
            const down = try improvingString(&buf, rows - last, last);
            try std.testing.expectEqualStrings(down, b.values[n_keys + 4].data.string.rowBytes(r));
            seen += 1;
        }
    }
    const want_groups: usize = switch (shape) {
        .grouped => IMPROVING_GROUPS,
        .single => 1,
        .sorted => IMPROVING_RUNS,
    };
    try std.testing.expectEqual(want_groups, seen);
    return account.peak_bytes;
}

test "MAX_BY, MIN/MAX and LAST keep a state bounded by the groups when every row improves them" {
    const a = std.testing.allocator;
    // A copy kept per improving row would add about 30 MB over the extra
    // 150K rows.
    for ([_]ImprovingShape{ .grouped, .single, .sorted }) |shape| {
        const small = try improvingPeak(a, 50_000, shape);
        const large = try improvingPeak(a, 200_000, shape);
        try std.testing.expect(large < small + (1 << 20));
    }
}
