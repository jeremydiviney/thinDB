//! Expand — the row source behind `generate_series(start, stop[, step])` and
//! `unnest(array, ...)`. Each upstream row expands into one row per value it
//! generates: the integers from its start column to its stop column
//! inclusive, step apart, as one BIGINT column; or its arrays' elements side
//! by side, one column per array, the shorter arrays padded with NULL. A NULL
//! series argument, a step pointing away from stop, and a NULL or empty array
//! give the row no values (PostgreSQL, DuckDB and StarRocks agree on each).
//!
//! A lateral call carries each upstream row's other columns onto the rows it
//! expands to. An outer one gives a row that expands to nothing one row
//! instead, its generated columns NULL (`LEFT JOIN unnest(...) ON TRUE`).

const std = @import("std");
const Allocator = std.mem.Allocator;

const types = @import("../types.zig");
const Column = types.Column;
const Type = types.Type;

const storage = @import("../storage/storage.zig");
const ColumnView = storage.ColumnView;

const engine = @import("../engine/engine.zig");
const ColumnStore = engine.ColumnStore;
const transform = @import("../engine/transform.zig");

const exec = @import("exec.zig");
const Query = exec.Query;
const Batch = exec.Batch;
const makeQuery = exec.makeQuery;

const nested = @import("nested.zig");

const predicate = @import("predicate.zig");
const Predicate = predicate.Predicate;

const BATCH_ROWS = 4096;

/// The position of the row an outer call gives a row that expands to
/// nothing: past the end of every array.
const PADDING: u32 = std.math.maxInt(u32);
const NO_ELEMENT: u32 = std.math.maxInt(u32);

const Error = exec.Error;

pub const Generator = enum { series, unnest };

pub const Spec = struct {
    generator: Generator,
    /// The upstream columns holding the call's arguments, in call order.
    args: []const []const u8,
    /// The generated columns' names: one for a series, one per array.
    columns: []const []const u8,
    lateral: bool = false,
    outer: bool = false,
};

pub const Expand = struct {
    allocator: Allocator,
    upstream: Query,
    upstream_done: bool = false,
    generator: Generator,
    /// Upstream column of each argument.
    args: []const usize,
    /// Upstream column of each carried output column, in output order.
    carry: []const usize,
    outer: bool,
    /// The carried columns, then the generated ones.
    schema: []Column,
    out: []ColumnStore,
    views: []ColumnView,
    /// The upstream row each output row expands from and, for unnest, its
    /// position in that row's arrays.
    src_rows: std.ArrayList(u32) = .empty,
    positions: std.ArrayList(u32) = .empty,
    elements: std.ArrayList(u32) = .empty,
    /// Each array argument's elements in the current upstream batch;
    /// `flats_live` of them hold any.
    flats: []nested.Flat,
    flats_live: usize = 0,
    /// The upstream batch being expanded and its next unread row. Its views
    /// stay valid until the next upstream pull, which comes only after
    /// every output row drawn from it has been gathered.
    batch: ?Batch = null,
    row: usize = 0,
    /// The current row's expansion: how many rows it gives, how many of
    /// them are out, and for a series its first value and step. u128 so a
    /// series spanning the whole BIGINT range still counts.
    counted: bool = false,
    count: u128 = 0,
    pos: u128 = 0,
    start: i128 = 0,
    step: i128 = 1,

    pub fn create(allocator: Allocator, upstream: Query, spec: Spec) !Query {
        const up_schema = upstream.outputSchema();
        const args = try allocator.alloc(usize, spec.args.len);
        errdefer allocator.free(args);
        for (spec.args, args) |name, *col| col.* = try argColumn(up_schema, name);
        for (args) |c| switch (spec.generator) {
            .series => if (!isInteger(up_schema[c].type)) return Error.SeriesNonIntegerArgument,
            .unnest => if (up_schema[c].type.arrayElement() == null) return Error.UnnestNonArrayArgument,
        };

        var carry: std.ArrayList(usize) = .empty;
        defer carry.deinit(allocator);
        if (spec.lateral) for (0..up_schema.len) |i| {
            if (std.mem.indexOfScalar(usize, args, i) == null) try carry.append(allocator, i);
        };
        const carry_cols = try carry.toOwnedSlice(allocator);
        errdefer allocator.free(carry_cols);

        const schema = try allocator.alloc(Column, carry_cols.len + spec.columns.len);
        errdefer allocator.free(schema);
        for (carry_cols, schema[0..carry_cols.len]) |c, *col| col.* = .{ .name = up_schema[c].name, .type = up_schema[c].type, .nullable = up_schema[c].nullable };
        const generated = schema[carry_cols.len..];
        var named: usize = 0;
        errdefer for (generated[0..named]) |col| allocator.free(col.name);
        for (spec.columns, generated, 0..) |name, *col, i| {
            col.* = switch (spec.generator) {
                .series => .{ .name = name, .type = .bigint, .nullable = spec.outer },
                .unnest => .{ .name = name, .type = up_schema[args[i]].type.arrayElement().?, .nullable = true },
            };
            col.name = try allocator.dupe(u8, name);
            named += 1;
        }

        const out = try allocator.alloc(ColumnStore, schema.len);
        errdefer allocator.free(out);
        var inited: usize = 0;
        errdefer for (out[0..inited]) |*s| s.deinit(allocator);
        for (schema, out) |col, *s| {
            s.* = try ColumnStore.init(allocator, col.type, col.nullable);
            inited += 1;
        }
        const views = try allocator.alloc(ColumnView, schema.len);
        errdefer allocator.free(views);
        const flats = try allocator.alloc(nested.Flat, if (spec.generator == .unnest) args.len else 0);
        errdefer allocator.free(flats);

        const self = try allocator.create(Expand);
        errdefer allocator.destroy(self);
        self.* = .{
            .allocator = allocator,
            .upstream = upstream,
            .generator = spec.generator,
            .args = args,
            .carry = carry_cols,
            .outer = spec.outer,
            .schema = schema,
            .out = out,
            .views = views,
            .flats = flats,
        };
        return makeQuery(allocator, self);
    }

    pub fn deinit(self: *Expand) void {
        const allocator = self.allocator;
        var up = self.upstream;
        up.deinit();
        self.freeFlats();
        allocator.free(self.flats);
        for (self.out) |*s| s.deinit(allocator);
        allocator.free(self.out);
        allocator.free(self.views);
        for (self.schema[self.carry.len..]) |col| allocator.free(col.name);
        allocator.free(self.schema);
        allocator.free(self.carry);
        allocator.free(self.args);
        self.src_rows.deinit(allocator);
        self.positions.deinit(allocator);
        self.elements.deinit(allocator);
        allocator.destroy(self);
    }

    pub fn next(self: *Expand) !?Batch {
        for (self.out) |*s| s.clear();
        self.src_rows.clearRetainingCapacity();
        self.positions.clearRetainingCapacity();
        while (true) {
            const b = self.batch orelse (try self.pull()) orelse return null;
            try self.fill(b);
            if (self.row == b.row_count) self.batch = null;
            if (self.src_rows.items.len > 0) return try self.finish(b);
        }
    }

    fn pull(self: *Expand) !?Batch {
        self.freeFlats();
        if (self.upstream_done) return null;
        const b = (try self.upstream.next()) orelse {
            self.upstream_done = true;
            return null;
        };
        const generated = self.schema[self.carry.len..];
        for (self.flats, 0..) |*flat, i| {
            flat.* = try nested.flatten(self.allocator, generated[i].type, b.values[self.args[i]], b.row_count);
            self.flats_live += 1;
        }
        self.batch = b;
        self.row = 0;
        self.counted = false;
        return b;
    }

    fn freeFlats(self: *Expand) void {
        for (self.flats[0..self.flats_live]) |*flat| flat.deinit(self.allocator);
        self.flats_live = 0;
    }

    /// Expand rows of `b` until the output batch is full or `b` runs out.
    fn fill(self: *Expand, b: Batch) !void {
        while (self.row < b.row_count and self.src_rows.items.len < BATCH_ROWS) {
            if (!self.counted) {
                self.count = try self.countAt(b, self.row);
                self.pos = 0;
                self.counted = true;
            }
            if (self.count == 0) {
                if (self.outer) try self.emitPadding();
                self.row += 1;
                self.counted = false;
                continue;
            }
            const room = BATCH_ROWS - self.src_rows.items.len;
            const take: usize = @intCast(@min(self.count - self.pos, room));
            try self.emit(take);
            self.pos += take;
            if (self.pos == self.count) {
                self.row += 1;
                self.counted = false;
            }
        }
    }

    fn countAt(self: *Expand, b: Batch, row: usize) !u128 {
        switch (self.generator) {
            .series => {
                const start = (try integerAt(b.values[self.args[0]], row)) orelse return 0;
                const stop = (try integerAt(b.values[self.args[1]], row)) orelse return 0;
                var step: i64 = 1;
                if (self.args.len == 3) step = (try integerAt(b.values[self.args[2]], row)) orelse return 0;
                if (step == 0) return Error.SeriesStepZero;
                self.start = start;
                self.step = step;
                const span = @as(i128, stop) - start;
                if ((step > 0 and span < 0) or (step < 0 and span > 0)) return 0;
                return @intCast(@divTrunc(span, step) + 1);
            },
            .unnest => {
                var longest: u32 = 0;
                for (self.flats) |flat| longest = @max(longest, flat.offsets.items[row + 1] - flat.offsets.items[row]);
                return longest;
            },
        }
    }

    /// The current row's next `take` output rows.
    fn emit(self: *Expand, take: usize) !void {
        try self.src_rows.appendNTimes(self.allocator, @intCast(self.row), take);
        switch (self.generator) {
            .series => {
                const s = &self.out[self.carry.len];
                const dst_start = s.rowCount();
                const values = &s.data.bigint;
                try values.ensureUnusedCapacity(self.allocator, take);
                var value = self.start + @as(i128, @intCast(self.pos)) * self.step;
                for (0..take) |_| {
                    values.appendAssumeCapacity(@intCast(value));
                    value += self.step;
                }
                try s.appendValidityRange(self.allocator, dst_start, null, take);
            },
            .unnest => {
                try self.positions.ensureUnusedCapacity(self.allocator, take);
                const first: u32 = @intCast(self.pos);
                for (0..take) |k| self.positions.appendAssumeCapacity(first + @as(u32, @intCast(k)));
            },
        }
    }

    fn emitPadding(self: *Expand) !void {
        try self.src_rows.append(self.allocator, @intCast(self.row));
        switch (self.generator) {
            .series => try self.out[self.carry.len].appendNulls(self.allocator, 1),
            .unnest => try self.positions.append(self.allocator, PADDING),
        }
    }

    /// The output batch: each carried column gathered from `b`, and for
    /// unnest each array's elements at the rows' positions.
    fn finish(self: *Expand, b: Batch) !Batch {
        for (self.carry, self.out[0..self.carry.len]) |c, *s| {
            try transform.appendByIndices(self.allocator, b.values[c], self.src_rows.items, s);
        }
        if (self.generator == .unnest) {
            for (self.flats, self.out[self.carry.len..]) |flat, *s| try self.gatherElements(flat, s);
        }
        for (self.out, self.views) |s, *v| v.* = s.view();
        return .{ .schema = self.schema, .values = self.views, .row_count = self.src_rows.items.len };
    }

    fn gatherElements(self: *Expand, flat: nested.Flat, out: *ColumnStore) !void {
        self.elements.clearRetainingCapacity();
        try self.elements.ensureTotalCapacity(self.allocator, self.src_rows.items.len);
        const offsets = flat.offsets.items;
        for (self.src_rows.items, self.positions.items) |r, p| {
            const len = offsets[r + 1] - offsets[r];
            self.elements.appendAssumeCapacity(if (p < len) offsets[r] + p else NO_ELEMENT);
        }
        const rows = self.elements.items;
        const view = flat.elements.view();
        var start: usize = 0;
        while (start < rows.len) {
            const missing = rows[start] == NO_ELEMENT;
            var end = start + 1;
            while (end < rows.len and (rows[end] == NO_ELEMENT) == missing) : (end += 1) {}
            if (missing) {
                try out.appendNulls(self.allocator, end - start);
            } else {
                try transform.appendByIndices(self.allocator, view, rows[start..end], out);
            }
            start = end;
        }
    }

    pub fn outputSchema(self: *Expand) []const Column {
        return self.schema;
    }

    // A hint on an output column says nothing about which upstream rows
    // produce matching values.
    pub fn addPrune(_: *Expand, _: Predicate) !void {}

    /// The arguments are only known once the upstream runs, so the row count
    /// has no bound here.
    pub fn stats(_: *Expand) exec.PipelineStats {
        return .{ .upper_rows = std.math.maxInt(u64) };
    }

    pub fn accountant(self: *Expand) ?*exec.memory.MemoryAccountant {
        return self.upstream.accountant();
    }

    pub fn explain(self: *Expand, out: *std.ArrayList(u8), allocator: Allocator, depth: usize) !void {
        const name = switch (self.generator) {
            .series => "Series",
            .unnest => "Unnest",
        };
        var buf: [32]u8 = undefined;
        const line = if (self.carry.len == 0 and !self.outer)
            name
        else
            try std.fmt.bufPrint(&buf, "{s} ({s})", .{ name, if (self.outer) "lateral, outer" else "lateral" });
        try exec.explainLine(out, allocator, depth, line);
        try self.upstream.explain(out, allocator, depth + 1);
    }
};

/// The upstream column named `name`. The arguments are computed last, so a
/// carried column of the same name comes before it.
fn argColumn(schema: []const Column, name: []const u8) !usize {
    var i = schema.len;
    while (i > 0) {
        i -= 1;
        if (std.mem.eql(u8, schema[i].name, name)) return i;
    }
    return error.ColumnNotFound;
}

fn isInteger(t: Type) bool {
    return switch (t) {
        .tinyint, .smallint, .int, .bigint, .largeint => true,
        else => false,
    };
}

/// Row `row` of an integer column, null when NULL. A LARGEINT outside the
/// BIGINT range can't bound a BIGINT sequence.
fn integerAt(view: ColumnView, row: usize) !?i64 {
    if (!view.isValid(row)) return null;
    return switch (view.data) {
        .tinyint => |v| v[row],
        .smallint => |v| v[row],
        .int => |v| v[row],
        .bigint => |v| v[row],
        .largeint => |v| std.math.cast(i64, v[row]) orelse error.ValueOutOfRange,
        else => Error.SeriesNonIntegerArgument,
    };
}

const SingleBatchSource = @import("single_batch.zig").SingleBatchSource;

fn seriesSpec(args: []const []const u8) Spec {
    return .{ .generator = .series, .args = args, .columns = &.{"x"} };
}

fn drainSeries(allocator: Allocator, q: *Query) ![]i64 {
    var out: std.ArrayList(i64) = .empty;
    errdefer out.deinit(allocator);
    while (try q.next()) |b| {
        try std.testing.expect(b.row_count <= BATCH_ROWS);
        try out.appendSlice(allocator, b.values[0].data.bigint);
    }
    return out.toOwnedSlice(allocator);
}

fn argBatch(schema: []const Column, views: []const ColumnView) Batch {
    return .{ .schema = schema, .values = views, .row_count = views[0].rowCount() };
}

test "series expands each argument row, skipping NULL arguments and steps away from stop" {
    const allocator = std.testing.allocator;
    const schema = [_]Column{
        .{ .name = "a", .type = .int, .nullable = true },
        .{ .name = "b", .type = .int },
        .{ .name = "s", .type = .smallint },
    };
    // Rows: 1..3 by 1; NULL start; 5 down to 2 by -1; 0..-2 by 1 (empty);
    // 10..20 by 4.
    const starts = [_]i32{ 1, 0, 5, 0, 10 };
    const valid = [_]u8{0b1_1101};
    const stops = [_]i32{ 3, 9, 2, -2, 20 };
    const steps = [_]i16{ 1, 1, -1, 1, 4 };
    const views = [_]ColumnView{
        .{ .data = .{ .int = &starts }, .nulls = &valid },
        .{ .data = .{ .int = &stops } },
        .{ .data = .{ .smallint = &steps } },
    };
    const up = try SingleBatchSource.create(allocator, argBatch(&schema, &views));
    var q = try Expand.create(allocator, up, .{ .generator = .series, .args = &.{ "a", "b", "s" }, .columns = &.{"generate_series"} });
    defer q.deinit();
    try std.testing.expectEqualStrings("generate_series", q.outputSchema()[0].name);
    const got = try drainSeries(allocator, &q);
    defer allocator.free(got);
    try std.testing.expectEqualSlices(i64, &.{ 1, 2, 3, 5, 4, 3, 2, 10, 14, 18 }, got);
}

test "series stops at the BIGINT bounds without wrapping and splits long runs into batches" {
    const allocator = std.testing.allocator;
    const max = std.math.maxInt(i64);
    const min = std.math.minInt(i64);
    const cases = .{
        .{ .start = max - 2, .stop = max, .step = 2, .want = &[_]i64{ max - 2, max } },
        .{ .start = max, .stop = max, .step = max, .want = &[_]i64{max} },
        .{ .start = min, .stop = min, .step = min, .want = &[_]i64{min} },
        .{ .start = min + 1, .stop = min, .step = -1, .want = &[_]i64{ min + 1, min } },
    };
    const schema = [_]Column{ .{ .name = "a", .type = .bigint }, .{ .name = "b", .type = .bigint }, .{ .name = "s", .type = .bigint } };
    inline for (cases) |c| {
        const starts = [_]i64{c.start};
        const stops = [_]i64{c.stop};
        const steps = [_]i64{c.step};
        const views = [_]ColumnView{ .{ .data = .{ .bigint = &starts } }, .{ .data = .{ .bigint = &stops } }, .{ .data = .{ .bigint = &steps } } };
        const up = try SingleBatchSource.create(allocator, argBatch(&schema, &views));
        var q = try Expand.create(allocator, up, seriesSpec(&.{ "a", "b", "s" }));
        defer q.deinit();
        const got = try drainSeries(allocator, &q);
        defer allocator.free(got);
        try std.testing.expectEqualSlices(i64, c.want, got);
    }

    const starts = [_]i64{1};
    const stops = [_]i64{3 * BATCH_ROWS + 7};
    const views = [_]ColumnView{ .{ .data = .{ .bigint = &starts } }, .{ .data = .{ .bigint = &stops } } };
    const up = try SingleBatchSource.create(allocator, argBatch(schema[0..2], &views));
    var q = try Expand.create(allocator, up, seriesSpec(&.{ "a", "b" }));
    defer q.deinit();
    const got = try drainSeries(allocator, &q);
    defer allocator.free(got);
    try std.testing.expectEqual(@as(usize, 3 * BATCH_ROWS + 7), got.len);
    for (got, 1..) |v, want| try std.testing.expectEqual(@as(i64, @intCast(want)), v);
}

test "series rejects a zero step and non-integer arguments" {
    const allocator = std.testing.allocator;
    const schema = [_]Column{ .{ .name = "a", .type = .bigint }, .{ .name = "b", .type = .bigint }, .{ .name = "s", .type = .bigint } };
    const starts = [_]i64{1};
    const stops = [_]i64{2};
    const steps = [_]i64{0};
    const views = [_]ColumnView{ .{ .data = .{ .bigint = &starts } }, .{ .data = .{ .bigint = &stops } }, .{ .data = .{ .bigint = &steps } } };
    const up = try SingleBatchSource.create(allocator, argBatch(&schema, &views));
    var q = try Expand.create(allocator, up, seriesSpec(&.{ "a", "b", "s" }));
    defer q.deinit();
    try std.testing.expectError(Error.SeriesStepZero, q.next());

    const dschema = [_]Column{ .{ .name = "a", .type = .double }, .{ .name = "b", .type = .bigint } };
    const dstarts = [_]f64{1.5};
    const dviews = [_]ColumnView{ .{ .data = .{ .double = &dstarts } }, .{ .data = .{ .bigint = &stops } } };
    var dup = try SingleBatchSource.create(allocator, argBatch(&dschema, &dviews));
    defer dup.deinit();
    try std.testing.expectError(Error.SeriesNonIntegerArgument, Expand.create(allocator, dup, seriesSpec(&.{ "a", "b" })));
}

/// An ARRAY column of `texts`, a null text being a NULL array.
fn arrayColumn(allocator: Allocator, t: Type, texts: []const ?[]const u8) !ColumnStore {
    var col = try ColumnStore.init(allocator, t, true);
    errdefer col.deinit(allocator);
    var bytes: std.ArrayList(u8) = .empty;
    defer bytes.deinit(allocator);
    for (texts) |text| {
        const at = col.rowCount();
        bytes.clearRetainingCapacity();
        if (text) |s| try nested.parseText(allocator, t, s, &bytes);
        try col.data.nested.appendValue(allocator, bytes.items);
        try col.appendValidBit(allocator, at, text != null);
    }
    return col;
}

/// Every row of `q` as `a,b,...` (NULL spelled out), rows joined by `;`.
fn drainInts(allocator: Allocator, q: *Query) ![]u8 {
    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(allocator);
    while (try q.next()) |b| {
        try std.testing.expect(b.row_count <= BATCH_ROWS);
        for (0..b.row_count) |row| {
            if (out.items.len > 0) try out.append(allocator, ';');
            for (b.values, 0..) |col, c| {
                if (c > 0) try out.append(allocator, ',');
                if (!col.isValid(row)) {
                    try out.appendSlice(allocator, "NULL");
                    continue;
                }
                switch (col.data) {
                    inline .int, .bigint => |s| try out.print(allocator, "{d}", .{s[row]}),
                    else => return error.TestUnexpectedType,
                }
            }
        }
    }
    return out.toOwnedSlice(allocator);
}

test "unnest zips arrays, padding the shorter with NULL, and gives NULL and empty arrays no rows" {
    const allocator = std.testing.allocator;
    const int_array = try types.arrayOf(.int);
    var xs = try arrayColumn(allocator, int_array, &.{ "[1, null, 3]", null, "[]", "[4]" });
    defer xs.deinit(allocator);
    var ys = try arrayColumn(allocator, int_array, &.{ "[10]", "[20, 21]", "[]", "[]" });
    defer ys.deinit(allocator);
    const schema = [_]Column{ .{ .name = "xs", .type = int_array, .nullable = true }, .{ .name = "ys", .type = int_array, .nullable = true } };
    const views = [_]ColumnView{ xs.view(), ys.view() };
    const cases = .{
        .{ .outer = false, .want = "1,10;NULL,NULL;3,NULL;NULL,20;NULL,21;4,NULL" },
        .{ .outer = true, .want = "1,10;NULL,NULL;3,NULL;NULL,20;NULL,21;NULL,NULL;4,NULL" },
    };
    inline for (cases) |c| {
        const up = try SingleBatchSource.create(allocator, argBatch(&schema, &views));
        var q = try Expand.create(allocator, up, .{ .generator = .unnest, .args = &.{ "xs", "ys" }, .columns = &.{ "unnest", "unnest_1" }, .outer = c.outer });
        defer q.deinit();
        try std.testing.expect(q.outputSchema()[0].type == .int);
        const got = try drainInts(allocator, &q);
        defer allocator.free(got);
        try std.testing.expectEqualStrings(c.want, got);
    }
}

test "lateral unnest carries the other upstream columns; outer keeps rows that expand to nothing" {
    const allocator = std.testing.allocator;
    const int_array = try types.arrayOf(.int);
    var xs = try arrayColumn(allocator, int_array, &.{ "[80, 85]", "[77, null]", null, "[]" });
    defer xs.deinit(allocator);
    const ids = [_]i64{ 1, 2, 3, 4 };
    const schema = [_]Column{ .{ .name = "id", .type = .bigint }, .{ .name = "xs", .type = int_array, .nullable = true } };
    const views = [_]ColumnView{ .{ .data = .{ .bigint = &ids } }, xs.view() };
    const cases = .{
        .{ .outer = false, .want = "1,80;1,85;2,77;2,NULL" },
        .{ .outer = true, .want = "1,80;1,85;2,77;2,NULL;3,NULL;4,NULL" },
    };
    inline for (cases) |c| {
        const up = try SingleBatchSource.create(allocator, argBatch(&schema, &views));
        var q = try Expand.create(allocator, up, .{ .generator = .unnest, .args = &.{"xs"}, .columns = &.{"x"}, .lateral = true, .outer = c.outer });
        defer q.deinit();
        try std.testing.expectEqual(@as(usize, 2), q.outputSchema().len);
        try std.testing.expectEqualStrings("id", q.outputSchema()[0].name);
        const got = try drainInts(allocator, &q);
        defer allocator.free(got);
        try std.testing.expectEqualStrings(c.want, got);
    }

    const counts = [_]i64{ 0, 2 * BATCH_ROWS + 1 };
    const ones = [_]i64{ 1, 1 };
    const sschema = [_]Column{ .{ .name = "id", .type = .bigint }, .{ .name = "one", .type = .bigint }, .{ .name = "n", .type = .bigint } };
    const sviews = [_]ColumnView{ .{ .data = .{ .bigint = &ids } }, .{ .data = .{ .bigint = &ones } }, .{ .data = .{ .bigint = &counts } } };
    const up = try SingleBatchSource.create(allocator, .{ .schema = &sschema, .values = &sviews, .row_count = 2 });
    var q = try Expand.create(allocator, up, .{ .generator = .series, .args = &.{ "one", "n" }, .columns = &.{"g"}, .lateral = true, .outer = true });
    defer q.deinit();
    var rows: usize = 0;
    var first = true;
    while (try q.next()) |b| {
        try std.testing.expect(b.row_count <= BATCH_ROWS);
        for (0..b.row_count) |row| {
            if (first) {
                try std.testing.expectEqual(@as(i64, 1), b.values[0].data.bigint[row]);
                try std.testing.expect(!b.values[1].isValid(row));
                first = false;
                continue;
            }
            try std.testing.expectEqual(@as(i64, 2), b.values[0].data.bigint[row]);
            rows += 1;
            try std.testing.expectEqual(@as(i64, @intCast(rows)), b.values[1].data.bigint[row]);
        }
    }
    try std.testing.expectEqual(@as(usize, 2 * BATCH_ROWS + 1), rows);
}

test "unnest rejects an argument that isn't an array" {
    const allocator = std.testing.allocator;
    const schema = [_]Column{.{ .name = "a", .type = .bigint }};
    const values = [_]i64{1};
    const views = [_]ColumnView{.{ .data = .{ .bigint = &values } }};
    var up = try SingleBatchSource.create(allocator, argBatch(&schema, &views));
    defer up.deinit();
    try std.testing.expectError(Error.UnnestNonArrayArgument, Expand.create(allocator, up, .{ .generator = .unnest, .args = &.{"a"}, .columns = &.{"unnest"} }));
}
