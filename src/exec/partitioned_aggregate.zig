//! Partition-parallel grouped aggregate over a buffered (non-table) input.
//!
//! The radix aggregate parallelises high-card GROUP BY only for integer keys
//! that bit-pack into a u128 and only for fixed-state aggregates; the silo
//! (the string/wide-key parallel path) sources exclusively from a table scan.
//! A grouped aggregate whose input is a materialized CTE buffer with a string
//! key and/or MAX_BY/ANY_VALUE therefore falls to the serial hash aggregate.
//!
//! This operator restores parallelism for that case by partitioning, not
//! combining: it hashes each input row's group key into one of N partitions,
//! then runs an independent serial `Aggregate` over each partition on its own
//! thread. Partitioning by the group key guarantees every row of a group lands
//! in exactly one partition, so the per-partition outputs concatenate with no
//! cross-partition merge — which is what lets it carry ANY aggregate the serial
//! core supports (MAX_BY, ANY_VALUE, COUNT(DISTINCT), …) and any key type.
//!
//! Each partition owns an arena backed by a thread-safe allocator (the workers
//! allocate concurrently); the per-query arena is never touched off-thread.

const std = @import("std");
const Allocator = std.mem.Allocator;

const exec = @import("exec.zig");
const types = @import("../types.zig");
const engine = @import("../engine/engine.zig");
const storage = @import("../storage/storage.zig");
const aggregate = @import("aggregate.zig");
const ir = @import("../ir/ir.zig");

const Column = types.Column;
const ColumnView = storage.ColumnView;
const Batch = exec.Batch;
const Query = exec.Query;
const AggSpec = ir.AggSpec;

/// Below this realized input row count the threading overhead outweighs the
/// serial hash aggregate, so the router keeps the serial path.
pub const MIN_ROWS_FOR_PARALLEL: u64 = 96 * 1024;

const MAX_PARTS: usize = 32;

/// A partition's gathered input columns (one store per upstream column) plus
/// the serial aggregate's retained output, both living in `arena`.
const Partition = struct {
    arena: std.heap.ArenaAllocator,
    in_cols: []engine.ColumnStore,
    in_rows: usize = 0,
    out_cols: []engine.ColumnStore = &.{},
    out_rows: usize = 0,
    err: ?anyerror = null,
};

/// Single-batch source over a partition's gathered input columns — the serial
/// Aggregate drains it exactly like a scan leaf.
const InputScan = struct {
    schema: []const Column,
    views: []ColumnView,
    rows: usize,
    yielded: bool = false,

    pub fn next(self: *InputScan) !?Batch {
        if (self.yielded) return null;
        self.yielded = true;
        if (self.rows == 0) return null;
        return Batch{ .schema = self.schema, .values = self.views, .row_count = self.rows };
    }
    pub fn deinit(_: *InputScan) void {}
    pub fn outputSchema(self: *InputScan) []const Column {
        return self.schema;
    }
    pub fn addPrune(_: *InputScan, _: exec.Predicate) !void {}
    pub fn stats(self: *InputScan) exec.PipelineStats {
        return .{ .upper_rows = self.rows };
    }
    pub fn accountant(_: *InputScan) ?*exec.memory.MemoryAccountant {
        return null;
    }
    pub fn explain(self: *InputScan, out: *std.ArrayList(u8), alloc: Allocator, depth: usize) !void {
        _ = self;
        try exec.explainLine(out, alloc, depth, "PartitionInput");
    }
};

pub const PartitionedAggregate = struct {
    allocator: Allocator,
    up: Query,
    group_cols: []const []const u8,
    group_indices: []const usize,
    aggs: []const AggSpec,
    output_schema: []Column,
    n_parts: usize,
    parts: []Partition,
    ran: bool = false,
    emit_part: usize = 0,
    emit_chunk: usize = 0,
    views: []ColumnView,

    pub fn create(
        allocator: Allocator,
        up: Query,
        group_cols: []const []const u8,
        aggs: []const AggSpec,
        n_parts_hint: usize,
    ) !Query {
        const up_schema = up.outputSchema();
        const out_schema = try aggregate.outputSchemaFor(allocator, up_schema, group_cols, aggs);
        errdefer allocator.free(out_schema);

        const gci = try allocator.alloc(usize, group_cols.len);
        errdefer allocator.free(gci);
        for (group_cols, gci) |name, *slot| {
            slot.* = types.findColumn(up_schema, name) orelse return error.ColumnNotFound;
        }

        const n_parts = @max(@as(usize, 2), @min(n_parts_hint, MAX_PARTS));
        const parts = try allocator.alloc(Partition, n_parts);
        errdefer allocator.free(parts);
        var inited: usize = 0;
        errdefer for (parts[0..inited]) |*p| p.arena.deinit();
        for (parts) |*p| {
            p.* = .{ .arena = std.heap.ArenaAllocator.init(std.heap.c_allocator), .in_cols = &.{} };
            const aa = p.arena.allocator();
            p.in_cols = try aa.alloc(engine.ColumnStore, up_schema.len);
            for (up_schema, p.in_cols) |sc, *store| {
                store.* = try engine.ColumnStore.init(aa, sc.type, sc.nullable);
            }
            inited += 1;
        }

        const views = try allocator.alloc(ColumnView, out_schema.len);
        errdefer allocator.free(views);

        const self = try allocator.create(PartitionedAggregate);
        self.* = .{
            .allocator = allocator,
            .up = up,
            .group_cols = group_cols,
            .group_indices = gci,
            .aggs = aggs,
            .output_schema = out_schema,
            .n_parts = n_parts,
            .parts = parts,
            .views = views,
        };
        return exec.makeQuery(allocator, self);
    }

    /// murmur3 finalizer — mixes a combined cell hash into the running row hash.
    fn mix64(x0: u64) u64 {
        var x = x0;
        x ^= x >> 33;
        x *%= 0xff51afd7ed558ccd;
        x ^= x >> 33;
        x *%= 0xc4ceb9fe1a85ec53;
        x ^= x >> 33;
        return x;
    }

    /// Fold one key column into every row's partition hash — column-outer so
    /// the type dispatch runs once per column, not once per row. The hash only
    /// routes rows to partitions (equal keys → equal hash is the sole
    /// requirement), so a NULL folds as a fixed sentinel and collisions are
    /// harmless.
    fn hashColumnInto(view: ColumnView, hashes: []u64) void {
        const NULL_SENTINEL: u64 = 0xFFFF_FFFF_FFFF_FFFF;
        switch (view.data) {
            .int, .date => |s| for (hashes, s[0..hashes.len], 0..) |*h, v, row| {
                h.* = mix64(h.* ^ (if (view.isValid(@intCast(row))) @as(u64, @bitCast(@as(i64, v))) else NULL_SENTINEL));
            },
            .bigint, .datetime, .decimal64 => |s| for (hashes, s[0..hashes.len], 0..) |*h, v, row| {
                h.* = mix64(h.* ^ (if (view.isValid(@intCast(row))) @as(u64, @bitCast(v)) else NULL_SENTINEL));
            },
            .boolean => |s| for (hashes, s[0..hashes.len], 0..) |*h, v, row| {
                h.* = mix64(h.* ^ (if (view.isValid(@intCast(row))) @as(u64, v) else NULL_SENTINEL));
            },
            .tinyint => |s| for (hashes, s[0..hashes.len], 0..) |*h, v, row| {
                h.* = mix64(h.* ^ (if (view.isValid(@intCast(row))) @as(u64, @bitCast(@as(i64, v))) else NULL_SENTINEL));
            },
            .smallint => |s| for (hashes, s[0..hashes.len], 0..) |*h, v, row| {
                h.* = mix64(h.* ^ (if (view.isValid(@intCast(row))) @as(u64, @bitCast(@as(i64, v))) else NULL_SENTINEL));
            },
            .float => |s| for (hashes, s[0..hashes.len], 0..) |*h, v, row| {
                h.* = mix64(h.* ^ (if (view.isValid(@intCast(row))) @as(u64, @as(u32, @bitCast(v))) else NULL_SENTINEL));
            },
            .double => |s| for (hashes, s[0..hashes.len], 0..) |*h, v, row| {
                h.* = mix64(h.* ^ (if (view.isValid(@intCast(row))) @as(u64, @bitCast(v)) else NULL_SENTINEL));
            },
            .largeint, .decimal128 => |s| for (hashes, s[0..hashes.len], 0..) |*h, v, row| {
                const u: u128 = @bitCast(v);
                h.* = mix64(h.* ^ (if (view.isValid(@intCast(row))) (@as(u64, @truncate(u)) ^ @as(u64, @truncate(u >> 64))) else NULL_SENTINEL));
            },
            .uuid => |s| for (hashes, s[0..hashes.len], 0..) |*h, v, row| {
                h.* = mix64(h.* ^ (if (view.isValid(@intCast(row))) (@as(u64, @truncate(v)) ^ @as(u64, @truncate(v >> 64))) else NULL_SENTINEL));
            },
            .varchar, .string, .char => |sv| for (hashes, 0..) |*h, row| {
                h.* = mix64(h.* ^ (if (view.isValid(@intCast(row))) std.hash.Wyhash.hash(0, sv.rowBytes(row)) else NULL_SENTINEL));
            },
        }
    }

    /// Phase A (serial): drain the upstream once, scattering each row into its
    /// partition's gathered input columns. Runs on the calling thread only.
    fn scatter(self: *PartitionedAggregate) !void {
        var idx_lists = try self.allocator.alloc(std.ArrayListUnmanaged(u32), self.n_parts);
        defer {
            for (idx_lists) |*l| l.deinit(self.allocator);
            self.allocator.free(idx_lists);
        }
        for (idx_lists) |*l| l.* = .empty;
        var hashes: std.ArrayListUnmanaged(u64) = .empty;
        defer hashes.deinit(self.allocator);

        while (try self.up.next()) |batch| {
            try hashes.resize(self.allocator, batch.row_count);
            @memset(hashes.items, 0x9e3779b97f4a7c15);
            for (self.group_indices) |ci| hashColumnInto(batch.values[ci], hashes.items);
            for (idx_lists) |*l| l.clearRetainingCapacity();
            for (hashes.items, 0..) |h, row| {
                try idx_lists[h % self.n_parts].append(self.allocator, @intCast(row));
            }
            for (idx_lists, self.parts) |*l, *part| {
                if (l.items.len == 0) continue;
                const aa = part.arena.allocator();
                for (part.in_cols, 0..) |*store, ci| {
                    try engine.transform.appendByIndices(aa, batch.values[ci], l.items, store);
                }
                part.in_rows += l.items.len;
            }
        }
    }

    /// Phase B (one worker per partition): run the serial Aggregate over the
    /// partition's gathered input and retain its output rows in the same arena.
    fn aggregateOne(self: *PartitionedAggregate, part: *Partition) void {
        runPartition(self, part) catch |e| {
            part.err = e;
        };
    }

    fn runPartition(self: *PartitionedAggregate, part: *Partition) !void {
        const aa = part.arena.allocator();
        const up_schema = self.up.outputSchema();

        const in_views = try aa.alloc(ColumnView, up_schema.len);
        for (part.in_cols, in_views) |*store, *v| v.* = store.view();
        var scan = InputScan{ .schema = up_schema, .views = in_views, .rows = part.in_rows };
        const src = exec.makeQuery(aa, &scan);

        var agg = try aggregate.Aggregate.create(aa, src, self.group_cols, self.aggs, null, null);
        defer agg.deinit();

        const out_cols = try aa.alloc(engine.ColumnStore, self.output_schema.len);
        for (self.output_schema, out_cols) |sc, *store| {
            store.* = try engine.ColumnStore.init(aa, sc.type, sc.nullable);
        }
        var out_rows: usize = 0;
        while (try agg.next()) |b| {
            var i: usize = 0;
            while (i < b.row_count) : (i += 1) {
                for (out_cols, 0..) |*store, ci| {
                    try engine.transform.appendOneRow(aa, b.values[ci], i, store);
                }
            }
            out_rows += b.row_count;
        }
        part.out_cols = out_cols;
        part.out_rows = out_rows;
    }

    fn run(self: *PartitionedAggregate) !void {
        try self.scatter();

        var threads: [MAX_PARTS]?std.Thread = .{null} ** MAX_PARTS;
        var t: usize = 1;
        while (t < self.n_parts) : (t += 1) {
            threads[t] = std.Thread.spawn(.{}, aggregateOne, .{ self, &self.parts[t] }) catch null;
            if (threads[t] == null) aggregateOne(self, &self.parts[t]);
        }
        aggregateOne(self, &self.parts[0]);
        for (threads[1..self.n_parts]) |maybe| if (maybe) |th| th.join();

        for (self.parts) |*p| if (p.err) |e| return e;
        self.ran = true;
    }

    pub fn next(self: *PartitionedAggregate) !?Batch {
        if (!self.ran) try self.run();
        while (self.emit_part < self.n_parts) {
            const part = &self.parts[self.emit_part];
            if (self.emit_chunk == 0 and part.out_rows > 0) {
                for (part.out_cols, self.views) |*store, *v| v.* = store.view();
                self.emit_chunk = 1;
                return Batch{ .schema = self.output_schema, .values = self.views, .row_count = part.out_rows };
            }
            self.emit_part += 1;
            self.emit_chunk = 0;
        }
        return null;
    }

    pub fn outputSchema(self: *PartitionedAggregate) []const Column {
        return self.output_schema;
    }

    pub fn addPrune(_: *PartitionedAggregate, _: exec.Predicate) !void {}

    pub fn stats(self: *PartitionedAggregate) exec.PipelineStats {
        return .{ .upper_rows = self.up.stats().upper_rows };
    }

    pub fn accountant(self: *PartitionedAggregate) ?*exec.memory.MemoryAccountant {
        return self.up.accountant();
    }

    pub fn explain(self: *PartitionedAggregate, out: *std.ArrayList(u8), alloc: Allocator, depth: usize) !void {
        var buf: [80]u8 = undefined;
        const line = std.fmt.bufPrint(&buf, "PartitionedAggregate parts={d}", .{self.n_parts}) catch "PartitionedAggregate";
        try exec.explainLine(out, alloc, depth, line);
        try self.up.explain(out, alloc, depth + 1);
    }

    pub fn deinit(self: *PartitionedAggregate) void {
        self.up.deinit();
        for (self.parts) |*p| p.arena.deinit();
        self.allocator.free(self.parts);
        self.allocator.free(self.group_indices);
        self.allocator.free(self.output_schema);
        self.allocator.free(self.views);
        self.allocator.destroy(self);
    }
};

const testing = std.testing;
const engine_store = engine.ColumnStore;

fn testReadI128(v: ColumnView, row: usize) i128 {
    return switch (v.data) {
        .int => |s| s[row],
        .bigint => |s| s[row],
        .largeint => |s| s[row],
        .smallint => |s| s[row],
        .tinyint => |s| s[row],
        else => unreachable,
    };
}

/// Drain a grouped query into `key|count|sum|maxby` lines (one per group),
/// sorted — the partition-parallel and serial aggregates emit the same groups
/// in different orders, so compare the canonicalized set.
fn testCollectSorted(allocator: Allocator, q: *Query) ![][]u8 {
    var lines: std.ArrayListUnmanaged([]u8) = .empty;
    errdefer {
        for (lines.items) |l| allocator.free(l);
        lines.deinit(allocator);
    }
    while (try q.next()) |b| {
        var row: usize = 0;
        while (row < b.row_count) : (row += 1) {
            const key = b.values[0].data.string.rowBytes(@intCast(row));
            const cnt = testReadI128(b.values[1], row);
            const sm = testReadI128(b.values[2], row);
            const mb = b.values[3].data.string.rowBytes(@intCast(row));
            const line = try std.fmt.allocPrint(allocator, "{s}|{d}|{d}|{s}", .{ key, cnt, sm, mb });
            try lines.append(allocator, line);
        }
    }
    std.mem.sort([]u8, lines.items, {}, struct {
        fn lt(_: void, a: []u8, b: []u8) bool {
            return std.mem.lessThan(u8, a, b);
        }
    }.lt);
    return lines.toOwnedSlice(allocator);
}

test "PartitionedAggregate matches serial aggregate on string key + MAX_BY" {
    const a = testing.allocator;
    const N = 4000;

    const schema = [_]Column{
        .{ .name = "key", .type = .string, .nullable = false },
        .{ .name = "val", .type = .bigint, .nullable = false },
        .{ .name = "ord", .type = .bigint, .nullable = false },
        .{ .name = "label", .type = .string, .nullable = false },
    };
    var stores: [4]engine_store = undefined;
    for (&stores, schema) |*s, col| s.* = try engine_store.init(a, col.type, col.nullable);
    defer for (&stores) |*s| s.deinit(a);

    var i: usize = 0;
    while (i < N) : (i += 1) {
        var kb: [8]u8 = undefined;
        const k = try std.fmt.bufPrint(&kb, "g{d}", .{i % 7});
        try stores[0].data.string.appendValue(a, k);
        try stores[1].data.bigint.append(a, @intCast(i));
        try stores[2].data.bigint.append(a, @intCast((i * 31) % N));
        var lb: [8]u8 = undefined;
        const lbl = try std.fmt.bufPrint(&lb, "L{d}", .{i % 13});
        try stores[3].data.string.appendValue(a, lbl);
    }

    var views: [4]ColumnView = undefined;
    for (&views, &stores) |*v, *s| v.* = s.view();
    const group_cols = [_][]const u8{"key"};
    const aggs = [_]AggSpec{
        .{ .func = .count, .col = null, .as = "c" },
        .{ .func = .sum, .col = "val", .as = "s" },
        .{ .func = .max_by, .col = "label", .arg2_col = "ord", .as = "mb" },
    };

    var scan_p = InputScan{ .schema = &schema, .views = &views, .rows = N };
    var pa = try PartitionedAggregate.create(a, exec.makeQuery(a, &scan_p), &group_cols, &aggs, 4);
    const par_lines = try testCollectSorted(a, &pa);
    defer {
        for (par_lines) |l| a.free(l);
        a.free(par_lines);
    }
    pa.deinit();

    var scan_s = InputScan{ .schema = &schema, .views = &views, .rows = N };
    var ser = try @import("aggregate.zig").Aggregate.create(a, exec.makeQuery(a, &scan_s), &group_cols, &aggs, null, null);
    const ser_lines = try testCollectSorted(a, &ser);
    defer {
        for (ser_lines) |l| a.free(l);
        a.free(ser_lines);
    }
    ser.deinit();

    try testing.expectEqual(@as(usize, 7), ser_lines.len);
    try testing.expectEqual(ser_lines.len, par_lines.len);
    for (par_lines, ser_lines) |p, s| try testing.expectEqualStrings(s, p);
}
