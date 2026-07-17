//! Keyed pipeline region runtime — data plane (REGION_PLAN.md, P0).
//!
//! A region partitions its input ONCE by a key's hash into `n_shards`
//! buckets at scan time, then runs its whole operator chain shard-locally
//! with no stage materialization between operators. This module is the
//! data plane: the hash exchange, the ordered single-gather consolidation
//! (the region's ordering contract is applied during the one copy — there
//! is never a separate re-sort/rematerialize pass), and the bulk column
//! movement kernels. Mechanisms and their measured effects come from the
//! rollforward probe in bench/rf_custom.zig (0.72s warm vs 5.3s engine).
//!
//! Ownership: Exchange and ShardData buffers are pooled — `clear` retains
//! capacity across runs (the slab-pool discipline that both fixed the
//! Windows allocator drift and cut steady-state stage time by a third).

const std = @import("std");
const Allocator = std.mem.Allocator;

const types = @import("../types.zig");
const Column = types.Column;
const storage = @import("../storage/storage.zig");
const ColumnView = storage.ColumnView;
const ColumnStore = @import("../engine/store.zig").ColumnStore;
const exec = @import("exec.zig");
const Batch = exec.Batch;

const HASH_SEED: u64 = 0x9e3779b9;
/// Bucket refs pack (worker, row) into a u32; buckets stay far below this.
const REF_ROW_BITS = 24;
const REF_ROW_MASK: u32 = (1 << REF_ROW_BITS) - 1;

// ---------------------------------------------------------------------------
// Bulk column movement (typed loops; dispatch hoisted out of the row loop)
// ---------------------------------------------------------------------------

/// Append `rows` (indices into `v`) onto `store`.
pub fn scatterColumn(alloc: Allocator, store: *ColumnStore, v: ColumnView, rows: []const u32) !void {
    const base = store.rowCount();
    switch (v.data) {
        inline .tinyint, .smallint, .int, .bigint, .largeint, .float, .double, .date, .datetime, .decimal64, .decimal128 => |s, tag| {
            const l = &@field(store.data, @tagName(tag));
            try l.ensureUnusedCapacity(alloc, rows.len);
            for (rows) |r| l.appendAssumeCapacity(s[r]);
        },
        .varchar, .string, .char, .json => |s| switch (store.data) {
            .varchar, .string, .char, .json => |*d| {
                var bytes: usize = 0;
                for (rows) |r| bytes += s.rowBytes(r).len;
                try d.ensureUnusedValueCapacity(alloc, rows.len, bytes);
                for (rows) |r| d.appendValueAssumeCapacity(s.rowBytes(r));
            },
            else => unreachable,
        },
        else => return error.UnsupportedQueryShape,
    }
    if (store.nulls != null) {
        for (rows, 0..) |r, k| try store.appendValidBit(alloc, base + k, v.isValid(r));
    }
}

/// Append `refs` rows gathered across per-worker source views (worker in
/// the top byte, bucket row in the low 24 bits) — the exchange-merge loop.
pub fn gatherColumn(alloc: Allocator, dst: *ColumnStore, srcs: []const ColumnView, refs: []const u32) !void {
    const base = dst.rowCount();
    switch (dst.data) {
        inline .tinyint, .smallint, .int, .bigint, .largeint, .float, .double, .date, .datetime, .decimal64, .decimal128 => |*l, tag| {
            try l.ensureUnusedCapacity(alloc, refs.len);
            for (refs) |r| {
                l.appendAssumeCapacity(@field(srcs[r >> REF_ROW_BITS].data, @tagName(tag))[r & REF_ROW_MASK]);
            }
        },
        .varchar, .string, .char, .json => |*d| {
            var bytes: usize = 0;
            for (refs) |r| bytes += stringViewOf(srcs[r >> REF_ROW_BITS]).rowBytes(r & REF_ROW_MASK).len;
            try d.ensureUnusedValueCapacity(alloc, refs.len, bytes);
            for (refs) |r| d.appendValueAssumeCapacity(stringViewOf(srcs[r >> REF_ROW_BITS]).rowBytes(r & REF_ROW_MASK));
        },
        else => return error.UnsupportedQueryShape,
    }
    if (dst.nulls != null) {
        for (refs, 0..) |r, k| {
            try dst.appendValidBit(alloc, base + k, srcs[r >> REF_ROW_BITS].isValid(r & REF_ROW_MASK));
        }
    }
}

/// Append the contiguous range [start,end) of `src` onto `dst` — a group's
/// partition view is a contiguous range of its shard buffer, so per-group
/// operator inputs are slice copies, never per-value appends.
pub fn appendStoreRange(alloc: Allocator, dst: *ColumnStore, src: *const ColumnStore, start: usize, end: usize) !void {
    const v = src.view();
    switch (v.data) {
        inline .tinyint, .smallint, .int, .bigint, .largeint, .float, .double, .date, .datetime, .decimal64, .decimal128 => |s, tag| {
            try @field(dst.data, @tagName(tag)).appendSlice(alloc, s[start..end]);
        },
        .varchar, .string, .char, .json => |sv| switch (dst.data) {
            .varchar, .string, .char, .json => |*d| try d.appendRange(alloc, sv, start, end),
            else => unreachable,
        },
        else => return error.UnsupportedQueryShape,
    }
    if (dst.nulls != null) {
        const base = dst.rowCount() - (end - start);
        for (start..end, 0..) |i, k| try dst.appendValidBit(alloc, base + k, v.isValid(i));
    }
}

fn stringViewOf(v: ColumnView) storage.StringView {
    return switch (v.data) {
        .varchar, .string, .char, .json => |s| s,
        else => unreachable,
    };
}

// ---------------------------------------------------------------------------
// Exchange: hash-scatter into (worker × shard) buckets
// ---------------------------------------------------------------------------

/// One (worker, shard) bucket: a private columnar buffer only its producing
/// worker appends to, so the scatter is lock-free. Consolidation reads all
/// workers' buckets for one shard.
const Bucket = struct {
    cols: []ColumnStore,
    rows: usize = 0,
};

pub const Exchange = struct {
    alloc: Allocator,
    schema: []const Column,
    n_workers: usize,
    n_shards: usize,
    /// Region key column (index into `schema`) — hash source. P0: one
    /// string-typed key column; composite keys hash-combine later.
    key_col: usize,
    buckets: []Bucket,

    pub fn init(alloc: Allocator, schema: []const Column, n_workers: usize, n_shards: usize, key_col: usize) !Exchange {
        const buckets = try alloc.alloc(Bucket, n_workers * n_shards);
        var built: usize = 0;
        errdefer {
            for (buckets[0..built]) |*b| {
                for (b.cols) |*c| c.deinit(alloc);
                alloc.free(b.cols);
            }
            alloc.free(buckets);
        }
        for (buckets) |*b| {
            const cols = try alloc.alloc(ColumnStore, schema.len);
            var inited: usize = 0;
            errdefer {
                for (cols[0..inited]) |*c| c.deinit(alloc);
                alloc.free(cols);
            }
            for (cols, schema) |*c, col| {
                c.* = try ColumnStore.init(alloc, col.type, col.nullable);
                inited += 1;
            }
            b.* = .{ .cols = cols };
            built += 1;
        }
        return .{
            .alloc = alloc,
            .schema = schema,
            .n_workers = n_workers,
            .n_shards = n_shards,
            .key_col = key_col,
            .buckets = buckets,
        };
    }

    pub fn deinit(self: *Exchange) void {
        for (self.buckets) |*b| {
            for (b.cols) |*c| c.deinit(self.alloc);
            self.alloc.free(b.cols);
        }
        self.alloc.free(self.buckets);
    }

    /// Pool reuse: retain every bucket's capacity for the next run.
    pub fn clear(self: *Exchange) void {
        for (self.buckets) |*b| {
            for (b.cols) |*c| c.clear();
            b.rows = 0;
        }
    }

    fn bucket(self: *Exchange, w: usize, shard: usize) *Bucket {
        return &self.buckets[w * self.n_shards + shard];
    }

    pub fn shardRows(self: *const Exchange, shard: usize) usize {
        var total: usize = 0;
        for (0..self.n_workers) |w| total += self.buckets[w * self.n_shards + shard].rows;
        return total;
    }

    /// Shard ids in descending row count: whales first, so the phase tail
    /// is small shards (region scheduler policy). Caller frees.
    pub fn shardOrderDesc(self: *const Exchange, alloc: Allocator) ![]u32 {
        const totals = try alloc.alloc(usize, self.n_shards);
        defer alloc.free(totals);
        for (0..self.n_shards) |s| totals[s] = self.shardRows(s);
        const order = try alloc.alloc(u32, self.n_shards);
        for (order, 0..) |*o, i| o.* = @intCast(i);
        std.mem.sortUnstable(u32, order, totals, struct {
            fn less(t: []const usize, a: u32, b: u32) bool {
                return t[a] > t[b];
            }
        }.less);
        return order;
    }

    /// Per-worker scatter state (reused row-index lists per batch).
    pub const Worker = struct {
        ex: *Exchange,
        w: usize,
        idx: []std.ArrayListUnmanaged(u32),

        pub fn deinit(self: *Worker) void {
            for (self.idx) |*l| l.deinit(self.ex.alloc);
            self.ex.alloc.free(self.idx);
        }

        /// Route one batch: pass 1 assigns each row a shard by key hash
        /// (NULL keys hash as empty — one shard, matching NULL-group
        /// semantics); pass 2 appends per (shard, column).
        pub fn push(self: *Worker, batch: Batch) !void {
            const ex = self.ex;
            for (self.idx) |*l| l.clearRetainingCapacity();
            const kv = batch.values[ex.key_col];
            for (0..batch.row_count) |r| {
                const key: []const u8 = if (kv.isValid(r)) stringViewOf(kv).rowBytes(r) else "";
                const shard = std.hash.Wyhash.hash(HASH_SEED, key) % ex.n_shards;
                try self.idx[shard].append(ex.alloc, @intCast(r));
            }
            for (0..ex.n_shards) |shard| {
                const rows = self.idx[shard].items;
                if (rows.len == 0) continue;
                const b = ex.bucket(self.w, shard);
                for (b.cols, batch.values) |*store, v| {
                    try scatterColumn(ex.alloc, store, v, rows);
                }
                b.rows += rows.len;
            }
        }
    };

    pub fn worker(self: *Exchange, w: usize) !Worker {
        const idx = try self.alloc.alloc(std.ArrayListUnmanaged(u32), self.n_shards);
        for (idx) |*l| l.* = .empty;
        return .{ .ex = self, .w = w, .idx = idx };
    }
};

// ---------------------------------------------------------------------------
// Ordered consolidation: the region's one materialization
// ---------------------------------------------------------------------------

pub const OrderKind = enum { string, int32, int64 };

/// One column of the region's ordering contract. The leading `group_prefix`
/// columns of the spec also define group identity (contiguous ranges in the
/// consolidated output).
pub const OrderCol = struct {
    col: usize,
    kind: OrderKind,
};

/// Normalized per-row sort key: u64 primary word per order column (strings
/// carry a big-endian prefix and fall back to byte compare on prefix ties).
/// NULLs order first, encoded below every non-null value.
const RowKey = struct {
    norm: u64,
    str: []const u8, // empty unless string kind and prefix may tie
};

fn normI32(valid: bool, v: i32) u64 {
    if (!valid) return 0;
    return (@as(u64, 1) << 33) | @as(u64, @as(u32, @bitCast(v)) ^ 0x8000_0000);
}

fn normI64(valid: bool, v: i64) u64 {
    // Loses the low bit of i64 range to keep the null flag in-word; exact
    // for every engine value today (dates/datetimes/ids are far smaller).
    if (!valid) return 0;
    return (@as(u64, 1) << 63) | (@as(u64, @as(u64, @bitCast(v)) ^ 0x8000_0000_0000_0000) >> 1);
}

fn normStrPrefix(valid: bool, s: []const u8) u64 {
    if (!valid) return 0;
    var b: [7]u8 = @splat(0);
    const n = @min(7, s.len);
    @memcpy(b[0..n], s[0..n]);
    var w: u64 = 1; // non-null flag above the 56 prefix bits
    for (b) |ch| w = (w << 8) | ch;
    return w;
}

/// Consolidated shard: the region operators run over these columns; group
/// ranges are contiguous by construction.
pub const ShardData = struct {
    cols: []ColumnStore = &.{},
    rows: usize = 0,
    ranges: std.ArrayListUnmanaged([2]u32) = .empty,
    built: bool = false,

    pub fn ensure(self: *ShardData, alloc: Allocator, schema: []const Column) !void {
        if (self.built) {
            for (self.cols) |*c| c.clear();
            self.ranges.clearRetainingCapacity();
            self.rows = 0;
            return;
        }
        self.cols = try alloc.alloc(ColumnStore, schema.len);
        var inited: usize = 0;
        errdefer {
            for (self.cols[0..inited]) |*c| c.deinit(alloc);
            alloc.free(self.cols);
            self.cols = &.{};
        }
        for (self.cols, schema) |*c, col| {
            c.* = try ColumnStore.init(alloc, col.type, col.nullable);
            inited += 1;
        }
        self.built = true;
    }

    pub fn deinit(self: *ShardData, alloc: Allocator) void {
        if (!self.built) return;
        for (self.cols) |*c| c.deinit(alloc);
        alloc.free(self.cols);
        self.ranges.deinit(alloc);
        self.* = .{};
    }
};

/// Gather one shard from the exchange buckets in sort order, recording the
/// group ranges (equal leading `group_prefix` sort columns). `scratch` is a
/// per-thread arena reset by the caller between shards.
pub fn consolidateOrdered(
    ex: *Exchange,
    shard: usize,
    sort_cols: []const OrderCol,
    group_prefix: usize,
    out: *ShardData,
    scratch: Allocator,
) !void {
    const total = ex.shardRows(shard);
    try out.ensure(ex.alloc, ex.schema);
    if (total == 0) return;

    // Per-column, per-worker source views.
    const n_cols = ex.schema.len;
    const views = try scratch.alloc(ColumnView, n_cols * ex.n_workers);
    for (0..n_cols) |ci| {
        for (0..ex.n_workers) |w| {
            views[ci * ex.n_workers + w] = ex.bucket(w, shard).cols[ci].view();
        }
    }

    // Normalized keys, columnar: keys[kc * total + row].
    const refs = try scratch.alloc(u32, total);
    const keys = try scratch.alloc(RowKey, sort_cols.len * total);
    {
        var k: usize = 0;
        for (0..ex.n_workers) |w| {
            const bucket_rows = ex.bucket(w, shard).rows;
            for (0..bucket_rows) |i| {
                refs[k] = @intCast((w << REF_ROW_BITS) | i);
                for (sort_cols, 0..) |sc, kc| {
                    const v = views[sc.col * ex.n_workers + w];
                    const valid = v.isValid(i);
                    keys[kc * total + k] = switch (sc.kind) {
                        .string => blk: {
                            const s: []const u8 = if (valid) stringViewOf(v).rowBytes(i) else "";
                            break :blk .{ .norm = normStrPrefix(valid, s), .str = s };
                        },
                        .int32 => .{ .norm = normI32(valid, switch (v.data) {
                            .int => |s| s[i],
                            .date => |s| s[i],
                            else => return error.UnsupportedQueryShape,
                        }), .str = "" },
                        .int64 => .{ .norm = normI64(valid, switch (v.data) {
                            .bigint => |s| s[i],
                            .datetime => |s| s[i],
                            else => return error.UnsupportedQueryShape,
                        }), .str = "" },
                    };
                }
                k += 1;
            }
        }
    }

    const Ctx = struct {
        keys: []const RowKey,
        n_sort: usize,
        total: usize,
        fn less(c: @This(), x: u32, y: u32) bool {
            for (0..c.n_sort) |kc| {
                const a = c.keys[kc * c.total + x];
                const b = c.keys[kc * c.total + y];
                if (a.norm != b.norm) return a.norm < b.norm;
                if (a.str.len != 0 or b.str.len != 0) {
                    switch (std.mem.order(u8, a.str, b.str)) {
                        .lt => return true,
                        .gt => return false,
                        .eq => {},
                    }
                }
            }
            return x < y; // stable tie-break on arrival order
        }
        fn sameGroup(c: @This(), prefix: usize, x: u32, y: u32) bool {
            for (0..prefix) |kc| {
                const a = c.keys[kc * c.total + x];
                const b = c.keys[kc * c.total + y];
                if (a.norm != b.norm) return false;
                if ((a.str.len != 0 or b.str.len != 0) and !std.mem.eql(u8, a.str, b.str)) return false;
            }
            return true;
        }
    };
    const ctx = Ctx{ .keys = keys, .n_sort = sort_cols.len, .total = total };

    const order = try scratch.alloc(u32, total);
    for (order, 0..) |*o, i| o.* = @intCast(i);
    std.mem.sortUnstable(u32, order, ctx, Ctx.less);

    // Single ordered gather, group ranges recorded as we go.
    const grefs = try scratch.alloc(u32, total);
    for (grefs, order) |*g, oi| g.* = refs[oi];

    var g_start: usize = 0;
    while (g_start < total) {
        var g_end = g_start + 1;
        while (g_end < total and ctx.sameGroup(group_prefix, order[g_start], order[g_end])) g_end += 1;
        const base: u32 = @intCast(out.cols[0].rowCount());
        for (out.cols, 0..) |*dst, ci| {
            try gatherColumn(ex.alloc, dst, views[ci * ex.n_workers ..][0..ex.n_workers], grefs[g_start..g_end]);
        }
        try out.ranges.append(ex.alloc, .{ base, @intCast(out.cols[0].rowCount()) });
        g_start = g_end;
    }
    out.rows = out.cols[0].rowCount();
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

const testing = std.testing;

test "region exchange + ordered consolidation: multiset, order, group ranges" {
    const alloc = testing.allocator;
    const schema = [_]Column{
        .{ .name = "k", .type = .string, .nullable = true },
        .{ .name = "g", .type = .int, .nullable = true },
        .{ .name = "v", .type = .bigint, .nullable = true },
    };

    var ex = try Exchange.init(alloc, &schema, 2, 4, 0);
    defer ex.deinit();

    // Two workers push interleaved rows for 4 keys (incl. a NULL key).
    const K = [_]?[]const u8{ "cust_b", "cust_a", null, "cust_a", "whale", "cust_b", "whale", "whale" };
    const G = [_]?i32{ 2, 1, 5, 1, 9, 1, 9, 8 };
    const V = [_]?i64{ 10, 20, 30, 40, 50, 60, 70, 80 };

    for (0..2) |w| {
        var kc = try ColumnStore.init(alloc, .string, true);
        defer kc.deinit(alloc);
        var gc = try ColumnStore.init(alloc, .int, true);
        defer gc.deinit(alloc);
        var vc = try ColumnStore.init(alloc, .bigint, true);
        defer vc.deinit(alloc);
        var n: u32 = 0;
        var i: usize = w;
        while (i < K.len) : (i += 2) {
            if (K[i]) |s| {
                switch (kc.data) {
                    .string => |*d| try d.appendValue(alloc, s),
                    else => unreachable,
                }
                try kc.appendValidBit(alloc, kc.rowCount() - 1, true);
            } else try kc.appendNulls(alloc, 1);
            if (G[i]) |x| {
                try gc.data.int.append(alloc, x);
                try gc.appendValidBit(alloc, gc.rowCount() - 1, true);
            } else try gc.appendNulls(alloc, 1);
            if (V[i]) |x| {
                try vc.data.bigint.append(alloc, x);
                try vc.appendValidBit(alloc, vc.rowCount() - 1, true);
            } else try vc.appendNulls(alloc, 1);
            n += 1;
        }
        var views = [_]ColumnView{ kc.view(), gc.view(), vc.view() };
        var wk = try ex.worker(w);
        defer wk.deinit();
        try wk.push(.{ .schema = &schema, .values = &views, .row_count = n });
    }

    // Consolidate every shard ordered by (k, g), grouped by k.
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const sort_cols = [_]OrderCol{
        .{ .col = 0, .kind = .string },
        .{ .col = 1, .kind = .int32 },
    };

    var sum_v: i64 = 0;
    var total_rows: usize = 0;
    var total_groups: usize = 0;
    const order = try ex.shardOrderDesc(alloc);
    defer alloc.free(order);

    for (order) |s| {
        var sd = ShardData{};
        defer sd.deinit(alloc);
        _ = arena.reset(.retain_capacity);
        try consolidateOrdered(&ex, s, &sort_cols, 1, &sd, arena.allocator());
        total_rows += sd.rows;
        total_groups += sd.ranges.items.len;

        const kv = sd.cols[0].view();
        const gv = sd.cols[1].view();
        const vv = sd.cols[2].view();
        for (0..sd.rows) |i| {
            if (vv.isValid(i)) sum_v += vv.data.bigint[i];
        }
        // Within each group: same key, nondecreasing g.
        for (sd.ranges.items) |rng| {
            const first_valid = kv.isValid(rng[0]);
            const first: []const u8 = if (first_valid) stringViewOf(kv).rowBytes(rng[0]) else "";
            var prev_g: i32 = std.math.minInt(i32);
            for (rng[0]..rng[1]) |i| {
                try testing.expectEqual(first_valid, kv.isValid(i));
                if (first_valid) try testing.expectEqualStrings(first, stringViewOf(kv).rowBytes(i));
                if (gv.isValid(i)) {
                    try testing.expect(gv.data.int[i] >= prev_g);
                    prev_g = gv.data.int[i];
                }
            }
        }
    }

    try testing.expectEqual(@as(usize, 8), total_rows);
    try testing.expectEqual(@as(usize, 4), total_groups); // cust_a, cust_b, whale, NULL
    try testing.expectEqual(@as(i64, 360), sum_v);
}

test "appendStoreRange: contiguous copy preserves values and validity" {
    const alloc = testing.allocator;
    var src = try ColumnStore.init(alloc, .bigint, true);
    defer src.deinit(alloc);
    for (0..10) |i| {
        if (i % 3 == 0) {
            try src.appendNulls(alloc, 1);
        } else {
            try src.data.bigint.append(alloc, @intCast(i * 100));
            try src.appendValidBit(alloc, src.rowCount() - 1, true);
        }
    }
    var dst = try ColumnStore.init(alloc, .bigint, true);
    defer dst.deinit(alloc);
    try appendStoreRange(alloc, &dst, &src, 2, 7);
    try testing.expectEqual(@as(usize, 5), dst.rowCount());
    const dv = dst.view();
    const sv = src.view();
    for (0..5) |i| {
        try testing.expectEqual(sv.isValid(i + 2), dv.isValid(i));
        if (dv.isValid(i)) try testing.expectEqual(sv.data.bigint[i + 2], dv.data.bigint[i]);
    }
}
