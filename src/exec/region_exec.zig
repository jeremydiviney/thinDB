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
const compute_mod = @import("compute.zig");
const single_batch = @import("single_batch.zig");
const udf_mod = @import("../udf.zig");

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
    return appendViewRange(alloc, dst, src.view(), start, end);
}

/// Range append from a view (same contract as `appendStoreRange`).
pub fn appendViewRange(alloc: Allocator, dst: *ColumnStore, v: ColumnView, start: usize, end: usize) !void {
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

/// Append row `i` of `v` onto `dst` (typed single-row append; group-agg and
/// probe emission). `dst`'s data variant must match `v`'s — the program
/// builder validates column types, so a mismatch is a compile bug upstream.
pub fn appendRowValue(alloc: Allocator, dst: *ColumnStore, v: ColumnView, i: usize) !void {
    if (!v.isValid(i)) return dst.appendNulls(alloc, 1);
    switch (v.data) {
        inline .tinyint, .smallint, .int, .bigint, .largeint, .float, .double, .date, .datetime, .decimal64, .decimal128 => |s, tag| {
            try @field(dst.data, @tagName(tag)).append(alloc, s[i]);
        },
        .varchar, .string, .char, .json => |s| switch (dst.data) {
            .varchar, .string, .char, .json => |*d| try d.appendValue(alloc, s.rowBytes(i)),
            else => unreachable,
        },
        else => return error.UnsupportedQueryShape,
    }
    if (dst.nulls != null) try dst.appendValidBit(alloc, dst.rowCount() - 1, true);
}

/// Append `count` copies of row `row` of `v` onto `dst` (fill_last emission).
fn appendRepeat(alloc: Allocator, dst: *ColumnStore, v: ColumnView, row: usize, count: usize) !void {
    if (!v.isValid(row)) return dst.appendNulls(alloc, count);
    switch (v.data) {
        inline .tinyint, .smallint, .int, .bigint, .largeint, .float, .double, .date, .datetime, .decimal64, .decimal128 => |s, tag| {
            const l = &@field(dst.data, @tagName(tag));
            try l.ensureUnusedCapacity(alloc, count);
            l.appendNTimesAssumeCapacity(s[row], count);
        },
        .varchar, .string, .char, .json => |s| switch (dst.data) {
            .varchar, .string, .char, .json => |*d| {
                const bytes = s.rowBytes(row);
                try d.ensureUnusedValueCapacity(alloc, count, bytes.len * count);
                for (0..count) |_| d.appendValueAssumeCapacity(bytes);
            },
            else => unreachable,
        },
        else => return error.UnsupportedQueryShape,
    }
    if (dst.nulls != null) {
        const base = dst.rowCount() - count;
        for (0..count) |k| try dst.appendValidBit(alloc, base + k, true);
    }
}

/// Integer-family read (program builder guarantees the column type).
fn viewI64(v: ColumnView, i: usize) ?i64 {
    if (!v.isValid(i)) return null;
    return switch (v.data) {
        .tinyint => |s| s[i],
        .smallint => |s| s[i],
        .int => |s| s[i],
        .bigint => |s| s[i],
        .date => |s| s[i],
        .datetime => |s| s[i],
        else => null,
    };
}

/// Engine comparison dialect for one column: NULLs first (below every
/// non-null value); bytewise for strings.
fn viewOrderRows(v: ColumnView, a: usize, b: usize) std.math.Order {
    const av = v.isValid(a);
    const bv = v.isValid(b);
    if (!av or !bv) {
        if (av == bv) return .eq;
        return if (!av) .lt else .gt;
    }
    return switch (v.data) {
        inline .tinyint, .smallint, .int, .bigint, .largeint, .float, .double, .date, .datetime, .decimal64, .decimal128 => |s| std.math.order(s[a], s[b]),
        .varchar, .string, .char, .json => |s| std.mem.order(u8, s.rowBytes(a), s.rowBytes(b)),
        else => .eq,
    };
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

/// Per-group hook run during consolidation, after a group's rows land in
/// `out` (rows [g_start..current)) and before its range closes — the hook
/// may append more rows (a union-append TVF's estimates land at the group
/// tail during the ONE gather, never via a second range copy).
pub const GroupTail = struct {
    ctx: *anyopaque,
    run: *const fn (ctx: *anyopaque, out: *ShardData, g_start: u32) anyerror!void,
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
    return consolidateOrderedTail(ex, shard, sort_cols, group_prefix, out, scratch, null);
}

pub fn consolidateOrderedTail(
    ex: *Exchange,
    shard: usize,
    sort_cols: []const OrderCol,
    group_prefix: usize,
    out: *ShardData,
    scratch: Allocator,
    tail: ?GroupTail,
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
        if (tail) |t| try t.run(t.ctx, out, base);
        try out.ranges.append(ex.alloc, .{ base, @intCast(out.cols[0].rowCount()) });
        g_start = g_end;
    }
    out.rows = out.cols[0].rowCount();
}

// ---------------------------------------------------------------------------
// Region program: compiled once, interpreted per shard (control plane).
//
// A program is a linear op list over a "frame": the current working set of
// columns (views), a row count, and the region-key group ranges (contiguous
// by construction — consolidateOrdered's contract). Aligned ops append
// columns without moving rows; restructuring ops (group_agg, inner
// hash_probe) replace the frame and rewrite the ranges. There is no
// materialization and no generic operator chain between ops — this IS the
// region's fused path (REGION_PLAN.md §6).
// ---------------------------------------------------------------------------

pub const OrderBy = struct { col: usize, desc: bool = false };

/// One output column of a group_agg op. Aggregation groups are
/// (range × subkeys); accumulators are generic but typed (the acknowledged
/// P0 loss vs bespoke structs). `first` = value at the sub-group's first
/// row in arrival order — covers group keys (constant within the group),
/// sub-keys, and ANY_VALUE semantics in one kind.
pub const AggOut = struct {
    name: []const u8,
    kind: union(enum) {
        first: usize,
        sum_int: usize,
        sum_float: usize,
        min_int: usize,
        max_int: usize,
        max_by: struct { val: usize, ord: usize },
    },
};

/// Broadcast join map: int-family key → row ordinal in the payload views.
pub const KeyMap = std.AutoHashMapUnmanaged(i64, u32);

pub const Payload = struct {
    name: []const u8,
    view: ColumnView,
    out_type: types.Type,
};

pub const RegionOp = union(enum) {
    /// Row-wise derived columns via the engine expression evaluator (one
    /// exec.Compute instance per worker, evalBatch over the whole shard).
    /// Same-named derived columns REPLACE their frame slot, others append —
    /// exec.Compute's contract.
    compute: struct { derived: []const compute_mod.Derived },
    /// ROW_NUMBER over each region-key range: argsort by `order`, ranks
    /// 1..n appended as a non-null bigint column. Stable on arrival order.
    ranks: struct { name: []const u8, order: []const OrderBy },
    /// LAST_VALUE(src) OVER (PARTITION BY region key) with the frame's
    /// current in-range order: the range's last row broadcast to every row.
    fill_last: struct { name: []const u8, src: usize },
    /// Keyed aggregation: groups are (range × subkeys). Output rows are
    /// emitted per range, sub-groups ordered by subkey values ascending
    /// NULLS FIRST; ranges rewritten to the per-range output spans.
    group_agg: struct { subkeys: []const usize, out: []const AggOut },
    /// Broadcast hash join on an int-family probe column. LEFT appends the
    /// payload columns (NULL on miss); INNER additionally drops non-matching
    /// rows (frame restructure, ranges rewritten in place).
    hash_probe: struct {
        probe: usize,
        map: *const KeyMap,
        payload: []const Payload,
        inner: bool,
    },
    /// Row-aligned TVF over the whole shard (rf_currency class): one raw-ABI
    /// call, computed output columns appended to the frame. Zero-copy input —
    /// the kernel's declared input-0 columns are frame views.
    tvf_aligned: TvfSpec,
    /// Per-range TVF (partition = region-key group). `union_append` copies
    /// each range then lets the kernel append its rows at the group tail
    /// (rf_estimates class; kernel output schema == frame schema), so
    /// run-contiguity holds by construction. Otherwise the kernel output
    /// REPLACES the frame (rf_gap_fill class). Ranges rewritten either way.
    tvf_grouped: struct {
        spec: TvfSpec,
        union_append: bool = false,
        /// Kernel-input row selection (the estimates date-window): only rows
        /// with `col` non-null and lo <= value <= hi feed the kernel. On
        /// union_append the filtered-out rows still flow through the copy.
        input_filter: ?struct { col: usize, lo: i64, hi: i64 } = null,
    },
    /// Final projection into the caller's per-shard output stores. Must be
    /// the program's last op.
    emit: struct { cols: []const usize },
};

/// One TVF call site, resolved from a udf.TableEntry by the recognizer.
/// `extra_parts` are prebuilt broadcast partitions (whole lookup tables),
/// passed verbatim to every call — TVF ABI6.
pub const TvfSpec = struct {
    process: udf_mod.TvfProcess,
    user_data: ?*anyopaque = null,
    args: []const ?types.Value = &.{},
    /// Frame columns in the kernel's declared input-0 column order.
    inputs: []const usize,
    extra_parts: []const udf_mod.TvfPartition = &.{},
    /// Kernel output columns (aligned: appended to the frame; grouped
    /// replace: the new frame schema; grouped union_append: must equal the
    /// frame schema — validated at build).
    out: []const Column,
};

const MAX_SUBKEYS = 3;
const SubKey = struct { v: [MAX_SUBKEYS]u64, valid: u8 };
const AccCell = struct { i: i64 = 0, f: f64 = 0, row: u32 = 0, seen: bool = false };

fn requireIntFamily(schema: []const Column, c: usize) !void {
    if (c >= schema.len) return error.UnsupportedQueryShape;
    switch (schema[c].type) {
        .tinyint, .smallint, .int, .bigint, .date, .datetime => {},
        else => return error.UnsupportedQueryShape,
    }
}

fn requireFloatFamily(schema: []const Column, c: usize) !void {
    if (c >= schema.len) return error.UnsupportedQueryShape;
    switch (schema[c].type) {
        .float, .double => {},
        else => return error.UnsupportedQueryShape,
    }
}

pub const Program = struct {
    arena: std.heap.ArenaAllocator,
    /// Borrowed from the builder (recognizer arena / test scope).
    ops: []const RegionOp,
    /// Frame schema before op i; schema_at[ops.len] = final frame.
    schema_at: []const []const Column,
    output_schema: []const Column,
    max_width: usize,
    registry: ?*const udf_mod.UdfRegistry,

    pub fn build(
        base_alloc: Allocator,
        entry_schema: []const Column,
        ops: []const RegionOp,
        registry: ?*const udf_mod.UdfRegistry,
    ) !Program {
        var arena = std.heap.ArenaAllocator.init(base_alloc);
        errdefer arena.deinit();
        const a = arena.allocator();

        const schema_at = try a.alloc([]const Column, ops.len + 1);
        schema_at[0] = try dupeSchema(a, entry_schema);
        var output_schema: []const Column = &.{};
        var max_width: usize = entry_schema.len;

        for (ops, 0..) |op, oi| {
            if (output_schema.len != 0) return error.UnsupportedQueryShape;
            const in = schema_at[oi];
            schema_at[oi + 1] = switch (op) {
                .compute => |c| try computeOutputSchema(base_alloc, a, in, c.derived, registry),
                .ranks => |r| blk: {
                    for (r.order) |ob| if (ob.col >= in.len) return error.UnsupportedQueryShape;
                    break :blk try appendedSchema(a, in, .{
                        .name = try a.dupe(u8, r.name),
                        .type = .bigint,
                        .nullable = false,
                    });
                },
                .fill_last => |f| blk: {
                    if (f.src >= in.len) return error.UnsupportedQueryShape;
                    break :blk try appendedSchema(a, in, .{
                        .name = try a.dupe(u8, f.name),
                        .type = in[f.src].type,
                        .nullable = true,
                    });
                },
                .group_agg => |g| blk: {
                    if (g.subkeys.len > MAX_SUBKEYS or g.out.len == 0) return error.UnsupportedQueryShape;
                    for (g.subkeys) |c| try requireIntFamily(in, c);
                    const cols = try a.alloc(Column, g.out.len);
                    for (g.out, cols) |spec, *col| {
                        const t: types.Type = switch (spec.kind) {
                            .first => |c| in[checkCol(in, c) catch return error.UnsupportedQueryShape].type,
                            .sum_int => |c| t: {
                                try requireIntFamily(in, c);
                                break :t .bigint;
                            },
                            .sum_float => |c| t: {
                                try requireFloatFamily(in, c);
                                break :t .double;
                            },
                            .min_int, .max_int => |c| t: {
                                try requireIntFamily(in, c);
                                break :t in[c].type;
                            },
                            .max_by => |mb| t: {
                                try requireIntFamily(in, mb.ord);
                                break :t in[checkCol(in, mb.val) catch return error.UnsupportedQueryShape].type;
                            },
                        };
                        col.* = .{ .name = try a.dupe(u8, spec.name), .type = t, .nullable = true };
                    }
                    break :blk cols;
                },
                .hash_probe => |h| blk: {
                    try requireIntFamily(in, h.probe);
                    const cols = try a.alloc(Column, in.len + h.payload.len);
                    @memcpy(cols[0..in.len], in);
                    for (h.payload, cols[in.len..]) |p, *col| {
                        col.* = .{ .name = try a.dupe(u8, p.name), .type = p.out_type, .nullable = true };
                    }
                    break :blk cols;
                },
                .tvf_aligned => |t| blk: {
                    for (t.inputs) |c| _ = try checkCol(in, c);
                    const cols = try a.alloc(Column, in.len + t.out.len);
                    @memcpy(cols[0..in.len], in);
                    for (t.out, cols[in.len..]) |src, *col| {
                        col.* = src;
                        col.name = try a.dupe(u8, src.name);
                    }
                    break :blk cols;
                },
                .tvf_grouped => |t| blk: {
                    for (t.spec.inputs) |c| _ = try checkCol(in, c);
                    if (t.input_filter) |f| try requireIntFamily(in, f.col);
                    if (t.union_append) {
                        if (t.spec.out.len != in.len) return error.UnsupportedQueryShape;
                        for (t.spec.out, in) |o, in_col| {
                            if (!std.meta.eql(o.type, in_col.type)) return error.UnsupportedQueryShape;
                        }
                        break :blk in;
                    }
                    break :blk try dupeSchema(a, t.spec.out);
                },
                .emit => |e| blk: {
                    const cols = try a.alloc(Column, e.cols.len);
                    for (e.cols, cols) |c, *col| {
                        col.* = in[checkCol(in, c) catch return error.UnsupportedQueryShape];
                    }
                    output_schema = cols;
                    break :blk in;
                },
            };
            max_width = @max(max_width, schema_at[oi + 1].len);
        }
        if (output_schema.len == 0) return error.UnsupportedQueryShape;

        return .{
            .arena = arena,
            .ops = ops,
            .schema_at = schema_at,
            .output_schema = output_schema,
            .max_width = max_width,
            .registry = registry,
        };
    }

    pub fn deinit(self: *Program) void {
        self.arena.deinit();
        self.* = undefined;
    }
};

fn checkCol(schema: []const Column, c: usize) !usize {
    if (c >= schema.len) return error.UnsupportedQueryShape;
    return c;
}

fn dupeSchema(a: Allocator, schema: []const Column) ![]Column {
    const out = try a.alloc(Column, schema.len);
    for (schema, out) |src, *dst| {
        dst.* = src;
        dst.name = try a.dupe(u8, src.name);
    }
    return out;
}

fn appendedSchema(a: Allocator, in: []const Column, col: Column) ![]Column {
    const out = try a.alloc(Column, in.len + 1);
    @memcpy(out[0..in.len], in);
    out[in.len] = col;
    return out;
}

/// Resolve a compute op's output schema by instantiating a throwaway
/// exec.Compute against the frame schema (the same resolution the per-worker
/// instances do at runtime, so the two can never disagree).
fn computeOutputSchema(
    gpa: Allocator,
    a: Allocator,
    in: []const Column,
    derived: []const compute_mod.Derived,
    registry: ?*const udf_mod.UdfRegistry,
) ![]const Column {
    var inst = try makeComputeInstance(gpa, in, derived, registry);
    defer inst.q.deinit();
    return dupeSchema(a, inst.ptr.output_schema);
}

const ComputeInstance = struct { q: exec.Query, ptr: *compute_mod.Compute };

/// A Compute over a zero-row SingleBatchSource: never pulled as a Query —
/// the interpreter drives it via evalBatch, the source exists to carry the
/// frame schema through create-time resolution. `schema` must outlive the
/// instance.
fn makeComputeInstance(
    gpa: Allocator,
    schema: []const Column,
    derived: []const compute_mod.Derived,
    registry: ?*const udf_mod.UdfRegistry,
) !ComputeInstance {
    const src = try single_batch.SingleBatchSource.create(gpa, .{
        .schema = schema,
        .values = &.{},
        .row_count = 0,
    });
    const q = try compute_mod.Compute.createWithRegistry(gpa, src, derived, registry);
    const ptr = exec.queryAs(compute_mod.Compute, q) orelse return error.UnsupportedQueryShape;
    return .{ .q = q, .ptr = ptr };
}

/// Per-worker interpreter state: every op's working stores are pooled for
/// the worker's lifetime (clear per shard, never free — the slab-pool
/// discipline). One worker runs many shards.
pub const RegionWorker = struct {
    alloc: Allocator,
    prog: *const Program,
    states: []OpState,
    /// Per-shard scratch (argsort buffers, keep lists); reset per shard.
    scratch: std.heap.ArenaAllocator,
    views: []ColumnView,

    const OpState = union(enum) {
        compute: ComputeInstance,
        ranks: struct { out: ColumnStore },
        fill_last: struct { out: ColumnStore },
        group_agg: struct {
            cols: []ColumnStore,
            ranges: std.ArrayListUnmanaged([2]u32) = .empty,
            cells: []std.ArrayListUnmanaged(AccCell),
            sub_first: std.ArrayListUnmanaged(u32) = .empty,
            map: std.AutoHashMapUnmanaged(SubKey, u32) = .empty,
        },
        hash_probe: struct {
            /// Full-frame compaction stores (inner only; empty for LEFT).
            cols: []ColumnStore,
            pay: []ColumnStore,
            ranges: std.ArrayListUnmanaged([2]u32) = .empty,
        },
        tvf: TvfState,
        emit: void,
    };

    /// Shared state shape for both TVF op kinds. The worker arena + state
    /// slot live for the worker (a kernel builds its lookup tables once per
    /// worker — TVF ABI6); the call arena resets per process() call.
    const TvfState = struct {
        out: []ColumnStore,
        /// Kernel-visible input copies for grouped calls (range offsets have
        /// no zero-copy view — the proven contiguous-copy pattern). Empty
        /// for aligned calls (whole-shard views are zero-copy).
        in_scratch: []ColumnStore,
        ranges: std.ArrayListUnmanaged([2]u32) = .empty,
        call_arena: std.heap.ArenaAllocator,
        worker_arena: std.heap.ArenaAllocator,
        worker_state: ?*anyopaque = null,
    };

    pub fn init(alloc: Allocator, prog: *const Program) !RegionWorker {
        const states = try alloc.alloc(OpState, prog.ops.len);
        var built: usize = 0;
        errdefer {
            deinitStates(alloc, states[0..built]);
            alloc.free(states);
        }
        for (prog.ops, states, 0..) |op, *st, oi| {
            st.* = switch (op) {
                .compute => |c| .{
                    .compute = try makeComputeInstance(alloc, prog.schema_at[oi], c.derived, prog.registry),
                },
                .ranks => .{ .ranks = .{ .out = try ColumnStore.init(alloc, .bigint, false) } },
                .fill_last => blk: {
                    const col = prog.schema_at[oi + 1][prog.schema_at[oi].len];
                    break :blk .{ .fill_last = .{ .out = try ColumnStore.init(alloc, col.type, true) } };
                },
                .group_agg => |g| blk: {
                    const cols = try initStores(alloc, prog.schema_at[oi + 1]);
                    errdefer freeStores(alloc, cols);
                    const cells = try alloc.alloc(std.ArrayListUnmanaged(AccCell), g.out.len);
                    for (cells) |*l| l.* = .empty;
                    break :blk .{ .group_agg = .{ .cols = cols, .cells = cells } };
                },
                .hash_probe => |h| blk: {
                    const out_schema = prog.schema_at[oi + 1];
                    const n_in = out_schema.len - h.payload.len;
                    const pay = try initStores(alloc, out_schema[n_in..]);
                    errdefer freeStores(alloc, pay);
                    const cols: []ColumnStore = if (h.inner)
                        try initStores(alloc, out_schema[0..n_in])
                    else
                        try alloc.alloc(ColumnStore, 0);
                    break :blk .{ .hash_probe = .{ .cols = cols, .pay = pay } };
                },
                .tvf_aligned => blk: {
                    const out = try initStores(alloc, prog.schema_at[oi + 1][prog.schema_at[oi].len..]);
                    errdefer freeStores(alloc, out);
                    break :blk .{ .tvf = .{
                        .out = out,
                        .in_scratch = try alloc.alloc(ColumnStore, 0),
                        .call_arena = std.heap.ArenaAllocator.init(alloc),
                        .worker_arena = std.heap.ArenaAllocator.init(alloc),
                    } };
                },
                .tvf_grouped => |t| blk: {
                    const in_schema = prog.schema_at[oi];
                    const out_schema = if (t.union_append) in_schema else prog.schema_at[oi + 1];
                    const out = try initStores(alloc, out_schema);
                    errdefer freeStores(alloc, out);
                    const in_scratch = try alloc.alloc(ColumnStore, t.spec.inputs.len);
                    var inited: usize = 0;
                    errdefer {
                        for (in_scratch[0..inited]) |*c| c.deinit(alloc);
                        alloc.free(in_scratch);
                    }
                    for (in_scratch, t.spec.inputs) |*c, ci| {
                        c.* = try ColumnStore.init(alloc, in_schema[ci].type, in_schema[ci].nullable);
                        inited += 1;
                    }
                    break :blk .{ .tvf = .{
                        .out = out,
                        .in_scratch = in_scratch,
                        .call_arena = std.heap.ArenaAllocator.init(alloc),
                        .worker_arena = std.heap.ArenaAllocator.init(alloc),
                    } };
                },
                .emit => .{ .emit = {} },
            };
            built += 1;
        }
        return .{
            .alloc = alloc,
            .prog = prog,
            .states = states,
            .scratch = std.heap.ArenaAllocator.init(alloc),
            .views = try alloc.alloc(ColumnView, prog.max_width),
        };
    }

    pub fn deinit(self: *RegionWorker) void {
        deinitStates(self.alloc, self.states);
        self.alloc.free(self.states);
        self.alloc.free(self.views);
        self.scratch.deinit();
        self.* = undefined;
    }

    fn initStores(alloc: Allocator, schema: []const Column) ![]ColumnStore {
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
        return cols;
    }

    fn freeStores(alloc: Allocator, cols: []ColumnStore) void {
        for (cols) |*c| c.deinit(alloc);
        alloc.free(cols);
    }

    fn deinitStates(alloc: Allocator, states: []OpState) void {
        for (states) |*st| switch (st.*) {
            .compute => |*c| c.q.deinit(),
            .ranks => |*s| s.out.deinit(alloc),
            .fill_last => |*s| s.out.deinit(alloc),
            .group_agg => |*s| {
                freeStores(alloc, s.cols);
                for (s.cells) |*l| l.deinit(alloc);
                alloc.free(s.cells);
                s.ranges.deinit(alloc);
                s.sub_first.deinit(alloc);
                s.map.deinit(alloc);
            },
            .hash_probe => |*s| {
                freeStores(alloc, s.cols);
                freeStores(alloc, s.pay);
                s.ranges.deinit(alloc);
            },
            .tvf => |*s| {
                freeStores(alloc, s.out);
                freeStores(alloc, s.in_scratch);
                s.ranges.deinit(alloc);
                s.call_arena.deinit();
                s.worker_arena.deinit();
            },
            .emit => {},
        };
    }

    const Frame = struct {
        views: []ColumnView,
        width: usize,
        rows: usize,
        ranges: []const [2]u32,
    };

    /// Run the program over one consolidated shard, appending the emitted
    /// rows onto `out` (one store per program output column, caller-owned).
    pub fn runShard(self: *RegionWorker, sd: *const ShardData, out: []ColumnStore) !void {
        return self.runShardFrom(sd, out, 0);
    }

    /// `start_op` > 0 when leading ops were pre-applied during consolidation
    /// (the union-append tail fusion): the shard data already carries their
    /// output and the frame schema at start_op equals the entry schema.
    pub fn runShardFrom(self: *RegionWorker, sd: *const ShardData, out: []ColumnStore, start_op: usize) !void {
        if (sd.rows == 0) return;
        defer _ = self.scratch.reset(.retain_capacity);

        var fr = Frame{
            .views = self.views,
            .width = self.prog.schema_at[start_op].len,
            .rows = sd.rows,
            .ranges = sd.ranges.items,
        };
        for (sd.cols, fr.views[0..fr.width]) |*c, *v| v.* = c.view();

        for (self.prog.ops[start_op..], self.states[start_op..], start_op..) |op, *st, oi| {
            switch (op) {
                .compute => {
                    const b = try st.compute.ptr.evalBatch(.{
                        .schema = self.prog.schema_at[oi],
                        .values = fr.views[0..fr.width],
                        .row_count = fr.rows,
                    });
                    @memcpy(fr.views[0..b.values.len], b.values);
                    fr.width = b.values.len;
                },
                .ranks => |r| try self.runRanks(r, &st.ranks.out, &fr),
                .fill_last => |f| {
                    const s = &st.fill_last;
                    s.out.clear();
                    for (fr.ranges) |rng| {
                        if (rng[1] == rng[0]) continue;
                        try appendRepeat(self.alloc, &s.out, fr.views[f.src], rng[1] - 1, rng[1] - rng[0]);
                    }
                    fr.views[fr.width] = s.out.view();
                    fr.width += 1;
                },
                .group_agg => |g| try self.runGroupAgg(g, &st.group_agg, &fr),
                .hash_probe => |h| try self.runHashProbe(h, &st.hash_probe, &fr),
                .tvf_aligned => |t| try self.runTvfAligned(t, &st.tvf, &fr),
                .tvf_grouped => |t| try self.runTvfGrouped(t, &st.tvf, &fr),
                .emit => |e| {
                    for (e.cols, out) |c, *dst| {
                        try appendViewRange(self.alloc, dst, fr.views[c], 0, fr.rows);
                    }
                },
            }
        }
    }

    const RankCtx = struct {
        views: []const ColumnView,
        order: []const OrderBy,
        base: u32,

        fn less(ctx: @This(), x: u32, y: u32) bool {
            for (ctx.order) |ob| {
                var o = viewOrderRows(ctx.views[ob.col], ctx.base + x, ctx.base + y);
                if (ob.desc) o = o.invert();
                if (o != .eq) return o == .lt;
            }
            return x < y;
        }
    };

    fn runRanks(
        self: *RegionWorker,
        r: anytype,
        store: *ColumnStore,
        fr: *Frame,
    ) !void {
        store.clear();
        const out_slice = try store.data.bigint.addManyAsSlice(self.alloc, fr.rows);
        const sa = self.scratch.allocator();
        for (fr.ranges) |rng| {
            const n = rng[1] - rng[0];
            if (n == 0) continue;
            const ord = try sa.alloc(u32, n);
            for (ord, 0..) |*o, k| o.* = @intCast(k);
            const ctx = RankCtx{ .views = fr.views[0..fr.width], .order = r.order, .base = rng[0] };
            std.mem.sortUnstable(u32, ord, ctx, RankCtx.less);
            for (ord, 1..) |li, rk| out_slice[rng[0] + li] = @intCast(rk);
        }
        fr.views[fr.width] = store.view();
        fr.width += 1;
    }

    fn makeSubKey(subkeys: []const usize, views: []const ColumnView, i: usize) SubKey {
        var k = SubKey{ .v = @splat(0), .valid = 0 };
        for (subkeys, 0..) |c, j| {
            if (viewI64(views[c], i)) |val| {
                k.v[j] = @bitCast(val);
                k.valid |= @as(u8, 1) << @intCast(j);
            }
        }
        return k;
    }

    const SubOrdCtx = struct {
        views: []const ColumnView,
        subkeys: []const usize,
        first: []const u32,

        fn less(ctx: @This(), x: u32, y: u32) bool {
            for (ctx.subkeys) |c| {
                const o = viewOrderRows(ctx.views[c], ctx.first[x], ctx.first[y]);
                if (o != .eq) return o == .lt;
            }
            return x < y;
        }
    };

    fn runGroupAgg(self: *RegionWorker, g: anytype, s: anytype, fr: *Frame) !void {
        const alloc = self.alloc;
        for (s.cols) |*c| c.clear();
        s.ranges.clearRetainingCapacity();
        const sa = self.scratch.allocator();

        for (fr.ranges) |rng| {
            s.map.clearRetainingCapacity();
            s.sub_first.clearRetainingCapacity();
            for (s.cells) |*l| l.clearRetainingCapacity();

            // Pass 1 — sub-group assignment only (the hash pass).
            const ord_of = try sa.alloc(u32, rng[1] - rng[0]);
            for (rng[0]..rng[1], ord_of) |i, *slot| {
                const key = makeSubKey(g.subkeys, fr.views, i);
                const gop = try s.map.getOrPut(alloc, key);
                if (!gop.found_existing) {
                    gop.value_ptr.* = @intCast(s.sub_first.items.len);
                    try s.sub_first.append(alloc, @intCast(i));
                    for (s.cells) |*l| try l.append(alloc, .{});
                }
                slot.* = gop.value_ptr.*;
            }

            // Pass 2 — one column-at-a-time sweep per agg spec, with the agg
            // kind, value type, and validity presence all hoisted out of the
            // row loop (the silo monomorphization discipline).
            for (g.out, s.cells) |spec, *cells| {
                switch (spec.kind) {
                    .first => {},
                    .sum_int => |c| accumSumInt(cells.items, ord_of, fr.views[c], rng[0]),
                    .sum_float => |c| accumSumFloat(cells.items, ord_of, fr.views[c], rng[0]),
                    .min_int => |c| accumMinMax(false, cells.items, ord_of, fr.views[c], rng[0]),
                    .max_int => |c| accumMinMax(true, cells.items, ord_of, fr.views[c], rng[0]),
                    .max_by => |mb| accumMaxBy(cells.items, ord_of, fr.views[mb.ord], rng[0]),
                }
            }

            const nsub = s.sub_first.items.len;
            const sord = try sa.alloc(u32, nsub);
            for (sord, 0..) |*o, k| o.* = @intCast(k);
            if (g.subkeys.len > 0) {
                const ctx = SubOrdCtx{
                    .views = fr.views[0..fr.width],
                    .subkeys = g.subkeys,
                    .first = s.sub_first.items,
                };
                std.mem.sortUnstable(u32, sord, ctx, SubOrdCtx.less);
            }

            const start: u32 = @intCast(if (s.cols.len > 0) s.cols[0].rowCount() else 0);
            for (sord) |gi| {
                for (g.out, s.cells, s.cols) |spec, *cells, *dst| {
                    const cell = &cells.items[gi];
                    switch (spec.kind) {
                        .first => |c| try appendRowValue(alloc, dst, fr.views[c], s.sub_first.items[gi]),
                        .sum_int => if (cell.seen) {
                            try dst.data.bigint.append(alloc, cell.i);
                            try dst.appendValidBit(alloc, dst.rowCount() - 1, true);
                        } else try dst.appendNulls(alloc, 1),
                        .sum_float => if (cell.seen) {
                            try dst.data.double.append(alloc, cell.f);
                            try dst.appendValidBit(alloc, dst.rowCount() - 1, true);
                        } else try dst.appendNulls(alloc, 1),
                        .min_int, .max_int => if (cell.seen) {
                            try appendI64As(alloc, dst, cell.i);
                        } else try dst.appendNulls(alloc, 1),
                        .max_by => |mb| if (cell.seen) {
                            try appendRowValue(alloc, dst, fr.views[mb.val], cell.row);
                        } else try dst.appendNulls(alloc, 1),
                    }
                }
            }
            try s.ranges.append(alloc, .{ start, @intCast(s.cols[0].rowCount()) });
        }

        fr.width = s.cols.len;
        for (s.cols, fr.views[0..fr.width]) |*c, *v| v.* = c.view();
        fr.rows = s.cols[0].rowCount();
        fr.ranges = s.ranges.items;
    }

    /// Assemble parts[0] over `in_views` plus the spec's broadcast parts,
    /// call the kernel, and validate rectangular output at `expect_rows`
    /// (null = any row count, but all output columns equal).
    fn callTvf(
        self: *RegionWorker,
        spec: TvfSpec,
        s: *TvfState,
        in_views: []const ColumnView,
        in_rows: usize,
        out_ptrs: []*ColumnStore,
        expect_rows: ?usize,
    ) !void {
        const sa = self.scratch.allocator();
        const parts = try sa.alloc(udf_mod.TvfPartition, 1 + spec.extra_parts.len);
        parts[0] = .{ .columns = in_views, .row_count = in_rows, .keys = &.{} };
        @memcpy(parts[1..], spec.extra_parts);

        var out = udf_mod.TvfOutput{ .columns = out_ptrs, .allocator = self.alloc };
        _ = s.call_arena.reset(.retain_capacity);
        var ctx = udf_mod.TvfContext{
            .arena = s.call_arena.allocator(),
            .user_data = spec.user_data,
            .args = spec.args,
            .worker_arena = s.worker_arena.allocator(),
            .worker_state = &s.worker_state,
        };
        try spec.process(&ctx, parts, &out);

        const rows = out_ptrs[0].rowCount();
        if (expect_rows) |want| {
            if (rows != want) return error.TableFnBadOutput;
        }
        for (out_ptrs[1..]) |c| {
            if (c.rowCount() != rows) return error.TableFnBadOutput;
        }
    }

    fn runTvfAligned(self: *RegionWorker, spec: TvfSpec, s: *TvfState, fr: *Frame) !void {
        for (s.out) |*c| c.clear();
        if (fr.rows > 0) {
            const sa = self.scratch.allocator();
            const in_views = try sa.alloc(ColumnView, spec.inputs.len);
            for (spec.inputs, in_views) |ci, *v| v.* = fr.views[ci];
            const out_ptrs = try sa.alloc(*ColumnStore, s.out.len);
            for (s.out, out_ptrs) |*c, *p| p.* = c;
            try self.callTvf(spec, s, in_views, fr.rows, out_ptrs, fr.rows);
        }
        for (s.out, fr.views[fr.width..][0..s.out.len]) |*c, *v| v.* = c.view();
        fr.width += s.out.len;
    }

    fn runTvfGrouped(self: *RegionWorker, t: anytype, s: *TvfState, fr: *Frame) !void {
        const alloc = self.alloc;
        for (s.out) |*c| c.clear();
        s.ranges.clearRetainingCapacity();
        const sa = self.scratch.allocator();
        const out_ptrs = try sa.alloc(*ColumnStore, s.out.len);
        for (s.out, out_ptrs) |*c, *p| p.* = c;

        for (fr.ranges) |rng| {
            const start: u32 = @intCast(s.out[0].rowCount());
            if (t.union_append) {
                for (s.out, fr.views[0..fr.width]) |*dst, v| {
                    try appendViewRange(alloc, dst, v, rng[0], rng[1]);
                }
            }
            try self.kernelOverRange(t, s, fr.views[0..fr.width], rng[0], rng[1], out_ptrs);
            try s.ranges.append(alloc, .{ start, @intCast(s.out[0].rowCount()) });
        }

        fr.width = s.out.len;
        for (s.out, fr.views[0..fr.width]) |*c, *v| v.* = c.view();
        fr.rows = s.out[0].rowCount();
        fr.ranges = s.ranges.items;
    }

    /// Select the kernel-visible rows of [lo,hi) per the input filter, copy
    /// them contiguous into the op's scratch partition, and call the kernel
    /// appending onto `out_ptrs`. Shared by the unfused per-range path and
    /// the consolidation-tail fusion.
    fn kernelOverRange(
        self: *RegionWorker,
        t: anytype,
        s: *TvfState,
        views: []const ColumnView,
        lo: usize,
        hi: usize,
        out_ptrs: []*ColumnStore,
    ) !void {
        const sa = self.scratch.allocator();
        var kin: std.ArrayListUnmanaged(u32) = .empty;
        if (t.input_filter) |f| {
            for (lo..hi) |i| {
                const v = viewI64(views[f.col], i) orelse continue;
                if (v >= f.lo and v <= f.hi) try kin.append(sa, @intCast(i));
            }
        } else {
            try kin.ensureUnusedCapacity(sa, hi - lo);
            for (lo..hi) |i| kin.appendAssumeCapacity(@intCast(i));
        }
        if (kin.items.len == 0) return;

        for (s.in_scratch) |*c| c.clear();
        for (s.in_scratch, t.spec.inputs) |*dst, ci| {
            try scatterColumn(self.alloc, dst, views[ci], kin.items);
        }
        const in_views = try sa.alloc(ColumnView, t.spec.inputs.len);
        for (s.in_scratch, in_views) |*c, *v| v.* = c.view();
        try self.callTvf(t.spec, s, in_views, kin.items.len, out_ptrs, null);
    }

    /// Lever-1 fusion (general region rule): a program whose FIRST op is a
    /// union-append grouped TVF runs it as a consolidation tail — its rows
    /// land during the ONE gather and the interpreter starts at op 1. The
    /// unfused path in runTvfGrouped remains for mid-program union-appends
    /// and direct runShard callers.
    pub fn fusedFirstTail(self: *RegionWorker) ?GroupTail {
        if (self.prog.ops.len == 0) return null;
        switch (self.prog.ops[0]) {
            .tvf_grouped => |t| if (t.union_append) {
                return .{ .ctx = self, .run = fusedTailRun };
            },
            else => {},
        }
        return null;
    }

    fn fusedTailRun(ctx: *anyopaque, out: *ShardData, g_start: u32) anyerror!void {
        const self: *RegionWorker = @ptrCast(@alignCast(ctx));
        const t = self.prog.ops[0].tvf_grouped;
        const s = &self.states[0].tvf;
        const sa = self.scratch.allocator();
        const width = self.prog.schema_at[0].len;
        // Views taken before the kernel appends (appends may regrow the
        // stores); they are only read during the input copy.
        const views = try sa.alloc(ColumnView, width);
        for (out.cols[0..width], views) |*c, *v| v.* = c.view();
        const out_ptrs = try sa.alloc(*ColumnStore, width);
        for (out.cols[0..width], out_ptrs) |*c, *p| p.* = c;
        try self.kernelOverRange(t, s, views, g_start, out.cols[0].rowCount(), out_ptrs);
    }

    fn runHashProbe(self: *RegionWorker, h: anytype, s: anytype, fr: *Frame) !void {
        const alloc = self.alloc;
        for (s.pay) |*c| c.clear();

        if (!h.inner) {
            for (0..fr.rows) |i| {
                const m: ?u32 = if (viewI64(fr.views[h.probe], i)) |k| h.map.get(k) else null;
                for (h.payload, s.pay) |p, *dst| {
                    if (m) |row| {
                        try appendRowValue(alloc, dst, p.view, row);
                    } else {
                        try dst.appendNulls(alloc, 1);
                    }
                }
            }
            for (s.pay, fr.views[fr.width..][0..s.pay.len]) |*c, *v| v.* = c.view();
            fr.width += s.pay.len;
            return;
        }

        for (s.cols) |*c| c.clear();
        s.ranges.clearRetainingCapacity();
        const sa = self.scratch.allocator();
        var keep: std.ArrayListUnmanaged(u32) = .empty;
        var match: std.ArrayListUnmanaged(u32) = .empty;

        for (fr.ranges) |rng| {
            keep.clearRetainingCapacity();
            match.clearRetainingCapacity();
            for (rng[0]..rng[1]) |i| {
                const k = viewI64(fr.views[h.probe], i) orelse continue;
                const row = h.map.get(k) orelse continue;
                try keep.append(sa, @intCast(i));
                try match.append(sa, row);
            }
            const start: u32 = @intCast(s.cols[0].rowCount());
            for (s.cols, fr.views[0..fr.width]) |*dst, v| {
                try scatterColumn(alloc, dst, v, keep.items);
            }
            for (h.payload, s.pay) |p, *dst| {
                for (match.items) |row| try appendRowValue(alloc, dst, p.view, row);
            }
            try s.ranges.append(alloc, .{ start, @intCast(s.cols[0].rowCount()) });
        }

        fr.width = s.cols.len + s.pay.len;
        for (s.cols, fr.views[0..s.cols.len]) |*c, *v| v.* = c.view();
        for (s.pay, fr.views[s.cols.len..fr.width]) |*c, *v| v.* = c.view();
        fr.rows = s.cols[0].rowCount();
        fr.ranges = s.ranges.items;
    }
};

// ---------------------------------------------------------------------------
// Region driver: E1 (parallel drain + entry compute + exchange scatter) and
// E2 (whale-first shard claims: consolidate -> program) across T threads.
// E3 (stage adoption) lives with the recognizer in net/cte_stages.
// ---------------------------------------------------------------------------

pub const DriverOpts = struct {
    n_threads: usize,
    n_shards: usize,
    /// Region key column in the ENTRY schema (post entry compute).
    key_col: usize,
    /// Ordering contract applied during consolidation; the leading
    /// `group_prefix` columns define the region-key group ranges.
    sort_cols: []const OrderCol,
    group_prefix: usize,
};

/// Per-shard program outputs. Shards with zero input rows have empty stores.
pub const RegionResult = struct {
    alloc: Allocator,
    schema: []const Column,
    shards: []ShardOut,

    pub const ShardOut = struct {
        cols: []ColumnStore,
        rows: usize = 0,
    };

    pub fn init(alloc: Allocator, schema: []const Column, n_shards: usize) !RegionResult {
        const shards = try alloc.alloc(ShardOut, n_shards);
        var built: usize = 0;
        errdefer {
            for (shards[0..built]) |*s| {
                for (s.cols) |*c| c.deinit(alloc);
                alloc.free(s.cols);
            }
            alloc.free(shards);
        }
        for (shards) |*s| {
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
            s.* = .{ .cols = cols };
            built += 1;
        }
        return .{ .alloc = alloc, .schema = schema, .shards = shards };
    }

    pub fn deinit(self: *RegionResult) void {
        for (self.shards) |*s| {
            for (s.cols) |*c| c.deinit(self.alloc);
            self.alloc.free(s.cols);
        }
        self.alloc.free(self.shards);
        self.* = undefined;
    }

    pub fn totalRows(self: *const RegionResult) usize {
        var n: usize = 0;
        for (self.shards) |s| n += s.rows;
        return n;
    }
};

const ScanPhase = struct {
    ex: *Exchange,
    sources: []exec.Query,
    next: std.atomic.Value(usize) = .init(0),
    entry_derived: []const compute_mod.Derived,
    scan_schema: []const Column,
    registry: ?*const udf_mod.UdfRegistry,
    errs: []?anyerror,

    fn worker(self: *ScanPhase, w: usize) void {
        self.workerInner(w) catch |e| {
            self.errs[w] = e;
        };
    }

    fn workerInner(self: *ScanPhase, w: usize) !void {
        var wk = try self.ex.worker(w);
        defer wk.deinit();
        // Entry compute (e.g. the lowercased key + literal columns) runs
        // per batch through the engine evaluator, one instance per worker.
        var inst: ?ComputeInstance = null;
        defer if (inst) |*i| i.q.deinit();
        if (self.entry_derived.len > 0) {
            inst = try makeComputeInstance(self.ex.alloc, self.scan_schema, self.entry_derived, self.registry);
        }
        while (true) {
            const i = self.next.fetchAdd(1, .monotonic);
            if (i >= self.sources.len) break;
            while (try self.sources[i].next()) |batch| {
                const routed = if (inst) |*ci| try ci.ptr.evalBatch(batch) else batch;
                try wk.push(routed);
            }
        }
    }
};

const ShardPhase = struct {
    ex: *Exchange,
    prog: *const Program,
    opts: DriverOpts,
    order: []const u32,
    next: std.atomic.Value(usize) = .init(0),
    result: *RegionResult,
    errs: []?anyerror,

    fn worker(self: *ShardPhase, w: usize) void {
        self.workerInner(w) catch |e| {
            self.errs[w] = e;
        };
    }

    fn workerInner(self: *ShardPhase, w: usize) !void {
        _ = w;
        const alloc = self.ex.alloc;
        var rw = try RegionWorker.init(alloc, self.prog);
        defer rw.deinit();
        var sd = ShardData{};
        defer sd.deinit(alloc);
        var scratch = std.heap.ArenaAllocator.init(alloc);
        defer scratch.deinit();
        const tail = rw.fusedFirstTail();
        const start_op: usize = if (tail != null) 1 else 0;

        while (true) {
            const oi = self.next.fetchAdd(1, .monotonic);
            if (oi >= self.order.len) break;
            const s = self.order[oi];
            if (self.ex.shardRows(s) == 0) continue;
            _ = scratch.reset(.retain_capacity);
            try consolidateOrderedTail(self.ex, s, self.opts.sort_cols, self.opts.group_prefix, &sd, scratch.allocator(), tail);
            const out = &self.result.shards[s];
            try rw.runShardFrom(&sd, out.cols, start_op);
            out.rows = if (out.cols.len > 0) out.cols[0].rowCount() else 0;
        }
    }
};

/// Run a region end to end: drain `sources` in parallel through the entry
/// compute into a hash exchange on the program's entry schema, then claim
/// shards whale-first, consolidating each in the region's order and running
/// the compiled program shard-locally. `sources` are drained (not deinit'd).
pub fn runRegion(
    alloc: Allocator,
    scan_schema: []const Column,
    sources: []exec.Query,
    entry_derived: []const compute_mod.Derived,
    prog: *const Program,
    opts: DriverOpts,
    result: *RegionResult,
) !void {
    var ex = try Exchange.init(alloc, prog.schema_at[0], opts.n_threads, opts.n_shards, opts.key_col);
    defer ex.deinit();

    const errs = try alloc.alloc(?anyerror, opts.n_threads);
    defer alloc.free(errs);
    @memset(errs, null);
    const threads = try alloc.alloc(std.Thread, opts.n_threads);
    defer alloc.free(threads);

    var scan_phase = ScanPhase{
        .ex = &ex,
        .sources = sources,
        .entry_derived = entry_derived,
        .scan_schema = scan_schema,
        .registry = prog.registry,
        .errs = errs,
    };
    var spawned: usize = 0;
    for (0..opts.n_threads - 1) |w| {
        threads[w] = try std.Thread.spawn(.{}, ScanPhase.worker, .{ &scan_phase, w });
        spawned += 1;
    }
    scan_phase.worker(opts.n_threads - 1);
    for (threads[0..spawned]) |t| t.join();
    for (errs) |e| if (e) |err| return err;

    const order = try ex.shardOrderDesc(alloc);
    defer alloc.free(order);
    @memset(errs, null);
    var shard_phase = ShardPhase{
        .ex = &ex,
        .prog = prog,
        .opts = opts,
        .order = order,
        .result = result,
        .errs = errs,
    };
    spawned = 0;
    for (0..opts.n_threads - 1) |w| {
        threads[w] = try std.Thread.spawn(.{}, ShardPhase.worker, .{ &shard_phase, w });
        spawned += 1;
    }
    shard_phase.worker(opts.n_threads - 1);
    for (threads[0..spawned]) |t| t.join();
    for (errs) |e| if (e) |err| return err;
}

// Group-agg accumulate sweeps: one call per (spec, range). The value-type
// switch runs once (inline arms monomorphize the loop per type) and the
// no-validity fast path skips the per-row bit check. The `unreachable`s
// hold by construction — Program.build rejects columns outside the family.

fn accumSumInt(cells: []AccCell, ord_of: []const u32, v: ColumnView, base: usize) void {
    switch (v.data) {
        inline .tinyint, .smallint, .int, .bigint, .date, .datetime => |vals| {
            if (v.nulls == null) {
                for (ord_of, vals[base..][0..ord_of.len]) |gi, x| {
                    cells[gi].i += x;
                    cells[gi].seen = true;
                }
            } else {
                for (ord_of, 0..) |gi, li| {
                    if (!v.isValid(base + li)) continue;
                    cells[gi].i += vals[base + li];
                    cells[gi].seen = true;
                }
            }
        },
        else => unreachable,
    }
}

fn accumSumFloat(cells: []AccCell, ord_of: []const u32, v: ColumnView, base: usize) void {
    switch (v.data) {
        inline .float, .double => |vals| {
            if (v.nulls == null) {
                for (ord_of, vals[base..][0..ord_of.len]) |gi, x| {
                    cells[gi].f += x;
                    cells[gi].seen = true;
                }
            } else {
                for (ord_of, 0..) |gi, li| {
                    if (!v.isValid(base + li)) continue;
                    cells[gi].f += vals[base + li];
                    cells[gi].seen = true;
                }
            }
        },
        else => unreachable,
    }
}

fn accumMinMax(comptime is_max: bool, cells: []AccCell, ord_of: []const u32, v: ColumnView, base: usize) void {
    switch (v.data) {
        inline .tinyint, .smallint, .int, .bigint, .date, .datetime => |vals| {
            for (ord_of, 0..) |gi, li| {
                if (!v.isValid(base + li)) continue;
                const x: i64 = vals[base + li];
                const cell = &cells[gi];
                cell.i = if (!cell.seen) x else if (is_max) @max(cell.i, x) else @min(cell.i, x);
                cell.seen = true;
            }
        },
        else => unreachable,
    }
}

fn accumMaxBy(cells: []AccCell, ord_of: []const u32, ord_v: ColumnView, base: usize) void {
    switch (ord_v.data) {
        inline .tinyint, .smallint, .int, .bigint, .date, .datetime => |vals| {
            for (ord_of, 0..) |gi, li| {
                if (!ord_v.isValid(base + li)) continue;
                const o: i64 = vals[base + li];
                const cell = &cells[gi];
                if (!cell.seen or o > cell.i) {
                    cell.i = o;
                    cell.row = @intCast(base + li);
                    cell.seen = true;
                }
            }
        },
        else => unreachable,
    }
}

fn appendI64As(alloc: Allocator, dst: *ColumnStore, v: i64) !void {
    switch (dst.data) {
        .tinyint => |*l| try l.append(alloc, @intCast(v)),
        .smallint => |*l| try l.append(alloc, @intCast(v)),
        .int => |*l| try l.append(alloc, @intCast(v)),
        .bigint => |*l| try l.append(alloc, v),
        .date => |*l| try l.append(alloc, @intCast(v)),
        .datetime => |*l| try l.append(alloc, v),
        else => return error.UnsupportedQueryShape,
    }
    if (dst.nulls != null) try dst.appendValidBit(alloc, dst.rowCount() - 1, true);
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

fn tAppendStr(alloc: Allocator, store: *ColumnStore, s: ?[]const u8) !void {
    if (s == null) return store.appendNulls(alloc, 1);
    switch (store.data) {
        .varchar, .string, .char, .json => |*d| try d.appendValue(alloc, s.?),
        else => unreachable,
    }
    if (store.nulls != null) try store.appendValidBit(alloc, store.rowCount() - 1, true);
}

fn tAppendInt(alloc: Allocator, store: *ColumnStore, v: ?i32) !void {
    if (v == null) return store.appendNulls(alloc, 1);
    try store.data.int.append(alloc, v.?);
    try store.appendValidBit(alloc, store.rowCount() - 1, true);
}

fn tAppendI64(alloc: Allocator, store: *ColumnStore, v: ?i64) !void {
    if (v == null) return store.appendNulls(alloc, 1);
    try store.data.bigint.append(alloc, v.?);
    try store.appendValidBit(alloc, store.rowCount() - 1, true);
}

/// Two region-key ranges: "a" = rows 0..5, "b" = rows 5..6.
fn tBuildShard(alloc: Allocator, schema: []const Column) !ShardData {
    var sd = ShardData{};
    errdefer sd.deinit(alloc);
    try sd.ensure(alloc, schema);
    const G = [_]?i32{ 2, 1, 2, null, 2, 1 };
    const V = [_]?i64{ 10, 30, null, 5, 20, 7 };
    for (G, V, 0..) |g, v, i| {
        try tAppendStr(alloc, &sd.cols[0], if (i < 5) "a" else "b");
        try tAppendInt(alloc, &sd.cols[1], g);
        try tAppendI64(alloc, &sd.cols[2], v);
    }
    sd.rows = 6;
    try sd.ranges.append(alloc, .{ 0, 5 });
    try sd.ranges.append(alloc, .{ 5, 6 });
    return sd;
}

test "region program: ranks -> group_agg -> fill_last -> left probe -> emit" {
    const alloc = testing.allocator;
    const entry = [_]Column{
        .{ .name = "k", .type = .string, .nullable = true },
        .{ .name = "g", .type = .int, .nullable = true },
        .{ .name = "v", .type = .bigint, .nullable = true },
    };
    var sd = try tBuildShard(alloc, &entry);
    defer sd.deinit(alloc);

    var names = try ColumnStore.init(alloc, .string, true);
    defer names.deinit(alloc);
    try tAppendStr(alloc, &names, "one");
    try tAppendStr(alloc, &names, "two");
    var map = KeyMap.empty;
    defer map.deinit(alloc);
    try map.put(alloc, 1, 0);
    try map.put(alloc, 2, 1);

    const agg_out = [_]AggOut{
        .{ .name = "k", .kind = .{ .first = 0 } },
        .{ .name = "g", .kind = .{ .first = 1 } },
        .{ .name = "sum_v", .kind = .{ .sum_int = 2 } },
        .{ .name = "top_v", .kind = .{ .max_by = .{ .val = 2, .ord = 3 } } },
    };
    const subkeys = [_]usize{1};
    const rank_order = [_]OrderBy{.{ .col = 2 }};
    const payload = [_]Payload{.{ .name = "nm", .view = names.view(), .out_type = .string }};
    const emit_cols = [_]usize{ 0, 1, 2, 3, 4, 5 };
    const ops = [_]RegionOp{
        .{ .ranks = .{ .name = "rn", .order = &rank_order } },
        .{ .group_agg = .{ .subkeys = &subkeys, .out = &agg_out } },
        .{ .fill_last = .{ .name = "ls", .src = 2 } },
        .{ .hash_probe = .{ .probe = 1, .map = &map, .payload = &payload, .inner = false } },
        .{ .emit = .{ .cols = &emit_cols } },
    };

    var prog = try Program.build(alloc, &entry, &ops, null);
    defer prog.deinit();
    try testing.expectEqual(@as(usize, 6), prog.output_schema.len);

    var worker = try RegionWorker.init(alloc, &prog);
    defer worker.deinit();

    const out = try alloc.alloc(ColumnStore, prog.output_schema.len);
    defer {
        for (out) |*c| c.deinit(alloc);
        alloc.free(out);
    }
    for (out, prog.output_schema) |*c, col| c.* = try ColumnStore.init(alloc, col.type, col.nullable);

    try worker.runShard(&sd, out);

    // Sub-groups per range ordered by g NULLS FIRST:
    //   a: (null: sum 5, top 5) (1: 30, 30) (2: 30, top_v = v at max rank = 20)
    //   b: (1: 7, 7)
    const exp_g = [_]?i32{ null, 1, 2, 1 };
    const exp_sum = [_]?i64{ 5, 30, 30, 7 };
    const exp_top = [_]?i64{ 5, 30, 20, 7 };
    const exp_ls = [_]i64{ 30, 30, 30, 7 };
    const exp_nm = [_]?[]const u8{ null, "one", "two", "one" };

    try testing.expectEqual(@as(usize, 4), out[0].rowCount());
    const kv = out[0].view();
    const gv = out[1].view();
    const sv = out[2].view();
    const tv = out[3].view();
    const lv = out[4].view();
    const nv = out[5].view();
    for (0..4) |i| {
        try testing.expectEqualStrings(if (i < 3) "a" else "b", stringViewOf(kv).rowBytes(i));
        if (exp_g[i]) |g| {
            try testing.expectEqual(g, gv.data.int[i]);
        } else try testing.expect(!gv.isValid(i));
        try testing.expectEqual(exp_sum[i].?, sv.data.bigint[i]);
        try testing.expectEqual(exp_top[i].?, tv.data.bigint[i]);
        try testing.expectEqual(exp_ls[i], lv.data.bigint[i]);
        if (exp_nm[i]) |nm| {
            try testing.expectEqualStrings(nm, stringViewOf(nv).rowBytes(i));
        } else try testing.expect(!nv.isValid(i));
    }
}

test "region program: group_agg min/max/sum_float sweeps (validity hoisted)" {
    const alloc = testing.allocator;
    const entry = [_]Column{
        .{ .name = "k", .type = .string, .nullable = true },
        .{ .name = "m", .type = .int, .nullable = true },
        .{ .name = "d", .type = .double, .nullable = true },
        .{ .name = "t", .type = .date, .nullable = true },
    };
    var sd = ShardData{};
    defer sd.deinit(alloc);
    try sd.ensure(alloc, &entry);
    const M = [_]?i32{ 1, 1, 2 };
    const D = [_]?f64{ 1.5, null, 2.5 };
    const T = [_]?i32{ 100, 90, null };
    for (M, D, T) |m, d, t| {
        try tAppendStr(alloc, &sd.cols[0], "a");
        try tAppendInt(alloc, &sd.cols[1], m);
        if (d) |x| {
            try sd.cols[2].data.double.append(alloc, x);
            try sd.cols[2].appendValidBit(alloc, sd.cols[2].rowCount() - 1, true);
        } else try sd.cols[2].appendNulls(alloc, 1);
        if (t) |x| {
            try sd.cols[3].data.date.append(alloc, x);
            try sd.cols[3].appendValidBit(alloc, sd.cols[3].rowCount() - 1, true);
        } else try sd.cols[3].appendNulls(alloc, 1);
    }
    sd.rows = 3;
    try sd.ranges.append(alloc, .{ 0, 3 });

    const agg_out = [_]AggOut{
        .{ .name = "m", .kind = .{ .first = 1 } },
        .{ .name = "sum_d", .kind = .{ .sum_float = 2 } },
        .{ .name = "min_t", .kind = .{ .min_int = 3 } },
        .{ .name = "max_t", .kind = .{ .max_int = 3 } },
    };
    const subkeys = [_]usize{1};
    const emit_cols = [_]usize{ 0, 1, 2, 3 };
    const ops = [_]RegionOp{
        .{ .group_agg = .{ .subkeys = &subkeys, .out = &agg_out } },
        .{ .emit = .{ .cols = &emit_cols } },
    };
    var prog = try Program.build(alloc, &entry, &ops, null);
    defer prog.deinit();
    var worker = try RegionWorker.init(alloc, &prog);
    defer worker.deinit();
    const out = try alloc.alloc(ColumnStore, prog.output_schema.len);
    defer {
        for (out) |*c| c.deinit(alloc);
        alloc.free(out);
    }
    for (out, prog.output_schema) |*c, col| c.* = try ColumnStore.init(alloc, col.type, col.nullable);

    try worker.runShard(&sd, out);

    // Sub-groups by m asc: m=1 -> sum_d 1.5, min_t 90, max_t 100;
    // m=2 -> sum_d 2.5, min/max NULL.
    try testing.expectEqual(@as(usize, 2), out[0].rowCount());
    const sv = out[1].view();
    const mn = out[2].view();
    const mx = out[3].view();
    try testing.expectEqual(@as(f64, 1.5), sv.data.double[0]);
    try testing.expectEqual(@as(i32, 90), mn.data.date[0]);
    try testing.expectEqual(@as(i32, 100), mx.data.date[0]);
    try testing.expectEqual(@as(f64, 2.5), sv.data.double[1]);
    try testing.expect(!mn.isValid(1));
    try testing.expect(!mx.isValid(1));
}

test "region program: inner probe drops non-matching sub-groups" {
    const alloc = testing.allocator;
    const entry = [_]Column{
        .{ .name = "k", .type = .string, .nullable = true },
        .{ .name = "g", .type = .int, .nullable = true },
        .{ .name = "v", .type = .bigint, .nullable = true },
    };
    var sd = try tBuildShard(alloc, &entry);
    defer sd.deinit(alloc);

    var names = try ColumnStore.init(alloc, .string, true);
    defer names.deinit(alloc);
    try tAppendStr(alloc, &names, "one");
    var map = KeyMap.empty;
    defer map.deinit(alloc);
    try map.put(alloc, 1, 0);

    const agg_out = [_]AggOut{
        .{ .name = "k", .kind = .{ .first = 0 } },
        .{ .name = "g", .kind = .{ .first = 1 } },
        .{ .name = "sum_v", .kind = .{ .sum_int = 2 } },
    };
    const subkeys = [_]usize{1};
    const payload = [_]Payload{.{ .name = "nm", .view = names.view(), .out_type = .string }};
    const emit_cols = [_]usize{ 0, 1, 2, 3 };
    const ops = [_]RegionOp{
        .{ .group_agg = .{ .subkeys = &subkeys, .out = &agg_out } },
        .{ .hash_probe = .{ .probe = 1, .map = &map, .payload = &payload, .inner = true } },
        .{ .emit = .{ .cols = &emit_cols } },
    };

    var prog = try Program.build(alloc, &entry, &ops, null);
    defer prog.deinit();
    var worker = try RegionWorker.init(alloc, &prog);
    defer worker.deinit();

    const out = try alloc.alloc(ColumnStore, prog.output_schema.len);
    defer {
        for (out) |*c| c.deinit(alloc);
        alloc.free(out);
    }
    for (out, prog.output_schema) |*c, col| c.* = try ColumnStore.init(alloc, col.type, col.nullable);

    try worker.runShard(&sd, out);

    // Only g=1 sub-groups survive: (a, 1, 30, one), (b, 1, 7, one).
    try testing.expectEqual(@as(usize, 2), out[0].rowCount());
    const kv = out[0].view();
    const sv = out[2].view();
    const nv = out[3].view();
    try testing.expectEqualStrings("a", stringViewOf(kv).rowBytes(0));
    try testing.expectEqualStrings("b", stringViewOf(kv).rowBytes(1));
    try testing.expectEqual(@as(i64, 30), sv.data.bigint[0]);
    try testing.expectEqual(@as(i64, 7), sv.data.bigint[1]);
    try testing.expectEqualStrings("one", stringViewOf(nv).rowBytes(0));
    try testing.expectEqualStrings("one", stringViewOf(nv).rowBytes(1));
}

test "region program: compute op runs the engine expression evaluator per shard" {
    const alloc = testing.allocator;
    const entry = [_]Column{
        .{ .name = "k", .type = .string, .nullable = true },
        .{ .name = "g", .type = .int, .nullable = true },
        .{ .name = "v", .type = .bigint, .nullable = true },
    };
    var sd = try tBuildShard(alloc, &entry);
    defer sd.deinit(alloc);

    const derived = [_]compute_mod.Derived{.{ .name = "v2", .expr = .{ .col_ref = "v" } }};
    const emit_cols = [_]usize{ 2, 3 };
    const ops = [_]RegionOp{
        .{ .compute = .{ .derived = &derived } },
        .{ .emit = .{ .cols = &emit_cols } },
    };

    var prog = try Program.build(alloc, &entry, &ops, null);
    defer prog.deinit();
    try testing.expectEqual(@as(usize, 4), prog.schema_at[1].len);

    var worker = try RegionWorker.init(alloc, &prog);
    defer worker.deinit();

    const out = try alloc.alloc(ColumnStore, prog.output_schema.len);
    defer {
        for (out) |*c| c.deinit(alloc);
        alloc.free(out);
    }
    for (out, prog.output_schema) |*c, col| c.* = try ColumnStore.init(alloc, col.type, col.nullable);

    try worker.runShard(&sd, out);

    try testing.expectEqual(@as(usize, 6), out[0].rowCount());
    const v0 = out[0].view();
    const v1 = out[1].view();
    for (0..6) |i| {
        try testing.expectEqual(v0.isValid(i), v1.isValid(i));
        if (v0.isValid(i)) try testing.expectEqual(v0.data.bigint[i], v1.data.bigint[i]);
    }
}

/// Aligned test kernel: out[0] = in[0] * 2 + broadcast[0].row0.
fn tKernelDouble(ctx: *const udf_mod.TvfContext, parts: []const udf_mod.TvfPartition, out: *udf_mod.TvfOutput) anyerror!void {
    _ = ctx;
    const v = parts[0].columns[0];
    const addend = viewI64(parts[1].columns[0], 0).?;
    const dst = out.columns[0];
    for (0..parts[0].row_count) |i| {
        if (viewI64(v, i)) |x| {
            try dst.data.bigint.append(out.allocator, x * 2 + addend);
            try dst.appendValidBit(out.allocator, dst.rowCount() - 1, true);
        } else try dst.appendNulls(out.allocator, 1);
    }
}

/// Grouped replace test kernel: one row per partition = (k of row 0, sum v).
fn tKernelSum(ctx: *const udf_mod.TvfContext, parts: []const udf_mod.TvfPartition, out: *udf_mod.TvfOutput) anyerror!void {
    _ = ctx;
    const p = parts[0];
    var sum: i64 = 0;
    for (0..p.row_count) |i| {
        if (viewI64(p.columns[1], i)) |x| sum += x;
    }
    try appendRowValue(out.allocator, out.columns[0], p.columns[0], 0);
    try out.columns[1].data.bigint.append(out.allocator, sum);
    try out.columns[1].appendValidBit(out.allocator, out.columns[1].rowCount() - 1, true);
}

/// Union-append test kernel over frame (k,g,v): appends one row per call —
/// (k of row 0, g=99, v=input row count).
fn tKernelEstimate(ctx: *const udf_mod.TvfContext, parts: []const udf_mod.TvfPartition, out: *udf_mod.TvfOutput) anyerror!void {
    _ = ctx;
    const p = parts[0];
    try appendRowValue(out.allocator, out.columns[0], p.columns[0], 0);
    try out.columns[1].data.int.append(out.allocator, 99);
    try out.columns[1].appendValidBit(out.allocator, out.columns[1].rowCount() - 1, true);
    try out.columns[2].data.bigint.append(out.allocator, @intCast(p.row_count));
    try out.columns[2].appendValidBit(out.allocator, out.columns[2].rowCount() - 1, true);
}

test "region program: tvf_aligned appends computed columns (broadcast part)" {
    const alloc = testing.allocator;
    const entry = [_]Column{
        .{ .name = "k", .type = .string, .nullable = true },
        .{ .name = "g", .type = .int, .nullable = true },
        .{ .name = "v", .type = .bigint, .nullable = true },
    };
    var sd = try tBuildShard(alloc, &entry);
    defer sd.deinit(alloc);

    var addend = try ColumnStore.init(alloc, .bigint, true);
    defer addend.deinit(alloc);
    try tAppendI64(alloc, &addend, 5);
    const bviews = [_]ColumnView{addend.view()};
    const extra = [_]udf_mod.TvfPartition{.{ .columns = &bviews, .row_count = 1, .keys = &.{} }};

    const inputs = [_]usize{2};
    const out_cols = [_]Column{.{ .name = "v2", .type = .bigint, .nullable = true }};
    const emit_cols = [_]usize{ 2, 3 };
    const ops = [_]RegionOp{
        .{ .tvf_aligned = .{
            .process = tKernelDouble,
            .inputs = &inputs,
            .extra_parts = &extra,
            .out = &out_cols,
        } },
        .{ .emit = .{ .cols = &emit_cols } },
    };

    var prog = try Program.build(alloc, &entry, &ops, null);
    defer prog.deinit();
    var worker = try RegionWorker.init(alloc, &prog);
    defer worker.deinit();
    const out = try alloc.alloc(ColumnStore, prog.output_schema.len);
    defer {
        for (out) |*c| c.deinit(alloc);
        alloc.free(out);
    }
    for (out, prog.output_schema) |*c, col| c.* = try ColumnStore.init(alloc, col.type, col.nullable);

    try worker.runShard(&sd, out);

    try testing.expectEqual(@as(usize, 6), out[0].rowCount());
    const v0 = out[0].view();
    const v1 = out[1].view();
    for (0..6) |i| {
        try testing.expectEqual(v0.isValid(i), v1.isValid(i));
        if (v0.isValid(i)) try testing.expectEqual(v0.data.bigint[i] * 2 + 5, v1.data.bigint[i]);
    }
}

test "region program: tvf_grouped replace emits per-range kernel output" {
    const alloc = testing.allocator;
    const entry = [_]Column{
        .{ .name = "k", .type = .string, .nullable = true },
        .{ .name = "g", .type = .int, .nullable = true },
        .{ .name = "v", .type = .bigint, .nullable = true },
    };
    var sd = try tBuildShard(alloc, &entry);
    defer sd.deinit(alloc);

    const inputs = [_]usize{ 0, 2 };
    const out_cols = [_]Column{
        .{ .name = "k", .type = .string, .nullable = true },
        .{ .name = "sum_v", .type = .bigint, .nullable = true },
    };
    const emit_cols = [_]usize{ 0, 1 };
    const ops = [_]RegionOp{
        .{ .tvf_grouped = .{ .spec = .{
            .process = tKernelSum,
            .inputs = &inputs,
            .out = &out_cols,
        } } },
        .{ .emit = .{ .cols = &emit_cols } },
    };

    var prog = try Program.build(alloc, &entry, &ops, null);
    defer prog.deinit();
    var worker = try RegionWorker.init(alloc, &prog);
    defer worker.deinit();
    const out = try alloc.alloc(ColumnStore, prog.output_schema.len);
    defer {
        for (out) |*c| c.deinit(alloc);
        alloc.free(out);
    }
    for (out, prog.output_schema) |*c, col| c.* = try ColumnStore.init(alloc, col.type, col.nullable);

    try worker.runShard(&sd, out);

    // Range "a": 10+30+5+20 = 65; range "b": 7.
    try testing.expectEqual(@as(usize, 2), out[0].rowCount());
    const kv = out[0].view();
    const sv = out[1].view();
    try testing.expectEqualStrings("a", stringViewOf(kv).rowBytes(0));
    try testing.expectEqualStrings("b", stringViewOf(kv).rowBytes(1));
    try testing.expectEqual(@as(i64, 65), sv.data.bigint[0]);
    try testing.expectEqual(@as(i64, 7), sv.data.bigint[1]);
}

test "region program: tvf_grouped union_append with input filter" {
    const alloc = testing.allocator;
    const entry = [_]Column{
        .{ .name = "k", .type = .string, .nullable = true },
        .{ .name = "g", .type = .int, .nullable = true },
        .{ .name = "v", .type = .bigint, .nullable = true },
    };
    var sd = try tBuildShard(alloc, &entry);
    defer sd.deinit(alloc);

    const inputs = [_]usize{ 0, 1, 2 };
    const emit_cols = [_]usize{ 0, 1, 2 };
    const ops = [_]RegionOp{
        .{ .tvf_grouped = .{
            .spec = .{
                .process = tKernelEstimate,
                .inputs = &inputs,
                .out = &entry,
            },
            .union_append = true,
            // Only v in [10, 30] feeds the kernel; range "b" (v=7) gets none.
            .input_filter = .{ .col = 2, .lo = 10, .hi = 30 },
        } },
        .{ .emit = .{ .cols = &emit_cols } },
    };

    var prog = try Program.build(alloc, &entry, &ops, null);
    defer prog.deinit();
    var worker = try RegionWorker.init(alloc, &prog);
    defer worker.deinit();
    const out = try alloc.alloc(ColumnStore, prog.output_schema.len);
    defer {
        for (out) |*c| c.deinit(alloc);
        alloc.free(out);
    }
    for (out, prog.output_schema) |*c, col| c.* = try ColumnStore.init(alloc, col.type, col.nullable);

    try worker.runShard(&sd, out);

    // Range "a" (5 rows, filtered kernel input {10,30,20}) gains one row
    // (k=a, g=99, v=3) at its tail; range "b" is unchanged (no kernel call).
    try testing.expectEqual(@as(usize, 7), out[0].rowCount());
    const kv = out[0].view();
    const gv = out[1].view();
    const vv = out[2].view();
    try testing.expectEqualStrings("a", stringViewOf(kv).rowBytes(5));
    try testing.expectEqual(@as(i32, 99), gv.data.int[5]);
    try testing.expectEqual(@as(i64, 3), vv.data.bigint[5]);
    try testing.expectEqualStrings("b", stringViewOf(kv).rowBytes(6));
    try testing.expectEqual(@as(i64, 7), vv.data.bigint[6]);
}

fn tRunDriver(alloc: Allocator, n_threads: usize) !void {
    const entry = [_]Column{
        .{ .name = "k", .type = .string, .nullable = true },
        .{ .name = "g", .type = .int, .nullable = true },
        .{ .name = "v", .type = .bigint, .nullable = true },
    };

    // Two source batches over the same schema.
    var stores: [2][3]ColumnStore = undefined;
    for (&stores) |*set| {
        set[0] = try ColumnStore.init(alloc, .string, true);
        set[1] = try ColumnStore.init(alloc, .int, true);
        set[2] = try ColumnStore.init(alloc, .bigint, true);
    }
    defer for (&stores) |*set| {
        for (set) |*c| c.deinit(alloc);
    };
    const K1 = [_]?[]const u8{ "a", "b", "a" };
    const G1 = [_]?i32{ 1, 1, 2 };
    const V1 = [_]?i64{ 10, 20, 30 };
    const K2 = [_]?[]const u8{ "b", "c", "a" };
    const G2 = [_]?i32{ 1, null, 1 };
    const V2 = [_]?i64{ 40, 50, 60 };
    for (K1, G1, V1) |k, g, v| {
        try tAppendStr(alloc, &stores[0][0], k);
        try tAppendInt(alloc, &stores[0][1], g);
        try tAppendI64(alloc, &stores[0][2], v);
    }
    for (K2, G2, V2) |k, g, v| {
        try tAppendStr(alloc, &stores[1][0], k);
        try tAppendInt(alloc, &stores[1][1], g);
        try tAppendI64(alloc, &stores[1][2], v);
    }
    var views: [2][3]ColumnView = undefined;
    for (&views, &stores) |*vs, *set| {
        for (vs, set) |*v, *c| v.* = c.view();
    }

    const sources = try alloc.alloc(exec.Query, 2);
    defer alloc.free(sources);
    var made: usize = 0;
    defer for (sources[0..made]) |*q| q.deinit();
    for (sources, &views, 0..) |*q, *vs, i| {
        q.* = try single_batch.SingleBatchSource.create(alloc, .{
            .schema = &entry,
            .values = vs,
            .row_count = if (i == 0) K1.len else K2.len,
        });
        made += 1;
    }

    const agg_out = [_]AggOut{
        .{ .name = "k", .kind = .{ .first = 0 } },
        .{ .name = "g", .kind = .{ .first = 1 } },
        .{ .name = "sum_v", .kind = .{ .sum_int = 2 } },
    };
    const subkeys = [_]usize{1};
    const emit_cols = [_]usize{ 0, 1, 2 };
    const ops = [_]RegionOp{
        .{ .group_agg = .{ .subkeys = &subkeys, .out = &agg_out } },
        .{ .emit = .{ .cols = &emit_cols } },
    };
    var prog = try Program.build(alloc, &entry, &ops, null);
    defer prog.deinit();

    var result = try RegionResult.init(alloc, prog.output_schema, 4);
    defer result.deinit();

    const sort_cols = [_]OrderCol{.{ .col = 0, .kind = .string }};
    try runRegion(alloc, &entry, sources, &.{}, &prog, .{
        .n_threads = n_threads,
        .n_shards = 4,
        .key_col = 0,
        .sort_cols = &sort_cols,
        .group_prefix = 1,
    }, &result);

    // Expected groups: (a,1)=70 (a,2)=30 (b,1)=60 (c,NULL)=50.
    try testing.expectEqual(@as(usize, 4), result.totalRows());
    var sum_all: i64 = 0;
    var seen_a1 = false;
    for (result.shards) |s| {
        if (s.rows == 0) continue;
        const kv = s.cols[0].view();
        const gv = s.cols[1].view();
        const sv = s.cols[2].view();
        for (0..s.rows) |i| {
            sum_all += sv.data.bigint[i];
            const k = stringViewOf(kv).rowBytes(i);
            if (std.mem.eql(u8, k, "a") and gv.isValid(i) and gv.data.int[i] == 1) {
                try testing.expectEqual(@as(i64, 70), sv.data.bigint[i]);
                seen_a1 = true;
            }
            if (std.mem.eql(u8, k, "c")) {
                try testing.expect(!gv.isValid(i));
                try testing.expectEqual(@as(i64, 50), sv.data.bigint[i]);
            }
        }
    }
    try testing.expectEqual(@as(i64, 210), sum_all);
    try testing.expect(seen_a1);
}

test "region driver: scan -> exchange -> whale-first shards -> program (serial)" {
    try tRunDriver(testing.allocator, 1);
}

test "region driver: parallel workers match serial (thread-safe allocator)" {
    // std.testing.allocator is single-threaded; the parallel claim path
    // runs on the smp allocator (no leak check — the serial twin has it).
    try tRunDriver(std.heap.smp_allocator, 3);
}

test "region driver: leading union-append TVF fuses into the consolidation gather" {
    const alloc = testing.allocator;
    const entry = [_]Column{
        .{ .name = "k", .type = .string, .nullable = true },
        .{ .name = "g", .type = .int, .nullable = true },
        .{ .name = "v", .type = .bigint, .nullable = true },
    };

    var stores: [3]ColumnStore = undefined;
    stores[0] = try ColumnStore.init(alloc, .string, true);
    stores[1] = try ColumnStore.init(alloc, .int, true);
    stores[2] = try ColumnStore.init(alloc, .bigint, true);
    defer for (&stores) |*c| c.deinit(alloc);
    const K = [_]?[]const u8{ "a", "b", "a", "c", "b", "a" };
    const G = [_]?i32{ 1, 1, 2, 3, 1, 1 };
    const V = [_]?i64{ 10, 20, 30, 50, 40, 60 };
    for (K, G, V) |k, g, v| {
        try tAppendStr(alloc, &stores[0], k);
        try tAppendInt(alloc, &stores[1], g);
        try tAppendI64(alloc, &stores[2], v);
    }
    var views: [3]ColumnView = undefined;
    for (&views, &stores) |*v, *c| v.* = c.view();

    const sources = try alloc.alloc(exec.Query, 1);
    defer alloc.free(sources);
    sources[0] = try single_batch.SingleBatchSource.create(alloc, .{
        .schema = &entry,
        .values = &views,
        .row_count = K.len,
    });
    defer sources[0].deinit();

    const inputs = [_]usize{ 0, 1, 2 };
    const emit_cols = [_]usize{ 0, 1, 2 };
    const ops = [_]RegionOp{
        .{ .tvf_grouped = .{
            .spec = .{ .process = tKernelEstimate, .inputs = &inputs, .out = &entry },
            .union_append = true,
            .input_filter = .{ .col = 2, .lo = 10, .hi = 30 },
        } },
        .{ .emit = .{ .cols = &emit_cols } },
    };
    var prog = try Program.build(alloc, &entry, &ops, null);
    defer prog.deinit();

    var result = try RegionResult.init(alloc, prog.output_schema, 4);
    defer result.deinit();
    const sort_cols = [_]OrderCol{.{ .col = 0, .kind = .string }};
    try runRegion(alloc, &entry, sources, &.{}, &prog, .{
        .n_threads = 1,
        .n_shards = 4,
        .key_col = 0,
        .sort_cols = &sort_cols,
        .group_prefix = 1,
    }, &result);

    // Filter keeps v in [10,30]: "a" {10,30} -> +1 row (a,99,2);
    // "b" {20} -> +1 row (b,99,1); "c" {} -> no kernel call.
    try testing.expectEqual(@as(usize, 8), result.totalRows());
    var appended: usize = 0;
    for (result.shards) |s| {
        if (s.rows == 0) continue;
        const kv = s.cols[0].view();
        const gv = s.cols[1].view();
        const vv = s.cols[2].view();
        for (0..s.rows) |i| {
            if (gv.isValid(i) and gv.data.int[i] == 99) {
                appended += 1;
                const k = stringViewOf(kv).rowBytes(i);
                const want: i64 = if (std.mem.eql(u8, k, "a")) 2 else 1;
                try testing.expect(!std.mem.eql(u8, k, "c"));
                try testing.expectEqual(want, vv.data.bigint[i]);
            }
        }
    }
    try testing.expectEqual(@as(usize, 2), appended);
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
