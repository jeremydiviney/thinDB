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

fn viewF64(v: ColumnView, i: usize) ?f64 {
    if (!v.isValid(i)) return null;
    return switch (v.data) {
        .float => |s| s[i],
        .double => |s| s[i],
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
    /// Final projection into the caller's per-shard output stores. Must be
    /// the program's last op.
    emit: struct { cols: []const usize },
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
        emit: void,
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
        if (sd.rows == 0) return;
        defer _ = self.scratch.reset(.retain_capacity);

        var fr = Frame{
            .views = self.views,
            .width = self.prog.schema_at[0].len,
            .rows = sd.rows,
            .ranges = sd.ranges.items,
        };
        for (sd.cols, fr.views[0..fr.width]) |*c, *v| v.* = c.view();

        for (self.prog.ops, self.states, 0..) |op, *st, oi| {
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

            for (rng[0]..rng[1]) |i| {
                const key = makeSubKey(g.subkeys, fr.views, i);
                const gop = try s.map.getOrPut(alloc, key);
                if (!gop.found_existing) {
                    gop.value_ptr.* = @intCast(s.sub_first.items.len);
                    try s.sub_first.append(alloc, @intCast(i));
                    for (s.cells) |*l| try l.append(alloc, .{});
                }
                const gi = gop.value_ptr.*;
                for (g.out, s.cells) |spec, *cells| {
                    const cell = &cells.items[gi];
                    switch (spec.kind) {
                        .first => {},
                        .sum_int => |c| if (viewI64(fr.views[c], i)) |v| {
                            cell.i += v;
                            cell.seen = true;
                        },
                        .sum_float => |c| if (viewF64(fr.views[c], i)) |v| {
                            cell.f += v;
                            cell.seen = true;
                        },
                        .min_int => |c| if (viewI64(fr.views[c], i)) |v| {
                            cell.i = if (cell.seen) @min(cell.i, v) else v;
                            cell.seen = true;
                        },
                        .max_int => |c| if (viewI64(fr.views[c], i)) |v| {
                            cell.i = if (cell.seen) @max(cell.i, v) else v;
                            cell.seen = true;
                        },
                        .max_by => |mb| if (viewI64(fr.views[mb.ord], i)) |o| {
                            if (!cell.seen or o > cell.i) {
                                cell.i = o;
                                cell.row = @intCast(i);
                                cell.seen = true;
                            }
                        },
                    }
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
