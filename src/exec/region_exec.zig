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
const store_mod = @import("../engine/store.zig");
const ColumnStore = store_mod.ColumnStore;
const exec = @import("exec.zig");
const core_scheduler = @import("../util/core_scheduler.zig");
const Batch = exec.Batch;
const compute_mod = @import("compute.zig");
const single_batch = @import("single_batch.zig");
const udf_mod = @import("../udf.zig");

extern "c" fn getenv(name: [*:0]const u8) ?[*:0]const u8;

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

/// scatterColumn for keep-lists dominated by consecutive-index runs (probe
/// restructure emits keep most rows and expand mostly 1:1): each maximal
/// run bulk-copies values and validity as a slice. For run-poor lists the
/// per-run call overhead loses to scatterColumn's per-element loop — the
/// emit picks per keep-list via runsDominate.
pub fn scatterColumnRuns(alloc: Allocator, store: *ColumnStore, v: ColumnView, rows: []const u32) !void {
    var i: usize = 0;
    while (i < rows.len) {
        var j = i + 1;
        while (j < rows.len and rows[j] == rows[j - 1] + 1) : (j += 1) {}
        const start: usize = rows[i];
        try appendViewRange(alloc, store, v, start, start + (j - i));
        i = j;
    }
}

/// True when bulk-copyable runs (length ≥ 4) cover at least half the list.
/// One scan per EMIT (not per column) — the statistics are identical for
/// every column of the frame.
fn runsDominate(rows: []const u32) bool {
    var covered: usize = 0;
    var i: usize = 0;
    while (i < rows.len) {
        var j = i + 1;
        while (j < rows.len and rows[j] == rows[j - 1] + 1) : (j += 1) {}
        if (j - i >= 4) covered += j - i;
        i = j;
    }
    return covered * 2 >= rows.len;
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
    if (dst.nulls != null) try appendGatherValidity(alloc, dst, srcs, refs, base);
}

/// Validity for a gathered append: grow the bitmap once, then set bits with
/// no per-row allocation path. Fresh bytes arrive zeroed (invalid), so only
/// true bits are written — same invariant `appendValidityRange` relies on.
fn appendGatherValidity(alloc: Allocator, dst: *ColumnStore, srcs: []const ColumnView, refs: []const u32, base: usize) !void {
    const nb = &dst.nulls.?;
    const need = (base + refs.len + 7) / 8;
    if (nb.items.len < need) try nb.appendNTimes(alloc, 0, need - nb.items.len);
    const bytes = nb.items;
    var all_valid = true;
    for (srcs) |s| {
        if (s.nulls != null) {
            all_valid = false;
            break;
        }
    }
    if (all_valid) {
        store_mod.setBitRangeTrue(bytes, base, refs.len);
        return;
    }
    for (refs, base..) |r, row| {
        if (srcs[r >> REF_ROW_BITS].isValid(r & REF_ROW_MASK)) {
            bytes[row >> 3] |= @as(u8, 1) << @intCast(row & 7);
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
        try dst.appendValidityRangeFrom(alloc, base, v.nulls, start, end - start);
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

/// LEFT-probe payload sentinel: no build-side match for this row.
const NO_MATCH: u32 = std.math.maxInt(u32);

/// Gather `ords` rows of `src` onto `dst`, appending NULL where the ordinal
/// is NO_MATCH. `dst` must be nullable (probe payloads always are).
fn gatherColumnOpt(alloc: Allocator, dst: *ColumnStore, src: ColumnView, ords: []const u32) !void {
    const base = dst.rowCount();
    switch (src.data) {
        inline .tinyint, .smallint, .int, .bigint, .largeint, .float, .double, .date, .datetime, .decimal64, .decimal128 => |vals, tag| {
            const l = &@field(dst.data, @tagName(tag));
            try l.ensureUnusedCapacity(alloc, ords.len);
            for (ords) |o| l.appendAssumeCapacity(if (o == NO_MATCH) 0 else vals[o]);
        },
        .varchar, .string, .char, .json => |sv| switch (dst.data) {
            .varchar, .string, .char, .json => |*d| {
                var bytes: usize = 0;
                for (ords) |o| {
                    if (o != NO_MATCH) bytes += sv.rowBytes(o).len;
                }
                try d.ensureUnusedValueCapacity(alloc, ords.len, bytes);
                for (ords) |o| d.appendValueAssumeCapacity(if (o == NO_MATCH) "" else sv.rowBytes(o));
            },
            else => unreachable,
        },
        else => return error.UnsupportedQueryShape,
    }
    for (ords, 0..) |o, k| {
        try dst.appendValidBit(alloc, base + k, o != NO_MATCH and src.isValid(o));
    }
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

/// Route-hash bytes for one key value: string payload bytes, or the raw
/// value bytes for fixed-width types. Rows with equal key values always
/// produce identical bytes, which is all shard routing needs. The builder
/// never routes on floats (bit-pattern hashing vs value equality); any
/// other unhashable type degrades to one shard, which is correct, just
/// unbalanced.
fn routeKeyBytes(v: ColumnView, r: usize) []const u8 {
    return switch (v.data) {
        .varchar, .string, .char, .json => |s| s.rowBytes(r),
        inline .tinyint, .smallint, .int, .bigint, .largeint, .float, .double, .date, .datetime, .decimal64, .decimal128 => |s| std.mem.asBytes(&s[r]),
        else => "",
    };
}

// ---------------------------------------------------------------------------
// Exchange: hash-scatter into (worker × shard) buckets
// ---------------------------------------------------------------------------

/// One (worker, shard) bucket: a private columnar buffer only its producing
/// worker appends to, so the scatter is lock-free. Consolidation reads all
/// workers' buckets for one shard. After its worker's scan finishes, the
/// bucket also carries its rows' normalized sort keys and a sorted
/// permutation — consolidation then merges pre-sorted runs instead of
/// sorting the whole shard, which moves the sort work into the scan phase
/// where it is balanced by input chunks rather than bounded by key skew.
const Bucket = struct {
    cols: []ColumnStore,
    rows: usize = 0,
    keys: std.ArrayListUnmanaged(RowKey) = .empty,
    order: std.ArrayListUnmanaged(u32) = .empty,
};

pub const Exchange = struct {
    /// Thread-safe base allocator: the bucket ARRAY, consolidation
    /// out-side stores, and per-run worker scratch (freed each run — arena
    /// memory is never reclaimed until reset, so per-run allocations there
    /// would accumulate across warm runs).
    alloc: Allocator,
    /// One arena per scan worker, owning that worker's bucket stores and
    /// merge keys. A worker only ever grows its own row of buckets, so the
    /// arenas see single-threaded use — and teardown of the exchange's
    /// multi-GB retained capacity collapses from ~10^5 individual frees to
    /// n_workers arena releases (the inline-teardown seconds the pool's
    /// eviction path used to pay).
    arenas: []std.heap.ArenaAllocator,
    schema: []const Column,
    n_workers: usize,
    n_shards: usize,
    /// Region key column (index into `schema`) — hash source. P0: one
    /// string-typed key column; composite keys hash-combine later.
    key_col: usize,
    buckets: []Bucket,

    /// Single-threaded by contract: only scan worker `w` (which owns bucket
    /// row w exclusively) may allocate from arena w. The consolidation-side
    /// lazy sortBucketKeys fallback also lands here — it never fires when a
    /// scan phase ran (every bucket is pre-sorted), and direct callers
    /// without one are single-threaded.
    fn workerAlloc(self: *Exchange, w: usize) Allocator {
        return self.arenas[w].allocator();
    }

    pub fn init(alloc: Allocator, schema: []const Column, n_workers: usize, n_shards: usize, key_col: usize) !Exchange {
        const arenas = try alloc.alloc(std.heap.ArenaAllocator, n_workers);
        errdefer alloc.free(arenas);
        for (arenas) |*ar| ar.* = std.heap.ArenaAllocator.init(alloc);
        errdefer for (arenas) |*ar| ar.deinit();
        const buckets = try alloc.alloc(Bucket, n_workers * n_shards);
        errdefer alloc.free(buckets);
        for (buckets, 0..) |*b, i| {
            const wa = arenas[i / n_shards].allocator();
            const cols = try wa.alloc(ColumnStore, schema.len);
            for (cols, schema) |*c, col| {
                c.* = try ColumnStore.init(wa, col.type, col.nullable);
            }
            b.* = .{ .cols = cols };
        }
        return .{
            .alloc = alloc,
            .arenas = arenas,
            .schema = schema,
            .n_workers = n_workers,
            .n_shards = n_shards,
            .key_col = key_col,
            .buckets = buckets,
        };
    }

    pub fn deinit(self: *Exchange) void {
        for (self.arenas) |*ar| ar.deinit();
        self.alloc.free(self.arenas);
        self.alloc.free(self.buckets);
    }

    /// Pool reuse: retain every bucket's capacity for the next run.
    pub fn clear(self: *Exchange) void {
        for (self.buckets) |*b| {
            for (b.cols) |*c| c.clear();
            b.rows = 0;
            b.keys.clearRetainingCapacity();
            b.order.clearRetainingCapacity();
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
        /// semantics); pass 2 appends per (shard, column). Rows where
        /// `keep` is false are dropped before routing (member filters).
        pub fn push(self: *Worker, batch: Batch, keep: ?[]const bool) !void {
            const ex = self.ex;
            for (self.idx) |*l| l.clearRetainingCapacity();
            const kv = batch.values[ex.key_col];
            for (0..batch.row_count) |r| {
                if (keep) |k| if (!k[r]) continue;
                const key: []const u8 = if (kv.isValid(r)) routeKeyBytes(kv, r) else "";
                const shard = std.hash.Wyhash.hash(HASH_SEED, key) % ex.n_shards;
                try self.idx[shard].append(ex.alloc, @intCast(r));
            }
            const wa = ex.workerAlloc(self.w);
            for (0..ex.n_shards) |shard| {
                const rows = self.idx[shard].items;
                if (rows.len == 0) continue;
                const b = ex.bucket(self.w, shard);
                for (b.cols, batch.values) |*store, v| {
                    try scatterColumn(wa, store, v, rows);
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

/// Fill one bucket column's normalized keys (row-major slot `kc` of
/// `n_sort`). One monomorphized sweep per (sort col, bucket) — the same
/// dispatch-hoisting discipline the old whole-shard fill used.
fn fillRowKeys(keys: []RowKey, n_sort: usize, kc: usize, kind: OrderKind, v: ColumnView, rows: usize) !void {
    switch (kind) {
        .string => {
            const sv = stringViewOf(v);
            for (0..rows) |i| {
                const valid = v.isValid(i);
                const s: []const u8 = if (valid) sv.rowBytes(i) else "";
                keys[i * n_sort + kc] = .{ .norm = normStrPrefix(valid, s), .str = s };
            }
        },
        .int32 => {
            const vals: []const i32 = switch (v.data) {
                .int => |s| s,
                .date => |s| s,
                else => return error.UnsupportedQueryShape,
            };
            for (0..rows) |i| {
                keys[i * n_sort + kc] = .{ .norm = normI32(v.isValid(i), vals[i]), .str = "" };
            }
        },
        .int64 => {
            const vals: []const i64 = switch (v.data) {
                .bigint => |s| s,
                .datetime => |s| s,
                else => return error.UnsupportedQueryShape,
            };
            for (0..rows) |i| {
                keys[i * n_sort + kc] = .{ .norm = normI64(v.isValid(i), vals[i]), .str = "" };
            }
        },
    }
}

fn keyCmp(a: []const RowKey, b: []const RowKey) std.math.Order {
    for (a, b) |x, y| {
        if (x.norm != y.norm) return if (x.norm < y.norm) .lt else .gt;
        if (x.str.len != 0 or y.str.len != 0) {
            const o = std.mem.order(u8, x.str, y.str);
            if (o != .eq) return o;
        }
    }
    return .eq;
}

fn keyEqPrefix(a: []const RowKey, b: []const RowKey, prefix: usize) bool {
    for (a[0..prefix], b[0..prefix]) |x, y| {
        if (x.norm != y.norm) return false;
        if ((x.str.len != 0 or y.str.len != 0) and !std.mem.eql(u8, x.str, y.str)) return false;
    }
    return true;
}

/// Build one bucket's keys and sorted permutation. Runs in the scan phase
/// (each worker sorts its own buckets — a whale key's rows are spread over
/// every worker's buckets by the row-chunk scan, so this work is balanced
/// regardless of key skew); consolidation falls back to it lazily for
/// callers that pushed rows without a scan phase.
pub fn sortBucketKeys(ex: *Exchange, w: usize, shard: usize, sort_cols: []const OrderCol) !void {
    const b = ex.bucket(w, shard);
    b.keys.clearRetainingCapacity();
    b.order.clearRetainingCapacity();
    const rows = b.rows;
    if (rows == 0) return;
    const n_sort = sort_cols.len;
    try b.keys.ensureTotalCapacity(ex.workerAlloc(w), rows * n_sort);
    b.keys.items.len = rows * n_sort;
    for (sort_cols, 0..) |sc, kc| {
        try fillRowKeys(b.keys.items, n_sort, kc, sc.kind, b.cols[sc.col].view(), rows);
    }
    try b.order.ensureTotalCapacity(ex.workerAlloc(w), rows);
    b.order.items.len = rows;
    for (b.order.items, 0..) |*o, i| o.* = @intCast(i);
    const Ctx = struct {
        keys: []const RowKey,
        n_sort: usize,
        fn less(c: @This(), x: u32, y: u32) bool {
            return switch (keyCmp(
                c.keys[x * c.n_sort ..][0..c.n_sort],
                c.keys[y * c.n_sort ..][0..c.n_sort],
            )) {
                .lt => true,
                .gt => false,
                .eq => x < y, // stable tie-break on arrival order
            };
        }
    };
    std.mem.sortUnstable(u32, b.order.items, Ctx{ .keys = b.keys.items, .n_sort = n_sort }, Ctx.less);
}

/// One pre-sorted bucket run being merged.
const RunState = struct {
    w: u32,
    keys: []const RowKey,
    ord: []const u32,
    pos: usize,
};

const MergeCtx = struct {
    runs: []RunState,
    n_sort: usize,

    fn headKey(c: *const MergeCtx, ri: u32) []const RowKey {
        const r = &c.runs[ri];
        return r.keys[r.ord[r.pos] * c.n_sort ..][0..c.n_sort];
    }

    /// Run order on key ties = worker index — reproduces the old global
    /// stable sort exactly (refs were laid out worker-major, arrival-minor).
    fn less(c: *const MergeCtx, a: u32, b: u32) bool {
        return switch (keyCmp(c.headKey(a), c.headKey(b))) {
            .lt => true,
            .gt => false,
            .eq => c.runs[a].w < c.runs[b].w,
        };
    }

    fn siftDown(c: *const MergeCtx, heap: []u32, start: usize) void {
        var i = start;
        while (true) {
            const l = 2 * i + 1;
            if (l >= heap.len) break;
            var m = l;
            const r = l + 1;
            if (r < heap.len and c.less(heap[r], heap[l])) m = r;
            if (!c.less(heap[m], heap[i])) break;
            std.mem.swap(u32, &heap[i], &heap[m]);
            i = m;
        }
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
    try out.ensure(ex.alloc, ex.schema);
    try consolidateAppendTail(ex, shard, sort_cols, group_prefix, out, scratch, tail);
}

/// Append-mode consolidation: like `consolidateOrderedTail` but the caller
/// owns `out`'s lifecycle — an execution bin consolidates each of its member
/// route partitions into ONE ShardData (partitions never share a key, so
/// concatenating their sorted groups is exactly the per-shard semantics).
pub fn consolidateAppendTail(
    ex: *Exchange,
    shard: usize,
    sort_cols: []const OrderCol,
    group_prefix: usize,
    out: *ShardData,
    scratch: Allocator,
    tail: ?GroupTail,
) !void {
    const total = ex.shardRows(shard);
    if (total == 0) return;

    // Per-column, per-worker source views.
    const n_cols = ex.schema.len;
    const views = try scratch.alloc(ColumnView, n_cols * ex.n_workers);
    for (0..n_cols) |ci| {
        for (0..ex.n_workers) |w| {
            views[ci * ex.n_workers + w] = ex.bucket(w, shard).cols[ci].view();
        }
    }

    // K-way merge of the pre-sorted per-worker bucket runs (keys and
    // permutations built during the scan phase; lazy fallback for callers
    // that pushed rows without one). Output order is byte-identical to the
    // old whole-shard sort: comparator unchanged, run-index tie-break
    // reproduces the worker-major stable arrival order.
    const n_sort = sort_cols.len;
    const runs = try scratch.alloc(RunState, ex.n_workers);
    var n_runs: usize = 0;
    for (0..ex.n_workers) |w| {
        const b = ex.bucket(w, shard);
        if (b.rows == 0) continue;
        if (b.order.items.len != b.rows or b.keys.items.len != b.rows * n_sort) {
            try sortBucketKeys(ex, w, shard, sort_cols);
        }
        runs[n_runs] = .{ .w = @intCast(w), .keys = b.keys.items, .ord = b.order.items, .pos = 0 };
        n_runs += 1;
    }

    const mctx = MergeCtx{ .runs = runs[0..n_runs], .n_sort = n_sort };
    const heap = try scratch.alloc(u32, n_runs);
    for (heap, 0..) |*h, i| h.* = @intCast(i);
    var i = n_runs / 2;
    while (i > 0) {
        i -= 1;
        mctx.siftDown(heap, i);
    }

    // Merged refs + group starts in one pass; the gather below is unchanged.
    const grefs = try scratch.alloc(u32, total);
    var bounds: std.ArrayListUnmanaged(u32) = .empty;
    var heap_len = n_runs;
    var prev: ?[]const RowKey = null;
    var out_i: u32 = 0;
    while (heap_len > 0) {
        const r = &runs[heap[0]];
        const row = r.ord[r.pos];
        const hk = r.keys[row * n_sort ..][0..n_sort];
        if (prev == null or !keyEqPrefix(prev.?, hk, group_prefix)) {
            try bounds.append(scratch, out_i);
        }
        prev = hk;
        grefs[out_i] = (r.w << REF_ROW_BITS) | row;
        out_i += 1;
        r.pos += 1;
        if (r.pos == r.ord.len) {
            heap[0] = heap[heap_len - 1];
            heap_len -= 1;
        }
        mctx.siftDown(heap[0..heap_len], 0);
    }
    try bounds.append(scratch, @intCast(total));

    for (bounds.items[0 .. bounds.items.len - 1], bounds.items[1..]) |g_start, g_end| {
        const base: u32 = @intCast(out.cols[0].rowCount());
        for (out.cols, 0..) |*dst, ci| {
            try gatherColumn(ex.alloc, dst, views[ci * ex.n_workers ..][0..ex.n_workers], grefs[g_start..g_end]);
        }
        if (tail) |t| try t.run(t.ctx, out, base);
        try out.ranges.append(ex.alloc, .{ base, @intCast(out.cols[0].rowCount()) });
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
        /// MAX over a string column: byte order, NULLs ignored.
        max_str: usize,
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

// ---- keyed_probe: generalized (multi-key, string-capable) probe -----------

pub const MAX_KEYED_PAIRS = 4;
/// Composite probe key: one i64 word per pair (ints verbatim, strings as
/// build-side interner ids). Unused trailing words stay 0.
pub const MultiKey = [MAX_KEYED_PAIRS]i64;
pub const MultiKeyMap = std.AutoHashMapUnmanaged(MultiKey, u32);
/// Build-side string domain: bytes → dense id (1-based; 0 never used so a
/// probe-side miss can never collide with a real key word).
pub const StrInterner = std.StringHashMapUnmanaged(u32);

/// `int_from_str_build`: SQL numeric coercion across an int-probe /
/// text-build equi-join (the report table stores externalPlanId as int, the
/// plan catalog as text) — build strings parse to i64; unparsable build
/// rows can never match an int and drop from the map.
pub const KeyedPairKind = enum { int, str, int_from_str_build };
pub const KeyedPair = struct {
    /// Frame column (probe side).
    probe: usize,
    /// Side/build column (index into the side schema / broadcast views).
    build: usize,
    kind: KeyedPairKind,
};

pub const KeyedPayload = struct {
    name: []const u8,
    /// Side/build column the payload gathers from.
    src: usize,
    out_type: types.Type,
};

/// Compile-time-drained build side (small broadcast joins): map and
/// interners are built once by the recognizer and shared read-only.
pub const BroadcastSide = struct {
    map: *const MultiKeyMap,
    /// One interner per pair; null for int pairs.
    interners: []const ?*const StrInterner,
    /// Every build column as a view (indexed by KeyedPair.build /
    /// KeyedPayload.src).
    views: []const ColumnView,
    rows: usize,
};

/// One co-partitioned side table of a region run: its own chunked scan +
/// entry computes, scattered through its own exchange by the SAME route
/// hash as the main scan — every potential match is then shard-local by
/// construction. Sources are query-lifetime (rebuilt per run, like the
/// main scan); the schema/derived slices must outlive the run.
pub const SideInput = struct {
    scan_schema: []const Column,
    /// Exchange (pre-aggregation) frame schema: scan output ++ entry-derived
    /// columns. Same slice as `schema` when `agg` is null.
    pre_schema: []const Column,
    /// Probe-visible side schema (post-aggregation when `agg` is set).
    schema: []const Column,
    sources: []exec.Query,
    entry_derived: []const compute_mod.Derived,
    /// Route key column in the pre-aggregation frame schema.
    key_col: usize,
    /// Per-bin GROUP BY applied after the scatter, before the probe map is
    /// built. Only valid because the group keys include the route key —
    /// every row of a group lands in the same shard, so shard-local
    /// aggregation is globally exact.
    agg: ?SideAgg = null,
};

pub const MAX_SIDE_GROUP_KEYS = 6;

/// SUM-only side GROUP BY (the rollforward CMT collapse shape). Post-agg
/// schema layout: col i < group_srcs.len is pre_schema[group_srcs[i]]
/// (group key, value from the group's first row); col group_srcs.len + k
/// is SUM(pre_schema[agg_srcs[k]]) with the engine's canonical widening.
/// `key_srcs` is the DISTINCT set of group sources — the map keys; alias
/// copies appear only in group_srcs (emission).
pub const SideAgg = struct {
    group_srcs: []const usize,
    key_srcs: []const usize,
    agg_srcs: []const usize,
};

/// Scan-fused membership filter: a compile-time-drained semi-join (INNER
/// equality join against a small distinct key set with no live payload)
/// or a large IN list, applied to entry batches BEFORE the exchange
/// scatter. Keys are sorted unique; the per-row test is a binary search —
/// no per-op frame restructure, and rows drop before they are ever
/// scattered.
pub const MemberFilter = struct {
    /// Entry-frame column (post entry compute) the filter tests.
    col: usize,
    /// Int-family probes: sorted unique key values.
    ints: []const i64 = &.{},
    /// String probes: sorted unique byte strings.
    strs: []const []const u8 = &.{},
    is_str: bool,

    /// NULL probe values never match (INNER-join equality semantics).
    pub fn mask(self: *const MemberFilter, v: ColumnView, n: usize, out: []bool) !void {
        if (self.is_str) {
            const sv = switch (v.data) {
                .varchar, .string, .char, .json => stringViewOf(v),
                else => return error.UnsupportedQueryShape,
            };
            for (0..n) |i| {
                if (!v.isValid(i)) {
                    out[i] = false;
                    continue;
                }
                out[i] = sortedBytesContains(self.strs, sv.rowBytes(i));
            }
        } else {
            for (0..n) |i| {
                const key = intAt(v, i) orelse {
                    out[i] = false;
                    continue;
                };
                out[i] = std.sort.binarySearch(i64, self.ints, key, struct {
                    fn order(k: i64, item: i64) std.math.Order {
                        return std.math.order(k, item);
                    }
                }.order) != null;
            }
        }
    }
};

fn sortedBytesContains(items: []const []const u8, key: []const u8) bool {
    var lo: usize = 0;
    var hi: usize = items.len;
    while (lo < hi) {
        const mid = lo + (hi - lo) / 2;
        switch (std.mem.order(u8, key, items[mid])) {
            .eq => return true,
            .lt => hi = mid,
            .gt => lo = mid + 1,
        }
    }
    return false;
}

pub const RegionOp = union(enum) {
    /// Row-wise derived columns via the engine expression evaluator (one
    /// exec.Compute instance per worker, evalBatch over the whole shard).
    /// Same-named derived columns REPLACE their frame slot, others append —
    /// exec.Compute's contract.
    compute: struct { derived: []const compute_mod.Derived },
    /// ROW_NUMBER over each rank partition: argsort by `order`, ranks 1..n
    /// appended as a non-null bigint column. Stable on arrival order. A
    /// partition is one region-key range, or — when `merge_on` is set — a
    /// run of ADJACENT ranges with equal `merge_on` value (a window
    /// partitioned coarser than the range keys, e.g. by the region key
    /// alone while ranges also split on a second column; consolidation
    /// sorts ranges so equal-prefix runs are consecutive by construction).
    ranks: struct { name: []const u8, order: []const OrderBy, merge_on: ?usize = null },
    /// LAST_VALUE(src) OVER (PARTITION BY region key) with the frame's
    /// current in-range order: the range's last row broadcast to every row.
    fill_last: struct { name: []const u8, src: usize },
    /// LAG(src, offset) over the frame's current in-range order: NULL for
    /// the first `offset` rows of each range, the value `offset` rows back
    /// otherwise.
    lag: struct { name: []const u8, src: usize, offset: usize },
    /// Keyed aggregation: groups are (range × subkeys). Output rows are
    /// emitted per range, sub-groups ordered by subkey values ascending
    /// NULLS FIRST; ranges rewritten to the per-range output spans. With
    /// `merge_on`, runs of ADJACENT ranges with equal merge_on value form
    /// one aggregation span (same contract as ranks.merge_on — a grouping
    /// coarser than the range keys, consecutive by the consolidation
    /// ordering).
    group_agg: struct { subkeys: []const usize, out: []const AggOut, merge_on: ?usize = null },
    /// Broadcast hash join on an int-family probe column. LEFT appends the
    /// payload columns (NULL on miss); INNER additionally drops non-matching
    /// rows (frame restructure, ranges rewritten in place).
    hash_probe: struct {
        probe: usize,
        map: *const KeyMap,
        payload: []const Payload,
        inner: bool,
    },
    /// Generalized probe: multi-column keys, int and string kinds. The build
    /// side is either a compile-time-drained broadcast block (build keys
    /// must be UNIQUE — a dup is a hard error) or a co-partitioned side
    /// table (`shard` = index into the run's sides; the per-bin map is
    /// built in worker scratch, dup build keys chain and the probe expands
    /// 1:N via a frame restructure). Emit semantics match hash_probe (LEFT
    /// append / INNER restructure) on unique builds.
    keyed_probe: struct {
        pairs: []const KeyedPair,
        side: union(enum) {
            broadcast: BroadcastSide,
            shard: usize,
        },
        payload: []const KeyedPayload,
        inner: bool,
    },
    /// Row-aligned TVF over the whole shard (rf_currency class): one raw-ABI
    /// call, computed output columns appended to the frame. Zero-copy input —
    /// the kernel's declared input-0 columns are frame views.
    tvf_aligned: TvfSpec,
    /// Per-range TVF (partition = region-key group). `union_append` copies
    /// each range then lets the kernel append its rows at the group tail
    /// (rf_estimates class; kernel output schema == frame schema up to the
    /// `inputs` permutation — out store k targets frame column inputs[k]),
    /// so run-contiguity holds by construction. `aligned_append` calls the
    /// kernel per range and APPENDS its columns row-aligned (rf_updown
    /// class: a row-aligned passthrough kernel whose state assumes one
    /// partition per call — whole-shard tvf_aligned would leak LAG/MIN
    /// state across key groups); rows and ranges are unchanged. Otherwise
    /// the kernel output REPLACES the frame (rf_gap_fill class) and the
    /// ranges are rewritten.
    tvf_grouped: struct {
        spec: TvfSpec,
        union_append: bool = false,
        /// Per-range row-aligned append (mutually exclusive with
        /// union_append and input_filter).
        aligned_append: bool = false,
        /// Kernel-input row selection (the estimates date-window): only rows
        /// with `col` non-null and lo <= value <= hi feed the kernel. On
        /// union_append the filtered-out rows still flow through the copy.
        input_filter: ?struct { col: usize, lo: i64, hi: i64 } = null,
    },
    /// Recognizer-folded constant columns: append `values[i]` to every
    /// frame row (NULL when null). Skips per-row expression evaluation for
    /// literal-only computes (e.g. blocks of typed zero columns).
    const_cols: struct { cols: []const Column, values: []const ?types.Value },
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
            errdefer if (getenv("THINDB_REGION_TRACE") != null) {
                std.debug.print("[region] Program.build failed at op {d} ({s}), in width {d}\n", .{ oi, @tagName(op), schema_at[oi].len });
            };
            if (output_schema.len != 0) return error.UnsupportedQueryShape;
            const in = schema_at[oi];
            schema_at[oi + 1] = switch (op) {
                .compute => |c| try computeOutputSchema(base_alloc, a, in, c.derived, registry),
                .ranks => |r| blk: {
                    for (r.order) |ob| if (ob.col >= in.len) return error.UnsupportedQueryShape;
                    if (r.merge_on) |c| _ = try checkCol(in, c);
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
                .lag => |l| blk: {
                    if (l.src >= in.len) return error.UnsupportedQueryShape;
                    break :blk try appendedSchema(a, in, .{
                        .name = try a.dupe(u8, l.name),
                        .type = in[l.src].type,
                        .nullable = true,
                    });
                },
                .group_agg => |g| blk: {
                    if (g.subkeys.len > MAX_SUBKEYS or g.out.len == 0) return error.UnsupportedQueryShape;
                    for (g.subkeys) |c| try requireIntFamily(in, c);
                    if (g.merge_on) |c| _ = try checkCol(in, c);
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
                            .max_str => |c| t: {
                                switch (in[checkCol(in, c) catch return error.UnsupportedQueryShape].type) {
                                    .varchar, .string, .char => {},
                                    else => return error.UnsupportedQueryShape,
                                }
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
                .keyed_probe => |k| blk: {
                    if (k.pairs.len == 0 or k.pairs.len > MAX_KEYED_PAIRS) return error.UnsupportedQueryShape;
                    for (k.pairs) |p| {
                        switch (p.kind) {
                            .int, .int_from_str_build => try requireIntFamily(in, p.probe),
                            .str => switch (in[try checkCol(in, p.probe)].type) {
                                .varchar, .string, .char => {},
                                else => return error.UnsupportedQueryShape,
                            },
                        }
                    }
                    const cols = try a.alloc(Column, in.len + k.payload.len);
                    @memcpy(cols[0..in.len], in);
                    for (k.payload, cols[in.len..]) |p, *col| {
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
                        if (t.aligned_append) return error.UnsupportedQueryShape;
                        // Kernel outputs land on frame columns through the
                        // `inputs` permutation (out k -> frame inputs[k]);
                        // each frame column is covered at most once, and
                        // uncovered ones (e.g. a row-loc tie-break carried
                        // only for the consolidation order) must be nullable
                        // — the kernel's appended rows get NULLs there.
                        if (t.spec.out.len != t.spec.inputs.len or t.spec.inputs.len > in.len) {
                            return error.UnsupportedQueryShape;
                        }
                        const seen = try a.alloc(bool, in.len);
                        @memset(seen, false);
                        for (t.spec.out, t.spec.inputs) |o, ci| {
                            if (seen[ci]) {
                                if (getenv("THINDB_REGION_TRACE") != null) {
                                    std.debug.print("[region] union_append: dup coverage of frame col {d} '{s}'\n", .{ ci, in[ci].name });
                                }
                                return error.UnsupportedQueryShape;
                            }
                            seen[ci] = true;
                            if (!std.meta.eql(o.type, in[ci].type)) {
                                if (getenv("THINDB_REGION_TRACE") != null) {
                                    std.debug.print("[region] union_append: type clash on frame col {d} '{s}' ({s} vs {s})\n", .{ ci, in[ci].name, @tagName(in[ci].type), @tagName(o.type) });
                                }
                                return error.UnsupportedQueryShape;
                            }
                        }
                        for (seen, in) |covered, in_col| {
                            if (!covered and !in_col.nullable) {
                                if (getenv("THINDB_REGION_TRACE") != null) {
                                    std.debug.print("[region] union_append: uncovered non-nullable col '{s}'\n", .{in_col.name});
                                }
                                return error.UnsupportedQueryShape;
                            }
                        }
                        break :blk in;
                    }
                    if (t.aligned_append) {
                        if (t.input_filter != null or t.spec.out.len == 0) {
                            return error.UnsupportedQueryShape;
                        }
                        const cols = try a.alloc(Column, in.len + t.spec.out.len);
                        @memcpy(cols[0..in.len], in);
                        for (t.spec.out, cols[in.len..]) |src, *col| {
                            col.* = src;
                            col.name = try a.dupe(u8, src.name);
                        }
                        break :blk cols;
                    }
                    break :blk try dupeSchema(a, t.spec.out);
                },
                .const_cols => |cc| blk: {
                    if (cc.cols.len != cc.values.len) return error.UnsupportedQueryShape;
                    const cols = try a.alloc(Column, in.len + cc.cols.len);
                    @memcpy(cols[0..in.len], in);
                    for (cc.cols, cols[in.len..]) |src, *col| {
                        col.* = src;
                        col.name = try a.dupe(u8, src.name);
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
pub fn computeOutputSchema(
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
    /// Per-op wall ticks (THINDB_REGION_TRACE only; null otherwise).
    op_ticks: ?[]i64 = null,
    /// Per-op probe sub-phase ticks [map build, probe loop, emit]
    /// (THINDB_REGION_TRACE only; null otherwise).
    probe_ticks: ?[][3]i64 = null,
    cur_op: usize = 0,
    consolidate_ticks: i64 = 0,
    /// Co-partitioned side frames for the CURRENT bin (set by the shard
    /// phase before runShardFrom; indexed by keyed_probe.side.shard).
    side_data: []const ShardData = &.{},

    const OpState = union(enum) {
        compute: ComputeInstance,
        ranks: struct { out: ColumnStore },
        fill_last: struct { out: ColumnStore },
        lag: struct { out: ColumnStore },
        group_agg: struct {
            cols: []ColumnStore,
            ranges: std.ArrayListUnmanaged([2]u32) = .empty,
            cells: []std.ArrayListUnmanaged(AccCell),
            sub_first: std.ArrayListUnmanaged(u32) = .empty,
            map: std.AutoHashMapUnmanaged(SubKey, u32) = .empty,
        },
        hash_probe: ProbeState,
        keyed_probe: ProbeState,
        tvf: TvfState,
        const_cols: struct { out: []ColumnStore },
        emit: void,
    };

    /// Shared state shape for both probe op kinds.
    const ProbeState = struct {
        /// Full-frame compaction stores (inner only; empty for LEFT).
        cols: []ColumnStore,
        pay: []ColumnStore,
        ranges: std.ArrayListUnmanaged([2]u32) = .empty,
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
                .lag => blk: {
                    const col = prog.schema_at[oi + 1][prog.schema_at[oi].len];
                    break :blk .{ .lag = .{ .out = try ColumnStore.init(alloc, col.type, true) } };
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
                .keyed_probe => |k| blk: {
                    const out_schema = prog.schema_at[oi + 1];
                    const n_in = out_schema.len - k.payload.len;
                    const pay = try initStores(alloc, out_schema[n_in..]);
                    errdefer freeStores(alloc, pay);
                    // Shard sides may hold dup build keys → the 1:N
                    // expansion restructures even a LEFT probe.
                    const cols: []ColumnStore = if (k.inner or k.side == .shard)
                        try initStores(alloc, out_schema[0..n_in])
                    else
                        try alloc.alloc(ColumnStore, 0);
                    break :blk .{ .keyed_probe = .{ .cols = cols, .pay = pay } };
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
                    const out_schema = if (t.union_append)
                        in_schema
                    else if (t.aligned_append)
                        prog.schema_at[oi + 1][in_schema.len..]
                    else
                        prog.schema_at[oi + 1];
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
                .const_cols => |cc| blk: {
                    const out = try initStores(alloc, prog.schema_at[oi + 1][prog.schema_at[oi].len..]);
                    _ = cc;
                    break :blk .{ .const_cols = .{ .out = out } };
                },
                .emit => .{ .emit = {} },
            };
            built += 1;
        }
        var op_ticks: ?[]i64 = null;
        var probe_ticks: ?[][3]i64 = null;
        if (getenv("THINDB_REGION_TRACE") != null) {
            const t = try alloc.alloc(i64, prog.ops.len);
            @memset(t, 0);
            op_ticks = t;
            const pt = try alloc.alloc([3]i64, prog.ops.len);
            @memset(pt, .{ 0, 0, 0 });
            probe_ticks = pt;
        }
        return .{
            .alloc = alloc,
            .prog = prog,
            .states = states,
            .scratch = std.heap.ArenaAllocator.init(alloc),
            .views = try alloc.alloc(ColumnView, prog.max_width),
            .op_ticks = op_ticks,
            .probe_ticks = probe_ticks,
        };
    }

    pub fn deinit(self: *RegionWorker) void {
        deinitStates(self.alloc, self.states);
        self.alloc.free(self.states);
        self.alloc.free(self.views);
        if (self.op_ticks) |t| self.alloc.free(t);
        if (self.probe_ticks) |t| self.alloc.free(t);
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
            .lag => |*s| s.out.deinit(alloc),
            .group_agg => |*s| {
                freeStores(alloc, s.cols);
                for (s.cells) |*l| l.deinit(alloc);
                alloc.free(s.cells);
                s.ranges.deinit(alloc);
                s.sub_first.deinit(alloc);
                s.map.deinit(alloc);
            },
            .hash_probe, .keyed_probe => |*s| {
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
            .const_cols => |*s| freeStores(alloc, s.out),
            .emit => {},
        };
    }

    /// Approximate retained capacity across the op states (pool cap input;
    /// engine Compute internals are not walked).
    pub fn retainedBytes(self: *const RegionWorker) usize {
        var n: usize = self.scratch.queryCapacity();
        for (self.states) |*st| switch (st.*) {
            .compute, .emit => {},
            .const_cols => |*s| for (s.out) |*c| {
                n += storeRetainedBytes(c);
            },
            .ranks => |*s| n += storeRetainedBytes(&s.out),
            .fill_last => |*s| n += storeRetainedBytes(&s.out),
            .lag => |*s| n += storeRetainedBytes(&s.out),
            .group_agg => |*s| {
                for (s.cols) |*c| n += storeRetainedBytes(c);
                for (s.cells) |*l| n += l.capacity * @sizeOf(AccCell);
            },
            .hash_probe, .keyed_probe => |*s| {
                for (s.cols) |*c| n += storeRetainedBytes(c);
                for (s.pay) |*c| n += storeRetainedBytes(c);
            },
            .tvf => |*s| {
                for (s.out) |*c| n += storeRetainedBytes(c);
                for (s.in_scratch) |*c| n += storeRetainedBytes(c);
                n += s.call_arena.queryCapacity() + s.worker_arena.queryCapacity();
            },
        };
        return n;
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

        const tick_ops = self.op_ticks != null;
        for (self.prog.ops[start_op..], self.states[start_op..], start_op..) |op, *st, oi| {
            self.cur_op = oi;
            const t_op = if (tick_ops) exec.prof.nowTicks() else 0;
            defer if (tick_ops) {
                self.op_ticks.?[oi] += exec.prof.nowTicks() - t_op;
            };
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
                .lag => |l| {
                    const s = &st.lag;
                    s.out.clear();
                    for (fr.ranges) |rng| {
                        const n: usize = rng[1] - rng[0];
                        if (n == 0) continue;
                        const lead: usize = @min(l.offset, n);
                        try s.out.appendNulls(self.alloc, lead);
                        if (n > lead) {
                            try appendViewRange(self.alloc, &s.out, fr.views[l.src], rng[0], rng[1] - lead);
                        }
                    }
                    fr.views[fr.width] = s.out.view();
                    fr.width += 1;
                },
                .group_agg => |g| try self.runGroupAgg(g, &st.group_agg, &fr),
                .hash_probe => |h| try self.runHashProbe(h, &st.hash_probe, &fr),
                .keyed_probe => |k| try self.runKeyedProbe(k, &st.keyed_probe, &fr),
                .tvf_aligned => |t| try self.runTvfAligned(t, &st.tvf, &fr),
                .tvf_grouped => |t| if (t.aligned_append)
                    try self.runTvfGroupedAligned(t, &st.tvf, &fr)
                else
                    try self.runTvfGrouped(t, &st.tvf, &fr),
                .const_cols => |cc| {
                    const s = &st.const_cols;
                    for (s.out, cc.values) |*store, v| {
                        store.clear();
                        try fillConst(self.alloc, store, v, fr.rows);
                    }
                    for (s.out, fr.views[fr.width..][0..s.out.len]) |*c, *v| v.* = c.view();
                    fr.width += s.out.len;
                },
                .emit => |e| {
                    for (e.cols, out) |c, *dst| {
                        try appendViewRange(self.alloc, dst, fr.views[c], 0, fr.rows);
                    }
                },
            }
        }
    }

    /// Lever 3: precomputed normalized sort keys per order column (the
    /// consolidation trick applied in-group). `keys == null` falls back to
    /// the generic comparator (float/decimal orders); `lossy` marks the
    /// i64-family norm (its low bit is folded into the null flag), which
    /// resolves norm ties through the exact comparator so ordering stays
    /// correct for arbitrary values.
    const NormCol = struct {
        keys: ?[]const RowKey,
        lossy: bool,
    };

    const RankCtx = struct {
        views: []const ColumnView,
        order: []const OrderBy,
        norms: []const NormCol,
        base: u32,

        fn less(ctx: @This(), x: u32, y: u32) bool {
            for (ctx.order, ctx.norms) |ob, nc| {
                var o: std.math.Order = .eq;
                if (nc.keys) |ks| {
                    const a = ks[ctx.base + x];
                    const b = ks[ctx.base + y];
                    if (a.norm != b.norm) {
                        o = if (a.norm < b.norm) .lt else .gt;
                    } else if (a.str.len != 0 or b.str.len != 0) {
                        o = std.mem.order(u8, a.str, b.str);
                    } else if (nc.lossy) {
                        o = viewOrderRows(ctx.views[ob.col], ctx.base + x, ctx.base + y);
                    }
                } else {
                    o = viewOrderRows(ctx.views[ob.col], ctx.base + x, ctx.base + y);
                }
                if (ob.desc) o = o.invert();
                if (o != .eq) return o == .lt;
            }
            return x < y;
        }
    };

    fn buildNormKeys(sa: Allocator, v: ColumnView, rows: usize) !NormCol {
        switch (v.data) {
            inline .tinyint, .smallint, .int, .date => |vals| {
                const ks = try sa.alloc(RowKey, rows);
                for (ks, 0..) |*k, i| k.* = .{ .norm = normI32(v.isValid(i), vals[i]), .str = "" };
                return .{ .keys = ks, .lossy = false };
            },
            inline .bigint, .datetime => |vals| {
                const ks = try sa.alloc(RowKey, rows);
                for (ks, 0..) |*k, i| k.* = .{ .norm = normI64(v.isValid(i), vals[i]), .str = "" };
                return .{ .keys = ks, .lossy = true };
            },
            .varchar, .string, .char, .json => |sv| {
                const ks = try sa.alloc(RowKey, rows);
                for (ks, 0..) |*k, i| {
                    const valid = v.isValid(i);
                    const s: []const u8 = if (valid) sv.rowBytes(i) else "";
                    k.* = .{ .norm = normStrPrefix(valid, s), .str = s };
                }
                return .{ .keys = ks, .lossy = false };
            },
            else => return .{ .keys = null, .lossy = false },
        }
    }

    fn runRanks(
        self: *RegionWorker,
        r: anytype,
        store: *ColumnStore,
        fr: *Frame,
    ) !void {
        store.clear();
        const out_slice = try store.data.bigint.addManyAsSlice(self.alloc, fr.rows);
        const sa = self.scratch.allocator();
        const norms = try sa.alloc(NormCol, r.order.len);
        for (r.order, norms) |ob, *nc| nc.* = try buildNormKeys(sa, fr.views[ob.col], fr.rows);
        var ri: usize = 0;
        while (ri < fr.ranges.len) : (ri += 1) {
            const lo = fr.ranges[ri][0];
            var hi = fr.ranges[ri][1];
            if (r.merge_on) |mc| {
                // A partition coarser than the range keys: absorb adjacent
                // ranges with the same merge_on value (equal-prefix runs are
                // consecutive by the consolidation ordering contract).
                while (ri + 1 < fr.ranges.len) {
                    const nxt = fr.ranges[ri + 1];
                    if (nxt[0] != nxt[1] and lo != hi and
                        viewOrderRows(fr.views[mc], lo, nxt[0]) != .eq) break;
                    hi = nxt[1];
                    ri += 1;
                }
            }
            const n = hi - lo;
            if (n == 0) continue;
            const ord = try sa.alloc(u32, n);
            for (ord, 0..) |*o, k| o.* = @intCast(k);
            const ctx = RankCtx{
                .views = fr.views[0..fr.width],
                .order = r.order,
                .norms = norms,
                .base = lo,
            };
            std.mem.sortUnstable(u32, ord, ctx, RankCtx.less);
            for (ord, 1..) |li, rk| out_slice[lo + li] = @intCast(rk);
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

        var ri: usize = 0;
        while (ri < fr.ranges.len) : (ri += 1) {
            var rng = fr.ranges[ri];
            if (g.merge_on) |mc| {
                // Aggregation span coarser than the range keys: absorb
                // adjacent ranges with the same merge_on value (equal runs
                // are consecutive by the consolidation ordering contract).
                while (ri + 1 < fr.ranges.len) {
                    const nxt = fr.ranges[ri + 1];
                    if (nxt[0] != nxt[1] and rng[0] != rng[1] and
                        viewOrderRows(fr.views[mc], rng[0], nxt[0]) != .eq) break;
                    rng[1] = nxt[1];
                    ri += 1;
                }
            }
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
                    .max_str => |c| accumMaxStr(cells.items, ord_of, fr.views[c], rng[0]),
                    .max_by => |mb| accumMaxBy(cells.items, ord_of, fr.views[mb.val], fr.views[mb.ord], rng[0]),
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

            // Emission is column-major with the agg kind (and the value
            // type inside scatterColumn) hoisted out of the group loop —
            // the same discipline as the accumulate sweeps. Row-sourced
            // kinds bulk-gather via scatterColumn when every sub-group has
            // a winner (the overwhelmingly common case) and fall back to
            // the per-group append only when NULL groups exist.
            const start: u32 = @intCast(if (s.cols.len > 0) s.cols[0].rowCount() else 0);
            const rows_buf = try sa.alloc(u32, nsub);
            for (g.out, s.cells, s.cols) |spec, *cells, *dst| {
                switch (spec.kind) {
                    .first => |c| {
                        for (sord, rows_buf) |gi, *r| r.* = s.sub_first.items[gi];
                        try scatterColumn(alloc, dst, fr.views[c], rows_buf);
                    },
                    .sum_int => for (sord) |gi| {
                        const cell = &cells.items[gi];
                        if (cell.seen) {
                            try dst.data.bigint.append(alloc, cell.i);
                            try dst.appendValidBit(alloc, dst.rowCount() - 1, true);
                        } else try dst.appendNulls(alloc, 1);
                    },
                    .sum_float => for (sord) |gi| {
                        const cell = &cells.items[gi];
                        if (cell.seen) {
                            try dst.data.double.append(alloc, cell.f);
                            try dst.appendValidBit(alloc, dst.rowCount() - 1, true);
                        } else try dst.appendNulls(alloc, 1);
                    },
                    .min_int, .max_int => for (sord) |gi| {
                        const cell = &cells.items[gi];
                        if (cell.seen) {
                            try appendI64As(alloc, dst, cell.i);
                        } else try dst.appendNulls(alloc, 1);
                    },
                    .max_str, .max_by => {
                        const src = switch (spec.kind) {
                            .max_str => |c| fr.views[c],
                            .max_by => |mb| fr.views[mb.val],
                            else => unreachable,
                        };
                        var all_seen = true;
                        for (sord) |gi| {
                            if (!cells.items[gi].seen) {
                                all_seen = false;
                                break;
                            }
                        }
                        if (all_seen) {
                            for (sord, rows_buf) |gi, *r| r.* = cells.items[gi].row;
                            try scatterColumn(alloc, dst, src, rows_buf);
                        } else {
                            for (sord) |gi| {
                                const cell = &cells.items[gi];
                                if (cell.seen) {
                                    try appendRowValue(alloc, dst, src, cell.row);
                                } else try dst.appendNulls(alloc, 1);
                            }
                        }
                    },
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
        // union_append: one out pointer PER KERNEL OUTPUT (spec.inputs maps
        // each back onto its frame column; partial coverage means fewer
        // outputs than frame columns — mirroring fusedTailRun). Sizing this
        // to s.out.len zipped garbage past spec.inputs' end.
        const out_ptrs = if (t.union_append)
            try sa.alloc(*ColumnStore, t.spec.inputs.len)
        else
            try sa.alloc(*ColumnStore, s.out.len);
        if (t.union_append) {
            for (out_ptrs, t.spec.inputs) |*p, ci| p.* = &s.out[ci];
        } else {
            for (s.out, out_ptrs) |*c, *p| p.* = c;
        }

        for (fr.ranges) |rng| {
            const start: u32 = @intCast(s.out[0].rowCount());
            if (t.union_append) {
                for (s.out, fr.views[0..fr.width]) |*dst, v| {
                    try appendViewRange(alloc, dst, v, rng[0], rng[1]);
                }
            }
            try self.kernelOverRange(t, s, fr.views[0..fr.width], rng[0], rng[1], out_ptrs, null);
            if (t.union_append) try padUncovered(alloc, s.out, t.spec.inputs);
            try s.ranges.append(alloc, .{ start, @intCast(s.out[0].rowCount()) });
        }

        fr.width = s.out.len;
        for (s.out, fr.views[0..fr.width]) |*c, *v| v.* = c.view();
        fr.rows = s.out[0].rowCount();
        fr.ranges = s.ranges.items;
    }

    /// aligned_append: one kernel call per range, output columns appended
    /// to the frame row-aligned (validated per range); rows and ranges are
    /// untouched.
    fn runTvfGroupedAligned(self: *RegionWorker, t: anytype, s: *TvfState, fr: *Frame) !void {
        for (s.out) |*c| c.clear();
        const sa = self.scratch.allocator();
        const out_ptrs = try sa.alloc(*ColumnStore, s.out.len);
        for (s.out, out_ptrs) |*c, *p| p.* = c;
        for (fr.ranges) |rng| {
            if (rng[0] == rng[1]) continue;
            const want = s.out[0].rowCount() + (rng[1] - rng[0]);
            try self.kernelOverRange(t, s, fr.views[0..fr.width], rng[0], rng[1], out_ptrs, want);
        }
        for (s.out, fr.views[fr.width..][0..s.out.len]) |*c, *v| v.* = c.view();
        fr.width += s.out.len;
    }

    /// Select the kernel-visible rows of [lo,hi) per the input filter and
    /// call the kernel appending onto `out_ptrs`. Unfiltered inputs pass
    /// ZERO-COPY range-sliced views (a range is a contiguous frame span);
    /// filtered inputs copy the surviving rows contiguous into the op's
    /// scratch partition. Shared by the unfused per-range path and the
    /// consolidation-tail fusion.
    fn kernelOverRange(
        self: *RegionWorker,
        t: anytype,
        s: *TvfState,
        views: []const ColumnView,
        lo: usize,
        hi: usize,
        out_ptrs: []*ColumnStore,
        expect_rows: ?usize,
    ) !void {
        const sa = self.scratch.allocator();
        if (t.input_filter == null) {
            if (hi == lo) return;
            const in_views = try sa.alloc(ColumnView, t.spec.inputs.len);
            for (t.spec.inputs, in_views) |ci, *v| v.* = try sliceViewRange(sa, views[ci], lo, hi);
            try self.callTvf(t.spec, s, in_views, hi - lo, out_ptrs, expect_rows);
            return;
        }

        var kin: std.ArrayListUnmanaged(u32) = .empty;
        const f = t.input_filter.?;
        for (lo..hi) |i| {
            const v = viewI64(views[f.col], i) orelse continue;
            if (v >= f.lo and v <= f.hi) try kin.append(sa, @intCast(i));
        }
        if (kin.items.len == 0) return;

        for (s.in_scratch) |*c| c.clear();
        for (s.in_scratch, t.spec.inputs) |*dst, ci| {
            try scatterColumn(self.alloc, dst, views[ci], kin.items);
        }
        const in_views = try sa.alloc(ColumnView, t.spec.inputs.len);
        for (s.in_scratch, in_views) |*c, *v| v.* = c.view();
        try self.callTvf(t.spec, s, in_views, kin.items.len, out_ptrs, expect_rows);
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
        const out_ptrs = try sa.alloc(*ColumnStore, t.spec.inputs.len);
        for (out_ptrs, t.spec.inputs) |*p, ci| p.* = &out.cols[ci];
        try self.kernelOverRange(t, s, views, g_start, out.cols[0].rowCount(), out_ptrs, null);
        try padUncovered(self.alloc, out.cols[0..width], t.spec.inputs);
    }

    /// Union-append with a partial `inputs` coverage: frame columns no
    /// kernel output maps to (nullable by build contract) get NULLs for the
    /// kernel's appended rows.
    fn padUncovered(alloc: Allocator, cols: []ColumnStore, inputs: []const usize) !void {
        if (inputs.len == 0 or inputs.len == cols.len) return;
        const target = cols[inputs[0]].rowCount();
        for (cols, 0..) |*c, ci| {
            var covered = false;
            for (inputs) |ic| {
                if (ic == ci) {
                    covered = true;
                    break;
                }
            }
            if (covered) continue;
            const have = c.rowCount();
            if (have < target) try c.appendNulls(alloc, target - have);
        }
    }

    /// Lever 6: one hoisted-type pass resolves every row's build-side
    /// ordinal (NO_MATCH on miss/NULL probe); payload fills are then bulk
    /// typed gathers instead of per-row typed appends.
    fn probeMatchOrds(sa: Allocator, map: *const KeyMap, v: ColumnView, rows: usize) ![]u32 {
        const ords = try sa.alloc(u32, rows);
        switch (v.data) {
            inline .tinyint, .smallint, .int, .bigint, .date, .datetime => |vals| {
                for (ords, 0..) |*slot, i| {
                    slot.* = if (v.isValid(i)) (map.get(vals[i]) orelse NO_MATCH) else NO_MATCH;
                }
            },
            else => unreachable, // build-validated int family
        }
        return ords;
    }

    fn runHashProbe(self: *RegionWorker, h: anytype, s: *ProbeState, fr: *Frame) !void {
        const sa = self.scratch.allocator();
        const tick = self.probe_ticks != null;
        const t0 = if (tick) exec.prof.nowTicks() else 0;
        const ords = try probeMatchOrds(sa, h.map, fr.views[h.probe], fr.rows);
        const t1 = if (tick) exec.prof.nowTicks() else 0;
        if (tick) self.probe_ticks.?[self.cur_op][1] += t1 - t0;
        const pay_views = try sa.alloc(ColumnView, h.payload.len);
        for (h.payload, pay_views) |p, *v| v.* = p.view;
        try self.emitProbe(s, fr, ords, pay_views, h.inner);
        if (tick) self.probe_ticks.?[self.cur_op][2] += exec.prof.nowTicks() - t1;
    }

    fn runKeyedProbe(self: *RegionWorker, k: anytype, s: *ProbeState, fr: *Frame) !void {
        const sa = self.scratch.allocator();
        const tick = self.probe_ticks != null;
        const t0 = if (tick) exec.prof.nowTicks() else 0;

        var map_storage: MultiKeyMap = .empty;
        var interner_storage: [MAX_KEYED_PAIRS]StrInterner = @splat(.empty);
        var map: *const MultiKeyMap = undefined;
        var interners_buf: [MAX_KEYED_PAIRS]?*const StrInterner = @splat(null);
        var interners: []const ?*const StrInterner = undefined;
        var build_views: []const ColumnView = undefined;
        var next: ?[]u32 = null;
        var has_dups = false;

        switch (k.side) {
            .broadcast => |b| {
                map = b.map;
                interners = b.interners;
                build_views = b.views;
            },
            .shard => |si| {
                // Co-partitioned side: build the map over THIS bin's side
                // rows (scratch-lifetime; capacity retained by the arena).
                // Dup build keys chain (newest at head, walked in reverse
                // for build order) and route the probe through the 1:N
                // expansion emit.
                const sd = &self.side_data[si];
                const views = try sa.alloc(ColumnView, sd.cols.len);
                for (sd.cols, views) |*c, *v| v.* = c.view();
                build_views = views;
                for (k.pairs, 0..) |p, pi| {
                    if (p.kind == .str) interners_buf[pi] = &interner_storage[pi];
                }
                next = try sa.alloc(u32, sd.rows);
                rows: for (0..sd.rows) |i| {
                    next.?[i] = NO_MATCH;
                    var key: MultiKey = @splat(0);
                    for (k.pairs, 0..) |p, pi| {
                        const v = views[p.build];
                        switch (p.kind) {
                            .int => key[pi] = intAt(v, i) orelse continue :rows,
                            .int_from_str_build => {
                                if (!v.isValid(i)) continue :rows;
                                const bytes = stringViewOf(v).rowBytes(i);
                                key[pi] = std.fmt.parseInt(i64, bytes, 10) catch continue :rows;
                            },
                            .str => {
                                if (!v.isValid(i)) continue :rows;
                                const gop = try interner_storage[pi].getOrPut(sa, stringViewOf(v).rowBytes(i));
                                if (!gop.found_existing) gop.value_ptr.* = @intCast(interner_storage[pi].count());
                                key[pi] = gop.value_ptr.*;
                            },
                        }
                    }
                    const gop = try map_storage.getOrPut(sa, key);
                    if (gop.found_existing) {
                        next.?[i] = gop.value_ptr.*;
                        gop.value_ptr.* = @intCast(i);
                        has_dups = true;
                    } else {
                        gop.value_ptr.* = @intCast(i);
                    }
                }
                map = &map_storage;
                interners = interners_buf[0..k.pairs.len];
            },
        }
        const t1 = if (tick) exec.prof.nowTicks() else 0;
        if (tick) self.probe_ticks.?[self.cur_op][0] += t1 - t0;

        // A build-side dup chain only matters if some probe row actually
        // hits it — bins where no probed key is duplicated take the cheap
        // non-expanding emit.
        var dup_probed = false;
        const ords = try sa.alloc(u32, fr.rows);
        rows: for (0..fr.rows) |i| {
            var key: MultiKey = @splat(0);
            for (k.pairs, 0..) |p, pi| {
                const v = fr.views[p.probe];
                switch (p.kind) {
                    .int, .int_from_str_build => key[pi] = intAt(v, i) orelse {
                        ords[i] = NO_MATCH;
                        continue :rows;
                    },
                    .str => {
                        if (!v.isValid(i)) {
                            ords[i] = NO_MATCH;
                            continue :rows;
                        }
                        const interner = interners[pi].?;
                        key[pi] = interner.get(stringViewOf(v).rowBytes(i)) orelse {
                            ords[i] = NO_MATCH;
                            continue :rows;
                        };
                    },
                }
            }
            const hit = map.get(key) orelse {
                ords[i] = NO_MATCH;
                continue :rows;
            };
            ords[i] = hit;
            if (has_dups and next.?[hit] != NO_MATCH) dup_probed = true;
        }

        const t2 = if (tick) exec.prof.nowTicks() else 0;
        if (tick) self.probe_ticks.?[self.cur_op][1] += t2 - t1;

        const pay_views = try sa.alloc(ColumnView, k.payload.len);
        for (k.payload, pay_views) |p, *v| v.* = build_views[p.src];
        if (dup_probed) {
            try self.emitProbeExpand(s, fr, ords, next.?, pay_views, k.inner);
        } else {
            try self.emitProbe(s, fr, ords, pay_views, k.inner);
        }
        if (tick) self.probe_ticks.?[self.cur_op][2] += exec.prof.nowTicks() - t2;
    }

    /// Shared probe emit: LEFT appends payload columns (NULL on miss);
    /// INNER additionally drops non-matching rows (frame restructure,
    /// ranges rewritten).
    fn emitProbe(self: *RegionWorker, s: *ProbeState, fr: *Frame, ords: []const u32, pay_views: []const ColumnView, inner: bool) !void {
        const alloc = self.alloc;
        const sa = self.scratch.allocator();
        for (s.pay) |*c| c.clear();

        if (!inner) {
            for (pay_views, s.pay) |pv, *dst| {
                try gatherColumnOpt(alloc, dst, pv, ords);
            }
            for (s.pay, fr.views[fr.width..][0..s.pay.len]) |*c, *v| v.* = c.view();
            fr.width += s.pay.len;
            return;
        }

        // Inner probe where every row matches: no row drops, so the frame
        // layout is untouched — payloads append LEFT-style and the full
        // restructure copy is skipped (the dominant probe cost on
        // high-match joins).
        {
            var all_match = true;
            for (ords) |o| {
                if (o == NO_MATCH) {
                    all_match = false;
                    break;
                }
            }
            if (all_match) {
                for (pay_views, s.pay) |pv, *dst| {
                    try gatherColumnOpt(alloc, dst, pv, ords);
                }
                for (s.pay, fr.views[fr.width..][0..s.pay.len]) |*c, *v| v.* = c.view();
                fr.width += s.pay.len;
                return;
            }
        }

        for (s.cols) |*c| c.clear();
        s.ranges.clearRetainingCapacity();
        var keep: std.ArrayListUnmanaged(u32) = .empty;
        var match: std.ArrayListUnmanaged(u32) = .empty;

        // One keep/match list across all ranges, then one scatter per
        // column — range boundaries recorded as output offsets.
        var range_start: u32 = 0;
        for (fr.ranges) |rng| {
            for (rng[0]..rng[1]) |i| {
                if (ords[i] == NO_MATCH) continue;
                try keep.append(sa, @intCast(i));
                try match.append(sa, ords[i]);
            }
            const range_end: u32 = @intCast(keep.items.len);
            try s.ranges.append(alloc, .{ range_start, range_end });
            range_start = range_end;
        }
        const use_runs = runsDominate(keep.items);
        for (s.cols, fr.views[0..fr.width]) |*dst, v| {
            if (use_runs)
                try scatterColumnRuns(alloc, dst, v, keep.items)
            else
                try scatterColumn(alloc, dst, v, keep.items);
        }
        for (pay_views, s.pay) |pv, *dst| {
            try gatherColumnOpt(alloc, dst, pv, match.items);
        }

        fr.width = s.cols.len + s.pay.len;
        for (s.cols, fr.views[0..s.cols.len]) |*c, *v| v.* = c.view();
        for (s.pay, fr.views[s.cols.len..fr.width]) |*c, *v| v.* = c.view();
        fr.rows = s.cols[0].rowCount();
        fr.ranges = s.ranges.items;
    }

    /// 1:N probe emit (dup build keys): a probe row with M chained matches
    /// becomes M output rows (LEFT: a matchless row becomes one NULL-payload
    /// row; INNER: it is dropped). Full frame restructure — probe columns
    /// scatter with repetition, ranges rewritten; expanded rows stay inside
    /// their range, so downstream range/group contracts hold.
    fn emitProbeExpand(self: *RegionWorker, s: *ProbeState, fr: *Frame, heads: []const u32, next: []const u32, pay_views: []const ColumnView, inner: bool) !void {
        const alloc = self.alloc;
        const sa = self.scratch.allocator();
        for (s.cols) |*c| c.clear();
        for (s.pay) |*c| c.clear();
        s.ranges.clearRetainingCapacity();
        var keep: std.ArrayListUnmanaged(u32) = .empty;
        var match: std.ArrayListUnmanaged(u32) = .empty;
        var chain: std.ArrayListUnmanaged(u32) = .empty;

        // One keep/match list across all ranges, then one scatter per
        // column — range boundaries recorded as output offsets.
        var range_start: u32 = 0;
        for (fr.ranges) |rng| {
            for (rng[0]..rng[1]) |i| {
                const head = heads[i];
                if (head == NO_MATCH) {
                    if (!inner) {
                        try keep.append(sa, @intCast(i));
                        try match.append(sa, NO_MATCH);
                    }
                    continue;
                }
                // Chains are prepend-built (newest first) — walk then
                // reverse so matches emit in build order.
                chain.clearRetainingCapacity();
                var m = head;
                while (m != NO_MATCH) : (m = next[m]) try chain.append(sa, m);
                std.mem.reverse(u32, chain.items);
                for (chain.items) |m2| {
                    try keep.append(sa, @intCast(i));
                    try match.append(sa, m2);
                }
            }
            const range_end: u32 = @intCast(keep.items.len);
            try s.ranges.append(alloc, .{ range_start, range_end });
            range_start = range_end;
        }
        const use_runs = runsDominate(keep.items);
        for (s.cols, fr.views[0..fr.width]) |*dst, v| {
            if (use_runs)
                try scatterColumnRuns(alloc, dst, v, keep.items)
            else
                try scatterColumn(alloc, dst, v, keep.items);
        }
        for (pay_views, s.pay) |pv, *dst| {
            try gatherColumnOpt(alloc, dst, pv, match.items);
        }

        fr.width = s.cols.len + s.pay.len;
        for (s.cols, fr.views[0..s.cols.len]) |*c, *v| v.* = c.view();
        for (s.pay, fr.views[s.cols.len..fr.width]) |*c, *v| v.* = c.view();
        fr.rows = s.cols[0].rowCount();
        fr.ranges = s.ranges.items;
    }
};

/// Zero-copy view over rows [lo,hi) of `v`. Fixed-width data slices
/// directly; string views slice their offsets (byte positions are absolute
/// into the shared bytes buffer, so no rebase is needed). The validity
/// bitmap slices in place on byte-aligned lo and otherwise re-bases into
/// `sa` scratch — rows/8 bytes, ~1/64 of the data a full copy would move.
fn sliceViewRange(sa: Allocator, v: ColumnView, lo: usize, hi: usize) !ColumnView {
    const data: @TypeOf(v.data) = switch (v.data) {
        inline .int, .bigint, .boolean, .float, .double, .date, .datetime, .tinyint, .smallint, .largeint, .decimal64, .decimal128, .uuid => |s, tag| @unionInit(@TypeOf(v.data), @tagName(tag), s[lo..hi]),
        inline .varchar, .string, .char, .json => |s, tag| @unionInit(@TypeOf(v.data), @tagName(tag), .{
            .offsets = s.offsets[lo .. hi + 1],
            .bytes = s.bytes,
        }),
    };
    var nulls: ?[]const u8 = null;
    if (v.nulls) |bm| {
        if (lo % 8 == 0) {
            nulls = bm[lo / 8 .. (hi + 7) / 8];
        } else {
            const n = hi - lo;
            const buf = try sa.alloc(u8, (n + 7) / 8);
            @memset(buf, 0);
            for (0..n) |i| {
                if (storage.column.isValidBit(bm, lo + i)) buf[i >> 3] |= @as(u8, 1) << @intCast(i & 7);
            }
            nulls = buf;
        }
    }
    return .{ .data = data, .nulls = nulls };
}

/// Int-family view read (NULL → null). Mirrors the recognizer's i64At.
fn intAt(v: ColumnView, i: usize) ?i64 {
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
    /// Order-aligned mode (#186): sources are per-key-interval scans over a
    /// table physically ordered by the declared keys — no hash exchange;
    /// each source's output is per-segment key-sorted runs (memtable
    /// residue last, unsorted) merged directly into the shard frame. The
    /// row-loc column that detects run boundaries is `loc_col`.
    ordered: bool = false,
    loc_col: usize = 0,
    /// Ordered mode: pre-scan per-interval row estimates (from RG quantile
    /// weights) driving the fused scan+exec LPT assignment. Empty = assume
    /// equal-sized intervals (the quantile bounds target that anyway).
    iv_rows_est: []const u64 = &.{},
    /// Scan-fused membership filters (compile-converted semi-joins),
    /// applied to entry batches before the exchange scatter. Hash-exchange
    /// path only — the compiler declines the conversion in ordered mode.
    member_filters: []const MemberFilter = &.{},
    /// Ordered mode: cache-ctx-owned measured-cost slot (len n_shards).
    /// All-zero = unfilled: the first run fills it with measured
    /// per-interval ticks (scan+exec), then it is frozen — later runs LPT
    /// on real costs (row counts miss group/append skew) while the
    /// assignment stays deterministic, keeping pooled slot high-waters put.
    iv_cost_slot: ?[]i64 = null,
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

    /// Reuse across runs (capacity retained).
    pub fn clear(self: *RegionResult) void {
        for (self.shards) |*s| {
            for (s.cols) |*c| c.clear();
            s.rows = 0;
        }
    }

    /// E3 exit transfer: move every shard's stores out as an OwnedChunks
    /// handle (shard order preserved; empty shards ride along as 0-row
    /// chunks — the adopter appends no views for them and their stores
    /// sweep normally). The result is left empty and safe to deinit.
    pub fn takeOwnedChunks(self: *RegionResult) !exec.OwnedChunks {
        const chunks = try self.alloc.alloc(exec.OwnedChunk, self.shards.len);
        for (self.shards, chunks) |*s, *c| {
            c.* = .{ .stores = s.cols, .rows = s.rows };
            s.cols = &.{};
            s.rows = 0;
        }
        const shards = self.shards;
        self.shards = &.{};
        self.alloc.free(shards);
        return .{ .chunks = chunks, .alloc = self.alloc };
    }

    pub fn totalRows(self: *const RegionResult) usize {
        var n: usize = 0;
        for (self.shards) |s| n += s.rows;
        return n;
    }
};

/// Lever 4: region slab pool. Everything a region run allocates that is
/// shape-stable across executions of the SAME compiled program — the
/// exchange buckets and the per-thread interpreter workers (op-state
/// stores, TVF worker arenas/state, shard scratch) — clears rather than
/// frees between runs, the discipline that took the probe from 1.33s cold
/// to 0.72s steady. The pool's owner is whoever owns the compiled program
/// (a plan-cache entry once the recognizer lands); per-shard RESULT stores
/// are deliberately NOT pooled — the E3 exit adopts them into a Stage.
/// `releaseRun` frees everything once retained capacity exceeds the cap.
pub const RegionPool = struct {
    alloc: Allocator,
    /// Approximate retained-capacity ceiling (store capacities + arena
    /// footprints; validity bitmaps excluded — ~1/64 of data).
    max_retained_bytes: usize = 512 << 20,
    prog: ?*const Program = null,
    n_shards: usize = 0,
    ex: ?Exchange = null,
    /// One exchange per co-partitioned side table (same n_shards / route
    /// hash as the main exchange).
    side_ex: []Exchange = &.{},
    /// Ordered-mode per-interval stores (pooled instead of exchange buckets).
    ivs: []OrderedInterval = &.{},
    slots: std.ArrayListUnmanaged(*WorkerSlot) = .empty,

    pub const WorkerSlot = struct {
        rw: RegionWorker,
        sd: ShardData = .{},
        /// Per-side gathered frames for the slot's CURRENT bin.
        sides: []ShardData = &.{},
        scratch: std.heap.ArenaAllocator,
    };

    pub fn init(alloc: Allocator, max_retained_bytes: usize) RegionPool {
        return .{ .alloc = alloc, .max_retained_bytes = max_retained_bytes };
    }

    pub fn deinit(self: *RegionPool) void {
        self.reset();
        self.slots.deinit(self.alloc);
        self.* = undefined;
    }

    fn reset(self: *RegionPool) void {
        if (self.ex) |*ex| ex.deinit();
        self.ex = null;
        for (self.side_ex) |*ex| ex.deinit();
        self.alloc.free(self.side_ex);
        self.side_ex = &.{};
        for (self.ivs) |*iv| {
            iv.sd.deinit(self.alloc);
            iv.runs.deinit(self.alloc);
        }
        self.alloc.free(self.ivs);
        self.ivs = &.{};
        for (self.slots.items) |slot| {
            slot.rw.deinit();
            slot.sd.deinit(self.alloc);
            for (slot.sides) |*sd| sd.deinit(self.alloc);
            self.alloc.free(slot.sides);
            slot.scratch.deinit();
            self.alloc.destroy(slot);
        }
        self.slots.clearRetainingCapacity();
        self.prog = null;
    }

    /// Prepare the pool for a run of `prog`: reuse (clear) when the program
    /// and shape match the previous run, rebuild otherwise. Ordered mode
    /// pools per-interval stores instead of exchange buckets.
    fn acquire(self: *RegionPool, prog: *const Program, opts: DriverOpts, sides: []const SideInput) !void {
        if (opts.ordered) {
            const match = self.prog == prog and self.ivs.len == opts.n_shards;
            if (!match) {
                self.reset();
                self.ivs = try self.alloc.alloc(OrderedInterval, opts.n_shards);
                @memset(self.ivs, .{});
                self.prog = prog;
                self.n_shards = opts.n_shards;
            } else {
                for (self.ivs) |*iv| {
                    if (iv.sd.built) {
                        for (iv.sd.cols) |*c| c.clear();
                        iv.sd.ranges.clearRetainingCapacity();
                        iv.sd.rows = 0;
                    }
                    iv.runs.clearRetainingCapacity();
                    iv.last_loc = std.math.minInt(i64);
                    iv.last_valid = true;
                }
            }
        } else {
            const match = self.prog == prog and self.n_shards == opts.n_shards and
                self.ex != null and self.ex.?.n_workers == opts.n_threads and
                self.side_ex.len == sides.len;
            if (!match) {
                self.reset();
                self.ex = try Exchange.init(self.alloc, prog.schema_at[0], opts.n_threads, opts.n_shards, opts.key_col);
                self.prog = prog;
                self.n_shards = opts.n_shards;
                const side_ex = try self.alloc.alloc(Exchange, sides.len);
                var built: usize = 0;
                errdefer {
                    for (side_ex[0..built]) |*ex| ex.deinit();
                    self.alloc.free(side_ex);
                }
                for (sides, side_ex) |side, *ex| {
                    ex.* = try Exchange.init(self.alloc, side.pre_schema, opts.n_threads, opts.n_shards, side.key_col);
                    built += 1;
                }
                self.side_ex = side_ex;
            } else {
                self.ex.?.clear();
                for (self.side_ex) |*ex| ex.clear();
            }
        }
        // Tick counters are per-RUN in the trace output; without this the
        // pooled path accumulates across runs and the per-op report lies.
        for (self.slots.items) |slot| {
            slot.rw.consolidate_ticks = 0;
            if (slot.rw.op_ticks) |ticks| @memset(ticks, 0);
            if (slot.rw.probe_ticks) |ticks| @memset(ticks, .{ 0, 0, 0 });
        }
        while (self.slots.items.len < opts.n_threads) {
            const slot = try self.alloc.create(WorkerSlot);
            errdefer self.alloc.destroy(slot);
            const slot_sides = try self.alloc.alloc(ShardData, sides.len);
            errdefer self.alloc.free(slot_sides);
            @memset(slot_sides, .{});
            slot.* = .{
                .rw = try RegionWorker.init(self.alloc, prog),
                .sides = slot_sides,
                .scratch = std.heap.ArenaAllocator.init(self.alloc),
            };
            try self.slots.append(self.alloc, slot);
        }
    }

    /// End-of-run retention policy: keep everything (cleared) for the next
    /// run while under the cap. Over the cap, release PARTIALLY: the
    /// exchanges track live data (re-scattered every run — retaining them
    /// is pure fault avoidance), while worker-slot stores ratchet toward
    /// whale-bin × slots and carry the blowup — free the fattest slots
    /// first until under the cap. Full reset only when the exchanges alone
    /// exceed it (or on any rebuild failure).
    pub fn releaseRun(self: *RegionPool) void {
        var total = self.retainedBytes();
        if (total <= self.max_retained_bytes) return;

        const n = self.slots.items.len;
        if (n == 0 or n > 128 or self.prog == null) {
            self.reset();
            return;
        }
        var sizes: [128]usize = undefined;
        var slot_total: usize = 0;
        for (self.slots.items, 0..) |slot, i| {
            sizes[i] = slotRetainedBytes(slot);
            slot_total += sizes[i];
        }
        if (total - slot_total > self.max_retained_bytes) {
            self.reset();
            return;
        }
        var released: usize = 0;
        while (total > self.max_retained_bytes) {
            var big: usize = 0;
            for (sizes[0..n], 0..) |s, i| {
                if (s > sizes[big]) big = i;
            }
            if (sizes[big] == 0) break;
            if (!self.releaseSlotStores(self.slots.items[big])) {
                self.reset();
                return;
            }
            total -= @min(sizes[big], total);
            sizes[big] = 0;
            released += 1;
        }
        if (getenv("THINDB_REGION_TRACE") != null) {
            std.debug.print("[region] partial release: {d}/{d} slots freed, retained ~{d}MB\n", .{
                released, n, self.retainedBytes() >> 20,
            });
        }
    }

    fn slotRetainedBytes(slot: *const WorkerSlot) usize {
        var n: usize = slot.rw.retainedBytes();
        for (slot.sd.cols) |*c| n += storeRetainedBytes(c);
        for (slot.sides) |*sd| {
            for (sd.cols) |*c| n += storeRetainedBytes(c);
        }
        n += slot.scratch.queryCapacity();
        return n;
    }

    /// Free one slot's ratcheted stores in place, leaving the slot valid
    /// for the next acquire (fresh empty op states; TVF worker state
    /// rebuilds lazily). Returns false when the rebuild fails — the caller
    /// falls back to a full reset.
    fn releaseSlotStores(self: *RegionPool, slot: *WorkerSlot) bool {
        const prog = self.prog orelse return false;
        const fresh = RegionWorker.init(self.alloc, prog) catch return false;
        slot.rw.deinit();
        slot.rw = fresh;
        slot.sd.deinit(self.alloc);
        slot.sd = .{};
        for (slot.sides) |*sd| {
            sd.deinit(self.alloc);
            sd.* = .{};
        }
        _ = slot.scratch.reset(.free_all);
        return true;
    }

    pub fn retainedBytes(self: *const RegionPool) usize {
        var n: usize = 0;
        // Exchanges are arena-backed: count the arenas' real footprint
        // (store capacities + growth history + merge keys) — the honest
        // number, since that is exactly what a reset now releases (and
        // releasing it is cheap: n_workers arena frees).
        if (self.ex) |*ex| {
            for (ex.arenas) |*ar| n += ar.queryCapacity();
        }
        for (self.side_ex) |*ex| {
            for (ex.arenas) |*ar| n += ar.queryCapacity();
        }
        for (self.ivs) |*iv| {
            for (iv.sd.cols) |*c| n += storeRetainedBytes(c);
        }
        for (self.slots.items) |slot| {
            n += slot.rw.retainedBytes();
            for (slot.sd.cols) |*c| n += storeRetainedBytes(c);
            for (slot.sides) |*sd| {
                for (sd.cols) |*c| n += storeRetainedBytes(c);
            }
            n += slot.scratch.queryCapacity();
        }
        return n;
    }
};

/// Approximate retained capacity of one store (validity bitmap excluded).
fn storeRetainedBytes(c: *const ColumnStore) usize {
    return switch (c.data) {
        inline .tinyint, .smallint, .int, .bigint, .largeint, .float, .double, .date, .datetime, .decimal64, .decimal128 => |l| l.capacity * @sizeOf(std.meta.Child(@TypeOf(l.items))),
        .varchar, .string, .char, .json => |s| blk: {
            var n: usize = s.offsets.capacity * @sizeOf(u32) + s.bytes.capacity;
            if (s.wide_offsets) |w| n += w.capacity * @sizeOf(u64);
            break :blk n;
        },
        else => 0,
    };
}

const ScanPhase = struct {
    ex: *Exchange,
    sources: []exec.Query,
    next: std.atomic.Value(usize) = .init(0),
    entry_derived: []const compute_mod.Derived,
    scan_schema: []const Column,
    registry: ?*const udf_mod.UdfRegistry,
    sort_cols: []const OrderCol,
    member_filters: []const MemberFilter = &.{},
    /// Co-partitioned side tables: one exchange + claim counter each; side
    /// buckets are never sorted (the probe map doesn't need order).
    sides: []SideScan,
    errs: []?anyerror,

    const SideScan = struct {
        ex: *Exchange,
        input: *const SideInput,
        next: std.atomic.Value(usize) = .init(0),
    };

    fn worker(self: *ScanPhase, w: usize) void {
        var lease = core_scheduler.global().tryAcquire();
        defer lease.release();
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
        var mask_buf: std.ArrayListUnmanaged(bool) = .empty;
        defer mask_buf.deinit(self.ex.alloc);
        var scratch_buf: std.ArrayListUnmanaged(bool) = .empty;
        defer scratch_buf.deinit(self.ex.alloc);
        while (true) {
            const i = self.next.fetchAdd(1, .monotonic);
            if (i >= self.sources.len) break;
            while (try self.sources[i].next()) |batch| {
                const routed = if (inst) |*ci| try ci.ptr.evalBatch(batch) else batch;
                var keep: ?[]const bool = null;
                if (self.member_filters.len > 0) {
                    try mask_buf.resize(self.ex.alloc, routed.row_count);
                    try self.member_filters[0].mask(routed.values[self.member_filters[0].col], routed.row_count, mask_buf.items);
                    for (self.member_filters[1..]) |*f| {
                        try scratch_buf.resize(self.ex.alloc, routed.row_count);
                        try f.mask(routed.values[f.col], routed.row_count, scratch_buf.items);
                        for (mask_buf.items, scratch_buf.items) |*m, s| m.* = m.* and s;
                    }
                    keep = mask_buf.items;
                }
                try wk.push(routed, keep);
            }
        }
        for (self.sides) |*side| {
            var swk = try side.ex.worker(w);
            defer swk.deinit();
            var sinst: ?ComputeInstance = null;
            defer if (sinst) |*i| i.q.deinit();
            if (side.input.entry_derived.len > 0) {
                sinst = try makeComputeInstance(side.ex.alloc, side.input.scan_schema, side.input.entry_derived, self.registry);
            }
            while (true) {
                const i = side.next.fetchAdd(1, .monotonic);
                if (i >= side.input.sources.len) break;
                while (try side.input.sources[i].next()) |batch| {
                    const routed = if (sinst) |*ci| try ci.ptr.evalBatch(batch) else batch;
                    try swk.push(routed, null);
                }
            }
        }
        // This worker's buckets are complete — sort them here, where the
        // work is balanced by input chunks, not by key skew.
        for (0..self.ex.n_shards) |s| {
            try sortBucketKeys(self.ex, w, s, self.sort_cols);
        }
    }
};

/// One execution bin: a set of route partitions consolidated into one
/// ShardData and run through the program ONCE. LPT bin-packing over the
/// measured partition sizes decouples routing granularity (many partitions
/// so whales isolate) from execution granularity (few program runs).
const Bin = struct {
    members: []const u32, // route partition ids, ascending (deterministic order)
    rows: usize,
};

/// LPT bin-packing: partitions descending by size into the least-loaded of
/// ~2×threads bins. Deterministic: ties break on partition id / bin index.
/// Returned bins are ordered descending by load — claim order IS whale-first.
fn packBins(alloc: Allocator, ex: *const Exchange, n_threads: usize) ![]Bin {
    const m = ex.n_shards;
    const totals = try alloc.alloc(usize, m);
    defer alloc.free(totals);
    var n_nonzero: usize = 0;
    for (0..m) |s| {
        totals[s] = ex.shardRows(s);
        if (totals[s] > 0) n_nonzero += 1;
    }
    if (n_nonzero == 0) return try alloc.alloc(Bin, 0);

    const by_size = try alloc.alloc(u32, n_nonzero);
    defer alloc.free(by_size);
    {
        var i: usize = 0;
        for (0..m) |s| {
            if (totals[s] > 0) {
                by_size[i] = @intCast(s);
                i += 1;
            }
        }
    }
    std.mem.sortUnstable(u32, by_size, totals, struct {
        fn less(t: []const usize, a: u32, b: u32) bool {
            if (t[a] != t[b]) return t[a] > t[b];
            return a < b;
        }
    }.less);

    const n_bins = @min(@max(2 * n_threads, 1), n_nonzero);
    const load = try alloc.alloc(usize, n_bins);
    defer alloc.free(load);
    @memset(load, 0);
    const bin_of = try alloc.alloc(u32, m);
    defer alloc.free(bin_of);
    for (by_size) |s| {
        var best: usize = 0;
        for (load, 0..) |l, b| {
            if (l < load[best]) best = b;
        }
        bin_of[s] = @intCast(best);
        load[best] += totals[s];
    }

    // Members flat-packed per bin, ascending partition id (walk ids in order).
    const members = try alloc.alloc(u32, n_nonzero);
    errdefer alloc.free(members);
    const offsets = try alloc.alloc(usize, n_bins);
    defer alloc.free(offsets);
    {
        const counts = try alloc.alloc(usize, n_bins);
        defer alloc.free(counts);
        @memset(counts, 0);
        for (by_size) |s| counts[bin_of[s]] += 1;
        var off: usize = 0;
        for (offsets, counts) |*o, c| {
            o.* = off;
            off += c;
        }
        const cursor = try alloc.alloc(usize, n_bins);
        defer alloc.free(cursor);
        @memcpy(cursor, offsets);
        for (0..m) |s| {
            if (totals[s] == 0) continue;
            const b = bin_of[s];
            members[cursor[b]] = @intCast(s);
            cursor[b] += 1;
        }
    }

    const bins = try alloc.alloc(Bin, n_bins);
    errdefer alloc.free(bins);
    for (bins, 0..) |*bin, b| {
        const end = if (b + 1 < n_bins) offsets[b + 1] else n_nonzero;
        bin.* = .{ .members = members[offsets[b]..end], .rows = load[b] };
    }
    std.mem.sortUnstable(Bin, bins, {}, struct {
        fn less(_: void, a: Bin, b: Bin) bool {
            if (a.rows != b.rows) return a.rows > b.rows;
            return a.members[0] < b.members[0];
        }
    }.less);
    return bins;
}

/// Per-bin side GROUP BY: fold every bucket row of the bin's partitions
/// into groups keyed by the agg's group columns (strings interned in
/// scratch; a NULL key is its own group via the mask word). Each group's
/// key values append to `ssd` at first sight (group order = first-seen
/// order), the SUM columns append after the fold. Shard-local grouping is
/// globally exact because the group keys include the route key.
fn aggregateSideBin(sex: *Exchange, ag: SideAgg, bin: Bin, ssd: *ShardData, sa: Allocator) !void {
    const n_keys = ag.group_srcs.len;
    const GroupKey = [MAX_SIDE_GROUP_KEYS + 1]i64;
    const Acc = struct { i: i128 = 0, f: f64 = 0, seen: bool = false };
    var map: std.AutoHashMapUnmanaged(GroupKey, u32) = .empty;
    var interners: [MAX_SIDE_GROUP_KEYS]StrInterner = @splat(.empty);
    const accs = try sa.alloc(std.ArrayListUnmanaged(Acc), ag.agg_srcs.len);
    @memset(accs, .empty);
    const views = try sa.alloc(ColumnView, sex.schema.len);
    var count: u32 = 0;

    for (bin.members) |s| {
        for (0..sex.n_workers) |sw| {
            const bkt = sex.bucket(sw, s);
            if (bkt.rows == 0) continue;
            for (bkt.cols, views) |*c, *v| v.* = c.view();
            for (0..bkt.rows) |i| {
                var key: GroupKey = @splat(0);
                var mask: i64 = 0;
                for (ag.key_srcs, 0..) |src, ki| {
                    const v = views[src];
                    if (!v.isValid(i)) {
                        mask |= @as(i64, 1) << @intCast(ki);
                        continue;
                    }
                    switch (v.data) {
                        inline .tinyint, .smallint, .int, .bigint, .date, .datetime => |vals| key[ki] = vals[i],
                        .varchar, .string, .char => {
                            // Bucket bytes are exchange-arena-backed — they
                            // outlive this map, so the slice keys directly.
                            const gop = try interners[ki].getOrPut(sa, stringViewOf(v).rowBytes(i));
                            if (!gop.found_existing) gop.value_ptr.* = @intCast(interners[ki].count());
                            key[ki] = gop.value_ptr.*;
                        },
                        else => return error.UnsupportedQueryShape,
                    }
                }
                key[MAX_SIDE_GROUP_KEYS] = mask;
                const gop = try map.getOrPut(sa, key);
                if (!gop.found_existing) {
                    gop.value_ptr.* = count;
                    count += 1;
                    for (ag.group_srcs, 0..) |src, ki| {
                        try appendRowValue(sex.alloc, &ssd.cols[ki], views[src], i);
                    }
                    for (accs) |*lane| try lane.append(sa, .{});
                }
                const ord = gop.value_ptr.*;
                for (ag.agg_srcs, accs) |src, *lane| {
                    const v = views[src];
                    if (!v.isValid(i)) continue;
                    const a = &lane.items[ord];
                    switch (v.data) {
                        inline .tinyint, .smallint, .int, .bigint, .largeint, .decimal64, .decimal128 => |vals| a.i += vals[i],
                        inline .float, .double => |vals| a.f += vals[i],
                        else => return error.UnsupportedQueryShape,
                    }
                    a.seen = true;
                }
            }
        }
    }

    for (accs, 0..) |lane, k| {
        const dst = &ssd.cols[n_keys + k];
        for (lane.items) |a| {
            if (!a.seen) {
                try dst.appendNulls(sex.alloc, 1);
                continue;
            }
            switch (dst.data) {
                .bigint => |*l| try l.append(sex.alloc, std.math.cast(i64, a.i) orelse return error.UnsupportedQueryShape),
                .largeint, .decimal128 => |*l| try l.append(sex.alloc, a.i),
                .double => |*l| try l.append(sex.alloc, a.f),
                else => return error.UnsupportedQueryShape,
            }
            if (dst.nulls != null) try dst.appendValidBit(sex.alloc, dst.rowCount() - 1, true);
        }
    }
}

fn freeBins(alloc: Allocator, bins: []Bin) void {
    if (bins.len > 0) {
        // members slices all view one flat allocation starting at bin with
        // the lowest offset — recover it via the minimum pointer.
        var base = bins[0].members.ptr;
        var total: usize = 0;
        for (bins) |b| {
            if (@intFromPtr(b.members.ptr) < @intFromPtr(base)) base = b.members.ptr;
            total += b.members.len;
        }
        alloc.free(base[0..total]);
    }
    alloc.free(bins);
}

const ShardPhase = struct {
    ex: *Exchange,
    opts: DriverOpts,
    bins: []const Bin,
    next: std.atomic.Value(usize) = .init(0),
    result: *RegionResult,
    slots: []const *RegionPool.WorkerSlot,
    side_ex: []Exchange,
    sides: []const SideInput,
    errs: []?anyerror,

    fn worker(self: *ShardPhase, w: usize) void {
        var lease = core_scheduler.global().tryAcquire();
        defer lease.release();
        self.workerInner(w) catch |e| {
            self.errs[w] = e;
        };
    }

    fn workerInner(self: *ShardPhase, w: usize) !void {
        const slot = self.slots[w];
        const rw = &slot.rw;
        const tail = rw.fusedFirstTail();
        const start_op: usize = if (tail != null) 1 else 0;

        while (true) {
            const bi = self.next.fetchAdd(1, .monotonic);
            if (bi >= self.bins.len) break;
            const bin = self.bins[bi];
            _ = slot.scratch.reset(.retain_capacity);
            const t_con = if (rw.op_ticks != null) exec.prof.nowTicks() else 0;
            try slot.sd.ensure(self.ex.alloc, self.ex.schema);
            for (bin.members) |s| {
                try consolidateAppendTail(self.ex, s, self.opts.sort_cols, self.opts.group_prefix, &slot.sd, slot.scratch.allocator(), tail);
            }
            // Side frames for this bin: plain per-partition bucket appends —
            // no sort, the keyed_probe map is order-insensitive. Aggregating
            // sides fold their buckets through the per-bin GROUP BY instead
            // of copying raw rows.
            for (self.side_ex, self.sides, slot.sides) |*sex, *side, *ssd| {
                try ssd.ensure(sex.alloc, side.schema);
                if (side.agg) |ag| {
                    try aggregateSideBin(sex, ag, bin, ssd, slot.scratch.allocator());
                } else {
                    for (bin.members) |s| {
                        for (0..sex.n_workers) |sw| {
                            const bkt = sex.bucket(sw, s);
                            if (bkt.rows == 0) continue;
                            for (ssd.cols, bkt.cols) |*dst, *src| {
                                try appendViewRange(sex.alloc, dst, src.view(), 0, bkt.rows);
                            }
                        }
                    }
                }
                ssd.rows = if (ssd.cols.len > 0) ssd.cols[0].rowCount() else 0;
            }
            rw.side_data = slot.sides;
            if (rw.op_ticks != null) rw.consolidate_ticks += exec.prof.nowTicks() - t_con;
            const out = &self.result.shards[bi];
            try rw.runShardFrom(&slot.sd, out.cols, start_op);
            out.rows = if (out.cols.len > 0) out.cols[0].rowCount() else 0;
        }
    }
};

/// Run a region end to end: drain `sources` in parallel through the entry
/// compute into a hash exchange on the program's entry schema, then claim
/// shards whale-first, consolidating each in the region's order and running
/// the compiled program shard-locally. `sources` are drained (not deinit'd).
/// One-shot form — all region state is built and freed within the call; use
/// `runRegionPooled` for repeated executions of the same program.
pub fn runRegion(
    alloc: Allocator,
    scan_schema: []const Column,
    sources: []exec.Query,
    entry_derived: []const compute_mod.Derived,
    prog: *const Program,
    opts: DriverOpts,
    result: *RegionResult,
) !void {
    var pool = RegionPool.init(alloc, 0);
    defer pool.deinit();
    return runRegionPooled(scan_schema, sources, entry_derived, &.{}, prog, opts, result, &pool);
}

/// Order-aligned driver (#186): each source IS one key interval of a table
/// physically ordered by the declared keys. Phase 1 drains each interval
/// into its own columnar store, recording run boundaries wherever the row
/// locator stops ascending (segment transitions; the memtable residue is
/// the trailing unsorted run). Phase 2 sorts only the residue run, merges
/// the runs with the standard RowKey machinery, gathers group ranges, and
/// executes the program — one interval, one program run, no exchange.
const OrderedInterval = struct {
    sd: ShardData = .{},
    runs: std.ArrayListUnmanaged(u32) = .empty, // run start offsets
    last_loc: i64 = std.math.minInt(i64),
    last_valid: bool = true,
};

fn drainOrderedInterval(
    alloc: Allocator,
    schema: []const Column,
    src: *exec.Query,
    iv: *OrderedInterval,
    inst: ?*ComputeInstance,
    loc_col: usize,
) !void {
    try iv.sd.ensure(alloc, schema);
    while (try src.next()) |batch| {
        const routed = if (inst) |ci| try ci.ptr.evalBatch(batch) else batch;
        if (routed.row_count == 0) continue;
        const base: u32 = @intCast(iv.sd.cols[0].rowCount());
        for (iv.sd.cols, routed.values) |*store, v| {
            try appendViewRange(alloc, store, v, 0, routed.row_count);
        }
        // Run boundaries: the locator ascends within one segment's rows;
        // any non-ascent (segment change, memtable residue) starts a new
        // run.
        const lv = routed.values[loc_col];
        for (0..routed.row_count) |r| {
            const valid = lv.isValid(r);
            const loc: i64 = if (valid) lv.data.bigint[r] else std.math.minInt(i64);
            if (iv.runs.items.len == 0 or loc <= iv.last_loc or valid != iv.last_valid) {
                if (iv.runs.items.len == 0 or base + r > iv.runs.items[iv.runs.items.len - 1]) {
                    try iv.runs.append(alloc, base + @as(u32, @intCast(r)));
                }
            }
            iv.last_loc = loc;
            iv.last_valid = valid;
        }
    }
    iv.sd.rows = iv.sd.cols[0].rowCount();
}

pub fn runRegionOrdered(
    scan_schema: []const Column,
    sources: []exec.Query,
    entry_derived: []const compute_mod.Derived,
    prog: *const Program,
    opts: DriverOpts,
    result: *RegionResult,
    pool: *RegionPool,
) !void {
    const alloc = pool.alloc;
    try pool.acquire(prog, opts, &.{});

    const errs = try alloc.alloc(?anyerror, opts.n_threads);
    defer alloc.free(errs);
    @memset(errs, null);
    const threads = try alloc.alloc(std.Thread, opts.n_threads);
    defer alloc.free(threads);

    const trace = getenv("THINDB_REGION_TRACE") != null;
    const t0 = if (trace) exec.prof.nowTicks() else 0;

    if (pool.ivs.len != sources.len) return error.UnsupportedQueryShape;
    const intervals = pool.ivs;

    // Static LPT assignment of intervals to workers BEFORE the scan (RG
    // quantile row estimates as the cost proxy, whales placed first), so
    // each worker scans its own intervals and executes them immediately —
    // no scan/exec barrier. Deterministic across runs, so each pooled
    // slot's op-state high-water stays put instead of every slot growing
    // to whale size over successive runs (which tipped the pool over its
    // cap and forced full resets).
    const have_est = opts.iv_rows_est.len == intervals.len;
    const cost_filled = blk: {
        const s = opts.iv_cost_slot orelse break :blk false;
        if (s.len != intervals.len) break :blk false;
        for (s) |c| {
            if (c != 0) break :blk true;
        }
        break :blk false;
    };
    const order = try alloc.alloc(u32, intervals.len);
    defer alloc.free(order);
    for (order, 0..) |*o, oi| o.* = @intCast(oi);
    const SizeDesc = struct {
        est: []const u64,
        cost: []const i64,
        fn weight(c: @This(), i: u32) u64 {
            if (c.cost.len > 0 and c.cost[i] > 0) return @intCast(c.cost[i]);
            return if (c.est.len > 0) c.est[i] else 1;
        }
        fn less(c: @This(), a: u32, b: u32) bool {
            const ra = c.weight(a);
            const rb = c.weight(b);
            if (ra != rb) return ra > rb;
            return a < b;
        }
    };
    const sd_ctx = SizeDesc{
        .est = if (have_est) opts.iv_rows_est else &.{},
        .cost = if (cost_filled) opts.iv_cost_slot.? else &.{},
    };
    std.mem.sortUnstable(u32, order, sd_ctx, SizeDesc.less);
    const assign_col = try alloc.alloc(usize, intervals.len);
    defer alloc.free(assign_col);
    const loads = try alloc.alloc(u64, opts.n_threads);
    defer alloc.free(loads);
    @memset(loads, 0);
    for (order) |ivi| {
        var best: usize = 0;
        for (loads[1..], 1..) |l, w| {
            if (l < loads[best]) best = w;
        }
        assign_col[ivi] = best;
        loads[best] += sd_ctx.weight(ivi);
    }

    const scan_ticks = try alloc.alloc(i64, opts.n_threads);
    defer alloc.free(scan_ticks);
    @memset(scan_ticks, 0);
    const iv_ticks = try alloc.alloc(i64, intervals.len);
    defer alloc.free(iv_ticks);
    @memset(iv_ticks, 0);

    var phase = OrderedExecPhase{
        .opts = opts,
        .schema = prog.schema_at[0],
        .sources = sources,
        .entry_derived = entry_derived,
        .scan_schema = scan_schema,
        .registry = prog.registry,
        .intervals = intervals,
        .order = order,
        .assign = assign_col,
        .scan_ticks = scan_ticks,
        .iv_ticks = iv_ticks,
        .result = result,
        .slots = pool.slots.items[0..opts.n_threads],
        .alloc = alloc,
        .errs = errs,
    };
    var spawned: usize = 0;
    for (0..opts.n_threads - 1) |w| {
        threads[w] = try std.Thread.spawn(.{}, OrderedExecPhase.worker, .{ &phase, w });
        spawned += 1;
    }
    phase.worker(opts.n_threads - 1);
    for (threads[0..spawned]) |t| t.join();
    for (errs) |e| if (e) |err| return err;

    if (opts.iv_cost_slot) |s| {
        if (!cost_filled and s.len == iv_ticks.len) @memcpy(s, iv_ticks);
    }

    if (trace) {
        const t2 = exec.prof.nowTicks();
        var rows: usize = 0;
        for (result.shards) |s| rows += s.rows;
        var scan_cpu: i64 = 0;
        for (scan_ticks) |t| scan_cpu += t;
        std.debug.print("[region] ORDERED FUSED total={d:.0}ms intervals={d} direct={d} scan_cpu={d:.0}ms out_rows={d} threads={d}\n", .{
            exec.prof.ticksToMs(t2 - t0), sources.len, phase.direct.load(.monotonic), exec.prof.ticksToMs(scan_cpu), rows, opts.n_threads,
        });
        var con: i64 = 0;
        for (pool.slots.items[0..opts.n_threads]) |slot| con += slot.rw.consolidate_ticks;
        std.debug.print("[region]   consolidate={d:.0}ms(cpu)", .{exec.prof.ticksToMs(con)});
        for (prog.ops, 0..) |op, oi| {
            var t: i64 = 0;
            for (pool.slots.items[0..opts.n_threads]) |slot| {
                if (slot.rw.op_ticks) |ticks| t += ticks[oi];
            }
            std.debug.print(" op{d}:{s}={d:.0}ms", .{ oi, @tagName(op), exec.prof.ticksToMs(t) });
        }
        std.debug.print("\n", .{});
        printProbePhases(pool, prog, opts.n_threads);
    }
    pool.releaseRun();
}

/// Probe sub-phase breakdown (map build / probe loop / emit), CPU summed
/// across workers; only ops with nonzero probe ticks print.
fn printProbePhases(pool: *RegionPool, prog: *const Program, n_threads: usize) void {
    var any = false;
    for (prog.ops, 0..) |op, oi| {
        var p: [3]i64 = .{ 0, 0, 0 };
        for (pool.slots.items[0..n_threads]) |slot| {
            if (slot.rw.probe_ticks) |ticks| {
                for (0..3) |k| p[k] += ticks[oi][k];
            }
        }
        if (p[0] + p[1] + p[2] == 0) continue;
        if (!any) {
            std.debug.print("[region]   probe-phases", .{});
            any = true;
        }
        std.debug.print(" op{d}:{s}[map={d:.0} probe={d:.0} emit={d:.0}]", .{
            oi, @tagName(op), exec.prof.ticksToMs(p[0]), exec.prof.ticksToMs(p[1]), exec.prof.ticksToMs(p[2]),
        });
    }
    if (any) std.debug.print("\n", .{});
}

const OrderedExecPhase = struct {
    opts: DriverOpts,
    schema: []const Column,
    sources: []exec.Query,
    entry_derived: []const compute_mod.Derived,
    scan_schema: []const Column,
    registry: ?*const udf_mod.UdfRegistry,
    intervals: []OrderedInterval,
    order: []const u32,
    assign: []const usize,
    scan_ticks: []i64,
    iv_ticks: []i64,
    direct: std.atomic.Value(usize) = .init(0),
    result: *RegionResult,
    slots: []const *RegionPool.WorkerSlot,
    alloc: Allocator,
    errs: []?anyerror,

    fn worker(self: *OrderedExecPhase, w: usize) void {
        var lease = core_scheduler.global().tryAcquire();
        defer lease.release();
        self.workerInner(w) catch |e| {
            self.errs[w] = e;
        };
    }

    fn workerInner(self: *OrderedExecPhase, w: usize) !void {
        const slot = self.slots[w];
        const rw = &slot.rw;
        const tail = rw.fusedFirstTail();
        const start_op: usize = if (tail != null) 1 else 0;
        const sort_cols = self.opts.sort_cols;
        const n_sort = sort_cols.len;

        var inst: ?ComputeInstance = null;
        defer if (inst) |*ci| ci.q.deinit();
        if (self.entry_derived.len > 0) {
            inst = try makeComputeInstance(self.alloc, self.scan_schema, self.entry_derived, self.registry);
        }

        for (self.order) |ivi| {
            if (self.assign[ivi] != w) continue;
            const i: usize = ivi;
            const iv = &self.intervals[i];
            const t_iv = exec.prof.nowTicks();
            defer self.iv_ticks[i] = exec.prof.nowTicks() - t_iv;

            const t_scan = if (rw.op_ticks != null) exec.prof.nowTicks() else 0;
            try drainOrderedInterval(
                self.alloc,
                self.schema,
                &self.sources[i],
                iv,
                if (inst) |*ci| ci else null,
                self.opts.loc_col,
            );
            if (rw.op_ticks != null) self.scan_ticks[w] += exec.prof.nowTicks() - t_scan;

            const total = iv.sd.rows;
            if (total == 0) continue;
            _ = slot.scratch.reset(.retain_capacity);
            const scratch = slot.scratch.allocator();
            const t_con = if (rw.op_ticks != null) exec.prof.nowTicks() else 0;

            // Keys for the whole interval, then per-run merge. Runs from the
            // scan are key-sorted (physical table order) EXCEPT any residue
            // run that contains non-ascending locators — sort those.
            const keys = try scratch.alloc(RowKey, total * n_sort);
            const views = try scratch.alloc(ColumnView, iv.sd.cols.len);
            for (iv.sd.cols, views) |*c, *v| v.* = c.view();
            for (sort_cols, 0..) |sc, kc| {
                try fillRowKeys(keys, n_sort, kc, sc.kind, views[sc.col], total);
            }
            const n_runs = iv.runs.items.len;

            // Direct run: ONE locator run whose keys verify ascending means
            // the interval store already IS the consolidated frame — walk
            // group boundaries in place and execute over it. No order
            // permutation, no merge, no gather (the copy that made v1 lose
            // to the hash path). A union-append first op runs unfused
            // (start_op 0); its own group copy costs what the gather did,
            // so direct still nets out the exchange scatter.
            if (n_runs == 1) one_run: {
                iv.sd.ranges.clearRetainingCapacity();
                var g_start: u32 = 0;
                var r: u32 = 1;
                const totn: u32 = @intCast(total);
                while (r < totn) : (r += 1) {
                    const prev_k = keys[(r - 1) * n_sort ..][0..n_sort];
                    const cur_k = keys[r * n_sort ..][0..n_sort];
                    switch (keyCmp(cur_k, prev_k)) {
                        .lt => {
                            iv.sd.ranges.clearRetainingCapacity();
                            break :one_run;
                        },
                        .gt => if (!keyEqPrefix(prev_k, cur_k, self.opts.group_prefix)) {
                            try iv.sd.ranges.append(self.alloc, .{ g_start, r });
                            g_start = r;
                        },
                        .eq => {},
                    }
                }
                try iv.sd.ranges.append(self.alloc, .{ g_start, totn });
                if (rw.op_ticks != null) rw.consolidate_ticks += exec.prof.nowTicks() - t_con;
                _ = self.direct.fetchAdd(1, .monotonic);
                const out = &self.result.shards[i];
                try rw.runShardFrom(&iv.sd, out.cols, 0);
                out.rows = if (out.cols.len > 0) out.cols[0].rowCount() else 0;
                continue;
            }

            const ord = try scratch.alloc(u32, total);
            for (ord, 0..) |*o, r| o.* = @intCast(r);
            const runs = try scratch.alloc(RunState, n_runs);
            const KCtx = struct {
                keys: []const RowKey,
                n_sort: usize,
                fn less(c: @This(), x: u32, y: u32) bool {
                    return switch (keyCmp(
                        c.keys[x * c.n_sort ..][0..c.n_sort],
                        c.keys[y * c.n_sort ..][0..c.n_sort],
                    )) {
                        .lt => true,
                        .gt => false,
                        .eq => x < y,
                    };
                }
            };
            for (0..n_runs) |ri| {
                const lo = iv.runs.items[ri];
                const hi: u32 = if (ri + 1 < n_runs) iv.runs.items[ri + 1] else @intCast(total);
                const slice = ord[lo..hi];
                // A run is sorted by construction only if its keys really
                // ascend — verify cheaply and sort when not (residue runs).
                var sorted = true;
                var r: u32 = lo;
                while (r + 1 < hi) : (r += 1) {
                    if (keyCmp(keys[(r + 1) * n_sort ..][0..n_sort], keys[r * n_sort ..][0..n_sort]) == .lt) {
                        sorted = false;
                        break;
                    }
                }
                if (!sorted) {
                    std.mem.sortUnstable(u32, slice, KCtx{ .keys = keys, .n_sort = n_sort }, KCtx.less);
                }
                runs[ri] = .{ .w = @intCast(ri), .keys = keys, .ord = slice, .pos = 0 };
            }

            const mctx = MergeCtx{ .runs = runs, .n_sort = n_sort };
            const heap = try scratch.alloc(u32, n_runs);
            for (heap, 0..) |*h, hi2| h.* = @intCast(hi2);
            var hb = n_runs / 2;
            while (hb > 0) {
                hb -= 1;
                mctx.siftDown(heap, hb);
            }
            const grefs = try scratch.alloc(u32, total);
            var bnds: std.ArrayListUnmanaged(u32) = .empty;
            var heap_len = n_runs;
            var prev: ?[]const RowKey = null;
            var out_i: u32 = 0;
            while (heap_len > 0) {
                const rr = &runs[heap[0]];
                const row = rr.ord[rr.pos];
                const hk = rr.keys[row * n_sort ..][0..n_sort];
                if (prev == null or !keyEqPrefix(prev.?, hk, self.opts.group_prefix)) {
                    try bnds.append(scratch, out_i);
                }
                prev = hk;
                grefs[out_i] = row;
                out_i += 1;
                rr.pos += 1;
                if (rr.pos == rr.ord.len) {
                    heap[0] = heap[heap_len - 1];
                    heap_len -= 1;
                }
                mctx.siftDown(heap[0..heap_len], 0);
            }
            try bnds.append(scratch, @intCast(total));

            // Gather group-by-group into the worker frame, then run ops.
            try slot.sd.ensure(self.alloc, self.schema);
            for (bnds.items[0 .. bnds.items.len - 1], bnds.items[1..]) |g_start, g_end| {
                const base: u32 = @intCast(slot.sd.cols[0].rowCount());
                for (slot.sd.cols, 0..) |*dst, ci| {
                    try gatherColumn(self.alloc, dst, views[ci .. ci + 1], grefs[g_start..g_end]);
                }
                if (tail) |t| try t.run(t.ctx, &slot.sd, base);
                try slot.sd.ranges.append(self.alloc, .{ base, @intCast(slot.sd.cols[0].rowCount()) });
            }
            slot.sd.rows = slot.sd.cols[0].rowCount();
            if (rw.op_ticks != null) rw.consolidate_ticks += exec.prof.nowTicks() - t_con;

            const out = &self.result.shards[i];
            try rw.runShardFrom(&slot.sd, out.cols, start_op);
            out.rows = if (out.cols.len > 0) out.cols[0].rowCount() else 0;
        }
    }
};

pub fn runRegionPooled(
    scan_schema: []const Column,
    sources: []exec.Query,
    entry_derived: []const compute_mod.Derived,
    sides: []const SideInput,
    prog: *const Program,
    opts: DriverOpts,
    result: *RegionResult,
    pool: *RegionPool,
) !void {
    if (opts.ordered) {
        // Order-aligned mode has no exchange to co-partition a side through.
        if (sides.len > 0) return error.UnsupportedQueryShape;
        return runRegionOrdered(scan_schema, sources, entry_derived, prog, opts, result, pool);
    }
    const alloc = pool.alloc;
    const trace = getenv("THINDB_REGION_TRACE") != null;
    const t_acq = if (trace) exec.prof.nowTicks() else 0;
    try pool.acquire(prog, opts, sides);
    if (trace) {
        std.debug.print("[region] pool acquire={d:.0}ms\n", .{exec.prof.ticksToMs(exec.prof.nowTicks() - t_acq)});
    }
    const ex = &pool.ex.?;

    const errs = try alloc.alloc(?anyerror, opts.n_threads);
    defer alloc.free(errs);
    @memset(errs, null);
    const threads = try alloc.alloc(std.Thread, opts.n_threads);
    defer alloc.free(threads);

    const t0 = if (trace) exec.prof.nowTicks() else 0;

    const side_scans = try alloc.alloc(ScanPhase.SideScan, sides.len);
    defer alloc.free(side_scans);
    for (sides, pool.side_ex, side_scans) |*side, *sex, *ss| {
        ss.* = .{ .ex = sex, .input = side };
    }
    var scan_phase = ScanPhase{
        .ex = ex,
        .sources = sources,
        .entry_derived = entry_derived,
        .scan_schema = scan_schema,
        .registry = prog.registry,
        .sort_cols = opts.sort_cols,
        .member_filters = opts.member_filters,
        .sides = side_scans,
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

    const t1 = if (trace) exec.prof.nowTicks() else 0;

    const bins = try packBins(alloc, ex, opts.n_threads);
    defer freeBins(alloc, bins);
    @memset(errs, null);
    var shard_phase = ShardPhase{
        .ex = ex,
        .opts = opts,
        .bins = bins,
        .result = result,
        .slots = pool.slots.items[0..opts.n_threads],
        .side_ex = pool.side_ex,
        .sides = sides,
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


    if (trace) {
        const t2 = exec.prof.nowTicks();
        var rows: usize = 0;
        for (result.shards) |s| rows += s.rows;
        const max_bin: usize = if (bins.len > 0) bins[0].rows else 0;
        std.debug.print("[region] scan+scatter={d:.0}ms shards={d:.0}ms out_rows={d} threads={d} parts={d} bins={d} max_bin={d}\n", .{
            exec.prof.ticksToMs(t1 - t0), exec.prof.ticksToMs(t2 - t1), rows, opts.n_threads, opts.n_shards, bins.len, max_bin,
        });
        // Per-op CPU (summed across workers; wall ≈ sum / threads when
        // load-balanced). Consolidation reported the same way.
        var con: i64 = 0;
        for (pool.slots.items[0..opts.n_threads]) |slot| con += slot.rw.consolidate_ticks;
        std.debug.print("[region]   consolidate={d:.0}ms(cpu)", .{exec.prof.ticksToMs(con)});
        for (prog.ops, 0..) |op, oi| {
            var t: i64 = 0;
            for (pool.slots.items[0..opts.n_threads]) |slot| {
                if (slot.rw.op_ticks) |ticks| t += ticks[oi];
            }
            std.debug.print(" op{d}:{s}={d:.0}ms", .{ oi, @tagName(op), exec.prof.ticksToMs(t) });
        }
        std.debug.print("\n", .{});
        printProbePhases(pool, prog, opts.n_threads);
    }

    pool.releaseRun();
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

fn accumMaxStr(cells: []AccCell, ord_of: []const u32, v: ColumnView, base: usize) void {
    switch (v.data) {
        inline .varchar, .string, .char => |sv| {
            for (ord_of, 0..) |gi, li| {
                if (!v.isValid(base + li)) continue;
                const s = sv.rowBytes(base + li);
                const cell = &cells[gi];
                if (!cell.seen or std.mem.order(u8, s, sv.rowBytes(cell.row)) == .gt) {
                    cell.row = @intCast(base + li);
                    cell.seen = true;
                }
            }
        },
        else => unreachable,
    }
}

fn accumMaxBy(cells: []AccCell, ord_of: []const u32, val_v: ColumnView, ord_v: ColumnView, base: usize) void {
    switch (ord_v.data) {
        inline .tinyint, .smallint, .int, .bigint, .date, .datetime => |vals| {
            for (ord_of, 0..) |gi, li| {
                // Engine MAX_BY pair semantics: a row with a NULL VALUE or
                // NULL key contributes nothing — the winner is the best-key
                // row among rows where both are present.
                if (!ord_v.isValid(base + li) or !val_v.isValid(base + li)) continue;
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

// ---------------------------------------------------------------------------
// E3: the region as a Query operator. The staged compiler wraps it in a
// forced materialize; Stage.ensureRun's takeOwnedChunks probe then adopts
// the per-shard output stores zero-copy (the same seam materialize-mode
// ParallelScan uses), so downstream MatScan / chunk machinery is unchanged.
// Unstaged callers pull normally — one batch per non-empty shard.
// ---------------------------------------------------------------------------

pub const RegionExecOp = struct {
    allocator: Allocator,
    scan_schema: []const Column,
    /// Owned; drained by the run and released immediately after it.
    sources: []exec.Query,
    entry_derived: []const compute_mod.Derived,
    /// Co-partitioned side tables (owned; side sources drained by the run).
    sides: []SideInput,
    prog: *const Program,
    opts: DriverOpts,
    /// Plan-owned pool for repeated executions; null = one-shot.
    pool: ?*RegionPool,
    /// Compile-time output row bound (the scan's upper bound; a region
    /// never invents rows beyond TVF appends — callers pass their best
    /// estimate, it only sizes downstream buffers).
    upper_rows: u64,
    result: ?RegionResult = null,
    emit_shard: usize = 0,
    views_buf: []ColumnView,
    /// Recognizer-owned state the program borrows (compiled Program,
    /// broadcast maps/stores, cloned expressions). Freed LAST in deinit —
    /// after the result/pool that reference it.
    owned_ctx: ?*anyopaque = null,
    owned_ctx_deinit: ?*const fn (*anyopaque) void = null,

    pub fn setOwnedCtx(self: *RegionExecOp, ctx: *anyopaque, dtor: *const fn (*anyopaque) void) void {
        self.owned_ctx = ctx;
        self.owned_ctx_deinit = dtor;
    }

    pub fn create(
        allocator: Allocator,
        scan_schema: []const Column,
        sources: []exec.Query,
        entry_derived: []const compute_mod.Derived,
        sides: []SideInput,
        prog: *const Program,
        opts: DriverOpts,
        pool: ?*RegionPool,
        upper_rows: u64,
    ) !exec.Query {
        const views_buf = try allocator.alloc(ColumnView, prog.output_schema.len);
        errdefer allocator.free(views_buf);
        const self = try allocator.create(RegionExecOp);
        errdefer allocator.destroy(self);
        self.* = .{
            .allocator = allocator,
            .scan_schema = scan_schema,
            .sources = sources,
            .entry_derived = entry_derived,
            .sides = sides,
            .prog = prog,
            .opts = opts,
            .pool = pool,
            .upper_rows = upper_rows,
            .views_buf = views_buf,
        };
        return exec.makeQuery(allocator, self);
    }

    fn releaseSides(self: *RegionExecOp) void {
        for (self.sides) |*side| {
            for (side.sources) |*s| s.deinit();
            self.allocator.free(side.sources);
        }
        self.allocator.free(self.sides);
        self.sides = &.{};
    }

    pub fn deinit(self: *RegionExecOp) void {
        for (self.sources) |*s| s.deinit();
        self.allocator.free(self.sources);
        self.releaseSides();
        if (self.result) |*r| r.deinit();
        self.allocator.free(self.views_buf);
        const owned_ctx = self.owned_ctx;
        const owned_dtor = self.owned_ctx_deinit;
        const allocator = self.allocator;
        allocator.destroy(self);
        if (owned_ctx) |ctx| owned_dtor.?(ctx);
    }

    pub fn ensureExecuted(self: *RegionExecOp) !void {
        if (self.result != null) return;
        const trace = getenv("THINDB_REGION_TRACE") != null;
        const t_all = if (trace) exec.prof.nowTicks() else 0;
        var result = try RegionResult.init(self.allocator, self.prog.output_schema, self.opts.n_shards);
        errdefer result.deinit();
        defer if (trace) {
            std.debug.print("[region] ensureExecuted total={d:.0}ms\n", .{exec.prof.ticksToMs(exec.prof.nowTicks() - t_all)});
        };
        if (self.pool) |p| {
            try runRegionPooled(self.scan_schema, self.sources, self.entry_derived, self.sides, self.prog, self.opts, &result, p);
        } else {
            if (self.sides.len > 0) return error.UnsupportedQueryShape;
            try runRegion(self.allocator, self.scan_schema, self.sources, self.entry_derived, self.prog, self.opts, &result);
        }
        // The scans' buffers are dead weight from here — release them now.
        for (self.sources) |*s| s.deinit();
        self.allocator.free(self.sources);
        self.sources = &.{};
        self.releaseSides();
        self.result = result;
    }

    /// Stage-adoption seam: run the region and hand the per-shard stores
    /// over. After this the operator has nothing left to emit.
    pub fn takeOwnedChunks(self: *RegionExecOp) !?exec.OwnedChunks {
        try self.ensureExecuted();
        const oc = try self.result.?.takeOwnedChunks();
        self.emit_shard = self.opts.n_shards;
        return oc;
    }

    pub fn next(self: *RegionExecOp) !?Batch {
        try self.ensureExecuted();
        const r = &self.result.?;
        while (self.emit_shard < r.shards.len) {
            const s = &r.shards[self.emit_shard];
            self.emit_shard += 1;
            if (s.rows == 0) continue;
            for (s.cols, self.views_buf) |*c, *v| v.* = c.view();
            return Batch{
                .schema = self.prog.output_schema,
                .values = self.views_buf,
                .row_count = s.rows,
            };
        }
        return null;
    }

    pub fn outputSchema(self: *RegionExecOp) []const Column {
        return self.prog.output_schema;
    }

    /// Emitted views point at the materialized per-shard result stores,
    /// which live until deinit/takeOwnedChunks — batches stay valid across
    /// next() calls, so a contiguous-stage consumer may fill in parallel.
    pub fn stableData(self: *RegionExecOp) bool {
        _ = self;
        return true;
    }

    pub fn addPrune(self: *RegionExecOp, pred: exec.Predicate) !void {
        _ = self;
        _ = pred;
    }

    pub fn stats(self: *RegionExecOp) exec.PipelineStats {
        const rows: u64 = if (self.result) |*r| @intCast(r.totalRows()) else self.upper_rows;
        return .{ .upper_rows = rows, .sort_state = .{ .keys = &.{}, .global = false } };
    }

    pub fn accountant(self: *RegionExecOp) ?*exec.memory.MemoryAccountant {
        _ = self;
        return null;
    }

    pub fn explain(self: *RegionExecOp, out: *std.ArrayList(u8), allocator: Allocator, depth: usize) !void {
        _ = self;
        try exec.explainLine(out, allocator, depth, "RegionExec");
    }
};

/// Bulk-fill a store with one constant (NULL when `v` is null).
fn fillConst(alloc: Allocator, store: *ColumnStore, v: ?types.Value, n: usize) !void {
    if (n == 0) return;
    const val = v orelse return store.appendNulls(alloc, n);
    switch (store.data) {
        .tinyint => |*l| try l.appendNTimes(alloc, @intCast(constI64(val)), n),
        .smallint => |*l| try l.appendNTimes(alloc, @intCast(constI64(val)), n),
        .int => |*l| try l.appendNTimes(alloc, @intCast(constI64(val)), n),
        .bigint => |*l| try l.appendNTimes(alloc, constI64(val), n),
        .date => |*l| try l.appendNTimes(alloc, @intCast(constI64(val)), n),
        .datetime => |*l| try l.appendNTimes(alloc, constI64(val), n),
        .float => |*l| try l.appendNTimes(alloc, @floatCast(constF64(val)), n),
        .double => |*l| try l.appendNTimes(alloc, constF64(val), n),
        .varchar, .string, .char, .json => |*d| {
            const s = switch (val) {
                .text => |t| t,
                else => return error.UnsupportedQueryShape,
            };
            for (0..n) |_| try d.appendValue(alloc, s);
        },
        else => return error.UnsupportedQueryShape,
    }
    if (store.nulls != null) {
        const base = store.rowCount() - n;
        for (0..n) |i| try store.appendValidBit(alloc, base + i, true);
    }
}

fn constI64(v: types.Value) i64 {
    return switch (v) {
        .tinyint => |x| x,
        .smallint => |x| x,
        .int => |x| x,
        .bigint => |x| x,
        .date => |x| x,
        .datetime => |x| x,
        .boolean => |x| @intFromBool(x),
        else => 0,
    };
}

fn constF64(v: types.Value) f64 {
    return switch (v) {
        .double => |x| x,
        .float => |x| x,
        else => @floatFromInt(constI64(v)),
    };
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
        try wk.push(.{ .schema = &schema, .values = &views, .row_count = n }, null);
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
    for (0..ex.n_shards) |s| {
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

test "region program: ranks normalized keys — lossy i64 ties and string prefix ties" {
    const alloc = testing.allocator;
    const entry = [_]Column{
        .{ .name = "k", .type = .string, .nullable = true },
        .{ .name = "s", .type = .string, .nullable = true },
        .{ .name = "v", .type = .bigint, .nullable = true },
    };
    var sd = ShardData{};
    defer sd.deinit(alloc);
    try sd.ensure(alloc, &entry);
    // v pairs 6/7 collapse to one norm word (low bit folds into the null
    // flag); s shares the 7-byte prefix "prefix0" so the norm ties too.
    const S = [_]?[]const u8{ "prefix0b", "prefix0a", null, "prefix0a" };
    const V = [_]?i64{ 7, 6, 6, null };
    for (S, V) |s, v| {
        try tAppendStr(alloc, &sd.cols[0], "a");
        try tAppendStr(alloc, &sd.cols[1], s);
        try tAppendI64(alloc, &sd.cols[2], v);
    }
    sd.rows = 4;
    try sd.ranges.append(alloc, .{ 0, 4 });

    const order = [_]OrderBy{ .{ .col = 2 }, .{ .col = 1, .desc = true } };
    const emit_cols = [_]usize{3};
    const ops = [_]RegionOp{
        .{ .ranks = .{ .name = "rn", .order = &order } },
        .{ .emit = .{ .cols = &emit_cols } },
    };
    var prog = try Program.build(alloc, &entry, &ops, null);
    defer prog.deinit();
    var worker = try RegionWorker.init(alloc, &prog);
    defer worker.deinit();
    const out = try alloc.alloc(ColumnStore, 1);
    defer {
        out[0].deinit(alloc);
        alloc.free(out);
    }
    out[0] = try ColumnStore.init(alloc, .bigint, false);

    try worker.runShard(&sd, out);

    // Order by (v ASC NULLS FIRST, s DESC NULLS LAST):
    //   row3 (v NULL)            -> rank 1
    //   v=6: row1 (s prefix0a) vs row2 (s NULL, last on DESC) -> ranks 2,3
    //   row0 (v=7)               -> rank 4
    const rn = out[0].view();
    try testing.expectEqual(@as(i64, 4), rn.data.bigint[0]);
    try testing.expectEqual(@as(i64, 2), rn.data.bigint[1]);
    try testing.expectEqual(@as(i64, 3), rn.data.bigint[2]);
    try testing.expectEqual(@as(i64, 1), rn.data.bigint[3]);
}

test "region program: lag op shifts within ranges" {
    const alloc = testing.allocator;
    const entry = [_]Column{
        .{ .name = "k", .type = .string, .nullable = true },
        .{ .name = "g", .type = .int, .nullable = true },
        .{ .name = "v", .type = .bigint, .nullable = true },
    };
    var sd = try tBuildShard(alloc, &entry);
    defer sd.deinit(alloc);

    const emit_cols = [_]usize{ 2, 3 };
    const ops = [_]RegionOp{
        .{ .lag = .{ .name = "lv", .src = 2, .offset = 1 } },
        .{ .emit = .{ .cols = &emit_cols } },
    };
    var prog = try Program.build(alloc, &entry, &ops, null);
    defer prog.deinit();
    var worker = try RegionWorker.init(alloc, &prog);
    defer worker.deinit();
    const out = try alloc.alloc(ColumnStore, 2);
    defer {
        for (out) |*c| c.deinit(alloc);
        alloc.free(out);
    }
    for (out, prog.output_schema) |*c, col| c.* = try ColumnStore.init(alloc, col.type, col.nullable);

    try worker.runShard(&sd, out);

    // v = 10,30,null,5,20 | 7 -> lag(1): null,10,30,null,5 | null.
    const exp = [_]?i64{ null, 10, 30, null, 5, null };
    const lv = out[1].view();
    for (exp, 0..) |e, i| {
        if (e) |x| {
            try testing.expectEqual(x, lv.data.bigint[i]);
        } else try testing.expect(!lv.isValid(i));
    }
}

test "region program: ranks merge_on spans adjacent same-key ranges" {
    const alloc = testing.allocator;
    const entry = [_]Column{
        .{ .name = "k", .type = .string, .nullable = true },
        .{ .name = "g", .type = .int, .nullable = true },
        .{ .name = "v", .type = .bigint, .nullable = true },
    };
    var sd = ShardData{};
    defer sd.deinit(alloc);
    try sd.ensure(alloc, &entry);
    // Ranges (k,g): (a,1)=rows 0..2, (a,2)=row 2..3, (b,1)=row 3..4 — the
    // rank partitions by k alone must span the two "a" ranges.
    const K = [_][]const u8{ "a", "a", "a", "b" };
    const G = [_]i32{ 1, 1, 2, 1 };
    const V = [_]i64{ 30, 10, 20, 5 };
    for (K, G, V) |k, g, v| {
        try tAppendStr(alloc, &sd.cols[0], k);
        try tAppendInt(alloc, &sd.cols[1], g);
        try tAppendI64(alloc, &sd.cols[2], v);
    }
    sd.rows = 4;
    try sd.ranges.append(alloc, .{ 0, 2 });
    try sd.ranges.append(alloc, .{ 2, 3 });
    try sd.ranges.append(alloc, .{ 3, 4 });

    const order = [_]OrderBy{.{ .col = 2 }};
    const emit_cols = [_]usize{3};
    const ops = [_]RegionOp{
        .{ .ranks = .{ .name = "rn", .order = &order, .merge_on = 0 } },
        .{ .emit = .{ .cols = &emit_cols } },
    };
    var prog = try Program.build(alloc, &entry, &ops, null);
    defer prog.deinit();
    var worker = try RegionWorker.init(alloc, &prog);
    defer worker.deinit();
    const out = try alloc.alloc(ColumnStore, 1);
    defer {
        out[0].deinit(alloc);
        alloc.free(out);
    }
    out[0] = try ColumnStore.init(alloc, .bigint, false);

    try worker.runShard(&sd, out);

    // Partition "a" = v {30,10,20} -> ranks 3,1,2; partition "b" -> 1.
    const exp = [_]i64{ 3, 1, 2, 1 };
    const rn = out[0].view();
    for (exp, 0..) |e, i| try testing.expectEqual(e, rn.data.bigint[i]);
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

test "region program: left probe carries a NULL payload value on a matched key" {
    const alloc = testing.allocator;
    const entry = [_]Column{
        .{ .name = "k", .type = .string, .nullable = true },
        .{ .name = "g", .type = .int, .nullable = true },
        .{ .name = "v", .type = .bigint, .nullable = true },
    };
    var sd = try tBuildShard(alloc, &entry);
    defer sd.deinit(alloc);

    // g=1 matches a NULL name; g=2 matches "two"; NULL/unmapped miss.
    var names = try ColumnStore.init(alloc, .string, true);
    defer names.deinit(alloc);
    try tAppendStr(alloc, &names, null);
    try tAppendStr(alloc, &names, "two");
    var map = KeyMap.empty;
    defer map.deinit(alloc);
    try map.put(alloc, 1, 0);
    try map.put(alloc, 2, 1);

    const payload = [_]Payload{.{ .name = "nm", .view = names.view(), .out_type = .string }};
    const emit_cols = [_]usize{ 1, 3 };
    const ops = [_]RegionOp{
        .{ .hash_probe = .{ .probe = 1, .map = &map, .payload = &payload, .inner = false } },
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
    const gv = out[0].view();
    const nv = out[1].view();
    for (0..6) |i| {
        if (!gv.isValid(i)) {
            try testing.expect(!nv.isValid(i)); // NULL probe misses
        } else if (gv.data.int[i] == 1) {
            try testing.expect(!nv.isValid(i)); // matched, payload NULL
        } else {
            try testing.expectEqualStrings("two", stringViewOf(nv).rowBytes(i));
        }
    }
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

test "region pool: repeated runs reuse cleared state; cap evicts at release" {
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
    const K = [_]?[]const u8{ "a", "b", "a", "c" };
    const V = [_]?i64{ 10, 20, 30, 40 };
    for (K, V) |k, v| {
        try tAppendStr(alloc, &stores[0], k);
        try tAppendInt(alloc, &stores[1], 1);
        try tAppendI64(alloc, &stores[2], v);
    }
    var views: [3]ColumnView = undefined;
    for (&views, &stores) |*v, *c| v.* = c.view();

    const agg_out = [_]AggOut{
        .{ .name = "k", .kind = .{ .first = 0 } },
        .{ .name = "sum_v", .kind = .{ .sum_int = 2 } },
    };
    const emit_cols = [_]usize{ 0, 1 };
    const ops = [_]RegionOp{
        .{ .group_agg = .{ .subkeys = &.{}, .out = &agg_out } },
        .{ .emit = .{ .cols = &emit_cols } },
    };
    var prog = try Program.build(alloc, &entry, &ops, null);
    defer prog.deinit();

    const sort_cols = [_]OrderCol{.{ .col = 0, .kind = .string }};
    const opts = DriverOpts{
        .n_threads = 1,
        .n_shards = 4,
        .key_col = 0,
        .sort_cols = &sort_cols,
        .group_prefix = 1,
    };

    var pool = RegionPool.init(alloc, 1 << 30);
    defer pool.deinit();
    var result = try RegionResult.init(alloc, prog.output_schema, 4);
    defer result.deinit();

    for (0..2) |_| {
        result.clear();
        var srcs = [1]exec.Query{try single_batch.SingleBatchSource.create(alloc, .{
            .schema = &entry,
            .values = &views,
            .row_count = K.len,
        })};
        defer srcs[0].deinit();
        try runRegionPooled(&entry, &srcs, &.{}, &.{}, &prog, opts, &result, &pool);
        try testing.expectEqual(@as(usize, 3), result.totalRows());
        var sum: i64 = 0;
        for (result.shards) |s| {
            const sv = if (s.rows > 0) s.cols[1].view() else continue;
            for (0..s.rows) |i| sum += sv.data.bigint[i];
        }
        try testing.expectEqual(@as(i64, 100), sum);
        // State retained (cleared, not freed) for the next run.
        try testing.expect(pool.ex != null);
        try testing.expect(pool.prog == &prog);
    }
    try testing.expect(pool.retainedBytes() > 0);

    // Cap 0: the next release frees everything.
    pool.max_retained_bytes = 0;
    result.clear();
    var srcs = [1]exec.Query{try single_batch.SingleBatchSource.create(alloc, .{
        .schema = &entry,
        .values = &views,
        .row_count = K.len,
    })};
    defer srcs[0].deinit();
    try runRegionPooled(&entry, &srcs, &.{}, &.{}, &prog, opts, &result, &pool);
    try testing.expectEqual(@as(usize, 3), result.totalRows());
    try testing.expect(pool.ex == null);
    try testing.expectEqual(@as(usize, 0), pool.retainedBytes());
}

fn tMakeRegionOp(alloc: Allocator, prog: *const Program, entry: []const Column, views: []ColumnView, rows: usize) !exec.Query {
    const sources = try alloc.alloc(exec.Query, 1);
    errdefer alloc.free(sources);
    sources[0] = try single_batch.SingleBatchSource.create(alloc, .{
        .schema = entry,
        .values = views,
        .row_count = rows,
    });
    const sort_cols_static = struct {
        const cols = [_]OrderCol{.{ .col = 0, .kind = .string }};
    };
    return RegionExecOp.create(alloc, entry, sources, &.{}, &.{}, prog, .{
        .n_threads = 1,
        .n_shards = 4,
        .key_col = 0,
        .sort_cols = &sort_cols_static.cols,
        .group_prefix = 1,
    }, null, rows);
}

test "region exec op: takeOwnedChunks adoption seam and next() drain" {
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
    const K = [_]?[]const u8{ "a", "b", "a", "c" };
    const V = [_]?i64{ 10, 20, 30, 40 };
    for (K, V) |k, v| {
        try tAppendStr(alloc, &stores[0], k);
        try tAppendInt(alloc, &stores[1], 1);
        try tAppendI64(alloc, &stores[2], v);
    }
    var views: [3]ColumnView = undefined;
    for (&views, &stores) |*v, *c| v.* = c.view();

    const agg_out = [_]AggOut{
        .{ .name = "k", .kind = .{ .first = 0 } },
        .{ .name = "sum_v", .kind = .{ .sum_int = 2 } },
    };
    const emit_cols = [_]usize{ 0, 1 };
    const ops = [_]RegionOp{
        .{ .group_agg = .{ .subkeys = &.{}, .out = &agg_out } },
        .{ .emit = .{ .cols = &emit_cols } },
    };
    var prog = try Program.build(alloc, &entry, &ops, null);
    defer prog.deinit();

    // Adoption seam: the stage-side contract (take before any next()).
    {
        var q = try tMakeRegionOp(alloc, &prog, &entry, &views, K.len);
        defer q.deinit();
        const oc = (try q.takeOwnedChunks()).?;
        var rows: usize = 0;
        var sum: i64 = 0;
        for (oc.chunks) |c| {
            try testing.expectEqual(@as(usize, 2), c.stores.len);
            rows += c.rows;
            const sv = c.stores[1].view();
            for (0..c.rows) |i| sum += sv.data.bigint[i];
        }
        try testing.expectEqual(@as(usize, 3), rows);
        try testing.expectEqual(@as(i64, 100), sum);
        exec.deinitOwnedChunks(oc);
    }

    // Pull path: one batch per non-empty shard.
    {
        var q = try tMakeRegionOp(alloc, &prog, &entry, &views, K.len);
        defer q.deinit();
        var rows: usize = 0;
        var sum: i64 = 0;
        while (try q.next()) |batch| {
            rows += batch.row_count;
            for (0..batch.row_count) |i| sum += batch.values[1].data.bigint[i];
        }
        try testing.expectEqual(@as(usize, 3), rows);
        try testing.expectEqual(@as(i64, 100), sum);
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
