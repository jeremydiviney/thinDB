//! Sort (ORDER BY) operator. Blocking — drains all upstream batches into
//! per-column buffers, builds a permutation via pdqsort, then emits sorted
//! batches in chunks of `batch_size`.

const std = @import("std");
const Allocator = std.mem.Allocator;

const types = @import("../types.zig");
const Column = types.Column;

const storage = @import("../storage/storage.zig");
const ColumnView = storage.ColumnView;

const engine = @import("../engine/engine.zig");
const ColumnStore = engine.ColumnStore;

const exec = @import("exec.zig");
const Query = exec.Query;
const Batch = exec.Batch;
const Error = exec.Error;
const makeQuery = exec.makeQuery;

const predicate = @import("predicate.zig");
const Predicate = predicate.Predicate;

pub const SortSpec = struct {
    col: []const u8,
    desc: bool = false,
};

/// MSD radix sort of a row-index permutation by a string column, byte by
/// byte. `tmp` is an n-sized scratch (same length as `perm`). All strings in
/// `perm` share the first `depth` bytes; bucket 0 holds rows whose string has
/// already ended (they sort first — a shorter prefix is lexicographically
/// less), buckets 1..256 hold rows by their byte at `depth` (value+1). Small
/// or deep slices fall back to comparison sort (correct because the shared
/// prefix doesn't affect their relative order); the depth cap also bounds
/// recursion for pathological long-common-prefix input.
fn radixSortStringPerm(perm: []u32, tmp: []u32, view: storage.StringView, depth: usize) void {
    if (perm.len <= 32 or depth >= 128) {
        sortStringPermAsc(perm, view);
        return;
    }
    const offsets = view.offsets;
    const bytes = view.bytes;

    var counts = [_]u32{0} ** 257;
    for (perm) |idx| {
        const start = offsets[idx];
        const end = offsets[idx + 1];
        const b: usize = if (depth >= end - start) 0 else @as(usize, bytes[start + depth]) + 1;
        counts[b] += 1;
    }

    // Degenerate level — every row has the same byte here (e.g. the shared
    // "http://" prefix of every URL). Skip the scatter + copy and just
    // descend; bucket 0 means every string ended (all equal → done).
    const total: u32 = @intCast(perm.len);
    for (counts, 0..) |cnt, b| {
        if (cnt == total) {
            if (b != 0) radixSortStringPerm(perm, tmp, view, depth + 1);
            return;
        }
        if (cnt != 0) break;
    }

    var bstart: [258]u32 = undefined;
    bstart[0] = 0;
    for (0..257) |i| bstart[i + 1] = bstart[i] + counts[i];

    var cursor: [257]u32 = undefined;
    for (0..257) |i| cursor[i] = bstart[i];
    for (perm) |idx| {
        const start = offsets[idx];
        const end = offsets[idx + 1];
        const b: usize = if (depth >= end - start) 0 else @as(usize, bytes[start + depth]) + 1;
        tmp[cursor[b]] = idx;
        cursor[b] += 1;
    }
    @memcpy(perm, tmp[0..perm.len]);

    // Bucket 0 = strings that ended at `depth`; all equal, no recursion.
    var b: usize = 1;
    while (b <= 256) : (b += 1) {
        const s = bstart[b];
        const e = bstart[b + 1];
        if (e - s > 1) radixSortStringPerm(perm[s..e], tmp[s..e], view, depth + 1);
    }
}

fn sortStringPermAsc(perm: []u32, view: storage.StringView) void {
    const Cmp = struct {
        v: storage.StringView,
        pub fn lessThan(c: @This(), a: u32, b: u32) bool {
            return std.mem.order(u8, c.v.rowBytes(a), c.v.rowBytes(b)) == .lt;
        }
    };
    std.sort.pdq(u32, perm, Cmp{ .v = view }, Cmp.lessThan);
}

fn sortStringPermCompare(perm: []u32, view: anytype, desc: bool) void {
    const Cmp = struct {
        v: @TypeOf(view),
        d: bool,
        pub fn lessThan(c: @This(), a: u32, b: u32) bool {
            const ord = std.mem.order(u8, c.v.rowBytes(a), c.v.rowBytes(b));
            return if (c.d) ord == .gt else ord == .lt;
        }
    };
    std.sort.pdq(u32, perm, Cmp{ .v = view, .d = desc }, Cmp.lessThan);
}

/// Emit the rows in `idxs` of `src` into `out`. For a string column that
/// crossed 4 GiB (u64 sidecar) the u32-offset `appendByIndices` can't read it,
/// so copy via `rowBytesWide` into the small per-batch `out` (which stays
/// narrow — emit batches are well under 4 GiB). Everything else takes the fast
/// `appendByIndices`.
fn emitColumn(allocator: Allocator, src: ColumnStore, idxs: []const u32, out: *ColumnStore) !void {
    switch (src.data) {
        inline .varchar, .string, .char => |s| {
            if (s.isWide()) {
                switch (out.data) {
                    inline .varchar, .string, .char => |*dst| {
                        for (idxs) |idx| try dst.appendValue(allocator, s.rowBytesWide(idx));
                    },
                    else => unreachable,
                }
                return;
            }
        },
        else => {},
    }
    try engine.memtable.appendByIndices(allocator, src.view(), idxs, out);
}

/// Sort `perm` by a single key column, with a comparator monomorphized to
/// the column's concrete type. The typed slice / StringView is captured
/// once, so each comparison avoids the tagged-union dispatch and view
/// reconstruction the generic multi-key comparator pays per call.
fn sortSingleKey(allocator: Allocator, perm: []u32, col: ColumnStore, desc: bool) void {
    switch (col.data) {
        inline .varchar, .string, .char => |s| {
            // A column past 4 GiB carries u64 offsets (StringStore.wide_offsets);
            // the u32-offset radix can't index it, so fall back to the generic
            // comparison sort over the wide view (correct, just slower). Rare.
            if (s.isWide()) {
                sortStringPermCompare(perm, s.wideView(), desc);
                return;
            }
            const view = s.view();
            // MSD radix sort by string bytes — O(n · key-prefix) instead of
            // the comparison sort's O(n log n · compare-length). Algorithmic
            // (no SIMD). Needs an n-sized index scratch; if that alloc fails,
            // fall back to a plain comparison sort.
            const tmp = allocator.alloc(u32, perm.len) catch {
                sortStringPermCompare(perm, view, desc);
                return;
            };
            defer allocator.free(tmp);
            radixSortStringPerm(perm, tmp, view, 0);
            // Radix produces ascending order; DESC just reverses (ties are
            // unspecified in an unstable sort, so this is fine).
            if (desc) std.mem.reverse(u32, perm);
        },
        inline else => |list| {
            const items = list.items;
            const Cmp = struct {
                it: @TypeOf(items),
                d: bool,
                pub fn lessThan(c: @This(), a: u32, b: u32) bool {
                    const Elem = @typeInfo(@TypeOf(c.it)).pointer.child;
                    // Floats use the NaN-last total order so sorts are
                    // deterministic; everything else is plain numeric order.
                    const ord = switch (@typeInfo(Elem)) {
                        .float => types.floatOrder(c.it[a], c.it[b]),
                        else => std.math.order(c.it[a], c.it[b]),
                    };
                    return if (c.d) ord == .gt else ord == .lt;
                }
            };
            std.sort.pdq(u32, perm, Cmp{ .it = items, .d = desc }, Cmp.lessThan);
        },
    }
}

pub const Sort = struct {
    allocator: Allocator,
    upstream: Query,
    schema: []const Column,

    sort_col_indices: []usize,
    sort_desc: []bool,
    /// Sort-key column names, allocated once at create-time so `stats()`
    /// can publish the output sort claim without re-allocating. Always
    /// populated (ascending or descending) — the per-key direction is
    /// `sort_desc`, which doubles as `SortState.descs`.
    sort_state_keys: []const []const u8,

    /// All upstream rows, accumulated by column. Built lazily on first
    /// `next()` call (Sort is a blocking operator).
    accumulated: []ColumnStore,
    accumulated_rows: u64 = 0,
    /// Bytes charged against the query budget for `accumulated` + `perm`.
    /// Released (and the buffers freed) once the last row is emitted —
    /// the sorted input is no longer a downstream dependency.
    reserved_bytes: usize = 0,
    evicted: bool = false,

    drained: bool = false,
    perm: []u32 = &.{},
    emit_offset: usize = 0,

    output_columns: []ColumnStore,
    views: []ColumnView,

    const batch_size: usize = 1024;

    pub fn create(allocator: Allocator, upstream: Query, sort_specs: []const SortSpec) !Query {
        if (sort_specs.len == 0) return Error.SortNoKeys;
        const schema = upstream.outputSchema();

        const sort_col_indices = try allocator.alloc(usize, sort_specs.len);
        errdefer allocator.free(sort_col_indices);
        const sort_desc = try allocator.alloc(bool, sort_specs.len);
        errdefer allocator.free(sort_desc);

        for (sort_specs, 0..) |spec, i| {
            sort_col_indices[i] = types.findColumn(schema, spec.col) orelse return Error.ColumnNotFound;
            sort_desc[i] = spec.desc;
        }
        // Publish the sort claim for every key, ascending or descending —
        // direction lives in `sort_desc` (= SortState.descs). Grouping is
        // direction-agnostic; an SMJ ascending-merge guards on direction
        // in joinKeysCovered.
        const sort_state_keys = try allocator.alloc([]const u8, sort_specs.len);
        for (sort_col_indices, 0..) |idx, i| sort_state_keys[i] = schema[idx].name;
        errdefer allocator.free(sort_state_keys);

        const accumulated = try allocator.alloc(ColumnStore, schema.len);
        errdefer allocator.free(accumulated);
        var inited: usize = 0;
        errdefer for (accumulated[0..inited]) |*c| c.deinit(allocator);
        for (schema, 0..) |col, i| {
            accumulated[i] = try ColumnStore.init(allocator, col.type, col.nullable);
            inited += 1;
        }

        const output_columns = try allocator.alloc(ColumnStore, schema.len);
        errdefer allocator.free(output_columns);
        var oinited: usize = 0;
        errdefer for (output_columns[0..oinited]) |*c| c.deinit(allocator);
        for (schema, 0..) |col, i| {
            output_columns[i] = try ColumnStore.init(allocator, col.type, col.nullable);
            oinited += 1;
        }

        const views = try allocator.alloc(ColumnView, schema.len);
        errdefer allocator.free(views);

        const self = try allocator.create(Sort);
        errdefer allocator.destroy(self);

        self.* = .{
            .allocator = allocator,
            .upstream = upstream,
            .schema = schema,
            .sort_col_indices = sort_col_indices,
            .sort_desc = sort_desc,
            .sort_state_keys = sort_state_keys,
            .accumulated = accumulated,
            .output_columns = output_columns,
            .views = views,
        };
        return makeQuery(allocator, self);
    }

    pub fn deinit(self: *Sort) void {
        var up = self.upstream;
        up.deinit();
        if (!self.evicted) {
            for (self.accumulated) |*c| c.deinit(self.allocator);
            self.allocator.free(self.accumulated);
            if (self.perm.len > 0) self.allocator.free(self.perm);
        }
        for (self.output_columns) |*c| c.deinit(self.allocator);
        self.allocator.free(self.output_columns);
        self.allocator.free(self.views);
        self.allocator.free(self.sort_col_indices);
        self.allocator.free(self.sort_desc);
        self.allocator.free(self.sort_state_keys);
        const allocator = self.allocator;
        allocator.destroy(self);
    }

    pub fn outputSchema(self: *Sort) []const Column {
        return self.schema;
    }

    pub fn addPrune(self: *Sort, pred: Predicate) !void {
        return self.upstream.addPrune(pred);
    }

    /// Sort makes the output globally sorted on its sort keys, in the
    /// requested per-key direction (`sort_desc`). Downstream grouping uses
    /// this regardless of direction; an ascending-merge SMJ guards on
    /// direction itself.
    pub fn stats(self: *Sort) exec.PipelineStats {
        const up = self.upstream.stats();
        return .{
            .upper_rows = up.upper_rows,
            .sort_state = .{
                .keys = self.sort_state_keys,
                .descs = self.sort_desc,
                .global = true,
            },
            // Sorting reorders rows but doesn't change any column's values,
            // so per-column stats (ndv + min/max) carry through unchanged.
            .column_stats = up.column_stats,
        };
    }

    pub fn accountant(self: *Sort) ?*exec.memory.MemoryAccountant {
        return self.upstream.accountant();
    }

    pub fn explain(self: *Sort, out: *std.ArrayList(u8), allocator: std.mem.Allocator, depth: usize) !void {
        try exec.explainLine(out, allocator, depth, "Sort");
        try self.upstream.explain(out, allocator, depth + 1);
    }

    /// Free the accumulated buffer + permutation and hand their bytes back
    /// to the query budget. Called once all sorted rows have been emitted
    /// (or on an empty input). Idempotent.
    fn evict(self: *Sort) void {
        if (self.evicted) return;
        for (self.accumulated) |*c| c.deinit(self.allocator);
        self.allocator.free(self.accumulated);
        self.accumulated = &.{};
        if (self.perm.len > 0) self.allocator.free(self.perm);
        self.perm = &.{};
        if (self.upstream.accountant()) |a| a.release(.sort, self.reserved_bytes);
        self.reserved_bytes = 0;
        self.evicted = true;
    }

    pub fn next(self: *Sort) !?Batch {
        if (!self.drained) try self.drainAndSort();

        const remaining = self.accumulated_rows - self.emit_offset;
        if (remaining == 0) {
            self.evict();
            return null;
        }
        const n: usize = @intCast(@min(@as(u64, batch_size), remaining));

        for (self.output_columns) |*c| c.clear();
        for (self.output_columns, 0..) |*out, ci| {
            try emitColumn(
                self.allocator,
                self.accumulated[ci],
                self.perm[self.emit_offset .. self.emit_offset + n],
                out,
            );
        }

        for (self.output_columns, 0..) |c, i| self.views[i] = c.view();
        self.emit_offset += n;
        return Batch{ .schema = self.schema, .values = self.views, .row_count = n };
    }

    fn drainAndSort(self: *Sort) !void {
        const acc = self.upstream.accountant();
        const row_bytes = exec.memory.estimateRowBytes(self.upstream.outputSchema());

        while (try self.upstream.next()) |batch| {
            const b = batch.row_count * row_bytes;
            if (acc) |a| try a.reserve(.sort, b);
            self.reserved_bytes += b;
            for (batch.values, 0..) |view, ci| {
                try engine.memtable.appendAllColumn(self.allocator, view, &self.accumulated[ci]);
            }
            self.accumulated_rows += batch.row_count;
        }

        const n: usize = @intCast(self.accumulated_rows);
        if (n == 0) {
            self.drained = true;
            return;
        }
        // Account for the perm array (u32 per row).
        if (acc) |a| try a.reserve(.sort, n * @sizeOf(u32));
        self.reserved_bytes += n * @sizeOf(u32);
        self.perm = try self.allocator.alloc(u32, n);
        for (self.perm, 0..) |*p, i| p.* = @intCast(i);

        // Single sort key (the common case: high-card GROUP BY sort, most
        // ORDER BY) gets a comparator monomorphized to the key column's
        // type — captured once, so the ~O(n log n) comparisons skip the
        // per-call union dispatch, ColumnStore indexing, and StringView
        // rebuild that the generic multi-key path pays. The typed string
        // arm is also the natural seam for a future vectorized compare.
        if (self.sort_col_indices.len == 1) {
            sortSingleKey(self.allocator, self.perm, self.accumulated[self.sort_col_indices[0]], self.sort_desc[0]);
            self.drained = true;
            return;
        }

        const Ctx = struct {
            accumulated: []const ColumnStore,
            indices: []const usize,
            desc: []const bool,

            pub fn lessThan(ctx: @This(), a: u32, b: u32) bool {
                for (ctx.indices, 0..) |ci, i| {
                    const ord = engine.memtable.compareInColumn(ctx.accumulated[ci], a, b);
                    if (ord == .lt) return !ctx.desc[i];
                    if (ord == .gt) return ctx.desc[i];
                }
                return false;
            }
        };

        std.sort.pdq(u32, self.perm, Ctx{
            .accumulated = self.accumulated,
            .indices = self.sort_col_indices,
            .desc = self.sort_desc,
        }, Ctx.lessThan);

        self.drained = true;
    }
};

test "sort: radix string sort produces correct lexicographic order" {
    const ta = std.testing.allocator;
    var prng = std.Random.DefaultPrng.init(0x5A17C0DE);
    const rnd = prng.random();
    // Tiny alphabet (incl. empty strings) → heavy duplicates + long shared
    // prefixes, exercising recursion, the ended-string bucket, and the
    // comparison fallback for small slices.
    const alpha = "ab/.";

    for (0..120) |iter| {
        const n = rnd.intRangeAtMost(usize, 0, 600);
        const max_len: usize = if (iter % 3 == 0) 3 else 12;

        var bytes: std.ArrayListUnmanaged(u8) = .empty;
        defer bytes.deinit(ta);
        const offsets = try ta.alloc(u32, n + 1);
        defer ta.free(offsets);
        offsets[0] = 0;
        for (0..n) |i| {
            const len = rnd.intRangeAtMost(usize, 0, max_len);
            for (0..len) |_| try bytes.append(ta, alpha[rnd.intRangeLessThan(usize, 0, alpha.len)]);
            offsets[i + 1] = @intCast(bytes.items.len);
        }
        const view = storage.StringView{ .offsets = offsets, .bytes = bytes.items };

        const perm = try ta.alloc(u32, n);
        defer ta.free(perm);
        for (perm, 0..) |*p, i| p.* = @intCast(i);
        const tmp = try ta.alloc(u32, n);
        defer ta.free(tmp);

        if (n > 0) radixSortStringPerm(perm, tmp, view, 0);

        // Keys must be non-decreasing.
        var i: usize = 1;
        while (i < n) : (i += 1) {
            try std.testing.expect(std.mem.order(u8, view.rowBytes(perm[i - 1]), view.rowBytes(perm[i])) != .gt);
        }
        // And `perm` must remain a permutation of 0..n.
        const seen = try ta.alloc(bool, n);
        defer ta.free(seen);
        @memset(seen, false);
        for (perm) |p| {
            try std.testing.expect(!seen[p]);
            seen[p] = true;
        }
    }
}
