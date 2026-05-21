//! Top-N operator — bounded-memory `ORDER BY ... LIMIT k [OFFSET m]`.
//!
//! A full Sort holds every input row before it can emit; for an
//! `ORDER BY ... LIMIT k` that's wasteful and can blow the query memory
//! budget on a large input. Top-N instead keeps only the `keep = limit +
//! offset` rows it might emit: it accumulates upstream batches and, once
//! the buffer grows past `2 * keep`, sorts and truncates back to the best
//! `keep` rows (releasing the dropped rows' bytes back to the query
//! accountant). Steady-state memory is O(keep), independent of input
//! size. The upstream (scan → filter) streams batch-by-batch into this
//! bounded buffer, so the whole pipeline stays bounded.
//!
//! The planner fuses `Limit(OrderBy(X))` into a single `TopN(X)` — see
//! the `.limit` compile path in net/local.zig.

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
const SortSpec = @import("sort.zig").SortSpec;

pub const TopN = struct {
    allocator: Allocator,
    upstream: Query,
    schema: []const Column,

    sort_col_indices: []usize,
    sort_desc: []bool,

    /// Rows we might emit = limit + offset. The buffer is pruned back to
    /// this whenever it grows past `2 * keep`.
    keep: usize,
    limit: usize,
    offset: usize,
    row_bytes: usize,

    accumulated: []ColumnStore,
    accumulated_rows: u64 = 0,
    /// Row bytes currently reserved against the query accountant. Tracked
    /// so prune can release exactly the dropped rows' share.
    reserved_bytes: usize = 0,

    drained: bool = false,
    evicted: bool = false,
    perm: []u32 = &.{},
    emit_cursor: usize = 0,
    emit_end: usize = 0,

    output_columns: []ColumnStore,
    views: []ColumnView,

    const batch_size: usize = 1024;

    pub fn create(
        allocator: Allocator,
        upstream: Query,
        sort_specs: []const SortSpec,
        limit: usize,
        offset: usize,
    ) !Query {
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

        const self = try allocator.create(TopN);
        errdefer allocator.destroy(self);

        // keep = limit + offset, saturating (a pathologically huge LIMIT
        // just degrades to a full sort — no pruning ever fires).
        const keep = std.math.add(usize, limit, offset) catch std.math.maxInt(usize);

        self.* = .{
            .allocator = allocator,
            .upstream = upstream,
            .schema = schema,
            .sort_col_indices = sort_col_indices,
            .sort_desc = sort_desc,
            .keep = keep,
            .limit = limit,
            .offset = offset,
            .row_bytes = exec.memory.estimateRowBytes(schema),
            .accumulated = accumulated,
            .output_columns = output_columns,
            .views = views,
        };
        return makeQuery(allocator, self);
    }

    pub fn deinit(self: *TopN) void {
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
        const allocator = self.allocator;
        allocator.destroy(self);
    }

    pub fn outputSchema(self: *TopN) []const Column {
        return self.schema;
    }

    pub fn addPrune(self: *TopN, pred: Predicate) !void {
        return self.upstream.addPrune(pred);
    }

    /// Top-N output is sorted on its keys (ascending keys only — same
    /// rule as Sort), but conservatively we don't publish a sort_state
    /// since a downstream consumer rarely depends on it after a LIMIT.
    pub fn stats(self: *TopN) exec.PipelineStats {
        const up = self.upstream.stats();
        return .{ .upper_rows = @min(@as(u64, self.limit), up.upper_rows) };
    }

    pub fn accountant(self: *TopN) ?*exec.memory.MemoryAccountant {
        return self.upstream.accountant();
    }

    fn comparator(self: *TopN, accumulated: []const ColumnStore) Comparator {
        return .{ .accumulated = accumulated, .indices = self.sort_col_indices, .desc = self.sort_desc };
    }

    const Comparator = struct {
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

    /// Free the bounded buffer + permutation and release the remaining
    /// reserved bytes once all kept rows have been emitted. Idempotent.
    fn evict(self: *TopN) void {
        if (self.evicted) return;
        for (self.accumulated) |*c| c.deinit(self.allocator);
        self.allocator.free(self.accumulated);
        self.accumulated = &.{};
        if (self.perm.len > 0) self.allocator.free(self.perm);
        self.perm = &.{};
        if (self.upstream.accountant()) |a| a.release(self.reserved_bytes);
        self.reserved_bytes = 0;
        self.evicted = true;
    }

    pub fn next(self: *TopN) !?Batch {
        if (!self.drained) try self.drainAndSelect();

        if (self.emit_cursor >= self.emit_end) {
            self.evict();
            return null;
        }
        const remaining = self.emit_end - self.emit_cursor;
        const n: usize = @min(batch_size, remaining);

        for (self.output_columns) |*c| c.clear();
        for (self.output_columns, 0..) |*out, ci| {
            try engine.memtable.appendByIndices(
                self.allocator,
                self.accumulated[ci].view(),
                self.perm[self.emit_cursor .. self.emit_cursor + n],
                out,
            );
        }
        for (self.output_columns, 0..) |c, i| self.views[i] = c.view();
        self.emit_cursor += n;
        return Batch{ .schema = self.schema, .values = self.views, .row_count = n };
    }

    fn drainAndSelect(self: *TopN) !void {
        const acc = self.upstream.accountant();
        const prune_threshold = std.math.mul(usize, self.keep, 2) catch std.math.maxInt(usize);

        while (try self.upstream.next()) |batch| {
            const b = batch.row_count * self.row_bytes;
            if (acc) |a| try a.reserve(b);
            self.reserved_bytes += b;
            for (batch.values, 0..) |view, ci| {
                try engine.memtable.appendAllColumn(self.allocator, view, &self.accumulated[ci]);
            }
            self.accumulated_rows += batch.row_count;
            if (self.keep > 0 and self.accumulated_rows > prune_threshold) {
                try self.prune(acc);
            }
        }

        const n: usize = @intCast(self.accumulated_rows);
        if (n == 0) {
            self.drained = true;
            return;
        }
        self.perm = try self.allocator.alloc(u32, n);
        for (self.perm, 0..) |*p, i| p.* = @intCast(i);
        std.sort.pdq(u32, self.perm, self.comparator(self.accumulated), Comparator.lessThan);

        // Emit window: rows [offset, offset+limit), clamped to n.
        self.emit_cursor = @min(self.offset, n);
        const end = std.math.add(usize, self.offset, self.limit) catch n;
        self.emit_end = @min(end, n);
        self.drained = true;
    }

    /// Sort the buffer, keep the best `keep` rows, drop the rest — and
    /// hand the dropped rows' reserved bytes back to the accountant so
    /// steady-state memory stays bounded.
    fn prune(self: *TopN, acc: ?*exec.memory.MemoryAccountant) !void {
        const n: usize = @intCast(self.accumulated_rows);
        var tmp_perm = try self.allocator.alloc(u32, n);
        defer self.allocator.free(tmp_perm);
        for (tmp_perm, 0..) |*p, i| p.* = @intCast(i);
        std.sort.pdq(u32, tmp_perm, self.comparator(self.accumulated), Comparator.lessThan);

        const keep_n: usize = @min(self.keep, n);

        var fresh = try self.allocator.alloc(ColumnStore, self.schema.len);
        var finited: usize = 0;
        errdefer {
            for (fresh[0..finited]) |*c| c.deinit(self.allocator);
            self.allocator.free(fresh);
        }
        for (self.schema, 0..) |col, i| {
            fresh[i] = try ColumnStore.init(self.allocator, col.type, col.nullable);
            finited += 1;
            try engine.memtable.appendByIndices(self.allocator, self.accumulated[i].view(), tmp_perm[0..keep_n], &fresh[i]);
        }

        const dropped = n - keep_n;
        const release_bytes = dropped * self.row_bytes;
        if (acc) |a| a.release(release_bytes);
        self.reserved_bytes -= release_bytes;

        for (self.accumulated) |*c| c.deinit(self.allocator);
        self.allocator.free(self.accumulated);
        self.accumulated = fresh;
        self.accumulated_rows = keep_n;
    }
};
