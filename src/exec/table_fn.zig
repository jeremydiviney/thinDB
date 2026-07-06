//! Table-valued UDF operator (`FROM TABLE(f(<subquery>) PARTITION BY k
//! ORDER BY o)`). Drains the input subquery, groups rows by the partition
//! key columns (ordered within each partition per the ORDER BY), and
//! invokes the registered function's process callback once per partition —
//! or exactly once with everything when the call is GLOBAL (no PARTITION
//! BY). Output is the function's declared output schema; the concat of
//! per-partition emissions is the operator's contract (same strong
//! contract as SEPARABLE BY).
//!
//! The TYPE CONTRACT enforced at create (= query compile):
//!   - the input subquery's schema must carry every declared input column,
//!     name-for-name and type-for-type, and nothing else;
//!   - a nullable upstream column cannot feed a NOT NULL declared column
//!     (the reverse widening is fine);
//!   - PARTITION BY presence must match the function's declared execution
//!     mode; partition/order columns must be declared input columns.
//!
//! v1 executes partitions SERIALLY; the partition loop is the seam where
//! the claiming-pool parallelism lands next.

const std = @import("std");
const Allocator = std.mem.Allocator;

const types = @import("../types.zig");
const Column = types.Column;
const Value = types.Value;

const storage = @import("../storage/storage.zig");
const ColumnView = storage.ColumnView;

const engine = @import("../engine/engine.zig");
const ColumnStore = engine.ColumnStore;
const transform = engine.transform;

const udf_mod = @import("../udf.zig");
const sort_mod = @import("sort.zig");
const SortSpec = sort_mod.SortSpec;

const exec = @import("exec.zig");
const Query = exec.Query;
const Batch = exec.Batch;
const Error = exec.Error;
const makeQuery = exec.makeQuery;

const predicate = @import("predicate.zig");
const Predicate = predicate.Predicate;

pub const TableFnExec = struct {
    allocator: Allocator,
    upstream: Query,
    entry: *const udf_mod.TableEntry,
    /// For each declared input column, its index in the upstream schema.
    input_map: []usize,
    /// Partition key / order columns as indices into the DECLARED input
    /// columns (post input_map remap).
    key_idx: []usize,
    order_idx: []usize,
    order_desc: []bool,
    output_cols: []ColumnStore,
    views: []ColumnView,
    done: bool = false,

    pub fn create(
        allocator: Allocator,
        upstream: Query,
        entry: *const udf_mod.TableEntry,
        partition_by: []const []const u8,
        order_by: []const SortSpec,
    ) !Query {
        // Execution mode vs call-site shape.
        switch (entry.execution) {
            .partitioned => if (partition_by.len == 0) return Error.TableFnExecutionMismatch,
            .global => if (partition_by.len != 0) return Error.TableFnExecutionMismatch,
            .either => {},
        }

        // Input shape contract: exact column set, exact types, sound
        // nullability (nullable upstream cannot feed NOT NULL declared).
        const up_schema = upstream.outputSchema();
        if (up_schema.len != entry.input_schema.len) return Error.TableFnInputMismatch;
        const input_map = try allocator.alloc(usize, entry.input_schema.len);
        errdefer allocator.free(input_map);
        for (entry.input_schema, input_map) |decl, *slot| {
            const ui = types.findColumn(up_schema, decl.name) orelse return Error.TableFnInputMismatch;
            if (!std.meta.eql(up_schema[ui].type, decl.type)) return Error.TableFnInputMismatch;
            if (up_schema[ui].nullable and !decl.nullable) return Error.TableFnInputMismatch;
            slot.* = ui;
        }

        const key_idx = try allocator.alloc(usize, partition_by.len);
        errdefer allocator.free(key_idx);
        for (partition_by, key_idx) |name, *slot| {
            slot.* = types.findColumn(entry.input_schema, name) orelse return Error.TableFnInputMismatch;
        }
        const order_idx = try allocator.alloc(usize, order_by.len);
        errdefer allocator.free(order_idx);
        const order_desc = try allocator.alloc(bool, order_by.len);
        errdefer allocator.free(order_desc);
        for (order_by, order_idx, order_desc) |spec, *oi, *od| {
            oi.* = types.findColumn(entry.input_schema, spec.col) orelse return Error.TableFnInputMismatch;
            od.* = spec.desc;
        }

        const output_cols = try allocator.alloc(ColumnStore, entry.output_schema.len);
        errdefer allocator.free(output_cols);
        var inited: usize = 0;
        errdefer for (output_cols[0..inited]) |*c| c.deinit(allocator);
        for (entry.output_schema, output_cols) |col, *store| {
            store.* = try ColumnStore.init(allocator, col.type, col.nullable);
            inited += 1;
        }
        const views = try allocator.alloc(ColumnView, entry.output_schema.len);
        errdefer allocator.free(views);

        const self = try allocator.create(TableFnExec);
        errdefer allocator.destroy(self);
        self.* = .{
            .allocator = allocator,
            .upstream = upstream,
            .entry = entry,
            .input_map = input_map,
            .key_idx = key_idx,
            .order_idx = order_idx,
            .order_desc = order_desc,
            .output_cols = output_cols,
            .views = views,
        };
        return makeQuery(allocator, self);
    }

    pub fn deinit(self: *TableFnExec) void {
        var up = self.upstream;
        up.deinit();
        for (self.output_cols) |*c| c.deinit(self.allocator);
        self.allocator.free(self.output_cols);
        self.allocator.free(self.views);
        self.allocator.free(self.input_map);
        self.allocator.free(self.key_idx);
        self.allocator.free(self.order_idx);
        self.allocator.free(self.order_desc);
        const allocator = self.allocator;
        allocator.destroy(self);
    }

    pub fn outputSchema(self: *TableFnExec) []const Column {
        return self.entry.output_schema;
    }

    pub fn addPrune(_: *TableFnExec, _: Predicate) !void {
        // Opaque boundary: output rows have no derivable relation to input
        // predicates.
        return Error.ColumnNotFound;
    }

    pub fn stats(self: *TableFnExec) exec.PipelineStats {
        _ = self;
        return .{ .upper_rows = std.math.maxInt(u64) };
    }

    pub fn accountant(self: *TableFnExec) ?*exec.memory.MemoryAccountant {
        return self.upstream.accountant();
    }

    pub fn explain(self: *TableFnExec, out: *std.ArrayList(u8), allocator: Allocator, depth: usize) !void {
        var line: std.ArrayList(u8) = .empty;
        defer line.deinit(allocator);
        try line.print(allocator, "TableFn {s} [{s}]", .{
            self.entry.name,
            if (self.key_idx.len == 0) "global" else "partitioned",
        });
        try exec.explainLine(out, allocator, depth, line.items);
        try self.upstream.explain(out, allocator, depth + 1);
    }

    pub fn next(self: *TableFnExec) !?Batch {
        if (self.done) return null;
        self.done = true;
        try self.execute();
        if (self.output_cols[0].rowCount() == 0) return null;
        for (self.output_cols, 0..) |c, i| self.views[i] = c.view();
        return Batch{
            .schema = self.entry.output_schema,
            .values = self.views,
            .row_count = self.output_cols[0].rowCount(),
        };
    }

    fn execute(self: *TableFnExec) !void {
        const n_cols = self.entry.input_schema.len;

        // Drain the input into buffers laid out in DECLARED column order.
        var input_cols = try self.allocator.alloc(ColumnStore, n_cols);
        var inited: usize = 0;
        defer {
            for (input_cols[0..inited]) |*c| c.deinit(self.allocator);
            self.allocator.free(input_cols);
        }
        for (self.entry.input_schema, input_cols) |col, *store| {
            store.* = try ColumnStore.init(self.allocator, col.type, col.nullable);
            inited += 1;
        }
        while (try self.upstream.next()) |batch| {
            for (self.input_map, input_cols) |ui, *store| {
                try transform.appendAllColumn(self.allocator, batch.values[ui], store);
            }
        }
        const n_rows = input_cols[0].rowCount();
        if (n_rows == 0) return;

        const input_views = try self.allocator.alloc(ColumnView, n_cols);
        defer self.allocator.free(input_views);
        for (input_cols, input_views) |c, *v| v.* = c.view();

        // Sort a row permutation by (partition keys, order keys): partitions
        // become contiguous runs and each run is already in ORDER BY order.
        const perm = try self.allocator.alloc(u32, n_rows);
        defer self.allocator.free(perm);
        for (perm, 0..) |*p, i| p.* = @intCast(i);
        if (self.key_idx.len + self.order_idx.len > 0) {
            const lctx = LessCtx{ .self = self, .views = input_views };
            std.mem.sortUnstable(u32, perm, lctx, LessCtx.less);
        }

        // Walk partition runs and invoke the callback per run. Global mode
        // (no keys) is one run covering everything.
        var scratch = try self.allocator.alloc(ColumnStore, n_cols);
        var sinited: usize = 0;
        defer {
            for (scratch[0..sinited]) |*c| c.deinit(self.allocator);
            self.allocator.free(scratch);
        }
        for (self.entry.input_schema, scratch) |col, *store| {
            store.* = try ColumnStore.init(self.allocator, col.type, col.nullable);
            sinited += 1;
        }
        const part_views = try self.allocator.alloc(ColumnView, n_cols);
        defer self.allocator.free(part_views);
        const key_vals = try self.allocator.alloc(?Value, self.key_idx.len);
        defer self.allocator.free(key_vals);
        var out = udf_mod.TvfOutput{
            .columns = try self.allocator.alloc(*ColumnStore, self.output_cols.len),
            .allocator = self.allocator,
        };
        defer self.allocator.free(out.columns);
        for (self.output_cols, out.columns) |*c, *slot| slot.* = c;

        var start: usize = 0;
        while (start < n_rows) {
            var end = start + 1;
            while (end < n_rows and self.sameKeys(input_views, perm[start], perm[end])) end += 1;

            for (scratch) |*c| {
                c.deinit(self.allocator);
            }
            sinited = 0;
            for (self.entry.input_schema, scratch) |col, *store| {
                store.* = try ColumnStore.init(self.allocator, col.type, col.nullable);
                sinited += 1;
            }
            for (input_views, scratch) |v, *store| {
                try transform.appendByIndices(self.allocator, v, perm[start..end], store);
            }
            for (scratch, part_views) |c, *v| v.* = c.view();
            for (self.key_idx, key_vals) |ki, *kv| {
                kv.* = if (input_views[ki].isValid(perm[start]))
                    valueAt(input_views[ki], perm[start])
                else
                    null;
            }

            var arena = std.heap.ArenaAllocator.init(self.allocator);
            defer arena.deinit();
            const ctx = udf_mod.TvfContext{
                .arena = arena.allocator(),
                .user_data = self.entry.user_data,
            };
            const part = udf_mod.TvfPartition{
                .columns = part_views,
                .row_count = end - start,
                .keys = key_vals,
            };
            try self.entry.process(&ctx, &part, &out);

            // Rectangular-output contract: every output column at the same
            // row count after each partition.
            const expect = self.output_cols[0].rowCount();
            for (self.output_cols[1..]) |c| {
                if (c.rowCount() != expect) return Error.TableFnOutputMismatch;
            }

            start = end;
        }
    }

    fn sameKeys(self: *const TableFnExec, views: []const ColumnView, a: u32, b: u32) bool {
        for (self.key_idx) |ki| {
            const va = views[ki].isValid(a);
            const vb = views[ki].isValid(b);
            if (va != vb) return false;
            if (!va) continue;
            const x = valueAt(views[ki], a) orelse return false;
            const y = valueAt(views[ki], b) orelse return false;
            if (x.compare(y) != .eq) return false;
        }
        return true;
    }

    const LessCtx = struct {
        self: *const TableFnExec,
        views: []const ColumnView,

        fn less(ctx: LessCtx, a: u32, b: u32) bool {
            for (ctx.self.key_idx) |ki| {
                switch (cmpAt(ctx.views[ki], a, b)) {
                    .lt => return true,
                    .gt => return false,
                    .eq => {},
                }
            }
            for (ctx.self.order_idx, ctx.self.order_desc) |oi, desc| {
                switch (cmpAt(ctx.views[oi], a, b)) {
                    .lt => return !desc,
                    .gt => return desc,
                    .eq => {},
                }
            }
            return a < b; // stable within equal keys
        }
    };

    /// NULLs sort first (matching the engine's validity-aware ordering).
    fn cmpAt(v: ColumnView, a: u32, b: u32) std.math.Order {
        const va = v.isValid(a);
        const vb = v.isValid(b);
        if (!va and !vb) return .eq;
        if (!va) return .lt;
        if (!vb) return .gt;
        const x = valueAt(v, a) orelse return .eq;
        const y = valueAt(v, b) orelse return .eq;
        return x.compare(y);
    }

    fn valueAt(v: ColumnView, row: usize) ?Value {
        return switch (v.data) {
            .int => |s| .{ .int = s[row] },
            .bigint => |s| .{ .bigint = s[row] },
            .tinyint => |s| .{ .tinyint = s[row] },
            .smallint => |s| .{ .smallint = s[row] },
            .date => |s| .{ .date = s[row] },
            .datetime => |s| .{ .datetime = s[row] },
            .float => |s| .{ .float = s[row] },
            .double => |s| .{ .double = s[row] },
            .decimal64 => |s| .{ .decimal64 = s[row] },
            .largeint => |s| .{ .largeint = s[row] },
            .boolean => |s| .{ .boolean = s[row] != 0 },
            .varchar, .string, .char => |s| .{ .text = s.bytes[s.offsets[row]..s.offsets[row + 1]] },
            else => null,
        };
    }
};
