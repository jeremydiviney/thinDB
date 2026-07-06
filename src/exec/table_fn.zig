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
//! Partitions execute in PARALLEL: workers claim run indices off an atomic
//! counter, each invoking process() into its own private output stores
//! (kernels need only be pure per-partition — no cross-partition state);
//! the final output concatenates per-run segments in run order, so results
//! are deterministic regardless of worker scheduling. Serial fallback for
//! DOP 1 / single-run (GLOBAL) / test builds.

const std = @import("std");
const builtin = @import("builtin");
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

/// Test hook: std.testing.allocator is single-threaded, so tests run the
/// serial path unless they opt in with a thread-safe allocator of their own.
pub var force_parallel_in_tests: bool = false;

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
    dop: usize,
    col_stats: []exec.ColStat,
    done: bool = false,
    stats_ready: bool = false,

    pub fn create(
        allocator: Allocator,
        upstream: Query,
        entry: *const udf_mod.TableEntry,
        partition_by: []const []const u8,
        order_by: []const SortSpec,
        dop: usize,
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
            if (!inputTypeMatches(up_schema[ui].type, decl.type)) return Error.TableFnInputMismatch;
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
        const col_stats = try allocator.alloc(exec.ColStat, entry.output_schema.len);
        errdefer allocator.free(col_stats);
        @memset(col_stats, .{});

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
            .col_stats = col_stats,
            .dop = @max(1, dop),
        };
        return makeQuery(allocator, self);
    }

    pub fn deinit(self: *TableFnExec) void {
        var up = self.upstream;
        up.deinit();
        for (self.output_cols) |*c| c.deinit(self.allocator);
        self.allocator.free(self.output_cols);
        self.allocator.free(self.views);
        self.allocator.free(self.col_stats);
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

    /// Before execution nothing can be bounded — a kernel may emit any
    /// number of rows per partition. After execution the whole output is
    /// materialized, so report EXACT figures: the row count, per-column
    /// NDV capped at the row count, and actual min/max scanned off the
    /// int-family output columns. Downstream consumers that consult stats
    /// lazily (GROUP BY routing at first pull) see the exact numbers.
    pub fn stats(self: *TableFnExec) exec.PipelineStats {
        if (!self.done) return .{ .upper_rows = std.math.maxInt(u64) };
        const n = self.output_cols[0].rowCount();
        if (!self.stats_ready) {
            self.computeOutputStats(n);
            self.stats_ready = true;
        }
        return .{ .upper_rows = n, .column_stats = self.col_stats };
    }

    fn computeOutputStats(self: *TableFnExec, n_rows: u64) void {
        const ndv_cap: u32 = if (n_rows > std.math.maxInt(u32)) std.math.maxInt(u32) else @intCast(n_rows);
        for (self.output_cols, self.col_stats) |c, *stat| {
            stat.* = .{ .ndv = .{ .exact = ndv_cap } };
            const view = c.view();
            var mn: i128 = std.math.maxInt(i128);
            var mx: i128 = std.math.minInt(i128);
            var any = false;
            const nrows = c.rowCount();
            switch (view.data) {
                inline .int, .bigint, .tinyint, .smallint, .date, .datetime, .largeint, .decimal64 => |s| {
                    for (s, 0..) |v, i| {
                        if (!view.isValid(i)) continue;
                        const w: i128 = v;
                        mn = @min(mn, w);
                        mx = @max(mx, w);
                        any = true;
                    }
                },
                else => {},
            }
            _ = nrows;
            if (any) {
                stat.min = mn;
                stat.max = mx;
            }
        }
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
        const prof = exec.prof;
        const trace = getenv("THINDB_TVF_TRACE") != null;
        var t0 = prof.nowTicks();
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
        if (trace) {
            std.debug.print("[tvf] drain {d} rows: {d:.0}ms\n", .{ n_rows, prof.ticksToMs(prof.nowTicks() - t0) });
            t0 = prof.nowTicks();
        }
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

        if (trace) {
            std.debug.print("[tvf] sort: {d:.0}ms\n", .{prof.ticksToMs(prof.nowTicks() - t0)});
            t0 = prof.nowTicks();
        }
        // Partition boundaries: contiguous runs of equal keys in the sorted
        // permutation. Global mode (no keys) is one run covering everything.
        var runs: std.ArrayList(Run) = .empty;
        defer runs.deinit(self.allocator);
        var start: usize = 0;
        while (start < n_rows) {
            var end = start + 1;
            while (end < n_rows and self.sameKeys(input_views, perm[start], perm[end])) end += 1;
            try runs.append(self.allocator, .{ .start = start, .end = end });
            start = end;
        }

        // std.testing.allocator is not thread-safe, so test builds stay
        // serial by default; a test that supplies its own thread-safe
        // allocator sets the override to exercise the parallel path.
        const allow_parallel = !builtin.is_test or force_parallel_in_tests;
        if (trace) {
            std.debug.print("[tvf] boundaries ({d} runs): {d:.0}ms\n", .{ runs.items.len, prof.ticksToMs(prof.nowTicks() - t0) });
            t0 = prof.nowTicks();
        }
        defer if (trace) std.debug.print("[tvf] partitions: {d:.0}ms\n", .{prof.ticksToMs(prof.nowTicks() - t0)});
        const n_workers = if (allow_parallel) @min(self.dop, runs.items.len) else 1;
        if (n_workers <= 1) {
            var runner = try Runner.init(self.allocator, self, input_views, perm, self.output_cols);
            defer runner.deinit();
            for (runs.items) |run| _ = try runner.runOne(run);
            return;
        }
        try self.executeParallel(input_views, perm, runs.items, n_workers);
    }

    const Run = struct { start: usize, end: usize };

    /// Where each run's output landed: which worker produced it and the
    /// row range inside that worker's private output stores. Written by
    /// exactly one worker per slot — no synchronization needed beyond the
    /// claim counter.
    const RunSeg = struct { worker: u32 = 0, off: usize = 0, len: usize = 0 };

    fn executeParallel(
        self: *TableFnExec,
        input_views: []const ColumnView,
        perm: []const u32,
        runs: []const Run,
        n_workers: usize,
    ) !void {
        const segs = try self.allocator.alloc(RunSeg, runs.len);
        defer self.allocator.free(segs);

        const Worker = struct {
            runner: Runner,
            out_stores: []ColumnStore,
            err: ?anyerror = null,

            fn main(
                w: *@This(),
                wid: u32,
                run_list: []const Run,
                seg_list: []RunSeg,
                next_run: *std.atomic.Value(usize),
                abort: *std.atomic.Value(bool),
            ) void {
                while (true) {
                    if (abort.load(.acquire)) return;
                    const r = next_run.fetchAdd(1, .monotonic);
                    if (r >= run_list.len) return;
                    const before = w.out_stores[0].rowCount();
                    const emitted = w.runner.runOne(run_list[r]) catch |err| {
                        w.err = err;
                        abort.store(true, .release);
                        return;
                    };
                    seg_list[r] = .{ .worker = wid, .off = before, .len = emitted };
                }
            }
        };

        const workers = try self.allocator.alloc(Worker, n_workers);
        var built: usize = 0;
        defer {
            for (workers[0..built]) |*w| {
                w.runner.deinit();
                for (w.out_stores) |*c| c.deinit(self.allocator);
                self.allocator.free(w.out_stores);
            }
            self.allocator.free(workers);
        }
        for (workers) |*w| {
            const out_stores = try self.allocator.alloc(ColumnStore, self.entry.output_schema.len);
            var inited: usize = 0;
            errdefer {
                for (out_stores[0..inited]) |*c| c.deinit(self.allocator);
                self.allocator.free(out_stores);
            }
            for (self.entry.output_schema, out_stores) |col, *store| {
                store.* = try ColumnStore.init(self.allocator, col.type, col.nullable);
                inited += 1;
            }
            w.* = .{
                .runner = try Runner.init(self.allocator, self, input_views, perm, out_stores),
                .out_stores = out_stores,
            };
            built += 1;
        }

        var next_run = std.atomic.Value(usize).init(0);
        var abort = std.atomic.Value(bool).init(false);
        const threads = try self.allocator.alloc(?std.Thread, n_workers);
        defer self.allocator.free(threads);
        @memset(threads, null);
        for (workers, threads, 0..) |*w, *th, wid| {
            th.* = std.Thread.spawn(.{}, Worker.main, .{
                w, @as(u32, @intCast(wid)), runs, segs, &next_run, &abort,
            }) catch null;
            // Spawn failure: this worker's runs are claimed by the others.
        }
        var spawned: usize = 0;
        for (threads) |th| {
            if (th) |t| {
                t.join();
                spawned += 1;
            }
        }
        if (spawned == 0) {
            // Total spawn failure — run everything on this thread.
            Worker.main(&workers[0], 0, runs, segs, &next_run, &abort);
        }
        for (workers[0..built]) |*w| {
            if (w.err) |err| return err;
        }
        // A claimed-but-never-run slot can only happen on abort; err above
        // covers it. If threads partially spawned, the survivors drained
        // the whole claim range.

        // Concatenate per-run segments IN RUN ORDER: output is
        // deterministic regardless of worker scheduling.
        var idx: std.ArrayList(u32) = .empty;
        defer idx.deinit(self.allocator);
        for (segs) |seg| {
            if (seg.len == 0) continue;
            idx.clearRetainingCapacity();
            try idx.ensureTotalCapacity(self.allocator, seg.len);
            for (0..seg.len) |i| idx.appendAssumeCapacity(@intCast(seg.off + i));
            const w = &workers[seg.worker];
            for (w.out_stores, self.output_cols) |src, *dst| {
                try transform.appendByIndices(self.allocator, src.view(), idx.items, dst);
            }
        }
    }

    /// Per-worker execution state: scratch input stores for the gathered
    /// partition, borrowed views, key values, and the output sink (pointing
    /// at either the operator's output columns — serial — or the worker's
    /// private stores). `runOne` gathers a run, invokes process(), and
    /// enforces the rectangular-output contract; returns rows emitted.
    const Runner = struct {
        allocator: Allocator,
        op: *const TableFnExec,
        input_views: []const ColumnView,
        perm: []const u32,
        scratch: []ColumnStore,
        part_views: []ColumnView,
        key_vals: []?Value,
        out_ptrs: []*ColumnStore,
        arena: std.heap.ArenaAllocator,

        fn init(
            allocator: Allocator,
            op: *const TableFnExec,
            input_views: []const ColumnView,
            perm: []const u32,
            out_stores: []ColumnStore,
        ) !Runner {
            const n_cols = op.entry.input_schema.len;
            const scratch = try allocator.alloc(ColumnStore, n_cols);
            errdefer allocator.free(scratch);
            var inited: usize = 0;
            errdefer for (scratch[0..inited]) |*c| c.deinit(allocator);
            for (op.entry.input_schema, scratch) |col, *store| {
                store.* = try ColumnStore.init(allocator, col.type, col.nullable);
                inited += 1;
            }
            const part_views = try allocator.alloc(ColumnView, n_cols);
            errdefer allocator.free(part_views);
            const key_vals = try allocator.alloc(?Value, op.key_idx.len);
            errdefer allocator.free(key_vals);
            const out_ptrs = try allocator.alloc(*ColumnStore, out_stores.len);
            errdefer allocator.free(out_ptrs);
            for (out_stores, out_ptrs) |*c, *slot| slot.* = c;
            return .{
                .allocator = allocator,
                .op = op,
                .input_views = input_views,
                .perm = perm,
                .scratch = scratch,
                .part_views = part_views,
                .key_vals = key_vals,
                .out_ptrs = out_ptrs,
                .arena = std.heap.ArenaAllocator.init(allocator),
            };
        }

        fn deinit(self: *Runner) void {
            self.arena.deinit();
            for (self.scratch) |*c| c.deinit(self.allocator);
            self.allocator.free(self.scratch);
            self.allocator.free(self.part_views);
            self.allocator.free(self.key_vals);
            self.allocator.free(self.out_ptrs);
        }

        fn runOne(self: *Runner, run: Run) !usize {
            const op = self.op;
            // clear() retains capacity: with ~229k partitions per query,
            // per-run store realloc + arena create were ~26µs/run of pure
            // overhead (6.1s of the flagship's 13.3s).
            for (self.scratch) |*store| store.clear();
            for (self.input_views, self.scratch) |v, *store| {
                try transform.appendByIndices(self.allocator, v, self.perm[run.start..run.end], store);
            }
            for (self.scratch, self.part_views) |c, *v| v.* = c.view();
            for (op.key_idx, self.key_vals) |ki, *kv| {
                kv.* = if (self.input_views[ki].isValid(self.perm[run.start]))
                    valueAt(self.input_views[ki], self.perm[run.start])
                else
                    null;
            }

            _ = self.arena.reset(.retain_capacity);
            const ctx = udf_mod.TvfContext{
                .arena = self.arena.allocator(),
                .user_data = op.entry.user_data,
            };
            const part = udf_mod.TvfPartition{
                .columns = self.part_views,
                .row_count = run.end - run.start,
                .keys = self.key_vals,
            };
            var out = udf_mod.TvfOutput{
                .columns = self.out_ptrs,
                .allocator = self.allocator,
            };
            const before = self.out_ptrs[0].rowCount();
            try op.entry.process(&ctx, &part, &out);

            const after = self.out_ptrs[0].rowCount();
            for (self.out_ptrs[1..]) |c| {
                if (c.rowCount() != after) return Error.TableFnOutputMismatch;
            }
            return after - before;
        }
    };

    /// Type contract match: exact equality, except the string family
    /// (varchar/string/char) is mutually compatible — all three share the
    /// same StringView representation, and a declared `[]const u8` input
    /// must accept a VARCHAR(n) table column.
    fn inputTypeMatches(actual: types.Type, declared: types.Type) bool {
        if (std.meta.eql(actual, declared)) return true;
        const string_family = switch (actual) {
            .varchar, .string, .char => true,
            else => false,
        };
        if (!string_family) return false;
        return switch (declared) {
            .varchar, .string, .char => true,
            else => false,
        };
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

extern "c" fn getenv(name: [*:0]const u8) ?[*:0]const u8;
