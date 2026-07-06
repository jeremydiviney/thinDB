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
    /// One upstream per declared input table. `upstream`/`input_map`/
    /// `key_idx`/`order_idx` below alias input 0 so the single-input
    /// execution path stays byte-identical; the multi path indexes these
    /// arrays directly.
    upstreams: []Query,
    input_maps: [][]usize,
    key_idxs: [][]usize,
    order_idxs: [][]usize,
    /// Owned scalar call arguments, handed to every process() call.
    call_args: []?Value,
    entry: *const udf_mod.TableEntry,
    /// For each declared input column, its index in the upstream schema.
    upstream: Query,
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
        ups: []const Query,
        entry: *const udf_mod.TableEntry,
        call_args: []const ?Value,
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
        if (ups.len != entry.input_schemas.len or ups.len == 0) return Error.TableFnInputMismatch;

        // Scalar argument contract: count and per-position type family.
        if (call_args.len != entry.arg_types.len) return Error.TableFnInputMismatch;
        for (call_args, entry.arg_types) |arg, want| {
            const v = arg orelse continue;
            if (!argTypeMatches(v, want)) return Error.TableFnInputMismatch;
        }
        const args = try allocator.alloc(?Value, call_args.len);
        errdefer allocator.free(args);
        var args_duped: usize = 0;
        errdefer for (args[0..args_duped]) |a| {
            if (a) |v| switch (v) {
                .text => |t| allocator.free(t),
                else => {},
            };
        };
        for (call_args, args) |src, *dst| {
            dst.* = if (src) |v| switch (v) {
                .text => |t| Value{ .text = try allocator.dupe(u8, t) },
                else => v,
            } else null;
            args_duped += 1;
        }

        const n_in = ups.len;
        const upstreams = try allocator.dupe(Query, ups);
        errdefer allocator.free(upstreams);

        // Per input: shape contract (exact column set, exact types, sound
        // nullability) + key/order columns resolved in ITS declared schema.
        const input_maps = try allocator.alloc([]usize, n_in);
        errdefer allocator.free(input_maps);
        const key_idxs = try allocator.alloc([]usize, n_in);
        errdefer allocator.free(key_idxs);
        const order_idxs = try allocator.alloc([]usize, n_in);
        errdefer allocator.free(order_idxs);
        var built_in: usize = 0;
        errdefer for (0..built_in) |i| {
            allocator.free(input_maps[i]);
            allocator.free(key_idxs[i]);
            allocator.free(order_idxs[i]);
        };
        for (0..n_in) |i| {
            const decl_schema = entry.input_schemas[i];
            const up_schema = upstreams[i].outputSchema();
            if (up_schema.len != decl_schema.len) return Error.TableFnInputMismatch;
            const imap = try allocator.alloc(usize, decl_schema.len);
            errdefer allocator.free(imap);
            for (decl_schema, imap) |decl, *slot| {
                const ui = types.findColumn(up_schema, decl.name) orelse return Error.TableFnInputMismatch;
                if (!inputTypeMatches(up_schema[ui].type, decl.type)) return Error.TableFnInputMismatch;
                if (up_schema[ui].nullable and !decl.nullable) return Error.TableFnInputMismatch;
                slot.* = ui;
            }
            const kidx = try allocator.alloc(usize, partition_by.len);
            errdefer allocator.free(kidx);
            for (partition_by, kidx) |name, *slot| {
                slot.* = types.findColumn(decl_schema, name) orelse return Error.TableFnInputMismatch;
            }
            const oidx = try allocator.alloc(usize, order_by.len);
            errdefer allocator.free(oidx);
            for (order_by, oidx) |spec, *slot| {
                slot.* = types.findColumn(decl_schema, spec.col) orelse return Error.TableFnInputMismatch;
            }
            input_maps[i] = imap;
            key_idxs[i] = kidx;
            order_idxs[i] = oidx;
            built_in += 1;
        }

        const order_desc = try allocator.alloc(bool, order_by.len);
        errdefer allocator.free(order_desc);
        for (order_by, order_desc) |spec, *od| od.* = spec.desc;

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
            .upstreams = upstreams,
            .input_maps = input_maps,
            .key_idxs = key_idxs,
            .order_idxs = order_idxs,
            .call_args = args,
            .entry = entry,
            .upstream = upstreams[0],
            .input_map = input_maps[0],
            .key_idx = key_idxs[0],
            .order_idx = order_idxs[0],
            .order_desc = order_desc,
            .output_cols = output_cols,
            .views = views,
            .col_stats = col_stats,
            .dop = @max(1, dop),
        };
        return makeQuery(allocator, self);
    }

    pub fn deinit(self: *TableFnExec) void {
        for (self.call_args) |a| {
            if (a) |v| switch (v) {
                .text => |t| self.allocator.free(t),
                else => {},
            };
        }
        self.allocator.free(self.call_args);
        for (self.upstreams) |*up| up.deinit();
        self.allocator.free(self.upstreams);
        for (self.output_cols) |*c| c.deinit(self.allocator);
        self.allocator.free(self.output_cols);
        self.allocator.free(self.views);
        self.allocator.free(self.col_stats);
        for (0..self.input_maps.len) |i| {
            self.allocator.free(self.input_maps[i]);
            self.allocator.free(self.key_idxs[i]);
            self.allocator.free(self.order_idxs[i]);
        }
        self.allocator.free(self.input_maps);
        self.allocator.free(self.key_idxs);
        self.allocator.free(self.order_idxs);
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
        if (self.upstreams.len > 1) return self.executeMulti();
        const prof = exec.prof;
        const trace = getenv("THINDB_TVF_TRACE") != null;
        var t0 = prof.nowTicks();
        const n_cols = self.entry.input_schemas[0].len;

        // Drain the input into buffers laid out in DECLARED column order.
        var input_cols = try self.allocator.alloc(ColumnStore, n_cols);
        var inited: usize = 0;
        defer {
            for (input_cols[0..inited]) |*c| c.deinit(self.allocator);
            self.allocator.free(input_cols);
        }
        for (self.entry.input_schemas[0], input_cols) |col, *store| {
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
        var digests: ?[]u128 = null;
        defer if (digests) |d| self.allocator.free(d);
        for (perm, 0..) |*p, i| p.* = @intCast(i);
        if (self.key_idx.len + self.order_idx.len > 0) {
            digests = try self.packedSort(input_views, perm);
            if (digests == null) {
                const lctx = LessCtx{ .self = self, .views = input_views };
                std.mem.sortUnstable(u32, perm, lctx, LessCtx.less);
            }
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
        if (digests) |d| {
            // Digest-grouped ordering: run boundaries are digest changes.
            while (start < n_rows) {
                var end = start + 1;
                while (end < n_rows and d[perm[start]] == d[perm[end]]) end += 1;
                try runs.append(self.allocator, .{ .start = start, .end = end });
                start = end;
            }
        } else while (start < n_rows) {
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

    /// One co-grouped partition's spans, one per input (start==end =
    /// this input has no rows for the group's key).
    const Span = struct { start: usize, end: usize };

    /// Multi-input execution: drain every input, digest-sort each on the
    /// SHARED partition keys (same hash - digests align across inputs),
    /// N-way merge the sorted digest streams into groups, and invoke the
    /// kernel once per group with one ALIGNED partition per input.
    /// GLOBAL (no keys): one group holding every input whole.
    fn executeMulti(self: *TableFnExec) !void {
        const a = self.allocator;
        const n_in = self.upstreams.len;

        const Drained = struct {
            cols: []ColumnStore,
            views: []ColumnView,
            perm: []u32,
            digests: []u128,
        };
        const ins = try a.alloc(Drained, n_in);
        var drained: usize = 0;
        defer {
            for (ins[0..drained]) |*d| {
                for (d.cols) |*c| c.deinit(a);
                a.free(d.cols);
                a.free(d.views);
                a.free(d.perm);
                a.free(d.digests);
            }
            a.free(ins);
        }
        for (0..n_in) |i| {
            const decl = self.entry.input_schemas[i];
            const cols = try a.alloc(ColumnStore, decl.len);
            var inited: usize = 0;
            errdefer {
                for (cols[0..inited]) |*c| c.deinit(a);
                a.free(cols);
            }
            for (decl, cols) |col, *store| {
                store.* = try ColumnStore.init(a, col.type, col.nullable);
                inited += 1;
            }
            var up = self.upstreams[i];
            while (try up.next()) |batch| {
                for (self.input_maps[i], cols) |ui, *store| {
                    try transform.appendAllColumn(a, batch.values[ui], store);
                }
            }
            const n_rows = cols[0].rowCount();
            const views = try a.alloc(ColumnView, decl.len);
            errdefer a.free(views);
            for (cols, views) |c, *v| v.* = c.view();
            const perm = try a.alloc(u32, n_rows);
            errdefer a.free(perm);
            for (perm, 0..) |*p, r| p.* = @intCast(r);
            var digests: []u128 = undefined;
            if (self.key_idxs[i].len + self.order_idxs[i].len > 0) {
                digests = (try self.packedSortFor(views, self.key_idxs[i], self.order_idxs[i], perm)) orelse
                    return Error.TableFnInputMismatch;
            } else {
                digests = try a.alloc(u128, n_rows);
                @memset(digests, 0);
            }
            ins[i] = .{ .cols = cols, .views = views, .perm = perm, .digests = digests };
            drained += 1;
        }

        // Per input: digest runs (contiguous in the sorted perm).
        const DRun = struct { digest: u128, start: usize, end: usize };
        const run_lists = try a.alloc([]DRun, n_in);
        var built_lists: usize = 0;
        defer {
            for (run_lists[0..built_lists]) |rl| a.free(rl);
            a.free(run_lists);
        }
        for (0..n_in) |i| {
            var list: std.ArrayList(DRun) = .empty;
            errdefer list.deinit(a);
            const d = ins[i].digests;
            const perm = ins[i].perm;
            var start: usize = 0;
            while (start < perm.len) {
                var end = start + 1;
                const dg = d[perm[start]];
                while (end < perm.len and d[perm[end]] == dg) end += 1;
                try list.append(a, .{ .digest = dg, .start = start, .end = end });
                start = end;
            }
            run_lists[i] = try list.toOwnedSlice(a);
            built_lists += 1;
        }

        // N-way merge into groups: a group exists for every digest present
        // in ANY input; absent inputs contribute an empty span.
        var groups: std.ArrayList([]Span) = .empty;
        defer {
            for (groups.items) |g| a.free(g);
            groups.deinit(a);
        }
        const cursors = try a.alloc(usize, n_in);
        defer a.free(cursors);
        @memset(cursors, 0);
        while (true) {
            var min_digest: u128 = std.math.maxInt(u128);
            var any = false;
            for (0..n_in) |i| {
                if (cursors[i] < run_lists[i].len) {
                    any = true;
                    min_digest = @min(min_digest, run_lists[i][cursors[i]].digest);
                }
            }
            if (!any) break;
            const spans = try a.alloc(Span, n_in);
            errdefer a.free(spans);
            for (0..n_in) |i| {
                if (cursors[i] < run_lists[i].len and run_lists[i][cursors[i]].digest == min_digest) {
                    const r = run_lists[i][cursors[i]];
                    spans[i] = .{ .start = r.start, .end = r.end };
                    cursors[i] += 1;
                } else {
                    spans[i] = .{ .start = 0, .end = 0 };
                }
            }
            try groups.append(a, spans);
        }

        // Execute groups: same claim-loop shape as the single-input path.
        const allow_parallel = !builtin.is_test or force_parallel_in_tests;
        const n_workers = if (allow_parallel) @min(self.dop, groups.items.len) else 1;
        const views_by_input = try a.alloc([]const ColumnView, n_in);
        defer a.free(views_by_input);
        const perms_by_input = try a.alloc([]const u32, n_in);
        defer a.free(perms_by_input);
        for (0..n_in) |i| {
            views_by_input[i] = ins[i].views;
            perms_by_input[i] = ins[i].perm;
        }
        if (n_workers <= 1) {
            var runner = try MultiRunner.init(a, self, views_by_input, perms_by_input, self.output_cols);
            defer runner.deinit();
            for (groups.items) |g| _ = try runner.runOne(g);
            return;
        }
        try self.executeMultiParallel(views_by_input, perms_by_input, groups.items, n_workers);
    }

    fn executeMultiParallel(
        self: *TableFnExec,
        views_by_input: []const []const ColumnView,
        perms_by_input: []const []const u32,
        groups: []const []Span,
        n_workers: usize,
    ) !void {
        const a = self.allocator;
        const segs = try a.alloc(RunSeg, groups.len);
        defer a.free(segs);
        @memset(segs, .{});

        const Worker = struct {
            runner: MultiRunner,
            out_stores: []ColumnStore,
            err: ?anyerror = null,

            fn main(
                w: *@This(),
                wid: u32,
                group_list: []const []Span,
                seg_list: []RunSeg,
                next_group: *std.atomic.Value(usize),
                abort: *std.atomic.Value(bool),
            ) void {
                while (true) {
                    if (abort.load(.acquire)) return;
                    const g = next_group.fetchAdd(1, .monotonic);
                    if (g >= group_list.len) return;
                    const before = w.out_stores[0].rowCount();
                    const emitted = w.runner.runOne(group_list[g]) catch |err| {
                        w.err = err;
                        abort.store(true, .release);
                        return;
                    };
                    seg_list[g] = .{ .worker = wid, .off = before, .len = emitted };
                }
            }
        };

        const workers = try a.alloc(Worker, n_workers);
        var built: usize = 0;
        defer {
            for (workers[0..built]) |*w| {
                w.runner.deinit();
                for (w.out_stores) |*c| c.deinit(a);
                a.free(w.out_stores);
            }
            a.free(workers);
        }
        for (workers) |*w| {
            const out_stores = try a.alloc(ColumnStore, self.entry.output_schema.len);
            var inited: usize = 0;
            errdefer {
                for (out_stores[0..inited]) |*c| c.deinit(a);
                a.free(out_stores);
            }
            for (self.entry.output_schema, out_stores) |col, *store| {
                store.* = try ColumnStore.init(a, col.type, col.nullable);
                inited += 1;
            }
            w.* = .{
                .runner = try MultiRunner.init(a, self, views_by_input, perms_by_input, out_stores),
                .out_stores = out_stores,
            };
            built += 1;
        }

        var next_group = std.atomic.Value(usize).init(0);
        var abort = std.atomic.Value(bool).init(false);
        const threads = try a.alloc(?std.Thread, n_workers);
        defer a.free(threads);
        @memset(threads, null);
        for (workers, threads, 0..) |*w, *th, wid| {
            th.* = std.Thread.spawn(.{}, Worker.main, .{
                w, @as(u32, @intCast(wid)), groups, segs, &next_group, &abort,
            }) catch null;
        }
        var spawned: usize = 0;
        for (threads) |th| {
            if (th) |t| {
                t.join();
                spawned += 1;
            }
        }
        if (spawned == 0) Worker.main(&workers[0], 0, groups, segs, &next_group, &abort);
        for (workers[0..built]) |*w| {
            if (w.err) |err| return err;
        }

        var idx: std.ArrayList(u32) = .empty;
        defer idx.deinit(a);
        for (segs) |seg| {
            if (seg.len == 0) continue;
            idx.clearRetainingCapacity();
            try idx.ensureTotalCapacity(a, seg.len);
            for (0..seg.len) |i| idx.appendAssumeCapacity(@intCast(seg.off + i));
            const w = &workers[seg.worker];
            for (w.out_stores, self.output_cols) |src, *dst| {
                try transform.appendByIndices(a, src.view(), idx.items, dst);
            }
        }
    }

    /// Per-worker multi-input execution state: one scratch set per input.
    const MultiRunner = struct {
        allocator: Allocator,
        op: *const TableFnExec,
        views_by_input: []const []const ColumnView,
        perms_by_input: []const []const u32,
        scratch: [][]ColumnStore,
        part_views: [][]ColumnView,
        parts: []udf_mod.TvfPartition,
        key_vals: []?Value,
        out_ptrs: []*ColumnStore,
        arena: std.heap.ArenaAllocator,

        fn init(
            allocator: Allocator,
            op: *const TableFnExec,
            views_by_input: []const []const ColumnView,
            perms_by_input: []const []const u32,
            out_stores: []ColumnStore,
        ) !MultiRunner {
            const n_in = op.upstreams.len;
            const scratch = try allocator.alloc([]ColumnStore, n_in);
            errdefer allocator.free(scratch);
            const part_views = try allocator.alloc([]ColumnView, n_in);
            errdefer allocator.free(part_views);
            var built: usize = 0;
            errdefer for (0..built) |i| {
                for (scratch[i]) |*c| c.deinit(allocator);
                allocator.free(scratch[i]);
                allocator.free(part_views[i]);
            };
            for (0..n_in) |i| {
                const decl = op.entry.input_schemas[i];
                const sc = try allocator.alloc(ColumnStore, decl.len);
                var inited: usize = 0;
                errdefer {
                    for (sc[0..inited]) |*c| c.deinit(allocator);
                    allocator.free(sc);
                }
                for (decl, sc) |col, *store| {
                    store.* = try ColumnStore.init(allocator, col.type, col.nullable);
                    inited += 1;
                }
                scratch[i] = sc;
                part_views[i] = try allocator.alloc(ColumnView, decl.len);
                built += 1;
            }
            const parts = try allocator.alloc(udf_mod.TvfPartition, n_in);
            errdefer allocator.free(parts);
            const key_vals = try allocator.alloc(?Value, op.key_idxs[0].len);
            errdefer allocator.free(key_vals);
            const out_ptrs = try allocator.alloc(*ColumnStore, out_stores.len);
            errdefer allocator.free(out_ptrs);
            for (out_stores, out_ptrs) |*c, *slot| slot.* = c;
            return .{
                .allocator = allocator,
                .op = op,
                .views_by_input = views_by_input,
                .perms_by_input = perms_by_input,
                .scratch = scratch,
                .part_views = part_views,
                .parts = parts,
                .key_vals = key_vals,
                .out_ptrs = out_ptrs,
                .arena = std.heap.ArenaAllocator.init(allocator),
            };
        }

        fn deinit(self: *MultiRunner) void {
            self.arena.deinit();
            for (0..self.scratch.len) |i| {
                for (self.scratch[i]) |*c| c.deinit(self.allocator);
                self.allocator.free(self.scratch[i]);
                self.allocator.free(self.part_views[i]);
            }
            self.allocator.free(self.scratch);
            self.allocator.free(self.part_views);
            self.allocator.free(self.parts);
            self.allocator.free(self.key_vals);
            self.allocator.free(self.out_ptrs);
        }

        fn runOne(self: *MultiRunner, spans: []const Span) !usize {
            const op = self.op;
            // Gather each input's span; empty spans leave arity-correct
            // zero-row columns (cleared scratch views).
            for (0..op.upstreams.len) |i| {
                for (self.scratch[i]) |*store| store.clear();
                const span = spans[i];
                if (span.end > span.start) {
                    const perm = self.perms_by_input[i];
                    for (self.views_by_input[i], self.scratch[i]) |v, *store| {
                        try transform.appendByIndices(self.allocator, v, perm[span.start..span.end], store);
                    }
                }
                for (self.scratch[i], self.part_views[i]) |c, *v| v.* = c.view();
            }
            // Key values from the first non-empty input.
            @memset(self.key_vals, null);
            for (0..op.upstreams.len) |i| {
                if (spans[i].end > spans[i].start) {
                    const row = self.perms_by_input[i][spans[i].start];
                    for (op.key_idxs[i], self.key_vals) |ki, *kv| {
                        kv.* = if (self.views_by_input[i][ki].isValid(row))
                            valueAt(self.views_by_input[i][ki], row)
                        else
                            null;
                    }
                    break;
                }
            }
            for (0..op.upstreams.len) |i| {
                self.parts[i] = .{
                    .columns = self.part_views[i],
                    .row_count = spans[i].end - spans[i].start,
                    .keys = self.key_vals,
                };
            }

            _ = self.arena.reset(.retain_capacity);
            const ctx = udf_mod.TvfContext{
                .arena = self.arena.allocator(),
                .user_data = op.entry.user_data,
                .args = op.call_args,
            };
            var out = udf_mod.TvfOutput{
                .columns = self.out_ptrs,
                .allocator = self.allocator,
            };
            const before = self.out_ptrs[0].rowCount();
            try op.entry.process(&ctx, self.parts, &out);
            const after = self.out_ptrs[0].rowCount();
            for (self.out_ptrs[1..]) |c| {
                if (c.rowCount() != after) return Error.TableFnOutputMismatch;
            }
            return after - before;
        }
    };

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
        if (getenv("THINDB_TVF_TRACE") != null) {
            var sum: i64 = 0;
            var mx: i64 = 0;
            for (workers[0..built]) |*w| {
                sum += w.runner.kernel_ticks;
                mx = @max(mx, w.runner.kernel_ticks);
            }
            std.debug.print("[tvf]   kernel: {d:.0}ms wall (busiest worker), {d:.0}ms cpu across {d} workers\n", .{
                exec.prof.ticksToMs(mx), exec.prof.ticksToMs(sum), built,
            });
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
        /// Ticks spent INSIDE the user kernel (process() calls), for the
        /// THINDB_TVF_TRACE phase breakdown.
        kernel_ticks: i64 = 0,

        fn init(
            allocator: Allocator,
            op: *const TableFnExec,
            input_views: []const ColumnView,
            perm: []const u32,
            out_stores: []ColumnStore,
        ) !Runner {
            const n_cols = op.entry.input_schemas[0].len;
            const scratch = try allocator.alloc(ColumnStore, n_cols);
            errdefer allocator.free(scratch);
            var inited: usize = 0;
            errdefer for (scratch[0..inited]) |*c| c.deinit(allocator);
            for (op.entry.input_schemas[0], scratch) |col, *store| {
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
                .args = op.call_args,
            };
            const parts = [_]udf_mod.TvfPartition{.{
                .columns = self.part_views,
                .row_count = run.end - run.start,
                .keys = self.key_vals,
            }};
            var out = udf_mod.TvfOutput{
                .columns = self.out_ptrs,
                .allocator = self.allocator,
            };
            const before = self.out_ptrs[0].rowCount();
            const k0 = exec.prof.nowTicks();
            try op.entry.process(&ctx, &parts, &out);
            self.kernel_ticks += exec.prof.nowTicks() - k0;

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

    /// One row's flattened sort identity: partition keys collapse to a
    /// u128 digest (digest equality = key equality, the same contract the
    /// wide-key GROUP BY relies on engine-wide), the first order column
    /// packs to an i64. Sorting these flat structs replaces the generic
    /// per-Value comparator — ~10× on multi-million-row inputs.
    const PackedKey = struct {
        digest: u128,
        ord: i64,
        ord_null: bool,
        row: u32,
    };

    const PackCtx = struct {
        desc: bool,

        fn less(ctx: PackCtx, a: PackedKey, b: PackedKey) bool {
            if (a.digest != b.digest) return a.digest < b.digest;
            // NULLs first, matching the engine's validity-aware ordering.
            if (a.ord_null != b.ord_null) return a.ord_null;
            if (a.ord != b.ord) return if (ctx.desc) a.ord > b.ord else a.ord < b.ord;
            return a.row < b.row;
        }
    };

    /// Sort `perm` via packed keys when the shape allows it (any partition
    /// key types; at most ONE order column, int-family ≤64-bit). Returns
    /// false to fall back to the generic comparator.
    fn packedSort(self: *const TableFnExec, views: []const ColumnView, perm: []u32) !?[]u128 {
        return self.packedSortFor(views, self.key_idx, self.order_idx, perm);
    }

    fn packedSortFor(self: *const TableFnExec, views: []const ColumnView, key_idx: []const usize, order_idx: []const usize, perm: []u32) !?[]u128 {
        if (order_idx.len > 1) return null;
        if (order_idx.len == 1) {
            switch (views[order_idx[0]].data) {
                .int, .bigint, .tinyint, .smallint, .date, .datetime, .decimal64, .boolean => {},
                else => return null,
            }
        }
        // Float keys: ±0.0 compare equal but hash differently — a digest
        // would split one real partition. Generic path handles them.
        for (key_idx) |ki| {
            switch (views[ki].data) {
                .float, .double => return null,
                else => {},
            }
        }

        const keys = try self.allocator.alloc(PackedKey, perm.len);
        defer self.allocator.free(keys);

        for (keys, 0..) |*k, i| {
            var h1 = std.hash.Wyhash.init(0x9e3779b97f4a7c15);
            var h2 = std.hash.Wyhash.init(0x517cc1b727220a95);
            for (key_idx) |ki| {
                const v = views[ki];
                if (!v.isValid(i)) {
                    h1.update(&.{0});
                    h2.update(&.{0});
                    continue;
                }
                h1.update(&.{1});
                h2.update(&.{1});
                switch (v.data) {
                    .varchar, .string, .char => |sv| {
                        const bytes = sv.rowBytes(i);
                        h1.update(bytes);
                        h2.update(bytes);
                        // Length delimiter: ("ab","c") must not collide
                        // with ("a","bc").
                        h1.update(std.mem.asBytes(&bytes.len));
                        h2.update(std.mem.asBytes(&bytes.len));
                    },
                    inline .int, .bigint, .tinyint, .smallint, .date, .datetime, .largeint, .decimal64, .decimal128, .uuid, .float, .double, .boolean => |s| {
                        h1.update(std.mem.asBytes(&s[i]));
                        h2.update(std.mem.asBytes(&s[i]));
                    },
                }
            }
            k.digest = (@as(u128, h1.final()) << 64) | h2.final();
            k.row = @intCast(i);
            if (order_idx.len == 1) {
                const ov = views[order_idx[0]];
                if (ov.isValid(i)) {
                    k.ord_null = false;
                    k.ord = switch (ov.data) {
                        .int, .date => |s| s[i],
                        .bigint, .datetime, .decimal64 => |s| s[i],
                        .tinyint => |s| s[i],
                        .smallint => |s| s[i],
                        .boolean => |s| s[i],
                        else => unreachable,
                    };
                } else {
                    k.ord_null = true;
                    k.ord = 0;
                }
            } else {
                k.ord_null = false;
                k.ord = 0;
            }
        }

        const desc = self.order_desc.len == 1 and self.order_desc[0];
        std.mem.sortUnstable(PackedKey, keys, PackCtx{ .desc = desc }, PackCtx.less);
        for (keys, perm) |k, *p| p.* = k.row;
        // Row-indexed digests for the boundary scan (digest equality =
        // key equality, so boundaries need no Value comparisons).
        const digests = try self.allocator.alloc(u128, perm.len);
        for (keys) |k| digests[k.row] = k.digest;
        return digests;
    }

    /// A literal Value satisfies a declared argument type: exact tag, any
    /// int-family literal into any int-family declared type, text into the
    /// string family, int into double.
    fn argTypeMatches(v: Value, want: types.Type) bool {
        const int_lit = switch (v) {
            .tinyint, .smallint, .int, .bigint, .largeint => true,
            else => false,
        };
        return switch (want) {
            .tinyint, .smallint, .int, .bigint, .largeint => int_lit,
            .float, .double => int_lit or v == .float or v == .double,
            .varchar, .string, .char => v == .text,
            .date => v == .date or int_lit,
            .datetime => v == .datetime or int_lit,
            .boolean => v == .boolean or int_lit,
            .decimal64 => v == .decimal64 or int_lit,
            .decimal128 => v == .decimal128 or int_lit,
            .uuid => v == .uuid,
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
