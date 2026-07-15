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
//!
//! Staged-compiler fast paths (installed by net/cte_stages): input columns
//! a filterless chain passes through from a stage's contiguous result are
//! BORROWED zero-copy instead of drain-copied (`borrow_src`/`borrow_map`);
//! an input provably pre-sorted by (partition ++ order) keys skips the sort
//! entirely (`input_ordered`). Kernel-visible columns are materialized once
//! in run-contiguous order (a no-op under an identity permutation), so
//! per-partition views are zero-copy slices rather than per-run gathers.

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
const ir = @import("../ir/ir.zig");
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
    /// Output stores handed to an adopting stage (mat_stage) — the stage
    /// owns their buffers now; deinit must not free them.
    adopted_out: bool = false,
    /// Zero-copy input borrowing (installed by the staged compiler): the
    /// single input chain is row-aligned (no filters) with `borrow_src`'s
    /// adopted contiguous result, so declared input columns the chain passes
    /// through untransformed take shallow views into its stores instead of
    /// being drain-copied. borrow_map[ci] = source store index for declared
    /// input column ci; null = derived/renamed → drained normally. The map
    /// is node-arena owned (never freed here); the source stage stays alive
    /// through execute() via the compiled upstream chain's MatScan use, and
    /// everything emitted or gathered from it is a copy, so no reference
    /// outlives the operator.
    borrow_src: ?*exec.mat_stage.Stage = null,
    borrow_map: []const ?usize = &.{},
    /// Multi-input generalization of borrow_src/borrow_map: one optional
    /// source stage + declared-column map per input. Node-arena owned;
    /// empty on single-input plans (those use the fields above).
    multi_borrow_srcs: []const ?*exec.mat_stage.Stage = &.{},
    multi_borrow_maps: []const []const ?usize = &.{},
    /// Installed by the staged compiler when the input provably arrives
    /// sorted by (partition_by ++ order_by) with engine comparison semantics
    /// (a covered window-stage ride): skip the sort — identity permutation,
    /// adjacent-equal run boundaries.
    input_ordered: bool = false,
    /// Observability (tests + trace attribution): columns the last execute
    /// actually bound as borrowed views (0 = bind declined or no plan).
    borrowed_bound: usize = 0,
    /// Output-order advertisement (installed by the staged compiler): the
    /// operator's output is grouped by these partition keys (equal keys
    /// adjacent — partition ORDER is unspecified) and ordered within each
    /// partition by the order keys, named in OUTPUT-schema terms. Set when
    /// provable (row_aligned with every key pass-through) or kernel-
    /// asserted (`ordered_output`). Downstream same-key windows / TVFs
    /// ride it instead of re-sorting. Slices are node-arena owned.
    advertised_keys: ?ir.WindowSpec = null,

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
        // Pairs with the staged compiler's registerUse when it installed
        // the borrow plan — borrowed views must outlive execute(), and the
        // upstream chain's own use releases at drain exhaustion.
        if (self.borrow_src) |src| src.releaseUse();
        for (self.multi_borrow_srcs) |maybe| {
            if (maybe) |src| src.releaseUse();
        }
        for (self.call_args) |a| {
            if (a) |v| switch (v) {
                .text => |t| self.allocator.free(t),
                else => {},
            };
        }
        self.allocator.free(self.call_args);
        for (self.upstreams) |*up| up.deinit();
        self.allocator.free(self.upstreams);
        if (!self.adopted_out) {
            for (self.output_cols) |*c| c.deinit(self.allocator);
        }
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
        std.debug.assert(!self.adopted_out);
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

    /// Buffers handed to an adopting stage (mat_stage.Stage.adopt_table_fn):
    /// one contiguous column per output-schema slot. Not arena-backed — the
    /// new owner deinits each store with its (same-lineage) allocator.
    pub const AdoptedBuffers = struct {
        stores: []ColumnStore,
        arenas: []std.heap.ArenaAllocator,
        arena_backed: []bool,
        rows: u64,
    };

    /// Run the whole pipeline without emitting (the adopting stage's
    /// barrier calls this instead of pulling `next()`).
    pub fn ensureExecuted(self: *TableFnExec) !void {
        if (self.done) return;
        self.done = true;
        try self.execute();
    }

    /// Ownership handover for TVF-output-as-stage: the operator's output
    /// IS the stage content, so move the stores out instead of letting the
    /// stage pull-copy 3M-row batches. Leaves the operator emitting nothing
    /// (deinit stays uniform via `adopted_out`).
    pub fn adoptBuffers(self: *TableFnExec) !AdoptedBuffers {
        std.debug.assert(self.done and !self.adopted_out);
        const stores = try self.allocator.alloc(ColumnStore, self.output_cols.len);
        errdefer self.allocator.free(stores);
        const arena_backed = try self.allocator.alloc(bool, self.output_cols.len);
        errdefer self.allocator.free(arena_backed);
        @memset(arena_backed, false);
        const arenas = try self.allocator.alloc(std.heap.ArenaAllocator, 0);
        @memcpy(stores, self.output_cols);
        self.adopted_out = true;
        return .{
            .stores = stores,
            .arenas = arenas,
            .arena_backed = arena_backed,
            .rows = self.output_cols[0].rowCount(),
        };
    }

    fn execute(self: *TableFnExec) !void {
        if (self.upstreams.len > 1) return self.executeMulti();
        const prof = exec.prof;
        const trace = getenv("THINDB_TVF_TRACE") != null;
        var t0 = prof.nowTicks();
        const n_cols = self.entry.input_schemas[0].len;

        // Zero-copy borrow bind: run the source stage (it runs lazily on
        // first pull anyway) and take shallow views into its adopted
        // contiguous stores for every column the compile-time walk proved
        // passes through the input chain untransformed. Any miss (no
        // contiguous result — chunked copy, sliced fill) degrades those
        // columns back to the drain copy.
        const borrowed_views = try self.allocator.alloc(?ColumnView, n_cols);
        defer self.allocator.free(borrowed_views);
        @memset(borrowed_views, null);
        var borrow_rows: u64 = 0;
        var n_borrowed: usize = 0;
        if (self.borrow_src) |src| bind: {
            src.ensureRun() catch break :bind;
            const res = src.result orelse break :bind;
            const ad = res.adopted orelse break :bind;
            // One contiguous store per column only — a SEPARABLE sliced
            // fill adopts N stores per column (slice parts), which can't be
            // shallow-referenced as single columns.
            if (ad.stores.len != res.schema.len) break :bind;
            for (self.borrow_map, 0..) |m, ci| {
                if (ci >= n_cols) break;
                const si = m orelse continue;
                borrowed_views[ci] = exec.mat_stage.MaterializedResult.presentAsSchemaType(
                    ad.stores[si].view(),
                    self.entry.input_schemas[0][ci].type,
                );
                n_borrowed += 1;
            }
            borrow_rows = res.total_rows;
        }
        self.borrowed_bound = n_borrowed;
        const owned_buf = try self.allocator.alloc(usize, n_cols);
        defer self.allocator.free(owned_buf);
        var n_owned: usize = 0;
        for (0..n_cols) |ci| {
            if (borrowed_views[ci] == null) {
                owned_buf[n_owned] = ci;
                n_owned += 1;
            }
        }
        const owned = owned_buf[0..n_owned];

        // Drain the input into buffers laid out in DECLARED column order —
        // borrowed columns skip the copy; the pull itself always runs (it
        // verifies the row-alignment contract and feeds the owned columns).
        // Per-column arenas make the parallel per-batch column copies
        // race-free (the #108 window fused-drain protocol); the upstream
        // pull itself stays on this thread — batches are only valid until
        // the next pull, so produce and copy can't overlap.
        const drain_backing = if (builtin.is_test) self.allocator else std.heap.c_allocator;
        const drain_arenas = try self.allocator.alloc(std.heap.ArenaAllocator, n_cols);
        var arenas_inited: usize = 0;
        defer {
            for (drain_arenas[0..arenas_inited]) |*a| a.deinit();
            self.allocator.free(drain_arenas);
        }
        for (drain_arenas) |*a| {
            a.* = std.heap.ArenaAllocator.init(drain_backing);
            arenas_inited += 1;
        }
        const input_cols = try self.allocator.alloc(ColumnStore, n_cols);
        defer self.allocator.free(input_cols);
        for (self.entry.input_schemas[0], input_cols, drain_arenas) |col, *store, *arena| {
            store.* = try ColumnStore.init(arena.allocator(), col.type, col.nullable);
        }

        var dp = DrainPar{ .op = self, .arenas = drain_arenas, .stores = input_cols, .owned = owned };
        var dworkers: [max_drain_workers]?std.Thread = .{null} ** max_drain_workers;
        var n_dworkers: usize = 0;
        var max_tiles: usize = 1;
        if (self.dop > 1 and owned.len >= 2 and (!builtin.is_test or force_parallel_in_tests)) {
            const want = @min(@min(self.dop - 1, owned.len - 1), max_drain_workers);
            while (n_dworkers < want) {
                dworkers[n_dworkers] = std.Thread.spawn(.{}, DrainPar.worker, .{&dp}) catch break;
                n_dworkers += 1;
            }
            dp.parked.store(n_dworkers, .release);
            max_tiles = (n_dworkers + 1) * 2;
            dp.preps = try self.allocator.alloc(transform.PreparedAppend, n_cols);
            dp.bounds = try self.allocator.alloc(usize, max_tiles + 1);
        }
        defer {
            dp.stop.store(true, .release);
            for (dworkers[0..n_dworkers]) |maybe| if (maybe) |t| t.join();
            if (dp.preps.len > 0) self.allocator.free(dp.preps);
            if (dp.bounds.len > 0) self.allocator.free(dp.bounds);
        }
        var accumulated: usize = 0;
        while (try self.upstream.next()) |batch| {
            if (n_dworkers > 0) {
                dp.batch = batch;
                dp.runPhase(.prepare, owned.len, n_dworkers);
                if (dp.failed.load(.acquire)) return error.OutOfMemory;
                // Row tiles on absolute 8-row boundaries: the first bound
                // absorbs the base misalignment so no two tiles share a
                // destination validity byte.
                const n = batch.row_count;
                const pad = (8 - (accumulated % 8)) % 8;
                const step = ((n / max_tiles) + 8) & ~@as(usize, 7);
                var nt: usize = 0;
                dp.bounds[0] = 0;
                while (dp.bounds[nt] < n) {
                    const next_b = @min(n, if (nt == 0) pad + step else dp.bounds[nt] + step);
                    nt += 1;
                    dp.bounds[nt] = @max(next_b, dp.bounds[nt - 1] + 1);
                    if (dp.bounds[nt] > n) dp.bounds[nt] = n;
                }
                dp.n_tiles = nt;
                dp.runPhase(.write, owned.len * nt, n_dworkers);
                // Rare non-positional columns take the classic serial
                // append — single caller, own arena.
                for (owned) |ci| {
                    if (dp.preps[ci].positional) continue;
                    try transform.appendColumnRange(
                        drain_arenas[ci].allocator(),
                        batch.values[self.input_map[ci]],
                        0,
                        n,
                        &input_cols[ci],
                    );
                }
            } else {
                for (owned) |ci| {
                    try transform.appendAllColumn(
                        drain_arenas[ci].allocator(),
                        batch.values[self.input_map[ci]],
                        &input_cols[ci],
                    );
                }
            }
            accumulated += batch.row_count;
        }
        // Borrowed stores are row-aligned with the drained chain by the
        // compile-time contract (no filters); a mismatch means it broke.
        if (n_borrowed > 0 and accumulated != borrow_rows) return Error.TableFnInputMismatch;
        const n_rows = accumulated;
        if (trace) {
            std.debug.print("[tvf] drain {d} rows: {d:.0}ms (borrowed {d}/{d} cols)\n", .{
                n_rows, prof.ticksToMs(prof.nowTicks() - t0), n_borrowed, n_cols,
            });
            t0 = prof.nowTicks();
        }
        if (n_rows == 0) return;

        const input_views = try self.allocator.alloc(ColumnView, n_cols);
        defer self.allocator.free(input_views);
        for (0..n_cols) |ci| {
            input_views[ci] = borrowed_views[ci] orelse input_cols[ci].view();
        }

        // Sort a row permutation by (partition keys, order keys): partitions
        // become contiguous runs and each run is already in ORDER BY order.
        // A provably pre-ordered input (input_ordered) keeps the identity
        // permutation — the drain order IS the sorted order.
        const perm = try self.allocator.alloc(u32, n_rows);
        defer self.allocator.free(perm);
        var digests: ?[]u128 = null;
        defer if (digests) |d| self.allocator.free(d);
        for (perm, 0..) |*p, i| p.* = @intCast(i);
        const have_keys = self.key_idx.len + self.order_idx.len > 0;
        const identity_perm = self.input_ordered or !have_keys;
        if (have_keys and !self.input_ordered) {
            digests = try self.packedSort(input_views, perm);
            if (digests == null) {
                const lctx = LessCtx{ .self = self, .views = input_views };
                try parallelSortPerm(LessCtx, self.allocator, perm, lctx, self.dop);
            }
        }

        if (trace) {
            if (self.input_ordered and have_keys) {
                std.debug.print("[tvf] sort: skipped (pre-ordered input)\n", .{});
            } else {
                std.debug.print("[tvf] sort: {d:.0}ms\n", .{prof.ticksToMs(prof.nowTicks() - t0)});
            }
            t0 = prof.nowTicks();
        }
        // Partition boundaries: contiguous runs of equal keys in the sorted
        // permutation. Global mode (no keys) is one run covering everything.
        var runs: std.ArrayList(Run) = .empty;
        defer runs.deinit(self.allocator);
        var max_run: usize = 0;
        var start: usize = 0;
        if (digests) |d| {
            // Digest-grouped ordering: run boundaries are digest changes.
            while (start < n_rows) {
                var end = start + 1;
                while (end < n_rows and d[perm[start]] == d[perm[end]]) end += 1;
                try runs.append(self.allocator, .{ .start = start, .end = end });
                max_run = @max(max_run, end - start);
                start = end;
            }
        } else while (start < n_rows) {
            var end = start + 1;
            while (end < n_rows and self.sameKeys(input_views, perm[start], perm[end])) end += 1;
            try runs.append(self.allocator, .{ .start = start, .end = end });
            max_run = @max(max_run, end - start);
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

        // Kernel-visible input columns in RUN-CONTIGUOUS order: with an
        // identity permutation the drained/borrowed columns already are;
        // otherwise ONE bulk permuted gather per kernel column (columns in
        // parallel) replaces the old per-run scratch copies — same bytes
        // moved, none of the per-run store/append overhead — and every
        // partition view becomes a zero-copy slice.
        const n_kernel: usize = self.entry.kernel_input_cols;
        const kernel_views = try self.allocator.alloc(ColumnView, n_kernel);
        defer self.allocator.free(kernel_views);
        var kernel_arenas: []std.heap.ArenaAllocator = &.{};
        var kernel_stores: []ColumnStore = &.{};
        var kernel_arenas_inited: usize = 0;
        defer {
            for (kernel_arenas[0..kernel_arenas_inited]) |*a| a.deinit();
            if (kernel_arenas.len > 0) self.allocator.free(kernel_arenas);
            if (kernel_stores.len > 0) self.allocator.free(kernel_stores);
        }
        if (identity_perm) {
            @memcpy(kernel_views, input_views[0..n_kernel]);
        } else {
            kernel_arenas = try self.allocator.alloc(std.heap.ArenaAllocator, n_kernel);
            for (kernel_arenas) |*a| {
                a.* = std.heap.ArenaAllocator.init(drain_backing);
                kernel_arenas_inited += 1;
            }
            kernel_stores = try self.allocator.alloc(ColumnStore, n_kernel);
            const jobs = try self.allocator.alloc(GatherJob, n_kernel);
            defer self.allocator.free(jobs);
            for (0..n_kernel) |ci| {
                const col = self.entry.input_schemas[0][ci];
                kernel_stores[ci] = try ColumnStore.init(kernel_arenas[ci].allocator(), col.type, col.nullable);
                jobs[ci] = .{
                    .src = input_views[ci],
                    .alloc = kernel_arenas[ci].allocator(),
                    .dst = &kernel_stores[ci],
                };
            }
            try runGatherJobs(jobs, perm, false, if (allow_parallel) self.dop else 1);
            for (kernel_stores, kernel_views) |c, *v| v.* = c.view();
            if (trace) {
                std.debug.print("[tvf]   kernel gather: {d:.0}ms ({d} cols)\n", .{
                    prof.ticksToMs(prof.nowTicks() - t0), n_kernel,
                });
                t0 = prof.nowTicks();
            }
        }

        defer if (trace) std.debug.print("[tvf] partitions: {d:.0}ms\n", .{prof.ticksToMs(prof.nowTicks() - t0)});
        const n_workers = if (allow_parallel) @min(self.dop, runs.items.len) else 1;
        if (n_workers <= 1) {
            var runner = try Runner.init(self.allocator, self, input_views, kernel_views, perm, max_run, self.output_cols);
            defer runner.deinit();
            for (runs.items) |run| _ = try runner.runOne(run);
        } else {
            try self.executeParallel(input_views, kernel_views, perm, max_run, runs.items, n_workers);
        }
        try self.gatherPassthrough(input_views, perm, identity_perm, if (allow_parallel) self.dop else 1);
    }

    /// Materialize pass-through output columns as ONE permuted bulk copy of
    /// their input sources. Runs are contiguous spans of `perm` processed in
    /// run order, so the concat of per-run gathers is exactly the full-perm
    /// gather — and doing it once per column kills the per-run append
    /// overhead (~3.5s cpu on 240k-run inputs). Columns are gathered in
    /// parallel: each is an independent destination store. An identity
    /// permutation (pre-ordered input) takes the straight range-copy path
    /// instead of the per-index gather.
    fn gatherPassthrough(self: *TableFnExec, input_views: []const ColumnView, perm: []const u32, contiguous: bool, dop: usize) !void {
        const pairs = self.entry.passthrough;
        if (pairs.len == 0) return;
        const t0 = exec.prof.nowTicks();
        const jobs = try self.allocator.alloc(GatherJob, pairs.len);
        defer self.allocator.free(jobs);
        for (pairs, jobs) |pp, *j| {
            j.* = .{
                .src = input_views[pp.in_idx],
                .alloc = self.allocator,
                .dst = &self.output_cols[pp.out_idx],
            };
        }
        try runGatherJobs(jobs, perm, contiguous, dop);
        if (getenv("THINDB_TVF_TRACE") != null) {
            std.debug.print("[tvf]   passthrough gather: {d:.0}ms wall ({d} cols{s})\n", .{
                exec.prof.ticksToMs(exec.prof.nowTicks() - t0),
                pairs.len,
                if (contiguous) ", contiguous" else "",
            });
        }
    }

    /// One independent column copy: `src` rows land in `dst` (via `alloc`)
    /// either in `perm` order or as one contiguous range copy.
    const GatherJob = struct {
        src: ColumnView,
        alloc: Allocator,
        dst: *ColumnStore,
    };

    /// Claim-loop parallel execution of independent column gathers. Each
    /// job's destination (and its allocator) is private to the job, so
    /// workers never contend beyond the claim counter. `contiguous` = the
    /// permutation is identity: bulk range copy instead of per-index gather.
    fn runGatherJobs(jobs: []const GatherJob, perm: []const u32, contiguous: bool, dop: usize) !void {
        if (jobs.len == 0) return;
        const Ctx = struct {
            jobs: []const GatherJob,
            perm: []const u32,
            contiguous: bool,
            next: std.atomic.Value(usize) = std.atomic.Value(usize).init(0),
            err: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),

            fn main(ctx: *@This()) void {
                while (true) {
                    const i = ctx.next.fetchAdd(1, .monotonic);
                    if (i >= ctx.jobs.len) return;
                    const j = ctx.jobs[i];
                    const res = if (ctx.contiguous)
                        transform.appendColumnRange(j.alloc, j.src, 0, ctx.perm.len, j.dst)
                    else
                        transform.appendByIndices(j.alloc, j.src, ctx.perm, j.dst);
                    res catch {
                        ctx.err.store(true, .release);
                        return;
                    };
                }
            }
        };
        var ctx = Ctx{ .jobs = jobs, .perm = perm, .contiguous = contiguous };
        const n_workers = @min(dop, jobs.len);
        if (n_workers <= 1) {
            Ctx.main(&ctx);
        } else {
            var threads: [16]?std.Thread = .{null} ** 16;
            const want = @min(n_workers, threads.len);
            for (threads[0..want]) |*th| th.* = std.Thread.spawn(.{}, Ctx.main, .{&ctx}) catch null;
            var spawned: usize = 0;
            for (threads[0..want]) |th| {
                if (th) |t| {
                    t.join();
                    spawned += 1;
                }
            }
            if (spawned == 0) Ctx.main(&ctx);
        }
        // The transform appends only fail on allocation.
        if (ctx.err.load(.acquire)) return error.OutOfMemory;
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
        const trace = getenv("THINDB_TVF_TRACE") != null;
        const prof = exec.prof;
        if (trace) std.debug.print("[tvf] multi {s}: {d} inputs\n", .{ self.entry.name, n_in });

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

            // Zero-copy borrow bind — the single-input contract per input:
            // a filterless chain over a stage's adopted contiguous result
            // takes shallow views for every column it passes through
            // untransformed; the pull below still runs (row-alignment
            // check + the remaining owned columns), but over a staged
            // input it is a buffer read, not chain execution.
            const borrowed = try a.alloc(?ColumnView, decl.len);
            defer a.free(borrowed);
            @memset(borrowed, null);
            var borrow_rows: u64 = 0;
            var n_borrowed: usize = 0;
            if (i < self.multi_borrow_srcs.len) {
                if (self.multi_borrow_srcs[i]) |src| bind: {
                    src.ensureRun() catch break :bind;
                    const res = src.result orelse break :bind;
                    const ad = res.adopted orelse break :bind;
                    if (ad.stores.len != res.schema.len) break :bind;
                    for (self.multi_borrow_maps[i], 0..) |m, ci| {
                        if (ci >= decl.len) break;
                        const si = m orelse continue;
                        borrowed[ci] = exec.mat_stage.MaterializedResult.presentAsSchemaType(
                            ad.stores[si].view(),
                            decl[ci].type,
                        );
                        n_borrowed += 1;
                    }
                    borrow_rows = res.total_rows;
                }
            }

            const t_drain = if (trace) prof.nowTicks() else 0;
            var accumulated: usize = 0;
            var up = self.upstreams[i];
            while (try up.next()) |batch| {
                for (self.input_maps[i], cols, 0..) |ui, *store, ci| {
                    if (borrowed[ci] != null) continue;
                    try transform.appendAllColumn(a, batch.values[ui], store);
                }
                accumulated += batch.row_count;
            }
            if (n_borrowed > 0 and accumulated != borrow_rows) return Error.TableFnInputMismatch;
            const n_rows = accumulated;
            if (trace) std.debug.print("[tvf]   input{d} drain {d} rows: {d:.0}ms (borrowed {d}/{d} cols)\n", .{
                i, n_rows, prof.ticksToMs(prof.nowTicks() - t_drain), n_borrowed, decl.len,
            });
            const views = try a.alloc(ColumnView, decl.len);
            errdefer a.free(views);
            for (cols, views, 0..) |c, *v, ci| v.* = borrowed[ci] orelse c.view();
            const perm = try a.alloc(u32, n_rows);
            errdefer a.free(perm);
            for (perm, 0..) |*p, r| p.* = @intCast(r);
            var digests: []u128 = undefined;
            const t_sort = if (trace) prof.nowTicks() else 0;
            var sort_path: []const u8 = "none";
            if (self.key_idxs[i].len + self.order_idxs[i].len > 0) {
                sort_path = "packed";
                digests = (try self.packedSortFor(views, self.key_idxs[i], self.order_idxs[i], perm)) orelse blk: {
                    sort_path = "generic-cmp";
                    // Non-packable ORDER BY (multi-column, or a string /
                    // non-int-family column): the generic fallback the
                    // single-input path takes. Digests are computed
                    // separately (over the PARTITION keys only) so run
                    // formation and cross-input matching stay byte-
                    // identical to the packed path; the sort is digest-
                    // major so each input's run list stays ascending for
                    // the merge.
                    const d = try a.alloc(u128, n_rows);
                    for (d, 0..) |*dg, r| dg.* = keyDigestAt(views, self.key_idxs[i], r);
                    const mctx = MultiLessCtx{
                        .digests = d,
                        .views = views,
                        .order_idx = self.order_idxs[i],
                        .order_desc = self.order_desc,
                    };
                    try parallelSortPerm(MultiLessCtx, a, perm, mctx, self.dop);
                    break :blk d;
                };
            } else {
                digests = try a.alloc(u128, n_rows);
                @memset(digests, 0);
            }
            if (trace) std.debug.print("[tvf]   input{d} sort ({s}): {d:.0}ms\n", .{
                i, sort_path, prof.ticksToMs(prof.nowTicks() - t_sort),
            });
            ins[i] = .{ .cols = cols, .views = views, .perm = perm, .digests = digests };
            drained += 1;
        }

        // Per input: digest runs (contiguous in the sorted perm).
        const t_merge = if (trace) prof.nowTicks() else 0;
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
        if (trace) std.debug.print("[tvf]   merge ({d} groups): {d:.0}ms\n", .{
            groups.items.len, prof.ticksToMs(prof.nowTicks() - t_merge),
        });
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
        const t_kernel = if (trace) prof.nowTicks() else 0;
        if (n_workers <= 1) {
            var runner = try MultiRunner.init(a, self, views_by_input, perms_by_input, self.output_cols);
            defer runner.deinit();
            for (groups.items) |g| _ = try runner.runOne(g);
            if (trace) std.debug.print("[tvf]   kernel (serial, {d} groups): {d:.0}ms\n", .{
                groups.items.len, prof.ticksToMs(prof.nowTicks() - t_kernel),
            });
        } else {
            try self.executeMultiParallel(views_by_input, perms_by_input, groups.items, n_workers);
            if (trace) std.debug.print("[tvf]   kernel ({d} workers, {d} groups): {d:.0}ms\n", .{
                n_workers, groups.items.len, prof.ticksToMs(prof.nowTicks() - t_kernel),
            });
        }
        // Pass-through fill (sources = input 0): groups iterate in ascending
        // merged-digest order and input 0's runs are digest-ascending along
        // its sorted perm, so the group-order concat of input 0's spans IS
        // its full permutation — one bulk gather per column, exactly like
        // the single-input path. Row-aligned kernels emit spans[0].len rows
        // per group (validated in runOne), so output rows align 1:1.
        const contiguous0 = self.key_idxs[0].len + self.order_idxs[0].len == 0;
        try self.gatherPassthrough(ins[0].views, ins[0].perm, contiguous0, if (allow_parallel) self.dop else 1);
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
                batch: usize,
            ) void {
                while (true) {
                    if (abort.load(.acquire)) return;
                    const base = next_group.fetchAdd(batch, .monotonic);
                    if (base >= group_list.len) return;
                    const end = @min(group_list.len, base + batch);
                    for (group_list[base..end], seg_list[base..end]) |group, *seg| {
                        // Offsets track the first COMPUTED store — pass-through
                        // private stores stay empty (bulk-gathered afterwards).
                        const before = w.runner.out_ptrs[0].rowCount();
                        const emitted = w.runner.runOne(group) catch |err| {
                            w.err = err;
                            abort.store(true, .release);
                            return;
                        };
                        seg.* = .{ .worker = wid, .off = before, .len = emitted };
                    }
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
        const batch = claimBatch(groups.len, n_workers);
        const threads = try a.alloc(?std.Thread, n_workers);
        defer a.free(threads);
        @memset(threads, null);
        for (workers, threads, 0..) |*w, *th, wid| {
            th.* = std.Thread.spawn(.{}, Worker.main, .{
                w, @as(u32, @intCast(wid)), groups, segs, &next_group, &abort, batch,
            }) catch null;
        }
        var spawned: usize = 0;
        for (threads) |th| {
            if (th) |t| {
                t.join();
                spawned += 1;
            }
        }
        if (spawned == 0) Worker.main(&workers[0], 0, groups, segs, &next_group, &abort, batch);
        for (workers[0..built]) |*w| {
            if (w.err) |err| return err;
        }

        // Pass-through columns are skipped — their private stores are
        // empty and the operator bulk-gathers them after the concat.
        const is_pass = try a.alloc(bool, self.output_cols.len);
        defer a.free(is_pass);
        @memset(is_pass, false);
        for (self.entry.passthrough) |pp| is_pass[pp.out_idx] = true;
        const stores_of = try a.alloc([]ColumnStore, built);
        defer a.free(stores_of);
        for (workers[0..built], stores_of) |*w, *s| s.* = w.out_stores;
        try self.concatRunSegs(segs, stores_of, is_pass);
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
                // Input 0's kernel-visible prefix only — carry columns are
                // pass-through sources and never reach the kernel. Other
                // inputs are handed whole.
                const decl = if (i == 0)
                    op.entry.input_schemas[0][0..op.entry.kernel_input_cols]
                else
                    op.entry.input_schemas[i];
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
            // Kernel output sink: computed stores only — pass-through
            // columns are operator-filled after all groups complete.
            const out_ptrs = try allocator.alloc(*ColumnStore, out_stores.len - op.entry.passthrough.len);
            errdefer allocator.free(out_ptrs);
            var n_computed: usize = 0;
            outer: for (out_stores, 0..) |*c, i| {
                for (op.entry.passthrough) |pp| {
                    if (pp.out_idx == i) continue :outer;
                }
                out_ptrs[n_computed] = c;
                n_computed += 1;
            }
            std.debug.assert(n_computed == out_ptrs.len);
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
            // Gather each input's span (input 0: kernel-visible prefix
            // only); empty spans leave arity-correct zero-row columns
            // (cleared scratch views).
            for (0..op.upstreams.len) |i| {
                for (self.scratch[i]) |*store| store.clear();
                const span = spans[i];
                if (span.end > span.start) {
                    const perm = self.perms_by_input[i];
                    for (self.views_by_input[i][0..self.scratch[i].len], self.scratch[i]) |v, *store| {
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
            // Row alignment is to INPUT 0: a group where input 0 arrived
            // empty must emit nothing (the pass-through fill is one bulk
            // gather over input 0's permutation).
            if (op.entry.row_aligned and after - before != spans[0].end - spans[0].start) {
                return Error.TableFnOutputMismatch;
            }
            return after - before;
        }
    };

    const Run = struct { start: usize, end: usize };

    /// Most drain-copy threads worth spawning: memcpy saturates memory
    /// bandwidth well below core count, and each thread only helps while
    /// there are unclaimed columns.
    const max_drain_workers: usize = 7;

    extern "kernel32" fn Sleep(ms: u32) callconv(.winapi) void;

    fn sleepBriefly() void {
        switch (builtin.os.tag) {
            .windows => Sleep(1),
            .linux => {
                const ts = [2]isize{ 0, std.time.ns_per_ms };
                _ = std.os.linux.syscall2(.nanosleep, @intFromPtr(&ts), 0);
            },
            else => std.Thread.yield() catch std.atomic.spinLoopHint(),
        }
    }

    /// Two-phase parallel per-batch input copy — the window fused-drain
    /// protocol (#108) adapted to the TVF input drain. Each batch runs two
    /// claim phases over a unique fetchAdd cursor:
    ///
    ///   .prepare — units are COLUMNS: extend each store to its final size
    ///              (transform.prepareAppend; per-column arenas make the
    ///              concurrent resizes race-free).
    ///   .write   — units are (column × row-tile): positional, alloc-free
    ///              fills. Tile bounds land on absolute 8-row boundaries so
    ///              no two writers share a validity byte.
    const DrainPar = struct {
        op: *TableFnExec,
        arenas: []std.heap.ArenaAllocator,
        stores: []ColumnStore,
        /// Declared column indices that actually drain-copy (borrowed
        /// columns are excluded — their views come from the source stage).
        owned: []const usize,
        batch: Batch = undefined,
        preps: []transform.PreparedAppend = &.{},
        bounds: []usize = &.{},
        n_tiles: usize = 0,
        n_units: usize = 0,
        mode: Mode = .prepare,
        gen: std.atomic.Value(usize) = std.atomic.Value(usize).init(0),
        unit_cursor: std.atomic.Value(usize) = std.atomic.Value(usize).init(0),
        units_done: std.atomic.Value(usize) = std.atomic.Value(usize).init(0),
        parked: std.atomic.Value(usize) = std.atomic.Value(usize).init(0),
        stop: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
        failed: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),

        const Mode = enum { prepare, write };

        fn worker(dp: *DrainPar) void {
            var seen: usize = 0;
            while (true) {
                var spins: usize = 0;
                while (dp.gen.load(.acquire) == seen) {
                    if (dp.stop.load(.acquire)) return;
                    spins += 1;
                    if (spins < 2048) {
                        std.atomic.spinLoopHint();
                    } else if (spins < 4096) {
                        std.Thread.yield() catch std.atomic.spinLoopHint();
                    } else {
                        sleepBriefly();
                    }
                }
                seen = dp.gen.load(.acquire);
                _ = dp.parked.fetchSub(1, .acq_rel);
                dp.runUnits();
                _ = dp.parked.fetchAdd(1, .acq_rel);
            }
        }

        fn runUnits(dp: *DrainPar) void {
            const ncols = dp.owned.len;
            while (true) {
                const u = dp.unit_cursor.fetchAdd(1, .acq_rel);
                if (u >= dp.n_units) return;
                switch (dp.mode) {
                    .prepare => {
                        const ci = dp.owned[u];
                        dp.preps[ci] = transform.prepareAppend(
                            dp.arenas[ci].allocator(),
                            dp.batch.values[dp.op.input_map[ci]],
                            dp.batch.row_count,
                            &dp.stores[ci],
                        ) catch blk: {
                            dp.failed.store(true, .release);
                            break :blk .{ .base_row = 0, .positional = false };
                        };
                    },
                    .write => {
                        // Interleave column-major so concurrent claims walk
                        // different columns (independent memory streams).
                        const ci = dp.owned[u % ncols];
                        const t = u / ncols;
                        if (dp.preps[ci].positional) {
                            transform.writeAppendSlice(
                                dp.batch.values[dp.op.input_map[ci]],
                                dp.bounds[t],
                                dp.bounds[t + 1],
                                &dp.stores[ci],
                                dp.preps[ci],
                            );
                        }
                    },
                }
                _ = dp.units_done.fetchAdd(1, .acq_rel);
            }
        }

        /// Publish one phase, participate, and wait for completion + full
        /// re-park before returning (after which shared state is single-
        /// owner again).
        fn runPhase(dp: *DrainPar, mode: Mode, n_units: usize, n_workers: usize) void {
            dp.mode = mode;
            dp.n_units = n_units;
            dp.unit_cursor.store(0, .release);
            dp.units_done.store(0, .release);
            _ = dp.gen.fetchAdd(1, .release);
            dp.runUnits();
            var spins: usize = 0;
            while (dp.units_done.load(.acquire) < n_units or
                dp.parked.load(.acquire) < n_workers)
            {
                spins += 1;
                if (spins < 4096) std.atomic.spinLoopHint() else std.Thread.yield() catch {};
            }
        }
    };

    /// Where each run's output landed: which worker produced it and the
    /// row range inside that worker's private output stores. Written by
    /// exactly one worker per slot — no synchronization needed beyond the
    /// claim counter.
    const RunSeg = struct { worker: u32 = 0, off: usize = 0, len: usize = 0 };

    /// Runs per claim-counter grab. Consecutive runs a worker claims land
    /// contiguously in its private stores, so batching turns the run-order
    /// output concat from one range copy per run×column into one per
    /// batch×column (230K-run row-generating kernels spent more time in
    /// appendColumnRange call overhead than in the kernel).
    fn claimBatch(n_runs: usize, n_workers: usize) usize {
        return @max(1, @min(64, n_runs / (n_workers * 8)));
    }

    /// Concatenate per-run worker segments into the output columns in run
    /// order, coalescing adjacent segments that sit contiguously in the
    /// same worker's private stores (whole claim batches, in the common
    /// case). Zero-length runs extend the current chain — their offset
    /// equals the running end.
    fn concatRunSegs(self: *TableFnExec, segs: []const RunSeg, stores_of: []const []ColumnStore, is_pass: []const bool) !void {
        const trace = getenv("THINDB_TVF_TRACE") != null;
        const t0 = if (trace) exec.prof.nowTicks() else 0;
        var n_chains: usize = 0;
        var s: usize = 0;
        while (s < segs.len) {
            const wid = segs[s].worker;
            const off = segs[s].off;
            var len: usize = 0;
            var e = s;
            while (e < segs.len and segs[e].worker == wid and segs[e].off == off + len) {
                len += segs[e].len;
                e += 1;
            }
            if (len > 0) {
                n_chains += 1;
                for (stores_of[wid], self.output_cols, is_pass) |src, *dst, skip| {
                    if (skip) continue;
                    try transform.appendColumnRange(self.allocator, src.view(), off, off + len, dst);
                }
            }
            s = e;
        }
        if (trace) std.debug.print("[tvf]   writer concat: {d:.0}ms ({d} segs -> {d} chains)\n", .{
            exec.prof.ticksToMs(exec.prof.nowTicks() - t0), segs.len, n_chains,
        });
    }

    fn executeParallel(
        self: *TableFnExec,
        input_views: []const ColumnView,
        kernel_views: []const ColumnView,
        perm: []const u32,
        max_run: usize,
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
                batch: usize,
            ) void {
                while (true) {
                    if (abort.load(.acquire)) return;
                    const base = next_run.fetchAdd(batch, .monotonic);
                    if (base >= run_list.len) return;
                    const end = @min(run_list.len, base + batch);
                    for (run_list[base..end], seg_list[base..end]) |run, *seg| {
                        // Offsets track the first COMPUTED store — pass-through
                        // private stores stay empty (bulk-gathered afterwards).
                        const before = w.runner.out_ptrs[0].rowCount();
                        const emitted = w.runner.runOne(run) catch |err| {
                            w.err = err;
                            abort.store(true, .release);
                            return;
                        };
                        seg.* = .{ .worker = wid, .off = before, .len = emitted };
                    }
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
                .runner = try Runner.init(self.allocator, self, input_views, kernel_views, perm, max_run, out_stores),
                .out_stores = out_stores,
            };
            built += 1;
        }

        var next_run = std.atomic.Value(usize).init(0);
        var abort = std.atomic.Value(bool).init(false);
        const batch = claimBatch(runs.len, n_workers);
        const threads = try self.allocator.alloc(?std.Thread, n_workers);
        defer self.allocator.free(threads);
        @memset(threads, null);
        for (workers, threads, 0..) |*w, *th, wid| {
            th.* = std.Thread.spawn(.{}, Worker.main, .{
                w, @as(u32, @intCast(wid)), runs, segs, &next_run, &abort, batch,
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
            Worker.main(&workers[0], 0, runs, segs, &next_run, &abort, batch);
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
        // deterministic regardless of worker scheduling. Each segment is a
        // contiguous row range of its worker's private stores, so the
        // concat is a bulk range copy per column — no per-row index list.
        // Pass-through columns are skipped — their private stores are
        // empty and the operator bulk-gathers them afterwards
        // (gatherPassthrough).
        const is_pass = try self.allocator.alloc(bool, self.output_cols.len);
        defer self.allocator.free(is_pass);
        @memset(is_pass, false);
        for (self.entry.passthrough) |pp| is_pass[pp.out_idx] = true;
        const stores_of = try self.allocator.alloc([]ColumnStore, built);
        defer self.allocator.free(stores_of);
        for (workers[0..built], stores_of) |*w, *s| s.* = w.out_stores;
        try self.concatRunSegs(segs, stores_of, is_pass);
    }

    /// Per-worker execution state: zero-copy partition views over the
    /// run-contiguous kernel columns, key values, and the output sink
    /// (pointing at either the operator's output columns — serial — or the
    /// worker's private stores). `runOne` slices a run, invokes process(),
    /// and enforces the rectangular-output contract; returns rows emitted.
    const Runner = struct {
        allocator: Allocator,
        op: *const TableFnExec,
        input_views: []const ColumnView,
        /// KERNEL-VISIBLE input columns (the first `entry.kernel_input_cols`)
        /// already in run-contiguous order — a run's partition views are
        /// slices [run.start, run.end), no per-run copies. Carry columns
        /// past that are pass-through sources only and never reach the
        /// kernel.
        kernel_views: []const ColumnView,
        perm: []const u32,
        part_views: []ColumnView,
        /// Per kernel column: bit-shift scratch for validity windows whose
        /// run start is not byte-aligned (a ColumnView bitmap has no bit
        /// offset). Empty for columns without a bitmap.
        valid_scratch: [][]u8,
        key_vals: []?Value,
        /// Kernel output sink: the COMPUTED output stores only (declared
        /// order, pass-through columns skipped). Pass-through stores are
        /// filled by the operator's bulk gather after all runs complete.
        out_ptrs: []*ColumnStore,
        arena: std.heap.ArenaAllocator,
        /// Ticks spent INSIDE the user kernel (process() calls), for the
        /// THINDB_TVF_TRACE phase breakdown.
        kernel_ticks: i64 = 0,

        fn init(
            allocator: Allocator,
            op: *const TableFnExec,
            input_views: []const ColumnView,
            kernel_views: []const ColumnView,
            perm: []const u32,
            max_run: usize,
            out_stores: []ColumnStore,
        ) !Runner {
            const n_kernel = kernel_views.len;
            const part_views = try allocator.alloc(ColumnView, n_kernel);
            errdefer allocator.free(part_views);
            const valid_scratch = try allocator.alloc([]u8, n_kernel);
            errdefer allocator.free(valid_scratch);
            var vs_built: usize = 0;
            errdefer for (valid_scratch[0..vs_built]) |vs| {
                if (vs.len > 0) allocator.free(vs);
            };
            for (kernel_views, valid_scratch) |kv, *vs| {
                vs.* = if (kv.nulls != null) try allocator.alloc(u8, (max_run + 7) / 8) else &.{};
                vs_built += 1;
            }
            const key_vals = try allocator.alloc(?Value, op.key_idx.len);
            errdefer allocator.free(key_vals);
            const out_ptrs = try allocator.alloc(*ColumnStore, out_stores.len - op.entry.passthrough.len);
            errdefer allocator.free(out_ptrs);
            var n_computed: usize = 0;
            outer: for (out_stores, 0..) |*c, i| {
                for (op.entry.passthrough) |pp| {
                    if (pp.out_idx == i) continue :outer;
                }
                out_ptrs[n_computed] = c;
                n_computed += 1;
            }
            std.debug.assert(n_computed == out_ptrs.len);
            return .{
                .allocator = allocator,
                .op = op,
                .input_views = input_views,
                .kernel_views = kernel_views,
                .perm = perm,
                .part_views = part_views,
                .valid_scratch = valid_scratch,
                .key_vals = key_vals,
                .out_ptrs = out_ptrs,
                .arena = std.heap.ArenaAllocator.init(allocator),
            };
        }

        fn deinit(self: *Runner) void {
            self.arena.deinit();
            for (self.valid_scratch) |vs| {
                if (vs.len > 0) self.allocator.free(vs);
            }
            self.allocator.free(self.valid_scratch);
            self.allocator.free(self.part_views);
            self.allocator.free(self.key_vals);
            self.allocator.free(self.out_ptrs);
        }

        fn runOne(self: *Runner, run: Run) !usize {
            const op = self.op;
            for (self.kernel_views, self.part_views, self.valid_scratch) |kv, *pv, vs| {
                pv.* = sliceRunView(kv, run.start, run.end, vs);
            }
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
            const n_rows = run.end - run.start;
            if (op.entry.row_aligned and after - before != n_rows) {
                return Error.TableFnOutputMismatch;
            }
            // Pass-through columns are NOT filled here: runs are contiguous
            // spans of the sorted permutation processed in run order, so the
            // concat of per-run gathers IS one full-permutation gather. The
            // operator does that once per column after all runs complete
            // (gatherPassthrough) — per-run appends measured ~3.5s cpu of
            // pure overhead on 240k-run inputs.
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
            .varchar, .string, .char, .json => true,
            else => false,
        };
        if (!string_family) return false;
        return switch (declared) {
            .varchar, .string, .char, .json => true,
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
            // NULLs first ascending / last descending — the engine's
            // validity-aware ordering (matches the generic comparator and
            // the window sort, which pre-ordered inputs ride).
            if (a.ord_null != b.ord_null) return if (ctx.desc) b.ord_null else a.ord_null;
            if (a.ord != b.ord) return if (ctx.desc) a.ord > b.ord else a.ord < b.ord;
            return a.row < b.row;
        }
    };

    /// One row's partition-key digest — the run-matching identity (digest
    /// equality = key equality, the same contract the wide-key GROUP BY
    /// relies on engine-wide). Must stay byte-identical between the packed
    /// sort and the generic multi-input fallback, and across co-partitioned
    /// inputs (their digests align in the N-way group merge).
    fn keyDigestAt(views: []const ColumnView, key_idx: []const usize, row: usize) u128 {
        var h1 = std.hash.Wyhash.init(0x9e3779b97f4a7c15);
        var h2 = std.hash.Wyhash.init(0x517cc1b727220a95);
        for (key_idx) |ki| {
            const v = views[ki];
            if (!v.isValid(row)) {
                h1.update(&.{0});
                h2.update(&.{0});
                continue;
            }
            h1.update(&.{1});
            h2.update(&.{1});
            switch (v.data) {
                .varchar, .string, .char, .json => |sv| {
                    const bytes = sv.rowBytes(row);
                    h1.update(bytes);
                    h2.update(bytes);
                    // Length delimiter: ("ab","c") must not collide
                    // with ("a","bc").
                    h1.update(std.mem.asBytes(&bytes.len));
                    h2.update(std.mem.asBytes(&bytes.len));
                },
                inline .int, .bigint, .tinyint, .smallint, .date, .datetime, .largeint, .decimal64, .decimal128, .uuid, .float, .double, .boolean => |s| {
                    h1.update(std.mem.asBytes(&s[row]));
                    h2.update(std.mem.asBytes(&s[row]));
                },
            }
        }
        return (@as(u128, h1.final()) << 64) | h2.final();
    }

    /// Generic multi-input comparator: partition DIGEST first (run
    /// formation and the ascending-digest run lists the N-way merge
    /// depends on must match the packed path exactly), then the order
    /// columns with the engine's validity-aware ordering (NULLs first
    /// ascending / last descending), then arrival for a stable tie.
    const MultiLessCtx = struct {
        digests: []const u128,
        views: []const ColumnView,
        order_idx: []const usize,
        order_desc: []const bool,

        fn less(ctx: MultiLessCtx, a: u32, b: u32) bool {
            const da = ctx.digests[a];
            const db = ctx.digests[b];
            if (da != db) return da < db;
            for (ctx.order_idx, ctx.order_desc) |oi, desc| {
                switch (cmpAt(ctx.views[oi], a, b)) {
                    .lt => return !desc,
                    .gt => return desc,
                    .eq => {},
                }
            }
            return a < b;
        }
    };

    /// Parallel samplesort over a row permutation for the generic-comparator
    /// fallbacks. Both LessCtx and MultiLessCtx end in an arrival-index
    /// tie-break, so the order is strict and total — the result is
    /// byte-identical to the serial sort no matter how rows scatter into
    /// buckets. Serial below the threshold, where bucket bookkeeping costs
    /// more than it saves.
    fn parallelSortPerm(comptime Ctx: type, allocator: Allocator, perm: []u32, ctx: Ctx, dop: usize) !void {
        const n = perm.len;
        const allow_parallel = !builtin.is_test or force_parallel_in_tests;
        if (!allow_parallel or dop <= 1 or n < (1 << 17)) {
            std.mem.sortUnstable(u32, perm, ctx, Ctx.less);
            return;
        }
        const workers: usize = @min(dop, 32);
        const bucket_count: usize = @min(@as(usize, 128), std.math.ceilPowerOfTwoAssert(usize, workers * 4));

        // Deterministic evenly-strided sample → splitters. The strict order
        // makes every sampled key distinct, so heavy-duplicate key columns
        // still split into level ranges.
        const sample_len = @min(n, bucket_count * 16);
        const sample = try allocator.alloc(u32, sample_len);
        defer allocator.free(sample);
        const stride = n / sample_len;
        for (sample, 0..) |*s, i| s.* = perm[i * stride];
        std.mem.sortUnstable(u32, sample, ctx, Ctx.less);
        const splitters = try allocator.alloc(u32, bucket_count - 1);
        defer allocator.free(splitters);
        for (splitters, 1..) |*sp, b| sp.* = sample[b * sample_len / bucket_count];

        const placed = try allocator.alloc(u32, n);
        defer allocator.free(placed);
        const counts = try allocator.alloc(usize, workers * bucket_count);
        defer allocator.free(counts);
        @memset(counts, 0);

        const Job = struct {
            perm: []u32,
            placed: []u32,
            splitters: []const u32,
            counts: []usize,
            sort_ctx: Ctx,
            workers: usize,
            bucket_count: usize,
            bucket_offsets: []usize,
            next_bucket: std.atomic.Value(usize) = .init(0),

            fn bucketOf(job: *const @This(), row: u32) usize {
                var lo: usize = 0;
                var hi: usize = job.splitters.len;
                while (lo < hi) {
                    const mid = (lo + hi) / 2;
                    if (Ctx.less(job.sort_ctx, row, job.splitters[mid])) hi = mid else lo = mid + 1;
                }
                return lo;
            }
            fn rowRange(job: *const @This(), w: usize) [2]usize {
                const per = (job.perm.len + job.workers - 1) / job.workers;
                const s = @min(job.perm.len, w * per);
                return .{ s, @min(job.perm.len, s + per) };
            }
            fn countPhase(job: *@This(), w: usize) void {
                const r = job.rowRange(w);
                const my = job.counts[w * job.bucket_count ..][0..job.bucket_count];
                for (job.perm[r[0]..r[1]]) |row| my[job.bucketOf(row)] += 1;
            }
            fn placePhase(job: *@This(), w: usize) void {
                const r = job.rowRange(w);
                const my = job.counts[w * job.bucket_count ..][0..job.bucket_count];
                for (job.perm[r[0]..r[1]]) |row| {
                    const b = job.bucketOf(row);
                    job.placed[my[b]] = row;
                    my[b] += 1;
                }
            }
            fn bucketPhase(job: *@This(), _: usize) void {
                while (true) {
                    const b = job.next_bucket.fetchAdd(1, .monotonic);
                    if (b >= job.bucket_count) return;
                    const s = job.bucket_offsets[b];
                    const e = job.bucket_offsets[b + 1];
                    std.mem.sortUnstable(u32, job.placed[s..e], job.sort_ctx, Ctx.less);
                }
            }
            // A failed spawn runs the phase inline on this thread — slower,
            // never wrong (each worker index owns a fixed row range).
            fn runPhase(job: *@This(), comptime phase: fn (*@This(), usize) void) void {
                var threads: [32]?std.Thread = @splat(null);
                for (1..job.workers) |w| {
                    threads[w] = std.Thread.spawn(.{}, phase, .{ job, w }) catch blk: {
                        phase(job, w);
                        break :blk null;
                    };
                }
                phase(job, 0);
                for (threads[1..job.workers]) |maybe| {
                    if (maybe) |t| t.join();
                }
            }
        };
        const bucket_offsets = try allocator.alloc(usize, bucket_count + 1);
        defer allocator.free(bucket_offsets);
        var job = Job{
            .perm = perm,
            .placed = placed,
            .splitters = splitters,
            .counts = counts,
            .sort_ctx = ctx,
            .workers = workers,
            .bucket_count = bucket_count,
            .bucket_offsets = bucket_offsets,
        };
        job.runPhase(Job.countPhase);
        // Bucket-major prefix over worker-local counts: each worker's
        // in-bucket writes land after every earlier worker's — placement is
        // deterministic, so the buckets (and the final perm) are too.
        var off: usize = 0;
        for (0..bucket_count) |b| {
            bucket_offsets[b] = off;
            for (0..workers) |w| {
                const c = counts[w * bucket_count + b];
                counts[w * bucket_count + b] = off;
                off += c;
            }
        }
        bucket_offsets[bucket_count] = off;
        job.runPhase(Job.placePhase);
        job.runPhase(Job.bucketPhase);
        @memcpy(perm, placed);
    }

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
            k.digest = keyDigestAt(views, key_idx, i);
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
            .varchar, .string, .char, .json => v == .text,
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

    /// Zero-copy view of rows [start, end) of a run-contiguous column.
    /// Data and string offsets slice at any offset (string offsets stay
    /// absolute into the full bytes buffer); the validity bitmap, which has
    /// no bit-offset field, is re-based into `scratch` when the window
    /// start is not byte-aligned. `scratch` must hold (end-start+7)/8 bytes
    /// whenever the source carries a bitmap.
    fn sliceRunView(v: ColumnView, start: usize, end: usize, scratch: []u8) ColumnView {
        const n = end - start;
        var out: ColumnView = switch (v.data) {
            .int => |s| .{ .data = .{ .int = s[start..][0..n] } },
            .bigint => |s| .{ .data = .{ .bigint = s[start..][0..n] } },
            .boolean => |s| .{ .data = .{ .boolean = s[start..][0..n] } },
            .tinyint => |s| .{ .data = .{ .tinyint = s[start..][0..n] } },
            .smallint => |s| .{ .data = .{ .smallint = s[start..][0..n] } },
            .float => |s| .{ .data = .{ .float = s[start..][0..n] } },
            .double => |s| .{ .data = .{ .double = s[start..][0..n] } },
            .date => |s| .{ .data = .{ .date = s[start..][0..n] } },
            .datetime => |s| .{ .data = .{ .datetime = s[start..][0..n] } },
            .largeint => |s| .{ .data = .{ .largeint = s[start..][0..n] } },
            .decimal64 => |s| .{ .data = .{ .decimal64 = s[start..][0..n] } },
            .decimal128 => |s| .{ .data = .{ .decimal128 = s[start..][0..n] } },
            .uuid => |s| .{ .data = .{ .uuid = s[start..][0..n] } },
            .varchar => |sv| .{ .data = .{ .varchar = .{ .offsets = sv.offsets[start..][0 .. n + 1], .bytes = sv.bytes } } },
            .string => |sv| .{ .data = .{ .string = .{ .offsets = sv.offsets[start..][0 .. n + 1], .bytes = sv.bytes } } },
            .char => |sv| .{ .data = .{ .char = .{ .offsets = sv.offsets[start..][0 .. n + 1], .bytes = sv.bytes } } },
            .json => |sv| .{ .data = .{ .json = .{ .offsets = sv.offsets[start..][0 .. n + 1], .bytes = sv.bytes } } },
        };
        if (v.nulls) |bm| {
            if (start % 8 == 0) {
                out.nulls = bm[start / 8 ..];
            } else {
                copyValidityWindow(bm, start, n, scratch);
                out.nulls = scratch[0 .. (n + 7) / 8];
            }
        }
        return out;
    }

    /// Re-base validity bits [start, start+n) of `src` to bit 0 of `dst`.
    fn copyValidityWindow(src: []const u8, start: usize, n: usize, dst: []u8) void {
        const n_bytes = (n + 7) / 8;
        const base = start / 8;
        const rem = start % 8;
        if (rem == 0) {
            @memcpy(dst[0..n_bytes], src[base..][0..n_bytes]);
            return;
        }
        const r: u3 = @intCast(rem);
        const inv: u3 = @intCast(8 - rem);
        for (dst[0..n_bytes], 0..) |*d, j| {
            const lo = src[base + j] >> r;
            const hi: u8 = if (base + j + 1 < src.len) src[base + j + 1] else 0;
            d.* = lo | (hi << inv);
        }
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
            .varchar, .string, .char, .json => |s| .{ .text = s.bytes[s.offsets[row]..s.offsets[row + 1]] },
            else => null,
        };
    }
};

extern "c" fn getenv(name: [*:0]const u8) ?[*:0]const u8;

test "copyValidityWindow re-bases arbitrary bit windows exactly" {
    var prng = std.Random.DefaultPrng.init(0xfeedface);
    var src: [8]u8 = undefined;
    prng.random().bytes(&src);
    const total = src.len * 8;
    var dst: [8]u8 = undefined;
    var start: usize = 0;
    while (start < 40) : (start += 1) {
        const lens = [_]usize{ 1, 3, 7, 8, 9, 16, 17, total - start };
        for (lens) |n| {
            if (start + n > total) continue;
            @memset(&dst, 0xAA);
            TableFnExec.copyValidityWindow(&src, start, n, &dst);
            for (0..n) |i| {
                const want = (src[(start + i) / 8] >> @intCast((start + i) % 8)) & 1 != 0;
                const got = (dst[i / 8] >> @intCast(i % 8)) & 1 != 0;
                try std.testing.expectEqual(want, got);
            }
        }
    }
}

test "sliceRunView: values, validity, and absolute string offsets" {
    var vals: [16]i64 = undefined;
    for (&vals, 0..) |*v, i| v.* = @intCast(i * 10);
    // Valid iff i % 3 != 0.
    var bm: [2]u8 = .{ 0, 0 };
    for (0..16) |i| {
        if (i % 3 != 0) bm[i / 8] |= @as(u8, 1) << @intCast(i % 8);
    }
    const v = ColumnView{ .data = .{ .bigint = &vals }, .nulls = &bm };

    // Misaligned start: validity re-based through scratch.
    var scratch: [2]u8 = undefined;
    const mid = TableFnExec.sliceRunView(v, 3, 11, &scratch);
    try std.testing.expectEqual(@as(usize, 8), mid.rowCount());
    for (0..8) |i| {
        try std.testing.expectEqual(@as(i64, @intCast((3 + i) * 10)), mid.data.bigint[i]);
        try std.testing.expectEqual((3 + i) % 3 != 0, mid.isValid(i));
    }

    // Byte-aligned start: the source bitmap is shared, no copy.
    const aligned = TableFnExec.sliceRunView(v, 8, 16, &scratch);
    try std.testing.expectEqual(@intFromPtr(&bm[1]), @intFromPtr(aligned.nulls.?.ptr));
    for (0..8) |i| {
        try std.testing.expectEqual((8 + i) % 3 != 0, aligned.isValid(i));
    }

    // Strings: offsets slice at any position, bytes stay absolute.
    const offsets = [_]u32{ 0, 2, 3, 3, 6 };
    const sv = ColumnView{ .data = .{ .string = .{ .offsets = &offsets, .bytes = "aabccc" } } };
    const strs = TableFnExec.sliceRunView(sv, 1, 3, &scratch);
    try std.testing.expectEqual(@as(usize, 2), strs.rowCount());
    try std.testing.expectEqualStrings("b", strs.data.string.rowBytes(0));
    try std.testing.expectEqualStrings("", strs.data.string.rowBytes(1));
}

test "packed sort keys: NULL order values first ascending, last descending" {
    const null_key = TableFnExec.PackedKey{ .digest = 7, .ord = 0, .ord_null = true, .row = 0 };
    const val_key = TableFnExec.PackedKey{ .digest = 7, .ord = -5, .ord_null = false, .row = 1 };
    const asc = TableFnExec.PackCtx{ .desc = false };
    const desc = TableFnExec.PackCtx{ .desc = true };
    try std.testing.expect(TableFnExec.PackCtx.less(asc, null_key, val_key));
    try std.testing.expect(!TableFnExec.PackCtx.less(asc, val_key, null_key));
    try std.testing.expect(!TableFnExec.PackCtx.less(desc, null_key, val_key));
    try std.testing.expect(TableFnExec.PackCtx.less(desc, val_key, null_key));
    // Different partitions always order by digest, regardless of desc.
    const other = TableFnExec.PackedKey{ .digest = 9, .ord = 0, .ord_null = false, .row = 2 };
    try std.testing.expect(TableFnExec.PackCtx.less(desc, null_key, other));
}
