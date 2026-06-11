//! Window function operator. Blocking — drains all upstream rows into
//! per-column buffers, then for each WindowSpec sorts a permutation by
//! (partition_by ++ order_by), walks partitions, and evaluates each
//! WindowCall associated with that spec. Output is emitted in the
//! ORIGINAL input row order: input columns followed by one output
//! column per WindowCall.
//!
//! Tier 1 scope:
//!   - ROW_NUMBER, RANK, DENSE_RANK
//!   - LAG, LEAD (offset + literal-or-col-ref default)
//!   - FIRST_VALUE, LAST_VALUE
//!   - SUM, COUNT, AVG, MIN, MAX (aggregate windows)
//!   - PARTITION BY + ORDER BY
//!   - Default frame (RANGE UNBOUNDED PRECEDING TO CURRENT ROW when
//!     ORDER BY present; ROWS UNBOUNDED PRECEDING TO UNBOUNDED
//!     FOLLOWING otherwise)
//!   - ROWS BETWEEN <preceding|current|following> AND <...>
//!
//! Out of scope (Tier 2+): RANGE framing with N PRECEDING, GROUPS
//! framing, EXCLUDE clauses, NTH_VALUE, NTILE/PERCENT_RANK/CUME_DIST,
//! IGNORE NULLS semantics in the operator (parsed but not honored),
//! string outputs from window functions.

const std = @import("std");
const Allocator = std.mem.Allocator;

const types = @import("../types.zig");
const Column = types.Column;
const Type = types.Type;
const TypeTag = types.TypeTag;
const Value = types.Value;

const ir = @import("../ir/ir.zig");

const storage = @import("../storage/storage.zig");
const ColumnView = storage.ColumnView;

const engine = @import("../engine/engine.zig");
const ColumnStore = engine.ColumnStore;
const transform = @import("../engine/transform.zig");

const exec = @import("exec.zig");
const Query = exec.Query;
const Batch = exec.Batch;
const Error = exec.Error;
const makeQuery = exec.makeQuery;

const predicate = @import("predicate.zig");
const Predicate = predicate.Predicate;

pub const Window = struct {
    allocator: Allocator,
    upstream: Query,

    /// Output schema = input columns + one column per WindowCall.
    schema: []Column,
    /// Borrowed view of the upstream's schema; used during evaluation.
    input_schema: []const Column,

    /// One entry per `ir.WindowSpec`, indexes resolved at create time.
    spec_indices: []SpecIndices,
    /// One entry per `ir.WindowCall`: how to evaluate it.
    call_plans: []CallPlan,

    /// IR pointers held for the lifetime of the operator (frame info etc.).
    specs: []const ir.WindowSpec,
    calls: []const ir.WindowCall,

    // Materialized state, built lazily on first `next()`.
    drained: bool = false,
    /// Bytes charged for the accumulated input; released + freed once all
    /// rows have been emitted (the input is no longer a dependency).
    reserved_bytes: usize = 0,
    evicted: bool = false,
    accumulated: []ColumnStore,        // input columns
    output_columns: []ColumnStore,     // window outputs (fixed-width types)
    /// Parallel scratch for string-typed outputs. `string_outputs[ci]`
    /// is `&.{}` when the call's output isn't a string type; otherwise
    /// it's a `[N]?[]const u8` indexed by original row position
    /// (`null` = SQL NULL). String slices borrow into the input
    /// column's StringStore (lifetime-safe because the input column
    /// lives as long as the operator).
    string_outputs: [][]?[]const u8,
    accumulated_rows: u64 = 0,

    /// Worker-thread budget for the partitioned parallel sort/eval path.
    /// 1 = fully serial (embedded default — no surprise threads).
    dop: usize = 1,

    // Batch emit state — emits input + output in original order.
    emit_offset: usize = 0,
    out_input_columns: []ColumnStore,  // staging for input cols per batch
    out_output_columns: []ColumnStore, // staging for output cols per batch
    views: []ColumnView,               // input + output views, parallel to schema

    const batch_size: usize = 1024;

    /// Resolved column indices for one window spec.
    const SpecIndices = struct {
        partition_cols: []usize,
        order_cols: []usize,
        order_desc: []bool,
    };

    /// Per-call evaluation plan: which spec it uses + how to drive it.
    const CallPlan = struct {
        spec_idx: usize,
        func: ir.WindowFunc,
        /// First-argument column index when the function takes one
        /// (LAG/LEAD/FIRST_VALUE/LAST_VALUE/aggregate). undefined for
        /// nullary functions (ROW_NUMBER/RANK/DENSE_RANK).
        value_col: usize = std.math.maxInt(usize),
        /// LAG/LEAD offset. 1 by default.
        offset: i64 = 1,
        /// LAG/LEAD default. Three forms:
        ///   .none      — return NULL on out-of-bounds (SQL standard
        ///                default).
        ///   .literal   — a constant value.
        ///   .col_ref   — return the current row's value of this column
        ///                (StarRocks v4 extension).
        default_kind: DefaultKind = .none,
        default_literal: Value = .{ .bigint = 0 },
        default_col: usize = std.math.maxInt(usize),
        /// COUNT(*) accepts no arg-column; tracked via this flag.
        count_star: bool = false,
        /// Honor `IGNORE NULLS` on null-skipping functions (LAG, LEAD,
        /// FIRST_VALUE, LAST_VALUE). Forwarded from the IR.
        ignore_nulls: bool = false,
        /// `NTILE(n)` bucket count. Stored as i64 because the user
        /// supplies it as an integer literal and we want to reject
        /// non-positive values cleanly.
        ntile_buckets: i64 = 1,
        /// `NTH_VALUE(expr, n)` 1-based offset within the frame.
        nth_offset: i64 = 1,
    };

    const DefaultKind = enum { none, literal, col_ref };

    pub fn create(
        allocator: Allocator,
        upstream: Query,
        specs: []const ir.WindowSpec,
        calls: []const ir.WindowCall,
        dop: usize,
    ) !Query {
        const input_schema = upstream.outputSchema();

        // Resolve every WindowSpec's column refs to indices.
        const spec_indices = try allocator.alloc(SpecIndices, specs.len);
        errdefer freeSpecIndices(allocator, spec_indices, 0);
        var sinit: usize = 0;
        errdefer freeSpecIndices(allocator, spec_indices, sinit);
        for (specs, 0..) |sp, si| {
            const pcols = try allocator.alloc(usize, sp.partition_by.len);
            errdefer allocator.free(pcols);
            for (sp.partition_by, 0..) |name, i| {
                pcols[i] = lookupCol(input_schema, name) orelse return Error.ColumnNotFound;
            }
            const ocols = try allocator.alloc(usize, sp.order_by.len);
            errdefer allocator.free(ocols);
            const odesc = try allocator.alloc(bool, sp.order_by.len);
            errdefer allocator.free(odesc);
            for (sp.order_by, 0..) |s, i| {
                ocols[i] = lookupCol(input_schema, s.col) orelse return Error.ColumnNotFound;
                odesc[i] = s.desc;
            }
            spec_indices[si] = .{
                .partition_cols = pcols,
                .order_cols = ocols,
                .order_desc = odesc,
            };
            sinit = si + 1;
        }

        // Resolve every call's args + frame + default-kind into a plan.
        const call_plans = try allocator.alloc(CallPlan, calls.len);
        errdefer allocator.free(call_plans);
        for (calls, 0..) |c, ci| call_plans[ci] = try buildCallPlan(c, input_schema);

        // Build output schema = input + output-column-per-call.
        const schema = try allocator.alloc(Column, input_schema.len + calls.len);
        errdefer allocator.free(schema);
        for (input_schema, 0..) |col, i| schema[i] = col;
        for (calls, 0..) |c, ci| {
            const out_type = try outputType(c, call_plans[ci], input_schema);
            schema[input_schema.len + ci] = .{
                .name = c.output_name,
                .type = out_type,
                .nullable = true, // window outputs may be NULL (out-of-bounds LAG, etc.)
            };
        }

        // Allocate column buffers for input and output (one per schema col).
        const accumulated = try allocator.alloc(ColumnStore, input_schema.len);
        errdefer allocator.free(accumulated);
        var ainit: usize = 0;
        errdefer for (accumulated[0..ainit]) |*c| c.deinit(allocator);
        for (input_schema, 0..) |col, i| {
            accumulated[i] = try ColumnStore.init(allocator, col.type, col.nullable);
            ainit = i + 1;
        }

        const output_columns = try allocator.alloc(ColumnStore, calls.len);
        errdefer allocator.free(output_columns);
        var oinit: usize = 0;
        errdefer for (output_columns[0..oinit]) |*c| c.deinit(allocator);
        for (calls, 0..) |_, ci| {
            const col = schema[input_schema.len + ci];
            output_columns[ci] = try ColumnStore.init(allocator, col.type, true);
            oinit = ci + 1;
        }

        // Per-call string scratch — empty for non-string outputs;
        // size-N `?[]const u8` slice for string outputs. Filled in
        // `preSizeColumn` so that `create()` can keep this allocation
        // path tidy.
        const string_outputs = try allocator.alloc([]?[]const u8, calls.len);
        errdefer allocator.free(string_outputs);
        for (string_outputs) |*s| s.* = &.{};

        const out_input_columns = try allocator.alloc(ColumnStore, input_schema.len);
        errdefer allocator.free(out_input_columns);
        var iinit: usize = 0;
        errdefer for (out_input_columns[0..iinit]) |*c| c.deinit(allocator);
        for (input_schema, 0..) |col, i| {
            out_input_columns[i] = try ColumnStore.init(allocator, col.type, col.nullable);
            iinit = i + 1;
        }

        const out_output_columns = try allocator.alloc(ColumnStore, calls.len);
        errdefer allocator.free(out_output_columns);
        var ooinit: usize = 0;
        errdefer for (out_output_columns[0..ooinit]) |*c| c.deinit(allocator);
        for (calls, 0..) |_, ci| {
            const col = schema[input_schema.len + ci];
            out_output_columns[ci] = try ColumnStore.init(allocator, col.type, true);
            ooinit = ci + 1;
        }

        const views = try allocator.alloc(ColumnView, schema.len);
        errdefer allocator.free(views);

        const self = try allocator.create(Window);
        errdefer allocator.destroy(self);

        self.* = .{
            .allocator = allocator,
            .upstream = upstream,
            .schema = schema,
            .input_schema = input_schema,
            .spec_indices = spec_indices,
            .call_plans = call_plans,
            .specs = specs,
            .calls = calls,
            .accumulated = accumulated,
            .output_columns = output_columns,
            .string_outputs = string_outputs,
            .out_input_columns = out_input_columns,
            .out_output_columns = out_output_columns,
            .views = views,
            .dop = @max(1, dop),
        };
        return makeQuery(allocator, self);
    }

    pub fn deinit(self: *Window) void {
        var up = self.upstream;
        up.deinit();
        if (!self.evicted) {
            for (self.accumulated) |*c| c.deinit(self.allocator);
            self.allocator.free(self.accumulated);
        }
        for (self.output_columns) |*c| c.deinit(self.allocator);
        self.allocator.free(self.output_columns);
        for (self.string_outputs) |s| if (s.len > 0) self.allocator.free(s);
        self.allocator.free(self.string_outputs);
        for (self.out_input_columns) |*c| c.deinit(self.allocator);
        self.allocator.free(self.out_input_columns);
        for (self.out_output_columns) |*c| c.deinit(self.allocator);
        self.allocator.free(self.out_output_columns);
        self.allocator.free(self.views);
        freeSpecIndices(self.allocator, self.spec_indices, self.spec_indices.len);
        self.allocator.free(self.call_plans);
        self.allocator.free(self.schema);
        const allocator = self.allocator;
        allocator.destroy(self);
    }

    pub fn outputSchema(self: *Window) []const Column {
        return self.schema;
    }

    pub fn addPrune(self: *Window, pred: Predicate) !void {
        return self.upstream.addPrune(pred);
    }

    pub fn stats(self: *Window) exec.PipelineStats {
        const up = self.upstream.stats();
        // Window emits in input order and only appends columns, so the
        // upstream's sort claim and per-column stats carry through. The
        // appended window-output columns sit past the input schema, so the
        // stats lookup reads them as unknown.
        return .{
            .upper_rows = up.upper_rows,
            .sort_state = up.sort_state,
            .column_stats = up.column_stats,
        };
    }

    pub fn accountant(self: *Window) ?*exec.memory.MemoryAccountant {
        return self.upstream.accountant();
    }

    pub fn explain(self: *Window, out: *std.ArrayList(u8), allocator: std.mem.Allocator, depth: usize) !void {
        try exec.explainLine(out, allocator, depth, "Window");
        try self.upstream.explain(out, allocator, depth + 1);
    }

    /// Free the accumulated input buffer and release its reserved bytes
    /// once every row has been emitted. Idempotent. The per-batch emit
    /// buffers (`out_input_columns` / `out_output_columns`) and the window
    /// output columns are freed later in `deinit`.
    fn evict(self: *Window) void {
        if (self.evicted) return;
        for (self.accumulated) |*c| c.deinit(self.allocator);
        self.allocator.free(self.accumulated);
        self.accumulated = &.{};
        if (self.upstream.accountant()) |a| a.release(.window, self.reserved_bytes);
        self.reserved_bytes = 0;
        self.evicted = true;
    }

    pub fn next(self: *Window) !?Batch {
        if (!self.drained) try self.drainAndEvaluate();

        const remaining = self.accumulated_rows - self.emit_offset;
        if (remaining == 0) {
            self.evict();
            return null;
        }
        const n: usize = @intCast(@min(@as(u64, batch_size), remaining));
        const lo = self.emit_offset;
        const hi = lo + n;

        // Stage rows lo..hi from each input column into out_input_columns.
        for (self.out_input_columns) |*c| c.clear();
        for (self.out_input_columns, 0..) |*out, ci| {
            try appendRangeFromStore(self.allocator, self.accumulated[ci], lo, hi, out);
        }
        // Same for output columns — but string outputs materialize from
        // the `string_outputs[ci]` scratch instead of the (empty)
        // ColumnStore.
        for (self.out_output_columns) |*c| c.clear();
        for (self.out_output_columns, 0..) |*out, ci| {
            if (self.string_outputs[ci].len > 0) {
                try appendStringScratchRange(self.allocator, self.string_outputs[ci], lo, hi, out);
            } else {
                try appendRangeFromStore(self.allocator, self.output_columns[ci], lo, hi, out);
            }
        }

        for (self.out_input_columns, 0..) |c, i| self.views[i] = c.view();
        const off = self.input_schema.len;
        for (self.out_output_columns, 0..) |c, i| self.views[off + i] = c.view();

        self.emit_offset = hi;
        return Batch{ .schema = self.schema, .values = self.views, .row_count = n };
    }

    fn drainAndEvaluate(self: *Window) !void {
        // Drain upstream into accumulated.
        const _dt = if (exec.prof.enabled) exec.prof.nowTicks() else 0;
        const row_bytes = exec.memory.estimateRowBytes(self.input_schema);
        const acc = self.upstream.accountant();
        while (try self.upstream.next()) |batch| {
            const b = batch.row_count * row_bytes;
            if (acc) |a| try a.reserve(.window, b);
            self.reserved_bytes += b;
            for (batch.values, 0..) |view, ci| {
                try transform.appendAllColumn(self.allocator, view, &self.accumulated[ci]);
            }
            self.accumulated_rows += batch.row_count;
        }
        if (exec.prof.enabled) exec.prof.addPhase("window.drain", @intCast(@max(0, exec.prof.nowTicks() - _dt)));
        const n: usize = @intCast(self.accumulated_rows);
        if (n == 0) {
            self.drained = true;
            return;
        }

        // Pre-size every output column to N rows, all-null. Evaluators
        // overwrite specific positions; rows they don't touch stay null
        // (correct semantics for OOB LAG/LEAD when default = .none).
        // For string-typed outputs we leave the ColumnStore empty and
        // allocate `string_outputs[ci]` instead — strings are written
        // by row position into the scratch and materialized into a
        // StringStore at emit time.
        for (self.output_columns, 0..) |*out, ci| {
            const out_type = self.schema[self.input_schema.len + ci].type;
            if (isStringType(out_type)) {
                const scratch = try self.allocator.alloc(?[]const u8, n);
                for (scratch) |*e| e.* = null;
                self.string_outputs[ci] = scratch;
            } else {
                try preSizeColumn(self.allocator, out, out_type, n);
            }
        }

        // For each spec, build a permutation and evaluate all its calls.
        // A partitioned spec over a large input takes the bucket-parallel
        // path: partitions are independent, so hash-scattering them across
        // buckets lets `dop` workers sort and evaluate with no coordination.
        for (self.spec_indices, 0..) |si, spec_i| {
            if (si.partition_cols.len > 0 and n >= parallel_min_rows and self.dop > 1) {
                try self.evaluateSpecParallel(si, spec_i, n);
                continue;
            }
            const perm = try self.buildPermutation(si);
            defer self.allocator.free(perm);
            const _et = if (exec.prof.enabled) exec.prof.nowTicks() else 0;
            for (self.call_plans, 0..) |plan, ci| {
                if (plan.spec_idx != spec_i) continue;
                try self.evaluateCall(plan, ci, perm, si);
            }
            if (exec.prof.enabled) exec.prof.addPhase("window.eval", @intCast(@max(0, exec.prof.nowTicks() - _et)));
        }

        self.drained = true;
    }

    /// Below this many rows the parallel path's scatter + spawn overhead
    /// outweighs the sort it splits.
    const parallel_min_rows: usize = 65536;

    /// Partition-bucket parallel sort + evaluation for one spec:
    ///   A (parallel) fill composite keys per row range + count rows per
    ///     (worker, bucket) — bucket = partition-digest low bits, so a
    ///     partition never spans buckets;
    ///   B (serial) exclusive prefix over the counts → per-worker write
    ///     cursors + bucket extents;
    ///   C (parallel) place pairs bucket-major;
    ///   D (parallel) workers claim buckets: sort the bucket, write its
    ///     perm slice, walk its partitions, evaluate every call of the
    ///     spec. Output cells land at original row indices — disjoint
    ///     across buckets (validity bit bytes excepted; those writes are
    ///     atomic RMW in setValid/setNull).
    fn evaluateSpecParallel(self: *Window, si: SpecIndices, spec_i: usize, n: usize) !void {
        const workers: usize = @min(@max(self.dop, 2), 32);
        const bucket_count: usize = @min(@as(usize, 256), std.math.ceilPowerOfTwoAssert(usize, workers * 4));

        const acc = self.upstream.accountant();
        const scratch_bytes = n * (2 * @sizeOf(KeyIdx) + @sizeOf(u32));
        if (acc) |a| try a.reserve(.window, scratch_bytes);
        defer if (acc) |a| a.release(.window, scratch_bytes);

        const pairs = try self.allocator.alloc(KeyIdx, n);
        defer self.allocator.free(pairs);
        const placed = try self.allocator.alloc(KeyIdx, n);
        defer self.allocator.free(placed);
        const perm = try self.allocator.alloc(u32, n);
        defer self.allocator.free(perm);
        const counts = try self.allocator.alloc(usize, workers * bucket_count);
        defer self.allocator.free(counts);
        @memset(counts, 0);
        const bucket_offsets = try self.allocator.alloc(usize, bucket_count + 1);
        defer self.allocator.free(bucket_offsets);

        var job = SpecParJob{
            .win = self,
            .si = si,
            .spec_i = spec_i,
            .n = n,
            .workers = workers,
            .bucket_count = bucket_count,
            .pairs = pairs,
            .placed = placed,
            .perm = perm,
            .counts = counts,
            .bucket_offsets = bucket_offsets,
        };

        const _kt = if (exec.prof.enabled) exec.prof.nowTicks() else 0;
        runRangePhase(&job, SpecParJob.phaseKeys);
        if (exec.prof.enabled) exec.prof.addPhase("window.par_keys", @intCast(@max(0, exec.prof.nowTicks() - _kt)));

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

        const _pt = if (exec.prof.enabled) exec.prof.nowTicks() else 0;
        runRangePhase(&job, SpecParJob.phasePlace);
        const _bt = if (exec.prof.enabled) exec.prof.nowTicks() else 0;
        if (exec.prof.enabled) exec.prof.addPhase("window.par_place", @intCast(@max(0, _bt - _pt)));
        runBucketPhase(&job);
        if (exec.prof.enabled) exec.prof.addPhase("window.par_sort_eval", @intCast(@max(0, exec.prof.nowTicks() - _bt)));

        if (job.err) |e| return e;
    }

    const SpecParJob = struct {
        win: *Window,
        si: SpecIndices,
        spec_i: usize,
        n: usize,
        workers: usize,
        bucket_count: usize,
        pairs: []KeyIdx,
        placed: []KeyIdx,
        perm: []u32,
        /// Phase A: per-(worker, bucket) row counts; phase B turns them
        /// into per-worker write cursors for phase C.
        counts: []usize,
        bucket_offsets: []usize,
        next_bucket: std.atomic.Value(usize) = .init(0),
        failed: std.atomic.Value(bool) = .init(false),
        err_mutex: std.atomic.Mutex = .unlocked,
        err: ?anyerror = null,

        fn range(self: *const SpecParJob, w: usize) struct { lo: usize, hi: usize } {
            const chunk = (self.n + self.workers - 1) / self.workers;
            const lo = @min(w * chunk, self.n);
            return .{ .lo = lo, .hi = @min(lo + chunk, self.n) };
        }

        fn phaseKeys(self: *SpecParJob, w: usize) void {
            const r = self.range(w);
            self.win.fillKeys(self.si, self.pairs, r.lo, r.hi);
            const my = self.counts[w * self.bucket_count ..][0..self.bucket_count];
            const mask: u64 = @intCast(self.bucket_count - 1);
            for (self.pairs[r.lo..r.hi]) |kp| my[@intCast(kp.hi & mask)] += 1;
        }

        fn phasePlace(self: *SpecParJob, w: usize) void {
            const r = self.range(w);
            const my = self.counts[w * self.bucket_count ..][0..self.bucket_count];
            const mask: u64 = @intCast(self.bucket_count - 1);
            for (self.pairs[r.lo..r.hi]) |kp| {
                const b: usize = @intCast(kp.hi & mask);
                self.placed[my[b]] = kp;
                my[b] += 1;
            }
        }

        fn phaseBuckets(self: *SpecParJob) void {
            const spec = self.win.specs[self.spec_i];
            while (!self.failed.load(.acquire)) {
                const b = self.next_bucket.fetchAdd(1, .monotonic);
                if (b >= self.bucket_count) return;
                const lo = self.bucket_offsets[b];
                const hi = self.bucket_offsets[b + 1];
                if (lo == hi) continue;
                const slice = self.placed[lo..hi];
                std.sort.pdq(KeyIdx, slice, self.win.sortCtx(self.si), SpecSortCtx.pairLess);
                for (self.perm[lo..hi], slice) |*p, kp| p.* = kp.idx;
                var p_start = lo;
                while (p_start < hi) {
                    const p_end = partitionEnd(self.win.accumulated, self.si.partition_cols, self.perm[0..hi], p_start);
                    for (self.win.call_plans, 0..) |plan, ci| {
                        if (plan.spec_idx != self.spec_i) continue;
                        const cell: OutCell = .{
                            .column = &self.win.output_columns[ci],
                            .string_scratch = if (self.win.string_outputs[ci].len > 0) self.win.string_outputs[ci] else null,
                        };
                        self.win.evaluateOnePartition(plan, spec, self.si, self.perm[0..hi], p_start, p_end, cell) catch |e| return self.fail(e);
                    }
                    p_start = p_end;
                }
            }
        }

        fn fail(self: *SpecParJob, e: anyerror) void {
            while (!self.err_mutex.tryLock()) std.atomic.spinLoopHint();
            if (self.err == null) self.err = e;
            self.err_mutex.unlock();
            self.failed.store(true, .release);
        }
    };

    /// Run a per-worker-range phase on `workers` lanes: spawn workers-1
    /// threads, run the rest inline (covers spawn failure by absorbing the
    /// unspawned ranges), join.
    fn runRangePhase(job: *SpecParJob, comptime phase: fn (*SpecParJob, usize) void) void {
        var threads: [32]std.Thread = undefined;
        var spawned: usize = 0;
        while (spawned < job.workers - 1) {
            threads[spawned] = std.Thread.spawn(.{}, phase, .{ job, spawned }) catch break;
            spawned += 1;
        }
        var w = spawned;
        while (w < job.workers) : (w += 1) phase(job, w);
        for (threads[0..spawned]) |t| t.join();
    }

    fn runBucketPhase(job: *SpecParJob) void {
        var threads: [32]std.Thread = undefined;
        var spawned: usize = 0;
        while (spawned < job.workers - 1) {
            threads[spawned] = std.Thread.spawn(.{}, SpecParJob.phaseBuckets, .{job}) catch break;
            spawned += 1;
        }
        SpecParJob.phaseBuckets(job);
        for (threads[0..spawned]) |t| t.join();
    }

    /// One row's precomputed composite sort key + original row index. The
    /// key is an order-CONSISTENT prefix of the spec's full (partition,
    /// order) comparison — it may tie where the real comparator wouldn't,
    /// but it never contradicts it. `hi` is the partition-key digest (any
    /// consistent partition order is valid — partitions are independent —
    /// but a digest tie falls back to comparing the real partition columns
    /// FIRST, so two digest-colliding partitions can never interleave).
    /// `lo` is the order-normalized first ORDER BY column. Most comparisons
    /// resolve on the two integers; ties run the exact column chain with an
    /// arrival-order tiebreak (deterministic output under unstable pdq).
    const KeyIdx = struct { hi: u64, lo: u64, idx: u32 };

    const SpecSortCtx = struct {
        cols: []const ColumnStore,
        part: []const usize,
        order: []const usize,
        desc: []const bool,

        pub fn pairLess(ctx: @This(), a: KeyIdx, b: KeyIdx) bool {
            if (a.hi != b.hi) return a.hi < b.hi;
            for (ctx.part) |ci| {
                const ord = transform.compareInColumn(ctx.cols[ci], a.idx, b.idx);
                if (ord == .lt) return true;
                if (ord == .gt) return false;
            }
            if (a.lo != b.lo) return a.lo < b.lo;
            for (ctx.order, 0..) |ci, i| {
                const ord = transform.compareInColumn(ctx.cols[ci], a.idx, b.idx);
                if (ord == .lt) return !ctx.desc[i];
                if (ord == .gt) return ctx.desc[i];
            }
            return a.idx < b.idx;
        }
    };

    fn sortCtx(self: *Window, si: SpecIndices) SpecSortCtx {
        return .{
            .cols = self.accumulated,
            .part = si.partition_cols,
            .order = si.order_cols,
            .desc = si.order_desc,
        };
    }

    /// Fill `pairs[lo..hi]` with each row's composite key. Free function of
    /// row ranges so the parallel path can shard it across workers.
    fn fillKeys(self: *Window, si: SpecIndices, pairs: []KeyIdx, lo: usize, hi: usize) void {
        const has_part = si.partition_cols.len > 0;
        const order0: ?usize = if (si.order_cols.len > 0) si.order_cols[0] else null;
        var i = lo;
        while (i < hi) : (i += 1) {
            const row: u32 = @intCast(i);
            var khi: u64 = 0;
            var klo: u64 = 0;
            if (has_part) {
                var h = std.hash.Wyhash.init(0x9e3779b97f4a7c15);
                for (si.partition_cols) |ci| digestCell(&h, self.accumulated[ci], row);
                khi = h.final();
                if (order0) |oc| klo = orderPrefix(self.accumulated[oc], row, si.order_desc[0]);
            } else if (order0) |oc| {
                khi = orderPrefix(self.accumulated[oc], row, si.order_desc[0]);
            }
            pairs[i] = .{ .hi = khi, .lo = klo, .idx = row };
        }
    }

    fn buildPermutation(self: *Window, si: SpecIndices) ![]u32 {
        const n: usize = @intCast(self.accumulated_rows);
        const perm = try self.allocator.alloc(u32, n);
        errdefer self.allocator.free(perm);
        const pairs = try self.allocator.alloc(KeyIdx, n);
        defer self.allocator.free(pairs);
        const _kt = if (exec.prof.enabled) exec.prof.nowTicks() else 0;
        self.fillKeys(si, pairs, 0, n);
        const _st = if (exec.prof.enabled) exec.prof.nowTicks() else 0;
        if (exec.prof.enabled) exec.prof.addPhase("window.keys", @intCast(@max(0, _st - _kt)));
        std.sort.pdq(KeyIdx, pairs, self.sortCtx(si), SpecSortCtx.pairLess);
        if (exec.prof.enabled) exec.prof.addPhase("window.sort", @intCast(@max(0, exec.prof.nowTicks() - _st)));
        for (perm, pairs) |*p, kp| p.* = kp.idx;
        return perm;
    }

    fn evaluateCall(
        self: *Window,
        plan: CallPlan,
        out_idx: usize,
        perm: []const u32,
        si: SpecIndices,
    ) !void {
        const spec = self.specs[plan.spec_idx];
        const cell: OutCell = .{
            .column = &self.output_columns[out_idx],
            .string_scratch = if (self.string_outputs[out_idx].len > 0) self.string_outputs[out_idx] else null,
        };

        // Walk partitions (runs of equal partition_by values in perm).
        var p_start: usize = 0;
        while (p_start < perm.len) {
            const p_end = partitionEnd(self.accumulated, si.partition_cols, perm, p_start);
            try self.evaluateOnePartition(plan, spec, si, perm, p_start, p_end, cell);
            p_start = p_end;
        }
    }

    fn evaluateOnePartition(
        self: *Window,
        plan: CallPlan,
        spec: ir.WindowSpec,
        si: SpecIndices,
        perm: []const u32,
        p_start: usize,
        p_end: usize,
        cell: OutCell,
    ) !void {
        switch (plan.func) {
            .row_number => try fillRowNumber(perm, p_start, p_end, cell.column),
            .rank => try fillRank(self.accumulated, si.order_cols, perm, p_start, p_end, cell.column, false),
            .dense_rank => try fillRank(self.accumulated, si.order_cols, perm, p_start, p_end, cell.column, true),
            .lag => try self.fillLagLead(plan, perm, p_start, p_end, cell, true),
            .lead => try self.fillLagLead(plan, perm, p_start, p_end, cell, false),
            .first_value => try fillFirstValue(self.accumulated[plan.value_col], perm, p_start, p_end, cell, plan.ignore_nulls),
            .last_value => try self.fillLastValue(plan, spec, perm, p_start, p_end, cell),
            .nth_value => try self.fillNthValue(plan, spec, perm, p_start, p_end, cell),
            .ntile => try fillNtile(plan, perm, p_start, p_end, cell.column),
            .percent_rank => try fillPercentRank(self.accumulated, si.order_cols, perm, p_start, p_end, cell.column),
            .cume_dist => try fillCumeDist(self.accumulated, si.order_cols, perm, p_start, p_end, cell.column),
            .sum, .avg, .count, .min, .max => try self.fillAggregate(plan, spec, perm, p_start, p_end, cell),
        }
    }

    fn fillLagLead(
        self: *Window,
        plan: CallPlan,
        perm: []const u32,
        p_start: usize,
        p_end: usize,
        cell: OutCell,
        is_lag: bool,
    ) !void {
        const offset: i64 = plan.offset;
        const value_col = self.accumulated[plan.value_col];
        const view = value_col.view();
        var i: usize = p_start;
        while (i < p_end) : (i += 1) {
            const orig = perm[i];
            const target_idx: ?usize = if (plan.ignore_nulls)
                findNthNonNull(view, perm, i, p_start, p_end, offset, is_lag)
            else
                directOffset(i, p_start, p_end, offset, is_lag);
            if (target_idx) |t| {
                try copyCellTo(value_col, perm[t], cell, orig);
            } else {
                switch (plan.default_kind) {
                    .none => setNullCell(cell, orig),
                    .literal => try writeLiteralCell(cell, orig, plan.default_literal),
                    .col_ref => {
                        const def_col = self.accumulated[plan.default_col];
                        try copyCellTo(def_col, orig, cell, orig);
                    },
                }
            }
        }
    }

    fn fillNthValue(
        self: *Window,
        plan: CallPlan,
        spec: ir.WindowSpec,
        perm: []const u32,
        p_start: usize,
        p_end: usize,
        cell: OutCell,
    ) !void {
        const value_col = self.accumulated[plan.value_col];
        const view = value_col.view();
        const n: i64 = plan.nth_offset;
        var i: usize = p_start;
        while (i < p_end) : (i += 1) {
            const orig = perm[i];
            const fb = computeFrameBounds(spec.frame, i, p_start, p_end);
            const fs: i64 = @max(fb.start, @as(i64, @intCast(p_start)));
            const fe: i64 = @min(fb.end_inclusive, @as(i64, @intCast(p_end - 1)));
            if (fe < fs) {
                setNullCell(cell, orig);
                continue;
            }
            const src_idx: ?usize = if (plan.ignore_nulls)
                nthNonNullInRange(view, perm, @intCast(fs), @intCast(fe), n)
            else blk: {
                // 1-based offset → absolute index in perm.
                const target: i64 = fs + (n - 1);
                if (target > fe) break :blk null;
                break :blk @as(?usize, @intCast(target));
            };
            if (src_idx) |idx| {
                try copyCellTo(value_col, perm[idx], cell, orig);
            } else {
                setNullCell(cell, orig);
            }
        }
    }

    fn fillLastValue(
        self: *Window,
        plan: CallPlan,
        spec: ir.WindowSpec,
        perm: []const u32,
        p_start: usize,
        p_end: usize,
        cell: OutCell,
    ) !void {
        const value_col = self.accumulated[plan.value_col];
        const view = value_col.view();
        var i: usize = p_start;
        while (i < p_end) : (i += 1) {
            const orig = perm[i];
            const fb = computeFrameBounds(spec.frame, i, p_start, p_end);
            const frame_end_clamped: i64 = @min(fb.end_inclusive, @as(i64, @intCast(p_end - 1)));
            const frame_start_clamped: i64 = @max(fb.start, @as(i64, @intCast(p_start)));
            if (frame_end_clamped < frame_start_clamped) {
                setNullCell(cell, orig);
                continue;
            }
            const src_idx: ?usize = if (plan.ignore_nulls)
                lastNonNullInRange(view, perm, @intCast(frame_start_clamped), @intCast(frame_end_clamped))
            else
                @as(usize, @intCast(frame_end_clamped));
            if (src_idx) |idx| {
                try copyCellTo(value_col, perm[idx], cell, orig);
            } else {
                setNullCell(cell, orig);
            }
        }
    }

    fn fillAggregate(
        self: *Window,
        plan: CallPlan,
        spec: ir.WindowSpec,
        perm: []const u32,
        p_start: usize,
        p_end: usize,
        cell: OutCell,
    ) !void {
        const shape = classifyFrame(spec.frame);
        switch (shape) {
            .whole_partition => return self.fillAggregateWholePartition(plan, perm, p_start, p_end, cell),
            .prefix_to_current => return self.fillAggregatePrefix(plan, perm, p_start, p_end, cell),
            .general => {},
        }

        var i: usize = p_start;
        while (i < p_end) : (i += 1) {
            const orig = perm[i];
            const fb = computeFrameBounds(spec.frame, i, p_start, p_end);
            const lo_i: i64 = @max(fb.start, @as(i64, @intCast(p_start)));
            const hi_i: i64 = @min(fb.end_inclusive, @as(i64, @intCast(p_end - 1)));
            if (lo_i > hi_i) {
                switch (plan.func) {
                    .count => try writeBigint(cell.column, orig, 0),
                    else => setNullCell(cell, orig),
                }
                continue;
            }
            try self.evalAggOverFrame(plan, perm, @intCast(lo_i), @intCast(hi_i), cell, orig);
        }
    }

    /// Whole-partition aggregate fast path: compute once, broadcast to
    /// every row's original position. Reduces aggregate cost from
    /// O(N×N) to O(N) per partition.
    fn fillAggregateWholePartition(
        self: *Window,
        plan: CallPlan,
        perm: []const u32,
        p_start: usize,
        p_end: usize,
        cell: OutCell,
    ) !void {
        if (p_start >= p_end) return;
        try self.evalAggOverFrame(plan, perm, p_start, p_end - 1, cell, perm[p_start]);
        var i: usize = p_start + 1;
        while (i < p_end) : (i += 1) {
            try broadcastOutputCell(cell, perm[p_start], perm[i]);
        }
    }

    /// Prefix / running aggregate fast path: single forward sweep,
    /// maintaining a running accumulator. Covers the SQL-default frame
    /// with ORDER BY (`UNBOUNDED PRECEDING TO CURRENT ROW`).
    fn fillAggregatePrefix(
        self: *Window,
        plan: CallPlan,
        perm: []const u32,
        p_start: usize,
        p_end: usize,
        cell: OutCell,
    ) !void {
        if (p_start >= p_end) return;
        switch (plan.func) {
            .count => try self.prefixCount(plan, perm, p_start, p_end, cell.column),
            .sum => try self.prefixSum(plan, perm, p_start, p_end, cell.column),
            .avg => try self.prefixAvg(plan, perm, p_start, p_end, cell.column),
            .min, .max => try self.prefixMinMax(plan, perm, p_start, p_end, cell, plan.func == .min),
            else => unreachable,
        }
    }

    fn prefixCount(
        self: *Window,
        plan: CallPlan,
        perm: []const u32,
        p_start: usize,
        p_end: usize,
        out: *ColumnStore,
    ) !void {
        var running: i64 = 0;
        var i: usize = p_start;
        if (plan.count_star) {
            while (i < p_end) : (i += 1) {
                running += 1;
                try writeBigint(out, perm[i], running);
            }
            return;
        }
        const view = self.accumulated[plan.value_col].view();
        while (i < p_end) : (i += 1) {
            if (isValid(view, perm[i])) running += 1;
            try writeBigint(out, perm[i], running);
        }
    }

    fn prefixSum(
        self: *Window,
        plan: CallPlan,
        perm: []const u32,
        p_start: usize,
        p_end: usize,
        out: *ColumnStore,
    ) !void {
        const col = self.accumulated[plan.value_col];
        const view = col.view();
        switch (col.data) {
            .int, .bigint, .tinyint, .smallint, .largeint => {
                var sum: i128 = 0;
                var saw_any = false;
                var i: usize = p_start;
                while (i < p_end) : (i += 1) {
                    if (isValid(view, perm[i])) {
                        sum += readIntAsI128(col, perm[i]);
                        saw_any = true;
                    }
                    if (saw_any) {
                        switch (out.data) {
                            .bigint => try writeBigint(out, perm[i], @intCast(sum)),
                            .largeint => try writeLargeint(out, perm[i], sum),
                            else => return Error.WindowUnsupported,
                        }
                    } else setNull(out, perm[i]);
                }
            },
            .float, .double => {
                var sum: f64 = 0;
                var saw_any = false;
                var i: usize = p_start;
                while (i < p_end) : (i += 1) {
                    if (isValid(view, perm[i])) {
                        sum += readFloatAsF64(col, perm[i]);
                        saw_any = true;
                    }
                    if (saw_any) try writeDouble(out, perm[i], sum) else setNull(out, perm[i]);
                }
            },
            else => return Error.WindowUnsupported,
        }
    }

    fn prefixAvg(
        self: *Window,
        plan: CallPlan,
        perm: []const u32,
        p_start: usize,
        p_end: usize,
        out: *ColumnStore,
    ) !void {
        const col = self.accumulated[plan.value_col];
        const view = col.view();
        var sum: f64 = 0;
        var n: i64 = 0;
        var i: usize = p_start;
        while (i < p_end) : (i += 1) {
            if (isValid(view, perm[i])) {
                sum += readNumericAsF64(col, perm[i]);
                n += 1;
            }
            if (n == 0) setNull(out, perm[i]) else try writeDouble(out, perm[i], sum / @as(f64, @floatFromInt(n)));
        }
    }

    fn prefixMinMax(
        self: *Window,
        plan: CallPlan,
        perm: []const u32,
        p_start: usize,
        p_end: usize,
        cell: OutCell,
        is_min: bool,
    ) !void {
        const col = self.accumulated[plan.value_col];
        const view = col.view();
        var best_idx: i64 = -1;
        var i: usize = p_start;
        while (i < p_end) : (i += 1) {
            const r = perm[i];
            if (isValid(view, r)) {
                if (best_idx < 0) {
                    best_idx = @intCast(r);
                } else {
                    const ord = transform.compareInColumn(col, @intCast(best_idx), r);
                    const replace = if (is_min) (ord == .gt) else (ord == .lt);
                    if (replace) best_idx = @intCast(r);
                }
            }
            if (best_idx < 0) {
                setNullCell(cell, r);
            } else {
                try copyCellTo(col, @intCast(best_idx), cell, r);
            }
        }
    }

    /// Naive per-row scan over the frame [lo, hi] inclusive.
    /// Tier 1 — O(N * frame_width). Sliding-window state is a Tier-2
    /// optimization (#145 SIMD pass or its own task).
    fn evalAggOverFrame(
        self: *Window,
        plan: CallPlan,
        perm: []const u32,
        lo: usize,
        hi: usize,
        cell: OutCell,
        out_idx: u32,
    ) !void {
        switch (plan.func) {
            .count => {
                var n: i64 = 0;
                if (plan.count_star) {
                    n = @intCast(hi + 1 - lo);
                } else {
                    const view = self.accumulated[plan.value_col].view();
                    var k: usize = lo;
                    while (k <= hi) : (k += 1) {
                        if (isValid(view, perm[k])) n += 1;
                    }
                }
                try writeBigint(cell.column, out_idx, n);
            },
            .sum => try self.frameSum(plan, perm, lo, hi, cell.column, out_idx),
            .avg => try self.frameAvg(plan, perm, lo, hi, cell.column, out_idx),
            .min => try self.frameMinMax(plan, perm, lo, hi, cell, out_idx, true),
            .max => try self.frameMinMax(plan, perm, lo, hi, cell, out_idx, false),
            else => return Error.WindowUnsupported,
        }
    }

    fn frameSum(
        self: *Window,
        plan: CallPlan,
        perm: []const u32,
        lo: usize,
        hi: usize,
        out: *ColumnStore,
        out_idx: u32,
    ) !void {
        const col = self.accumulated[plan.value_col];
        const view = col.view();
        var saw_value = false;
        switch (col.data) {
            .int, .bigint, .tinyint, .smallint, .largeint => {
                var sum: i128 = 0;
                var k: usize = lo;
                while (k <= hi) : (k += 1) {
                    const r = perm[k];
                    if (!isValid(view, r)) continue;
                    saw_value = true;
                    sum += readIntAsI128(col, r);
                }
                if (!saw_value) {
                    setNull(out, out_idx);
                } else {
                    // SUM widens to bigint by default in this v1; for
                    // largeint inputs we widen to largeint.
                    switch (out.data) {
                        .bigint => try writeBigint(out, out_idx, @intCast(sum)),
                        .largeint => try writeLargeint(out, out_idx, sum),
                        else => return Error.WindowUnsupported,
                    }
                }
            },
            .float, .double => {
                var sum: f64 = 0;
                var k: usize = lo;
                while (k <= hi) : (k += 1) {
                    const r = perm[k];
                    if (!isValid(view, r)) continue;
                    saw_value = true;
                    sum += readFloatAsF64(col, r);
                }
                if (!saw_value) setNull(out, out_idx) else try writeDouble(out, out_idx, sum);
            },
            else => return Error.WindowUnsupported,
        }
    }

    fn frameAvg(
        self: *Window,
        plan: CallPlan,
        perm: []const u32,
        lo: usize,
        hi: usize,
        out: *ColumnStore,
        out_idx: u32,
    ) !void {
        const col = self.accumulated[plan.value_col];
        const view = col.view();
        var sum: f64 = 0;
        var n: i64 = 0;
        switch (col.data) {
            .int, .bigint, .tinyint, .smallint, .largeint, .float, .double => {
                var k: usize = lo;
                while (k <= hi) : (k += 1) {
                    const r = perm[k];
                    if (!isValid(view, r)) continue;
                    sum += readNumericAsF64(col, r);
                    n += 1;
                }
            },
            else => return Error.WindowUnsupported,
        }
        if (n == 0) setNull(out, out_idx) else try writeDouble(out, out_idx, sum / @as(f64, @floatFromInt(n)));
    }

    fn frameMinMax(
        self: *Window,
        plan: CallPlan,
        perm: []const u32,
        lo: usize,
        hi: usize,
        cell: OutCell,
        out_idx: u32,
        is_min: bool,
    ) !void {
        const col = self.accumulated[plan.value_col];
        const view = col.view();
        var best_idx: i64 = -1;
        var k: usize = lo;
        while (k <= hi) : (k += 1) {
            const r = perm[k];
            if (!isValid(view, r)) continue;
            if (best_idx < 0) {
                best_idx = @intCast(r);
                continue;
            }
            const ord = transform.compareInColumn(col, @intCast(best_idx), r);
            const replace = if (is_min) (ord == .gt) else (ord == .lt);
            if (replace) best_idx = @intCast(r);
        }
        if (best_idx < 0) {
            setNullCell(cell, out_idx);
        } else {
            try copyCellTo(col, @intCast(best_idx), cell, out_idx);
        }
    }
};

// ---------------------------------------------------------------------------
// Free helpers — no Window methods, used internally.
// ---------------------------------------------------------------------------

fn buildCallPlan(c: ir.WindowCall, schema: []const Column) !Window.CallPlan {
    var plan: Window.CallPlan = .{
        .spec_idx = c.spec_idx,
        .func = c.func,
        .ignore_nulls = c.ignore_nulls,
    };
    switch (c.func) {
        .row_number, .rank, .dense_rank => {},
        .lag, .lead => {
            // args: expr, [offset], [default]
            if (c.args.len < 1) return Error.WindowUnsupported;
            plan.value_col = try exprColIdx(c.args[0], schema);
            if (c.args.len >= 2) {
                plan.offset = try exprIntLiteral(c.args[1]);
                if (plan.offset < 0) return Error.WindowUnsupported;
            }
            if (c.args.len >= 3) {
                switch (c.args[2]) {
                    .col_ref => |name| {
                        plan.default_kind = .col_ref;
                        plan.default_col = lookupCol(schema, name) orelse return Error.ColumnNotFound;
                    },
                    .lit => |v| {
                        plan.default_kind = .literal;
                        plan.default_literal = v;
                    },
                    .null_lit, .call, .case, .scalar_subquery, .exists_subquery, .var_ref => return Error.WindowUnsupported,
                }
            }
        },
        .first_value, .last_value => {
            plan.value_col = try exprColIdx(c.args[0], schema);
        },
        .nth_value => {
            plan.value_col = try exprColIdx(c.args[0], schema);
            plan.nth_offset = try exprIntLiteral(c.args[1]);
            if (plan.nth_offset < 1) return Error.WindowUnsupported;
        },
        .ntile => {
            plan.ntile_buckets = try exprIntLiteral(c.args[0]);
            if (plan.ntile_buckets < 1) return Error.WindowUnsupported;
        },
        .cume_dist, .percent_rank => {},
        .sum, .avg, .count, .min, .max => {
            switch (c.args[0]) {
                .col_ref => |name| {
                    if (std.mem.eql(u8, name, "*")) {
                        if (c.func != .count) return Error.WindowUnsupported;
                        plan.count_star = true;
                    } else {
                        plan.value_col = lookupCol(schema, name) orelse return Error.ColumnNotFound;
                    }
                },
                else => return Error.WindowUnsupported,
            }
        },
    }
    return plan;
}

fn outputType(c: ir.WindowCall, plan: Window.CallPlan, schema: []const Column) !Type {
    return switch (c.func) {
        .row_number, .rank, .dense_rank => .bigint,
        .count => .bigint,
        .sum => blk: {
            const t = schema[plan.value_col].type;
            switch (t) {
                .int, .bigint, .tinyint, .smallint => break :blk Type{ .bigint = {} },
                .largeint => break :blk Type{ .largeint = {} },
                .float, .double => break :blk Type{ .double = {} },
                else => return Error.WindowUnsupported,
            }
        },
        .avg => Type{ .double = {} },
        .ntile => .bigint,
        .percent_rank, .cume_dist => Type{ .double = {} },
        // String input is supported here — the operator writes into a
        // separate `string_outputs` scratch (see Window.string_outputs).
        .min, .max, .lag, .lead, .first_value, .last_value, .nth_value => schema[plan.value_col].type,
    };
}

fn exprColIdx(e: ir.Expr, schema: []const Column) !usize {
    return switch (e) {
        .col_ref => |name| lookupCol(schema, name) orelse return Error.ColumnNotFound,
        else => Error.WindowUnsupported,
    };
}

fn exprIntLiteral(e: ir.Expr) !i64 {
    return switch (e) {
        .lit => |v| switch (v) {
            .int => |x| x,
            .bigint => |x| x,
            .smallint => |x| x,
            .tinyint => |x| x,
            else => Error.WindowUnsupported,
        },
        else => Error.WindowUnsupported,
    };
}

fn lookupCol(schema: []const Column, name: []const u8) ?usize {
    return types.findColumn(schema, name);
}

fn freeSpecIndices(allocator: Allocator, specs: []Window.SpecIndices, n: usize) void {
    for (specs[0..n]) |si| {
        allocator.free(si.partition_cols);
        allocator.free(si.order_cols);
        allocator.free(si.order_desc);
    }
    allocator.free(specs);
}

/// Pre-size an output ColumnStore to `n` rows, initial state = all NULL
/// (zero data + zero-valid validity bitmap). Evaluators overwrite the
/// data + flip validity bits per row they fill.
fn preSizeColumn(allocator: Allocator, out: *ColumnStore, t: Type, n: usize) !void {
    out.clear();
    switch (out.data) {
        .int => |*l| try l.appendNTimes(allocator, 0, n),
        .bigint => |*l| try l.appendNTimes(allocator, 0, n),
        .boolean => |*l| try l.appendNTimes(allocator, 0, n),
        .tinyint => |*l| try l.appendNTimes(allocator, 0, n),
        .smallint => |*l| try l.appendNTimes(allocator, 0, n),
        .largeint => |*l| try l.appendNTimes(allocator, 0, n),
        .float => |*l| try l.appendNTimes(allocator, 0, n),
        .double => |*l| try l.appendNTimes(allocator, 0, n),
        .date => |*l| try l.appendNTimes(allocator, 0, n),
        .datetime => |*l| try l.appendNTimes(allocator, 0, n),
        .decimal64 => |*l| try l.appendNTimes(allocator, 0, n),
        .decimal128 => |*l| try l.appendNTimes(allocator, 0, n),
        .uuid => |*l| try l.appendNTimes(allocator, 0, n),
        .varchar, .string, .char => return Error.WindowUnsupported,
    }
    _ = t;
    if (out.nulls) |*nb| {
        const bytes_needed = (n + 7) / 8;
        try nb.appendNTimes(allocator, 0, bytes_needed); // 0 = NULL
    }
}

fn appendRangeFromStore(
    allocator: Allocator,
    src: ColumnStore,
    lo: usize,
    hi: usize,
    out: *ColumnStore,
) !void {
    const indices_buf = try allocator.alloc(u32, hi - lo);
    defer allocator.free(indices_buf);
    for (indices_buf, 0..) |*p, i| p.* = @intCast(lo + i);
    try transform.appendByIndices(allocator, src.view(), indices_buf, out);
}

/// Build a row range of a staged string-output ColumnStore from the
/// `[N]?[]const u8` scratch. Each scratch entry is either a slice into
/// an input column's StringStore (lifetime-safe — input columns live
/// as long as the operator) or `null` for SQL NULL.
fn appendStringScratchRange(
    allocator: Allocator,
    scratch: []const ?[]const u8,
    lo: usize,
    hi: usize,
    out: *ColumnStore,
) !void {
    var i: usize = lo;
    while (i < hi) : (i += 1) {
        const row: usize = i - lo;
        const bytes = scratch[i] orelse "";
        switch (out.data) {
            .string => |*ss| try ss.appendValue(allocator, bytes),
            .varchar => |*ss| try ss.appendValue(allocator, bytes),
            .char => |*ss| try ss.appendValue(allocator, bytes),
            else => return Error.WindowUnsupported,
        }
        try out.appendValidBit(allocator, row, scratch[i] != null);
    }
}

fn isStringType(t: Type) bool {
    return switch (t) {
        .string, .varchar, .char => true,
        else => false,
    };
}

/// A destination cell for a window-function output row write.
/// Either targets the pre-sized `ColumnStore` (fixed-width types) or
/// the `string_scratch` slice (string-typed outputs). The two are
/// mutually exclusive — exactly one of them is "live" for a given
/// output call.
const OutCell = struct {
    column: *ColumnStore,
    string_scratch: ?[]?[]const u8 = null,
};

fn setNullCell(cell: OutCell, row: u32) void {
    if (cell.string_scratch) |s| {
        s[row] = null;
        return;
    }
    setNull(cell.column, row);
}

fn copyCellTo(src: ColumnStore, src_row: u32, cell: OutCell, out_row: u32) !void {
    const view = src.view();
    if (!isValid(view, src_row)) {
        setNullCell(cell, out_row);
        return;
    }
    if (cell.string_scratch) |s| {
        s[out_row] = switch (src.data) {
            .string => |ss| ss.view().rowBytes(src_row),
            .varchar => |ss| ss.view().rowBytes(src_row),
            .char => |ss| ss.view().rowBytes(src_row),
            else => return Error.WindowUnsupported,
        };
        return;
    }
    try copyCell(src, src_row, cell.column, out_row);
}

/// Mirror an already-written output cell's value at `src_row` to
/// another row `dst_row`. Used by the whole-partition aggregate
/// broadcast path so we compute the aggregate once and copy across.
fn broadcastOutputCell(cell: OutCell, src_row: u32, dst_row: u32) !void {
    if (cell.string_scratch) |s| {
        s[dst_row] = s[src_row];
        return;
    }
    try copyCell(cell.column.*, src_row, cell.column, dst_row);
}

fn writeLiteralCell(cell: OutCell, row: u32, lit: Value) !void {
    if (cell.string_scratch) |s| {
        s[row] = switch (lit) {
            .text => |t| t,
            else => return Error.WindowUnsupported,
        };
        return;
    }
    try writeLiteral(cell.column, row, lit);
}

/// `perm` is sorted by (partition_by, order_by). Find the end of the
/// current partition starting at `start` — the smallest index `e` >
/// `start` such that the partition-key tuple at `perm[e]` differs from
/// `perm[start]` (or `perm.len` if the partition runs to the end).
fn partitionEnd(
    cols: []const ColumnStore,
    part_cols: []const usize,
    perm: []const u32,
    start: usize,
) usize {
    if (part_cols.len == 0) return perm.len;
    var e: usize = start + 1;
    const ref = perm[start];
    while (e < perm.len) : (e += 1) {
        for (part_cols) |ci| {
            if (transform.compareInColumn(cols[ci], ref, perm[e]) != .eq) return e;
        }
    }
    return e;
}

fn fillRowNumber(perm: []const u32, p_start: usize, p_end: usize, out: *ColumnStore) !void {
    var i: usize = p_start;
    var rn: i64 = 1;
    while (i < p_end) : (i += 1) {
        try writeBigint(out, perm[i], rn);
        rn += 1;
    }
}

fn fillRank(
    cols: []const ColumnStore,
    order_cols: []const usize,
    perm: []const u32,
    p_start: usize,
    p_end: usize,
    out: *ColumnStore,
    dense: bool,
) !void {
    var i: usize = p_start;
    var prev_rank: i64 = 1;
    while (i < p_end) : (i += 1) {
        const cur_rank = if (i == p_start) blk: {
            prev_rank = 1;
            break :blk 1;
        } else if (orderEquals(cols, order_cols, perm[i - 1], perm[i])) blk: {
            break :blk prev_rank;
        } else blk: {
            if (dense) {
                prev_rank += 1;
                break :blk prev_rank;
            } else {
                // RANK uses position-in-partition for the new group.
                break :blk @as(i64, @intCast(i - p_start)) + 1;
            }
        };
        try writeBigint(out, perm[i], cur_rank);
        prev_rank = cur_rank;
    }
}

fn orderEquals(
    cols: []const ColumnStore,
    order_cols: []const usize,
    a: u32,
    b: u32,
) bool {
    for (order_cols) |ci| {
        if (transform.compareInColumn(cols[ci], a, b) != .eq) return false;
    }
    return true;
}

fn fillFirstValue(
    value_col: ColumnStore,
    perm: []const u32,
    p_start: usize,
    p_end: usize,
    cell: OutCell,
    ignore_nulls: bool,
) !void {
    if (p_start >= p_end) return;
    const view = value_col.view();
    const first_src_idx: ?usize = if (ignore_nulls)
        firstNonNullInRange(view, perm, p_start, p_end - 1)
    else
        @as(?usize, p_start);
    var i: usize = p_start;
    while (i < p_end) : (i += 1) {
        if (first_src_idx) |idx| {
            try copyCellTo(value_col, perm[idx], cell, perm[i]);
        } else {
            setNullCell(cell, perm[i]);
        }
    }
}

/// Direct offset for non-IGNORE-NULLS LAG/LEAD: just `cur ± offset`,
/// clamped to partition bounds (returns null if out of range).
fn directOffset(cur: usize, p_start: usize, p_end: usize, offset: i64, is_lag: bool) ?usize {
    const target: i64 = if (is_lag)
        @as(i64, @intCast(cur)) - offset
    else
        @as(i64, @intCast(cur)) + offset;
    if (target < @as(i64, @intCast(p_start))) return null;
    if (target >= @as(i64, @intCast(p_end))) return null;
    return @intCast(target);
}

/// LAG/LEAD with IGNORE NULLS: walk backward (LAG) or forward (LEAD)
/// through the partition, counting only rows where the value column is
/// non-null. Return the perm index of the Nth non-null hit, or null
/// when the partition runs out before reaching it.
fn findNthNonNull(
    view: ColumnView,
    perm: []const u32,
    cur: usize,
    p_start: usize,
    p_end: usize,
    offset: i64,
    is_lag: bool,
) ?usize {
    var seen: i64 = 0;
    var k: i64 = @intCast(cur);
    while (true) {
        k += if (is_lag) -1 else 1;
        if (k < @as(i64, @intCast(p_start))) return null;
        if (k >= @as(i64, @intCast(p_end))) return null;
        if (isValid(view, perm[@intCast(k)])) {
            seen += 1;
            if (seen == offset) return @intCast(k);
        }
    }
}

/// First perm index in [lo, hi] (inclusive) whose value column is
/// non-null. Used by FIRST_VALUE IGNORE NULLS.
fn firstNonNullInRange(view: ColumnView, perm: []const u32, lo: usize, hi: usize) ?usize {
    var k: usize = lo;
    while (k <= hi) : (k += 1) {
        if (isValid(view, perm[k])) return k;
    }
    return null;
}

/// Last perm index in [lo, hi] (inclusive) whose value column is
/// non-null. Used by LAST_VALUE IGNORE NULLS.
fn lastNonNullInRange(view: ColumnView, perm: []const u32, lo: usize, hi: usize) ?usize {
    var k: i64 = @intCast(hi);
    while (k >= @as(i64, @intCast(lo))) : (k -= 1) {
        if (isValid(view, perm[@intCast(k)])) return @intCast(k);
    }
    return null;
}

/// Find the Nth (1-based) perm index in [lo, hi] whose value column
/// is non-null. Used by NTH_VALUE IGNORE NULLS.
fn nthNonNullInRange(view: ColumnView, perm: []const u32, lo: usize, hi: usize, n: i64) ?usize {
    var seen: i64 = 0;
    var k: usize = lo;
    while (k <= hi) : (k += 1) {
        if (isValid(view, perm[k])) {
            seen += 1;
            if (seen == n) return k;
        }
    }
    return null;
}

/// NTILE(n) — distribute the ordered partition into `n` buckets that
/// differ in size by at most one. The first `N % n` buckets are larger
/// (ceil) and contain rows 0..larger_zone-1; the remaining buckets
/// have floor(N/n) rows each.
fn fillNtile(
    plan: anytype,
    perm: []const u32,
    p_start: usize,
    p_end: usize,
    out: *ColumnStore,
) !void {
    const N: i64 = @intCast(p_end - p_start);
    const n: i64 = plan.ntile_buckets;
    if (N == 0) return;
    const small_size: i64 = @divTrunc(N, n);
    const large_count: i64 = @mod(N, n);
    const large_size: i64 = small_size + 1;
    const large_zone: i64 = large_count * large_size;
    var i: usize = p_start;
    while (i < p_end) : (i += 1) {
        const pos: i64 = @intCast(i - p_start);
        const bucket: i64 = if (pos < large_zone)
            @divTrunc(pos, large_size) + 1
        else
            // After the large zone, advance through the smaller buckets.
            large_count + @divTrunc(pos - large_zone, @max(small_size, 1)) + 1;
        try writeBigint(out, perm[i], bucket);
    }
}

/// PERCENT_RANK = (rank - 1) / (partition_size - 1) where `rank` is the
/// RANK() value (with-gaps). For a partition of size 1, returns 0.
fn fillPercentRank(
    cols: []const ColumnStore,
    order_cols: []const usize,
    perm: []const u32,
    p_start: usize,
    p_end: usize,
    out: *ColumnStore,
) !void {
    if (p_start >= p_end) return;
    const N: i64 = @intCast(p_end - p_start);
    if (N == 1) {
        try writeDouble(out, perm[p_start], 0);
        return;
    }
    const denom: f64 = @floatFromInt(N - 1);
    var rank_in_partition: i64 = 1;
    var i: usize = p_start;
    while (i < p_end) : (i += 1) {
        if (i != p_start and !orderEquals(cols, order_cols, perm[i - 1], perm[i])) {
            rank_in_partition = @intCast(i - p_start + 1);
        }
        const v: f64 = @as(f64, @floatFromInt(rank_in_partition - 1)) / denom;
        try writeDouble(out, perm[i], v);
    }
}

/// CUME_DIST = (count of rows in partition with order_by ≤ current's
/// order_by) / partition_size. Peer rows (equal order_by) share the
/// same cume_dist value.
fn fillCumeDist(
    cols: []const ColumnStore,
    order_cols: []const usize,
    perm: []const u32,
    p_start: usize,
    p_end: usize,
    out: *ColumnStore,
) !void {
    if (p_start >= p_end) return;
    const N: i64 = @intCast(p_end - p_start);
    const denom: f64 = @floatFromInt(N);
    var i: usize = p_start;
    while (i < p_end) {
        // Find the run of rows that are peers (equal on order_by).
        var j: usize = i + 1;
        while (j < p_end and orderEquals(cols, order_cols, perm[i], perm[j])) : (j += 1) {}
        // CUME_DIST counts rows-preceding-or-peer = j - p_start (the
        // count of rows whose order_by ≤ current's).
        const count: i64 = @intCast(j - p_start);
        const v: f64 = @as(f64, @floatFromInt(count)) / denom;
        var k: usize = i;
        while (k < j) : (k += 1) try writeDouble(out, perm[k], v);
        i = j;
    }
}

/// Compute the [start, end_inclusive] frame indices (in PERMUTATION
/// space) for the current row at perm[cur]. Indices are relative to
/// the whole perm array; callers clamp to partition bounds.
const FrameBounds = struct {
    start: i64,
    end_inclusive: i64,
};

fn computeFrameBounds(frame: ir.Frame, cur: usize, p_start: usize, p_end: usize) FrameBounds {
    const c: i64 = @intCast(cur);
    const ps: i64 = @intCast(p_start);
    const pe_inclusive: i64 = @as(i64, @intCast(p_end)) - 1;

    const start = boundToIndex(frame.start, c, ps, pe_inclusive, true);
    const end = boundToIndex(frame.end, c, ps, pe_inclusive, false);
    return .{ .start = start, .end_inclusive = end };
}

/// Classify the frame for aggregate fast-path selection.
const FrameShape = enum {
    /// UNBOUNDED PRECEDING TO UNBOUNDED FOLLOWING — single value per
    /// partition, broadcast to every row.
    whole_partition,
    /// UNBOUNDED PRECEDING TO CURRENT ROW — running aggregate, one
    /// forward sweep.
    prefix_to_current,
    /// Anything else (N PRECEDING / N FOLLOWING / sliding) falls back
    /// to the naive per-row scan.
    general,
};

fn classifyFrame(frame: ir.Frame) FrameShape {
    const start_is_unbounded = std.meta.activeTag(frame.start) == .unbounded_preceding;
    if (!start_is_unbounded) return .general;
    return switch (frame.end) {
        .unbounded_following => .whole_partition,
        .current_row => .prefix_to_current,
        else => .general,
    };
}

fn boundToIndex(b: ir.FrameBound, cur: i64, p_start: i64, p_end_inclusive: i64, is_start: bool) i64 {
    _ = is_start;
    return switch (b) {
        .unbounded_preceding => p_start,
        .preceding => |n| cur - @as(i64, @intCast(n)),
        .current_row => cur,
        .following => |n| cur + @as(i64, @intCast(n)),
        .unbounded_following => p_end_inclusive,
    };
}

// ---------------------------------------------------------------------------
// Cell-level read/write helpers
// ---------------------------------------------------------------------------

fn isValid(view: ColumnView, row: u32) bool {
    if (view.nulls) |nb| {
        const byte_idx = row >> 3;
        const bit: u3 = @intCast(row & 7);
        return (nb[byte_idx] & (@as(u8, 1) << bit)) != 0;
    }
    return true;
}

/// Map row `row` of an ORDER BY column to a u64 whose unsigned order never
/// CONTRADICTS `transform.compareInColumn` on that column: norm(a) < norm(b)
/// implies real(a) < real(b). Ties are allowed (string 8-byte prefixes,
/// i128 high halves) — the caller's fallback comparator resolves them.
/// Mirrors the comparator exactly: validity is NOT consulted (NULL slots
/// order by their raw placeholder values), floats use `floatOrder` (every
/// NaN equal-and-largest, -0 == +0). `desc` inverts the mapping.
fn orderPrefix(col: ColumnStore, row: u32, desc: bool) u64 {
    const SIGN64: u64 = 1 << 63;
    const norm: u64 = switch (col.data) {
        .int => |l| @as(u64, @bitCast(@as(i64, l.items[row]))) ^ SIGN64,
        .bigint => |l| @as(u64, @bitCast(l.items[row])) ^ SIGN64,
        .boolean => |l| @as(u64, l.items[row]),
        .tinyint => |l| @as(u64, @bitCast(@as(i64, l.items[row]))) ^ SIGN64,
        .smallint => |l| @as(u64, @bitCast(@as(i64, l.items[row]))) ^ SIGN64,
        .date => |l| @as(u64, @bitCast(@as(i64, l.items[row]))) ^ SIGN64,
        .datetime => |l| @as(u64, @bitCast(l.items[row])) ^ SIGN64,
        .decimal64 => |l| @as(u64, @bitCast(l.items[row])) ^ SIGN64,
        .largeint => |l| @truncate((@as(u128, @bitCast(l.items[row])) ^ (@as(u128, 1) << 127)) >> 64),
        .decimal128 => |l| @truncate((@as(u128, @bitCast(l.items[row])) ^ (@as(u128, 1) << 127)) >> 64),
        .uuid => |l| @truncate(l.items[row] >> 64),
        .float => |l| floatNorm(@as(f64, l.items[row])),
        .double => |l| floatNorm(l.items[row]),
        .varchar, .string, .char => |s| stringPrefix(s.rowBytesWide(row)),
    };
    return if (desc) ~norm else norm;
}

/// IEEE order-normalization matching `types.floatOrder`: every NaN maps to
/// the single largest code (floatOrder treats all NaNs as equal-largest),
/// -0.0 canonicalizes to +0.0 (they compare equal), everything else uses
/// the sign-fold bit trick.
fn floatNorm(v: f64) u64 {
    if (std.math.isNan(v)) return std.math.maxInt(u64);
    const c: f64 = if (v == 0) 0 else v;
    const bits: u64 = @bitCast(c);
    return if (bits & (1 << 63) != 0) ~bits else bits | (1 << 63);
}

/// First 8 bytes big-endian, zero-padded — preserves `std.mem.order` for
/// everything the prefix can see; equal prefixes fall back.
fn stringPrefix(bytes: []const u8) u64 {
    var k: u64 = 0;
    const n = @min(bytes.len, 8);
    for (bytes[0..n], 0..) |c, j| k |= @as(u64, c) << @intCast(56 - j * 8);
    return k;
}

/// Fold row `row` of a PARTITION BY column into a digest. Requirement is
/// equality-consistency only (equal values per `compareInColumn` ⇒ equal
/// digest): canonical widths for the int family, NaN/-0 canonicalized for
/// floats, length-prefixed bytes for strings (guards multi-column chains).
fn digestCell(h: *std.hash.Wyhash, col: ColumnStore, row: u32) void {
    switch (col.data) {
        .int => |l| hashInt(h, @as(i64, l.items[row])),
        .bigint => |l| hashInt(h, l.items[row]),
        .boolean => |l| hashInt(h, @as(i64, l.items[row])),
        .tinyint => |l| hashInt(h, @as(i64, l.items[row])),
        .smallint => |l| hashInt(h, @as(i64, l.items[row])),
        .date => |l| hashInt(h, @as(i64, l.items[row])),
        .datetime => |l| hashInt(h, l.items[row]),
        .decimal64 => |l| hashInt(h, l.items[row]),
        .largeint => |l| hashI128(h, l.items[row]),
        .decimal128 => |l| hashI128(h, l.items[row]),
        .uuid => |l| h.update(std.mem.asBytes(&l.items[row])),
        .float => |l| hashInt(h, @as(i64, @bitCast(floatNorm(@as(f64, l.items[row]))))),
        .double => |l| hashInt(h, @as(i64, @bitCast(floatNorm(l.items[row])))),
        .varchar, .string, .char => |s| {
            const bytes = s.rowBytesWide(row);
            var len: u64 = bytes.len;
            h.update(std.mem.asBytes(&len));
            h.update(bytes);
        },
    }
}

fn hashInt(h: *std.hash.Wyhash, v: i64) void {
    h.update(std.mem.asBytes(&v));
}

fn hashI128(h: *std.hash.Wyhash, v: i128) void {
    h.update(std.mem.asBytes(&v));
}

// Validity writes are atomic RMW: the parallel eval path has workers from
// different partition buckets setting bits that share a byte (8 rows/byte,
// bucket membership is hash-scattered over original row indices). Value
// slots are per-row distinct memory and need no synchronization. Uncontended
// lock-or is a few ns — noise next to the cell write it accompanies.
fn setNull(out: *ColumnStore, row: u32) void {
    const nb = if (out.nulls) |*n| n else return;
    const byte_idx = row >> 3;
    const bit: u3 = @intCast(row & 7);
    _ = @atomicRmw(u8, &nb.items[byte_idx], .And, ~(@as(u8, 1) << bit), .monotonic);
}

fn setValid(out: *ColumnStore, row: u32) void {
    const nb = if (out.nulls) |*n| n else return;
    const byte_idx = row >> 3;
    const bit: u3 = @intCast(row & 7);
    _ = @atomicRmw(u8, &nb.items[byte_idx], .Or, @as(u8, 1) << bit, .monotonic);
}

fn writeBigint(out: *ColumnStore, row: u32, v: i64) !void {
    switch (out.data) {
        .bigint => |*l| l.items[row] = v,
        else => return Error.WindowUnsupported,
    }
    setValid(out, row);
}

fn writeLargeint(out: *ColumnStore, row: u32, v: i128) !void {
    switch (out.data) {
        .largeint => |*l| l.items[row] = v,
        else => return Error.WindowUnsupported,
    }
    setValid(out, row);
}

fn writeDouble(out: *ColumnStore, row: u32, v: f64) !void {
    switch (out.data) {
        .double => |*l| l.items[row] = v,
        else => return Error.WindowUnsupported,
    }
    setValid(out, row);
}

fn writeLiteral(out: *ColumnStore, row: u32, lit: Value) !void {
    switch (out.data) {
        .int => |*l| l.items[row] = switch (lit) {
            .int => |x| x,
            .bigint => |x| @intCast(x),
            else => return Error.WindowUnsupported,
        },
        .bigint => |*l| l.items[row] = switch (lit) {
            .int => |x| x,
            .bigint => |x| x,
            else => return Error.WindowUnsupported,
        },
        .boolean => |*l| l.items[row] = switch (lit) {
            .boolean => |x| if (x) @as(u8, 1) else @as(u8, 0),
            else => return Error.WindowUnsupported,
        },
        .double => |*l| l.items[row] = switch (lit) {
            .double => |x| x,
            .float => |x| x,
            else => return Error.WindowUnsupported,
        },
        .float => |*l| l.items[row] = switch (lit) {
            .float => |x| x,
            else => return Error.WindowUnsupported,
        },
        else => return Error.WindowUnsupported,
    }
    setValid(out, row);
}

fn copyCell(src: ColumnStore, src_row: u32, out: *ColumnStore, out_row: u32) !void {
    const view = src.view();
    if (!isValid(view, src_row)) {
        setNull(out, out_row);
        return;
    }
    switch (src.data) {
        .int => |l| switch (out.data) {
            .int => |*o| o.items[out_row] = l.items[src_row],
            else => return Error.WindowUnsupported,
        },
        .bigint => |l| switch (out.data) {
            .bigint => |*o| o.items[out_row] = l.items[src_row],
            else => return Error.WindowUnsupported,
        },
        .tinyint => |l| switch (out.data) {
            .tinyint => |*o| o.items[out_row] = l.items[src_row],
            else => return Error.WindowUnsupported,
        },
        .smallint => |l| switch (out.data) {
            .smallint => |*o| o.items[out_row] = l.items[src_row],
            else => return Error.WindowUnsupported,
        },
        .largeint => |l| switch (out.data) {
            .largeint => |*o| o.items[out_row] = l.items[src_row],
            else => return Error.WindowUnsupported,
        },
        .float => |l| switch (out.data) {
            .float => |*o| o.items[out_row] = l.items[src_row],
            else => return Error.WindowUnsupported,
        },
        .double => |l| switch (out.data) {
            .double => |*o| o.items[out_row] = l.items[src_row],
            else => return Error.WindowUnsupported,
        },
        .boolean => |l| switch (out.data) {
            .boolean => |*o| o.items[out_row] = l.items[src_row],
            else => return Error.WindowUnsupported,
        },
        .date => |l| switch (out.data) {
            .date => |*o| o.items[out_row] = l.items[src_row],
            else => return Error.WindowUnsupported,
        },
        .datetime => |l| switch (out.data) {
            .datetime => |*o| o.items[out_row] = l.items[src_row],
            else => return Error.WindowUnsupported,
        },
        .decimal64 => |l| switch (out.data) {
            .decimal64 => |*o| o.items[out_row] = l.items[src_row],
            else => return Error.WindowUnsupported,
        },
        .decimal128 => |l| switch (out.data) {
            .decimal128 => |*o| o.items[out_row] = l.items[src_row],
            else => return Error.WindowUnsupported,
        },
        .uuid => |l| switch (out.data) {
            .uuid => |*o| o.items[out_row] = l.items[src_row],
            else => return Error.WindowUnsupported,
        },
        .varchar, .string, .char => return Error.WindowUnsupported,
    }
    setValid(out, out_row);
}

fn readIntAsI128(col: ColumnStore, row: u32) i128 {
    return switch (col.data) {
        .int => |l| @intCast(l.items[row]),
        .bigint => |l| @intCast(l.items[row]),
        .tinyint => |l| @intCast(l.items[row]),
        .smallint => |l| @intCast(l.items[row]),
        .largeint => |l| l.items[row],
        else => 0,
    };
}

fn readFloatAsF64(col: ColumnStore, row: u32) f64 {
    return switch (col.data) {
        .float => |l| @floatCast(l.items[row]),
        .double => |l| l.items[row],
        else => 0,
    };
}

fn readNumericAsF64(col: ColumnStore, row: u32) f64 {
    return switch (col.data) {
        .int => |l| @floatFromInt(l.items[row]),
        .bigint => |l| @floatFromInt(l.items[row]),
        .tinyint => |l| @floatFromInt(l.items[row]),
        .smallint => |l| @floatFromInt(l.items[row]),
        .largeint => |l| @floatFromInt(l.items[row]),
        .float => |l| @floatCast(l.items[row]),
        .double => |l| l.items[row],
        else => 0,
    };
}
