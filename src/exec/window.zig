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
const builtin = @import("builtin");
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
    /// spec_group[i] = the canonical (first) spec index with the same
    /// resolved sort keys (partition cols + order cols + directions).
    /// Specs differing only in FRAME share one sort/permutation: the
    /// canonical spec's pass evaluates every call of the whole group
    /// (each call still reads its own spec's frame).
    spec_group: []usize,
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
    accumulated: []ColumnStore, // input columns
    /// One arena per accumulated column, backing every allocation that
    /// column ever makes. Two jobs: (a) the parallel drain's workers append
    /// different columns concurrently, and per-column arenas make those
    /// allocations race-free without a shared-allocator lock; (b) evict()
    /// frees the whole input in one arena sweep per column. Backed by the
    /// (thread-safe) c_allocator normally; by the operator's own allocator
    /// under tests so std.testing.allocator still audits the memory (the
    /// drain is serial there).
    acc_arenas: []std.heap.ArenaAllocator,
    output_columns: []ColumnStore, // window outputs (fixed-width types)
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

    /// Ride-the-order (set by the staged compiler, cte_stages.rideSource):
    /// `assume_sorted` — the input provably arrives sorted on this window's
    /// (single) sort-key set, so the sort collapses to an identity
    /// permutation over key runs. `emit_sorted` — a downstream same-key
    /// consumer wants this window's ADOPTED stage in spec order: the
    /// canonical permutation is retained and adoption gathers by it.
    assume_sorted: bool = false,
    emit_sorted: bool = false,
    sorted_perm: ?[]u32 = null,
    /// Compile-proven grouping: the staged compiler PAIRED this window with
    /// an upstream same-partition window that emits in its sorted order
    /// through a grouping-preserving chain, so `groupedByPartition` holds
    /// without a stats claim. Set only inside SEPARABLE slice compiles.
    assume_grouped: bool = false,
    /// Streaming next() emits in `sorted_perm` order (per-batch gather into
    /// `emit_gather_cols`) instead of the zero-copy original-order views —
    /// the downstream half of the pairing above. Requires emit_sorted (the
    /// perm is only retained then). Costs one output copy pass; buys the
    /// paired consumer its full sort.
    emit_sorted_stream: bool = false,
    emit_gather_cols: []ColumnStore = &.{},
    /// When riding (assume_sorted), the SOURCE's key set — the order this
    /// window's output actually preserves, which may be STRONGER than its
    /// own spec (a partition-only window riding a (partition, month) source
    /// emits (partition, month) order). Consumers read effectiveEmitKeys.
    inherited_order: ?ir.WindowSpec = null,
    /// Column borrowing (set by the staged compiler): the input chain is
    /// row-aligned with `borrow_src`'s adopted contiguous result (serial
    /// scan + computes/projections, no filters), so pass-through input
    /// columns take shallow references to its stores instead of being
    /// re-accumulated. borrow_map[ci] = source store index, null =
    /// compute-derived / renamed → accumulate normally. The source stage
    /// is kept alive by this window's own stage (Stage.pinned_upstream).
    borrow_src: ?*exec.mat_stage.Stage = null,
    borrow_map: []const ?usize = &.{},
    /// Runtime: bind succeeded (source ran and adopted contiguous).
    borrowing: bool = false,

    // Batch emit state — emits input + output in original order.
    emit_offset: usize = 0,
    out_output_columns: []ColumnStore, // staging for output cols per batch
    views: []ColumnView, // input + output views, parallel to schema

    const batch_size: usize = 8192;

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

    /// The key set this window's ADOPTED output is (or will be) ordered
    /// by: the inherited source order when riding, else its own single
    /// sort-key set. Null = no usable order (multiple key groups).
    pub fn effectiveEmitKeys(self: *const Window) ?ir.WindowSpec {
        if (self.assume_sorted) {
            if (self.inherited_order) |k| return k;
        }
        if (self.specs.len > 0 and self.singleSortGroup()) return self.specs[0];
        return null;
    }

    fn isBorrowed(self: *const Window, ci: usize) bool {
        return self.borrowing and ci < self.borrow_map.len and self.borrow_map[ci] != null;
    }

    /// Per-row bytes the ADOPTED result will physically own. Borrowed input
    /// columns are shallow references into the pinned upstream stage's stores
    /// (already charged there), so charging them again in the adopting stage
    /// multiplies the bill once per borrower down a window chain — a long
    /// chain then trips MemoryBudgetExceeded at a small fraction of its real
    /// footprint. The sorted-perm path gathers every column into fresh
    /// stores, so borrowing doesn't discount it.
    pub fn adoptedRowBytesEstimate(self: *const Window) usize {
        if (!self.borrowing or self.sorted_perm != null)
            return exec.memory.estimateRowBytes(self.schema);
        var total: usize = 0;
        for (0..self.schema.len) |ci| {
            if (ci < self.input_schema.len and self.isBorrowed(ci)) continue;
            total += exec.memory.estimateRowBytes(self.schema[ci .. ci + 1]);
        }
        return total;
    }

    /// True when every spec shares one resolved sort-key set — the only
    /// shape ride-the-order handles (one permutation describes the op).
    pub fn singleSortGroup(self: *const Window) bool {
        for (self.spec_group) |g| {
            if (g != 0) return false;
        }
        return true;
    }

    fn sameSortKeys(a: SpecIndices, b: SpecIndices) bool {
        return std.mem.eql(usize, a.partition_cols, b.partition_cols) and
            std.mem.eql(usize, a.order_cols, b.order_cols) and
            std.mem.eql(bool, a.order_desc, b.order_desc);
    }

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
        // Single errdefer: `sinit` is read at unwind time, so this also covers
        // the nothing-initialized case (a second errdefer would double-free
        // the outer slice).
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

        const spec_group = try allocator.alloc(usize, specs.len);
        errdefer allocator.free(spec_group);
        for (spec_indices, 0..) |si_a, i| {
            spec_group[i] = i;
            for (spec_indices[0..i], 0..) |si_b, j| {
                if (sameSortKeys(si_b, si_a)) {
                    spec_group[i] = spec_group[j];
                    break;
                }
            }
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
                .nullable = outputNullable(c, call_plans[ci], input_schema),
            };
        }

        // Allocate column buffers for input and output (one per schema col).
        // Every accumulated column's allocations flow through its own arena
        // (see the field doc); the stores themselves need no per-store
        // deinit — the arena sweep in evict()/deinit() reclaims everything.
        const acc_arenas = try allocator.alloc(std.heap.ArenaAllocator, input_schema.len);
        errdefer allocator.free(acc_arenas);
        const arena_backing = if (builtin.is_test) allocator else std.heap.c_allocator;
        for (acc_arenas) |*a| a.* = std.heap.ArenaAllocator.init(arena_backing);
        errdefer for (acc_arenas) |*a| a.deinit();
        const accumulated = try allocator.alloc(ColumnStore, input_schema.len);
        errdefer allocator.free(accumulated);
        for (input_schema, 0..) |col, i| {
            accumulated[i] = try ColumnStore.init(acc_arenas[i].allocator(), col.type, col.nullable);
        }

        const output_columns = try allocator.alloc(ColumnStore, calls.len);
        errdefer allocator.free(output_columns);
        var oinit: usize = 0;
        errdefer for (output_columns[0..oinit]) |*c| c.deinit(allocator);
        for (calls, 0..) |_, ci| {
            const col = schema[input_schema.len + ci];
            output_columns[ci] = try ColumnStore.init(allocator, col.type, col.nullable);
            oinit = ci + 1;
        }

        // Per-call string scratch — empty for non-string outputs;
        // size-N `?[]const u8` slice for string outputs. Filled in
        // `preSizeColumn` so that `create()` can keep this allocation
        // path tidy.
        const string_outputs = try allocator.alloc([]?[]const u8, calls.len);
        errdefer allocator.free(string_outputs);
        for (string_outputs) |*s| s.* = &.{};

        const out_output_columns = try allocator.alloc(ColumnStore, calls.len);
        errdefer allocator.free(out_output_columns);
        var ooinit: usize = 0;
        errdefer for (out_output_columns[0..ooinit]) |*c| c.deinit(allocator);
        for (calls, 0..) |_, ci| {
            const col = schema[input_schema.len + ci];
            out_output_columns[ci] = try ColumnStore.init(allocator, col.type, col.nullable);
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
            .spec_group = spec_group,
            .call_plans = call_plans,
            .specs = specs,
            .calls = calls,
            .accumulated = accumulated,
            .acc_arenas = acc_arenas,
            .output_columns = output_columns,
            .string_outputs = string_outputs,
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
            for (self.acc_arenas) |*a| a.deinit();
            self.allocator.free(self.acc_arenas);
            self.allocator.free(self.accumulated);
        }
        for (self.output_columns) |*c| c.deinit(self.allocator);
        self.allocator.free(self.output_columns);
        for (self.string_outputs) |s| if (s.len > 0) self.allocator.free(s);
        self.allocator.free(self.string_outputs);
        for (self.out_output_columns) |*c| c.deinit(self.allocator);
        self.allocator.free(self.out_output_columns);
        self.allocator.free(self.views);
        if (self.emit_gather_cols.len > 0) {
            for (self.emit_gather_cols) |*c| c.deinit(self.allocator);
            self.allocator.free(self.emit_gather_cols);
        }
        if (self.sorted_perm) |perm| self.allocator.free(perm);
        freeSpecIndices(self.allocator, self.spec_indices, self.spec_indices.len);
        self.allocator.free(self.spec_group);
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
        // stats lookup reads them as unknown. The sorted-stream emit mode
        // REORDERS rows — the input claim no longer describes the output
        // (the paired consumer is compile-proven instead).
        return .{
            .upper_rows = up.upper_rows,
            .sort_state = if (self.emit_sorted_stream) .{} else up.sort_state,
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
    /// buffers (`out_output_columns`, string outputs only) and the window
    /// output columns are freed later in `deinit`.
    fn evict(self: *Window) void {
        if (self.evicted) return;
        for (self.acc_arenas) |*a| a.deinit();
        self.allocator.free(self.acc_arenas);
        self.acc_arenas = &.{};
        self.allocator.free(self.accumulated);
        self.accumulated = &.{};
        if (self.upstream.accountant()) |a| a.release(.window, self.reserved_bytes);
        self.reserved_bytes = 0;
        self.evicted = true;
    }

    /// Everything a stage takes over when it adopts this window's
    /// materialized result instead of pull-copying the emit stream
    /// (mat_stage.Stage.adopt_window). `stores` is one contiguous column
    /// per schema slot; entries flagged in `arena_backed` are reclaimed by
    /// sweeping `arenas`, the rest deinit with the window's (= the stage's)
    /// allocator. All slices are the new owner's to free.
    pub const AdoptedBuffers = struct {
        stores: []ColumnStore,
        arenas: []std.heap.ArenaAllocator,
        arena_backed: []bool,
        rows: u64,
    };

    /// Run the drain + evaluation without emitting (the adopting stage's
    /// barrier calls this instead of pulling `next()`).
    pub fn ensureDrained(self: *Window) !void {
        if (!self.drained) try self.drainAndEvaluate();
    }

    /// Sorted variant of the handover: gather EVERY adopted column by the
    /// retained canonical permutation (bucket-major clustered order:
    /// partitions contiguous, order-sorted within — what a same-key rider
    /// needs) into fresh per-column arenas, in parallel by column claim.
    /// The window's original buffers stay put; the adopting stage tears the
    /// pipeline down right after adoption, freeing them.
    fn adoptBuffersSorted(self: *Window, perm: []const u32) !AdoptedBuffers {
        const alloc = self.allocator;
        const ncols = self.schema.len;
        const stores = try alloc.alloc(ColumnStore, ncols);
        errdefer alloc.free(stores);
        const arena_backed = try alloc.alloc(bool, ncols);
        errdefer alloc.free(arena_backed);
        @memset(arena_backed, true);
        const arenas = try alloc.alloc(std.heap.ArenaAllocator, ncols);
        errdefer alloc.free(arenas);
        const arena_backing = if (builtin.is_test) alloc else std.heap.c_allocator;
        for (arenas) |*a| a.* = std.heap.ArenaAllocator.init(arena_backing);
        errdefer for (arenas) |*a| a.deinit();

        const Gather = struct {
            win: *Window,
            perm: []const u32,
            arenas: []std.heap.ArenaAllocator,
            stores: []ColumnStore,
            cursor: std.atomic.Value(usize) = std.atomic.Value(usize).init(0),
            failed: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),

            fn run(g: *@This()) void {
                const total = g.win.schema.len;
                while (!g.failed.load(.acquire)) {
                    const ci = g.cursor.fetchAdd(1, .acq_rel);
                    if (ci >= total) return;
                    g.gatherOne(ci) catch {
                        g.failed.store(true, .release);
                        return;
                    };
                }
            }

            fn gatherOne(g: *@This(), ci: usize) !void {
                const w = g.win;
                const nin = w.input_schema.len;
                const aa = g.arenas[ci].allocator();
                var st = try ColumnStore.init(aa, w.schema[ci].type, w.schema[ci].nullable);
                if (ci < nin) {
                    try transform.appendByIndices(aa, w.accumulated[ci].view(), g.perm, &st);
                } else if (w.string_outputs[ci - nin].len > 0) {
                    const scratch = w.string_outputs[ci - nin];
                    for (g.perm, 0..) |r, out_row| {
                        const bytes = scratch[r] orelse "";
                        switch (st.data) {
                            .varchar, .string, .char, .json => |*ss| try ss.appendValue(aa, bytes),
                            else => return Error.WindowUnsupported,
                        }
                        try st.appendValidBit(aa, @intCast(out_row), scratch[r] != null);
                    }
                } else {
                    try transform.appendByIndices(aa, w.output_columns[ci - nin].view(), g.perm, &st);
                }
                g.stores[ci] = st;
            }
        };
        var g = Gather{ .win = self, .perm = perm, .arenas = arenas, .stores = stores };
        var threads: [max_drain_workers]?std.Thread = .{null} ** max_drain_workers;
        var spawned: usize = 0;
        if (self.dop > 1 and ncols >= 2 and !builtin.is_test) {
            const want = @min(@min(self.dop - 1, ncols - 1), max_drain_workers);
            while (spawned < want) {
                threads[spawned] = std.Thread.spawn(.{}, Gather.run, .{&g}) catch break;
                spawned += 1;
            }
        }
        Gather.run(&g);
        for (threads[0..spawned]) |maybe| if (maybe) |t| t.join();
        if (g.failed.load(.acquire)) return error.OutOfMemory;

        if (self.upstream.accountant()) |a| a.release(.window, self.reserved_bytes);
        self.reserved_bytes = 0;
        self.emit_offset = @intCast(self.accumulated_rows);
        return .{ .stores = stores, .arenas = arenas, .arena_backed = arena_backed, .rows = self.accumulated_rows };
    }

    /// Ownership handover for window-output-as-stage: move the accumulated
    /// input columns (with their arenas) and the evaluated output columns
    /// out of the operator. String-typed window outputs materialize once
    /// here — the same appendStringScratchRange work per-batch emit would
    /// have done, minus the re-copy into stage chunks. Leaves the window in
    /// its post-evict state (emits nothing; deinit stays uniform).
    pub fn adoptBuffers(self: *Window) !AdoptedBuffers {
        std.debug.assert(self.drained and self.emit_offset == 0 and !self.evicted);
        if (self.sorted_perm) |perm| return self.adoptBuffersSorted(perm);
        const alloc = self.allocator;
        const nin = self.input_schema.len;
        const rows: usize = @intCast(self.accumulated_rows);

        const stores = try alloc.alloc(ColumnStore, self.schema.len);
        errdefer alloc.free(stores);
        const arena_backed = try alloc.alloc(bool, self.schema.len);
        errdefer alloc.free(arena_backed);
        var n_str: usize = 0;
        for (self.string_outputs) |s| {
            if (s.len > 0) n_str += 1;
        }
        const arenas = try alloc.alloc(std.heap.ArenaAllocator, nin + n_str);
        errdefer alloc.free(arenas);

        // Fallible work first, so the move below can't half-complete:
        // 1. String outputs → fresh arena-backed contiguous stores (their
        //    scratch slices borrow into `accumulated`, still alive here).
        const arena_backing = if (builtin.is_test) alloc else std.heap.c_allocator;
        var str_built: usize = 0;
        errdefer for (arenas[nin .. nin + str_built]) |*a| a.deinit();
        {
            var ai: usize = nin;
            for (self.string_outputs, 0..) |scr, ci| {
                if (scr.len == 0) continue;
                arenas[ai] = std.heap.ArenaAllocator.init(arena_backing);
                str_built += 1;
                const sa = arenas[ai].allocator();
                var st = try ColumnStore.init(sa, self.schema[nin + ci].type, true);
                try appendStringScratchRange(sa, scr, 0, rows, &st);
                stores[nin + ci] = st;
                arena_backed[nin + ci] = true;
                ai += 1;
            }
        }
        // 2. Fresh empty replacements for the fixed-width output columns
        //    (deinit frees `output_columns` uniformly).
        const repl = try alloc.alloc(ColumnStore, self.calls.len);
        defer alloc.free(repl);
        var repl_built: usize = 0;
        errdefer for (repl[0..repl_built]) |*c| c.deinit(alloc);
        for (self.calls, 0..) |_, ci| {
            repl[ci] = try ColumnStore.init(alloc, self.schema[nin + ci].type, true);
            repl_built += 1;
        }

        // Infallible moves.
        for (self.accumulated, 0..) |c, i| {
            stores[i] = c;
            arena_backed[i] = true;
        }
        for (self.acc_arenas, 0..) |a, i| arenas[i] = a;
        for (self.output_columns, 0..) |*c, ci| {
            if (self.string_outputs[ci].len > 0) {
                // Scratch-backed: the (empty) placeholder store stays put;
                // its adopted replacement was materialized above. Free the
                // unused fresh replacement.
                repl[ci].deinit(alloc);
                continue;
            }
            stores[nin + ci] = c.*;
            arena_backed[nin + ci] = false;
            c.* = repl[ci];
        }
        alloc.free(self.acc_arenas);
        self.acc_arenas = &.{};
        alloc.free(self.accumulated);
        self.accumulated = &.{};
        if (self.upstream.accountant()) |a| a.release(.window, self.reserved_bytes);
        self.reserved_bytes = 0;
        self.evicted = true;
        self.emit_offset = rows;
        return .{ .stores = stores, .arenas = arenas, .arena_backed = arena_backed, .rows = self.accumulated_rows };
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

        // Sorted-stream emit (window-chain pairing): gather rows in
        // sorted_perm order into per-batch staging columns so the paired
        // downstream window receives partition-grouped input. One copy pass
        // in exchange for the consumer's full sort.
        if (self.emit_sorted_stream) {
            if (self.sorted_perm) |perm| {
                if (self.emit_gather_cols.len == 0) {
                    const cols = try self.allocator.alloc(ColumnStore, self.schema.len);
                    var made: usize = 0;
                    errdefer {
                        for (cols[0..made]) |*c| c.deinit(self.allocator);
                        self.allocator.free(cols);
                    }
                    while (made < self.schema.len) : (made += 1) {
                        cols[made] = try ColumnStore.init(self.allocator, self.schema[made].type, true);
                    }
                    self.emit_gather_cols = cols;
                }
                const idxs = perm[@intCast(lo)..@intCast(hi)];
                for (self.accumulated, 0..) |c, i| {
                    self.emit_gather_cols[i].clear();
                    try transform.appendByIndices(self.allocator, c.view(), idxs, &self.emit_gather_cols[i]);
                    self.views[i] = self.emit_gather_cols[i].view();
                }
                const goff = self.input_schema.len;
                for (self.output_columns, 0..) |c, ci| {
                    self.emit_gather_cols[goff + ci].clear();
                    if (self.string_outputs[ci].len > 0) {
                        try appendStringScratchIndices(self.allocator, self.string_outputs[ci], idxs, &self.emit_gather_cols[goff + ci]);
                    } else {
                        try transform.appendByIndices(self.allocator, c.view(), idxs, &self.emit_gather_cols[goff + ci]);
                    }
                    self.views[goff + ci] = self.emit_gather_cols[goff + ci].view();
                }
                self.emit_offset = hi;
                return Batch{ .schema = self.schema, .values = self.views, .row_count = n };
            }
        }

        // Zero-copy emit: views slice the accumulated/output columns
        // directly — `batch_size` is a multiple of 8 so the validity
        // bitmaps slice on byte boundaries. The buffers stay alive until
        // `evict()` (the call after the last batch), satisfying the
        // batch-valid-until-next-call contract. Only string-typed window
        // outputs materialize through a staging column (their values live
        // in the per-row scratch, not a StringStore).
        for (self.accumulated, 0..) |c, i| self.views[i] = subView(c.view(), lo, n);
        const off = self.input_schema.len;
        for (self.output_columns, 0..) |c, ci| {
            if (self.string_outputs[ci].len > 0) {
                self.out_output_columns[ci].clear();
                try appendStringScratchRange(self.allocator, self.string_outputs[ci], lo, hi, &self.out_output_columns[ci]);
                self.views[off + ci] = self.out_output_columns[ci].view();
            } else {
                self.views[off + ci] = subView(c.view(), lo, n);
            }
        }

        self.emit_offset = hi;
        return Batch{ .schema = self.schema, .values = self.views, .row_count = n };
    }

    /// Shared control block for the parallel drain. Each batch runs two
    /// claim phases over a unique fetchAdd cursor:
    ///
    ///   .prepare — units are COLUMNS: extend each store to its final size
    ///              (transform.prepareAppend; per-column arenas make the
    ///              concurrent resizes race-free).
    ///   .write   — units are (column × row-tile): positional, alloc-free
    ///              fills (transform.writeAppendSlice). Tiling by rows is
    ///              what beats per-column skew — one fat string column no
    ///              longer bounds the whole batch.
    ///
    /// The conn thread publishes a phase, participates, then waits for
    /// (a) every unit done and (b) every worker re-parked before mutating
    /// shared state — claims are unique within a cycle and cycles never
    /// overlap, so no unit runs twice. Tile bounds land on absolute 8-row
    /// boundaries so no two writers share a validity byte.
    const ParDrain = struct {
        win: *Window,
        owned: []const usize = &.{},
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

        fn worker(pd: *ParDrain) void {
            var seen: usize = 0;
            while (true) {
                var spins: usize = 0;
                while (pd.gen.load(.acquire) == seen) {
                    if (pd.stop.load(.acquire)) return;
                    spins += 1;
                    if (spins < 2048) {
                        std.atomic.spinLoopHint();
                    } else if (spins < 4096) {
                        std.Thread.yield() catch std.atomic.spinLoopHint();
                    } else {
                        // Long producer gaps (a nested blocking op below):
                        // stop burning the core. The conn thread always
                        // participates, so a late wake only means this
                        // worker claims fewer units.
                        sleepBriefly();
                    }
                }
                seen = pd.gen.load(.acquire);
                _ = pd.parked.fetchSub(1, .acq_rel);
                pd.runUnits();
                _ = pd.parked.fetchAdd(1, .acq_rel);
            }
        }

        fn runUnits(pd: *ParDrain) void {
            const w = pd.win;
            const ncols = pd.owned.len;
            while (true) {
                const u = pd.unit_cursor.fetchAdd(1, .acq_rel);
                if (u >= pd.n_units) return;
                switch (pd.mode) {
                    .prepare => {
                        const ci = pd.owned[u];
                        pd.preps[ci] = transform.prepareAppend(
                            w.acc_arenas[ci].allocator(),
                            pd.batch.values[ci],
                            pd.batch.row_count,
                            &w.accumulated[ci],
                        ) catch blk: {
                            pd.failed.store(true, .release);
                            break :blk .{ .base_row = 0, .positional = false };
                        };
                    },
                    .write => {
                        // Interleave column-major so concurrent claims walk
                        // different columns (independent memory streams).
                        const ci = pd.owned[u % ncols];
                        const t = u / ncols;
                        if (pd.preps[ci].positional) {
                            transform.writeAppendSlice(
                                pd.batch.values[ci],
                                pd.bounds[t],
                                pd.bounds[t + 1],
                                &w.accumulated[ci],
                                pd.preps[ci],
                            );
                        }
                    },
                }
                _ = pd.units_done.fetchAdd(1, .acq_rel);
            }
        }

        /// Publish one phase, participate, and wait for completion + full
        /// re-park before returning (after which shared state is single-
        /// owner again).
        fn runPhase(pd: *ParDrain, mode: Mode, n_units: usize, n_workers: usize) void {
            pd.mode = mode;
            pd.n_units = n_units;
            pd.unit_cursor.store(0, .release);
            pd.units_done.store(0, .release);
            _ = pd.gen.fetchAdd(1, .release);
            pd.runUnits();
            var spins: usize = 0;
            while (pd.units_done.load(.acquire) < n_units or
                pd.parked.load(.acquire) < n_workers)
            {
                spins += 1;
                if (spins < 4096) std.atomic.spinLoopHint() else std.Thread.yield() catch {};
            }
        }
    };

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

    /// Most drain-copy threads worth spawning: memcpy saturates memory
    /// bandwidth well below core count, and each thread only helps while
    /// there are unclaimed columns.
    const max_drain_workers: usize = 7;

    fn drainAndEvaluate(self: *Window) !void {
        // Drain upstream into accumulated. With dop > 1 the per-batch column
        // copies fan out across a small worker pool (see ParDrain); the
        // upstream pull itself stays on this thread (batches are only valid
        // until the next pull, so produce and copy can't overlap).
        const acc = self.upstream.accountant();
        const ncols = self.input_schema.len;

        // Bind borrowed columns: run the source (it runs lazily on first
        // pull anyway), and take shallow references into its adopted
        // contiguous stores. Any miss (no result / chunked copy result)
        // degrades to normal full accumulation.
        var owned_buf = try self.allocator.alloc(usize, ncols);
        defer self.allocator.free(owned_buf);
        var borrow_expect: u64 = 0;
        if (self.borrow_src) |src| bind: {
            src.ensureRun() catch break :bind;
            const res = src.result orelse break :bind;
            const ad = res.adopted orelse break :bind;
            // One contiguous store per column only — a SEPARABLE sliced fill
            // adopts N stores per column (slice parts), which can't be
            // shallow-referenced as single columns. Degrade to accumulation.
            if (ad.stores.len != res.schema.len) break :bind;
            for (self.borrow_map, 0..) |m, ci| {
                if (m) |src_idx| self.accumulated[ci] = ad.stores[src_idx];
            }
            self.borrowing = true;
            borrow_expect = res.total_rows;
        }
        var n_owned: usize = 0;
        for (0..ncols) |ci| {
            if (!self.isBorrowed(ci)) {
                owned_buf[n_owned] = ci;
                n_owned += 1;
            }
        }
        const owned = owned_buf[0..n_owned];
        const row_bytes = blk: {
            if (!self.borrowing) break :blk exec.memory.estimateRowBytes(self.input_schema);
            var b: usize = 0;
            for (owned) |ci| b += exec.memory.estimateRowBytes(self.input_schema[ci .. ci + 1]);
            break :blk b;
        };
        var pd = ParDrain{ .win = self, .owned = owned };
        var workers: [max_drain_workers]?std.Thread = .{null} ** max_drain_workers;
        var n_workers: usize = 0;
        var max_tiles: usize = 1;
        if (self.dop > 1 and ncols >= 2 and !builtin.is_test) {
            const want = @min(@min(self.dop - 1, ncols - 1), max_drain_workers);
            while (n_workers < want) {
                workers[n_workers] = std.Thread.spawn(.{}, ParDrain.worker, .{&pd}) catch break;
                n_workers += 1;
            }
            pd.parked.store(n_workers, .release);
            max_tiles = (n_workers + 1) * 2;
            pd.preps = try self.allocator.alloc(transform.PreparedAppend, ncols);
            pd.bounds = try self.allocator.alloc(usize, max_tiles + 1);
        }
        defer {
            pd.stop.store(true, .release);
            for (workers[0..n_workers]) |maybe| if (maybe) |t| t.join();
            if (pd.preps.len > 0) self.allocator.free(pd.preps);
            if (pd.bounds.len > 0) self.allocator.free(pd.bounds);
        }
        var drain_up_ticks: i64 = 0;
        var drain_ap_ticks: i64 = 0;
        var drain_batches: usize = 0;
        while (true) {
            const _ut = if (exec.prof.enabled) exec.prof.nowTicks() else 0;
            const got = try self.upstream.next();
            if (exec.prof.enabled) {
                const d = @max(0, exec.prof.nowTicks() - _ut);
                exec.prof.addPhase("window.drain.upstream", @intCast(d));
                drain_up_ticks += d;
                drain_batches += 1;
            }
            const batch = got orelse break;
            const _at = if (exec.prof.enabled) exec.prof.nowTicks() else 0;
            const b = batch.row_count * row_bytes;
            if (acc) |a| try a.reserve(.window, b);
            self.reserved_bytes += b;
            if (n_workers > 0) {
                pd.batch = batch;
                pd.runPhase(.prepare, owned.len, n_workers);
                if (pd.failed.load(.acquire)) return error.OutOfMemory;
                // Row tiles on absolute 8-row boundaries: the first bound
                // absorbs the base misalignment so no two tiles share a
                // destination validity byte.
                const n = batch.row_count;
                const base: usize = @intCast(self.accumulated_rows);
                const pad = (8 - (base % 8)) % 8;
                const step = ((n / max_tiles) + 8) & ~@as(usize, 7);
                var nt: usize = 0;
                pd.bounds[0] = 0;
                while (pd.bounds[nt] < n) {
                    const next_b = @min(n, if (nt == 0) pad + step else pd.bounds[nt] + step);
                    nt += 1;
                    pd.bounds[nt] = @max(next_b, pd.bounds[nt - 1] + 1);
                    if (pd.bounds[nt] > n) pd.bounds[nt] = n;
                }
                pd.n_tiles = nt;
                pd.runPhase(.write, owned.len * nt, n_workers);
                // Rare non-positional columns (a wide string store) take
                // the classic serial append — single caller, own arena.
                for (owned) |ci| {
                    if (pd.preps[ci].positional) continue;
                    const aa = self.acc_arenas[ci].allocator();
                    try transform.appendColumnRange(aa, batch.values[ci], 0, n, &self.accumulated[ci]);
                }
            } else {
                for (owned) |ci| {
                    const aa = self.acc_arenas[ci].allocator();
                    try reserveAggressive(aa, &self.accumulated[ci], batch.row_count);
                    try transform.appendAllColumn(aa, batch.values[ci], &self.accumulated[ci]);
                }
            }
            self.accumulated_rows += batch.row_count;
            if (exec.prof.enabled) {
                const d = @max(0, exec.prof.nowTicks() - _at);
                exec.prof.addPhase("window.drain.append", @intCast(d));
                drain_ap_ticks += d;
            }
        }
        if (exec.prof.enabled) {
            std.debug.print("[hprof] window.drain rows={d} batches={d} upstream={d:.2} ms append={d:.2} ms borrow={} calls={d}\n", .{
                self.accumulated_rows,
                drain_batches,
                exec.prof.ticksToMs(drain_up_ticks),
                exec.prof.ticksToMs(drain_ap_ticks),
                self.borrowing,
                self.calls.len,
            });
            if (exec.prof.ticksToMs(drain_up_ticks) > 100.0) explain: {
                var buf: std.ArrayList(u8) = .empty;
                defer buf.deinit(self.allocator);
                self.upstream.explain(&buf, self.allocator, 2) catch break :explain;
                std.debug.print("[hprof] window.drain slow upstream plan:\n{s}", .{buf.items});
            }
        }
        // Borrowed stores are row-aligned by the compile-time contract
        // (no filters in the chain); a mismatch means that contract broke.
        if (self.borrowing and self.accumulated_rows != borrow_expect) return Error.WindowUnsupported;
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
        // An UNpartitioned spec still parallelizes its SORT (samplesort —
        // range buckets from sampled splitters; bucket concatenation is the
        // total order); its evaluation stays serial, since rank/frame state
        // crosses bucket seams (carry fixups are a later phase).
        const ride_single = self.singleSortGroup();
        for (self.spec_indices, 0..) |si, spec_i| {
            if (self.spec_group[spec_i] != spec_i) continue; // shares the canonical spec's pass
            if (self.assume_sorted and ride_single) {
                // Input arrives sorted on this window's keys (established at
                // compile: an upstream same-key window emitted its stage in
                // spec order and the chain here is order-preserving), so the
                // permutation is the identity — evaluation walks key runs.
                const perm = try self.allocator.alloc(u32, n);
                for (perm, 0..) |*p, i| p.* = @intCast(i);
                errdefer self.allocator.free(perm);
                const _et = if (exec.prof.enabled) exec.prof.nowTicks() else 0;
                for (self.call_plans, 0..) |plan, ci| {
                    if (self.spec_group[plan.spec_idx] != spec_i) continue;
                    try self.evaluateCall(plan, ci, perm, si);
                }
                if (exec.prof.enabled) exec.prof.addPhase("window.eval.sorted", @intCast(@max(0, exec.prof.nowTicks() - _et)));
                // Identity order: even an emit_sorted adoption stays
                // zero-copy (null sorted_perm = buffers already in order).
                self.allocator.free(perm);
                continue;
            }
            if (si.partition_cols.len > 0 and n >= parallel_min_rows and self.dop > 1) {
                try self.evaluateSpecParallel(si, spec_i, n);
                continue;
            }
            const global_par = si.partition_cols.len == 0 and n >= parallel_min_rows and self.dop > 1;
            const perm = if (global_par)
                try self.buildPermutationSamplesort(si, n)
            else if (self.groupedByPartition(si))
                try self.buildPermutationGrouped(si)
            else
                try self.buildPermutation(si);
            const retain = self.emit_sorted and ride_single;
            defer if (!retain) self.allocator.free(perm);
            if (retain) self.sorted_perm = perm;
            const _et = if (exec.prof.enabled) exec.prof.nowTicks() else 0;
            for (self.call_plans, 0..) |plan, ci| {
                if (self.spec_group[plan.spec_idx] != spec_i) continue;
                // Position-pure calls over the single global partition
                // shard their OUTPUT range across workers (full-perm
                // visibility makes offsets and rank walk-backs local);
                // state-carrying calls (dense_rank, running aggregates,
                // distribution functions) stay serial.
                if (global_par and globalCallParallel(plan)) {
                    try self.evaluateCallGlobalParallel(plan, ci, perm, si, n);
                } else {
                    try self.evaluateCall(plan, ci, perm, si);
                }
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
        // Bucket-major clustered order: partitions are contiguous and
        // order-sorted within — exactly what a same-key rider needs (it
        // never requires a GLOBAL partition order, and no sort_state is
        // advertised). Retained for the adoption gather.
        const retain_perm = self.emit_sorted and self.singleSortGroup();
        if (retain_perm) self.sorted_perm = perm;
        defer if (!retain_perm) self.allocator.free(perm);
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
        /// Range-bucket splitters for the unpartitioned samplesort mode
        /// (empty = digest mode). Bucket b holds keys in
        /// [splitters[b-1], splitters[b]) under the full pairLess order —
        /// the idx tiebreak makes every key unique, so ranges are clean.
        splitters: []const KeyIdx = &.{},
        /// Samplesort only parallelizes the sort: phase D writes the perm
        /// slices and skips evaluation (rank/frame state crosses seams).
        sort_only: bool = false,
        next_bucket: std.atomic.Value(usize) = .init(0),
        failed: std.atomic.Value(bool) = .init(false),
        err_mutex: std.atomic.Mutex = .unlocked,
        err: ?anyerror = null,

        fn range(self: *const SpecParJob, w: usize) struct { lo: usize, hi: usize } {
            const chunk = (self.n + self.workers - 1) / self.workers;
            const lo = @min(w * chunk, self.n);
            return .{ .lo = lo, .hi = @min(lo + chunk, self.n) };
        }

        fn bucketOf(self: *const SpecParJob, kp: KeyIdx) usize {
            if (self.splitters.len == 0) {
                return @intCast(kp.hi & @as(u64, @intCast(self.bucket_count - 1)));
            }
            const ctx = self.win.sortCtx(self.si);
            var lo: usize = 0;
            var hi: usize = self.splitters.len;
            while (lo < hi) {
                const mid = lo + (hi - lo) / 2;
                if (SpecSortCtx.pairLess(ctx, kp, self.splitters[mid])) hi = mid else lo = mid + 1;
            }
            return lo;
        }

        fn phaseKeys(self: *SpecParJob, w: usize) void {
            const r = self.range(w);
            self.win.fillKeys(self.si, self.pairs, r.lo, r.hi);
            const my = self.counts[w * self.bucket_count ..][0..self.bucket_count];
            for (self.pairs[r.lo..r.hi]) |kp| my[self.bucketOf(kp)] += 1;
        }

        /// Samplesort runs key fill and bucket counting as separate phases —
        /// the splitters are sampled from the filled keys in between.
        fn phaseKeysOnly(self: *SpecParJob, w: usize) void {
            const r = self.range(w);
            self.win.fillKeys(self.si, self.pairs, r.lo, r.hi);
        }

        fn phaseCount(self: *SpecParJob, w: usize) void {
            const r = self.range(w);
            const my = self.counts[w * self.bucket_count ..][0..self.bucket_count];
            for (self.pairs[r.lo..r.hi]) |kp| my[self.bucketOf(kp)] += 1;
        }

        fn phasePlace(self: *SpecParJob, w: usize) void {
            const r = self.range(w);
            const my = self.counts[w * self.bucket_count ..][0..self.bucket_count];
            for (self.pairs[r.lo..r.hi]) |kp| {
                const b = self.bucketOf(kp);
                self.placed[my[b]] = kp;
                my[b] += 1;
            }
        }

        fn phaseBuckets(self: *SpecParJob) void {
            while (!self.failed.load(.acquire)) {
                const b = self.next_bucket.fetchAdd(1, .monotonic);
                if (b >= self.bucket_count) return;
                const lo = self.bucket_offsets[b];
                const hi = self.bucket_offsets[b + 1];
                if (lo == hi) continue;
                const slice = self.placed[lo..hi];
                std.sort.pdq(KeyIdx, slice, self.win.sortCtx(self.si), SpecSortCtx.pairLess);
                for (self.perm[lo..hi], slice) |*p, kp| p.* = kp.idx;
                if (self.sort_only) continue;
                var p_start = lo;
                while (p_start < hi) {
                    const p_end = partitionEnd(self.win.accumulated, self.si.partition_cols, self.perm[0..hi], p_start);
                    for (self.win.call_plans, 0..) |plan, ci| {
                        if (self.win.spec_group[plan.spec_idx] != self.spec_i) continue;
                        const cell: OutCell = .{
                            .column = &self.win.output_columns[ci],
                            .string_scratch = if (self.win.string_outputs[ci].len > 0) self.win.string_outputs[ci] else null,
                        };
                        self.win.evaluateOnePartition(plan, self.win.specs[plan.spec_idx], self.si, self.perm[0..hi], p_start, p_end, cell) catch |e| return self.fail(e);
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

    /// Parallel samplesort for an UNpartitioned spec: fill keys in
    /// parallel, sample ~16 keys per bucket and sort the sample to pick
    /// bucket_count-1 splitters, range-scatter rows (count + prefix +
    /// place), then workers claim buckets and sort them. Buckets are
    /// ordered relative to each other, so the concatenated bucket perms
    /// form the global sorted permutation, which the caller evaluates
    /// serially. Returns the perm; caller frees.
    fn buildPermutationSamplesort(self: *Window, si: SpecIndices, n: usize) ![]u32 {
        const workers: usize = @min(@max(self.dop, 2), 32);
        const bucket_count: usize = @min(@as(usize, 256), std.math.ceilPowerOfTwoAssert(usize, workers * 4));

        const acc = self.upstream.accountant();
        const scratch_bytes = n * (2 * @sizeOf(KeyIdx)) + n * @sizeOf(u32);
        if (acc) |a| try a.reserve(.window, scratch_bytes);
        defer if (acc) |a| a.release(.window, scratch_bytes);

        const pairs = try self.allocator.alloc(KeyIdx, n);
        defer self.allocator.free(pairs);
        const placed = try self.allocator.alloc(KeyIdx, n);
        defer self.allocator.free(placed);
        const perm = try self.allocator.alloc(u32, n);
        errdefer self.allocator.free(perm);
        const counts = try self.allocator.alloc(usize, workers * bucket_count);
        defer self.allocator.free(counts);
        @memset(counts, 0);
        const bucket_offsets = try self.allocator.alloc(usize, bucket_count + 1);
        defer self.allocator.free(bucket_offsets);

        var job = SpecParJob{
            .win = self,
            .si = si,
            .spec_i = 0, // unused: sort_only never evaluates
            .n = n,
            .workers = workers,
            .bucket_count = bucket_count,
            .pairs = pairs,
            .placed = placed,
            .perm = perm,
            .counts = counts,
            .bucket_offsets = bucket_offsets,
            .sort_only = true,
        };

        const _kt = if (exec.prof.enabled) exec.prof.nowTicks() else 0;
        runRangePhase(&job, SpecParJob.phaseKeysOnly);
        if (exec.prof.enabled) exec.prof.addPhase("window.par_keys", @intCast(@max(0, exec.prof.nowTicks() - _kt)));

        // Sample evenly, sort the sample, take every (len/bucket_count)th
        // entry as a splitter. The idx tiebreak in pairLess makes every
        // key distinct, so heavy duplicates still split into level ranges.
        const sample_len = @min(n, bucket_count * 16);
        const sample = try self.allocator.alloc(KeyIdx, sample_len);
        defer self.allocator.free(sample);
        const stride = n / sample_len;
        for (sample, 0..) |*s, i| s.* = pairs[i * stride];
        std.sort.pdq(KeyIdx, sample, self.sortCtx(si), SpecSortCtx.pairLess);
        const splitters = try self.allocator.alloc(KeyIdx, bucket_count - 1);
        defer self.allocator.free(splitters);
        for (splitters, 1..) |*sp, b| sp.* = sample[b * sample_len / bucket_count];
        job.splitters = splitters;

        const _ct = if (exec.prof.enabled) exec.prof.nowTicks() else 0;
        runRangePhase(&job, SpecParJob.phaseCount);
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
        runRangePhase(&job, SpecParJob.phasePlace);
        const _bt = if (exec.prof.enabled) exec.prof.nowTicks() else 0;
        if (exec.prof.enabled) exec.prof.addPhase("window.par_place", @intCast(@max(0, _bt - _ct)));
        runBucketPhase(&job);
        if (exec.prof.enabled) exec.prof.addPhase("window.par_sort", @intCast(@max(0, exec.prof.nowTicks() - _bt)));

        if (job.err) |e| return e; // errdefer frees perm
        return perm;
    }

    fn globalCallParallel(plan: CallPlan) bool {
        return switch (plan.func) {
            .row_number, .rank, .lag, .lead => true,
            else => false,
        };
    }

    /// Shard one position-pure call's output range over the global (single-
    /// partition) perm across `dop` workers. ROW_NUMBER is its position;
    /// RANK walks back from the range start to its peer-run start (worst
    /// case all-tied keys degenerates to overlapping walks — a degenerate
    /// query shape, still correct); LAG/LEAD read neighbors through the
    /// full perm.
    fn evaluateCallGlobalParallel(self: *Window, plan: CallPlan, out_idx: usize, perm: []const u32, si: SpecIndices, n: usize) !void {
        const cell: OutCell = .{
            .column = &self.output_columns[out_idx],
            .string_scratch = if (self.string_outputs[out_idx].len > 0) self.string_outputs[out_idx] else null,
        };
        var job = GlobalEvalJob{
            .win = self,
            .si = si,
            .plan = plan,
            .cell = cell,
            .perm = perm,
            .n = n,
            .workers = @min(@max(self.dop, 2), 32),
        };
        var threads: [32]std.Thread = undefined;
        var spawned: usize = 0;
        while (spawned < job.workers - 1) {
            threads[spawned] = std.Thread.spawn(.{}, GlobalEvalJob.worker, .{ &job, spawned }) catch break;
            spawned += 1;
        }
        var w = spawned;
        while (w < job.workers) : (w += 1) GlobalEvalJob.worker(&job, w);
        for (threads[0..spawned]) |t| t.join();
        if (job.err) |e| return e;
    }

    const GlobalEvalJob = struct {
        win: *Window,
        si: SpecIndices,
        plan: CallPlan,
        cell: OutCell,
        perm: []const u32,
        n: usize,
        workers: usize,
        err_mutex: std.atomic.Mutex = .unlocked,
        err: ?anyerror = null,

        fn worker(self: *GlobalEvalJob, w: usize) void {
            const chunk = (self.n + self.workers - 1) / self.workers;
            const lo = @min(w * chunk, self.n);
            const hi = @min(lo + chunk, self.n);
            if (lo >= hi) return;
            self.run(lo, hi) catch |e| {
                while (!self.err_mutex.tryLock()) std.atomic.spinLoopHint();
                if (self.err == null) self.err = e;
                self.err_mutex.unlock();
            };
        }

        fn run(self: *GlobalEvalJob, lo: usize, hi: usize) !void {
            switch (self.plan.func) {
                .row_number => {
                    var i = lo;
                    while (i < hi) : (i += 1) {
                        try writeBigint(self.cell.column, self.perm[i], @intCast(i + 1));
                    }
                },
                .rank => {
                    const cols = self.win.accumulated;
                    const oc = self.si.order_cols;
                    var s = lo;
                    while (s > 0 and orderEquals(cols, oc, self.perm[s - 1], self.perm[s])) s -= 1;
                    var prev_rank: i64 = @intCast(s + 1);
                    var i = lo;
                    while (i < hi) : (i += 1) {
                        if (i > 0 and !orderEquals(cols, oc, self.perm[i - 1], self.perm[i])) {
                            prev_rank = @intCast(i + 1);
                        } else if (i == 0) {
                            prev_rank = 1;
                        }
                        try writeBigint(self.cell.column, self.perm[i], prev_rank);
                    }
                },
                .lag => try self.win.fillLagLeadRange(self.plan, self.perm, 0, self.n, lo, hi, self.cell, true),
                .lead => try self.win.fillLagLeadRange(self.plan, self.perm, 0, self.n, lo, hi, self.cell, false),
                else => unreachable,
            }
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
                const ord = transform.compareInColumnNullsFirst(ctx.cols[ci], a.idx, b.idx);
                if (ord == .lt) return true;
                if (ord == .gt) return false;
            }
            if (a.lo != b.lo) return a.lo < b.lo;
            for (ctx.order, 0..) |ci, i| {
                const ord = transform.compareInColumnNullsFirst(ctx.cols[ci], a.idx, b.idx);
                if (ord == .lt) return !ctx.desc[i];
                if (ord == .gt) return ctx.desc[i];
            }
            return a.idx < b.idx;
        }

        /// Grouped-path comparator: rows are known to share a partition, so
        /// skip the digest and the partition VALUE compares entirely — the
        /// full pairLess re-compares every partition string on every tie,
        /// which dominates when partitions are tiny and numerous.
        pub fn pairLessOrderOnly(ctx: @This(), a: KeyIdx, b: KeyIdx) bool {
            if (a.lo != b.lo) return a.lo < b.lo;
            for (ctx.order, 0..) |ci, i| {
                const ord = transform.compareInColumnNullsFirst(ctx.cols[ci], a.idx, b.idx);
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

    /// The input arrives GROUPED by this spec's partition columns: the
    /// upstream's global sort claim LEADS with exactly the partition-column
    /// set (any order among them, any direction), so partitions are already
    /// contiguous in row order and only the within-partition order is open.
    fn groupedByPartition(self: *Window, si: SpecIndices) bool {
        if (si.partition_cols.len == 0) return false;
        if (self.assume_grouped) return true;
        const ss = self.upstream.stats().sort_state;
        if (!ss.global) return false;
        if (ss.keys.len < si.partition_cols.len) return false;
        for (si.partition_cols) |ci| {
            const name = self.input_schema[ci].name;
            var found = false;
            for (ss.keys[0..si.partition_cols.len]) |key| {
                if (types.columnNameEql(key, name)) {
                    found = true;
                    break;
                }
            }
            if (!found) return false;
        }
        return true;
    }

    /// Grouped fast path (see groupedByPartition): skip the global sort —
    /// walk partition boundaries over the IDENTITY order and sort only
    /// WITHIN each partition by the spec keys. Replaces one n·log n sort
    /// over wide composite keys with an O(n) boundary walk plus tiny
    /// per-partition sorts (partitions in the target workloads average a
    /// few dozen rows). The DRAM traffic that a full permutation sort
    /// generates is what dilates concurrent SEPARABLE slices — this path
    /// is their window engine.
    fn buildPermutationGrouped(self: *Window, si: SpecIndices) ![]u32 {
        const n: usize = @intCast(self.accumulated_rows);
        const perm = try self.allocator.alloc(u32, n);
        errdefer self.allocator.free(perm);
        const pairs = try self.allocator.alloc(KeyIdx, n);
        defer self.allocator.free(pairs);
        const _kt = if (exec.prof.enabled) exec.prof.nowTicks() else 0;
        // Order-only keys: within-partition sorts never cross partitions,
        // so the per-row partition digest (and the partition value compares
        // in the comparator) are pure waste here.
        const order0: ?usize = if (si.order_cols.len > 0) si.order_cols[0] else null;
        for (pairs, 0..) |*kp, i| {
            const row: u32 = @intCast(i);
            kp.* = .{
                .hi = 0,
                .lo = if (order0) |oc| orderPrefix(self.accumulated[oc], row, si.order_desc[0]) else 0,
                .idx = row,
            };
        }
        for (perm, 0..) |*p, i| p.* = @intCast(i);
        const _st = if (exec.prof.enabled) exec.prof.nowTicks() else 0;
        if (exec.prof.enabled) exec.prof.addPhase("window.keys", @intCast(@max(0, _st - _kt)));
        var lo: usize = 0;
        while (lo < n) {
            const hi = partitionEnd(self.accumulated, si.partition_cols, perm, lo);
            std.sort.pdq(KeyIdx, pairs[lo..hi], self.sortCtx(si), SpecSortCtx.pairLessOrderOnly);
            lo = hi;
        }
        if (exec.prof.enabled) exec.prof.addPhase("window.sort.grouped", @intCast(@max(0, exec.prof.nowTicks() - _st)));
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
        return self.fillLagLeadRange(plan, perm, p_start, p_end, p_start, p_end, cell, is_lag);
    }

    /// LAG/LEAD over partition bounds [p_start, p_end) writing only output
    /// positions [out_lo, out_hi) — the parallel global path shards the
    /// output range across workers while every worker keeps full-partition
    /// visibility for the offset lookups.
    fn fillLagLeadRange(
        self: *Window,
        plan: CallPlan,
        perm: []const u32,
        p_start: usize,
        p_end: usize,
        out_lo: usize,
        out_hi: usize,
        cell: OutCell,
        is_lag: bool,
    ) !void {
        const offset: i64 = plan.offset;
        const value_col = self.accumulated[plan.value_col];
        const view = value_col.view();
        var i: usize = out_lo;
        while (i < out_hi) : (i += 1) {
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
            inline .int, .bigint, .tinyint, .smallint, .largeint => |l| {
                const items = l.items;
                var sum: i128 = 0;
                var saw_any = false;
                var i: usize = p_start;
                while (i < p_end) : (i += 1) {
                    if (isValid(view, perm[i])) {
                        sum += @intCast(items[perm[i]]);
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
            inline .float, .double => |l| {
                const items = l.items;
                var sum: f64 = 0;
                var saw_any = false;
                var i: usize = p_start;
                while (i < p_end) : (i += 1) {
                    if (isValid(view, perm[i])) {
                        sum += @floatCast(items[perm[i]]);
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
        switch (col.data) {
            inline .int, .bigint, .tinyint, .smallint, .largeint, .float, .double => |l| {
                const items = l.items;
                const is_float = @typeInfo(@TypeOf(items[0])) == .float;
                var i: usize = p_start;
                while (i < p_end) : (i += 1) {
                    if (isValid(view, perm[i])) {
                        sum += if (is_float) @floatCast(items[perm[i]]) else @floatFromInt(items[perm[i]]);
                        n += 1;
                    }
                    if (n == 0) setNull(out, perm[i]) else try writeDouble(out, perm[i], sum / @as(f64, @floatFromInt(n)));
                }
            },
            else => return Error.WindowUnsupported,
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
        // Bind the typed slice ONCE (inline switch), then index it in the row
        // loop — no per-element data-union dispatch.
        switch (col.data) {
            inline .int, .bigint, .tinyint, .smallint, .largeint => |l| {
                const items = l.items;
                var sum: i128 = 0;
                var k: usize = lo;
                while (k <= hi) : (k += 1) {
                    const r = perm[k];
                    if (!isValid(view, r)) continue;
                    saw_value = true;
                    sum += @intCast(items[r]);
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
            inline .float, .double => |l| {
                const items = l.items;
                var sum: f64 = 0;
                var k: usize = lo;
                while (k <= hi) : (k += 1) {
                    const r = perm[k];
                    if (!isValid(view, r)) continue;
                    saw_value = true;
                    sum += @floatCast(items[r]);
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
            inline .int, .bigint, .tinyint, .smallint, .largeint => |l| {
                const items = l.items;
                var k: usize = lo;
                while (k <= hi) : (k += 1) {
                    const r = perm[k];
                    if (!isValid(view, r)) continue;
                    sum += @floatFromInt(items[r]);
                    n += 1;
                }
            },
            inline .float, .double => |l| {
                const items = l.items;
                var k: usize = lo;
                while (k <= hi) : (k += 1) {
                    const r = perm[k];
                    if (!isValid(view, r)) continue;
                    sum += @floatCast(items[r]);
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
                    .null_lit => {
                        plan.default_kind = .none;
                    },
                    .call, .case, .scalar_subquery, .exists_subquery, .var_ref => return Error.WindowUnsupported,
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

fn outputNullable(c: ir.WindowCall, plan: Window.CallPlan, schema: []const Column) bool {
    return switch (c.func) {
        .row_number, .rank, .dense_rank, .count, .ntile, .percent_rank, .cume_dist => false,
        .lag, .lead => schema[plan.value_col].nullable or switch (plan.default_kind) {
            .none => true,
            .literal => false,
            .col_ref => schema[plan.default_col].nullable,
        },
        .sum, .avg, .min, .max, .first_value, .last_value, .nth_value => true,
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
        .varchar, .string, .char, .json => return Error.WindowUnsupported,
    }
    _ = t;
    if (out.nulls) |*nb| {
        const bytes_needed = (n + 7) / 8;
        try nb.appendNTimes(allocator, 0, bytes_needed); // 0 = NULL
    }
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

/// Indexed variant for the sorted-stream emit: pick scratch entries by
/// permutation indices instead of a contiguous range.
fn appendStringScratchIndices(
    allocator: Allocator,
    scratch: []const ?[]const u8,
    indices: []const u32,
    out: *ColumnStore,
) !void {
    for (indices, 0..) |idx, row| {
        const bytes = scratch[idx] orelse "";
        switch (out.data) {
            .string => |*ss| try ss.appendValue(allocator, bytes),
            .varchar => |*ss| try ss.appendValue(allocator, bytes),
            .char => |*ss| try ss.appendValue(allocator, bytes),
            else => return Error.WindowUnsupported,
        }
        try out.appendValidBit(allocator, row, scratch[idx] != null);
    }
}

fn isStringType(t: Type) bool {
    return switch (t) {
        .string, .varchar, .char, .json => true,
        else => false,
    };
}

/// Quadruple-on-grow capacity reservation for the fixed-width accumulation
/// columns. ArrayList's doubling re-touches the whole column repeatedly as
/// tens of millions of rows arrive batch-by-batch — on Windows every fresh
/// capacity page is demand-zero faulted, which dominated the drain phase.
/// Quadrupling cuts the realloc copy/fault traffic to a third. StringStore
/// columns grow internally and are left alone.
fn reserveAggressive(allocator: Allocator, out: *ColumnStore, add_rows: usize) !void {
    switch (out.data) {
        .varchar, .string, .char, .json => {},
        inline else => |*l| {
            const need = l.items.len + add_rows;
            if (need > l.capacity) try l.ensureTotalCapacity(allocator, @max(need, l.capacity * 4));
        },
    }
}

/// Borrowed sub-range view over an emitted column — the zero-copy emit
/// path. `off` must be a multiple of 8 so the validity bitmap slices on a
/// byte boundary (the emit batch size guarantees it). String offsets are
/// absolute into the full bytes buffer, so the offsets window slices
/// without rebasing.
fn subView(v: ColumnView, off: usize, n: usize) ColumnView {
    return transform.subViewAligned(v, off, n);
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
            // NULL keys form ONE partition (NULLs are "not distinct"), so the
            // boundary check must be validity-aware — a NULL slot's payload
            // bytes would otherwise split or merge partitions arbitrarily.
            if (transform.compareInColumnNullsFirst(cols[ci], ref, perm[e]) != .eq) return e;
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
        if (transform.compareInColumnNullsFirst(cols[ci], a, b) != .eq) return false;
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
pub fn orderPrefix(col: ColumnStore, row: u32, desc: bool) u64 {
    const SIGN64: u64 = 1 << 63;
    // NULL normalizes below every value (prefix 0 pre-flip): it can only TIE
    // with a real minimum's prefix, and ties resolve through the exact
    // validity-aware comparator — the prefix stays order-consistent.
    if (!transform.rowIsValid(col, row)) return if (desc) ~@as(u64, 0) else 0;
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
        .varchar, .string, .char, .json => |s| stringPrefix(s.rowBytesWide(row)),
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
    // A NULL slot's payload bytes are encoding artifacts — digest a sentinel
    // instead so every NULL key lands in the same partition bucket and never
    // shares one with a real value (digest ties fall back to the validity-
    // aware comparator, so a sentinel collision stays correct).
    if (!transform.rowIsValid(col, row)) {
        hashInt(h, @bitCast(@as(u64, 0x6e756c6c_6b657921))); // "nullkey!"
        return;
    }
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
        .varchar, .string, .char, .json => |s| {
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
            .int => |x| @floatFromInt(x),
            .bigint => |x| @floatFromInt(x),
            .smallint => |x| @floatFromInt(x),
            .tinyint => |x| @floatFromInt(x),
            else => return Error.WindowUnsupported,
        },
        .float => |*l| l.items[row] = switch (lit) {
            .float => |x| x,
            .double => |x| @floatCast(x),
            .int => |x| @floatFromInt(x),
            .bigint => |x| @floatFromInt(x),
            .smallint => |x| @floatFromInt(x),
            .tinyint => |x| @floatFromInt(x),
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
        .varchar, .string, .char, .json => return Error.WindowUnsupported,
    }
    setValid(out, out_row);
}
